const std = @import("std");
const builtin = @import("builtin");
const quic_zig = @import("quic_zig");

const endpoint_mod = @import("endpoint.zig");
const length_framer = @import("length_framer.zig");
const listener_mod = @import("listener.zig");
const native_framer = @import("native_framer.zig");
const quic_zig_adapter = @import("quic_zig_adapter.zig");
const quic_options = @import("options.zig");
const quic_close = @import("close.zig");
const outbound_queue = @import("outbound_queue.zig");
const scheduler = @import("scheduler.zig");

const log = std.log.scoped(.rpc_quic_transport);
const Net = std.Io.net;

const ClientOptions = quic_options.ClientOptions;
const ServerOptions = quic_options.ServerOptions;
const TransportMode = quic_options.TransportMode;
const NativeOptions = quic_options.NativeOptions;
const LengthDelimitedFramer = length_framer.LengthDelimitedFramer;
const NativeControlFramer = native_framer.ControlFramer;
const OutboundQueue = outbound_queue.OutboundQueue;

const Role = endpoint_mod.Role;
const Endpoint = endpoint_mod.Endpoint;

const NativeQueuedKind = enum {
    inline_rpc,
    data_rpc,
};

const NativeQueuedFrame = struct {
    sequence: u64,
    bytes: []u8,
    kind: NativeQueuedKind,
    control: ?[]u8 = null,
    control_offset: usize = 0,
    data_offset: usize = 0,
    stream_id: ?u64 = null,
    stream_finished: bool = false,

    fn free(self: *NativeQueuedFrame, allocator: std.mem.Allocator) void {
        if (self.control) |control| allocator.free(control);
        allocator.free(self.bytes);
        self.* = undefined;
    }
};

const NativeOutboundQueue = struct {
    mu: std.atomic.Mutex = .unlocked,
    items: std.ArrayListUnmanaged(NativeQueuedFrame) = .empty,
    head: usize = 0,
    queued_items: usize = 0,
    queued_bytes: usize = 0,
    queued_data_items: usize = 0,
    queued_data_bytes: usize = 0,
    max_items: usize = quic_options.default_max_outbound_queue_items,
    max_bytes: usize = quic_options.default_max_outbound_queue_bytes,
    inline_threshold: usize = quic_options.default_native_inline_frame_threshold,
    max_control_frame_bytes: usize = quic_options.default_native_max_control_frame_bytes,
    max_pending_data_streams: usize = quic_options.default_native_max_pending_data_streams,
    max_pending_data_bytes: usize = quic_options.default_native_max_pending_data_bytes,
    next_sequence: u64 = 0,
    next_uni_stream_id: u64 = 2,
    closed: bool = false,
    flushing: bool = false,

    pub fn init(
        role: Role,
        max_items: usize,
        max_bytes: usize,
        options: NativeOptions,
    ) NativeOutboundQueue {
        return .{
            .max_items = max_items,
            .max_bytes = max_bytes,
            .inline_threshold = options.inline_frame_threshold,
            .max_control_frame_bytes = options.max_control_frame_bytes,
            .max_pending_data_streams = options.max_pending_data_streams,
            .max_pending_data_bytes = options.max_pending_data_bytes,
            .next_uni_stream_id = switch (role) {
                .client => 2,
                .server => 3,
            },
        };
    }

    fn lock(self: *NativeOutboundQueue) void {
        while (!self.mu.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn enqueue(
        self: *NativeOutboundQueue,
        allocator: std.mem.Allocator,
        frame: []const u8,
    ) error{ BrokenPipe, OutboundQueueFull, OutOfMemory }!void {
        self.lock();
        defer self.mu.unlock();
        if (self.closed) return error.BrokenPipe;

        const kind: NativeQueuedKind = if (frame.len <= self.inline_threshold) .inline_rpc else .data_rpc;
        const new_bytes = std.math.add(usize, self.queued_bytes, frame.len) catch return error.OutboundQueueFull;
        if (self.queued_items >= self.max_items or new_bytes > self.max_bytes) {
            return error.OutboundQueueFull;
        }
        if (kind == .data_rpc) {
            const new_data_items = std.math.add(usize, self.queued_data_items, 1) catch return error.OutboundQueueFull;
            const new_data_bytes = std.math.add(usize, self.queued_data_bytes, frame.len) catch return error.OutboundQueueFull;
            if (new_data_items > self.max_pending_data_streams or new_data_bytes > self.max_pending_data_bytes) {
                return error.OutboundQueueFull;
            }
        }

        const bytes = allocator.alloc(u8, frame.len) catch return error.OutOfMemory;
        errdefer allocator.free(bytes);
        std.mem.copyForwards(u8, bytes, frame);

        self.items.append(allocator, .{
            .sequence = self.next_sequence,
            .bytes = bytes,
            .kind = kind,
        }) catch return error.OutOfMemory;
        self.next_sequence +%= 1;
        self.queued_items += 1;
        self.queued_bytes = new_bytes;
        if (kind == .data_rpc) {
            self.queued_data_items += 1;
            self.queued_data_bytes += frame.len;
        }
    }

    pub fn close(self: *NativeOutboundQueue) void {
        self.lock();
        self.closed = true;
        self.mu.unlock();
    }

    pub fn isEmpty(self: *NativeOutboundQueue) bool {
        self.lock();
        defer self.mu.unlock();
        return self.queued_items == 0;
    }

    pub fn drain(self: *NativeOutboundQueue, allocator: std.mem.Allocator) void {
        self.lock();
        defer self.mu.unlock();
        for (self.items.items[self.head..]) |*item| item.free(allocator);
        self.items.deinit(allocator);
        self.items = .empty;
        self.head = 0;
        self.queued_items = 0;
        self.queued_bytes = 0;
        self.queued_data_items = 0;
        self.queued_data_bytes = 0;
    }

    pub fn flush(
        self: *NativeOutboundQueue,
        allocator: std.mem.Allocator,
        conn: *quic_zig.Connection,
    ) !void {
        if (!self.beginFlush()) return;
        defer self.endFlush();

        while (true) {
            var item = self.takeFront() orelse return;
            const complete = self.flushItem(allocator, conn, &item) catch |err| {
                self.requeueFront(allocator, item);
                return err;
            };
            if (!complete) {
                self.requeueFront(allocator, item);
                return;
            }
            self.releaseItem(allocator, item);
        }
    }

    fn beginFlush(self: *NativeOutboundQueue) bool {
        self.lock();
        defer self.mu.unlock();
        if (self.flushing) return false;
        self.flushing = true;
        return true;
    }

    fn endFlush(self: *NativeOutboundQueue) void {
        self.lock();
        self.flushing = false;
        self.mu.unlock();
    }

    fn flushItem(
        self: *NativeOutboundQueue,
        allocator: std.mem.Allocator,
        conn: *quic_zig.Connection,
        item: *NativeQueuedFrame,
    ) !bool {
        if (item.kind == .data_rpc) {
            if (!try self.flushDataStream(conn, item)) return false;
        }
        if (item.control == null) {
            item.control = switch (item.kind) {
                .inline_rpc => try native_framer.encodeInlineRpc(
                    allocator,
                    item.sequence,
                    item.bytes,
                    self.max_control_frame_bytes,
                ),
                .data_rpc => try native_framer.encodeDataRpc(
                    allocator,
                    item.sequence,
                    item.stream_id orelse return error.InvalidFrame,
                    item.bytes.len,
                    self.max_control_frame_bytes,
                ),
            };
        }
        return try writeQueuedControl(conn, item);
    }

    fn flushDataStream(
        self: *NativeOutboundQueue,
        conn: *quic_zig.Connection,
        item: *NativeQueuedFrame,
    ) !bool {
        if (item.stream_id == null) {
            const stream_id = self.next_uni_stream_id;
            _ = conn.openUni(stream_id) catch |err| switch (err) {
                error.StreamLimitExceeded => return false,
                else => return err,
            };
            item.stream_id = stream_id;
            self.next_uni_stream_id +%= 4;
        }

        const stream_id = item.stream_id.?;
        while (item.data_offset < item.bytes.len) {
            const written = conn.streamWrite(stream_id, item.bytes[item.data_offset..]) catch |err| switch (err) {
                error.StreamNotFound => return error.InvalidFrame,
                else => return err,
            };
            if (written == 0) return false;
            item.data_offset += written;
        }
        if (!item.stream_finished) {
            try conn.streamFinish(stream_id);
            item.stream_finished = true;
        }
        return true;
    }

    fn writeQueuedControl(conn: *quic_zig.Connection, item: *NativeQueuedFrame) !bool {
        const control = item.control orelse return error.InvalidFrame;
        while (item.control_offset < control.len) {
            const written = conn.streamWrite(quic_options.baseline_stream_id, control[item.control_offset..]) catch |err| switch (err) {
                error.StreamNotFound => return false,
                else => return err,
            };
            if (written == 0) return false;
            item.control_offset += written;
        }
        return true;
    }

    fn takeFront(self: *NativeOutboundQueue) ?NativeQueuedFrame {
        self.lock();
        defer self.mu.unlock();
        if (self.head >= self.items.items.len) return null;
        const item = self.items.items[self.head];
        self.head += 1;
        return item;
    }

    fn requeueFront(
        self: *NativeOutboundQueue,
        allocator: std.mem.Allocator,
        item: NativeQueuedFrame,
    ) void {
        var free_item = false;

        self.lock();
        if (self.closed) {
            self.subtractItemCounts(item);
            free_item = true;
        } else {
            std.debug.assert(self.head > 0);
            self.head -= 1;
            self.items.items[self.head] = item;
        }
        self.mu.unlock();

        if (free_item) {
            var owned = item;
            owned.free(allocator);
        }
    }

    fn releaseItem(
        self: *NativeOutboundQueue,
        allocator: std.mem.Allocator,
        item: NativeQueuedFrame,
    ) void {
        self.lock();
        self.subtractItemCounts(item);
        self.compactIfNeeded();
        self.mu.unlock();

        var owned = item;
        owned.free(allocator);
    }

    fn subtractItemCounts(self: *NativeOutboundQueue, item: NativeQueuedFrame) void {
        self.queued_items -= 1;
        self.queued_bytes -= item.bytes.len;
        if (item.kind == .data_rpc) {
            self.queued_data_items -= 1;
            self.queued_data_bytes -= item.bytes.len;
        }
    }

    fn compactIfNeeded(self: *NativeOutboundQueue) void {
        if (self.head == 0) return;
        if (self.head == self.items.items.len) {
            self.items.clearRetainingCapacity();
            self.head = 0;
            return;
        }
        if (self.head < 32 or self.head < self.items.items.len - self.head) return;

        const live = self.items.items.len - self.head;
        std.mem.copyForwards(
            NativeQueuedFrame,
            self.items.items[0..live],
            self.items.items[self.head..],
        );
        self.items.items.len = live;
        self.head = 0;
    }
};

const NativePendingData = struct {
    sequence: u64,
    stream_id: u64,
    bytes: []u8,
    offset: usize = 0,
};

const NativeState = struct {
    control_ready: bool = false,
    preamble: [native_framer.preface.len + native_framer.encodedHelloLen()]u8 = undefined,
    preamble_len: usize = 0,
    preamble_offset: usize = 0,
    received_preface_len: usize = 0,
    hello_received: bool = false,
    inbound: NativeControlFramer,
    outbound: NativeOutboundQueue,
    next_in_sequence: u64 = 0,
    pending_data: ?NativePendingData = null,

    pub fn init(
        allocator: std.mem.Allocator,
        role: Role,
        max_message_bytes: usize,
        max_outbound_queue_items: usize,
        max_outbound_queue_bytes: usize,
        native_options: NativeOptions,
    ) NativeState {
        return .{
            .inbound = NativeControlFramer.init(allocator, .{
                .max_control_frame_bytes = native_options.max_control_frame_bytes,
                .max_rpc_frame_bytes = max_message_bytes,
            }),
            .outbound = NativeOutboundQueue.init(
                role,
                max_outbound_queue_items,
                max_outbound_queue_bytes,
                native_options,
            ),
        };
    }

    pub fn deinit(self: *NativeState, allocator: std.mem.Allocator) void {
        self.inbound.deinit();
        self.outbound.drain(allocator);
        if (self.pending_data) |pending| allocator.free(pending.bytes);
        self.pending_data = null;
    }

    pub fn resetInbound(self: *NativeState, allocator: std.mem.Allocator) void {
        self.inbound.reset();
        if (self.pending_data) |pending| allocator.free(pending.bytes);
        self.pending_data = null;
    }
};

/// A single vat-to-vat Cap'n Proto RPC session over QUIC.
///
/// Client-side instances own one `ClientEndpoint`. Server-side compatibility
/// instances own one `Listener` and attach the first accepted `Session` to the
/// peer-facing callbacks. This intentionally preserves the first QUIC design
/// step as "one QUIC connection equals one authenticated vat session" while
/// making the future listener fanout boundary explicit.
pub const Connection = struct {
    pub const StepMode = scheduler.StepMode;
    pub const StepResult = scheduler.StepResult;

    allocator: std.mem.Allocator,
    io: std.Io,
    role: Role,
    endpoint: Endpoint,
    udp_rx_buf: []u8,
    udp_tx_buf: []u8,
    stream_read_buf: []u8,
    max_message_bytes: usize,
    mode: TransportMode,
    inbound: LengthDelimitedFramer,
    outbound: OutboundQueue = .{},
    native_state: NativeState,
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    wake_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// POSIX-only run-loop wake pipe. [0] is read by the run loop; [1] is
    /// written by cross-thread `sendFrame()` / `requestClose()` calls.
    wake_fds: ?[2]std.posix.fd_t = null,
    reveal_close_reason_on_wire: bool = false,
    close_status: ?quic_close.Status = null,
    close_reason_buf: [quic_close.max_wire_reason_bytes]u8 = undefined,
    close_reason_len: usize = 0,
    baseline_ready: bool = false,

    ctx: ?*anyopaque = null,
    on_message: ?*const fn (conn: *Connection, frame: []const u8) anyerror!void = null,
    on_error: ?*const fn (conn: *Connection, err: anyerror) void = null,
    on_close: ?*const fn (conn: *Connection) void = null,
    callback_depth: usize = 0,
    deinit_requested: bool = false,
    deinitialized: bool = false,

    owner_thread_id: ?std.Thread.Id = null,

    pub fn initClient(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: ClientOptions,
    ) !Connection {
        try quic_options.validateClientOptions(options);

        const local_addr = options.local_addr orelse quic_zig_adapter.defaultClientBindAddress(options.remote_addr);
        const socket = try Net.IpAddress.bind(&local_addr, io, .{
            .mode = .dgram,
            .protocol = .udp,
        });
        errdefer socket.close(io);

        var client = try quic_zig.Client.connect(.{
            .allocator = allocator,
            .server_name = options.server_name,
            .alpn_protocols = options.alpn_protocols,
            .transport_params = options.transport_params,
            .ca_pem = options.ca_pem,
        });
        errdefer client.deinit();

        return try initCommon(
            allocator,
            io,
            .client,
            .{ .client = .{
                .socket = socket,
                .transport = client,
                .remote_addr = options.remote_addr,
                .start_timestamp = std.Io.Timestamp.now(io, .awake),
                .receive_timeout = options.receive_timeout,
            } },
            options.udp_rx_buffer_size,
            options.udp_tx_buffer_size,
            options.stream_read_buffer_size,
            options.max_message_bytes,
            options.max_outbound_queue_items,
            options.max_outbound_queue_bytes,
            options.mode,
            options.native,
            false,
        );
    }

    pub fn initServer(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: ServerOptions,
    ) !Connection {
        var listener = try listener_mod.Listener.init(allocator, io, options);
        errdefer listener.deinit();

        return try initCommon(
            allocator,
            io,
            .server,
            .{ .server = .{ .listener = listener } },
            options.udp_rx_buffer_size,
            options.udp_tx_buffer_size,
            options.stream_read_buffer_size,
            options.max_message_bytes,
            options.max_outbound_queue_items,
            options.max_outbound_queue_bytes,
            options.mode,
            options.native,
            options.reveal_close_reason_on_wire,
        );
    }

    fn initCommon(
        allocator: std.mem.Allocator,
        io: std.Io,
        role: Role,
        endpoint: Endpoint,
        udp_rx_buffer_size: usize,
        udp_tx_buffer_size: usize,
        stream_read_buffer_size: usize,
        max_message_bytes: usize,
        max_outbound_queue_items: usize,
        max_outbound_queue_bytes: usize,
        mode: TransportMode,
        native_options: NativeOptions,
        reveal_close_reason_on_wire: bool,
    ) !Connection {
        const udp_rx_buf = try allocator.alloc(u8, udp_rx_buffer_size);
        errdefer allocator.free(udp_rx_buf);
        const udp_tx_buf = try allocator.alloc(u8, udp_tx_buffer_size);
        errdefer allocator.free(udp_tx_buf);
        const stream_read_buf = try allocator.alloc(u8, stream_read_buffer_size);
        errdefer allocator.free(stream_read_buf);

        return .{
            .allocator = allocator,
            .io = io,
            .role = role,
            .endpoint = endpoint,
            .udp_rx_buf = udp_rx_buf,
            .udp_tx_buf = udp_tx_buf,
            .stream_read_buf = stream_read_buf,
            .max_message_bytes = max_message_bytes,
            .mode = mode,
            .inbound = LengthDelimitedFramer.init(allocator, max_message_bytes),
            .outbound = OutboundQueue.init(max_outbound_queue_items, max_outbound_queue_bytes),
            .native_state = NativeState.init(
                allocator,
                role,
                max_message_bytes,
                max_outbound_queue_items,
                max_outbound_queue_bytes,
                native_options,
            ),
            .wake_fds = createWakeFds(),
            .reveal_close_reason_on_wire = reveal_close_reason_on_wire,
            .owner_thread_id = if (comptime builtin.target.os.tag == .freestanding) null else std.Thread.getCurrentId(),
        };
    }

    fn createWakeFds() ?[2]std.posix.fd_t {
        if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return null;
        var fds: [2]std.posix.fd_t = undefined;
        if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
            return null;
        }
        setNonBlocking(fds[0]);
        setNonBlocking(fds[1]);
        return fds;
    }

    fn setNonBlocking(fd: std.posix.fd_t) void {
        if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
        const rc = std.posix.system.fcntl(fd, std.posix.F.GETFL, @as(usize, 0));
        if (std.posix.errno(rc) != .SUCCESS) return;
        var flags: usize = @intCast(rc);
        flags |= @as(usize, 1 << @bitOffsetOf(std.posix.O, "NONBLOCK"));
        _ = std.posix.system.fcntl(fd, std.posix.F.SETFL, flags);
    }

    pub fn deinit(self: *Connection) void {
        if (self.deinitialized) return;
        if (self.callback_depth != 0) {
            self.deinit_requested = true;
            self.requestClose();
            return;
        }
        self.deinitNow(true);
    }

    fn deinitNow(self: *Connection, comptime check_affinity: bool) void {
        if (self.deinitialized) return;
        if (check_affinity) self.assertThreadAffinity();
        self.deinitialized = true;
        self.deinit_requested = false;
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
        self.outbound.drain(self.allocator);
        self.inbound.deinit();
        self.native_state.outbound.close();
        self.native_state.deinit(self.allocator);
        self.ctx = null;
        self.on_message = null;
        self.on_error = null;
        self.on_close = null;
        self.closeWakeFds();
        switch (self.endpoint) {
            .client => |*client| client.deinit(self.io),
            .server => |*server| server.deinit(),
        }
        self.allocator.free(self.udp_rx_buf);
        self.allocator.free(self.udp_tx_buf);
        self.allocator.free(self.stream_read_buf);
    }

    fn closeWakeFds(self: *Connection) void {
        if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
        const fds = self.wake_fds orelse return;
        self.wake_fds = null;
        _ = std.posix.system.close(fds[0]);
        _ = std.posix.system.close(fds[1]);
    }

    pub fn start(
        self: *Connection,
        ctx: *anyopaque,
        on_message: *const fn (conn: *Connection, frame: []const u8) anyerror!void,
        on_error: *const fn (conn: *Connection, err: anyerror) void,
        on_close: *const fn (conn: *Connection) void,
    ) void {
        self.assertThreadAffinity();
        self.ctx = ctx;
        self.on_message = on_message;
        self.on_error = on_error;
        self.on_close = on_close;
    }

    pub fn run(self: *Connection) void {
        while (!self.close_requested.load(.acquire)) {
            self.step() catch |err| {
                log.debug("QUIC connection step failed: {}", .{err});
                self.terminateInternalError(err);
                break;
            };
            if (self.activeQuicConn()) |conn| {
                if (conn.isClosed() and self.selectedOutboundEmpty()) {
                    _ = self.close_requested.swap(true, .acq_rel);
                }
            }
        }

        self.flushCloseDatagram();
        self.outbound.close();
        self.native_state.outbound.close();
        if (self.on_close) |cb| self.invokeCloseCallback(cb);
        self.completeDeferredDeinit();
    }

    pub fn sendFrame(self: *Connection, frame: []const u8) !void {
        if (self.close_requested.load(.acquire)) return error.BrokenPipe;
        if (frame.len == 0 or frame.len > self.max_message_bytes) return error.FrameTooLarge;
        if (frame.len > std.math.maxInt(u32)) return error.FrameTooLarge;

        switch (self.mode) {
            .baseline => try self.outbound.enqueue(self.allocator, frame),
            .native => try self.native_state.outbound.enqueue(self.allocator, frame),
        }
        self.wake();
    }

    /// Wake a blocked `run()` loop from any thread.
    ///
    /// POSIX builds use a socketpair so the UDP wait is interrupted
    /// immediately. Other targets keep a wake flag and the scheduler caps
    /// blocking waits to a short interval until a native event primitive lands.
    pub fn wake(self: *Connection) void {
        if (self.wake_requested.swap(true, .acq_rel)) return;
        const fds = self.wake_fds orelse return;
        writeWakeByte(fds[1]);
    }

    pub fn close(self: *Connection) void {
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
        self.native_state.outbound.close();
        self.closeActiveQuicConn(.normal, null);
        self.wake();
    }

    pub fn requestClose(self: *Connection) void {
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
        self.native_state.outbound.close();
        self.recordCloseStatus(.normal, null);
        self.wake();
    }

    pub fn isClosing(self: *const Connection) bool {
        return self.close_requested.load(.acquire);
    }

    pub fn closeStatus(self: *const Connection) ?quic_close.Status {
        return self.close_status;
    }

    pub fn getAddress(self: *const Connection) Net.IpAddress {
        return switch (self.endpoint) {
            .client => |*client| client.getAddress(),
            .server => |*server| server.listener.getAddress(),
        };
    }

    pub fn assertThreadAffinity(self: *const Connection) void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        if (builtin.mode == .Debug) {
            const owner = self.owner_thread_id orelse return;
            const current = std.Thread.getCurrentId();
            if (current != owner) {
                @panic("QUIC Connection method called from wrong thread");
            }
        }
    }

    pub fn step(self: *Connection) !void {
        _ = try self.stepOnce(.wait);
    }

    pub fn stepOnce(self: *Connection, mode: StepMode) !StepResult {
        var now_us = self.nowUs();
        const next_deadline_us = self.nextTimerDeadlineUs(now_us);
        const waited_for = scheduler.receiveWaitDuration(.{
            .mode = mode,
            .receive_timeout = self.receiveTimeout(),
            .now_us = now_us,
            .next_deadline_us = next_deadline_us,
            .immediate_work = self.hasImmediateWork(),
            .wake_supported = self.hasWakeSupport(),
        });
        var result = StepResult{
            .waited_for = waited_for,
            .next_deadline_us = next_deadline_us,
        };
        const receive_result = try self.receiveOne(now_us, waited_for);
        result.received_datagram = receive_result.received_datagram;
        result.wake_drained = receive_result.wake_drained;

        now_us = self.nowUs();
        try self.advanceActive();
        try self.serviceModeStreams();
        try self.drainOutgoingDatagrams(now_us);

        now_us = self.nowUs();
        try self.tickActive(now_us);
        try self.serviceModeStreams();
        try self.drainOutgoingDatagrams(now_us);

        self.reapServerIfClosed();
        result.closed = self.isTransportDrainedClosed();
        if (result.closed) {
            _ = self.close_requested.swap(true, .acq_rel);
        }
        return result;
    }

    fn flushCloseDatagram(self: *Connection) void {
        const conn = self.activeQuicConn() orelse return;
        if (!conn.isClosed()) {
            self.recordCloseStatus(.normal, null);
            self.closeQuicConn(conn);
        }
        self.drainOutgoingDatagrams(self.nowUs()) catch |err| {
            log.debug("failed to flush QUIC close datagram: {}", .{err});
        };
    }

    const ReceiveResult = struct {
        received_datagram: bool = false,
        wake_drained: bool = false,
    };

    fn receiveOne(self: *Connection, now_us: u64, wait_duration: std.Io.Duration) !ReceiveResult {
        var result = ReceiveResult{};
        if (self.consumeWakeRequested()) {
            result.wake_drained = true;
            return result;
        }

        var receive_timeout = wait_duration;
        if (try self.waitForUdpOrWake(wait_duration)) |poll_result| {
            result.wake_drained = poll_result.wake_drained;
            if (!poll_result.socket_ready) return result;
            receive_timeout = std.Io.Duration.zero;
        }

        const socket = self.activeSocket();
        const msg = socket.receiveTimeout(self.io, self.udp_rx_buf, .{
            .duration = .{
                .raw = receive_timeout,
                .clock = .awake,
            },
        }) catch |err| switch (err) {
            error.Timeout => return result,
            else => return err,
        };
        if (msg.flags.trunc) return error.DatagramTooLarge;
        result.received_datagram = true;

        switch (self.endpoint) {
            .client => |*client| {
                const remote = client.remote_addr;
                if (!Net.IpAddress.eql(&msg.from, &remote)) return result;
                try client.transport.conn.handle(msg.data, quic_zig_adapter.ipAddressToPathAddress(msg.from), now_us);
            },
            .server => |*server| {
                _ = try server.listener.feedDatagram(msg.data, msg.from, now_us);
                self.setServerSlotIfAccepted();
                try server.listener.drainStatelessResponses();
            },
        }
        return result;
    }

    const PollResult = struct {
        socket_ready: bool = false,
        wake_drained: bool = false,
    };

    fn waitForUdpOrWake(self: *Connection, wait_duration: std.Io.Duration) !?PollResult {
        if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return null;
        const fds = self.wake_fds orelse return null;
        const socket = self.activeSocket();

        var poll_fds = [2]std.posix.pollfd{
            .{ .fd = socket.handle, .events = std.posix.POLL.IN, .revents = 0 },
            .{ .fd = fds[0], .events = std.posix.POLL.IN, .revents = 0 },
        };
        const timeout_ms = scheduler.durationToPollTimeoutMs(wait_duration);
        while (true) {
            const rc = std.posix.system.poll(@ptrCast(&poll_fds), poll_fds.len, timeout_ms);
            if (rc > 0) break;
            if (rc == 0) return PollResult{};
            if (std.posix.errno(rc) == .INTR) continue;
            return error.PollFailed;
        }

        var result = PollResult{};
        if (poll_fds[1].revents & (std.posix.POLL.IN | std.posix.POLL.HUP | std.posix.POLL.ERR) != 0) {
            self.drainWakePipe();
            _ = self.wake_requested.swap(false, .acq_rel);
            result.wake_drained = true;
        }
        if (poll_fds[0].revents & std.posix.POLL.NVAL != 0) return error.BrokenPipe;
        result.socket_ready =
            poll_fds[0].revents & (std.posix.POLL.IN | std.posix.POLL.HUP | std.posix.POLL.ERR) != 0;
        return result;
    }

    fn consumeWakeRequested(self: *Connection) bool {
        if (!self.wake_requested.swap(false, .acq_rel)) return false;
        self.drainWakePipe();
        return true;
    }

    fn drainWakePipe(self: *Connection) void {
        if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
        const fds = self.wake_fds orelse return;
        var buf: [64]u8 = undefined;
        while (true) {
            const rc = std.posix.system.read(fds[0], &buf, buf.len);
            if (rc > 0) continue;
            if (rc == 0) return;
            switch (std.posix.errno(rc)) {
                .INTR => continue,
                .AGAIN => return,
                else => return,
            }
        }
    }

    fn writeWakeByte(fd: std.posix.fd_t) void {
        if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
        const byte = [_]u8{1};
        while (true) {
            const rc = std.posix.system.write(fd, &byte, byte.len);
            if (rc > 0) return;
            if (rc == 0) return;
            switch (std.posix.errno(rc)) {
                .INTR => continue,
                .AGAIN => return,
                else => return,
            }
        }
    }

    fn hasWakeSupport(self: *const Connection) bool {
        return self.wake_fds != null;
    }

    fn receiveTimeout(self: *const Connection) std.Io.Duration {
        return switch (self.endpoint) {
            .client => |*client| client.receive_timeout,
            .server => |*server| server.listener.receive_timeout,
        };
    }

    fn activeSocket(self: *Connection) *Net.Socket {
        return switch (self.endpoint) {
            .client => |*client| &client.socket,
            .server => |*server| &server.listener.socket,
        };
    }

    fn hasImmediateWork(self: *Connection) bool {
        if (self.wake_requested.load(.acquire)) return true;
        if (self.activeQuicConn()) |conn| {
            if (conn.canSend()) return true;
            if (!self.selectedOutboundEmpty()) {
                switch (self.mode) {
                    .baseline => {
                        if (self.baseline_ready) return true;
                        if (self.role == .client and conn.handshakeDone()) return true;
                    },
                    .native => {
                        if (self.native_state.control_ready) return true;
                        if (self.role == .client and conn.handshakeDone()) return true;
                    },
                }
            }
        }
        return false;
    }

    fn nextTimerDeadlineUs(self: *Connection, now_us: u64) ?u64 {
        const conn = self.activeQuicConn() orelse return null;
        const deadline = conn.nextTimerDeadline(now_us) orelse return null;
        return deadline.at_us;
    }

    fn isTransportDrainedClosed(self: *Connection) bool {
        const conn = self.activeQuicConn() orelse return false;
        return conn.isClosed() and self.selectedOutboundEmpty();
    }

    fn selectedOutboundEmpty(self: *Connection) bool {
        return switch (self.mode) {
            .baseline => self.outbound.isEmpty(),
            .native => self.native_state.outbound.isEmpty(),
        };
    }

    fn advanceActive(self: *Connection) !void {
        const conn = self.activeQuicConn() orelse return;
        if (!conn.handshakeDone()) {
            try conn.advance();
        }
    }

    fn tickActive(self: *Connection, now_us: u64) !void {
        const conn = self.activeQuicConn() orelse return;
        try conn.tick(now_us);
    }

    fn serviceModeStreams(self: *Connection) !void {
        switch (self.mode) {
            .baseline => try self.serviceBaselineStream(),
            .native => try self.serviceNativeStreams(),
        }
    }

    fn serviceBaselineStream(self: *Connection) !void {
        const conn = self.activeQuicConn() orelse return;
        if (!try self.ensureBaselineStream(conn)) return;
        try self.outbound.flush(self.allocator, conn);
        try self.readBaselineStream(conn);
    }

    fn ensureBaselineStream(self: *Connection, conn: *quic_zig.Connection) !bool {
        if (self.baseline_ready) return true;
        if (conn.stream(quic_options.baseline_stream_id) != null) {
            self.baseline_ready = true;
            return true;
        }
        if (self.role == .client) {
            if (!conn.handshakeDone()) return false;
            _ = conn.openBidi(quic_options.baseline_stream_id) catch |err| switch (err) {
                error.StreamAlreadyOpen => {},
                else => return err,
            };
            self.baseline_ready = true;
            return true;
        }
        return false;
    }

    fn readBaselineStream(self: *Connection, conn: *quic_zig.Connection) !void {
        while (!self.close_requested.load(.acquire)) {
            const n = conn.streamRead(quic_options.baseline_stream_id, self.stream_read_buf) catch |err| switch (err) {
                error.StreamNotFound => return,
                else => return err,
            };
            if (n == 0) return;
            try self.inbound.push(self.stream_read_buf[0..n]);
            try self.dispatchAvailableFrames();
        }
    }

    fn dispatchAvailableFrames(self: *Connection) !void {
        while (true) {
            if (self.on_message == null or self.on_error == null) return;
            const frame = self.inbound.popFrame() catch |err| {
                if (err == error.OutOfMemory) {
                    self.terminateFrameError(err);
                    return;
                }
                self.terminateFrameError(err);
                return;
            };
            const bytes = frame orelse return;
            defer self.allocator.free(bytes);
            try self.dispatchRpcFrame(bytes);
            if (self.deinit_requested) return;
        }
    }

    fn dispatchRpcFrame(self: *Connection, frame: []const u8) !void {
        if (self.on_message == null or self.on_error == null) return;
        self.invokeMessageCallback(self.on_message.?, frame) catch |err| {
            self.terminateCallbackError(err);
            return;
        };
    }

    fn serviceNativeStreams(self: *Connection) !void {
        const conn = self.activeQuicConn() orelse return;
        if (!try self.ensureNativeControlStream(conn)) return;
        if (!try self.flushNativePreamble(conn)) return;
        try self.native_state.outbound.flush(self.allocator, conn);
        self.readNativeControlStream(conn) catch |err| {
            if (isNativeFrameError(err)) {
                self.terminateFrameError(err);
                return;
            }
            return err;
        };
        self.processNativeControlFrames(conn) catch |err| {
            if (isNativeFrameError(err)) {
                self.terminateFrameError(err);
                return;
            }
            return err;
        };
    }

    fn ensureNativeControlStream(self: *Connection, conn: *quic_zig.Connection) !bool {
        if (self.native_state.control_ready) return true;
        if (conn.stream(quic_options.baseline_stream_id) != null) {
            self.native_state.control_ready = true;
            return true;
        }
        if (self.role == .client) {
            if (!conn.handshakeDone()) return false;
            _ = conn.openBidi(quic_options.baseline_stream_id) catch |err| switch (err) {
                error.StreamAlreadyOpen => {},
                else => return err,
            };
            self.native_state.control_ready = true;
            return true;
        }
        return false;
    }

    fn flushNativePreamble(self: *Connection, conn: *quic_zig.Connection) !bool {
        var native_state = &self.native_state;
        if (native_state.preamble_offset == native_state.preamble_len and native_state.preamble_len != 0) return true;
        if (native_state.preamble_len == 0) {
            @memcpy(native_state.preamble[0..native_framer.preface.len], native_framer.preface);
            const hello_len = try native_framer.encodeHello(native_state.preamble[native_framer.preface.len..]);
            native_state.preamble_len = native_framer.preface.len + hello_len;
        }

        while (native_state.preamble_offset < native_state.preamble_len) {
            const remaining = native_state.preamble[native_state.preamble_offset..native_state.preamble_len];
            const written = conn.streamWrite(quic_options.baseline_stream_id, remaining) catch |err| switch (err) {
                error.StreamNotFound => return false,
                else => return err,
            };
            if (written == 0) return false;
            native_state.preamble_offset += written;
        }
        return true;
    }

    fn readNativeControlStream(self: *Connection, conn: *quic_zig.Connection) !void {
        while (!self.close_requested.load(.acquire)) {
            const n = conn.streamRead(quic_options.baseline_stream_id, self.stream_read_buf) catch |err| switch (err) {
                error.StreamNotFound => return,
                else => return err,
            };
            if (n == 0) return;
            try self.pushNativeControlBytes(self.stream_read_buf[0..n]);
        }
    }

    fn pushNativeControlBytes(self: *Connection, bytes: []const u8) !void {
        var remaining = bytes;
        var native_state = &self.native_state;
        if (native_state.received_preface_len < native_framer.preface.len) {
            const need = native_framer.preface.len - native_state.received_preface_len;
            const take = @min(need, remaining.len);
            const prefix_start = native_state.received_preface_len;
            const prefix_end = prefix_start + take;
            if (!std.mem.eql(u8, remaining[0..take], native_framer.preface[prefix_start..prefix_end])) {
                return error.InvalidFrame;
            }
            native_state.received_preface_len = prefix_end;
            remaining = remaining[take..];
        }
        if (native_state.received_preface_len == native_framer.preface.len and remaining.len > 0) {
            try native_state.inbound.push(remaining);
        }
    }

    fn processNativeControlFrames(self: *Connection, conn: *quic_zig.Connection) !void {
        while (!self.close_requested.load(.acquire)) {
            if (self.native_state.pending_data != null) {
                if (!try self.readPendingNativeData(conn)) return;
                continue;
            }

            const frame = (try self.native_state.inbound.popFrame()) orelse return;
            switch (frame) {
                .hello => |hello| {
                    if (self.native_state.hello_received) return error.InvalidFrame;
                    if (hello.version != native_framer.version) return error.InvalidFrame;
                    self.native_state.hello_received = true;
                },
                .inline_rpc => |inline_frame| {
                    defer frame.deinit(self.allocator);
                    try self.ensureNativeRpcSequence(inline_frame.sequence);
                    self.native_state.next_in_sequence +%= 1;
                    try self.dispatchRpcFrame(inline_frame.frame);
                    if (self.deinit_requested) return;
                },
                .data_rpc => |data| {
                    try self.ensureNativeRpcSequence(data.sequence);
                    try self.startPendingNativeData(data);
                    if (!try self.readPendingNativeData(conn)) return;
                },
            }
        }
    }

    fn ensureNativeRpcSequence(self: *Connection, sequence: u64) !void {
        if (!self.native_state.hello_received) return error.InvalidFrame;
        if (sequence != self.native_state.next_in_sequence) return error.InvalidFrame;
    }

    fn startPendingNativeData(self: *Connection, data: native_framer.DataRpc) !void {
        if (self.native_state.pending_data != null) return error.InvalidFrame;
        if (!self.isPeerInitiatedUniStreamId(data.stream_id)) return error.InvalidFrame;
        if (data.length == 0 or data.length > self.max_message_bytes) return error.FrameTooLarge;
        if (data.length > self.native_state.outbound.max_pending_data_bytes) return error.FrameTooLarge;

        const bytes = try self.allocator.alloc(u8, data.length);
        errdefer self.allocator.free(bytes);
        self.native_state.pending_data = .{
            .sequence = data.sequence,
            .stream_id = data.stream_id,
            .bytes = bytes,
        };
    }

    fn readPendingNativeData(self: *Connection, conn: *quic_zig.Connection) !bool {
        if (self.native_state.pending_data == null) return true;
        var pending = &self.native_state.pending_data.?;
        const stream = conn.stream(pending.stream_id) orelse return false;
        if (stream.recv.final_size) |final_size| {
            if (final_size != pending.bytes.len) return error.InvalidFrame;
        }

        while (pending.offset < pending.bytes.len) {
            const n = conn.streamRead(pending.stream_id, pending.bytes[pending.offset..]) catch |err| switch (err) {
                error.StreamNotFound => return false,
                else => return err,
            };
            if (n == 0) break;
            pending.offset += n;
            if (stream.recv.final_size) |final_size| {
                if (final_size != pending.bytes.len) return error.InvalidFrame;
            }
        }

        if (pending.offset < pending.bytes.len) return false;
        if (stream.recv.final_size == null) return false;

        const bytes = pending.bytes;
        self.native_state.pending_data = null;
        defer self.allocator.free(bytes);
        self.native_state.next_in_sequence +%= 1;
        try self.dispatchRpcFrame(bytes);
        return true;
    }

    fn isPeerInitiatedUniStreamId(self: *const Connection, stream_id: u64) bool {
        const is_uni = (stream_id & 0b10) != 0;
        if (!is_uni) return false;
        const client_initiated = (stream_id & 0b01) == 0;
        return client_initiated != (self.role == .client);
    }

    fn isNativeFrameError(err: anyerror) bool {
        return switch (err) {
            error.InvalidFrame, error.FrameTooLarge, error.OutOfMemory => true,
            else => false,
        };
    }

    fn terminateFrameError(self: *Connection, err: anyerror) void {
        log.debug("fatal QUIC frame error, closing connection: {}", .{err});
        self.closeActiveQuicConn(quic_close.codeForFrameError(err), err);
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
        self.native_state.outbound.close();
        self.inbound.reset();
        self.native_state.resetInbound(self.allocator);
        self.on_message = null;
        const on_error = self.on_error;
        self.on_error = null;
        self.wake();
        if (on_error) |cb| self.invokeErrorCallback(cb, err);
    }

    fn terminateCallbackError(self: *Connection, err: anyerror) void {
        log.debug("QUIC message callback failed, closing connection: {}", .{err});
        self.closeActiveQuicConn(.peer_callback_failure, err);
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
        self.native_state.outbound.close();
        self.on_message = null;
        const on_error = self.on_error;
        self.on_error = null;
        self.wake();
        if (on_error) |cb| self.invokeErrorCallback(cb, err);
    }

    fn terminateInternalError(self: *Connection, err: anyerror) void {
        self.closeActiveQuicConn(quic_close.codeForStepError(err), err);
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
        self.native_state.outbound.close();
        self.wake();
        self.invokeOnError(err);
    }

    fn closeActiveQuicConn(
        self: *Connection,
        code: quic_close.ApplicationCloseCode,
        err: ?anyerror,
    ) void {
        self.recordCloseStatus(code, err);
        const conn = self.activeQuicConn() orelse return;
        self.closeQuicConn(conn);
    }

    fn closeQuicConn(self: *Connection, conn: *quic_zig.Connection) void {
        if (self.close_status == null) {
            self.recordCloseStatus(.normal, null);
        }
        const status = self.close_status orelse return;
        conn.close(false, status.codeValue(), self.close_reason_buf[0..self.close_reason_len]);
    }

    fn recordCloseStatus(
        self: *Connection,
        code: quic_close.ApplicationCloseCode,
        err: ?anyerror,
    ) void {
        if (self.close_status != null) return;
        const reason = quic_close.prepareWireReason(
            &self.close_reason_buf,
            code,
            err,
            self.reveal_close_reason_on_wire,
        );
        self.close_reason_len = reason.len;
        self.close_status = .{
            .code = code,
            .err = err,
            .reason_truncated = reason.truncated,
            .reason_reveals_detail = reason.reveals_detail,
        };
    }

    fn drainOutgoingDatagrams(self: *Connection, now_us: u64) !void {
        switch (self.endpoint) {
            .client => |*client| {
                while (try client.transport.conn.pollDatagram(self.udp_tx_buf, now_us)) |out| {
                    const dest = if (out.to) |addr|
                        quic_zig_adapter.pathAddressToIpAddress(addr) orelse client.remote_addr
                    else
                        client.remote_addr;
                    try client.socket.send(self.io, &dest, self.udp_tx_buf[0..out.len]);
                }
            },
            .server => |*server| {
                const session = server.session.handle() orelse return;
                try server.listener.drainSessionDatagrams(session, self.udp_tx_buf, now_us);
            },
        }
    }

    fn activeQuicConn(self: *Connection) ?*quic_zig.Connection {
        return switch (self.endpoint) {
            .client => |*client| client.transport.conn,
            .server => |*server| server.session.quicConnection(),
        };
    }

    fn setServerSlotIfAccepted(self: *Connection) void {
        switch (self.endpoint) {
            .client => return,
            .server => |*server| {
                _ = server.session.adoptFirstAccepted(&server.listener.server);
            },
        }
    }

    fn reapServerIfClosed(self: *Connection) void {
        switch (self.endpoint) {
            .client => return,
            .server => |*server| {
                if (server.session.handle()) |session| {
                    if (!session.isClosed()) return;
                    const reaped = server.listener.reapClosedSessions();
                    if (reaped > 0) {
                        server.session.clear();
                        _ = self.close_requested.swap(true, .acq_rel);
                    }
                } else {
                    _ = server.listener.reapClosedSessions();
                }
            },
        }
    }

    fn nowUs(self: *Connection) u64 {
        return switch (self.endpoint) {
            .client => |*client| client.nowUs(self.io),
            .server => |*server| server.listener.nowUs(),
        };
    }

    fn invokeOnError(self: *Connection, err: anyerror) void {
        const cb = self.on_error orelse return;
        self.invokeErrorCallback(cb, err);
    }

    fn invokeErrorCallback(
        self: *Connection,
        cb: *const fn (conn: *Connection, callback_err: anyerror) void,
        err: anyerror,
    ) void {
        self.callback_depth += 1;
        defer self.callback_depth -= 1;
        cb(self, err);
    }

    fn invokeMessageCallback(
        self: *Connection,
        cb: *const fn (conn: *Connection, frame: []const u8) anyerror!void,
        frame: []const u8,
    ) !void {
        self.callback_depth += 1;
        defer self.callback_depth -= 1;
        try cb(self, frame);
    }

    fn invokeCloseCallback(
        self: *Connection,
        cb: *const fn (conn: *Connection) void,
    ) void {
        self.callback_depth += 1;
        defer self.callback_depth -= 1;
        cb(self);
    }

    fn completeDeferredDeinit(self: *Connection) void {
        if (self.deinit_requested and self.callback_depth == 0) {
            self.deinitNow(false);
        }
    }
};

fn testRemoteAddr() Net.IpAddress {
    return .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 4433,
    } };
}

fn initTestClient(allocator: std.mem.Allocator) !Connection {
    return try Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = testRemoteAddr(),
        .server_name = "localhost",
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    });
}

fn initTestNativeClient(allocator: std.mem.Allocator, native_options: NativeOptions) !Connection {
    return try Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = testRemoteAddr(),
        .server_name = "localhost",
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        .mode = .native,
        .native = native_options,
    });
}

test "QUIC sendFrame requests scheduler wake" {
    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();

    try std.testing.expect(!conn.wake_requested.load(.acquire));
    try conn.sendFrame("abc");
    try std.testing.expect(conn.wake_requested.load(.acquire));
    try std.testing.expect(!conn.outbound.isEmpty());
    try std.testing.expect(conn.consumeWakeRequested());
    try std.testing.expect(!conn.wake_requested.load(.acquire));
}

test "QUIC native sendFrame uses native outbound queue" {
    var conn = try initTestNativeClient(std.testing.allocator, .{});
    defer conn.deinit();

    try conn.sendFrame("abc");
    try std.testing.expect(conn.wake_requested.load(.acquire));
    try std.testing.expect(conn.outbound.isEmpty());
    try std.testing.expect(!conn.native_state.outbound.isEmpty());
}

test "QUIC native control dispatch preserves order behind pending data stream" {
    const Harness = struct {
        const State = struct {
            messages: usize = 0,
        };

        fn onMessage(conn: *Connection, _: []const u8) !void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.messages += 1;
        }

        fn onError(_: *Connection, _: anyerror) void {}
    };

    var conn = try initTestNativeClient(std.testing.allocator, .{
        .inline_frame_threshold = 16,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 1024,
    });
    defer conn.deinit();

    var state = Harness.State{};
    conn.ctx = &state;
    conn.on_message = Harness.onMessage;
    conn.on_error = Harness.onError;
    conn.native_state.hello_received = true;

    const data_control = try native_framer.encodeDataRpc(std.testing.allocator, 0, 3, 8, 64);
    defer std.testing.allocator.free(data_control);
    const inline_control = try native_framer.encodeInlineRpc(std.testing.allocator, 1, "later", 64);
    defer std.testing.allocator.free(inline_control);

    try conn.native_state.inbound.push(data_control);
    try conn.native_state.inbound.push(inline_control);
    try conn.processNativeControlFrames(conn.activeQuicConn().?);

    try std.testing.expectEqual(@as(usize, 0), state.messages);
    try std.testing.expect(conn.native_state.pending_data != null);
    try std.testing.expectEqual(@as(u64, 0), conn.native_state.next_in_sequence);
    try std.testing.expect(conn.native_state.inbound.buffer.items.len > 0);
}

test "QUIC native control rejects missing and duplicate hello frames" {
    {
        var conn = try initTestNativeClient(std.testing.allocator, .{});
        defer conn.deinit();

        const inline_control = try native_framer.encodeInlineRpc(std.testing.allocator, 0, "rpc", 64);
        defer std.testing.allocator.free(inline_control);

        try conn.native_state.inbound.push(inline_control);
        try std.testing.expectError(
            error.InvalidFrame,
            conn.processNativeControlFrames(conn.activeQuicConn().?),
        );
    }

    {
        var conn = try initTestNativeClient(std.testing.allocator, .{});
        defer conn.deinit();

        var hello: [native_framer.encodedHelloLen()]u8 = undefined;
        const hello_len = try native_framer.encodeHello(&hello);

        try conn.native_state.inbound.push(hello[0..hello_len]);
        try conn.processNativeControlFrames(conn.activeQuicConn().?);
        try std.testing.expect(conn.native_state.hello_received);

        try conn.native_state.inbound.push(hello[0..hello_len]);
        try std.testing.expectError(
            error.InvalidFrame,
            conn.processNativeControlFrames(conn.activeQuicConn().?),
        );
    }
}

test "QUIC native control rejects non-monotonic rpc sequences" {
    var conn = try initTestNativeClient(std.testing.allocator, .{});
    defer conn.deinit();
    conn.native_state.hello_received = true;
    conn.native_state.next_in_sequence = 1;

    const stale_inline = try native_framer.encodeInlineRpc(std.testing.allocator, 0, "stale", 64);
    defer std.testing.allocator.free(stale_inline);

    try conn.native_state.inbound.push(stale_inline);
    try std.testing.expectError(
        error.InvalidFrame,
        conn.processNativeControlFrames(conn.activeQuicConn().?),
    );
    try std.testing.expectEqual(@as(u64, 1), conn.native_state.next_in_sequence);
}

test "QUIC native validates data stream references and budgets" {
    var conn = try initTestNativeClient(std.testing.allocator, .{
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 8,
    });
    defer conn.deinit();

    try std.testing.expectError(error.InvalidFrame, conn.startPendingNativeData(.{
        .sequence = 0,
        .stream_id = 0,
        .length = 4,
    }));
    try std.testing.expectError(error.InvalidFrame, conn.startPendingNativeData(.{
        .sequence = 0,
        .stream_id = 2,
        .length = 4,
    }));
    try std.testing.expectError(error.FrameTooLarge, conn.startPendingNativeData(.{
        .sequence = 0,
        .stream_id = 3,
        .length = 0,
    }));
    try std.testing.expectError(error.FrameTooLarge, conn.startPendingNativeData(.{
        .sequence = 0,
        .stream_id = 3,
        .length = 9,
    }));
    try std.testing.expect(conn.native_state.pending_data == null);
}

test "QUIC native frame errors record typed terminal close status" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var state = Harness.State{};
    var conn = try initTestNativeClient(std.testing.allocator, .{});
    defer conn.deinit();
    conn.ctx = &state;
    conn.on_message = Harness.onMessage;
    conn.on_error = Harness.onError;

    const pending_bytes = try std.testing.allocator.alloc(u8, 4);
    conn.native_state.pending_data = .{
        .sequence = 0,
        .stream_id = 3,
        .bytes = pending_bytes,
    };

    conn.terminateFrameError(error.InvalidFrame);

    try std.testing.expect(conn.isClosing());
    try std.testing.expect(conn.native_state.pending_data == null);
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(quic_close.ApplicationCloseCode.frame_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), status.err);
    try std.testing.expectEqualStrings("rpc frame error", conn.close_reason_buf[0..conn.close_reason_len]);
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC native control allocation OOM is terminal when serviced" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var state = Harness.State{};
    var conn = try initTestNativeClient(std.testing.allocator, .{});
    defer conn.deinit();
    conn.native_state.inbound.deinit();
    conn.native_state.inbound = NativeControlFramer.init(failing.allocator(), .{
        .max_control_frame_bytes = 64,
        .max_rpc_frame_bytes = 64,
    });
    conn.ctx = &state;
    conn.on_message = Harness.onMessage;
    conn.on_error = Harness.onError;
    conn.native_state.hello_received = true;

    const inline_control = try native_framer.encodeInlineRpc(std.testing.allocator, 0, "abc", 64);
    defer std.testing.allocator.free(inline_control);

    try conn.native_state.inbound.push(inline_control);
    conn.processNativeControlFrames(conn.activeQuicConn().?) catch |err| {
        switch (err) {
            error.InvalidFrame, error.FrameTooLarge, error.OutOfMemory => conn.terminateFrameError(err),
            else => return err,
        }
    };

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(quic_close.ApplicationCloseCode.internal_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), status.err);
    try std.testing.expectEqualStrings("rpc transport error", conn.close_reason_buf[0..conn.close_reason_len]);
    try std.testing.expectEqual(@as(usize, 0), conn.native_state.inbound.buffer.items.len);
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC dispatch treats malformed frames as terminal" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var state = Harness.State{};
    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();
    conn.ctx = &state;
    conn.on_message = Harness.onMessage;
    conn.on_error = Harness.onError;

    const bad_frame = [_]u8{ 0, 0, 0, 0 };
    try conn.inbound.push(&bad_frame);
    try conn.dispatchAvailableFrames();

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(quic_close.ApplicationCloseCode.frame_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), status.err);
    try std.testing.expectEqualStrings("rpc frame error", conn.close_reason_buf[0..conn.close_reason_len]);
    try std.testing.expect(!status.reason_reveals_detail);
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC dispatch can reveal sanitized frame error details when configured" {
    const Harness = struct {
        fn onMessage(_: *Connection, _: []const u8) !void {}
        fn onError(_: *Connection, _: anyerror) void {}
    };

    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();
    conn.reveal_close_reason_on_wire = true;
    conn.on_message = Harness.onMessage;
    conn.on_error = Harness.onError;

    const bad_frame = [_]u8{ 0, 0, 0, 0 };
    try conn.inbound.push(&bad_frame);
    try conn.dispatchAvailableFrames();

    const status = conn.closeStatus().?;
    try std.testing.expect(status.reason_reveals_detail);
    try std.testing.expectEqualStrings("rpc frame error: InvalidFrame", conn.close_reason_buf[0..conn.close_reason_len]);
}

test "QUIC dispatch treats inbound frame allocation OOM as terminal" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var state = Harness.State{};
    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();
    conn.inbound.deinit();
    conn.inbound = LengthDelimitedFramer.init(failing.allocator(), 1024);
    conn.ctx = &state;
    conn.on_message = Harness.onMessage;
    conn.on_error = Harness.onError;

    var encoded: [7]u8 = undefined;
    std.mem.writeInt(u32, encoded[0..4], 3, .little);
    @memcpy(encoded[4..7], "abc");

    try conn.inbound.push(&encoded);
    try conn.dispatchAvailableFrames();

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(quic_close.ApplicationCloseCode.internal_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), status.err);
    try std.testing.expectEqualStrings("rpc transport error", conn.close_reason_buf[0..conn.close_reason_len]);
    try std.testing.expectEqual(@as(usize, 0), conn.inbound.buffer.items.len);
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC dispatch treats message callback failure as terminal" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {
            return error.CallbackFailed;
        }

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var state = Harness.State{};
    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();
    conn.ctx = &state;
    conn.on_message = Harness.onMessage;
    conn.on_error = Harness.onError;

    var encoded: [7]u8 = undefined;
    std.mem.writeInt(u32, encoded[0..4], 3, .little);
    @memcpy(encoded[4..7], "abc");

    try conn.inbound.push(&encoded);
    try conn.dispatchAvailableFrames();

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.CallbackFailed), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(quic_close.ApplicationCloseCode.peer_callback_failure, status.code);
    try std.testing.expectEqualStrings("rpc callback error", conn.close_reason_buf[0..conn.close_reason_len]);
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC deinit requested from error callback is deferred without panic" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            close_count: usize = 0,
            deinit_seen_in_error: bool = false,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, _: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            conn.deinit();
            state.deinit_seen_in_error = conn.deinit_requested;
        }

        fn onClose(conn: *Connection) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.close_count += 1;
        }
    };

    var state = Harness.State{};
    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();
    conn.ctx = &state;
    conn.on_message = Harness.onMessage;
    conn.on_error = Harness.onError;
    conn.on_close = Harness.onClose;

    conn.terminateFrameError(error.InvalidFrame);
    if (conn.on_close) |cb| conn.invokeCloseCallback(cb);

    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(usize, 1), state.close_count);
    try std.testing.expect(state.deinit_seen_in_error);
    try std.testing.expect(conn.deinit_requested);
}
