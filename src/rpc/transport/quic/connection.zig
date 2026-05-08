const std = @import("std");
const builtin = @import("builtin");
const nullq = @import("nullq");

const endpoint_mod = @import("endpoint.zig");
const length_framer = @import("length_framer.zig");
const nullq_adapter = @import("nullq_adapter.zig");
const quic_options = @import("options.zig");
const quic_close = @import("close.zig");
const outbound_queue = @import("outbound_queue.zig");
const scheduler = @import("scheduler.zig");

const log = std.log.scoped(.rpc_quic_transport);
const Net = std.Io.net;

const ClientOptions = quic_options.ClientOptions;
const ServerOptions = quic_options.ServerOptions;
const LengthDelimitedFramer = length_framer.LengthDelimitedFramer;
const OutboundQueue = outbound_queue.OutboundQueue;

const Role = endpoint_mod.Role;
const Endpoint = endpoint_mod.Endpoint;
const ServerEndpoint = endpoint_mod.ServerEndpoint;

/// A single vat-to-vat Cap'n Proto RPC session over QUIC.
///
/// Client-side instances own one `nullq.Client` and one UDP socket. Server-side
/// instances own one `nullq.Server` configured for one active QUIC connection by
/// default; this intentionally models the first QUIC design step as "one QUIC
/// connection equals one authenticated vat session". A future multi-client
/// listener can fan out `nullq.Server.Slot`s to multiple `Connection`s without
/// changing the peer-facing send/start/close shape.
pub const Connection = struct {
    pub const StepMode = scheduler.StepMode;
    pub const StepResult = scheduler.StepResult;

    allocator: std.mem.Allocator,
    io: std.Io,
    role: Role,
    socket: Net.Socket,
    endpoint: Endpoint,
    remote_addr: ?Net.IpAddress = null,
    start_timestamp: std.Io.Timestamp,
    receive_timeout: std.Io.Duration,
    udp_rx_buf: []u8,
    udp_tx_buf: []u8,
    stream_read_buf: []u8,
    max_message_bytes: usize,
    inbound: LengthDelimitedFramer,
    outbound: OutboundQueue = .{},
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    socket_closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
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
        if (options.alpn_protocols.len == 0) return error.InvalidConfig;
        if (options.udp_rx_buffer_size == 0 or
            options.udp_tx_buffer_size == 0 or
            options.stream_read_buffer_size == 0 or
            options.max_message_bytes == 0 or
            options.max_outbound_queue_items == 0 or
            options.max_outbound_queue_bytes == 0)
        {
            return error.InvalidConfig;
        }

        const local_addr = options.local_addr orelse nullq_adapter.defaultClientBindAddress(options.remote_addr);
        const socket = try Net.IpAddress.bind(&local_addr, io, .{
            .mode = .dgram,
            .protocol = .udp,
        });
        errdefer socket.close(io);

        var client = try nullq.Client.connect(.{
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
            socket,
            .{ .client = client },
            options.receive_timeout,
            options.udp_rx_buffer_size,
            options.udp_tx_buffer_size,
            options.stream_read_buffer_size,
            options.max_message_bytes,
            options.max_outbound_queue_items,
            options.max_outbound_queue_bytes,
            options.remote_addr,
            false,
        );
    }

    pub fn initServer(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: ServerOptions,
    ) !Connection {
        const server_config = try quic_options.nullqServerConfigFromOptions(allocator, options);

        const socket = try Net.IpAddress.bind(&options.listen_addr, io, .{
            .mode = .dgram,
            .protocol = .udp,
        });
        errdefer socket.close(io);

        var server = try nullq.Server.init(server_config);
        errdefer server.deinit();

        return try initCommon(
            allocator,
            io,
            .server,
            socket,
            .{ .server = .{ .server = server } },
            options.receive_timeout,
            options.udp_rx_buffer_size,
            options.udp_tx_buffer_size,
            options.stream_read_buffer_size,
            options.max_message_bytes,
            options.max_outbound_queue_items,
            options.max_outbound_queue_bytes,
            null,
            options.reveal_close_reason_on_wire,
        );
    }

    fn initCommon(
        allocator: std.mem.Allocator,
        io: std.Io,
        role: Role,
        socket: Net.Socket,
        endpoint: Endpoint,
        receive_timeout: std.Io.Duration,
        udp_rx_buffer_size: usize,
        udp_tx_buffer_size: usize,
        stream_read_buffer_size: usize,
        max_message_bytes: usize,
        max_outbound_queue_items: usize,
        max_outbound_queue_bytes: usize,
        remote_addr: ?Net.IpAddress,
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
            .socket = socket,
            .endpoint = endpoint,
            .remote_addr = remote_addr,
            .start_timestamp = std.Io.Timestamp.now(io, .awake),
            .receive_timeout = receive_timeout,
            .udp_rx_buf = udp_rx_buf,
            .udp_tx_buf = udp_tx_buf,
            .stream_read_buf = stream_read_buf,
            .max_message_bytes = max_message_bytes,
            .inbound = LengthDelimitedFramer.init(allocator, max_message_bytes),
            .outbound = OutboundQueue.init(max_outbound_queue_items, max_outbound_queue_bytes),
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
        self.ctx = null;
        self.on_message = null;
        self.on_error = null;
        self.on_close = null;
        self.closeWakeFds();
        if (!self.socket_closed.swap(true, .acq_rel)) {
            self.socket.close(self.io);
        }
        switch (self.endpoint) {
            .client => |*client| client.deinit(),
            .server => |*server| server.server.deinit(),
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
                if (conn.isClosed() and self.outbound.isEmpty()) {
                    _ = self.close_requested.swap(true, .acq_rel);
                }
            }
        }

        self.flushCloseDatagram();
        self.outbound.close();
        if (self.on_close) |cb| self.invokeCloseCallback(cb);
        self.completeDeferredDeinit();
    }

    pub fn sendFrame(self: *Connection, frame: []const u8) !void {
        if (self.close_requested.load(.acquire)) return error.BrokenPipe;
        if (frame.len == 0 or frame.len > self.max_message_bytes) return error.FrameTooLarge;
        if (frame.len > std.math.maxInt(u32)) return error.FrameTooLarge;

        try self.outbound.enqueue(self.allocator, frame);
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
        self.closeActiveQuicConn(.normal, null);
        self.wake();
    }

    pub fn requestClose(self: *Connection) void {
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
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
        return self.socket.address;
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
            .receive_timeout = self.receive_timeout,
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
        try self.serviceBaselineStream();
        try self.drainOutgoingDatagrams(now_us);

        now_us = self.nowUs();
        try self.tickActive(now_us);
        try self.serviceBaselineStream();
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

        const msg = self.socket.receiveTimeout(self.io, self.udp_rx_buf, .{
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
                const remote = self.remote_addr.?;
                if (!Net.IpAddress.eql(&msg.from, &remote)) return result;
                try client.conn.handle(msg.data, nullq_adapter.ipAddressToPathAddress(msg.from), now_us);
            },
            .server => |*server| {
                const from = nullq_adapter.ipAddressToPathAddress(msg.from);
                _ = try server.server.feed(msg.data, from, now_us);
                self.setServerSlotIfAccepted();
                try self.drainStatelessResponses(server);
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

        var poll_fds = [2]std.posix.pollfd{
            .{ .fd = self.socket.handle, .events = std.posix.POLL.IN, .revents = 0 },
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

    fn hasImmediateWork(self: *Connection) bool {
        if (self.wake_requested.load(.acquire)) return true;
        if (self.activeQuicConn()) |conn| {
            if (conn.canSend()) return true;
            if (!self.outbound.isEmpty()) {
                if (self.baseline_ready) return true;
                if (self.role == .client and conn.handshakeDone()) return true;
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
        return conn.isClosed() and self.outbound.isEmpty();
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

    fn serviceBaselineStream(self: *Connection) !void {
        const conn = self.activeQuicConn() orelse return;
        if (!try self.ensureBaselineStream(conn)) return;
        try self.outbound.flush(self.allocator, conn);
        try self.readBaselineStream(conn);
    }

    fn ensureBaselineStream(self: *Connection, conn: *nullq.Connection) !bool {
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

    fn readBaselineStream(self: *Connection, conn: *nullq.Connection) !void {
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
            self.invokeMessageCallback(self.on_message.?, bytes) catch |err| {
                self.terminateCallbackError(err);
                return;
            };
            if (self.deinit_requested) return;
        }
    }

    fn terminateFrameError(self: *Connection, err: anyerror) void {
        log.debug("fatal QUIC frame error, closing connection: {}", .{err});
        self.closeActiveQuicConn(quic_close.codeForFrameError(err), err);
        _ = self.close_requested.swap(true, .acq_rel);
        self.outbound.close();
        self.inbound.reset();
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

    fn closeQuicConn(self: *Connection, conn: *nullq.Connection) void {
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
                while (try client.conn.pollDatagram(self.udp_tx_buf, now_us)) |out| {
                    const dest = if (out.to) |addr|
                        nullq_adapter.pathAddressToIpAddress(addr) orelse self.remote_addr.?
                    else
                        self.remote_addr.?;
                    try self.socket.send(self.io, &dest, self.udp_tx_buf[0..out.len]);
                }
            },
            .server => |*server| {
                const session = server.session.handle() orelse return;
                while (try session.pollDatagram(self.udp_tx_buf, now_us)) |out| {
                    try self.socket.send(self.io, &out.dest, out.bytes);
                }
            },
        }
    }

    fn drainStatelessResponses(self: *Connection, server: *ServerEndpoint) !void {
        while (server.server.drainStatelessResponse()) |response| {
            const dest = nullq_adapter.pathAddressToIpAddress(response.dst) orelse continue;
            try self.socket.send(self.io, &dest, response.slice());
        }
    }

    fn activeQuicConn(self: *Connection) ?*nullq.Connection {
        return switch (self.endpoint) {
            .client => |*client| client.conn,
            .server => |*server| server.session.quicConnection(),
        };
    }

    fn setServerSlotIfAccepted(self: *Connection) void {
        switch (self.endpoint) {
            .client => return,
            .server => |*server| {
                _ = server.session.adoptFirstAccepted(&server.server);
            },
        }
    }

    fn reapServerIfClosed(self: *Connection) void {
        switch (self.endpoint) {
            .client => return,
            .server => |*server| {
                if (server.session.handle()) |session| {
                    if (!session.isClosed()) return;
                    const reaped = server.server.reap();
                    if (reaped > 0) {
                        server.session.clear();
                        _ = self.close_requested.swap(true, .acq_rel);
                    }
                } else {
                    _ = server.server.reap();
                }
            },
        }
    }

    fn nowUs(self: *Connection) u64 {
        const now = std.Io.Timestamp.now(self.io, .awake);
        const delta = self.start_timestamp.durationTo(now).toMicroseconds();
        if (delta <= 0) return 0;
        return @intCast(delta);
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
