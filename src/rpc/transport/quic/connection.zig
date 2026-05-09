const std = @import("std");
const builtin = @import("builtin");
const quic_zig = @import("quic_zig");

const baseline_engine = @import("baseline_engine.zig");
const endpoint_mod = @import("endpoint.zig");
const length_framer = @import("length_framer.zig");
const listener_mod = @import("listener.zig");
const native_engine = @import("native_engine.zig");
const native_framer = @import("native_framer.zig");
const quic_zig_adapter = @import("quic_zig_adapter.zig");
const quic_options = @import("options.zig");
const quic_close = @import("close.zig");
const scheduler = @import("scheduler.zig");
const wake_mod = @import("wake.zig");

const log = std.log.scoped(.rpc_quic_transport);
const Net = std.Io.net;

const ClientOptions = quic_options.ClientOptions;
const ServerOptions = quic_options.ServerOptions;
const TransportMode = quic_options.TransportMode;
const NativeOptions = quic_options.NativeOptions;
const BaselineEngine = baseline_engine.BaselineEngine;
const LengthDelimitedFramer = length_framer.LengthDelimitedFramer;
const NativeControlFramer = native_framer.ControlFramer;
const NativeEngine = native_engine.NativeEngine;
const PollResult = wake_mod.PollResult;

const Role = endpoint_mod.Role;
const Endpoint = endpoint_mod.Endpoint;
const EndpointDriver = endpoint_mod.EndpointDriver;

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
    baseline: BaselineEngine,
    native: NativeEngine,
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    wake_state: wake_mod.Handle = .{},
    reveal_close_reason_on_wire: bool = false,
    close_status: ?quic_close.Status = null,
    close_reason_buf: [quic_close.max_wire_reason_bytes]u8 = undefined,
    close_reason_len: usize = 0,

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
            .baseline = BaselineEngine.init(
                allocator,
                max_message_bytes,
                max_outbound_queue_items,
                max_outbound_queue_bytes,
            ),
            .native = NativeEngine.init(
                allocator,
                role,
                max_message_bytes,
                max_outbound_queue_items,
                max_outbound_queue_bytes,
                native_options,
            ),
            .wake_state = wake_mod.Handle.init(),
            .reveal_close_reason_on_wire = reveal_close_reason_on_wire,
            .owner_thread_id = if (comptime builtin.target.os.tag == .freestanding) null else std.Thread.getCurrentId(),
        };
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
        self.baseline.close();
        self.baseline.deinit(self.allocator);
        self.native.close();
        self.native.deinit(self.allocator);
        self.ctx = null;
        self.on_message = null;
        self.on_error = null;
        self.on_close = null;
        self.wake_state.deinit();
        self.endpointDriver().deinit();
        self.allocator.free(self.udp_rx_buf);
        self.allocator.free(self.udp_tx_buf);
        self.allocator.free(self.stream_read_buf);
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
        self.baseline.close();
        self.native.close();
        if (self.on_close) |cb| self.invokeCloseCallback(cb);
        self.completeDeferredDeinit();
    }

    pub fn sendFrame(self: *Connection, frame: []const u8) !void {
        if (self.close_requested.load(.acquire)) return error.BrokenPipe;
        if (frame.len == 0 or frame.len > self.max_message_bytes) return error.FrameTooLarge;
        if (frame.len > std.math.maxInt(u32)) return error.FrameTooLarge;

        switch (self.mode) {
            .baseline => try self.baseline.enqueue(self.allocator, frame),
            .native => try self.native.enqueue(self.allocator, frame),
        }
        self.wake();
    }

    /// Wake a blocked `run()` loop from any thread.
    ///
    /// POSIX builds use a socketpair so the UDP wait is interrupted
    /// immediately. Other targets keep a wake flag and the scheduler caps
    /// blocking waits to a short interval until a native event primitive lands.
    pub fn wake(self: *Connection) void {
        self.wake_state.request();
    }

    pub fn close(self: *Connection) void {
        _ = self.close_requested.swap(true, .acq_rel);
        self.baseline.close();
        self.native.close();
        self.closeActiveQuicConn(.normal, null);
        self.wake();
    }

    pub fn requestClose(self: *Connection) void {
        _ = self.close_requested.swap(true, .acq_rel);
        self.baseline.close();
        self.native.close();
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
        return self.endpoint.getAddress();
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

        const socket = self.endpointDriver().activeSocket();
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

        _ = try self.endpointDriver().handleDatagram(msg.data, msg.from, now_us);
        return result;
    }

    fn waitForUdpOrWake(self: *Connection, wait_duration: std.Io.Duration) !?PollResult {
        const socket = self.endpointDriver().activeSocket();
        return try self.wake_state.waitForSocket(socket.handle, wait_duration);
    }

    fn consumeWakeRequested(self: *Connection) bool {
        return self.wake_state.consumeRequested();
    }

    fn hasWakeSupport(self: *const Connection) bool {
        return self.wake_state.isSupported();
    }

    fn wakeRequested(self: *const Connection) bool {
        return self.wake_state.isRequested();
    }

    fn receiveTimeout(self: *const Connection) std.Io.Duration {
        return self.endpoint.receiveTimeout();
    }

    fn endpointDriver(self: *Connection) EndpointDriver {
        return self.endpoint.driver(self.io);
    }

    fn hasImmediateWork(self: *Connection) bool {
        if (self.wakeRequested()) return true;
        if (self.activeQuicConn()) |conn| {
            if (conn.canSend()) return true;
            if (!self.selectedOutboundEmpty()) {
                switch (self.mode) {
                    .baseline => if (self.baseline.hasImmediateWork(self.role, conn)) return true,
                    .native => if (self.native.hasImmediateWork(self.role, conn)) return true,
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
            .baseline => self.baseline.outboundEmpty(),
            .native => self.native.outboundEmpty(),
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
        const conn = self.activeQuicConn() orelse return;
        switch (self.mode) {
            .baseline => try self.baseline.service(self.baselineOwner(), conn),
            .native => try self.native.service(self.nativeOwner(), conn),
        }
    }

    fn baselineOwner(self: *Connection) baseline_engine.Owner {
        return .{
            .ptr = self,
            .allocator = self.allocator,
            .role = self.role,
            .stream_read_buf = self.stream_read_buf,
            .is_closing = baselineOwnerIsClosing,
            .callbacks_ready = baselineOwnerCallbacksReady,
            .dispatch_rpc_frame = baselineOwnerDispatchRpcFrame,
            .terminate_frame_error = baselineOwnerTerminateFrameError,
            .deinit_requested = baselineOwnerDeinitRequested,
        };
    }

    fn baselineOwnerIsClosing(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.close_requested.load(.acquire);
    }

    fn baselineOwnerCallbacksReady(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.on_message != null and self.on_error != null;
    }

    fn baselineOwnerDispatchRpcFrame(ptr: *anyopaque, frame: []const u8) !void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        try self.dispatchRpcFrame(frame);
    }

    fn baselineOwnerTerminateFrameError(ptr: *anyopaque, err: anyerror) void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        self.terminateFrameError(err);
    }

    fn baselineOwnerDeinitRequested(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.deinit_requested;
    }

    fn nativeOwner(self: *Connection) native_engine.Owner {
        return .{
            .ptr = self,
            .allocator = self.allocator,
            .role = self.role,
            .max_message_bytes = self.max_message_bytes,
            .stream_read_buf = self.stream_read_buf,
            .is_closing = nativeOwnerIsClosing,
            .dispatch_rpc_frame = nativeOwnerDispatchRpcFrame,
            .terminate_frame_error = nativeOwnerTerminateFrameError,
            .deinit_requested = nativeOwnerDeinitRequested,
        };
    }

    fn nativeOwnerIsClosing(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.close_requested.load(.acquire);
    }

    fn nativeOwnerDispatchRpcFrame(ptr: *anyopaque, frame: []const u8) !void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        try self.dispatchRpcFrame(frame);
    }

    fn nativeOwnerTerminateFrameError(ptr: *anyopaque, err: anyerror) void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        self.terminateFrameError(err);
    }

    fn nativeOwnerDeinitRequested(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.deinit_requested;
    }

    fn dispatchRpcFrame(self: *Connection, frame: []const u8) !void {
        if (self.on_message == null or self.on_error == null) return;
        self.invokeMessageCallback(self.on_message.?, frame) catch |err| {
            self.terminateCallbackError(err);
            return;
        };
    }

    fn terminateFrameError(self: *Connection, err: anyerror) void {
        log.debug("fatal QUIC frame error, closing connection: {}", .{err});
        self.closeActiveQuicConn(quic_close.codeForFrameError(err), err);
        _ = self.close_requested.swap(true, .acq_rel);
        self.baseline.close();
        self.native.close();
        self.baseline.resetInbound();
        self.native.resetInbound(self.allocator);
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
        self.baseline.close();
        self.native.close();
        self.on_message = null;
        const on_error = self.on_error;
        self.on_error = null;
        self.wake();
        if (on_error) |cb| self.invokeErrorCallback(cb, err);
    }

    fn terminateInternalError(self: *Connection, err: anyerror) void {
        self.closeActiveQuicConn(quic_close.codeForStepError(err), err);
        _ = self.close_requested.swap(true, .acq_rel);
        self.baseline.close();
        self.native.close();
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
        try self.endpointDriver().drainOutgoingDatagrams(self.udp_tx_buf, now_us);
    }

    fn activeQuicConn(self: *Connection) ?*quic_zig.Connection {
        return self.endpointDriver().quicConnection();
    }

    fn reapServerIfClosed(self: *Connection) void {
        if (self.endpointDriver().reapClosed()) {
            _ = self.close_requested.swap(true, .acq_rel);
        }
    }

    fn nowUs(self: *Connection) u64 {
        return self.endpointDriver().nowUs();
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

    try std.testing.expect(!conn.wakeRequested());
    try conn.sendFrame("abc");
    try std.testing.expect(conn.wakeRequested());
    try std.testing.expect(!conn.baseline.outbound.isEmpty());
    try std.testing.expect(conn.consumeWakeRequested());
    try std.testing.expect(!conn.wakeRequested());
}

test "QUIC native sendFrame uses native outbound queue" {
    var conn = try initTestNativeClient(std.testing.allocator, .{});
    defer conn.deinit();

    try conn.sendFrame("abc");
    try std.testing.expect(conn.wakeRequested());
    try std.testing.expect(conn.baseline.outbound.isEmpty());
    try std.testing.expect(!conn.native.outbound.isEmpty());
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
    conn.native.hello_received = true;

    const data_control = try native_framer.encodeDataRpc(std.testing.allocator, 0, 3, 8, 64);
    defer std.testing.allocator.free(data_control);
    const inline_control = try native_framer.encodeInlineRpc(std.testing.allocator, 1, "later", 64);
    defer std.testing.allocator.free(inline_control);

    try conn.native.inbound.push(data_control);
    try conn.native.inbound.push(inline_control);
    try conn.native.processControlFrames(conn.nativeOwner(), conn.activeQuicConn().?);

    try std.testing.expectEqual(@as(usize, 0), state.messages);
    try std.testing.expect(conn.native.pending_data != null);
    try std.testing.expectEqual(@as(u64, 0), conn.native.next_in_sequence);
    try std.testing.expect(conn.native.inbound.buffer.items.len > 0);
}

test "QUIC native control rejects missing and duplicate hello frames" {
    {
        var conn = try initTestNativeClient(std.testing.allocator, .{});
        defer conn.deinit();

        const inline_control = try native_framer.encodeInlineRpc(std.testing.allocator, 0, "rpc", 64);
        defer std.testing.allocator.free(inline_control);

        try conn.native.inbound.push(inline_control);
        try std.testing.expectError(
            error.InvalidFrame,
            conn.native.processControlFrames(conn.nativeOwner(), conn.activeQuicConn().?),
        );
    }

    {
        var conn = try initTestNativeClient(std.testing.allocator, .{});
        defer conn.deinit();

        var hello: [native_framer.encodedHelloLen()]u8 = undefined;
        const hello_len = try native_framer.encodeHello(&hello);

        try conn.native.inbound.push(hello[0..hello_len]);
        try conn.native.processControlFrames(conn.nativeOwner(), conn.activeQuicConn().?);
        try std.testing.expect(conn.native.hello_received);

        try conn.native.inbound.push(hello[0..hello_len]);
        try std.testing.expectError(
            error.InvalidFrame,
            conn.native.processControlFrames(conn.nativeOwner(), conn.activeQuicConn().?),
        );
    }
}

test "QUIC native control rejects non-monotonic rpc sequences" {
    var conn = try initTestNativeClient(std.testing.allocator, .{});
    defer conn.deinit();
    conn.native.hello_received = true;
    conn.native.next_in_sequence = 1;

    const stale_inline = try native_framer.encodeInlineRpc(std.testing.allocator, 0, "stale", 64);
    defer std.testing.allocator.free(stale_inline);

    try conn.native.inbound.push(stale_inline);
    try std.testing.expectError(
        error.InvalidFrame,
        conn.native.processControlFrames(conn.nativeOwner(), conn.activeQuicConn().?),
    );
    try std.testing.expectEqual(@as(u64, 1), conn.native.next_in_sequence);
}

test "QUIC native validates data stream references and budgets" {
    var conn = try initTestNativeClient(std.testing.allocator, .{
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 8,
    });
    defer conn.deinit();

    try std.testing.expectError(error.InvalidFrame, conn.native.startPendingData(conn.nativeOwner(), .{
        .sequence = 0,
        .stream_id = 0,
        .length = 4,
    }));
    try std.testing.expectError(error.InvalidFrame, conn.native.startPendingData(conn.nativeOwner(), .{
        .sequence = 0,
        .stream_id = 2,
        .length = 4,
    }));
    try std.testing.expectError(error.FrameTooLarge, conn.native.startPendingData(conn.nativeOwner(), .{
        .sequence = 0,
        .stream_id = 3,
        .length = 0,
    }));
    try std.testing.expectError(error.FrameTooLarge, conn.native.startPendingData(conn.nativeOwner(), .{
        .sequence = 0,
        .stream_id = 3,
        .length = 9,
    }));
    try std.testing.expect(conn.native.pending_data == null);
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
    conn.native.pending_data = .{
        .sequence = 0,
        .stream_id = 3,
        .bytes = pending_bytes,
    };

    conn.terminateFrameError(error.InvalidFrame);

    try std.testing.expect(conn.isClosing());
    try std.testing.expect(conn.native.pending_data == null);
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
    conn.native.inbound.deinit();
    conn.native.inbound = NativeControlFramer.init(failing.allocator(), .{
        .max_control_frame_bytes = 64,
        .max_rpc_frame_bytes = 64,
    });
    conn.ctx = &state;
    conn.on_message = Harness.onMessage;
    conn.on_error = Harness.onError;
    conn.native.hello_received = true;

    const inline_control = try native_framer.encodeInlineRpc(std.testing.allocator, 0, "abc", 64);
    defer std.testing.allocator.free(inline_control);

    try conn.native.inbound.push(inline_control);
    conn.native.processControlFrames(conn.nativeOwner(), conn.activeQuicConn().?) catch |err| {
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
    try std.testing.expectEqual(@as(usize, 0), conn.native.inbound.buffer.items.len);
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
    try conn.baseline.inbound.push(&bad_frame);
    try conn.baseline.dispatchAvailableFrames(conn.baselineOwner());

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
    try conn.baseline.inbound.push(&bad_frame);
    try conn.baseline.dispatchAvailableFrames(conn.baselineOwner());

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
    conn.baseline.inbound.deinit();
    conn.baseline.inbound = LengthDelimitedFramer.init(failing.allocator(), 1024);
    conn.ctx = &state;
    conn.on_message = Harness.onMessage;
    conn.on_error = Harness.onError;

    var encoded: [7]u8 = undefined;
    std.mem.writeInt(u32, encoded[0..4], 3, .little);
    @memcpy(encoded[4..7], "abc");

    try conn.baseline.inbound.push(&encoded);
    try conn.baseline.dispatchAvailableFrames(conn.baselineOwner());

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(quic_close.ApplicationCloseCode.internal_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), status.err);
    try std.testing.expectEqualStrings("rpc transport error", conn.close_reason_buf[0..conn.close_reason_len]);
    try std.testing.expectEqual(@as(usize, 0), conn.baseline.inbound.buffer.items.len);
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

    try conn.baseline.inbound.push(&encoded);
    try conn.baseline.dispatchAvailableFrames(conn.baselineOwner());

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
