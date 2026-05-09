const std = @import("std");
const builtin = @import("builtin");
const quic_zig = @import("quic_zig");

const baseline_engine = @import("baseline_engine.zig");
const callback_lifecycle_mod = @import("callback_lifecycle.zig");
const datagram_io = @import("datagram_io.zig");
const engine_owner = @import("engine_owner.zig");
const endpoint_factory = @import("endpoint_factory.zig");
const endpoint_mod = @import("endpoint.zig");
const length_framer = @import("length_framer.zig");
const mode_router = @import("mode_router.zig");
const native_engine = @import("native_engine.zig");
const native_framer = @import("native_framer.zig");
const quic_options = @import("options.zig");
const quic_close = @import("close.zig");
const scheduler = @import("scheduler.zig");
const termination = @import("termination.zig");
const wake_mod = @import("wake.zig");

const log = std.log.scoped(.rpc_quic_transport);
const Net = std.Io.net;

const ClientOptions = quic_options.ClientOptions;
const ServerOptions = quic_options.ServerOptions;
const TransportMode = quic_options.TransportMode;
const NativeOptions = quic_options.NativeOptions;
const BaselineEngine = baseline_engine.BaselineEngine;
const EngineOwner = engine_owner.Owner;
const ModeRouter = mode_router.Router;
const NativeEngine = native_engine.NativeEngine;
const TerminationPolicy = termination.Policy;

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
    const CallbackLifecycle = callback_lifecycle_mod.State(Connection);

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
    close_state: quic_close.State = .{},

    ctx: ?*anyopaque = null,
    on_message: ?*const fn (conn: *Connection, frame: []const u8) anyerror!void = null,
    on_error: ?*const fn (conn: *Connection, err: anyerror) void = null,
    on_close: ?*const fn (conn: *Connection) void = null,
    callback_lifecycle: CallbackLifecycle = .{},

    owner_thread_id: ?std.Thread.Id = null,

    pub fn initClient(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: ClientOptions,
    ) !Connection {
        try quic_options.validateClientOptions(options);

        var created = try endpoint_factory.initClient(allocator, io, options);
        errdefer created.deinit(io);

        return try initCommon(
            allocator,
            io,
            created.role,
            created.endpoint,
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
        var created = try endpoint_factory.initServer(allocator, io, options);
        errdefer created.deinit(io);

        return try initCommon(
            allocator,
            io,
            created.role,
            created.endpoint,
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
            .close_state = quic_close.State.init(reveal_close_reason_on_wire),
            .owner_thread_id = if (comptime builtin.target.os.tag == .freestanding) null else std.Thread.getCurrentId(),
        };
    }

    pub fn deinit(self: *Connection) void {
        switch (self.callback_lifecycle.decideDeinit()) {
            .already_deinitialized => return,
            .defer_until_callback_exits => {
                self.requestClose();
                return;
            },
            .deinit_now => self.deinitNow(true),
        }
    }

    fn deinitNow(self: *Connection, comptime check_affinity: bool) void {
        if (!self.callback_lifecycle.beginDeinit()) return;
        if (check_affinity) self.assertThreadAffinity();
        self.enterClosing();
        self.baseline.deinit(self.allocator);
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
        self.closeEngines();
        if (self.on_close) |cb| self.callback_lifecycle.invokeClose(self, cb);
        if (self.callback_lifecycle.shouldCompleteDeferredDeinit()) self.deinitNow(false);
    }

    pub fn sendFrame(self: *Connection, frame: []const u8) !void {
        if (self.close_requested.load(.acquire)) return error.BrokenPipe;
        if (frame.len == 0 or frame.len > self.max_message_bytes) return error.FrameTooLarge;
        if (frame.len > std.math.maxInt(u32)) return error.FrameTooLarge;

        try self.selectedMode().enqueue(self.allocator, frame);
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
        self.enterClosing();
        self.closeActiveQuicConn(.normal, null);
        self.wake();
    }

    pub fn requestClose(self: *Connection) void {
        self.enterClosing();
        self.recordCloseStatus(.normal, null);
        self.wake();
    }

    pub fn isClosing(self: *const Connection) bool {
        return self.close_requested.load(.acquire);
    }

    pub fn closeStatus(self: *const Connection) ?quic_close.Status {
        return self.close_state.status();
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
        const receive_result = try datagram_io.receiveOne(.{
            .io = self.io,
            .driver = self.endpointDriver(),
            .wake = &self.wake_state,
            .rx_buf = self.udp_rx_buf,
            .now_us = now_us,
            .wait_duration = waited_for,
        });
        result.received_datagram = receive_result.received_datagram;
        result.wake_drained = receive_result.wake_drained;

        now_us = self.nowUs();
        try self.advanceActive();
        try self.serviceModeStreams();
        try datagram_io.drainOutgoingDatagrams(self.endpointDriver(), self.udp_tx_buf, now_us);

        now_us = self.nowUs();
        try self.tickActive(now_us);
        try self.serviceModeStreams();
        try datagram_io.drainOutgoingDatagrams(self.endpointDriver(), self.udp_tx_buf, now_us);

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
        datagram_io.drainOutgoingDatagrams(self.endpointDriver(), self.udp_tx_buf, self.nowUs()) catch |err| {
            log.debug("failed to flush QUIC close datagram: {}", .{err});
        };
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
                return self.selectedMode().hasImmediateWork(self.role, conn);
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
        return self.selectedMode().outboundEmpty();
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
        try self.selectedMode().service(self.engineOwner(), conn);
    }

    fn selectedMode(self: *Connection) ModeRouter {
        return .{
            .mode = self.mode,
            .baseline = &self.baseline,
            .native = &self.native,
        };
    }

    fn baselineOwner(self: *Connection) baseline_engine.Owner {
        return self.engineOwner().baseline();
    }

    fn nativeOwner(self: *Connection) native_engine.Owner {
        return self.engineOwner().native();
    }

    fn engineOwner(self: *Connection) EngineOwner {
        return .{
            .ptr = self,
            .allocator = self.allocator,
            .role = self.role,
            .max_message_bytes = self.max_message_bytes,
            .stream_read_buf = self.stream_read_buf,
            .is_closing = ownerIsClosing,
            .callbacks_ready = ownerCallbacksReady,
            .dispatch_rpc_frame = ownerDispatchRpcFrame,
            .terminate_frame_error = ownerTerminateFrameError,
            .deinit_requested = ownerDeinitRequested,
        };
    }

    fn ownerIsClosing(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.close_requested.load(.acquire);
    }

    fn ownerCallbacksReady(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.on_message != null and self.on_error != null;
    }

    fn ownerDispatchRpcFrame(ptr: *anyopaque, frame: []const u8) !void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        try self.dispatchRpcFrame(frame);
    }

    fn ownerTerminateFrameError(ptr: *anyopaque, err: anyerror) void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        self.terminateFrameError(err);
    }

    fn ownerDeinitRequested(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.callback_lifecycle.deinitRequested();
    }

    fn dispatchRpcFrame(self: *Connection, frame: []const u8) !void {
        if (self.on_message == null or self.on_error == null) return;
        self.callback_lifecycle.invokeMessage(self, self.on_message.?, frame) catch |err| {
            self.terminateCallbackError(err);
            return;
        };
    }

    fn terminateFrameError(self: *Connection, err: anyerror) void {
        log.debug("fatal QUIC frame error, closing connection: {}", .{err});
        self.applyTermination(termination.policy(.frame_error, err), err);
    }

    fn terminateCallbackError(self: *Connection, err: anyerror) void {
        log.debug("QUIC message callback failed, closing connection: {}", .{err});
        self.applyTermination(termination.policy(.callback_error, err), err);
    }

    fn terminateInternalError(self: *Connection, err: anyerror) void {
        self.applyTermination(termination.policy(.internal_error, err), err);
    }

    fn applyTermination(
        self: *Connection,
        policy: TerminationPolicy,
        err: anyerror,
    ) void {
        self.closeActiveQuicConn(policy.code, err);
        self.enterClosing();
        if (policy.reset_inbound) self.resetInbound();
        if (policy.clear_message_callback) self.on_message = null;
        const on_error = if (policy.clear_error_callback) self.takeErrorCallback() else self.on_error;
        self.wake();
        if (policy.notify_error_callback) {
            if (on_error) |cb| self.callback_lifecycle.invokeError(self, cb, err);
        }
    }

    fn enterClosing(self: *Connection) void {
        _ = self.close_requested.swap(true, .acq_rel);
        self.closeEngines();
    }

    fn closeEngines(self: *Connection) void {
        self.baseline.close();
        self.native.close();
    }

    fn resetInbound(self: *Connection) void {
        self.baseline.resetInbound();
        self.native.resetInbound(self.allocator);
    }

    fn takeErrorCallback(
        self: *Connection,
    ) ?*const fn (conn: *Connection, err: anyerror) void {
        const cb = self.on_error;
        self.on_error = null;
        return cb;
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
        self.recordCloseStatus(.normal, null);
        const status = self.close_state.status() orelse return;
        conn.close(false, status.codeValue(), self.close_state.reason());
    }

    fn recordCloseStatus(
        self: *Connection,
        code: quic_close.ApplicationCloseCode,
        err: ?anyerror,
    ) void {
        self.close_state.record(code, err);
    }

    fn closeReason(self: *const Connection) []const u8 {
        return self.close_state.reason();
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
};

pub const TestAccess = struct {
    pub const ApplicationCloseCode = quic_close.ApplicationCloseCode;
    pub const NativeOptions = quic_options.NativeOptions;

    pub fn wakeRequested(conn: *const Connection) bool {
        return conn.wakeRequested();
    }

    pub fn consumeWakeRequested(conn: *Connection) bool {
        return conn.wake_state.consumeRequested();
    }

    pub fn processNativeControlFrames(conn: *Connection) !void {
        try conn.native.processControlFrames(conn.nativeOwner(), conn.activeQuicConn().?);
    }

    pub fn startNativePendingData(
        conn: *Connection,
        data: native_framer.DataRpc,
    ) !void {
        try conn.native.startPendingData(conn.nativeOwner(), data);
    }

    pub fn resetNativeInbound(
        conn: *Connection,
        allocator: std.mem.Allocator,
        max_control_frame_bytes: usize,
        max_rpc_frame_bytes: usize,
    ) void {
        conn.native.inbound.deinit();
        conn.native.inbound = native_framer.ControlFramer.init(allocator, .{
            .max_control_frame_bytes = max_control_frame_bytes,
            .max_rpc_frame_bytes = max_rpc_frame_bytes,
        });
    }

    pub fn dispatchBaselineFrames(conn: *Connection) !void {
        try conn.baseline.dispatchAvailableFrames(conn.baselineOwner());
    }

    pub fn resetBaselineInbound(
        conn: *Connection,
        allocator: std.mem.Allocator,
        max_message_bytes: usize,
    ) void {
        conn.baseline.inbound.deinit();
        conn.baseline.inbound = length_framer.LengthDelimitedFramer.init(allocator, max_message_bytes);
    }

    pub fn terminateFrameError(conn: *Connection, err: anyerror) void {
        conn.terminateFrameError(err);
    }

    pub fn closeReason(conn: *const Connection) []const u8 {
        return conn.closeReason();
    }

    pub fn deinitRequested(conn: *const Connection) bool {
        return conn.callback_lifecycle.deinitRequested();
    }

    pub fn invokeCloseCallback(
        conn: *Connection,
        cb: *const fn (conn: *Connection) void,
    ) void {
        conn.callback_lifecycle.invokeClose(conn, cb);
    }
};
