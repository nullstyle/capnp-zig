const std = @import("std");
const builtin = @import("builtin");
const quic_zig = @import("quic_zig");

const baseline_engine = @import("baseline_engine.zig");
const callback_lifecycle_mod = @import("callback_lifecycle.zig");
const close_controller_mod = @import("close_controller.zig");
const connection_loop = @import("connection_loop.zig");
const engine_owner = @import("engine_owner.zig");
const endpoint_factory = @import("endpoint_factory.zig");
const endpoint_mod = @import("endpoint.zig");
const length_framer = @import("length_framer.zig");
const mode_router = @import("mode_router.zig");
const native_engine = @import("native_engine.zig");
const native_framer = @import("native_framer.zig");
const quic_options = @import("options.zig");
const quic_close = @import("close.zig");
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
const EndpointRuntime = endpoint_mod.Runtime;

/// A single vat-to-vat Cap'n Proto RPC session over QUIC.
///
/// Client-side instances own one `ClientEndpoint`. Server-side compatibility
/// instances own one `Listener` and attach the first accepted `Session` to the
/// peer-facing callbacks. This intentionally preserves the first QUIC design
/// step as "one QUIC connection equals one authenticated vat session" while
/// making the future listener fanout boundary explicit.
pub const Connection = struct {
    pub const StepMode = connection_loop.StepMode;
    pub const StepResult = connection_loop.StepResult;
    const CallbackLifecycle = callback_lifecycle_mod.State(Connection);
    const CloseController = close_controller_mod.Controller;

    allocator: std.mem.Allocator,
    role: Role,
    endpoint: EndpointRuntime,
    udp_rx_buf: []u8,
    udp_tx_buf: []u8,
    stream_read_buf: []u8,
    max_message_bytes: usize,
    mode: TransportMode,
    baseline: BaselineEngine,
    native: NativeEngine,
    close_controller: CloseController = .{},
    wake_state: wake_mod.Handle = .{},

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
            .role = role,
            .endpoint = EndpointRuntime.init(endpoint, io),
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
            .close_controller = CloseController.init(reveal_close_reason_on_wire),
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
        self.callback_lifecycle.clearCallbacks();
        self.wake_state.deinit();
        self.endpoint.deinit();
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
        self.callback_lifecycle.start(ctx, on_message, on_error, on_close);
    }

    pub fn context(self: *const Connection) ?*anyopaque {
        return self.callback_lifecycle.context();
    }

    pub fn run(self: *Connection) void {
        connection_loop.run(self.loopOwner());
    }

    pub fn sendFrame(self: *Connection, frame: []const u8) !void {
        if (self.close_controller.isRequested()) return error.BrokenPipe;
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
        self.close_controller.closeActive(self.activeQuicConn(), .normal, null);
        self.wake();
    }

    pub fn requestClose(self: *Connection) void {
        self.close_controller.requestNormal(&self.baseline, &self.native);
        self.wake();
    }

    pub fn isClosing(self: *const Connection) bool {
        return self.close_controller.isRequested();
    }

    pub fn closeStatus(self: *const Connection) ?quic_close.Status {
        return self.close_controller.status();
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
        return try connection_loop.stepOnce(self.loopOwner(), mode);
    }

    fn wakeRequested(self: *const Connection) bool {
        return self.wake_state.isRequested();
    }

    fn endpointDriver(self: *Connection) EndpointDriver {
        return self.endpoint.driver();
    }

    fn selectedOutboundEmpty(self: *Connection) bool {
        return self.selectedMode().outboundEmpty();
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

    fn loopOwner(self: *Connection) connection_loop.Owner {
        return .{
            .ptr = self,
            .io = self.endpoint.io,
            .role = self.role,
            .udp_rx_buf = self.udp_rx_buf,
            .udp_tx_buf = self.udp_tx_buf,
            .wake = &self.wake_state,
            .driver = loopDriver,
            .selected_mode = loopSelectedMode,
            .selected_outbound_empty = loopSelectedOutboundEmpty,
            .engine_owner = loopEngineOwner,
            .close_requested = loopCloseRequested,
            .request_close = loopRequestClose,
            .terminate_internal_error = loopTerminateInternalError,
            .flush_close_datagram = loopFlushCloseDatagram,
            .close_engines = loopCloseEngines,
            .invoke_close_callback = loopInvokeCloseCallback,
            .complete_deferred_deinit = loopCompleteDeferredDeinit,
        };
    }

    fn loopDriver(ptr: *anyopaque) EndpointDriver {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.endpointDriver();
    }

    fn loopSelectedMode(ptr: *anyopaque) ModeRouter {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.selectedMode();
    }

    fn loopSelectedOutboundEmpty(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.selectedOutboundEmpty();
    }

    fn loopEngineOwner(ptr: *anyopaque) EngineOwner {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.engineOwner();
    }

    fn loopCloseRequested(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.close_controller.isRequested();
    }

    fn loopRequestClose(ptr: *anyopaque) void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        self.close_controller.request();
    }

    fn loopTerminateInternalError(ptr: *anyopaque, err: anyerror) void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        self.terminateInternalError(err);
    }

    fn loopFlushCloseDatagram(ptr: *anyopaque) void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        self.close_controller.flushCloseDatagram(
            self.activeQuicConn(),
            self.endpointDriver(),
            self.udp_tx_buf,
            self.nowUs(),
        );
    }

    fn loopCloseEngines(ptr: *anyopaque) void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        self.close_controller.closeEngines(&self.baseline, &self.native);
    }

    fn loopInvokeCloseCallback(ptr: *anyopaque) void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        if (self.callback_lifecycle.closeCallback()) |cb| {
            self.callback_lifecycle.invokeClose(self, cb);
        }
    }

    fn loopCompleteDeferredDeinit(ptr: *anyopaque) void {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        if (self.callback_lifecycle.shouldCompleteDeferredDeinit()) self.deinitNow(false);
    }

    fn ownerIsClosing(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.close_controller.isRequested();
    }

    fn ownerCallbacksReady(ptr: *anyopaque) bool {
        const self: *Connection = @ptrCast(@alignCast(ptr));
        return self.callback_lifecycle.callbacksReady();
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
        if (!self.callback_lifecycle.callbacksReady()) return;
        const on_message = self.callback_lifecycle.messageCallback() orelse return;
        self.callback_lifecycle.invokeMessage(self, on_message, frame) catch |err| {
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
        self.close_controller.closeActive(self.activeQuicConn(), policy.code, err);
        self.enterClosing();
        if (policy.reset_inbound) self.resetInbound();
        if (policy.clear_message_callback) self.callback_lifecycle.clearMessageCallback();
        const on_error = if (policy.clear_error_callback)
            self.callback_lifecycle.takeErrorCallback()
        else
            self.callback_lifecycle.errorCallback();
        self.wake();
        if (policy.notify_error_callback) {
            if (on_error) |cb| self.callback_lifecycle.invokeError(self, cb, err);
        }
    }

    fn enterClosing(self: *Connection) void {
        self.close_controller.enterClosing(&self.baseline, &self.native);
    }

    fn resetInbound(self: *Connection) void {
        self.baseline.resetInbound();
        self.native.resetInbound(self.allocator);
    }

    fn closeReason(self: *const Connection) []const u8 {
        return self.close_controller.reason();
    }

    fn activeQuicConn(self: *Connection) ?*quic_zig.Connection {
        return self.endpoint.activeQuicConnection();
    }

    fn nowUs(self: *Connection) u64 {
        return self.endpoint.nowUs();
    }
};

pub const TestAccess = struct {
    pub const ApplicationCloseCode = quic_close.ApplicationCloseCode;
    pub const CloseCallback = Connection.CallbackLifecycle.CloseCallback;
    pub const ErrorCallback = Connection.CallbackLifecycle.ErrorCallback;
    pub const MessageCallback = Connection.CallbackLifecycle.MessageCallback;
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

    pub fn revealCloseDetailOnWire(conn: *Connection, reveal: bool) void {
        conn.close_controller.setRevealDetailOnWire(reveal);
    }

    pub fn setCallbacks(
        conn: *Connection,
        ctx: ?*anyopaque,
        on_message: ?MessageCallback,
        on_error: ?ErrorCallback,
        on_close: ?CloseCallback,
    ) void {
        conn.callback_lifecycle.setCallbacks(ctx, on_message, on_error, on_close);
    }

    pub fn closeCallback(conn: *const Connection) ?CloseCallback {
        return conn.callback_lifecycle.closeCallback();
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
