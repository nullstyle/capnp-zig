const std = @import("std");
const builtin = @import("builtin");
const quic_zig = @import("quic_zig");

const baseline_engine = @import("baseline_engine.zig");
const callback_lifecycle_mod = @import("callback_lifecycle.zig");
const close_controller_mod = @import("close_controller.zig");
const connection_adapters = @import("connection_adapters.zig");
const connection_init = @import("connection_init.zig");
const connection_loop = @import("connection_loop.zig");
const connection_termination = @import("connection_termination.zig");
const engine_owner = @import("engine_owner.zig");
const endpoint_factory = @import("endpoint_factory.zig");
const endpoint_mod = @import("endpoint.zig");
const length_framer = @import("length_framer.zig");
const mode_router = @import("mode_router.zig");
const native_engine = @import("native_engine.zig");
const native_framer = @import("native_framer.zig");
const quic_options = @import("options.zig");
const quic_close = @import("close.zig");
const wake_mod = @import("wake.zig");

const Net = std.Io.net;

const ClientOptions = quic_options.ClientOptions;
const ServerOptions = quic_options.ServerOptions;
const TransportMode = quic_options.TransportMode;
const BaselineEngine = baseline_engine.BaselineEngine;
const EngineOwner = engine_owner.Owner;
const ModeRouter = mode_router.Router;
const NativeEngine = native_engine.NativeEngine;

const Role = endpoint_mod.Role;
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
    const Adapters = connection_adapters.State(Connection);
    const CloseController = close_controller_mod.Controller;
    const Termination = connection_termination.State(Connection);

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

        return try connection_init.init(
            Connection,
            allocator,
            io,
            created.role,
            created.endpoint,
            connection_init.Config.fromClient(options),
        );
    }

    pub fn initServer(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: ServerOptions,
    ) !Connection {
        var created = try endpoint_factory.initServer(allocator, io, options);
        errdefer created.deinit(io);

        return try connection_init.init(
            Connection,
            allocator,
            io,
            created.role,
            created.endpoint,
            connection_init.Config.fromServer(options),
        );
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
        Termination.enterClosing(self);
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
        Termination.close(self);
    }

    pub fn requestClose(self: *Connection) void {
        Termination.requestClose(self);
    }

    pub fn isClosing(self: *const Connection) bool {
        return Termination.isClosing(self);
    }

    pub fn closeStatus(self: *const Connection) ?quic_close.Status {
        return Termination.status(self);
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

    pub const AdapterAccess = struct {
        pub fn dispatchRpcFrame(conn: *Connection, frame: []const u8) !void {
            try conn.dispatchRpcFrame(frame);
        }

        pub fn terminateFrameError(conn: *Connection, err: anyerror) void {
            Termination.frameError(conn, err);
        }

        pub fn terminateInternalError(conn: *Connection, err: anyerror) void {
            Termination.internalError(conn, err);
        }

        pub fn deinitNowUnchecked(conn: *Connection) void {
            conn.deinitNow(false);
        }
    };

    fn wakeRequested(self: *const Connection) bool {
        return self.wake_state.isRequested();
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
        return Adapters.engineOwner(self);
    }

    fn loopOwner(self: *Connection) connection_loop.Owner {
        return Adapters.loopOwner(self);
    }

    fn dispatchRpcFrame(self: *Connection, frame: []const u8) !void {
        if (!self.callback_lifecycle.callbacksReady()) return;
        const on_message = self.callback_lifecycle.messageCallback() orelse return;
        self.callback_lifecycle.invokeMessage(self, on_message, frame) catch |err| {
            Termination.callbackError(self, err);
            return;
        };
    }

    fn activeQuicConn(self: *Connection) ?*quic_zig.Connection {
        return self.endpoint.activeQuicConnection();
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
        Connection.Termination.frameError(conn, err);
    }

    pub fn closeReason(conn: *const Connection) []const u8 {
        return Connection.Termination.reason(conn);
    }

    pub fn revealCloseDetailOnWire(conn: *Connection, reveal: bool) void {
        Connection.Termination.revealDetailOnWire(conn, reveal);
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
