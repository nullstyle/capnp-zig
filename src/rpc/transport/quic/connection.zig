const std = @import("std");
const builtin = @import("builtin");
const quic_zig = @import("quic_zig");

const events = @import("../../events.zig");
const baseline_engine = @import("baseline_engine.zig");
const callback_lifecycle_mod = @import("callback_lifecycle.zig");
const close_controller_mod = @import("close_controller.zig");
const connection_adapters = @import("connection_adapters.zig");
const connection_dispatch = @import("connection_dispatch.zig");
const connection_init = @import("connection_init.zig");
const connection_loop = @import("connection_loop.zig");
const connection_termination = @import("connection_termination.zig");
const engine_owner = @import("engine_owner.zig");
const endpoint_mod = @import("endpoint.zig");
const native_engine = @import("native_engine.zig");
const quic_options = @import("options.zig");
const quic_close = @import("close.zig");
const udp_receive_bridge = @import("udp_receive_bridge.zig");
const wake_mod = @import("wake.zig");

const Net = std.Io.net;

const ClientOptions = quic_options.ClientOptions;
const ServerOptions = quic_options.ServerOptions;
const TransportMode = quic_options.TransportMode;
const BaselineEngine = baseline_engine.BaselineEngine;
const EngineOwner = engine_owner.Owner;
const NativeEngine = native_engine.NativeEngine;

const Role = endpoint_mod.Role;
const EndpointRuntime = endpoint_mod.Runtime;

/// Compatibility transport for a single vat-to-vat Cap'n Proto RPC session.
///
/// Client-side instances own one `ClientEndpoint`. Server-side compatibility
/// instances own one `Listener` and attach the first accepted `Session` to the
/// peer-facing callbacks. This intentionally preserves the first QUIC design
/// step as "one QUIC connection equals one authenticated vat session" while
/// making the future listener fanout boundary explicit. New multi-session
/// server work should build around `Listener` and `Session`; this type should
/// remain the one-session adapter used by the existing peer transport.
pub const Connection = struct {
    pub const StepMode = connection_loop.StepMode;
    pub const StepResult = connection_loop.StepResult;
    const CallbackLifecycle = callback_lifecycle_mod.State(Connection);
    const Adapters = connection_adapters.State(Connection);
    const CloseController = close_controller_mod.Controller;
    const Dispatch = connection_dispatch.State(Connection);
    const Termination = connection_termination.State(Connection);

    allocator: std.mem.Allocator,
    role: Role,
    endpoint: EndpointRuntime,
    udp_rx_buf: []u8,
    udp_tx_buf: []u8,
    stream_read_buf: []u8,
    observer: ?events.Observer = null,
    max_message_bytes: usize,
    mode: TransportMode,
    baseline: BaselineEngine,
    native: NativeEngine,
    close_controller: CloseController = .{},
    wake_state: wake_mod.Handle = .{},
    udp_receive: udp_receive_bridge.Bridge = .{},

    /// True once the `.closing` connection phase has been emitted, so the
    /// event fires at most once. Owner/loop-thread only (see
    /// `Termination.emitClosingOnce`).
    closing_emitted: bool = false,

    callback_lifecycle: CallbackLifecycle = .{},

    /// Thread ID captured at init. Widened to `u64` (not `std.Thread.Id`,
    /// whose width varies by target) so the rendered api-snapshot surface is
    /// platform-stable.
    owner_thread_id: ?u64 = null,

    /// When true, thread-affinity checks also run in release builds (always on
    /// in Debug). Mirrors the TCP connection's field and
    /// `Peer.enableRuntimeThreadChecks`.
    runtime_thread_checks: bool = false,

    pub fn initClient(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: ClientOptions,
    ) !Connection {
        return try connection_init.initClient(Connection, allocator, io, options);
    }

    pub fn initServer(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: ServerOptions,
    ) !Connection {
        return try connection_init.initServer(Connection, allocator, io, options);
    }

    /// Re-capture thread affinity on the current thread. Legal only at a
    /// quiescent handoff point — no other thread may touch this connection
    /// concurrently (mirrors the TCP connection's `adoptOwnerThread`).
    pub fn adoptOwnerThread(self: *Connection) void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        self.owner_thread_id = std.Thread.getCurrentId();
    }

    pub fn deinit(self: *Connection) void {
        switch (self.callback_lifecycle.decideDeinit()) {
            .already_deinitialized => return,
            .defer_until_callback_exits => {
                // Re-entrant deinit from a callback on the run-loop thread,
                // which need not be the owner: do not assert affinity here, only
                // request close. deinitNow (the real teardown) asserts.
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
        self.udp_receive.cancel(self.endpoint.io);
        // Mark the wake as already-requested so late cross-thread wakers
        // short-circuit before touching the fds; the Handle's internal mutex
        // is the actual close-vs-write correctness mechanism.
        self.wake_state.requested.store(true, .release);
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
        events.emitConnection(self.observer, eventSource(self.mode), eventRole(self.role), .started);
    }

    pub fn context(self: *const Connection) ?*anyopaque {
        return self.callback_lifecycle.context();
    }

    pub fn run(self: *Connection) void {
        // Does NOT auto-adopt affinity (see the TCP connection's run() for the
        // rationale): a connection run on a thread other than its constructor's
        // must adopt first via adoptOwnerThread().
        connection_loop.run(self.loopOwner());
    }

    pub fn sendFrame(self: *Connection, frame: []const u8) !void {
        // Intentionally NOT affinity-checked, unlike the TCP connection. QUIC
        // sendFrame enqueues into a mutex-guarded outbound queue that the loop
        // flushes (mode_router.enqueue), so it is safe to call from any thread.
        // The TCP transport made the opposite contract choice (owner-thread-only
        // sendFrame, cross-thread work via wake()); that asymmetry is by design,
        // not a missing check.
        return try Dispatch.sendFrame(self, frame);
    }

    /// Wake a blocked `run()` loop from any thread.
    ///
    /// POSIX builds use a socketpair so the UDP wait is interrupted
    /// immediately. Other targets keep a wake flag and the scheduler caps
    /// blocking waits to a short interval until a native event primitive lands.
    pub fn wake(self: *Connection) void {
        self.wake_state.request();
        if (comptime builtin.target.os.tag == .windows) {
            self.udp_receive.wake(self.endpoint.io);
        }
    }

    pub fn close(self: *Connection) void {
        Termination.emitClosingOnce(self);
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

    pub fn activeQuicConnection(self: *Connection) ?*quic_zig.Connection {
        return self.endpoint.activeQuicConnection();
    }

    /// Assert that the caller is on the thread that owns this connection.
    /// Always enforced in Debug; enforced in release only when
    /// `runtime_thread_checks` is set — parity with the TCP connection, which
    /// previously had a per-connection release opt-in the QUIC side lacked
    /// (the check here compiled out of release entirely). Panics on violation:
    /// a wrong-thread call means unsynchronized access is already happening,
    /// which is not something an error return can make safe. Enable the opt-in
    /// via `runtime_thread_checks` (mirrors `Peer.enableRuntimeThreadChecks`).
    pub fn assertThreadAffinity(self: *const Connection) void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        if (comptime builtin.mode != .debug) {
            if (!self.runtime_thread_checks) return;
        }
        const owner = self.owner_thread_id orelse return;
        const current = std.Thread.getCurrentId();
        if (current != owner) {
            @panic("QUIC Connection method called from wrong thread: not thread-safe; all calls must be on the owner thread (see adoptOwnerThread / runtime_thread_checks)");
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
            try Dispatch.dispatchRpcFrame(conn, frame);
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

    pub const TestingHooks = if (builtin.is_test) struct {
        pub const CloseCallback = CallbackLifecycle.CloseCallback;
        pub const ErrorCallback = CallbackLifecycle.ErrorCallback;
        pub const MessageCallback = CallbackLifecycle.MessageCallback;

        pub fn wakeRequested(conn: *const Connection) bool {
            return conn.wakeRequested();
        }

        pub fn baselineOwner(conn: *Connection) baseline_engine.Owner {
            return conn.baselineOwner();
        }

        pub fn nativeOwner(conn: *Connection) native_engine.Owner {
            return conn.nativeOwner();
        }

        pub fn activeQuicConn(conn: *Connection) ?*quic_zig.Connection {
            return conn.activeQuicConn();
        }

        pub fn terminateFrameError(conn: *Connection, err: anyerror) void {
            Termination.frameError(conn, err);
        }

        pub fn closeReason(conn: *const Connection) []const u8 {
            return Termination.reason(conn);
        }

        pub fn revealCloseDetailOnWire(conn: *Connection, reveal: bool) void {
            Termination.revealDetailOnWire(conn, reveal);
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

        pub fn udpReceiveInFlight(conn: *const Connection) bool {
            return conn.udp_receive.hasInFlight();
        }

        pub fn udpReceiveCancellationCount(conn: *const Connection) usize {
            return conn.udp_receive.cancellationCount();
        }

        pub fn invokeCloseCallback(
            conn: *Connection,
            cb: *const fn (conn: *Connection) void,
        ) void {
            conn.callback_lifecycle.invokeClose(conn, cb);
        }
    } else struct {};

    fn wakeRequested(self: *const Connection) bool {
        return self.wake_state.isRequested();
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

    fn activeQuicConn(self: *Connection) ?*quic_zig.Connection {
        return self.activeQuicConnection();
    }
};

fn eventSource(mode: TransportMode) events.Source {
    return switch (mode) {
        .baseline => .quic_baseline,
        .native => .quic_native,
    };
}

fn eventRole(role: Role) events.Role {
    return switch (role) {
        .client => .client,
        .server => .server,
    };
}
