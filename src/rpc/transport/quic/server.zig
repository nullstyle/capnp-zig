const std = @import("std");
const quic_zig = @import("quic_zig");

const baseline_engine = @import("baseline_engine.zig");
const callback_lifecycle_mod = @import("callback_lifecycle.zig");
const close_controller_mod = @import("close_controller.zig");
const connection_dispatch = @import("connection_dispatch.zig");
const connection_init = @import("connection_init.zig");
const connection_termination = @import("connection_termination.zig");
const engine_owner_mod = @import("engine_owner.zig");
const listener_mod = @import("listener.zig");
const mode_router = @import("mode_router.zig");
const native_engine = @import("native_engine.zig");
const quic_close = @import("close.zig");
const quic_options = @import("options.zig");
const scheduler = @import("scheduler.zig");
const session_mod = @import("session.zig");
const wake_mod = @import("wake.zig");

const Net = std.Io.net;

const BaselineEngine = baseline_engine.BaselineEngine;
const CloseController = close_controller_mod.Controller;
const Config = connection_init.Config;
const EngineOwner = engine_owner_mod.Owner;
const NativeEngine = native_engine.NativeEngine;
const ServerOptions = quic_options.ServerOptions;

const log = std.log.scoped(.rpc_quic_server);

/// Poll-driven multi-session QUIC RPC server.
///
/// `Server` owns one UDP listener and one `quic_zig.Server`, then attaches a
/// small `ServerSession` transport driver to each accepted slot. Applications
/// can poll for accepted sessions, install callbacks per session, and step one
/// chosen session or all sessions without routing unrelated peers through a
/// single compatibility `Connection`.
pub const Server = struct {
    pub const StepMode = scheduler.StepMode;
    pub const StepResult = scheduler.StepResult;
    pub const Session = ServerSession;

    allocator: std.mem.Allocator,
    io: std.Io,
    listener: listener_mod.Listener,
    config: Config,
    udp_rx_buf: []u8,
    udp_tx_buf: []u8,
    sessions: std.ArrayList(ServerSession) = .empty,
    wake_state: wake_mod.Handle = .{},
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: ServerOptions,
    ) !Server {
        var listener = try listener_mod.Listener.init(allocator, io, options);
        errdefer listener.deinit();

        const udp_rx_buf = try allocator.alloc(u8, options.udp_rx_buffer_size);
        errdefer allocator.free(udp_rx_buf);
        const udp_tx_buf = try allocator.alloc(u8, options.udp_tx_buffer_size);
        errdefer allocator.free(udp_tx_buf);

        var sessions = std.ArrayList(ServerSession).empty;
        errdefer sessions.deinit(allocator);
        try sessions.ensureTotalCapacity(allocator, options.max_concurrent_connections);

        return .{
            .allocator = allocator,
            .io = io,
            .listener = listener,
            .config = Config.fromServer(options),
            .udp_rx_buf = udp_rx_buf,
            .udp_tx_buf = udp_tx_buf,
            .sessions = sessions,
            .wake_state = wake_mod.Handle.init(),
        };
    }

    pub fn deinit(self: *Server) void {
        self.requestClose();
        var i: usize = self.sessions.items.len;
        while (i > 0) {
            i -= 1;
            self.sessions.items[i].deinit(self.allocator);
        }
        self.sessions.deinit(self.allocator);
        self.wake_state.deinit();
        self.listener.deinit();
        self.allocator.free(self.udp_rx_buf);
        self.allocator.free(self.udp_tx_buf);
        self.* = undefined;
    }

    pub fn getAddress(self: *const Server) Net.IpAddress {
        return self.listener.getAddress();
    }

    pub fn sessionCapacity(self: *const Server) u32 {
        return self.listener.sessionCapacity();
    }

    pub fn quicConnectionCount(self: *const Server) usize {
        return self.listener.sessionCount();
    }

    pub fn sessionCount(self: *const Server) usize {
        return self.sessions.items.len;
    }

    pub fn sessionAt(self: *Server, index: usize) ?*ServerSession {
        if (index >= self.sessions.items.len) return null;
        return &self.sessions.items[index];
    }

    pub fn sessionById(self: *Server, id: u64) ?*ServerSession {
        const index = self.findSessionIndexById(id) orelse return null;
        return &self.sessions.items[index];
    }

    pub fn receiveOne(self: *Server) !?listener_mod.FeedOutcome {
        const result = try self.receiveOneFor(self.listener.receive_timeout);
        return result.outcome;
    }

    pub fn step(self: *Server) !void {
        _ = try self.stepOnce(.wait);
    }

    pub fn stepOnce(self: *Server, mode: StepMode) !StepResult {
        var now_us = self.listener.nowUs();
        const next_deadline_us = self.nextTimerDeadlineUs(now_us);
        const waited_for = scheduler.receiveWaitDuration(.{
            .mode = mode,
            .receive_timeout = self.listener.receive_timeout,
            .now_us = now_us,
            .next_deadline_us = next_deadline_us,
            .immediate_work = self.hasImmediateWork(),
            .wake_supported = self.wake_state.isSupported(),
        });

        var result = StepResult{
            .waited_for = waited_for,
            .next_deadline_us = next_deadline_us,
        };

        const receive_result = try self.receiveOneFor(waited_for);
        result.received_datagram = receive_result.received_datagram;
        result.wake_drained = receive_result.wake_drained;

        now_us = self.listener.nowUs();
        try self.stepAllSessionsAt(now_us);

        now_us = self.listener.nowUs();
        try self.listener.tick(now_us);
        try self.stepAllSessionsAt(now_us);

        self.reapClosedSessions();
        result.closed = self.isClosing() and self.sessionCount() == 0 and self.quicConnectionCount() == 0;
        return result;
    }

    pub fn stepSession(self: *Server, index: usize) !void {
        if (index >= self.sessions.items.len) return error.InvalidSession;
        const now_us = self.listener.nowUs();
        try self.stepSessionAt(&self.sessions.items[index], now_us);
        self.reapClosedSessions();
    }

    pub fn run(self: *Server) void {
        while (!self.isClosing()) {
            _ = self.stepOnce(.wait) catch |err| {
                log.debug("QUIC server step failed: {}", .{err});
                self.close();
                break;
            };
        }

        while (self.sessionCount() > 0 or self.quicConnectionCount() > 0) {
            _ = self.stepOnce(.poll) catch |err| {
                log.debug("QUIC server shutdown step failed: {}", .{err});
                break;
            };
            if (self.sessionCount() == 0 and self.quicConnectionCount() == 0) break;
            std.Io.sleep(self.io, std.Io.Duration.fromMilliseconds(1), .awake) catch break;
        }
    }

    pub fn close(self: *Server) void {
        self.requestClose();
        for (self.sessions.items) |*session| {
            session.close();
        }
        self.wake();
    }

    pub fn requestClose(self: *Server) void {
        _ = self.close_requested.swap(true, .acq_rel);
        for (self.sessions.items) |*session| {
            session.requestClose();
        }
        self.wake();
    }

    pub fn isClosing(self: *const Server) bool {
        return self.close_requested.load(.acquire);
    }

    pub fn wake(self: *Server) void {
        self.wake_state.request();
    }

    fn receiveOneFor(self: *Server, wait_duration: std.Io.Duration) !ReceiveResult {
        var result = ReceiveResult{};
        if (self.wake_state.consumeRequested()) {
            result.wake_drained = true;
            return result;
        }

        var receive_timeout = wait_duration;
        if (try self.wake_state.waitForSocket(self.listener.socket.handle, wait_duration)) |poll_result| {
            result.wake_drained = poll_result.wake_drained;
            if (!poll_result.socket_ready) return result;
            receive_timeout = std.Io.Duration.zero;
        }

        const msg = self.listener.socket.receiveTimeout(self.io, self.udp_rx_buf, .{
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
        result.outcome = try self.listener.feedDatagram(msg.data, msg.from, self.listener.nowUs());
        try self.listener.drainStatelessResponses();
        result.accepted_sessions = try self.adoptAcceptedSessions();
        return result;
    }

    fn adoptAcceptedSessions(self: *Server) !usize {
        var adopted: usize = 0;
        const slots = self.listener.server.iterator();
        for (slots, 0..) |slot, ordinal| {
            if (self.findSessionIndexBySlot(slot) != null) continue;
            var new_session = try ServerSession.init(
                self.allocator,
                session_mod.AcceptedSession.fromSession(ordinal, session_mod.Session.fromSlot(slot)),
                self.config,
                &self.wake_state,
            );
            errdefer new_session.deinit(self.allocator);
            try self.sessions.append(self.allocator, new_session);
            adopted += 1;
        }
        self.refreshSessionOrdinals();
        return adopted;
    }

    fn refreshSessionOrdinals(self: *Server) void {
        const slots = self.listener.server.iterator();
        for (self.sessions.items) |*server_session| {
            for (slots, 0..) |slot, ordinal| {
                if (server_session.slot == slot) {
                    server_session.ordinal = ordinal;
                    break;
                }
            }
        }
    }

    fn stepAllSessionsAt(self: *Server, now_us: u64) !void {
        var index: usize = 0;
        while (index < self.sessions.items.len) : (index += 1) {
            self.stepSessionAt(&self.sessions.items[index], now_us) catch |err| {
                log.debug("QUIC server session step failed: {}", .{err});
                self.sessions.items[index].terminateInternalError(err);
                try self.flushClosingSession(&self.sessions.items[index], now_us);
            };
        }
    }

    fn stepSessionAt(self: *Server, server_session: *ServerSession, now_us: u64) !void {
        const conn = server_session.activeQuicConnection() orelse return;
        if (server_session.isClosing()) {
            try self.flushClosingSession(server_session, now_us);
            try conn.tick(now_us);
            try self.listener.drainAcceptedSessionDatagrams(server_session.acceptedSession(), self.udp_tx_buf, now_us);
            return;
        }

        if (!conn.handshakeDone()) {
            try conn.advance();
        }
        try server_session.serviceMode(conn);
        try self.listener.drainAcceptedSessionDatagrams(server_session.acceptedSession(), self.udp_tx_buf, now_us);

        try conn.tick(now_us);
        try server_session.serviceMode(conn);
        try self.listener.drainAcceptedSessionDatagrams(server_session.acceptedSession(), self.udp_tx_buf, now_us);

        if (conn.isClosed() and server_session.outboundEmpty()) {
            server_session.requestClose();
        }
        if (server_session.isClosing()) {
            try self.flushClosingSession(server_session, now_us);
        }
    }

    fn flushClosingSession(self: *Server, server_session: *ServerSession, now_us: u64) !void {
        server_session.close_controller.closeActive(
            server_session.activeQuicConnection(),
            .normal,
            null,
        );
        try self.listener.drainAcceptedSessionDatagrams(server_session.acceptedSession(), self.udp_tx_buf, now_us);
        server_session.invokeCloseCallbackOnce();
    }

    fn reapClosedSessions(self: *Server) void {
        var index: usize = 0;
        while (index < self.sessions.items.len) {
            const conn = self.sessions.items[index].activeQuicConnection() orelse {
                self.removeSessionAt(index);
                continue;
            };
            if (conn.closeState() == .closed) {
                self.removeSessionAt(index);
                continue;
            }
            index += 1;
        }
        _ = self.listener.reapClosedSessions();
        self.refreshSessionOrdinals();
    }

    fn removeSessionAt(self: *Server, index: usize) void {
        self.sessions.items[index].deinit(self.allocator);
        _ = self.sessions.swapRemove(index);
    }

    fn hasImmediateWork(self: *Server) bool {
        if (self.wake_state.isRequested()) return true;
        for (self.sessions.items) |*server_session| {
            const conn = server_session.activeQuicConnection() orelse continue;
            if (conn.canSend()) return true;
            if (server_session.hasImmediateWork(conn)) return true;
            if (server_session.isClosing()) return true;
        }
        return false;
    }

    fn nextTimerDeadlineUs(self: *Server, now_us: u64) ?u64 {
        var next: ?u64 = null;
        for (self.sessions.items) |*server_session| {
            const conn = server_session.activeQuicConnection() orelse continue;
            const deadline = conn.nextTimerDeadline(now_us) orelse continue;
            if (next == null or deadline.at_us < next.?) {
                next = deadline.at_us;
            }
        }
        return next;
    }

    fn findSessionIndexBySlot(self: *Server, slot: *quic_zig.Server.Slot) ?usize {
        for (self.sessions.items, 0..) |*server_session, index| {
            if (server_session.slot == slot) return index;
        }
        return null;
    }

    fn findSessionIndexById(self: *Server, id: u64) ?usize {
        for (self.sessions.items, 0..) |*server_session, index| {
            if (server_session.id == id) return index;
        }
        return null;
    }
};

pub const ReceiveResult = struct {
    received_datagram: bool = false,
    wake_drained: bool = false,
    outcome: ?listener_mod.FeedOutcome = null,
    accepted_sessions: usize = 0,
};

pub const ServerSession = struct {
    pub const MessageCallback = CallbackLifecycle.MessageCallback;
    pub const ErrorCallback = CallbackLifecycle.ErrorCallback;
    pub const CloseCallback = CallbackLifecycle.CloseCallback;

    const CallbackLifecycle = callback_lifecycle_mod.State(ServerSession);
    const Dispatch = connection_dispatch.State(ServerSession);
    const Termination = connection_termination.State(ServerSession);

    allocator: std.mem.Allocator,
    slot: *quic_zig.Server.Slot,
    id: u64,
    ordinal: usize,
    peer_addr: ?Net.IpAddress,
    stream_read_buf: []u8,
    max_message_bytes: usize,
    mode: quic_options.TransportMode,
    baseline: BaselineEngine,
    native: NativeEngine,
    close_controller: CloseController,
    wake_state: *wake_mod.Handle,
    callback_lifecycle: CallbackLifecycle = .{},
    close_callback_invoked: bool = false,

    fn init(
        allocator: std.mem.Allocator,
        accepted: session_mod.AcceptedSession,
        config: Config,
        wake_state: *wake_mod.Handle,
    ) !ServerSession {
        const stream_read_buf = try allocator.alloc(u8, config.stream_read_buffer_size);
        errdefer allocator.free(stream_read_buf);

        return .{
            .allocator = allocator,
            .slot = accepted.session.slot,
            .id = accepted.session.slot.slot_id,
            .ordinal = accepted.ordinal,
            .peer_addr = accepted.peerAddress(),
            .stream_read_buf = stream_read_buf,
            .max_message_bytes = config.max_message_bytes,
            .mode = config.mode,
            .baseline = BaselineEngine.init(
                allocator,
                config.max_message_bytes,
                config.max_outbound_queue_items,
                config.max_outbound_queue_bytes,
            ),
            .native = NativeEngine.init(
                allocator,
                .server,
                config.max_message_bytes,
                config.max_outbound_queue_items,
                config.max_outbound_queue_bytes,
                config.native_options,
            ),
            .close_controller = CloseController.init(config.reveal_close_reason_on_wire),
            .wake_state = wake_state,
        };
    }

    fn deinit(self: *ServerSession, allocator: std.mem.Allocator) void {
        Termination.enterClosing(self);
        self.baseline.deinit(allocator);
        self.native.deinit(allocator);
        self.callback_lifecycle.clearCallbacks();
        allocator.free(self.stream_read_buf);
        self.* = undefined;
    }

    pub fn start(
        self: *ServerSession,
        ctx: *anyopaque,
        on_message: MessageCallback,
        on_error: ErrorCallback,
        on_close: CloseCallback,
    ) void {
        self.callback_lifecycle.start(ctx, on_message, on_error, on_close);
    }

    pub fn context(self: *const ServerSession) ?*anyopaque {
        return self.callback_lifecycle.context();
    }

    pub fn sendFrame(self: *ServerSession, frame: []const u8) !void {
        return try Dispatch.sendFrame(self, frame);
    }

    pub fn close(self: *ServerSession) void {
        Termination.close(self);
    }

    pub fn requestClose(self: *ServerSession) void {
        Termination.requestClose(self);
    }

    pub fn isClosing(self: *const ServerSession) bool {
        return Termination.isClosing(self);
    }

    pub fn closeStatus(self: *const ServerSession) ?quic_close.Status {
        return Termination.status(self);
    }

    pub fn acceptedSession(self: *ServerSession) session_mod.AcceptedSession {
        return session_mod.AcceptedSession.fromSession(self.ordinal, session_mod.Session.fromSlot(self.slot));
    }

    pub fn activeQuicConnection(self: *ServerSession) ?*quic_zig.Connection {
        return self.slot.conn;
    }

    pub fn peerAddress(self: *const ServerSession) ?Net.IpAddress {
        return self.peer_addr;
    }

    pub fn wake(self: *ServerSession) void {
        self.wake_state.request();
    }

    fn terminateInternalError(self: *ServerSession, err: anyerror) void {
        Termination.internalError(self, err);
    }

    fn serviceMode(self: *ServerSession, conn: *quic_zig.Connection) !void {
        try mode_router.fromConnection(self).service(self.engineOwner(), conn);
    }

    fn outboundEmpty(self: *ServerSession) bool {
        return mode_router.fromConnection(self).outboundEmpty();
    }

    fn hasImmediateWork(self: *ServerSession, conn: *quic_zig.Connection) bool {
        if (!self.outboundEmpty()) {
            return mode_router.fromConnection(self).hasImmediateWork(.server, conn);
        }
        return mode_router.fromConnection(self).hasImmediateWork(.server, conn);
    }

    fn invokeCloseCallbackOnce(self: *ServerSession) void {
        if (self.close_callback_invoked) return;
        const cb = self.callback_lifecycle.closeCallback() orelse return;
        self.close_callback_invoked = true;
        self.callback_lifecycle.invokeClose(self, cb);
    }

    fn engineOwner(self: *ServerSession) EngineOwner {
        return .{
            .ptr = self,
            .allocator = self.allocator,
            .role = .server,
            .max_message_bytes = self.max_message_bytes,
            .stream_read_buf = self.stream_read_buf,
            .is_closing = engineIsClosing,
            .callbacks_ready = engineCallbacksReady,
            .dispatch_rpc_frame = engineDispatchRpcFrame,
            .terminate_frame_error = engineTerminateFrameError,
            .deinit_requested = engineDeinitRequested,
        };
    }

    fn cast(ptr: *anyopaque) *ServerSession {
        return @ptrCast(@alignCast(ptr));
    }

    fn engineIsClosing(ptr: *anyopaque) bool {
        return cast(ptr).close_controller.isRequested();
    }

    fn engineCallbacksReady(ptr: *anyopaque) bool {
        return cast(ptr).callback_lifecycle.callbacksReady();
    }

    fn engineDispatchRpcFrame(ptr: *anyopaque, frame: []const u8) !void {
        try Dispatch.dispatchRpcFrame(cast(ptr), frame);
    }

    fn engineTerminateFrameError(ptr: *anyopaque, err: anyerror) void {
        Termination.frameError(cast(ptr), err);
    }

    fn engineDeinitRequested(ptr: *anyopaque) bool {
        return cast(ptr).callback_lifecycle.deinitRequested();
    }
};
