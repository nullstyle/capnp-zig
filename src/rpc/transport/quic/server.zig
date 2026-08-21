const std = @import("std");
const builtin = @import("builtin");
const quic_zig = @import("quic");

const baseline_engine = @import("baseline_engine.zig");
const callback_lifecycle_mod = @import("callback_lifecycle.zig");
const datagram_drop = @import("datagram_drop.zig");
const datagram_io = @import("datagram_io.zig");
const close_controller_mod = @import("close_controller.zig");
const connection_dispatch = @import("connection_dispatch.zig");
const connection_init = @import("connection_init.zig");
const connection_termination = @import("connection_termination.zig");
const engine_owner_mod = @import("engine_owner.zig");
const events = @import("../../events.zig");
const endpoint_mod = @import("endpoint.zig");
const listener_mod = @import("listener.zig");
const mode_router = @import("mode_router.zig");
const native_engine = @import("native_engine.zig");
const non_windows_receive = @import("non_windows_receive.zig");
const quic_close = @import("close.zig");
const quic_options = @import("options.zig");
const scheduler = @import("scheduler.zig");
const session_mod = @import("session.zig");
const udp_receive_bridge = @import("udp_receive_bridge.zig");
const wake_mod = @import("wake.zig");

const Net = std.Io.net;

const BaselineEngine = baseline_engine.BaselineEngine;
const CloseController = close_controller_mod.Controller;
const Config = connection_init.Config;
const EngineOwner = engine_owner_mod.Owner;
const Role = endpoint_mod.Role;
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
    // Session addresses are part of the transport binding once a Peer is
    // attached. Keep each session in its own allocation so ArrayList growth
    // and swap-removal can never move a live callback target.
    sessions: std.ArrayList(*ServerSession) = .empty,
    wake_state: wake_mod.Handle = .{},
    udp_receive: udp_receive_bridge.Bridge = .{},
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Identifies the thread that drives the step/run loop. `sessions` is only
    /// ever appended to or swap-removed on this thread, so cross-thread
    /// `close`/`requestClose` must never touch the list — they only raise the
    /// `close_requested` flag and wake the loop, which then closes every
    /// session on its own thread. Claimed on the first loop-thread step and
    /// used by `assertLoopThread` for a debug-only affinity check. Widened to
    /// `u64` (not `std.Thread.Id`, whose width varies by target) so the
    /// rendered api-snapshot surface is platform-stable.
    loop_thread_id: ?u64 = null,

    /// When true, the loop-thread affinity checks also run in release builds
    /// (always on in Debug). Mirrors the connection field.
    runtime_thread_checks: bool = false,

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

        var sessions = std.ArrayList(*ServerSession).empty;
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

    /// Tear down the server and every remaining session.
    ///
    /// Teardown contract: quiesce external wakers first. Any thread that
    /// holds a `*ServerSession` (or calls `Server.wake`) must have stopped
    /// and synchronized before `deinit` runs — sessions and the wake/receive
    /// state they borrow are destroyed here, so a late cross-thread
    /// `wake`/`close`/`sendFrame` is a use-after-free. See
    /// `ServerSession.wake` for the per-session contract.
    pub fn deinit(self: *Server) void {
        self.requestClose();
        self.udp_receive.cancel(self.io);
        var i: usize = self.sessions.items.len;
        while (i > 0) {
            i -= 1;
            // Fire on_close exactly once for every still-live session before
            // dropping it, so tearing down the server with active sessions
            // notifies their owners (matching connection_loop.run).
            const session = self.sessions.items[i];
            session.invokeCloseCallbackOnce();
            session.deinit(self.allocator);
            self.allocator.destroy(session);
        }
        self.sessions.deinit(self.allocator);
        self.wake_state.deinit();
        self.listener.deinit();
        self.allocator.free(self.udp_rx_buf);
        self.allocator.free(self.udp_tx_buf);
        self.* = undefined;
    }

    /// Listener address, immutable after init, so intentionally not
    /// loop-thread-guarded (safe to read from any thread).
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
        self.assertLoopThread();
        return self.sessions.items.len;
    }

    pub fn sessionAt(self: *Server, index: usize) ?*ServerSession {
        self.assertLoopThread();
        if (index >= self.sessions.items.len) return null;
        return self.sessions.items[index];
    }

    pub fn sessionById(self: *Server, id: u64) ?*ServerSession {
        self.assertLoopThread();
        const index = self.findSessionIndexById(id) orelse return null;
        return self.sessions.items[index];
    }

    /// Receive and feed at most one datagram.
    ///
    /// `null` means nothing was fed to QUIC this call — a timeout, a wake, or
    /// an oversized datagram dropped as a per-datagram fault. See
    /// `droppedDatagramCount` for the drop tally; `stepOnce` reports the same
    /// fact per step as `StepResult.dropped_datagram`.
    pub fn receiveOne(self: *Server) !?listener_mod.FeedOutcome {
        const result = try self.receiveOneFor(self.listener.receive_timeout);
        return result.outcome;
    }

    /// Oversized inbound datagrams dropped on this server's UDP endpoint.
    ///
    /// A non-zero, growing count with healthy sessions means some peer is
    /// sending datagrams larger than `udp_rx_buffer_size` — either an attacker
    /// (harmless, and the point of dropping rather than failing) or a
    /// legitimate peer behind a path MTU this endpoint is not sized for, which
    /// is a misconfiguration this counter is how you notice.
    pub fn droppedDatagramCount(self: *const Server) u64 {
        self.assertLoopThread();
        return self.listener.droppedDatagramCount();
    }

    pub fn step(self: *Server) !void {
        _ = try self.stepOnce(.wait);
    }

    pub fn stepOnce(self: *Server, mode: StepMode) !StepResult {
        self.claimLoopThread();
        // Apply any cross-thread server- or session-level close requests on the
        // loop thread before touching the session list, so all list mutation
        // and close-state mutation stays single-threaded.
        self.closeAllSessionsOnLoop();
        self.drainSessionCloseRequests();

        var now_us = self.listener.nowUs();
        const next_deadline_us = self.nextTimerDeadlineUs(now_us);
        const waited_for = scheduler.receiveWaitDuration(.{
            .mode = mode,
            .receive_timeout = self.listener.receive_timeout,
            .now_us = now_us,
            .next_deadline_us = next_deadline_us,
            .immediate_work = self.hasImmediateWork(),
            .wake_supported = self.wake_state.isSupported() or builtin.target.os.tag == .windows,
        });

        var result = StepResult{
            .waited_for = waited_for,
            .next_deadline_us = next_deadline_us,
        };

        if (self.isClosing()) {
            // Stop the sole Windows receive before any session close callback
            // can free its transport owner. QUIC close datagrams are outbound
            // and do not require another receive to be launched.
            self.udp_receive.cancel(self.io);
        } else {
            const receive_result = try self.receiveOneFor(waited_for);
            result.received_datagram = receive_result.received_datagram;
            result.dropped_datagram = receive_result.dropped_datagram;
            result.wake_drained = receive_result.wake_drained;
        }

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
        self.claimLoopThread();
        self.closeAllSessionsOnLoop();
        self.drainSessionCloseRequests();
        if (index >= self.sessions.items.len) return error.InvalidSession;
        const now_us = self.listener.nowUs();
        try self.stepSessionAt(self.sessions.items[index], now_us);
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

    /// Request shutdown of the server and all of its sessions.
    ///
    /// Safe to call from any thread. Only raises the atomic `close_requested`
    /// flag and wakes the loop; the actual per-session close runs on the loop
    /// thread via `closeAllSessionsOnLoop`, which is the sole mutator of the
    /// session list. This avoids racing the loop's accept/reap path.
    pub fn close(self: *Server) void {
        self.requestClose();
    }

    /// Request shutdown without forcing an immediate connection-level close.
    ///
    /// Safe to call from any thread; see `close` for the cross-thread contract.
    pub fn requestClose(self: *Server) void {
        _ = self.close_requested.swap(true, .acq_rel);
        self.wake();
    }

    pub fn isClosing(self: *const Server) bool {
        return self.close_requested.load(.acquire);
    }

    pub fn wake(self: *Server) void {
        self.wake_state.request();
        if (comptime builtin.target.os.tag == .windows) {
            self.udp_receive.wake(self.io);
        }
    }

    /// Record the current thread as the loop thread on first step. Subsequent
    /// steps assert affinity in debug builds so accidental cross-thread
    /// stepping (which would race the session list) fails loudly.
    fn claimLoopThread(self: *Server) void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        const loop_tid = self.loop_thread_id orelse {
            self.loop_thread_id = std.Thread.getCurrentId();
            return;
        };
        if (builtin.mode == .debug) {
            if (std.Thread.getCurrentId() != loop_tid) {
                @panic("QUIC Server stepped from a thread other than the loop thread");
            }
        }
    }

    /// Read-side counterpart to `claimLoopThread`: asserts the caller is on the
    /// loop thread without latching, so it is safe on `*const Server`. Skips
    /// until the loop thread is claimed. Guards the session-list accessors,
    /// which read `sessions` (mutated loop-thread-only). Debug-always, release
    /// opt-in via `runtime_thread_checks`.
    fn assertLoopThread(self: *const Server) void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        if (comptime builtin.mode != .debug) {
            if (!self.runtime_thread_checks) return;
        }
        const loop_tid = self.loop_thread_id orelse return;
        if (std.Thread.getCurrentId() != loop_tid) {
            @panic("QUIC Server session accessor called from a thread other than the loop thread");
        }
    }

    /// Loop-thread close of every live session when the server is shutting
    /// down. Iterates and mutates close state only on the loop thread, so it
    /// cannot race the accept/reap path.
    fn closeAllSessionsOnLoop(self: *Server) void {
        if (!self.isClosing()) return;
        for (self.sessions.items) |session| {
            session.closeOnLoop();
        }
    }

    /// Loop-thread drain of any per-session cross-thread `requestClose`, run
    /// before the session list is otherwise touched.
    fn drainSessionCloseRequests(self: *Server) void {
        for (self.sessions.items) |session| {
            session.drainPendingClose();
        }
    }

    /// Receive one datagram for the whole endpoint.
    ///
    /// Truncation is a per-datagram fault on both platforms, matching the
    /// single-connection loop: drop, count, keep serving. It matters more
    /// here than there. Failing this call fails `stepOnce`, and `run` responds
    /// to a failed step by closing the server — so returning an error for an
    /// oversized datagram would let any host that can reach this UDP port kill
    /// every session on it with one spoofed packet. Socket-fatal errors still
    /// propagate.
    fn receiveOneFor(self: *Server, wait_duration: std.Io.Duration) !ReceiveResult {
        var result = ReceiveResult{};
        if (self.wake_state.consumeRequested()) {
            if (comptime builtin.target.os.tag == .windows) {
                self.udp_receive.consumeWake(self.io);
            }
            result.wake_drained = true;
            return result;
        }

        if (comptime builtin.target.os.tag == .windows) {
            // Same ICMP-fault classification as `datagram_io.receiveOneWindows`
            // (the single-connection loop): PortUnreachable /
            // ConnectionResetByPeer describe a datagram that did not arrive,
            // not a broken endpoint, and an off-path packet can provoke them.
            // A bare `try` here let a departed peer's ICMP feedback fail
            // `stepOnce` — on Windows that killed the whole fanout endpoint.
            const received = self.udp_receive.receive(
                self.io,
                self.listener.socket,
                self.udp_rx_buf,
                wait_duration,
            ) catch |err| {
                if (datagram_io.isTransientPeerFault(err)) {
                    datagram_drop.reportPeerFault(err);
                    result.dropped_datagram = true;
                    return result;
                }
                return err;
            };
            switch (received) {
                .timeout => return result,
                .wake => {
                    result.wake_drained = self.wake_state.consumeRequested();
                    return result;
                },
                .truncated => {
                    self.listener.recordDroppedDatagram(self.udp_rx_buf.len);
                    result.dropped_datagram = true;
                    return result;
                },
                .datagram => |msg| {
                    result.received_datagram = true;
                    result.outcome = try self.listener.feedDatagram(
                        msg.data,
                        msg.from,
                        self.listener.nowUs(),
                    );
                    try self.listener.drainStatelessResponses();
                    result.accepted_sessions = try self.adoptAcceptedSessions();
                    return result;
                },
            }
        }

        var receive_timeout = wait_duration;
        if (try self.wake_state.waitForSocket(self.listener.socket.handle, wait_duration)) |poll_result| {
            result.wake_drained = poll_result.wake_drained;
            if (!poll_result.socket_ready) return result;
            receive_timeout = std.Io.Duration.zero;
        }

        const msg = non_windows_receive.receive(&self.listener.socket, self.io, self.udp_rx_buf, receive_timeout) catch |err| switch (err) {
            error.Timeout => return result,
            else => {
                // Mirror of the Windows arm above and of
                // `datagram_io.receiveOne`: ICMP-driven faults are
                // per-datagram, never endpoint-fatal.
                if (datagram_io.isTransientPeerFault(err)) {
                    datagram_drop.reportPeerFault(err);
                    result.dropped_datagram = true;
                    return result;
                }
                return err;
            },
        };
        if (msg.flags.trunc) {
            self.listener.recordDroppedDatagram(self.udp_rx_buf.len);
            result.dropped_datagram = true;
            return result;
        }

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
            const new_session = try self.allocator.create(ServerSession);
            errdefer self.allocator.destroy(new_session);
            new_session.* = try ServerSession.init(
                self.allocator,
                session_mod.AcceptedSession.fromSession(ordinal, session_mod.Session.fromSlot(slot)),
                self.config,
                &self.wake_state,
                &self.udp_receive,
                self.io,
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
        for (self.sessions.items) |server_session| {
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
            const session = self.sessions.items[index];
            self.stepSessionAt(session, now_us) catch |err| {
                log.debug("QUIC server session step failed: {}", .{err});
                session.terminateInternalError(err);
                try self.flushClosingSession(session, now_us);
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

        try conn.advance();
        try server_session.serviceMode(conn, now_us);
        try self.listener.drainAcceptedSessionDatagrams(server_session.acceptedSession(), self.udp_tx_buf, now_us);

        try conn.tick(now_us);
        try server_session.serviceMode(conn, now_us);
        try self.listener.drainAcceptedSessionDatagrams(server_session.acceptedSession(), self.udp_tx_buf, now_us);

        // Drive the session peer's deadline sweep on the step cadence,
        // after the service passes (an arrived Return wins over its
        // deadline) and under callback-depth accounting (a re-entrant
        // deinit defers). Mirrors connection_loop.stepOnce.
        server_session.invokeTickFloored(now_us);

        if (conn.isClosed() and server_session.outboundEmpty()) {
            server_session.closeOnLoop();
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
        // Guarantee on_close fires exactly once for every session before it is
        // dropped — mirroring connection_loop.run's unconditional close
        // callback. invokeCloseCallbackOnce is idempotent (a session already
        // flushed through flushClosingSession will not double-fire) and a
        // no-op when no close callback is registered.
        const session = self.sessions.swapRemove(index);
        session.invokeCloseCallbackOnce();
        session.deinit(self.allocator);
        self.allocator.destroy(session);
    }

    fn hasImmediateWork(self: *Server) bool {
        if (self.wake_state.isRequested()) return true;
        for (self.sessions.items) |server_session| {
            const conn = server_session.activeQuicConnection() orelse continue;
            if (conn.canSend()) return true;
            if (server_session.hasImmediateWork(conn)) return true;
            if (server_session.isClosing()) return true;
        }
        return false;
    }

    fn nextTimerDeadlineUs(self: *Server, now_us: u64) ?u64 {
        var next: ?u64 = null;
        for (self.sessions.items) |server_session| {
            const conn = server_session.activeQuicConnection() orelse continue;
            const deadline = conn.nextTimerDeadline(now_us) orelse continue;
            if (next) |current| {
                if (deadline.at_us >= current) continue;
                next = deadline.at_us;
            } else {
                next = deadline.at_us;
            }
        }
        return next;
    }

    fn findSessionIndexBySlot(self: *Server, slot: *quic_zig.Server.Slot) ?usize {
        for (self.sessions.items, 0..) |server_session, index| {
            if (server_session.slot == slot) return index;
        }
        return null;
    }

    fn findSessionIndexById(self: *Server, id: u64) ?usize {
        for (self.sessions.items, 0..) |server_session, index| {
            if (server_session.id == id) return index;
        }
        return null;
    }
};

pub const ReceiveResult = struct {
    received_datagram: bool = false,
    /// An oversized datagram arrived and was dropped instead of being fed to
    /// QUIC. Never set together with `received_datagram`, and never a reason
    /// to stop serving: see `receiveOneFor`.
    dropped_datagram: bool = false,
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
    role: Role,
    peer_addr: ?Net.IpAddress,
    stream_read_buf: []u8,
    max_message_bytes: usize,
    mode: quic_options.TransportMode,
    observer: ?events.Observer,
    baseline: BaselineEngine,
    native: NativeEngine,
    close_controller: CloseController,
    wake_state: *wake_mod.Handle,
    udp_receive: *udp_receive_bridge.Bridge,
    io: std.Io,
    callback_lifecycle: CallbackLifecycle = .{},
    close_callback_invoked: bool = false,

    /// Called on the server loop thread on the per-session step cadence
    /// (floored at `quic_options.min_tick_interval_us`).
    /// `Peer.attachConnection` wires this to the peer's deadline sweep —
    /// without it, call deadlines silently never fire for server-side
    /// peers over QUIC. Mirrors the client Connection's `on_tick`.
    on_tick: ?*const fn (conn: *ServerSession) void = null,

    /// Wall-clock of the last `on_tick` invocation for this session.
    last_tick_us: u64 = 0,
    /// True once the `.closing` connection phase has been emitted for this
    /// session, so the event fires at most once. Loop-thread only (see
    /// `Termination.emitClosingOnce`).
    closing_emitted: bool = false,

    fn init(
        allocator: std.mem.Allocator,
        accepted: session_mod.AcceptedSession,
        config: Config,
        wake_state: *wake_mod.Handle,
        udp_receive: *udp_receive_bridge.Bridge,
        io: std.Io,
    ) !ServerSession {
        const stream_read_buf = try allocator.alloc(u8, config.stream_read_buffer_size);
        errdefer allocator.free(stream_read_buf);

        return .{
            .allocator = allocator,
            .slot = accepted.session.slot,
            .id = accepted.session.slot.slot_id,
            .ordinal = accepted.ordinal,
            .role = .server,
            .peer_addr = accepted.peerAddress(),
            .stream_read_buf = stream_read_buf,
            .max_message_bytes = config.max_message_bytes,
            .mode = config.mode,
            .observer = config.observer,
            .baseline = BaselineEngine.init(
                allocator,
                config.max_message_bytes,
                config.max_outbound_queue_items,
                config.max_outbound_queue_bytes,
                false,
            ),
            .native = NativeEngine.init(
                allocator,
                .server,
                config.max_message_bytes,
                config.max_outbound_queue_items,
                config.max_outbound_queue_bytes,
                config.native_options,
                false,
            ),
            .close_controller = CloseController.init(config.reveal_close_reason_on_wire),
            .wake_state = wake_state,
            .udp_receive = udp_receive,
            .io = io,
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

    /// Invoke this session's `on_tick` (the peer deadline sweep) if the
    /// cadence floor has elapsed. Loop-thread only; runs under
    /// callback-depth accounting so a re-entrant deinit is deferred.
    fn invokeTickFloored(self: *ServerSession, now_us: u64) void {
        const cb = self.on_tick orelse return;
        if (now_us -| self.last_tick_us < quic_options.min_tick_interval_us) return;
        self.last_tick_us = now_us;
        self.callback_lifecycle.invokeTick(self, cb);
    }

    pub fn sendFrame(self: *ServerSession, frame: []const u8) !void {
        return try Dispatch.sendFrame(self, frame);
    }

    /// Request a normal close of this session. Safe to call from any thread:
    /// only flags + wakes; the run loop performs the engine close and status
    /// record on its own thread via `drainPendingClose`/`closeOnLoop`, and
    /// emits the deferred `.closing` observer event there (observer callbacks
    /// fire on the loop thread only).
    ///
    /// Lifetime: see `wake` — the `*ServerSession` and the Server-owned state
    /// it reaches through borrowed pointers die when the session is reaped or
    /// the `Server` is deinitialized, so external threads must quiesce first.
    pub fn close(self: *ServerSession) void {
        Termination.requestClose(self);
    }

    /// Request a normal close of this session. Safe to call from any thread;
    /// see `close` for the threading and lifetime contract.
    pub fn requestClose(self: *ServerSession) void {
        Termination.requestClose(self);
    }

    /// Loop-thread synchronous close. Closes the engines and records the
    /// terminal status on the caller's thread; only the run loop may call this.
    fn closeOnLoop(self: *ServerSession) void {
        Termination.close(self);
    }

    /// Loop-thread drain of a deferred cross-thread `close`/`requestClose`.
    fn drainPendingClose(self: *ServerSession) void {
        Termination.drainPendingClose(self);
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

    /// Wake the server loop from any thread.
    ///
    /// Lifetime: `wake_state` and `udp_receive` are borrowed pointers into the
    /// owning `Server`, and the `*ServerSession` itself is destroyed when the
    /// server reaps the session (post-close) or in `Server.deinit`. There is
    /// no handshake that stops an external waker mid-call, so any thread
    /// holding a `*ServerSession` for cross-thread `wake`/`close`/`sendFrame`
    /// must quiesce (stop calling and synchronize, e.g. join the thread or
    /// drain its queue) before the session can be reaped or the server
    /// deinitialized. Typically: `requestClose()` the session, wait for its
    /// `on_close`, and stop using the pointer from then on.
    pub fn wake(self: *ServerSession) void {
        self.wake_state.request();
        if (comptime builtin.target.os.tag == .windows) {
            self.udp_receive.wake(self.io);
        }
    }

    fn terminateInternalError(self: *ServerSession, err: anyerror) void {
        Termination.internalError(self, err);
    }

    fn serviceMode(self: *ServerSession, conn: *quic_zig.Connection, now_us: u64) !void {
        try mode_router.fromConnection(self).service(self.engineOwner(), conn, now_us);
    }

    fn outboundEmpty(self: *ServerSession) bool {
        return mode_router.fromConnection(self).outboundEmpty();
    }

    fn hasImmediateWork(self: *ServerSession, conn: *quic_zig.Connection) bool {
        // Only report immediate work when there is actually something queued to
        // send. The engine's hasImmediateWork returns true unconditionally once
        // stream 0 is established (server role), so without this guard an idle
        // established session reports immediate work forever — receiveWaitDuration
        // then returns zero and Server.run() spins the socket at 100% CPU.
        // Mirrors connection_loop.hasImmediateWork.
        if (!self.outboundEmpty()) {
            return mode_router.fromConnection(self).hasImmediateWork(.server, conn);
        }
        return false;
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
            .observer = self.observer,
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
