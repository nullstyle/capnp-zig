const std = @import("std");
const capnpc = @import("capnpc-zig");
const loopback = @import("loopback_test_support.zig");

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const message = capnpc.message;
const Peer = capnpc.rpc.peer.Peer;
const ProvisionIndex = capnpc.rpc.peer.ProvisionIndex;
const quic = capnpc.rpc.transport.quic;
const quic_vat_network = capnpc.rpc.vat.quic_network;

const Mode = quic.TransportMode;

const ClientState = struct {
    pipeline: bool = false,
    payload_size: usize = 0,
    bootstrap_returned: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    bootstrap_target: ?cap_table.ResolvedCap = null,
    call_returned: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    call_returns: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    payload: [4096]u8 = undefined,

    fn onBootstrap(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ClientState = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.ExpectedBootstrapResults;
        const results = ret.results orelse return error.MissingBootstrapResults;
        const descriptor = try results.content.getCapability();
        const resolved = try caps.resolveCapability(descriptor);
        self.bootstrap_target = resolved;
        self.bootstrap_returned.store(true, .release);

        if (self.pipeline) return;
        _ = try peer.sendCallResolved(
            resolved,
            0x5155_4943,
            7,
            self,
            buildCall,
            onCallReturn,
        );
    }

    fn buildCall(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *ClientState = @ptrCast(@alignCast(ctx_ptr));
        if (self.payload_size == 0) {
            _ = try call.initCapTableTyped(0);
            return;
        }
        for (self.payload[0..self.payload_size], 0..) |*byte, index| byte.* = @truncate(index);
        var payload = try call.payloadTyped();
        try payload.setContentData(self.payload[0..self.payload_size]);
        _ = try call.initCapTableTyped(0);
    }

    fn onCallReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ClientState = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.ExpectedCallResults;
        _ = self.call_returns.fetchAdd(1, .acq_rel);
        self.call_returned.store(true, .release);
    }

    fn peerError(ctx: ?*anyopaque, _: *Peer, _: anyerror) void {
        const self: *ClientState = @ptrCast(@alignCast(ctx.?));
        self.failed.store(true, .release);
    }

    fn peerClose(ctx: ?*anyopaque, _: *Peer) void {
        const self: *ClientState = @ptrCast(@alignCast(ctx.?));
        _ = self.closes.fetchAdd(1, .acq_rel);
    }
};

const ServerState = struct {
    calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ServerState = @ptrCast(@alignCast(ctx_ptr));
        _ = self.calls.fetchAdd(1, .acq_rel);
        try peer.sendReturnEmptyStruct(call.question_id);
    }

    fn peerError(ctx: ?*anyopaque, _: *Peer, _: anyerror) void {
        const self: *ServerState = @ptrCast(@alignCast(ctx.?));
        self.failed.store(true, .release);
    }

    fn peerClose(ctx: ?*anyopaque, _: *Peer) void {
        const self: *ServerState = @ptrCast(@alignCast(ctx.?));
        _ = self.closes.fetchAdd(1, .acq_rel);
    }
};

const BasicOptions = struct {
    mode: Mode,
    verify_ca: bool = false,
    pipeline: bool = false,
    payload_size: usize = 0,
};

const BasicResult = struct {
    client_closes: usize,
    server_closes: usize,
};

fn runConnection(conn: *quic.Connection) void {
    conn.run();
}

fn waitForCall(client: *const ClientState, server: *const ServerState) bool {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
        if (client.call_returned.load(.acquire)) return true;
        if (client.failed.load(.acquire) or server.failed.load(.acquire)) return false;
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    return client.call_returned.load(.acquire);
}

fn runBasic(options: BasicOptions) !BasicResult {
    const allocator = std.testing.allocator;
    const native_options = quic.NativeOptions{
        .inline_frame_threshold = 128,
        .max_control_frame_bytes = 256,
        .max_pending_data_streams = 8,
        .max_pending_data_bytes = 16 * 1024,
    };

    var server_conn = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(10),
        .mode = options.mode,
        .native = native_options,
    });
    defer server_conn.deinit();

    var client_conn = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_conn.getAddress(),
        .server_name = "localhost",
        .ca_pem = if (options.verify_ca) loopback.loopback_cert_pem else null,
        .insecure_skip_verify = !options.verify_ca,
        .receive_timeout = std.Io.Duration.fromMilliseconds(10),
        .mode = options.mode,
        .native = native_options,
    });
    defer client_conn.deinit();

    var server_state = ServerState{};
    var server_peer = Peer.init(allocator, &server_conn);
    defer server_peer.deinit();
    server_peer.disableThreadAffinity();
    _ = try server_peer.setBootstrap(.{ .ctx = &server_state, .on_call = ServerState.onCall });
    server_peer.start(&server_state, ServerState.peerError, ServerState.peerClose);

    var client_state = ClientState{
        .pipeline = options.pipeline,
        .payload_size = options.payload_size,
    };
    var client_peer = Peer.init(allocator, &client_conn);
    defer client_peer.deinit();
    client_peer.disableThreadAffinity();
    client_peer.start(&client_state, ClientState.peerError, ClientState.peerClose);

    const bootstrap_qid = try client_peer.sendBootstrap(&client_state, ClientState.onBootstrap);
    if (options.pipeline) {
        _ = try client_peer.sendCallResolved(
            .{ .promised = .{
                .question_id = bootstrap_qid,
                .transform = .{ .list = null },
            } },
            0x5155_4943,
            7,
            &client_state,
            ClientState.buildCall,
            ClientState.onCallReturn,
        );
    }

    var server_thread = try std.Thread.spawn(.{}, runConnection, .{&server_conn});
    var client_thread = try std.Thread.spawn(.{}, runConnection, .{&client_conn});
    var joined = false;
    defer if (!joined) {
        client_conn.requestClose();
        server_conn.requestClose();
        client_thread.join();
        server_thread.join();
    };

    const completed = waitForCall(&client_state, &server_state);
    // Leave time for the automatic Finish generated after the call Return to
    // traverse the same transport before orderly teardown.
    if (completed) loopback.sleepMs(10);
    client_conn.requestClose();
    server_conn.requestClose();
    client_thread.join();
    server_thread.join();
    joined = true;

    if (!completed) return error.QuicPeerRoundTripTimedOut;
    // Joining the transport owners gives a race-free snapshot proving both
    // the Return and its automatic Finish completed their protocol lifecycle.
    try std.testing.expectEqual(@as(u32, 0), client_peer.stats().outbound_questions);
    try std.testing.expectEqual(@as(u32, 0), server_peer.stats().active_inbound_questions);
    try std.testing.expect(client_state.bootstrap_returned.load(.acquire));
    try std.testing.expect(!client_state.failed.load(.acquire));
    try std.testing.expect(!server_state.failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), server_state.calls.load(.acquire));
    return .{
        .client_closes = client_state.closes.load(.acquire),
        .server_closes = server_state.closes.load(.acquire),
    };
}

test "Peer over QUIC verified-CA baseline completes Bootstrap Call Return Finish" {
    _ = try runBasic(.{ .mode = .baseline, .verify_ca = true });
}

test "Peer over native QUIC completes Bootstrap Call Return Finish" {
    _ = try runBasic(.{ .mode = .native });
}

test "Peer over native QUIC pipelines a call on the returned bootstrap capability" {
    _ = try runBasic(.{ .mode = .native, .pipeline = true });
}

test "Peer over native QUIC carries a large RPC frame on a data stream" {
    _ = try runBasic(.{ .mode = .native, .payload_size = 2048 });
}

test "Peer over QUIC graceful close notifies both peers exactly once" {
    const result = try runBasic(.{ .mode = .baseline });
    try std.testing.expectEqual(@as(usize, 1), result.client_closes);
    try std.testing.expectEqual(@as(usize, 1), result.server_closes);
}

test "Peer over QUIC abrupt remote shutdown notifies the surviving peer" {
    const allocator = std.testing.allocator;
    var short_idle_params = quic.defaultTransportParams();
    short_idle_params.max_idle_timeout_ms = 100;
    var server_conn = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .transport_params = short_idle_params,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
    });
    defer server_conn.deinit();
    var client_conn = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_conn.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .transport_params = short_idle_params,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
    });
    defer client_conn.deinit();

    var server_state = ServerState{};
    var server_peer = Peer.init(allocator, &server_conn);
    defer server_peer.deinit();
    _ = try server_peer.setBootstrap(.{ .ctx = &server_state, .on_call = ServerState.onCall });
    server_peer.start(&server_state, ServerState.peerError, ServerState.peerClose);
    var client_state = ClientState{};
    var client_peer = Peer.init(allocator, &client_conn);
    defer client_peer.deinit();
    client_peer.start(&client_state, ClientState.peerError, ClientState.peerClose);
    _ = try client_peer.sendBootstrap(&client_state, ClientState.onBootstrap);

    // Drive establishment and one RPC round trip on the owner thread. Keeping
    // both endpoints single-threaded lets the test make the server's QUIC
    // state terminal without racing a transport owner.
    var waited_ms: u64 = 0;
    while (!client_state.call_returned.load(.acquire) and waited_ms < loopback.loopback_timeout_ms) : (waited_ms += 1) {
        _ = try server_conn.stepOnce(.poll);
        _ = try client_conn.stepOnce(.poll);
        loopback.sleepMs(1);
    }
    if (!client_state.call_returned.load(.acquire)) return error.QuicPeerRoundTripTimedOut;

    // Abruptly retire the local QUIC state without queuing CONNECTION_CLOSE.
    // `run()` observes the already-terminal state, so its flush has no close
    // frame to send. The surviving endpoint can only discover the loss via
    // its negotiated idle timeout.
    const active_server = server_conn.activeQuicConnection() orelse return error.MissingQuicConnection;
    active_server.enterClosed(.local, .transport, 0, 0, "test transport loss", 0);
    server_conn.run();
    try std.testing.expectEqual(@as(usize, 1), server_state.closes.load(.acquire));

    client_conn.run();
    try std.testing.expectEqual(@as(usize, 1), client_state.closes.load(.acquire));
}

const FanoutResult = struct {
    completed: [2]bool,
    closed: [2]usize,
    first_address_stable: bool,
    victim_client: ?usize = null,
    sibling_fresh_call_returned: bool = false,
    victim_server_peer_detached: bool = false,
    post_reap_send_rejected: bool = false,
};

fn driveFanoutStep(
    server: *quic.Server,
    clients: *[2]quic.Connection,
    close_finalized: *[2]bool,
) !void {
    _ = try server.stepOnce(.poll);
    for (clients, 0..) |*conn, index| {
        if (close_finalized[index]) continue;
        if (!conn.isClosing()) _ = try conn.stepOnce(.poll);
        if (conn.isClosing()) {
            // `stepOnce` owns protocol progress; `run` on an already-closing
            // connection performs the exactly-once terminal callback path.
            conn.run();
            close_finalized[index] = true;
        }
    }
    loopback.sleepMs(1);
}

/// Attaches a server `Peer` to each fanout session from the server's accept
/// hook, the event form of scanning `sessionAt()` for new sessions.
const FanoutAcceptor = struct {
    allocator: std.mem.Allocator,
    states: *[2]ServerState,
    peers: *[2]Peer,
    sessions: [2]*quic.ServerSession = undefined,
    count: usize = 0,

    fn onAccepted(ctx: ?*anyopaque, _: *quic.Server, session: *quic.ServerSession) anyerror!void {
        const self: *FanoutAcceptor = @ptrCast(@alignCast(ctx.?));
        if (self.count >= self.peers.len) return error.UnexpectedFanoutSession;
        const index = self.count;
        self.peers[index] = Peer.init(self.allocator, session);
        const peer = &self.peers[index];
        errdefer {
            _ = peer.takeAttachedConnection(*quic.ServerSession);
            peer.deinit();
        }
        _ = try peer.setBootstrap(.{
            .ctx = &self.states[index],
            .on_call = ServerState.onCall,
        });
        peer.start(&self.states[index], ServerState.peerError, ServerState.peerClose);
        self.sessions[index] = session;
        self.count += 1;
    }
};

fn runFanout(close_first: bool) !FanoutResult {
    const allocator = std.testing.allocator;
    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 2,
        .mode = .native,
    });
    defer server.deinit();

    var clients: [2]quic.Connection = undefined;
    var client_initialized: usize = 0;
    defer for (clients[0..client_initialized]) |*conn| conn.deinit();
    for (&clients) |*conn| {
        conn.* = try quic.Connection.initClient(allocator, std.testing.io, .{
            .remote_addr = server.getAddress(),
            .server_name = "localhost",
            .insecure_skip_verify = true,
            .mode = .native,
        });
        client_initialized += 1;
    }

    var client_states = [2]ClientState{ .{}, .{} };
    var client_peers: [2]Peer = undefined;
    var client_peer_count: usize = 0;
    defer for (client_peers[0..client_peer_count]) |*peer| peer.deinit();
    for (&client_peers, 0..) |*peer, index| {
        peer.* = Peer.init(allocator, &clients[index]);
        client_peer_count += 1;
        peer.start(&client_states[index], ClientState.peerError, ClientState.peerClose);
        _ = try peer.sendBootstrap(&client_states[index], ClientState.onBootstrap);
    }

    var client_close_finalized = [2]bool{ false, false };

    var server_states = [2]ServerState{ .{}, .{} };
    var server_peers: [2]Peer = undefined;
    // Server peers are attached by the accept hook, inside the step that
    // adopts each session, so this loop never scans `sessionAt()`.
    var acceptor = FanoutAcceptor{
        .allocator = allocator,
        .states = &server_states,
        .peers = &server_peers,
    };
    defer for (server_peers[0..acceptor.count]) |*peer| peer.deinit();
    server.setOnSessionAccepted(&acceptor, FanoutAcceptor.onAccepted);
    var first_address_stable = false;

    // Establish both sessions and complete each initial Bootstrap -> Call ->
    // Return flow before selecting a victim. All endpoints are stepped on this
    // owner thread, so the later fresh sibling call cannot race Peer state.
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += 1) {
        try driveFanoutStep(&server, &clients, &client_close_finalized);

        // The first session must keep the address its Peer borrowed after a
        // second session is adopted and the list grows.
        if (acceptor.count == 2 and !first_address_stable) {
            const first = acceptor.sessions[0];
            first_address_stable = server.sessionById(first.id) == first;
        }

        if (acceptor.count == 2 and
            client_states[0].call_returns.load(.acquire) >= 1 and
            client_states[1].call_returns.load(.acquire) >= 1)
        {
            break;
        }
    }
    const server_peer_count = acceptor.count;
    if (server_peer_count != 2 or
        client_states[0].call_returns.load(.acquire) < 1 or
        client_states[1].call_returns.load(.acquire) < 1)
    {
        return error.QuicFanoutInitialCallsTimedOut;
    }

    var victim_client: ?usize = null;
    var sibling_fresh_call_returned = false;
    var victim_server_peer_detached = false;
    var post_reap_send_rejected = false;

    if (close_first) {
        const victim_session = acceptor.sessions[0];
        victim_session.requestClose();

        // Wait for the victim's remote Peer close callback and for the server
        // to reap the heap-stable transport. Exactly one client must close.
        waited_ms = 0;
        while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += 1) {
            try driveFanoutStep(&server, &clients, &client_close_finalized);
            const closed_0 = client_states[0].closes.load(.acquire) > 0;
            const closed_1 = client_states[1].closes.load(.acquire) > 0;
            if (closed_0 != closed_1 and
                server.sessionCount() == 1 and
                !server_peers[0].hasAttachedTransport())
            {
                victim_client = if (closed_0) 0 else 1;
                break;
            }
        }
        const victim_index = victim_client orelse return error.QuicFanoutVictimDidNotClose;
        const sibling_index = 1 - victim_index;
        try std.testing.expectEqual(@as(usize, 1), client_states[victim_index].closes.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), client_states[sibling_index].closes.load(.acquire));
        try std.testing.expect(!client_close_finalized[sibling_index]);

        victim_server_peer_detached = !server_peers[0].hasAttachedTransport();
        const PostReap = struct {
            fn onReturn(
                _: *anyopaque,
                _: *Peer,
                _: protocol.Return,
                _: *const cap_table.InboundCapTable,
            ) anyerror!void {}
        };
        var post_reap_ctx: u8 = 0;
        if (server_peers[0].sendBootstrap(&post_reap_ctx, PostReap.onReturn)) |_| {
            return error.PostReapPeerUnexpectedlySent;
        } else |err| {
            if (err != error.TransportNotAttached) return err;
            post_reap_send_rejected = true;
        }

        // A pre-close success on either side is not isolation evidence. Start
        // a brand-new call only after the victim is closed and reaped, then
        // require the sibling to receive its Return over the surviving session.
        const sibling_target = client_states[sibling_index].bootstrap_target orelse
            return error.MissingSiblingBootstrapCapability;
        _ = try client_peers[sibling_index].sendCallResolved(
            sibling_target,
            0x5155_4943,
            8,
            &client_states[sibling_index],
            ClientState.buildCall,
            ClientState.onCallReturn,
        );
        waited_ms = 0;
        while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += 1) {
            try driveFanoutStep(&server, &clients, &client_close_finalized);
            if (client_states[sibling_index].call_returns.load(.acquire) >= 2) {
                sibling_fresh_call_returned = true;
                break;
            }
            if (client_states[sibling_index].closes.load(.acquire) != 0) break;
        }
        if (!sibling_fresh_call_returned) return error.QuicFanoutSiblingFreshCallTimedOut;
    }

    const result = FanoutResult{
        .completed = .{
            client_states[0].call_returned.load(.acquire),
            client_states[1].call_returned.load(.acquire),
        },
        .closed = .{
            client_states[0].closes.load(.acquire),
            client_states[1].closes.load(.acquire),
        },
        .first_address_stable = first_address_stable,
        .victim_client = victim_client,
        .sibling_fresh_call_returned = sibling_fresh_call_returned,
        .victim_server_peer_detached = victim_server_peer_detached,
        .post_reap_send_rejected = post_reap_send_rejected,
    };

    // Complete transport callbacks before Peer defers run. This guarantees all
    // bindings are detached while their heap sessions are still valid.
    for (&clients, 0..) |*conn, index| {
        if (client_close_finalized[index]) continue;
        conn.requestClose();
        conn.run();
        client_close_finalized[index] = true;
    }
    server.requestClose();
    server.run();
    return result;
}

test "Peer over QUIC fanout serves two sessions from stable addresses" {
    const result = try runFanout(false);
    try std.testing.expect(result.first_address_stable);
    try std.testing.expect(result.completed[0]);
    try std.testing.expect(result.completed[1]);
}

test "Peer over QUIC fanout close isolation preserves the sibling session" {
    const result = try runFanout(true);
    try std.testing.expect(result.first_address_stable);
    const victim = result.victim_client orelse return error.MissingFanoutVictim;
    const sibling = 1 - victim;
    try std.testing.expectEqual(@as(usize, 1), result.closed[victim]);
    try std.testing.expectEqual(@as(usize, 0), result.closed[sibling]);
    try std.testing.expect(result.sibling_fresh_call_returned);
    try std.testing.expect(result.victim_server_peer_detached);
    try std.testing.expect(result.post_reap_send_rejected);
}

// ---------------------------------------------------------------------------
// One-call sessions: `quic.serve` (PeerServer) and `quic.connect`
// (ClientSession), the QUIC twins of the TCP ServerSession/ClientSession.
// No test code here touches a raw Connection, ServerSession, or sessionAt().
// ---------------------------------------------------------------------------

const ServedState = struct {
    // `Thread.Id`, not u64: it is u32 on Linux and Windows, and 32-bit
    // targets have no 64-bit atomics.
    loop_thread: std.atomic.Value(std.Thread.Id) = std.atomic.Value(std.Thread.Id).init(0),
    // Written on the server's run() thread; read after it is joined.
    accepts: usize = 0,
    calls: usize = 0,
    closes: usize = 0,
    errors: usize = 0,
    off_loop_thread: usize = 0,
    wrong_session: usize = 0,
    /// `reapPassCount()` when the first session closed. No session had
    /// closed before then, so no session could be gone, and any pass before
    /// that point was a wasted O(sessions) sweep on every step.
    reap_passes_at_first_close: ?u64 = null,

    fn onAccept(ctx: ?*anyopaque, session: *quic.PeerServer.Session) anyerror!void {
        const self: *ServedState = @ptrCast(@alignCast(ctx.?));
        self.accepts += 1;
        if (std.Thread.getCurrentId() != self.loop_thread.load(.acquire)) self.off_loop_thread += 1;
        session.user_data = self;
        _ = try session.peer.setBootstrap(.{ .ctx = self, .on_call = onCall });
    }

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ServedState = @ptrCast(@alignCast(ctx_ptr));
        // Generated handlers only get a `*Peer`; `fromPeer` recovers the
        // owning session.
        const session = quic.PeerServer.Session.fromPeer(peer);
        if (session.user_data != @as(?*anyopaque, self) or session.isClosed()) self.wrong_session += 1;
        self.calls += 1;
        try peer.sendReturnEmptyStruct(call.question_id);
    }

    fn onError(ctx: ?*anyopaque, _: *quic.PeerServer.Session, _: anyerror) void {
        const self: *ServedState = @ptrCast(@alignCast(ctx.?));
        self.errors += 1;
    }

    fn onClose(ctx: ?*anyopaque, session: *quic.PeerServer.Session) void {
        const self: *ServedState = @ptrCast(@alignCast(ctx.?));
        if (!session.isClosed()) self.wrong_session += 1;
        if (self.reap_passes_at_first_close == null) {
            self.reap_passes_at_first_close = session.owner.reapPassCount();
        }
        self.closes += 1;
    }
};

fn runServed(server: *quic.PeerServer, state: *ServedState) void {
    state.loop_thread.store(std.Thread.getCurrentId(), .release);
    server.run();
}

const SessionClientState = struct {
    returned: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    closes: usize = 0,
    failure: ?anyerror = null,
    cause: capnpc.rpc.events.DisconnectCause = .unknown,

    fn onBootstrap(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        if (ret.tag != .results) return error.ExpectedBootstrapResults;
        const results = ret.results orelse return error.MissingBootstrapResults;
        const target = try caps.resolveCapability(try results.content.getCapability());
        _ = try peer.sendCallResolved(target, 0x5155_4943, 7, ctx_ptr, buildEmptyCall, onReturn);
    }

    fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        _ = try call.initCapTableTyped(0);
    }

    fn onReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *SessionClientState = @ptrCast(@alignCast(ctx_ptr));
        // Done either way: a graceful close makes `run()` return.
        defer quic.ClientSession.fromPeer(peer).close();
        if (ret.tag != .results) return error.ExpectedCallResults;
        self.returned.store(true, .release);
    }

    fn onClose(ctx: ?*anyopaque, _: *quic.ClientSession) void {
        const self: *SessionClientState = @ptrCast(@alignCast(ctx.?));
        self.closes += 1;
    }
};

fn runSessionClient(state: *SessionClientState, server_addr: std.Io.net.IpAddress) void {
    const session = quic.connect(std.testing.allocator, std.testing.io, .{
        .conn = .{
            .remote_addr = server_addr,
            .server_name = "localhost",
            // Verified, not skipped: the one-call path keeps TLS honest.
            .ca_pem = loopback.loopback_cert_pem,
            .receive_timeout = std.Io.Duration.fromMilliseconds(1),
            // Bound a broken run instead of hanging on the 30 s defaults.
            .handshake_timeout_ms = 10_000,
        },
        .default_call_timeout_ms = 10_000,
        .ctx = state,
        .on_close = SessionClientState.onClose,
    }) catch |err| {
        state.failure = err;
        return;
    };
    defer session.deinit();
    _ = session.peer.sendBootstrap(state, SessionClientState.onBootstrap) catch |err| {
        state.failure = err;
        return;
    };
    session.run();
    state.cause = session.closeCause();
}

fn runServedRoundTrips(comptime client_count: usize) !void {
    const allocator = std.testing.allocator;

    var served = ServedState{};
    const server = try quic.serve(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .max_concurrent_connections = client_count,
    }, .{
        .ctx = &served,
        .on_accept = ServedState.onAccept,
        .on_error = ServedState.onError,
        .on_close = ServedState.onClose,
    });
    // `deinit` runs on this thread after the run() thread is joined.
    defer server.deinit();

    var server_thread = try std.Thread.spawn(.{}, runServed, .{ server, &served });
    var server_joined = false;
    defer if (!server_joined) {
        server.requestStop();
        server_thread.join();
    };

    var client_states: [client_count]SessionClientState = @splat(.{});
    var client_threads: [client_count]std.Thread = undefined;
    var spawned: usize = 0;
    defer for (client_threads[0..spawned]) |thread| thread.join();
    for (&client_threads, &client_states) |*thread, *state| {
        thread.* = try std.Thread.spawn(.{}, runSessionClient, .{ state, server.getAddress() });
        spawned += 1;
    }
    // Each client closes itself after its Return, so joining is the wait.
    for (client_threads[0..spawned]) |thread| thread.join();
    spawned = 0;

    server.requestStop();
    server_thread.join();
    server_joined = true;

    for (&client_states) |*state| {
        if (state.failure) |err| return err;
        try std.testing.expect(state.returned.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), state.closes);
        try std.testing.expectEqual(capnpc.rpc.events.DisconnectCause.local_close, state.cause);
    }
    try std.testing.expectEqual(@as(usize, client_count), served.accepts);
    try std.testing.expectEqual(@as(usize, client_count), served.calls);
    try std.testing.expectEqual(@as(usize, client_count), served.closes);
    try std.testing.expectEqual(@as(usize, 0), served.errors);
    try std.testing.expectEqual(@as(usize, 0), served.wrong_session);
    try std.testing.expectEqual(@as(usize, 0), served.off_loop_thread);
    // run() returned only after every session drained, and its last
    // after-step pass freed every peer.
    try std.testing.expectEqual(@as(usize, 0), server.sessionCount());
    // The reap looks for freed transports only once a session has closed:
    // while every session was live it never ran, though the loop stepped
    // through every handshake and call by then.
    try std.testing.expectEqual(@as(?u64, 0), served.reap_passes_at_first_close);
    try std.testing.expect(server.reapPassCount() > 0);
    // Every close was counted down as it was reaped, so the server is back
    // on the no-scan path rather than sweeping on every later step.
    try std.testing.expectEqual(@as(usize, 0), server.closed_pending);
}

test "QUIC serve and connect run one round trip per client with no hand-rolled session loop" {
    try runServedRoundTrips(3);
}

test "QUIC serve reaps many closing sessions and skips the reap while all are live" {
    // More sessions, closing close together, so a reap pass can find
    // several closed sessions still draining while others are live.
    try runServedRoundTrips(16);
}

// ---------------------------------------------------------------------------
// Session lifecycle edges, ported from the TCP ClientSession/ServerSession
// suites: close or requestStop before run, deinit without run, connect under
// allocation failure, and a rejected accept. Then PeerServer close isolation
// and its rule that on_error never follows on_close.
// ---------------------------------------------------------------------------

/// `on_error`/`on_close` counts for a ClientSession. Atomic because some
/// tests run the session on its own thread and read these from the test
/// thread after joining it.
const ClientCounters = struct {
    errors: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn onError(ctx: ?*anyopaque, _: *quic.ClientSession, _: anyerror) void {
        const self: *ClientCounters = @ptrCast(@alignCast(ctx.?));
        _ = self.errors.fetchAdd(1, .acq_rel);
    }

    fn onClose(ctx: ?*anyopaque, _: *quic.ClientSession) void {
        const self: *ClientCounters = @ptrCast(@alignCast(ctx.?));
        _ = self.closes.fetchAdd(1, .acq_rel);
    }
};

fn lifecycleConnectOptions(server_addr: std.Io.net.IpAddress, counters: *ClientCounters) quic.ConnectOptions {
    return .{
        .conn = .{
            .remote_addr = server_addr,
            .server_name = "localhost",
            .ca_pem = loopback.loopback_cert_pem,
            .receive_timeout = std.Io.Duration.fromMilliseconds(1),
            // Bound a broken run instead of hanging on the 30 s defaults.
            .handshake_timeout_ms = 10_000,
        },
        .default_call_timeout_ms = 10_000,
        .ctx = counters,
        .on_error = ClientCounters.onError,
        .on_close = ClientCounters.onClose,
    };
}

fn lifecycleServerOptions(max_sessions: u32) quic.ServerOptions {
    return .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .max_concurrent_connections = max_sessions,
    };
}

fn refuseEverySession(_: ?*anyopaque, _: *quic.PeerServer.Session) anyerror!void {
    return error.UnexpectedSession;
}

/// A bound PeerServer that is never run. Its UDP socket absorbs a client's
/// datagrams the way a TCP listen backlog absorbs a dial, so a client test
/// needs no live server; it is also the "deinit without run" server case.
fn idlePeerServer(allocator: std.mem.Allocator) !*quic.PeerServer {
    return quic.serve(allocator, std.testing.io, lifecycleServerOptions(1), .{
        .on_accept = refuseEverySession,
    });
}

/// Records how a question's callback ran (exactly once, and with what).
const ReturnWaiter = struct {
    fired: usize = 0,
    disconnected: usize = 0,

    fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ReturnWaiter = @ptrCast(@alignCast(ctx));
        self.fired += 1;
        if (ret.tag != .exception) return;
        const ex = ret.exception orelse return;
        if (std.mem.eql(u8, ex.reason, capnpc.rpc.peer.disconnected_reason)) self.disconnected += 1;
    }
};

test "QUIC ClientSession close before run: run returns, on_close fires exactly once, fromPeer recovers" {
    const allocator = std.testing.allocator;
    const server = try idlePeerServer(allocator);
    defer server.deinit();

    var counters = ClientCounters{};
    const session = try quic.connect(allocator, std.testing.io, lifecycleConnectOptions(server.getAddress(), &counters));
    defer session.deinit();
    try std.testing.expectEqual(session, quic.ClientSession.fromPeer(&session.peer));

    // Calls before run() only queue; the close must still settle them.
    var waiter = ReturnWaiter{};
    _ = try session.peer.sendBootstrap(&waiter, ReturnWaiter.onReturn);

    session.close();
    session.close(); // idempotent
    session.run();

    try std.testing.expectEqual(@as(usize, 1), counters.closes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), counters.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), waiter.fired);
    try std.testing.expectEqual(@as(usize, 1), waiter.disconnected);
}

test "QUIC ClientSession requestStop before run: run returns, on_close fires once, deinit leak-free" {
    const allocator = std.testing.allocator;
    const server = try idlePeerServer(allocator);
    defer server.deinit();

    var counters = ClientCounters{};
    const session = try quic.connect(allocator, std.testing.io, lifecycleConnectOptions(server.getAddress(), &counters));
    defer session.deinit();
    var waiter = ReturnWaiter{};
    _ = try session.peer.sendBootstrap(&waiter, ReturnWaiter.onReturn);

    session.requestStop();
    session.run();

    try std.testing.expectEqual(@as(usize, 1), counters.closes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), counters.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), waiter.fired);
    try std.testing.expectEqual(@as(usize, 1), waiter.disconnected);
}

test "QUIC ClientSession deinit without run fires no session callback and leaks nothing" {
    const allocator = std.testing.allocator;
    const server = try idlePeerServer(allocator);
    defer server.deinit();

    var counters = ClientCounters{};
    const session = try quic.connect(allocator, std.testing.io, lifecycleConnectOptions(server.getAddress(), &counters));
    // A queued frame and an outstanding question are the state a deinit
    // without run must free (std.testing.allocator fails on a leak).
    var waiter = ReturnWaiter{};
    _ = try session.peer.sendBootstrap(&waiter, ReturnWaiter.onReturn);
    session.deinit();

    // No session callback: they belong to run(). The question is still
    // settled, once, rather than stranded.
    try std.testing.expectEqual(@as(usize, 0), counters.closes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), counters.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), waiter.fired);
    try std.testing.expectEqual(@as(usize, 1), waiter.disconnected);
}

fn connectThenDeinit(allocator: std.mem.Allocator, server_addr: std.Io.net.IpAddress) !void {
    var counters = ClientCounters{};
    const session = try quic.connect(allocator, std.testing.io, lifecycleConnectOptions(server_addr, &counters));
    session.deinit();
}

test "QUIC connect never leaks under allocation failure" {
    const server = try idlePeerServer(std.testing.allocator);
    defer server.deinit();
    // Fails every allocation connect makes, one at a time: each must
    // surface as error.OutOfMemory with everything allocated so far freed.
    try std.testing.checkAllAllocationFailures(std.testing.allocator, connectThenDeinit, .{server.getAddress()});
}

test "QUIC PeerServer deinit without run, and requestStop before run, leak nothing" {
    const allocator = std.testing.allocator;

    const never_run = try idlePeerServer(allocator);
    never_run.deinit();

    var served = ServedState{};
    const server = try quic.serve(allocator, std.testing.io, lifecycleServerOptions(1), .{
        .ctx = &served,
        .on_accept = ServedState.onAccept,
        .on_error = ServedState.onError,
        .on_close = ServedState.onClose,
    });
    defer server.deinit();
    served.loop_thread.store(std.Thread.getCurrentId(), .release);
    server.requestStop();
    server.run(); // returns at once: nothing to drain

    try std.testing.expectEqual(@as(usize, 0), server.sessionCount());
    try std.testing.expectEqual(@as(usize, 0), served.accepts);
    try std.testing.expectEqual(@as(usize, 0), served.closes);
    try std.testing.expectEqual(@as(usize, 0), served.errors);
}

/// Refuses every session while `refuse` is set and serves the rest. A
/// refused session gets a bootstrap first, so its discarded peer has
/// capability state to free.
const GatedAcceptor = struct {
    refuse: std.atomic.Value(bool) = std.atomic.Value(bool).init(true),
    // Written on the run() thread; read after it is joined.
    refused: usize = 0,
    accepted: usize = 0,
    calls: usize = 0,
    errors: usize = 0,
    closes: usize = 0,

    fn onAccept(ctx: ?*anyopaque, session: *quic.PeerServer.Session) anyerror!void {
        const self: *GatedAcceptor = @ptrCast(@alignCast(ctx.?));
        _ = try session.peer.setBootstrap(.{ .ctx = self, .on_call = onCall });
        if (self.refuse.load(.acquire)) {
            self.refused += 1;
            return error.TestSessionRefused;
        }
        self.accepted += 1;
    }

    fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *GatedAcceptor = @ptrCast(@alignCast(ctx_ptr));
        self.calls += 1;
        try peer.sendReturnEmptyStruct(call.question_id);
    }

    fn onError(ctx: ?*anyopaque, _: *quic.PeerServer.Session, _: anyerror) void {
        const self: *GatedAcceptor = @ptrCast(@alignCast(ctx.?));
        self.errors += 1;
    }

    fn onClose(ctx: ?*anyopaque, _: *quic.PeerServer.Session) void {
        const self: *GatedAcceptor = @ptrCast(@alignCast(ctx.?));
        self.closes += 1;
    }
};

test "QUIC PeerServer on_accept error discards the session's peer, leaks nothing, and the next dial is served" {
    const allocator = std.testing.allocator;

    var gate = GatedAcceptor{};
    const server = try quic.serve(allocator, std.testing.io, lifecycleServerOptions(4), .{
        .ctx = &gate,
        .on_accept = GatedAcceptor.onAccept,
        .on_error = GatedAcceptor.onError,
        .on_close = GatedAcceptor.onClose,
    });
    defer server.deinit();
    const server_thread = try std.Thread.spawn(.{}, quic.PeerServer.run, .{server});
    var server_joined = false;
    defer if (!server_joined) {
        server.requestStop();
        server_thread.join();
    };

    // First dial: refused inside on_accept. The server's close reaches the
    // client during the handshake (see `Server.setOnSessionAccepted`), and
    // the client ends on it although its Bootstrap still waits for 1-RTT
    // keys. The handshake timeout and the call deadline (10 s, from
    // `lifecycleConnectOptions`) are far away, so the bootstrap settles as
    // "disconnected" only if the client ends on the server's close.
    var refused_counters = ClientCounters{};
    var refused_options = lifecycleConnectOptions(server.getAddress(), &refused_counters);
    refused_options.conn.handshake_timeout_ms = 20_000;
    refused_options.default_call_timeout_ms = 20_000;
    var refused_elapsed_ms: i64 = 0;
    var refused_waiter = ReturnWaiter{};
    var refused_cause: capnpc.rpc.events.DisconnectCause = .unknown;
    {
        const refused = try quic.connect(allocator, std.testing.io, refused_options);
        defer refused.deinit();
        _ = try refused.peer.sendBootstrap(&refused_waiter, ReturnWaiter.onReturn);
        const started_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        refused.run();
        refused_elapsed_ms = @intCast(@divFloor(std.Io.Clock.awake.now(std.testing.io).nanoseconds - started_ns, std.time.ns_per_ms));
        refused_cause = refused.closeCause();
    }

    // Second dial, after the gate opens: served end to end.
    gate.refuse.store(false, .release);
    var served_client = SessionClientState{};
    runSessionClient(&served_client, server.getAddress());

    server.requestStop();
    server_thread.join();
    server_joined = true;

    if (served_client.failure) |err| return err;
    try std.testing.expect(served_client.returned.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), served_client.closes);

    try std.testing.expectEqual(@as(usize, 1), refused_counters.closes.load(.acquire));
    // Through quic-zig v0.25.0 this was the client's own `.handshake_timeout`.
    try std.testing.expectEqual(capnpc.rpc.events.DisconnectCause.peer_close, refused_cause);
    try std.testing.expectEqual(@as(usize, 1), refused_waiter.fired);
    try std.testing.expectEqual(@as(usize, 1), refused_waiter.disconnected);
    // The client ended on the close, not on a timer: half the handshake
    // timeout leaves room for the close's draining period (three probe
    // timeouts) on a slow runner.
    errdefer std.debug.print("refused dial ran {d} ms\n", .{refused_elapsed_ms});
    try std.testing.expect(refused_elapsed_ms < 10_000);

    try std.testing.expect(gate.refused >= 1);
    try std.testing.expectEqual(@as(usize, 1), gate.accepted);
    try std.testing.expectEqual(@as(usize, 1), gate.calls);
    // on_close fires only for a session whose peer started; a refused
    // session's peer never did, and it never reached on_error either.
    try std.testing.expectEqual(@as(usize, 1), gate.closes);
    try std.testing.expectEqual(@as(usize, 0), gate.errors);
    // Nothing of a refused session stays listed (std.testing.allocator
    // also fails the test if its peer or Session was not freed).
    try std.testing.expectEqual(@as(usize, 0), server.sessionCount());
}

const isolation_close_me_method: u16 = 9;
const isolation_probe_method: u16 = 8;
const isolation_call_method: u16 = 7;

/// Server side of the close-isolation test. A `close_me` call closes the
/// caller's session from inside its handler (`Session.close()` on the run
/// thread); a probe records what the server sees once that session closed.
const IsolationServer = struct {
    // Written on the run() thread; read after it is joined.
    accepts: usize = 0,
    calls: usize = 0,
    closes: usize = 0,
    errors: usize = 0,
    victim_id: ?u64 = null,
    victim_closed: bool = false,
    probes_after_victim_close: usize = 0,
    count_at_reap: ?usize = null,
    /// Set by the first probe served after the victim's on_close that finds
    /// the reap done (`sessionCount()` back to 1). The probing client reads
    /// it on its own thread, after that probe's Return arrives.
    reaped: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn onAccept(ctx: ?*anyopaque, session: *quic.PeerServer.Session) anyerror!void {
        const self: *IsolationServer = @ptrCast(@alignCast(ctx.?));
        self.accepts += 1;
        _ = try session.peer.setBootstrap(.{ .ctx = self, .on_call = onCall });
    }

    fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *IsolationServer = @ptrCast(@alignCast(ctx_ptr));
        const session = quic.PeerServer.Session.fromPeer(peer);
        self.calls += 1;
        try peer.sendReturnEmptyStruct(call.question_id);
        switch (call.method_id) {
            isolation_close_me_method => {
                self.victim_id = session.id;
                session.close();
            },
            isolation_probe_method => {
                if (!self.victim_closed or self.reaped.load(.acquire)) return;
                self.probes_after_victim_close += 1;
                const count = session.owner.sessionCount();
                if (count == 1) {
                    self.count_at_reap = count;
                    self.reaped.store(true, .release);
                }
            },
            else => {},
        }
    }

    fn onError(ctx: ?*anyopaque, _: *quic.PeerServer.Session, _: anyerror) void {
        const self: *IsolationServer = @ptrCast(@alignCast(ctx.?));
        self.errors += 1;
    }

    fn onClose(ctx: ?*anyopaque, session: *quic.PeerServer.Session) void {
        const self: *IsolationServer = @ptrCast(@alignCast(ctx.?));
        self.closes += 1;
        if (self.victim_id == session.id) self.victim_closed = true;
    }
};

fn buildIsolationCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
    _ = try call.initCapTableTyped(0);
}

/// The sibling: one round trip, then probes until the server reports the
/// victim closed and reaped, then one brand-new call, then it closes itself.
///
/// `errors_at_fresh` snapshots the session's `on_error` count when that call
/// returns, before the close. Closing (from inside a callback, or by a
/// cross-thread stop) makes the writes the peer attempts after the callback
/// returns, such as the call's Finish, fail into `on_error`: close-path
/// noise, not the isolation under test (the TCP session suites snapshot the
/// same way).
const ProbingClient = struct {
    server: *const IsolationServer,
    counters: *const ClientCounters,
    target: ?cap_table.ResolvedCap = null,
    /// The first round trip completed (read by the test thread).
    ready: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    // Client-thread only; read after its thread is joined.
    probes: usize = 0,
    fresh_returned: bool = false,
    errors_at_fresh: ?usize = null,
    failure: ?anyerror = null,

    const probe_interval_ms: u64 = 5;
    const max_probes: usize = 2_000;

    fn onBootstrap(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ProbingClient = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) return self.fail(peer, error.ExpectedBootstrapResults);
        const results = ret.results orelse return self.fail(peer, error.MissingBootstrapResults);
        self.target = try caps.resolveCapability(try results.content.getCapability());
        _ = try peer.sendCallResolved(self.target.?, 0x5155_4943, isolation_call_method, self, buildIsolationCall, onFirstReturn);
    }

    fn onFirstReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ProbingClient = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) return self.fail(peer, error.ExpectedCallResults);
        self.ready.store(true, .release);
        _ = try peer.sendCallResolved(self.target.?, 0x5155_4943, isolation_probe_method, self, buildIsolationCall, onProbeReturn);
    }

    fn onProbeReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ProbingClient = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) return self.fail(peer, error.ProbeFailed);
        self.probes += 1;
        if (self.server.reaped.load(.acquire)) {
            // Sent only after the victim closed and was reaped.
            _ = try peer.sendCallResolved(self.target.?, 0x5155_4943, isolation_call_method, self, buildIsolationCall, onFreshReturn);
            return;
        }
        if (self.probes >= max_probes) return self.fail(peer, error.VictimReapNotObserved);
        loopback.sleepMs(probe_interval_ms);
        _ = try peer.sendCallResolved(self.target.?, 0x5155_4943, isolation_probe_method, self, buildIsolationCall, onProbeReturn);
    }

    fn onFreshReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ProbingClient = @ptrCast(@alignCast(ctx));
        self.errors_at_fresh = self.counters.errors.load(.acquire);
        if (ret.tag != .results) return self.fail(peer, error.FreshCallFailed);
        self.fresh_returned = true;
        quic.ClientSession.fromPeer(peer).close();
    }

    fn fail(self: *ProbingClient, peer: *Peer, err: anyerror) void {
        if (self.failure == null) self.failure = err;
        quic.ClientSession.fromPeer(peer).close();
    }
};

/// The victim: asks the server to close its session, then waits for the
/// close (it never closes itself).
const VictimClient = struct {
    fn onBootstrap(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        if (ret.tag != .results) return error.ExpectedBootstrapResults;
        const results = ret.results orelse return error.MissingBootstrapResults;
        const target = try caps.resolveCapability(try results.content.getCapability());
        _ = try peer.sendCallResolved(target, 0x5155_4943, isolation_close_me_method, ctx, buildIsolationCall, onCloseMeReturn);
    }

    // The Return races the server's close; either outcome is fine.
    fn onCloseMeReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
};

fn runClientSession(session: *quic.ClientSession) void {
    session.run();
}

/// Free a session whose `run()` ran on a thread the caller has joined. A
/// ClientSession is thread-affine and `run()` adopted that thread, so move
/// affinity back first: the same quiescent handoff `run()` performs on entry.
fn deinitJoinedClient(session: *quic.ClientSession) void {
    session.peer.adoptOwnerThread();
    session.conn.adoptOwnerThread();
    session.deinit();
}

/// Stops every listed endpoint after `budget_ms` unless `done` is set
/// first, so a regression fails instead of hanging the suite.
const LifecycleWatchdog = struct {
    clients: [2]?*quic.ClientSession = .{ null, null },
    server: ?*quic.PeerServer = null,
    budget_ms: u64 = 20_000,
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    fired: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn run(self: *LifecycleWatchdog) void {
        var waited_ms: u64 = 0;
        while (waited_ms < self.budget_ms and !self.done.load(.acquire)) : (waited_ms += 10) {
            loopback.sleepMs(10);
        }
        if (self.done.load(.acquire)) return;
        self.fired.store(true, .release);
        for (self.clients) |client| if (client) |session| session.requestStop();
        if (self.server) |server| server.requestStop();
    }
};

test "QUIC PeerServer close isolation: closing one Session keeps the sibling serving, and the reap drops sessionCount to 1" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var isolation = IsolationServer{};
    const server = try quic.serve(allocator, io, lifecycleServerOptions(2), .{
        .ctx = &isolation,
        .on_accept = IsolationServer.onAccept,
        .on_error = IsolationServer.onError,
        .on_close = IsolationServer.onClose,
    });
    defer server.deinit();
    var server_thread: ?std.Thread = try std.Thread.spawn(.{}, quic.PeerServer.run, .{server});
    defer if (server_thread) |thread| {
        server.requestStop();
        thread.join();
    };

    var sibling_counters = ClientCounters{};
    const sibling = try quic.connect(allocator, io, lifecycleConnectOptions(server.getAddress(), &sibling_counters));
    defer deinitJoinedClient(sibling);
    var victim_counters = ClientCounters{};
    const victim = try quic.connect(allocator, io, lifecycleConnectOptions(server.getAddress(), &victim_counters));
    defer deinitJoinedClient(victim);

    // Joined before either session is freed: it may still requestStop them.
    var watchdog = LifecycleWatchdog{ .clients = .{ sibling, victim }, .server = server };
    var watchdog_thread: ?std.Thread = try std.Thread.spawn(.{}, LifecycleWatchdog.run, .{&watchdog});
    defer if (watchdog_thread) |thread| {
        watchdog.done.store(true, .release);
        thread.join();
    };

    var probing = ProbingClient{ .server = &isolation, .counters = &sibling_counters };
    _ = try sibling.peer.sendBootstrap(&probing, ProbingClient.onBootstrap);
    var sibling_thread: ?std.Thread = try std.Thread.spawn(.{}, runClientSession, .{sibling});
    defer if (sibling_thread) |thread| {
        sibling.requestStop();
        thread.join();
    };

    // The victim dials only once the sibling has a live, proven session, so
    // the server holds two sessions when it closes the victim's.
    var waited_ms: u64 = 0;
    while (!probing.ready.load(.acquire) and waited_ms < 10_000) : (waited_ms += loopback.loopback_poll_ms) {
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    if (!probing.ready.load(.acquire)) return error.SiblingFirstRoundTripTimedOut;

    var victim_state: u8 = 0;
    _ = try victim.peer.sendBootstrap(&victim_state, VictimClient.onBootstrap);
    var victim_thread: ?std.Thread = try std.Thread.spawn(.{}, runClientSession, .{victim});
    defer if (victim_thread) |thread| {
        victim.requestStop();
        thread.join();
    };

    // The victim's run() returns once the server's close reaches it; the
    // sibling's once its fresh post-reap call returned and it closed itself.
    victim_thread.?.join();
    victim_thread = null;
    sibling_thread.?.join();
    sibling_thread = null;
    server.requestStop();
    server_thread.?.join();
    server_thread = null;
    watchdog.done.store(true, .release);
    watchdog_thread.?.join();
    watchdog_thread = null;

    try std.testing.expect(!watchdog.fired.load(.acquire));
    if (probing.failure) |err| return err;
    // The sibling survived the victim's close and reap: a brand-new call it
    // sent only after both returned over its own session, and nothing
    // reached its on_error before its own close.
    try std.testing.expect(probing.fresh_returned);
    try std.testing.expectEqual(@as(?usize, 0), probing.errors_at_fresh);
    try std.testing.expectEqual(@as(usize, 1), sibling_counters.closes.load(.acquire));
    try std.testing.expectEqual(capnpc.rpc.events.DisconnectCause.local_close, sibling.closeCause());
    try std.testing.expectEqual(@as(usize, 1), victim_counters.closes.load(.acquire));

    try std.testing.expectEqual(@as(usize, 2), isolation.accepts);
    try std.testing.expect(isolation.victim_id != null);
    try std.testing.expect(isolation.victim_closed);
    // A probe served after the victim's on_close found the reap done.
    try std.testing.expect(isolation.reaped.load(.acquire));
    try std.testing.expect(isolation.probes_after_victim_close >= 1);
    try std.testing.expectEqual(@as(?usize, 1), isolation.count_at_reap);
    try std.testing.expectEqual(@as(usize, 2), isolation.closes);
    try std.testing.expectEqual(@as(usize, 0), isolation.errors);
    try std.testing.expectEqual(@as(usize, 0), server.sessionCount());
}

/// Holds a ClientSession open after one round trip; only the server closes
/// it.
const HoldingClient = struct {
    returned: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn onBootstrap(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        if (ret.tag != .results) return error.ExpectedBootstrapResults;
        const results = ret.results orelse return error.MissingBootstrapResults;
        const target = try caps.resolveCapability(try results.content.getCapability());
        _ = try peer.sendCallResolved(target, 0x5155_4943, isolation_call_method, ctx, buildIsolationCall, onReturn);
    }

    fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *HoldingClient = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) return error.ExpectedCallResults;
        self.returned.store(true, .release);
    }
};

test "QUIC PeerServer never calls on_error after on_close: a closed session's drain-phase error is dropped" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var served = ServedState{};
    served.loop_thread.store(std.Thread.getCurrentId(), .release);
    const server = try quic.serve(allocator, io, lifecycleServerOptions(1), .{
        .ctx = &served,
        .on_accept = ServedState.onAccept,
        .on_error = ServedState.onError,
        .on_close = ServedState.onClose,
    });
    defer server.deinit();

    var client_counters = ClientCounters{};
    const client = try quic.connect(allocator, io, lifecycleConnectOptions(server.getAddress(), &client_counters));
    defer deinitJoinedClient(client);
    var holding = HoldingClient{};
    _ = try client.peer.sendBootstrap(&holding, HoldingClient.onBootstrap);
    var client_thread: ?std.Thread = try std.Thread.spawn(.{}, runClientSession, .{client});
    defer if (client_thread) |thread| {
        client.requestStop();
        thread.join();
    };

    // White-box: this thread steps the PeerServer's own fanout server,
    // which makes it the run() thread, so it can stop between steps where
    // `run()` would not. Its accept hook still builds and starts each
    // session's peer; only the after-step reap is skipped until the final
    // `run()` below, so the closed session stays allocated meanwhile.
    var waited_ms: u64 = 0;
    while (!holding.returned.load(.acquire) and waited_ms < 10_000) : (waited_ms += 1) {
        _ = try server.server.stepOnce(.poll);
        loopback.sleepMs(1);
    }
    if (!holding.returned.load(.acquire)) return error.QuicRoundTripTimedOut;
    try std.testing.expectEqual(@as(usize, 1), server.sessionCount());
    const session = server.sessions.items[0];
    const session_id = session.id;

    session.close();
    waited_ms = 0;
    while (served.closes == 0 and waited_ms < 10_000) : (waited_ms += 1) {
        _ = try server.server.stepOnce(.poll);
        loopback.sleepMs(1);
    }
    try std.testing.expectEqual(@as(usize, 1), served.closes);
    try std.testing.expect(session.isClosed());

    // The closed session's QUIC connection is still draining, so the server
    // still steps it, and a step error there (`terminateInternalError`)
    // reaches the transport's error callback, which is still installed.
    // Raise exactly that callback now, after on_close.
    const transport = server.server.sessionById(session_id) orelse return error.ClosedTransportAlreadyReaped;
    const on_transport_error = transport.callback_lifecycle.errorCallback() orelse return error.TransportErrorCallbackCleared;
    transport.callback_lifecycle.invokeError(transport, on_transport_error, error.TestDrainPhaseFailure);
    try std.testing.expectEqual(@as(usize, 0), served.errors);

    // Hand the rest to run() on this same thread: it drains, reaps, and
    // returns. The client's run() returns once the close reaches it.
    server.requestStop();
    server.run();
    client_thread.?.join();
    client_thread = null;

    try std.testing.expectEqual(@as(usize, 1), served.accepts);
    try std.testing.expectEqual(@as(usize, 1), served.closes);
    try std.testing.expectEqual(@as(usize, 0), served.errors);
    try std.testing.expectEqual(@as(usize, 0), served.wrong_session);
    try std.testing.expectEqual(@as(usize, 0), server.sessionCount());
    try std.testing.expectEqual(@as(usize, 1), client_counters.closes.load(.acquire));
}

// ---------------------------------------------------------------------------
// Deadline cancellation over QUIC: the transport's on_tick plumbing drives
// Peer.checkDeadlines from the run loop. Without it (the gap the QUIC soak
// found: cancelled=0), this test hangs until its wait budget and fails —
// no other mechanism expires a call over QUIC. The fanout ServerSession
// shares the same floored invokeTick mechanism via Server.stepSessionAt.
// ---------------------------------------------------------------------------

/// Bootstrap target that swallows every call: counts it, never Returns.
/// The client's 1ms deadline is then the only way its question resolves.
const NeverAnswerServer = struct {
    calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn onCall(
        ctx_ptr: *anyopaque,
        _: *Peer,
        _: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *NeverAnswerServer = @ptrCast(@alignCast(ctx_ptr));
        _ = self.calls.fetchAdd(1, .acq_rel);
    }

    fn peerError(ctx: ?*anyopaque, _: *Peer, _: anyerror) void {
        const self: *NeverAnswerServer = @ptrCast(@alignCast(ctx.?));
        self.failed.store(true, .release);
    }

    fn peerClose(_: ?*anyopaque, _: *Peer) void {}
};

const DeadlineClient = struct {
    cancelled: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    wrong_outcome: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn onBootstrap(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *DeadlineClient = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) {
            self.failed.store(true, .release);
            return;
        }
        const results = ret.results orelse return error.MissingBootstrapResults;
        const descriptor = try results.content.getCapability();
        const resolved = try caps.resolveCapability(descriptor);
        // Arm the 1ms default HERE, not before sendBootstrap: the QUIC
        // handshake takes tens of milliseconds, so a deadline armed at
        // bootstrap-send time expires the bootstrap question itself
        // before the transport even connects.
        peer.setTimeouts(.{ .default_call_timeout_ms = 1 });
        _ = try peer.sendCallResolved(
            resolved,
            0x5155_4943,
            7,
            self,
            buildCall,
            onCallReturn,
        );
    }

    fn buildCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        _ = try call.initCapTableTyped(0);
    }

    fn onCallReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *DeadlineClient = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag == .exception) {
            const reason = if (ret.exception) |ex| ex.reason else "";
            if (std.mem.eql(u8, reason, "deadline exceeded")) {
                self.cancelled.store(true, .release);
                return;
            }
        }
        self.wrong_outcome.store(true, .release);
    }

    fn peerError(ctx: ?*anyopaque, _: *Peer, _: anyerror) void {
        const self: *DeadlineClient = @ptrCast(@alignCast(ctx.?));
        self.failed.store(true, .release);
    }

    fn peerClose(_: ?*anyopaque, _: *Peer) void {}
};

test "Peer over QUIC cancels a call when its deadline expires" {
    const allocator = std.testing.allocator;

    var server_conn = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    });
    defer server_conn.deinit();

    var client_conn = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_conn.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    });
    defer client_conn.deinit();

    var server_state = NeverAnswerServer{};
    var server_peer = Peer.init(allocator, &server_conn);
    defer server_peer.deinit();
    server_peer.disableThreadAffinity();
    _ = try server_peer.setBootstrap(.{ .ctx = &server_state, .on_call = NeverAnswerServer.onCall });
    server_peer.start(&server_state, NeverAnswerServer.peerError, NeverAnswerServer.peerClose);

    var client_state = DeadlineClient{};
    var client_peer = Peer.init(allocator, &client_conn);
    defer client_peer.deinit();
    client_peer.disableThreadAffinity();
    // Clock before start; the 1ms default deadline is armed inside
    // onBootstrap (see there). The server never answers the real call, so
    // only the transport tick driving Peer.checkDeadlines can resolve it.
    client_peer.setClockIo(std.testing.io);
    client_peer.start(&client_state, DeadlineClient.peerError, DeadlineClient.peerClose);

    _ = try client_peer.sendBootstrap(&client_state, DeadlineClient.onBootstrap);

    var server_thread = try std.Thread.spawn(.{}, runConnection, .{&server_conn});
    var client_thread = try std.Thread.spawn(.{}, runConnection, .{&client_conn});
    var joined = false;
    defer if (!joined) {
        client_conn.requestClose();
        server_conn.requestClose();
        client_thread.join();
        server_thread.join();
    };

    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
        if (client_state.cancelled.load(.acquire)) break;
        if (client_state.wrong_outcome.load(.acquire)) break;
        if (client_state.failed.load(.acquire) or server_state.failed.load(.acquire)) break;
        loopback.sleepMs(loopback.loopback_poll_ms);
    }

    client_conn.requestClose();
    server_conn.requestClose();
    client_thread.join();
    server_thread.join();
    joined = true;

    try std.testing.expect(!client_state.wrong_outcome.load(.acquire));
    try std.testing.expect(!client_state.failed.load(.acquire));
    try std.testing.expect(!server_state.failed.load(.acquire));
    // Deliberately NOT asserting the server observed the call: with a 1ms
    // deadline the sweep can legitimately cancel before the Call datagram
    // is processed server-side. The property under test is only that the
    // deadline resolves the question at all.
    try std.testing.expect(client_state.cancelled.load(.acquire));
}

// ============================================================================
// Durable-caps rendezvous surfaces (QuicVatNetwork, docs/quic-durable-caps-plan.md)
// ============================================================================

test "QUIC dictated initial DCID is validated and observable at the accepted session" {
    const allocator = std.testing.allocator;

    // Length outside 8..20 is refused before any socket work.
    const short_dcid = [_]u8{0xab};
    try std.testing.expectError(error.InvalidConfig, quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = loopback.testListenAddr(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .initial_dcid = &short_dcid,
    }));

    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 1,
    });
    defer server.deinit();

    // A ticket-style dictated DCID: 8 bytes (in production, CSPRNG-minted).
    const dictated = [_]u8{ 0x7a, 0x3c, 0x91, 0x04, 0xee, 0x5f, 0xd2, 0x18 };
    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .initial_dcid = &dictated,
    });
    defer client.deinit();

    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms and server.sessionCount() == 0) : (waited_ms += 1) {
        _ = try server.stepOnce(.poll);
        if (!client.isClosing()) _ = try client.stepOnce(.poll);
        loopback.sleepMs(1);
    }
    const session = server.sessionAt(0) orelse return error.DictatedDcidDialTimedOut;

    // The accepted session surfaces the exact bytes the dial dictated — the
    // hook a rendezvous embedder matches provision tickets against.
    try std.testing.expectEqualSlices(u8, &dictated, session.initialDcid());

    client.requestClose();
    client.run();
    server.requestClose();
    server.run();
}

// -- L3 three-party handoff over three real QUIC connections -----------------
//
// Same scenario as tests/rpc/peer/rpc_three_party_handoff_pickup_test.zig
// (B resolves a promise export to Carol on C; A auto-picks-up via its
// VatNetwork), but every connection is a real QUIC transport and the
// VatNetwork is the QuicVatNetwork: B mints a provision ticket naming
// "vat-c", A redeems it against its pre-established pool. C is a fanout
// server with TWO sessions (from B and from A) sharing one ProvisionIndex,
// so the Accept lands on a different peer than the Provide.

const NUMBER_INTERFACE_ID: u64 = 0xC0C0_C0C0_C0C0_C001;
const GET_NUMBER_METHOD_ID: u16 = 0;

const L3VatState = struct {
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn peerError(ctx: ?*anyopaque, _: *Peer, _: anyerror) void {
        const self: *L3VatState = @ptrCast(@alignCast(ctx.?));
        self.failed.store(true, .release);
    }

    fn peerClose(ctx: ?*anyopaque, _: *Peer) void {
        const self: *L3VatState = @ptrCast(@alignCast(ctx.?));
        _ = self.closes.fetchAdd(1, .acq_rel);
    }
};

const L3Carol = struct {
    get_number_calls: u32 = 0,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *L3Carol = @ptrCast(@alignCast(ctx_ptr));
        if (call.interface_id != NUMBER_INTERFACE_ID or call.method_id != GET_NUMBER_METHOD_ID) {
            return error.UnexpectedMethod;
        }
        self.get_number_calls += 1;
        const ReturnCtx = struct {
            fn build(_: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                const results = try any.initStruct(1, 0);
                results.writeU32(0, 42);
            }
        };
        var ret_ctx: u8 = 0;
        try peer.sendReturnResults(call.question_id, &ret_ctx, ReturnCtx.build);
    }
};

const L3Introducer = struct {
    promise_export_id: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *L3Introducer = @ptrCast(@alignCast(ctx_ptr));
        const promise_id = try peer.addPromiseExport();
        self.promise_export_id = promise_id;

        const PromiseReturnCtx = struct {
            promise_id: u32,
            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = @ptrCast(@alignCast(bctx));
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                try any.setCapability(.{ .id = bself.promise_id });
            }
        };
        var ret_ctx = PromiseReturnCtx{ .promise_id = promise_id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, PromiseReturnCtx.build);
    }
};

/// Bootstrap/call Return probe that retains the returned capability and
/// records its import id.
const L3ImportProbe = struct {
    import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *L3ImportProbe = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.UnexpectedReturn;
        const payload = ret.results orelse return error.MissingPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.CapNotImported,
        };
    }
};

const L3GetNumberCall = struct {
    result: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *L3GetNumberCall = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.UnexpectedGetNumberReturn;
        const payload = ret.results orelse return error.MissingGetNumberPayload;
        const content = try payload.content.getStruct();
        self.result = content.readU32(0);
    }
};

const L3PickupHandler = struct {
    expected_promise_id: u32,
    carol_import_id: ?u32 = null,
    fired: bool = false,

    fn onPickup(
        ctx_ptr: *anyopaque,
        _: *Peer,
        promise_id: u32,
        accept_peer: *Peer,
        ret: protocol.Return,
        accept_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *L3PickupHandler = @ptrCast(@alignCast(ctx_ptr));
        self.fired = true;
        if (promise_id != self.expected_promise_id) return error.UnexpectedPromiseId;
        if (ret.tag != .results) return error.UnexpectedAcceptReturn;
        const payload = ret.results orelse return error.MissingAcceptPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(accept_caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.carol_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.AcceptedCarolNotImported,
        };
        std.debug.assert(accept_peer.caps.hasImport(self.carol_import_id.?));
    }
};

fn anyL3Failure(a: *const L3VatState, b: *const L3VatState, c: *const L3VatState) bool {
    return a.failed.load(.acquire) or b.failed.load(.acquire) or c.failed.load(.acquire);
}

/// Owns every transport and peer of the three-vat topology so ONE deinit can
/// enforce the teardown order on every exit path, including error returns:
/// close all transports FIRST (close callbacks must fire into live Peers —
/// `Server.deinit` invokes each live session's close callback, so a Peer
/// deinit'd earlier would be a use-after-free), then deinit peers, then
/// transports. Individual defers cannot express this: the error arms return
/// mid-setup, where LIFO order would deinit session-attached peers before
/// the servers that still hold callbacks into them.
const L3Harness = struct {
    b_server: ?quic.Server = null,
    c_server: ?quic.Server = null,
    conns: [3]?quic.Connection = .{ null, null, null },
    // Creation order; deinit runs the reverse.
    c_from_b: ?Peer = null,
    b_to_c: ?Peer = null,
    c_from_a: ?Peer = null,
    a_to_c: ?Peer = null,
    b_from_a: ?Peer = null,
    a_to_b: ?Peer = null,
    torn_down: bool = false,

    fn step(self: *L3Harness) !void {
        if (self.b_server) |*srv| _ = try srv.stepOnce(.poll);
        if (self.c_server) |*srv| _ = try srv.stepOnce(.poll);
        for (&self.conns) |*maybe_conn| {
            if (maybe_conn.*) |*conn| {
                if (!conn.isClosing()) _ = try conn.stepOnce(.poll);
            }
        }
        loopback.sleepMs(1);
    }

    fn closeTransports(self: *L3Harness) void {
        if (self.torn_down) return;
        self.torn_down = true;
        for (&self.conns) |*maybe_conn| {
            if (maybe_conn.*) |*conn| {
                conn.requestClose();
                conn.run();
            }
        }
        if (self.b_server) |*srv| {
            srv.requestClose();
            srv.run();
        }
        if (self.c_server) |*srv| {
            srv.requestClose();
            srv.run();
        }
    }

    fn deinit(self: *L3Harness) void {
        self.closeTransports();
        if (self.a_to_b) |*p| p.deinit();
        if (self.b_from_a) |*p| p.deinit();
        if (self.a_to_c) |*p| p.deinit();
        if (self.c_from_a) |*p| p.deinit();
        if (self.b_to_c) |*p| p.deinit();
        if (self.c_from_b) |*p| p.deinit();
        for (&self.conns) |*maybe_conn| {
            if (maybe_conn.*) |*conn| conn.deinit();
        }
        if (self.b_server) |*srv| srv.deinit();
        if (self.c_server) |*srv| srv.deinit();
    }
};

test "L3 three-party handoff over QUIC: ticketed auto-pickup hands C's cap to A directly" {
    const allocator = std.testing.allocator;

    // Handler state must outlive the harness: transport close callbacks fire
    // into these during teardown.
    var a_state = L3VatState{};
    var b_state = L3VatState{};
    var c_state = L3VatState{};
    var carol = L3Carol{};
    var introducer = L3Introducer{};

    // Vat-wide provision index on C: the Provide arrives on c_from_b, the
    // Accept on c_from_a — cross-peer serving requires the shared index.
    // Declared before the harness so it outlives the peers attached to it.
    var index = ProvisionIndex.init(allocator, .{});
    defer index.deinit();

    // VatNetworks outlive the peers that borrow them.
    var net_b = try quic_vat_network.QuicVatNetwork(Peer).init(allocator, .{ .seed = @splat(0xb0) });
    defer net_b.deinit();
    var net_a = try quic_vat_network.QuicVatNetwork(Peer).init(allocator, .{ .seed = @splat(0xa0) });
    defer net_a.deinit();

    var h = L3Harness{};
    defer h.deinit();

    h.b_server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 1,
    });
    h.c_server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 2,
    });
    const b_server = &h.b_server.?;
    const c_server = &h.c_server.?;

    // -- Establish B<->C first so C's session order is deterministic. ---------
    h.conns[0] = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = c_server.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
    });

    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms and c_server.sessionCount() == 0) : (waited_ms += 1) {
        try h.step();
    }
    const c_from_b_session = c_server.sessionAt(0) orelse return error.BToCDialTimedOut;

    h.c_from_b = Peer.init(allocator, c_from_b_session);
    const c_from_b = &h.c_from_b.?;
    _ = try c_from_b.setBootstrap(.{ .ctx = &carol, .on_call = L3Carol.onCall });
    c_from_b.start(&c_state, L3VatState.peerError, L3VatState.peerClose);
    try c_from_b.attachProvisionIndex(&index);

    h.b_to_c = Peer.init(allocator, &h.conns[0].?);
    const b_to_c = &h.b_to_c.?;
    b_to_c.start(&b_state, L3VatState.peerError, L3VatState.peerClose);

    // -- Then A<->C (pre-established pool entry for the ticket redemption). ---
    h.conns[1] = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = c_server.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
    });

    waited_ms = 0;
    while (waited_ms < loopback.loopback_timeout_ms and c_server.sessionCount() < 2) : (waited_ms += 1) {
        try h.step();
        if (anyL3Failure(&a_state, &b_state, &c_state)) return error.L3PeerFailed;
    }
    const c_from_a_session = c_server.sessionAt(1) orelse return error.AToCDialTimedOut;

    h.c_from_a = Peer.init(allocator, c_from_a_session);
    const c_from_a = &h.c_from_a.?;
    c_from_a.start(&c_state, L3VatState.peerError, L3VatState.peerClose);
    try c_from_a.attachProvisionIndex(&index);

    h.a_to_c = Peer.init(allocator, &h.conns[1].?);
    const a_to_c = &h.a_to_c.?;
    a_to_c.start(&a_state, L3VatState.peerError, L3VatState.peerClose);

    // -- Then A<->B. ----------------------------------------------------------
    h.conns[2] = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = b_server.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
    });

    waited_ms = 0;
    while (waited_ms < loopback.loopback_timeout_ms and b_server.sessionCount() == 0) : (waited_ms += 1) {
        try h.step();
        if (anyL3Failure(&a_state, &b_state, &c_state)) return error.L3PeerFailed;
    }
    const b_from_a_session = b_server.sessionAt(0) orelse return error.AToBDialTimedOut;

    h.b_from_a = Peer.init(allocator, b_from_a_session);
    const b_from_a = &h.b_from_a.?;
    _ = try b_from_a.setBootstrap(.{ .ctx = &introducer, .on_call = L3Introducer.onCall });
    b_from_a.start(&b_state, L3VatState.peerError, L3VatState.peerClose);

    h.a_to_b = Peer.init(allocator, &h.conns[2].?);
    const a_to_b = &h.a_to_b.?;
    a_to_b.start(&a_state, L3VatState.peerError, L3VatState.peerClose);

    // -- Wire the vat networks. -----------------------------------------------
    const c_hint = quic_vat_network.AddrHint.ip4(.{ 127, 0, 0, 1 }, c_server.getAddress().getPort());
    try net_b.addVat("vat-c", &.{c_hint});
    b_from_a.attachVatNetwork(net_b.network());

    try net_a.registerPeer("vat-c", a_to_c);
    a_to_b.attachVatNetwork(net_a.network());

    // -- (1) B imports Carol from C. ------------------------------------------
    var carol_probe = L3ImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, L3ImportProbe.onReturn);
    waited_ms = 0;
    while (waited_ms < loopback.loopback_timeout_ms and carol_probe.import_id == null) : (waited_ms += 1) {
        try h.step();
        if (anyL3Failure(&a_state, &b_state, &c_state)) return error.L3PeerFailed;
    }
    const carol_import_id = carol_probe.import_id orelse return error.CarolBootstrapTimedOut;

    // -- (2) A bootstraps B's Introducer and calls getPromise(). --------------
    var introducer_probe = L3ImportProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, L3ImportProbe.onReturn);
    waited_ms = 0;
    while (waited_ms < loopback.loopback_timeout_ms and introducer_probe.import_id == null) : (waited_ms += 1) {
        try h.step();
        if (anyL3Failure(&a_state, &b_state, &c_state)) return error.L3PeerFailed;
    }
    const introducer_import_id = introducer_probe.import_id orelse return error.IntroducerBootstrapTimedOut;

    var promise_probe = L3ImportProbe{};
    _ = try a_to_b.sendCall(
        introducer_import_id,
        0x1234_5678_9abc_def0,
        0,
        &promise_probe,
        null,
        L3ImportProbe.onReturn,
    );
    waited_ms = 0;
    while (waited_ms < loopback.loopback_timeout_ms and promise_probe.import_id == null) : (waited_ms += 1) {
        try h.step();
        if (anyL3Failure(&a_state, &b_state, &c_state)) return error.L3PeerFailed;
    }
    const promise_import_id = promise_probe.import_id orelse return error.GetPromiseTimedOut;
    const promise_export_id = introducer.promise_export_id orelse return error.PromiseNotMinted;
    try std.testing.expectEqual(promise_export_id, promise_import_id);

    // -- (3) A arms auto-pickup; B originates the ticketed handoff. -----------
    var pickup = L3PickupHandler{ .expected_promise_id = promise_import_id };
    a_to_b.setHandoffPickupHandler(&pickup, L3PickupHandler.onPickup);

    const b_network = b_from_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(b_from_a, "vat-c");
    defer introduction.deinit(allocator);

    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = carol_import_id,
        .promised_answer = null,
    };
    const handle = try b_from_a.resolvePromiseExportToThirdParty(
        promise_import_id,
        b_to_c,
        provided_target,
        recipient,
        introduction.to_contact,
    );

    // -- (4) The pickup fires over real transport, no manual Accept. ----------
    waited_ms = 0;
    while (waited_ms < loopback.loopback_timeout_ms and pickup.carol_import_id == null) : (waited_ms += 1) {
        try h.step();
        if (anyL3Failure(&a_state, &b_state, &c_state)) return error.PeerFailedDuringHandoff;
    }
    try std.testing.expect(pickup.fired);
    const accepted_carol_id = pickup.carol_import_id orelse return error.AutoPickupTimedOut;
    // Fulfilled off-peer via direct pickup, not the vine-proxy fallback.
    try std.testing.expect(!a_to_b.resolved_imports.contains(promise_import_id));

    // -- (5) A calls getNumber() on its DIRECT import of Carol -> 42. ---------
    var get_number = L3GetNumberCall{};
    _ = try a_to_c.sendCall(
        accepted_carol_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &get_number,
        null,
        L3GetNumberCall.onReturn,
    );
    waited_ms = 0;
    while (waited_ms < loopback.loopback_timeout_ms and get_number.result == null) : (waited_ms += 1) {
        try h.step();
        if (anyL3Failure(&a_state, &b_state, &c_state)) return error.L3PeerFailed;
    }
    try std.testing.expectEqual(@as(u32, 42), get_number.result orelse return error.GetNumberTimedOut);
    try std.testing.expectEqual(@as(u32, 1), carol.get_number_calls);

    // -- (6) The vine drains: A's runtime released it, Finishing B's Provide. -
    waited_ms = 0;
    while (waited_ms < loopback.loopback_timeout_ms and b_from_a.exports.contains(handle.vine_id)) : (waited_ms += 1) {
        try h.step();
        if (anyL3Failure(&a_state, &b_state, &c_state)) return error.L3PeerFailed;
    }
    try std.testing.expect(!b_from_a.exports.contains(handle.vine_id));

    // Teardown order is owned by the harness: transports close first (their
    // close callbacks land in live peers), then peers, then transports free.
}

// ============================================================================
// Death certificate: typed close causes (docs/quic-durable-caps-plan.md)
// ============================================================================

const rpc_events = capnpc.rpc.events;

/// Client probe that records the typed disconnect cause at BOTH places the
/// contract promises it: inside the question callback cancelled by the
/// disconnect, and inside on_close.
const CauseClient = struct {
    bootstrap_target: ?cap_table.ResolvedCap = null,
    disconnected_reason_seen: bool = false,
    cause_at_cancel: ?rpc_events.DisconnectCause = null,
    closes: u32 = 0,
    cause_at_close: ?rpc_events.DisconnectCause = null,
    failed: bool = false,

    fn onBootstrap(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *CauseClient = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.ExpectedBootstrapResults;
        const results = ret.results orelse return error.MissingBootstrapResults;
        const descriptor = try results.content.getCapability();
        self.bootstrap_target = try caps.resolveCapability(descriptor);
    }

    fn onCallReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *CauseClient = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .exception) return;
        const reason = if (ret.exception) |ex| ex.reason else "";
        // The synthetic reason text stays exactly "disconnected" for every
        // cause; the typed cause rides the peer, readable right here.
        self.disconnected_reason_seen = std.mem.eql(u8, reason, capnpc.rpc.peer.disconnected_reason);
        self.cause_at_cancel = peer.lastDisconnectCause();
    }

    /// Context-free params builder. CauseClient must never be handed to
    /// `ClientState.buildCall`: that casts the ctx to a *ClientState, whose
    /// `payload_size` lands on CauseClient's import id and whose `payload`
    /// array runs past the end of the object.
    fn buildCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        _ = try call.initCapTableTyped(0);
    }

    fn peerError(ctx: ?*anyopaque, _: *Peer, _: anyerror) void {
        const self: *CauseClient = @ptrCast(@alignCast(ctx.?));
        self.failed = true;
    }

    fn peerClose(ctx: ?*anyopaque, peer: *Peer) void {
        const self: *CauseClient = @ptrCast(@alignCast(ctx.?));
        self.closes += 1;
        self.cause_at_close = peer.lastDisconnectCause();
    }
};

/// Step a compat server/client pair once on this thread, finalizing each
/// side's terminal callbacks via run() once it starts closing (the
/// driveFanoutStep pattern for a two-Connection topology).
fn stepCompatPairOnce(
    server_conn: *quic.Connection,
    client_conn: *quic.Connection,
    finalized: *[2]bool,
) !void {
    const conns = [2]*quic.Connection{ server_conn, client_conn };
    for (conns, 0..) |conn, index| {
        if (finalized[index]) continue;
        if (!conn.isClosing()) _ = try conn.stepOnce(.poll);
        if (conn.isClosing()) {
            conn.run();
            finalized[index] = true;
        }
    }
    loopback.sleepMs(1);
}

test "Peer over QUIC carries a typed peer-close cause to cancelled questions and on_close" {
    const allocator = std.testing.allocator;

    var server_conn = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    });
    defer server_conn.deinit();
    var client_conn = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_conn.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    });
    defer client_conn.deinit();

    var server_state = NeverAnswerServer{};
    var server_peer = Peer.init(allocator, &server_conn);
    defer server_peer.deinit();
    _ = try server_peer.setBootstrap(.{ .ctx = &server_state, .on_call = NeverAnswerServer.onCall });
    server_peer.start(&server_state, NeverAnswerServer.peerError, NeverAnswerServer.peerClose);

    var client_state = CauseClient{};
    var client_peer = Peer.init(allocator, &client_conn);
    defer client_peer.deinit();
    client_peer.start(&client_state, CauseClient.peerError, CauseClient.peerClose);
    _ = try client_peer.sendBootstrap(&client_state, CauseClient.onBootstrap);

    var finalized = [2]bool{ false, false };
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms and client_state.bootstrap_target == null) : (waited_ms += 1) {
        try stepCompatPairOnce(&server_conn, &client_conn, &finalized);
    }
    const target = client_state.bootstrap_target orelse return error.BootstrapTimedOut;

    // A call the server will never answer: in flight when the close lands.
    _ = try client_peer.sendCallResolved(target, 0x5155_4943, 7, &client_state, CauseClient.buildCall, CauseClient.onCallReturn);
    waited_ms = 0;
    while (waited_ms < 50) : (waited_ms += 1) {
        try stepCompatPairOnce(&server_conn, &client_conn, &finalized);
    }

    // Server closes cleanly; the client experiences a REMOTE close.
    server_conn.requestClose();
    waited_ms = 0;
    while (waited_ms < loopback.loopback_timeout_ms and client_state.closes == 0) : (waited_ms += 1) {
        try stepCompatPairOnce(&server_conn, &client_conn, &finalized);
    }

    try std.testing.expect(!client_state.failed);
    try std.testing.expectEqual(@as(u32, 1), client_state.closes);
    // The in-flight question was cancelled with the unchanged reason text,
    // and the typed cause was already readable inside its callback.
    try std.testing.expect(client_state.disconnected_reason_seen);
    try std.testing.expectEqual(rpc_events.DisconnectCause.peer_close, client_state.cause_at_cancel orelse return error.NoCancelCause);
    try std.testing.expectEqual(rpc_events.DisconnectCause.peer_close, client_state.cause_at_close orelse return error.NoCloseCause);
    // Both transports agree on who closed: the client saw the peer close,
    // the server saw its own clean local close.
    try std.testing.expectEqual(rpc_events.DisconnectCause.peer_close, client_conn.closeCause());
    try std.testing.expectEqual(rpc_events.DisconnectCause.local_close, server_conn.closeCause());
}

/// What a client certified after its server crashed and restarted, read at
/// every point the cause is observable.
const CrashRestartCertificate = struct {
    failed: bool,
    closes: u32,
    disconnected_reason_seen: bool,
    cause_at_cancel: ?rpc_events.DisconnectCause,
    cause_at_close: ?rpc_events.DisconnectCause,
    peer_cause: rpc_events.DisconnectCause,
    conn_cause: rpc_events.DisconnectCause,
    /// `statelessResetsSent()` on the restarted server.
    resets_sent: u64,
    /// The client's `lastAuthenticatedReceiveNs()` after the crash, once it
    /// has read everything A sent, and again once the reset has closed it.
    alive_before_reset_ns: ?u64,
    alive_after_reset_ns: ?u64,
};

/// Bootstrap a client against a server built from `server_options`, crash
/// that server with no close ceremony, restart it on the same port from the
/// same options (so with the same `stateless_reset_key`, if any), call into
/// it, and wait up to `wait_ms` for the client to close. `server_options`
/// must listen on an ephemeral port.
fn crashRestartCertificate(
    allocator: std.mem.Allocator,
    server_options: quic.ServerOptions,
    client_transport_params: @TypeOf(quic.defaultTransportParams()),
    wait_ms: u64,
) !CrashRestartCertificate {
    // `Server.deinit` fires each live session's close callback, so the
    // server must be torn down while its bound peer is still alive. Plain
    // defers run LIFO and would do the opposite on every error arm (the
    // peer is created later, so its defer runs first), turning a timeout
    // into a use-after-free panic that hides the real failure. One owner,
    // one order, every exit path.
    const AVat = struct {
        server: ?quic.Server = null,
        peer: ?Peer = null,

        fn crash(self: *@This()) void {
            if (self.server) |*srv| srv.deinit();
            self.server = null;
            if (self.peer) |*p| {
                _ = p.takeAttachedConnection(*quic.ServerSession);
                p.deinit();
            }
            self.peer = null;
        }
    };
    var a = AVat{};
    defer a.crash();

    a.server = try quic.Server.init(allocator, std.testing.io, server_options);
    const a_server = &a.server.?;
    const a_port = a_server.getAddress().getPort();

    var client_conn = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = a_server.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        .transport_params = client_transport_params,
    });
    defer client_conn.deinit();

    var client_state = CauseClient{};
    var client_peer = Peer.init(allocator, &client_conn);
    defer client_peer.deinit();
    client_peer.start(&client_state, CauseClient.peerError, CauseClient.peerClose);
    _ = try client_peer.sendBootstrap(&client_state, CauseClient.onBootstrap);

    // Bind a serving peer to A's session once it exists, so the bootstrap
    // completes and the client has the peer's reset token installed (the
    // section 18.2 transport parameter rides the handshake).
    var a_state = ServerState{};

    var client_finalized = false;
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms and client_state.bootstrap_target == null) : (waited_ms += 1) {
        _ = try a_server.stepOnce(.poll);
        if (a.peer == null) {
            if (a_server.sessionAt(0)) |session| {
                a.peer = Peer.init(allocator, session);
                _ = try a.peer.?.setBootstrap(.{ .ctx = &a_state, .on_call = ServerState.onCall });
                a.peer.?.start(&a_state, ServerState.peerError, ServerState.peerClose);
            }
        }
        if (!client_conn.isClosing()) _ = try client_conn.stepOnce(.poll);
        loopback.sleepMs(1);
    }
    const target = client_state.bootstrap_target orelse return error.BootstrapTimedOut;

    // CRASH: destroy A without any close ceremony. Nothing reaches the
    // wire (the server is sans-IO at teardown), so the client still
    // believes the connection is alive.
    a.crash();

    // Packets A sent just before the crash can still be in flight (loopback
    // delivery is asynchronous on some hosts). Read them until the client
    // has heard nothing for a while, so that from here on the only datagram
    // it can receive is B's stateless reset.
    var quiet_steps: u32 = 0;
    var drain_steps: u32 = 0;
    while (quiet_steps < 10 and drain_steps < 1_000) : (drain_steps += 1) {
        const step = try client_conn.stepOnce(.poll);
        quiet_steps = if (step.received_datagram) 0 else quiet_steps + 1;
        loopback.sleepMs(1);
    }
    const alive_before_reset_ns = client_conn.lastAuthenticatedReceiveNs();

    // RESTART: same port, same options, empty connection table.
    var b_options = server_options;
    b_options.listen_addr = try std.Io.net.IpAddress.parse("127.0.0.1", a_port);
    // A's port went back to the kernel's ephemeral pool when it died, and
    // sibling test binaries bind port 0 continuously; a brief retry keeps a
    // lost race from reading as a death-certificate failure.
    var b_server = blk: {
        var attempt: u32 = 0;
        while (true) : (attempt += 1) {
            break :blk quic.Server.init(allocator, std.testing.io, b_options) catch |err| {
                if (attempt >= 20) return err;
                loopback.sleepMs(5);
                continue;
            };
        }
    };
    defer b_server.deinit();

    // The client calls into the void: B cannot route the DCID. With the
    // shared reset key it answers with a stateless reset, and the client's
    // installed token turns that into a proven crash-restart certificate.
    _ = try client_peer.sendCallResolved(target, 0x5155_4943, 7, &client_state, CauseClient.buildCall, CauseClient.onCallReturn);

    waited_ms = 0;
    while (waited_ms < wait_ms and client_state.closes == 0) : (waited_ms += 1) {
        _ = try b_server.stepOnce(.poll);
        if (!client_conn.isClosing()) {
            _ = try client_conn.stepOnce(.poll);
        } else if (!client_finalized) {
            client_conn.run();
            client_finalized = true;
        }
        loopback.sleepMs(1);
    }

    return .{
        .failed = client_state.failed,
        .closes = client_state.closes,
        .disconnected_reason_seen = client_state.disconnected_reason_seen,
        .cause_at_cancel = client_state.cause_at_cancel,
        .cause_at_close = client_state.cause_at_close,
        .peer_cause = client_peer.lastDisconnectCause(),
        .conn_cause = client_conn.closeCause(),
        .resets_sent = b_server.statelessResetsSent(),
        .alive_before_reset_ns = alive_before_reset_ns,
        .alive_after_reset_ns = client_conn.lastAuthenticatedReceiveNs(),
    };
}

/// The reset ended the connection, but it is not evidence that the server
/// was alive: it leaves the client's last authenticated receive where the
/// crash left it. `WarmRedialClient`'s health check relies on exactly this.
fn expectResetIsNotLiveness(cert: CrashRestartCertificate) !void {
    const before_ns = cert.alive_before_reset_ns orelse return error.NoAuthenticatedReceive;
    try std.testing.expectEqual(@as(?u64, before_ns), cert.alive_after_reset_ns);
}

test "Peer over QUIC proves a crash-restart via DisconnectCause.stateless_reset" {
    // One reset key shared across the "crash": server B derives the same
    // per-CID tokens server A advertised, which is what makes A's death
    // provable to a client that never saw a CONNECTION_CLOSE.
    const reset_key: [32]u8 = @splat(0x42);
    const cert = try crashRestartCertificate(std.testing.allocator, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 1,
        .stateless_reset_key = reset_key,
    }, quic.defaultTransportParams(), loopback.loopback_timeout_ms);

    try std.testing.expect(!cert.failed);
    try std.testing.expectEqual(@as(u32, 1), cert.closes);
    // The question cancelled by the reset carries the unchanged reason
    // text, with the proof readable in its callback and in on_close.
    try std.testing.expect(cert.disconnected_reason_seen);
    try std.testing.expectEqual(rpc_events.DisconnectCause.stateless_reset, cert.cause_at_cancel orelse return error.NoCancelCause);
    try std.testing.expectEqual(rpc_events.DisconnectCause.stateless_reset, cert.cause_at_close orelse return error.NoCloseCause);
    try std.testing.expectEqual(rpc_events.DisconnectCause.stateless_reset, cert.peer_cause);
    try std.testing.expectEqual(rpc_events.DisconnectCause.stateless_reset, cert.conn_cause);
    // And the restarted endpoint counted the reset it sent — the churn
    // observability signal.
    try std.testing.expect(cert.resets_sent >= 1);
    try expectResetIsNotLiveness(cert);
}

// ---------------------------------------------------------------------------
// The hardened preset (withProductionServerHardening) must carry the death
// certificate too: a server built the documented way is the one production
// runs, and without a reset key its clients only ever see idle_timeout.
// ---------------------------------------------------------------------------

/// Fixed keys for the hardened-preset tests. Production generates each once
/// and persists it (docs/quic-transport.md, "Production Defaults"): the
/// restarted server must hold the SAME bytes, which these constants model.
const hardened_retry_key: quic.ServerRetryTokenKey = @splat(0x61);
const hardened_new_token_key: quic.ServerNewTokenKey = @splat(0x62);
const hardened_reset_key: quic.StatelessResetKey = @splat(0x63);

/// Client idle timeout for the hardened crash-restart tests. Short, so a
/// server that sends no stateless reset (the ablation) ends in a certified
/// `.idle_timeout` inside the test's wait instead of after the 30 s default.
/// The reset arrives within milliseconds of the first post-crash send, so it
/// wins this race by three orders of magnitude.
const hardened_client_idle_timeout_ms: u64 = 2_000;
/// Post-crash wait for the hardened tests: covers the idle timeout above
/// (plus its 3 x PTO floor) so the ablation reports its cause, not a hang.
const hardened_wait_ms: u64 = 3 * loopback.loopback_timeout_ms;

fn hardenedClientTransportParams() @TypeOf(quic.defaultTransportParams()) {
    var params = quic.defaultTransportParams();
    params.max_idle_timeout_ms = hardened_client_idle_timeout_ms;
    return params;
}

/// A server built the documented way: base options through
/// `withProductionServerHardening`, Retry and NEW_TOKEN included.
fn hardenedServerOptions(listen_addr: std.Io.net.IpAddress) quic.ServerOptions {
    return quic.withProductionServerHardening(.{
        .listen_addr = listen_addr,
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 2,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    }, .{
        .retry_token_key = hardened_retry_key,
        .stateless_reset_key = hardened_reset_key,
        .new_token_key = hardened_new_token_key,
    });
}

test "withProductionServerHardening server proves a crash-restart via DisconnectCause.stateless_reset" {
    const cert = try crashRestartCertificate(
        std.testing.allocator,
        hardenedServerOptions(loopback.testListenAddr()),
        hardenedClientTransportParams(),
        hardened_wait_ms,
    );

    try std.testing.expect(!cert.failed);
    // The decisive assertion. A preset without a reset key certifies
    // `.idle_timeout` here: the restarted server drops the stale-CID
    // datagrams silently, and the client can prove nothing.
    try std.testing.expectEqual(rpc_events.DisconnectCause.stateless_reset, cert.peer_cause);
    try std.testing.expectEqual(@as(u32, 1), cert.closes);
    try std.testing.expect(cert.disconnected_reason_seen);
    try std.testing.expectEqual(rpc_events.DisconnectCause.stateless_reset, cert.cause_at_cancel orelse return error.NoCancelCause);
    try std.testing.expectEqual(rpc_events.DisconnectCause.stateless_reset, cert.cause_at_close orelse return error.NoCloseCause);
    try std.testing.expectEqual(rpc_events.DisconnectCause.stateless_reset, cert.conn_cause);
    try std.testing.expect(cert.resets_sent >= 1);
    try expectResetIsNotLiveness(cert);
}

test "QUIC fanout session local close certifies .local_close to its bound peer" {
    const allocator = std.testing.allocator;

    // Same ownership rule as the crash-restart test: the server outlives no
    // bound peer, so one owner tears both down in the only safe order.
    const Vat = struct {
        server: ?quic.Server = null,
        peer: ?Peer = null,

        fn teardown(self: *@This()) void {
            if (self.server) |*srv| srv.deinit();
            self.server = null;
            if (self.peer) |*p| {
                _ = p.takeAttachedConnection(*quic.ServerSession);
                p.deinit();
            }
            self.peer = null;
        }
    };
    var vat = Vat{};
    defer vat.teardown();

    vat.server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 1,
    });
    const server = &vat.server.?;

    var client_conn = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    });
    defer client_conn.deinit();

    var server_state = CauseClient{};
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms and vat.peer == null) : (waited_ms += 1) {
        _ = try server.stepOnce(.poll);
        if (server.sessionAt(0)) |session| {
            vat.peer = Peer.init(allocator, session);
            vat.peer.?.start(&server_state, CauseClient.peerError, CauseClient.peerClose);
        }
        if (!client_conn.isClosing()) _ = try client_conn.stepOnce(.poll);
        loopback.sleepMs(1);
    }
    if (vat.peer == null) return error.SessionNeverAccepted;
    const session = server.sessionAt(0) orelse return error.SessionDisappeared;

    // The public per-session close: the QUIC-level close happens inside the
    // loop's own flush, AFTER the capnp close controller has recorded its
    // status. Capturing the certificate before that flush yielded `.unknown`
    // here, then silently flipped to `.local_close` a step later — after the
    // close callback had already read it.
    session.requestClose();

    waited_ms = 0;
    while (waited_ms < loopback.loopback_timeout_ms and server_state.closes == 0) : (waited_ms += 1) {
        _ = try server.stepOnce(.poll);
        if (!client_conn.isClosing()) _ = try client_conn.stepOnce(.poll);
        loopback.sleepMs(1);
    }

    try std.testing.expectEqual(@as(u32, 1), server_state.closes);
    try std.testing.expectEqual(
        rpc_events.DisconnectCause.local_close,
        server_state.cause_at_close orelse return error.NoCloseCause,
    );

    client_conn.requestClose();
    client_conn.run();
}

// ---------------------------------------------------------------------------
// Auto warm redial (WarmRedialClient): the ladder's integration rung — a
// crash-restart proven by .stateless_reset triggers an automatic resumed
// redial plus sturdy-ref re-restore, healing the app's capability.
// ---------------------------------------------------------------------------

const RedialEcho = struct {
    const interface_id: u64 = 0xec40_5155_4943_0001;
    const sturdy_ref = "sturdy:redial-echo/1";
    var ctx_anchor: u8 = 0;

    fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        _ = ctx;
        _ = caps;
        if (call.interface_id != interface_id or call.method_id != 0) {
            return peer.sendReturnException(call.question_id, "unknown method");
        }
        try peer.sendReturnEmptyStruct(call.question_id);
    }

    fn onRestore(ctx: *anyopaque, peer: *Peer, ref: []const u8) anyerror!capnpc.rpc.peer.RestoreOutcome {
        _ = ctx;
        if (!std.mem.eql(u8, ref, sturdy_ref)) return .unknown;
        return .{ .existing = try peer.addExport(.{ .ctx = @ptrCast(&ctx_anchor), .on_call = onCall }) };
    }
};

/// One server incarnation: crash-restart-safe holder in the AVat shape (one
/// owner, one teardown order, every exit path).
const RedialVat = struct {
    server: ?quic.Server = null,
    peer: ?Peer = null,
    /// Server-side 0-RTT verdict of the session this incarnation bound,
    /// once its handshake is done (see `observeEarlyData`).
    bound_status: ?quic.EarlyDataStatus = null,
    /// Restores this incarnation ran before its session's handshake
    /// completed: restores that rode 0-RTT.
    restores_before_handshake: u32 = 0,

    fn observeEarlyData(self: *RedialVat) void {
        const server = &(self.server orelse return);
        const session = server.sessionAt(0) orelse return;
        const quic_conn = session.activeQuicConnection() orelse return;
        if (quic_conn.handshakeDone()) self.bound_status = quic_conn.earlyDataStatus();
    }

    fn bindIfNeeded(self: *RedialVat, allocator: std.mem.Allocator) !void {
        if (self.peer != null) return;
        const server = &(self.server orelse return);
        if (server.sessionAt(0)) |session| {
            self.peer = Peer.init(allocator, session);
            _ = try self.peer.?.setBootstrap(.{ .ctx = @ptrCast(&RedialEcho.ctx_anchor), .on_call = RedialEcho.onCall });
            try self.peer.?.setRestorer(@ptrCast(self), onRestore);
            self.peer.?.start(null, null, null);
            // Frames that arrived before the peer was bound wait in the
            // session; dispatch them now, before the next receive can
            // complete the handshake, so `restores_before_handshake` sees a
            // restore that rode 0-RTT.
            try server.stepSession(0);
        }
    }

    fn onRestore(ctx: *anyopaque, peer: *Peer, ref: []const u8) anyerror!capnpc.rpc.peer.RestoreOutcome {
        const self: *RedialVat = @ptrCast(@alignCast(ctx));
        if (peer.getAttachedConnection(*quic.ServerSession)) |session| {
            if (session.activeQuicConnection()) |quic_conn| {
                if (!quic_conn.handshakeDone()) self.restores_before_handshake += 1;
            }
        }
        return RedialEcho.onRestore(@ptrCast(&RedialEcho.ctx_anchor), peer, ref);
    }

    fn crash(self: *RedialVat) void {
        if (self.server) |*srv| srv.deinit();
        self.server = null;
        if (self.peer) |*p| {
            _ = p.takeAttachedConnection(*quic.ServerSession);
            p.deinit();
        }
        self.peer = null;
        self.bound_status = null;
        self.restores_before_handshake = 0;
    }
};

const RedialAppState = struct {
    /// True: every rebind starts a nonstop echo chain (an app that keeps
    /// calling, so it detects a death on its next send). False: the app
    /// sends nothing after a rebind, and the connection goes idle.
    perpetual_echo: bool = true,
    rebinds: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    echo_ok: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    gave_up: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Written on the run thread BEFORE the `gave_up` release-store; read it
    /// through `giveUpCause`, which acquires that flag first.
    give_up_cause: ?rpc_events.DisconnectCause = null,
    /// The local UDP port of the generation behind each of the first rebinds,
    /// 0 when unknown. Written on the run thread BEFORE the `rebinds`
    /// release-increment; read entry `i` once `rebinds` (acquire) is past `i`.
    local_ports: [4]u16 = @splat(0),
    // Run-thread-only within a generation; rewritten by each rebind.
    last_peer: ?*Peer = null,
    last_cap: ?cap_table.ResolvedCap = null,
    outcome: ?quic.WarmRedialClient.Outcome = null,

    /// The cause the client certified when it gave up, or null while it has
    /// not given up.
    fn giveUpCause(self: *const RedialAppState) ?rpc_events.DisconnectCause {
        if (!self.gave_up.load(.acquire)) return null;
        return self.give_up_cause;
    }

    /// The local port of the generation behind rebind `index` (0-based), or
    /// null while that rebind has not happened.
    fn rebindPort(self: *const RedialAppState, index: usize) ?u16 {
        if (self.rebinds.load(.acquire) <= index) return null;
        return self.local_ports[index];
    }

    fn onRebind(ctx: ?*anyopaque, peer: *Peer, cap: cap_table.ResolvedCap) void {
        const self: *RedialAppState = @ptrCast(@alignCast(ctx.?));
        const index = self.rebinds.load(.monotonic);
        if (index < self.local_ports.len) {
            if (peer.getAttachedConnection(*quic.Connection)) |conn| {
                self.local_ports[index] = conn.getAddress().getPort();
            }
        }
        _ = self.rebinds.fetchAdd(1, .release);
        self.last_peer = peer;
        self.last_cap = cap;
        if (self.perpetual_echo) self.sendEcho(peer, cap);
    }

    fn sendEcho(self: *RedialAppState, peer: *Peer, cap: cap_table.ResolvedCap) void {
        _ = peer.sendCallResolved(cap, RedialEcho.interface_id, 0, self, null, onEchoReturn) catch {};
    }

    fn onEchoReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        _ = caps;
        const self: *RedialAppState = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) return; // the crash's synthetic disconnected Return ends this chain
        _ = self.echo_ok.fetchAdd(1, .monotonic);
        // Perpetual traffic: real apps detect a stateless reset by SENDING;
        // an idle connection would sit until idle timeout.
        self.sendEcho(peer, self.last_cap orelse return);
    }

    fn onGiveUp(ctx: ?*anyopaque, cause: rpc_events.DisconnectCause) void {
        const self: *RedialAppState = @ptrCast(@alignCast(ctx.?));
        self.give_up_cause = cause;
        self.gave_up.store(true, .release);
    }

    fn runClient(self: *RedialAppState, client: *quic.WarmRedialClient) void {
        self.outcome = client.run() catch null;
    }
};

fn restartVatOnPort(vat: *RedialVat, allocator: std.mem.Allocator, port: u16, reset_key: [32]u8) !void {
    try restartVatWith(vat, allocator, .{
        .listen_addr = try std.Io.net.IpAddress.parse("127.0.0.1", port),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 2,
        .stateless_reset_key = reset_key,
    });
}

/// Restart `vat` from `options`, which must name the crashed server's port.
fn restartVatWith(vat: *RedialVat, allocator: std.mem.Allocator, options: quic.ServerOptions) !void {
    var attempt: u32 = 0;
    vat.server = blk: {
        while (true) : (attempt += 1) {
            break :blk quic.Server.init(allocator, std.testing.io, options) catch |err| {
                if (attempt >= 20) return err;
                loopback.sleepMs(5);
                continue;
            };
        }
    };
}

test "WarmRedialClient auto-heals a restored capability across a crash-restart" {
    const allocator = std.testing.allocator;
    const reset_key: [32]u8 = @splat(0x4e);

    var vat = RedialVat{};
    defer vat.crash();
    vat.server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 2,
        .stateless_reset_key = reset_key,
    });
    const port = vat.server.?.getAddress().getPort();

    var app = RedialAppState{};
    var client = try quic.WarmRedialClient.init(
        allocator,
        std.testing.io,
        .{
            .remote_addr = vat.server.?.getAddress(),
            .server_name = "localhost",
            .insecure_skip_verify = true,
            .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        },
        RedialEcho.sturdy_ref,
        .{ .max_redials = 3, .backoff_ms = 10 },
        &app,
        RedialAppState.onRebind,
        RedialAppState.onGiveUp,
    );
    defer client.deinit();
    var thread = try std.Thread.spawn(.{}, RedialAppState.runClient, .{ &app, &client });
    var joined = false;
    defer if (!joined) {
        client.requestStop();
        thread.join();
    };

    // Phase 1: cold dial, pipelined restore, first echo round trip.
    var waited: u64 = 0;
    while (waited < loopback.loopback_timeout_ms and app.echo_ok.load(.acquire) == 0) : (waited += 1) {
        _ = try vat.server.?.stepOnce(.poll);
        try vat.bindIfNeeded(allocator);
        loopback.sleepMs(1);
    }
    try std.testing.expect(app.echo_ok.load(.acquire) > 0);
    try std.testing.expectEqual(@as(u32, 1), app.rebinds.load(.acquire));
    const echo_before_crash = app.echo_ok.load(.acquire);

    // CRASH + RESTART: no close ceremony, same port, same reset key.
    vat.crash();
    try restartVatOnPort(&vat, allocator, port, reset_key);

    // Phase 2: the in-flight echo chain draws a stateless reset from the
    // restarted server; the client must redial resumed, re-restore, and the
    // HEALED capability must answer.
    waited = 0;
    while (waited < loopback.loopback_timeout_ms and
        (app.rebinds.load(.acquire) < 2 or app.echo_ok.load(.acquire) <= echo_before_crash)) : (waited += 1)
    {
        _ = try vat.server.?.stepOnce(.poll);
        try vat.bindIfNeeded(allocator);
        loopback.sleepMs(1);
    }
    try std.testing.expectEqual(@as(u32, 2), app.rebinds.load(.acquire));
    try std.testing.expect(app.echo_ok.load(.acquire) > echo_before_crash);
    try std.testing.expect(vat.server.?.statelessResetsSent() >= 1);
    try std.testing.expect(!app.gave_up.load(.acquire));

    client.requestStop();
    thread.join();
    joined = true;

    const outcome = app.outcome orelse return error.NoOutcome;
    try std.testing.expectEqual(@as(u32, 2), outcome.generations);
    try std.testing.expectEqual(@as(u32, 1), outcome.redials);
    try std.testing.expectEqual(@as(u32, 1), outcome.total_redials);
    try std.testing.expectEqual(@as(u32, 2), outcome.rebinds);
}

test "WarmRedialClient with a zero redial budget does NOT heal (ablation)" {
    const allocator = std.testing.allocator;
    const reset_key: [32]u8 = @splat(0x4f);

    var vat = RedialVat{};
    defer vat.crash();
    vat.server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 2,
        .stateless_reset_key = reset_key,
    });
    const port = vat.server.?.getAddress().getPort();

    var app = RedialAppState{};
    var client = try quic.WarmRedialClient.init(
        allocator,
        std.testing.io,
        .{
            .remote_addr = vat.server.?.getAddress(),
            .server_name = "localhost",
            .insecure_skip_verify = true,
            .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        },
        RedialEcho.sturdy_ref,
        .{ .max_redials = 0, .backoff_ms = 10 },
        &app,
        RedialAppState.onRebind,
        RedialAppState.onGiveUp,
    );
    defer client.deinit();
    var thread = try std.Thread.spawn(.{}, RedialAppState.runClient, .{ &app, &client });
    var joined = false;
    defer if (!joined) {
        client.requestStop();
        thread.join();
    };

    var waited: u64 = 0;
    while (waited < loopback.loopback_timeout_ms and app.echo_ok.load(.acquire) == 0) : (waited += 1) {
        _ = try vat.server.?.stepOnce(.poll);
        try vat.bindIfNeeded(allocator);
        loopback.sleepMs(1);
    }
    try std.testing.expect(app.echo_ok.load(.acquire) > 0);

    vat.crash();
    try restartVatOnPort(&vat, allocator, port, reset_key);

    // Budget zero: the reset is detected but never acted on.
    waited = 0;
    while (waited < loopback.loopback_timeout_ms and !app.gave_up.load(.acquire)) : (waited += 1) {
        _ = try vat.server.?.stepOnce(.poll);
        loopback.sleepMs(1);
    }
    try std.testing.expect(app.gave_up.load(.acquire));
    thread.join();
    joined = true;

    const outcome = app.outcome orelse return error.NoOutcome;
    try std.testing.expectEqual(@as(u32, 1), outcome.generations);
    try std.testing.expectEqual(@as(u32, 0), outcome.redials);
    try std.testing.expectEqual(@as(u32, 0), outcome.total_redials);
    try std.testing.expectEqual(@as(u32, 1), outcome.rebinds);
    try std.testing.expectEqual(rpc_events.DisconnectCause.stateless_reset, outcome.last_cause);
}

test "WarmRedialClient heals across a crash-restart of a withProductionServerHardening server" {
    const allocator = std.testing.allocator;

    var vat = RedialVat{};
    defer vat.crash();
    vat.server = try quic.Server.init(allocator, std.testing.io, hardenedServerOptions(loopback.testListenAddr()));
    const port = vat.server.?.getAddress().getPort();

    var app = RedialAppState{};
    var client = try quic.WarmRedialClient.init(
        allocator,
        std.testing.io,
        .{
            .remote_addr = vat.server.?.getAddress(),
            .server_name = "localhost",
            .insecure_skip_verify = true,
            .receive_timeout = std.Io.Duration.fromMilliseconds(5),
            .transport_params = hardenedClientTransportParams(),
        },
        RedialEcho.sturdy_ref,
        // Default cause policy: only `.stateless_reset` redials, so the
        // rebind below is itself the client's certificate.
        .{ .max_redials = 3, .backoff_ms = 10 },
        &app,
        RedialAppState.onRebind,
        RedialAppState.onGiveUp,
    );
    defer client.deinit();
    var thread = try std.Thread.spawn(.{}, RedialAppState.runClient, .{ &app, &client });
    var joined = false;
    defer if (!joined) {
        client.requestStop();
        thread.join();
    };

    // Phase 1: cold dial (through Retry), pipelined restore, first echo.
    var waited: u64 = 0;
    while (waited < loopback.loopback_timeout_ms and app.echo_ok.load(.acquire) == 0) : (waited += 1) {
        _ = try vat.server.?.stepOnce(.poll);
        try vat.bindIfNeeded(allocator);
        loopback.sleepMs(1);
    }
    try std.testing.expect(app.echo_ok.load(.acquire) > 0);
    try std.testing.expectEqual(@as(u32, 1), app.rebinds.load(.acquire));
    const echo_before_crash = app.echo_ok.load(.acquire);

    // CRASH + RESTART from the same hardened options (same persisted keys).
    vat.crash();
    try restartVatWith(&vat, allocator, hardenedServerOptions(try std.Io.net.IpAddress.parse("127.0.0.1", port)));

    waited = 0;
    while (waited < hardened_wait_ms and !app.gave_up.load(.acquire) and
        (app.rebinds.load(.acquire) < 2 or app.echo_ok.load(.acquire) <= echo_before_crash)) : (waited += 1)
    {
        _ = try vat.server.?.stepOnce(.poll);
        try vat.bindIfNeeded(allocator);
        loopback.sleepMs(1);
    }
    // A preset without a reset key fails here: the client certifies
    // `.idle_timeout`, which the default policy does not redial on.
    try std.testing.expectEqual(@as(?rpc_events.DisconnectCause, null), app.giveUpCause());
    try std.testing.expectEqual(@as(u32, 2), app.rebinds.load(.acquire));
    try std.testing.expect(app.echo_ok.load(.acquire) > echo_before_crash);
    try std.testing.expect(vat.server.?.statelessResetsSent() >= 1);

    client.requestStop();
    thread.join();
    joined = true;

    const outcome = app.outcome orelse return error.NoOutcome;
    try std.testing.expectEqual(@as(u32, 2), outcome.generations);
    try std.testing.expectEqual(@as(u32, 2), outcome.rebinds);
}

const TicketServer = enum {
    /// `withProductionServerHardening`: Retry is on, so a dial without a
    /// valid NEW_TOKEN gets a Retry.
    hardened,
    /// No `retry_token_key`: every dial's first flight reaches the server.
    no_retry,
};

/// A warm-restore (`.restore_only`) server with an optional persisted
/// session-ticket key. The hardened kind reuses the same
/// `hardened_new_token_key` on every restart, as the key rules require.
fn ticketedServerOptions(kind: TicketServer, listen_addr: std.Io.net.IpAddress, key: ?*const quic.SessionTicketKey) quic.ServerOptions {
    const base: quic.ServerOptions = .{
        .listen_addr = listen_addr,
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 2,
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    };
    return switch (kind) {
        .hardened => quic.withProductionServerHardening(base, .{
            .retry_token_key = hardened_retry_key,
            .stateless_reset_key = hardened_reset_key,
            .new_token_key = hardened_new_token_key,
            .early_data = .restore_only,
            .session_ticket_key = key,
        }),
        .no_retry => blk: {
            var options = base;
            options.stateless_reset_key = hardened_reset_key;
            options.early_data = .without_replay_protection;
            options.early_dispatch = .restore_only;
            options.session_ticket_key = key;
            break :blk options;
        },
    };
}

/// What the heal finds at the first generation's local port.
const HealPort = enum {
    /// Free: the heal can dial from the port that earned its NEW_TOKEN.
    free,
    /// Taken: the test binds the first generation's port as soon as that
    /// generation's socket closes and holds it until the end, so the heal
    /// must fall back to an ephemeral port.
    taken,
};

/// The client's pause before the heal when the test takes the first
/// generation's port. The test binds that port within a few milliseconds of
/// the first generation's close, so this leaves far more than the 300 ms
/// margin that Windows CI needs.
const taken_port_backoff_ms: u64 = 1_000;

/// Bind `port` on the unspecified IPv4 address, where a client dial without
/// `local_addr` binds. Null while another socket holds the port.
fn holdUdpPort(port: u16) !?std.Io.net.Socket {
    var addr: std.Io.net.IpAddress = .{ .ip4 = .unspecified(port) };
    return std.Io.net.IpAddress.bind(&addr, std.testing.io, .{ .mode = .dgram, .protocol = .udp }) catch |err| switch (err) {
        error.AddressInUse => null,
        else => err,
    };
}

const TicketHeal = struct {
    /// Server-side 0-RTT verdict of the healed (second) generation.
    healed_status: quic.EarlyDataStatus,
    /// Restores the restarted server ran before the heal's handshake
    /// completed: 1 when the heal's restore rode 0-RTT.
    early_restores: u32,
    /// Retries the restarted server sent (only the heal dials it).
    restarted_retries: u64,
    /// Local port of the first generation, which earned the NEW_TOKEN.
    first_port: u16,
    /// Local port of the healed (second) generation.
    healed_port: u16,
    outcome: quic.WarmRedialClient.Outcome,
};

/// Heal a restored capability across a crash-restart of a `.restore_only`
/// server of `kind` that loads `key` on both starts. The first generation
/// dials cold and captures a session ticket, and from the hardened server
/// also a NEW_TOKEN; the heal offers both. `heal_port` says whether the
/// heal can dial from the first generation's port.
fn healAcrossCrashWithTicketKey(kind: TicketServer, key: ?*const quic.SessionTicketKey, heal_port: HealPort) !TicketHeal {
    const allocator = std.testing.allocator;

    var vat = RedialVat{};
    defer vat.crash();
    vat.server = try quic.Server.init(allocator, std.testing.io, ticketedServerOptions(kind, loopback.testListenAddr(), key));
    const port = vat.server.?.getAddress().getPort();

    var app = RedialAppState{};
    var client = try quic.WarmRedialClient.init(
        allocator,
        std.testing.io,
        .{
            .remote_addr = vat.server.?.getAddress(),
            .server_name = "localhost",
            .insecure_skip_verify = true,
            .receive_timeout = std.Io.Duration.fromMilliseconds(5),
            .transport_params = hardenedClientTransportParams(),
        },
        RedialEcho.sturdy_ref,
        .{ .max_redials = 3, .backoff_ms = switch (heal_port) {
            .free => 10,
            .taken => taken_port_backoff_ms,
        } },
        &app,
        RedialAppState.onRebind,
        RedialAppState.onGiveUp,
    );
    defer client.deinit();
    var thread = try std.Thread.spawn(.{}, RedialAppState.runClient, .{ &app, &client });
    var joined = false;
    defer if (!joined) {
        client.requestStop();
        thread.join();
    };

    // Phase 1: cold dial (through a Retry on the hardened server), restore,
    // first echo, and the warm state the heal offers: a session ticket, and
    // from the hardened server the NEW_TOKEN that lets the heal skip Retry.
    var have_warm_state = false;
    var waited: u64 = 0;
    while (waited < hardened_wait_ms and (app.echo_ok.load(.acquire) == 0 or !have_warm_state)) : (waited += 1) {
        _ = try vat.server.?.stepOnce(.poll);
        try vat.bindIfNeeded(allocator);
        if (!have_warm_state) {
            if (try client.exportWarmState(allocator)) |warm| {
                defer allocator.free(warm);
                const decoded = try quic.warm_state.decode(warm);
                have_warm_state = kind == .no_retry or decoded.token.len > 0;
            }
        }
        loopback.sleepMs(1);
    }
    try std.testing.expect(have_warm_state);
    try std.testing.expect(app.echo_ok.load(.acquire) > 0);
    try std.testing.expectEqual(@as(u32, 1), app.rebinds.load(.acquire));
    const echo_before_crash = app.echo_ok.load(.acquire);
    const first_port = app.rebindPort(0) orelse return error.NoFirstRebind;
    try std.testing.expect(first_port != 0);

    // CRASH + RESTART with the same persisted keys (ticket key included).
    vat.crash();
    try restartVatWith(&vat, allocator, ticketedServerOptions(kind, try std.Io.net.IpAddress.parse("127.0.0.1", port), key));

    // With `.taken`, the bind below fails while the first generation holds
    // its port, and succeeds once that generation has closed its socket,
    // during the client's backoff before the heal.
    var held: ?std.Io.net.Socket = null;
    defer if (held) |socket| socket.close(std.testing.io);
    waited = 0;
    while (waited < hardened_wait_ms and !app.gave_up.load(.acquire) and
        (app.rebinds.load(.acquire) < 2 or app.echo_ok.load(.acquire) <= echo_before_crash or vat.bound_status == null)) : (waited += 1)
    {
        if (heal_port == .taken and held == null) held = try holdUdpPort(first_port);
        _ = try vat.server.?.stepOnce(.poll);
        try vat.bindIfNeeded(allocator);
        vat.observeEarlyData();
        loopback.sleepMs(1);
    }
    try std.testing.expectEqual(@as(?rpc_events.DisconnectCause, null), app.giveUpCause());
    try std.testing.expectEqual(@as(u32, 2), app.rebinds.load(.acquire));
    try std.testing.expect(app.echo_ok.load(.acquire) > echo_before_crash);
    try std.testing.expect(vat.server.?.statelessResetsSent() >= 1);
    // The test took the port before the heal dialed (the backoff above).
    if (heal_port == .taken) try std.testing.expect(held != null);
    const healed_status = vat.bound_status orelse return error.NoEarlyDataVerdict;
    const early_restores = vat.restores_before_handshake;
    const restarted_retries = vat.server.?.listener.server.metricsSnapshot().feeds_retry_sent;
    const healed_port = app.rebindPort(1) orelse return error.NoHealRebind;

    client.requestStop();
    thread.join();
    joined = true;

    return .{
        .healed_status = healed_status,
        .early_restores = early_restores,
        .restarted_retries = restarted_retries,
        .first_port = first_port,
        .healed_port = healed_port,
        .outcome = app.outcome orelse return error.NoOutcome,
    };
}

test "WarmRedialClient heal after a crash-restart rides 0-RTT when the server persists its session-ticket key and sends no Retry" {
    const key: quic.SessionTicketKey = @splat(0x3c);
    const heal = try healAcrossCrashWithTicketKey(.no_retry, &key, .free);

    try std.testing.expectEqual(quic.EarlyDataStatus.accepted, heal.healed_status);
    try std.testing.expectEqual(@as(u64, 0), heal.restarted_retries);
    // The restarted server ran the heal's restore before its handshake
    // completed: the restore rode 0-RTT.
    try std.testing.expectEqual(@as(u32, 1), heal.early_restores);
    try std.testing.expectEqual(@as(u32, 2), heal.outcome.generations);
    try std.testing.expectEqual(@as(u32, 2), heal.outcome.rebinds);
    // The cold first generation offered no early data; the heal's rode
    // 0-RTT. Neither dial got a Retry.
    try std.testing.expectEqual(@as(u32, 1), heal.outcome.zero_rtt_generations);
    try std.testing.expectEqual(@as(u32, 0), heal.outcome.retried_generations);
    try std.testing.expectEqual(@as(u32, 0), heal.outcome.port_fallback_generations);
}

test "WarmRedialClient heal after a crash-restart of a hardened server with a session-ticket key rides 0-RTT from the port that earned its NEW_TOKEN" {
    const key: quic.SessionTicketKey = @splat(0x3d);
    const heal = try healAcrossCrashWithTicketKey(.hardened, &key, .free);

    // The heal dials from the first generation's port. quic-zig binds a
    // NEW_TOKEN to the client's address and port, the restarted server
    // keeps the same `new_token_key`, and its token clock continues across
    // the restart, so the NEW_TOKEN is valid and the dial skips the Retry.
    try std.testing.expectEqual(heal.first_port, heal.healed_port);
    try std.testing.expectEqual(@as(u64, 0), heal.restarted_retries);
    // The same ticket key resumes the session, and the restore runs before
    // the restarted server's handshake completes: it rode 0-RTT.
    try std.testing.expectEqual(quic.EarlyDataStatus.accepted, heal.healed_status);
    try std.testing.expectEqual(@as(u32, 1), heal.early_restores);
    try std.testing.expectEqual(@as(u32, 2), heal.outcome.generations);
    try std.testing.expectEqual(@as(u32, 2), heal.outcome.rebinds);
    // Only the cold first generation got a Retry; the heal counts as 0-RTT.
    try std.testing.expectEqual(@as(u32, 1), heal.outcome.zero_rtt_generations);
    try std.testing.expectEqual(@as(u32, 1), heal.outcome.retried_generations);
    try std.testing.expectEqual(@as(u32, 0), heal.outcome.port_fallback_generations);
}

test "WarmRedialClient heal falls back to an ephemeral port when its previous port is taken, and pays a Retry" {
    const key: quic.SessionTicketKey = @splat(0x3e);
    const heal = try healAcrossCrashWithTicketKey(.hardened, &key, .taken);

    // The capability still heals, from another port, and the client counts
    // the fallback.
    try std.testing.expect(heal.healed_port != heal.first_port);
    try std.testing.expectEqual(@as(u32, 1), heal.outcome.port_fallback_generations);
    try std.testing.expectEqual(@as(u32, 2), heal.outcome.generations);
    try std.testing.expectEqual(@as(u32, 2), heal.outcome.rebinds);
    // From the new port the NEW_TOKEN is invalid, so the restarted server
    // sends a Retry. The quic-zig v0.27.0 client sends its 0-RTT data again
    // after the Retry, so the restarted server still runs the restore before
    // its handshake completes, one round trip later (through v0.25.0 it ran
    // only after the handshake: quic-zig finding F8).
    try std.testing.expectEqual(@as(u64, 1), heal.restarted_retries);
    try std.testing.expectEqual(quic.EarlyDataStatus.accepted, heal.healed_status);
    try std.testing.expectEqual(@as(u32, 1), heal.early_restores);
    try std.testing.expectEqual(@as(u32, 0), heal.outcome.zero_rtt_generations);
    try std.testing.expectEqual(@as(u32, 2), heal.outcome.retried_generations);
}

test "WarmRedialClient heal after a crash-restart without a session-ticket key is refused 0-RTT (ablation)" {
    const heal = try healAcrossCrashWithTicketKey(.hardened, null, .free);

    // The capability still heals, through a full handshake.
    try std.testing.expectEqual(quic.EarlyDataStatus.rejected, heal.healed_status);
    try std.testing.expectEqual(@as(u32, 2), heal.outcome.generations);
    try std.testing.expectEqual(@as(u32, 2), heal.outcome.rebinds);
    try std.testing.expectEqual(@as(u32, 0), heal.early_restores);
    try std.testing.expectEqual(@as(u32, 0), heal.outcome.zero_rtt_generations);
    // The NEW_TOKEN still skips the Retry: it does not depend on the ticket
    // key. Only the cold first generation got one.
    try std.testing.expectEqual(heal.first_port, heal.healed_port);
    try std.testing.expectEqual(@as(u64, 0), heal.restarted_retries);
    try std.testing.expectEqual(@as(u32, 1), heal.outcome.retried_generations);
}

test "WarmRedialClient budget counts consecutive failures: resets between healthy generations never exhaust it" {
    const allocator = std.testing.allocator;
    const reset_key: [32]u8 = @splat(0x52);
    // One crash more than the budget: a LIFETIME budget gives up on it.
    const max_redials: u32 = 3;
    const crashes: u32 = max_redials + 1;
    // Each generation answers echo traffic for `hold_ms` after its rebind
    // before the next crash. Those answers are the client's proof that the
    // server stayed alive well past the threshold that makes it healthy.
    const min_healthy_ms: u64 = 100;
    const hold_ms: u64 = 3 * min_healthy_ms;

    var vat = RedialVat{};
    defer vat.crash();
    vat.server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 2,
        .stateless_reset_key = reset_key,
    });
    const port = vat.server.?.getAddress().getPort();

    var app = RedialAppState{};
    var client = try quic.WarmRedialClient.init(
        allocator,
        std.testing.io,
        .{
            .remote_addr = vat.server.?.getAddress(),
            .server_name = "localhost",
            .insecure_skip_verify = true,
            .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        },
        RedialEcho.sturdy_ref,
        .{ .max_redials = max_redials, .backoff_ms = 10, .min_healthy_ms = min_healthy_ms },
        &app,
        RedialAppState.onRebind,
        RedialAppState.onGiveUp,
    );
    defer client.deinit();
    var thread = try std.Thread.spawn(.{}, RedialAppState.runClient, .{ &app, &client });
    var joined = false;
    defer if (!joined) {
        client.requestStop();
        thread.join();
    };

    var crash: u32 = 0;
    while (true) : (crash += 1) {
        // This generation (the initial dial, then one per crash) must
        // rebind and carry a healed echo.
        const echo_floor = app.echo_ok.load(.acquire);
        var waited: u64 = 0;
        while (waited < loopback.loopback_timeout_ms and !app.gave_up.load(.acquire) and
            (app.rebinds.load(.acquire) < crash + 1 or app.echo_ok.load(.acquire) <= echo_floor)) : (waited += 1)
        {
            _ = try vat.server.?.stepOnce(.poll);
            try vat.bindIfNeeded(allocator);
            loopback.sleepMs(1);
        }
        try std.testing.expectEqual(@as(?rpc_events.DisconnectCause, null), app.giveUpCause());
        try std.testing.expectEqual(crash + 1, app.rebinds.load(.acquire));
        try std.testing.expect(app.echo_ok.load(.acquire) > echo_floor);
        if (crash == crashes) break;

        // Stay healthy: keep answering echoes for at least `hold_ms` (each
        // pass sleeps at least 1 ms, so this is a lower bound on wall time).
        var held: u64 = 0;
        while (held < hold_ms) : (held += 1) {
            _ = try vat.server.?.stepOnce(.poll);
            loopback.sleepMs(1);
        }
        vat.crash();
        try restartVatOnPort(&vat, allocator, port, reset_key);
    }

    client.requestStop();
    thread.join();
    joined = true;

    const outcome = app.outcome orelse return error.NoOutcome;
    try std.testing.expectEqual(crashes + 1, outcome.generations);
    try std.testing.expectEqual(crashes + 1, outcome.rebinds);
    try std.testing.expectEqual(crashes, outcome.total_redials);
    // Every crash followed a healthy generation, so the streak never grew
    // past the redial that the latest crash spent (0 if the final
    // generation also outlived the threshold before the stop).
    try std.testing.expect(outcome.redials <= 1);
}

/// A server that dies in every generation: each incarnation crashes once the
/// client has rebound to it, and a fresh one restarts on the same port with
/// the same reset key.
const CrashLoop = struct {
    reset_key: [32]u8,
    policy: quic.WarmRedialClient.Policy,
    /// See `RedialAppState.perpetual_echo`.
    perpetual_echo: bool = true,
    client_transport_params: @TypeOf(quic.defaultTransportParams()) = quic.defaultTransportParams(),
    /// How long a dying incarnation keeps serving after the client's rebind
    /// before it crashes.
    settle_ms: u64 = 0,
    /// How long each restarted incarnation goes unserviced. The client's
    /// packets wait in its socket, so the death certificate (the stateless
    /// reset) reaches the client this much later: the detection latency of a
    /// client that calls rarely.
    detect_delay_ms: u64 = 0,
};

const CrashLoopResult = struct {
    crashes: u32,
    give_up_cause: ?rpc_events.DisconnectCause,
    outcome: ?quic.WarmRedialClient.Outcome,
};

fn monotonicMs() u64 {
    const ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    return @intCast(@divTrunc(ns, std.time.ns_per_ms));
}

/// Run a `WarmRedialClient` against `loop` until it gives up, or until a
/// bound that covers `max_redials + 2` generations. Returns what the client
/// certified; the caller asserts.
fn runCrashLoop(allocator: std.mem.Allocator, loop: CrashLoop) !CrashLoopResult {
    var vat = RedialVat{};
    defer vat.crash();
    vat.server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .max_concurrent_connections = 2,
        .stateless_reset_key = loop.reset_key,
    });
    const port = vat.server.?.getAddress().getPort();

    var app = RedialAppState{ .perpetual_echo = loop.perpetual_echo };
    var client = try quic.WarmRedialClient.init(
        allocator,
        std.testing.io,
        .{
            .remote_addr = vat.server.?.getAddress(),
            .server_name = "localhost",
            .insecure_skip_verify = true,
            .receive_timeout = std.Io.Duration.fromMilliseconds(5),
            .transport_params = loop.client_transport_params,
        },
        RedialEcho.sturdy_ref,
        loop.policy,
        &app,
        RedialAppState.onRebind,
        RedialAppState.onGiveUp,
    );
    defer client.deinit();
    var thread = try std.Thread.spawn(.{}, RedialAppState.runClient, .{ &app, &client });
    var joined = false;
    defer if (!joined) {
        client.requestStop();
        thread.join();
    };

    // One generation costs a dial, the incarnation's life, and the client's
    // detection of its death: a (possibly late) reset while it calls, its
    // idle timeout once it stops.
    const idle_ms: u64 = if (loop.perpetual_echo) 0 else loop.client_transport_params.max_idle_timeout_ms;
    const generation_ms = loopback.loopback_timeout_ms + loop.settle_ms + loop.detect_delay_ms + idle_ms;
    const deadline_ms = monotonicMs() + @as(u64, loop.policy.max_redials + 2) * generation_ms;

    var crashes: u32 = 0;
    var crash_due_ms: ?u64 = null;
    var serve_from_ms: u64 = 0;
    while (!app.gave_up.load(.acquire)) {
        const now_ms = monotonicMs();
        if (now_ms >= deadline_ms) break;
        if (now_ms >= serve_from_ms) {
            _ = try vat.server.?.stepOnce(.poll);
            try vat.bindIfNeeded(allocator);
        }
        if (app.rebinds.load(.acquire) > crashes) {
            // This incarnation has rebound the client: kill it once it has
            // served for `settle_ms`.
            const due_ms = crash_due_ms orelse now_ms + loop.settle_ms;
            crash_due_ms = due_ms;
            if (now_ms >= due_ms) {
                vat.crash();
                try restartVatOnPort(&vat, allocator, port, loop.reset_key);
                crashes += 1;
                crash_due_ms = null;
                serve_from_ms = monotonicMs() + loop.detect_delay_ms;
            }
        }
        loopback.sleepMs(1);
    }

    const give_up_cause = app.giveUpCause();
    if (give_up_cause != null) {
        thread.join();
        joined = true;
    }
    return .{
        .crashes = crashes,
        .give_up_cause = give_up_cause,
        .outcome = if (joined) app.outcome else null,
    };
}

/// The crash loop must exhaust the budget: exactly `max_redials` redials,
/// one rebind per incarnation, and the last death certified as `cause`.
fn expectCrashLoopGaveUp(result: CrashLoopResult, max_redials: u32, cause: rpc_events.DisconnectCause) !void {
    try std.testing.expectEqual(@as(?rpc_events.DisconnectCause, cause), result.give_up_cause);
    const outcome = result.outcome orelse return error.NoOutcome;
    try std.testing.expectEqual(max_redials + 1, result.crashes);
    try std.testing.expectEqual(max_redials + 1, outcome.generations);
    try std.testing.expectEqual(max_redials + 1, outcome.rebinds);
    try std.testing.expectEqual(max_redials, outcome.redials);
    try std.testing.expectEqual(max_redials, outcome.total_redials);
    try std.testing.expectEqual(cause, outcome.last_cause);
}

test "WarmRedialClient budget still gives up on a server that dies right after every rebind" {
    const max_redials: u32 = 3;
    const result = try runCrashLoop(std.testing.allocator, .{
        .reset_key = @splat(0x53),
        // Every generation rebinds, then dies at once: none can live for a
        // minute, so none counts as healthy and none refunds the budget.
        .policy = .{ .max_redials = max_redials, .backoff_ms = 10, .min_healthy_ms = 60_000 },
    });
    try expectCrashLoopGaveUp(result, max_redials, .stateless_reset);
}

// Health must be measured against evidence that the server is alive, not
// against when the client noticed it was dead. In the two tests below every
// dead generation lasts several times `min_healthy_ms` on the client's clock,
// yet the server proves itself alive for only a few milliseconds after each
// rebind. A budget that counts time-until-detection as health resets the
// streak on every one of them and redials forever.

test "WarmRedialClient budget gives up on a crash loop whose deaths are detected late" {
    const max_redials: u32 = 3;
    const min_healthy_ms: u64 = 250;
    const result = try runCrashLoop(std.testing.allocator, .{
        .reset_key = @splat(0x54),
        .policy = .{ .max_redials = max_redials, .backoff_ms = 10, .min_healthy_ms = min_healthy_ms },
        // The certificate is the usual stateless reset, but it arrives three
        // times `min_healthy_ms` after the incarnation died: the shape of a
        // client that calls rarely (every 15 s against the 10 s default).
        .detect_delay_ms = 3 * min_healthy_ms,
    });
    try expectCrashLoopGaveUp(result, max_redials, .stateless_reset);
}

test "WarmRedialClient budget gives up when every generation idles out after its rebind" {
    const max_redials: u32 = 3;
    // Scaled stand-in for the defaults (10 s health threshold, 30 s idle
    // timeout): the idle timeout is three times the threshold, so a budget
    // that counts idle time as health refunds every generation. 600 ms, not
    // 200: `settle_ms` (half of it) must let the dying incarnation ACK
    // everything in flight, and on a loaded windows-latest runner 100 ms was
    // not enough. A packet still unacknowledged at the crash is retransmitted
    // into the restarted incarnation, which answers with a stateless reset,
    // so the generation ends in `.stateless_reset` instead of the idle
    // timeout under test (CI run 37172642153).
    const min_healthy_ms: u64 = 600;
    var params = quic.defaultTransportParams();
    params.max_idle_timeout_ms = 3 * min_healthy_ms;
    const result = try runCrashLoop(std.testing.allocator, .{
        .reset_key = @splat(0x55),
        .policy = .{
            .max_redials = max_redials,
            .backoff_ms = 10,
            .min_healthy_ms = min_healthy_ms,
            .redial_on_idle_timeout = true,
        },
        // The app sends nothing after its rebind, so it never draws a reset
        // from the restarted incarnation: each generation ends in its idle
        // timeout. The dying incarnation first serves long enough to
        // acknowledge everything in flight; an unacknowledged packet would be
        // retransmitted into the restarted incarnation and draw a reset.
        .perpetual_echo = false,
        .client_transport_params = params,
        .settle_ms = min_healthy_ms / 2,
    });
    try expectCrashLoopGaveUp(result, max_redials, .idle_timeout);
}

// ---------------------------------------------------------------------------
// Unauthenticated datagrams. UDP lets anyone who can reach an endpoint send
// it a datagram, and a QUIC packet is authenticated only by its AEAD tag:
// every header field in front of the tag (connection-ID lengths, the token
// length, the Length field, the size of the datagram) is whatever the sender
// wrote. RFC 9000 §12.2 / RFC 9001 §5.5: a packet that does not
// authenticate is discarded, and it must not end the connection.
//
// quic-zig v0.24.1 and every earlier tag returned an error from
// `Connection.handle` for such a packet (a datagram too short for the
// header-protection sample, a Length larger than the datagram, a
// connection-ID length over 20, ...). `Server.feed` closes the connection
// on that error, and so does our client loop
// (src/rpc/transport/quic/client_endpoint.zig `handleDatagram` does `try
// conn.handle(...)`, the step fails, `run()` terminates). So one datagram of
// 12 bytes from anyone who saw a connection ID ended a capnp-zig QUIC
// connection, client or server. Fixed in quic-zig v0.25.0 (commit 7209b55):
// every failure before a packet authenticates is a dropped packet.
//
// Each test runs a real `quic.connect` client against a real `quic.serve`
// server through `udp_tap.zig`, a relay that adds one datagram of its own
// from the peer's address, then requires the connection to stay up: no
// close on either side, and another RPC round trip on the same connection.
// ---------------------------------------------------------------------------

const udp_tap = @import("udp_tap.zig");

const unauth_interface_id: u64 = 0x5155_4943;
const unauth_method_id: u16 = 7;
/// Bound on every wait below. Generous: Windows CI is slow.
const unauth_wait_ms: u64 = 10_000;
/// Time a connection ended by the extra datagram has to show it before the
/// next round trip, which also catches it (the extra datagram is relayed
/// before that round trip's datagrams).
const unauth_settle_ms: u64 = 300;
/// The shortest 1-RTT packet the receiver can take the header-protection
/// sample from: first byte, an 8-byte connection ID, 4 bytes from which the
/// packet number is read, the 16-byte sample (RFC 9001 §5.4.2).
const unauth_min_short_packet_len: usize = 1 + quic.default_quic_local_cid_len + 4 + 16;
/// Far longer than the default max_ack_delay (25 ms), so an ACK owed for
/// the last round trip has gone out on its own, even on a slow Windows
/// runner (where 50 ms left too little margin to rely on).
const unauth_ack_wait_ms: u64 = 300;
/// Bound on the round trips spent waiting for an ACK-only datagram.
const unauth_max_setup_round_trips: usize = 20;

/// Server side: answers every call on its bootstrap capability, and records
/// how its session ended. Callbacks run on the server's `run()` thread.
const UnauthServer = struct {
    accepts: std.atomic.Value(usize) = .init(0),
    calls: std.atomic.Value(usize) = .init(0),
    errors: std.atomic.Value(usize) = .init(0),
    closes: std.atomic.Value(usize) = .init(0),
    cause: std.atomic.Value(rpc_events.DisconnectCause) = .init(.unknown),

    fn onAccept(ctx: ?*anyopaque, session: *quic.PeerServer.Session) anyerror!void {
        const self: *UnauthServer = @ptrCast(@alignCast(ctx.?));
        _ = self.accepts.fetchAdd(1, .acq_rel);
        _ = try session.peer.setBootstrap(.{ .ctx = self, .on_call = onCall });
    }

    fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *UnauthServer = @ptrCast(@alignCast(ctx_ptr));
        _ = self.calls.fetchAdd(1, .acq_rel);
        try peer.sendReturnEmptyStruct(call.question_id);
    }

    fn onError(ctx: ?*anyopaque, _: *quic.PeerServer.Session, _: anyerror) void {
        const self: *UnauthServer = @ptrCast(@alignCast(ctx.?));
        _ = self.errors.fetchAdd(1, .acq_rel);
    }

    fn onClose(ctx: ?*anyopaque, session: *quic.PeerServer.Session) void {
        const self: *UnauthServer = @ptrCast(@alignCast(ctx.?));
        self.cause.store(session.closeCause(), .release);
        _ = self.closes.fetchAdd(1, .acq_rel);
    }
};

/// Client side. A ClientSession is thread-affine and its `run()` blocks, so
/// the test thread asks for calls through `requested`, and the session's own
/// loop sends them from its `on_tick` (wrapped below, after the peer's
/// deadline sweep it already drives). `stop` closes the session the same way.
const UnauthClient = struct {
    // Test thread -> loop thread.
    requested: std.atomic.Value(usize) = .init(0),
    stop: std.atomic.Value(bool) = .init(false),

    // Loop thread -> test thread.
    bootstrapped: std.atomic.Value(bool) = .init(false),
    returned: std.atomic.Value(usize) = .init(0),
    failures: std.atomic.Value(usize) = .init(0),
    closes: std.atomic.Value(usize) = .init(0),
    cause: std.atomic.Value(rpc_events.DisconnectCause) = .init(.unknown),
    /// Written before `failures` is bumped (release); read after an acquire
    /// load of it sees the bump, or after the client thread is joined.
    last_error: ?anyerror = null,
    connect_failure: ?anyerror = null,

    // Loop thread only.
    issued: usize = 0,
    target: ?cap_table.ResolvedCap = null,
    peer_tick: ?*const fn (conn: *quic.Connection) void = null,

    fn fail(self: *UnauthClient, err: anyerror) void {
        self.last_error = err;
        _ = self.failures.fetchAdd(1, .acq_rel);
    }

    fn onBootstrap(ctx_ptr: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *UnauthClient = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.ExpectedBootstrapResults;
        const results = ret.results orelse return error.MissingBootstrapResults;
        const cap = try results.content.getCapability();
        // Kept for every later call, so retained past this callback.
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        self.target = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.bootstrapped.store(true, .release);
    }

    fn buildCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        _ = try call.initCapTableTyped(0);
    }

    fn onReturn(ctx_ptr: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *UnauthClient = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) {
            self.fail(error.ExpectedCallResults);
            return;
        }
        _ = self.returned.fetchAdd(1, .acq_rel);
    }

    fn onTick(conn: *quic.Connection) void {
        const session: *quic.ClientSession = @alignCast(@fieldParentPtr("conn", conn));
        const self: *UnauthClient = @ptrCast(@alignCast(session.user_ctx.?));
        if (self.peer_tick) |peer_tick| peer_tick(conn);
        if (self.stop.load(.acquire)) {
            session.close();
            return;
        }
        const target = self.target orelse return;
        while (self.issued < self.requested.load(.acquire)) : (self.issued += 1) {
            _ = session.peer.sendCallResolved(target, unauth_interface_id, unauth_method_id, self, buildCall, onReturn) catch |err| {
                self.fail(err);
                return;
            };
        }
    }

    fn onError(ctx: ?*anyopaque, _: *quic.ClientSession, err: anyerror) void {
        const self: *UnauthClient = @ptrCast(@alignCast(ctx.?));
        self.fail(err);
    }

    fn onClose(ctx: ?*anyopaque, session: *quic.ClientSession) void {
        const self: *UnauthClient = @ptrCast(@alignCast(ctx.?));
        self.cause.store(session.closeCause(), .release);
        _ = self.closes.fetchAdd(1, .acq_rel);
    }
};

fn runUnauthClient(state: *UnauthClient, dial_addr: std.Io.net.IpAddress) void {
    const session = quic.connect(std.testing.allocator, std.testing.io, .{
        .conn = .{
            .remote_addr = dial_addr,
            .server_name = "localhost",
            .ca_pem = loopback.loopback_cert_pem,
            .receive_timeout = std.Io.Duration.fromMilliseconds(1),
            .handshake_timeout_ms = unauth_wait_ms,
        },
        .default_call_timeout_ms = unauth_wait_ms,
        .ctx = state,
        .on_error = UnauthClient.onError,
        .on_close = UnauthClient.onClose,
    }) catch |err| {
        state.connect_failure = err;
        _ = state.closes.fetchAdd(1, .acq_rel);
        return;
    };
    defer session.deinit();
    // `Peer.init` pointed the connection's tick at the peer's deadline sweep;
    // the wrapper keeps calling it.
    state.peer_tick = session.conn.on_tick;
    session.conn.on_tick = UnauthClient.onTick;
    _ = session.peer.sendBootstrap(state, UnauthClient.onBootstrap) catch |err| {
        state.fail(err);
        state.stop.store(true, .release);
    };
    session.run();
}

fn runUnauthServer(server: *quic.PeerServer) void {
    server.run();
}

fn runUnauthTap(tap: *udp_tap.UdpTap) void {
    tap.run();
}

const UnauthOptions = struct {
    corrupt_first_server_datagram: bool = false,
};

/// Server, tap and client, each on its own thread. Initialized in place:
/// the threads hold pointers into it.
const UnauthHarness = struct {
    served: UnauthServer = .{},
    client: UnauthClient = .{},
    server: *quic.PeerServer = undefined,
    tap: udp_tap.UdpTap = undefined,
    server_thread: ?std.Thread = null,
    tap_thread: ?std.Thread = null,
    client_thread: ?std.Thread = null,

    fn start(self: *UnauthHarness, options: UnauthOptions) !void {
        const allocator = std.testing.allocator;
        self.* = .{};
        self.server = try quic.serve(allocator, std.testing.io, .{
            .listen_addr = loopback.testListenAddr(),
            .tls_cert_pem = loopback.loopback_cert_pem,
            .tls_key_pem = loopback.loopback_key_pem,
            .receive_timeout = std.Io.Duration.fromMilliseconds(1),
            .max_concurrent_connections = 1,
        }, .{
            .ctx = &self.served,
            .on_accept = UnauthServer.onAccept,
            .on_error = UnauthServer.onError,
            .on_close = UnauthServer.onClose,
        });
        errdefer self.server.deinit();
        self.tap = try udp_tap.UdpTap.init(allocator, std.testing.io, self.server.getAddress());
        errdefer self.tap.deinit();
        self.tap.corrupt_first_server_datagram = options.corrupt_first_server_datagram;

        self.server_thread = try std.Thread.spawn(.{}, runUnauthServer, .{self.server});
        errdefer self.joinThreads();
        self.tap_thread = try std.Thread.spawn(.{}, runUnauthTap, .{&self.tap});
        self.client_thread = try std.Thread.spawn(.{}, runUnauthClient, .{ &self.client, self.tap.address() });
    }

    /// Join every thread and free everything.
    fn stop(self: *UnauthHarness) void {
        self.joinThreads();
        self.tap.deinit();
        self.server.deinit();
    }

    /// Client first, so the server sees its close while the tap still
    /// relays.
    fn joinThreads(self: *UnauthHarness) void {
        if (self.client_thread) |thread| {
            self.client.stop.store(true, .release);
            thread.join();
            self.client_thread = null;
        }
        if (self.server_thread) |thread| {
            self.server.requestStop();
            thread.join();
            self.server_thread = null;
        }
        if (self.tap_thread) |thread| {
            self.tap.stop.store(true, .release);
            thread.join();
            self.tap_thread = null;
        }
    }

    fn connectionEnded(self: *UnauthHarness) bool {
        return self.client.closes.load(.acquire) > 0 or
            self.client.failures.load(.acquire) > 0 or
            self.served.closes.load(.acquire) > 0 or
            self.served.errors.load(.acquire) > 0;
    }

    /// No close on either side, no error, one accepted session. On a
    /// failure, print what each side certified: that is the regression's
    /// evidence.
    fn expectOpen(self: *UnauthHarness) !void {
        if (self.connectionEnded()) {
            std.debug.print(
                "unauthenticated datagram ended the RPC connection: client closes={d} cause={t} last_error={any} | server closes={d} cause={t} errors={d} | extra datagrams={d}, last {d} bytes (source {d} bytes)\n",
                .{
                    self.client.closes.load(.acquire),
                    self.client.cause.load(.acquire),
                    if (self.client.failures.load(.acquire) > 0) self.client.last_error else null,
                    self.served.closes.load(.acquire),
                    self.served.cause.load(.acquire),
                    self.served.errors.load(.acquire),
                    self.tap.injected.load(.acquire),
                    self.tap.last_injected_len.load(.acquire),
                    self.tap.last_source_len.load(.acquire),
                },
            );
        }
        if (self.client.connect_failure) |err| return err;
        try std.testing.expectEqual(rpc_events.DisconnectCause.unknown, self.client.cause.load(.acquire));
        try std.testing.expectEqual(rpc_events.DisconnectCause.unknown, self.served.cause.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), self.client.closes.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), self.served.closes.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), self.client.failures.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), self.served.errors.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), self.served.accepts.load(.acquire));
    }

    /// One more Call -> Return on the bootstrap capability. The first one
    /// also waits for the handshake and the bootstrap.
    fn roundTrip(self: *UnauthHarness) !void {
        var waited_ms: u64 = 0;
        while (!self.client.bootstrapped.load(.acquire)) : (waited_ms += loopback.loopback_poll_ms) {
            if (self.connectionEnded() or waited_ms >= unauth_wait_ms) {
                try self.expectOpen();
                return error.UnauthBootstrapTimedOut;
            }
            loopback.sleepMs(loopback.loopback_poll_ms);
        }
        const want = self.client.requested.fetchAdd(1, .acq_rel) + 1;
        waited_ms = 0;
        while (self.client.returned.load(.acquire) < want) : (waited_ms += loopback.loopback_poll_ms) {
            if (self.connectionEnded() or waited_ms >= unauth_wait_ms) {
                try self.expectOpen();
                return error.UnauthRoundTripTimedOut;
            }
            loopback.sleepMs(loopback.loopback_poll_ms);
        }
        try std.testing.expectEqual(want, self.served.calls.load(.acquire));
    }

    /// Have the tap send one extra datagram toward `direction`, and wait
    /// until it has.
    fn inject(self: *UnauthHarness, direction: udp_tap.Direction, injection: udp_tap.Injection) !void {
        const before = self.tap.injected.load(.acquire);
        const unserved_before = self.tap.unserved.load(.acquire);
        self.tap.request(direction, injection);
        var waited_ms: u64 = 0;
        while (self.tap.injected.load(.acquire) == before) : (waited_ms += loopback.loopback_poll_ms) {
            if (self.tap.unserved.load(.acquire) != unserved_before) return error.UnauthTapHadNothingToSend;
            if (waited_ms >= unauth_wait_ms) return error.UnauthInjectTimedOut;
            loopback.sleepMs(loopback.loopback_poll_ms);
        }
    }

    /// Round trips until the tap has relayed toward `direction` a 1-RTT
    /// datagram whose half is too short for the header-protection sample:
    /// an ACK alone, about 31 bytes. Which round trip leaves one depends on
    /// ACK timing, so wait out the ACK delay between tries, and give up
    /// after a bounded number of them.
    fn roundTripsUntilShortDatagram(self: *UnauthHarness, direction: udp_tap.Direction) !void {
        var tries: usize = 0;
        while (true) : (tries += 1) {
            loopback.sleepMs(unauth_ack_wait_ms);
            const shortest = self.tap.shortest_len[@backingInt(direction)].load(.acquire);
            if (shortest != 0 and shortest / 2 < unauth_min_short_packet_len) return;
            if (tries >= unauth_max_setup_round_trips) return error.UnauthNoShortDatagram;
            try self.roundTrip();
        }
    }

    /// The connection must survive what was just injected: still open after
    /// the settle time, and able to carry another round trip.
    fn expectSurvived(self: *UnauthHarness) !void {
        loopback.sleepMs(unauth_settle_ms);
        try self.expectOpen();
        try self.roundTrip();
        try self.expectOpen();
        try std.testing.expectEqual(@as(usize, 0), self.tap.socket_errors.load(.acquire));
    }
};

fn expectForgedShortHeader(tap: *const udp_tap.UdpTap) !void {
    // First byte + 8-byte connection ID + 3 bytes, built from no real packet.
    try std.testing.expectEqual(@as(usize, 12), tap.last_injected_len.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), tap.last_source_len.load(.acquire));
}

fn expectHalfDatagram(tap: *const udp_tap.UdpTap) !void {
    const source_len = tap.last_source_len.load(.acquire);
    const cut_len = tap.last_injected_len.load(.acquire);
    try std.testing.expectEqual(source_len / 2, cut_len);
    // Half of the shortest datagram (an ACK) is too short for the
    // header-protection sample, so it never reaches the AEAD: the cut that
    // quic-zig v0.24.1 returned from `handle` as an error. Half of a longer
    // datagram would reach the AEAD and be dropped even there.
    try std.testing.expect(cut_len < unauth_min_short_packet_len);
}

test "unauthenticated datagram: a 12-byte forgery with the client's connection ID ends no client connection" {
    var h: UnauthHarness = undefined;
    try h.start(.{});
    defer h.stop();
    try h.roundTrip();
    try h.inject(.to_client, .forged_short_header);
    try expectForgedShortHeader(&h.tap);
    try h.expectSurvived();
}

test "unauthenticated datagram: a server datagram cut to half its length ends no client connection" {
    var h: UnauthHarness = undefined;
    try h.start(.{});
    defer h.stop();
    try h.roundTrip();
    try h.roundTripsUntilShortDatagram(.to_client);
    try h.inject(.to_client, .half_of_shortest);
    try expectHalfDatagram(&h.tap);
    try h.expectSurvived();
}

test "unauthenticated datagram: a handshake datagram with a changed header byte ends no client connection" {
    // The server's first datagram (its Initial, with the handshake flight)
    // reaches the client twice: first with byte 5, the Destination
    // Connection ID Length, changed to 0xf7, then as sent. 0xf7 is longer
    // than any connection ID (20 bytes at most), so the header cannot be
    // parsed.
    var h: UnauthHarness = undefined;
    try h.start(.{ .corrupt_first_server_datagram = true });
    defer h.stop();
    try h.roundTrip();
    try std.testing.expectEqual(@as(usize, 1), h.tap.injected.load(.acquire));
    try h.expectSurvived();
}

test "unauthenticated datagram: a 12-byte forgery with the server's connection ID ends no server session" {
    var h: UnauthHarness = undefined;
    try h.start(.{});
    defer h.stop();
    try h.roundTrip();
    try h.inject(.to_server, .forged_short_header);
    try expectForgedShortHeader(&h.tap);
    try h.expectSurvived();
}

test "unauthenticated datagram: a client datagram cut to half its length ends no server session" {
    var h: UnauthHarness = undefined;
    try h.start(.{});
    defer h.stop();
    try h.roundTrip();
    try h.roundTripsUntilShortDatagram(.to_server);
    try h.inject(.to_server, .half_of_shortest);
    try expectHalfDatagram(&h.tap);
    try h.expectSurvived();
}
