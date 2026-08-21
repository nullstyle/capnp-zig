const std = @import("std");
const capnpc = @import("capnpc-zig");
const loopback = @import("loopback_test_support.zig");

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const Peer = capnpc.rpc.peer.Peer;
const quic = capnpc.rpc.transport.quic;

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
    var server_peer_count: usize = 0;
    defer for (server_peers[0..server_peer_count]) |*peer| peer.deinit();
    var first_session_address: usize = 0;
    var first_address_stable = false;

    // Establish both sessions and complete each initial Bootstrap -> Call ->
    // Return flow before selecting a victim. All endpoints are stepped on this
    // owner thread, so the later fresh sibling call cannot race Peer state.
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += 1) {
        try driveFanoutStep(&server, &clients, &client_close_finalized);
        while (server_peer_count < server.sessionCount()) {
            const session = server.sessionAt(server_peer_count) orelse return error.MissingFanoutSession;
            if (server_peer_count == 0) {
                first_session_address = @intFromPtr(session);
            }
            server_peers[server_peer_count] = Peer.init(allocator, session);
            const peer = &server_peers[server_peer_count];
            _ = try peer.setBootstrap(.{
                .ctx = &server_states[server_peer_count],
                .on_call = ServerState.onCall,
            });
            peer.start(
                &server_states[server_peer_count],
                ServerState.peerError,
                ServerState.peerClose,
            );
            server_peer_count += 1;
        }

        if (server_peer_count == 2 and !first_address_stable) {
            const still_first = server.sessionAt(0) orelse return error.MissingFirstFanoutSession;
            first_address_stable = @intFromPtr(still_first) == first_session_address;
        }

        if (server_peer_count == 2 and
            client_states[0].call_returns.load(.acquire) >= 1 and
            client_states[1].call_returns.load(.acquire) >= 1)
        {
            break;
        }
    }
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
        const victim_session = server.sessionAt(0) orelse return error.MissingFirstFanoutSession;
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
