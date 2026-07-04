//! Compile-gate for docs/getting-started-rpc.md.
//!
//! Mechanism: every fenced Zig snippet in the guide is pinned here as a
//! verbatim mirror (same identifiers, same bodies), compiled against the
//! real library and the REAL generated modules — `pingpong` is
//! examples/pingpong.zig and `matchmaking` is
//! tests/e2e/zig/generated/matchmaking.zig, wired up in build.zig. On top
//! of type-checking, the quickstart halves are actually run against each
//! other over a loopback socket, and the unwrap() table is exercised arm
//! by arm. If the guide drifts from the shipped API, this file stops
//! compiling (or these tests fail) under `zig build test-docs-snippets`.
//!
//! When editing docs/getting-started-rpc.md, keep each mirror below in
//! sync with the section named in its comment.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const pingpong = @import("pingpong");
const matchmaking = @import("matchmaking");

const rpc = capnpc.rpc;
const message = capnpc.message;
const PingPong = pingpong.PingPong;

// ---------------------------------------------------------------------------
// Guide section 3, "The Server Half"
// ---------------------------------------------------------------------------

fn handlePing(
    _: *anyopaque,
    _: *rpc.peer.Peer,
    params: PingPong.Ping.Params.Reader,
    results: *PingPong.Ping.Results.Builder,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const value = try params.getCount();
    try results.setCount(value + 1);
}

fn onServerPeerError(_: ?*anyopaque, peer: *rpc.peer.Peer, _: anyerror) void {
    // A peer error means the transport is done; close it so run() unwinds.
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

/// Accept one connection and serve it until the client disconnects.
fn serveOne(listener: *rpc.transport.tcp.Listener, server: *PingPong.Server) void {
    const conn = listener.accept() catch return; // heap-allocated Connection
    const allocator = conn.allocator;

    const peer = allocator.create(rpc.peer.Peer) catch {
        conn.deinit();
        allocator.destroy(conn);
        return;
    };
    peer.* = rpc.peer.Peer.init(allocator, conn);

    if (PingPong.setBootstrap(peer, server)) |_| {
        peer.start(null, onServerPeerError, null);
        conn.run(); // blocks until the connection closes
    } else |_| {}

    // Teardown — legal ONLY after run() has returned (or was never called):
    // detach first, then peer, then connection.
    _ = peer.takeAttachedConnection(*rpc.transport.tcp.Connection);
    peer.deinit();
    allocator.destroy(peer);
    conn.deinit();
    allocator.destroy(conn);
}

fn startServer(allocator: std.mem.Allocator, io: std.Io) !void {
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 7001);
    var listener = try rpc.transport.tcp.Listener.init(allocator, io, address, .{});
    defer listener.close();

    var server = PingPong.Server{
        .ctx = undefined, // passed as the first argument to every handler
        .vtable = .{ .ping = handlePing },
    };

    serveOne(&listener, &server);
}

fn setupBackend(init: std.process.Init) !capnpc.io_backend.Backend {
    // .process_init reuses init.io; .threaded / .evented construct their own.
    return capnpc.io_backend.Backend.init(.process_init, init.gpa, init.io);
}

// ---------------------------------------------------------------------------
// Guide section 4, "The Client Half"
// ---------------------------------------------------------------------------

const ClientState = struct {
    result: ?u32 = null,
    err: ?anyerror = null,
};

fn runClient(
    allocator: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
    state: *ClientState,
) !void {
    const session = try rpc.transport.tcp.connect(allocator, io, address, .{});
    defer session.deinit(); // legal here: runs after session.run() returns

    // Request the remote's root capability. Calls are legal immediately —
    // outbound frames enqueue; nothing reads the socket until run().
    _ = try PingPong.Client.fromBootstrap(&session.peer, state, onBootstrap);

    session.run(); // blocks; every callback fires on this thread

    if (state.err) |err| return err;
}

fn onBootstrap(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: PingPong.BootstrapResponse,
) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx_ptr));
    const client = response.unwrap() catch |err| {
        state.err = err;
        rpc.transport.tcp.ClientSession.fromPeer(peer).close();
        return;
    };
    _ = try client.callPing(state, buildPing, onPingReturn);
}

// ---------------------------------------------------------------------------
// Guide section 5, "Making Calls"
// ---------------------------------------------------------------------------

fn buildPing(_: *anyopaque, params: *PingPong.Ping.Params.Builder) anyerror!void {
    try params.setCount(41);
}

fn onPingReturn(
    ctx_ptr: *anyopaque,
    peer: *rpc.peer.Peer,
    response: PingPong.Ping.Response,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const state: *ClientState = @ptrCast(@alignCast(ctx_ptr));
    const session = rpc.transport.tcp.ClientSession.fromPeer(peer);
    defer session.close(); // graceful close; idempotent; legal from callbacks

    const results = response.unwrap() catch |err| {
        state.err = err;
        return;
    };
    state.result = try results.getCount();
}

fn describeFailure(response: PingPong.Ping.Response) ?u32 {
    const results = response.unwrap() catch |err| {
        if (err == error.RemoteException) {
            std.log.warn("remote exception: {s}", .{response.exception.reason});
        }
        return null;
    };
    return results.getCount() catch null;
}

// ---------------------------------------------------------------------------
// Guide section 6, "Capability Lifecycle"
// ---------------------------------------------------------------------------

const App = struct {
    service: ?PingPong.Client = null,
};

fn dropService(app: *App) void {
    if (app.service) |client| {
        client.release(); // balances the bootstrap/resolve retain; at most once
        app.service = null;
    }
}

// ---------------------------------------------------------------------------
// Guide section 7, "Typed Pipelining" (matchmaking is the real generated
// module from tests/e2e/zig/generated/). The callbacks are scaffolding the
// guide refers to but does not reproduce.
// ---------------------------------------------------------------------------

const MatchApp = struct {
    failures: usize = 0,
};

fn findAndReady(app: *MatchApp, service: matchmaking.MatchmakingService.Client) !void {
    // Send findMatch, then immediately call methods on the promised
    // controller — before findMatch's Return has resolved.
    const pipeline = try service.callFindMatchPipelined(app, buildFindMatch, onFindMatchReturn);
    const promised = pipeline.getController();
    _ = try promised.callSignalReady(app, buildSignalReady, onSignalReadyReturn);
    _ = try promised.callGetInfo(app, null, onMatchInfoReturn);
}

fn buildFindMatch(_: *anyopaque, params: *matchmaking.MatchmakingService.FindMatch.Params.Builder) anyerror!void {
    var player = try params.initPlayer();
    var id = try player.initId();
    try id.setId(1);
    try player.setName("DocPlayer");
    try params.setMode(.Duel);
}

fn buildSignalReady(_: *anyopaque, params: *matchmaking.MatchController.SignalReady.Params.Builder) anyerror!void {
    var player = try params.initPlayer();
    try player.setId(1);
}

fn onFindMatchReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: matchmaking.MatchmakingService.FindMatch.Response,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const app: *MatchApp = @ptrCast(@alignCast(ctx_ptr));
    _ = response.unwrap() catch {
        app.failures += 1;
    };
}

fn onSignalReadyReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: matchmaking.MatchController.SignalReady.Response,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const app: *MatchApp = @ptrCast(@alignCast(ctx_ptr));
    _ = response.unwrap() catch {
        app.failures += 1;
    };
}

fn onMatchInfoReturn(
    ctx_ptr: *anyopaque,
    _: *rpc.peer.Peer,
    response: matchmaking.MatchController.GetInfo.Response,
    _: *const rpc.caps.table.InboundCapTable,
) anyerror!void {
    const app: *MatchApp = @ptrCast(@alignCast(ctx_ptr));
    _ = response.unwrap() catch {
        app.failures += 1;
    };
}

// ---------------------------------------------------------------------------
// Guide section 8, "Timeouts"
// ---------------------------------------------------------------------------

fn connectWithTimeouts(
    allocator: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
) !*rpc.transport.tcp.ClientSession {
    return rpc.transport.tcp.connect(allocator, io, address, .{
        // Deadline stamped on every call (default 30_000; null disables).
        .default_call_timeout_ms = 10_000,
        // Graceful close(): force-cancel still-outstanding questions after
        // this drain bound (default 5_000).
        .shutdown_drain_timeout_ms = 2_000,
    });
}

fn pingWithShortDeadline(session: *rpc.transport.tcp.ClientSession, state: *ClientState, client: PingPong.Client) !void {
    const qid = try client.callPing(state, buildPing, onPingReturn);
    try session.peer.setQuestionDeadline(qid, 250); // this call only: 250ms
}

// ---------------------------------------------------------------------------
// Guide section 9, "Cancellation"
// ---------------------------------------------------------------------------

fn abandonPing(session: *rpc.transport.tcp.ClientSession, qid: u32) !void {
    try session.peer.cancelQuestion(qid, "user clicked cancel");
}

// ---------------------------------------------------------------------------
// Guide section 10, "Shutdown and the Threading Contract"
// ---------------------------------------------------------------------------

/// Graceful stop, from the session thread (e.g. inside any callback):
fn stopGracefully(session: *rpc.transport.tcp.ClientSession) void {
    session.close(); // idempotent; drains, then run() unwinds
}

/// Abort from ANY other thread — the single thread-safe entry point.
/// No graceful drain; in-flight questions resolve as Disconnected.
fn stopFromElsewhere(session: *rpc.transport.tcp.ClientSession) void {
    session.requestStop();
}

fn onSessionError(ctx: ?*anyopaque, _: *rpc.transport.tcp.ClientSession, err: anyerror) void {
    const state: *ClientState = @ptrCast(@alignCast(ctx.?));
    if (state.err == null) state.err = err;
}

fn onSessionClose(_: ?*anyopaque, _: *rpc.transport.tcp.ClientSession) void {
    // Last callback before run() returns. Observe, don't deinit.
}

fn connectWithCallbacks(
    allocator: std.mem.Allocator,
    io: std.Io,
    address: std.Io.net.IpAddress,
    state: *ClientState,
) !*rpc.transport.tcp.ClientSession {
    return rpc.transport.tcp.connect(allocator, io, address, .{
        .ctx = state,
        .on_error = onSessionError,
        .on_close = onSessionClose,
    });
}

// ---------------------------------------------------------------------------
// Enforcement
// ---------------------------------------------------------------------------

test "compile-gate: every guide snippet type-checks against the shipped API" {
    // Taking a function's address forces full semantic analysis of its body.
    _ = &startServer;
    _ = &setupBackend;
    _ = &describeFailure;
    _ = &dropService;
    _ = &findAndReady;
    _ = &connectWithTimeouts;
    _ = &pingWithShortDeadline;
    _ = &abandonPing;
    _ = &stopGracefully;
    _ = &stopFromElsewhere;
    _ = &connectWithCallbacks;
}

test "quickstart: the guide's server and client halves round-trip over loopback" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // Ephemeral port instead of the guide's fixed 7001 so CI can't collide.
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try rpc.transport.tcp.Listener.init(allocator, io, address, .{});
    defer listener.close();

    var server = PingPong.Server{
        .ctx = undefined,
        .vtable = .{ .ping = handlePing },
    };
    const server_thread = try std.Thread.spawn(.{}, serveOne, .{ &listener, &server });
    defer server_thread.join();

    var state = ClientState{};
    try runClient(allocator, io, listener.getAddress(), &state);
    try std.testing.expectEqual(@as(?u32, 42), state.result);
}

test "unwrap table: every Return arm maps to the CallError the guide documents" {
    // The reason literals quoted in the guide's table.
    try std.testing.expectEqualStrings("disconnected", rpc.peer.disconnected_reason);
    try std.testing.expectEqualStrings("peer shutting down", rpc.peer.shutdown_reason);
    try std.testing.expectEqualStrings("deadline exceeded", rpc.peer.deadline_reason);

    const disconnected = PingPong.Ping.Response{ .exception = .{
        .reason = rpc.peer.disconnected_reason,
        .trace = "",
        .type_value = 0,
    } };
    try std.testing.expectError(error.Disconnected, disconnected.unwrap());

    const shutdown = PingPong.Ping.Response{ .exception = .{
        .reason = rpc.peer.shutdown_reason,
        .trace = "",
        .type_value = 0,
    } };
    try std.testing.expectError(error.Disconnected, shutdown.unwrap());

    const timed_out = PingPong.Ping.Response{ .exception = .{
        .reason = rpc.peer.deadline_reason,
        .trace = "",
        .type_value = 0,
    } };
    try std.testing.expectError(error.CallTimedOut, timed_out.unwrap());

    const remote = PingPong.Ping.Response{ .exception = .{
        .reason = "application failure",
        .trace = "",
        .type_value = 0,
    } };
    try std.testing.expectError(error.RemoteException, remote.unwrap());
    // Guide section 5: after a RemoteException the reason stays readable
    // on the union arm.
    try std.testing.expectEqual(@as(?u32, null), describeFailure(remote));

    const canceled = PingPong.Ping.Response{ .canceled = {} };
    try std.testing.expectError(error.Canceled, canceled.unwrap());

    const elsewhere = PingPong.Ping.Response{ .results_sent_elsewhere = {} };
    try std.testing.expectError(error.UnexpectedReturn, elsewhere.unwrap());

    // Bootstrap unwrap shares the same mapping.
    const bootstrap_gone = PingPong.BootstrapResponse{ .exception = .{
        .reason = rpc.peer.disconnected_reason,
        .trace = "",
        .type_value = 0,
    } };
    try std.testing.expectError(error.Disconnected, bootstrap_gone.unwrap());
}

test "unwrap success arm: describeFailure reads a real Results.Reader" {
    var builder = message.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    var results = try PingPong.PingResults.Builder.init(&builder);
    try results.setCount(7);

    const bytes = try builder.toBytes();
    defer std.testing.allocator.free(bytes);

    var msg = try message.Message.init(std.testing.allocator, bytes, .{});
    defer msg.deinit();

    const response = PingPong.Ping.Response{
        .results = try PingPong.PingResults.Reader.init(&msg),
    };
    try std.testing.expectEqual(@as(?u32, 7), describeFailure(response));
}

test "CallError error set matches the guide's table exactly" {
    comptime {
        const expected = [_][]const u8{
            "RemoteException",
            "Disconnected",
            "CallTimedOut",
            "Canceled",
            "UnexpectedReturn",
        };
        const error_names = @typeInfo(rpc.peer.CallError).error_set.error_names.?;
        if (error_names.len != expected.len) {
            @compileError("rpc.peer.CallError changed arity; update the table in docs/getting-started-rpc.md");
        }
        for (error_names) |error_name| {
            var found = false;
            for (expected) |name| {
                if (std.mem.eql(u8, error_name, name)) found = true;
            }
            if (!found) {
                @compileError("rpc.peer.CallError gained '" ++ error_name ++ "'; update the table in docs/getting-started-rpc.md");
            }
        }
    }
}

test "session lifecycle snippets run: options, callbacks, close and requestStop" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    // A listen backlog completes the TCP handshake; no accept thread needed
    // for connect() (same trick as rpc_client_session_test.zig).
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try rpc.transport.tcp.Listener.init(allocator, io, address, .{});
    defer listener.close();

    {
        const session = try connectWithTimeouts(allocator, io, listener.getAddress());
        stopGracefully(session); // guide section 10: close() before run() is legal
        session.run();
        session.deinit();
    }

    var state = ClientState{};
    {
        const session = try connectWithCallbacks(allocator, io, listener.getAddress(), &state);
        // Guide section 9: cancelQuestion targets a question id; an unknown
        // id is the documented error, proving the entry point is live.
        try std.testing.expectError(error.UnknownQuestion, abandonPing(session, 12345));
        stopFromElsewhere(session); // thread-safe abort (here: same thread)
        session.run();
        session.deinit();
    }
}
