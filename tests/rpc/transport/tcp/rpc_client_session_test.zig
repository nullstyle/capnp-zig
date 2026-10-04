const std = @import("std");
const capnpc = @import("capnpc-zig");

const tcp = capnpc.rpc.transport.tcp;
const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const peer_impl = capnpc.rpc.peer;
const Peer = peer_impl.Peer;
const ClientSession = tcp.ClientSession;

// Lifecycle coverage for ClientSession: the one blessed connect/run/close/
// deinit ordering that replaces the hand-rolled (and twice-divergent)
// consumer recipes. Servers here are minimal on purpose — a listen socket
// whose backlog completes the TCP handshake is enough for connect(), and an
// accept-then-drop thread is enough to drive the EOF/terminal path.

const Counters = struct {
    errors: usize = 0,
    closes: usize = 0,

    fn onError(ctx: ?*anyopaque, _: *ClientSession, _: anyerror) void {
        const self: *Counters = @ptrCast(@alignCast(ctx.?));
        self.errors += 1;
    }
    fn onClose(ctx: ?*anyopaque, _: *ClientSession) void {
        const self: *Counters = @ptrCast(@alignCast(ctx.?));
        self.closes += 1;
    }
};

fn listenLoopback(io: std.Io, backlog: u31) !struct { fd: tcp.SocketFd, address: std.Io.net.IpAddress } {
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    const server = try tcp.createListenSocket(io, address, backlog, false);
    return .{ .fd = .{ .handle = server.socket.handle }, .address = server.socket.address };
}

test "close before run: run returns, on_close fires exactly once, fromPeer recovers" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const server = try listenLoopback(io, 64);
    defer tcp.closeFd(io, server.fd);

    var counters = Counters{};
    const session = try tcp.connect(allocator, io, server.address, .{
        .ctx = &counters,
        .on_error = Counters.onError,
        .on_close = Counters.onClose,
    });

    try std.testing.expectEqual(session, ClientSession.fromPeer(&session.peer));

    session.close();
    session.close(); // idempotent
    session.run();

    try std.testing.expectEqual(@as(usize, 1), counters.closes);
    session.deinit();
}

test "requestStop before run: run returns without traffic, deinit leak-free" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const server = try listenLoopback(io, 64);
    defer tcp.closeFd(io, server.fd);

    var counters = Counters{};
    const session = try tcp.connect(allocator, io, server.address, .{
        .ctx = &counters,
        .on_error = Counters.onError,
        .on_close = Counters.onClose,
    });

    session.requestStop();
    session.run();

    try std.testing.expectEqual(@as(usize, 1), counters.closes);
    session.deinit();
}

const AcceptDrop = struct {
    listener: *tcp.Listener,

    fn main(self: AcceptDrop) void {
        const conn = self.listener.accept() catch return;
        const allocator = conn.allocator;
        // Drop the server end immediately: the client observes EOF.
        conn.close();
        conn.deinit();
        allocator.destroy(conn);
    }
};

const DisconnectWaiter = struct {
    session: *ClientSession = undefined,
    fired: usize = 0,
    disconnects: usize = 0,

    fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *DisconnectWaiter = @ptrCast(@alignCast(ctx));
        self.fired += 1;
        if (ret.tag == .exception) {
            if (ret.exception) |ex| {
                if (std.mem.eql(u8, ex.reason, peer_impl.disconnected_reason)) self.disconnects += 1;
            }
        }
        // Closing from a question callback (inside run()) must be legal and
        // idempotent with the close already in progress.
        self.session.close();
    }
};

test "server EOF fails the in-flight question with Disconnected; close-from-callback is safe" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    const server = try tcp.createListenSocket(io, address, 1, false);
    var listener = tcp.Listener.initFd(allocator, io, .{ .handle = server.socket.handle }, .{});
    defer listener.close();

    const accept_thread = try std.Thread.spawn(.{}, AcceptDrop.main, .{AcceptDrop{ .listener = &listener }});
    // An early failure must not leave the thread running: closing the
    // listener unblocks an accept that never got a connection.
    var accept_thread_live = true;
    defer if (accept_thread_live) {
        listener.close();
        accept_thread.join();
    };

    var counters = Counters{};
    const session = try tcp.connect(allocator, io, server.socket.address, .{
        .ctx = &counters,
        .on_error = Counters.onError,
        .on_close = Counters.onClose,
    });
    defer session.deinit();

    var waiter = DisconnectWaiter{ .session = session };
    _ = try session.peer.sendBootstrap(&waiter, DisconnectWaiter.onReturn);

    session.run();
    accept_thread.join();
    accept_thread_live = false;

    // The dropped transport resolved the outstanding bootstrap exactly once
    // with the Disconnected terminal before on_close fired.
    try std.testing.expectEqual(@as(usize, 1), waiter.fired);
    try std.testing.expectEqual(@as(usize, 1), waiter.disconnects);
    try std.testing.expectEqual(@as(usize, 1), counters.closes);
}

// ---------------------------------------------------------------------------
// A timed-out call's callback error and the session (Stable behavior)
// ---------------------------------------------------------------------------
//
// Both Stable sessions close their transport on `on_error` and stamp a 30s
// call deadline by default. When a deadline cancels a question and its
// callback returns `unwrap()`'s `error.CallTimedOut` (the `try
// response.unwrap()` idiom), the failure is reported as a `.cancel_failure`
// observer event only, so the session keeps running and later calls work.
// Each test runs a real ClientSession against a real ServerSession; one has
// the client time out a call, the other the server.

const Persistent = capnpc.rpc.generated.persistent.Persistent;
const ServerSession = tcp.ServerSession;
const events = capnpc.rpc.events;

/// `on_error`/`on_close` counters for either session type.
fn SessionCounters(comptime Session: type) type {
    return struct {
        errors: usize = 0,
        last_error: ?anyerror = null,
        closes: usize = 0,

        fn onError(ctx: ?*anyopaque, _: *Session, err: anyerror) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.errors += 1;
            self.last_error = err;
        }
        fn onClose(ctx: ?*anyopaque, _: *Session) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.closes += 1;
        }
    };
}

/// Records `.cancel_failure` observer events. Runs on the session thread.
const CancelFailureRecorder = struct {
    count: usize = 0,
    last: ?events.CancelFailureEvent = null,

    fn onEvent(ctx: *anyopaque, event: events.Event) void {
        const self: *CancelFailureRecorder = @ptrCast(@alignCast(ctx));
        switch (event) {
            .cancel_failure => |failure| {
                self.count += 1;
                self.last = failure;
            },
            else => {},
        }
    }

    fn observer(self: *CancelFailureRecorder) events.Observer {
        return events.Observer.init(self, onEvent);
    }
};

/// A Persistent server that parks (never answers) its first `park` save
/// calls and answers every later one with empty results.
const ParkingServer = struct {
    park: usize,
    calls: usize = 0,
    answered: usize = 0,
    server: Persistent.Server = undefined,

    fn bind(self: *ParkingServer, peer: *Peer) !void {
        self.server = .{ .ctx = self, .vtable = .{ .save = unusedSave, .save_deferred = saveDeferred } };
        _ = try Persistent.setBootstrap(peer, &self.server);
    }

    fn unusedSave(_: *anyopaque, _: *Peer, _: Persistent.Save.Params.Reader, _: *Persistent.Save.Results.Builder, _: *const cap_table.InboundCapTable) anyerror!void {
        return error.DeferredHandlerExpected;
    }

    fn saveDeferred(ctx: *anyopaque, _: *Peer, _: Persistent.Save.Params.Reader, _: *const cap_table.InboundCapTable, sender: Persistent.Save.ReturnSender) anyerror!void {
        const self: *ParkingServer = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        if (self.calls <= self.park) return; // parked: never answered
        try sender.sendResults(self, buildEmptyResults);
        self.answered += 1;
    }

    fn buildEmptyResults(_: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        var payload = try ret.payloadTyped();
        var content = try payload.initContent();
        _ = try content.initStruct(0, 1);
    }
};

/// Drives one peer through the scenario against the remote's ParkingServer
/// (`park = 2`): bootstrap, then call A (parked, 100ms deadline) and a pacer
/// P (parked, 400ms deadline). A's callback uses the idiom under test. P's
/// deadline fires after A's, so P's callback issues the later call B (the
/// third save, answered). B's callback closes this peer's session.
///
/// `errors_at_b` snapshots this side's `on_error` count when B completes,
/// before the close. Closing from inside B's callback makes the writes the
/// peer attempts after the callback returns (such as B's Finish) fail with
/// BrokenPipe and reach `on_error`: close-path noise, not the behavior under
/// test.
const TimeoutThenCall = struct {
    /// false: A's callback returns `try response.unwrap()`'s error.
    /// true: it catches the error.
    a_catches: bool,
    /// This side's session `on_error` count (same thread as the callbacks).
    errors: *const usize,
    errors_at_b: ?usize = null,
    client: ?Persistent.Client = null,
    a_id: ?u32 = null,
    a_fired: usize = 0,
    a_err: ?anyerror = null,
    p_err: ?anyerror = null,
    b_ok: usize = 0,
    b_err: ?anyerror = null,
    bootstrap_err: ?anyerror = null,

    fn start(self: *TimeoutThenCall, peer: *Peer) !void {
        _ = try Persistent.bootstrap(peer, self, onBootstrap);
    }

    fn onBootstrap(ctx: *anyopaque, peer: *Peer, response: Persistent.BootstrapResponse) anyerror!void {
        const self: *TimeoutThenCall = @ptrCast(@alignCast(ctx));
        const client = response.unwrap() catch |err| {
            self.bootstrap_err = err;
            closeSession(peer);
            return;
        };
        self.client = client;
        const a_id = try client.callSave(self, null, onA);
        try peer.setQuestionDeadline(a_id, 100);
        self.a_id = a_id;
        const p_id = try client.callSave(self, null, onPacer);
        try peer.setQuestionDeadline(p_id, 400);
    }

    fn onA(ctx: *anyopaque, _: *Peer, response: Persistent.Save.Response, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *TimeoutThenCall = @ptrCast(@alignCast(ctx));
        self.a_fired += 1;
        if (response.unwrap()) |_| {} else |err| self.a_err = err;
        if (self.a_catches) return;
        _ = try response.unwrap();
    }

    fn onPacer(ctx: *anyopaque, peer: *Peer, response: Persistent.Save.Response, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *TimeoutThenCall = @ptrCast(@alignCast(ctx));
        _ = response.unwrap() catch |err| {
            self.p_err = err;
            // Only a live session times P out; a closed one fails it with
            // Disconnected, and then there is nothing left to call.
            if (err == error.CallTimedOut) _ = try self.client.?.callSave(self, null, onB);
            return;
        };
        closeSession(peer); // the parking server never answers P
    }

    fn onB(ctx: *anyopaque, peer: *Peer, response: Persistent.Save.Response, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *TimeoutThenCall = @ptrCast(@alignCast(ctx));
        self.errors_at_b = self.errors.*;
        defer closeSession(peer);
        _ = response.unwrap() catch |err| {
            self.b_err = err;
            return;
        };
        self.b_ok += 1;
        self.client.?.release();
    }

    fn closeSession(peer: *Peer) void {
        if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
    }
};

/// Requests a stop on the client session after `budget_ms`, so a regression
/// fails instead of hanging. `stopped` records that it fired.
const SessionWatchdog = struct {
    session: *ClientSession,
    budget_ms: u64 = 10_000,
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    stopped: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn run(self: *SessionWatchdog, io: std.Io) void {
        var waited_ms: u64 = 0;
        while (waited_ms < self.budget_ms and !self.done.load(.acquire)) : (waited_ms += 10) {
            sleepMs(io, 10);
        }
        if (!self.done.load(.acquire)) {
            self.stopped.store(true, .release);
            self.session.requestStop();
        }
    }

    fn sleepMs(io: std.Io, ms: u64) void {
        const duration: std.Io.Clock.Duration = .{
            .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
            .clock = .awake,
        };
        duration.sleep(io) catch {};
    }
};

/// Joins and frees what a session test started, in the one order that
/// cannot hang: the watchdog first (it reaches into the session), then the
/// client session (its deinit gives an accepted server EOF), then the server
/// thread (closing the listener first unblocks an accept that never got a
/// connection). The success path calls `finish` before its assertions; the
/// deferred call then does nothing, so an early `try` failure can never
/// leave a thread running or a session allocated.
const SessionTeardown = struct {
    listener: *tcp.Listener,
    server_thread: ?std.Thread,
    session: ?*ClientSession = null,
    watchdog: ?*SessionWatchdog = null,
    watchdog_thread: ?std.Thread = null,

    fn finish(self: *SessionTeardown) void {
        if (self.watchdog_thread) |thread| {
            self.watchdog.?.done.store(true, .release);
            thread.join();
            self.watchdog_thread = null;
        }
        if (self.session) |session| {
            session.deinit();
            self.session = null;
        }
        if (self.server_thread) |thread| {
            self.listener.close();
            thread.join();
            self.server_thread = null;
        }
    }
};

fn listenOne(io: std.Io) !struct { listener: tcp.Listener, address: std.Io.net.IpAddress } {
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    const listen = try tcp.createListenSocket(io, address, 1, false);
    return .{
        .listener = tcp.Listener.initFd(std.testing.allocator, io, .{ .handle = listen.socket.handle }, .{}),
        .address = listen.socket.address,
    };
}

// -- The client times out ----------------------------------------------------

const ParkingServerThread = struct {
    listener: *tcp.Listener,
    parking: ParkingServer = .{ .park = 2 },
    counters: SessionCounters(ServerSession) = .{},

    fn main(self: *ParkingServerThread) void {
        const session = ServerSession.accept(std.testing.allocator, self.listener, .{
            .ctx = &self.counters,
            .on_error = SessionCounters(ServerSession).onError,
            .on_close = SessionCounters(ServerSession).onClose,
        }) catch return;
        defer session.deinit();
        self.parking.bind(&session.peer) catch return;
        session.run();
    }
};

fn expectClientTimeoutLeavesSessionsOpen(a_catches: bool) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var listen = try listenOne(io);
    defer listen.listener.close();
    var server = ParkingServerThread{ .listener = &listen.listener };
    const server_thread = try std.Thread.spawn(.{}, ParkingServerThread.main, .{&server});
    var teardown = SessionTeardown{ .listener = &listen.listener, .server_thread = server_thread };
    defer teardown.finish();

    var counters = SessionCounters(ClientSession){};
    var failures = CancelFailureRecorder{};
    teardown.session = try tcp.connect(allocator, io, listen.address, .{
        .ctx = &counters,
        .on_error = SessionCounters(ClientSession).onError,
        .on_close = SessionCounters(ClientSession).onClose,
        .observer = failures.observer(),
    });
    const session = teardown.session.?;

    var app = TimeoutThenCall{ .a_catches = a_catches, .errors = &counters.errors };
    try app.start(&session.peer);

    var watchdog = SessionWatchdog{ .session = session };
    teardown.watchdog = &watchdog;
    teardown.watchdog_thread = try std.Thread.spawn(.{}, SessionWatchdog.run, .{ &watchdog, io });
    session.run();
    teardown.finish();

    try std.testing.expectEqual(@as(?anyerror, null), app.bootstrap_err);
    try std.testing.expectEqual(@as(usize, 1), app.a_fired);
    try std.testing.expectEqual(@as(?anyerror, error.CallTimedOut), app.a_err);
    // The session outlived A's timeout: P timed out later, and B, sent
    // after that, completed a round trip.
    try std.testing.expectEqual(@as(?anyerror, error.CallTimedOut), app.p_err);
    try std.testing.expectEqual(@as(?anyerror, null), app.b_err);
    try std.testing.expectEqual(@as(usize, 1), app.b_ok);
    // Nothing reached the session's on_error before B's callback closed it.
    try std.testing.expectEqual(@as(?usize, 0), app.errors_at_b);
    // run() returned because B's callback closed the session.
    try std.testing.expect(!watchdog.stopped.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), counters.closes);

    if (a_catches) {
        // A callback that catches has nothing to report.
        try std.testing.expectEqual(@as(usize, 0), failures.count);
    } else {
        // The returned error is visible, as an observer event only.
        try std.testing.expectEqual(@as(usize, 1), failures.count);
        const failure = failures.last.?;
        try std.testing.expectEqual(events.TimeoutKind.call_deadline, failure.kind);
        try std.testing.expectEqual(app.a_id.?, failure.question_id);
        try std.testing.expectEqual(@as(anyerror, error.CallTimedOut), failure.err);
    }

    // The ServerSession served all three saves and never saw an error.
    try std.testing.expectEqual(@as(usize, 3), server.parking.calls);
    try std.testing.expectEqual(@as(usize, 1), server.parking.answered);
    try std.testing.expectEqual(@as(usize, 0), server.counters.errors);
    try std.testing.expectEqual(@as(usize, 1), server.counters.closes);
}

test "ClientSession: a timed-out call whose callback returns the unwrap error leaves the session open" {
    try expectClientTimeoutLeavesSessionsOpen(false);
}

test "ClientSession: a timed-out call whose callback catches the unwrap error leaves the session open" {
    try expectClientTimeoutLeavesSessionsOpen(true);
}

// -- The server times out ----------------------------------------------------

/// The server's bootstrap: answers the client's one save (the kick), then
/// starts the scenario against the client's ParkingServer from the server's
/// own peer.
const KickedServer = struct {
    app: *TimeoutThenCall,
    kicks: usize = 0,
    server: Persistent.Server = undefined,

    fn bind(self: *KickedServer, peer: *Peer) !void {
        self.server = .{ .ctx = self, .vtable = .{ .save = ParkingServer.unusedSave, .save_deferred = onKick } };
        _ = try Persistent.setBootstrap(peer, &self.server);
    }

    fn onKick(ctx: *anyopaque, peer: *Peer, _: Persistent.Save.Params.Reader, _: *const cap_table.InboundCapTable, sender: Persistent.Save.ReturnSender) anyerror!void {
        const self: *KickedServer = @ptrCast(@alignCast(ctx));
        self.kicks += 1;
        try sender.sendResults(self, ParkingServer.buildEmptyResults);
        try self.app.start(peer);
    }
};

const KickedServerThread = struct {
    listener: *tcp.Listener,
    app: TimeoutThenCall,
    kicked: KickedServer = undefined,
    counters: SessionCounters(ServerSession) = .{},
    failures: CancelFailureRecorder = .{},

    fn main(self: *KickedServerThread) void {
        const session = ServerSession.accept(std.testing.allocator, self.listener, .{
            .ctx = &self.counters,
            .on_error = SessionCounters(ServerSession).onError,
            .on_close = SessionCounters(ServerSession).onClose,
            .observer = self.failures.observer(),
        }) catch return;
        defer session.deinit();
        self.app.errors = &self.counters.errors;
        self.kicked = .{ .app = &self.app };
        self.kicked.bind(&session.peer) catch return;
        session.run();
    }
};

/// The client side: kicks the server once, then serves its ParkingServer.
const Kicker = struct {
    server: ?Persistent.Client = null,
    kick_err: ?anyerror = null,
    kicked: usize = 0,

    fn onBootstrap(ctx: *anyopaque, _: *Peer, response: Persistent.BootstrapResponse) anyerror!void {
        const self: *Kicker = @ptrCast(@alignCast(ctx));
        const server = response.unwrap() catch |err| {
            self.kick_err = err;
            return;
        };
        self.server = server;
        _ = try server.callSave(self, null, onKicked);
    }

    fn onKicked(ctx: *anyopaque, _: *Peer, response: Persistent.Save.Response, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *Kicker = @ptrCast(@alignCast(ctx));
        defer self.server.?.release();
        _ = response.unwrap() catch |err| {
            self.kick_err = err;
            return;
        };
        self.kicked += 1;
    }
};

fn expectServerTimeoutLeavesSessionsOpen(a_catches: bool) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var listen = try listenOne(io);
    defer listen.listener.close();
    var server = KickedServerThread{
        .listener = &listen.listener,
        // `errors` is pointed at the server's counters on its thread.
        .app = .{ .a_catches = a_catches, .errors = undefined },
    };
    const server_thread = try std.Thread.spawn(.{}, KickedServerThread.main, .{&server});
    var teardown = SessionTeardown{ .listener = &listen.listener, .server_thread = server_thread };
    defer teardown.finish();

    var counters = SessionCounters(ClientSession){};
    teardown.session = try tcp.connect(allocator, io, listen.address, .{
        .ctx = &counters,
        .on_error = SessionCounters(ClientSession).onError,
        .on_close = SessionCounters(ClientSession).onClose,
    });
    const session = teardown.session.?;
    var parking = ParkingServer{ .park = 2 };
    try parking.bind(&session.peer);
    var kicker = Kicker{};
    _ = try Persistent.bootstrap(&session.peer, &kicker, Kicker.onBootstrap);

    // The server closes its session when its B completes; the client then
    // sees EOF and run() returns.
    var watchdog = SessionWatchdog{ .session = session };
    teardown.watchdog = &watchdog;
    teardown.watchdog_thread = try std.Thread.spawn(.{}, SessionWatchdog.run, .{ &watchdog, io });
    session.run();
    teardown.finish();

    try std.testing.expectEqual(@as(?anyerror, null), kicker.kick_err);
    try std.testing.expectEqual(@as(usize, 1), kicker.kicked);
    try std.testing.expectEqual(@as(usize, 1), server.kicked.kicks);
    try std.testing.expect(!watchdog.stopped.load(.acquire));

    const app = &server.app;
    try std.testing.expectEqual(@as(?anyerror, null), app.bootstrap_err);
    try std.testing.expectEqual(@as(usize, 1), app.a_fired);
    try std.testing.expectEqual(@as(?anyerror, error.CallTimedOut), app.a_err);
    // The ServerSession outlived A's timeout: P timed out later, and B, sent
    // after that, completed a round trip to the client.
    try std.testing.expectEqual(@as(?anyerror, error.CallTimedOut), app.p_err);
    try std.testing.expectEqual(@as(?anyerror, null), app.b_err);
    try std.testing.expectEqual(@as(usize, 1), app.b_ok);
    // Nothing reached the server session's on_error before B's callback
    // closed it, and the close was its only one.
    try std.testing.expectEqual(@as(?usize, 0), app.errors_at_b);
    try std.testing.expectEqual(@as(usize, 1), server.counters.closes);

    if (a_catches) {
        try std.testing.expectEqual(@as(usize, 0), server.failures.count);
    } else {
        try std.testing.expectEqual(@as(usize, 1), server.failures.count);
        const failure = server.failures.last.?;
        try std.testing.expectEqual(events.TimeoutKind.call_deadline, failure.kind);
        try std.testing.expectEqual(app.a_id.?, failure.question_id);
        try std.testing.expectEqual(@as(anyerror, error.CallTimedOut), failure.err);
    }

    // The client served all three saves.
    try std.testing.expectEqual(@as(usize, 3), parking.calls);
    try std.testing.expectEqual(@as(usize, 1), parking.answered);
    try std.testing.expectEqual(@as(usize, 1), counters.closes);
}

test "ServerSession: a timed-out call whose callback returns the unwrap error leaves the session open" {
    try expectServerTimeoutLeavesSessionsOpen(false);
}

test "ServerSession: a timed-out call whose callback catches the unwrap error leaves the session open" {
    try expectServerTimeoutLeavesSessionsOpen(true);
}

test "connect never leaks under allocation failure" {
    const io = std.testing.io;

    const server = try listenLoopback(io, 64);
    defer tcp.closeFd(io, server.fd);

    var fail_index: usize = 0;
    while (fail_index < 24) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        const session = tcp.connect(failing.allocator(), io, server.address, .{}) catch continue;
        // Injection index beyond connect's allocations: tear down cleanly.
        session.close();
        session.run();
        session.deinit();
    }
    // std.testing.allocator (backing the failing wrapper) reports any leak
    // at test end, whichever allocation the injection hit.
}
