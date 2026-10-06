//! `WorkerPool.initListener` over a `rpc.transport.unix.listen` listener
//! (sprint item 9).
//!
//! The pool's workers park in `poll` on the listen socket and a wake door.
//! Shutdown writes the door; it never dials the socket file. These cases
//! pin that shutdown is prompt with workers parked, and stays prompt after
//! the socket file is gone (a dial would find nothing there and the old
//! nudge loop would retry forever). Every shutdown runs under a watchdog,
//! so a stuck one fails the suite with a message instead of hanging it.
//!
//! Shutdown also stays prompt when every worker is busy and a connection
//! nobody accepted carries a lingering socket: the kernel does that
//! socket's final close inside the listener's final close, which
//! `Listener.close` leaves to the closer. And `initListener` takes the
//! listener by pointer and marks the caller's copy closed, so a leftover
//! `defer listener.close()` cannot close it under the pool.
//!
//! The pool accepts with its own raw syscalls, so it keeps the listener's
//! other promises itself: its workers take no connection while the fd
//! closer's `.socket` lane is at its bound (as `Listener.accept` does, on a
//! `unix.listen` listener and on `Listener.initFd` over an AF_UNIX socket),
//! and every connection gets the listener's fd passing (as from
//! `ServerSession.accept`).
//!
//! Every case runs in its own private directory (mode 0700) under /tmp,
//! with short names (`sun_path` is 104 bytes on Darwin). Linux and macOS
//! run the suite. The TCP-listener cases need no fd passing, only
//! `initListener` (`park_door_supported`), so a build with
//! `-Dfd-passing=false` runs them too: it serves and shuts down a pool on a
//! TCP listener. Every other target compiles the file, and runs the
//! unsupported-target case where `initListener` is unsupported.

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");
const support = @import("fd_test_support.zig");

const unix = capnpc.rpc.transport.unix;
const tcp = capnpc.rpc.transport.tcp;
const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const WorkerPool = capnpc.rpc.integration.WorkerPool;
const Connection = tcp.Connection;
const Peer = capnpc.rpc.peer.Peer;
const Source = capnpc.rpc.events.Source;

const posix = support.posix;
const sys = support.sys;
const Fd = support.Fd;
const testing = std.testing;

const is_linux = support.is_linux;
const supported = support.supported;
/// Where `initListener` works: Linux and every Darwin target, with or
/// without fd passing. The TCP-listener cases need only this.
const park_door_supported = capnpc.rpc.integration.worker_pool.park_door_supported;

/// The bound on a pool shutdown (sprint item 9). An idle pool shuts down in
/// a few milliseconds, so the bound leaves far more than the 300 ms of
/// margin slow CI runners need.
const shutdown_bound_ms: i64 = 2000;

/// Workers parked on the listener in the shutdown cases.
const parked_workers: u32 = 4;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn unlinkPath(path: []const u8) !void {
    var z: [256]u8 = undefined;
    _ = try support.check(sys.unlink(support.nulTerminated(&z, path)), "unlink");
}

fn fcntlGet(fd: Fd, cmd: i32) !usize {
    const rc = if (is_linux and !builtin.link_libc)
        sys.fcntl(fd, cmd, 0)
    else
        sys.fcntl(fd, cmd);
    return support.check(rc, "fcntl");
}

const nonblock_bit: usize = @as(u32, @bitCast(posix.O{ .NONBLOCK = true }));

fn isNonBlocking(fd: Fd) !bool {
    return (try fcntlGet(fd, posix.F.GETFL)) & nonblock_bit != 0;
}

fn hasCloexec(fd: Fd) !bool {
    return (try fcntlGet(fd, posix.F.GETFD)) & posix.FD_CLOEXEC != 0;
}

fn onPeerError(_: ?*anyopaque, peer: *Peer, _: anyerror) void {
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn onPeerClose(_: ?*anyopaque, _: *Peer) void {}

/// An accept callback that starts the peer and does nothing else. The
/// shutdown cases never connect, so it never runs there.
fn onAcceptStart(_: *anyopaque, peer: *Peer, _: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
    peer.start(null, onPeerError, onPeerClose);
    return .accept;
}

fn runPool(pool: *WorkerPool) void {
    pool.run() catch |err| std.debug.print("WorkerPool.run failed: {t}\n", .{err});
}

/// Wait until `count` workers are parked on the listener.
fn waitParked(pool: *WorkerPool, count: u32) !void {
    const start = support.nowNs();
    while (pool.acceptors_parked.load(.acquire) != count) {
        if (support.msSince(start) > 2000) {
            std.debug.print("{d} of {d} workers parked after 2 s\n", .{ pool.acceptors_parked.load(.acquire), count });
            return error.WorkersNotParked;
        }
        support.sleepMs(1);
    }
    // Registered, and now (almost certainly) inside poll itself.
    support.sleepMs(50);
}

/// Panics if `done` is still false `shutdown_bound_ms` after `start`. A
/// shutdown that never ends would otherwise hang the suite, and CI would
/// report only a job timeout.
///
/// With `rescue`, the first time the bound passes it ends that linger
/// instead (the shutdown then finishes, late, and the test fails on its
/// time) and panics only if the shutdown is still running one more bound
/// later.
const Watchdog = struct {
    done: std.atomic.Value(bool) = .init(false),
    what: []const u8,
    rescue: ?*support.LingeringSocket = null,

    fn main(self: *Watchdog) void {
        const start = support.nowNs();
        var bound_ms = shutdown_bound_ms;
        while (!self.done.load(.acquire)) {
            if (support.msSince(start) > bound_ms) {
                if (self.rescue) |lingering| {
                    std.debug.print("{s}: still running after {d} ms (bound {d} ms); ending the linger\n", .{ self.what, support.msSince(start), shutdown_bound_ms });
                    lingering.endLinger();
                    self.rescue = null;
                    bound_ms += shutdown_bound_ms;
                } else {
                    std.debug.panic("{s}: still running after {d} ms (bound {d} ms)", .{ self.what, support.msSince(start), shutdown_bound_ms });
                }
            }
            support.sleepMs(5);
        }
    }
};

/// Shut the pool down and join the thread running it, under a watchdog
/// (see `Watchdog` for `rescue`). Returns the milliseconds that took.
fn timedShutdown(pool: *WorkerPool, run_thread: std.Thread, what: []const u8) !i64 {
    return timedShutdownRescuing(pool, run_thread, what, null);
}

fn timedShutdownRescuing(pool: *WorkerPool, run_thread: std.Thread, what: []const u8, rescue: ?*support.LingeringSocket) !i64 {
    var watchdog: Watchdog = .{ .what = what, .rescue = rescue };
    const watchdog_thread = try std.Thread.spawn(.{}, Watchdog.main, .{&watchdog});
    const start = support.nowNs();
    pool.shutdown();
    run_thread.join();
    const elapsed = support.msSince(start);
    watchdog.done.store(true, .release);
    watchdog_thread.join();
    return elapsed;
}

/// A pool of `parked_workers` on a fresh `unix.listen` listener at `path`.
fn initUnixPool(path: []const u8, ctx: *anyopaque, on_accept: WorkerPool.AcceptFn) !WorkerPool {
    return initUnixPoolOf(path, ctx, on_accept, parked_workers);
}

fn initUnixPoolOf(path: []const u8, ctx: *anyopaque, on_accept: WorkerPool.AcceptFn, concurrency: u32) !WorkerPool {
    var listener = try unix.listen(testing.allocator, testing.io, path, .{});
    // On success the pool owns it and this copy is marked closed, so the
    // close here does nothing; on error it closes the caller's listener.
    defer listener.close();
    return WorkerPool.initListener(testing.allocator, &listener, ctx, on_accept, .{ .concurrency = concurrency });
}

// ---------------------------------------------------------------------------
// Shutdown with parked workers
// ---------------------------------------------------------------------------

test "WorkerPool.initListener: 4 workers parked on a Unix listener shut down in under 2 s" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var ctx: u8 = 0;
    var pool = try initUnixPool(path, &ctx, onAcceptStart);
    defer pool.deinit();
    // A blocking listener would let a worker that loses the race for a
    // connection block in accept, where the wake door cannot reach it.
    try testing.expect(try isNonBlocking(pool.server.socket.handle));
    const run_thread = try std.Thread.spawn(.{}, runPool, .{&pool});

    try waitParked(&pool, parked_workers);
    const elapsed = try timedShutdown(&pool, run_thread, "shutdown of 4 parked workers");
    try testing.expect(elapsed < shutdown_bound_ms);
    try testing.expectEqual(@as(u32, 0), pool.acceptors_parked.load(.acquire));

    // The pool closed the listener with `Listener.close`: the socket file is
    // gone and the lock is free, so the path binds again.
    try testing.expect(!support.pathExists(path));
    var again = try unix.listen(testing.allocator, testing.io, path, .{});
    again.close();
}

test "WorkerPool.initListener: unlinking the socket file, then shutting down, still takes under 2 s" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var ctx: u8 = 0;
    var pool = try initUnixPool(path, &ctx, onAcceptStart);
    defer pool.deinit();
    const run_thread = try std.Thread.spawn(.{}, runPool, .{&pool});

    try waitParked(&pool, parked_workers);
    // Nothing at the path any more: a dial to it fails (ENOENT), so only a
    // wake that does not go through the path can end the wait.
    try unlinkPath(path);
    try testing.expect(!support.pathExists(path));

    const elapsed = try timedShutdown(&pool, run_thread, "shutdown of 4 parked workers after unlink");
    try testing.expect(elapsed < shutdown_bound_ms);
    try testing.expectEqual(@as(u32, 0), pool.acceptors_parked.load(.acquire));
    // `Listener.close` leaves the path alone: it no longer names our file.
    try testing.expect(!support.pathExists(path));
}

test "WorkerPool.initListener: a TCP listener with no known address also shuts down in under 2 s" {
    if (comptime !park_door_supported) return error.SkipZigTest;

    // `Listener.initFd` records no address (0.0.0.0:0), so a dial nudge
    // could not reach it either. The wake door does not need one.
    var bound = try tcp.Listener.init(testing.allocator, testing.io, .{ .ip4 = .loopback(0) }, .{});
    var listener = tcp.Listener.initFd(testing.allocator, testing.io, bound.listenHandle(), .{});
    // From here the pool owns that fd, so `bound` is closed only if the pool
    // was never made.

    var ctx: u8 = 0;
    var pool = WorkerPool.initListener(testing.allocator, &listener, &ctx, onAcceptStart, .{ .concurrency = parked_workers }) catch |err| {
        bound.close();
        return err;
    };
    defer pool.deinit();
    const run_thread = try std.Thread.spawn(.{}, runPool, .{&pool});

    try waitParked(&pool, parked_workers);
    const elapsed = try timedShutdown(&pool, run_thread, "shutdown of 4 workers parked on an address-less TCP listener");
    try testing.expect(elapsed < shutdown_bound_ms);
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

/// The pool side: answers every call with an empty struct, and records what
/// it saw of the accepted socket.
const EchoPool = struct {
    handler_ctx: u8 = 0,
    accepted: std.atomic.Value(u32) = .init(0),
    unix_source: std.atomic.Value(bool) = .init(false),
    nonblocking: std.atomic.Value(bool) = .init(true),
    cloexec: std.atomic.Value(bool) = .init(false),

    fn onCall(_: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        try peer.sendReturnEmptyStruct(call.question_id);
    }

    fn onAccept(ctx: *anyopaque, peer: *Peer, conn: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
        const self: *EchoPool = @ptrCast(@alignCast(ctx));
        self.unix_source.store(conn.transport.source == .unix, .release);
        self.nonblocking.store(try isNonBlocking(conn.transport.fd), .release);
        self.cloexec.store(try hasCloexec(conn.transport.fd), .release);
        _ = try peer.setBootstrap(.{ .ctx = &self.handler_ctx, .on_call = onCall });
        peer.start(null, onPeerError, onPeerClose);
        _ = self.accepted.fetchAdd(1, .acq_rel);
        return .accept;
    }
};

/// Bootstrap, one call, then close.
const ClientApp = struct {
    peer: *Peer = undefined,
    calls_ok: usize = 0,
    call_returned: bool = false,
    failed: bool = false,

    fn close(self: *ClientApp) void {
        if (!self.peer.isAttachedTransportClosing()) self.peer.closeAttachedTransport();
    }

    fn fail(self: *ClientApp) void {
        self.failed = true;
        self.close();
    }

    fn onBootstrap(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ClientApp = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) return self.fail();
        const payload = ret.results orelse return self.fail();
        const cap = payload.content.getCapability() catch return self.fail();
        const id = switch (try caps.resolveCapability(cap)) {
            .imported => |imported| imported.id,
            else => return self.fail(),
        };
        _ = try peer.sendCallResolved(.{ .imported = .{ .id = id } }, 0x1234, 0, self, buildEmpty, onCallReturn);
    }

    fn buildEmpty(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        _ = try call.initCapTableTyped(0);
    }

    fn onCallReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ClientApp = @ptrCast(@alignCast(ctx));
        if (ret.tag == .results) self.calls_ok += 1;
        self.call_returned = true;
        self.close();
    }
};

test "WorkerPool.initListener: serves bootstrap and one call over a Unix socket, on a blocking close-on-exec socket" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var server: EchoPool = .{};
    var pool = try initUnixPool(path, &server, EchoPool.onAccept);
    defer pool.deinit();
    const run_thread = try std.Thread.spawn(.{}, runPool, .{&pool});
    var run_joined = false;
    defer if (!run_joined) {
        pool.shutdown();
        run_thread.join();
    };

    var app: ClientApp = .{};
    {
        // A call deadline bounds the wait, so an unserved call fails the
        // test instead of hanging it.
        const session = try unix.connect(testing.allocator, testing.io, path, .{
            .session = .{ .default_call_timeout_ms = 5_000 },
        });
        defer session.deinit();
        app.peer = &session.peer;
        _ = try session.peer.sendBootstrap(&app, ClientApp.onBootstrap);
        session.run();
    }

    try testing.expect(!app.failed);
    try testing.expect(app.call_returned);
    try testing.expectEqual(@as(usize, 1), app.calls_ok);

    try testing.expectEqual(@as(u32, 1), server.accepted.load(.acquire));
    try testing.expect(server.unix_source.load(.acquire));
    // Darwin's accept copies O_NONBLOCK from the (non-blocking) listener;
    // the pool must clear it, or a full socket buffer panics std.Io's write.
    try testing.expect(!server.nonblocking.load(.acquire));
    try testing.expect(server.cloexec.load(.acquire));

    run_joined = true;
    const elapsed = try timedShutdown(&pool, run_thread, "shutdown after serving one connection");
    try testing.expect(elapsed < shutdown_bound_ms);
}

test "WorkerPool.initListener: serves bootstrap and one call over a TCP listener, with or without fd passing" {
    if (comptime !park_door_supported) return error.SkipZigTest;

    var listener = try tcp.Listener.init(testing.allocator, testing.io, .{ .ip4 = .loopback(0) }, .{});
    // On success the pool owns it and this copy is marked closed, so the
    // close here does nothing; on error it closes the caller's listener.
    defer listener.close();
    const address = listener.getAddress();

    var server: EchoPool = .{};
    var pool = try WorkerPool.initListener(testing.allocator, &listener, &server, EchoPool.onAccept, .{ .concurrency = parked_workers });
    defer pool.deinit();
    const run_thread = try std.Thread.spawn(.{}, runPool, .{&pool});
    var run_joined = false;
    defer if (!run_joined) {
        pool.shutdown();
        run_thread.join();
    };

    var app: ClientApp = .{};
    {
        // A call deadline bounds the wait, so an unserved call fails the
        // test instead of hanging it.
        const session = try tcp.ClientSession.connect(testing.allocator, testing.io, address, .{
            .default_call_timeout_ms = 5_000,
        });
        defer session.deinit();
        app.peer = &session.peer;
        _ = try session.peer.sendBootstrap(&app, ClientApp.onBootstrap);
        session.run();
    }

    try testing.expect(!app.failed);
    try testing.expect(app.call_returned);
    try testing.expectEqual(@as(usize, 1), app.calls_ok);

    try testing.expectEqual(@as(u32, 1), server.accepted.load(.acquire));
    try testing.expect(!server.unix_source.load(.acquire));
    // The pool's own accept keeps its promises on TCP too.
    try testing.expect(!server.nonblocking.load(.acquire));
    try testing.expect(server.cloexec.load(.acquire));

    run_joined = true;
    const elapsed = try timedShutdown(&pool, run_thread, "shutdown after serving one TCP connection");
    try testing.expect(elapsed < shutdown_bound_ms);
}

/// Counts accepts and rejects every connection, so its worker parks again
/// at once.
const RejectCounter = struct {
    count: std.atomic.Value(u32) = .init(0),

    fn onAccept(ctx: *anyopaque, _: *Peer, _: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
        const self: *RejectCounter = @ptrCast(@alignCast(ctx));
        _ = self.count.fetchAdd(1, .acq_rel);
        return .reject;
    }
};

test "WorkerPool.initListener: workers that lose the race for a burst of connections park again" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var counter: RejectCounter = .{};
    var pool = try initUnixPool(path, &counter, RejectCounter.onAccept);
    defer pool.deinit();
    const run_thread = try std.Thread.spawn(.{}, runPool, .{&pool});
    var run_joined = false;
    defer if (!run_joined) {
        pool.shutdown();
        run_thread.join();
    };
    try waitParked(&pool, parked_workers);

    // Every worker wakes for each connection; all but one find nothing to
    // accept. Each such worker must park again, not block in accept.
    const burst = 32;
    var clients: [burst]Fd = undefined;
    var opened: usize = 0;
    defer for (clients[0..opened]) |fd| support.closeFd(fd);
    var addr: posix.sockaddr.un = .{ .family = posix.AF.UNIX, .path = undefined };
    @memset(&addr.path, 0);
    @memcpy(addr.path[0..path.len], path);
    while (opened < burst) : (opened += 1) {
        const fd: Fd = @intCast(try support.check(sys.socket(posix.AF.UNIX, posix.SOCK.STREAM, 0), "socket"));
        clients[opened] = fd;
        _ = try support.check(sys.connect(fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.un)), "connect");
    }

    const start = support.nowNs();
    while (counter.count.load(.acquire) < burst and support.msSince(start) < 5000) support.sleepMs(1);
    try testing.expectEqual(@as(u32, burst), counter.count.load(.acquire));
    try waitParked(&pool, parked_workers);

    run_joined = true;
    const elapsed = try timedShutdown(&pool, run_thread, "shutdown after a burst of 32 connections");
    try testing.expect(elapsed < shutdown_bound_ms);
}

// ---------------------------------------------------------------------------
// A blocking fd in the backlog
// ---------------------------------------------------------------------------

/// Long enough that the linger never ends by itself during the test; the
/// test ends it by closing the accepting side.
const stall_linger_seconds = 60;

/// How long the closer may take to finish once the linger is ended.
const closer_drain_ms = 6000;

/// Counts accepts and serves each connection until it closes.
const AcceptCounter = struct {
    count: std.atomic.Value(u32) = .init(0),

    fn onAccept(ctx: *anyopaque, peer: *Peer, _: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
        const self: *AcceptCounter = @ptrCast(@alignCast(ctx));
        peer.start(null, onPeerError, onPeerClose);
        _ = self.count.fetchAdd(1, .acq_rel);
        return .accept;
    }
};

test "WorkerPool.initListener: a backlog connection carrying a lingering socket does not hold up shutdown while every worker is busy" {
    if (comptime !supported) return error.SkipZigTest;
    try support.fd_io.closer.ensureStarted();
    try support.waitCloserIdle(closer_drain_ms);

    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var counter: AcceptCounter = .{};
    var pool = try initUnixPoolOf(path, &counter, AcceptCounter.onAccept, 1);
    defer pool.deinit();
    const run_thread = try std.Thread.spawn(.{}, runPool, .{&pool});
    var run_joined = false;
    defer if (!run_joined) {
        pool.shutdown();
        run_thread.join();
    };

    // The only worker serves a client that never speaks.
    const idle = try support.connectPath(path);
    defer support.closeFd(idle);
    const start = support.nowNs();
    while (counter.count.load(.acquire) < 1 and support.msSince(start) < 2000) support.sleepMs(1);
    try testing.expectEqual(@as(u32, 1), counter.count.load(.acquire));

    // Nobody is left to accept the next connection: it stays in the backlog
    // with a lingering socket riding on it, until the listener's final close.
    var lingering = try support.LingeringSocket.open(stall_linger_seconds);
    defer lingering.deinit();
    try support.queueLingeringInBacklog(path, &lingering);

    run_joined = true;
    const elapsed = try timedShutdownRescuing(&pool, run_thread, "shutdown with a lingering fd in the backlog", &lingering);
    errdefer std.debug.print("shutdown took {d} ms\n", .{elapsed});
    try testing.expect(elapsed < shutdown_bound_ms);
    try testing.expectEqual(@as(u32, 1), counter.count.load(.acquire));
    try testing.expect(!support.pathExists(path));
    // Setup check: the listener's final close really blocks (on the
    // closer's socket lane), so the bound above means something.
    support.sleepMs(200);
    try testing.expect(support.fd_io.closer.pendingIn(.socket) != 0);

    lingering.endLinger();
    try support.waitCloserIdle(closer_drain_ms);
}

// ---------------------------------------------------------------------------
// The socket lane's bound, and fd passing
// ---------------------------------------------------------------------------

/// Stalls the closer's `.socket` lane: a connection torn down with a
/// lingering socket still unread in its queue. Returns the peer's end, which
/// the caller closes only after the linger ended (on macOS that close waits
/// for the stuck `shutdown(SHUT_RD)` on the lane; FD-0 pins it).
fn stallSocketLane(lingering: *support.LingeringSocket) !Fd {
    const d = try support.socketPair();
    errdefer support.closeFd(d[0]);
    var td = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = d[1] }, 64);
    try support.sendWithFds(d[0], "unread", &.{lingering.client});
    lingering.closeClient();
    td.deinit();
    try support.expectLaneStuck(.socket, 200);
    return d[0];
}

/// Counts the `.backpressure` events that say the `.socket` lane is full.
const LaneEvents = struct {
    socket_close_queue_full: std.atomic.Value(u32) = .init(0),
    last_limit: std.atomic.Value(usize) = .init(0),

    fn observer(self: *LaneEvents) capnpc.rpc.events.Observer {
        return capnpc.rpc.events.Observer.init(self, onEvent);
    }

    fn onEvent(ctx: *anyopaque, event: capnpc.rpc.events.Event) void {
        const self: *LaneEvents = @ptrCast(@alignCast(ctx));
        switch (event) {
            .backpressure => |b| if (b.err == error.SocketCloseQueueFull) {
                self.last_limit.store(b.limit orelse 0, .release);
                _ = self.socket_close_queue_full.fetchAdd(1, .acq_rel);
            },
            else => {},
        }
    }
};

/// How the pool's AF_UNIX listener was made: by `unix.listen`, or raw and
/// wrapped with `Listener.initFd` (a service manager's socket activation).
const GatedListener = enum { unix_listen, init_fd };

/// Behind a stuck socket-lane close, a pool on the AF_UNIX listener `kind`
/// stops accepting at the lane's bound, and shutdown still ends the wait.
fn poolGateCase(kind: GatedListener) !void {
    if (comptime !supported) return error.SkipZigTest;
    try support.fd_io.closer.ensureStarted();
    try support.waitCloserIdle(closer_drain_ms);
    const before = support.FdSnapshot.take();
    {
        const budget_limit = support.BudgetLimit.set(16);
        defer budget_limit.restore();
        const bound = support.fd_io.closer.socketLaneBound();
        try testing.expectEqual(support.fd_io.closer.min_socket_lane_bound, bound);

        var dir: support.TestDir = .{};
        try dir.init();
        defer dir.deinit();
        var path_buf: [96]u8 = undefined;
        const path = dir.path(&path_buf, "s");

        // Every connection is rejected, so its worker tears it down at once
        // with the peer's byte unread: on both kernels its close goes to the
        // `.socket` lane.
        var counter: RejectCounter = .{};
        var lane_events: LaneEvents = .{};
        var listener = switch (kind) {
            .unix_listen => try unix.listen(testing.allocator, testing.io, path, .{}),
            .init_fd => tcp.Listener.initFd(testing.allocator, testing.io, .{ .handle = try support.listenPath(path) }, .{}),
        };
        defer listener.close();
        var pool = try WorkerPool.initListener(testing.allocator, &listener, &counter, RejectCounter.onAccept, .{
            .concurrency = 1,
            .connection_options = .{ .observer = lane_events.observer() },
        });
        defer pool.deinit();

        var lingering = try support.LingeringSocket.open(stall_linger_seconds);
        defer lingering.deinit();
        const stall_peer = try stallSocketLane(&lingering);
        defer {
            // End the linger first: on macOS this close waits for it.
            lingering.endLinger();
            support.closeFd(stall_peer);
        }
        // macOS queues two jobs per teardown (the read half of the
        // shutdown, then the close), Linux one (the close).
        const per_teardown: usize = if (support.is_macos) 2 else 1;
        const stall_jobs = support.fd_io.closer.pendingIn(.socket);
        try testing.expectEqual(per_teardown, stall_jobs);

        // A peer that reconnects in a loop, leaving one byte unread each
        // time. They all wait in the backlog before the pool runs, so each
        // byte is there when its connection is torn down.
        const reconnects = 3 * bound;
        for (0..reconnects) |_| {
            const client = try support.connectPath(path);
            defer support.closeFd(client);
            try support.sendWithFds(client, "u", &.{});
        }
        const held_before = support.FdSnapshot.take();

        const run_thread = try std.Thread.spawn(.{}, runPool, .{&pool});
        var run_joined = false;
        defer if (!run_joined) {
            pool.shutdown();
            run_thread.join();
        };
        // Let the worker take what it will.
        support.sleepMs(500);
        const accepted: usize = counter.count.load(.acquire);
        var held: [64]Fd = undefined;
        const n_held = support.FdSnapshot.take().added(held_before, &held);
        errdefer std.debug.print("accepted {d} of {d} reconnects; {d} fd(s) held; socket lane {d}, bound {d}\n", .{ accepted, reconnects, n_held, support.fd_io.closer.pendingIn(.socket), bound });
        // The stuck close plus the jobs of each accepted connection fill
        // the lane; each of those connections holds one fd.
        const fit = (bound - stall_jobs + per_teardown - 1) / per_teardown;
        try testing.expectEqual(fit, accepted);
        try testing.expectEqual(stall_jobs + fit * per_teardown, support.fd_io.closer.pendingIn(.socket));
        try testing.expectEqual(fit, n_held);
        // One event for the wait, to the pool's observer.
        try testing.expectEqual(@as(u32, 1), lane_events.socket_close_queue_full.load(.acquire));
        try testing.expectEqual(bound, lane_events.last_limit.load(.acquire));

        // Shutdown ends the wait at once, and nothing more was accepted.
        run_joined = true;
        const elapsed = try timedShutdown(&pool, run_thread, "shutdown of a pool waiting at the socket lane's bound");
        try testing.expect(elapsed < shutdown_bound_ms);
        try testing.expectEqual(fit, counter.count.load(.acquire));
        // `Listener.close` removes only a `unix.listen` listener's file.
        try testing.expectEqual(kind == .init_fd, support.pathExists(path));

        // Every fd is released once the blocked close ends.
        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}

test "WorkerPool.initListener: behind a stuck socket-lane close, the workers stop accepting at the lane's bound, and shutdown still ends the wait" {
    // The accept gate of `Listener.accept` (threat table row 8), on the
    // pool's own raw accept: each AF_UNIX close queued behind a stuck one
    // keeps its fd, so a peer that reconnects in a loop must wait in the
    // kernel's backlog once the lane holds `socketLaneBound()` jobs.
    try poolGateCase(.unix_listen);
}

test "WorkerPool.initListener: on a Listener.initFd AF_UNIX socket the workers also stop accepting at the socket lane's bound" {
    // A service manager's socket reaches the app as a raw fd: the pool
    // gates it as it gates a `unix.listen` listener.
    try poolGateCase(.init_fd);
}

/// Records what fd passing an accepted connection got, then rejects it.
const FdPassingProbe = struct {
    seen: std.atomic.Value(bool) = .init(false),
    max_fds_per_message: u8 = 0,
    max_live_imports: u32 = 0,

    fn onAccept(ctx: *anyopaque, peer: *Peer, conn: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
        const self: *FdPassingProbe = @ptrCast(@alignCast(ctx));
        self.max_fds_per_message = conn.transport.maxFdsPerMessage();
        self.max_live_imports = peer.fds.max_live_imports;
        self.seen.store(true, .release);
        return .reject;
    }
};

test "WorkerPool.initListener: every connection gets the listener's fd passing, as from ServerSession.accept" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var listener = try unix.listen(testing.allocator, testing.io, path, .{
        .fd_passing = .{ .max_fds_per_message = 4, .max_live_imported_fds = 7 },
    });
    defer listener.close();
    var probe: FdPassingProbe = .{};
    var pool = try WorkerPool.initListener(testing.allocator, &listener, &probe, FdPassingProbe.onAccept, .{ .concurrency = 1 });
    defer pool.deinit();
    const run_thread = try std.Thread.spawn(.{}, runPool, .{&pool});
    var run_joined = false;
    defer if (!run_joined) {
        pool.shutdown();
        run_thread.join();
    };

    const client = try support.connectPath(path);
    defer support.closeFd(client);
    const start = support.nowNs();
    while (!probe.seen.load(.acquire) and support.msSince(start) < 2000) support.sleepMs(1);
    try testing.expect(probe.seen.load(.acquire));
    try testing.expectEqual(@as(u8, 4), probe.max_fds_per_message);
    try testing.expectEqual(@as(u32, 7), probe.max_live_imports);

    run_joined = true;
    const elapsed = try timedShutdown(&pool, run_thread, "shutdown of a pool with fd passing on");
    try testing.expect(elapsed < shutdown_bound_ms);
}

// ---------------------------------------------------------------------------
// Ownership and errors
// ---------------------------------------------------------------------------

test "WorkerPool.initListener: deinit without run closes the listener and frees the path" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var ctx: u8 = 0;
    var pool = try initUnixPool(path, &ctx, onAcceptStart);
    try testing.expect(support.pathExists(path));
    pool.deinit();

    try testing.expect(!support.pathExists(path));
    var again = try unix.listen(testing.allocator, testing.io, path, .{});
    again.close();
}

test "WorkerPool.initListener: refused setups leave the listener with the caller" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var listener = try unix.listen(testing.allocator, testing.io, path, .{});
    var ctx: u8 = 0;
    try testing.expectError(
        error.InvalidConcurrency,
        WorkerPool.initListener(testing.allocator, &listener, &ctx, onAcceptStart, .{ .concurrency = 0 }),
    );
    // Still the caller's, still open, still blocking, still serving.
    try testing.expect(!listener.close_requested.load(.acquire));
    try testing.expect(!try isNonBlocking(listener.listenHandle().handle));
    try testing.expect(support.pathExists(path));
    listener.close();

    try testing.expectError(
        error.ListenerClosed,
        WorkerPool.initListener(testing.allocator, &listener, &ctx, onAcceptStart, .{ .concurrency = 1 }),
    );
}

test "WorkerPool.initListener: the caller's copy is marked closed, so a leftover close is harmless" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    {
        var listener = try unix.listen(testing.allocator, testing.io, path, .{});
        // The pattern of a server without a pool (examples/rpc_pingpong_unix.zig).
        // Once the pool owns the listener it must do nothing.
        defer listener.close();
        var ctx: u8 = 0;
        var pool = try WorkerPool.initListener(testing.allocator, &listener, &ctx, onAcceptStart, .{ .concurrency = 1 });
        defer pool.deinit();
        const listen_fd = pool.server.socket.handle;

        // A stray close of the caller's copy: the path stays, the listen fd
        // stays open, and the pool still holds the path's lock.
        listener.close();
        try testing.expectError(error.ListenerClosed, listener.accept());
        try testing.expect(support.pathExists(path));
        try testing.expect(support.isOpen(listen_fd));
        try testing.expectError(error.AddressInUse, unix.listen(testing.allocator, testing.io, path, .{}));
        // The pool's own copy is live.
        try testing.expect(!pool.listener.?.close_requested.load(.acquire));
    }
    // The pool's shutdown closed it: the path is gone and binds again.
    try testing.expect(!support.pathExists(path));
    var again = try unix.listen(testing.allocator, testing.io, path, .{});
    again.close();
}

test "WorkerPool.initListener: unsupported targets return UnixSocketsUnsupported" {
    // The pool's own gate, not fd passing's: `-Dfd-passing=false` skips the
    // AF_UNIX cases above on Linux and macOS, but `initListener` still works
    // there.
    if (comptime capnpc.rpc.integration.worker_pool.park_door_supported) return error.SkipZigTest;

    var listener = try tcp.Listener.init(testing.allocator, testing.io, .{ .ip4 = .loopback(0) }, .{});
    defer listener.close();
    var ctx: u8 = 0;
    try testing.expectError(
        error.UnixSocketsUnsupported,
        WorkerPool.initListener(testing.allocator, &listener, &ctx, onAcceptStart, .{ .concurrency = 1 }),
    );
}
