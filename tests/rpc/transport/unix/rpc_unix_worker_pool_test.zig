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
//! Every case runs in its own private directory (mode 0700) under /tmp,
//! with short names (`sun_path` is 104 bytes on Darwin). Linux and macOS
//! run the suite; every other target compiles it and runs only the
//! unsupported-target case.

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

/// The bound on a pool shutdown (sprint item 9). An idle pool shuts down in
/// a few milliseconds, so the bound leaves far more than the 300 ms of
/// margin slow CI runners need.
const shutdown_bound_ms: i64 = 2000;

/// Workers parked on the listener in the shutdown cases.
const parked_workers: u32 = 4;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

var dir_counter: std.atomic.Value(u32) = .init(0);

/// A private (0700) directory under /tmp, removed with everything in it.
const TestDir = struct {
    buf: [64]u8 = undefined,
    dir: []const u8 = &.{},

    fn init(self: *TestDir) !void {
        const n = dir_counter.fetchAdd(1, .monotonic);
        self.dir = try std.fmt.bufPrint(&self.buf, "/tmp/czwp-{d}-{d}", .{ sys.getpid(), n });
        std.Io.Dir.cwd().deleteTree(testing.io, self.dir) catch {};
        var z: [65]u8 = undefined;
        _ = try support.check(sys.mkdir(nulTerminated(&z, self.dir), 0o700), "mkdir");
    }

    fn deinit(self: *TestDir) void {
        std.Io.Dir.cwd().deleteTree(testing.io, self.dir) catch {};
    }

    /// `<dir>/<name>` in `out`.
    fn path(self: *const TestDir, out: []u8, name: []const u8) []const u8 {
        return std.fmt.bufPrint(out, "{s}/{s}", .{ self.dir, name }) catch unreachable;
    }
};

fn nulTerminated(buf: []u8, bytes: []const u8) [*:0]const u8 {
    @memcpy(buf[0..bytes.len], bytes);
    buf[bytes.len] = 0;
    return @ptrCast(buf.ptr);
}

/// Whether anything is at `path` (`lstat`).
fn pathExists(path: []const u8) bool {
    var z: [256]u8 = undefined;
    const path_z = nulTerminated(&z, path);
    if (is_linux) {
        const linux = std.os.linux;
        var stx: linux.Statx = undefined;
        const rc = linux.statx(linux.AT.FDCWD, path_z, linux.AT.SYMLINK_NOFOLLOW, .{ .TYPE = true }, &stx);
        return linux.errno(rc) == .SUCCESS;
    } else {
        var st: std.c.Stat = undefined;
        return posix.errno(std.c.fstatat(std.c.AT.FDCWD, path_z, &st, std.c.AT.SYMLINK_NOFOLLOW)) == .SUCCESS;
    }
}

fn unlinkPath(path: []const u8) !void {
    var z: [256]u8 = undefined;
    _ = try support.check(sys.unlink(nulTerminated(&z, path)), "unlink");
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
const Watchdog = struct {
    done: std.atomic.Value(bool) = .init(false),
    what: []const u8,

    fn main(self: *Watchdog) void {
        const start = support.nowNs();
        while (!self.done.load(.acquire)) {
            if (support.msSince(start) > shutdown_bound_ms) {
                std.debug.panic("{s}: still running after {d} ms (bound {d} ms)", .{ self.what, support.msSince(start), shutdown_bound_ms });
            }
            support.sleepMs(5);
        }
    }
};

/// Shut the pool down and join the thread running it, under a watchdog.
/// Returns the milliseconds that took.
fn timedShutdown(pool: *WorkerPool, run_thread: std.Thread, what: []const u8) !i64 {
    var watchdog: Watchdog = .{ .what = what };
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
    var listener = try unix.listen(testing.allocator, testing.io, path, .{});
    errdefer listener.close();
    return WorkerPool.initListener(testing.allocator, listener, ctx, on_accept, .{ .concurrency = parked_workers });
}

// ---------------------------------------------------------------------------
// Shutdown with parked workers
// ---------------------------------------------------------------------------

test "WorkerPool.initListener: 4 workers parked on a Unix listener shut down in under 2 s" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: TestDir = .{};
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
    try testing.expect(!pathExists(path));
    var again = try unix.listen(testing.allocator, testing.io, path, .{});
    again.close();
}

test "WorkerPool.initListener: unlinking the socket file, then shutting down, still takes under 2 s" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: TestDir = .{};
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
    try testing.expect(!pathExists(path));

    const elapsed = try timedShutdown(&pool, run_thread, "shutdown of 4 parked workers after unlink");
    try testing.expect(elapsed < shutdown_bound_ms);
    try testing.expectEqual(@as(u32, 0), pool.acceptors_parked.load(.acquire));
    // `Listener.close` leaves the path alone: it no longer names our file.
    try testing.expect(!pathExists(path));
}

test "WorkerPool.initListener: a TCP listener with no known address also shuts down in under 2 s" {
    if (comptime !supported) return error.SkipZigTest;

    // `Listener.initFd` records no address (0.0.0.0:0), so a dial nudge
    // could not reach it either. The wake door does not need one.
    var bound = try tcp.Listener.init(testing.allocator, testing.io, .{ .ip4 = .loopback(0) }, .{});
    const listener = tcp.Listener.initFd(testing.allocator, testing.io, bound.listenHandle(), .{});
    // From here the pool owns that fd, so `bound` is closed only if the pool
    // was never made.

    var ctx: u8 = 0;
    var pool = WorkerPool.initListener(testing.allocator, listener, &ctx, onAcceptStart, .{ .concurrency = parked_workers }) catch |err| {
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

    var dir: TestDir = .{};
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

    var dir: TestDir = .{};
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
// Ownership and errors
// ---------------------------------------------------------------------------

test "WorkerPool.initListener: deinit without run closes the listener and frees the path" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var ctx: u8 = 0;
    var pool = try initUnixPool(path, &ctx, onAcceptStart);
    try testing.expect(pathExists(path));
    pool.deinit();

    try testing.expect(!pathExists(path));
    var again = try unix.listen(testing.allocator, testing.io, path, .{});
    again.close();
}

test "WorkerPool.initListener: refused setups leave the listener with the caller" {
    if (comptime !supported) return error.SkipZigTest;

    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var listener = try unix.listen(testing.allocator, testing.io, path, .{});
    var ctx: u8 = 0;
    try testing.expectError(
        error.InvalidConcurrency,
        WorkerPool.initListener(testing.allocator, listener, &ctx, onAcceptStart, .{ .concurrency = 0 }),
    );
    // Still the caller's, still blocking, still serving.
    try testing.expect(!try isNonBlocking(listener.listenHandle().handle));
    try testing.expect(pathExists(path));
    listener.close();

    try testing.expectError(
        error.ListenerClosed,
        WorkerPool.initListener(testing.allocator, listener, &ctx, onAcceptStart, .{ .concurrency = 1 }),
    );
}

test "WorkerPool.initListener: unsupported targets return UnixSocketsUnsupported" {
    if (comptime supported) return error.SkipZigTest;

    var listener = try tcp.Listener.init(testing.allocator, testing.io, .{ .ip4 = .loopback(0) }, .{});
    defer listener.close();
    var ctx: u8 = 0;
    try testing.expectError(
        error.UnixSocketsUnsupported,
        WorkerPool.initListener(testing.allocator, listener, &ctx, onAcceptStart, .{ .concurrency = 1 }),
    );
}
