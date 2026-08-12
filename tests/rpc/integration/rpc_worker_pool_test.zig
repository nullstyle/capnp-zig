const std = @import("std");
const capnpc = @import("capnpc-zig");

const WorkerPool = capnpc.rpc.integration.worker_pool.WorkerPool;
const Connection = capnpc.rpc.transport.tcp.Connection;
const Peer = capnpc.rpc.peer.Peer;
const tcp = capnpc.rpc.transport.tcp;
const net = std.Io.net;

fn onAcceptNoop(_: *anyopaque, peer: *Peer, _: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
    // Just start the peer so it wires up; it will close when the client
    // disconnects.
    peer.start(null, onPeerError, onPeerClose);
    return .accept;
}

fn onPeerError(_: ?*anyopaque, peer: *Peer, _: anyerror) void {
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn onPeerClose(_: ?*anyopaque, peer: *Peer) void {
    _ = peer;
}

test "WorkerPool: Join leases default secure and explicit null opts out" {
    const defaults = WorkerPool.Config{};
    try std.testing.expectEqual(@as(?u64, 30_000), defaults.join_timeout_ms);
    try std.testing.expectEqual(@as(?u32, null), defaults.connection_options.tick_interval_ms);

    const compatibility = WorkerPool.Config{ .join_timeout_ms = null };
    try std.testing.expectEqual(@as(?u64, null), compatibility.join_timeout_ms);
}

test "WorkerPool: init and deinit with concurrency=1" {
    const allocator = std.testing.allocator;
    var dummy_ctx: u8 = 0;
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&dummy_ctx),
        onAcceptNoop,
        .{ .concurrency = 1 },
    );
    try std.testing.expectEqual(@as(?u64, 30_000), pool.join_timeout_ms);
    try std.testing.expectEqual(@as(?u32, 100), pool.conn_options.tick_interval_ms);
    pool.deinit();
}

test "WorkerPool: init and deinit with concurrency=4" {
    const allocator = std.testing.allocator;
    var dummy_ctx: u8 = 0;
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&dummy_ctx),
        onAcceptNoop,
        .{ .concurrency = 4 },
    );
    pool.deinit();
}

test "WorkerPool: concurrency=0 returns error" {
    const allocator = std.testing.allocator;
    var dummy_ctx: u8 = 0;
    try std.testing.expectError(error.InvalidConcurrency, WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&dummy_ctx),
        onAcceptNoop,
        .{ .concurrency = 0 },
    ));
}

test "WorkerPool: single worker run and immediate shutdown" {
    const allocator = std.testing.allocator;
    var dummy_ctx: u8 = 0;
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&dummy_ctx),
        onAcceptNoop,
        .{ .concurrency = 1 },
    );
    defer pool.deinit();

    // Signal shutdown before run — workers will exit on first timer check.
    pool.shutdown();
    try pool.run();
}

test "WorkerPool: multi-worker run and immediate shutdown" {
    const allocator = std.testing.allocator;
    var dummy_ctx: u8 = 0;
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&dummy_ctx),
        onAcceptNoop,
        .{ .concurrency = 2 },
    );
    defer pool.deinit();

    pool.shutdown();
    try pool.run();
}

const AcceptCounter = struct {
    count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    fn onAccept(ctx: *anyopaque, peer: *Peer, _: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
        const self: *AcceptCounter = @ptrCast(@alignCast(ctx));
        _ = self.count.fetchAdd(1, .monotonic);
        peer.start(null, onPeerError, onPeerClose);
        return .accept;
    }
};

const AcceptFailureCounter = struct {
    count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    fn onAccept(ctx: *anyopaque, _: *Peer, _: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
        const self: *AcceptFailureCounter = @ptrCast(@alignCast(ctx));
        _ = self.count.fetchAdd(1, .monotonic);
        return error.TestAcceptFailed;
    }
};

const RejectCounter = struct {
    count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    fn onAccept(ctx: *anyopaque, _: *Peer, _: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
        const self: *RejectCounter = @ptrCast(@alignCast(ctx));
        _ = self.count.fetchAdd(1, .monotonic);
        return .reject;
    }
};

/// Records whether the pool handed the accepted peer a real entropy source.
///
/// A pool peer with `entropy == null` falls back to COUNTER accept-embargo ids
/// (`nextAcceptEmbargoId`), where the spec wants unguessable ones. `workerMain`
/// used to do `Peer.init` + `setClockIo` and nothing else, so every pool peer
/// took that fallback.
const EntropyObserver = struct {
    count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    saw_entropy: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn onAccept(ctx: *anyopaque, peer: *Peer, _: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
        const self: *EntropyObserver = @ptrCast(@alignCast(ctx));
        // Read BEFORE start(): the pool must have installed it by the time the
        // accept callback can observe the peer.
        self.saw_entropy.store(peer.entropy != null, .release);
        _ = self.count.fetchAdd(1, .monotonic);
        peer.start(null, onPeerError, onPeerClose);
        return .accept;
    }
};

const ActiveCounter = struct {
    count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    fn onAccept(ctx: *anyopaque, peer: *Peer, _: *Connection, _: u32) anyerror!WorkerPool.AcceptDecision {
        const self: *ActiveCounter = @ptrCast(@alignCast(ctx));
        _ = self.count.fetchAdd(1, .monotonic);
        peer.start(null, onPeerError, onPeerClose);
        return .accept;
    }
};

// Cross-platform connection helpers over std.Io so the
// shutdown-with-active-connection tests below run on every platform,
// including Windows. Earlier versions used std.posix
// socket/connect/getsockname/nanosleep, which do not compile on Windows and
// forced those tests to skip there — leaving the Connection.requestClose
// shutdown path (commit fb5fbfc) with zero Windows regression coverage.

/// Open a raw TCP connection to `addr` via std.Io and return the connected
/// socket handle. The caller owns the handle and must close it with
/// `tcp.closeFd`.
fn rawTcpConnect(io: std.Io, addr: net.IpAddress) !tcp.SocketFd {
    var connect_addr = addr;
    const stream = try net.IpAddress.connect(&connect_addr, io, .{ .mode = .stream, .protocol = .tcp });
    return .{ .handle = stream.socket.handle };
}

/// Sleep `ms` milliseconds on the awake clock via std.Io.
fn sleepMs(io: std.Io, ms: u64) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}

fn spawnPoolThread(pool: *WorkerPool) !std.Thread {
    return std.Thread.spawn(.{}, struct {
        fn call(p: *WorkerPool) void {
            p.run() catch {};
        }
    }.call, .{pool});
}

fn connectUntilAccepted(
    io: std.Io,
    addr: net.IpAddress,
    count: *std.atomic.Value(u32),
    keep_connected: bool,
) !?tcp.SocketFd {
    var attempts: u32 = 0;
    while (count.load(.acquire) == 0 and attempts < 200) : (attempts += 1) {
        const client = rawTcpConnect(io, addr) catch |err| {
            switch (err) {
                error.ConnectionRefused,
                error.ConnectionResetByPeer,
                error.NetworkUnreachable,
                => {
                    sleepMs(io, 10);
                    continue;
                },
                else => return err,
            }
        };

        var wait_attempts: u32 = 0;
        while (count.load(.acquire) == 0 and wait_attempts < 20) : (wait_attempts += 1) {
            sleepMs(io, 10);
        }

        if (count.load(.acquire) > 0) {
            if (keep_connected) return client;
            tcp.closeFd(io, client);
            return null;
        }

        tcp.closeFd(io, client);
    }
    return error.AcceptTimedOut;
}

test "WorkerPool: single worker accepts connection then shuts down" {
    const allocator = std.testing.allocator;

    var counter = AcceptCounter{};
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&counter),
        AcceptCounter.onAccept,
        .{ .concurrency = 1 },
    );
    defer pool.deinit();

    // listen() resolves the ephemeral port into the bound address; connect there.
    const connect_addr = pool.server.socket.address;

    const pool_thread = try spawnPoolThread(&pool);
    errdefer {
        pool.shutdown();
        pool_thread.join();
    }

    _ = try connectUntilAccepted(std.testing.io, connect_addr, &counter.count, false);

    pool.shutdown();
    pool_thread.join();

    try std.testing.expect(counter.count.load(.acquire) >= 1);
}

test "WorkerPool: on_accept error closes accepted connection" {
    const allocator = std.testing.allocator;

    var counter = AcceptFailureCounter{};
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&counter),
        AcceptFailureCounter.onAccept,
        .{ .concurrency = 1 },
    );
    defer pool.deinit();

    const connect_addr = pool.server.socket.address;
    const pool_thread = try spawnPoolThread(&pool);
    errdefer {
        pool.shutdown();
        pool_thread.join();
    }

    _ = try connectUntilAccepted(std.testing.io, connect_addr, &counter.count, false);

    pool.shutdown();
    pool_thread.join();

    try std.testing.expectEqual(@as(u32, 1), counter.count.load(.acquire));
}

test "WorkerPool: on_accept reject closes accepted connection" {
    const allocator = std.testing.allocator;

    var counter = RejectCounter{};
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&counter),
        RejectCounter.onAccept,
        .{ .concurrency = 1 },
    );
    defer pool.deinit();

    const connect_addr = pool.server.socket.address;
    const pool_thread = try spawnPoolThread(&pool);
    errdefer {
        pool.shutdown();
        pool_thread.join();
    }

    _ = try connectUntilAccepted(std.testing.io, connect_addr, &counter.count, false);

    pool.shutdown();
    pool_thread.join();

    try std.testing.expectEqual(@as(u32, 1), counter.count.load(.acquire));
}

test "WorkerPool: shutdown closes active connection so run can return" {
    const allocator = std.testing.allocator;

    var counter = ActiveCounter{};
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&counter),
        ActiveCounter.onAccept,
        .{ .concurrency = 1 },
    );
    defer pool.deinit();

    const connect_addr = pool.server.socket.address;
    const pool_thread = try spawnPoolThread(&pool);
    errdefer {
        pool.shutdown();
        pool_thread.join();
    }

    const client_fd = (try connectUntilAccepted(std.testing.io, connect_addr, &counter.count, true)).?;
    defer tcp.closeFd(std.testing.io, client_fd);

    pool.shutdown();
    pool_thread.join();

    try std.testing.expectEqual(@as(u32, 1), counter.count.load(.acquire));
}

fn nowNs(io: std.Io) i64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

test "WorkerPool: graceful shutdown returns promptly with no active connections" {
    const allocator = std.testing.allocator;

    var counter = AcceptCounter{};
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&counter),
        AcceptCounter.onAccept,
        .{ .concurrency = 1 },
    );
    defer pool.deinit();

    const pool_thread = try spawnPoolThread(&pool);
    errdefer {
        pool.shutdown();
        pool_thread.join();
    }

    const start = nowNs(std.testing.io);
    // A long drain bound must not be waited out when nothing is active.
    pool.shutdownGraceful(5000);
    const elapsed = nowNs(std.testing.io) - start;
    pool_thread.join();

    try std.testing.expect(elapsed < 4000 * std.time.ns_per_ms);
}

test "WorkerPool: graceful shutdown drains an active connection that finishes on its own" {
    const allocator = std.testing.allocator;

    var counter = ActiveCounter{};
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&counter),
        ActiveCounter.onAccept,
        .{ .concurrency = 1 },
    );
    defer pool.deinit();

    const connect_addr = pool.server.socket.address;
    const pool_thread = try spawnPoolThread(&pool);
    errdefer {
        pool.shutdown();
        pool_thread.join();
    }

    const client_fd = (try connectUntilAccepted(std.testing.io, connect_addr, &counter.count, true)).?;

    // The client disconnects on its own shortly after the graceful
    // shutdown starts; the drain should observe the worker going idle and
    // return well before the full bound.
    const Disconnecter = struct {
        fn run(fd: tcp.SocketFd, disc_io: std.Io) void {
            sleepMs(disc_io, 80);
            tcp.closeFd(disc_io, fd);
        }
    };
    const disconnect_thread = try std.Thread.spawn(.{}, Disconnecter.run, .{ client_fd, std.testing.io });

    const start = nowNs(std.testing.io);
    pool.shutdownGraceful(10_000);
    const elapsed = nowNs(std.testing.io) - start;

    disconnect_thread.join();
    pool_thread.join();

    try std.testing.expectEqual(@as(u32, 1), counter.count.load(.acquire));
    try std.testing.expect(elapsed < 8000 * std.time.ns_per_ms);
}

test "WorkerPool: an accepted peer is given a real entropy source" {
    const allocator = std.testing.allocator;

    var observer = EntropyObserver{};
    var pool = try WorkerPool.init(
        allocator,
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&observer),
        EntropyObserver.onAccept,
        .{ .concurrency = 1 },
    );
    defer pool.deinit();

    const connect_addr = pool.server.socket.address;
    const pool_thread = try spawnPoolThread(&pool);
    errdefer {
        pool.shutdown();
        pool_thread.join();
    }

    _ = try connectUntilAccepted(std.testing.io, connect_addr, &observer.count, false);

    pool.shutdown();
    pool_thread.join();

    try std.testing.expect(observer.count.load(.acquire) >= 1);
    // The assertion that matters: without it, pool peers silently keep counter
    // embargo ids where the spec asks for unguessable ones.
    try std.testing.expect(observer.saw_entropy.load(.acquire));
}
