const std = @import("std");
const capnpc = @import("capnpc-zig");

const WorkerPool = capnpc.rpc.worker_pool.WorkerPool;
const Connection = capnpc.rpc.connection.Connection;
const Peer = capnpc.rpc.peer.Peer;

fn onAcceptNoop(_: *anyopaque, peer: *Peer, _: *Connection, _: u32) void {
    // Just start the peer so it wires up; it will close when the client
    // disconnects.
    peer.start(onPeerError, onPeerClose);
}

fn onPeerError(peer: *Peer, _: anyerror) void {
    if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
}

fn onPeerClose(peer: *Peer) void {
    const allocator = peer.allocator;
    const conn = peer.takeAttachedConnection(*Connection);
    peer.deinit();
    allocator.destroy(peer);
    if (conn) |c| {
        c.deinit();
        allocator.destroy(c);
    }
}

test "WorkerPool: init and deinit with concurrency=1" {
    const allocator = std.testing.allocator;
    var dummy_ctx: u8 = 0;
    var pool = try WorkerPool.init(
        allocator,
        try std.net.Address.parseIp4("127.0.0.1", 0),
        @ptrCast(&dummy_ctx),
        onAcceptNoop,
        .{ .concurrency = 1 },
    );
    pool.deinit();
}

test "WorkerPool: init and deinit with concurrency=4" {
    const allocator = std.testing.allocator;
    var dummy_ctx: u8 = 0;
    if (comptime !@hasDecl(std.posix.SO, "REUSEPORT")) {
        try std.testing.expectError(error.ReusePortUnsupported, WorkerPool.init(
            allocator,
            try std.net.Address.parseIp4("127.0.0.1", 0),
            @ptrCast(&dummy_ctx),
            onAcceptNoop,
            .{ .concurrency = 4 },
        ));
        return;
    }
    var pool = try WorkerPool.init(
        allocator,
        try std.net.Address.parseIp4("127.0.0.1", 0),
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
        try std.net.Address.parseIp4("127.0.0.1", 0),
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
        try std.net.Address.parseIp4("127.0.0.1", 0),
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
    if (comptime !@hasDecl(std.posix.SO, "REUSEPORT")) {
        try std.testing.expectError(error.ReusePortUnsupported, WorkerPool.init(
            allocator,
            try std.net.Address.parseIp4("127.0.0.1", 0),
            @ptrCast(&dummy_ctx),
            onAcceptNoop,
            .{ .concurrency = 2 },
        ));
        return;
    }
    var pool = try WorkerPool.init(
        allocator,
        try std.net.Address.parseIp4("127.0.0.1", 0),
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

    fn onAccept(ctx: *anyopaque, peer: *Peer, _: *Connection, _: u32) void {
        const self: *AcceptCounter = @ptrCast(@alignCast(ctx));
        _ = self.count.fetchAdd(1, .monotonic);
        peer.start(onPeerError, onPeerClose);
    }
};

test "WorkerPool: single worker accepts connection then shuts down" {
    const allocator = std.testing.allocator;

    var counter = AcceptCounter{};
    var pool = try WorkerPool.init(
        allocator,
        try std.net.Address.parseIp4("127.0.0.1", 0),
        @ptrCast(&counter),
        AcceptCounter.onAccept,
        .{ .concurrency = 1 },
    );
    defer pool.deinit();

    // Retrieve the actual bound port from the first worker's fd.
    var sa: std.posix.sockaddr.in = undefined;
    var sa_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    try std.posix.getsockname(pool.workers[0].listen_fd, @ptrCast(&sa), &sa_len);
    const port = std.mem.bigToNative(u16, sa.port);

    // Run pool in a background thread.
    const pool_thread = try std.Thread.spawn(.{}, struct {
        fn call(p: *WorkerPool) void {
            p.run() catch {};
        }
    }.call, .{&pool});

    // Give the worker thread a moment to start the event loop.
    std.Thread.sleep(50 * std.time.ns_per_ms);

    // Connect a client.
    const client = std.net.tcpConnectToAddress(std.net.Address.parseIp4("127.0.0.1", port) catch unreachable) catch |err| {
        std.debug.print("client connect failed: {}\n", .{err});
        pool.shutdown();
        pool_thread.join();
        return err;
    };
    // Close client immediately — the accept should still have fired.
    client.close();

    // Give accept callback time to fire.
    std.Thread.sleep(100 * std.time.ns_per_ms);

    pool.shutdown();
    pool_thread.join();

    try std.testing.expect(counter.count.load(.acquire) >= 1);
}
