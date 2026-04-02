const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const WorkerPool = capnpc.rpc.worker_pool.WorkerPool;
const Connection = capnpc.rpc.connection.Connection;
const Peer = capnpc.rpc.peer.Peer;
const net = std.Io.net;

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
        std.testing.io,
        .{ .ip4 = .loopback(0) },
        @ptrCast(&dummy_ctx),
        onAcceptNoop,
        .{ .concurrency = 1 },
    );
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

    fn onAccept(ctx: *anyopaque, peer: *Peer, _: *Connection, _: u32) void {
        const self: *AcceptCounter = @ptrCast(@alignCast(ctx));
        _ = self.count.fetchAdd(1, .monotonic);
        peer.start(onPeerError, onPeerClose);
    }
};

// POSIX-only helpers and integration test — guarded because std.posix
// socket/getsockname APIs do not compile on Windows.
const posix_helpers = if (builtin.target.os.tag != .windows) struct {
    /// Helper: create a raw TCP connection to addr, return the fd.
    fn rawTcpConnect(addr: net.IpAddress) !std.posix.fd_t {
        const socket_cloexec_unsupported = builtin.target.os.tag.isDarwin() or builtin.target.os.tag == .haiku;
        const family: c_uint = switch (addr) {
            .ip4 => std.posix.AF.INET,
            .ip6 => std.posix.AF.INET6,
        };
        const flags: c_uint = std.posix.SOCK.STREAM | if (socket_cloexec_unsupported) @as(c_uint, 0) else std.posix.SOCK.CLOEXEC;
        const fd_rc = std.posix.system.socket(family, flags, 0);
        if (std.posix.errno(fd_rc) != .SUCCESS) return error.SocketCreateFailed;
        const fd: std.posix.fd_t = @intCast(fd_rc);
        errdefer closeFd(fd);

        if (socket_cloexec_unsupported) {
            _ = std.posix.system.fcntl(fd, std.posix.F.SETFD, @as(usize, std.posix.FD_CLOEXEC));
        }

        const storage = ipAddressToSockaddr(addr);
        while (true) {
            switch (std.posix.errno(std.posix.system.connect(fd, &storage.addr.any, storage.len))) {
                .SUCCESS => return fd,
                .INTR => continue,
                .CONNREFUSED => return error.ConnectionRefused,
                .CONNRESET => return error.ConnectionResetByPeer,
                .NETUNREACH => return error.NetworkUnreachable,
                .HOSTUNREACH => return error.HostUnreachable,
                .TIMEDOUT => return error.Timeout,
                else => return error.ConnectFailed,
            }
        }
    }

    fn sleepMs(ms: u64) void {
        const ts = std.posix.timespec{
            .sec = @intCast(ms / 1000),
            .nsec = @intCast((ms % 1000) * 1_000_000),
        };
        _ = std.posix.system.nanosleep(&ts, null);
    }

    fn closeFd(fd: std.posix.fd_t) void {
        switch (std.posix.errno(std.posix.system.close(fd))) {
            .SUCCESS, .INTR, .BADF => {},
            else => {},
        }
    }

    const SockAddrStorage = capnpc.rpc.runtime.SockAddrStorage;
    const ipAddressToSockaddr = capnpc.rpc.runtime.ipAddressToSockaddr;

    fn getSockPort(fd: std.posix.fd_t) !u16 {
        var storage: std.posix.sockaddr.storage = undefined;
        var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.storage);
        if (std.posix.errno(std.posix.system.getsockname(fd, @ptrCast(&storage), &addr_len)) != .SUCCESS) {
            return error.GetSockNameFailed;
        }
        const sa: *const std.posix.sockaddr = @ptrCast(&storage);
        if (sa.family == std.posix.AF.INET) {
            const sa_in: *const std.posix.sockaddr.in = @ptrCast(@alignCast(sa));
            return std.mem.bigToNative(u16, sa_in.port);
        } else if (sa.family == std.posix.AF.INET6) {
            const sa_in6: *const std.posix.sockaddr.in6 = @ptrCast(@alignCast(sa));
            return std.mem.bigToNative(u16, sa_in6.port);
        }
        return error.UnsupportedAddressFamily;
    }
} else struct {};

test "WorkerPool: single worker accepts connection then shuts down" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
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

    // Retrieve the actual bound port from the shared listen fd.
    const port = try posix_helpers.getSockPort(pool.server.socket.handle);

    // Run pool in a background thread.
    const pool_thread = try std.Thread.spawn(.{}, struct {
        fn call(p: *WorkerPool) void {
            p.run() catch {};
        }
    }.call, .{&pool});

    const connect_addr: net.IpAddress = .{ .ip4 = .loopback(port) };

    // Poll until an accept is observed, issuing connect attempts while waiting.
    // This avoids flaky fixed-sleep timing on slower CI machines.
    // Use iteration counter (~200 * 10ms = 2s timeout).
    var attempts: u32 = 0;
    while (counter.count.load(.acquire) == 0 and attempts < 200) : (attempts += 1) {
        const client_fd = posix_helpers.rawTcpConnect(connect_addr) catch |err| {
            switch (err) {
                error.ConnectionRefused,
                error.ConnectionResetByPeer,
                error.NetworkUnreachable,
                => {
                    posix_helpers.sleepMs(10);
                    continue;
                },
                else => {
                    pool.shutdown();
                    pool_thread.join();
                    return err;
                },
            }
        };
        posix_helpers.closeFd(client_fd);
        posix_helpers.sleepMs(10);
    }

    pool.shutdown();
    pool_thread.join();

    try std.testing.expect(counter.count.load(.acquire) >= 1);
}
