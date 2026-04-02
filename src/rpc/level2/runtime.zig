const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_runtime);
const Connection = @import("connection.zig").Connection;
const net = std.Io.net;

/// Minimal RPC runtime context.
///
/// In the previous xev-based architecture, Runtime wrapped the event loop.
/// With synchronous POSIX I/O, the runtime is a thin holder for shared
/// state (allocator). Connection read loops are driven directly by
/// calling `Connection.run()` on a dedicated thread.
pub const Runtime = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !Runtime {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Runtime) void {
        _ = self;
    }
};

/// TCP listener that accepts inbound connections and wraps them in
/// `Connection` objects.
///
/// Uses `std.Io` for cross-platform socket operations. Call `accept()`
/// in a loop to accept connections; each call blocks until a client
/// connects.
///
/// ## Cleanup
///
/// Call `close()` to stop accepting. This closes the listening socket
/// which also unblocks any thread blocked in `accept()`.
pub const Listener = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    server: net.Server,
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    conn_options: Connection.Options,

    /// Bind and listen on the given address.
    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        addr: net.IpAddress,
        conn_options: Connection.Options,
    ) !Listener {
        const server = try createListenSocket(io, addr, 128, false);
        return .{
            .allocator = allocator,
            .io = io,
            .server = server,
            .conn_options = conn_options,
        };
    }

    /// Wrap an already-bound and listening socket fd.
    ///
    /// Use this when the parent process creates the listening socket and
    /// passes the fd to the child (e.g., to avoid ephemeral port races
    /// in test harnesses).
    pub fn initFd(
        allocator: std.mem.Allocator,
        io: std.Io,
        fd: net.Socket.Handle,
        conn_options: Connection.Options,
    ) Listener {
        return .{
            .allocator = allocator,
            .io = io,
            .server = .{
                .socket = .{ .handle = fd, .address = .{ .ip4 = .unspecified(0) } },
                .options = if (net.Server.AcceptOptions != void) .{ .mode = .stream, .protocol = .tcp } else {},
            },
            .conn_options = conn_options,
        };
    }

    /// Accept a single connection. Blocks until a client connects.
    /// Returns a heap-allocated Connection.
    pub fn accept(self: *Listener) !*Connection {
        if (self.close_requested.load(.acquire)) return error.ListenerClosed;

        const stream = try self.server.accept(self.io);
        const client_fd = stream.socket.handle;
        errdefer closeFd(self.io, client_fd);

        enableTcpNoDelay(client_fd);

        return self.createConnection(client_fd);
    }

    /// Close the listening socket. Idempotent.
    /// This also unblocks any thread blocked in `accept()`.
    pub fn close(self: *Listener) void {
        if (self.close_requested.load(.acquire)) return;
        self.close_requested.store(true, .release);
        closeFd(self.io, self.server.socket.handle);
    }

    /// Return the bound address. Useful for resolving ephemeral ports (port 0).
    pub fn getAddress(self: *const Listener) net.IpAddress {
        return self.server.socket.address;
    }

    /// Return the underlying socket handle.
    pub fn listenHandle(self: *const Listener) net.Socket.Handle {
        return self.server.socket.handle;
    }

    /// Allocate and initialize a Connection. Uses errdefer to guarantee
    /// the heap allocation is freed if Connection.init fails.
    fn createConnection(self: *Listener, fd: net.Socket.Handle) !*Connection {
        const conn_ptr = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn_ptr);

        conn_ptr.* = try Connection.init(
            self.allocator,
            self.io,
            fd,
            self.conn_options,
        );
        return conn_ptr;
    }

    fn enableTcpNoDelay(fd: net.Socket.Handle) void {
        if (comptime builtin.target.os.tag == .windows) return;
        std.posix.setsockopt(
            fd,
            std.posix.IPPROTO.TCP,
            std.posix.TCP.NODELAY,
            &std.mem.toBytes(@as(c_int, 1)),
        ) catch |err| {
            log.debug("failed to set TCP_NODELAY on accepted socket: {}", .{err});
        };
    }
};

// ---------------------------------------------------------------------------
// Cross-platform socket helpers (via std.Io)
// ---------------------------------------------------------------------------

/// Create a TCP listening socket bound to `addr` using std.Io.
pub fn createListenSocket(io: std.Io, addr: net.IpAddress, backlog: u31, _: bool) !net.Server {
    return net.IpAddress.listen(&addr, io, .{
        .kernel_backlog = backlog,
        .reuse_address = true,
    });
}

/// Close a socket handle via Io.
pub fn closeFd(io: std.Io, fd: net.Socket.Handle) void {
    io.vtable.netClose(io.userdata, (&fd)[0..1]);
}

/// Storage union for POSIX socket addresses.
pub const SockAddrStorage = extern union {
    any: std.posix.sockaddr,
    in: std.posix.sockaddr.in,
    in6: std.posix.sockaddr.in6,
};

/// Convert an IpAddress to a POSIX sockaddr for bind/connect.
pub fn ipAddressToSockaddr(addr: net.IpAddress) struct { addr: SockAddrStorage, len: std.posix.socklen_t } {
    switch (addr) {
        .ip4 => |ip4| {
            return .{
                .addr = .{ .in = .{
                    .port = std.mem.nativeToBig(u16, ip4.port),
                    .addr = @bitCast(ip4.bytes),
                } },
                .len = @sizeOf(std.posix.sockaddr.in),
            };
        },
        .ip6 => |ip6| {
            return .{
                .addr = .{ .in6 = .{
                    .port = std.mem.nativeToBig(u16, ip6.port),
                    .flowinfo = ip6.flow,
                    .addr = ip6.bytes,
                    .scope_id = if (ip6.interface.isNone()) 0 else ip6.interface.index,
                } },
                .len = @sizeOf(std.posix.sockaddr.in6),
            };
        },
    }
}

test "runtime init and deinit" {
    var rt = try Runtime.init(std.testing.allocator);
    rt.deinit();
}

test "createConnection returns OOM when Connection allocation fails" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const addr: net.IpAddress = .{ .ip4 = .loopback(0) };
    const server = try createListenSocket(std.testing.io, addr, 128, false);

    var listener = Listener.initFd(std.testing.allocator, std.testing.io, server.socket.handle, .{});
    defer listener.close();

    // fail_index = 0: the very first allocation (create(Connection)) fails.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var fail_listener = Listener{
        .allocator = failing.allocator(),
        .io = std.testing.io,
        .server = server,
        .conn_options = .{},
    };

    // Need a real connected fd for createConnection.
    // Create a socketpair to get a valid fd.
    const fds = try createSocketPair();
    defer closeFd(std.testing.io, fds[0]);
    defer closeFd(std.testing.io, fds[1]);

    try std.testing.expectError(error.OutOfMemory, fail_listener.createConnection(fds[0]));
}

test "createConnection errdefer frees Connection when Transport.init fails" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const addr: net.IpAddress = .{ .ip4 = .loopback(0) };
    const server = try createListenSocket(std.testing.io, addr, 128, false);

    var listener = Listener.initFd(std.testing.allocator, std.testing.io, server.socket.handle, .{});
    defer listener.close();

    // fail_index = 1: the first allocation (create(Connection)) succeeds,
    // but the second allocation (read buffer inside Transport.init) fails.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var fail_listener = Listener{
        .allocator = failing.allocator(),
        .io = std.testing.io,
        .server = server,
        .conn_options = .{},
    };

    const fds = try createSocketPair();
    defer closeFd(std.testing.io, fds[0]);
    defer closeFd(std.testing.io, fds[1]);

    try std.testing.expectError(error.OutOfMemory, fail_listener.createConnection(fds[0]));
}

/// Create a UNIX socketpair for testing. POSIX-only.
fn createSocketPair() ![2]std.posix.fd_t {
    if (comptime builtin.target.os.tag == .windows) return error.SocketPairFailed;
    var fds: [2]std.posix.fd_t = undefined;
    if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
        return error.SocketPairFailed;
    }
    return fds;
}
