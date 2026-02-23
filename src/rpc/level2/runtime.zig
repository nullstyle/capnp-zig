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
/// Uses blocking POSIX socket operations. Call `accept()` in a loop
/// to accept connections; each call blocks until a client connects.
///
/// ## Cleanup
///
/// Call `close()` to stop accepting. This closes the listening socket
/// which also unblocks any thread blocked in `accept()`.
pub const Listener = struct {
    allocator: std.mem.Allocator,
    fd: std.posix.fd_t,
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    conn_options: Connection.Options,

    /// Bind and listen on the given address.
    pub fn init(
        allocator: std.mem.Allocator,
        addr: net.IpAddress,
        conn_options: Connection.Options,
    ) !Listener {
        const fd = try createListenSocket(addr, 128, false);
        return .{
            .allocator = allocator,
            .fd = fd,
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
        fd: std.posix.fd_t,
        conn_options: Connection.Options,
    ) Listener {
        return .{
            .allocator = allocator,
            .fd = fd,
            .conn_options = conn_options,
        };
    }

    /// Accept a single connection. Blocks until a client connects.
    /// Returns a heap-allocated Connection.
    pub fn accept(self: *Listener) !*Connection {
        if (self.close_requested.load(.acquire)) return error.ListenerClosed;

        const client_fd = try sysAccept(self.fd);
        errdefer closeFd(client_fd);

        enableTcpNoDelay(client_fd);

        return self.createConnection(client_fd);
    }

    /// Close the listening socket. Idempotent.
    /// This also unblocks any thread blocked in `accept()`.
    pub fn close(self: *Listener) void {
        if (self.close_requested.load(.acquire)) return;
        self.close_requested.store(true, .release);
        closeFd(self.fd);
    }

    /// Return the bound address. Useful for resolving ephemeral ports (port 0).
    pub fn getAddress(self: *const Listener) !net.IpAddress {
        return getSockAddress(self.fd);
    }

    /// Allocate and initialize a Connection. Uses errdefer to guarantee
    /// the heap allocation is freed if Connection.init fails.
    fn createConnection(self: *Listener, fd: std.posix.fd_t) !*Connection {
        const conn_ptr = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn_ptr);

        conn_ptr.* = try Connection.init(
            self.allocator,
            fd,
            self.conn_options,
        );
        return conn_ptr;
    }

    fn enableTcpNoDelay(fd: std.posix.fd_t) void {
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
// Raw POSIX socket helpers
// ---------------------------------------------------------------------------

/// On Darwin, SOCK_CLOEXEC is not supported by the raw socket() syscall.
const socket_cloexec_unsupported = builtin.target.os.tag.isDarwin() or builtin.target.os.tag == .haiku;

/// Create a TCP listening socket bound to `addr`.
pub fn createListenSocket(addr: net.IpAddress, backlog: u31, reuseport: bool) !std.posix.fd_t {
    if (builtin.target.os.tag == .windows) return error.SocketCreateFailed;
    const family: c_uint = switch (addr) {
        .ip4 => std.posix.AF.INET,
        .ip6 => std.posix.AF.INET6,
    };
    const flags: c_uint = std.posix.SOCK.STREAM | if (socket_cloexec_unsupported) @as(c_uint, 0) else std.posix.SOCK.CLOEXEC;
    const fd_rc = std.posix.system.socket(family, flags, 0);
    if (std.posix.errno(fd_rc) != .SUCCESS) return error.SocketCreateFailed;
    const fd: std.posix.fd_t = @intCast(fd_rc);
    errdefer closeFd(fd);

    // Set CLOEXEC via fcntl on platforms that don't support it in socket().
    if (socket_cloexec_unsupported) {
        _ = std.posix.system.fcntl(fd, std.posix.F.SETFD, @as(usize, std.posix.FD_CLOEXEC));
    }

    // SO_REUSEADDR
    try std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

    // SO_REUSEPORT
    if (comptime @hasDecl(std.posix.SO, "REUSEPORT")) {
        if (reuseport) {
            try std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
        }
    }

    // Bind
    var storage = ipAddressToSockaddr(addr);
    if (std.posix.errno(std.posix.system.bind(fd, &storage.addr.any, storage.len)) != .SUCCESS) {
        return error.BindFailed;
    }

    // Listen
    if (std.posix.errno(std.posix.system.listen(fd, backlog)) != .SUCCESS) {
        return error.ListenFailed;
    }

    return fd;
}

/// Accept one connection from a listening socket. Blocks until a client connects.
fn sysAccept(listen_fd: std.posix.fd_t) !std.posix.fd_t {
    while (true) {
        const rc = std.posix.system.accept(listen_fd, null, null);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .CONNABORTED => return error.ConnectionAborted,
            .INVAL => return error.SocketNotListening,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOBUFS, .NOMEM => return error.SystemResources,
            else => return error.AcceptFailed,
        }
    }
}

/// Close a file descriptor, tolerating EINTR and EBADF.
pub fn closeFd(fd: std.posix.fd_t) void {
    switch (std.posix.errno(std.posix.system.close(fd))) {
        .SUCCESS, .INTR, .BADF => {},
        else => {},
    }
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

/// Retrieve the bound address of a socket (resolves ephemeral ports).
fn getSockAddress(fd: std.posix.fd_t) !net.IpAddress {
    var storage: std.posix.sockaddr.storage = undefined;
    var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.storage);
    if (std.posix.errno(std.posix.system.getsockname(fd, @ptrCast(&storage), &addr_len)) != .SUCCESS) {
        return error.GetSockNameFailed;
    }
    const sa: *const std.posix.sockaddr = @ptrCast(&storage);
    return sockaddrToIpAddress(sa);
}

/// Convert a POSIX sockaddr back to an IpAddress.
fn sockaddrToIpAddress(sa: *const std.posix.sockaddr) net.IpAddress {
    if (sa.family == std.posix.AF.INET) {
        const sa_in: *const std.posix.sockaddr.in = @ptrCast(@alignCast(sa));
        return .{ .ip4 = .{
            .bytes = @bitCast(sa_in.addr),
            .port = std.mem.bigToNative(u16, sa_in.port),
        } };
    } else if (sa.family == std.posix.AF.INET6) {
        const sa_in6: *const std.posix.sockaddr.in6 = @ptrCast(@alignCast(sa));
        return .{ .ip6 = .{
            .bytes = sa_in6.addr,
            .port = std.mem.bigToNative(u16, sa_in6.port),
            .flow = sa_in6.flowinfo,
            .interface = .none,
        } };
    }
    // Fallback — should not happen for TCP sockets.
    return .{ .ip4 = .loopback(0) };
}

test "runtime init and deinit" {
    var rt = try Runtime.init(std.testing.allocator);
    rt.deinit();
}

test "createConnection returns OOM when Connection allocation fails" {
    const addr: net.IpAddress = .{ .ip4 = .loopback(0) };
    const fd = try createListenSocket(addr, 128, false);

    var listener = Listener.initFd(std.testing.allocator, fd, .{});
    defer listener.close();

    // fail_index = 0: the very first allocation (create(Connection)) fails.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var fail_listener = Listener{
        .allocator = failing.allocator(),
        .fd = fd,
        .conn_options = .{},
    };

    // Need a real connected fd for createConnection.
    // Create a socketpair to get a valid fd.
    const fds = try createSocketPair();
    defer closeFd(fds[0]);
    defer closeFd(fds[1]);

    try std.testing.expectError(error.OutOfMemory, fail_listener.createConnection(fds[0]));
}

test "createConnection errdefer frees Connection when Transport.init fails" {
    const addr: net.IpAddress = .{ .ip4 = .loopback(0) };
    const fd = try createListenSocket(addr, 128, false);

    var listener = Listener.initFd(std.testing.allocator, fd, .{});
    defer listener.close();

    // fail_index = 1: the first allocation (create(Connection)) succeeds,
    // but the second allocation (read buffer inside Transport.init) fails.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var fail_listener = Listener{
        .allocator = failing.allocator(),
        .fd = fd,
        .conn_options = .{},
    };

    const fds = try createSocketPair();
    defer closeFd(fds[0]);
    defer closeFd(fds[1]);

    try std.testing.expectError(error.OutOfMemory, fail_listener.createConnection(fds[0]));
}

/// Create a UNIX socketpair for testing.
fn createSocketPair() ![2]std.posix.fd_t {
    var fds: [2]std.posix.fd_t = undefined;
    if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
        return error.SocketPairFailed;
    }
    return fds;
}
