const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_runtime);
const Connection = @import("./connection.zig").Connection;
const events = @import("../../events.zig");
const net = std.Io.net;

/// Re-export of the platform-stable socket wrapper used by all public
/// handle-taking entry points in the TCP transport layer.
pub const SocketFd = @import("./stream_transport.zig").SocketFd;

/// Minimal RPC runtime context.
///
/// Runtime is now a thin holder for shared state (allocator). Connection read
/// loops are driven directly by calling `Connection.run()` on a dedicated
/// thread.
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
        socket: SocketFd,
        conn_options: Connection.Options,
    ) Listener {
        return .{
            .allocator = allocator,
            .io = io,
            .server = .{
                .socket = .{ .handle = socket.handle, .address = .{ .ip4 = .unspecified(0) } },
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
        errdefer closeFd(self.io, .{ .handle = client_fd });

        setTcpNoDelay(.{ .handle = client_fd });

        return self.createConnection(client_fd);
    }

    /// Accept a single connection and return only its socket, with Nagle
    /// disabled — the caller owns wiring it into a `Connection`. Used by
    /// `ServerSession`, which embeds the `Connection` by value rather than
    /// taking the heap `*Connection` `accept()` produces.
    pub fn acceptFd(self: *Listener) !SocketFd {
        if (self.close_requested.load(.acquire)) return error.ListenerClosed;
        const stream = try self.server.accept(self.io);
        const client_fd = stream.socket.handle;
        setTcpNoDelay(.{ .handle = client_fd });
        return .{ .handle = client_fd };
    }

    /// The `std.Io` this listener accepts on. A `ServerSession` built from
    /// this listener must use the same backend.
    pub fn ioBackend(self: *const Listener) std.Io {
        return self.io;
    }

    /// Close the listening socket. Idempotent.
    /// This also unblocks any thread blocked in `accept()`.
    pub fn close(self: *Listener) void {
        if (self.close_requested.swap(true, .acq_rel)) return;
        // Shut the socket down before closing: on POSIX a bare close does not
        // reliably wake a thread parked in accept() on this fd (the documented
        // contract of this method), while shutdown does.
        shutdownFd(self.io, .{ .handle = self.server.socket.handle });
        closeFd(self.io, .{ .handle = self.server.socket.handle });
    }

    /// Return the bound address. Useful for resolving ephemeral ports (port 0).
    pub fn getAddress(self: *const Listener) net.IpAddress {
        return self.server.socket.address;
    }

    /// Return the underlying socket handle.
    pub fn listenHandle(self: *const Listener) SocketFd {
        return .{ .handle = self.server.socket.handle };
    }

    /// Allocate and initialize a Connection. Uses errdefer to guarantee
    /// the heap allocation is freed if Connection.init fails.
    fn createConnection(self: *Listener, fd: net.Socket.Handle) !*Connection {
        const conn_ptr = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn_ptr);

        conn_ptr.* = try Connection.init(
            self.allocator,
            self.io,
            .{ .handle = fd },
            self.conn_options,
        );
        events.emitConnection(self.conn_options.observer, .tcp, .server, .accepted);
        return conn_ptr;
    }
};

/// Disable Nagle on a connected TCP socket. Loopback control channels and
/// RPC frames are latency-sensitive; with Nagle on, delayed ACKs can hold
/// small writes for ~40-200ms, which breaks tick/idle timing.
// Takes the SocketFd wrapper (not net.Socket.Handle): the raw handle type
// varies by target (i32 vs *anyopaque), which would break the
// platform-identical api-snapshot invariant for a pub decl.
pub fn setTcpNoDelay(socket: SocketFd) void {
    if (comptime builtin.target.os.tag == .windows) {
        // std's Windows sockets are raw AFD handles: ws2_32.setsockopt
        // rejects them, and std does not yet expose its internal AFD
        // socket-option helper. Nagle stays on for Windows until
        // upstream does (tracked in docs/windows-first-class-plan.md
        // phase 4).
        return;
    }
    std.posix.setsockopt(
        socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
        &std.mem.toBytes(@as(c_int, 1)),
    ) catch |err| {
        log.debug("failed to set TCP_NODELAY: {}", .{err});
    };
}

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

/// Close a socket via Io.
pub fn closeFd(io: std.Io, socket: SocketFd) void {
    // `netClose` takes `[]const net.Socket`, not raw handles. `address` is
    // never read on the close path, so an undefined one is correct here (the
    // same shape the transport already uses for handle-only close/shutdown).
    const sockets = [_]net.Socket{.{ .handle = socket.handle, .address = undefined }};
    io.vtable.netClose(io.userdata, &sockets);
}

/// Shut down a socket for both directions via Io, ignoring errors. On POSIX a
/// bare `close()` does not reliably unblock a thread parked in `accept()`/
/// `read()` on the fd; a prior `shutdown()` does.
/// Harmless on Windows.
pub fn shutdownFd(io: std.Io, socket: SocketFd) void {
    io.vtable.netShutdown(io.userdata, socket.handle, .both) catch {};
}

/// Create a pair of connected stream sockets over loopback TCP using
/// `std.Io`, portable to every platform. POSIX `socketpair(2)` does not
/// exist on Windows, so this is the cross-platform building block for the
/// connection wake channel and for tests that feed a `Connection` or
/// `Transport` raw bytes.
///
/// Returns `[2]SocketFd`; both ends are connected to each other and the
/// temporary listener is closed before returning.
pub fn createLoopbackSocketPair(io: std.Io) ![2]SocketFd {
    var server = try createListenSocket(io, .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, 1, false);
    defer closeFd(io, .{ .handle = server.socket.handle });

    var connect_addr = server.socket.address;
    const client_stream = try net.IpAddress.connect(&connect_addr, io, .{ .mode = .stream, .protocol = .tcp });
    errdefer closeFd(io, .{ .handle = client_stream.socket.handle });

    const accepted_stream = try server.accept(io);
    // Nagle + delayed ACK can hold sub-MSS writes for ~40-200ms, which is
    // longer than the tick/idle windows this pair exists to exercise.
    setTcpNoDelay(.{ .handle = client_stream.socket.handle });
    setTcpNoDelay(.{ .handle = accepted_stream.socket.handle });
    return .{
        .{ .handle = client_stream.socket.handle },
        .{ .handle = accepted_stream.socket.handle },
    };
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
    const addr: net.IpAddress = .{ .ip4 = .loopback(0) };
    const server = try createListenSocket(std.testing.io, addr, 128, false);

    var listener = Listener.initFd(std.testing.allocator, std.testing.io, .{ .handle = server.socket.handle }, .{});
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
    const fds = try createLoopbackSocketPair(std.testing.io);
    defer closeFd(std.testing.io, fds[0]);
    defer closeFd(std.testing.io, fds[1]);

    try std.testing.expectError(error.OutOfMemory, fail_listener.createConnection(fds[0].handle));
}

test "createConnection errdefer frees Connection when Transport.init fails" {
    const addr: net.IpAddress = .{ .ip4 = .loopback(0) };
    const server = try createListenSocket(std.testing.io, addr, 128, false);

    var listener = Listener.initFd(std.testing.allocator, std.testing.io, .{ .handle = server.socket.handle }, .{});
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

    const fds = try createLoopbackSocketPair(std.testing.io);
    defer closeFd(std.testing.io, fds[0]);
    defer closeFd(std.testing.io, fds[1]);

    try std.testing.expectError(error.OutOfMemory, fail_listener.createConnection(fds[0].handle));
}
