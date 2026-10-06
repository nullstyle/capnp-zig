const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_runtime);
const Connection = @import("./connection.zig").Connection;
const events = @import("../../events.zig");
const unix_socket_mod = @import("../unix/socket.zig");
const fd_io = @import("../unix/fd_io.zig");
const fd_passing_mod = @import("../fd_passing.zig");
const client_wiring = @import("./client_wiring.zig");
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
///
/// ## AF_UNIX
///
/// `rpc.transport.unix.listen` returns a `Listener` too (Experimental,
/// Linux and macOS with `-Dfd-passing` on). It accepts the same way;
/// `unixPath` returns its path, and `close` also removes its socket file
/// and releases its lock. Any AF_UNIX listener (this one, or `initFd` on an
/// AF_UNIX socket, such as one a service manager hands over) waits to
/// accept while the fd closer's
/// `.socket` lane is full (`awaitSocketLane`), and its final close runs on
/// that lane (see `close`).
pub const Listener = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    server: net.Server,
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    conn_options: Connection.Options,
    /// Set only by `rpc.transport.unix.listen`: the socket file this
    /// listener owns (path, identity, held lock). Internal state; read the
    /// path with `unixPath`. Experimental.
    unix_socket: ?unix_socket_mod.SocketFile = null,
    /// Fd passing on every connection this listener accepts
    /// (`rpc.transport.unix.ListenOptions.fd_passing`). Experimental; only
    /// an AF_UNIX listener may turn it on.
    fd_passing: fd_passing_mod.FdPassing = .{},
    /// True for an AF_UNIX listener where fd passing is compiled in (Linux
    /// and macOS, `-Dfd-passing` on): every listener from
    /// `rpc.transport.unix.listen`, and `initFd` on any socket `getsockname`
    /// reports as neither IPv4 nor IPv6 (`initFd` reads the family once).
    /// Its connections read in drain mode, and their socket closes can queue
    /// on the fd closer's `.socket` lane, so it accepts behind that lane's
    /// gate (`awaitSocketLane`), as does a `WorkerPool` serving it. Internal
    /// state. Experimental.
    non_ip_socket: bool = false,

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
    /// in test harnesses, or a service manager's socket activation).
    ///
    /// Where fd passing is compiled in (Linux and macOS, `-Dfd-passing` on)
    /// this reads the socket's family once (`getsockname`). An AF_UNIX
    /// socket (any family but IPv4 and IPv6) gets the accept gate of a
    /// `rpc.transport.unix.listen` listener (`awaitSocketLane`) and its
    /// off-thread final close (`close`). With `-Dfd-passing=false` on Linux
    /// or macOS, an AF_UNIX listener accepts, but every connection's first
    /// read fails (see "AF_UNIX sockets without fd passing" on
    /// `tcp.Transport`), and its final close runs inline.
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
            .non_ip_socket = if (comptime fd_io.supported) isNonIpSocket(socket) else false,
        };
    }

    /// Accept a single connection. Blocks until a client connects.
    /// Returns a heap-allocated Connection.
    ///
    /// An AF_UNIX listener (from `rpc.transport.unix.listen`, or `initFd` on
    /// an AF_UNIX socket) also waits while the fd closer's `.socket` lane is
    /// full (see `awaitSocketLane`).
    pub fn accept(self: *Listener) !*Connection {
        if (self.close_requested.load(.acquire)) return error.ListenerClosed;
        try self.awaitSocketCloses();

        const stream = try self.server.accept(self.io);
        const client_fd = stream.socket.handle;
        const conn = blk: {
            errdefer closeFd(self.io, .{ .handle = client_fd });
            setTcpNoDelay(.{ .handle = client_fd });
            break :blk try self.createConnection(client_fd);
        };
        // Before anything reads: fd passing starts at the stream's first
        // byte. The connection owns the socket now; its deinit closes it.
        client_wiring.enableFdPassing(conn, self.fd_passing) catch |err| {
            conn.deinit();
            self.allocator.destroy(conn);
            return err;
        };
        return conn;
    }

    /// Accept a single connection and return only its socket, with Nagle
    /// disabled when it is an IP socket — the caller owns wiring it into a
    /// `Connection`. Used by `ServerSession`, which embeds the `Connection`
    /// by value rather than taking the heap `*Connection` `accept()`
    /// produces.
    pub fn acceptFd(self: *Listener) !SocketFd {
        if (self.close_requested.load(.acquire)) return error.ListenerClosed;
        try self.awaitSocketCloses();
        const stream = try self.server.accept(self.io);
        const client_fd = stream.socket.handle;
        setTcpNoDelay(.{ .handle = client_fd });
        return .{ .handle = client_fd };
    }

    /// An AF_UNIX listener (`non_ip_socket`: one from
    /// `rpc.transport.unix.listen`, or `initFd` on an AF_UNIX socket) takes
    /// no connection while the fd closer's `.socket` lane is full
    /// (`awaitSocketLane`). Returns `error.ListenerClosed` once `close` is
    /// called.
    fn awaitSocketCloses(self: *Listener) error{ListenerClosed}!void {
        if (!self.non_ip_socket) return;
        return awaitSocketLane(self.conn_options.observer, &self.close_requested);
    }

    /// The `std.Io` this listener accepts on. A `ServerSession` built from
    /// this listener must use the same backend.
    pub fn ioBackend(self: *const Listener) std.Io {
        return self.io;
    }

    /// Close the listening socket. Idempotent.
    /// This also unblocks any thread blocked in `accept()`.
    ///
    /// A listener from `rpc.transport.unix.listen` first removes its socket
    /// file (only while the path still names that file) and releases its
    /// lock last, so no other `unix.listen` can bind the path in between.
    ///
    /// Where fd passing is compiled in (Linux and macOS, `-Dfd-passing` on)
    /// an AF_UNIX listener (one from `rpc.transport.unix.listen`, or any
    /// socket `getsockname` reports as neither IPv4 nor IPv6) never does its
    /// final close on the calling thread. The connections still in its
    /// accept queue are released inside that close, with any fds riding on
    /// their unread messages, and one of those closes can block. The final close runs on the fd closer's
    /// `.socket` lane instead (see `closeListenSocket`). This fd is still
    /// closed before `close` returns, so a thread parked in `accept` wakes at
    /// once, and the lock is released without waiting for that final close.
    pub fn close(self: *Listener) void {
        if (self.close_requested.swap(true, .acq_rel)) return;
        if (self.unix_socket) |*file| file.unlinkIfOurs();
        defer if (self.unix_socket) |*file| file.releaseLock();
        // POSIX: a bare close does not reliably wake a thread parked in
        // accept() on this fd (the documented contract of this method), while
        // shutdown does.
        //
        // WINDOWS: shutdown() on a LISTENING socket is invalid and fails with
        // INVALID_PARAMETER. std routes that through `unexpectedStatus`, whose
        // debug-mode diagnostic makes the test binary exit non-zero even
        // though the `catch {}` below swallows the error value — a real CI
        // failure with zero failing tests. `closesocket` already unblocks a
        // pending accept there, so the shutdown is not merely harmful, it is
        // unnecessary.
        if (comptime builtin.target.os.tag != .windows) {
            shutdownFd(self.io, .{ .handle = self.server.socket.handle });
        }
        closeListenSocket(self.io, .{ .handle = self.server.socket.handle }, if (self.unix_socket) |*file| file else null);
    }

    /// Return the bound address. Useful for resolving ephemeral ports (port 0).
    /// A listener from `rpc.transport.unix.listen` (or `initFd`) has no IP
    /// address: this returns 0.0.0.0:0 there; see `unixPath`.
    pub fn getAddress(self: *const Listener) net.IpAddress {
        return self.server.socket.address;
    }

    /// The socket file's path for a listener from
    /// `rpc.transport.unix.listen`; null for any other listener (TCP, or
    /// `initFd`). The slice points into this listener. Experimental.
    pub fn unixPath(self: *const Listener) ?[]const u8 {
        if (self.unix_socket) |*file| return file.path();
        return null;
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
        events.emitConnection(self.conn_options.observer, conn_ptr.transport.source, .server, .accepted);
        return conn_ptr;
    }
};

/// Disable Nagle on a connected TCP socket. Loopback control channels and
/// RPC frames are latency-sensitive; with Nagle on, delayed ACKs can hold
/// small writes for ~40-200ms, which breaks tick/idle timing.
///
/// Best-effort and never fails. It is a no-op on a socket that is not an
/// IP socket (for example an AF_UNIX socket handed to `Listener.initFd`),
/// because TCP_NODELAY does not exist there.
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
    // A Listener built with `initFd` may wrap an AF_UNIX socket, so the
    // accepted fd is not necessarily TCP. The attempt could only fail there
    // (EOPNOTSUPP), so skip it.
    if (isNonIpSocket(socket)) return;
    // Best-effort, via the raw syscall: the socket came off accept() on a
    // live network, so the peer can reset it between accept and here —
    // macOS then fails setsockopt with EINVAL (observed steadily in the
    // TCP soak at >=32 workers), and a torn-down fd fails with EBADF.
    // std.posix.setsockopt makes those arms `unreachable` ("always a race
    // condition"), which aborts the whole process, so it cannot be used on
    // an fd whose peer races us. Any failure here just means Nagle stays
    // on for a connection that is usually already dead.
    const opt = std.mem.toBytes(@as(c_int, 1));
    const rc = std.posix.system.setsockopt(
        socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
        &opt,
        opt.len,
    );
    switch (std.posix.errno(rc)) {
        .SUCCESS => {},
        // Log the raw number, never the tag name: `std.posix.E` is
        // non-exhaustive and does not name every errno a kernel returns
        // (macOS 102, EOPNOTSUPP, has no tag), and formatting an unnamed
        // value with `{t}` panics a Debug build.
        else => |err| log.debug("failed to set TCP_NODELAY: errno {d}", .{@backingInt(err)}),
    }
}

/// True only when `getsockname` reports a family other than IPv4 or IPv6.
/// When the family cannot be read (the call fails, or returns too few
/// bytes), this returns false: `setTcpNoDelay` makes its best-effort
/// attempt anyway, `Listener.initFd` treats the socket as IP (no accept
/// gate; `rpc.transport.unix.listen` sets its own listener's flag itself),
/// and `closeListenSocket` closes inline. The failure is never propagated:
/// `setTcpNoDelay` returns void, so `Listener.acceptFd`'s error set (which
/// feeds the frozen `ServerSession.accept`) stays as it is, and `initFd`
/// stays infallible.
fn isNonIpSocket(socket: SocketFd) bool {
    var addr: std.posix.sockaddr.storage = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.storage);
    const rc = std.posix.system.getsockname(socket.handle, @ptrCast(&addr), &len);
    if (std.posix.errno(rc) != .SUCCESS) return false;
    const family_end = @offsetOf(std.posix.sockaddr.storage, "family") + @sizeOf(std.posix.sa_family_t);
    if (len < family_end) return false;
    return switch (addr.family) {
        std.posix.AF.INET, std.posix.AF.INET6 => false,
        else => true,
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

/// The close of `Listener.close`. An IP listener (and every listener on
/// Windows, and every listener in a build without fd passing) closes
/// inline, as before.
///
/// Where fd passing is compiled in (Linux and macOS, `-Dfd-passing` on) an
/// AF_UNIX listener never does its final close on this thread: one from
/// `rpc.transport.unix.listen` (`unix_file` set), or any other socket
/// `getsockname` reports as neither IPv4 nor IPv6 (a `Listener.initFd` on
/// an AF_UNIX socket). The kernel releases the
/// connections still in a listener's accept queue inside the listener's
/// final close, on the closing thread, and closes the fds riding on their
/// unread messages there; `shutdown` disposes of none of them (measured with
/// a 3 s linger: close 3006 ms on Linux and 3001 ms on macOS, shutdown 0 ms
/// on both). A peer that connects, attaches a lingering TCP socket and is
/// never accepted would otherwise block this thread for the linger time
/// (without end on Linux).
///
/// So this takes a close-on-exec duplicate first, closes the listener's own
/// fd here, and hands the duplicate to the closer's `.socket` lane, which
/// does the final close. The close here drops a reference that is not the
/// last, so it cannot block, and on macOS it is what wakes a thread parked
/// in `accept` (shutdown does not): that wake never waits for the lane. If
/// the duplicate cannot be made (fd table full), the listener's own fd goes
/// to the lane instead, and on macOS a parked `accept` then wakes when the
/// lane gets to it.
///
/// A `unix.listen` listener hands off under the `.socket` slot `listen`
/// reserved (`SocketFile.close_reservation`), so this never allocates and
/// never falls back to an inline close. Any other AF_UNIX listener's
/// hand-off needs a queue allocation; if that fails, the closer runs the
/// close inline, with a warning (see `fd_io.closer`).
fn closeListenSocket(io: std.Io, socket: SocketFd, unix_file: ?*unix_socket_mod.SocketFile) void {
    if (comptime fd_io.supported) {
        if (unix_file != null or isNonIpSocket(socket)) {
            const reservation: ?*fd_io.closer.Reservation = if (unix_file) |file| &file.close_reservation else null;
            const duplicate = fd_io.dupCloexec(socket.handle) catch |err| {
                log.debug("duplicating a listening socket failed ({t}); the closer closes it", .{err});
                fd_io.closer.handOffSocketClose(reservation, socket.handle);
                return;
            };
            closeFd(io, socket);
            fd_io.closer.handOffSocketClose(reservation, duplicate);
            return;
        }
    }
    closeFd(io, socket);
}

/// How often an accept waiting in `awaitSocketLane` looks at its cancel
/// flag, at most.
const socket_lane_wait_ms: u32 = 50;

/// The accept gate of an AF_UNIX listener: one from
/// `rpc.transport.unix.listen`, or `Listener.initFd` on an AF_UNIX socket
/// (`Listener.accept`, `Listener.acceptFd`, and a `WorkerPool` serving
/// either through `initListener`). Experimental; where fd passing is
/// compiled in (Linux and macOS, `-Dfd-passing` on; elsewhere it returns
/// at once).
///
/// Returns at once while the fd closer's `.socket` lane holds fewer than
/// `fd_io.closer.socketLaneBound()` jobs; otherwise waits until it does.
/// Each job there holds one socket fd, and a close there blocks while that
/// socket still carries a peer's fd whose close blocks; every AF_UNIX close
/// queued behind it keeps its fd until then (macOS sends every AF_UNIX
/// close there, Linux every close with unread bytes). A peer that stalls one
/// close and then reconnects in a loop would otherwise fill the fd table.
/// Waiting leaves its connections in the kernel's backlog, outside this
/// process's fd table. Emits one `.backpressure` event per wait to
/// `observer` (`resource = .attached_fds`, `err =
/// error.SocketCloseQueueFull`, `attempted_bytes` = the lane's pending jobs,
/// `limit` = the bound). Returns `error.ListenerClosed` once `cancel` is
/// set, checking it at least every 50 ms.
pub fn awaitSocketLane(observer: ?events.Observer, cancel: *const std.atomic.Value(bool)) error{ListenerClosed}!void {
    if (comptime !fd_io.supported) return;
    if (!fd_io.closer.socketLaneFull()) return;
    const bound = fd_io.closer.socketLaneBound();
    log.warn("fd closer: {d} AF_UNIX socket job(s) pending (bound {d}); accept waits", .{ fd_io.closer.pendingIn(.socket), bound });
    events.emitBackpressure(observer, .unix, .server, .attached_fds, fd_io.closer.pendingIn(.socket), bound, error.SocketCloseQueueFull);
    while (fd_io.closer.socketLaneFull()) {
        if (cancel.load(.acquire)) return error.ListenerClosed;
        fd_io.closer.waitSocketLane(socket_lane_wait_ms);
    }
    if (cancel.load(.acquire)) return error.ListenerClosed;
}

/// Shut down a socket for both directions via Io, ignoring errors. On POSIX a
/// bare `close()` does not reliably unblock a thread parked in `accept()`/
/// `read()` on the fd; a prior `shutdown()` does.
///
/// Safe for CONNECTED sockets on Windows, but NOT for listening ones: see
/// `Listener.close`, which skips it there because Windows rejects shutdown on
/// a listening socket with INVALID_PARAMETER.
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
