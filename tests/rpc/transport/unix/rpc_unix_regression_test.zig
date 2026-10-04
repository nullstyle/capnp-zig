const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const tcp = capnpc.rpc.transport.tcp;
const net = std.Io.net;
const posix = std.posix;
const sys = posix.system;

// Regression: accepting on an AF_UNIX listener panicked a Debug build on
// macOS.
//
// `Listener.accept`/`acceptFd` always called `setTcpNoDelay`. On an AF_UNIX
// socket, setsockopt(IPPROTO_TCP, TCP_NODELAY) fails, and on macOS the errno
// is 102 (EOPNOTSUPP). std's darwin `E` enum has no tag for 102, so the
// `log.debug("... {t}", .{err})` that reported the failure hit
// `@tagName` on an unnamed enum value: "panic: invalid enum value".
//
// The format only runs when the debug log level is enabled. The test runner
// resets `std.testing.log_level` to `.warn` before EVERY test, so each test
// below raises it to `.debug` inside its own body; without that, the bug is
// invisible to the suite.
//
// The fix skips TCP_NODELAY on any socket that is not IPv4 or IPv6. The
// second half of this file pins the other side of that skip: every TCP path
// still sets the option.
//
// Unix-domain sockets are POSIX-only in this sprint; Windows skips.

const unix_supported = builtin.target.os.tag != .windows and builtin.target.os.tag != .wasi;

var path_counter: std.atomic.Value(u32) = .init(0);

/// A Unix listener built the way a caller that owns its socket does it:
/// bind+listen outside the library, then hand the fd to `Listener.initFd`.
const UnixListenerFixture = struct {
    path_buf: [96]u8 = undefined,
    path: []const u8 = &.{},
    listener: tcp.Listener = undefined,

    fn init(self: *UnixListenerFixture, gpa: std.mem.Allocator, io: std.Io) !void {
        // Short and unique per process and per test: sun_path is 104 bytes on
        // Darwin, and concurrent lanes may run this binary at the same time.
        const n = path_counter.fetchAdd(1, .monotonic);
        self.path = try std.fmt.bufPrint(&self.path_buf, "/tmp/capnpzig-r4-{d}-{d}.sock", .{ sys.getpid(), n });
        std.Io.Dir.cwd().deleteFile(io, self.path) catch {};
        const address = try net.UnixAddress.init(self.path);
        const server = try address.listen(io, .{ .kernel_backlog = 4 });
        self.listener = tcp.Listener.initFd(gpa, io, .{ .handle = server.socket.handle }, .{});
    }

    /// Queue one client connection. The kernel completes it against the
    /// backlog, so the test can accept on the same thread afterwards.
    fn connectClient(self: *const UnixListenerFixture, io: std.Io) !net.Stream {
        const address = try net.UnixAddress.init(self.path);
        return address.connect(io);
    }

    fn deinit(self: *UnixListenerFixture, io: std.Io) void {
        self.listener.close();
        std.Io.Dir.cwd().deleteFile(io, self.path) catch {};
    }
};

test "ServerSession.accept on a Unix listener built with Listener.initFd does not panic at debug log level" {
    if (comptime !unix_supported) return error.SkipZigTest;
    std.testing.log_level = .debug;
    defer std.testing.log_level = .warn;

    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var fixture: UnixListenerFixture = .{};
    try fixture.init(gpa, io);
    defer fixture.deinit(io);

    const client = try fixture.connectClient(io);
    defer tcp.closeFd(io, .{ .handle = client.socket.handle });

    // ServerSession.accept -> Listener.acceptFd -> setTcpNoDelay: the frozen
    // Stable entry point that panicked.
    var session = try tcp.ServerSession.accept(gpa, &fixture.listener, .{});
    defer session.deinit();
}

test "Listener.accept on a Unix listener built with Listener.initFd does not panic at debug log level" {
    if (comptime !unix_supported) return error.SkipZigTest;
    std.testing.log_level = .debug;
    defer std.testing.log_level = .warn;

    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var fixture: UnixListenerFixture = .{};
    try fixture.init(gpa, io);
    defer fixture.deinit(io);

    const client = try fixture.connectClient(io);
    defer tcp.closeFd(io, .{ .handle = client.socket.handle });

    const conn = try fixture.listener.accept();
    defer {
        conn.deinit();
        gpa.destroy(conn);
    }
}

test "a Unix accept never attempts TCP_NODELAY (nothing is logged for it at debug level)" {
    if (comptime !unix_supported) return error.SkipZigTest;
    std.testing.log_level = .debug;
    defer std.testing.log_level = .warn;

    const gpa = std.testing.allocator;
    const io = std.testing.io;

    var fixture: UnixListenerFixture = .{};
    try fixture.init(gpa, io);
    defer fixture.deinit(io);

    const client = try fixture.connectClient(io);
    defer tcp.closeFd(io, .{ .handle = client.socket.handle });

    // TCP_NODELAY is an IP-socket option. On an AF_UNIX socket the attempt
    // can only fail (EOPNOTSUPP: 102 on macOS, 95 on Linux), and the only
    // trace it leaves is the debug log line. So capture stderr around the
    // accept and require that no such line appears: this is what proves the
    // non-IP skip, on Linux as well as on macOS.
    var capture = try StderrCapture.begin();
    const accepted = fixture.listener.acceptFd();
    var captured_buf: [4096]u8 = undefined;
    const captured = capture.end(&captured_buf);

    const fd = try accepted;
    defer tcp.closeFd(io, fd);

    if (std.mem.indexOf(u8, captured, "TCP_NODELAY") != null) {
        std.debug.print("unexpected TCP_NODELAY attempt on an AF_UNIX socket; captured stderr:\n{s}\n", .{captured});
        return error.TcpNoDelayAttemptedOnUnixSocket;
    }
}

// ---------------------------------------------------------------------------
// The other side of the skip: IP sockets must still get TCP_NODELAY.
//
// The non-IP skip sits on every TCP accept and connect path
// (`ClientSession.connect`, `Listener.accept`, `Listener.acceptFd`,
// `createLoopbackSocketPair`). If it ever misread the family, Nagle would
// come back on every TCP connection and nothing else would notice: data
// still flows, only small writes get held for a delayed ACK. The sockaddr
// layouts differ (Darwin: a length byte, then an 8-bit family; Linux: a
// 16-bit family), and a later edit could narrow the check to IPv4. So read
// the option back with getsockopt on every path, for IPv4 and IPv6.
//
// setTcpNoDelay is a no-op on Windows (raw AFD handles), so these skip there.
// ---------------------------------------------------------------------------

const nodelay_supported = builtin.target.os.tag != .windows and builtin.target.os.tag != .wasi;

fn expectNoDelay(socket: tcp.SocketFd) !void {
    var value: c_int = 0;
    var len: posix.socklen_t = @sizeOf(c_int);
    const rc = sys.getsockopt(socket.handle, posix.IPPROTO.TCP, posix.TCP.NODELAY, @ptrCast(&value), &len);
    if (posix.errno(rc) != .SUCCESS) return error.GetSockOptFailed;
    if (value == 0) return error.TcpNoDelayNotSet;
}

/// Open a TCP listener on `bind`, then check TCP_NODELAY on the client fd
/// from `ClientSession.connect` and on the server fds from
/// `Listener.acceptFd` (the `ServerSession.accept` path) and
/// `Listener.accept`.
fn expectNoDelayOnEveryTcpPath(gpa: std.mem.Allocator, io: std.Io, bind: net.IpAddress) !void {
    var listener = try tcp.Listener.init(gpa, io, bind, .{});
    defer listener.close();
    const address = listener.server.socket.address;

    // The kernel completes both connects against the backlog, so one thread
    // can connect and then accept. Accepts come out in connect order.
    const session = try tcp.ClientSession.connect(gpa, io, address, .{});
    defer session.deinit();
    const raw_client = try net.IpAddress.connect(&address, io, .{ .mode = .stream, .protocol = .tcp });
    defer tcp.closeFd(io, .{ .handle = raw_client.socket.handle });

    try expectNoDelay(.{ .handle = session.conn.transport.fd });

    const accepted_fd = try listener.acceptFd();
    defer tcp.closeFd(io, accepted_fd);
    try expectNoDelay(accepted_fd);

    const conn = try listener.accept();
    defer {
        conn.deinit();
        gpa.destroy(conn);
    }
    try expectNoDelay(.{ .handle = conn.transport.fd });
}

test "IPv4: connect, acceptFd and accept still set TCP_NODELAY (the non-IP skip must not fire)" {
    if (comptime !nodelay_supported) return error.SkipZigTest;
    try expectNoDelayOnEveryTcpPath(std.testing.allocator, std.testing.io, .{ .ip4 = .loopback(0) });
}

test "IPv6: connect, acceptFd and accept still set TCP_NODELAY (the non-IP skip must not fire)" {
    if (comptime !nodelay_supported) return error.SkipZigTest;
    // Probe first, so that a host without IPv6 loopback (some containers)
    // skips instead of failing. The real check below reports every error.
    var probe = tcp.Listener.init(std.testing.allocator, std.testing.io, .{ .ip6 = .loopback(0) }, .{}) catch |err| switch (err) {
        error.AddressFamilyUnsupported, error.AddressUnavailable => return error.SkipZigTest,
        else => return err,
    };
    probe.close();
    try expectNoDelayOnEveryTcpPath(std.testing.allocator, std.testing.io, .{ .ip6 = .loopback(0) });
}

test "createLoopbackSocketPair still sets TCP_NODELAY on both ends (the tick/idle suites rely on it)" {
    if (comptime !nodelay_supported) return error.SkipZigTest;
    const io = std.testing.io;
    const pair = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, pair[0]);
    defer tcp.closeFd(io, pair[1]);
    try expectNoDelay(pair[0]);
    try expectNoDelay(pair[1]);
}

/// Redirects fd 2 into a pipe so a test can read what the log function
/// printed. Only for short windows: nothing drains the pipe until `end`, so
/// the captured text must stay far below the pipe's capacity.
const StderrCapture = struct {
    saved_stderr: posix.fd_t,
    read_end: posix.fd_t,

    fn begin() !StderrCapture {
        var fds: [2]posix.fd_t = undefined;
        try check(sys.pipe(&fds));
        errdefer {
            _ = sys.close(fds[0]);
            _ = sys.close(fds[1]);
        }
        const saved_rc = sys.dup(posix.STDERR_FILENO);
        try check(saved_rc);
        const saved: posix.fd_t = @intCast(saved_rc);
        errdefer _ = sys.close(saved);
        try check(sys.dup2(fds[1], posix.STDERR_FILENO));
        // fd 2 now holds the only write end.
        _ = sys.close(fds[1]);
        return .{ .saved_stderr = saved, .read_end = fds[0] };
    }

    /// Restore fd 2, then return everything written to it since `begin`
    /// (truncated to `buf.len`).
    fn end(self: *StderrCapture, buf: []u8) []const u8 {
        // Replacing fd 2 closes the pipe's last write end, so the reads
        // below reach EOF once the captured bytes are drained.
        _ = sys.dup2(self.saved_stderr, posix.STDERR_FILENO);
        _ = sys.close(self.saved_stderr);
        defer _ = sys.close(self.read_end);
        var len: usize = 0;
        while (len < buf.len) {
            const rc = sys.read(self.read_end, buf[len..].ptr, buf.len - len);
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    if (rc == 0) break;
                    len += @intCast(rc);
                },
                .INTR => continue,
                else => break,
            }
        }
        return buf[0..len];
    }

    fn check(rc: anytype) !void {
        if (posix.errno(rc) != .SUCCESS) return error.StderrCaptureFailed;
    }
};
