//! `rpc.transport.unix.listen` and `rpc.transport.unix.connect` (sprint
//! item 7): Cap'n Proto RPC over a socket file.
//!
//! Every case runs in its own private directory (mode 0700) under /tmp, with
//! short names: `sun_path` is 104 bytes on Darwin, and concurrent lanes may
//! run this binary at the same time. Linux and macOS run the suite; every
//! other target compiles it, and only the stub test runs there.

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");
const support = @import("fd_test_support.zig");

const unix = capnpc.rpc.transport.unix;
const tcp = capnpc.rpc.transport.tcp;
const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const Peer = capnpc.rpc.peer.Peer;

const posix = support.posix;
const sys = support.sys;
const Fd = support.Fd;
const testing = std.testing;

const is_linux = support.is_linux;
const supported = support.supported;

/// `sun_path`'s length on this target (104 on Darwin, 108 on Linux).
const sun_path_len: usize = if (supported) @typeInfo(@FieldType(posix.sockaddr.un, "path")).array.len else 108;

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
        self.dir = try std.fmt.bufPrint(&self.buf, "/tmp/czu7-{d}-{d}", .{ sys.getpid(), n });
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

const FileInfo = struct { dev: u64, ino: u64, mode: u32 };

/// `lstat(path)`, or null when nothing is there.
fn lstatPath(path: []const u8) ?FileInfo {
    var z: [256]u8 = undefined;
    const path_z = nulTerminated(&z, path);
    if (is_linux) {
        const linux = std.os.linux;
        var stx: linux.Statx = undefined;
        const rc = linux.statx(linux.AT.FDCWD, path_z, linux.AT.SYMLINK_NOFOLLOW, .{ .TYPE = true, .MODE = true, .INO = true }, &stx);
        if (linux.errno(rc) != .SUCCESS) return null;
        return .{ .dev = (@as(u64, stx.dev_major) << 32) | stx.dev_minor, .ino = stx.ino, .mode = stx.mode };
    } else {
        var st: std.c.Stat = undefined;
        if (posix.errno(std.c.fstatat(std.c.AT.FDCWD, path_z, &st, std.c.AT.SYMLINK_NOFOLLOW)) != .SUCCESS) return null;
        return .{ .dev = @as(u32, @bitCast(st.dev)), .ino = st.ino, .mode = st.mode };
    }
}

fn isSocket(info: FileInfo) bool {
    return info.mode & 0o170000 == 0o140000;
}

fn sockaddrFor(path: []const u8) posix.sockaddr.un {
    var addr: posix.sockaddr.un = .{ .family = posix.AF.UNIX, .path = undefined };
    @memset(&addr.path, 0);
    @memcpy(addr.path[0..path.len], path);
    return addr;
}

fn rawSocket(nonblocking: bool) !Fd {
    const fd: Fd = @intCast(try support.check(sys.socket(posix.AF.UNIX, posix.SOCK.STREAM, 0), "socket"));
    if (nonblocking) try support.setNonBlocking(fd, true);
    return fd;
}

/// A socket file with no server behind it: what a server that died leaves.
fn makeStaleSocketFile(path: []const u8) !void {
    const fd = try rawSocket(false);
    defer support.closeFd(fd);
    var addr = sockaddrFor(path);
    _ = try support.check(sys.bind(fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.un)), "bind");
}

fn writeRegularFile(path: []const u8, contents: []const u8) !void {
    try std.Io.Dir.cwd().writeFile(testing.io, .{ .sub_path = path, .data = contents });
}

/// A raw client connect; returns the connected fd.
fn rawConnect(path: []const u8) !Fd {
    const fd = try rawSocket(false);
    errdefer support.closeFd(fd);
    var addr = sockaddrFor(path);
    _ = try support.check(sys.connect(fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.un)), "connect");
    return fd;
}

fn hasCloexec(fd: Fd) !bool {
    const rc = if (is_linux and !builtin.link_libc)
        sys.fcntl(fd, posix.F.GETFD, 0)
    else
        sys.fcntl(fd, posix.F.GETFD);
    return (try support.check(rc, "fcntl(F_GETFD)")) & posix.FD_CLOEXEC != 0;
}

/// A connected AF_UNIX client fd that the listener accepted: proves the
/// listener at `path` is alive and serving.
fn expectServes(listener: *tcp.Listener, path: []const u8) !void {
    const client = try rawConnect(path);
    defer support.closeFd(client);
    const accepted = try listener.acceptFd();
    tcp.closeFd(testing.io, accepted);
}

// ---------------------------------------------------------------------------
// RPC: an echo bootstrap, and a client that bootstraps and makes one call
// ---------------------------------------------------------------------------

const EchoServer = struct {
    fn onCall(_: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        try peer.sendReturnEmptyStruct(call.question_id);
    }
};

const ServerCtx = struct {
    listener: *tcp.Listener,
    handler_ctx: u8 = 0,
    accepted_source: ?capnpc.rpc.events.Source = null,
    err: ?anyerror = null,

    fn main(self: *ServerCtx) void {
        var session = tcp.ServerSession.accept(testing.allocator, self.listener, .{}) catch |err| {
            self.err = err;
            return;
        };
        defer session.deinit();
        self.accepted_source = session.conn.transport.source;
        _ = session.peer.setBootstrap(.{ .ctx = &self.handler_ctx, .on_call = EchoServer.onCall }) catch |err| {
            self.err = err;
            return;
        };
        session.run();
    }
};

/// Bootstrap, one call, then close. `close` is the session's own close
/// (ClientSession) or a peer-level close (a raw Peer).
const ClientApp = struct {
    peer: *Peer = undefined,
    calls_ok: usize = 0,
    call_returned: bool = false,
    failed: bool = false,

    fn close(self: *ClientApp) void {
        if (!self.peer.isAttachedTransportClosing()) self.peer.closeAttachedTransport();
    }

    fn onBootstrap(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ClientApp = @ptrCast(@alignCast(ctx));
        const payload = if (ret.tag == .results) ret.results else null;
        const cap = (payload orelse {
            self.failed = true;
            self.close();
            return;
        }).content.getCapability() catch {
            self.failed = true;
            self.close();
            return;
        };
        const resolved = try caps.resolveCapability(cap);
        const id = switch (resolved) {
            .imported => |imported| imported.id,
            else => {
                self.failed = true;
                self.close();
                return;
            },
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

// ---------------------------------------------------------------------------
// Bootstrap + one call
// ---------------------------------------------------------------------------

test "unix.listen + unix.connect: bootstrap and one call, both ends report Source.unix" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    const io = testing.io;

    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var listener = try unix.listen(gpa, io, path, .{});
    defer listener.close();
    try testing.expectEqualStrings(path, listener.unixPath().?);

    var server_ctx = ServerCtx{ .listener = &listener };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.main, .{&server_ctx});

    var app = ClientApp{};
    const session = unix.connect(gpa, io, path, .{}) catch |err| {
        listener.close();
        server_thread.join();
        return err;
    };
    defer session.deinit();
    app.peer = &session.peer;
    try testing.expectEqual(capnpc.rpc.events.Source.unix, session.conn.transport.source);

    _ = try session.peer.sendBootstrap(&app, ClientApp.onBootstrap);
    session.run();
    server_thread.join();

    try testing.expectEqual(@as(?anyerror, null), server_ctx.err);
    try testing.expectEqual(@as(?capnpc.rpc.events.Source, .unix), server_ctx.accepted_source);
    try testing.expect(!app.failed);
    try testing.expect(app.call_returned);
    try testing.expectEqual(@as(usize, 1), app.calls_ok);
}

test "Listener.accept on a unix.listen listener reports Source.unix in its accepted event" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;

    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var recorder: support.Recorder = .{};
    var listener = try unix.listen(gpa, testing.io, path, .{ .conn = .{ .observer = recorder.observer() } });
    defer listener.close();

    const client = try rawConnect(path);
    defer support.closeFd(client);
    const conn = try listener.accept();
    defer {
        conn.deinit();
        gpa.destroy(conn);
    }
    try testing.expectEqual(capnpc.rpc.events.Source.unix, conn.transport.source);
    try testing.expect(recorder.connection_count > 0);
    for (recorder.connection_sources[0..recorder.connection_count]) |source| {
        try testing.expectEqual(capnpc.rpc.events.Source.unix, source);
    }
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

test "a path as long as sun_path returns NameTooLong; one byte shorter binds and connects" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    const io = testing.io;

    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();

    // `<dir>/aaaa...`, exactly `len` bytes.
    var long_buf: [256]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&long_buf, "{s}/", .{dir.dir});
    @memset(long_buf[prefix.len..], 'a');

    const too_long = long_buf[0..sun_path_len];
    try testing.expectError(error.NameTooLong, unix.listen(gpa, io, too_long, .{}));
    try testing.expectError(error.NameTooLong, unix.connect(gpa, io, too_long, .{}));
    // Far past sun_path: the guard, not a buffer, must stop it.
    const far_too_long = long_buf[0..200];
    try testing.expectError(error.NameTooLong, unix.listen(gpa, io, far_too_long, .{}));
    try testing.expectError(error.NameTooLong, unix.connect(gpa, io, far_too_long, .{}));
    try testing.expect(lstatPath(too_long) == null);

    const longest = long_buf[0 .. sun_path_len - 1];
    var listener = try unix.listen(gpa, io, longest, .{});
    defer listener.close();
    try testing.expectEqualStrings(longest, listener.unixPath().?);
    try testing.expect(isSocket(lstatPath(longest).?));

    const session = try unix.connect(gpa, io, longest, .{});
    defer session.deinit();
    const accepted = try listener.acceptFd();
    tcp.closeFd(io, accepted);
}

test "abstract names, empty paths and paths with a NUL are refused" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    const io = testing.io;

    try testing.expectError(error.AbstractNameUnsupported, unix.listen(gpa, io, "\x00czu7-abstract", .{}));
    try testing.expectError(error.AbstractNameUnsupported, unix.connect(gpa, io, "\x00czu7-abstract", .{}));
    try testing.expectError(error.BadPathName, unix.listen(gpa, io, "", .{}));
    try testing.expectError(error.BadPathName, unix.connect(gpa, io, "", .{}));
    try testing.expectError(error.BadPathName, unix.listen(gpa, io, "/tmp/czu7\x00x", .{}));
    try testing.expectError(error.BadPathName, unix.connect(gpa, io, "/tmp/czu7\x00x", .{}));
}

// ---------------------------------------------------------------------------
// Stale files and the lock
// ---------------------------------------------------------------------------

test "a stale socket file with reclaim_stale off returns AddressInUse and stays" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    try makeStaleSocketFile(path);
    const before = lstatPath(path).?;
    try testing.expectError(error.AddressInUse, unix.listen(testing.allocator, testing.io, path, .{}));
    const after = lstatPath(path).?;
    try testing.expectEqual(before.ino, after.ino);
}

test "a stale socket file with reclaim_stale on is replaced and the listener serves" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    try makeStaleSocketFile(path);
    const stale = lstatPath(path).?;
    var listener = try unix.listen(testing.allocator, testing.io, path, .{ .reclaim_stale = true });
    defer listener.close();
    const fresh = lstatPath(path).?;
    try testing.expect(isSocket(fresh));
    try testing.expect(fresh.ino != stale.ino or fresh.dev != stale.dev);
    try expectServes(&listener, path);
}

test "reclaim_stale leaves a file that is not a socket alone" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    try writeRegularFile(path, "not a socket");
    try testing.expectError(error.AddressInUse, unix.listen(testing.allocator, testing.io, path, .{ .reclaim_stale = true }));
    var contents_buf: [32]u8 = undefined;
    const contents = try std.Io.Dir.cwd().readFile(testing.io, path, &contents_buf);
    try testing.expectEqualStrings("not a socket", contents);
}

test "a reclaimer against a live listener gets AddressInUse, and the live listener still serves" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var live = try unix.listen(testing.allocator, testing.io, path, .{});
    defer live.close();
    const live_file = lstatPath(path).?;

    try testing.expectError(error.AddressInUse, unix.listen(testing.allocator, testing.io, path, .{ .reclaim_stale = true }));
    try testing.expectError(error.AddressInUse, unix.listen(testing.allocator, testing.io, path, .{}));

    const now = lstatPath(path).?;
    try testing.expectEqual(live_file.dev, now.dev);
    try testing.expectEqual(live_file.ino, now.ino);
    try expectServes(&live, path);
}

const Racer = struct {
    path: []const u8,
    go: *std.atomic.Value(bool),
    delay_us: u64,
    result: ?(unix.ListenError!tcp.Listener) = null,

    fn run(self: *Racer) void {
        while (!self.go.load(.acquire)) std.atomic.spinLoopHint();
        if (self.delay_us != 0) {
            std.Io.sleep(testing.io, .fromNanoseconds(@intCast(self.delay_us * std.time.ns_per_us)), .awake) catch {};
        }
        self.result = unix.listen(testing.allocator, testing.io, self.path, .{ .reclaim_stale = true });
    }
};

test "racing reclaimers on a stale file: exactly one wins, every loser gets AddressInUse while it lives" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    const racer_count = 3;
    const rounds = 30;
    var round: usize = 0;
    while (round < rounds) : (round += 1) {
        try makeStaleSocketFile(path);
        var go = std.atomic.Value(bool).init(false);
        var racers: [racer_count]Racer = undefined;
        var threads: [racer_count]std.Thread = undefined;
        for (&racers, 0..) |*racer, i| {
            // Some rounds start everyone at once; others stagger the
            // racers, so a late one meets a listener that already finished.
            const delay_us: u64 = ((round + i) % 4) * 150;
            racer.* = .{ .path = path, .go = &go, .delay_us = delay_us };
            threads[i] = try std.Thread.spawn(.{}, Racer.run, .{racer});
        }
        go.store(true, .release);
        for (threads) |t| t.join();

        var winner: ?*tcp.Listener = null;
        var winners: usize = 0;
        var losers_in_use: usize = 0;
        for (&racers) |*racer| {
            if (racer.result.?) |*listener| {
                winners += 1;
                winner = listener;
            } else |err| {
                if (err == error.AddressInUse) losers_in_use += 1 else std.debug.print("round {d}: a racer failed with {s}\n", .{ round, @errorName(err) });
            }
        }
        defer for (&racers) |*racer| {
            if (racer.result.?) |*listener| listener.close() else |_| {}
        };
        try testing.expectEqual(@as(usize, 1), winners);
        try testing.expectEqual(@as(usize, racer_count - 1), losers_in_use);
        try expectServes(winner.?, path);
    }
}

test "close releases the lock: a later listen on the same path binds" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");
    var lock_buf: [96]u8 = undefined;
    const lock_path = dir.path(&lock_buf, "s.lock");

    var first = try unix.listen(testing.allocator, testing.io, path, .{});
    first.close();
    first.close(); // idempotent
    try testing.expect(lstatPath(path) == null);
    // The lock file stays: removing it would let two servers lock two files.
    try testing.expect(lstatPath(lock_path) != null);

    var second = try unix.listen(testing.allocator, testing.io, path, .{});
    defer second.close();
    try expectServes(&second, path);
}

// ---------------------------------------------------------------------------
// Permissions and flags
// ---------------------------------------------------------------------------

test "the socket file gets socket_mode (0600 by default) and bits outside 0o777 are refused" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    {
        var listener = try unix.listen(testing.allocator, testing.io, path, .{});
        defer listener.close();
        const info = lstatPath(path).?;
        try testing.expect(isSocket(info));
        try testing.expectEqual(@as(u32, 0o600), info.mode & 0o777);
    }
    {
        var listener = try unix.listen(testing.allocator, testing.io, path, .{ .socket_mode = 0o660 });
        defer listener.close();
        try testing.expectEqual(@as(u32, 0o660), lstatPath(path).?.mode & 0o777);
    }
    try testing.expectError(error.InvalidSocketMode, unix.listen(testing.allocator, testing.io, path, .{ .socket_mode = 0o4600 }));
    try testing.expect(lstatPath(path) == null);
}

test "the listening socket, its lock and the client socket are close-on-exec" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var listener = try unix.listen(testing.allocator, testing.io, path, .{});
    defer listener.close();
    try testing.expect(try hasCloexec(listener.listenHandle().handle));
    try testing.expect(try hasCloexec(listener.unix_socket.?.lock_fd));

    const session = try unix.connect(testing.allocator, testing.io, path, .{});
    defer session.deinit();
    try testing.expect(try hasCloexec(session.conn.transport.fd));
    const accepted = try listener.acceptFd();
    defer tcp.closeFd(testing.io, accepted);
    try testing.expect(try hasCloexec(accepted.handle));
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

test "close unlinks only its own inode: a file that replaced the path stays" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var listener = try unix.listen(testing.allocator, testing.io, path, .{});
    defer listener.close();
    // Someone else removes our file and puts their own at the path.
    var z: [128]u8 = undefined;
    _ = try support.check(sys.unlink(nulTerminated(&z, path)), "unlink");
    try makeStaleSocketFile(path);
    const theirs = lstatPath(path).?;

    listener.close();
    const after = lstatPath(path) orelse return error.ReplacementWasUnlinked;
    try testing.expectEqual(theirs.dev, after.dev);
    try testing.expectEqual(theirs.ino, after.ino);
}

test "close unlinks the socket file it created" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var listener = try unix.listen(testing.allocator, testing.io, path, .{});
    try testing.expect(lstatPath(path) != null);
    listener.close();
    try testing.expect(lstatPath(path) == null);
}

const Acceptor = struct {
    listener: *tcp.Listener,
    parked: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    result: ?anyerror = null,

    fn run(self: *Acceptor) void {
        self.parked.store(true, .release);
        if (self.listener.acceptFd()) |fd| {
            tcp.closeFd(testing.io, fd);
            self.result = error.UnexpectedConnection;
        } else |err| {
            self.result = err;
        }
        self.done.store(true, .release);
    }
};

test "Listener.close wakes a thread parked in accept" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var listener = try unix.listen(testing.allocator, testing.io, path, .{});
    defer listener.close();

    var acceptor = Acceptor{ .listener = &listener };
    const thread = try std.Thread.spawn(.{}, Acceptor.run, .{&acceptor});
    while (!acceptor.parked.load(.acquire)) std.atomic.spinLoopHint();
    // Give the thread time to enter accept(2): a close before that is a
    // different (racy) path that this test does not cover.
    support.sleepMs(300);
    try testing.expect(!acceptor.done.load(.acquire));

    const start = support.nowNs();
    listener.close();
    while (!acceptor.done.load(.acquire) and support.msSince(start) < 2000) support.sleepMs(10);
    const woke = acceptor.done.load(.acquire);
    if (!woke) {
        // Unblock it before failing: a connect would reach a live accept,
        // but the path is gone; nothing else can. Report and leak the thread.
        std.debug.print("accept did not wake within 2 s of close\n", .{});
        return error.AcceptNotWoken;
    }
    thread.join();
    // Linux wakes it through shutdown (EINVAL), macOS through close
    // (ECONNABORTED). ListenerClosed means the thread had not reached
    // accept(2) after 300 ms (a very slow host): close still stopped it.
    const woken_by: anyerror = if (is_linux) error.SocketNotListening else error.ConnectionAborted;
    const result = acceptor.result orelse return error.NoAcceptResult;
    if (result != woken_by and result != error.ListenerClosed) {
        std.debug.print("accept returned {s}\n", .{@errorName(result)});
        return error.UnexpectedAcceptResult;
    }
}

// ---------------------------------------------------------------------------
// Connect
// ---------------------------------------------------------------------------

test "connect errors: nothing at the path, a regular file, and a stale socket file" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var buf: [96]u8 = undefined;

    try testing.expectError(error.FileNotFound, unix.connect(testing.allocator, testing.io, dir.path(&buf, "missing"), .{}));
    const regular = dir.path(&buf, "file");
    try writeRegularFile(regular, "x");
    try testing.expectError(error.ConnectionRefused, unix.connect(testing.allocator, testing.io, regular, .{}));
    const stale = dir.path(&buf, "stale");
    try makeStaleSocketFile(stale);
    try testing.expectError(error.ConnectionRefused, unix.connect(testing.allocator, testing.io, stale, .{}));
}

/// Fill the listener's backlog with raw non-blocking connects that nobody
/// accepts. Returns how many it queued; their fds are in `out`.
fn fillBacklog(path: []const u8, out: []Fd) !usize {
    var addr = sockaddrFor(path);
    var n: usize = 0;
    while (n < out.len) {
        const fd = try rawSocket(true);
        const rc = sys.connect(fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.un));
        switch (posix.errno(rc)) {
            .SUCCESS => {
                out[n] = fd;
                n += 1;
            },
            // Linux: the backlog is full. macOS: refused when full.
            .AGAIN, .CONNREFUSED => {
                support.closeFd(fd);
                return n;
            },
            else => |err| {
                support.closeFd(fd);
                std.debug.print("backlog connect failed: errno {d}\n", .{@backingInt(err)});
                return error.SyscallFailed;
            },
        }
    }
    return error.BacklogNeverFilled;
}

const Connector = struct {
    path: []const u8,
    timeout_ms: ?u64,
    done: std.atomic.Value(bool) = .init(false),
    result: ?(unix.ConnectError!*tcp.ClientSession) = null,
    elapsed_ms: i64 = 0,

    fn run(self: *Connector) void {
        const start = support.nowNs();
        self.result = unix.connect(testing.allocator, testing.io, self.path, .{ .connect_timeout_ms = self.timeout_ms });
        self.elapsed_ms = support.msSince(start);
        self.done.store(true, .release);
    }

    /// Tear down a session the connector thread made. The session is
    /// thread-affine: this (test) thread adopts it first.
    fn finish(self: *Connector) void {
        if (self.result) |r| {
            if (r) |session| {
                session.peer.adoptOwnerThread();
                session.conn.adoptOwnerThread();
                session.deinit();
            } else |_| {}
        }
    }
};

/// Accept (and close) every queued connection until `connector` finishes:
/// the way out when a connect that should have timed out did not.
fn drainUntilDone(listener: *tcp.Listener, connector: *Connector, backlog: []const Fd) void {
    for (backlog) |fd| support.closeFd(fd);
    var i: usize = 0;
    while (!connector.done.load(.acquire) and i < 64) : (i += 1) {
        const fd = listener.acceptFd() catch return;
        tcp.closeFd(testing.io, fd);
    }
}

test "connect_timeout_ms: a full backlog times out on Linux and is refused on macOS" {
    if (comptime !supported) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var listener = try unix.listen(testing.allocator, testing.io, path, .{ .backlog = 1 });
    defer listener.close();
    var queued: [16]Fd = undefined;
    const n = try fillBacklog(path, &queued);
    var queued_open = true;
    defer if (queued_open) for (queued[0..n]) |fd| support.closeFd(fd);

    var connector = Connector{ .path = path, .timeout_ms = 300 };
    const thread = try std.Thread.spawn(.{}, Connector.run, .{&connector});
    const start = support.nowNs();
    while (!connector.done.load(.acquire) and support.msSince(start) < 3000) support.sleepMs(10);
    if (!connector.done.load(.acquire)) {
        queued_open = false;
        drainUntilDone(&listener, &connector, queued[0..n]);
        thread.join();
        connector.finish();
        std.debug.print("connect with connect_timeout_ms = 300 still waited after 3 s\n", .{});
        return error.ConnectIgnoredTimeout;
    }
    thread.join();
    defer connector.finish();

    if (is_linux) {
        try testing.expectError(error.Timeout, connector.result.?);
        try testing.expect(connector.elapsed_ms >= 250);
    } else {
        try testing.expectError(error.ConnectionRefused, connector.result.?);
    }
}

test "connect waits within connect_timeout_ms until the backlog has room (Linux)" {
    if (comptime !(supported and is_linux)) return error.SkipZigTest;
    var dir: TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    var listener = try unix.listen(testing.allocator, testing.io, path, .{ .backlog = 1 });
    defer listener.close();
    var queued: [16]Fd = undefined;
    const n = try fillBacklog(path, &queued);
    defer for (queued[0..n]) |fd| support.closeFd(fd);

    var connector = Connector{ .path = path, .timeout_ms = 5000 };
    const thread = try std.Thread.spawn(.{}, Connector.run, .{&connector});
    support.sleepMs(300);
    try testing.expect(!connector.done.load(.acquire));
    // One accept makes room.
    const accepted = try listener.acceptFd();
    defer tcp.closeFd(testing.io, accepted);
    thread.join();
    defer connector.finish();
    _ = try connector.result.?;
    try testing.expect(connector.elapsed_ms < 3000);
}

/// `SO_SNDTIMEO`'s value: two `long`s on Linux, libc's `timeval` on Darwin.
const SendTimeout = if (is_linux) extern struct { sec: c_long, usec: c_long } else posix.timeval;

fn setSendTimeoutRaw(fd: Fd, sec: i32) posix.E {
    const tv: SendTimeout = .{ .sec = sec, .usec = 0 };
    const bytes = std.mem.asBytes(&tv);
    return posix.errno(sys.setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDTIMEO, bytes, bytes.len));
}

fn sendTimeoutSec(fd: Fd) !i64 {
    var tv: SendTimeout = undefined;
    var len: posix.socklen_t = @sizeOf(SendTimeout);
    _ = try support.check(sys.getsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDTIMEO, @ptrCast(&tv), &len), "getsockopt(SO_SNDTIMEO)");
    return tv.sec;
}

test "after the connect, the send-timeout reset clears it on Linux and never fails on a socket the peer closed" {
    if (comptime !supported) return error.SkipZigTest;
    const internals = capnpc.rpc.testing.unix_socket;

    // A live socket. Linux: the reset clears the bound the connect set, so
    // no write through std.Io can see EAGAIN. macOS: connect never sets the
    // option (a full backlog refuses at once), and the reset leaves it alone.
    {
        const fds = try support.socketPair();
        defer support.closeFd(fds[0]);
        defer support.closeFd(fds[1]);
        try testing.expectEqual(posix.E.SUCCESS, setSendTimeoutRaw(fds[0], 5));
        try internals.finishConnect(fds[0], true);
        try testing.expectEqual(@as(i64, if (is_linux) 0 else 5), try sendTimeoutSec(fds[0]));
    }

    // A server that accepts and closes at once can shut the client socket
    // down between connect(2) and the reset. XNU then refuses every socket
    // option with EINVAL, and `connect` must not turn that into Unexpected.
    const fds = try support.socketPair();
    defer support.closeFd(fds[0]);
    support.closeFd(fds[1]);
    if (!is_linux) {
        // The premise: XNU refuses the option on this socket.
        try testing.expectEqual(posix.E.INVAL, setSendTimeoutRaw(fds[0], 0));
    }
    try internals.finishConnect(fds[0], true);
    try internals.clearSendTimeout(fds[0]);
}

// ---------------------------------------------------------------------------
// Peers over socketpair(AF_UNIX)
// ---------------------------------------------------------------------------

const PairServer = struct {
    conn: *tcp.Connection,
    peer: *Peer,

    fn main(self: *PairServer) void {
        self.peer.adoptOwnerThread();
        self.conn.adoptOwnerThread();
        self.peer.start(null, null, null);
        self.conn.run();
    }
};

test "Peers over socketpair(AF_UNIX): bootstrap and one call, Source.unix on both ends" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = testing.allocator;
    const io = testing.io;

    const fds = try support.socketPair();
    var server_conn = tcp.Connection.init(gpa, io, .{ .handle = fds[0] }, .{}) catch |err| {
        support.closeFd(fds[0]);
        support.closeFd(fds[1]);
        return err;
    };
    defer server_conn.deinit();
    var client_conn = tcp.Connection.init(gpa, io, .{ .handle = fds[1] }, .{}) catch |err| {
        support.closeFd(fds[1]);
        return err;
    };
    defer client_conn.deinit();
    try testing.expectEqual(capnpc.rpc.events.Source.unix, server_conn.transport.source);
    try testing.expectEqual(capnpc.rpc.events.Source.unix, client_conn.transport.source);

    var server_peer = Peer.init(gpa, &server_conn);
    var handler_ctx: u8 = 0;
    _ = try server_peer.setBootstrap(.{ .ctx = &handler_ctx, .on_call = EchoServer.onCall });
    var client_peer = Peer.init(gpa, &client_conn);

    var server = PairServer{ .conn = &server_conn, .peer = &server_peer };
    const server_thread = try std.Thread.spawn(.{}, PairServer.main, .{&server});

    var app = ClientApp{ .peer = &client_peer };
    client_peer.start(null, null, null);
    _ = try client_peer.sendBootstrap(&app, ClientApp.onBootstrap);
    client_conn.run();
    server_thread.join();

    // The server side ran on its thread; tear it down here.
    server_peer.adoptOwnerThread();
    server_conn.adoptOwnerThread();
    _ = server_peer.takeAttachedConnection(*tcp.Connection);
    server_peer.deinit();
    _ = client_peer.takeAttachedConnection(*tcp.Connection);
    client_peer.deinit();

    try testing.expect(!app.failed);
    try testing.expect(app.call_returned);
    try testing.expectEqual(@as(usize, 1), app.calls_ok);
}

// ---------------------------------------------------------------------------
// Targets without AF_UNIX support
// ---------------------------------------------------------------------------

test "listen and connect return UnixSocketsUnsupported where AF_UNIX is not supported" {
    if (comptime unix.supported) return error.SkipZigTest;
    try testing.expectError(error.UnixSocketsUnsupported, unix.listen(testing.allocator, testing.io, "/tmp/czu7-x", .{}));
    try testing.expectError(error.UnixSocketsUnsupported, unix.connect(testing.allocator, testing.io, "/tmp/czu7-x", .{}));
}
