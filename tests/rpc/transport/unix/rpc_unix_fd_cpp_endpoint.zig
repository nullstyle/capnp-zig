//! The Zig half of the C++ fd-passing e2e (`rpc_unix_fd_cpp_test.zig`,
//! sprint item 15). The C++ driver (`rpc_unix_fd_cpp_driver.cpp`) forks
//! and execs this program with one end of an AF_UNIX socketpair. It runs
//! one Cap'n Proto connection over that socket through the real
//! `tcp.Connection` and `Peer`, with fd passing on
//! (`Connection.enableFdPassing`), as the server or the client of
//! `rpc_fd.capnp`'s `FdTest.writeToFd`.
//!
//! argv: <socket fd> <server|client> <max fds per message>
//!       <second fd expected: 0|1> <fill size>...
//!
//! Each fill size is one `writeToFd` call on the same connection. A
//! capability that carries an fd here owns it, as the reference's
//! `TestFdCap` does: the fd closes when the remote releases the
//! capability. The process exits 0 only if every check passed and, after
//! teardown, its fd table is back where it started (minus the socket),
//! the process fd budget counts nothing, and the allocator has no leaks.
//!
//! The test writes this file into a temporary directory next to
//! `generated.zig` (the `rpc_fd.capnp` binding) and builds it there; the
//! build does not compile it.

const std = @import("std");
const capnp = @import("capnpc-zig");
const g = @import("generated.zig");

const rpc = capnp.rpc;
const Peer = rpc.peer.Peer;
const tcp = rpc.transport.tcp;
const fd_io = rpc.transport.unix.fd_io;
const InboundCapTable = rpc.caps.table.InboundCapTable;

const posix = std.posix;
const sys = posix.system;
const Fd = posix.fd_t;

/// Warnings and errors only: a Debug build logs every frame otherwise, and
/// the driver's output is what a failing test prints.
pub const std_options: std.Options = .{ .log_level = .warn };

/// The most `writeToFd` calls one run makes.
const max_calls = 8;
/// How long a pipe check waits for data and EOF. The copies that must close
/// first are closed by the remote and by this process's closer thread.
const pipe_wait_ms: i32 = 10_000;

/// The fill byte at `i`. Both implementations write and check it, so a
/// message that took many reads must arrive intact.
fn fillByte(i: usize) u8 {
    return @intCast(i % 251);
}

fn ival(rc: anytype) isize {
    return switch (@typeInfo(@TypeOf(rc)).int.signedness) {
        .signed => @intCast(rc),
        .unsigned => @bitCast(rc),
    };
}

fn check(rc: anytype, what: []const u8) error{SyscallFailed}!usize {
    const err = posix.errno(rc);
    if (err != .SUCCESS) {
        std.debug.print("endpoint: {s} failed with errno {d}\n", .{ what, @backingInt(err) });
        return error.SyscallFailed;
    }
    return @intCast(ival(rc));
}

fn closeFd(fd: Fd) void {
    _ = sys.close(fd);
}

fn isOpen(fd: Fd) bool {
    return posix.errno(sys.fcntl(fd, posix.F.GETFD)) == .SUCCESS;
}

/// A close-on-exec pipe: `.{ read_end, write_end }`.
fn openPipe() ![2]Fd {
    var fds: [2]Fd = undefined;
    _ = try check(sys.pipe(&fds), "pipe");
    for (fds) |fd| _ = sys.fcntl(fd, posix.F.SETFD, @as(c_int, posix.FD_CLOEXEC));
    return fds;
}

fn writeAll(fd: Fd, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) off += try check(sys.write(fd, bytes[off..].ptr, bytes.len - off), "write");
}

/// Everything `fd` gives until EOF, within `pipe_wait_ms`. EOF needs every
/// copy of the pipe's write end closed, in both processes.
fn readToEof(fd: Fd, buf: []u8) ![]const u8 {
    var len: usize = 0;
    var waited_ms: i32 = 0;
    const step_ms: i32 = 20;
    while (true) {
        var pfd = [1]posix.pollfd{.{ .fd = fd, .events = posix.POLL.IN, .revents = 0 }};
        if (try check(sys.poll(&pfd, 1, step_ms), "poll") == 0) {
            waited_ms += step_ms;
            if (waited_ms >= pipe_wait_ms) {
                std.debug.print("endpoint: no EOF within {d} ms; read so far: \"{s}\"\n", .{ pipe_wait_ms, buf[0..len] });
                return error.WriteEndStillOpen;
            }
            continue;
        }
        if (len == buf.len) return error.UnexpectedPipeData;
        const n = try check(sys.read(fd, buf[len..].ptr, buf.len - len), "read");
        if (n == 0) return buf[0..len];
        len += n;
    }
}

fn expectPipe(fd: Fd, expected: []const u8, what: []const u8) !void {
    var buf: [16]u8 = undefined;
    const got = try readToEof(fd, &buf);
    if (!std.mem.eql(u8, got, expected)) {
        std.debug.print("endpoint: {s} read \"{s}\", expected \"{s}\"\n", .{ what, got, expected });
        return error.UnexpectedPipeData;
    }
}

/// The open fds below `scanned`. Fds are allocated lowest first, and this
/// process holds a few dozen at most.
const scanned = 1024;
const FdSet = std.StaticBitSet(scanned);

fn openFds() FdSet {
    var set: FdSet = .empty;
    for (0..scanned) |fd| {
        if (isOpen(@intCast(fd))) set.set(fd);
    }
    return set;
}

fn sleepMs(ms: i32) void {
    // A poll on no fds: a plain wait that needs no Io instance.
    var none: [1]posix.pollfd = undefined;
    _ = sys.poll(&none, 0, ms);
}

/// After teardown: within `pipe_wait_ms` (the closer thread closes the
/// received fds), the fd table is `before` without `socket`, and the
/// process fd budget counts nothing.
fn expectBackAtBaseline(before: FdSet, socket: Fd) !void {
    var want = before;
    want.unset(@intCast(socket));
    var waited_ms: i32 = 0;
    while (true) {
        const now = openFds();
        if (now.eql(want) and fd_io.closer.pending() == 0 and fd_io.budget.inUse() == 0) return;
        if (waited_ms >= pipe_wait_ms) {
            const diff = now.xorWith(want);
            var it = diff.iterator(.{});
            while (it.next()) |fd| {
                std.debug.print("endpoint: fd {d} is {s}\n", .{ fd, if (now.isSet(fd)) "still open" else "closed, but not by us" });
            }
            std.debug.print("endpoint: closer pending {d}, fd budget in use {d}\n", .{ fd_io.closer.pending(), fd_io.budget.inUse() });
            return error.FdTableNotAtBaseline;
        }
        sleepMs(10);
        waited_ms += 10;
    }
}

/// The import id behind a capability pointer of an inbound message.
fn importId(caps: *const InboundCapTable, cap: capnp.message.Capability) !u32 {
    return switch (try caps.resolveCapability(cap)) {
        .imported => |imported| imported.id,
        else => error.NotAnImport,
    };
}

/// An `FdCap` export that owns `fd`, like the reference's `TestFdCap`: the
/// export carries it (`Peer.setExportFd`), and it closes when the export
/// goes away (the remote released it, or `Peer.deinit`).
const OwnedFdCap = struct {
    fd: Fd,
    owner: *Releases,

    /// Counts the `OwnedFdCap`s that went away, and wakes the connection
    /// loop (when it has a wake channel) so the client looks at the count
    /// outside the peer's release path.
    const Releases = struct {
        count: usize = 0,
        conn: ?*tcp.Connection = null,
    };

    fn onCall(_: *anyopaque, peer: *Peer, call: rpc.wire.protocol.Call, _: *const InboundCapTable) anyerror!void {
        try peer.sendReturnException(call.question_id, "FdCap has no methods");
    }

    fn deinit(allocator: std.mem.Allocator, ctx: *anyopaque) void {
        const self: *OwnedFdCap = @ptrCast(@alignCast(ctx));
        closeFd(self.fd);
        self.owner.count += 1;
        if (self.owner.conn) |conn| conn.wake();
        allocator.destroy(self);
    }

    /// Export a new `FdCap` that owns `fd` (closed here on failure).
    fn create(peer: *Peer, fd: Fd, owner: *Releases) !u32 {
        const self = peer.allocator.create(OwnedFdCap) catch |err| {
            closeFd(fd);
            return err;
        };
        self.* = .{ .fd = fd, .owner = owner };
        const id = peer.addExportWithDeinit(.{ .ctx = self, .on_call = onCall }, deinit) catch |err| {
            closeFd(fd);
            peer.allocator.destroy(self);
            return err;
        };
        // On failure the export still owns the fd: Peer.deinit closes it.
        try peer.setExportFd(id, .{ .fd = fd });
        return id;
    }
};

const Config = struct {
    fills: []const u32,
    expect_second: bool,
};

/// The `FdTest` server, like the reference's `TestMoreStuffImpl.writeToFd`.
const Server = struct {
    config: Config,
    calls: usize = 0,
    releases: OwnedFdCap.Releases = .{},
    failed: ?anyerror = null,

    fn writeToFd(
        ctx: *anyopaque,
        peer: *Peer,
        params: g.FdTest.WriteToFd.Params.Reader,
        results: *g.FdTest.WriteToFd.Results.Builder,
        caps: *const InboundCapTable,
    ) anyerror!void {
        const self: *Server = @ptrCast(@alignCast(ctx));
        self.handle(peer, params, results, caps) catch |err| {
            std.debug.print("endpoint: server call {d} failed: {t}\n", .{ self.calls, err });
            if (self.failed == null) self.failed = err;
            return err;
        };
    }

    fn handle(
        self: *Server,
        peer: *Peer,
        params: g.FdTest.WriteToFd.Params.Reader,
        results: *g.FdTest.WriteToFd.Results.Builder,
        caps: *const InboundCapTable,
    ) !void {
        if (self.calls >= self.config.fills.len) return error.UnexpectedCall;
        const fill = try (try params.getFill()).slice();
        if (fill.len != self.config.fills[self.calls]) return error.FillSizeMismatch;
        for (fill, 0..) |byte, i| if (byte != fillByte(i)) return error.FillCorrupted;
        self.calls += 1;

        // The first fd must be there (the reference asserts it).
        const fd1 = peer.importFd(try importId(caps, try params.getFdCap1())) orelse return error.FirstFdMissing;
        try writeAll(fd1.fd, "foo");
        const fd2 = peer.importFd(try importId(caps, try params.getFdCap2()));
        try results.setSecondFdPresent(fd2 != null);
        if (fd2) |h| try writeAll(h.fd, "bar");

        // A pipe with "baz" in it and its write end closed: the caller
        // reads to EOF through the read end, which `fdCap3` carries.
        const pipe = try openPipe();
        writeAll(pipe[1], "baz") catch |err| {
            closeFd(pipe[0]);
            closeFd(pipe[1]);
            return err;
        };
        closeFd(pipe[1]);
        const id = try OwnedFdCap.create(peer, pipe[0], &self.releases);
        try results.setFdCap3Capability(.{ .id = id });
    }
};

/// The `FdTest` client, like the reference's "send FD over RPC" and "FD per
/// message limit" tests: for each fill size, two pipes, their write ends on
/// `fdCap1` and `fdCap2` (reversed, as there), one call.
const Client = struct {
    config: Config,
    peer: *Peer = undefined,
    client: ?g.FdTest.Client = null,
    /// The pipe read ends of each call: `in1` (its write end rides on
    /// `fdCap2`) and `in2` (on `fdCap1`).
    in1: [max_calls]Fd = @splat(-1),
    in2: [max_calls]Fd = @splat(-1),
    started: usize = 0,
    returned: usize = 0,
    releases: OwnedFdCap.Releases = .{},
    checked: bool = false,
    failed: ?anyerror = null,
    /// The call being built (`build`).
    fill_size: u32 = 0,
    cap1: u32 = 0,
    cap2: u32 = 0,

    fn fail(self: *Client, err: anyerror) void {
        std.debug.print("endpoint: client failed: {t}\n", .{err});
        if (self.failed == null) self.failed = err;
        if (!self.peer.isAttachedTransportClosing()) self.peer.closeAttachedTransport();
    }

    fn onBootstrap(ctx: *anyopaque, _: *Peer, response: g.FdTest.BootstrapResponse) anyerror!void {
        const self: *Client = @ptrCast(@alignCast(ctx));
        self.client = response.unwrap() catch |err| return self.fail(err);
        self.startCall() catch |err| return self.fail(err);
    }

    fn startCall(self: *Client) !void {
        const i = self.started;
        const p1 = try openPipe();
        self.in1[i] = p1[0];
        const p2 = openPipe() catch |err| {
            closeFd(p1[1]);
            return err;
        };
        self.in2[i] = p2[0];
        self.started += 1;
        // Order reversal intentional, as in the reference.
        self.cap1 = OwnedFdCap.create(self.peer, p2[1], &self.releases) catch |err| {
            closeFd(p1[1]);
            return err;
        };
        self.cap2 = try OwnedFdCap.create(self.peer, p1[1], &self.releases);
        self.fill_size = self.config.fills[i];
        _ = try self.client.?.callWriteToFd(self, build, onReturn);
    }

    fn build(ctx: *anyopaque, params: *g.FdTest.WriteToFd.Params.Builder) anyerror!void {
        const self: *Client = @ptrCast(@alignCast(ctx));
        const fill = try params.initFill(self.fill_size);
        for (0..self.fill_size) |i| try fill.set(@intCast(i), fillByte(i));
        try params.setFdCap1Capability(.{ .id = self.cap1 });
        try params.setFdCap2Capability(.{ .id = self.cap2 });
    }

    fn onReturn(ctx: *anyopaque, peer: *Peer, response: g.FdTest.WriteToFd.Response, caps: *const InboundCapTable) anyerror!void {
        const self: *Client = @ptrCast(@alignCast(ctx));
        self.handleReturn(peer, response, caps) catch |err| return self.fail(err);
    }

    fn handleReturn(self: *Client, peer: *Peer, response: g.FdTest.WriteToFd.Response, caps: *const InboundCapTable) !void {
        const results = try response.unwrap();
        const second = try results.getSecondFdPresent();
        if (second != self.config.expect_second) {
            std.debug.print("endpoint: call {d}: secondFdPresent is {}, expected {}\n", .{ self.returned, second, self.config.expect_second });
            return error.SecondFdPresentMismatch;
        }
        // The fd on the returned capability; borrowed until the import is
        // released, right after this callback.
        const fd3 = peer.importFd(try importId(caps, try results.getFdCap3())) orelse return error.ThirdFdMissing;
        try expectPipe(fd3.fd, "baz", "fdCap3");
        self.returned += 1;
        if (self.started < self.config.fills.len) return self.startCall();
        // The remote releases fdCap1 and fdCap2 after this Return; the
        // pipes are checked once it has released all of them.
        if (self.releases.conn) |conn| conn.wake();
    }

    /// Once every call returned and the remote released every capability
    /// this side sent: check the pipes while the connection is still up,
    /// then close it. Runs on the connection's thread, from its wake.
    fn maybeFinish(self: *Client) void {
        if (self.checked or self.failed != null) return;
        if (self.returned != self.config.fills.len) return;
        if (self.releases.count != 2 * self.config.fills.len) return;
        self.checked = true;
        self.checkPipes() catch |err| return self.fail(err);
        if (!self.peer.isAttachedTransportClosing()) self.peer.closeAttachedTransport();
    }

    /// Every copy of each write end is closed now: this process's own
    /// (closed with its export), the dup the transport sent (closed after
    /// the send), and the remote's received copy (closed before it sent
    /// the Release). So each read end gives its data, then EOF.
    fn checkPipes(self: *Client) !void {
        for (0..self.config.fills.len) |i| {
            try expectPipe(self.in1[i], if (self.config.expect_second) "bar" else "", "in1");
            try expectPipe(self.in2[i], "foo", "in2");
        }
    }

    fn closePipes(self: *Client) void {
        for ([_][]Fd{ &self.in1, &self.in2 }) |ends| {
            for (ends) |*fd| {
                if (fd.* >= 0) closeFd(fd.*);
                fd.* = -1;
            }
        }
    }
};

/// The client of the running connection, for `onWake`.
var running_client: ?*Client = null;

fn onWake(_: *tcp.Connection) void {
    if (running_client) |client| client.maybeFinish();
}

/// One connection over `socket`, from init to teardown. `server` and
/// `client` outlive it: the peer's exports point into them until
/// `Peer.deinit`.
fn runConnection(
    gpa: std.mem.Allocator,
    io: std.Io,
    socket: Fd,
    max_fds: u8,
    serving: bool,
    server: *Server,
    client: *Client,
) !void {
    var conn = tcp.Connection.init(gpa, io, .{ .handle = socket }, .{}) catch |err| {
        closeFd(socket);
        return err;
    };
    defer conn.deinit();
    try conn.enableFdPassing(max_fds);

    var peer = Peer.init(gpa, &conn);
    defer {
        _ = peer.takeAttachedConnection(*tcp.Connection);
        peer.deinit();
    }
    peer.setClockIo(io);

    if (serving) {
        var fd_test = g.FdTest.Server{ .ctx = server, .vtable = .{ .writeToFd = Server.writeToFd } };
        _ = try g.FdTest.setBootstrap(&peer, &fd_test);
        peer.start(null, null, null);
        conn.run();
    } else {
        client.peer = &peer;
        client.releases.conn = &conn;
        defer client.releases.conn = null;
        running_client = client;
        defer running_client = null;
        try conn.enableWake(onWake);
        _ = try g.FdTest.Client.fromBootstrap(&peer, client, Client.onBootstrap);
        peer.start(null, null, null);
        // The bootstrap import is released by `Peer.deinit`.
        conn.run();
    }
}

pub fn main(init: std.process.Init) !void {
    var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
    defer if (debug_allocator.deinit() != .ok) @panic("fd e2e endpoint leaked memory");
    const gpa = debug_allocator.allocator();

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, gpa);
    defer args.deinit();
    _ = args.next();
    const socket = try std.fmt.parseInt(Fd, args.next() orelse return error.MissingFd, 10);
    const serving = std.mem.eql(u8, args.next() orelse return error.MissingRole, "server");
    const max_fds = try std.fmt.parseInt(u8, args.next() orelse return error.MissingMaxFds, 10);
    const expect_second = std.mem.eql(u8, args.next() orelse return error.MissingSecond, "1");
    var fills_buf: [max_calls]u32 = undefined;
    var fill_count: usize = 0;
    while (args.next()) |arg| {
        if (fill_count == max_calls) return error.TooManyCalls;
        fills_buf[fill_count] = try std.fmt.parseInt(u32, arg, 10);
        fill_count += 1;
    }
    if (fill_count == 0) return error.NoCalls;
    const config: Config = .{ .fills = fills_buf[0..fill_count], .expect_second = expect_second };

    const baseline = openFds();
    var server = Server{ .config = config };
    var client = Client{ .config = config };
    defer client.closePipes();
    try runConnection(gpa, init.io, socket, max_fds, serving, &server, &client);

    if (serving) {
        if (server.failed) |err| return err;
        if (server.calls != config.fills.len) {
            std.debug.print("endpoint: served {d} calls, expected {d}\n", .{ server.calls, config.fills.len });
            return error.MissingCalls;
        }
    } else {
        if (client.failed) |err| return err;
        if (!client.checked) {
            std.debug.print("endpoint: the connection ended after {d} of {d} returns and {d} of {d} releases\n", .{
                client.returned, config.fills.len, client.releases.count, 2 * config.fills.len,
            });
            return error.ConnectionEndedEarly;
        }
        client.closePipes();
    }
    try expectBackAtBaseline(baseline, socket);
}
