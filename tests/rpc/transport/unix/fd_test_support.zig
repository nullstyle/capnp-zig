//! Raw-syscall helpers shared by the AF_UNIX fd suites
//! (`rpc_unix_fd_drain_test.zig`, `rpc_unix_linger_test.zig`,
//! `rpc_unix_fd_send_test.zig`, `rpc_unix_fd_boundary_test.zig`).
//!
//! The "peer" in these suites is a raw socket that attaches fds with
//! `sendmsg`, the way a hostile local process would. Everything here calls
//! `posix.system` directly: Linux without libc, Linux with glibc (the TSan
//! lane) and macOS libc.

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

pub const posix = std.posix;
pub const sys = posix.system;
const cmsg = std.Io.net.cmsg;
const cmsg_align = std.Io.net.cmsg_align;
const testing = std.testing;

pub const is_linux = builtin.os.tag == .linux;
pub const is_macos = builtin.os.tag == .macos;
/// The kernels these suites pin. Every other target skips (Windows has no
/// SCM_RIGHTS), but every target compiles the files.
pub const supported = is_linux or is_macos;

pub const Fd = posix.fd_t;
pub const events = capnpc.rpc.events;
pub const fd_io = capnpc.rpc.transport.unix.fd_io;

fn ival(rc: anytype) isize {
    return switch (@typeInfo(@TypeOf(rc)).int.signedness) {
        .signed => @intCast(rc),
        .unsigned => @bitCast(rc),
    };
}

/// The non-negative result of a syscall, or the errno number printed and a
/// failure.
pub fn check(rc: anytype, what: []const u8) error{SyscallFailed}!usize {
    const err = posix.errno(rc);
    if (err != .SUCCESS) {
        std.debug.print("{s} failed with errno {d}\n", .{ what, @backingInt(err) });
        return error.SyscallFailed;
    }
    return @intCast(ival(rc));
}

pub fn closeFd(fd: Fd) void {
    _ = sys.close(fd);
}

pub fn socketPair() ![2]Fd {
    var fds: [2]Fd = undefined;
    _ = try check(sys.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &fds), "socketpair");
    return fds;
}

pub fn pipePair() ![2]Fd {
    var fds: [2]Fd = undefined;
    _ = try check(sys.pipe(&fds), "pipe");
    return fds;
}

fn fdFlags(fd: Fd) ?usize {
    const rc = if (is_linux and !builtin.link_libc)
        sys.fcntl(fd, posix.F.GETFD, 0)
    else
        sys.fcntl(fd, posix.F.GETFD);
    if (posix.errno(rc) != .SUCCESS) return null;
    return @intCast(ival(rc));
}

pub fn isOpen(fd: Fd) bool {
    return fdFlags(fd) != null;
}

/// `fd` is open and close-on-exec.
pub fn isCloexec(fd: Fd) bool {
    const flags = fdFlags(fd) orelse return false;
    return flags & posix.FD_CLOEXEC != 0;
}

pub fn setNonBlocking(fd: Fd, on: bool) !void {
    const nonblock: usize = @as(u32, @bitCast(posix.O{ .NONBLOCK = true }));
    const get = if (is_linux and !builtin.link_libc)
        sys.fcntl(fd, posix.F.GETFL, 0)
    else
        sys.fcntl(fd, posix.F.GETFL);
    const old = try check(get, "fcntl(F_GETFL)");
    const new = if (on) old | nonblock else old & ~nonblock;
    const set = if (is_linux and !builtin.link_libc)
        sys.fcntl(fd, posix.F.SETFL, new)
    else
        sys.fcntl(fd, posix.F.SETFL, @as(c_int, @intCast(new)));
    _ = try check(set, "fcntl(F_SETFL)");
}

/// True when every write end of the pipe whose read end is `read_end` is
/// closed: the read end polls ready within `timeout_ms` and reads EOF. The
/// pipe must hold no unread data.
pub fn pipeWritersClosed(read_end: Fd, timeout_ms: i32) bool {
    var pfd = [1]posix.pollfd{.{ .fd = read_end, .events = posix.POLL.IN, .revents = 0 }};
    const rc = sys.poll(&pfd, 1, timeout_ms);
    if (posix.errno(rc) != .SUCCESS or ival(rc) <= 0) return false;
    var byte: [1]u8 = undefined;
    const n = sys.read(read_end, &byte, 1);
    return posix.errno(n) == .SUCCESS and ival(n) == 0;
}

/// How long a positive check waits for an fd the closer must close.
pub const closed_wait_ms: i32 = 2000;
/// How long a negative check waits for a pipe writer that must stay open.
pub const still_open_wait_ms: i32 = 50;

pub fn nowNs() i96 {
    return std.Io.Clock.awake.now(testing.io).nanoseconds;
}

pub fn msSince(start_ns: i96) i64 {
    return @intCast(@divFloor(nowNs() - start_ns, std.time.ns_per_ms));
}

pub fn sleepMs(ms: i64) void {
    std.Io.sleep(testing.io, .fromMilliseconds(ms), .awake) catch {};
}

const max_send_fds = 300;

fn buildRights(buf: []align(cmsg_align) u8, fds: []const Fd) []align(cmsg_align) u8 {
    const data_len = fds.len * @sizeOf(Fd);
    const total = cmsg.space(data_len);
    @memset(buf[0..total], 0);
    const header: *align(cmsg_align) posix.cmsghdr = @ptrCast(buf.ptr);
    header.len = @intCast(cmsg.len(@intCast(data_len)));
    header.level = posix.SOL.SOCKET;
    header.type = posix.SCM.RIGHTS;
    @memcpy(cmsg.data(header)[0..data_len], std.mem.sliceAsBytes(fds));
    return buf[0..total];
}

/// One raw `sendmsg`; a non-empty `fds` goes out as one SCM_RIGHTS cmsg.
fn sendOnce(sock: Fd, bytes: []const u8, fds: []const Fd) !usize {
    var control_buf: [cmsg.space(max_send_fds * @sizeOf(Fd))]u8 align(cmsg_align) = undefined;
    const control: []const u8 = if (fds.len == 0) &.{} else buildRights(&control_buf, fds);
    var iov = [1]posix.iovec_const{.{ .base = bytes.ptr, .len = bytes.len }};
    const msg: posix.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = if (control.len == 0) null else control.ptr,
        .controllen = @intCast(control.len),
        .flags = 0,
    };
    while (true) {
        const rc = sys.sendmsg(sock, &msg, 0);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(ival(rc)),
            .INTR => continue,
            else => |err| {
                std.debug.print("sendmsg failed with errno {d}\n", .{@backingInt(err)});
                return error.SyscallFailed;
            },
        }
    }
}

/// Sends all of `bytes` from the raw peer socket, with `fds` attached to the
/// first chunk only.
pub fn sendWithFds(sock: Fd, bytes: []const u8, fds: []const Fd) !void {
    var offset: usize = 0;
    var first = true;
    while (offset < bytes.len) {
        offset += try sendOnce(sock, bytes[offset..], if (first) fds else &.{});
        first = false;
    }
}

/// A valid one-segment Cap'n Proto frame whose root holds `value`. The
/// caller frees it with `allocator`.
pub fn buildFrame(allocator: std.mem.Allocator, value: u32) ![]const u8 {
    var builder = capnpc.message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(1, 0);
    root.writeU32(0, value);
    return builder.toBytes();
}

/// `count` pipes. The test keeps the read ends to watch for EOF and sends
/// the write ends.
pub const Pipes = struct {
    read_ends: [16]Fd = undefined,
    write_ends: [16]Fd = undefined,
    count: usize = 0,

    pub fn open(count: usize) !Pipes {
        var self: Pipes = .{};
        errdefer self.closeAll();
        while (self.count < count) {
            const p = try pipePair();
            self.read_ends[self.count] = p[0];
            self.write_ends[self.count] = p[1];
            self.count += 1;
        }
        return self;
    }

    pub fn writers(self: *const Pipes) []const Fd {
        return self.write_ends[0..self.count];
    }

    /// Close the test's own copies of the write ends, after sending them.
    /// From then on only the receiver's copies keep each pipe open.
    pub fn closeWriters(self: *Pipes) void {
        for (self.write_ends[0..self.count]) |*w| {
            if (w.* >= 0) closeFd(w.*);
            w.* = -1;
        }
    }

    pub fn closeAll(self: *Pipes) void {
        self.closeWriters();
        for (self.read_ends[0..self.count]) |r| closeFd(r);
        self.count = 0;
    }

    /// Fails unless pipe `i` sees EOF within `closed_wait_ms`, for each `i`
    /// in `indexes`: every copy of its write end is closed.
    pub fn expectClosed(self: *const Pipes, indexes: []const usize) !void {
        for (indexes) |i| {
            if (!pipeWritersClosed(self.read_ends[i], closed_wait_ms)) {
                std.debug.print("pipe {d}: a copy of its write end is still open\n", .{i});
                return error.AttachedFdStillOpen;
            }
        }
    }

    /// Fails unless pipe `i` still has a copy of its write end open, for
    /// each `i` in `indexes`.
    pub fn expectOpen(self: *const Pipes, indexes: []const usize) !void {
        for (indexes) |i| {
            if (pipeWritersClosed(self.read_ends[i], still_open_wait_ms)) {
                std.debug.print("pipe {d}: every copy of its write end is closed\n", .{i});
                return error.AttachedFdClosedEarly;
            }
        }
    }

    /// Fails unless every pipe sees EOF within `closed_wait_ms`: the
    /// receiver closed every write end it got.
    pub fn expectAllWritersClosed(self: *const Pipes) !void {
        for (self.read_ends[0..self.count], 0..) |r, i| {
            if (!pipeWritersClosed(r, closed_wait_ms)) {
                std.debug.print("pipe {d}: a write end the peer attached is still open\n", .{i});
                return error.AttachedFdStillOpen;
            }
        }
    }
};

/// Which fds below `max_scanned_fd` are open. Fds are allocated lowest
/// first, and these suites hold a few dozen at most.
pub const max_scanned_fd = 2048;

pub const FdSnapshot = struct {
    open: std.StaticBitSet(max_scanned_fd),
    /// The process fd budget's count (`fd_io.budget.inUse()`) when taken.
    budget_in_use: usize = 0,

    pub fn take() FdSnapshot {
        var snapshot: FdSnapshot = .{ .open = .empty, .budget_in_use = fd_io.budget.inUse() };
        var fd: usize = 0;
        while (fd < max_scanned_fd) : (fd += 1) {
            if (isOpen(@intCast(fd))) snapshot.open.set(fd);
        }
        return snapshot;
    }

    /// The fds open in `after` that were closed in `before`. Returns the
    /// count; the first `out.len` land in `out`.
    pub fn added(after: FdSnapshot, before: FdSnapshot, out: []Fd) usize {
        var count: usize = 0;
        var it = after.open.iterator(.{});
        while (it.next()) |fd| {
            if (before.open.isSet(fd)) continue;
            if (count < out.len) out[count] = @intCast(fd);
            count += 1;
        }
        return count;
    }

    pub fn highest(snapshot: FdSnapshot) usize {
        return snapshot.open.findLastSet() orelse 0;
    }
};

/// Waits (up to `closed_wait_ms`, for the closer thread) until the fd table
/// is exactly `before` again, and the process fd budget counts what it did
/// then; prints the difference and fails otherwise.
pub fn expectBackAtBaseline(before: FdSnapshot) !void {
    const start = nowNs();
    while (true) {
        const after = FdSnapshot.take();
        var leaked: [16]Fd = undefined;
        const n_leaked = after.added(before, &leaked);
        var lost: [16]Fd = undefined;
        const n_lost = before.added(after, &lost);
        if (n_leaked == 0 and n_lost == 0 and after.budget_in_use == before.budget_in_use) return;
        if (msSince(start) >= closed_wait_ms) {
            std.debug.print("fd table not back at baseline: {d} new fd(s) {any}, {d} closed fd(s) {any}\n", .{
                n_leaked, leaked[0..@min(n_leaked, leaked.len)], n_lost, lost[0..@min(n_lost, lost.len)],
            });
            if (after.budget_in_use != before.budget_in_use) {
                std.debug.print("process fd budget counts {d} fd(s), {d} at the baseline\n", .{ after.budget_in_use, before.budget_in_use });
                return error.FdBudgetNotAtBaseline;
            }
            return error.FdTableNotAtBaseline;
        }
        sleepMs(10);
    }
}

/// Waits (up to `closed_wait_ms`, for the closer thread) until the process
/// fd budget counts exactly `want`.
pub fn expectBudgetInUse(want: usize) !void {
    const start = nowNs();
    while (fd_io.budget.inUse() != want) {
        if (msSince(start) >= closed_wait_ms) {
            std.debug.print("process fd budget counts {d} fd(s), expected {d}\n", .{ fd_io.budget.inUse(), want });
            return error.FdBudgetMismatch;
        }
        sleepMs(5);
    }
}

/// Sets the process fd budget's limit for one test, and restores it.
pub const BudgetLimit = struct {
    previous: usize,

    pub fn set(limit: usize) BudgetLimit {
        return .{ .previous = fd_io.budget.setLimit(limit) };
    }

    pub fn restore(self: BudgetLimit) void {
        _ = fd_io.budget.setLimit(self.previous);
    }
};

/// Waits until the closer threads have done everything handed to them.
pub fn waitCloserIdle(timeout_ms: i64) !void {
    const start = nowNs();
    while (fd_io.closer.pending() != 0) {
        if (msSince(start) >= timeout_ms) {
            std.debug.print("fd closer still has {d} fd(s) pending after {d} ms\n", .{ fd_io.closer.pending(), timeout_ms });
            return error.CloserStillBusy;
        }
        sleepMs(10);
    }
}

/// Waits until one closer lane has done everything handed to it.
pub fn waitLaneIdle(lane: fd_io.closer.Lane, timeout_ms: i64) !void {
    const start = nowNs();
    while (fd_io.closer.pendingIn(lane) != 0) {
        if (msSince(start) >= timeout_ms) {
            std.debug.print("fd closer lane {t} still has {d} job(s) pending after {d} ms\n", .{ lane, fd_io.closer.pendingIn(lane), timeout_ms });
            return error.CloserStillBusy;
        }
        sleepMs(10);
    }
}

/// Setup check for the stall tests: `lane` still has a job after
/// `settle_ms`, so the lingering close in it really blocks (otherwise the
/// test proves nothing).
pub fn expectLaneStuck(lane: fd_io.closer.Lane, settle_ms: i64) !void {
    sleepMs(settle_ms);
    if (fd_io.closer.pendingIn(lane) == 0) {
        std.debug.print("setup: the lingering close on the {t} lane did not block\n", .{lane});
        return error.LingerDidNotBlock;
    }
}

/// A TCP client socket whose final close lingers: the accepting side never
/// reads, so unsent data stays queued, and SO_LINGER is on. Its final close
/// blocks the closing thread for `seconds`, or until `endLinger`.
pub const LingeringSocket = struct {
    listener: Fd,
    server_side: Fd,
    client: Fd,

    var chunk: [64 * 1024]u8 = @splat(0xab);

    pub fn open(seconds: i32) !LingeringSocket {
        const listener: Fd = @intCast(try check(sys.socket(posix.AF.INET, posix.SOCK.STREAM, 0), "socket"));
        errdefer closeFd(listener);
        var addr: posix.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f00_0001) };
        _ = try check(sys.bind(listener, @ptrCast(&addr), @sizeOf(posix.sockaddr.in)), "bind");
        _ = try check(sys.listen(listener, 1), "listen");
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        _ = try check(sys.getsockname(listener, @ptrCast(&addr), &addr_len), "getsockname");
        const client: Fd = @intCast(try check(sys.socket(posix.AF.INET, posix.SOCK.STREAM, 0), "socket"));
        errdefer closeFd(client);
        _ = try check(sys.connect(client, @ptrCast(&addr), @sizeOf(posix.sockaddr.in)), "connect");
        const server_side: Fd = @intCast(try check(sys.accept(listener, null, null), "accept"));
        errdefer closeFd(server_side);

        // Fill the send queue; the accepting side never reads. One pass is
        // not enough on macOS: receive-buffer autotuning drains it and the
        // close then does not linger. Refill until two passes 10 ms apart
        // add nothing.
        try setNonBlocking(client, true);
        var queued = try fillSendQueue(client);
        try testing.expect(queued > 0);
        var quiet_passes: usize = 0;
        var passes: usize = 0;
        while (quiet_passes < 2) : (passes += 1) {
            if (passes == 200) return error.SendQueueNeverSettled;
            sleepMs(10);
            const more = try fillSendQueue(client);
            queued += more;
            quiet_passes = if (more == 0) quiet_passes + 1 else 0;
        }
        try setNonBlocking(client, false);

        // Darwin's SO_LINGER counts clock ticks; SO_LINGER_SEC counts
        // seconds, as Linux's SO_LINGER does.
        const linger_opt = if (is_macos) posix.SO.LINGER_SEC else posix.SO.LINGER;
        const lg: posix.linger = .{ .onoff = 1, .linger = seconds };
        _ = try check(sys.setsockopt(client, posix.SOL.SOCKET, linger_opt, std.mem.asBytes(&lg), @sizeOf(posix.linger)), "setsockopt(SO_LINGER)");
        return .{ .listener = listener, .server_side = server_side, .client = client };
    }

    /// Sends on the non-blocking `client` until EAGAIN; returns the bytes sent.
    fn fillSendQueue(client: Fd) !usize {
        var queued: usize = 0;
        while (queued < 64 * 1024 * 1024) {
            const rc = sys.write(client, &chunk, chunk.len);
            switch (posix.errno(rc)) {
                .SUCCESS => queued += @intCast(rc),
                .INTR => {},
                .AGAIN => return queued,
                else => |err| {
                    std.debug.print("filling the TCP send queue failed with errno {d}\n", .{@backingInt(err)});
                    return error.SyscallFailed;
                },
            }
        }
        return error.SendQueueNeverFilled;
    }

    /// Our own copy of the lingering socket, after it was attached.
    pub fn closeClient(self: *LingeringSocket) void {
        if (self.client >= 0) closeFd(self.client);
        self.client = -1;
    }

    /// Close the accepting side. That resets the connection, which ends
    /// any linger still running on the closer thread.
    pub fn endLinger(self: *LingeringSocket) void {
        if (self.server_side >= 0) closeFd(self.server_side);
        self.server_side = -1;
    }

    pub fn deinit(self: *LingeringSocket) void {
        self.closeClient();
        self.endLinger();
        closeFd(self.listener);
    }
};

/// True once `fd` is closed, within `timeout_ms`. Only for an fd number
/// nothing else in the process can reuse in the meantime.
pub fn waitClosed(fd: Fd, timeout_ms: i64) bool {
    const start = nowNs();
    while (isOpen(fd)) {
        if (msSince(start) >= timeout_ms) return false;
        sleepMs(5);
    }
    return true;
}

/// A thread blocked in one `Transport.read`, for tests that must see a
/// reader wake (or not). Pinned: start it in place.
pub const BlockedReader = struct {
    thread: std.Thread = undefined,
    done: std.atomic.Value(bool) = .init(false),
    result: ?(capnpc.rpc.transport.tcp.Transport.ReadError!usize) = null,
    joined: bool = true,

    pub fn start(self: *BlockedReader, transport: *capnpc.rpc.transport.tcp.Transport) !void {
        self.* = .{};
        self.thread = try std.Thread.spawn(.{}, run, .{ self, transport });
        self.joined = false;
    }

    fn run(self: *BlockedReader, transport: *capnpc.rpc.transport.tcp.Transport) void {
        self.result = transport.read();
        self.done.store(true, .release);
    }

    /// True once the read returned, within `timeout_ms`.
    pub fn waitDone(self: *BlockedReader, timeout_ms: i64) bool {
        const waited_from = nowNs();
        while (!self.done.load(.acquire)) {
            if (msSince(waited_from) >= timeout_ms) return false;
            sleepMs(5);
        }
        return true;
    }

    /// Joins the thread. A read still blocked is ended first by shutting
    /// down the write half of `peer`: the reader then sees end of stream.
    pub fn finish(self: *BlockedReader, peer: Fd) void {
        if (self.joined) return;
        if (!self.done.load(.acquire)) _ = sys.shutdown(peer, posix.SHUT.WR);
        self.thread.join();
        self.joined = true;
    }
};

/// Records the events a transport emits. Observer callbacks run on the
/// connection's loop thread, which is the test thread in these suites.
pub const Recorder = struct {
    attached_fds: [32]events.ResourceRejectionEvent = undefined,
    attached_count: usize = 0,
    connection_sources: [16]events.Source = undefined,
    connection_count: usize = 0,
    close_err: ?anyerror = null,
    closed: bool = false,
    protocol_errs: [8]anyerror = undefined,
    protocol_count: usize = 0,

    pub fn observer(self: *Recorder) events.Observer {
        return events.Observer.init(self, onEvent);
    }

    fn onEvent(ctx: *anyopaque, event: events.Event) void {
        const self: *Recorder = @ptrCast(@alignCast(ctx));
        switch (event) {
            .resource_rejection => |r| if (r.resource == .attached_fds and self.attached_count < self.attached_fds.len) {
                self.attached_fds[self.attached_count] = r;
                self.attached_count += 1;
            },
            .connection => |c| if (self.connection_count < self.connection_sources.len) {
                self.connection_sources[self.connection_count] = c.source;
                self.connection_count += 1;
            },
            .close => |c| {
                self.closed = true;
                self.close_err = c.err;
            },
            .protocol_error => |p| if (self.protocol_count < self.protocol_errs.len) {
                self.protocol_errs[self.protocol_count] = p.err;
                self.protocol_count += 1;
            },
            else => {},
        }
    }

    /// The `.attached_fds` rejections whose `err` is `err`.
    pub fn countErr(self: *const Recorder, err: anyerror) usize {
        var n: usize = 0;
        for (self.attached_fds[0..self.attached_count]) |r| {
            if (r.err == err) n += 1;
        }
        return n;
    }

    /// Total `attempted` over the `.attached_fds` rejections whose `err` is
    /// `err`.
    pub fn attemptedFor(self: *const Recorder, err: anyerror) usize {
        var n: usize = 0;
        for (self.attached_fds[0..self.attached_count]) |r| {
            if (r.err == err) n += r.attempted orelse 0;
        }
        return n;
    }
};

/// The frames a `Connection` delivered.
pub const Inbox = struct {
    frames: usize = 0,
    first_frame_ns: ?i96 = null,
    errors: usize = 0,
    last_error: ?anyerror = null,
    close_after_frames: ?usize = null,
};
