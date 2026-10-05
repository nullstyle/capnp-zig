//! The send side of fd passing: `fd_io.sendWithFds`, and fds in a
//! `Transport`'s write queue (`enqueueWriteWithFds`).
//!
//! Item 10 of docs/sprint-plan-2026-10-04.md. The queue holds its own dup of
//! every fd it is asked to send, and every dup must be closed exactly once:
//! after its send, after a failed or skipped send, and when the queue is
//! drained at teardown. These tests attach pipe write ends, then require
//! that each pipe's read end sees EOF (every copy of the write end is
//! closed) and that the fd table returns to its baseline.
//!
//! Linux and macOS run the tests; other targets compile the file and run
//! only the stub and TCP tests. Tests that hold more than a few dozen fds
//! raise the soft RLIMIT_NOFILE themselves (the macOS default is 256).

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");
const support = @import("fd_test_support.zig");

const testing = std.testing;
const posix = support.posix;
const sys = support.sys;
const fd_io = support.fd_io;
const events = support.events;
const tcp = capnpc.rpc.transport.tcp;
const Transport = tcp.Transport;
const Fd = support.Fd;

/// Starts the closer threads (they open no fds) so the baseline the test
/// takes next already includes everything that lives for the whole process.
fn warmUp() !void {
    try fd_io.closer.ensureStarted();
    try support.waitCloserIdle(5000);
}

/// Raises the soft RLIMIT_NOFILE to at least `want` (never lowers it) and
/// remembers the old value.
const FdHeadroom = struct {
    saved: posix.rlimit,

    fn ensure(want: u64) !FdHeadroom {
        const saved = try posix.getrlimit(.NOFILE);
        if (saved.cur < want) {
            var raised = saved;
            raised.cur = @min(want, saved.max);
            try posix.setrlimit(.NOFILE, raised);
        }
        return .{ .saved = saved };
    }

    fn restore(self: FdHeadroom) void {
        posix.setrlimit(.NOFILE, self.saved) catch |err| {
            std.debug.print("could not restore RLIMIT_NOFILE: {t}\n", .{err});
        };
    }
};

/// A message larger than any AF_UNIX socket buffer, so the writer blocks in
/// it until the peer reads or closes.
const blocking_len = 16 * 1024 * 1024;

/// How long the peer waits for more bytes before it fails the test.
const read_wait_ms: i32 = 5000;

/// The peer side: reads exactly `want` bytes from `sock` with
/// `fd_io.recvWithFds`, closes every fd that arrives, and reports them.
const PeerRead = struct {
    bytes: usize = 0,
    fds: usize = 0,
    reads_with_fds: usize = 0,
    /// Offset in the stream of the first byte of the read that brought the
    /// first fds.
    first_fd_offset: ?usize = null,

    fn readExactly(sock: Fd, want: usize) !PeerRead {
        var out: PeerRead = .{};
        var data: [64 * 1024]u8 = undefined;
        var control: [fd_io.controlSpace(fd_io.max_fds_per_read)]u8 align(8) = undefined;
        var fds: [fd_io.max_fds_per_read]Fd = undefined;
        while (out.bytes < want) {
            // Never block for good: a test that fails mid-stream must fail,
            // not hang.
            var pfd = [1]posix.pollfd{.{ .fd = sock, .events = posix.POLL.IN, .revents = 0 }};
            if (try support.check(sys.poll(&pfd, 1, read_wait_ms), "poll") == 0) {
                std.debug.print("peer read: no data for {d} ms after {d} of {d} bytes ({d} fds)\n", .{ read_wait_ms, out.bytes, want, out.fds });
                return error.PeerReadTimedOut;
            }
            const room = @min(data.len, want - out.bytes);
            const got = try fd_io.recvWithFds(sock, data[0..room], &control, &fds);
            if (got.data_len == 0) return error.UnexpectedEndOfStream;
            try testing.expect(!got.control_truncated);
            if (got.fd_count != 0) {
                if (out.first_fd_offset == null) out.first_fd_offset = out.bytes;
                out.reads_with_fds += 1;
                out.fds += got.fd_count;
                for (fds[0..got.fd_count]) |fd| support.closeFd(fd);
            }
            out.bytes += got.data_len;
        }
        return out;
    }
};

/// Waits until the writer took every queued item into its batch (the queue
/// is empty while the transport still accounts for bytes in flight).
fn waitBatchTaken(transport: *Transport) !void {
    const start = support.nowNs();
    while (true) {
        const stats = transport.queueStats();
        if (stats.items == 0 and stats.bytes != 0) return;
        if (support.msSince(start) >= 5000) {
            std.debug.print("writer never took the batch: {any}\n", .{stats});
            return error.WriterStalled;
        }
        support.sleepMs(2);
    }
}

fn waitClosing(transport: *Transport) !void {
    const start = support.nowNs();
    while (!transport.isClosing()) {
        if (support.msSince(start) >= 5000) return error.WriterNeverFailed;
        support.sleepMs(2);
    }
}

/// Records the backpressure events a transport emits.
const BackpressureRecorder = struct {
    seen: [8]events.BackpressureEvent = undefined,
    count: usize = 0,

    fn observer(self: *BackpressureRecorder) events.Observer {
        return events.Observer.init(self, onEvent);
    }

    fn onEvent(ctx: *anyopaque, event: events.Event) void {
        const self: *BackpressureRecorder = @ptrCast(@alignCast(ctx));
        switch (event) {
            .backpressure => |b| if (self.count < self.seen.len) {
                self.seen[self.count] = b;
                self.count += 1;
            },
            else => {},
        }
    }
};

// ---------------------------------------------------------------------------
// fd_io.sendWithFds
// ---------------------------------------------------------------------------

test "sendWithFds is a stub where fd passing is compiled out" {
    if (fd_io.supported) return error.SkipZigTest;
    try testing.expectError(error.UnixSocketsUnsupported, fd_io.sendWithFds(0, "x", &.{}));
    try testing.expectError(error.UnixSocketsUnsupported, fd_io.dupCloexec(0));
}

test "sendWithFds refuses more than 253 fds, and fds with no bytes, before the syscall" {
    if (!support.supported) return error.SkipZigTest;
    const sp = try support.socketPair();
    defer support.closeFd(sp[0]);
    defer support.closeFd(sp[1]);
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();

    const too_many: [fd_io.max_fds_per_send + 1]Fd = @splat(pipes.write_ends[0]);
    try testing.expectError(error.TooManyFds, fd_io.sendWithFds(sp[0], "x", &too_many));
    try testing.expectError(error.FdsWithoutData, fd_io.sendWithFds(sp[0], "", pipes.writers()));

    // Nothing reached the peer.
    var pfd = [1]posix.pollfd{.{ .fd = sp[1], .events = posix.POLL.IN, .revents = 0 }};
    try testing.expectEqual(@as(usize, 0), try support.check(sys.poll(&pfd, 1, 0), "poll"));
}

test "sendWithFds carries 253 fds, the most one message takes" {
    if (!support.supported) return error.SkipZigTest;
    const headroom = try FdHeadroom.ensure(1024);
    defer headroom.restore();
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[1]);
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();

        const all: [fd_io.max_fds_per_send]Fd = @splat(pipes.write_ends[0]);
        try fd_io.sendWithFds(sp[0], "one message", &all);
        support.closeFd(sp[0]);
        pipes.closeWriters();

        const got = try PeerRead.readExactly(sp[1], "one message".len);
        try testing.expectEqual(fd_io.max_fds_per_send, got.fds);
        try testing.expectEqual(@as(usize, 1), got.reads_with_fds);
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

const LargeSend = struct {
    sock: Fd,
    bytes: []const u8,
    fds: []const Fd,
    result: ?fd_io.SendError!void = null,

    fn run(self: *LargeSend) void {
        self.result = fd_io.sendWithFds(self.sock, self.bytes, self.fds);
    }
};

test "sendWithFds writes a message larger than the socket buffer, with the fds on its first bytes only" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        var peer_open = true;
        defer if (peer_open) support.closeFd(sp[1]);
        var pipes = try support.Pipes.open(2);
        defer pipes.closeAll();
        // A blocking sendmsg takes the whole message in one call. A
        // non-blocking one takes what fits, so the message goes out in many
        // chunks (and the EAGAIN wait runs between them).
        try support.setNonBlocking(sp[0], true);

        const len = 4 * 1024 * 1024;
        const bytes = try testing.allocator.alloc(u8, len);
        defer testing.allocator.free(bytes);
        for (bytes, 0..) |*b, i| b.* = @truncate(i);

        var sender: LargeSend = .{ .sock = sp[0], .bytes = bytes, .fds = pipes.writers() };
        const thread = try std.Thread.spawn(.{}, LargeSend.run, .{&sender});
        const got = PeerRead.readExactly(sp[1], len);
        // A failed read leaves the sender blocked: the peer's close wakes
        // every wait it can be in. End it before the join.
        if (got) |_| {} else |_| {
            support.closeFd(sp[1]);
            peer_open = false;
        }
        thread.join();
        support.closeFd(sp[0]);
        try sender.result.?;

        const read = try got;
        try testing.expectEqual(@as(usize, len), read.bytes);
        // Both fds, once, with the first bytes of the stream.
        try testing.expectEqual(@as(usize, 2), read.fds);
        try testing.expectEqual(@as(usize, 1), read.reads_with_fds);
        try testing.expectEqual(@as(?usize, 0), read.first_fd_offset);

        pipes.closeWriters();
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

/// Fills `sock`'s path to the peer until a non-blocking send would block,
/// then makes `sock` blocking again. Returns the bytes it took.
fn fillSocket(sock: Fd) !usize {
    try support.setNonBlocking(sock, true);
    var chunk: [4096]u8 = undefined;
    @memset(&chunk, 0xF1);
    var filled: usize = 0;
    while (true) {
        const rc = sys.write(sock, &chunk, chunk.len);
        switch (posix.errno(rc)) {
            .SUCCESS => filled += @intCast(rc),
            .AGAIN => break,
            .INTR => {},
            else => |err| {
                std.debug.print("fill write failed: errno {d}\n", .{@backingInt(err)});
                return error.SyscallFailed;
            },
        }
    }
    try support.setNonBlocking(sock, false);
    return filled;
}

const BlockedSend = struct {
    sock: Fd,
    fds: []const Fd,
    done: std.atomic.Value(bool) = .init(false),
    result: ?fd_io.SendError!void = null,

    fn run(self: *BlockedSend) void {
        self.result = fd_io.sendWithFds(self.sock, "after a full buffer", self.fds);
        self.done.store(true, .release);
    }
};

test "sendWithFds on a blocking socket waits while the peer's buffer is full (macOS refused the fds with EMSGSIZE)" {
    if (!support.supported) return error.SkipZigTest;
    const headroom = try FdHeadroom.ensure(1024);
    defer headroom.restore();
    try warmUp();
    const before = support.FdSnapshot.take();
    // One fd, and the most one message takes: XNU refuses a control
    // message larger than the room left, and 253 fds need about 1 KiB.
    for ([_]usize{ 1, fd_io.max_fds_per_send }) |fd_count| {
        const sp = try support.socketPair();
        var peer_open = true;
        defer if (peer_open) support.closeFd(sp[1]);
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();
        var all: [fd_io.max_fds_per_send]Fd = @splat(pipes.write_ends[0]);

        const filled = try fillSocket(sp[0]);
        var sender: BlockedSend = .{ .sock = sp[0], .fds = all[0..fd_count] };
        const thread = try std.Thread.spawn(.{}, BlockedSend.run, .{&sender});
        support.sleepMs(300);
        const waited = !sender.done.load(.acquire);
        const got = PeerRead.readExactly(sp[1], filled + "after a full buffer".len);
        // A failed read leaves the sender blocked: the peer's close wakes
        // every wait it can be in. End it before the join.
        if (got) |_| {} else |_| {
            support.closeFd(sp[1]);
            peer_open = false;
        }
        thread.join();
        support.closeFd(sp[0]);

        try sender.result.?;
        try testing.expect(waited);
        const read = try got;
        try testing.expectEqual(fd_count, read.fds);
        try testing.expectEqual(@as(?usize, filled), read.first_fd_offset);
        pipes.closeWriters();
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

var sigpipes_seen: std.atomic.Value(u32) = .init(0);

fn countSigpipe(_: posix.SIG) callconv(.c) void {
    _ = sigpipes_seen.fetchAdd(1, .monotonic);
}

test "sendWithFds to a closed peer returns BrokenPipe and raises no SIGPIPE" {
    if (!support.supported) return error.SkipZigTest;
    // Count SIGPIPEs instead of ignoring them (a Transport made earlier in
    // this process set SIG_IGN), so a raised signal shows up here.
    const counting: posix.Sigaction = .{
        .handler = .{ .handler = countSigpipe },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    var previous: posix.Sigaction = undefined;
    posix.sigaction(posix.SIG.PIPE, &counting, &previous);
    defer posix.sigaction(posix.SIG.PIPE, &previous, null);
    sigpipes_seen.store(0, .monotonic);

    const sp = try support.socketPair();
    defer support.closeFd(sp[1]);
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    support.closeFd(sp[0]);

    try testing.expectError(error.BrokenPipe, fd_io.sendWithFds(sp[1], "x", pipes.writers()));
    try testing.expectError(error.BrokenPipe, fd_io.sendWithFds(sp[1], "x", &.{}));
    // macOS posts SIGPIPE to the process, and another thread (a closer
    // thread) may take it: give it time to land.
    support.sleepMs(100);
    try testing.expectEqual(@as(u32, 0), sigpipes_seen.load(.monotonic));
}

test "dupCloexec returns a close-on-exec copy, and InvalidFd for a closed fd" {
    if (!support.supported) return error.SkipZigTest;
    var pipes = try support.Pipes.open(1);
    defer pipes.closeAll();
    const dup = try fd_io.dupCloexec(pipes.write_ends[0]);
    defer support.closeFd(dup);
    try testing.expect(dup != pipes.write_ends[0]);
    const rc = if (support.is_linux and !builtin.link_libc)
        sys.fcntl(dup, posix.F.GETFD, 0)
    else
        sys.fcntl(dup, posix.F.GETFD);
    const flags = try support.check(rc, "fcntl(F_GETFD)");
    try testing.expect(flags & posix.FD_CLOEXEC != 0);

    const closed = try support.pipePair();
    support.closeFd(closed[0]);
    support.closeFd(closed[1]);
    try testing.expectError(error.InvalidFd, fd_io.dupCloexec(closed[1]));
}

// ---------------------------------------------------------------------------
// Transport.enqueueWriteWithFds
// ---------------------------------------------------------------------------

test "a TCP Transport refuses fds and keeps working" {
    const pair = try tcp.createLoopbackSocketPair(testing.io);
    defer tcp.closeFd(testing.io, pair[1]);
    var transport = try Transport.init(testing.allocator, testing.io, pair[0], 64);
    defer transport.deinit();
    const fake = [_]i32{0};
    try testing.expectError(error.FdPassingUnsupported, transport.enqueueWriteWithFds("x", &fake));
    try transport.startWriter();
    try testing.expectError(error.FdPassingUnsupported, transport.enqueueWriteWithFds("x", &fake));
    try testing.expectEqual(@as(usize, 0), transport.queueStats().fds);
    // No fds is a plain enqueueWrite.
    try transport.enqueueWriteWithFds("plain", &.{});
}

test "enqueueWriteWithFds before startWriter sends at once, with no dups" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();

        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        defer transport.deinit();
        try transport.enqueueWriteWithFds("direct", pipes.writers());
        try testing.expectEqual(@as(usize, 0), transport.queueStats().fds);

        const got = try PeerRead.readExactly(sp[0], "direct".len);
        try testing.expectEqual(@as(usize, 1), got.fds);
        pipes.closeWriters();
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "N queued sends: every fd arrives, and every dup is closed after its send" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(2);
        defer pipes.closeAll();

        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        defer transport.deinit();
        try transport.startWriter();

        const frame = try support.buildFrame(testing.allocator, 0xFD);
        defer testing.allocator.free(frame);
        const sends = 60;
        for (0..sends) |_| try transport.enqueueWriteWithFds(frame, pipes.writers());

        const got = try PeerRead.readExactly(sp[0], sends * frame.len);
        try testing.expectEqual(@as(usize, sends * 2), got.fds);

        // The dups are the transport's last copies: once the closer has
        // them, the pipes see EOF while the transport is still alive.
        pipes.closeWriters();
        try pipes.expectAllWritersClosed();
        try support.waitLaneIdle(.sent, 2000);
        try testing.expectEqual(@as(usize, 0), transport.queueStats().fds);
    }
    try support.expectBackAtBaseline(before);
}

test "the fds a transport sends refer to the caller's files" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();

        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        defer transport.deinit();
        try transport.startWriter();
        try transport.enqueueWriteWithFds("pipe", pipes.writers());
        // The caller may close its own fd as soon as the enqueue returns.
        pipes.closeWriters();

        var data: [16]u8 = undefined;
        var control: [fd_io.controlSpace(4)]u8 align(8) = undefined;
        var fds: [4]Fd = undefined;
        const got = try fd_io.recvWithFds(sp[0], &data, &control, &fds);
        try testing.expectEqual(@as(usize, 1), got.fd_count);
        try testing.expectEqualStrings("pipe", data[0..got.data_len]);
        _ = try support.check(sys.write(fds[0], "hi", 2), "write to the received fd");
        support.closeFd(fds[0]);
        var echoed: [2]u8 = undefined;
        _ = try support.check(sys.read(pipes.read_ends[0], &echoed, 2), "read the pipe");
        try testing.expectEqualStrings("hi", &echoed);
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "an fd message right behind one that fills the path to the peer still goes out, and the connection stays up" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();

        // How many bytes the path from sp[1] to sp[0] holds.
        const capacity = try fillSocket(sp[1]);
        _ = try PeerRead.readExactly(sp[0], capacity);

        const big = try testing.allocator.alloc(u8, 1024 * 1024);
        defer testing.allocator.free(big);
        @memset(big, 0xD4);

        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        defer transport.deinit();
        try transport.startWriter();
        try transport.enqueueWrite(big);
        try transport.enqueueWriteWithFds("fd after full", pipes.writers());
        pipes.closeWriters();

        // Take all of `big` but what the path holds: the writer finishes
        // `big` with the path full, and the fd message finds no room (macOS
        // used to fail that send with EMSGSIZE and drop the connection).
        _ = try PeerRead.readExactly(sp[0], big.len - capacity);
        support.sleepMs(300);
        const rest = try PeerRead.readExactly(sp[0], capacity + "fd after full".len);
        try testing.expectEqual(@as(usize, 1), rest.fds);
        try testing.expect(!transport.isClosing());
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

/// `Transport.deinit` on a helper thread, so a teardown that hangs fails the
/// test instead of the run.
const Teardown = struct {
    transport: *Transport,
    done: std.atomic.Value(bool) = .init(false),

    fn run(self: *Teardown) void {
        self.transport.deinit();
        self.done.store(true, .release);
    }

    fn waitDone(self: *Teardown, timeout_ms: i64) bool {
        const start = support.nowNs();
        while (!self.done.load(.acquire)) {
            if (support.msSince(start) >= timeout_ms) return false;
            support.sleepMs(5);
        }
        return true;
    }
};

test "teardown while an fd message waits for room on a full path does not hang, and closes its dup" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        var peer_open = true;
        defer if (peer_open) support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();

        const capacity = try fillSocket(sp[1]);
        _ = try PeerRead.readExactly(sp[0], capacity);
        // macOS: exactly what the path holds, so the writer finishes it and
        // then waits inside the fd message's send (EMSGSIZE, then POLLOUT).
        // Linux counts buffer space per skb, so one big write packs more
        // than the 4 KiB fill measured: take more than the path holds, and
        // the writer waits in `filler` with the fd message behind it.
        const filler_len = if (support.is_macos) capacity else 4 * capacity;
        const filler = try testing.allocator.alloc(u8, filler_len);
        defer testing.allocator.free(filler);
        @memset(filler, 0xE5);

        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        var torn_down = false;
        defer if (!torn_down) transport.deinit();
        try transport.startWriter();
        // `filler` fills the path and the fd message waits behind it.
        // Nobody reads.
        try transport.enqueueWrite(filler);
        try transport.enqueueWriteWithFds("waits for room", pipes.writers());
        pipes.closeWriters();
        support.sleepMs(300);
        try testing.expect(!transport.isClosing());
        try testing.expectEqual(@as(usize, 1), transport.queueStats().fds);

        var teardown: Teardown = .{ .transport = &transport };
        const thread = try std.Thread.spawn(.{}, Teardown.run, .{&teardown});
        torn_down = true;
        if (!teardown.waitDone(2000)) {
            // Unstick it (the peer's close wakes every wait), then fail.
            support.closeFd(sp[0]);
            peer_open = false;
            thread.join();
            return error.TeardownHung;
        }
        thread.join();
        // The send failed after the wake: its dup went to the closer.
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "teardown with fd items still queued closes their dups (WriteQueue.drain)" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(3);
        defer pipes.closeAll();

        const big = try testing.allocator.alloc(u8, blocking_len);
        defer testing.allocator.free(big);
        @memset(big, 0x5A);

        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        var transport_live = true;
        defer if (transport_live) transport.deinit();
        try transport.startWriter();
        // The writer blocks inside `big`: nobody reads sp[0].
        try transport.enqueueWrite(big);
        try waitBatchTaken(&transport);
        for (pipes.writers()) |w| try transport.enqueueWriteWithFds("queued", &.{ w, w });
        try testing.expectEqual(@as(usize, 6), transport.queueStats().fds);
        try testing.expectEqual(@as(usize, 3), transport.queueStats().items);
        pipes.closeWriters();

        // deinit shuts the socket down (the blocked write fails), joins the
        // writer, and drains the three queued items.
        transport.deinit();
        transport_live = false;
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "a writer error mid-batch closes the dups of the items it skips" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        var peer_open = true;
        defer if (peer_open) support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(2);
        defer pipes.closeAll();

        const first_len = 1024 * 1024;
        const first = try testing.allocator.alloc(u8, first_len);
        defer testing.allocator.free(first);
        @memset(first, 0xA1);
        const big = try testing.allocator.alloc(u8, blocking_len);
        defer testing.allocator.free(big);
        @memset(big, 0xB2);

        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        defer transport.deinit();
        try transport.startWriter();

        // Batch 1 is `first` alone: the writer blocks in it.
        try transport.enqueueWrite(first);
        try waitBatchTaken(&transport);
        // These queue up behind it and become batch 2: `big`, then two items
        // with fds.
        try transport.enqueueWrite(big);
        try transport.enqueueWriteWithFds("skipped-1", pipes.writers()[0..1]);
        try transport.enqueueWriteWithFds("skipped-2", pipes.writers()[1..2]);
        pipes.closeWriters();

        // Let `first` through; the writer takes batch 2 and blocks in `big`.
        _ = try PeerRead.readExactly(sp[0], first_len);
        try waitBatchTaken(&transport);
        try testing.expectEqual(@as(usize, 2), transport.queueStats().fds);

        // The peer goes away: `big` fails, and the two items after it in the
        // batch are never sent.
        support.closeFd(sp[0]);
        peer_open = false;
        try waitClosing(&transport);

        // Their dups are closed by the writer, before any teardown.
        try pipes.expectAllWritersClosed();
        try support.waitLaneIdle(.sent, 2000);
        try testing.expectEqual(@as(usize, 0), transport.queueStats().fds);
    }
    try support.expectBackAtBaseline(before);
}

test "at most 256 fds in flight per transport, then FdQueueFull with a backpressure event" {
    if (!support.supported) return error.SkipZigTest;
    const headroom = try FdHeadroom.ensure(1024);
    defer headroom.restore();
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();
        const w = pipes.write_ends[0];

        const big = try testing.allocator.alloc(u8, blocking_len);
        defer testing.allocator.free(big);
        @memset(big, 0xC3);

        var recorder: BackpressureRecorder = .{};
        var transport = try Transport.initWithOptions(testing.allocator, testing.io, .{ .handle = sp[1] }, .{
            .read_buffer_size = 64,
            .observer = recorder.observer(),
        });
        var transport_live = true;
        defer if (transport_live) transport.deinit();
        try transport.startWriter();
        try transport.enqueueWrite(big);
        try waitBatchTaken(&transport);

        const most: [fd_io.max_fds_per_send]Fd = @splat(w);
        const too_many: [fd_io.max_fds_per_send + 1]Fd = @splat(w);
        const rest: [Transport.max_queued_fds - fd_io.max_fds_per_send]Fd = @splat(w);
        try testing.expectError(error.TooManyFds, transport.enqueueWriteWithFds("x", &too_many));
        try testing.expectError(error.FdsWithoutData, transport.enqueueWriteWithFds("", &most));
        try transport.enqueueWriteWithFds("most", &most);
        try transport.enqueueWriteWithFds("rest", &rest);
        try testing.expectEqual(Transport.max_queued_fds, transport.queueStats().fds);

        try testing.expectError(error.FdQueueFull, transport.enqueueWriteWithFds("one more", &.{w}));
        try testing.expectEqual(@as(usize, 1), recorder.count);
        const event = recorder.seen[0];
        try testing.expectEqual(events.Resource.attached_fds, event.resource);
        try testing.expectEqual(events.Source.unix, event.source);
        try testing.expectEqual(@as(?usize, 1), event.attempted_bytes);
        try testing.expectEqual(@as(?usize, Transport.max_queued_fds), event.limit);
        try testing.expectEqual(@as(anyerror, error.FdQueueFull), event.err);
        // The refusal costs no fds, and the transport stays open.
        try testing.expectEqual(Transport.max_queued_fds, transport.queueStats().fds);
        try testing.expect(!transport.isClosing());
        try transport.enqueueWrite("no fds still queue");

        pipes.closeWriters();
        transport.deinit();
        transport_live = false;
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "a dup that fails mid-message closes the dups already made" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(2);
        defer pipes.closeAll();

        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        defer transport.deinit();
        try transport.startWriter();

        // A number no fd has: a freshly closed one would be reused by the
        // first dups.
        const not_open: Fd = support.max_scanned_fd - 1;
        try testing.expect(!support.isOpen(not_open));
        const fds = [_]Fd{ pipes.write_ends[0], pipes.write_ends[1], not_open };
        try testing.expectError(error.InvalidFd, transport.enqueueWriteWithFds("bad", &fds));
        const stats = transport.queueStats();
        try testing.expectEqual(@as(usize, 0), stats.fds);
        try testing.expectEqual(@as(usize, 0), stats.items);

        // The two dups made before the bad fd are closed: nothing else
        // holds the write ends.
        pipes.closeWriters();
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "enqueueWriteWithFds fails cleanly at every allocation" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();

        const Run = struct {
            fn run(allocator: std.mem.Allocator, socket: Fd, fds: []const Fd) !void {
                var transport = try Transport.init(allocator, testing.io, .{ .handle = socket }, 64);
                // The socket belongs to the test: mark it closed so deinit
                // neither shuts it down nor closes it.
                transport.fd_closed.store(true, .release);
                defer transport.deinit();
                try transport.startWriter();
                try transport.enqueueWriteWithFds("oom", fds);
            }
        };
        try testing.checkAllAllocationFailures(testing.allocator, Run.run, .{ sp[0], pipes.writers() });

        // Whatever reached sp[1] is still in flight there; closing it lets
        // the kernel drop those copies.
        support.closeFd(sp[0]);
        pipes.closeWriters();
        support.closeFd(sp[1]);
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}
