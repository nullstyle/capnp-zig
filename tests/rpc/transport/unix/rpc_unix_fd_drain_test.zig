//! Drain mode: an AF_UNIX `Transport`/`Connection` closes every fd a peer
//! attaches, off the reading thread.
//!
//! Item 6 of docs/sprint-plan-2026-10-04.md (a security fix). Before drain
//! mode, `Transport.read` was a plain read. On macOS a plain read installs
//! the fds a peer attached into this process's fd table, where nothing can
//! find or close them: any local process that could connect leaked fds into
//! the server. These tests attach pipe write ends to valid frames from a raw
//! peer socket and require that every write end is closed (the pipe's read
//! end sees EOF) and that the fd table returns to its baseline.
//!
//! Linux and macOS run the tests; other targets skip them but compile the
//! file. The fd budgets here stay far below the macOS default soft limit
//! (256).

const std = @import("std");
const capnpc = @import("capnpc-zig");
const support = @import("fd_test_support.zig");

const testing = std.testing;
const posix = support.posix;
const sys = support.sys;
const fd_io = support.fd_io;
const tcp = capnpc.rpc.transport.tcp;
const Connection = tcp.Connection;
const Fd = support.Fd;

fn onMessage(conn: *Connection, _: []const u8) anyerror!void {
    const inbox: *support.Inbox = @ptrCast(@alignCast(conn.context().?));
    inbox.frames += 1;
    if (inbox.first_frame_ns == null) inbox.first_frame_ns = support.nowNs();
    if (inbox.close_after_frames) |n| {
        if (inbox.frames >= n) conn.close();
    }
}

fn onError(conn: *Connection, err: anyerror) void {
    const inbox: *support.Inbox = @ptrCast(@alignCast(conn.context().?));
    inbox.errors += 1;
    inbox.last_error = err;
}

fn onClose(_: *Connection) void {}

/// Starts the closer thread (it opens no fds) so the baseline the test takes
/// next already includes everything that lives for the whole process.
fn warmUp() !void {
    try fd_io.closer.ensureStarted();
    try support.waitCloserIdle(5000);
}

/// Runs a `Connection` over `socket` on this thread until the peer's EOF (or
/// a close), then tears it down.
fn runConnection(socket: Fd, inbox: *support.Inbox, recorder: *support.Recorder) !void {
    var conn = try Connection.init(testing.allocator, testing.io, .{ .handle = socket }, .{
        .observer = recorder.observer(),
    });
    conn.start(inbox, onMessage, onError, onClose);
    conn.run();
    conn.deinit();
}

test "a Connection on an AF_UNIX socket closes the pipe write end a peer attached to a frame (macOS leaked it)" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();

        const frame = try support.buildFrame(testing.allocator, 0xF00D);
        defer testing.allocator.free(frame);
        try support.sendWithFds(sp[0], frame, pipes.writers());
        pipes.closeWriters();
        support.closeFd(sp[0]); // EOF after the frame

        var inbox: support.Inbox = .{};
        var recorder: support.Recorder = .{};
        try runConnection(sp[1], &inbox, &recorder);

        try testing.expectEqual(@as(usize, 1), inbox.frames);
        try testing.expectEqual(@as(usize, 0), inbox.errors);
        // The only copy of the write end left was the one the peer attached.
        try pipes.expectAllWritersClosed();

        // One rejection event, from a `.unix` source, naming the one fd.
        try testing.expectEqual(@as(usize, 1), recorder.countErr(error.AttachedFdsRejected));
        try testing.expectEqual(@as(usize, 1), recorder.attemptedFor(error.AttachedFdsRejected));
        try testing.expectEqual(support.events.Source.unix, recorder.attached_fds[0].source);
        try testing.expect(recorder.connection_count != 0);
        for (recorder.connection_sources[0..recorder.connection_count]) |source| {
            try testing.expectEqual(support.events.Source.unix, source);
        }
    }
    try support.expectBackAtBaseline(before);
}

test "every attached fd on every read is closed: 20 frames with 3 fds each" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        var pipes = try support.Pipes.open(3);
        defer pipes.closeAll();

        const frame = try support.buildFrame(testing.allocator, 7);
        defer testing.allocator.free(frame);
        const frames = 20;
        // The same three write ends ride on every frame: the kernel installs
        // a new copy for each, and a pipe sees EOF only after all 20 copies
        // of its write end are closed.
        for (0..frames) |_| try support.sendWithFds(sp[0], frame, pipes.writers());
        pipes.closeWriters();
        support.closeFd(sp[0]);

        var inbox: support.Inbox = .{};
        var recorder: support.Recorder = .{};
        try runConnection(sp[1], &inbox, &recorder);

        try testing.expectEqual(@as(usize, frames), inbox.frames);
        try testing.expectEqual(@as(usize, 0), inbox.errors);
        try pipes.expectAllWritersClosed();
        try testing.expectEqual(@as(usize, frames * 3), recorder.attemptedFor(error.AttachedFdsRejected));
    }
    try support.expectBackAtBaseline(before);
}

test "Transport.deinit hands an AF_UNIX socket with unread fds to the closer" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(2);
        defer pipes.closeAll();

        var transport = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        try testing.expect(transport.drain != null);
        try testing.expectEqual(support.events.Source.unix, transport.source);

        // Nobody reads this: the fds are still in flight when the socket
        // closes, and the kernel closes them inside that final close.
        try support.sendWithFds(sp[0], "unread", pipes.writers());
        pipes.closeWriters();
        transport.deinit();

        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "drain-mode Transport.init fails cleanly at every allocation" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const sp = try support.socketPair();
    defer support.closeFd(sp[0]);
    defer support.closeFd(sp[1]);

    const Init = struct {
        fn run(allocator: std.mem.Allocator, socket: Fd) !void {
            var transport = try tcp.Transport.init(allocator, testing.io, .{ .handle = socket }, 64);
            try testing.expect(transport.drain != null);
            // The socket belongs to the test: mark it closed so deinit only
            // frees the transport's own state.
            transport.fd_closed.store(true, .release);
            transport.deinit();
        }
    };
    try testing.checkAllAllocationFailures(testing.allocator, Init.run, .{sp[1]});
}

test "a TCP Transport keeps the plain read path and reports .tcp" {
    const pair = try tcp.createLoopbackSocketPair(testing.io);
    defer tcp.closeFd(testing.io, pair[1]);
    var transport = try tcp.Transport.init(testing.allocator, testing.io, pair[0], 64);
    defer transport.deinit();
    try testing.expect(transport.drain == null);
    try testing.expectEqual(support.events.Source.tcp, transport.source);
}

test "drain-mode readTimeout keeps its deadline, then reads and drains" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        var pipes = try support.Pipes.open(1);
        defer pipes.closeAll();

        var transport = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        defer transport.deinit();

        const start = support.nowNs();
        try testing.expectError(error.Timeout, transport.readTimeout(.{ .duration = .{
            .raw = .fromMilliseconds(30),
            .clock = .awake,
        } }));
        try testing.expect(support.msSince(start) >= 25);

        try support.sendWithFds(sp[0], "hi", pipes.writers());
        pipes.closeWriters();
        const n = try transport.readTimeout(.{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        try testing.expectEqualStrings("hi", transport.read_buf[0..n]);
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "fd_io.recvWithFds waits out EAGAIN on a non-blocking socket instead of failing" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const sp = try support.socketPair();
    defer support.closeFd(sp[0]);
    defer support.closeFd(sp[1]);
    try support.setNonBlocking(sp[1], true);

    const Sender = struct {
        fn run(sock: Fd) void {
            support.sleepMs(50);
            support.sendWithFds(sock, "late", &.{}) catch {};
        }
    };
    const sender = try std.Thread.spawn(.{}, Sender.run, .{sp[0]});
    defer sender.join();

    var data: [16]u8 = undefined;
    var control: [256]u8 = undefined;
    var fds: [8]fd_io.Fd = undefined;
    const got = try fd_io.recvWithFds(sp[1], &data, &control, &fds);
    try testing.expectEqualStrings("late", data[0..got.data_len]);
    try testing.expectEqual(@as(usize, 0), got.fd_count);
}

// ---------------------------------------------------------------------------
// Truncation and the clamped parser
// ---------------------------------------------------------------------------

test "a control buffer too small for the attached fds: every visible fd is closed; the macOS leak is sent minus visible" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        defer support.closeFd(sp[1]);
        var pipes = try support.Pipes.open(4);
        defer pipes.closeAll();
        try support.sendWithFds(sp[0], "T", pipes.writers());
        pipes.closeWriters();

        // Room for one fd of the four. The transport never reads this way
        // (512 slots); this is the residual `fd_io` documents for callers
        // that pass a small buffer.
        const pre_read = support.FdSnapshot.take();
        var data: [8]u8 = undefined;
        var control: [fd_io.controlSpace(1)]u8 = undefined;
        var fds: [1]fd_io.Fd = undefined;
        const got = try fd_io.recvWithFds(sp[1], &data, &control, &fds);
        try testing.expectEqual(@as(usize, 1), got.data_len);
        try testing.expect(got.control_truncated);
        // The visible fd. macOS keeps the truncated header's full cmsg_len,
        // so only a parser that clamps it to the bytes present sees it.
        try testing.expectEqual(@as(usize, 1), got.fd_count);
        _ = fd_io.closer.handOff(null, fds[0..got.fd_count]);

        // The first attached fd is the visible one: its pipe sees EOF.
        try testing.expect(support.pipeWritersClosed(pipes.read_ends[0], support.closed_wait_ms));
        try support.waitCloserIdle(5000);

        var installed: [8]Fd = undefined;
        const n_installed = support.FdSnapshot.take().added(pre_read, &installed);
        if (support.is_macos) {
            // macOS installed all four. Three are invisible: the leak is
            // sent minus visible, and their pipes stay open.
            try testing.expectEqual(@as(usize, 4 - 1), n_installed);
            for (pipes.read_ends[1..pipes.count]) |r| {
                try testing.expect(!support.pipeWritersClosed(r, support.still_open_wait_ms));
            }
            // Clean up the leak by fd number, which only a test can know.
            for (installed[0..n_installed]) |fd| support.closeFd(fd);
        } else {
            // Linux closed the three that did not fit, inside recvmsg.
            try testing.expectEqual(@as(usize, 0), n_installed);
        }
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

/// One SCM_RIGHTS cmsg in std's native layout.
fn writeRights(buf: []u8, fds: []const fd_io.Fd) usize {
    const cmsg = std.Io.net.cmsg;
    const total = cmsg.space(fds.len * @sizeOf(fd_io.Fd));
    @memset(buf[0..total], 0);
    var header = std.mem.zeroes(posix.cmsghdr);
    header.len = cmsg.len(@intCast(fds.len * @sizeOf(fd_io.Fd)));
    header.level = posix.SOL.SOCKET;
    header.type = posix.SCM.RIGHTS;
    @memcpy(buf[0..@sizeOf(posix.cmsghdr)], std.mem.asBytes(&header));
    const data_offset = cmsg.len(0);
    @memcpy(buf[data_offset..][0 .. fds.len * @sizeOf(fd_io.Fd)], std.mem.sliceAsBytes(fds));
    return total;
}

test "fd_io.parseRights clamps a truncated header and walks every SCM_RIGHTS message" {
    if (!support.supported) return error.SkipZigTest;
    const cmsg = std.Io.net.cmsg;
    // Every byte of each fd differs, so a wrong byte order decodes to a
    // different number.
    const sent = [_]fd_io.Fd{ 7, 0x0102, 0x01_0203 };
    var buf: [256]u8 align(8) = undefined;
    var out: [8]fd_io.Fd = undefined;

    // A whole message.
    const one = writeRights(&buf, &sent);
    try testing.expectEqual(@as(usize, 3), fd_io.parseRights(buf[0..one], &out));
    try testing.expectEqualSlices(fd_io.Fd, &sent, out[0..3]);

    // Truncated after the first fd, with cmsg_len still claiming three (what
    // macOS writes): the clamped parser reports the one fd present.
    const truncated_len = cmsg.len(0) + @sizeOf(fd_io.Fd);
    try testing.expectEqual(@as(usize, 1), fd_io.parseRights(buf[0..truncated_len], &out));
    try testing.expectEqual(sent[0], out[0]);

    // `fds_out` bounds what is reported.
    try testing.expectEqual(@as(usize, 2), fd_io.parseRights(buf[0..one], out[0..2]));

    // Two messages back to back, the first a non-RIGHTS cmsg that is skipped.
    var two_buf: [256]u8 align(8) = undefined;
    const first_len = writeRights(&two_buf, &.{ 11, 12 });
    var other = std.mem.zeroes(posix.cmsghdr);
    @memcpy(std.mem.asBytes(&other), two_buf[0..@sizeOf(posix.cmsghdr)]);
    other.type = posix.SCM.RIGHTS + 100;
    @memcpy(two_buf[0..@sizeOf(posix.cmsghdr)], std.mem.asBytes(&other));
    const second_len = writeRights(two_buf[first_len..], &.{ 21, 22, 23 });
    try testing.expectEqual(@as(usize, 3), fd_io.parseRights(two_buf[0 .. first_len + second_len], &out));
    try testing.expectEqualSlices(fd_io.Fd, &.{ 21, 22, 23 }, out[0..3]);

    // A hostile zero cmsg_len stops the walk instead of looping on it.
    var zero_buf: [64]u8 align(8) = @splat(0);
    try testing.expectEqual(@as(usize, 0), fd_io.parseRights(&zero_buf, &out));

    // A cmsg_len far past the buffer is clamped, not trusted. (The slice
    // ends at CMSG_LEN, as `recvWithFds` offers it, not at CMSG_SPACE, whose
    // padding would read as one more fd on 64-bit Linux.)
    var huge = std.mem.zeroes(posix.cmsghdr);
    @memcpy(std.mem.asBytes(&huge), buf[0..@sizeOf(posix.cmsghdr)]);
    huge.len = std.math.maxInt(@FieldType(posix.cmsghdr, "len"));
    @memcpy(buf[0..@sizeOf(posix.cmsghdr)], std.mem.asBytes(&huge));
    const exact_len = cmsg.len(0) + sent.len * @sizeOf(fd_io.Fd);
    try testing.expectEqual(@as(usize, 3), fd_io.parseRights(buf[0..exact_len], &out));
    try testing.expectEqualSlices(fd_io.Fd, &sent, out[0..3]);
}

// ---------------------------------------------------------------------------
// Fd-table exhaustion and the closer-queue bound
// ---------------------------------------------------------------------------

test "EMFILE: the connection survives, the drop is reported once, and no fd stays open" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const sp = try support.socketPair();
        var pipes = try support.Pipes.open(5);
        defer pipes.closeAll();

        const frame_a = try support.buildFrame(testing.allocator, 1);
        defer testing.allocator.free(frame_a);
        const frame_b = try support.buildFrame(testing.allocator, 2);
        defer testing.allocator.free(frame_b);
        try support.sendWithFds(sp[0], frame_a, pipes.writers());
        try support.sendWithFds(sp[0], frame_b, &.{});
        pipes.closeWriters();
        support.closeFd(sp[0]);

        var inbox: support.Inbox = .{};
        var recorder: support.Recorder = .{};
        var conn = try Connection.init(testing.allocator, testing.io, .{ .handle = sp[1] }, .{
            .observer = recorder.observer(),
        });
        conn.start(&inbox, onMessage, onError, onClose);

        {
            // Leave exactly two free fd slots: set the soft limit just above
            // the highest open fd, fill every hole below it, free two.
            const saved = try posix.getrlimit(.NOFILE);
            defer posix.setrlimit(.NOFILE, saved) catch {};
            var lowered = saved;
            lowered.cur = support.FdSnapshot.take().highest() + 1 + 2;
            try posix.setrlimit(.NOFILE, lowered);
            var fillers: [256]Fd = undefined;
            var n_fillers: usize = 0;
            defer for (fillers[0..n_fillers]) |fd| support.closeFd(fd);
            while (n_fillers < fillers.len) {
                const rc = sys.dup(pipes.read_ends[0]);
                if (posix.errno(rc) != .SUCCESS) break;
                fillers[n_fillers] = @intCast(rc);
                n_fillers += 1;
            }
            try testing.expect(n_fillers >= 2 and n_fillers < fillers.len);
            support.closeFd(fillers[n_fillers - 1]);
            support.closeFd(fillers[n_fillers - 2]);
            n_fillers -= 2;

            conn.run();
        }
        conn.deinit();

        // Both frames dispatched: the connection survived the fd table.
        try testing.expectEqual(@as(usize, 2), inbox.frames);
        try testing.expectEqual(@as(usize, 0), inbox.errors);
        try testing.expectEqual(@as(?anyerror, null), recorder.close_err);
        if (support.is_macos) {
            // macOS fails the first recvmsg and installs nothing; the one
            // retry returns the data and the kernel drops the fds. One event,
            // one retry: no spin.
            try testing.expectEqual(@as(usize, 1), recorder.countErr(error.ProcessFdQuotaExceeded));
            try testing.expectEqual(@as(usize, 0), recorder.countErr(error.AttachedFdsRejected));
        } else {
            // Linux installs the two that fit and sets MSG_CTRUNC; it closes
            // the other three inside recvmsg (the documented residual).
            try testing.expectEqual(@as(usize, 2), recorder.attemptedFor(error.AttachedFdsRejected));
            try testing.expectEqual(@as(usize, 1), recorder.countErr(error.AttachedFdsTruncated));
        }
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "fds past the closer-queue bound close the connection that sent them, with a typed cause" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const previous_limit = fd_io.closer.setQueueLimit(2);
        defer _ = fd_io.closer.setQueueLimit(previous_limit);

        const sp = try support.socketPair();
        var pipes = try support.Pipes.open(4);
        defer pipes.closeAll();

        const frame = try support.buildFrame(testing.allocator, 3);
        defer testing.allocator.free(frame);
        try support.sendWithFds(sp[0], frame, pipes.writers());
        try support.sendWithFds(sp[0], frame, &.{});
        pipes.closeWriters();
        // EOF after the second frame, so a connection that ignored the bound
        // reads both frames and ends cleanly (a failure, not a hang).
        support.closeFd(sp[0]);

        var inbox: support.Inbox = .{};
        var recorder: support.Recorder = .{};
        try runConnection(sp[1], &inbox, &recorder);

        // Four fds in one read put the queue past 2. The read's bytes are
        // dropped and the connection closes with a typed cause before it
        // reaches the second frame or the EOF.
        try testing.expectEqual(@as(usize, 0), inbox.frames);
        try testing.expectEqual(@as(?anyerror, error.SystemResources), inbox.last_error);
        try testing.expectEqual(@as(?anyerror, error.SystemResources), recorder.close_err);
        try testing.expectEqual(@as(usize, 1), recorder.countErr(error.FdCloseQueueFull));
        for (recorder.attached_fds[0..recorder.attached_count]) |r| {
            if (r.err != error.FdCloseQueueFull) continue;
            try testing.expectEqual(@as(?usize, 2), r.limit);
            try testing.expect((r.attempted orelse 0) > 2);
        }
        // The fds already received still went to the closer.
        try pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}
