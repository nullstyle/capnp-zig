//! Fd passing, receive side: exact-boundary reads.
//!
//! Item 11 of docs/sprint-plan-2026-10-04.md (owner default D2). With
//! `enableFdPassing`, an AF_UNIX `Transport` reads one Cap'n Proto frame at a
//! time (the head, the rest of the segment table, the body), and no `recvmsg`
//! crosses the end of the current part. So every fd belongs to the frame
//! whose bytes carried it, on Linux and on macOS. Bulk reads cannot do that:
//! they attach an fd to a read's last bytes on Linux and to its first byte
//! on macOS (FD-0).
//!
//! These tests replay FD-0's exact-boundary cases (E1-E4, E3, E3b) through
//! the real `Connection` and `Transport`, pin the policy (the per-message
//! cap, two fd batches in one frame, CTRUNC and EMFILE, hostile headers,
//! frames never dispatched, fds nobody took), and fuzz random read splits,
//! random fd placement and hostile headers. Every fd sent is a pipe write
//! end: its inode says which pipe it is, and the pipe's read end sees EOF
//! once every copy of it is closed.
//!
//! Linux and macOS run the tests; other targets compile the file and run
//! only the TCP test. Fd counts stay far below the macOS default soft limit
//! (256).

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
const Connection = tcp.Connection;
const Framer = capnpc.rpc.wire.framing.Framer;
const Fd = support.Fd;

/// Starts the closer threads (they open no fds) so the baseline the test
/// takes next already includes everything that lives for the whole process.
fn warmUp() !void {
    try fd_io.closer.ensureStarted();
    try support.waitCloserIdle(5000);
}

/// The file `fd` refers to, as its inode number. Every pipe has its own, and
/// a received copy of a write end has the write end's.
fn fdIdentity(fd: Fd) !u64 {
    if (comptime support.is_linux) {
        const linux = std.os.linux;
        var stx: linux.Statx = undefined;
        const rc = linux.statx(fd, "", linux.AT.EMPTY_PATH, .{ .INO = true }, &stx);
        if (linux.errno(rc) != .SUCCESS) return error.SyscallFailed;
        return stx.ino;
    } else if (comptime support.is_macos) {
        var st: std.c.Stat = undefined;
        _ = try support.check(std.c.fstat(fd, &st), "fstat");
        return st.ino;
    } else {
        return error.Unsupported;
    }
}

/// A one-segment frame of `total` bytes (8 + a multiple of 8) whose body is
/// all `tag`.
fn frameBytes(buf: []u8, total: usize, tag: u8) []u8 {
    std.debug.assert(total >= 8 and (total - 8) % 8 == 0);
    std.mem.writeInt(u32, buf[0..4], 0, .little);
    std.mem.writeInt(u32, buf[4..8], @intCast((total - 8) / 8), .little);
    @memset(buf[8..total], tag);
    return buf[0..total];
}

/// A raw peer: one end of a socket pair that sends frames with pipe write
/// ends attached. The other end goes to the `Transport` or `Connection`
/// under test, which owns it from then on.
const RawPeer = struct {
    sp: [2]Fd,
    pipes: support.Pipes,
    ids: [16]u64 = @splat(0),

    fn open(pipe_count: usize) !RawPeer {
        const sp = try support.socketPair();
        errdefer support.closeFd(sp[0]);
        errdefer support.closeFd(sp[1]);
        var self: RawPeer = .{ .sp = sp, .pipes = try support.Pipes.open(pipe_count) };
        errdefer self.pipes.closeAll();
        for (self.pipes.writers(), 0..) |w, i| self.ids[i] = try fdIdentity(w);
        return self;
    }

    /// Sends `bytes` with the write ends of `pipe_indexes` attached to the
    /// first byte.
    fn send(self: *RawPeer, bytes: []const u8, pipe_indexes: []const usize) !void {
        var fds: [16]Fd = undefined;
        for (pipe_indexes, 0..) |p, i| fds[i] = self.pipes.write_ends[p];
        try fd_io.sendWithFds(self.sp[0], bytes, fds[0..pipe_indexes.len]);
    }

    /// Closes the sending end (the receiver then sees EOF after what was
    /// sent) and the test's own copies of the write ends.
    fn finishSending(self: *RawPeer) void {
        self.pipes.closeWriters();
        if (self.sp[0] >= 0) support.closeFd(self.sp[0]);
        self.sp[0] = -1;
    }

    fn deinit(self: *RawPeer) void {
        self.finishSending();
        self.pipes.closeAll();
    }
};

/// What `on_message` saw for one frame.
const Seen = struct {
    len: usize,
    /// The first body byte (0 for a frame with no body).
    tag: u8,
    fd_count: usize,
    /// Identities of the fds taken, in order.
    ids: [8]u64 = @splat(0),
};

const Capture = struct {
    seen: [16]Seen = undefined,
    count: usize = 0,
    /// Take every fd of each frame (and close it). Otherwise leave them all
    /// for the transport to release after dispatch.
    take: bool = true,
    errors: usize = 0,
    last_error: ?anyerror = null,
    dispatched: std.atomic.Value(usize) = .init(0),

    fn expectFrame(self: *const Capture, index: usize, tag: u8, ids: []const u64) !void {
        if (index >= self.count) {
            std.debug.print("frame {d} was not dispatched ({d} were)\n", .{ index, self.count });
            return error.FrameMissing;
        }
        const s = self.seen[index];
        testing.expectEqual(tag, s.tag) catch |err| {
            std.debug.print("frame {d}: tag {c}, want {c}\n", .{ index, s.tag, tag });
            return err;
        };
        testing.expectEqual(ids.len, s.fd_count) catch |err| {
            std.debug.print("frame {d} ({c}): {d} fd(s), want {d}\n", .{ index, tag, s.fd_count, ids.len });
            return err;
        };
        try testing.expectEqualSlices(u64, ids, s.ids[0..ids.len]);
    }
};

fn onMessage(conn: *Connection, frame: []const u8) anyerror!void {
    const capture: *Capture = @ptrCast(@alignCast(conn.context().?));
    var seen: Seen = .{
        .len = frame.len,
        .tag = if (frame.len > 8) frame[8] else 0,
        .fd_count = conn.transport.frameFdCount(),
    };
    if (capture.take) {
        for (0..@min(seen.fd_count, seen.ids.len)) |i| {
            const fd = conn.transport.takeFrameFd(i) orelse return error.FrameFdMissing;
            defer support.closeFd(fd);
            seen.ids[i] = try fdIdentity(fd);
        }
    }
    if (capture.count < capture.seen.len) {
        capture.seen[capture.count] = seen;
        capture.count += 1;
    }
    _ = capture.dispatched.fetchAdd(1, .release);
}

fn onError(conn: *Connection, err: anyerror) void {
    const capture: *Capture = @ptrCast(@alignCast(conn.context().?));
    capture.errors += 1;
    capture.last_error = err;
}

fn onClose(_: *Connection) void {}

const RunOptions = struct {
    read_buffer_size: usize = 64 * 1024,
    max_fds: u8 = 8,
    max_buffered_frame_bytes: usize = Framer.default_max_buffered_bytes,
    /// With a tick the run loop polls the socket and reads only once data
    /// is there, so an idle connection does not call `read`.
    tick_interval_ms: ?u32 = null,
    allocator: std.mem.Allocator = testing.allocator,
};

/// Runs a `Connection` with fd passing on over `socket` on this thread until
/// the peer's EOF (or an error), then tears it down.
fn runConnection(socket: Fd, options: RunOptions, capture: *Capture, recorder: *support.Recorder) !void {
    var conn = try Connection.init(options.allocator, testing.io, .{ .handle = socket }, .{
        .read_buffer_size = options.read_buffer_size,
        .observer = recorder.observer(),
        .max_buffered_frame_bytes = options.max_buffered_frame_bytes,
        .tick_interval_ms = options.tick_interval_ms,
    });
    defer conn.deinit();
    try conn.enableFdPassing(options.max_fds);
    conn.start(capture, onMessage, onError, onClose);
    conn.run();
}

// ---------------------------------------------------------------------------
// Every target
// ---------------------------------------------------------------------------

test "fd passing is refused on a TCP transport, and its frame-fd calls are empty" {
    const pair = try tcp.createLoopbackSocketPair(testing.io);
    defer tcp.closeFd(testing.io, pair[1]);
    var transport = try Transport.init(testing.allocator, testing.io, pair[0], 64);
    defer transport.deinit();
    try testing.expectError(error.FdPassingUnsupported, transport.enableFdPassing(.{ .max_fds_per_message = 4 }));
    try testing.expectEqual(@as(usize, 0), transport.frameFdCount());
    try testing.expectEqual(@as(?fd_io.Fd, null), transport.takeFrameFd(0));
    transport.releaseFrameFds();
}

// ---------------------------------------------------------------------------
// FD-0's exact-boundary cases, through the transport
// ---------------------------------------------------------------------------

const small_len = 104; // 8 header + 96 body bytes (12 words)

/// E1-E4, E3 and E3b, each over a `Connection` reading with
/// `read_buffer_size`. A buffer smaller than a frame makes a bulk read stop
/// inside a frame, which is where Linux would glue the next frame's fd onto
/// it. `large` runs E4 (256 KiB), too slow one byte at a time.
fn exactBoundaryCases(read_buffer_size: usize, large: bool) !void {
    const options: RunOptions = .{ .read_buffer_size = read_buffer_size };
    var a_buf: [small_len]u8 = undefined;
    var b_buf: [small_len]u8 = undefined;
    var c_buf: [small_len]u8 = undefined;
    var d_buf: [small_len]u8 = undefined;
    const a = frameBytes(&a_buf, small_len, 'A');
    const b = frameBytes(&b_buf, small_len, 'B');
    const c = frameBytes(&c_buf, small_len, 'C');
    const d = frameBytes(&d_buf, small_len, 'D');

    { // E1: A | B+fd | C
        var peer = try RawPeer.open(1);
        defer peer.deinit();
        try peer.send(a, &.{});
        try peer.send(b, &.{0});
        try peer.send(c, &.{});
        peer.finishSending();
        var capture: Capture = .{};
        var recorder: support.Recorder = .{};
        try runConnection(peer.sp[1], options, &capture, &recorder);
        try testing.expectEqual(@as(usize, 0), capture.errors);
        try testing.expectEqual(@as(usize, 3), capture.count);
        try capture.expectFrame(0, 'A', &.{});
        try capture.expectFrame(1, 'B', &.{peer.ids[0]});
        try capture.expectFrame(2, 'C', &.{});
        try peer.pipes.expectAllWritersClosed();
    }
    { // E2: B+1fd | D+2fd, back to back
        var peer = try RawPeer.open(3);
        defer peer.deinit();
        try peer.send(b, &.{0});
        try peer.send(d, &.{ 1, 2 });
        peer.finishSending();
        var capture: Capture = .{};
        var recorder: support.Recorder = .{};
        try runConnection(peer.sp[1], options, &capture, &recorder);
        try testing.expectEqual(@as(usize, 0), capture.errors);
        try testing.expectEqual(@as(usize, 2), capture.count);
        try capture.expectFrame(0, 'B', &.{peer.ids[0]});
        try capture.expectFrame(1, 'D', &.{ peer.ids[1], peer.ids[2] });
        try peer.pipes.expectAllWritersClosed();
    }
    { // E3: hostile, fd attached mid-frame (A[0..52] plain, A[52..]+fd), then C
        var peer = try RawPeer.open(1);
        defer peer.deinit();
        try peer.send(a[0..52], &.{});
        try peer.send(a[52..], &.{0});
        try peer.send(c, &.{});
        peer.finishSending();
        var capture: Capture = .{};
        var recorder: support.Recorder = .{};
        try runConnection(peer.sp[1], options, &capture, &recorder);
        try testing.expectEqual(@as(usize, 0), capture.errors);
        try testing.expectEqual(@as(usize, 2), capture.count);
        try capture.expectFrame(0, 'A', &.{peer.ids[0]});
        try capture.expectFrame(1, 'C', &.{});
        try peer.pipes.expectAllWritersClosed();
    }
    { // E3b: hostile, fd attached at header byte 4 (A[0..4], A[4..]+fd), then C
        var peer = try RawPeer.open(1);
        defer peer.deinit();
        try peer.send(a[0..4], &.{});
        try peer.send(a[4..], &.{0});
        try peer.send(c, &.{});
        peer.finishSending();
        var capture: Capture = .{};
        var recorder: support.Recorder = .{};
        try runConnection(peer.sp[1], options, &capture, &recorder);
        try testing.expectEqual(@as(usize, 0), capture.errors);
        try testing.expectEqual(@as(usize, 2), capture.count);
        try capture.expectFrame(0, 'A', &.{peer.ids[0]});
        try capture.expectFrame(1, 'C', &.{});
        try peer.pipes.expectAllWritersClosed();
    }
    if (large) { // E4: large B (256 KiB body) + fd, sent in partial chunks by a writer thread, then C
        var peer = try RawPeer.open(1);
        defer peer.deinit();
        const big_len = 8 + 256 * 1024;
        const big_buf = try testing.allocator.alloc(u8, big_len);
        defer testing.allocator.free(big_buf);
        const big = frameBytes(big_buf, big_len, 'B');
        const Writer = struct {
            fn run(sock: Fd, big_frame: []const u8, fd: Fd, tail: []const u8, result: *?anyerror) void {
                defer support.closeFd(sock);
                fd_io.sendWithFds(sock, big_frame, &.{fd}) catch |err| {
                    result.* = err;
                    return;
                };
                fd_io.sendWithFds(sock, tail, &.{}) catch |err| {
                    result.* = err;
                };
            }
        };
        var writer_result: ?anyerror = null;
        // The writer owns the sending end from here and closes it.
        const sock = peer.sp[0];
        peer.sp[0] = -1;
        const writer = try std.Thread.spawn(.{}, Writer.run, .{ sock, big, peer.pipes.write_ends[0], c, &writer_result });
        var capture: Capture = .{};
        var recorder: support.Recorder = .{};
        const run_result = runConnection(peer.sp[1], options, &capture, &recorder);
        writer.join();
        try run_result;
        if (writer_result) |err| return err;
        try testing.expectEqual(@as(usize, 0), capture.errors);
        try testing.expectEqual(@as(usize, 2), capture.count);
        try testing.expectEqual(@as(usize, big_len), capture.seen[0].len);
        try capture.expectFrame(0, 'B', &.{peer.ids[0]});
        try capture.expectFrame(1, 'C', &.{});
        peer.finishSending();
        try peer.pipes.expectAllWritersClosed();
    }
}

test "exact-boundary reads put each fd in the frame whose bytes carried it (E1-E4, E3, E3b), 64 KiB reads" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    try exactBoundaryCases(64 * 1024, true);
    try support.expectBackAtBaseline(before);
}

test "exact-boundary reads put each fd in the frame whose bytes carried it (E1-E4, E3, E3b), reads smaller than a frame" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    // 60 bytes: a read ends inside each 104-byte frame. A bulk read that
    // reached past A's end would take B's fd with A's tail on Linux.
    try exactBoundaryCases(60, true);
    try exactBoundaryCases(1, false);
    try support.expectBackAtBaseline(before);
}

test "with fd passing off (the default) a read is still a bulk read, and every fd is closed" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var peer = try RawPeer.open(1);
        defer peer.deinit();
        var a_buf: [small_len]u8 = undefined;
        var b_buf: [small_len]u8 = undefined;
        var both: [2 * small_len]u8 = undefined;
        @memcpy(both[0..small_len], frameBytes(&a_buf, small_len, 'A'));
        @memcpy(both[small_len..], frameBytes(&b_buf, small_len, 'B'));

        var plain = try Transport.init(testing.allocator, testing.io, .{ .handle = peer.sp[1] }, 4096);
        defer plain.deinit();
        try peer.send(&both, &.{0});
        // Drain mode: one read takes both frames, and the fd goes to the
        // closer.
        try testing.expectEqual(@as(usize, 2 * small_len), try plain.read());
        try testing.expectEqual(@as(usize, 0), plain.frameFdCount());
        peer.finishSending();
        try peer.pipes.expectAllWritersClosed();
    }
    {
        var peer = try RawPeer.open(1);
        defer peer.deinit();
        var a_buf: [small_len]u8 = undefined;
        var b_buf: [small_len]u8 = undefined;
        var both: [2 * small_len]u8 = undefined;
        @memcpy(both[0..small_len], frameBytes(&a_buf, small_len, 'A'));
        @memcpy(both[small_len..], frameBytes(&b_buf, small_len, 'B'));

        var passing = try Transport.init(testing.allocator, testing.io, .{ .handle = peer.sp[1] }, 4096);
        defer passing.deinit();
        try passing.enableFdPassing(.{ .max_fds_per_message = 4 });
        try peer.send(&both, &.{0});
        // Fd passing: the head, then the body; never into the next frame.
        try testing.expectEqual(@as(usize, 8), try passing.read());
        try testing.expectEqual(@as(usize, 0), passing.frameFdCount());
        try testing.expectEqual(@as(usize, small_len - 8), try passing.read());
        try testing.expectEqual(@as(usize, 1), passing.frameFdCount());
        try testing.expectEqual(@as(usize, 8), try passing.read());
        // That read released A's untaken fd.
        try testing.expectEqual(@as(usize, 0), passing.frameFdCount());
        peer.finishSending();
        try peer.pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

// ---------------------------------------------------------------------------
// The policy
// ---------------------------------------------------------------------------

test "more fds than max_fds_per_message: the frame keeps the first ones, the extras are closed at once" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var peer = try RawPeer.open(4);
        defer peer.deinit();
        var a_buf: [small_len]u8 = undefined;
        var b_buf: [small_len]u8 = undefined;
        try peer.send(frameBytes(&a_buf, small_len, 'A'), &.{ 0, 1, 2, 3 });
        try peer.send(frameBytes(&b_buf, small_len, 'B'), &.{});
        peer.finishSending();
        var capture: Capture = .{};
        var recorder: support.Recorder = .{};
        try runConnection(peer.sp[1], .{ .max_fds = 2 }, &capture, &recorder);
        try testing.expectEqual(@as(usize, 0), capture.errors);
        try testing.expectEqual(@as(usize, 2), capture.count);
        try capture.expectFrame(0, 'A', &.{ peer.ids[0], peer.ids[1] });
        try capture.expectFrame(1, 'B', &.{});
        try testing.expectEqual(@as(usize, 1), recorder.countErr(error.AttachedFdsOverLimit));
        for (recorder.attached_fds[0..recorder.attached_count]) |r| {
            if (r.err != error.AttachedFdsOverLimit) continue;
            try testing.expectEqual(@as(?usize, 4), r.attempted);
            try testing.expectEqual(@as(?usize, 2), r.limit);
        }
        try testing.expectEqual(@as(usize, 0), recorder.protocol_count);
        try peer.pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "fds in two batches within one frame: a protocol error closes every fd and the connection" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var peer = try RawPeer.open(4);
        defer peer.deinit();
        var a_buf: [small_len]u8 = undefined;
        var b_buf: [small_len]u8 = undefined;
        var c_buf: [small_len]u8 = undefined;
        const b = frameBytes(&b_buf, small_len, 'B');
        try peer.send(frameBytes(&a_buf, small_len, 'A'), &.{0});
        try peer.send(b[0..30], &.{1});
        try peer.send(b[30..], &.{2});
        try peer.send(frameBytes(&c_buf, small_len, 'C'), &.{3});
        peer.finishSending();
        var capture: Capture = .{};
        var recorder: support.Recorder = .{};
        try runConnection(peer.sp[1], .{}, &capture, &recorder);
        // A is dispatched; B and everything after it are not.
        try testing.expectEqual(@as(usize, 1), capture.count);
        try capture.expectFrame(0, 'A', &.{peer.ids[0]});
        try testing.expectEqual(@as(?anyerror, error.ConnectionResetByPeer), capture.last_error);
        try testing.expectEqual(@as(usize, 1), recorder.protocol_count);
        try testing.expectEqual(@as(anyerror, error.MultipleAttachedFdBatches), recorder.protocol_errs[0]);
        // B's fds and C's (never read: the kernel closes them with the
        // socket) are all closed.
        try peer.pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "a hostile segment count ends the connection at the head, before the framer sees it" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    for ([_]u32{ 512, 0xFFFF_FFFF }) |raw_count_minus_one| {
        var peer = try RawPeer.open(2);
        defer peer.deinit();
        var a_buf: [small_len]u8 = undefined;
        try peer.send(frameBytes(&a_buf, small_len, 'A'), &.{});
        var hostile: [64]u8 = @splat(0x5a);
        std.mem.writeInt(u32, hostile[0..4], raw_count_minus_one, .little);
        std.mem.writeInt(u32, hostile[4..8], 1, .little);
        try peer.send(&hostile, &.{ 0, 1 });
        peer.finishSending();
        var capture: Capture = .{};
        var recorder: support.Recorder = .{};
        try runConnection(peer.sp[1], .{}, &capture, &recorder);
        try testing.expectEqual(@as(usize, 1), capture.count);
        try capture.expectFrame(0, 'A', &.{});
        try testing.expectEqual(@as(?anyerror, error.ConnectionResetByPeer), capture.last_error);
        try testing.expectEqual(@as(usize, 1), recorder.protocol_count);
        try testing.expectEqual(@as(anyerror, error.InvalidFrame), recorder.protocol_errs[0]);
        try peer.pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

/// Tracks the bytes an allocator holds, and the most it ever held.
const PeakAllocator = struct {
    inner: std.mem.Allocator,
    live: usize = 0,
    peak: usize = 0,

    fn grew(self: *PeakAllocator, added: usize, removed: usize) void {
        self.live = self.live + added - removed;
        self.peak = @max(self.peak, self.live);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        const result = self.inner.vtable.alloc(self.inner.ptr, len, alignment, ret_addr) orelse return null;
        self.grew(len, 0);
        return result;
    }
    fn resize(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        if (!self.inner.vtable.resize(self.inner.ptr, buf, alignment, new_len, ret_addr)) return false;
        self.grew(new_len, buf.len);
        return true;
    }
    fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        const result = self.inner.vtable.remap(self.inner.ptr, buf, alignment, new_len, ret_addr) orelse return null;
        self.grew(new_len, buf.len);
        return result;
    }
    fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        self.inner.vtable.free(self.inner.ptr, buf, alignment, ret_addr);
        self.grew(0, buf.len);
    }
    fn allocator(self: *PeakAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
};

test "a header past max_buffered_frame_bytes ends the connection before any byte of its body is read or buffered" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    const limit = 4096;
    // A one-segment header claiming 1 MiB, and a two-segment one whose
    // sizes add up past the limit; each followed by junk the framer would
    // buffer (it fits under the limit) if the transport read the body.
    const Oversize = struct { count_minus_one: u32, sizes: [2]u32 };
    for ([_]Oversize{
        .{ .count_minus_one = 0, .sizes = .{ 128 * 1024, 0 } },
        .{ .count_minus_one = 1, .sizes = .{ 300, 300 } },
    }) |h| {
        var peer = try RawPeer.open(1);
        defer peer.deinit();
        var a_buf: [small_len]u8 = undefined;
        try peer.send(frameBytes(&a_buf, small_len, 'A'), &.{});
        var hostile: [16 + 2000]u8 = @splat(0x5a);
        std.mem.writeInt(u32, hostile[0..4], h.count_minus_one, .little);
        std.mem.writeInt(u32, hostile[4..8], h.sizes[0], .little);
        std.mem.writeInt(u32, hostile[8..12], h.sizes[1], .little);
        try peer.send(&hostile, &.{0});
        peer.finishSending();

        var peak: PeakAllocator = .{ .inner = testing.allocator };
        var capture: Capture = .{};
        var recorder: support.Recorder = .{};
        var conn = try Connection.init(peak.allocator(), testing.io, .{ .handle = peer.sp[1] }, .{
            .read_buffer_size = 4096,
            .observer = recorder.observer(),
            .max_buffered_frame_bytes = limit,
        });
        try conn.enableFdPassing(4);
        conn.start(&capture, onMessage, onError, onClose);
        // Everything allocated from here on is framing: the frames' bytes.
        const fixed = peak.live;
        peak.peak = fixed;
        conn.run();
        const framing_peak = peak.peak - fixed;
        conn.deinit();

        // Frame A (104 bytes, buffered and copied out once) is all the
        // framer ever held. Reading the hostile body would have buffered
        // its 2000 junk bytes.
        testing.expect(framing_peak < 1024) catch |err| {
            std.debug.print("framing peak {d} bytes\n", .{framing_peak});
            return err;
        };
        try testing.expectEqual(@as(usize, 0), peak.live);
        try testing.expectEqual(@as(usize, 1), capture.count);
        try capture.expectFrame(0, 'A', &.{});
        try testing.expectEqual(@as(?anyerror, error.ConnectionResetByPeer), capture.last_error);
        try testing.expectEqual(@as(usize, 1), recorder.protocol_count);
        try testing.expectEqual(@as(anyerror, error.FrameTooLarge), recorder.protocol_errs[0]);
        try peer.pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "EMFILE or MSG_CTRUNC: the frame arrives with no fds, the drop is reported, the connection stays" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var peer = try RawPeer.open(5);
        defer peer.deinit();
        var a_buf: [small_len]u8 = undefined;
        var b_buf: [small_len]u8 = undefined;
        try peer.send(frameBytes(&a_buf, small_len, 'A'), &.{ 0, 1, 2, 3, 4 });
        try peer.send(frameBytes(&b_buf, small_len, 'B'), &.{});
        peer.finishSending();

        var capture: Capture = .{};
        var recorder: support.Recorder = .{};
        var conn = try Connection.init(testing.allocator, testing.io, .{ .handle = peer.sp[1] }, .{
            .observer = recorder.observer(),
        });
        try conn.enableFdPassing(8);
        conn.start(&capture, onMessage, onError, onClose);
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
                const rc = sys.dup(peer.pipes.read_ends[0]);
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

        // Both frames dispatched, A with no fds: the connection survived.
        try testing.expectEqual(@as(usize, 0), capture.errors);
        try testing.expectEqual(@as(?anyerror, null), recorder.close_err);
        try testing.expectEqual(@as(usize, 2), capture.count);
        try capture.expectFrame(0, 'A', &.{});
        try capture.expectFrame(1, 'B', &.{});
        try testing.expectEqual(@as(usize, 0), recorder.protocol_count);
        if (support.is_macos) {
            // macOS fails the first recvmsg and installs nothing; the one
            // retry returns the data, and the kernel dropped the fds.
            try testing.expectEqual(@as(usize, 1), recorder.countErr(error.ProcessFdQuotaExceeded));
        } else {
            // Linux installs the two that fit and sets MSG_CTRUNC; the frame
            // keeps neither, and the closer closes both.
            try testing.expectEqual(@as(usize, 1), recorder.countErr(error.AttachedFdsTruncated));
            try testing.expectEqual(@as(usize, 2), recorder.attemptedFor(error.AttachedFdsTruncated));
        }
        try peer.pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "fds on_message does not take are closed right after dispatch, while the connection waits for more" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var peer = try RawPeer.open(2);
        defer peer.deinit();
        var a_buf: [small_len]u8 = undefined;
        try peer.send(frameBytes(&a_buf, small_len, 'A'), &.{ 0, 1 });
        peer.pipes.closeWriters();
        // The sending end stays open: after A the connection blocks in its
        // next read. A watcher waits for A's dispatch, checks that both
        // write ends are closed by then, and only then ends the stream.
        const Watcher = struct {
            fn run(capture: *Capture, pipes: *const support.Pipes, sock: Fd, closed_in_time: *bool) void {
                defer support.closeFd(sock);
                const start = support.nowNs();
                while (capture.dispatched.load(.acquire) == 0) {
                    if (support.msSince(start) > 5000) return;
                    support.sleepMs(5);
                }
                closed_in_time.* = true;
                for (pipes.read_ends[0..pipes.count]) |r| {
                    if (!support.pipeWritersClosed(r, support.closed_wait_ms)) closed_in_time.* = false;
                }
            }
        };
        var capture: Capture = .{ .take = false };
        var recorder: support.Recorder = .{};
        var closed_in_time = false;
        const sock = peer.sp[0];
        peer.sp[0] = -1;
        const watcher = try std.Thread.spawn(.{}, Watcher.run, .{ &capture, &peer.pipes, sock, &closed_in_time });
        // The tick makes the loop poll before each read: while the peer is
        // idle no read runs, so only the release after dispatch can close
        // the fds in time.
        const run_result = runConnection(peer.sp[1], .{ .tick_interval_ms = 50 }, &capture, &recorder);
        watcher.join();
        try run_result;
        try testing.expectEqual(@as(usize, 1), capture.count);
        try testing.expectEqual(@as(usize, 2), capture.seen[0].fd_count);
        try testing.expect(closed_in_time);
        try testing.expectEqual(@as(usize, 2), recorder.attemptedFor(error.AttachedFdsRejected));
    }
    try support.expectBackAtBaseline(before);
}

// ---------------------------------------------------------------------------
// The transport API
// ---------------------------------------------------------------------------

test "takeFrameFd hands out each fd once, by index; the rest go to the closer at release" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var peer = try RawPeer.open(3);
        defer peer.deinit();
        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = peer.sp[1] }, 4096);
        defer transport.deinit();
        try transport.enableFdPassing(.{ .max_fds_per_message = 8 });
        var a_buf: [small_len]u8 = undefined;
        try peer.send(frameBytes(&a_buf, small_len, 'A'), &.{ 0, 1, 2 });
        peer.pipes.closeWriters();

        try testing.expectEqual(@as(usize, 8), try transport.read());
        try testing.expectEqual(@as(usize, 0), transport.frameFdCount());
        try testing.expectEqual(@as(?fd_io.Fd, null), transport.takeFrameFd(0));
        try testing.expectEqual(@as(usize, small_len - 8), try transport.read());
        try testing.expectEqual(@as(usize, 3), transport.frameFdCount());
        try testing.expectEqual(@as(?fd_io.Fd, null), transport.takeFrameFd(3));
        const middle = transport.takeFrameFd(1) orelse return error.FrameFdMissing;
        try testing.expectEqual(@as(?fd_io.Fd, null), transport.takeFrameFd(1));
        try testing.expectEqual(peer.ids[1], try fdIdentity(middle));

        transport.releaseFrameFds();
        try testing.expectEqual(@as(usize, 0), transport.frameFdCount());
        // The two not taken are closed; the taken one is ours.
        try testing.expect(support.pipeWritersClosed(peer.pipes.read_ends[0], support.closed_wait_ms));
        try testing.expect(support.pipeWritersClosed(peer.pipes.read_ends[2], support.closed_wait_ms));
        try testing.expect(!support.pipeWritersClosed(peer.pipes.read_ends[1], support.still_open_wait_ms));
        support.closeFd(middle);
        try testing.expect(support.pipeWritersClosed(peer.pipes.read_ends[1], support.closed_wait_ms));
        peer.finishSending();
        try testing.expectEqual(@as(usize, 0), try transport.read());
    }
    try support.expectBackAtBaseline(before);
}

test "a frame cut off by end of stream: its fds are closed when reading ends, not only at teardown" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var peer = try RawPeer.open(2);
        defer peer.deinit();
        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = peer.sp[1] }, 4096);
        defer transport.deinit();
        try transport.enableFdPassing(.{ .max_fds_per_message = 8 });
        var a_buf: [small_len]u8 = undefined;
        const a = frameBytes(&a_buf, small_len, 'A');
        try peer.send(a[0..50], &.{ 0, 1 });
        peer.finishSending();

        try testing.expectEqual(@as(usize, 8), try transport.read());
        try testing.expectEqual(@as(usize, 42), try transport.read());
        try testing.expectEqual(@as(usize, 0), try transport.read());
        // Before deinit: the end of the stream released them.
        try peer.pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "teardown with a frame half read closes the fds it holds" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var peer = try RawPeer.open(2);
        defer peer.deinit();
        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = peer.sp[1] }, 4096);
        try transport.enableFdPassing(.{ .max_fds_per_message = 8 });
        var a_buf: [small_len]u8 = undefined;
        const a = frameBytes(&a_buf, small_len, 'A');
        try peer.send(a[0..50], &.{ 0, 1 });
        peer.pipes.closeWriters();
        // The sender stays connected: no end of stream. The transport holds
        // both fds for the half-read frame when it is torn down.
        try testing.expectEqual(@as(usize, 8), try transport.read());
        try testing.expect(!support.pipeWritersClosed(peer.pipes.read_ends[0], support.still_open_wait_ms));
        transport.deinit();
        try peer.pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "enableFdPassing: only before the first read; 0 turns it off again" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var peer = try RawPeer.open(1);
        defer peer.deinit();
        var transport = try Transport.init(testing.allocator, testing.io, .{ .handle = peer.sp[1] }, 4096);
        defer transport.deinit();
        try transport.enableFdPassing(.{ .max_fds_per_message = 4 });
        try transport.enableFdPassing(.{ .max_fds_per_message = 200 });
        try transport.enableFdPassing(.{ .max_fds_per_message = 0 });
        var a_buf: [small_len]u8 = undefined;
        try peer.send(frameBytes(&a_buf, small_len, 'A'), &.{0});
        // Off again: a bulk read, and the fd goes to the closer.
        try testing.expectEqual(@as(usize, small_len), try transport.read());
        try testing.expectEqual(@as(usize, 0), transport.frameFdCount());
        try testing.expectError(error.AlreadyReading, transport.enableFdPassing(.{ .max_fds_per_message = 4 }));
        peer.finishSending();
        try peer.pipes.expectAllWritersClosed();
    }
    try support.expectBackAtBaseline(before);
}

test "enableFdPassing and teardown fail cleanly at every allocation" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const sp = try support.socketPair();
    defer support.closeFd(sp[0]);
    defer support.closeFd(sp[1]);
    const Init = struct {
        fn run(allocator: std.mem.Allocator, socket: Fd) !void {
            var transport = try Transport.init(allocator, testing.io, .{ .handle = socket }, 64);
            // The socket belongs to the test: mark it closed so deinit only
            // frees the transport's own state.
            defer {
                transport.fd_closed.store(true, .release);
                transport.deinit();
            }
            try transport.enableFdPassing(.{ .max_fds_per_message = 253 });
            try testing.expectEqual(@as(usize, 0), transport.frameFdCount());
        }
    };
    try testing.checkAllAllocationFailures(testing.allocator, Init.run, .{sp[1]});
}

// ---------------------------------------------------------------------------
// Fuzz: random read splits, random fd placement, hostile headers
// ---------------------------------------------------------------------------

const Hostile = enum { none, count_overflow, too_many_segments, too_many_words, too_many_bytes };

/// One fuzz case: the byte stream, where its frames start, and how it is
/// cut into sends, some carrying fds.
const FuzzCase = struct {
    stream: std.ArrayList(u8) = .empty,
    /// Start offset of each frame, and of the hostile header last (if any).
    starts: std.ArrayList(usize) = .empty,
    /// Byte length of each frame (for the hostile header: the bytes of it
    /// the transport may read, the head or the whole header).
    lens: std.ArrayList(usize) = .empty,
    hostile: Hostile = .none,
    /// Sends: [cut_start, next cut) with `fd_counts[i]` fds from the pool.
    cuts: std.ArrayList(usize) = .empty,
    fd_counts: std.ArrayList(usize) = .empty,

    fn deinit(self: *FuzzCase, allocator: std.mem.Allocator) void {
        self.stream.deinit(allocator);
        self.starts.deinit(allocator);
        self.lens.deinit(allocator);
        self.cuts.deinit(allocator);
        self.fd_counts.deinit(allocator);
    }

    fn validFrames(self: *const FuzzCase) usize {
        return self.starts.items.len - @intFromBool(self.hostile != .none);
    }

    /// The frame (index into `starts`) holding byte `offset`, or null for
    /// junk after a hostile header.
    fn frameAt(self: *const FuzzCase, offset: usize) ?usize {
        for (self.starts.items, self.lens.items, 0..) |start, len, i| {
            if (offset >= start and offset < start + len) return i;
        }
        return null;
    }
};

const pool_size = 12;

fn buildCase(allocator: std.mem.Allocator, rand: std.Random, limit: usize) !FuzzCase {
    var case: FuzzCase = .{};
    errdefer case.deinit(allocator);
    const frame_count = rand.intRangeAtMost(usize, 1, 6);
    for (0..frame_count) |_| {
        const segments = rand.intRangeAtMost(u32, 1, 5);
        var sizes: [5]u32 = undefined;
        var words: usize = 0;
        for (sizes[0..segments]) |*s| {
            s.* = rand.intRangeAtMost(u32, 0, 12);
            words += s.*;
        }
        const header_len: usize = 4 + 4 * @as(usize, segments) + @as(usize, if (segments % 2 == 0) 4 else 0);
        std.debug.assert(header_len + words * 8 <= limit);
        try case.starts.append(allocator, case.stream.items.len);
        try case.lens.append(allocator, header_len + words * 8);
        var header: [32]u8 = @splat(0);
        std.mem.writeInt(u32, header[0..4], segments - 1, .little);
        for (sizes[0..segments], 0..) |s, i| std.mem.writeInt(u32, header[4 + 4 * i ..][0..4], s, .little);
        try case.stream.appendSlice(allocator, header[0..header_len]);
        for (0..words * 8) |_| try case.stream.append(allocator, rand.int(u8));
    }
    if (rand.uintLessThan(u8, 3) == 0) {
        case.hostile = switch (rand.uintLessThan(u8, 4)) {
            0 => .count_overflow,
            1 => .too_many_segments,
            2 => .too_many_words,
            else => .too_many_bytes,
        };
        var header: [16]u8 = @splat(0);
        var header_len: usize = 8;
        switch (case.hostile) {
            .count_overflow => std.mem.writeInt(u32, header[0..4], 0xFFFF_FFFF, .little),
            .too_many_segments => std.mem.writeInt(u32, header[0..4], rand.intRangeAtMost(u32, 512, 0xFFFF), .little),
            .too_many_words => std.mem.writeInt(u32, header[4..8], @intCast(Framer.max_frame_words + 1), .little),
            .too_many_bytes => {
                // Three segments: the head, then 8 more header bytes.
                std.mem.writeInt(u32, header[0..4], 2, .little);
                std.mem.writeInt(u32, header[4..8], @intCast(limit / 16), .little);
                std.mem.writeInt(u32, header[8..12], @intCast(limit / 16), .little);
                std.mem.writeInt(u32, header[12..16], 1, .little);
                header_len = 16;
            },
            .none => unreachable,
        }
        try case.starts.append(allocator, case.stream.items.len);
        try case.lens.append(allocator, header_len);
        try case.stream.appendSlice(allocator, header[0..header_len]);
        // Junk the transport must never read.
        const junk = rand.uintAtMost(usize, 300);
        for (0..junk) |_| try case.stream.append(allocator, rand.int(u8));
    }

    // Cut points: random offsets, plus some at the spots that matter (a
    // frame's first byte, header byte 4, the end of the head).
    const len = case.stream.items.len;
    var cut_set: std.ArrayList(usize) = .empty;
    defer cut_set.deinit(allocator);
    try cut_set.append(allocator, 0);
    for (0..rand.uintAtMost(usize, 10)) |_| try cut_set.append(allocator, rand.uintLessThan(usize, len));
    for (case.starts.items) |start| {
        for ([_]usize{ 0, 4, 8 }) |delta| {
            if (start + delta < len and rand.boolean()) try cut_set.append(allocator, start + delta);
        }
    }
    std.mem.sort(usize, cut_set.items, {}, std.sort.asc(usize));
    var fds_left: usize = pool_size;
    var previous: ?usize = null;
    for (cut_set.items) |cut| {
        if (previous) |p| if (p == cut) continue;
        previous = cut;
        try case.cuts.append(allocator, cut);
        var n: usize = 0;
        if (fds_left != 0 and rand.uintLessThan(u8, 3) == 0) n = rand.intRangeAtMost(usize, 1, @min(fds_left, 3));
        fds_left -= n;
        try case.fd_counts.append(allocator, n);
    }
    return case;
}

/// What the case must deliver: for each frame dispatched, the pool indexes
/// of its fds; and whether reading must end with a protocol error, at the
/// frame after the last one dispatched.
const Expected = struct {
    frames: usize,
    fds: [8][3]usize = undefined,
    fd_counts: [8]usize = @splat(0),
    /// Where the send carrying each frame's fds starts, and how many it
    /// carries (for the census).
    batch_start: [8]usize = @splat(0),
    batch_len: [8]usize = @splat(0),
    violation: bool,
};

fn expectedOutcome(case: *const FuzzCase, max_fds: usize) Expected {
    var batches: [8]usize = @splat(0);
    var out: Expected = .{ .frames = 0, .violation = false };
    var pool_at: usize = 0;
    for (case.cuts.items, case.fd_counts.items) |cut, n| {
        defer pool_at += n;
        if (n == 0) continue;
        const frame = case.frameAt(cut) orelse continue;
        batches[frame] += 1;
        if (batches[frame] == 1) {
            const keep = @min(n, max_fds);
            for (0..keep) |i| out.fds[frame][i] = pool_at + i;
            out.fd_counts[frame] = keep;
            out.batch_start[frame] = cut;
            out.batch_len[frame] = n;
        }
    }
    for (0..case.validFrames()) |f| {
        if (batches[f] > 1) {
            out.violation = true;
            out.frames = f;
            return out;
        }
    }
    out.frames = case.validFrames();
    out.violation = case.hostile != .none;
    return out;
}

fn sendCase(sock: Fd, case: *const FuzzCase, pool: []const Fd) void {
    defer support.closeFd(sock);
    var pool_at: usize = 0;
    for (case.cuts.items, case.fd_counts.items, 0..) |cut, n, i| {
        const end = if (i + 1 < case.cuts.items.len) case.cuts.items[i + 1] else case.stream.items.len;
        // Ends once the receiver closed its end after a protocol error.
        fd_io.sendWithFds(sock, case.stream.items[cut..end], pool[pool_at..][0..n]) catch return;
        pool_at += n;
    }
}

/// What the fuzz cases covered, so a generator that stopped producing a
/// case cannot pass unnoticed.
const Census = struct {
    frames_with_fds: usize = 0,
    /// Fds on a send that starts past the frame's first byte (E3, E3b).
    mid_frame_fds: usize = 0,
    /// A frame that kept fewer fds than its send carried.
    capped: usize = 0,
    /// Two fd batches in one frame.
    batch_violations: usize = 0,
    /// Cases ended by each kind of hostile header.
    hostile: [std.enums.values(Hostile).len]usize = @splat(0),
    /// Frames with fds read in parts smaller than the head.
    small_reads: usize = 0,

    fn note(self: *Census, case: *const FuzzCase, want: *const Expected, read_size: usize) void {
        for (0..want.frames) |f| {
            if (want.fd_counts[f] == 0) continue;
            self.frames_with_fds += 1;
            if (want.batch_start[f] != case.starts.items[f]) self.mid_frame_fds += 1;
            if (want.batch_len[f] > want.fd_counts[f]) self.capped += 1;
            if (read_size < FrameHeadLen) self.small_reads += 1;
        }
        if (want.violation and want.frames < case.validFrames()) self.batch_violations += 1;
        if (case.hostile != .none and want.frames == case.validFrames()) self.hostile[@backingInt(case.hostile)] += 1;
    }

    fn expectCovered(self: *const Census) !void {
        var covered = self.frames_with_fds >= 100 and self.mid_frame_fds >= 20 and
            self.capped >= 20 and self.batch_violations >= 20 and self.small_reads >= 20;
        for (std.enums.values(Hostile)) |kind| {
            if (kind != .none and self.hostile[@backingInt(kind)] < 5) covered = false;
        }
        if (!covered) {
            std.debug.print("fuzz census too thin: {any}\n", .{self.*});
            return error.FuzzCensusTooThin;
        }
    }
};

/// The bytes every frame starts with (the segment count and the first size).
const FrameHeadLen = 8;

fn fuzzOne(seed: u64, ids: *const [pool_size]u64, pipes: *support.Pipes, census: *Census) !void {
    const allocator = testing.allocator;
    var prng = std.Random.DefaultPrng.init(seed);
    const rand = prng.random();
    const limit: usize = if (rand.boolean()) 1024 else 4096;
    const read_sizes = [_]usize{ 1, 3, 7, 8, 9, 13, 61, 100, 512, 4096 };
    const read_size = read_sizes[rand.uintLessThan(usize, read_sizes.len)];
    const max_fds = rand.intRangeAtMost(u8, 1, 4);
    var case = try buildCase(allocator, rand, limit);
    defer case.deinit(allocator);
    const want = expectedOutcome(&case, max_fds);

    const sp = try support.socketPair();
    var peak: PeakAllocator = .{ .inner = allocator };
    var transport = try Transport.init(peak.allocator(), testing.io, .{ .handle = sp[1] }, read_size);
    var transport_live = true;
    defer if (transport_live) transport.deinit();
    try transport.enableFdPassing(.{ .max_fds_per_message = max_fds, .max_buffered_frame_bytes = limit });
    var framer = Framer.initWithOptions(peak.allocator(), .{ .max_buffered_bytes = limit });
    defer framer.deinit();
    const fixed = peak.live;
    peak.peak = fixed;

    const sender = try std.Thread.spawn(.{}, sendCase, .{ sp[0], &case, pipes.writers() });
    var sender_joined = false;
    defer if (!sender_joined) {
        transport.shutdown();
        sender.join();
    };

    var got_frames: usize = 0;
    var consumed: usize = 0;
    var failure: ?anyerror = null;
    while (true) {
        const n = transport.read() catch |err| {
            failure = err;
            break;
        };
        if (n == 0) break;
        consumed += n;
        try framer.push(transport.read_buf[0..n]);
        // Never more than one frame's bytes, and never past the limit.
        try testing.expect(framer.bufferedBytes() <= limit);
        var popped: usize = 0;
        while (try framer.popFrame()) |frame| {
            defer allocator.free(frame);
            popped += 1;
            const f = got_frames;
            got_frames += 1;
            if (f >= want.frames) {
                std.debug.print("seed {d}: frame {d} dispatched, want {d} frames\n", .{ seed, f, want.frames });
                return error.TooManyFrames;
            }
            try testing.expectEqualSlices(u8, case.stream.items[case.starts.items[f]..][0..case.lens.items[f]], frame);
            // Exactly this frame's fds, in order: no cross-frame attribution.
            testing.expectEqual(want.fd_counts[f], transport.frameFdCount()) catch |err| {
                std.debug.print("seed {d}: frame {d}: {d} fd(s), want {d}\n", .{ seed, f, transport.frameFdCount(), want.fd_counts[f] });
                return err;
            };
            for (0..want.fd_counts[f]) |i| {
                const fd = transport.takeFrameFd(i) orelse return error.FrameFdMissing;
                defer support.closeFd(fd);
                try testing.expectEqual(ids[want.fds[f][i]], try fdIdentity(fd));
            }
        }
        // One read completes at most one frame.
        try testing.expect(popped <= 1);
    }
    if (want.violation) {
        testing.expectEqual(@as(?anyerror, error.ConnectionResetByPeer), failure) catch |err| {
            std.debug.print("seed {d}: hostile {t}, reading ended with {?}\n", .{ seed, case.hostile, failure });
            return err;
        };
        // Nothing of the offending frame past its header was read.
        const at = case.starts.items[want.frames];
        try testing.expect(consumed <= at + case.lens.items[want.frames]);
        if (case.hostile != .none and want.frames == case.validFrames()) {
            try testing.expect(consumed <= at + 16);
        }
    } else {
        try testing.expectEqual(@as(?anyerror, null), failure);
    }
    try testing.expectEqual(want.frames, got_frames);
    // The framer never held more than one frame (twice: buffered and the
    // copy out), with ArrayList growth.
    try testing.expect(peak.peak - fixed <= 3 * limit);

    transport.deinit();
    transport_live = false;
    sender.join();
    sender_joined = true;
    census.note(&case, &want, read_size);
}

test "fuzz: random read splits, fd placement and hostile headers; no leak, no cross-frame fd, nothing past the limits" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    var census: Census = .{};
    var seed: u64 = 1;
    while (seed <= 300) : (seed += 1) {
        var pipes = try support.Pipes.open(pool_size);
        defer pipes.closeAll();
        var ids: [pool_size]u64 = undefined;
        for (pipes.writers(), 0..) |w, i| ids[i] = try fdIdentity(w);
        fuzzOne(seed, &ids, &pipes, &census) catch |err| {
            std.debug.print("fuzz seed {d} failed\n", .{seed});
            return err;
        };
        pipes.closeWriters();
        // Every copy of every write end, sent or not, is closed: taken and
        // closed by the test, released to the closer, or still in flight in
        // the socket the transport closed.
        pipes.expectAllWritersClosed() catch |err| {
            std.debug.print("fuzz seed {d}: an fd leaked\n", .{seed});
            return err;
        };
    }
    try census.expectCovered();
    try support.expectBackAtBaseline(before);
}
