//! A received fd whose close blocks must never stall the reader or the
//! teardown of an AF_UNIX connection.
//!
//! Item 6 of docs/sprint-plan-2026-10-04.md. A peer can attach a TCP socket
//! that has `SO_LINGER {1, 3}` and unsent data. The final close of that
//! socket blocks the closing thread for the linger time (3 s). Before drain
//! mode, Linux did that close inside the plain read, on the reader thread:
//! the frame the socket rode on dispatched 3 s late, and every frame behind
//! it waited too. macOS blocks the same way once the unsent data is really
//! stuck (it needs `SO_LINGER_SEC` and a send queue refilled until it
//! settles; see the FD-0 suite), so these tests run on both kernels.
//!
//! Drain mode hands every received fd, and at `deinit` the socket itself, to
//! the closer thread. The reader and the deinit path never wait for a close.
//!
//! Linux and macOS run the tests; other targets skip them but compile the
//! file.

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

/// The plan's numbers: a 3 s linger, a frame that dispatches within 100 ms,
/// and close plus deinit within 1 s.
const linger_seconds = 3;
const dispatch_max_ms = 100;
const teardown_max_ms = 1000;
/// How long the closer may take to finish the lingering close once the test
/// lets it (it closes the accepting side, which ends the linger at once).
const closer_drain_ms = (linger_seconds + 3) * 1000;

/// A TCP client socket whose final close lingers: the accepting side never
/// reads, so unsent data stays queued, and SO_LINGER is on.
const LingeringSocket = struct {
    listener: Fd,
    server_side: Fd,
    client: Fd,

    var chunk: [64 * 1024]u8 = @splat(0xab);

    fn open() !LingeringSocket {
        const listener: Fd = @intCast(try support.check(sys.socket(posix.AF.INET, posix.SOCK.STREAM, 0), "socket"));
        errdefer support.closeFd(listener);
        var addr: posix.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f00_0001) };
        _ = try support.check(sys.bind(listener, @ptrCast(&addr), @sizeOf(posix.sockaddr.in)), "bind");
        _ = try support.check(sys.listen(listener, 1), "listen");
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        _ = try support.check(sys.getsockname(listener, @ptrCast(&addr), &addr_len), "getsockname");
        const client: Fd = @intCast(try support.check(sys.socket(posix.AF.INET, posix.SOCK.STREAM, 0), "socket"));
        errdefer support.closeFd(client);
        _ = try support.check(sys.connect(client, @ptrCast(&addr), @sizeOf(posix.sockaddr.in)), "connect");
        const server_side: Fd = @intCast(try support.check(sys.accept(listener, null, null), "accept"));
        errdefer support.closeFd(server_side);

        // Fill the send queue; the accepting side never reads. One pass is
        // not enough on macOS: receive-buffer autotuning drains it and the
        // close then does not linger. Refill until two passes 10 ms apart
        // add nothing.
        try support.setNonBlocking(client, true);
        var queued = try fillSendQueue(client);
        try testing.expect(queued > 0);
        var quiet_passes: usize = 0;
        var passes: usize = 0;
        while (quiet_passes < 2) : (passes += 1) {
            if (passes == 200) return error.SendQueueNeverSettled;
            support.sleepMs(10);
            const more = try fillSendQueue(client);
            queued += more;
            quiet_passes = if (more == 0) quiet_passes + 1 else 0;
        }
        try support.setNonBlocking(client, false);

        // Darwin's SO_LINGER counts clock ticks; SO_LINGER_SEC counts
        // seconds, as Linux's SO_LINGER does.
        const linger_opt = if (support.is_macos) posix.SO.LINGER_SEC else posix.SO.LINGER;
        const lg: posix.linger = .{ .onoff = 1, .linger = linger_seconds };
        _ = try support.check(sys.setsockopt(client, posix.SOL.SOCKET, linger_opt, std.mem.asBytes(&lg), @sizeOf(posix.linger)), "setsockopt(SO_LINGER)");
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
    fn closeClient(self: *LingeringSocket) void {
        if (self.client >= 0) support.closeFd(self.client);
        self.client = -1;
    }

    /// Close the accepting side. That resets the connection, which ends
    /// any linger still running on the closer thread.
    fn endLinger(self: *LingeringSocket) void {
        if (self.server_side >= 0) support.closeFd(self.server_side);
        self.server_side = -1;
    }

    fn deinit(self: *LingeringSocket) void {
        self.closeClient();
        self.endLinger();
        support.closeFd(self.listener);
    }
};

const Inbox = support.Inbox;

fn onMessage(conn: *Connection, _: []const u8) anyerror!void {
    const inbox: *Inbox = @ptrCast(@alignCast(conn.context().?));
    inbox.frames += 1;
    if (inbox.first_frame_ns == null) inbox.first_frame_ns = support.nowNs();
    if (inbox.close_after_frames) |n| {
        if (inbox.frames >= n) conn.close();
    }
}

fn onError(conn: *Connection, err: anyerror) void {
    const inbox: *Inbox = @ptrCast(@alignCast(conn.context().?));
    inbox.errors += 1;
    inbox.last_error = err;
}

fn onClose(_: *Connection) void {}

fn warmUp() !void {
    try fd_io.closer.ensureStarted();
    try support.waitCloserIdle(closer_drain_ms);
}

test "a frame that carries a lingering socket dispatches at once, and close plus deinit stay fast" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var lingering = try LingeringSocket.open();
        defer lingering.deinit();
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);

        const frame = try support.buildFrame(testing.allocator, 0x11);
        defer testing.allocator.free(frame);
        try support.sendWithFds(sp[0], frame, &.{lingering.client});
        // From here the receiver's copy is the last one: its close is the
        // final close, and it lingers.
        lingering.closeClient();

        var inbox: Inbox = .{ .close_after_frames = 1 };
        var recorder: support.Recorder = .{};
        var conn = try Connection.init(testing.allocator, testing.io, .{ .handle = sp[1] }, .{
            .observer = recorder.observer(),
        });
        conn.start(&inbox, onMessage, onError, onClose);

        const run_start = support.nowNs();
        conn.run(); // the frame's callback closes the connection
        conn.deinit();
        const teardown_end = support.nowNs();
        const dispatched_at = inbox.first_frame_ns orelse teardown_end;
        const dispatch_ms: i64 = @intCast(@divFloor(dispatched_at - run_start, std.time.ns_per_ms));
        // From the close (in the frame's callback) to the end of deinit.
        const teardown_ms: i64 = @intCast(@divFloor(teardown_end - dispatched_at, std.time.ns_per_ms));
        errdefer std.debug.print("dispatch took {d} ms, close plus deinit took {d} ms\n", .{ dispatch_ms, teardown_ms });

        try testing.expectEqual(@as(usize, 1), inbox.frames);
        try testing.expect(dispatch_ms < dispatch_max_ms);
        try testing.expect(teardown_ms < teardown_max_ms);
        try testing.expectEqual(@as(usize, 1), recorder.attemptedFor(error.AttachedFdsRejected));

        // The lingering close is the closer's alone. End it early, and let
        // the closer finish before the baseline check.
        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}

test "deinit of an AF_UNIX transport with an unread lingering socket in its queue does not block" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var lingering = try LingeringSocket.open();
        defer lingering.deinit();
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);

        var transport = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64);
        // Nothing reads this. The lingering socket is still in flight when
        // the transport's socket closes, and the kernel does its final close
        // inside that close.
        try support.sendWithFds(sp[0], "unread", &.{lingering.client});
        lingering.closeClient();

        const start = support.nowNs();
        transport.deinit();
        const deinit_ms = support.msSince(start);
        errdefer std.debug.print("deinit took {d} ms\n", .{deinit_ms});
        try testing.expect(deinit_ms < teardown_max_ms);

        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}
