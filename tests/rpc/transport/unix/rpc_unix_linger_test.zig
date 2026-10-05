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
//! the closer. The reader and the deinit path never wait for a close.
//!
//! The second half keeps a closer lane stuck on purpose (a 60 s linger the
//! test ends early) and checks what that may hold up: a received fd's close
//! holds up no other connection's socket close or shutdown; the closer's
//! bound caps the fds this process holds, across reconnecting peers and for
//! readers that were already waiting; and a connection torn down with a
//! blocking fd still unread stalls only the socket lane (Linux still closes
//! orderly sockets inline; a blocked reader still notices `shutdown`).
//!
//! The last part is the listening socket. The kernel does the final close of
//! the fds riding on connections still in a listener's backlog inside the
//! listener's own final close. `Listener.close` must return at once anyway,
//! and while that close holds the socket lane, closing another listener must
//! still wake a thread parked in its `accept` (macOS wakes it only through
//! the close itself).
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
const unix = capnpc.rpc.transport.unix;
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
/// The stall tests keep a closer lane stuck on purpose. Their linger is long
/// enough that it never ends by itself during the test; each test ends it by
/// closing the accepting side.
const stall_linger_seconds = 60;
/// How long a stuck lane must stay stuck before a stall test trusts it.
const stall_settle_ms = 200;

const LingeringSocket = support.LingeringSocket;
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
        var lingering = try LingeringSocket.open(linger_seconds);
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
        var lingering = try LingeringSocket.open(linger_seconds);
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

// ---------------------------------------------------------------------------
// A close that blocks for good: what it may and may not hold up
// ---------------------------------------------------------------------------

/// A connection whose peer attached a lingering socket that the transport
/// has read: the closer's `.received` lane is then stuck in that socket's
/// final close until `lingering.endLinger()`.
const Stall = struct {
    peer: Fd,
    transport: tcp.Transport,

    fn receivedLane(lingering: *LingeringSocket) !Stall {
        const sp = try support.socketPair();
        errdefer support.closeFd(sp[0]);
        var transport = tcp.Transport.init(testing.allocator, testing.io, .{ .handle = sp[1] }, 64) catch |err| {
            support.closeFd(sp[1]);
            return err;
        };
        errdefer transport.deinit();
        try support.sendWithFds(sp[0], "x", &.{lingering.client});
        lingering.closeClient();
        try testing.expectEqual(@as(usize, 1), try transport.read());
        try expectStuck(.received);
        return .{ .peer = sp[0], .transport = transport };
    }

    fn deinit(self: *Stall) void {
        self.transport.deinit();
        support.closeFd(self.peer);
    }
};

/// Setup check: `lane` still has a job after `stall_settle_ms`, so the
/// lingering close in it really blocks (otherwise the test proves nothing).
fn expectStuck(lane: fd_io.closer.Lane) !void {
    support.sleepMs(stall_settle_ms);
    if (fd_io.closer.pendingIn(lane) == 0) {
        std.debug.print("setup: the lingering close on the {t} lane did not block\n", .{lane});
        return error.LingerDidNotBlock;
    }
}

fn expectClosedSoon(fd: Fd, name: []const u8) !void {
    if (support.waitClosed(fd, teardown_max_ms)) return;
    std.debug.print("{s}'s socket was still open {d} ms after deinit\n", .{ name, teardown_max_ms });
    return error.SocketCloseHeldUp;
}

test "a received fd whose close blocks holds up no other connection's socket close or shutdown" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var lingering = try LingeringSocket.open(stall_linger_seconds);
        defer lingering.deinit();
        var stall = try Stall.receivedLane(&lingering);
        defer stall.deinit();

        // B never sends an fd, but its peer's bytes are still unread at
        // teardown, so even Linux hands its close to the closer (the
        // `.socket` lane). It must not wait behind the stuck received fd.
        const b = try support.socketPair();
        defer support.closeFd(b[0]);
        var tb = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = b[1] }, 64);
        try support.sendWithFds(b[0], "unread", &.{});
        tb.deinit();
        try expectClosedSoon(b[1], "B");

        // C: a reader blocked in read(), and shutdown() from this thread, as
        // Connection.requestClose does.
        const c = try support.socketPair();
        defer support.closeFd(c[0]);
        var tc = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = c[1] }, 64);
        defer tc.deinit();
        var reader: support.BlockedReader = .{};
        try reader.start(&tc);
        defer reader.finish(c[0]);
        support.sleepMs(100);
        tc.shutdown();
        try testing.expect(reader.waitDone(teardown_max_ms));
        try testing.expectEqual(@as(usize, 0), try reader.result.?);

        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}

test "while a received fd's close blocks, reconnecting peers cannot grow this process's fds past the bound" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const limit = 8;
        const per_message = 20;
        const previous_limit = fd_io.closer.setQueueLimit(limit);
        defer _ = fd_io.closer.setQueueLimit(previous_limit);

        var lingering = try LingeringSocket.open(stall_linger_seconds);
        defer lingering.deinit();
        var stall = try Stall.receivedLane(&lingering);
        defer stall.deinit();

        const p = try support.pipePair();
        defer support.closeFd(p[0]);
        defer support.closeFd(p[1]);
        // One fd number, many copies: what the sender attaches costs it
        // nothing, and each copy is a new fd here.
        var copies: [per_message]Fd = undefined;
        @memset(&copies, p[1]);

        const held_before = support.FdSnapshot.take();
        var recorder: support.Recorder = .{};
        for (0..4) |_| {
            const sp = try support.socketPair();
            defer support.closeFd(sp[0]);
            try support.sendWithFds(sp[0], "y", &copies);
            var t = try tcp.Transport.initWithOptions(testing.allocator, testing.io, .{ .handle = sp[1] }, .{
                .read_buffer_size = 64,
                .observer = recorder.observer(),
            });
            defer t.deinit();
            try testing.expectError(error.SystemResources, t.read());
        }
        // A refused connection's close, its fds still in flight, runs on the
        // socket lane; let those finish before counting.
        try support.waitLaneIdle(.socket, closer_drain_ms);

        // Only the first connection read anything, and its read crossed the
        // bound. The other three found the lane full and took nothing.
        var held: [4]Fd = undefined;
        const n_held = support.FdSnapshot.take().added(held_before, &held);
        errdefer std.debug.print("{d} fd(s) held after 4 connections; bound {d}, one read {d}\n", .{ n_held, limit, per_message });
        try testing.expectEqual(@as(usize, per_message), n_held);
        try testing.expectEqual(@as(usize, 1 + per_message), fd_io.closer.pendingIn(.received));
        try testing.expectEqual(@as(usize, 4), recorder.countErr(error.FdCloseQueueFull));

        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}

test "a reader already waiting for data takes no fds once the received lane is full" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const limit = 8;
        const previous_limit = fd_io.closer.setQueueLimit(limit);
        defer _ = fd_io.closer.setQueueLimit(previous_limit);

        const p = try support.pipePair();
        defer support.closeFd(p[0]);
        defer support.closeFd(p[1]);
        var copies: [20]Fd = undefined;
        @memset(&copies, p[1]);

        // B's reader starts waiting while the closer is idle: an idle
        // connection, the way a server holds most of them.
        const b = try support.socketPair();
        defer support.closeFd(b[0]);
        var recorder: support.Recorder = .{};
        var tb = try tcp.Transport.initWithOptions(testing.allocator, testing.io, .{ .handle = b[1] }, .{
            .read_buffer_size = 64,
            .observer = recorder.observer(),
        });
        defer tb.deinit();
        var reader: support.BlockedReader = .{};
        try reader.start(&tb);
        defer reader.finish(b[0]);
        support.sleepMs(100);

        // Another peer stalls the received lane, then fills it past the bound.
        var lingering = try LingeringSocket.open(stall_linger_seconds);
        defer lingering.deinit();
        var stall = try Stall.receivedLane(&lingering);
        defer stall.deinit();
        try support.sendWithFds(stall.peer, "z", copies[0 .. limit + 2]);
        try testing.expectError(error.SystemResources, stall.transport.read());
        const full = fd_io.closer.pendingIn(.received);
        try testing.expect(full > limit);

        // Now B's peer attaches fds. B's reader wakes, finds the lane full
        // and takes none: they stay in flight until B's socket closes.
        const held_before = support.FdSnapshot.take();
        try support.sendWithFds(b[0], "w", &copies);
        try testing.expect(reader.waitDone(teardown_max_ms));
        try testing.expectError(error.SystemResources, reader.result.?);
        var held: [4]Fd = undefined;
        try testing.expectEqual(@as(usize, 0), support.FdSnapshot.take().added(held_before, &held));
        try testing.expectEqual(full, fd_io.closer.pendingIn(.received));
        try testing.expectEqual(@as(usize, 1), recorder.countErr(error.FdCloseQueueFull));
        try testing.expectEqual(@as(usize, 0), recorder.countErr(error.AttachedFdsRejected));

        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}

test "a connection torn down with a blocking fd still unread stalls only the socket lane" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var lingering = try LingeringSocket.open(stall_linger_seconds);
        defer lingering.deinit();

        // D: its peer attaches the lingering socket, and the transport is
        // torn down before reading it. The kernel does the lingering close
        // inside D's socket close (macOS: already inside its shutdown), on
        // the `.socket` lane, which stays stuck.
        const d = try support.socketPair();
        defer support.closeFd(d[0]);
        var td = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = d[1] }, 64);
        try support.sendWithFds(d[0], "unread", &.{lingering.client});
        lingering.closeClient();
        const start = support.nowNs();
        td.deinit();
        try testing.expect(support.msSince(start) < teardown_max_ms);
        try expectStuck(.socket);

        // E: an orderly connection with nothing unread. On Linux nothing can
        // be in flight after shutdown(SHUT_RD), so its close runs inline and
        // does not wait behind D. macOS cannot tell and queues it behind D
        // (the documented residual); the baseline check below sees it closed
        // once D's close ends.
        const e = try support.socketPair();
        defer support.closeFd(e[0]);
        var te = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = e[1] }, 64);
        te.deinit();
        if (support.is_linux) try expectClosedSoon(e[1], "E");

        // F: a reader blocked in read(), and shutdown() from this thread.
        // Linux shuts down inline. On macOS the read half waits behind D on
        // the socket lane, and the reader notices on its poll tick.
        const f = try support.socketPair();
        defer support.closeFd(f[0]);
        var tf = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = f[1] }, 64);
        defer tf.deinit();
        var reader: support.BlockedReader = .{};
        try reader.start(&tf);
        defer reader.finish(f[0]);
        support.sleepMs(100);
        tf.shutdown();
        try testing.expect(reader.waitDone(teardown_max_ms));
        try testing.expectEqual(@as(usize, 0), try reader.result.?);

        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}

// ---------------------------------------------------------------------------
// A listener whose backlog holds a blocking fd
// ---------------------------------------------------------------------------

/// `listener.close()` on a helper thread, timed. A close still running after
/// `teardown_max_ms` is ended with `lingering.endLinger()`, so a failure
/// never hangs the suite, and fails the test.
fn timedListenerClose(listener: *tcp.Listener, lingering: *LingeringSocket) !i64 {
    const Closer = struct {
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This(), l: *tcp.Listener) void {
            l.close();
            self.done.store(true, .release);
        }
    };
    var closer: Closer = .{};
    const start = support.nowNs();
    const thread = try std.Thread.spawn(.{}, Closer.run, .{ &closer, listener });
    while (!closer.done.load(.acquire) and support.msSince(start) < teardown_max_ms) support.sleepMs(1);
    const elapsed = support.msSince(start);
    if (!closer.done.load(.acquire)) {
        lingering.endLinger();
        thread.join();
        std.debug.print("Listener.close was still running after {d} ms: the backlog's lingering close ran on its thread\n", .{elapsed});
        return error.ListenerCloseBlocked;
    }
    thread.join();
    return elapsed;
}

/// A thread parked in `acceptFd` on `listener`.
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

test "Listener.close returns at once while a connection in its backlog carries a lingering socket" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var path_buf: [96]u8 = undefined;
    const path = dir.path(&path_buf, "s");

    const before = support.FdSnapshot.take();
    {
        var lingering = try LingeringSocket.open(stall_linger_seconds);
        defer lingering.deinit();
        var listener = try unix.listen(testing.allocator, testing.io, path, .{});
        defer listener.close();
        // Nobody accepts: the connection, and the lingering socket riding
        // on it, stay in the backlog until the listener's final close.
        try support.queueLingeringInBacklog(path, &lingering);

        const close_ms = try timedListenerClose(&listener, &lingering);
        errdefer std.debug.print("Listener.close took {d} ms\n", .{close_ms});
        try testing.expect(close_ms < teardown_max_ms);
        // Setup check: the final close really blocks, on the socket lane.
        try expectStuck(.socket);

        // While it runs, the path is gone and its lock is free: a new
        // listener binds the path at once.
        try testing.expect(!support.pathExists(path));
        var again = try unix.listen(testing.allocator, testing.io, path, .{});
        again.close();

        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}

test "while a listener's final close blocks the socket lane, closing another listener still wakes its parked accept" {
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    var dir: support.TestDir = .{};
    try dir.init();
    defer dir.deinit();
    var a_buf: [96]u8 = undefined;
    const path_a = dir.path(&a_buf, "a");
    var b_buf: [96]u8 = undefined;
    const path_b = dir.path(&b_buf, "b");

    const before = support.FdSnapshot.take();
    {
        var lingering = try LingeringSocket.open(stall_linger_seconds);
        defer lingering.deinit();

        // A: its final close holds the socket lane until endLinger.
        var a = try unix.listen(testing.allocator, testing.io, path_a, .{});
        defer a.close();
        try support.queueLingeringInBacklog(path_a, &lingering);
        _ = try timedListenerClose(&a, &lingering);
        try expectStuck(.socket);

        // B: a thread parked in accept. Linux wakes it through `shutdown`,
        // macOS only through the close of B's fd, which must not wait
        // behind A on the lane.
        var b = try unix.listen(testing.allocator, testing.io, path_b, .{});
        defer b.close();
        var acceptor: Acceptor = .{ .listener = &b };
        const thread = try std.Thread.spawn(.{}, Acceptor.run, .{&acceptor});
        var joined = false;
        // On failure: close B, then free the lane, so the thread wakes.
        defer if (!joined) {
            b.close();
            lingering.endLinger();
            thread.join();
        };
        while (!acceptor.parked.load(.acquire)) std.atomic.spinLoopHint();
        // Give the thread time to enter accept(2).
        support.sleepMs(300);
        try testing.expect(!acceptor.done.load(.acquire));

        const start = support.nowNs();
        b.close();
        while (!acceptor.done.load(.acquire) and support.msSince(start) < teardown_max_ms) support.sleepMs(5);
        const woke = acceptor.done.load(.acquire);
        // A's close was still running on the lane when B's accept woke.
        const lane_busy = fd_io.closer.pendingIn(.socket) != 0;
        if (!woke) std.debug.print("accept did not wake within {d} ms of close while the socket lane was stuck\n", .{teardown_max_ms});
        try testing.expect(woke);
        try testing.expect(lane_busy);
        thread.join();
        joined = true;

        // Linux: EINVAL from shutdown; macOS: ECONNABORTED from the close.
        // ListenerClosed: the thread had not reached accept(2) yet.
        const woken_by: anyerror = if (support.is_linux) error.SocketNotListening else error.ConnectionAborted;
        const result = acceptor.result orelse return error.NoAcceptResult;
        if (result != woken_by and result != error.ListenerClosed) {
            std.debug.print("accept returned {s}\n", .{@errorName(result)});
            return error.UnexpectedAcceptResult;
        }

        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}
