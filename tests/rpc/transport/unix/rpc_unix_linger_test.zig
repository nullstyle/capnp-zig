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
//! Behind a stuck socket-lane close, a `unix.listen` listener stops
//! accepting at the lane's bound, so reconnecting peers cannot grow this
//! process's fds past it.
//!
//! Then the listening socket. The kernel does the final close of the fds
//! riding on connections still in a listener's backlog inside the
//! listener's own final close. `Listener.close` must return at once anyway,
//! and while that close holds the socket lane, closing another listener must
//! still wake a thread parked in its `accept` (macOS wakes it only through
//! the close itself).
//!
//! The last part: an fd riding on an out-of-band byte (Linux) reaches the
//! closer like any other, and readers that wake together take at most one
//! read past the received lane's bound.
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

fn expectStuck(lane: fd_io.closer.Lane) !void {
    return support.expectLaneStuck(lane, stall_settle_ms);
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
        const previous_limit = fd_io.budget.setLimit(limit);
        defer _ = fd_io.budget.setLimit(previous_limit);

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
        const previous_limit = fd_io.budget.setLimit(limit);
        defer _ = fd_io.budget.setLimit(previous_limit);

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
// The socket lane's bound
// ---------------------------------------------------------------------------

/// Stalls the closer's `.socket` lane: a connection torn down with a
/// lingering socket still unread in its queue. The kernel does that socket's
/// final close inside the connection's own close, on the `.socket` lane.
/// Returns the peer's end, which the caller closes only after the linger
/// ended: on macOS a close of the other end waits while the stuck
/// `shutdown(SHUT_RD)` on the lane disposes of the lingering socket
/// (measured: the full 60 s; FD-0 pins it).
fn stallSocketLane(lingering: *LingeringSocket) !Fd {
    const d = try support.socketPair();
    errdefer support.closeFd(d[0]);
    var td = try tcp.Transport.init(testing.allocator, testing.io, .{ .handle = d[1] }, 64);
    try support.sendWithFds(d[0], "unread", &.{lingering.client});
    lingering.closeClient();
    td.deinit();
    try expectStuck(.socket);
    return d[0];
}

/// Accepts on `listener` until it is closed: each connection is wrapped in
/// a transport and torn down with its peer's bytes unread, which sends its
/// close to the `.socket` lane on both kernels.
const TearDownAcceptor = struct {
    listener: *tcp.Listener,
    accepted: std.atomic.Value(usize) = .init(0),
    last_err: ?anyerror = null,
    thread: std.Thread = undefined,

    fn start(self: *TearDownAcceptor) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    fn run(self: *TearDownAcceptor) void {
        while (true) {
            const fd = self.listener.acceptFd() catch |err| {
                self.last_err = err;
                return;
            };
            var t = tcp.Transport.init(testing.allocator, testing.io, fd, 64) catch |err| {
                self.last_err = err;
                support.closeFd(fd.handle);
                return;
            };
            t.deinit();
            _ = self.accepted.fetchAdd(1, .acq_rel);
        }
    }
};

/// Records the listener's `.backpressure` events.
const ListenerEvents = struct {
    socket_close_queue_full: usize = 0,
    last_limit: ?usize = null,

    fn observer(self: *ListenerEvents) capnpc.rpc.events.Observer {
        return capnpc.rpc.events.Observer.init(self, onEvent);
    }

    fn onEvent(ctx: *anyopaque, event: capnpc.rpc.events.Event) void {
        const self: *ListenerEvents = @ptrCast(@alignCast(ctx));
        switch (event) {
            .backpressure => |b| if (b.err == error.SocketCloseQueueFull) {
                self.socket_close_queue_full += 1;
                self.last_limit = b.limit;
            },
            else => {},
        }
    }
};

test "behind a stuck socket-lane close, a Unix listener stops accepting at the lane's bound, so reconnecting peers cannot grow this process's fds past it" {
    // Each AF_UNIX close queued behind a stuck one keeps its socket fd until
    // the stuck close ends: on macOS every AF_UNIX close goes to the
    // `.socket` lane, on Linux every close with bytes unread (a peer forces
    // that by tearing down mid-frame, or by filling the `.received` lane).
    // A listener from `unix.listen` takes no connection while the lane holds
    // `socketLaneBound()` jobs; the rest wait in the kernel's backlog.
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const budget_limit = support.BudgetLimit.set(16);
        defer budget_limit.restore();
        const bound = fd_io.closer.socketLaneBound();
        try testing.expectEqual(fd_io.closer.min_socket_lane_bound, bound);

        var dir: support.PrivateDir = .{};
        try dir.init();
        defer dir.deinit();
        var listener_events: ListenerEvents = .{};
        var listener = try capnpc.rpc.transport.unix.listen(testing.allocator, testing.io, dir.socketPath(), .{
            .conn = .{ .observer = listener_events.observer() },
        });
        var listener_open = true;
        defer if (listener_open) listener.close();

        var lingering = try LingeringSocket.open(stall_linger_seconds);
        defer lingering.deinit();
        const stall_peer = try stallSocketLane(&lingering);
        defer {
            // End the linger first: on macOS this close waits for it.
            lingering.endLinger();
            support.closeFd(stall_peer);
        }
        // macOS queues two jobs per teardown (the read half of the
        // shutdown, then the close), Linux one (the close).
        const per_teardown: usize = if (support.is_macos) 2 else 1;
        const stall_jobs = fd_io.closer.pendingIn(.socket);
        try testing.expectEqual(per_teardown, stall_jobs);

        var acceptor: TearDownAcceptor = .{ .listener = &listener };
        try acceptor.start();
        var acceptor_running = true;
        defer if (acceptor_running) {
            listener.close();
            listener_open = false;
            acceptor.thread.join();
        };

        // A peer that reconnects in a loop, leaving one byte unread each time.
        const reconnects = 3 * bound;
        const held_before = support.FdSnapshot.take();
        for (0..reconnects) |_| {
            const client = try support.rawConnect(dir.socketPath());
            defer support.closeFd(client);
            try support.sendWithFds(client, "u", &.{});
        }
        // Let the acceptor take what it will.
        support.sleepMs(500);
        const accepted = acceptor.accepted.load(.acquire);
        var held: [64]Fd = undefined;
        const n_held = support.FdSnapshot.take().added(held_before, &held);
        errdefer std.debug.print("accepted {d} of {d} reconnects; {d} fd(s) held; socket lane {d}, bound {d}\n", .{ accepted, reconnects, n_held, fd_io.closer.pendingIn(.socket), bound });
        // The stuck close plus the jobs of each accepted connection fill
        // the lane; each of those connections holds one fd.
        const fit = (bound - stall_jobs + per_teardown - 1) / per_teardown;
        try testing.expectEqual(fit, accepted);
        try testing.expectEqual(stall_jobs + fit * per_teardown, fd_io.closer.pendingIn(.socket));
        try testing.expectEqual(fit, n_held);

        // close wakes the waiting acceptor.
        listener.close();
        listener_open = false;
        acceptor.thread.join();
        acceptor_running = false;
        try testing.expectEqual(@as(?anyerror, error.ListenerClosed), acceptor.last_err);
        // One event for the wait (the acceptor thread emitted it).
        try testing.expectEqual(@as(usize, 1), listener_events.socket_close_queue_full);
        try testing.expectEqual(@as(?usize, bound), listener_events.last_limit);

        // Every fd is released once the blocked close ends.
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

/// A raw AF_UNIX listening socket bound at `path`, as a parent process
/// would hand one to `Listener.initFd`. The caller closes it.
fn rawUnixListener(path: []const u8) !Fd {
    var addr: posix.sockaddr.un = .{ .family = posix.AF.UNIX, .path = undefined };
    @memset(&addr.path, 0);
    @memcpy(addr.path[0..path.len], path);
    const fd: Fd = @intCast(try support.check(sys.socket(posix.AF.UNIX, posix.SOCK.STREAM, 0), "socket"));
    errdefer support.closeFd(fd);
    _ = try support.check(sys.bind(fd, @ptrCast(&addr), @sizeOf(posix.sockaddr.un)), "bind");
    _ = try support.check(sys.listen(fd, 8), "listen");
    return fd;
}

test "Listener.close of a Listener.initFd on an AF_UNIX socket also leaves the final close to the socket lane" {
    // `initFd` knows nothing of the socket: `close` asks `getsockname`, and
    // any socket that is not IPv4 or IPv6 gets the same off-thread final
    // close as a `unix.listen` listener (its hand-off allocates instead of
    // using a reserved slot).
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
        var listener = tcp.Listener.initFd(testing.allocator, testing.io, .{ .handle = try rawUnixListener(path) }, .{});
        defer listener.close();
        try support.queueLingeringInBacklog(path, &lingering);

        const close_ms = try timedListenerClose(&listener, &lingering);
        errdefer std.debug.print("Listener.close took {d} ms\n", .{close_ms});
        try testing.expect(close_ms < teardown_max_ms);
        // Setup check: the final close really blocks, on the socket lane.
        try expectStuck(.socket);

        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}

test "Listener.close does not wait for the final close of a pending connection that carries a lingering socket" {
    // Connections still in the accept queue are released inside the final
    // close of the listening socket, with the fds riding on their unread
    // messages: closed inline, that took 3 s on Linux and on macOS.
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        var dir: support.PrivateDir = .{};
        try dir.init();
        defer dir.deinit();
        var listener = try capnpc.rpc.transport.unix.listen(testing.allocator, testing.io, dir.socketPath(), .{});
        var listener_open = true;
        defer if (listener_open) listener.close();

        var lingering = try LingeringSocket.open(linger_seconds);
        defer lingering.deinit();
        // A peer connects, attaches the lingering socket and leaves; nobody
        // accepts it.
        {
            const client = try support.rawConnect(dir.socketPath());
            defer support.closeFd(client);
            try support.sendWithFds(client, "x", &.{lingering.client});
            lingering.closeClient();
        }

        const start = support.nowNs();
        listener.close();
        listener_open = false;
        const close_ms = support.msSince(start);
        errdefer std.debug.print("Listener.close took {d} ms\n", .{close_ms});
        try testing.expect(close_ms < teardown_max_ms);

        // The lock is already released: a new listener takes the path.
        var next = try capnpc.rpc.transport.unix.listen(testing.allocator, testing.io, dir.socketPath(), .{});
        next.close();

        lingering.endLinger();
        try support.waitCloserIdle(closer_drain_ms);
    }
    try support.expectBackAtBaseline(before);
}

// ---------------------------------------------------------------------------
// Out-of-band bytes (Linux) and concurrent readers
// ---------------------------------------------------------------------------

/// One case of the MSG_OOB test: a lingering socket rides on an out-of-band
/// byte (the first byte of an empty frame), and the frame's other bytes and
/// a second frame follow in band. Returns false when the kernel refuses
/// MSG_OOB on AF_UNIX (built without CONFIG_AF_UNIX_OOB): then nothing out
/// of band can arrive.
fn oobCase(fd_passing: bool) !bool {
    var lingering = try LingeringSocket.open(linger_seconds);
    defer lingering.deinit();
    const sp = try support.socketPair();
    defer support.closeFd(sp[0]);
    var recorder: support.Recorder = .{};
    var t = try tcp.Transport.initWithOptions(testing.allocator, testing.io, .{ .handle = sp[1] }, .{
        .read_buffer_size = 64,
        .observer = recorder.observer(),
    });
    defer t.deinit();
    if (fd_passing) try t.enableFdPassing(.{ .max_fds_per_message = 4 });

    // An empty one-segment frame: its first byte out of band, carrying the
    // fd; the rest, then a second frame, in band.
    const frame = [8]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
    switch (support.sendFlagsErrno(sp[0], frame[0..1], &.{lingering.client}, posix.MSG.OOB)) {
        .ok => |n| try testing.expectEqual(@as(usize, 1), n),
        .err => |err| {
            std.debug.print("MSG_OOB on AF_UNIX refused (errno {d}); nothing to test\n", .{@backingInt(err)});
            return false;
        },
    }
    lingering.closeClient();
    try support.sendWithFds(sp[0], frame[1..], &.{});
    try support.sendWithFds(sp[0], &frame, &.{});

    const start = support.nowNs();
    var total: usize = 0;
    while (total < frame.len) {
        const n = try t.read();
        if (n == 0) return error.UnexpectedEndOfStream;
        total += n;
    }
    const read_ms = support.msSince(start);
    errdefer std.debug.print("fd_passing={}: the first frame took {d} ms to read\n", .{ fd_passing, read_ms });
    // Without SO_OOBINLINE the read skipped the out-of-band byte, and the
    // kernel closed the lingering socket inside it: 3 s.
    try testing.expect(read_ms < teardown_max_ms);
    try testing.expect(total >= frame.len);
    if (fd_passing) {
        // The fd came with the frame's first byte: the frame keeps it.
        try testing.expectEqual(@as(usize, 1), t.frameFdCount());
        t.releaseFrameFds();
    }
    // Either way the fd reached the closer, which reports it.
    try testing.expectEqual(@as(usize, 1), recorder.attemptedFor(error.AttachedFdsRejected));

    lingering.endLinger();
    try support.waitCloserIdle(closer_drain_ms);
    return true;
}

test "MSG_OOB: a lingering socket sent out of band reaches the closer like any fd, and the read does not wait for its close" {
    // Linux 5.15+ takes MSG_OOB on AF_UNIX stream sockets. Without
    // SO_OOBINLINE a normal recvmsg skips the out-of-band message and the
    // kernel frees it, closing its fds inside that recvmsg, on the reader
    // thread: drain mode and fd passing both read past it. macOS refuses
    // MSG_OOB on AF_UNIX, so nothing out of band can arrive there.
    if (!support.supported) return error.SkipZigTest;
    try warmUp();
    const before = support.FdSnapshot.take();
    if (support.is_macos) {
        const sp = try support.socketPair();
        defer support.closeFd(sp[0]);
        defer support.closeFd(sp[1]);
        const p = try support.pipePair();
        defer support.closeFd(p[0]);
        defer support.closeFd(p[1]);
        switch (support.sendFlagsErrno(sp[0], "!", &.{p[1]}, posix.MSG.OOB)) {
            .ok => {
                std.debug.print("macOS took MSG_OOB on AF_UNIX: set SO_OOBINLINE there too (fd_io.setOobInline)\n", .{});
                return error.MacosTookOutOfBandBytes;
            },
            .err => {},
        }
    } else {
        for ([_]bool{ false, true }) |fd_passing| {
            if (!try oobCase(fd_passing)) return error.SkipZigTest;
        }
    }
    try support.expectBackAtBaseline(before);
}

/// A thread that sends one message with `fds` once `go` is set.
const GoSender = struct {
    fn run(sock: Fd, fds: []const Fd, go: *std.atomic.Value(bool)) void {
        while (!go.load(.acquire)) std.atomic.spinLoopHint();
        support.sendWithFds(sock, "w", fds) catch {};
    }
};

test "readers that wake together take at most one read past the received lane's bound, however many they are" {
    // While a received close blocks, each reader that passed the closer's
    // check before the lane filled used to hand off one more read's fds (up
    // to 254), so the bound grew with the number of readers woken at once.
    // A read claim stands for one read's worst case: once the limit has no
    // room for another, the next reader waits for the claim to come back,
    // then finds the lane full and reads nothing.
    if (!support.supported) return error.SkipZigTest;
    // Every reader past the check could install a full message.
    const limit_saved = try support.raiseFdLimit(8192, 2 * claim_readers * claim_per_message) orelse return error.SkipZigTest;
    defer support.restoreFdLimit(limit_saved);
    try warmUp();
    const before = support.FdSnapshot.take();
    {
        const budget_limit = support.BudgetLimit.set(claim_limit);
        defer budget_limit.restore();

        const p = try support.pipePair();
        defer support.closeFd(p[0]);
        defer support.closeFd(p[1]);
        var copies: [claim_per_message]Fd = undefined;
        @memset(&copies, p[1]);

        // A few rounds: the race the claims close is a matter of timing.
        for (0..3) |round| {
            try claimRound(round, &copies);
            try support.waitCloserIdle(closer_drain_ms);
        }
    }
    try support.expectBackAtBaseline(before);
}

/// True when this process can still open a socket.
fn canOpenSocket() bool {
    const rc = sys.socket(posix.AF.UNIX, posix.SOCK.STREAM, 0);
    if (posix.errno(rc) != .SUCCESS) return false;
    support.closeFd(@intCast(rc));
    return true;
}

/// A reader thread that does one `Transport.read` once `go` is set.
const GoReader = struct {
    transport: *tcp.Transport,
    result: ?(tcp.Transport.ReadError!usize) = null,

    fn run(self: *GoReader, go: *std.atomic.Value(bool)) void {
        while (!go.load(.acquire)) std.atomic.spinLoopHint();
        self.result = self.transport.read();
    }
};

test "at a soft RLIMIT_NOFILE of 1024 and the default budget, peers that stall the received lane and then send from many connections at once cannot fill the fd table" {
    // The recommended setup (docs/rpc-unix-sockets.md, "The process fd
    // budget"): the soft limit at 1024, the budget at a quarter of it. A
    // peer stalls the `.received` lane, and 8 connections each have a full
    // message (253 fds) waiting; their readers all start at the same
    // moment. The lane may end one read past the budget, not one read per
    // connection, so this process can still open sockets (accept, connect,
    // open) while the close blocks.
    if (!support.supported) return error.SkipZigTest;
    const soft = 1024;
    const readers_n = 8;
    // The messages are sent while the soft limit is still high: on Linux
    // the sender's limit caps the user's fds in flight (ETOOMANYREFS).
    const previous = try support.raiseFdLimit(4096, 2 * readers_n * claim_per_message) orelse return error.SkipZigTest;
    defer support.restoreFdLimit(previous);
    try warmUp();
    const before = support.FdSnapshot.take();
    // Above that many fds already open, 1024 means something else.
    if (before.highest() > 64) return error.SkipZigTest;
    {
        const budget_limit = support.BudgetLimit.set(soft / 4);
        defer budget_limit.restore();

        const p = try support.pipePair();
        defer support.closeFd(p[0]);
        defer support.closeFd(p[1]);
        var copies: [claim_per_message]Fd = undefined;
        @memset(&copies, p[1]);

        var lingering = try LingeringSocket.open(stall_linger_seconds);
        defer lingering.deinit(); // ends the linger
        var stall = try Stall.receivedLane(&lingering);
        defer stall.deinit();

        var pairs: [readers_n][2]Fd = undefined;
        var transports: [readers_n]tcp.Transport = undefined;
        var opened: usize = 0;
        defer for (0..opened) |i| {
            transports[i].deinit();
            support.closeFd(pairs[i][0]);
        };
        while (opened < readers_n) : (opened += 1) {
            pairs[opened] = try support.socketPair();
            transports[opened] = tcp.Transport.init(testing.allocator, testing.io, .{ .handle = pairs[opened][1] }, 64) catch |err| {
                support.closeFd(pairs[opened][0]);
                support.closeFd(pairs[opened][1]);
                return err;
            };
            try support.sendWithFds(pairs[opened][0], "w", &copies);
        }

        var go = std.atomic.Value(bool).init(false);
        var readers: [readers_n]GoReader = undefined;
        var threads: [readers_n]std.Thread = undefined;
        var spawned: usize = 0;
        defer {
            go.store(true, .release);
            for (threads[0..spawned]) |t| t.join();
        }
        while (spawned < readers_n) : (spawned += 1) {
            readers[spawned] = .{ .transport = &transports[spawned] };
            threads[spawned] = try std.Thread.spawn(.{}, GoReader.run, .{ &readers[spawned], &go });
        }
        // From here on the process holds at most 1024 fds.
        try posix.setrlimit(.NOFILE, .{ .cur = soft, .max = previous.max });
        go.store(true, .release);
        for (threads[0..spawned]) |t| t.join();
        spawned = 0;

        const lane = fd_io.closer.pendingIn(.received);
        const open_now = support.FdSnapshot.take().highest();
        errdefer std.debug.print("received lane {d} (budget {d}); highest open fd {d} of {d}\n", .{ lane, soft / 4, open_now, soft });
        try testing.expect(lane < soft / 4 + fd_io.closer.max_fds_per_recv);
        try testing.expect(canOpenSocket());
    }
    try support.waitCloserIdle(closer_drain_ms);
    try support.expectBackAtBaseline(before);
}

const claim_readers = 16;
const claim_per_message = 253;
const claim_limit = 16;

/// One round of the concurrent-readers test: a stuck received lane one
/// arrival short of its limit, `claim_readers` readers parked in poll, and
/// one message of `copies` for each, sent at once. Everything it opened is
/// closed or handed to the closer when it returns.
fn claimRound(round: usize, copies: []const Fd) !void {
    var lingering = try LingeringSocket.open(stall_linger_seconds);
    defer lingering.deinit(); // ends the linger
    var stall = try Stall.receivedLane(&lingering);
    defer stall.deinit();
    // One arrival short of the limit.
    try support.sendWithFds(stall.peer, "z", copies[0 .. claim_limit - 2]);
    try testing.expectEqual(@as(usize, 1), try stall.transport.read());
    try testing.expectEqual(@as(usize, claim_limit - 1), fd_io.closer.admission().pending);

    var pairs: [claim_readers][2]Fd = undefined;
    var transports: [claim_readers]tcp.Transport = undefined;
    var blocked: [claim_readers]support.BlockedReader = undefined;
    var opened: usize = 0;
    defer for (0..opened) |i| {
        blocked[i].finish(pairs[i][0]);
        transports[i].deinit();
        support.closeFd(pairs[i][0]);
    };
    while (opened < claim_readers) : (opened += 1) {
        pairs[opened] = try support.socketPair();
        transports[opened] = tcp.Transport.init(testing.allocator, testing.io, .{ .handle = pairs[opened][1] }, 64) catch |err| {
            support.closeFd(pairs[opened][0]);
            support.closeFd(pairs[opened][1]);
            return err;
        };
        blocked[opened].start(&transports[opened]) catch |err| {
            transports[opened].deinit();
            support.closeFd(pairs[opened][0]);
            return err;
        };
    }
    // Every reader is parked in poll.
    support.sleepMs(100);

    var go = std.atomic.Value(bool).init(false);
    var senders: [claim_readers]std.Thread = undefined;
    var spawned: usize = 0;
    defer {
        go.store(true, .release);
        for (senders[0..spawned]) |s| s.join();
    }
    while (spawned < claim_readers) : (spawned += 1) {
        senders[spawned] = try std.Thread.spawn(.{}, GoSender.run, .{ pairs[spawned][0], copies, &go });
    }
    support.sleepMs(20);
    go.store(true, .release);

    // Every reader ends its connection: the one that read crossed the
    // bound with its own fds, the others found the lane full.
    var refused: usize = 0;
    for (&blocked) |*b| {
        try testing.expect(b.waitDone(closer_drain_ms));
        if (b.result.?) |_| {} else |err| {
            if (err == error.SystemResources) refused += 1;
        }
    }
    const lane = fd_io.closer.pendingIn(.received);
    const messages_in = (lane - (claim_limit - 1)) / claim_per_message;
    errdefer std.debug.print("round {d}: {d} of {d} messages got in; received lane {d}, bound {d} + one read {d}\n", .{ round, messages_in, claim_readers, lane, claim_limit, fd_io.closer.max_fds_per_recv });
    try testing.expectEqual(@as(usize, claim_readers), refused);
    // Exactly one message's fds got in: the lane stays within one read of
    // its bound.
    try testing.expectEqual(@as(usize, claim_limit - 1 + claim_per_message), lane);
    try testing.expect(lane <= claim_limit + fd_io.closer.max_fds_per_recv);
    try testing.expectEqual(@as(usize, 0), fd_io.closer.readClaims());
}
