const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");
const io_write_compat = @import("io-write-compat");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const events = capnpc.rpc.events;
const Connection = capnpc.rpc.transport.tcp.Connection;
const Peer = peer_impl.Peer;

const tcp = capnpc.rpc.transport.tcp;

/// Portable connected pair: loopback TCP via std.Io, so these suites run
/// on every platform (POSIX socketpair does not exist on Windows).
fn createSocketPair(io: std.Io) ![2]tcp.SocketFd {
    return tcp.createLoopbackSocketPair(io);
}

fn closeFd(io: std.Io, socket: tcp.SocketFd) void {
    tcp.closeFd(io, socket);
}

fn writeBytes(io: std.Io, socket: tcp.SocketFd, bytes: []const u8) void {
    _ = io_write_compat.write(io, socket.handle, bytes) catch {};
}

const EventRecorder = struct {
    idle_timeouts: usize = 0,
    call_deadline_timeouts: usize = 0,
    closed: usize = 0,

    fn onEvent(ctx_ptr: *anyopaque, event: events.Event) void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        switch (event) {
            .timeout => |t| switch (t.kind) {
                .idle_connection => self.idle_timeouts += 1,
                .call_deadline => self.call_deadline_timeouts += 1,
                else => {},
            },
            .connection => |c| {
                if (c.phase == .closed) self.closed += 1;
            },
            else => {},
        }
    }

    fn observer(self: *@This()) events.Observer {
        return events.Observer.init(self, onEvent);
    }
};

const ReturnRecorder = struct {
    exception_count: usize = 0,
    saw_deadline_reason: bool = false,

    fn onReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        _ = peer;
        _ = inbound_caps;
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag == .exception) {
            self.exception_count += 1;
            if (ret.exception) |ex| {
                if (std.mem.eql(u8, ex.reason, "deadline exceeded")) self.saw_deadline_reason = true;
            }
        }
    }
};

test "tick drives peer deadline sweep and idle timeout reaps the connection" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair(io);
    defer closeFd(io, fds[1]);

    var event_recorder = EventRecorder{};
    var return_recorder = ReturnRecorder{};

    var conn = try Connection.init(allocator, io, fds[0], .{
        .tick_interval_ms = 10,
        .idle_timeout_ms = 200,
        .observer = event_recorder.observer(),
    });

    var peer = Peer.initDetached(allocator);
    peer.attachConnection(&conn);
    peer.setClockIo(io);
    peer.setTimeouts(.{ .default_call_timeout_ms = 50 });
    peer.setObserver(event_recorder.observer());
    peer.start(null, null, null);

    // The remote end (fds[1]) stays silent: the bootstrap question can only
    // complete via deadline cancellation, and the connection only exits the
    // run loop via idle reaping.
    _ = try peer.sendBootstrap(&return_recorder, ReturnRecorder.onReturn);

    conn.run();

    try std.testing.expectEqual(@as(usize, 1), return_recorder.exception_count);
    try std.testing.expect(return_recorder.saw_deadline_reason);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.call_deadline_timeouts);
    try std.testing.expect(event_recorder.idle_timeouts >= 1);

    _ = peer.takeAttachedConnection(*Connection);
    peer.deinit();
    conn.deinit();
}

test "on_tick fires repeatedly while the connection is idle" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair(io);
    defer closeFd(io, fds[1]);

    const TickState = struct {
        ticks: usize = 0,

        fn onMessage(_: *Connection, _: []const u8) anyerror!void {}
        fn onError(_: *Connection, _: anyerror) void {}
        fn onClose(_: *Connection) void {}
        fn onTick(conn: *Connection) void {
            const state: *@This() = @ptrCast(@alignCast(conn.context().?));
            state.ticks += 1;
        }
    };

    var state = TickState{};
    var conn = try Connection.init(allocator, io, fds[0], .{
        .tick_interval_ms = 10,
        .idle_timeout_ms = 120,
    });
    defer conn.deinit();
    conn.start(&state, TickState.onMessage, TickState.onError, TickState.onClose);
    conn.on_tick = TickState.onTick;

    conn.run();

    // ~120ms of idle at a 10ms cadence: expect a healthy number of ticks
    // before the idle reap, with margin for slow CI.
    try std.testing.expect(state.ticks >= 3);
}

test "traffic resets the idle clock" {
    // Windows: the loopback pair cannot disable Nagle (std's AFD socket
    // handles reject ws2_32.setsockopt and std does not expose its AFD
    // option helper yet), and Nagle + delayed ACK stretches the feed
    // cadence past any reasonable idle bound. Ticks and idle reaping
    // themselves are covered on Windows by the other tests in this file.
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair(io);

    const Feeder = struct {
        fn run(fd: tcp.SocketFd, write_io: std.Io) void {
            // Feed partial frame bytes every 50ms for ~400ms, keeping the
            // connection alive past several idle windows. Write before each
            // sleep: on a loaded machine a delayed thread spawn must not
            // leave the connection idle long enough to be reaped before the
            // first byte arrives. The cadence (50ms) vs the idle bound
            // (250ms) leaves ~200ms of scheduling-jitter margin: CI runners
            // routinely stall threads for tens of milliseconds, which is
            // exactly what made tighter versions of this test flake.
            var i: usize = 0;
            while (i < 8) : (i += 1) {
                writeBytes(write_io, fd, &[_]u8{0});
                sleepMs(write_io, 50);
            }
            closeFd(write_io, fd);
        }

        fn sleepMs(sleep_io: std.Io, ms: u64) void {
            const duration: std.Io.Clock.Duration = .{
                .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
                .clock = .awake,
            };
            duration.sleep(sleep_io) catch {};
        }
    };

    const NoopCallbacks = struct {
        fn onMessage(_: *Connection, _: []const u8) anyerror!void {}
        fn onError(_: *Connection, _: anyerror) void {}
        fn onClose(_: *Connection) void {}
    };

    var conn = try Connection.init(allocator, io, fds[0], .{
        .tick_interval_ms = 10,
        .idle_timeout_ms = 250,
    });
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.start(&dummy, NoopCallbacks.onMessage, NoopCallbacks.onError, NoopCallbacks.onClose);

    const start_ns: i64 = @intCast(std.Io.Clock.awake.now(io).nanoseconds);
    const feeder = try std.Thread.spawn(.{}, Feeder.run, .{ fds[1], io });
    conn.run();
    // Measure before joining the feeder so a premature idle reap (the
    // regression this guards against) is not masked by the join wait.
    const elapsed_ns: i64 = @as(i64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) - start_ns;
    feeder.join();

    // The feeder kept the connection alive for ~400ms, which exceeds the
    // 250ms idle bound: traffic must have reset the idle clock at least
    // once. The margin (assert at 350ms vs the 250ms bound) absorbs the
    // scheduling jitter of loaded CI runners.
    try std.testing.expect(elapsed_ns >= 350 * std.time.ns_per_ms);
}

// ---------------------------------------------------------------------------
// First-frame deadline (Connection.first_frame_timeout_ms)
// ---------------------------------------------------------------------------

const FrameCounter = struct {
    frames: usize = 0,

    fn onMessage(conn: *Connection, _: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(conn.context().?));
        self.frames += 1;
    }
    fn onError(_: *Connection, _: anyerror) void {}
    fn onClose(_: *Connection) void {}
};

fn awakeNowNs(io: std.Io) i64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn sleepAwakeMs(io: std.Io, ms: u64) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}

test "first-frame deadline reaps a silent connection" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair(io);
    defer closeFd(io, fds[1]);

    var event_recorder = EventRecorder{};
    var counter = FrameCounter{};
    // No idle bound and no tick: the first-frame deadline alone must arm the
    // default tick and reap.
    var conn = try Connection.init(allocator, io, fds[0], .{ .observer = event_recorder.observer() });
    defer conn.deinit();
    conn.first_frame_timeout_ms = 150;
    conn.start(&counter, FrameCounter.onMessage, FrameCounter.onError, FrameCounter.onClose);

    // A regression must fail, not hang: past 3s the watchdog closes the
    // connection itself, which emits no timeout event.
    const Watchdog = struct {
        done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

        fn run(self: *@This(), watched: *Connection, watch_io: std.Io) void {
            var waited_ms: u64 = 0;
            while (waited_ms < 3_000 and !self.done.load(.acquire)) : (waited_ms += 10) sleepAwakeMs(watch_io, 10);
            if (!self.done.load(.acquire)) watched.requestClose();
        }
    };
    var watchdog = Watchdog{};
    const watchdog_thread = try std.Thread.spawn(.{}, Watchdog.run, .{ &watchdog, &conn, io });

    conn.run();
    // Measured from the connection's own deadline origin (same clock).
    const elapsed_ns = awakeNowNs(io) - conn.init_ns;
    watchdog.done.store(true, .release);
    watchdog_thread.join();

    try std.testing.expect(elapsed_ns >= 150 * std.time.ns_per_ms);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.idle_timeouts);
    try std.testing.expectEqual(@as(usize, 0), counter.frames);
}

test "first-frame deadline reaps a remote that trickles a frame it never completes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair(io);
    defer closeFd(io, fds[1]);

    const Trickler = struct {
        stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

        fn run(self: *@This(), fd: tcp.SocketFd, write_io: std.Io) void {
            // A header promising one 255-word segment, then one body byte
            // every 20ms: the frame never completes, and a byte always lands
            // well inside the 100ms tick, so poll never times out and only
            // the after-read check can see the deadline. Bounded at ~3s so a
            // regression fails the elapsed assertion instead of hanging.
            writeBytes(write_io, fd, &[_]u8{ 0, 0, 0, 0, 0xff, 0, 0, 0 });
            var i: usize = 0;
            while (i < 150 and !self.stop.load(.acquire)) : (i += 1) {
                sleepAwakeMs(write_io, 20);
                writeBytes(write_io, fd, &[_]u8{0});
            }
        }
    };

    var event_recorder = EventRecorder{};
    var counter = FrameCounter{};
    var conn = try Connection.init(allocator, io, fds[0], .{
        .tick_interval_ms = 100,
        .observer = event_recorder.observer(),
    });
    defer conn.deinit();
    conn.first_frame_timeout_ms = 300;
    conn.start(&counter, FrameCounter.onMessage, FrameCounter.onError, FrameCounter.onClose);

    var trickler = Trickler{};
    const feeder = try std.Thread.spawn(.{}, Trickler.run, .{ &trickler, fds[1], io });
    conn.run();
    // Measured from the connection's own deadline origin (same clock).
    const elapsed_ns = awakeNowNs(io) - conn.init_ns;
    trickler.stop.store(true, .release);
    feeder.join();

    try std.testing.expect(elapsed_ns >= 300 * std.time.ns_per_ms);
    try std.testing.expect(elapsed_ns < 2_000 * std.time.ns_per_ms);
    try std.testing.expectEqual(@as(usize, 1), event_recorder.idle_timeouts);
    try std.testing.expectEqual(@as(usize, 0), counter.frames);
}

test "the first complete frame disarms the first-frame deadline" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair(io);

    // One complete frame (one segment of zero words), written before the
    // run loop starts. The deadline is a full second, not 100ms: on Windows
    // the read runs as an io.concurrent task, and on a loaded CI runner that
    // task can start more than 100ms late, so a tick reaped the connection
    // with the frame already queued (seen on windows-latest). The speaker
    // then stays silent well past the deadline, then sends EOF.
    writeBytes(io, fds[1], &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 });
    const Speaker = struct {
        fn run(fd: tcp.SocketFd, write_io: std.Io) void {
            sleepAwakeMs(write_io, 2_500);
            closeFd(write_io, fd);
        }
    };

    var event_recorder = EventRecorder{};
    var counter = FrameCounter{};
    var conn = try Connection.init(allocator, io, fds[0], .{
        .tick_interval_ms = 10,
        .observer = event_recorder.observer(),
    });
    defer conn.deinit();
    conn.first_frame_timeout_ms = 1_000;
    conn.start(&counter, FrameCounter.onMessage, FrameCounter.onError, FrameCounter.onClose);

    const speaker = try std.Thread.spawn(.{}, Speaker.run, .{ fds[1], io });
    conn.run();
    speaker.join();

    // run() ended on the speaker's EOF, not on the first-frame deadline.
    try std.testing.expectEqual(@as(usize, 1), counter.frames);
    try std.testing.expectEqual(@as(usize, 0), event_recorder.idle_timeouts);
}

// ---------------------------------------------------------------------------
// Deadline reads (Transport.readTimeout)
// ---------------------------------------------------------------------------

test "readTimeout expires cleanly on a silent peer instead of panicking" {
    // The trap this exists to replace: arming SO_RCVTIMEO on the raw fd
    // makes a timed-out recv return EAGAIN, and Io.Threaded classifies
    // EAGAIN as a programmer bug (errnoBug) — a debug-build panic on a
    // perfectly normal deadline. Owning the deadline at the OPERATION
    // level cancels the read instead, so the timeout is just a value.
    const pair = try tcp.createLoopbackSocketPair(std.testing.io);
    defer tcp.closeFd(std.testing.io, pair[1]);

    var transport = try tcp.Transport.init(std.testing.allocator, std.testing.io, pair[0], 64);
    defer transport.deinit();

    // Peer never writes: the deadline is the only thing that can end this.
    const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    try std.testing.expectError(error.Timeout, transport.readTimeout(.{
        .duration = .{ .raw = std.Io.Duration.fromMilliseconds(50), .clock = .awake },
    }));
    const elapsed_ms = @divFloor(std.Io.Clock.awake.now(std.testing.io).nanoseconds - started, std.time.ns_per_ms);

    // It actually waited (not an instant spurious error) and returned.
    try std.testing.expect(elapsed_ms >= 40);
}

test "readTimeout delivers data that arrives before the deadline" {
    const pair = try tcp.createLoopbackSocketPair(std.testing.io);
    defer tcp.closeFd(std.testing.io, pair[1]);

    var transport = try tcp.Transport.init(std.testing.allocator, std.testing.io, pair[0], 64);
    defer transport.deinit();

    const payload = "deadline-read";
    try io_write_compat.writeAll(std.testing.io, pair[1].handle, payload);

    const n = try transport.readTimeout(.{ .duration = .{ .raw = std.Io.Duration.fromMilliseconds(2_000), .clock = .awake } });
    try std.testing.expectEqual(payload.len, n);
}

test "readTimeout expires before delayed data and leaves it for the next read" {
    const DelayedPeer = struct {
        var expired: std.atomic.Value(bool) = .init(false);
        var write_failed: std.atomic.Value(bool) = .init(false);

        fn awaitConcurrent(userdata: ?*anyopaque, batch: *std.Io.Batch, timeout: std.Io.Timeout) std.Io.Batch.AwaitConcurrentError!void {
            std.testing.io.vtable.batchAwaitConcurrent(userdata, batch, timeout) catch |err| {
                if (err == error.Timeout) expired.store(true, .release);
                return err;
            };
        }

        fn run(fd: tcp.SocketFd) void {
            const io = std.testing.io;
            defer tcp.closeFd(io, fd);
            const end = std.Io.Clock.awake.now(io).nanoseconds + 5 * std.time.ns_per_s;
            while (!expired.load(.acquire)) {
                if (std.Io.Clock.awake.now(io).nanoseconds >= end) {
                    write_failed.store(true, .release);
                    return;
                }
                std.Io.sleep(io, .fromMilliseconds(1), .awake) catch {
                    write_failed.store(true, .release);
                    return;
                };
            }
            // Start the delay only after the actual backend deadline fires.
            // Broken cleanup waits for this byte and returns it as late data.
            std.Io.sleep(io, .fromMilliseconds(250), .awake) catch {
                write_failed.store(true, .release);
                return;
            };
            io_write_compat.writeAll(io, fd.handle, "after-deadline") catch {
                write_failed.store(true, .release);
            };
        }
    };
    DelayedPeer.expired.store(false, .release);
    DelayedPeer.write_failed.store(false, .release);
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = DelayedPeer.awaitConcurrent;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    var transport = try tcp.Transport.init(std.testing.allocator, io, pair[0], 64);
    defer transport.deinit();
    const feeder = std.Thread.spawn(.{}, DelayedPeer.run, .{pair[1]}) catch |err| {
        tcp.closeFd(io, pair[1]);
        return err;
    };
    defer feeder.join();
    const timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(50), .clock = .awake } };
    try std.testing.expectError(error.Timeout, transport.readTimeout(timeout.toDeadline(io)));
    const n = try transport.readTimeout(.{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    try std.testing.expectEqualStrings("after-deadline", transport.read_buf[0..n]);
    try std.testing.expect(!DelayedPeer.write_failed.load(.acquire));
}

fn withoutNetReadBatchConcurrency(userdata: ?*anyopaque, batch: *std.Io.Batch, timeout: std.Io.Timeout) std.Io.Batch.AwaitConcurrentError!void {
    var index = batch.submitted.head;
    while (index != .none) {
        const submission = batch.storage[index.toIndex()].submission;
        if (submission.operation == .net_read) return error.ConcurrencyUnavailable;
        index = submission.node.next;
    }
    return std.testing.io.vtable.batchAwaitConcurrent(userdata, batch, timeout);
}

test "readTimeout tolerates a rejected batch slot already returned to unused" {
    const RejectedSubmission = struct {
        fn awaitConcurrent(userdata: ?*anyopaque, batch: *std.Io.Batch, timeout: std.Io.Timeout) std.Io.Batch.AwaitConcurrentError!void {
            const index = batch.submitted.head;
            const submission = batch.storage[index.toIndex()].submission;
            if (submission.operation != .net_read) return std.testing.io.vtable.batchAwaitConcurrent(userdata, batch, timeout);
            // The pinned Windows backend does this while rejecting net_read:
            // its error cleanup releases the sole slot, then restores the old
            // submitted head. No socket operation has started.
            batch.storage[index.toIndex()] = .{ .unused = .{ .prev = .none, .next = .none } };
            batch.unused = .{ .head = index, .tail = index };
            return error.ConcurrencyUnavailable;
        }
    };
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = RejectedSubmission.awaitConcurrent;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, pair[1]);
    var transport = try tcp.Transport.init(std.testing.allocator, io, pair[0], 64);
    defer transport.deinit();
    const payload = "after-rejection";
    try io_write_compat.writeAll(io, pair[1].handle, payload);
    const n = try transport.readTimeout(.{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    try std.testing.expectEqualStrings(payload, transport.read_buf[0..n]);
}

test "readTimeout works without batch concurrency and leaves the socket reusable" {
    // The pinned Windows backend supports cancellable reads but not concurrent
    // net_read batches. Model that public Io capability on every test host.
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = withoutNetReadBatchConcurrency;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    var peer_closed = false;
    defer if (!peer_closed) tcp.closeFd(io, pair[1]);
    var transport = try tcp.Transport.init(std.testing.allocator, io, pair[0], 64);
    defer transport.deinit();

    for (0..2) |_| {
        const timeout: std.Io.Timeout = .{ .duration = .{ .raw = std.Io.Duration.fromMilliseconds(20), .clock = .awake } };
        try std.testing.expectError(error.Timeout, transport.readTimeout(timeout.toDeadline(io)));
        // The canceled read must have finished: it cannot remain in the
        // background and consume bytes belonging to the next read.
        const payload = "after-timeout";
        try io_write_compat.writeAll(io, pair[1].handle, payload);
        const n = try transport.readTimeout(.{ .duration = .{ .raw = std.Io.Duration.fromMilliseconds(2_000), .clock = .awake } });
        try std.testing.expectEqualStrings(payload, transport.read_buf[0..n]);
    }
    tcp.closeFd(io, pair[1]);
    peer_closed = true;
    try std.testing.expectEqual(@as(usize, 0), try transport.readTimeout(.{ .duration = .{ .raw = std.Io.Duration.fromMilliseconds(2_000), .clock = .awake } }));
}

/// The writer side of the stream-race test, with the progress a failure
/// report needs: how many bytes were sent and whether the socket was closed.
const StreamFeed = struct {
    write_failed: std.atomic.Value(bool) = .init(false),
    written: std.atomic.Value(usize) = .init(0),
    closed: std.atomic.Value(bool) = .init(false),

    fn run(feed: *StreamFeed, write_io: std.Io, fd: tcp.SocketFd, bytes: []const u8) void {
        defer {
            tcp.closeFd(write_io, fd);
            feed.closed.store(true, .release);
        }
        std.Io.sleep(write_io, .fromMilliseconds(10), .awake) catch {
            feed.write_failed.store(true, .release);
            return;
        };
        for (bytes) |byte| {
            io_write_compat.writeAll(write_io, fd.handle, &.{byte}) catch {
                feed.write_failed.store(true, .release);
                return;
            };
            _ = feed.written.fetchAdd(1, .release);
            std.Io.sleep(write_io, .fromMilliseconds(1), .awake) catch {
                feed.write_failed.store(true, .release);
                return;
            };
        }
    }
};

/// Prints what separates the causes of an unexpected timed-read error, then
/// reads the rest of the stream and says whether it continued in order.
///
/// Windows CI failed this way once (run 38015605467, `error.Unexpected`).
/// The trace shows the pinned std's Windows batch await returned success with
/// no completion. In that backend this happens only when the receive's APC
/// reports STATUS_CANCELLED: `batchApc` moves the operation to the unused
/// list and the await loop then ends. Who cancelled it is not known.
/// The report tells these cases apart:
/// - the peer had closed (`written` = 128 and `closed`);
/// - the receive was cancelled without consuming bytes (the rest arrives in order);
/// - the batch lost a receive that was still in flight (later bytes are
///   missing or out of order);
/// - the socket handle was closed under the reader (the next read fails).
fn reportTimedReadFailure(
    transport: *tcp.Transport,
    io: std.Io,
    err: anyerror,
    payload: []const u8,
    received: usize,
    feed: *const StreamFeed,
    stats: struct { reads: usize, timeouts: usize, started: i96, end: i96 },
) void {
    const now = std.Io.Clock.awake.now(io).nanoseconds;
    std.debug.print(
        "timed read failed: {s} after {d} reads and {d} timeouts, {d} ms into the test; " ++
            "received {d} of {d} bytes; peer wrote {d}, peer closed: {}, peer write failed: {}\n",
        .{
            @errorName(err),                  stats.reads,
            stats.timeouts,                   @divFloor(now - stats.started, std.time.ns_per_ms),
            received,                         payload.len,
            feed.written.load(.acquire),      feed.closed.load(.acquire),
            feed.write_failed.load(.acquire),
        },
    );
    var offset = received;
    while (std.Io.Clock.awake.now(io).nanoseconds < stats.end) {
        const n = transport.readTimeout(.{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } }) catch |next_err| switch (next_err) {
            error.Timeout => continue,
            else => {
                std.debug.print("continuation: {s} at byte {d}\n", .{ @errorName(next_err), offset });
                return;
            },
        };
        if (n == 0) {
            std.debug.print("continuation: end of stream at byte {d} of {d}\n", .{ offset, payload.len });
            return;
        }
        if (n > payload.len - offset or !std.mem.eql(u8, payload[offset..][0..n], transport.read_buf[0..n])) {
            std.debug.print("continuation: {d} bytes read at byte {d} do not continue the stream (first is {d})\n", .{ n, offset, transport.read_buf[0] });
            return;
        }
        offset += n;
    }
    std.debug.print("continuation: test deadline at byte {d} of {d}\n", .{ offset, payload.len });
}

test "readTimeout preserves stream bytes across repeated deadline races" {
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = withoutNetReadBatchConcurrency;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    var transport = try tcp.Transport.init(std.testing.allocator, io, pair[0], 64);
    defer transport.deinit();

    var payload: [128]u8 = undefined;
    for (&payload, 0..) |*byte, index| byte.* = @intCast(index);
    var feed: StreamFeed = .{};
    const feeder = std.Thread.spawn(.{}, StreamFeed.run, .{ &feed, io, pair[1], &payload }) catch |err| {
        tcp.closeFd(io, pair[1]);
        return err;
    };
    defer feeder.join();

    const started = std.Io.Clock.awake.now(io).nanoseconds;
    const end = started + 10 * std.time.ns_per_s;
    var received: usize = 0;
    var reads: usize = 0;
    var timeouts: usize = 0;
    while (true) {
        try std.testing.expect(std.Io.Clock.awake.now(io).nanoseconds < end);
        const n = transport.readTimeout(.{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } }) catch |err| switch (err) {
            error.Timeout => {
                timeouts += 1;
                continue;
            },
            else => {
                reportTimedReadFailure(&transport, io, err, &payload, received, &feed, .{
                    .reads = reads,
                    .timeouts = timeouts,
                    .started = started,
                    .end = end,
                });
                return err;
            },
        };
        reads += 1;
        if (n == 0) break;
        try std.testing.expect(n <= payload.len - received);
        try std.testing.expectEqualSlices(u8, payload[received..][0..n], transport.read_buf[0..n]);
        received += n;
    }
    try std.testing.expectEqual(payload.len, received);
    try std.testing.expect(!feed.write_failed.load(.acquire));
}

/// Recreates, on purpose, the batch state behind the Windows timed-read
/// flake of the test above.
///
/// In Windows CI loops of that test (about 11,000 runs), about 1% failed
/// with `error.Unexpected`: the pinned std's (0.17.0) `batchAwaitConcurrent`
/// returned success with nothing completed. The AFD receive had ended
/// STATUS_CANCELLED within about 0.1 ms of being posted, and `batchApc`
/// files a cancelled operation under `unused`, so the await loop found
/// nothing pending and returned. Nothing was in flight afterwards
/// (`NtCancelIoFileEx(fd, NULL)` returned STATUS_NOT_FOUND), and the
/// receive took no bytes (the rest of the stream arrived in order). It
/// always came right after a timed read on the same thread whose timeout
/// cancellation raced the receive's data completion. The cause is inside
/// Windows: neither the thread alert nor the reuse of the IOSB address is it.
///
/// For the AFD receives (`device_io_control`) it intercepts (by default
/// the first one the read posts), this await builds that state from the
/// real backend's own steps:
/// 1. It posts the receive with an await whose deadline has already passed.
///    The peer has sent nothing yet, so the receive stays pending.
/// 2. It cancels the receive with the backend's `batchCancel` (after the
///    same thread alert that the transport's `cancelWindowsReceive` sends).
///    The IOSB ends STATUS_CANCELLED with no bytes, and `batchApc` files
///    the slot under `unused`.
/// 3. Only then do the peer's bytes arrive (unless `send` is false), and
///    only in the last receive it intercepts: with `inject_count` above 1,
///    the receives before it stay silent, so each one ends cancelled too.
/// 4. It returns success with nothing completed.
/// After step 2 the batch is exactly what the CI probe printed: nothing
/// pending, nothing completed, the slot unused, no receive in flight, and
/// no bytes taken. `matched` counts the injections that built that state,
/// so a test cannot pass against a different one. Other awaits pass
/// through. `posts` counts every receive the read posted, intercepted or
/// not.
///
/// The counters are atomics: a `Connection` reads on an io worker thread.
/// `arm` sets the options before the read starts, and they stay fixed
/// while it runs.
const SpuriouslyCancelledReceive = struct {
    const Mode = enum {
        /// Build the state above, in steps 1 to 4.
        inject,
        /// Pass-through: post the receive (step 1), report it pending in
        /// `posted`, then wait for it with the caller's own timeout. A test
        /// then acts on a receive it knows is in flight.
        posted_receive,
    };

    /// What else happens around step 3.
    const AfterCancel = enum {
        none,
        /// Shut the transport down (`Transport.shutdown`) after step 2:
        /// the transport closes as Windows cancels the receive.
        close_transport,
        /// As the last step before returning, after any write: wait (at most
        /// 5 s) for an Io cancellation of this task, acknowledge it and
        /// re-arm it (`recancel`). The cancellation is then still pending
        /// when the read finds its receive cancelled. `ready_for_cancel`
        /// says the wait has begun.
        await_io_cancel,
    };

    const Options = struct {
        /// The connected peer, which sends `bytes` in step 3.
        peer: tcp.SocketFd,
        mode: Mode = .inject,
        /// The first intercepted receive, as counted in `posts` (from 0).
        inject_from: usize = 0,
        /// How many receives in a row, from `inject_from`, it intercepts.
        inject_count: usize = 1,
        /// Sleep past the caller's deadline before step 3.
        outlive_deadline: bool = false,
        /// Whether step 3 of the last intercepted receive writes `bytes`
        /// from the peer.
        send: bool = true,
        bytes: []const u8 = first_bytes,
        after_cancel: AfterCancel = .none,
        /// The transport `.close_transport` shuts down.
        transport: ?*tcp.Transport = null,
    };

    const first_bytes = "after-spurious-cancel";

    /// Intercepts nothing until `arm`.
    var options: Options = .{ .peer = undefined, .inject_count = 0 };
    /// The awaits whose submitted head is an AFD receive: each one is a
    /// receive the read posted.
    var posts: std.atomic.Value(usize) = .init(0);
    var injected: std.atomic.Value(usize) = .init(0);
    /// The injections whose batch, after step 2, was the CI probe's state.
    var matched: std.atomic.Value(usize) = .init(0);
    /// Step 3 wrote the peer's bytes.
    var sent: std.atomic.Value(bool) = .init(false);
    var write_failed: std.atomic.Value(bool) = .init(false);
    /// `.posted_receive`: the receive is pending.
    var posted: std.atomic.Value(bool) = .init(false);
    /// `.await_io_cancel`: the wait for the cancellation has begun.
    var ready_for_cancel: std.atomic.Value(bool) = .init(false);
    /// `.await_io_cancel`: the cancellation arrived, and was re-armed.
    var cancel_seen: std.atomic.Value(bool) = .init(false);

    fn arm(armed: Options) void {
        options = armed;
        posts.store(0, .release);
        injected.store(0, .release);
        matched.store(0, .release);
        sent.store(false, .release);
        write_failed.store(false, .release);
        posted.store(false, .release);
        ready_for_cancel.store(false, .release);
        cancel_seen.store(false, .release);
    }

    /// `count` injections, each of which built the CI probe's state, and no
    /// failed write of the peer's bytes.
    fn expectInjected(count: usize) !void {
        try std.testing.expectEqual(count, injected.load(.acquire));
        try std.testing.expectEqual(count, matched.load(.acquire));
        try std.testing.expect(!write_failed.load(.acquire));
    }

    fn awaitConcurrent(userdata: ?*anyopaque, batch: *std.Io.Batch, timeout: std.Io.Timeout) std.Io.Batch.AwaitConcurrentError!void {
        if (comptime builtin.os.tag == .windows) {
            const head = batch.submitted.head;
            if (head != .none and batch.storage[head.toIndex()].submission.operation == .device_io_control) {
                const index = posts.fetchAdd(1, .acq_rel);
                if (index >= options.inject_from and index - options.inject_from < options.inject_count) {
                    const last = index - options.inject_from + 1 == options.inject_count;
                    return switch (options.mode) {
                        .inject => inject(userdata, batch, timeout, last),
                        .posted_receive => postedReceive(userdata, batch, timeout),
                    };
                }
            }
        }
        return std.testing.io.vtable.batchAwaitConcurrent(userdata, batch, timeout);
    }

    fn inject(userdata: ?*anyopaque, batch: *std.Io.Batch, timeout: std.Io.Timeout, last: bool) std.Io.Batch.AwaitConcurrentError!void {
        // 1. Post the receive. The deadline has passed, so the await
        // reports Timeout and leaves the receive pending.
        try postPending(userdata, batch);
        const was_posted = batch.pending.head != .none and batch.completed.head == .none;
        // 2. Cancel it. The backend's batchCancel first waits for an APC or
        // an alert, so alert this thread first.
        if (batch.pending.head != .none) alertThisThread();
        std.testing.io.vtable.batchCancel(userdata, batch);
        if (was_posted and batch.pending.head == .none and
            batch.completed.head == .none and batch.unused.head != .none)
        {
            _ = matched.fetchAdd(1, .acq_rel);
        }
        _ = injected.fetchAdd(1, .acq_rel);
        if (options.after_cancel == .close_transport) {
            const transport = options.transport orelse @panic("close_transport needs the transport");
            transport.shutdown();
        }
        if (options.outlive_deadline) {
            const deadline = timeout.toDeadline(std.testing.io);
            while (deadline.toDurationFromNow(std.testing.io)) |remaining| {
                if (remaining.raw.nanoseconds <= 0) break;
                try std.Io.sleep(std.testing.io, .fromMilliseconds(5), .awake);
            }
        }
        // 3. The peer's bytes arrive now, after the last cancelled receive.
        if (options.send and last) {
            io_write_compat.writeAll(std.testing.io, options.peer.handle, options.bytes) catch {
                write_failed.store(true, .release);
            };
            sent.store(true, .release);
        }
        // Last, so no Io call of this wrapper meets the re-armed cancellation.
        if (options.after_cancel == .await_io_cancel) awaitIoCancel();
        // 4. Success, with nothing completed.
    }

    fn postedReceive(userdata: ?*anyopaque, batch: *std.Io.Batch, timeout: std.Io.Timeout) std.Io.Batch.AwaitConcurrentError!void {
        try postPending(userdata, batch);
        if (batch.completed.head != .none) return;
        posted.store(batch.pending.head != .none, .release);
        return std.testing.io.vtable.batchAwaitConcurrent(userdata, batch, timeout);
    }

    /// Posts the submitted receive with an await whose deadline has passed:
    /// it returns at once, and a silent peer's receive stays pending.
    fn postPending(userdata: ?*anyopaque, batch: *std.Io.Batch) std.Io.Batch.AwaitConcurrentError!void {
        const passed: std.Io.Timeout = .{ .duration = .{ .raw = .zero, .clock = .awake } };
        std.testing.io.vtable.batchAwaitConcurrent(userdata, batch, passed) catch |err| switch (err) {
            error.Timeout => {},
            else => return err,
        };
    }

    fn alertThisThread() void {
        const windows = std.os.windows;
        const status = windows.ntdll.NtAlertThread(windows.GetCurrentThread());
        if (status != .SUCCESS) std.debug.panic("cannot alert this thread: NTSTATUS=0x{x}", .{@backingInt(status)});
    }

    fn awaitIoCancel() void {
        const io = std.testing.io;
        ready_for_cancel.store(true, .release);
        const end = std.Io.Clock.awake.now(io).nanoseconds + 5 * std.time.ns_per_s;
        while (std.Io.Clock.awake.now(io).nanoseconds < end) {
            std.Io.sleep(io, .fromMilliseconds(1), .awake) catch |err| switch (err) {
                error.Canceled => {
                    io.recancel();
                    cancel_seen.store(true, .release);
                    return;
                },
            };
        }
    }
};

/// Reads exactly `expected` with timed reads, and fails on any other bytes.
fn expectTimedStream(transport: *tcp.Transport, expected: []const u8, timeout: std.Io.Timeout) !void {
    var received: usize = 0;
    while (received < expected.len) {
        const n = try transport.readTimeout(timeout);
        try std.testing.expect(n != 0);
        try std.testing.expect(n <= expected.len - received);
        try std.testing.expectEqualSlices(u8, expected[received..][0..n], transport.read_buf[0..n]);
        received += n;
    }
}

test "readTimeout posts again a receive Windows cancelled without data" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = SpuriouslyCancelledReceive.awaitConcurrent;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    var peer_closed = false;
    defer if (!peer_closed) tcp.closeFd(io, pair[1]);
    var transport = try tcp.Transport.init(std.testing.allocator, io, pair[0], 64);
    defer transport.deinit();
    SpuriouslyCancelledReceive.arm(.{ .peer = pair[1] });

    // The receive ended cancelled and took nothing, so the read must post
    // it again and return the peer's bytes before its deadline, not fail
    // with error.Unexpected.
    const generous: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    try expectTimedStream(&transport, SpuriouslyCancelledReceive.first_bytes, generous);
    try SpuriouslyCancelledReceive.expectInjected(1);

    // The stream continues in order, and the end of stream still reads as 0.
    try io_write_compat.writeAll(io, pair[1].handle, "and-then-the-rest");
    try expectTimedStream(&transport, "and-then-the-rest", generous);
    tcp.closeFd(io, pair[1]);
    peer_closed = true;
    try std.testing.expectEqual(@as(usize, 0), try transport.readTimeout(generous));
}

test "readTimeout reports Timeout when a receive Windows cancelled without data outlives the deadline" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = SpuriouslyCancelledReceive.awaitConcurrent;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, pair[1]);
    var transport = try tcp.Transport.init(std.testing.allocator, io, pair[0], 64);
    defer transport.deinit();
    SpuriouslyCancelledReceive.arm(.{ .peer = pair[1], .outlive_deadline = true });

    // The deadline passed while the receive was cancelled: the read times
    // out, and the bytes stay for the next read.
    const short: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(50), .clock = .awake } };
    try std.testing.expectError(error.Timeout, transport.readTimeout(short));
    try SpuriouslyCancelledReceive.expectInjected(1);
    const generous: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    try expectTimedStream(&transport, SpuriouslyCancelledReceive.first_bytes, generous);
}

test "readTimeout retains a completed read when the batch reports a concurrency error" {
    const CompletedBeforeError = struct {
        fn awaitConcurrent(_: ?*anyopaque, batch: *std.Io.Batch, _: std.Io.Timeout) std.Io.Batch.AwaitConcurrentError!void {
            // Io permits completions to remain available after an await
            // error. Perform an actual socket read before reporting one.
            try batch.awaitAsync(std.testing.io);
            return error.ConcurrencyUnavailable;
        }
    };
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = CompletedBeforeError.awaitConcurrent;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, pair[1]);
    var transport = try tcp.Transport.init(std.testing.allocator, io, pair[0], 64);
    defer transport.deinit();
    const payload = "already-read";
    try io_write_compat.writeAll(io, pair[1].handle, payload);
    const n = try transport.readTimeout(.{ .duration = .{ .raw = std.Io.Duration.fromMilliseconds(50), .clock = .awake } });
    try std.testing.expectEqualStrings(payload, transport.read_buf[0..n]);
}

/// A read whose socket receive completes just as the caller's task is
/// cancelled. Both arms take the bytes from the socket, set `consumed`,
/// then block in a 60 s cancellable sleep, so the task's cancellation lands
/// after the receive took the bytes and before the read sees them:
/// - `awaitConcurrent`, for the transport's own AFD receive on Windows: a
///   real completion (`awaitAsync`), then the sleep, then `error.Canceled`
///   from it. The read finds the completed receive after the cancellation.
/// - `operate`, for std's `net_read`: a real read, then the sleep. On
///   Windows it then reports `error.Canceled`, as std's (0.17.0)
///   `deviceIoControl` does when cancellation races the receive's APC: it
///   drops the bytes AFD took. That is the loss the transport's own receive
///   exists to avoid.
/// Other batches get `error.ConcurrencyUnavailable`, as std's Windows
/// backend gives a `net_read` batch.
const CompletionAtCancel = struct {
    var consumed: std.atomic.Value(bool) = .init(false);
    var finished: std.atomic.Value(bool) = .init(false);

    fn reset() void {
        consumed.store(false, .release);
        finished.store(false, .release);
    }

    fn awaitConcurrent(_: ?*anyopaque, batch: *std.Io.Batch, _: std.Io.Timeout) std.Io.Batch.AwaitConcurrentError!void {
        if (comptime builtin.os.tag == .windows) {
            const submission = batch.storage[batch.submitted.head.toIndex()].submission;
            if (submission.operation == .device_io_control) {
                try batch.awaitAsync(std.testing.io);
                consumed.store(true, .release);
                defer finished.store(true, .release);
                // Model cancellation arriving after AFD completed the
                // receive but before the batch await reports completion.
                try std.Io.sleep(std.testing.io, .fromSeconds(60), .awake);
                return;
            }
        }
        return error.ConcurrencyUnavailable;
    }

    fn operate(userdata: ?*anyopaque, operation: std.Io.Operation) std.Io.Cancelable!std.Io.Operation.Result {
        const result = try std.testing.io.vtable.operate(userdata, operation);
        if (operation == .net_read) {
            _ = result.net_read catch return result;
            consumed.store(true, .release);
            // The socket read succeeded. Hold its publication until task
            // cancellation joins it, then report those consumed bytes.
            std.Io.sleep(std.testing.io, .fromSeconds(60), .awake) catch {};
            finished.store(true, .release);
            // The pinned Windows ordinary read loses a successful IOSB
            // when cancellation races its APC. The timed path must use
            // an owned receive batch, preserving that completion instead.
            if (comptime builtin.os.tag == .windows) return error.Canceled;
        }
        return result;
    }

    /// Waits (at most 2 s) until a read has taken the peer's bytes.
    fn awaitConsumed() !void {
        const wait_until = std.Io.Clock.awake.now(std.testing.io).nanoseconds + std.time.ns_per_s * 2;
        while (!consumed.load(.acquire)) {
            try std.testing.expect(std.Io.Clock.awake.now(std.testing.io).nanoseconds < wait_until);
            try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
        }
    }
};

/// Task functions that read, then pass one cancellation point
/// (`checkCancel`): `rearmed` records whether a cancellation of the task
/// was still pending after the read returned.
const ReadThenCancelPoint = struct {
    var rearmed: std.atomic.Value(bool) = .init(false);

    fn untimed(transport: *tcp.Transport) tcp.Transport.ReadError!usize {
        const n = try transport.read();
        cancelPoint();
        return n;
    }

    fn timed(transport: *tcp.Transport, timeout: std.Io.Timeout) tcp.Transport.ReadTimeoutError!usize {
        const n = try transport.readTimeout(timeout);
        cancelPoint();
        return n;
    }

    fn cancelPoint() void {
        std.testing.io.checkCancel() catch {
            rearmed.store(true, .release);
        };
    }
};

test "readTimeout preserves a successful read while joining caller cancellation" {
    CompletionAtCancel.reset();
    ReadThenCancelPoint.rearmed.store(false, .release);
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = CompletionAtCancel.awaitConcurrent;
    vtable.operate = CompletionAtCancel.operate;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, pair[1]);
    var transport = try tcp.Transport.init(std.testing.allocator, io, pair[0], 64);
    defer transport.deinit();
    const payload = "already-consumed";
    try io_write_compat.writeAll(io, pair[1].handle, payload);

    var read = try std.Io.concurrent(std.testing.io, ReadThenCancelPoint.timed, .{
        &transport,
        std.Io.Timeout{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } },
    });
    defer _ = read.cancel(std.testing.io) catch 0;
    try CompletionAtCancel.awaitConsumed();
    const result = read.cancel(std.testing.io);
    try std.testing.expect(CompletionAtCancel.finished.load(.acquire));
    try std.testing.expectEqualStrings(payload, transport.read_buf[0..payload.len]);
    try std.testing.expectEqual(payload.len, try result);
    // The read returned the bytes, and on Windows the cancellation it met is
    // still pending at the task's next cancellation point. Elsewhere the
    // timed read does not re-arm it yet: its task path
    // (`ioReadVecTaskTimeout`, which this wrapper forces) returns the bytes
    // and drops the cancellation, so this asserts nothing there.
    if (comptime builtin.os.tag == .windows) try std.testing.expect(ReadThenCancelPoint.rearmed.load(.acquire));
}

// ---------------------------------------------------------------------------
// Untimed reads (Transport.read) on Windows
// ---------------------------------------------------------------------------
//
// The stray STATUS_CANCELLED above reaches untimed reads too. In a CI
// experiment, `Transport.read` right after a timed read on the same socket
// and thread got it in 28 of 1,600 runs. std's (0.17.0) `netReadWindows`
// treats a cancellation it did not ask for as unreachable, so each of those
// runs aborted. A `Connection` reads through `Transport.read` too, on an io
// worker (`winReadTask`). The tests below recreate the state on purpose
// with `SpuriouslyCancelledReceive`, on each untimed read: `read`,
// `readTimeout(.none)`, and the `Connection` loop, the last one also right
// after a timed read made on another thread. They also pin down how a read
// ends when something asked for that: an Io cancellation, a transport that
// is closing when Windows cancels the receive, or the transport and then
// its handle closed under the read.
//
// Each test that blocks in a read the fix must complete has a watchdog. If
// the wrapper never ran (the read did not take the transport's own receive),
// the watchdog ends the read after 5 s, so the test fails on its counts
// instead of hanging.

/// One empty Cap'n Proto frame: one segment of zero words.
const empty_frame = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };

/// Polls `flag` every millisecond for at most `limit_ms`. Returns whether it
/// was set.
fn waitUntil(flag: *const std.atomic.Value(bool), limit_ms: u64) bool {
    const io = std.testing.io;
    const end = std.Io.Clock.awake.now(io).nanoseconds + @as(i96, limit_ms) * std.time.ns_per_ms;
    while (!flag.load(.acquire)) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= end) return false;
        std.Io.sleep(io, .fromMilliseconds(1), .awake) catch return flag.load(.acquire);
    }
    return true;
}

/// Ends a read that should have completed by itself: after 5 s it shuts the
/// peer down, so the pending read sees the end of the stream and the test
/// fails instead of hanging.
const ReadWatchdog = struct {
    const limit_ms = 5_000;

    done: std.atomic.Value(bool) = .init(false),
    fired: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,

    fn start(self: *ReadWatchdog, peer: tcp.SocketFd) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{ self, peer });
    }

    /// Idempotent.
    fn stop(self: *ReadWatchdog) void {
        self.done.store(true, .release);
        if (self.thread) |thread| thread.join();
        self.thread = null;
    }

    fn run(self: *ReadWatchdog, peer: tcp.SocketFd) void {
        if (waitUntil(&self.done, limit_ms)) return;
        self.fired.store(true, .release);
        tcp.runtime.shutdownFd(std.testing.io, peer);
    }
};

/// Ends a `Connection` test once `SpuriouslyCancelledReceive` has sent the
/// peer's frame: it shuts the peer down, so the loop reads the frame, then
/// the end of the stream, and `run` returns. If the frame was not sent
/// within 5 s, it calls `requestClose` instead (`fired`), so the test fails
/// on its counts instead of hanging.
const FrameThenEnd = struct {
    done: std.atomic.Value(bool) = .init(false),
    fired: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,

    fn start(self: *FrameThenEnd, conn: *Connection, peer: tcp.SocketFd) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{ self, conn, peer });
    }

    /// Idempotent.
    fn stop(self: *FrameThenEnd) void {
        self.done.store(true, .release);
        if (self.thread) |thread| thread.join();
        self.thread = null;
    }

    fn run(self: *FrameThenEnd, conn: *Connection, peer: tcp.SocketFd) void {
        const io = std.testing.io;
        const end = std.Io.Clock.awake.now(io).nanoseconds + ReadWatchdog.limit_ms * std.time.ns_per_ms;
        while (std.Io.Clock.awake.now(io).nanoseconds < end) {
            if (self.done.load(.acquire)) return;
            if (SpuriouslyCancelledReceive.sent.load(.acquire)) {
                tcp.runtime.shutdownFd(io, peer);
                return;
            }
            std.Io.sleep(io, .fromMilliseconds(1), .awake) catch {};
        }
        self.fired.store(true, .release);
        conn.requestClose();
    }
};

/// The two untimed reads of `Transport`.
const UntimedRead = enum {
    read,
    read_timeout_none,

    fn call(how: UntimedRead, transport: *tcp.Transport) tcp.Transport.ReadTimeoutError!usize {
        return switch (how) {
            .read => transport.read(),
            .read_timeout_none => transport.readTimeout(.none),
        };
    }
};

/// Checks that the `first_n` bytes the last read returned start `expected`,
/// then reads the rest of it with `how`. Fails on any other bytes.
fn expectUntimedStream(transport: *tcp.Transport, how: UntimedRead, expected: []const u8, first_n: usize) !void {
    var n = first_n;
    var received: usize = 0;
    while (true) {
        try std.testing.expect(n != 0);
        try std.testing.expect(n <= expected.len - received);
        try std.testing.expectEqualSlices(u8, expected[received..][0..n], transport.read_buf[0..n]);
        received += n;
        if (received == expected.len) return;
        n = try how.call(transport);
    }
}

/// A `Transport` on a loopback pair whose Io sends every batch await through
/// `SpuriouslyCancelledReceive`, and the watchdog of its reads. Initialise
/// it in place (`init`): the transport keeps a pointer to `vtable`.
const WrappedTransport = struct {
    vtable: std.Io.VTable,
    pair: [2]tcp.SocketFd,
    peer_open: bool,
    transport: tcp.Transport,
    watchdog: ReadWatchdog,

    fn init(self: *WrappedTransport) !void {
        self.vtable = std.testing.io.vtable.*;
        self.vtable.batchAwaitConcurrent = SpuriouslyCancelledReceive.awaitConcurrent;
        self.watchdog = .{};
        self.pair = try tcp.createLoopbackSocketPair(self.io());
        self.peer_open = true;
        errdefer self.closePeer();
        self.transport = try tcp.Transport.init(std.testing.allocator, self.io(), self.pair[0], 64);
    }

    fn deinit(self: *WrappedTransport) void {
        self.watchdog.stop();
        self.transport.deinit();
        self.closePeer();
    }

    fn io(self: *const WrappedTransport) std.Io {
        return .{ .userdata = std.testing.io.userdata, .vtable = &self.vtable };
    }

    fn peer(self: *const WrappedTransport) tcp.SocketFd {
        return self.pair[1];
    }

    /// Starts the watchdog: after 5 s it shuts the peer down.
    fn guard(self: *WrappedTransport) !void {
        try self.watchdog.start(self.pair[1]);
    }

    /// Stops the watchdog first, so it never touches a closed handle.
    fn closePeer(self: *WrappedTransport) void {
        self.watchdog.stop();
        if (!self.peer_open) return;
        tcp.closeFd(self.io(), self.pair[1]);
        self.peer_open = false;
    }

    fn send(self: *WrappedTransport, bytes: []const u8) !void {
        try io_write_compat.writeAll(self.io(), self.pair[1].handle, bytes);
    }

    /// The stream goes on in order, and its end reads as 0.
    fn expectStreamContinuesToEnd(self: *WrappedTransport, how: UntimedRead) !void {
        try self.send("and-then-the-rest");
        try expectUntimedStream(&self.transport, how, "and-then-the-rest", try how.call(&self.transport));
        try std.testing.expect(!self.watchdog.fired.load(.acquire));
        self.closePeer();
        try std.testing.expectEqual(@as(usize, 0), try how.call(&self.transport));
    }
};

/// An untimed read whose first receive Windows cancels without data: it
/// posts the receive again and returns the peer's bytes, and the stream
/// goes on. Before, std's `netReadWindows` aborted the process.
fn expectUntimedRepost(how: UntimedRead) !void {
    var fx: WrappedTransport = undefined;
    try fx.init();
    defer fx.deinit();
    SpuriouslyCancelledReceive.arm(.{ .peer = fx.peer() });
    try fx.guard();

    const n = try how.call(&fx.transport);
    // The cancelled receive and the one posted again.
    try std.testing.expectEqual(@as(usize, 2), SpuriouslyCancelledReceive.posts.load(.acquire));
    try expectUntimedStream(&fx.transport, how, SpuriouslyCancelledReceive.first_bytes, n);
    try SpuriouslyCancelledReceive.expectInjected(1);
    try fx.expectStreamContinuesToEnd(how);
}

test "read posts again a receive Windows cancelled without data" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    try expectUntimedRepost(.read);
}

test "readTimeout(.none) posts again a receive Windows cancelled without data" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    try expectUntimedRepost(.read_timeout_none);
}

test "read right after a timed read on the same thread posts again a receive Windows cancelled without data" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    var fx: WrappedTransport = undefined;
    try fx.init();
    defer fx.deinit();
    // Nothing is injected into the timed read.
    SpuriouslyCancelledReceive.arm(.{ .peer = fx.peer(), .inject_from = std.math.maxInt(usize) });
    try fx.guard();

    // The CI experiment's sequence: a timed read, then an untimed read on
    // the same socket and thread, whose receive Windows cancels.
    const short: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } };
    try std.testing.expectError(error.Timeout, fx.transport.readTimeout(short));
    const timed_posts = SpuriouslyCancelledReceive.posts.load(.acquire);
    try std.testing.expect(timed_posts >= 1);
    SpuriouslyCancelledReceive.options.inject_from = timed_posts;

    const n = try fx.transport.read();
    try std.testing.expectEqual(timed_posts + 2, SpuriouslyCancelledReceive.posts.load(.acquire));
    try expectUntimedStream(&fx.transport, .read, SpuriouslyCancelledReceive.first_bytes, n);
    try SpuriouslyCancelledReceive.expectInjected(1);
    try fx.expectStreamContinuesToEnd(.read);
}

/// Runs a `Connection` whose first receive (from `inject_from`) Windows
/// cancels without data, then gets one empty frame and the end of the
/// stream. With `timed_read_first`, the test thread makes a timed read on
/// the connection's transport before `run`: the receive the loop's io
/// worker posts is then the next one on that socket, on another thread.
fn expectConnectionRepost(timed_read_first: bool) !void {
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = SpuriouslyCancelledReceive.awaitConcurrent;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, pair[1]);
    var counter = FrameCounter{};
    var conn = try Connection.init(std.testing.allocator, io, pair[0], .{ .tick_interval_ms = 10 });
    defer conn.deinit();
    conn.start(&counter, FrameCounter.onMessage, FrameCounter.onError, FrameCounter.onClose);
    SpuriouslyCancelledReceive.arm(.{
        .peer = pair[1],
        .bytes = &empty_frame,
        .inject_from = if (timed_read_first) std.math.maxInt(usize) else 0,
    });

    if (timed_read_first) {
        // What an accept hook may do before `run` (WorkerPool's AcceptFn,
        // or code between `accept` and `run`).
        const short: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } };
        try std.testing.expectError(error.Timeout, conn.transport.readTimeout(short));
        SpuriouslyCancelledReceive.options.inject_from = SpuriouslyCancelledReceive.posts.load(.acquire);
        try std.testing.expect(SpuriouslyCancelledReceive.options.inject_from >= 1);
    }
    const first_untimed = SpuriouslyCancelledReceive.options.inject_from;

    var ender: FrameThenEnd = .{};
    try ender.start(&conn, pair[1]);
    defer ender.stop();
    conn.run();
    ender.stop();

    // run() ended on the peer's end of stream, after the frame.
    try std.testing.expect(!ender.fired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), counter.frames);
    try std.testing.expect(conn.last_error == null);
    try SpuriouslyCancelledReceive.expectInjected(1);
    try std.testing.expect(SpuriouslyCancelledReceive.posts.load(.acquire) >= first_untimed + 2);
}

test "Connection's Windows read loop posts again a receive Windows cancelled without data" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    try expectConnectionRepost(false);
}

test "Connection reads on after a timed read on its transport, across threads, when Windows cancels the receive without data" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    try expectConnectionRepost(true);
}

/// A read whose receive Windows cancels without data just as the transport
/// closes returns 0, as a read of a closing transport does, and posts
/// nothing on a handle that may be closed already.
fn expectCloseEndsRead(how: enum { read, timed }) !void {
    var fx: WrappedTransport = undefined;
    try fx.init();
    defer fx.deinit();
    SpuriouslyCancelledReceive.arm(.{
        .peer = fx.peer(),
        .send = false,
        .after_cancel = .close_transport,
        .transport = &fx.transport,
    });
    try fx.guard();

    const generous: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    const n = switch (how) {
        .read => try fx.transport.read(),
        .timed => try fx.transport.readTimeout(generous),
    };
    try std.testing.expectEqual(@as(usize, 0), n);
    try std.testing.expectEqual(@as(usize, 1), SpuriouslyCancelledReceive.posts.load(.acquire));
    try SpuriouslyCancelledReceive.expectInjected(1);
    try std.testing.expect(!fx.watchdog.fired.load(.acquire));
}

test "read returns 0 without posting again when the transport closes as Windows cancels its receive" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    try expectCloseEndsRead(.read);
}

test "readTimeout returns 0 without posting again when the transport closes as Windows cancels its receive" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    try expectCloseEndsRead(.timed);
}

test "an Io cancellation that lands with a receive Windows cancelled without data ends read with error.Canceled" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    var fx: WrappedTransport = undefined;
    try fx.init();
    defer fx.deinit();
    // The peer stays silent until the read has ended, so the wrapper makes
    // no Io call after the cancellation is re-armed.
    SpuriouslyCancelledReceive.arm(.{ .peer = fx.peer(), .send = false, .after_cancel = .await_io_cancel });

    var read = try std.Io.concurrent(std.testing.io, tcp.Transport.read, .{&fx.transport});
    defer _ = read.cancel(std.testing.io) catch 0;
    // Cancel once the wrapper has built the state and waits for the
    // cancellation: it then meets the read's check before a new receive,
    // not the cancelled receive's await.
    try std.testing.expect(waitUntil(&SpuriouslyCancelledReceive.ready_for_cancel, ReadWatchdog.limit_ms));
    try std.testing.expectError(error.Canceled, read.cancel(std.testing.io));
    try std.testing.expect(SpuriouslyCancelledReceive.cancel_seen.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), SpuriouslyCancelledReceive.posts.load(.acquire));
    try SpuriouslyCancelledReceive.expectInjected(1);

    // No receive is left in flight: the next read gets every byte.
    try fx.send("after-io-cancel");
    try expectUntimedStream(&fx.transport, .read, "after-io-cancel", try fx.transport.read());
}

test "read posts again 20 receives in a row that Windows cancelled without data, waiting longer between them" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    var fx: WrappedTransport = undefined;
    try fx.init();
    defer fx.deinit();
    // In CI one untimed read met 9 in a row, which a limit of 8 re-posts
    // per read turned into error.Unexpected. Only the last one is followed
    // by the peer's bytes.
    const in_a_row = 20;
    SpuriouslyCancelledReceive.arm(.{ .peer = fx.peer(), .inject_count = in_a_row });
    try fx.guard();

    const started = std.Io.Clock.awake.now(std.testing.io);
    const n = try fx.transport.read();
    const elapsed = started.durationTo(std.Io.Clock.awake.now(std.testing.io));
    // The 20 cancelled receives and the one posted after the last.
    try std.testing.expectEqual(@as(usize, in_a_row + 1), SpuriouslyCancelledReceive.posts.load(.acquire));
    try expectUntimedStream(&fx.transport, .read, SpuriouslyCancelledReceive.first_bytes, n);
    try SpuriouslyCancelledReceive.expectInjected(in_a_row);
    // The read waits before each re-post after the first, 50 us and then
    // twice as long each time up to 5 ms (`SpuriousCancelRetry` in
    // stream_transport.zig): about 66 ms over these 20. Posted back to
    // back, 20 receives take a few ms. A sleep never ends early, so this
    // floor cannot fail a read that backs off.
    try std.testing.expect(elapsed.toMilliseconds() >= 20);
    try fx.expectStreamContinuesToEnd(.read);
}

test "an Io cancellation ends a pending read and leaves the stream intact" {
    var fx: WrappedTransport = undefined;
    try fx.init();
    defer fx.deinit();
    SpuriouslyCancelledReceive.arm(.{ .peer = fx.peer(), .mode = .posted_receive });

    var read = try std.Io.concurrent(std.testing.io, tcp.Transport.read, .{&fx.transport});
    defer _ = read.cancel(std.testing.io) catch 0;
    // Cancel while the receive is pending. The transport's own receive on
    // Windows reports it posted; elsewhere (std's read) give the read 50 ms
    // to block.
    if (comptime builtin.os.tag == .windows) {
        _ = waitUntil(&SpuriouslyCancelledReceive.posted, 1_000);
    } else {
        try std.Io.sleep(std.testing.io, .fromMilliseconds(50), .awake);
    }
    try std.testing.expectError(error.Canceled, read.cancel(std.testing.io));
    // A cancellation that was asked for is never posted again.
    if (comptime builtin.os.tag == .windows) try std.testing.expect(SpuriouslyCancelledReceive.posts.load(.acquire) <= 1);

    // Nothing is left in flight: the next read gets every byte.
    try fx.send("after-cancel");
    try expectUntimedStream(&fx.transport, .read, "after-cancel", try fx.transport.read());
}

/// A task function that waits (at most 5 s) for an Io cancellation of its
/// task, re-arms it (`recancel`), then reads. The read starts with the
/// cancellation pending, as after an earlier read of the task that returned
/// bytes and re-armed one.
const ReadAfterCancel = struct {
    var waiting: std.atomic.Value(bool) = .init(false);
    var reading: std.atomic.Value(bool) = .init(false);

    fn reset() void {
        waiting.store(false, .release);
        reading.store(false, .release);
    }

    fn run(transport: *tcp.Transport) tcp.Transport.ReadError!usize {
        waiting.store(true, .release);
        std.Io.sleep(std.testing.io, .fromSeconds(5), .awake) catch |err| switch (err) {
            error.Canceled => {
                std.testing.io.recancel();
                reading.store(true, .release);
                return transport.read();
            },
        };
        // No cancellation within 5 s: a result the test rejects.
        return error.Unexpected;
    }
};

test "a read that starts with an Io cancellation pending ends with error.Canceled and posts no receive" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    var fx: WrappedTransport = undefined;
    try fx.init();
    defer fx.deinit();
    // Count every receive posted; intercept none.
    SpuriouslyCancelledReceive.arm(.{ .peer = fx.peer(), .inject_count = 0 });
    ReadAfterCancel.reset();
    // The peer's bytes are queued first: a receive posted now would take
    // them at once, and the read would return them.
    const queued = "queued-before-the-read";
    try fx.send(queued);

    var read = try std.Io.concurrent(std.testing.io, ReadAfterCancel.run, .{&fx.transport});
    defer _ = read.cancel(std.testing.io) catch 0;
    try std.testing.expect(waitUntil(&ReadAfterCancel.waiting, ReadWatchdog.limit_ms));
    try std.testing.expectError(error.Canceled, read.cancel(std.testing.io));
    try std.testing.expect(ReadAfterCancel.reading.load(.acquire));
    // The read checked the cancellation before it posted anything.
    try std.testing.expectEqual(@as(usize, 0), SpuriouslyCancelledReceive.posts.load(.acquire));

    // The bytes are still in the socket: the next read gets all of them.
    try fx.guard();
    try expectUntimedStream(&fx.transport, .read, queued, try fx.transport.read());
    try std.testing.expect(SpuriouslyCancelledReceive.posts.load(.acquire) >= 1);
    try std.testing.expect(!fx.watchdog.fired.load(.acquire));
}

/// An Io that cannot run the transport's AFD receive batch concurrently:
/// `batchAwaitConcurrent` gives `error.ConcurrencyUnavailable` for it, with
/// nothing posted. It counts those refusals and std's `net_read`
/// operations.
const NoReceiveConcurrency = struct {
    var refused: std.atomic.Value(usize) = .init(0);
    var net_reads: std.atomic.Value(usize) = .init(0);

    fn reset() void {
        refused.store(0, .release);
        net_reads.store(0, .release);
    }

    fn awaitConcurrent(userdata: ?*anyopaque, batch: *std.Io.Batch, timeout: std.Io.Timeout) std.Io.Batch.AwaitConcurrentError!void {
        const head = batch.submitted.head;
        if (head != .none and batch.storage[head.toIndex()].submission.operation == .device_io_control) {
            _ = refused.fetchAdd(1, .acq_rel);
            return error.ConcurrencyUnavailable;
        }
        return std.testing.io.vtable.batchAwaitConcurrent(userdata, batch, timeout);
    }

    fn operate(userdata: ?*anyopaque, operation: std.Io.Operation) std.Io.Cancelable!std.Io.Operation.Result {
        if (operation == .net_read) _ = net_reads.fetchAdd(1, .acq_rel);
        return std.testing.io.vtable.operate(userdata, operation);
    }
};

test "read takes std's read when the Io cannot run the transport's receive batch concurrently" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    NoReceiveConcurrency.reset();
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = NoReceiveConcurrency.awaitConcurrent;
    vtable.operate = NoReceiveConcurrency.operate;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    var peer_closed = false;
    defer if (!peer_closed) tcp.closeFd(io, pair[1]);
    var transport = try tcp.Transport.init(std.testing.allocator, io, pair[0], 64);
    defer transport.deinit();

    // Each read: the transport's own receive is refused before anything is
    // posted, and std's read takes the bytes.
    const payload = "through-std-read";
    try io_write_compat.writeAll(io, pair[1].handle, payload);
    try expectUntimedStream(&transport, .read, payload, try transport.read());
    const reads = NoReceiveConcurrency.net_reads.load(.acquire);
    try std.testing.expect(reads >= 1);
    try std.testing.expectEqual(reads, NoReceiveConcurrency.refused.load(.acquire));

    // The end of the stream reads as 0 the same way.
    tcp.closeFd(io, pair[1]);
    peer_closed = true;
    try std.testing.expectEqual(@as(usize, 0), try transport.read());
    try std.testing.expectEqual(reads + 1, NoReceiveConcurrency.net_reads.load(.acquire));
    try std.testing.expectEqual(reads + 1, NoReceiveConcurrency.refused.load(.acquire));
}

test "a read cancelled as its receive completes keeps the bytes and re-arms the cancellation" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    CompletionAtCancel.reset();
    ReadThenCancelPoint.rearmed.store(false, .release);
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = CompletionAtCancel.awaitConcurrent;
    vtable.operate = CompletionAtCancel.operate;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, pair[1]);
    var transport = try tcp.Transport.init(std.testing.allocator, io, pair[0], 64);
    defer transport.deinit();
    const payload = "consumed-at-cancel";
    try io_write_compat.writeAll(io, pair[1].handle, payload);

    var read = try std.Io.concurrent(std.testing.io, ReadThenCancelPoint.untimed, .{&transport});
    defer _ = read.cancel(std.testing.io) catch 0;
    try CompletionAtCancel.awaitConsumed();
    const result = read.cancel(std.testing.io);
    try std.testing.expect(CompletionAtCancel.finished.load(.acquire));
    // The bytes the receive took reach the caller (std's read drops them),
    // and the cancellation is still pending at the task's next
    // cancellation point.
    try std.testing.expectEqual(payload.len, try result);
    try std.testing.expectEqualStrings(payload, transport.read_buf[0..payload.len]);
    try std.testing.expect(ReadThenCancelPoint.rearmed.load(.acquire));
}

test "read returns 0 when another thread closes the transport under its pending receive" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    var fx: WrappedTransport = undefined;
    try fx.init();
    defer fx.deinit();
    SpuriouslyCancelledReceive.arm(.{ .peer = fx.peer(), .mode = .posted_receive });

    const Closer = struct {
        closed_under_read: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This(), wrapped: *WrappedTransport) void {
            if (!waitUntil(&SpuriouslyCancelledReceive.posted, ReadWatchdog.limit_ms)) {
                // The read never posted the transport's own receive: end it
                // through the peer instead.
                tcp.runtime.shutdownFd(std.testing.io, wrapped.peer());
                return;
            }
            // What `deinit` from another thread does to the socket, without
            // freeing the read buffer under the read: close the transport,
            // then its handle. This test assumes Windows then ends the
            // pending receive STATUS_CANCELLED, which std's read treats as
            // unreachable; no Windows run has shown that status yet. If
            // Windows reports another one (LOCAL_DISCONNECT, say), the read
            // fails with that status's error and this test fails on it.
            wrapped.transport.close();
            wrapped.transport.fd_closed.store(true, .release);
            tcp.closeFd(wrapped.io(), .{ .handle = wrapped.transport.fd });
            self.closed_under_read.store(true, .release);
        }
    };
    var closer: Closer = .{};
    const thread = try std.Thread.spawn(.{}, Closer.run, .{ &closer, &fx });
    const n = fx.transport.read();
    thread.join();

    try std.testing.expect(closer.closed_under_read.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), try n);
    try std.testing.expectEqual(@as(usize, 1), SpuriouslyCancelledReceive.posts.load(.acquire));
}

test "requestClose ends a Connection whose read is pending, without posting the receive again" {
    var vtable = std.testing.io.vtable.*;
    vtable.batchAwaitConcurrent = SpuriouslyCancelledReceive.awaitConcurrent;
    const io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    const pair = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, pair[1]);
    var counter = FrameCounter{};
    // No tick: the loop waits on the read alone.
    var conn = try Connection.init(std.testing.allocator, io, pair[0], .{});
    defer conn.deinit();
    conn.start(&counter, FrameCounter.onMessage, FrameCounter.onError, FrameCounter.onClose);
    SpuriouslyCancelledReceive.arm(.{ .peer = pair[1], .mode = .posted_receive });

    const Closer = struct {
        // A plain field: it is read only after thread.join(), and
        // x86-linux-gnu has no 64-bit atomics.
        requested_ns: i64 = 0,

        fn run(self: *@This(), closing: *Connection) void {
            // Close while the read is pending. On Windows the wrapper reports
            // the transport's own receive posted; elsewhere (std's read) give
            // the loop 50 ms to block.
            if (comptime builtin.os.tag == .windows) {
                _ = waitUntil(&SpuriouslyCancelledReceive.posted, 1_000);
            } else {
                std.Io.sleep(std.testing.io, .fromMilliseconds(50), .awake) catch {};
            }
            self.requested_ns = awakeNowNs(std.testing.io);
            closing.requestClose();
        }
    };
    var closer: Closer = .{};
    const thread = try std.Thread.spawn(.{}, Closer.run, .{ &closer, &conn });
    conn.run();
    const returned_ns = awakeNowNs(io);
    thread.join();

    try std.testing.expect(returned_ns - closer.requested_ns < 2 * std.time.ns_per_s);
    try std.testing.expect(conn.last_error == null);
    try std.testing.expectEqual(@as(usize, 0), counter.frames);
    // A cancellation that was asked for is never posted again.
    if (comptime builtin.os.tag == .windows) try std.testing.expect(SpuriouslyCancelledReceive.posts.load(.acquire) <= 1);
}
