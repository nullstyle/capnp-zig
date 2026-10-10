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
/// For the first AFD receive (`device_io_control`) it sees, this await
/// builds that state from the real backend's own steps:
/// 1. It posts the receive with an await whose deadline has already passed.
///    The peer has sent nothing yet, so the receive stays pending.
/// 2. It cancels the receive with the backend's `batchCancel` (after the
///    same thread alert that the transport's `cancelWindowsReceive` sends).
///    The IOSB ends STATUS_CANCELLED with no bytes, and `batchApc` files
///    the slot under `unused`.
/// 3. Only then do the peer's bytes arrive.
/// 4. It returns success with nothing completed.
/// After step 2 the batch is exactly what the CI probe printed: nothing
/// pending, nothing completed, the slot unused, no receive in flight, and
/// no bytes taken. `state_matched` records that, so the test cannot pass
/// against a different state. Later awaits pass through.
const SpuriouslyCancelledReceive = struct {
    /// The connected peer, which sends `first_bytes` after step 2.
    var peer: tcp.SocketFd = undefined;
    /// Sleep past the caller's deadline before step 3.
    var outlive_deadline = false;
    var injected: usize = 0;
    var state_matched = false;
    var write_failed = false;

    const first_bytes = "after-spurious-cancel";

    fn arm(peer_fd: tcp.SocketFd, past_deadline: bool) void {
        peer = peer_fd;
        outlive_deadline = past_deadline;
        injected = 0;
        state_matched = false;
        write_failed = false;
    }

    fn awaitConcurrent(userdata: ?*anyopaque, batch: *std.Io.Batch, timeout: std.Io.Timeout) std.Io.Batch.AwaitConcurrentError!void {
        const inner = std.testing.io.vtable;
        if (comptime builtin.os.tag == .windows) {
            const head = batch.submitted.head;
            if (injected == 0 and head != .none and batch.storage[head.toIndex()].submission.operation == .device_io_control) {
                injected += 1;
                // 1. Post the receive. The deadline has passed, so the await
                // reports Timeout and leaves the receive pending.
                const passed: std.Io.Timeout = .{ .duration = .{ .raw = .zero, .clock = .awake } };
                inner.batchAwaitConcurrent(userdata, batch, passed) catch |err| switch (err) {
                    error.Timeout => {},
                    else => return err,
                };
                const posted = batch.pending.head != .none and batch.completed.head == .none;
                // 2. Cancel it. The backend's batchCancel first waits for an
                // APC or an alert, so alert this thread first.
                if (batch.pending.head != .none) {
                    const windows = std.os.windows;
                    const status = windows.ntdll.NtAlertThread(windows.GetCurrentThread());
                    if (status != .SUCCESS) std.debug.panic("cannot alert the test thread: NTSTATUS=0x{x}", .{@backingInt(status)});
                }
                inner.batchCancel(userdata, batch);
                state_matched = posted and batch.pending.head == .none and
                    batch.completed.head == .none and batch.unused.head != .none;
                if (outlive_deadline) {
                    const deadline = timeout.toDeadline(std.testing.io);
                    while (deadline.toDurationFromNow(std.testing.io)) |remaining| {
                        if (remaining.raw.nanoseconds <= 0) break;
                        try std.Io.sleep(std.testing.io, .fromMilliseconds(5), .awake);
                    }
                }
                // 3. The peer's bytes arrive now.
                io_write_compat.writeAll(std.testing.io, peer.handle, first_bytes) catch {
                    write_failed = true;
                };
                // 4. Success, with nothing completed.
                return;
            }
        }
        return inner.batchAwaitConcurrent(userdata, batch, timeout);
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
    SpuriouslyCancelledReceive.arm(pair[1], false);

    // The receive ended cancelled and took nothing, so the read must post
    // it again and return the peer's bytes before its deadline, not fail
    // with error.Unexpected.
    const generous: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    try expectTimedStream(&transport, SpuriouslyCancelledReceive.first_bytes, generous);
    try std.testing.expectEqual(@as(usize, 1), SpuriouslyCancelledReceive.injected);
    try std.testing.expect(SpuriouslyCancelledReceive.state_matched);
    try std.testing.expect(!SpuriouslyCancelledReceive.write_failed);

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
    SpuriouslyCancelledReceive.arm(pair[1], true);

    // The deadline passed while the receive was cancelled: the read times
    // out, and the bytes stay for the next read.
    const short: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(50), .clock = .awake } };
    try std.testing.expectError(error.Timeout, transport.readTimeout(short));
    try std.testing.expectEqual(@as(usize, 1), SpuriouslyCancelledReceive.injected);
    try std.testing.expect(SpuriouslyCancelledReceive.state_matched);
    try std.testing.expect(!SpuriouslyCancelledReceive.write_failed);
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

test "readTimeout preserves a successful read while joining caller cancellation" {
    const CompletionAtCancel = struct {
        var consumed: std.atomic.Value(bool) = .init(false);
        var finished: std.atomic.Value(bool) = .init(false);

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
    };
    CompletionAtCancel.consumed.store(false, .release);
    CompletionAtCancel.finished.store(false, .release);
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

    var read = try std.Io.concurrent(std.testing.io, tcp.Transport.readTimeout, .{
        &transport,
        std.Io.Timeout{ .duration = .{ .raw = .fromSeconds(60), .clock = .awake } },
    });
    defer _ = read.cancel(std.testing.io) catch 0;
    const wait_until = std.Io.Clock.awake.now(std.testing.io).nanoseconds + std.time.ns_per_s * 2;
    while (!CompletionAtCancel.consumed.load(.acquire)) {
        try std.testing.expect(std.Io.Clock.awake.now(std.testing.io).nanoseconds < wait_until);
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    const result = read.cancel(std.testing.io);
    try std.testing.expect(CompletionAtCancel.finished.load(.acquire));
    try std.testing.expectEqualStrings(payload, transport.read_buf[0..payload.len]);
    try std.testing.expectEqual(payload.len, try result);
}
