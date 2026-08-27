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
