const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const events = capnpc.rpc.events;
const Connection = capnpc.rpc.transport.tcp.Connection;
const Peer = peer_impl.Peer;

fn createSocketPair() ![2]std.posix.fd_t {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    var fds: [2]std.posix.fd_t = undefined;
    if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
        return error.SocketPairFailed;
    }
    return fds;
}

fn closeFd(io: std.Io, fd: std.posix.fd_t) void {
    io.vtable.netClose(io.userdata, (&fd)[0..1]);
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
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair();
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
    peer.start(null, null);

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
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair();
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
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair();

    const Feeder = struct {
        fn run(fd: std.posix.fd_t, write_io: std.Io) void {
            // Feed partial frame bytes every 20ms for ~100ms, keeping the
            // connection alive past several idle windows. Write before each
            // sleep: on a loaded machine a delayed thread spawn must not
            // leave the connection idle long enough to be reaped before the
            // first byte arrives.
            var i: usize = 0;
            while (i < 5) : (i += 1) {
                const byte = [_]u8{0};
                _ = std.posix.system.write(fd, &byte, 1);
                sleepMs(write_io, 20);
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
        .idle_timeout_ms = 60,
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

    // The feeder kept the connection alive for ~100ms, which exceeds the
    // 60ms idle bound: traffic must have reset the idle clock at least once.
    try std.testing.expect(elapsed_ns >= 90 * std.time.ns_per_ms);
}
