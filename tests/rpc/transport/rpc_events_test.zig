const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const rpc_events = capnpc.rpc.events;
const HostPeer = capnpc.rpc.integration.host_peer.HostPeer;
const Peer = capnpc.rpc.peer.Peer;
const Transport = capnpc.rpc.transport.tcp.Transport;

/// Dummy socket handle for tests that drive a fake Io vtable — never used
/// for real I/O. `fd_t` is an integer on POSIX and a pointer (HANDLE) on
/// Windows, so a literal 0 does not compile there.
const fake_socket_handle: std.Io.net.Socket.Handle =
    if (builtin.os.tag == .windows) std.os.windows.INVALID_HANDLE_VALUE else 0;

const Recorder = struct {
    events: [32]rpc_events.Event = undefined,
    count: usize = 0,

    fn observer(self: *Recorder) rpc_events.Observer {
        return rpc_events.Observer.init(self, onEvent);
    }

    fn onEvent(ctx: *anyopaque, event: rpc_events.Event) void {
        const self: *Recorder = @ptrCast(@alignCast(ctx));
        if (self.count >= self.events.len) @panic("too many RPC test events");
        self.events[self.count] = event;
        self.count += 1;
    }

    fn last(self: *const Recorder) rpc_events.Event {
        std.debug.assert(self.count != 0);
        return self.events[self.count - 1];
    }
};

/// Records events together with the thread that delivered them, to pin the
/// observer threading contract (`events.Observer`): callbacks fire on the
/// owner/loop thread only. Safe without a mutex precisely because of that
/// contract — the asserts below would fail before any data race mattered.
const ThreadRecorder = struct {
    events: [32]rpc_events.Event = undefined,
    thread_ids: [32]std.Thread.Id = undefined,
    count: usize = 0,

    fn observer(self: *ThreadRecorder) rpc_events.Observer {
        return rpc_events.Observer.init(self, onEvent);
    }

    fn onEvent(ctx: *anyopaque, event: rpc_events.Event) void {
        const self: *ThreadRecorder = @ptrCast(@alignCast(ctx));
        if (self.count >= self.events.len) @panic("too many RPC test events");
        self.events[self.count] = event;
        self.thread_ids[self.count] = std.Thread.getCurrentId();
        self.count += 1;
    }

    fn indexOfPhase(self: *const ThreadRecorder, phase: rpc_events.ConnectionPhase) ?usize {
        for (self.events[0..self.count], 0..) |event, i| {
            if (event == .connection and event.connection.phase == phase) return i;
        }
        return null;
    }

    fn phaseCount(self: *const ThreadRecorder, phase: rpc_events.ConnectionPhase) usize {
        var total: usize = 0;
        for (self.events[0..self.count]) |event| {
            if (event == .connection and event.connection.phase == phase) total += 1;
        }
        return total;
    }
};

const TcpConnection = capnpc.rpc.transport.tcp.Connection;

fn tcpNoopMessage(_: *TcpConnection, _: []const u8) anyerror!void {}
fn tcpNoopError(_: *TcpConnection, _: anyerror) void {}
fn tcpNoopClose(_: *TcpConnection) void {}

fn tcpAdoptAndRun(conn: *TcpConnection) void {
    // Adopt before entering the loop: the sanctioned handoff for running a
    // connection on a thread other than the one that constructed it.
    conn.adoptOwnerThread();
    conn.run();
}

test "tcp cross-thread requestClose defers .closing to the run-loop thread, once" {
    const tcp = capnpc.rpc.transport.tcp;
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, fds[1]);

    var recorder = ThreadRecorder{};
    var conn = try TcpConnection.init(allocator, io, fds[0], .{ .observer = recorder.observer() });
    var dummy: u8 = 0;
    conn.start(&dummy, tcpNoopMessage, tcpNoopError, tcpNoopClose);

    const runner = try std.Thread.spawn(.{}, tcpAdoptAndRun, .{&conn});
    // Cross-thread close request: must not invoke the observer on THIS
    // thread — the run loop emits the deferred `.closing` on its own thread
    // when it observes the shutdown.
    conn.requestClose();
    runner.join();
    // The run thread owned the connection; re-adopt (quiescent after join)
    // for the owner-thread-only deinit.
    conn.adoptOwnerThread();
    conn.deinit();

    const requester_tid = std.Thread.getCurrentId();
    try std.testing.expectEqual(@as(usize, 1), recorder.phaseCount(.closing));
    const closing_index = recorder.indexOfPhase(.closing).?;
    const closed_index = recorder.indexOfPhase(.closed).?;
    try std.testing.expect(closing_index < closed_index);
    // `.closing` and `.closed` fired on the loop thread, not the requester.
    try std.testing.expect(recorder.thread_ids[closing_index] != requester_tid);
    try std.testing.expectEqual(recorder.thread_ids[closed_index], recorder.thread_ids[closing_index]);
}

test "tcp owner-thread close() emits .closing synchronously and the loop does not repeat it" {
    const tcp = capnpc.rpc.transport.tcp;
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, fds[1]);

    var recorder = ThreadRecorder{};
    var conn = try TcpConnection.init(allocator, io, fds[0], .{ .observer = recorder.observer() });
    defer conn.deinit();
    var dummy: u8 = 0;
    conn.start(&dummy, tcpNoopMessage, tcpNoopError, tcpNoopClose);

    // Raise the deferred-close flag too: the loop-exit path must yield to the
    // synchronous owner-thread emit rather than double-firing.
    conn.requestClose();
    conn.close();
    try std.testing.expectEqual(@as(usize, 1), recorder.phaseCount(.closing));

    // Loop exits immediately (already closing) and must not emit a second
    // `.closing` even though the cross-thread request flag is still raised.
    conn.run();
    try std.testing.expectEqual(@as(usize, 1), recorder.phaseCount(.closing));
    try std.testing.expectEqual(@as(usize, 1), recorder.phaseCount(.closed));
}

test "tcp observer reports frame send metadata without frame bytes" {
    const net = std.Io.net;

    const FakeIo = struct {
        const State = struct {
            bytes_written: usize = 0,
        };

        fn netWrite(
            userdata: ?*anyopaque,
            _: net.Socket.Handle,
            header: []const u8,
            data: []const []const u8,
            _: usize,
        ) net.Stream.Writer.Error!usize {
            const state: *State = @ptrCast(@alignCast(userdata.?));
            var len: usize = header.len;
            for (data) |chunk| len += chunk.len;
            state.bytes_written += len;
            return len;
        }

        fn netShutdown(_: ?*anyopaque, _: net.Socket.Handle, _: net.ShutdownHow) net.ShutdownError!void {}

        fn netClose(_: ?*anyopaque, _: []const net.Socket.Handle) void {}
    };

    var recorder = Recorder{};
    var io_state = FakeIo.State{};
    var vtable = std.testing.io.vtable.*;
    vtable.netWrite = FakeIo.netWrite;
    vtable.netShutdown = FakeIo.netShutdown;
    vtable.netClose = FakeIo.netClose;
    const io = std.Io{
        .userdata = &io_state,
        .vtable = &vtable,
    };

    var transport = try Transport.initWithOptions(std.testing.allocator, io, .{ .handle = fake_socket_handle }, .{
        .read_buffer_size = 64,
        .observer = recorder.observer(),
    });
    defer transport.deinit();

    const secret_frame = "secret frame bytes stay off the event API";
    try transport.enqueueWrite(secret_frame);

    try std.testing.expectEqual(@as(usize, 1), recorder.count);
    const event = recorder.last();
    try std.testing.expect(event == .frame);
    try std.testing.expectEqual(rpc_events.Source.tcp, event.frame.source);
    try std.testing.expectEqual(rpc_events.FrameStage.sent, event.frame.stage);
    try std.testing.expectEqual(secret_frame.len, event.frame.bytes);
    try std.testing.expectEqual(secret_frame.len, io_state.bytes_written);
}

test "host peer observer reports queue rejection without payload material" {
    const allocator = std.testing.allocator;

    var recorder = Recorder{};
    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.setObserver(recorder.observer());
    host.setLimits(.{ .outbound_count_limit = 1 });
    host.start(null, null, null);

    try host.peer.sendReturnException(1, "first internal reason");
    try std.testing.expectEqual(@as(usize, 1), host.pendingOutgoingCount());

    try std.testing.expectError(
        error.OutgoingQueueLimitExceeded,
        host.peer.sendReturnException(2, "second internal reason"),
    );

    const event = recorder.last();
    try std.testing.expect(event == .resource_rejection);
    try std.testing.expectEqual(rpc_events.Source.host_peer, event.resource_rejection.source);
    try std.testing.expectEqual(rpc_events.Resource.host_outbound_frames, event.resource_rejection.resource);
    try std.testing.expectEqual(@as(?usize, 1), event.resource_rejection.limit);
    try std.testing.expectEqual(error.OutgoingQueueLimitExceeded, event.resource_rejection.err);
}

test "peer observer reports decode failure without raw frame data" {
    const allocator = std.testing.allocator;

    var recorder = Recorder{};
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setObserver(recorder.observer());

    try std.testing.expectError(error.TruncatedMessage, peer.handleFrame(&[_]u8{}));

    const event = recorder.last();
    try std.testing.expect(event == .protocol_error);
    try std.testing.expectEqual(rpc_events.Source.peer, event.protocol_error.source);
    try std.testing.expectEqual(error.TruncatedMessage, event.protocol_error.err);
    try std.testing.expect(event.protocol_error.message_tag == null);
}

test "parked Accept timeout event carries only its answer id" {
    var recorder = Recorder{};

    rpc_events.emitParkedAcceptTimeout(recorder.observer(), 73);

    try std.testing.expectEqual(@as(usize, 1), recorder.count);
    const event = recorder.last();
    try std.testing.expect(event == .timeout);
    try std.testing.expectEqual(rpc_events.Source.peer, event.timeout.source);
    try std.testing.expectEqual(rpc_events.TimeoutKind.parked_accept, event.timeout.kind);
    try std.testing.expectEqual(@as(?u32, 73), event.timeout.answer_id);
    try std.testing.expectEqual(@as(?u32, null), event.timeout.question_id);

    // These compile-time names are the complete resource metadata exposed for
    // parked-Accept admission; tokens, embargoes, and frame contents have no
    // place in either event payload.
    try std.testing.expectEqualStrings("parked_accepts", @tagName(rpc_events.Resource.parked_accepts));
    try std.testing.expectEqualStrings("parked_accept_bytes", @tagName(rpc_events.Resource.parked_accept_bytes));
}
