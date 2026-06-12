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
    host.start(null, null);

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
