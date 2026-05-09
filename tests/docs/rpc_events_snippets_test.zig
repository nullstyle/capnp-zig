const std = @import("std");
const capnpc = @import("capnpc-zig");

const rpc = capnpc.rpc;
const rpc_events = rpc.events;

const Recorder = struct {
    events: [8]rpc_events.Event = undefined,
    count: usize = 0,

    fn observer(self: *Recorder) rpc_events.Observer {
        return rpc_events.Observer.init(self, onEvent);
    }

    fn onEvent(ctx: *anyopaque, event: rpc_events.Event) void {
        const self: *Recorder = @ptrCast(@alignCast(ctx));
        if (self.count >= self.events.len) @panic("too many RPC snippet events");
        self.events[self.count] = event;
        self.count += 1;
    }

    fn at(self: *const Recorder, index: usize) rpc_events.Event {
        std.debug.assert(index < self.count);
        return self.events[index];
    }

    fn last(self: *const Recorder) rpc_events.Event {
        std.debug.assert(self.count != 0);
        return self.events[self.count - 1];
    }
};

test "rpc.events observer snippet records redacted connection and frame metadata" {
    var recorder = Recorder{};
    const observer = recorder.observer();

    rpc_events.emitConnection(observer, .tcp, .client, .initialized);
    rpc_events.emitFrame(observer, .tcp, .client, .received, 128);
    rpc_events.emitClose(observer, .tcp, .client, null);

    try std.testing.expectEqual(@as(usize, 3), recorder.count);

    const connection = recorder.at(0);
    try std.testing.expect(connection == .connection);
    try std.testing.expectEqual(rpc_events.Source.tcp, connection.connection.source);
    try std.testing.expectEqual(rpc_events.Role.client, connection.connection.role);
    try std.testing.expectEqual(rpc_events.ConnectionPhase.initialized, connection.connection.phase);

    const frame = recorder.at(1);
    try std.testing.expect(frame == .frame);
    try std.testing.expectEqual(rpc_events.Source.tcp, frame.frame.source);
    try std.testing.expectEqual(rpc_events.FrameStage.received, frame.frame.stage);
    try std.testing.expectEqual(@as(usize, 128), frame.frame.bytes);

    const close = recorder.at(2);
    try std.testing.expect(close == .close);
    try std.testing.expect(close.close.err == null);
}

test "rpc.events protocol error snippet carries metadata without payload bytes" {
    var recorder = Recorder{};
    const observer = recorder.observer();

    var peer = rpc.peer.Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    peer.setObserver(observer);

    rpc_events.emitProtocolError(observer, .peer, .unknown, error.TruncatedMessage, null);

    const event = recorder.last();
    try std.testing.expect(event == .protocol_error);
    try std.testing.expectEqual(rpc_events.Source.peer, event.protocol_error.source);
    try std.testing.expectEqual(error.TruncatedMessage, event.protocol_error.err);
    try std.testing.expect(event.protocol_error.message_tag == null);
}
