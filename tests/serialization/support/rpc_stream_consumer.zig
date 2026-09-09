const std = @import("std");
const capnpc = @import("capnpc-zig");
const g = @import("generated.zig");
const rpc = capnpc.rpc;
const Peer = rpc.peer.Peer;
const State = struct {
    total: u32 = 0,
    drained: bool = false,
    fn i(ctx: *anyopaque, _: *Peer, params: g.TestStreaming.DoStreamI.Params.Reader, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.total += try params.getI();
    }
    fn j(_: *anyopaque, _: *Peer, _: g.TestStreaming.DoStreamJ.Params.Reader, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
    fn finish(_: *anyopaque, _: *Peer, _: g.TestStreaming.FinishStream.Params.Reader, _: *g.TestStreaming.FinishStream.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
    fn build(_: *anyopaque, params: *g.TestStreaming.DoStreamI.Params.Builder) anyerror!void {
        try params.setI(17);
    }
    fn drain(ctx: *anyopaque, err: ?anyerror) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.drained = err == null;
    }
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const peer: *Peer = @ptrCast(@alignCast(ctx));
        try peer.handleFrame(bytes);
    }
};
test "stream result is bundled and generated stream calls acknowledge" {
    try std.testing.expect(g.TestStreaming.DoStreamI.Results == rpc.generated.stream.StreamResult);
    var client_peer = Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, State.send);
    server_peer.setSendFrameOverride(&client_peer, State.send);
    var state = State{};
    var server = g.TestStreaming.Server{ .ctx = &state, .vtable = .{ .doStreamI = State.i, .doStreamJ = State.j, .finishStream = State.finish } };
    const id = try g.TestStreaming.exportServer(&server_peer, &server);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&client_peer, id));
    try client.callDoStreamI(&state, State.build);
    client.waitStreaming(&state, State.drain);
    try std.testing.expectEqual(@as(u32, 17), state.total);
    try std.testing.expect(state.drained);
}
