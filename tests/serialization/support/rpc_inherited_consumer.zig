const std = @import("std");
const capnpc = @import("capnpc-zig");
const g = @import("generated.zig");
const external = @import("rpc_inherited_external.zig");
const rpc = capnpc.rpc;
const protocol = rpc.wire.protocol;
const Peer = rpc.peer.Peer;
const Capture = struct {
    expected_id: u64,
    calls: usize = 0,
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        var decoded = try protocol.DecodedMessage.init(std.testing.allocator, bytes);
        defer decoded.deinit();
        if (decoded.tag != .call) return;
        const call = try decoded.asCall();
        try std.testing.expectEqual(self.expected_id, call.interface_id);
        try std.testing.expectEqual(@as(u16, 0), call.method_id);
        self.calls += 1;
    }
    fn first(_: *anyopaque, _: *Peer, _: g.First.Ping.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
    fn second(_: *anyopaque, _: *Peer, _: g.Second.Ping.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
    fn leaf(_: *anyopaque, _: *Peer, _: g.Leaf.Ping.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
    fn companion(_: *anyopaque, _: *Peer, _: g.CompanionBase.Ping.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
    fn own(_: *anyopaque, _: *Peer, _: g.Own.Ping.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
};
test "colliding inherited calls keep declaring interface identity" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var capture = Capture{ .expected_id = g.First.interface_id };
    peer.setSendFrameOverride(&capture, Capture.send);
    const client = g.Both.Client.init(&peer, 7);
    _ = try client.callPingFromFirst(&capture, null, Capture.first);
    capture.expected_id = g.Second.interface_id;
    _ = try client.callPingFromSecond(&capture, null, Capture.second);
    const own = g.Own.Client.init(&peer, 7);
    capture.expected_id = g.Own.interface_id;
    _ = try own.callPing(&capture, null, Capture.own);
    capture.expected_id = g.First.interface_id;
    _ = try own.callPingFromFirst(&capture, null, Capture.first);
    // Diamond de-duplication keeps the original noncolliding public name.
    _ = try g.Diamond.Client.init(&peer, 7).callPing(&capture, null, Capture.first);
    const pending = try client.callPingFromFirstPipelined(&capture, null, Capture.first);
    capture.expected_id = g.Leaf.interface_id;
    _ = try pending.getService().callPing(&capture, null, Capture.leaf);
    capture.expected_id = g.CompanionBase.interface_id;
    _ = try g.Companions.Client.init(&peer, 7).callPingFromCompanionBase_cdcde10962f5eb1f(&capture, null, Capture.companion);
    _ = &g.FamilyOnly.Client.callPingFromCompanionBase;
    _ = &g.FamilyOnly.Client.callPingWithOptions;
    _ = &g.FamilyNames.Client.callPingFromCompanionBase_cdcde10962f5eb1f;
    _ = &g.FamilyNames.Client.callPingFromCompanionBaseWithOptions_beb4094e5bcb2ad7;
    _ = &g.Companions.Client.callPingFromCompanionBaseWithOptions;
    _ = &g.Companions.Client.callPingFromCompanionBase_cdcde10962f5eb1fWithOptions;
    try std.testing.expectEqual(@as(usize, 8), capture.calls);
    // Flattened qualified names A.B and AB collide, so suffixes include IDs.
    _ = &g.Fallback.Client.callPingFromAB_ca1fd246754ef5bd;
    _ = &g.Fallback.Client.callPingFromAB_d6c9ac3ac16b809b;
    _ = &g.Both.PipelinedClient.callPingFromFirst;
    _ = &g.Imported.Client.callPingFromFirst;
    _ = &g.Imported.Client.callPingFromRpcInheritedExternalFirst;
    try std.testing.expect(g.First.interface_id != external.First.interface_id);
    _ = g.Both.VTable{ .pingFromFirst = Handler.first, .pingFromSecond = Handler.second };
}
const Handler = struct {
    first_calls: usize = 0,
    second_calls: usize = 0,
    fn first(ctx: *anyopaque, _: *Peer, _: g.First.Ping.Params.Reader, result: *g.First.Ping.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.first_calls += 1;
        try result.setValue(11);
    }
    fn second(ctx: *anyopaque, _: *Peer, _: g.Second.Ping.Params.Reader, result: *g.Second.Ping.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.second_calls += 1;
        try result.setValue(22);
    }
};
const Link = struct {
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const peer: *Peer = @ptrCast(@alignCast(ctx));
        try peer.handleFrame(bytes);
    }
};
const Results = struct {
    first: ?u32 = null,
    second: ?u32 = null,
    fn onFirst(ctx: *anyopaque, _: *Peer, response: g.First.Ping.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.first = try (try response.unwrap()).getValue();
    }
    fn onSecond(ctx: *anyopaque, _: *Peer, response: g.Second.Ping.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.second = try (try response.unwrap()).getValue();
    }
};
test "qualified VTable handlers dispatch by declaring interface and return separately" {
    var client_peer = Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, Link.send);
    server_peer.setSendFrameOverride(&client_peer, Link.send);
    var handlers = Handler{};
    var server = g.Both.Server{ .ctx = &handlers, .vtable = .{ .pingFromFirst = Handler.first, .pingFromSecond = Handler.second } };
    const id = try g.Both.exportServer(&server_peer, &server);
    const client = g.Both.Client.init(&client_peer, id);
    var results = Results{};
    _ = try client.callPingFromFirst(&results, null, Results.onFirst);
    _ = try client.callPingFromSecond(&results, null, Results.onSecond);
    try std.testing.expectEqual(@as(?u32, 11), results.first);
    try std.testing.expectEqual(@as(?u32, 22), results.second);
    try std.testing.expectEqual(@as(usize, 1), handlers.first_calls);
    try std.testing.expectEqual(@as(usize, 1), handlers.second_calls);
}
