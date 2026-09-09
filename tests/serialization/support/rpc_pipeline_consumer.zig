const std = @import("std");
const capnpc = @import("capnpc-zig");
const generated = @import("generated.zig");
const Peer = capnpc.rpc.peer.Peer;
const protocol = capnpc.rpc.wire.protocol;
const Capture = struct {
    expected: []const u16,
    calls: usize = 0,
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        var decoded = try protocol.DecodedMessage.init(std.testing.allocator, bytes);
        defer decoded.deinit();
        if (decoded.tag != .call) return;
        const call = try decoded.asCall();
        try std.testing.expectEqual(generated.Service.interface_id, call.interface_id);
        try std.testing.expectEqual(@as(u16, 0), call.method_id);
        const answer = call.target.promised_answer.?;
        try std.testing.expectEqual(@as(u32, 91), answer.question_id);
        try std.testing.expectEqual(self.expected.len, answer.transform.len());
        for (self.expected, 0..) |expected, index| {
            const op = try answer.transform.get(@intCast(index));
            try std.testing.expectEqual(protocol.PromisedAnswerOpTag.getPointerField, op.tag);
            try std.testing.expectEqual(expected, op.pointer_index);
        }
        self.calls += 1;
    }
    fn result(_: *anyopaque, _: *Peer, _: generated.Service.Ping.Response, _: *const capnpc.rpc.caps.table.InboundCapTable) anyerror!void {}
};
test "nested and group pipeline getters send exact pointer paths" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var capture = Capture{ .expected = &.{ 1, 1 } };
    peer.setSendFrameOverride(&capture, Capture.send);
    const pipeline = generated.Factory.NestedPipeline{ .peer = &peer, .question_id = 91 };
    const holder = try pipeline.getHolder();
    const service = try holder.getService();
    _ = try service.callPing(&capture, null, Capture.result);
    capture.expected = &.{ 1, 3 };
    const group = holder.getDetails();
    _ = try (try group.getNested()).callPing(&capture, null, Capture.result);
    capture.expected = &.{ 1, 2, 2, 1 };
    _ = try (try (try (try holder.getNext()).getNext()).getService()).callPing(&capture, null, Capture.result);
    try std.testing.expectEqual(@as(usize, 3), capture.calls);
    try std.testing.expect(!@hasDecl(@TypeOf(holder), "getUnsafe"));
    try std.testing.expect(!@hasDecl(@TypeOf(holder), "getSelected"));
    var deep = holder;
    for (0..62) |_| deep = try deep.getNext();
    _ = try deep.getService();
    deep = try deep.getNext();
    try std.testing.expectError(error.PipelineDepthLimit, deep.getService());
    try std.testing.expectError(error.PipelineDepthLimit, deep.getNext());
    // Existing direct getters keep their infallible signature.
    const direct = generated.Factory.DirectPipeline{ .peer = &peer, .question_id = 91 };
    capture.expected = &.{0};
    _ = try direct.getService().callPing(&capture, null, Capture.result);
}
