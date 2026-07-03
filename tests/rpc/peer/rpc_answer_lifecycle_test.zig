const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const Peer = peer_impl.Peer;
const peer_test_hooks = Peer.test_hooks;

// Regression coverage for the RPC answer-lifecycle fixes: every inbound Call
// must receive exactly one Return, including pipelined calls queued against
// an answer that fails or is cancelled by a Finish. Before these fixes a
// queued pipelined call whose target answer returned an exception (or was
// cancelled) never received any Return, hanging a compliant caller forever.

/// Captures outbound frames and decodes Returns for assertions.
const ReturnCapture = struct {
    allocator: std.mem.Allocator,
    frames: std.ArrayList([]u8),

    fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        const copy = try self.allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, copy, frame);
        try self.frames.append(self.allocator, copy);
    }

    fn deinit(self: *@This()) void {
        for (self.frames.items) |frame| self.allocator.free(frame);
        self.frames.deinit(self.allocator);
    }

    /// Count Return frames for `answer_id` carrying `tag`.
    fn countReturns(self: *@This(), answer_id: u32, tag: protocol.ReturnTag) usize {
        var n: usize = 0;
        for (self.frames.items) |frame| {
            var decoded = protocol.DecodedMessage.init(self.allocator, frame) catch continue;
            defer decoded.deinit();
            if (decoded.tag != .@"return") continue;
            const ret = decoded.asReturn() catch continue;
            if (ret.answer_id == answer_id and ret.tag == tag) n += 1;
        }
        return n;
    }
};

fn buildCallFrame(allocator: std.mem.Allocator, question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    // A minimal, decodable Call. The target is irrelevant to the terminal
    // drain, which only reads the question id to address the child Return.
    var call = try builder.beginCall(question_id, 0xABCD, 0);
    try call.setTargetImportedCap(7);
    _ = try call.initCapTableTyped(0);
    return builder.finish();
}

fn newCapture(allocator: std.mem.Allocator) ReturnCapture {
    return .{ .allocator = allocator, .frames = std.ArrayList([]u8).empty };
}

test "exception Return drains queued pipelined calls with their own Return" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    // A remote pipelines a call (question 100) on promised answer 5 before
    // answer 5's Return has arrived.
    const child_frame = try buildCallFrame(allocator, 100);
    defer allocator.free(child_frame);
    const inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    try peer_test_hooks.queuePromisedCall(&peer, 5, child_frame, inbound);
    try std.testing.expectEqual(@as(usize, 1), peer.pending_promises.count());

    // Answer 5's handler fails.
    try peer.sendReturnException(5, "boom");

    // Both the parent answer and the queued pipelined child receive exactly
    // one exception Return, and the queued bucket is drained (no leak).
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(5, .exception));
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(100, .exception));
    try std.testing.expectEqual(@as(usize, 0), peer.pending_promises.count());
}

test "Finish cancelling a queued pipelined call sends Return(canceled)" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    // Queue a pipelined call (question 42) against not-yet-resolved answer 9.
    const child_frame = try buildCallFrame(allocator, 42);
    defer allocator.free(child_frame);
    const inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    try peer_test_hooks.queuePromisedCall(&peer, 9, child_frame, inbound);

    // The remote cancels question 42 with a Finish before it was deliverable
    // (require_early_cancellation defaults to false).
    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = 42,
        .release_result_caps = true,
        .require_early_cancellation = false,
    });

    // The cancelled queued call must receive its mandated Return(canceled),
    // and the queue must be empty.
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(42, .canceled));
    try std.testing.expectEqual(@as(usize, 0), peer.pending_promises.count());
}

test "loopback answer with results is not recorded in resolved_answers" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    const Handlers = struct {
        fn onCall(_: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
            // Return real results (an empty struct) so that, before the fix,
            // the loopback answer would be recorded in resolved_answers.
            try p.sendReturnEmptyStruct(call.question_id);
        }
        fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const returned: *bool = @ptrCast(@alignCast(ctx));
            returned.* = true;
            try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
        }
    };

    var server_ctx: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &server_ctx, .on_call = Handlers.onCall });

    var returned = false;
    // A call whose resolved target is a local export is delivered via loopback.
    _ = try peer.sendCallResolved(
        .{ .exported = .{ .id = export_id } },
        0x99,
        0,
        &returned,
        null,
        Handlers.onReturn,
    );

    try std.testing.expect(returned);
    // The loopback answer must not linger in resolved_answers: no Finish ever
    // clears it, and its id (from our outbound counter) would otherwise
    // collide with the remote's inbound question-id space.
    try std.testing.expectEqual(@as(usize, 0), peer.resolved_answers.count());
}

test "cancelling one queued call preserves send order of the survivors (E-order)" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    // Three calls (questions 1,2,3) pipelined in order on answer 7.
    for ([_]u32{ 1, 2, 3 }) |qid| {
        const frame = try buildCallFrame(allocator, qid);
        defer allocator.free(frame);
        const inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
        try peer_test_hooks.queuePromisedCall(&peer, 7, frame, inbound);
    }

    // Cancel the middle one via Finish.
    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = 2,
        .release_result_caps = true,
        .require_early_cancellation = false,
    });

    // The surviving bucket must still hold [1, 3] in that order: orderedRemove
    // (not swapRemove) preserves E-order for replay.
    const bucket = peer.pending_promises.getPtr(7) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), bucket.items.len);
    var first = try protocol.DecodedMessage.init(allocator, bucket.items[0].frame);
    defer first.deinit();
    var second = try protocol.DecodedMessage.init(allocator, bucket.items[1].frame);
    defer second.deinit();
    try std.testing.expectEqual(@as(u32, 1), (try first.asCall()).question_id);
    try std.testing.expectEqual(@as(u32, 3), (try second.asCall()).question_id);
}
