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

/// A minimal, decodable inbound Call whose target is the local export
/// `export_id` (delivered via handleFrame so it is a real remote call — not
/// loopback — and its resolved answer is recorded).
fn buildExportCallFrame(allocator: std.mem.Allocator, question_id: u32, export_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, 0xABCD, 0);
    try call.setTargetImportedCap(export_id);
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

test "late Return after Finish (async handler) is not recorded in resolved_answers" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    // Async handler: stashes the answer id and returns WITHOUT answering, so
    // the inbound question stays active past the Finish.
    const Async = struct {
        var pending_answer: ?u32 = null;
        fn onCall(_: *anyopaque, _: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
            pending_answer = call.question_id;
        }
    };
    Async.pending_answer = null;

    var server_ctx: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &server_ctx, .on_call = Async.onCall });

    // Inbound Call (question 7) targeting the export; handler leaves it pending.
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(7, 0xABCD, 0);
    try call.setTargetImportedCap(export_id);
    _ = try call.initCapTableTyped(0);
    const call_frame = try builder.finish();
    defer allocator.free(call_frame);
    try peer.handleFrame(call_frame);
    try std.testing.expectEqual(@as(?u32, 7), Async.pending_answer);

    // Finish for question 7 arrives before the async Return (cancellation race).
    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = 7,
        .release_result_caps = true,
        .require_early_cancellation = false,
    });

    // The async handler finally answers.
    try peer.sendReturnEmptyStruct(7);

    // The late Return is delivered exactly once but NOT recorded: no lingering
    // resolved_answers entry to poison reuse of question id 7.
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(7, .results));
    try std.testing.expectEqual(@as(usize, 0), peer.resolved_answers.count());
}

test "outstanding questions are failed with Disconnected on connection close" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    const Noop = struct {
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
    };
    peer.setSendFrameOverride(&peer, Noop.send);

    const Waiter = struct {
        fired: usize = 0,
        disconnects: usize = 0,
        fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.fired += 1;
            if (ret.tag == .exception) {
                if (ret.exception) |ex| {
                    if (std.mem.eql(u8, ex.reason, "disconnected")) self.disconnects += 1;
                }
            }
        }
    };

    // Two outstanding questions whose Returns never arrive.
    var w1 = Waiter{};
    var w2 = Waiter{};
    _ = try peer.sendBootstrap(&w1, Waiter.onReturn);
    _ = try peer.sendBootstrap(&w2, Waiter.onReturn);
    try std.testing.expectEqual(@as(usize, 2), peer.questions.count());

    // The transport drops: the connection's on_close drives onConnectionClose.
    peer_test_hooks.onConnectionClose(&peer);

    // Every waiter's callback fired exactly once with the Disconnected signal,
    // and no question is left hanging.
    try std.testing.expectEqual(@as(usize, 1), w1.fired);
    try std.testing.expectEqual(@as(usize, 1), w1.disconnects);
    try std.testing.expectEqual(@as(usize, 1), w2.fired);
    try std.testing.expectEqual(@as(usize, 1), w2.disconnects);
    try std.testing.expectEqual(@as(usize, 0), peer.questions.count());
}

test "a queued pipelined call's question id is still detected as duplicate" {
    // Guards the quadratic-scan fix: duplicate detection now matches the id
    // decoded once at enqueue instead of re-decoding every queued frame. The
    // behavior (reject a reused id) must be unchanged.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    // Queue a pipelined call with question id 100 behind unresolved answer 5.
    const child = try buildCallFrame(allocator, 100);
    defer allocator.free(child);
    const inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    try peer_test_hooks.queuePromisedCall(&peer, 5, child, inbound);

    // An inbound Call reusing question id 100 must be rejected as a duplicate.
    const dup = try buildCallFrame(allocator, 100);
    defer allocator.free(dup);
    try std.testing.expectError(error.DuplicateQuestionId, peer.handleFrame(dup));
}

test "outstanding call context is freed at Peer.deinit via deinit_ctx" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    // A valid import target for the outbound call.
    try peer.caps.noteImport(7);

    // Heap context exactly like generated client stubs allocate.
    const Ctx = struct { value: u32 };
    const CtxOps = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
        fn deinitCtx(a: std.mem.Allocator, ptr: *anyopaque) void {
            a.destroy(@as(*Ctx, @ptrCast(@alignCast(ptr))));
        }
    };
    const ctx = try allocator.create(Ctx);
    ctx.* = .{ .value = 7 };

    // Mirror the generated stub: sendCall, then register deinit_ctx.
    const qid = try peer.sendCall(7, 0xABCD, 0, ctx, null, CtxOps.onReturn);
    peer.setQuestionDeinitCtx(qid, CtxOps.deinitCtx);

    // Deinit with the question still outstanding (connection dropped before a
    // Return). deinit_ctx must free the heap ctx — before this fix generated
    // code never registered it, so std.testing.allocator would report a leak.
    peer.deinit();
    // No explicit destroy(ctx): deinit_ctx now owns it.
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

test "successful call at the resolved_answers cap sends exactly one Return" {
    const allocator = std.testing.allocator;
    // A small resolved-answers budget we can fill deterministically. A hostile
    // peer reaches the real default (4096) by sending that many calls without
    // Finish; the mechanism under test is identical.
    var peer = Peer.initDetachedWithLimits(allocator, .{ .max_resolved_answers = 2 });
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    // Export whose handler answers every call with an empty-struct results
    // Return (a successful, results-bearing answer that gets recorded).
    const Handlers = struct {
        fn onCall(_: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
            try p.sendReturnEmptyStruct(call.question_id);
        }
    };
    var server_ctx: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &server_ctx, .on_call = Handlers.onCall });

    // Fill resolved_answers to the cap with successful, never-Finished inbound
    // calls so their resolved-answer entries persist.
    const cap = peer.limits.max_resolved_answers;
    var qid: u32 = 1;
    while (qid <= cap) : (qid += 1) {
        const frame = try buildExportCallFrame(allocator, qid, export_id);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
    try std.testing.expectEqual(cap, peer.resolved_answers.count());

    // A further successful call cannot record its resolved answer — the budget
    // is full. It must still receive EXACTLY ONE Return. Before the fix the
    // results frame was sent first and then recordResolvedAnswer's count-limit
    // check failed, propagating into the dispatch catch which sent a SECOND
    // (exception) Return for the same answer: two Returns for one call (audit
    // 2026-07-03 item 7). The reserve-before-send rewrite rejects the call up
    // front, so it now returns a single exception Return.
    const boundary_qid: u32 = @intCast(cap + 1);
    const frame = try buildExportCallFrame(allocator, boundary_qid, export_id);
    defer allocator.free(frame);
    try peer.handleFrame(frame);

    const results = capture.countReturns(boundary_qid, .results);
    const exceptions = capture.countReturns(boundary_qid, .exception);
    try std.testing.expectEqual(@as(usize, 1), results + exceptions);
    // Current design: a limit rejection surfaces as a single exception Return,
    // and no results frame reached the wire.
    try std.testing.expectEqual(@as(usize, 0), results);
    try std.testing.expectEqual(@as(usize, 1), exceptions);
    // The rejected call left no resolved-answer entry (still exactly `cap`).
    try std.testing.expectEqual(cap, peer.resolved_answers.count());
}
