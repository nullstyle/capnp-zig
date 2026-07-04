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

test "provided target preserves import origin under an export/import id collision" {
    const allocator = std.testing.allocator;
    // `capture` must outlive `peer`: peer.deinit sends a Release frame for the
    // imported cap, which the override captures — so the capture is declared
    // first (torn down last).
    var capture = newCapture(allocator);
    defer capture.deinit();

    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    // Remote-forced, spec-legal collision: export 9 and import 9 coexist.
    try peer.caps.noteExport(9);
    try peer.caps.noteImport(9);

    // A Provide whose resolved target is the IMPORT 9 must be returned to the
    // accepting (third) peer as receiverHosted{9} — never substituted by our
    // own senderHosted export 9 (which is what an id-space re-derivation would
    // pick, since classifyCap checks exports before imports).
    var target = try peer_test_hooks.makeProvideTarget(&peer, .{ .imported = .{ .id = 9 } });
    defer target.deinit(allocator);
    try peer_test_hooks.sendReturnProvidedTarget(&peer, 55, &target);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 55), ret.answer_id);
    const payload = ret.results orelse return error.MissingResults;
    const cap_list = payload.cap_table orelse return error.MissingCapTable;
    const desc = try protocol.CapDescriptor.fromReader(try cap_list.get(0));
    try std.testing.expectEqual(protocol.CapDescriptorTag.receiverHosted, desc.tag);
    try std.testing.expectEqual(@as(u32, 9), desc.id.?);
}

test "validation-work budget aborts a connection that exceeds its rate" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    // A frozen clock so no time-based refill happens between frames.
    var clock = capnpc.rpc.time.TestClock{};
    peer.setClock(clock.clock());

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    // Learn one frame's exact validation cost, then set the burst to exactly
    // that: the first frame fits, a second (no elapsed time → no refill) does not.
    const frame = try buildCallFrame(allocator, 1);
    defer allocator.free(frame);
    var probe = try protocol.DecodedMessage.init(allocator, frame);
    const cost = probe.msg.traversal_words_used;
    probe.deinit();
    try std.testing.expect(cost > 0);

    peer.setLimits(.{ .max_validation_words_per_second = 1000, .max_validation_burst_words = cost });

    // First frame fits the budget exactly (the bucket starts full).
    try peer.handleFrame(frame);
    // Second frame at the same instant exhausts the bucket → abort.
    try std.testing.expectError(error.ValidationBudgetExceeded, peer.handleFrame(frame));
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

// Shared fixture for the deinit terminal-callback tests: a heap call ctx
// shaped exactly like generated client stubs (callback destroys the ctx
// unconditionally; deinit_ctx covers the never-delivered case) plus
// externally-owned counters that outlive the ctx.
const TerminalCounters = struct {
    fired: usize = 0,
    disconnects: usize = 0,
};

const TerminalCtx = struct {
    counters: *TerminalCounters,
    allocator: std.mem.Allocator,

    fn onReturn(ctx_ptr: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *TerminalCtx = @ptrCast(@alignCast(ctx_ptr));
        // Generated callbacks destroy their ctx unconditionally.
        defer self.allocator.destroy(self);
        self.counters.fired += 1;
        if (ret.tag == .exception) {
            if (ret.exception) |ex| {
                if (std.mem.eql(u8, ex.reason, peer_impl.disconnected_reason)) {
                    self.counters.disconnects += 1;
                }
            }
        }
    }

    fn deinitCtx(a: std.mem.Allocator, ptr: *anyopaque) void {
        a.destroy(@as(*TerminalCtx, @ptrCast(@alignCast(ptr))));
    }
};

test "Peer.deinit fires the terminal Disconnected callback exactly once" {
    const allocator = std.testing.allocator;
    var counters = TerminalCounters{};
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    try peer.caps.noteImport(7);
    const ctx = try allocator.create(TerminalCtx);
    ctx.* = .{ .counters = &counters, .allocator = allocator };
    const qid = try peer.sendCall(7, 0xABCD, 0, ctx, null, TerminalCtx.onReturn);
    peer.setQuestionDeinitCtx(qid, TerminalCtx.deinitCtx);

    // Deinit with the question still in flight: the caller must observe a
    // terminal Disconnected Return through the normal callback (previously
    // the ctx was silently freed and the caller never heard anything).
    peer.deinit();
    try std.testing.expectEqual(@as(usize, 1), counters.fired);
    try std.testing.expectEqual(@as(usize, 1), counters.disconnects);
    // No explicit destroy(ctx): delivery transferred ownership to the
    // callback; std.testing.allocator would report a double-free or leak.
}

test "Peer.deinit does not re-fire the callback for an already-cancelled question" {
    const allocator = std.testing.allocator;
    var counters = TerminalCounters{};
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    try peer.caps.noteImport(7);
    const ctx = try allocator.create(TerminalCtx);
    ctx.* = .{ .counters = &counters, .allocator = allocator };
    const qid = try peer.sendCall(7, 0xABCD, 0, ctx, null, TerminalCtx.onReturn);
    peer.setQuestionDeinitCtx(qid, TerminalCtx.deinitCtx);

    // The question is cancelled first (deadline/cancel path): the callback
    // fires now, and the entry stays parked to absorb the late Return.
    try peer.cancelQuestion(qid, peer_impl.deadline_reason);
    try std.testing.expectEqual(@as(usize, 1), counters.fired);

    // Deinit must NOT deliver a second (terminal) callback for it.
    peer.deinit();
    try std.testing.expectEqual(@as(usize, 1), counters.fired);
}

test "deinit terminal pass never leaks the call ctx under allocation failure" {
    // Whatever allocation fails while synthesizing the terminal exception
    // Return, ctx ownership must resolve exactly once: either the callback
    // runs (and destroys it) or the deinit_ctx fallback frees it.
    var fail_index: usize = 0;
    while (fail_index < 32) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        const allocator = failing.allocator();
        var counters = TerminalCounters{};

        var peer = Peer.initDetached(allocator);
        peer.disableThreadAffinity();
        // The capture is test instrumentation, not code under test — keep it
        // off the failing allocator so injection stays inside the peer.
        var capture = newCapture(std.testing.allocator);
        defer capture.deinit();
        peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

        peer.caps.noteImport(7) catch {
            peer.deinit();
            continue;
        };
        const ctx = allocator.create(TerminalCtx) catch {
            peer.deinit();
            continue;
        };
        ctx.* = .{ .counters = &counters, .allocator = allocator };
        const qid = peer.sendCall(7, 0xABCD, 0, ctx, null, TerminalCtx.onReturn) catch {
            allocator.destroy(ctx);
            peer.deinit();
            continue;
        };
        peer.setQuestionDeinitCtx(qid, TerminalCtx.deinitCtx);

        // std.testing.allocator (backing the failing wrapper) reports any
        // leak or double-free at test end, whichever path the injection hit.
        peer.deinit();
        try std.testing.expect(counters.fired <= 1);
    }
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

/// Shared state for the answer-held reference regression tests: a "service"
/// export whose calls are counted, exported inside another answer's results.
const AnswerHeldState = struct {
    service_calls: u32 = 0,
    service_export_id: u32 = 0,
};

fn onAnswerHeldServiceCall(ctx_ptr: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
    const state: *AnswerHeldState = @ptrCast(@alignCast(ctx_ptr));
    state.service_calls += 1;
    try p.sendReturnEmptyStruct(call.question_id);
}

/// Results struct with one pointer slot; slot 0 carries the service cap.
fn buildAnswerHeldServiceResults(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
    const state: *AnswerHeldState = @ptrCast(@alignCast(ctx_ptr));
    var payload = try ret.payloadTyped();
    var any = try payload.initContent();
    const results = try any.initStruct(0, 1);
    var slot = try results.getAnyPointer(0);
    try slot.setCapability(.{ .id = state.service_export_id });
}

fn buildReleaseFrame(allocator: std.mem.Allocator, id: u32, count: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildRelease(id, count);
    return builder.finish();
}

/// A Call pipelined on answer `target_question_id`'s results pointer field 0.
fn buildPipelinedCallFrame(allocator: std.mem.Allocator, question_id: u32, target_question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, 0xABCD, 0);
    try call.setTargetPromisedAnswerWithOps(target_question_id, &[_]protocol.PromisedAnswerOp{
        .{ .tag = .getPointerField, .pointer_index = 0 },
    });
    _ = try call.initCapTableTyped(0);
    return builder.finish();
}

fn buildFinishFrame(allocator: std.mem.Allocator, question_id: u32, release_result_caps: bool) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildFinish(question_id, release_result_caps, false);
    return builder.finish();
}

test "resolved answer keeps its pipeline target alive across an early Release" {
    // Per the answer-lifecycle contract, a resolved answer keeps its pipeline
    // targets alive until Finish, independent of the client's Release of the
    // caps it imported from the Return. Before the answer-held reference fix,
    // the target's aliveness was coupled solely to the export-table wire
    // refcount: Return (ref 1) -> Release (ref 0, export destroyed) -> the
    // pipelined call failed to dispatch ("unknown promised capability").
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const Handlers = struct {
        fn onFactoryCall(ctx_ptr: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
            try p.sendReturnResults(call.question_id, ctx_ptr, buildAnswerHeldServiceResults);
        }
    };

    var state = AnswerHeldState{};
    state.service_export_id = try peer.addExport(.{ .ctx = &state, .on_call = onAnswerHeldServiceCall });
    const factory_export_id = try peer.addExport(.{ .ctx = &state, .on_call = Handlers.onFactoryCall });

    // Q1: the factory answers synchronously with results exporting the
    // service cap. The recorded answer takes its answer-held reference.
    const q1_frame = try buildExportCallFrame(allocator, 10, factory_export_id);
    defer allocator.free(q1_frame);
    try peer.handleFrame(q1_frame);
    try std.testing.expect(peer.resolved_answers.contains(10));
    {
        const entry = peer.exports.get(state.service_export_id) orelse return error.MissingExport;
        try std.testing.expectEqual(@as(u32, 1), entry.ref_count); // wire ref from the Return descriptor
        try std.testing.expectEqual(@as(u32, 1), entry.answer_ref_count); // answer-held until Finish
    }

    // The remote drops the imported cap right away — legal before Finish.
    // Only the wire ref may die; the export must survive for pipelining.
    const release_frame = try buildReleaseFrame(allocator, state.service_export_id, 1);
    defer allocator.free(release_frame);
    try peer.handleFrame(release_frame);
    {
        const entry = peer.exports.get(state.service_export_id) orelse return error.ExportDestroyedByEarlyRelease;
        try std.testing.expectEqual(@as(u32, 0), entry.ref_count);
        try std.testing.expectEqual(@as(u32, 1), entry.answer_ref_count);
    }

    // Q2 pipelines on Q1's results pointer field 0: it must still reach the
    // service handler and answer with results, not an exception.
    const q2_frame = try buildPipelinedCallFrame(allocator, 11, 10);
    defer allocator.free(q2_frame);
    try peer.handleFrame(q2_frame);
    try std.testing.expectEqual(@as(u32, 1), state.service_calls);
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(11, .results));
    try std.testing.expectEqual(@as(usize, 0), capture.countReturns(11, .exception));

    // Finish for Q1 (releaseResultCaps=false: the wire ref is already gone)
    // releases the answer-held reference; both counts at zero destroy the
    // export.
    const finish_frame = try buildFinishFrame(allocator, 10, false);
    defer allocator.free(finish_frame);
    try peer.handleFrame(finish_frame);
    try std.testing.expect(!peer.resolved_answers.contains(10));
    try std.testing.expect(!peer.exports.contains(state.service_export_id));
}

test "queued pipelined call dispatches when Release lands before the answer commit" {
    // The exact interleaving surfaced by the typed-pipelining runtime probe
    // with a synchronous in-process wire: the remote processes Q1's Return —
    // including sending Release for the imported cap — while our
    // sendReturnResults is still on the stack, BEFORE
    // commitReservedResolvedAnswer drains the parked pipelined call. The
    // answer-held reference is taken at reserve time (pre-send), so the
    // export survives the mid-resolution Release and the drain dispatches.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    const Injector = struct {
        capture: ReturnCapture,
        peer: *Peer,
        release_frame: []const u8,
        injected: bool = false,

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            try ReturnCapture.onFrame(&self.capture, frame);
            if (self.injected) return;
            var decoded = protocol.DecodedMessage.init(std.testing.allocator, frame) catch return;
            defer decoded.deinit();
            if (decoded.tag != .@"return") return;
            const ret = decoded.asReturn() catch return;
            if (ret.answer_id != 10 or ret.tag != .results) return;
            // The synchronous remote releases the cap it just imported from
            // Q1's Return before our sender returns to its commit step.
            self.injected = true;
            try self.peer.handleFrame(self.release_frame);
        }
    };

    var state = AnswerHeldState{};
    state.service_export_id = try peer.addExport(.{ .ctx = &state, .on_call = onAnswerHeldServiceCall });

    const release_frame = try buildReleaseFrame(allocator, state.service_export_id, 1);
    defer allocator.free(release_frame);

    var injector = Injector{
        .capture = newCapture(allocator),
        .peer = &peer,
        .release_frame = release_frame,
    };
    defer injector.capture.deinit();
    peer.setSendFrameOverride(&injector, Injector.onFrame);

    // Park a pipelined call (question 42) against unresolved answer 10.
    const q2_frame = try buildPipelinedCallFrame(allocator, 42, 10);
    defer allocator.free(q2_frame);
    const inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    try peer_test_hooks.queuePromisedCall(&peer, 10, q2_frame, inbound);

    // Resolve answer 10 with results exporting the service cap. The send
    // override injects the Release mid-flight; the commit then drains the
    // parked call, which must still reach the service handler.
    try peer.sendReturnResults(10, &state, buildAnswerHeldServiceResults);

    try std.testing.expect(injector.injected);
    try std.testing.expectEqual(@as(u32, 1), state.service_calls);
    try std.testing.expectEqual(@as(usize, 1), injector.capture.countReturns(42, .results));
    try std.testing.expectEqual(@as(usize, 0), injector.capture.countReturns(42, .exception));

    // The export survived the mid-resolution Release on the answer-held ref.
    const entry = peer.exports.get(state.service_export_id) orelse return error.ExportDestroyedByEarlyRelease;
    try std.testing.expectEqual(@as(u32, 0), entry.ref_count);
    try std.testing.expectEqual(@as(u32, 1), entry.answer_ref_count);
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
