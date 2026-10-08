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

    /// Count captured frames whose message-union tag is `tag`.
    fn countTag(self: *@This(), tag: protocol.MessageTag) usize {
        var n: usize = 0;
        for (self.frames.items) |frame| {
            var decoded = protocol.DecodedMessage.init(self.allocator, frame) catch continue;
            defer decoded.deinit();
            if (decoded.tag == tag) n += 1;
        }
        return n;
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

/// A well-formed frame whose message-union discriminant is an unknown tag:
/// it passes the full validation walk (so its traversal cost is a normal
/// call's) but dispatches to nothing — `DecodedMessage` reports
/// `error.InvalidMessageTag` and the peer echoes `Unimplemented`. The
/// message-union discriminant is the u16 at the root struct's data offset 0.
fn buildUnknownTagFrame(allocator: std.mem.Allocator, question_id: u32) ![]u8 {
    const base = try buildCallFrame(allocator, question_id);
    defer allocator.free(base);
    const buf = try allocator.dupe(u8, base);
    errdefer allocator.free(buf);
    // Frame header (8 bytes for a single segment) + root struct pointer word
    // (8 bytes) puts the root struct's data word 0 at byte 16; the message
    // union discriminant is its low u16.
    std.mem.writeInt(u16, buf[16..18], 63, .little);
    return buf;
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

test "Finish cancelling a queued call drains calls pipelined on it" {
    // A queued call (100) is pipelined on unresolved answer 10; a grandchild
    // (200) is in turn pipelined on 100's own answer. When Finish(100) cancels
    // the queued call, 100's answer will never be produced — so 200 can never
    // be satisfied and must also receive a Return. Before the fix the cancel
    // sent Return(canceled) for 100 but left pending_promises[100] untouched:
    // 200 hung forever and its orphan bucket could later replay against an
    // unrelated answer if the remote reused id 100.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    // Queue call 100 against unresolved answer 10.
    const call_100 = try buildCallFrame(allocator, 100);
    defer allocator.free(call_100);
    const inbound_100 = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    try peer_test_hooks.queuePromisedCall(&peer, 10, call_100, inbound_100);

    // Queue grandchild 200 pipelined on 100's own (future) answer.
    const call_200 = try buildPipelinedCallFrame(allocator, 200, 100);
    defer allocator.free(call_200);
    const inbound_200 = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    try peer_test_hooks.queuePromisedCall(&peer, 100, call_200, inbound_200);
    try std.testing.expectEqual(@as(usize, 2), peer.pending_promises.count());

    // Cancel the queued call 100.
    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = 100,
        .release_result_caps = true,
        .require_early_cancellation = false,
    });

    // 100 gets its Return(canceled); 200 gets an exception Return (its target
    // was cancelled); both buckets are drained.
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(100, .canceled));
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(200, .exception));
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

test "undispatchable frames are charged against the validation budget" {
    // A frame that validates fully but carries an unknown message tag never
    // dispatches — it only echoes Unimplemented (which re-walks and clones the
    // same payload). Before the fix that path skipped the budget charge, so a
    // hostile peer got unlimited validation CPU (and echo amplification) from
    // frames that never dispatch. Now the failed frame is charged first.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var clock = capnpc.rpc.time.TestClock{};
    peer.setClock(clock.clock());

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const unknown = try buildUnknownTagFrame(allocator, 1);
    defer allocator.free(unknown);
    // Confirm the frame is well-formed but undispatchable, and learn its cost.
    var probe_cost: usize = 0;
    try std.testing.expectError(
        error.InvalidMessageTag,
        protocol.DecodedMessage.initCounting(allocator, unknown, &probe_cost),
    );
    try std.testing.expect(probe_cost > 0);

    // Budget for exactly one such frame; a frozen clock means no refill.
    peer.setLimits(.{ .max_validation_words_per_second = 1000, .max_validation_burst_words = probe_cost });

    // First undispatchable frame fits and is echoed as Unimplemented.
    try peer.handleFrame(unknown);
    try std.testing.expectEqual(@as(usize, 1), capture.countTag(.unimplemented));
    try std.testing.expectEqual(@as(usize, 0), capture.countTag(.abort));

    // Second at the same instant exhausts the now-charged budget → abort,
    // with no second Unimplemented echo.
    try std.testing.expectError(error.ValidationBudgetExceeded, peer.handleFrame(unknown));
    try std.testing.expectEqual(@as(usize, 1), capture.countTag(.unimplemented));
    try std.testing.expectEqual(@as(usize, 1), capture.countTag(.abort));
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

/// A pipelined call whose transform is empty: the target answer's payload
/// content IS the capability (the classic call-on-bootstrap-promise shape).
fn buildBootstrapPipelinedCallFrame(allocator: std.mem.Allocator, question_id: u32, target_question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, 0xABCD, 0);
    try call.setTargetPromisedAnswer(target_question_id);
    _ = try call.initCapTableTyped(0);
    return builder.finish();
}

fn buildBootstrapFrame(allocator: std.mem.Allocator, question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildBootstrap(question_id);
    return builder.finish();
}

/// A handler that holds its question open (the async-answer pattern): it
/// records the question id and returns without sending any Return, so the
/// question stays in active_inbound_questions until the app answers later.
const AsyncHoldState = struct {
    held_question_id: u32 = 0,
    calls: u32 = 0,
};

fn onAsyncHoldCall(ctx_ptr: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
    _ = p;
    const state: *AsyncHoldState = @ptrCast(@alignCast(ctx_ptr));
    state.held_question_id = call.question_id;
    state.calls += 1;
}

/// A handler that answers synchronously with cap-less results, so every call
/// records a resolved answer (empty-struct tag Returns are not recorded).
const ResultsEchoState = struct {
    calls: u32 = 0,
    export_id: u32 = 0,
};

fn onResultsEchoCall(ctx_ptr: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
    const state: *ResultsEchoState = @ptrCast(@alignCast(ctx_ptr));
    state.calls += 1;
    try p.sendReturnResults(call.question_id, ctx_ptr, buildEchoResults);
}

fn buildEchoResults(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
    _ = ctx_ptr;
    var payload = try ret.payloadTyped();
    var any = try payload.initContent();
    _ = try any.initStruct(1, 0);
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

test "reentrant Finish during Return send drains queued promise before cleanup" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    const finish_frame = try buildFinishFrame(allocator, 10, false);
    defer allocator.free(finish_frame);

    const Injector = struct {
        capture: ReturnCapture,
        peer: *Peer,
        finish_frame: []const u8,
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
            self.injected = true;
            try self.peer.handleFrame(self.finish_frame);
        }
    };

    var state = AnswerHeldState{};
    state.service_export_id = try peer.addExport(.{ .ctx = &state, .on_call = onAnswerHeldServiceCall });

    var injector = Injector{
        .capture = newCapture(allocator),
        .peer = &peer,
        .finish_frame = finish_frame,
    };
    defer injector.capture.deinit();
    peer.setSendFrameOverride(&injector, Injector.onFrame);

    const child_frame = try buildPipelinedCallFrame(allocator, 42, 10);
    defer allocator.free(child_frame);
    const inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    try peer_test_hooks.queuePromisedCall(&peer, 10, child_frame, inbound);

    try peer.sendReturnResults(10, &state, buildAnswerHeldServiceResults);

    try std.testing.expect(injector.injected);
    try std.testing.expectEqual(@as(usize, 1), injector.capture.countReturns(10, .results));
    try std.testing.expectEqual(@as(u32, 1), state.service_calls);
    try std.testing.expectEqual(@as(usize, 1), injector.capture.countReturns(42, .results));
    try std.testing.expect(!peer.resolved_answers.contains(10));
    try std.testing.expect(!peer.resolving_answers.contains(10));
    try std.testing.expect(!peer.finished_early_answers.contains(10));
}

test "reentrant Finish during Bootstrap Return send drains queued pipelined call before cleanup" {
    // Bootstrap answers commit through the same reserve → send →
    // commit-or-cleanup discipline as sendReturnResults. Before that, a Finish
    // delivered synchronously while the Bootstrap Return send was on the stack
    // found nothing to clean (bootstrap ids are never active or resolving), and
    // the record afterwards stranded a resolved answer — plus its answer-held
    // export reference — that no later Finish could ever clear.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    const finish_frame = try buildFinishFrame(allocator, 10, true);
    defer allocator.free(finish_frame);

    const Injector = struct {
        capture: ReturnCapture,
        peer: *Peer,
        finish_frame: []const u8,
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
            self.injected = true;
            try self.peer.handleFrame(self.finish_frame);
        }
    };

    var state = AnswerHeldState{};
    const bootstrap_export_id = try peer.setBootstrap(.{ .ctx = &state, .on_call = onAnswerHeldServiceCall });
    state.service_export_id = bootstrap_export_id;

    var injector = Injector{
        .capture = newCapture(allocator),
        .peer = &peer,
        .finish_frame = finish_frame,
    };
    defer injector.capture.deinit();
    peer.setSendFrameOverride(&injector, Injector.onFrame);

    // The caller pipelines a call on the bootstrap answer before Bootstrap
    // itself is delivered (the classic call-on-bootstrap-promise shape).
    const child_frame = try buildBootstrapPipelinedCallFrame(allocator, 42, 10);
    defer allocator.free(child_frame);
    const inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    try peer_test_hooks.queuePromisedCall(&peer, 10, child_frame, inbound);

    const bootstrap_frame = try buildBootstrapFrame(allocator, 10);
    defer allocator.free(bootstrap_frame);
    try peer.handleFrame(bootstrap_frame);

    // The parked pipelined call replayed against the committed answer before
    // the reentrant Finish's cleanup removed it.
    try std.testing.expect(injector.injected);
    try std.testing.expectEqual(@as(usize, 1), injector.capture.countReturns(10, .results));
    try std.testing.expectEqual(@as(u32, 1), state.service_calls);
    try std.testing.expectEqual(@as(usize, 1), injector.capture.countReturns(42, .results));
    try std.testing.expect(!peer.resolved_answers.contains(10));
    try std.testing.expect(!peer.resolving_answers.contains(10));
    try std.testing.expect(!peer.finished_early_answers.contains(10));
    // releaseResultCaps=true dropped the descriptor's wire reference and the
    // cleanup dropped the answer-held reference. The bootstrap export entry
    // itself persists for the connection lifetime by design (it must stay
    // servable for the next Bootstrap), but it holds no leaked references.
    const entry = peer.exports.get(bootstrap_export_id) orelse return error.MissingBootstrapExport;
    try std.testing.expectEqual(@as(u32, 0), entry.ref_count);
    try std.testing.expectEqual(@as(u32, 0), entry.answer_ref_count);
}

test "late Return after cancelling Finish releases result caps per the Finish flag" {
    // Finish-before-Return with releaseResultCaps=true: the caller will never
    // release caps from a Return it receives after its Finish, so the late
    // Return sender must drop the wire references its results descriptors
    // took. Before this fix they leaked for the connection lifetime.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    var svc = AnswerHeldState{};
    svc.service_export_id = try peer.addExport(.{ .ctx = &svc, .on_call = onAnswerHeldServiceCall });
    var holder = AsyncHoldState{};
    const factory_export_id = try peer.addExport(.{ .ctx = &holder, .on_call = onAsyncHoldCall });

    // The handler holds question 10 open (async answer pattern).
    const call_frame = try buildExportCallFrame(allocator, 10, factory_export_id);
    defer allocator.free(call_frame);
    try peer.handleFrame(call_frame);
    try std.testing.expectEqual(@as(u32, 1), holder.calls);
    try std.testing.expectEqual(@as(usize, 0), capture.countReturns(10, .results));

    // A pipelined call parks on the unresolved answer before the cancel.
    const child_frame = try buildPipelinedCallFrame(allocator, 42, 10);
    defer allocator.free(child_frame);
    try peer.handleFrame(child_frame);

    // The remote cancels: Finish before any Return, releaseResultCaps=true.
    // The parked child survives the Finish (it is addressed by its own id).
    const finish_frame = try buildFinishFrame(allocator, 10, true);
    defer allocator.free(finish_frame);
    try peer.handleFrame(finish_frame);
    try std.testing.expect(peer.finished_early_answers.contains(10));

    // The app answers late with cap-bearing results.
    try peer.sendReturnResults(10, &svc, buildAnswerHeldServiceResults);

    // Exactly one late Return went out, the parked child replayed with its
    // own Return (spec: every Call gets exactly one Return), nothing stayed
    // recorded, and the tombstone drained.
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(10, .results));
    try std.testing.expectEqual(@as(u32, 1), svc.service_calls);
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(42, .results));
    try std.testing.expectEqual(@as(usize, 0), peer.pending_promises.count());
    try std.testing.expect(!peer.resolved_answers.contains(10));
    try std.testing.expect(!peer.finished_early_answers.contains(10));
    // The Finish's releaseResultCaps dropped the wire reference the results
    // descriptor took; the transient commit's answer-held reference was
    // released by the immediate cleanup, so the export is destroyed rather
    // than leaked.
    try std.testing.expect(!peer.exports.contains(svc.service_export_id));
}

test "reusing a question id with an undischarged early-Finish tombstone is rejected" {
    // A compliant caller may not reuse a question id before it has received
    // the id's Return (which drains the tombstone). A violator that reuses it
    // earlier must be rejected: otherwise the reused id's Return sender would
    // consume the STALE tombstone — skipping its record and applying the old
    // Finish's releaseResultCaps flag to the new answer's result caps, a
    // remote-forceable premature export release.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    var svc = AnswerHeldState{};
    svc.service_export_id = try peer.addExport(.{ .ctx = &svc, .on_call = onAnswerHeldServiceCall });
    var holder = AsyncHoldState{};
    const factory_export_id = try peer.addExport(.{ .ctx = &holder, .on_call = onAsyncHoldCall });

    const call_frame = try buildExportCallFrame(allocator, 10, factory_export_id);
    defer allocator.free(call_frame);
    try peer.handleFrame(call_frame);

    // Finish-before-Return (releaseResultCaps=false: the caller says it will
    // release result caps individually once the late Return arrives).
    const finish_frame = try buildFinishFrame(allocator, 10, false);
    defer allocator.free(finish_frame);
    try peer.handleFrame(finish_frame);
    try std.testing.expect(peer.finished_early_answers.contains(10));

    // The violator reuses the id before the late Return has drained it.
    const dup_frame = try buildExportCallFrame(allocator, 10, factory_export_id);
    defer allocator.free(dup_frame);
    try std.testing.expectError(error.DuplicateQuestionId, peer.handleFrame(dup_frame));
    try std.testing.expectEqual(@as(u32, 1), holder.calls);
    try std.testing.expect(peer.finished_early_answers.contains(10));

    // The late Return still drains the tombstone normally afterwards, keeping
    // the wire reference for the remote to Release individually.
    try peer.sendReturnResults(10, &svc, buildAnswerHeldServiceResults);
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(10, .results));
    try std.testing.expect(!peer.finished_early_answers.contains(10));
    try std.testing.expect(!peer.resolved_answers.contains(10));
    const entry = peer.exports.get(svc.service_export_id) orelse return error.MissingExport;
    try std.testing.expectEqual(@as(u32, 1), entry.ref_count);
    try std.testing.expectEqual(@as(u32, 0), entry.answer_ref_count);
}

test "nested resolved-answer reservations survive the map's load-factor boundary" {
    // A synchronous transport can deliver a nested inbound Call while an
    // outer results Return send is on the stack. The nested reserve→commit
    // consumes a resolved_answers slot that the outer reservation's
    // ensureUnusedCapacity counted on; at an exact load-factor boundary the
    // outer infallible commit then underflowed the map's reserved-slot
    // accounting. Sweep filler counts across the first growth boundaries so
    // at least one iteration lands on a boundary regardless of std.HashMap's
    // internal growth policy.
    const allocator = std.testing.allocator;
    var filler: u32 = 0;
    while (filler <= 16) : (filler += 1) {
        var peer = Peer.initDetached(allocator);
        peer.disableThreadAffinity();
        defer peer.deinit();

        var echo = ResultsEchoState{};
        echo.export_id = try peer.addExport(.{ .ctx = &echo, .on_call = onResultsEchoCall });

        const Injector = struct {
            peer: *Peer,
            nested_frame: []const u8,
            injected: bool = false,

            fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
                const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
                if (self.injected) return;
                var decoded = protocol.DecodedMessage.init(std.testing.allocator, frame) catch return;
                defer decoded.deinit();
                if (decoded.tag != .@"return") return;
                const ret = decoded.asReturn() catch return;
                if (ret.answer_id != 500 or ret.tag != .results) return;
                self.injected = true;
                try self.peer.handleFrame(self.nested_frame);
            }
        };

        const nested_frame = try buildExportCallFrame(allocator, 501, echo.export_id);
        defer allocator.free(nested_frame);
        var injector = Injector{ .peer = &peer, .nested_frame = nested_frame };
        peer.setSendFrameOverride(&injector, Injector.onFrame);

        // Fill resolved_answers to this sweep point with synchronous calls.
        var i: u32 = 0;
        while (i < filler) : (i += 1) {
            const f = try buildExportCallFrame(allocator, 1000 + i, echo.export_id);
            defer allocator.free(f);
            try peer.handleFrame(f);
        }

        // The outer call's Return send delivers the nested call reentrantly:
        // the nested answer reserves and commits while the outer reservation
        // is still open.
        const outer_frame = try buildExportCallFrame(allocator, 500, echo.export_id);
        defer allocator.free(outer_frame);
        try peer.handleFrame(outer_frame);

        try std.testing.expect(injector.injected);
        try std.testing.expect(peer.resolved_answers.contains(500));
        try std.testing.expect(peer.resolved_answers.contains(501));
        try std.testing.expectEqual(filler + 2, peer.resolved_answers.count());
        try std.testing.expectEqual(@as(u32, filler + 2), echo.calls);
        try std.testing.expectEqual(@as(u32, 0), peer.resolved_answer_reservations);
    }
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

// ---------------------------------------------------------------------------
// Pipelined capabilities in call PARAMS (capnp-swift handoff H9).
//
// A caller may pass a capability it does not hold yet: the result of one of
// its own still-unanswered questions, as a `receiverAnswer` cap descriptor in
// the params cap table. The C++ reference does this whenever a pipelined cap
// is an argument. The receiving Peer must hand the handler the capability the
// answer resolved to, not the raw `.promised` placeholder.

/// Probe export: records what the Peer handed its handler for params cap 0.
const ParamCapProbe = struct {
    calls: u32 = 0,
    seen: ?cap_table.ResolvedCap = null,

    fn onCall(ctx_ptr: *anyopaque, p: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ParamCapProbe = @ptrCast(@alignCast(ctx_ptr));
        self.calls += 1;
        const params = try call.params.content.getStruct();
        const cap = try params.readCapability(0);
        self.seen = try caps.resolveCapability(cap);
        try p.sendReturnEmptyStruct(call.question_id);
    }
};

/// A Call on export `target_export_id` whose params struct carries one
/// capability: `receiverAnswer(answer_id, [getPointerField 0])`, i.e. the
/// pointer-0 result of the caller's own question `answer_id`.
fn buildPipelinedParamCallFrame(
    allocator: std.mem.Allocator,
    question_id: u32,
    target_export_id: u32,
    answer_id: u32,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, 0xABCD, 1);
    try call.setTargetImportedCap(target_export_id);
    var payload = try call.payloadTyped();
    var any = try payload.initContent();
    const params = try any.initStruct(0, 1);
    var slot = try params.getAnyPointer(0);
    try slot.setCapability(.{ .id = 0 });
    var cap_list = try call.initCapTableTyped(1);
    const entry = try cap_list.get(0);
    try protocol.CapDescriptor.writeReceiverAnswer(entry._builder, answer_id, &[_]protocol.PromisedAnswerOp{
        .{ .tag = .getPointerField, .pointer_index = 0 },
    });
    return builder.finish();
}

test "receiverAnswer param resolves to the answer's export before the handler runs (H9)" {
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

    // E: the capability q1's results carry. F: the factory that returns it.
    var state = AnswerHeldState{};
    state.service_export_id = try peer.addExport(.{ .ctx = &state, .on_call = onAnswerHeldServiceCall });
    const factory_export_id = try peer.addExport(.{ .ctx = &state, .on_call = Handlers.onFactoryCall });
    var probe = ParamCapProbe{};
    const probe_export_id = try peer.addExport(.{ .ctx = &probe, .on_call = ParamCapProbe.onCall });

    // q1: answered synchronously with results { ptr0 = E }.
    const q1_frame = try buildExportCallFrame(allocator, 10, factory_export_id);
    defer allocator.free(q1_frame);
    try peer.handleFrame(q1_frame);
    try std.testing.expect(peer.resolved_answers.contains(10));

    // q3: the caller pipelines q1's result into q3's params before it has seen
    // q1's Return (from this Peer's side the answer is already recorded).
    const q3_frame = try buildPipelinedParamCallFrame(allocator, 11, probe_export_id, 10);
    defer allocator.free(q3_frame);
    try peer.handleFrame(q3_frame);

    try std.testing.expectEqual(@as(u32, 1), probe.calls);
    const seen = probe.seen orelse return error.ProbeNotCalled;
    switch (seen) {
        .exported => |exported| try std.testing.expectEqual(state.service_export_id, exported.id),
        else => return error.ParamCapNotResolved,
    }
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(11, .results));
    try std.testing.expectEqual(@as(usize, 0), capture.countReturns(11, .exception));
}

test "receiverAnswer param on a failed answer fails the call with that exception (H9)" {
    // The C++ reference hands the handler a broken capability carrying the
    // answer's exception. `ResolvedCap` has no broken variant, and a null cap
    // would hide the failure, so the Peer fails the call itself with a copy of
    // the answer's exception: same reason, same type.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const Handlers = struct {
        fn onFailingCall(_: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
            try p.sendReturnExceptionTyped(call.question_id, "boom", .overloaded);
        }
    };
    var failing_ctx: u8 = 0;
    const failing_export_id = try peer.addExport(.{ .ctx = &failing_ctx, .on_call = Handlers.onFailingCall });
    var probe = ParamCapProbe{};
    const probe_export_id = try peer.addExport(.{ .ctx = &probe, .on_call = ParamCapProbe.onCall });

    const q1_frame = try buildExportCallFrame(allocator, 10, failing_export_id);
    defer allocator.free(q1_frame);
    try peer.handleFrame(q1_frame);
    try std.testing.expect(peer.failed_answers.contains(10));

    const q3_frame = try buildPipelinedParamCallFrame(allocator, 11, probe_export_id, 10);
    defer allocator.free(q3_frame);
    try peer.handleFrame(q3_frame);

    try std.testing.expectEqual(@as(u32, 0), probe.calls);
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(11, .exception));
    for (capture.frames.items) |frame| {
        var decoded = try protocol.DecodedMessage.init(allocator, frame);
        defer decoded.deinit();
        if (decoded.tag != .@"return") continue;
        const ret = try decoded.asReturn();
        if (ret.answer_id != 11) continue;
        const ex = ret.exception orelse return error.MissingException;
        try std.testing.expectEqualStrings("boom", ex.reason);
        try std.testing.expectEqual(protocol.ExceptionType.overloaded, ex.kind());
    }
}

/// q3 for the queued-replay test: target `promisedAnswer(target_answer_id,
/// [ptr0])`, params cap `receiverAnswer(param_answer_id, [ptr0])`.
fn buildPipelinedTargetAndParamCallFrame(
    allocator: std.mem.Allocator,
    question_id: u32,
    target_answer_id: u32,
    param_answer_id: u32,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, 0xABCD, 1);
    try call.setTargetPromisedAnswerWithOps(target_answer_id, &[_]protocol.PromisedAnswerOp{
        .{ .tag = .getPointerField, .pointer_index = 0 },
    });
    var payload = try call.payloadTyped();
    var any = try payload.initContent();
    const params = try any.initStruct(0, 1);
    var slot = try params.getAnyPointer(0);
    try slot.setCapability(.{ .id = 0 });
    var cap_list = try call.initCapTableTyped(1);
    const entry = try cap_list.get(0);
    try protocol.CapDescriptor.writeReceiverAnswer(entry._builder, param_answer_id, &[_]protocol.PromisedAnswerOp{
        .{ .tag = .getPointerField, .pointer_index = 0 },
    });
    return builder.finish();
}

test "receiverAnswer param resolves when a queued call replays (H9)" {
    // q3 targets a promise (q0, deferred) and carries q1's result in params.
    // It parks until q0 returns; by then q1 is answered, so the replay must
    // resolve the param. The replay reads the queued frame copy: the original
    // inbound frame is freed before q0 returns.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const Deferred = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    const Handlers = struct {
        fn onFactoryCall(ctx_ptr: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
            try p.sendReturnResults(call.question_id, ctx_ptr, buildAnswerHeldServiceResults);
        }
    };

    var state = AnswerHeldState{};
    state.service_export_id = try peer.addExport(.{ .ctx = &state, .on_call = onAnswerHeldServiceCall });
    const factory_export_id = try peer.addExport(.{ .ctx = &state, .on_call = Handlers.onFactoryCall });
    var deferred_ctx: u8 = 0;
    const deferred_export_id = try peer.addExport(.{ .ctx = &deferred_ctx, .on_call = Deferred.onCall });
    var probe = ParamCapProbe{};
    const probe_export_id = try peer.addExport(.{ .ctx = &probe, .on_call = ParamCapProbe.onCall });

    // q0: deferred; its eventual results carry the probe in pointer 0.
    const q0_frame = try buildExportCallFrame(allocator, 5, deferred_export_id);
    defer allocator.free(q0_frame);
    try peer.handleFrame(q0_frame);

    // q1: answered at once with results { ptr0 = E }.
    const q1_frame = try buildExportCallFrame(allocator, 10, factory_export_id);
    defer allocator.free(q1_frame);
    try peer.handleFrame(q1_frame);

    {
        const q3_frame = try buildPipelinedTargetAndParamCallFrame(allocator, 11, 5, 10);
        defer allocator.free(q3_frame);
        try peer.handleFrame(q3_frame);
    }
    try std.testing.expect(peer.pending_promises.contains(5));
    try std.testing.expectEqual(@as(u32, 0), probe.calls);

    // q0 answers with the probe: the parked q3 replays onto it.
    var probe_state = AnswerHeldState{ .service_export_id = probe_export_id };
    try peer.sendReturnResults(5, &probe_state, buildAnswerHeldServiceResults);

    try std.testing.expectEqual(@as(u32, 1), probe.calls);
    const seen = probe.seen orelse return error.ProbeNotCalled;
    switch (seen) {
        .exported => |exported| try std.testing.expectEqual(state.service_export_id, exported.id),
        else => return error.ParamCapNotResolved,
    }
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(11, .results));
}

test "a replayed call's unresolved receiverAnswer param reads from the queued frame (H9)" {
    // q3 parks on q0; its param names q1, which is still pending when q3
    // replays, so the handler gets the `.promised` entry. That entry's
    // transform must read the queued frame copy. Before the fix it still
    // pointed into the original inbound frame, freed right after q3 queued.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const Probe = struct {
        calls: u32 = 0,
        question_id: ?u32 = null,
        transform_len: ?u32 = null,
        pointer_index: ?u16 = null,

        fn onCall(ctx_ptr: *anyopaque, p: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            self.calls += 1;
            const entry = try caps.get(0);
            if (entry == .promised) {
                self.question_id = entry.promised.question_id;
                self.transform_len = entry.promised.transform.len();
                const op = try entry.promised.transform.get(0);
                self.pointer_index = op.pointer_index;
            }
            try p.sendReturnEmptyStruct(call.question_id);
        }
    };
    const Deferred = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var deferred_ctx: u8 = 0;
    const deferred_export_id = try peer.addExport(.{ .ctx = &deferred_ctx, .on_call = Deferred.onCall });
    var probe = Probe{};
    const probe_export_id = try peer.addExport(.{ .ctx = &probe, .on_call = Probe.onCall });

    // q0 and q1 both stay pending.
    for ([_]u32{ 5, 10 }) |qid| {
        const frame = try buildExportCallFrame(allocator, qid, deferred_export_id);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
    {
        const q3_frame = try buildPipelinedTargetAndParamCallFrame(allocator, 11, 5, 10);
        defer allocator.free(q3_frame);
        try peer.handleFrame(q3_frame);
    }
    try std.testing.expect(peer.pending_promises.contains(5));

    var probe_state = AnswerHeldState{ .service_export_id = probe_export_id };
    try peer.sendReturnResults(5, &probe_state, buildAnswerHeldServiceResults);

    try std.testing.expectEqual(@as(u32, 1), probe.calls);
    try std.testing.expectEqual(@as(?u32, 10), probe.question_id);
    try std.testing.expectEqual(@as(?u32, 1), probe.transform_len);
    try std.testing.expectEqual(@as(?u16, 0), probe.pointer_index);

    try peer.sendReturnEmptyStruct(10);
}

test "receiverAnswer param on a still-pending answer dispatches at once as .promised (H9 limit)" {
    // Deliberate limit, pinned so a change is a decision, not an accident:
    // when the named answer is still pending (deferred or forwarded), the call
    // is NOT delayed. Delaying it would let later calls on the same target
    // overtake it (E-order) and would need Disembargo reflections queued
    // behind it. The handler sees the `.promised` entry, as before.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const Deferred = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    var deferred_ctx: u8 = 0;
    const deferred_export_id = try peer.addExport(.{ .ctx = &deferred_ctx, .on_call = Deferred.onCall });
    var probe = ParamCapProbe{};
    const probe_export_id = try peer.addExport(.{ .ctx = &probe, .on_call = ParamCapProbe.onCall });

    const q1_frame = try buildExportCallFrame(allocator, 10, deferred_export_id);
    defer allocator.free(q1_frame);
    try peer.handleFrame(q1_frame);

    const q3_frame = try buildPipelinedParamCallFrame(allocator, 11, probe_export_id, 10);
    defer allocator.free(q3_frame);
    try peer.handleFrame(q3_frame);

    try std.testing.expectEqual(@as(u32, 1), probe.calls);
    const seen = probe.seen orelse return error.ProbeNotCalled;
    switch (seen) {
        .promised => |promised| try std.testing.expectEqual(@as(u32, 10), promised.question_id),
        else => return error.UnexpectedResolution,
    }
    try std.testing.expectEqual(@as(usize, 0), peer.pending_promises.count());

    try peer.sendReturnEmptyStruct(10);
}

// ---------------------------------------------------------------------------
// Parked calls whose answer resolves to a still-unresolved promise export
// (capnp-swift handoff H10).
//
// A call pipelined on promisedAnswer(q) that arrives BEFORE q's Return parks
// on q. When q's results carry a promise export P that is not resolved yet,
// the call must park again, on P, and replay when P resolves: exactly what
// happens to a call that arrives after the Return. This needs a server that
// defers or forwards its answer, as capnp-swift's host does.

const PromiseServiceState = struct {
    calls: u32 = 0,
    last_question_id: ?u32 = null,
};

fn onPromiseServiceCall(ctx_ptr: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
    const state: *PromiseServiceState = @ptrCast(@alignCast(ctx_ptr));
    state.calls += 1;
    state.last_question_id = call.question_id;
    try p.sendReturnEmptyStruct(call.question_id);
}

test "call parked before a Return re-parks on the promise export the Return carries (H10)" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const Deferred = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    var deferred_ctx: u8 = 0;
    const deferred_export_id = try peer.addExport(.{ .ctx = &deferred_ctx, .on_call = Deferred.onCall });
    var service = PromiseServiceState{};
    const service_export_id = try peer.addExport(.{ .ctx = &service, .on_call = onPromiseServiceCall });
    const promise_export_id = try peer.addPromiseExport();

    // q (5): the server defers its answer.
    const q_frame = try buildExportCallFrame(allocator, 5, deferred_export_id);
    defer allocator.free(q_frame);
    try peer.handleFrame(q_frame);

    // c (6): pipelined on q's result pointer 0, before q's Return.
    const c_frame = try buildPipelinedCallFrame(allocator, 6, 5);
    defer allocator.free(c_frame);
    try peer.handleFrame(c_frame);
    try std.testing.expect(peer.pending_promises.contains(5));

    // q returns results { ptr0 = P }, P an unresolved promise export.
    var results_state = AnswerHeldState{ .service_export_id = promise_export_id };
    try peer.sendReturnResults(5, &results_state, buildAnswerHeldServiceResults);

    // c must wait on P: no Return for it yet, and it sits in P's queue.
    try std.testing.expectEqual(@as(usize, 0), capture.countReturns(6, .exception));
    try std.testing.expectEqual(@as(usize, 0), capture.countReturns(6, .results));
    try std.testing.expect(peer.pending_export_promises.contains(promise_export_id));
    try std.testing.expectEqual(@as(u32, 0), service.calls);

    // P resolves to the service: c replays onto it and is answered once.
    try peer.resolvePromiseExportToExport(promise_export_id, service_export_id);
    try std.testing.expectEqual(@as(u32, 1), service.calls);
    try std.testing.expectEqual(@as(?u32, 6), service.last_question_id);
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(6, .results));
    try std.testing.expectEqual(@as(usize, 0), capture.countReturns(6, .exception));
    try std.testing.expect(!peer.pending_export_promises.contains(promise_export_id));
}

test "calls parked on a promise export re-park when it resolves to another unresolved promise (H10)" {
    // The same defect one hop later: P resolves to P2, which is itself still
    // a promise. Calls parked on P must move to P2, not fail.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    var service = PromiseServiceState{};
    const service_export_id = try peer.addExport(.{ .ctx = &service, .on_call = onPromiseServiceCall });
    const outer_promise_id = try peer.addPromiseExport();
    const inner_promise_id = try peer.addPromiseExport();

    // c (6) targets P directly and parks on it.
    const c_frame = try buildExportCallFrame(allocator, 6, outer_promise_id);
    defer allocator.free(c_frame);
    try peer.handleFrame(c_frame);
    try std.testing.expect(peer.pending_export_promises.contains(outer_promise_id));

    try peer.resolvePromiseExportToExport(outer_promise_id, inner_promise_id);
    try std.testing.expectEqual(@as(usize, 0), capture.countReturns(6, .exception));
    try std.testing.expect(peer.pending_export_promises.contains(inner_promise_id));
    try std.testing.expectEqual(@as(u32, 0), service.calls);

    // A call arriving now on P follows the chain to P2 and parks behind c.
    const d_frame = try buildExportCallFrame(allocator, 7, outer_promise_id);
    defer allocator.free(d_frame);
    try peer.handleFrame(d_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.countReturns(7, .exception));

    try peer.resolvePromiseExportToExport(inner_promise_id, service_export_id);
    try std.testing.expectEqual(@as(u32, 2), service.calls);
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(6, .results));
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(7, .results));
    // E-order: c (sent first) was delivered first.
    try std.testing.expectEqual(@as(?u32, 7), service.last_question_id);
}

test "a parked call answered with an exception at replay releases its param imports" {
    // c parks on q with an unsatisfiable transform and one senderHosted param
    // cap (an import reference this vat now holds). When q returns, the
    // replay answers c with an exception and must send the Release for that
    // import. Before, the drain sent the exception and dropped the reference.
    const allocator = std.testing.allocator;
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var capture = newCapture(allocator);
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const Deferred = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    var deferred_ctx: u8 = 0;
    const deferred_export_id = try peer.addExport(.{ .ctx = &deferred_ctx, .on_call = Deferred.onCall });
    var service = PromiseServiceState{};
    const service_export_id = try peer.addExport(.{ .ctx = &service, .on_call = onPromiseServiceCall });

    const q_frame = try buildExportCallFrame(allocator, 5, deferred_export_id);
    defer allocator.free(q_frame);
    try peer.handleFrame(q_frame);

    const remote_export_id: u32 = 77;
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        // [ptr0, ptr0]: the second step reads a struct field of a capability.
        var call = try builder.beginCall(6, 0xABCD, 0);
        try call.setTargetPromisedAnswerWithOps(5, &[_]protocol.PromisedAnswerOp{
            .{ .tag = .getPointerField, .pointer_index = 0 },
            .{ .tag = .getPointerField, .pointer_index = 0 },
        });
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        const params = try any.initStruct(0, 1);
        var slot = try params.getAnyPointer(0);
        try slot.setCapability(.{ .id = 0 });
        var cap_list = try call.initCapTableTyped(1);
        protocol.CapDescriptor.writeSenderHosted(try cap_list.get(0), remote_export_id);
        const c_frame = try builder.finish();
        defer allocator.free(c_frame);
        try peer.handleFrame(c_frame);
    }
    try std.testing.expect(peer.pending_promises.contains(5));
    try std.testing.expect(peer.caps.hasImport(remote_export_id));

    var results_state = AnswerHeldState{ .service_export_id = service_export_id };
    try peer.sendReturnResults(5, &results_state, buildAnswerHeldServiceResults);

    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(6, .exception));
    try std.testing.expectEqual(@as(u32, 0), service.calls);
    try std.testing.expectEqual(@as(usize, 1), capture.countTag(.release));
    try std.testing.expect(!peer.caps.hasImport(remote_export_id));
}

// ---------------------------------------------------------------------------
// Exactly one terminal per question at transport close (capnp-swift handoff
// H8): the cancel pass must not depend on the allocator, and it must never
// deliver a second terminal to a question that already saw its Return.

/// Counts a question's terminal callbacks; `disconnects` counts the synthetic
/// Disconnected ones.
const TerminalWaiter = struct {
    fired: usize = 0,
    disconnects: usize = 0,

    fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *TerminalWaiter = @ptrCast(@alignCast(ctx));
        self.fired += 1;
        if (ret.tag != .exception) return;
        const ex = ret.exception orelse return;
        if (std.mem.eql(u8, ex.reason, peer_impl.disconnected_reason)) self.disconnects += 1;
    }
};

test "transport close ends every open question even when no allocation succeeds (H8)" {
    // `capture` outlives the peer (deinit may still send frames).
    var capture = newCapture(std.testing.allocator);
    defer capture.deinit();

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var peer = Peer.initDetached(failing.allocator());
    peer.disableThreadAffinity();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    try peer.caps.noteImport(7);
    var waiters = [_]TerminalWaiter{ .{}, .{}, .{} };
    for (&waiters) |*waiter| {
        _ = try peer.sendCall(7, 0xABCD, 0, waiter, null, TerminalWaiter.onReturn);
    }
    try std.testing.expectEqual(@as(usize, 3), peer.questions.count());

    // From here on every allocation and resize of the peer fails.
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    peer.notifyTransportClosed();
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);

    // Every question got its Disconnected terminal from the close itself, not
    // from the later deinit.
    for (waiters) |waiter| {
        try std.testing.expectEqual(@as(usize, 1), waiter.fired);
        try std.testing.expectEqual(@as(usize, 1), waiter.disconnects);
    }
    try std.testing.expectEqual(@as(usize, 0), peer.questions.count());

    peer.deinit();
    for (waiters) |waiter| try std.testing.expectEqual(@as(usize, 1), waiter.fired);
}

test "the deadline sweep cancels an expired question even when no allocation succeeds (H8)" {
    var capture = newCapture(std.testing.allocator);
    defer capture.deinit();

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var clock = capnpc.rpc.time.TestClock{};
    var peer = Peer.initDetached(failing.allocator());
    peer.disableThreadAffinity();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);
    peer.setClock(clock.clock());

    try peer.caps.noteImport(7);
    var waiter = TerminalWaiter{};
    const qid = try peer.sendCall(7, 0xABCD, 0, &waiter, null, TerminalWaiter.onReturn);
    try peer.setQuestionDeadline(qid, 5);
    clock.advanceMs(10);

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    const cancelled = peer.checkDeadlines();
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);

    // The expired call got its terminal in this tick, not at some later one.
    try std.testing.expectEqual(@as(usize, 1), cancelled);
    try std.testing.expectEqual(@as(usize, 1), waiter.fired);

    peer.deinit();
    try std.testing.expectEqual(@as(usize, 1), waiter.fired);
}

/// Fails exactly one allocation, the `fail_at`-th from arming, and lets the
/// ones after it through: a transient OOM. `std.testing.FailingAllocator`
/// fails every allocation after its index, which hides a swallowed failure
/// whenever a later allocation in the same operation fails too.
const OneShotFailingAllocator = struct {
    backing: std.mem.Allocator,
    remaining: ?usize = null,
    induced: bool = false,

    fn arm(self: *OneShotFailingAllocator, fail_at: usize) void {
        self.remaining = fail_at;
        self.induced = false;
    }

    fn allocator(self: *OneShotFailingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *OneShotFailingAllocator = @ptrCast(@alignCast(ctx));
        if (self.remaining) |remaining| {
            if (remaining == 0) {
                self.remaining = null;
                self.induced = true;
                return null;
            }
            self.remaining = remaining - 1;
        }
        return self.backing.rawAlloc(len, alignment, ra);
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
        const self: *OneShotFailingAllocator = @ptrCast(@alignCast(ctx));
        return self.backing.rawResize(memory, alignment, new_len, ra);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
        const self: *OneShotFailingAllocator = @ptrCast(@alignCast(ctx));
        return self.backing.rawRemap(memory, alignment, new_len, ra);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *OneShotFailingAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ra);
    }
};

test "a call queued under a transient OOM still gets exactly one Return when its answer fails" {
    // Queueing decodes the call's question id once, for the drains. An OOM
    // there used to store the call with a null id, as if it were not a call,
    // and the failure drain then skipped its Return. Fail each allocation of
    // the enqueue in turn: either the enqueue fails (the caller answers the
    // call) or the queued call gets its terminal.
    var fail_at: usize = 0;
    var finished = false;
    while (!finished) : (fail_at += 1) {
        var capture = newCapture(std.testing.allocator);
        defer capture.deinit();
        var one_shot = OneShotFailingAllocator{ .backing = std.testing.allocator };
        var peer = Peer.initDetached(one_shot.allocator());
        peer.disableThreadAffinity();
        defer peer.deinit();
        peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

        const child_frame = try buildCallFrame(std.testing.allocator, 100);
        defer std.testing.allocator.free(child_frame);
        var inbound = try cap_table.InboundCapTable.init(one_shot.allocator(), null, &peer.caps);

        one_shot.arm(fail_at);
        const queued = peer_test_hooks.queuePromisedCall(&peer, 5, child_frame, inbound);
        const induced = one_shot.induced;
        one_shot.remaining = null;

        if (queued) |_| {
            try peer.sendReturnException(5, "boom");
            try std.testing.expectEqual(@as(usize, 1), capture.countReturns(100, .exception));
            finished = !induced;
        } else |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            inbound.deinit();
        }
    }
    try std.testing.expect(fail_at > 1);
}

/// A finished results Return for `answer_id` with an empty struct payload.
fn buildEmptyResultsReturnFrame(allocator: std.mem.Allocator, answer_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .results);
    var payload = try ret.payloadTyped();
    _ = try payload.initContent();
    return builder.finish();
}

test "a transient OOM after a Return's callback ran never re-delivers it at close" {
    // Return handling can fail AFTER the question's callback has run: the
    // automatic Finish cannot be built. The question then went back into the
    // questions table, and transport close delivered it a second, synthetic
    // Disconnected terminal: a second callback into a spent context
    // (capnp-deno's WASM ABI guards its L3 contexts against exactly this).
    // Fail each allocation of the Return's handling in turn: the caller must
    // see one terminal in total across the Return, the close and deinit.
    const allocator = std.testing.allocator;
    var fail_at: usize = 0;
    var finished = false;
    while (!finished) : (fail_at += 1) {
        var capture = newCapture(allocator);
        defer capture.deinit();
        var one_shot = OneShotFailingAllocator{ .backing = allocator };
        var peer = Peer.initDetached(one_shot.allocator());
        peer.disableThreadAffinity();
        peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

        try peer.caps.noteImport(7);
        var waiter = TerminalWaiter{};
        const qid = try peer.sendCall(7, 0xABCD, 0, &waiter, null, TerminalWaiter.onReturn);
        const frame = try buildEmptyResultsReturnFrame(allocator, qid);
        defer allocator.free(frame);

        one_shot.arm(fail_at);
        peer.handleFrame(frame) catch |err| try std.testing.expectEqual(error.OutOfMemory, err);
        finished = !one_shot.induced;
        one_shot.remaining = null;

        // An OOM before the callback leaves the question open, and the close
        // gives it its one (Disconnected) terminal; an OOM after it must not.
        peer.notifyTransportClosed();
        peer.deinit();
        try std.testing.expectEqual(@as(usize, 1), waiter.fired);
    }
    try std.testing.expect(fail_at > 1);
}

test "a callback that fails with OOM is not re-delivered at close" {
    // The callback itself ran and saw the Return; its OutOfMemory propagates
    // out of handleFrame, but the question must not come back for a second,
    // synthetic terminal.
    const allocator = std.testing.allocator;
    var capture = newCapture(allocator);
    defer capture.deinit();
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const OomOnce = struct {
        fired: usize = 0,
        fn onReturn(ctx: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.fired += 1;
            if (self.fired == 1) return error.OutOfMemory;
        }
    };
    try peer.caps.noteImport(7);
    var waiter = OomOnce{};
    const qid = try peer.sendCall(7, 0xABCD, 0, &waiter, null, OomOnce.onReturn);
    const frame = try buildEmptyResultsReturnFrame(allocator, qid);
    defer allocator.free(frame);

    try std.testing.expectError(error.OutOfMemory, peer.handleFrame(frame));
    try std.testing.expect(!peer.questions.contains(qid));
    peer.notifyTransportClosed();
    peer.deinit();
    try std.testing.expectEqual(@as(usize, 1), waiter.fired);
}

test "close and deinit after a retained call's Return deliver no second terminal" {
    // capnp-deno reported Return machinery re-delivering into spent questions
    // at shutdown. Pin exactly-once for retained calls, with and without
    // noFinishNeeded, across transport close and deinit.
    for ([_]bool{ true, false }) |no_finish_needed| {
        const allocator = std.testing.allocator;
        var capture = newCapture(allocator);
        defer capture.deinit();
        var peer = Peer.initDetached(allocator);
        peer.disableThreadAffinity();
        peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

        try peer.caps.noteImport(7);
        var waiter = TerminalWaiter{};
        const qid = try peer.sendCallWithOptions(7, 0xABCD, 0, &waiter, null, TerminalWaiter.onReturn, .{
            .result_lifetime = .retained,
        });

        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        var ret = try builder.beginReturn(qid, .results);
        ret.setNoFinishNeeded(no_finish_needed);
        var payload = try ret.payloadTyped();
        _ = try payload.initContent();
        const frame = try builder.finish();
        defer allocator.free(frame);
        try peer.handleFrame(frame);
        try std.testing.expectEqual(@as(usize, 1), waiter.fired);

        peer.notifyTransportClosed();
        try std.testing.expectEqual(@as(usize, 1), waiter.fired);
        peer.deinit();
        try std.testing.expectEqual(@as(usize, 1), waiter.fired);
        try std.testing.expectEqual(@as(usize, 0), waiter.disconnects);
    }
}
