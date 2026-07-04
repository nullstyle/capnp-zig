const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const Peer = capnpc.rpc.peer.Peer;
const peer_test_hooks = Peer.test_hooks;

// Reusable allocation-failure injection over the fallible peer helpers. Each
// impl runs a self-contained operation; std.testing.checkAllAllocationFailures
// re-runs it with every allocation failing in turn and asserts the operation
// propagates error.OutOfMemory with no leak and no double-free. This guards the
// queue-ownership and question-rollback error paths systematically.
//
// (Peer.handleFrame deliberately swallows OOM as a non-fatal error, so it is
// not a checkAllAllocationFailures target; these exercise the helpers that do
// propagate — where the ownership/rollback bugs actually lived.)

fn noopSend(_: *anyopaque, _: []const u8) anyerror!void {}
fn noopReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
fn noopCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}

fn buildCallFrame(allocator: std.mem.Allocator, question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, 0xABCD, 0);
    try call.setTargetImportedCap(7);
    _ = try call.initCapTableTyped(0);
    return builder.finish();
}

// Build a Call frame whose params carry `count` senderHosted cap descriptors,
// so the decoded params.cap_table drives InboundCapTable.init through the
// import-noting + rollback path.
fn buildCallFrameWithCaps(allocator: std.mem.Allocator, question_id: u32, count: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, 0xABCD, 0);
    try call.setTargetImportedCap(7);
    var cap_list = try call.initCapTableTyped(count);
    var idx: u32 = 0;
    while (idx < count) : (idx += 1) {
        const desc = try cap_list.get(idx);
        // Distinct import ids so each entry notes a fresh import in the table.
        protocol.CapDescriptor.writeSenderHosted(desc, 1000 + idx);
    }
    return builder.finish();
}

fn queuePromisedCallOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    const frame = buildCallFrame(allocator, 100) catch |err| {
        inbound.deinit();
        return err;
    };
    defer allocator.free(frame);

    peer_test_hooks.queuePromisedCall(&peer, 5, frame, inbound) catch |err| {
        // Ownership contract: on failure the caller retains `inbound`.
        inbound.deinit();
        return err;
    };
    // On success the queue owns `inbound` and a copy of the frame; Peer.deinit
    // frees both, and the original `frame` is freed by the defer above.
}

fn sendBootstrapOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var ctx: u8 = 0;
    const qid = try peer.sendBootstrap(&ctx, noopReturn);
    // On OOM sendBootstrap must roll the question back (no leak). On success,
    // remove the question before deinit: the terminal question pass would
    // otherwise synthesize a Disconnected Return for it with fallible
    // (swallowed) allocations, which checkAllAllocationFailures flags.
    peer_test_hooks.removeQuestion(&peer, qid);
}

// InboundCapTable.init decodes each cap descriptor and notes the import in the
// connection cap table. If a later noteImport OOMs, the errdefer must release
// the imports already noted for earlier entries — otherwise their ref-counts
// leak into the table for the connection lifetime.
fn inboundCapTableInitOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    const frame = try buildCallFrameWithCaps(allocator, 100, 3);
    defer allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    const call = try decoded.asCall();

    var inbound = try cap_table.InboundCapTable.init(allocator, call.params.cap_table, &peer.caps);
    // On success the caller owns the table; free its slices. The noted imports
    // live in peer.caps and are freed with the cap table by Peer.deinit.
    inbound.deinit();
}

// sendReturnResults reserves the resolved-answer record resources (count
// budget, map slot, frame copy) BEFORE the send so recording is infallible
// afterward; on OOM it must roll the reservation back and leave no resolved
// answer behind. This is the plain results path (no outbound caps).
fn sendReturnResultsOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    const BuildCtx = struct {
        fn build(_: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            _ = try any.initStruct(0, 0);
            _ = try ret.initCapTableTyped(0);
        }
    };
    var ctx: u8 = 0;
    try peer.sendReturnResults(42, &ctx, BuildCtx.build);
    // On success a resolved answer is recorded for id 42; Peer.deinit frees the
    // stored frame copy. On OOM the reservation is rolled back (no leak, no
    // stray resolved answer).
}

// sendReturnResults over a payload that references a local export exercises the
// outbound cap-effects path: encodeReturnPayloadCapsWithEffects discovers the
// senderHosted cap, onOutboundCap bumps the export ref, and on OOM the effects
// (and the export-ref bump) must roll back before the reservation does.
fn sendReturnResultsWithCapOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_ctx: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &handler_ctx, .on_call = noopCall });

    const BuildCtx = struct {
        export_id: u32,
        fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            const ctx: *const @This() = @ptrCast(@alignCast(ctx_ptr));
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            // A bare capability pointer to a local export; the encoder rederives
            // the cap table from it and classifies it as senderHosted.
            try any.setCapability(.{ .id = ctx.export_id });
        }
    };
    var ctx = BuildCtx{ .export_id = export_id };
    try peer.sendReturnResults(43, &ctx, BuildCtx.build);
    // On success the export ref is bumped and a resolved answer recorded; both
    // are freed by Peer.deinit. On OOM the cap effects roll the ref bump back
    // and the reservation is released, leaving the export at its prior count.
}

// sendReturnResults routes a `send_results_to_yourself` marker to a
// resultsSentElsewhere tag Return (a distinct, cap-free frame build) rather
// than the normal results payload. Exercises the sendReturnTag build+send path.
fn sendReturnResultsSentElsewhereOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    // Pre-mark the answer as results-sent-to-self so sendReturnResults takes the
    // resultsSentElsewhere branch. put may itself OOM (part of the injected op),
    // which is fine: nothing else is allocated yet.
    try peer.send_results_to_yourself.put(44, {});

    const BuildCtx = struct {
        fn build(_: *anyopaque, _: *protocol.ReturnBuilder) anyerror!void {
            unreachable; // resultsSentElsewhere path never invokes the builder.
        }
    };
    var ctx: u8 = 0;
    try peer.sendReturnResults(44, &ctx, BuildCtx.build);
    // resultsSentElsewhere is a terminal tag Return that records no resolved
    // answer, so there is nothing to clean up beyond Peer.deinit.
}

// sendCall with a local export in params exercises the full outbound
// param-cap chain: question allocation, payload cap encoding (export-ref
// bump via onOutboundCap), the question_param_export_refs record (taken
// BEFORE the frame is sent, so OOM propagates), and the send. On OOM every
// stage must roll back — question removed (which also frees the record),
// export ref bump reverted — leaving no partial record behind.
fn sendCallWithParamCapOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_ctx: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &handler_ctx, .on_call = noopCall });

    const BuildCtx = struct {
        export_id: u32,
        fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            const ctx: *const @This() = @ptrCast(@alignCast(ctx_ptr));
            var payload = try call.payloadTyped();
            var any = try payload.initContent();
            // A bare capability pointer to a local export; the encoder
            // classifies it as senderHosted, bumps the export ref, and the
            // peer records it under the question id.
            try any.setCapability(.{ .id = ctx.export_id });
        }
    };
    var ctx = BuildCtx{ .export_id = export_id };
    const qid = try peer.sendCall(7, 0xABCD, 0, &ctx, BuildCtx.build, noopReturn);
    // On success a param-export record exists for qid. Drain the question
    // before scope end: removeQuestion frees the record, and the terminal
    // question pass in deinit would otherwise synthesize a Return with
    // fallible (swallowed) allocations that the harness flags.
    peer_test_hooks.removeQuestion(&peer, qid);
}

// Note: sendCall / caps.noteImport tolerate a best-effort (non-fatal)
// allocation on the outbound path, so they are not all-or-nothing
// checkAllAllocationFailures targets (the harness would flag the recovered
// allocation as a "swallowed" OOM even though there is no leak).
// sendCallWithParamCapOomImpl above is the exception: the import-target
// param-cap path allocates all-or-nothing, so it IS harness-clean.
//
// handleFinish is likewise NOT a clean target: its two allocating branches —
// the finished_early_answers tombstone (`... catch reportNonfatalError`) and
// the queued-call cancel Return (`sendReturnTag(...) catch reportNonfatalError`
// in cancelQueuedPendingQuestionInMap) — deliberately swallow OOM as
// best-effort, so injecting a failure there is reported as a swallowed OOM
// rather than a propagated one. Its non-allocating cleanup branch (resolved
// answer take/free) offers nothing to inject. handleBootstrap propagates
// cleanly, but is private and not exposed via test_hooks, so it cannot be
// reached without editing src. sendReturnException wraps a clean
// sendReturnExceptionNoDrain but then calls failQueuedPromisedCalls, whose
// worklist seed append swallows OOM (best-effort) even with no queued children,
// so the public entry is not all-or-nothing either.

test "queuePromisedCall propagates OOM without leak or double-free" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, queuePromisedCallOomImpl, .{});
}

test "sendBootstrap rolls back cleanly under OOM injection" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sendBootstrapOomImpl, .{});
}

test "sendCall with param caps rolls back the export record under OOM injection" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sendCallWithParamCapOomImpl, .{});
}

test "InboundCapTable.init rolls back noted imports under OOM injection" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, inboundCapTableInitOomImpl, .{});
}

test "sendReturnResults reserves and rolls back the resolved answer under OOM injection" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sendReturnResultsOomImpl, .{});
}

test "sendReturnResults rolls back outbound cap refs under OOM injection" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sendReturnResultsWithCapOomImpl, .{});
}

test "sendReturnResults resultsSentElsewhere path propagates OOM cleanly" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sendReturnResultsSentElsewhereOomImpl, .{});
}
