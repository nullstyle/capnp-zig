//! Proof test for Cap'n Proto RPC Level-3 three-party handoff ORIGINATION —
//! the REDIRECTED-RETURN slice (Phase P5 of docs/rpc-l3-origination-plan.md):
//! a `Call` with `sendResultsTo = thirdParty` (rpc.capnp:494-505) whose results
//! are delivered to a *third* vat via a `ThirdPartyAnswer` (rpc.capnp:906-942),
//! not returned to the caller. Independent of the Provide/Accept slices (P1-P4).
//!
//! Three vats — B (caller/introducer), C (callee, host of the called cap), and
//! A (the third party that receives the results) — are wired over a synchronous
//! in-process loopback. A `Peer` binds to exactly one remote, so:
//!
//!   Vat B:  b_to_c  (B<->C)          — sends the redirected Call, gets resultsSentElsewhere
//!   Vat C:  c_to_b  (B<->C)          — receives the Call, answers B with resultsSentElsewhere
//!           c_to_a  (C<->A)          — sends ThirdPartyAnswer + results Return to A
//!   Vat A:  a_to_c  (C<->A)          — primed to await; receives ThirdPartyAnswer + results
//!
//! Scenario:
//!   1. C hosts Carol, a `getNumber()` cap returning 42, as its bootstrap.
//!   2. B imports Carol via a bootstrap on B<->C.
//!   3. A is primed to await results for this call: `registerPendingThirdPartyAwait`
//!      parks A's result callback keyed by the completion token bytes.
//!   4. B calls Carol with `sendResultsTo = thirdParty(recipient_token)` — the
//!      recipient token names A (byte-identical to A's completion token).
//!   5. C's Carol handler sees `send_results_to = thirdParty`. It:
//!        a. sends a `ThirdPartyAnswer{ completion, answerId=X }` to A on c_to_a,
//!           where X is a CALLEE-allocated answer id in [2^30, 2^31);
//!        b. sends the real results `Return{ answerId=X, n=42 }` to A on c_to_a;
//!        c. settles B's question by calling `sendReturnResultsSentElsewhere` on
//!           c_to_b, which emits `Return{resultsSentElsewhere}` — the tag the
//!           spec mandates for a Return answering a Call whose sendResultsTo was
//!           not `caller`. B learns the call completed; it never sees the value.
//!
//! Note `c_to_b` opts in with `setThirdPartyResultPolicy(.application)`. The
//! default is to refuse such a Call: a vat that cannot reach the third party must
//! not accept the call and then drop its results.
//!   6. A adopts its parked await under X (handleThirdPartyAnswer), the results
//!      Return completes A's callback with 42, and A sends a Finish(X) to C
//!      (rpc.capnp:924-926) which drains C's answer table.
//!   7. Teardown: every third-party table on all three peers drains under
//!      std.testing.allocator (any leak fails the test).

const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const vat_network = capnpc.rpc.vat.network;
const message = capnpc.message;
const Peer = peer_impl.Peer;

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

// -- getNumber() wire shape --------------------------------------------------
// Params: empty struct. Results: one data word, `n :UInt32` at byte offset 0.

const NUMBER_INTERFACE_ID: u64 = 0xC0C0_C0C0_C0C0_C001;
const GET_NUMBER_METHOD_ID: u16 = 0;

fn readNumberN(payload: protocol.Payload) !u32 {
    const content = try payload.content.getStruct();
    return content.readU32(0);
}

const NumberReturnCtx = struct {
    n: u32,
    fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *const NumberReturnCtx = castCtx(*const NumberReturnCtx, ctx_ptr);
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        const results = try any.initStruct(1, 0);
        results.writeU32(0, self.n);
    }
};

// -- Vat C: Carol, the callee. Redirects results to the third party. ---------

const Carol = struct {
    // The C<->A peer on which Carol delivers redirected results.
    c_to_a: *Peer,
    get_number_calls: u32 = 0,
    // The callee-allocated answer id C minted for the ThirdPartyAnswer.
    third_party_answer_id: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Carol = castCtx(*Carol, ctx_ptr);
        if (call.interface_id != NUMBER_INTERFACE_ID or call.method_id != GET_NUMBER_METHOD_ID) {
            return error.UnexpectedMethod;
        }
        self.get_number_calls += 1;

        // This call must be redirected: it carried sendResultsTo=thirdParty.
        if (call.send_results_to.tag != .thirdParty) return error.ExpectedThirdPartyRedirect;
        const completion = call.send_results_to.third_party orelse
            return error.MissingThirdPartyRecipient;

        // (a) Announce the answer to the third party (A) on the C<->A connection
        //     with a callee-allocated answer id in [2^30, 2^31).
        const answer_id = try self.c_to_a.sendThirdPartyAnswer(completion);
        self.third_party_answer_id = answer_id;

        // (b) Deliver the real results to A under the same callee-allocated id.
        var ret_ctx = NumberReturnCtx{ .n = 42 };
        try self.c_to_a.sendReturnResults(answer_id, &ret_ctx, NumberReturnCtx.build);

        // (c) Settle the caller's (B's) question with `resultsSentElsewhere` --
        //     the tag the protocol mandates for a Return answering a Call whose
        //     sendResultsTo was not `caller`. `awaitFromThirdParty` is a
        //     different message: it is what an INTRODUCER sends to the ORIGINAL
        //     caller on a different connection, and this three-peer harness does
        //     not model that link.
        try peer.sendReturnResultsSentElsewhere(call.question_id);
    }
};

// -- Vat B: imports Carol via bootstrap on B<->C -----------------------------

const CarolImportProbe = struct {
    carol_import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *CarolImportProbe = castCtx(*CarolImportProbe, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
        const payload = ret.results orelse return error.MissingBootstrapPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.carol_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.CarolNotImported,
        };
    }
};

// -- Vat B: caller of the redirected call ------------------------------------
//
// B is the caller; its question is settled with `resultsSentElsewhere`, NOT the
// results. B's callback fires with that tag and the question drains normally.
//
// A single ctx carries both the outbound recipient token (for the build fn) and
// the observed Return tag (for the on_return fn); `sendCall` shares one ctx.

const RedirectedCall = struct {
    recipient: message.AnyPointerReader,
    // Set by onReturn. For a redirected call the callback fires with
    // `resultsSentElsewhere`; a `.results` tag here would mean the payload
    // leaked back to B instead of going to A.
    callback_invoked: bool = false,
    observed_tag: ?protocol.ReturnTag = null,

    // Set sendResultsTo = thirdParty(recipient) on the outbound Call.
    fn buildCall(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *RedirectedCall = castCtx(*RedirectedCall, ctx_ptr);
        try call.setSendResultsToThirdParty(self.recipient);
    }

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *RedirectedCall = castCtx(*RedirectedCall, ctx_ptr);
        self.callback_invoked = true;
        self.observed_tag = ret.tag;
    }
};

// -- Vat A: the third party. Receives the redirected results. ----------------

const ThirdPartyResult = struct {
    result: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *ThirdPartyResult = castCtx(*ThirdPartyResult, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedThirdPartyResultReturn;
        const payload = ret.results orelse return error.MissingThirdPartyResultPayload;
        self.result = try readNumberN(payload);
    }
};

// -- Two-connection synchronous wire -----------------------------------------
//
// B<->C: frames forward straight to the paired endpoint.
// C<->A: frames forward straight to the paired endpoint.

const Link = struct {
    forwarding: bool = true,

    b_to_c: ?*Peer = null,
    c_to_b: ?*Peer = null,
    c_to_a: ?*Peer = null,
    a_to_c: ?*Peer = null,

    fn bToCSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        if (self.c_to_b) |peer| try peer.handleFrame(frame);
    }

    fn cToBSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        if (self.b_to_c) |peer| try peer.handleFrame(frame);
    }

    fn cToASend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        if (self.a_to_c) |peer| try peer.handleFrame(frame);
    }

    fn aToCSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        if (self.c_to_a) |peer| try peer.handleFrame(frame);
    }
};

test "three-party handoff redirected return: sendResultsTo=thirdParty delivers results to A via ThirdPartyAnswer" {
    const allocator = std.testing.allocator;

    var link = Link{};
    // Stop forwarding before any peer.deinit (LIFO) so teardown frames are not
    // delivered into a half-torn-down peer.
    defer link.forwarding = false;

    // -- Vat C: two endpoints. ------------------------------------------------
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();

    // -- Vat B: one endpoint. -------------------------------------------------
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();

    // -- Vat A: one endpoint. -------------------------------------------------
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    // Opt in to application-owned third-party result routing: Carol delivers the
    // results to A itself and then settles B's question. Without this the peer
    // refuses the call outright, which is the correct default for a vat that
    // cannot reach a third party.
    c_to_b.setThirdPartyResultPolicy(.application);
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;

    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    // -- C hosts Carol as its bootstrap; Carol delivers redirected results on
    //    the C<->A connection. -------------------------------------------------
    var carol = Carol{ .c_to_a = &c_to_a };
    _ = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = Carol.onCall });

    // -- (1) B imports Carol via a bootstrap on B<->C. ------------------------
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;

    // -- (2) A is primed to await the redirected results. ---------------------
    // The completion token bytes must equal the recipient token bytes B sends.
    const recipient_nonce = "redirect-nonce-1";
    const token = try vat_network.encodeNonceToken(allocator, recipient_nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();

    var third_party_result = ThirdPartyResult{};
    try a_to_c.registerPendingThirdPartyAwait(
        completion,
        &third_party_result,
        ThirdPartyResult.onReturn,
    );
    // A parked exactly one await, keyed by the completion bytes.
    try std.testing.expectEqual(@as(usize, 1), a_to_c.pending_third_party_awaits.count());

    // -- (3) B calls getNumber() on Carol with sendResultsTo=thirdParty(A). ---
    var redirected = RedirectedCall{ .recipient = completion };
    const b_question_id = try b_to_c.sendCall(
        carol_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );

    // -- Assertions: the redirected return reached A, not B. ------------------

    // Carol ran exactly once and minted a ThirdPartyAnswer with a callee id.
    try std.testing.expectEqual(@as(u32, 1), carol.get_number_calls);
    const answer_id = carol.third_party_answer_id orelse return error.NoThirdPartyAnswerSent;

    // The answer id is in the callee-allocated range [2^30, 2^31): bit 30 set,
    // bit 31 clear (rpc.capnp:936-941).
    try std.testing.expect(peer_impl.third_party.isThirdPartyAnswerId(answer_id));
    try std.testing.expect((answer_id & 0x4000_0000) != 0);
    try std.testing.expect((answer_id & 0x8000_0000) == 0);

    // A received the ThirdPartyAnswer + results Return and observed 42 — the
    // value travelled C -> A directly, never through B.
    try std.testing.expectEqual(@as(u32, 42), third_party_result.result orelse
        return error.ThirdPartyResultNotDelivered);

    // A adopted its parked await: the await table drained and the callee id is
    // now recorded as an adopted answer (mapped back to A's synthetic question).
    try std.testing.expectEqual(@as(usize, 0), a_to_c.pending_third_party_awaits.count());
    try std.testing.expectEqual(@as(usize, 0), a_to_c.pending_third_party_answers.count());

    // B's question was settled with `resultsSentElsewhere`: B learns the call
    // completed and that the results went somewhere else, and never observes the
    // value itself. B's callback DOES fire (with that tag) and its question
    // drains normally -- no entry is parked until teardown.
    try std.testing.expect(redirected.callback_invoked);
    try std.testing.expectEqual(
        @as(?protocol.ReturnTag, .resultsSentElsewhere),
        redirected.observed_tag,
    );
    try std.testing.expect(!b_to_c.questions.contains(b_question_id));
    try std.testing.expectEqual(@as(usize, 0), b_to_c.pending_third_party_awaits.count());

    // C's per-call redirect marker was consumed on the settle to B.
    try std.testing.expectEqual(@as(usize, 0), c_to_b.send_results_to_third_party.count());

    // A sent its Finish(answer_id) for the redirected results Return, per
    // rpc.capnp:924-926 ("the receiver ... must eventually send a Finish"). In a
    // real async transport that Finish clears C's answer-table entry for the
    // redirected results; in this synchronous loopback the Finish RE-ENTERS C
    // during the send, before the resolved-answers slot is committed
    // (reserve-send-commit ordering, see reserveResolvedAnswer), so C's frame
    // copy for `answer_id` is instead released at deinit. Both paths are
    // leak-free under std.testing.allocator — the drain assertion is the
    // teardown itself. C's active-inbound table (the live-call set) is empty:
    // the redirected answer id was never a live inbound question on C<->A.
    try std.testing.expect(!c_to_a.active_inbound_questions.contains(answer_id));
    try std.testing.expectEqual(@as(usize, 0), c_to_a.active_inbound_questions.count());

    // -- Teardown drains everything. ------------------------------------------
    // Nothing is parked until teardown any more: settling with
    // `resultsSentElsewhere` drains B's question at the point the Return
    // arrives.
    try std.testing.expectEqual(@as(usize, 0), b_to_c.pending_third_party_awaits.count());

    // Release B's Carol import so B<->C caps drain.
    try b_to_c.releaseImport(carol_import_id, 1);
    try std.testing.expect(!b_to_c.caps.hasImport(carol_import_id));

    // No third-party bookkeeping lingers on the delivery peers.
    try std.testing.expectEqual(@as(usize, 0), a_to_c.adopted_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 0), a_to_c.pending_third_party_returns.count());
}
