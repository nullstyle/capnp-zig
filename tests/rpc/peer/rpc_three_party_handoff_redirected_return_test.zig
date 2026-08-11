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
const peer_test_hooks = Peer.test_hooks;

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

/// Same application behavior as Carol, but intentionally knows nothing about
/// the recipient connection. Under `.vat_network`, the peer runtime performs
/// ThirdPartyAnswer, result delivery, capability remap, and source settlement.
const AutomaticCarol = struct {
    get_number_calls: u32 = 0,
    deinit_before_return: bool = false,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *AutomaticCarol = castCtx(*AutomaticCarol, ctx_ptr);
        if (call.interface_id != NUMBER_INTERFACE_ID or call.method_id != GET_NUMBER_METHOD_ID) {
            return error.UnexpectedMethod;
        }
        self.get_number_calls += 1;
        if (self.deinit_before_return) peer.deinit();
        var ret_ctx = NumberReturnCtx{ .n = 42 };
        try peer.sendReturnResults(call.question_id, &ret_ctx, NumberReturnCtx.build);
    }
};

const PING_INTERFACE_ID: u64 = 0xC0C0_C0C0_C0C0_C002;

const DeferredCapabilityCarol = struct {
    export_id: u32 = 0,
    pending_peer: ?*Peer = null,
    pending_answer_id: ?u32 = null,
    ping_calls: u32 = 0,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *DeferredCapabilityCarol = castCtx(*DeferredCapabilityCarol, ctx_ptr);
        if (call.interface_id == NUMBER_INTERFACE_ID and call.method_id == GET_NUMBER_METHOD_ID) {
            self.pending_peer = peer;
            self.pending_answer_id = call.question_id;
            return;
        }
        if (call.interface_id == PING_INTERFACE_ID and call.method_id == 0) {
            self.ping_calls += 1;
            var ret_ctx = NumberReturnCtx{ .n = 99 };
            try peer.sendReturnResults(call.question_id, &ret_ctx, NumberReturnCtx.build);
            return;
        }
        return error.UnexpectedMethod;
    }

    fn complete(self: *DeferredCapabilityCarol) !void {
        const BuildCtx = struct {
            export_id: u32,
            fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const ctx: *@This() = castCtx(*@This(), ctx_ptr);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                try any.setCapability(.{ .id = ctx.export_id });
            }
        };
        const peer = self.pending_peer orelse return error.NoPendingRedirect;
        const answer_id = self.pending_answer_id orelse return error.NoPendingRedirect;
        var build_ctx = BuildCtx{ .export_id = self.export_id };
        try peer.sendReturnResults(answer_id, &build_ctx, BuildCtx.build);
        self.pending_peer = null;
        self.pending_answer_id = null;
    }
};

const CapabilityResult = struct {
    import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *CapabilityResult = castCtx(*CapabilityResult, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedThirdPartyResultReturn;
        const payload = ret.results orelse return error.MissingThirdPartyResultPayload;
        const cap = try payload.content.getCapability();
        var mutable: *cap_table.InboundCapTable = @constCast(caps);
        const resolved = try mutable.resolveCapability(cap);
        try mutable.retainCapability(cap);
        self.import_id = switch (resolved) {
            .imported => |imported| imported.id,
            else => return error.ExpectedImportedProxy,
        };
    }
};

const NumberResult = struct {
    value: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *NumberResult = castCtx(*NumberResult, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedNumberReturn;
        self.value = try readNumberN(ret.results orelse return error.MissingNumberPayload);
    }
};

const TagResult = struct {
    tag: ?protocol.ReturnTag = null,
    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *TagResult = castCtx(*TagResult, ctx_ptr);
        self.tag = ret.tag;
    }
};

const PipelinedCallWithParamCap = struct {
    export_id: u32,
    observed_tag: ?protocol.ReturnTag = null,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *const PipelinedCallWithParamCap = castCtx(*const PipelinedCallWithParamCap, ctx_ptr);
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        try any.setCapability(.{ .id = self.export_id });
    }

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *PipelinedCallWithParamCap = castCtx(*PipelinedCallWithParamCap, ctx_ptr);
        self.observed_tag = ret.tag;
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
    callback_count: u32 = 0,
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
        self.callback_count += 1;
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
    finish_on_third_party_answer: bool = false,
    deinit_source_on_third_party_answer: bool = false,
    deinit_target_on_return: bool = false,
    fail_third_party_answer_before_forward: bool = false,
    fail_third_party_answer_after_forward: bool = false,
    fail_target_results_before_forward: bool = false,
    fail_target_results_after_forward: bool = false,
    fail_source_marker_after_forward: bool = false,
    rewrite_next_finish_release_result_caps: ?bool = null,

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
        if (self.b_to_c) |peer| {
            var fail_after_forward = false;
            if (self.fail_source_marker_after_forward) {
                var decoded = try protocol.DecodedMessage.init(peer.allocator, frame);
                defer decoded.deinit();
                if (decoded.tag == .@"return" and
                    (try decoded.asReturn()).tag == .resultsSentElsewhere)
                {
                    self.fail_source_marker_after_forward = false;
                    fail_after_forward = true;
                }
            }
            try peer.handleFrame(frame);
            if (fail_after_forward) return error.InjectedSourceMarkerSendFailure;
        }
    }

    fn cToASend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        if (self.a_to_c) |peer| {
            var fail_after_forward = false;
            if (self.fail_third_party_answer_before_forward or
                self.fail_third_party_answer_after_forward or
                self.fail_target_results_before_forward or
                self.fail_target_results_after_forward)
            {
                var decoded = try protocol.DecodedMessage.init(peer.allocator, frame);
                defer decoded.deinit();
                if (decoded.tag == .thirdPartyAnswer and
                    self.fail_third_party_answer_before_forward)
                {
                    self.fail_third_party_answer_before_forward = false;
                    return error.InjectedThirdPartyAnswerSendFailure;
                }
                if (decoded.tag == .thirdPartyAnswer and
                    self.fail_third_party_answer_after_forward)
                {
                    self.fail_third_party_answer_after_forward = false;
                    fail_after_forward = true;
                }
                if (decoded.tag == .@"return" and
                    self.fail_target_results_before_forward and
                    (try decoded.asReturn()).tag == .results)
                {
                    self.fail_target_results_before_forward = false;
                    return error.InjectedTargetResultSendFailure;
                }
                if (decoded.tag == .@"return" and
                    self.fail_target_results_after_forward and
                    (try decoded.asReturn()).tag == .results)
                {
                    self.fail_target_results_after_forward = false;
                    fail_after_forward = true;
                }
            }
            try peer.handleFrame(frame);
            if (fail_after_forward) return error.InjectedTrailingTargetResultSendFailure;
            if (self.finish_on_third_party_answer or
                self.deinit_source_on_third_party_answer or
                self.deinit_target_on_return)
            {
                var decoded = try protocol.DecodedMessage.init(peer.allocator, frame);
                defer decoded.deinit();
                if (decoded.tag == .thirdPartyAnswer) {
                    const answer = try decoded.asThirdPartyAnswer();
                    if (self.finish_on_third_party_answer) {
                        try peer.sendFinishForHost(answer.answer_id, false, false);
                    }
                    if (self.deinit_source_on_third_party_answer) {
                        if (self.c_to_b) |source| source.deinit();
                    }
                }
                if (decoded.tag == .@"return" and self.deinit_target_on_return) {
                    if (self.c_to_a) |target| target.deinit();
                }
            }
        }
    }

    fn aToCSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        if (self.c_to_a) |peer| {
            if (self.rewrite_next_finish_release_result_caps) |release_result_caps| {
                var decoded = try protocol.DecodedMessage.init(peer.allocator, frame);
                defer decoded.deinit();
                if (decoded.tag == .finish) {
                    const finish = try decoded.asFinish();
                    self.rewrite_next_finish_release_result_caps = null;
                    var builder = protocol.MessageBuilder.init(peer.allocator);
                    defer builder.deinit();
                    try builder.buildFinish(
                        finish.question_id,
                        release_result_caps,
                        finish.require_early_cancellation,
                    );
                    const rewritten = try builder.finish();
                    defer peer.allocator.free(rewritten);
                    try peer.handleFrame(rewritten);
                    return;
                }
            }
            try peer.handleFrame(frame);
        }
    }
};

const RouteCloseProbe = struct {
    other: ?*Peer = null,
    calls: u32 = 0,
    owned_routes: usize = std.math.maxInt(usize),
    incoming_routes: usize = std.math.maxInt(usize),
    other_incoming_routes: usize = std.math.maxInt(usize),

    fn onClose(ctx: ?*anyopaque, peer: *Peer) void {
        const self: *RouteCloseProbe = castCtx(*RouteCloseProbe, ctx orelse return);
        self.calls += 1;
        self.owned_routes = peer.automatic_third_party_routes.count();
        self.incoming_routes = peer.incoming_automatic_third_party_routes.count();
        if (self.other) |other| {
            self.other_incoming_routes = other.incoming_automatic_third_party_routes.count();
        }
    }
};

const SourceFinishDeinitProbe = struct {
    source: *Peer,
    target: *Peer,
    calls: u32 = 0,
    tag: ?protocol.ReturnTag = null,
    source_routes_before_callback: usize = std.math.maxInt(usize),
    source_payloads_before_callback: usize = std.math.maxInt(usize),
    target_routes_before_callback: usize = std.math.maxInt(usize),

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *SourceFinishDeinitProbe = castCtx(*SourceFinishDeinitProbe, ctx_ptr);
        self.calls += 1;
        self.tag = ret.tag;
        // Route ownership and the target backlink must both be detached before
        // the terminal callback is allowed to reenter either peer.
        self.source_routes_before_callback = self.source.automatic_third_party_routes.count();
        self.source_payloads_before_callback = self.source.send_results_to_third_party.count();
        self.target_routes_before_callback = self.target.incoming_automatic_third_party_routes.count();
        self.source.deinit();
    }
};

const AutomaticSendFailure = enum {
    third_party_answer,
    third_party_answer_after_forward,
    target_result,
    target_result_after_forward,
    source_marker_after_forward,
};

fn runAutomaticSendFailure(kind: AutomaticSendFailure) !void {
    const allocator = std.testing.allocator;
    var link = Link{};
    switch (kind) {
        .third_party_answer => link.fail_third_party_answer_before_forward = true,
        .third_party_answer_after_forward => link.fail_third_party_answer_after_forward = true,
        .target_result => link.fail_target_results_before_forward = true,
        .target_result_after_forward => link.fail_target_results_after_forward = true,
        .source_marker_after_forward => link.fail_source_marker_after_forward = true,
    }
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-send-failure";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);
    var carol = AutomaticCarol{};
    _ = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = AutomaticCarol.onCall });
    var probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&probe, CarolImportProbe.onReturn);
    const import_id = probe.carol_import_id orelse return error.CarolBootstrapFailed;
    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();
    var recipient = TagResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &recipient, TagResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };

    if (kind == .source_marker_after_forward) {
        var saw_marker_error = false;
        _ = b_to_c.sendCall(
            import_id,
            NUMBER_INTERFACE_ID,
            GET_NUMBER_METHOD_ID,
            &redirected,
            RedirectedCall.buildCall,
            RedirectedCall.onReturn,
        ) catch |err| {
            try std.testing.expectEqual(error.AutomaticThirdPartySourceSettlementFailed, err);
            saw_marker_error = true;
        };
        try std.testing.expect(saw_marker_error);
    } else {
        _ = try b_to_c.sendCall(
            import_id,
            NUMBER_INTERFACE_ID,
            GET_NUMBER_METHOD_ID,
            &redirected,
            RedirectedCall.buildCall,
            RedirectedCall.onReturn,
        );
    }

    if (kind == .third_party_answer_after_forward) {
        // The sender cannot prove whether the announcement was consumed. Its
        // rollback is locally complete, while transport closure is what makes
        // the recipient's already-adopted await terminal.
        try std.testing.expectEqual(@as(?protocol.ReturnTag, null), recipient.tag);
        try std.testing.expectEqual(@as(usize, 0), c_to_b.automatic_third_party_routes.count());
        try std.testing.expectEqual(@as(usize, 0), c_to_a.incoming_automatic_third_party_routes.count());
        try std.testing.expectEqual(@as(usize, 0), c_to_a.active_inbound_questions.count());
        c_to_a.notifyTransportClosed();
        a_to_c.notifyTransportClosed();
    }

    try std.testing.expectEqual(@as(usize, 0), c_to_b.automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_a.incoming_automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_b.send_results_to_third_party.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_a.active_inbound_questions.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_b.cross_peer_proxy_links.items.len);
    try std.testing.expectEqual(@as(usize, 0), c_to_a.cross_peer_proxy_links.items.len);
    try std.testing.expectEqual(@as(usize, 0), a_to_c.pending_third_party_returns.count());
    try std.testing.expectEqual(@as(u32, 1), redirected.callback_count);
    if (kind == .target_result_after_forward) {
        try std.testing.expect(!link.fail_target_results_after_forward);
    }
    switch (kind) {
        .third_party_answer => {
            try std.testing.expectEqual(@as(u32, 0), carol.get_number_calls);
            try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), redirected.observed_tag);
            try std.testing.expectEqual(@as(?protocol.ReturnTag, null), recipient.tag);
        },
        .third_party_answer_after_forward => {
            try std.testing.expectEqual(@as(u32, 0), carol.get_number_calls);
            try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), redirected.observed_tag);
            try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), recipient.tag);
        },
        .target_result => {
            try std.testing.expectEqual(@as(u32, 1), carol.get_number_calls);
            try std.testing.expectEqual(@as(?protocol.ReturnTag, .resultsSentElsewhere), redirected.observed_tag);
            try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), recipient.tag);
        },
        .target_result_after_forward => {
            try std.testing.expectEqual(@as(u32, 1), carol.get_number_calls);
            try std.testing.expectEqual(@as(?protocol.ReturnTag, .resultsSentElsewhere), redirected.observed_tag);
            try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), recipient.tag);
        },
        .source_marker_after_forward => {
            try std.testing.expectEqual(@as(u32, 1), carol.get_number_calls);
            try std.testing.expectEqual(@as(?protocol.ReturnTag, .resultsSentElsewhere), redirected.observed_tag);
            try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), recipient.tag);
        },
    }
    try b_to_c.releaseImport(import_id, 1);
}

fn noopAutomaticFrame(_: *anyopaque, _: []const u8) anyerror!void {}

fn automaticCapabilityRedirectImpl(
    source_allocator: std.mem.Allocator,
    target_allocator: std.mem.Allocator,
    network_allocator: std.mem.Allocator,
) !void {
    var source = Peer.initDetached(source_allocator);
    source.disableThreadAffinity();
    var target = Peer.initDetached(target_allocator);
    target.disableThreadAffinity();
    var sink: u8 = 0;
    source.setSendFrameOverride(&sink, noopAutomaticFrame);
    target.setSendFrameOverride(&sink, noopAutomaticFrame);
    var export_id: ?u32 = null;
    defer {
        // Terminal-close cleanup is part of the invariant under test: every
        // partial admission/result state must become locally detached without
        // requiring either peer to have been deinited first.
        source.notifyTransportClosed();
        target.notifyTransportClosed();
        std.debug.assert(source.automatic_third_party_routes.count() == 0);
        std.debug.assert(target.incoming_automatic_third_party_routes.count() == 0);
        std.debug.assert(target.active_inbound_questions.count() == 0);
        target.deinit();
        // Destroying any committed target proxy releases its source handoff
        // pin and backlink. Failed remaps must have rolled both back already.
        std.debug.assert(source.cross_peer_proxy_links.items.len == 0);
        if (export_id) |id| {
            const entry = source.exports.getEntry(id) orelse @panic("OOM cleanup lost app export");
            std.debug.assert(entry.value_ptr.handoff_ref_count == 0);
        }
        source.deinit();
    }

    const NoopCap = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    var cap_ctx: u8 = 0;
    const local_export = try source.addExport(.{ .ctx = &cap_ctx, .on_call = NoopCap.onCall });
    export_id = local_export;

    const nonce = "automatic-oom-capability";
    var net = vat_network.LoopbackVatNetwork(Peer).init(network_allocator);
    defer net.deinit();
    try net.register(nonce, &target);
    source.attachVatNetwork(net.network());
    source.setThirdPartyResultPolicy(.vat_network);

    const token = try vat_network.encodeNonceToken(source_allocator, nonce);
    defer source_allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(source_allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();
    try peer_test_hooks.noteSendResultsToThirdParty(&source, 77, completion);

    const BuildCtx = struct {
        export_id: u32,
        fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            const self: *@This() = castCtx(*@This(), ctx_ptr);
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            try any.setCapability(.{ .id = self.export_id });
        }
    };
    var build_ctx = BuildCtx{ .export_id = local_export };
    try source.sendReturnResults(77, &build_ctx, BuildCtx.build);
}

fn automaticCapabilityRedirectOomImpl(allocator: std.mem.Allocator) !void {
    return automaticCapabilityRedirectImpl(allocator, allocator, allocator);
}

fn checkAutomaticCapabilityRedirectAllocationFailures() !void {
    const allocation_count = blk: {
        var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
        var failing = std.testing.FailingAllocator.init(debug_allocator.allocator(), .{});
        try automaticCapabilityRedirectOomImpl(failing.allocator());
        const count = failing.alloc_index;
        if (debug_allocator.deinit() != .ok) return error.MemoryLeakDetected;
        break :blk count;
    };

    for (0..allocation_count) |fail_index| {
        var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
        var failing = std.testing.FailingAllocator.init(
            debug_allocator.allocator(),
            .{ .fail_index = fail_index },
        );
        automaticCapabilityRedirectOomImpl(failing.allocator()) catch |err| {
            if (err != error.OutOfMemory and
                err != error.AutomaticThirdPartySourceSettlementFailed)
            {
                return err;
            }
        };
        if (!failing.has_induced_failure) return error.NondeterministicMemoryUsage;
        if (debug_allocator.deinit() != .ok) return error.MemoryLeakDetected;
    }
}

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

test "automatic vat-network redirected return delivers once and drains both route indexes" {
    const allocator = std.testing.allocator;

    var link = Link{};
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-redirect-nonce";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);

    var carol = AutomaticCarol{};
    _ = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = AutomaticCarol.onCall });
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;

    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();

    var third_party_result = ThirdPartyResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &third_party_result, ThirdPartyResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    _ = try b_to_c.sendCall(
        carol_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );

    try std.testing.expectEqual(@as(u32, 1), carol.get_number_calls);
    try std.testing.expectEqual(@as(u32, 42), third_party_result.result orelse
        return error.ThirdPartyResultNotDelivered);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .resultsSentElsewhere), redirected.observed_tag);
    try std.testing.expectEqual(@as(usize, 0), c_to_b.automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_a.incoming_automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_b.send_results_to_third_party.count());
    try b_to_c.releaseImport(carol_import_id, 1);
}

test "automatic redirect rolls back when ThirdPartyAnswer cannot be sent" {
    try runAutomaticSendFailure(.third_party_answer);
}

test "ambiguous ThirdPartyAnswer send failure drains the recipient on transport close" {
    try runAutomaticSendFailure(.third_party_answer_after_forward);
}

test "automatic redirect replaces a failed target result send with one target exception" {
    try runAutomaticSendFailure(.target_result);
}

test "automatic redirect commits a target result consumed before a trailing send error" {
    try runAutomaticSendFailure(.target_result_after_forward);
}

test "automatic redirect never double-returns when source marker reports failure after delivery" {
    try runAutomaticSendFailure(.source_marker_after_forward);
}

test "automatic capability redirect drains routes, synthetic answers, proxies, and pins under OOM" {
    try checkAutomaticCapabilityRedirectAllocationFailures();
}

test "automatic capability redirect supports distinct source, target, and network allocators" {
    var source_debug: std.heap.DebugAllocator(.{}) = .init;
    var target_debug: std.heap.DebugAllocator(.{}) = .init;
    var network_debug: std.heap.DebugAllocator(.{}) = .init;
    try automaticCapabilityRedirectImpl(
        source_debug.allocator(),
        target_debug.allocator(),
        network_debug.allocator(),
    );
    try std.testing.expectEqual(.ok, network_debug.deinit());
    try std.testing.expectEqual(.ok, target_debug.deinit());
    try std.testing.expectEqual(.ok, source_debug.deinit());
}

test "automatic redirected capability remaps through a pinned proxy and replays recipient pipelining" {
    const allocator = std.testing.allocator;
    var link = Link{ .fail_target_results_after_forward = true };
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-capability-redirect";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);

    var carol = DeferredCapabilityCarol{};
    carol.export_id = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = DeferredCapabilityCarol.onCall });
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;

    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();

    var cap_result = CapabilityResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &cap_result, CapabilityResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    const source_answer_id = try b_to_c.sendCall(
        carol_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );
    _ = source_answer_id;

    const route = c_to_b.automatic_third_party_routes.get(carol.pending_answer_id orelse
        return error.NoPendingRedirect) orelse return error.NoAutomaticRoute;
    const target_answer_id = route.target_answer_id;
    try std.testing.expect(c_to_a.active_inbound_questions.contains(target_answer_id));

    var pipelined = NumberResult{};
    _ = try a_to_c.sendCallPromisedWithOps(
        target_answer_id,
        &.{},
        PING_INTERFACE_ID,
        0,
        &pipelined,
        null,
        NumberResult.onReturn,
    );
    try std.testing.expect(c_to_a.pending_promises.contains(target_answer_id));

    try carol.complete();
    try std.testing.expect(!link.fail_target_results_after_forward);
    try std.testing.expectEqual(@as(u32, 99), pipelined.value orelse return error.PipelineDidNotReplay);
    try std.testing.expectEqual(@as(u32, 1), carol.ping_calls);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .resultsSentElsewhere), redirected.observed_tag);
    try std.testing.expectEqual(@as(usize, 0), c_to_a.pending_promises.count());

    // The returned import names a proxy export on C<->A. Calling it directly
    // reaches Carol on C<->B; releasing it drains the proxy and its source pin.
    const direct_import = cap_result.import_id orelse return error.MissingRedirectedCapability;
    var direct = NumberResult{};
    _ = try a_to_c.sendCall(direct_import, PING_INTERFACE_ID, 0, &direct, null, NumberResult.onReturn);
    try std.testing.expectEqual(@as(u32, 99), direct.value orelse return error.ProxyCallDidNotReturn);
    try std.testing.expectEqual(@as(u32, 2), carol.ping_calls);
    try a_to_c.releaseImport(direct_import, 1);

    try std.testing.expectEqual(@as(usize, 0), c_to_b.automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_a.incoming_automatic_third_party_routes.count());
    try b_to_c.releaseImport(carol_import_id, 1);
}

test "trailing target send error with Finish releaseResultCaps drains the proxy and source pin" {
    const allocator = std.testing.allocator;
    var link = Link{
        .fail_target_results_after_forward = true,
        .rewrite_next_finish_release_result_caps = true,
    };
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-capability-trailing-finish-release";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);

    var carol = DeferredCapabilityCarol{};
    carol.export_id = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = DeferredCapabilityCarol.onCall });
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;

    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();

    var cap_result = CapabilityResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &cap_result, CapabilityResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    _ = try b_to_c.sendCall(
        carol_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );

    try carol.complete();

    try std.testing.expect(!link.fail_target_results_after_forward);
    try std.testing.expect(link.rewrite_next_finish_release_result_caps == null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .resultsSentElsewhere), redirected.observed_tag);
    try std.testing.expectEqual(@as(usize, 0), c_to_b.automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_a.incoming_automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_a.cross_peer_proxy_links.items.len);
    try std.testing.expectEqual(@as(usize, 0), a_to_c.pending_third_party_returns.count());
    const source_export = c_to_b.exports.get(carol.export_id) orelse return error.MissingSourceExport;
    try std.testing.expectEqual(@as(u32, 0), source_export.handoff_ref_count);

    // The callback retained the imported id only so the test could observe it;
    // releaseResultCaps=true transferred no continuing wire ownership. Forget
    // the local test handle without emitting a duplicate Release.
    const stale_import = cap_result.import_id orelse return error.MissingRedirectedCapability;
    try a_to_c.forgetImportRefsForHost(stale_import, 1);
    try std.testing.expect(!a_to_c.caps.imports.contains(stale_import));
    try b_to_c.releaseImport(carol_import_id, 1);
}

test "automatic redirect honors recipient Finish before results without stale synthetic state" {
    const allocator = std.testing.allocator;
    var link = Link{ .finish_on_third_party_answer = true };
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-early-finish";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);
    var carol = AutomaticCarol{};
    _ = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = AutomaticCarol.onCall });
    var probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&probe, CarolImportProbe.onReturn);
    const import_id = probe.carol_import_id orelse return error.CarolBootstrapFailed;

    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();
    var recipient = ThirdPartyResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &recipient, ThirdPartyResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    _ = try b_to_c.sendCall(
        import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );

    try std.testing.expectEqual(@as(?u32, null), recipient.result);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .resultsSentElsewhere), redirected.observed_tag);
    try std.testing.expectEqual(@as(usize, 0), c_to_a.active_inbound_questions.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_a.finished_early_answers.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_a.incoming_automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_b.automatic_third_party_routes.count());
    try b_to_c.releaseImport(import_id, 1);
}

test "recipient Finish after pipelining drains the synthetic child and its retained param cap" {
    const allocator = std.testing.allocator;
    var link = Link{};
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-finish-after-pipeline";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);

    var carol = DeferredCapabilityCarol{};
    carol.export_id = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = DeferredCapabilityCarol.onCall });
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;

    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();

    var recipient = TagResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &recipient, TagResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    _ = try b_to_c.sendCall(
        carol_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );

    const source_answer_id = carol.pending_answer_id orelse return error.NoPendingRedirect;
    const route = c_to_b.automatic_third_party_routes.get(source_answer_id) orelse
        return error.NoAutomaticRoute;
    const target_answer_id = route.target_answer_id;

    const NoopCap = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    var cap_ctx: u8 = 0;
    const param_export_id = try a_to_c.addExport(.{ .ctx = &cap_ctx, .on_call = NoopCap.onCall });
    var pipelined = PipelinedCallWithParamCap{ .export_id = param_export_id };
    _ = try a_to_c.sendCallPromisedWithOps(
        target_answer_id,
        &.{},
        PING_INTERFACE_ID,
        0,
        &pipelined,
        PipelinedCallWithParamCap.build,
        PipelinedCallWithParamCap.onReturn,
    );
    try std.testing.expect(c_to_a.pending_promises.contains(target_answer_id));
    try std.testing.expect(c_to_a.caps.imports.contains(param_export_id));
    try std.testing.expectEqual(@as(u32, 1), (a_to_c.exports.get(param_export_id) orelse return error.MissingParamExport).ref_count);

    try a_to_c.sendFinishForHost(target_answer_id, false, false);

    try std.testing.expect(!c_to_a.pending_promises.contains(target_answer_id));
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), pipelined.observed_tag);
    try std.testing.expect(!c_to_a.caps.imports.contains(param_export_id));
    try std.testing.expect(a_to_c.exports.get(param_export_id) == null);
    try std.testing.expect(!c_to_a.active_inbound_questions.contains(target_answer_id));
    try std.testing.expect(!c_to_a.incoming_automatic_third_party_routes.contains(target_answer_id));

    try carol.complete();
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .resultsSentElsewhere), redirected.observed_tag);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), recipient.tag);
    try std.testing.expectEqual(@as(usize, 0), c_to_b.automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_b.send_results_to_third_party.count());
    try b_to_c.releaseImport(carol_import_id, 1);
}

test "vat-network redirect without an attached network fails before handler dispatch" {
    const allocator = std.testing.allocator;
    var link = Link{};
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_b.setThirdPartyResultPolicy(.vat_network);

    var carol = AutomaticCarol{};
    _ = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = AutomaticCarol.onCall });
    var probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&probe, CarolImportProbe.onReturn);
    const import_id = probe.carol_import_id orelse return error.CarolBootstrapFailed;
    const token = try vat_network.encodeNonceToken(allocator, "missing-network");
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    var redirected = RedirectedCall{ .recipient = try token_msg.getRootAnyPointer() };
    _ = try b_to_c.sendCall(
        import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );

    try std.testing.expectEqual(@as(u32, 0), carol.get_number_calls);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), redirected.observed_tag);
    try std.testing.expectEqual(@as(usize, 0), c_to_b.automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_b.send_results_to_third_party.count());
    try b_to_c.releaseImport(import_id, 1);
}

test "source Finish drains both route maps before a target exception callback deinits the source" {
    const allocator = std.testing.allocator;
    var link = Link{};
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-source-finish-callback-deinit";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);

    var carol = DeferredCapabilityCarol{};
    carol.export_id = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = DeferredCapabilityCarol.onCall });
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;

    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();

    var recipient = SourceFinishDeinitProbe{ .source = &c_to_b, .target = &c_to_a };
    try a_to_c.registerPendingThirdPartyAwait(completion, &recipient, SourceFinishDeinitProbe.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    const source_answer_id = try b_to_c.sendCall(
        carol_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );
    const route = c_to_b.automatic_third_party_routes.get(source_answer_id) orelse
        return error.NoAutomaticRoute;
    const target_answer_id = route.target_answer_id;
    try std.testing.expectEqual(@as(usize, 1), c_to_b.automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 1), c_to_a.incoming_automatic_third_party_routes.count());

    try b_to_c.sendFinishForHost(source_answer_id, false, false);

    try std.testing.expectEqual(@as(u32, 1), recipient.calls);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), recipient.tag);
    try std.testing.expectEqual(@as(usize, 0), recipient.source_routes_before_callback);
    try std.testing.expectEqual(@as(usize, 0), recipient.source_payloads_before_callback);
    try std.testing.expectEqual(@as(usize, 0), recipient.target_routes_before_callback);
    try std.testing.expect(c_to_b.in_deinit);
    try std.testing.expect(!c_to_b.automatic_third_party_deinit_deferred);
    try std.testing.expectEqual(@as(u32, 0), c_to_b.automatic_third_party_operation_depth);
    try std.testing.expectEqual(@as(usize, 0), c_to_a.incoming_automatic_third_party_routes.count());
    try std.testing.expect(!c_to_a.active_inbound_questions.contains(target_answer_id));
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), redirected.observed_tag);
    // Source teardown disconnects the bootstrap import rather than receiving a
    // normal Return; stop forwarding before the remaining local peers deinit.
    link.forwarding = false;
}

test "source deinit reentrant from ThirdPartyAnswer send defers safely and completes teardown" {
    const allocator = std.testing.allocator;
    var link = Link{ .deinit_source_on_third_party_answer = true };
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-source-reentrant-deinit";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);
    var carol = AutomaticCarol{};
    _ = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = AutomaticCarol.onCall });
    var probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&probe, CarolImportProbe.onReturn);
    const import_id = probe.carol_import_id orelse return error.CarolBootstrapFailed;
    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();
    var recipient = TagResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &recipient, TagResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    _ = try b_to_c.sendCall(
        import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );

    try std.testing.expect(c_to_b.in_deinit);
    try std.testing.expect(!c_to_b.automatic_third_party_deinit_deferred);
    try std.testing.expectEqual(@as(u32, 0), c_to_b.automatic_third_party_dispatch_depth);
    try std.testing.expectEqual(@as(u32, 0), c_to_b.automatic_third_party_operation_depth);
    try std.testing.expectEqual(@as(u32, 0), carol.get_number_calls);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), redirected.observed_tag);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), recipient.tag);
    // The source peer is fully torn down, so its bootstrap import on B is now
    // disconnected rather than explicitly releasable.
    link.forwarding = false;
}

test "target deinit reentrant from redirected Return send defers until route commit" {
    const allocator = std.testing.allocator;
    var link = Link{ .deinit_target_on_return = true };
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-target-reentrant-deinit";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);
    var carol = AutomaticCarol{};
    _ = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = AutomaticCarol.onCall });
    var probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&probe, CarolImportProbe.onReturn);
    const import_id = probe.carol_import_id orelse return error.CarolBootstrapFailed;
    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();
    var recipient = ThirdPartyResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &recipient, ThirdPartyResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    _ = try b_to_c.sendCall(
        import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );

    try std.testing.expect(c_to_a.in_deinit);
    try std.testing.expect(!c_to_a.automatic_third_party_deinit_deferred);
    try std.testing.expectEqual(@as(u32, 0), c_to_a.automatic_third_party_operation_depth);
    try std.testing.expectEqual(@as(u32, 42), recipient.result orelse return error.ThirdPartyResultNotDelivered);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .resultsSentElsewhere), redirected.observed_tag);
    try std.testing.expectEqual(@as(usize, 0), c_to_b.automatic_third_party_routes.count());
    // The target connection has already completed teardown; stop the synthetic
    // transport before releasing the source-side bootstrap import.
    link.forwarding = false;
}

test "source deinit from automatic call handler waits for terminal redirect delivery" {
    const allocator = std.testing.allocator;
    var link = Link{};
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-handler-deinit";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);
    var carol = AutomaticCarol{ .deinit_before_return = true };
    _ = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = AutomaticCarol.onCall });
    var probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&probe, CarolImportProbe.onReturn);
    const import_id = probe.carol_import_id orelse return error.CarolBootstrapFailed;
    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();
    var recipient = ThirdPartyResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &recipient, ThirdPartyResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    _ = try b_to_c.sendCall(
        import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );

    try std.testing.expect(c_to_b.in_deinit);
    try std.testing.expectEqual(@as(u32, 0), c_to_b.automatic_third_party_dispatch_depth);
    try std.testing.expectEqual(@as(u32, 0), c_to_b.automatic_third_party_operation_depth);
    try std.testing.expectEqual(@as(u32, 42), recipient.result orelse return error.ThirdPartyResultNotDelivered);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .resultsSentElsewhere), redirected.observed_tag);
    try std.testing.expectEqual(@as(usize, 0), c_to_a.incoming_automatic_third_party_routes.count());
    link.forwarding = false;
}

test "source transport close without deinit retires routes before the close callback" {
    const allocator = std.testing.allocator;
    var link = Link{};
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-source-close";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);
    var carol = DeferredCapabilityCarol{};
    carol.export_id = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = DeferredCapabilityCarol.onCall });
    var probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&probe, CarolImportProbe.onReturn);
    const import_id = probe.carol_import_id orelse return error.CarolBootstrapFailed;
    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();
    var recipient = TagResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &recipient, TagResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    _ = try b_to_c.sendCall(
        import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );
    try std.testing.expectEqual(@as(usize, 1), c_to_b.automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 1), c_to_a.incoming_automatic_third_party_routes.count());

    var close_probe = RouteCloseProbe{ .other = &c_to_a };
    c_to_b.start(&close_probe, null, RouteCloseProbe.onClose);
    c_to_b.notifyTransportClosed();
    c_to_b.notifyTransportClosed();

    try std.testing.expect(c_to_b.transport_close_notified);
    try std.testing.expect(!c_to_b.in_deinit);
    try std.testing.expectEqual(@as(u32, 1), close_probe.calls);
    try std.testing.expectEqual(@as(usize, 0), close_probe.owned_routes);
    try std.testing.expectEqual(@as(usize, 0), close_probe.other_incoming_routes);
    try std.testing.expectEqual(@as(usize, 0), c_to_b.send_results_to_third_party.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_a.active_inbound_questions.count());
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), recipient.tag);
    link.forwarding = false;
}

test "target transport close without deinit detaches its synthetic answer and source completes disconnected" {
    const allocator = std.testing.allocator;
    var link = Link{};
    defer link.forwarding = false;
    var c_to_b = Peer.initDetached(allocator);
    c_to_b.disableThreadAffinity();
    defer c_to_b.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    link.b_to_c = &b_to_c;
    link.c_to_b = &c_to_b;
    link.c_to_a = &c_to_a;
    link.a_to_c = &a_to_c;
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    c_to_b.setSendFrameOverride(&link, Link.cToBSend);
    c_to_a.setSendFrameOverride(&link, Link.cToASend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);

    const nonce = "automatic-target-close";
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    try net.register(nonce, &c_to_a);
    c_to_b.attachVatNetwork(net.network());
    c_to_b.setThirdPartyResultPolicy(.vat_network);
    var carol = DeferredCapabilityCarol{};
    carol.export_id = try c_to_b.setBootstrap(.{ .ctx = &carol, .on_call = DeferredCapabilityCarol.onCall });
    var probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&probe, CarolImportProbe.onReturn);
    const import_id = probe.carol_import_id orelse return error.CarolBootstrapFailed;
    const token = try vat_network.encodeNonceToken(allocator, nonce);
    defer allocator.free(token);
    var token_msg = try message.Message.initUnvalidated(allocator, token);
    defer token_msg.deinit();
    const completion = try token_msg.getRootAnyPointer();
    var recipient = TagResult{};
    try a_to_c.registerPendingThirdPartyAwait(completion, &recipient, TagResult.onReturn);
    var redirected = RedirectedCall{ .recipient = completion };
    _ = try b_to_c.sendCall(
        import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &redirected,
        RedirectedCall.buildCall,
        RedirectedCall.onReturn,
    );
    const source_answer_id = carol.pending_answer_id orelse return error.NoPendingRedirect;
    const target_answer_id = (c_to_b.automatic_third_party_routes.get(source_answer_id) orelse
        return error.NoAutomaticRoute).target_answer_id;

    var close_probe = RouteCloseProbe{};
    c_to_a.start(&close_probe, null, RouteCloseProbe.onClose);
    c_to_a.notifyTransportClosed();
    c_to_a.notifyTransportClosed();

    try std.testing.expect(c_to_a.transport_close_notified);
    try std.testing.expect(!c_to_a.in_deinit);
    try std.testing.expectEqual(@as(u32, 1), close_probe.calls);
    try std.testing.expectEqual(@as(usize, 0), close_probe.incoming_routes);
    try std.testing.expect(!c_to_a.active_inbound_questions.contains(target_answer_id));
    try std.testing.expectEqual(@as(usize, 1), c_to_b.automatic_third_party_routes.count());
    try carol.complete();
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), redirected.observed_tag);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), recipient.tag);
    try std.testing.expectEqual(@as(usize, 0), c_to_b.automatic_third_party_routes.count());
    try std.testing.expectEqual(@as(usize, 0), c_to_b.send_results_to_third_party.count());
    link.forwarding = false;
}

test "retained awaitFromThirdParty finishes the intermediate and adopted wire answers" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8) = .empty,

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const self: *@This() = castCtx(*@This(), ctx_ptr);
            try self.frames.append(self.allocator, try self.allocator.dupe(u8, frame));
        }

        fn deinit(self: *@This()) void {
            for (self.frames.items) |frame| self.allocator.free(frame);
            self.frames.deinit(self.allocator);
        }
    };
    const Result = struct {
        returned: bool = false,
        answer_id: ?u32 = null,

        fn onReturn(
            ctx_ptr: *anyopaque,
            _: *Peer,
            ret: protocol.Return,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const self: *@This() = castCtx(*@This(), ctx_ptr);
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            self.returned = true;
            self.answer_id = ret.answer_id;
        }
    };

    var capture = Capture{ .allocator = allocator };
    defer capture.deinit();
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var result = Result{};
    const logical_question_id = try peer.sendCallWithOptions(
        7,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &result,
        null,
        Result.onReturn,
        .{ .result_lifetime = .retained },
    );
    const adopted_answer_id: u32 = 0x4000_0077;

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("retained-redirect-completion");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_message = try message.Message.init(allocator, completion_bytes, .{});
    defer completion_message.deinit();
    const completion = try completion_message.getRootAnyPointer();

    var await_builder = protocol.MessageBuilder.init(allocator);
    defer await_builder.deinit();
    var await_ret = try await_builder.beginReturn(logical_question_id, .awaitFromThirdParty);
    try await_ret.setAcceptFromThirdParty(completion);
    const await_frame = try await_builder.finish();
    defer allocator.free(await_frame);
    try peer.handleFrame(await_frame);

    // Retention applies to the adopted answer, not the forwarding-only answer
    // named by the original Call. The latter is Finished immediately.
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);
    var intermediate_finish = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer intermediate_finish.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, intermediate_finish.tag);
    try std.testing.expectEqual(logical_question_id, (try intermediate_finish.asFinish()).question_id);

    var answer_builder = protocol.MessageBuilder.init(allocator);
    defer answer_builder.deinit();
    try answer_builder.buildThirdPartyAnswer(adopted_answer_id, completion);
    const answer_frame = try answer_builder.finish();
    defer allocator.free(answer_frame);
    try peer.handleFrame(answer_frame);

    var final_builder = protocol.MessageBuilder.init(allocator);
    defer final_builder.deinit();
    var final_ret = try final_builder.beginReturn(adopted_answer_id, .exception);
    try final_ret.setException("redirect complete");
    const final_frame = try final_builder.finish();
    defer allocator.free(final_frame);
    try peer.handleFrame(final_frame);

    try std.testing.expect(result.returned);
    try std.testing.expectEqual(@as(?u32, logical_question_id), result.answer_id);
    try std.testing.expectEqual(@as(usize, 1), peer.stats().retained_questions);
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    // Force the allocator directly onto the adopted wire id while that answer
    // is retained but no longer present in `questions`. It must reserve both
    // the logical and adopted identities and choose the next free id.
    peer.next_question_id = adopted_answer_id;
    var other_result = Result{};
    const other_question_id = try peer.sendCall(
        8,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &other_result,
        null,
        Result.onReturn,
    );
    try std.testing.expectEqual(adopted_answer_id + 1, other_question_id);
    try std.testing.expect(other_question_id != logical_question_id);

    var other_return_builder = protocol.MessageBuilder.init(allocator);
    defer other_return_builder.deinit();
    var other_return = try other_return_builder.beginReturn(other_question_id, .exception);
    other_return.setNoFinishNeeded(true);
    try other_return.setException("unrelated call complete");
    const other_return_frame = try other_return_builder.finish();
    defer allocator.free(other_return_frame);
    try peer.handleFrame(other_return_frame);
    try std.testing.expect(other_result.returned);

    try peer.finishRetainedQuestion(logical_question_id, false);
    try std.testing.expectEqual(@as(usize, 0), peer.stats().retained_questions);
    try std.testing.expectEqual(@as(usize, 4), capture.frames.items.len);
    var adopted_finish = try protocol.DecodedMessage.init(allocator, capture.frames.items[3]);
    defer adopted_finish.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, adopted_finish.tag);
    try std.testing.expectEqual(adopted_answer_id, (try adopted_finish.asFinish()).question_id);
}

test "retained redirected callback error consumes adopted alias and remains explicitly finishable" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8) = .empty,

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const self: *@This() = castCtx(*@This(), ctx_ptr);
            try self.frames.append(self.allocator, try self.allocator.dupe(u8, frame));
        }

        fn deinit(self: *@This()) void {
            for (self.frames.items) |frame| self.allocator.free(frame);
            self.frames.deinit(self.allocator);
        }
    };
    const FailingResult = struct {
        calls: usize = 0,
        logical_answer_id: ?u32 = null,

        fn onReturn(
            ctx_ptr: *anyopaque,
            _: *Peer,
            ret: protocol.Return,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const self: *@This() = castCtx(*@This(), ctx_ptr);
            self.calls += 1;
            self.logical_answer_id = ret.answer_id;
            return error.IntentionalRetainedCallbackFailure;
        }
    };

    var capture = Capture{ .allocator = allocator };
    defer capture.deinit();
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var result = FailingResult{};
    const logical_question_id = try peer.sendCallWithOptions(
        9,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &result,
        null,
        FailingResult.onReturn,
        .{ .result_lifetime = .retained },
    );
    const adopted_answer_id: u32 = 0x4000_0079;

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("retained-redirect-error");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_message = try message.Message.init(allocator, completion_bytes, .{});
    defer completion_message.deinit();
    const completion = try completion_message.getRootAnyPointer();

    var await_builder = protocol.MessageBuilder.init(allocator);
    defer await_builder.deinit();
    var await_ret = try await_builder.beginReturn(logical_question_id, .awaitFromThirdParty);
    try await_ret.setAcceptFromThirdParty(completion);
    const await_frame = try await_builder.finish();
    defer allocator.free(await_frame);
    try peer.handleFrame(await_frame);

    var answer_builder = protocol.MessageBuilder.init(allocator);
    defer answer_builder.deinit();
    try answer_builder.buildThirdPartyAnswer(adopted_answer_id, completion);
    const answer_frame = try answer_builder.finish();
    defer allocator.free(answer_frame);
    try peer.handleFrame(answer_frame);

    var final_builder = protocol.MessageBuilder.init(allocator);
    defer final_builder.deinit();
    var final_ret = try final_builder.beginReturn(adopted_answer_id, .exception);
    try final_ret.setException("callback rejects result");
    const final_frame = try final_builder.finish();
    defer allocator.free(final_frame);
    // Public frame dispatch reports callback failures through the peer's
    // non-fatal error path; the wire handler itself remains usable.
    try peer.handleFrame(final_frame);

    try std.testing.expectEqual(@as(usize, 1), result.calls);
    try std.testing.expectEqual(@as(?u32, logical_question_id), result.logical_answer_id);
    try std.testing.expect(!peer.questions.contains(adopted_answer_id));
    try std.testing.expectEqual(@as(usize, 0), peer.adopted_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 1), peer.stats().retained_questions);

    try peer.finishRetainedQuestion(logical_question_id, false);
    try std.testing.expectEqual(@as(usize, 0), peer.stats().retained_questions);
    try std.testing.expectEqual(@as(usize, 3), capture.frames.items.len);
    var adopted_finish = try protocol.DecodedMessage.init(allocator, capture.frames.items[2]);
    defer adopted_finish.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, adopted_finish.tag);
    try std.testing.expectEqual(adopted_answer_id, (try adopted_finish.asFinish()).question_id);
}
