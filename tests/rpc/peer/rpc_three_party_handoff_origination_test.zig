//! Proof test for Cap'n Proto RPC Level-3 three-party handoff ORIGINATION —
//! the `Provide` + `Accept` slice (Phases P0+P1+P2 of
//! docs/rpc-l3-origination-plan.md). No embargo/disembargo (P4), no
//! resolve-auto-trigger (P3): the test drives Provide then Accept explicitly.
//!
//! Three vats — A (recipient), B (introducer), C (host of the provided cap) —
//! are wired over a synchronous in-process loopback. Because a `Peer` binds to
//! exactly one remote, each vat owns one `Peer` endpoint PER connection:
//!
//!   Vat A:  a_to_b  (A<->B)   a_to_c  (A<->C)
//!   Vat B:  b_to_a  (A<->B)   b_to_c  (B<->C)
//!   Vat C:  c       (single peer; receives B's Provide AND A's Accept — the
//!                    provide-state lookup and the accepted-cap Return share one
//!                    cap table, so Carol's export id is consistent across both)
//!
//! A's and B's question-id spaces are offset so C can route each Return it emits
//! back to the vat that asked (deterministic; no id collisions).
//!
//! Scenario:
//!   1. C hosts Carol, a `getNumber()`-style cap that returns 42.
//!   2. B imports Carol via a bootstrap on the B<->C connection.
//!   3. A bootstraps B's Introducer service on A<->B and calls `introduce()`.
//!      B's handler `sendProvide`s Carol (Provide -> C on B<->C), minting a vine
//!      on b_to_a, and returns the vine as the result cap. The Return emits a
//!      `thirdPartyHosted{ id = contact, vineId }` descriptor to A (P1 emission).
//!      A imports the vine and retains it.
//!   4. A obtains the introduction from the loopback VatNetwork
//!      (connectToIntroduced -> { peer = a_to_c, completion }) and `sendAccept`s
//!      on a_to_c. C's handleAccept matches the completion to the stored
//!      provision and Returns Carol as a normal cap; A imports Carol directly.
//!   5. A calls getNumber() on its DIRECT import of Carol -> 42 (proves the
//!      handoff produced a working cap, not a vine proxy).
//!   6. A releases the vine -> B Finishes its held-open Provide question.
//!   7. Teardown: every ref released; all six/one peers deinit with empty
//!      tables under std.testing.allocator (any leak fails the test).

const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const vat_network = capnpc.rpc.vat.network;
const message = capnpc.message;
const Peer = peer_impl.Peer;
const harness = @import("three_party_handoff_harness.zig");

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

// -- Vat C: Carol, the provided capability -----------------------------------

const Carol = struct {
    get_number_calls: u32 = 0,

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
        var ret_ctx = NumberReturnCtx{ .n = 42 };
        try peer.sendReturnResults(call.question_id, &ret_ctx, NumberReturnCtx.build);
    }
};

// -- Vat B: observes its bootstrap of C (imports Carol) ----------------------

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

// -- Vat B: Introducer bootstrap service -------------------------------------
//
// `introduce()` originates the handoff: it sendProvides Carol (naming A as the
// recipient) and returns the vine as the result cap, so the Return emits the
// `thirdPartyHosted` descriptor to A.

const Introducer = struct {
    b_to_c: *Peer,
    b_to_a: *Peer,
    carol_import_id: u32,
    recipient_nonce: []const u8,
    // The ThirdPartyToContact token bytes (owned by the introduction; freed here
    // after sendProvide has copied them into the mark).
    provide_question_id: ?u32 = null,
    vine_id: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Introducer = castCtx(*Introducer, ctx_ptr);
        // The call arrives on b_to_a (A<->B); `peer` is b_to_a.
        std.debug.assert(peer == self.b_to_a);

        // Mint the introduction via the vat network attached to b_to_a. The
        // recipient hint is the pre-registered nonce naming A's connection to C.
        const network = self.b_to_a.vat_network orelse return error.NoVatNetwork;
        var introduction = try network.mintIntroduction(self.b_to_a, self.recipient_nonce);
        defer introduction.deinit(self.b_to_a.allocator);

        // Build an AnyPointerReader over the ThirdPartyToAwait for Provide.recipient.
        var await_msg = try message.Message.initUnvalidated(self.b_to_a.allocator, introduction.to_await);
        defer await_msg.deinit();
        const recipient = try await_msg.getRootAnyPointer();

        // Provide's target names Carol on C: importedCap = Carol's import id on
        // the B<->C connection (== Carol's export id on C).
        const provided_target = protocol.MessageTarget{
            .tag = .importedCap,
            .imported_cap = self.carol_import_id,
            .promised_answer = null,
        };

        // ORIGINATE: Provide -> C on b_to_c; vine minted on b_to_a; the
        // ThirdPartyToContact is marked so the vine's descriptor becomes
        // thirdPartyHosted.
        const handle = try self.b_to_c.sendProvide(
            provided_target,
            recipient,
            self.b_to_a,
            introduction.to_contact,
        );
        self.provide_question_id = handle.question_id;
        self.vine_id = handle.vine_id;

        // Return the vine as the result cap; the Return payload encoder emits
        // thirdPartyHosted{ id = contact, vineId } for it.
        const VineReturnCtx = struct {
            vine_id: u32,
            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = castCtx(*const @This(), bctx);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                try any.setCapability(.{ .id = bself.vine_id });
            }
        };
        var ret_ctx = VineReturnCtx{ .vine_id = handle.vine_id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, VineReturnCtx.build);
    }
};

// -- Vat A: bootstrap probe for the Introducer -------------------------------

const IntroducerProbe = struct {
    introducer_import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *IntroducerProbe = castCtx(*IntroducerProbe, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
        const payload = ret.results orelse return error.MissingBootstrapPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.introducer_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.IntroducerNotImported,
        };
    }
};

// -- Vat A: the introduce() Return — imports the vine (thirdPartyHosted) ------

const IntroduceCall = struct {
    vine_import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *IntroduceCall = castCtx(*IntroduceCall, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedIntroduceReturn;
        const payload = ret.results orelse return error.MissingIntroducePayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        // A thirdPartyHosted descriptor resolves (Level-1/2 proxy fallback) to
        // the vine import. Retain it: the test releases it explicitly to drive
        // B's Provide Finish.
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.vine_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.VineNotImported,
        };
    }
};

// -- Vat A: the Accept Return — imports Carol directly ------------------------

const AcceptCall = struct {
    carol_import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *AcceptCall = castCtx(*AcceptCall, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedAcceptReturn;
        const payload = ret.results orelse return error.MissingAcceptPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.carol_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.AcceptedCarolNotImported,
        };
    }
};

// -- Vat A: getNumber() call on the accepted Carol ---------------------------

const GetNumberCall = struct {
    result: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *GetNumberCall = castCtx(*GetNumberCall, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedGetNumberReturn;
        const payload = ret.results orelse return error.MissingGetNumberPayload;
        self.result = try readNumberN(payload);
    }
};

const GetNumberResultOrExceptionCall = struct {
    result: ?u32 = null,
    returned: bool = false,
    exception: bool = false,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *GetNumberResultOrExceptionCall = castCtx(*GetNumberResultOrExceptionCall, ctx_ptr);
        self.returned = true;
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse return error.MissingGetNumberPayload;
                self.result = try readNumberN(payload);
            },
            .exception => self.exception = true,
            else => return error.UnexpectedGetNumberReturn,
        }
    }
};

// -- Three-connection synchronous wire ---------------------------------------
//
// Frames from A<->B and B<->C forward straight to the paired endpoint. Vat C's
// single peer emits Returns for both B's questions and A's Accept question; each
// is routed back to the vat that asked, tracked by question id (A and B use
// disjoint id ranges, so there is never ambiguity).

const Vat = enum { a, b, c };

const Link = struct {
    allocator: std.mem.Allocator,
    forwarding: bool = true,

    a_to_b: ?*Peer = null,
    b_to_a: ?*Peer = null,
    b_to_c: ?*Peer = null,
    c: ?*Peer = null,
    a_to_c: ?*Peer = null,

    // -- Fault injection / observation for the ownership regression (issue #56).
    // When set, the NEXT `.finish` frame B sends to C is rejected (and the flag
    // self-resets), simulating a post-callback send failure on the forwarded
    // question — the exact interleaving where the return callback already
    // answered VatA before `sendCall`'s trailing work fails.
    fail_next_bToC_finish: bool = false,
    // When set, count every `.return` frame B sends to A. Used to prove VatA
    // receives EXACTLY ONE Return for a forwarded parked call — never a second
    // (exception) Return from a mishandled post-callback failure.
    count_bToA_returns: bool = false,
    bToA_return_count: u32 = 0,

    // Which vat originated each question C is answering (answer_id -> origin).
    c_question_origin: std.AutoHashMap(u32, Vat),

    fn init(allocator: std.mem.Allocator) Link {
        return .{
            .allocator = allocator,
            .c_question_origin = std.AutoHashMap(u32, Vat).init(allocator),
        };
    }

    fn deinit(self: *Link) void {
        self.c_question_origin.deinit();
    }

    fn recordCQuestion(self: *Link, frame: []const u8, origin: Vat) !void {
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        const qid: ?u32 = switch (decoded.tag) {
            .bootstrap => (try decoded.asBootstrap()).question_id,
            .call => (try decoded.asCall()).question_id,
            .accept => (try decoded.asAccept()).question_id,
            else => null,
        };
        if (qid) |id| try self.c_question_origin.put(id, origin);
    }

    // A -> B (on the A<->B wire): deliver to b_to_a.
    fn aToBSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        if (self.b_to_a) |peer| try peer.handleFrame(frame);
    }

    // B -> A (on the A<->B wire): deliver to a_to_b.
    fn bToASend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (self.count_bToA_returns) {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (decoded.tag == .@"return") self.bToA_return_count += 1;
        }
        if (!self.forwarding) return;
        if (self.a_to_b) |peer| try peer.handleFrame(frame);
    }

    // B -> C (on the B<->C wire): deliver to c; remember B owns these questions.
    fn bToCSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        try self.recordCQuestion(frame, .b);
        // Fault injection (issue #56 ownership regression): simulate OOM while
        // sending the forwarded question's auto-Finish. This is the ONE error
        // `maybeSendAutoFinish` re-raises rather than swallowing as non-fatal
        // (peer_return_dispatch.zig) — so it propagates back out of `sendCall`
        // AFTER the return callback already answered VatA, exercising the exact
        // catch-arm ownership hazard. The fix's settled flag must make that catch
        // swallow the trailing error instead of sending a second (exception)
        // Return to VatA.
        if (self.fail_next_bToC_finish) {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (decoded.tag == .finish) {
                self.fail_next_bToC_finish = false;
                return error.OutOfMemory;
            }
        }
        if (!self.forwarding) return;
        if (self.c) |peer| try peer.handleFrame(frame);
    }

    // A -> C (on the A<->C wire): deliver to c; remember A owns these questions.
    fn aToCSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        try self.recordCQuestion(frame, .a);
        if (!self.forwarding) return;
        if (self.c) |peer| try peer.handleFrame(frame);
    }

    // C -> (A or B): route frames that answer existing questions by the recorded
    // question owner. Calls/Releases initiated by C in these tests are calls to,
    // or drops of, B-hosted proxy exports, so they route to B.
    fn cSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        const target: ?Vat = switch (decoded.tag) {
            .@"return" => self.c_question_origin.get((try decoded.asReturn()).answer_id),
            .finish => self.c_question_origin.get((try decoded.asFinish()).question_id) orelse .b,
            .call, .release => .b,
            else => null,
        };
        const dest = target orelse return; // unrouted control frame: drop safely
        switch (dest) {
            .a => if (self.a_to_c) |peer| try peer.handleFrame(frame),
            .b => if (self.b_to_c) |peer| try peer.handleFrame(frame),
            .c => {},
        }
    }
};

test "three-party handoff origination: Provide+Accept hands C's cap to A directly" {
    const allocator = std.testing.allocator;

    var link = Link.init(allocator);
    defer link.deinit();
    // Stop forwarding before any peer.deinit (LIFO) so teardown frames are not
    // delivered into a half-torn-down peer.
    defer link.forwarding = false;

    // Loopback vat network: nonce -> the peer A uses to reach C (a_to_c).
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    // -- Vat C: a single peer serving both B's Provide and A's Accept. --------
    var c = Peer.initDetached(allocator);
    c.disableThreadAffinity();
    defer c.deinit();

    // -- Vat B: two endpoints. ------------------------------------------------
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var b_to_a = Peer.initDetached(allocator);
    b_to_a.disableThreadAffinity();
    defer b_to_a.deinit();

    // -- Vat A: two endpoints. ------------------------------------------------
    var a_to_b = Peer.initDetached(allocator);
    a_to_b.disableThreadAffinity();
    defer a_to_b.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    // Offset A's question spaces above B's so C can route unambiguously.
    b_to_c.next_question_id = 0;
    a_to_c.next_question_id = 1000;
    a_to_b.next_question_id = 2000;

    link.a_to_b = &a_to_b;
    link.b_to_a = &b_to_a;
    link.b_to_c = &b_to_c;
    link.c = &c;
    link.a_to_c = &a_to_c;

    a_to_b.setSendFrameOverride(&link, Link.aToBSend);
    b_to_a.setSendFrameOverride(&link, Link.bToASend);
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);
    c.setSendFrameOverride(&link, Link.cSend);

    // Register the introduction: A reaches C via a_to_c.
    const recipient_nonce = "handoff-nonce-1";
    try net.register(recipient_nonce, &a_to_c);

    // Attach the vat network to b_to_a (B originates on it) and a_to_c (A picks
    // up on it — not strictly required for this flow but mirrors real wiring).
    b_to_a.attachVatNetwork(net.network());
    a_to_c.attachVatNetwork(net.network());

    // -- C hosts Carol as its bootstrap. --------------------------------------
    var carol = Carol{};
    const carol_export_id = try c.setBootstrap(.{ .ctx = &carol, .on_call = Carol.onCall });

    // -- (1) B imports Carol via a bootstrap on B<->C. ------------------------
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;
    try std.testing.expectEqual(carol_export_id, carol_import_id);

    // -- B's Introducer bootstrap service on the A<->B connection. ------------
    var introducer = Introducer{
        .b_to_c = &b_to_c,
        .b_to_a = &b_to_a,
        .carol_import_id = carol_import_id,
        .recipient_nonce = recipient_nonce,
    };
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = Introducer.onCall });

    // -- (2) A bootstraps the Introducer. -------------------------------------
    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse
        return error.IntroducerBootstrapFailed;

    // -- (3) A calls introduce(): B originates the handoff and returns the vine.
    var introduce_call = IntroduceCall{};
    _ = try a_to_b.sendCall(
        introducer_import_id,
        0x1234_5678_9abc_def0, // Introducer interface id (unused by handler)
        0, // introduce method id
        &introduce_call,
        null,
        IntroduceCall.onReturn,
    );

    // B originated the Provide (held-open question) and minted the vine.
    const provide_question_id = introducer.provide_question_id orelse return error.ProvideNotSent;
    const vine_id = introducer.vine_id orelse return error.VineNotMinted;
    // A received a thirdPartyHosted descriptor -> imported the vine.
    const vine_import_id = introduce_call.vine_import_id orelse return error.VineNotImportedByA;
    try std.testing.expectEqual(vine_id, vine_import_id);

    // C recorded the provision (held open, no Return yet); B's vine export lives
    // on b_to_a with a wire ref from A; the coupling is recorded.
    try std.testing.expect(c.provides_by_question.contains(provide_question_id));
    try std.testing.expect(b_to_a.exports.contains(vine_id));
    try std.testing.expectEqual(@as(u32, 1), b_to_a.exports.get(vine_id).?.ref_count);
    try harness.expectOutboundProvideCoupled(&b_to_a, &b_to_c, vine_id, provide_question_id, carol_import_id);
    try harness.expectImport(&a_to_b, vine_import_id);

    // -- (4) A obtains the introduction from the vat network and Accepts. -----
    const contact_token = try vat_network.encodeNonceToken(allocator, recipient_nonce);
    defer allocator.free(contact_token);
    var contact_msg = try message.Message.initUnvalidated(allocator, contact_token);
    defer contact_msg.deinit();
    const contact = try contact_msg.getRootAnyPointer();

    const a_network = a_to_c.vat_network orelse return error.NoVatNetworkOnA;
    var introduced = try a_network.connectToIntroduced(contact);
    defer introduced.deinit(allocator);
    // The network resolved the contact to A's connection to C.
    try std.testing.expectEqual(&a_to_c, introduced.peer);

    var completion_msg = try message.Message.initUnvalidated(allocator, introduced.completion);
    defer completion_msg.deinit();
    const provision = try completion_msg.getRootAnyPointer();

    var accept_call = AcceptCall{};
    _ = try introduced.peer.sendAccept(provision, null, &accept_call, AcceptCall.onReturn);

    // A imported Carol DIRECTLY from C's Accept Return.
    const accepted_carol_id = accept_call.carol_import_id orelse return error.AcceptDidNotResolve;
    try harness.expectImport(&a_to_c, accepted_carol_id);

    // -- (5) A calls getNumber() on its direct import of Carol -> 42. ---------
    var get_number = GetNumberCall{};
    _ = try a_to_c.sendCall(
        accepted_carol_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &get_number,
        null,
        GetNumberCall.onReturn,
    );
    try std.testing.expectEqual(@as(u32, 42), get_number.result orelse return error.GetNumberDidNotReturn);
    try std.testing.expectEqual(@as(u32, 1), carol.get_number_calls);

    // -- (6) A releases the vine -> B Finishes the held-open Provide. ---------
    try std.testing.expect(c.provides_by_question.contains(provide_question_id));
    try a_to_b.releaseImport(vine_import_id, 1);

    // The vine export is destroyed on B, the coupling is cleared, and B's
    // Provide question is gone (Finished). C dropped the provision.
    try std.testing.expect(!b_to_a.exports.contains(vine_id));
    try harness.expectNoOutboundProvide(&b_to_a, vine_id);
    try std.testing.expect(!c.provides_by_question.contains(provide_question_id));
    // The provision is fully unregistered on C (both lookup tables drained).
    try harness.expectNoProvideState(&c);

    // -- Teardown: release every remaining import so all tables drain. --------
    // A's direct Carol import (retained in AcceptCall).
    try a_to_c.releaseImport(accepted_carol_id, 1);
    // B's Carol import (retained in CarolImportProbe).
    try b_to_c.releaseImport(carol_import_id, 1);
    // A's Introducer import (retained in IntroducerProbe). B's Introducer
    // bootstrap export persists by design.
    try a_to_b.releaseImport(introducer_import_id, 1);

    // Everything drains: no vine, no provisions, no dangling imports.
    try harness.expectNoImport(&a_to_c, accepted_carol_id);
    try harness.expectNoImport(&b_to_c, carol_import_id);
    try harness.expectNoImport(&a_to_b, introducer_import_id);
    // C holds no lingering imports and no third-party-hosted marks.
    try std.testing.expectEqual(@as(u32, 0), @as(u32, @intCast(c.caps.imports.count())));
}

// -- BUG #55 chaos: drop the B<->C peer FIRST, then Release the vine ----------
//
// The previously-forbidden teardown order for the cross-peer vine->Provide
// coupling: the host-of-provided-cap connection (B<->C) is destroyed while a
// coupled vine is still live on the host-of-recipient connection (B<->A). Under
// the old borrowed-raw-`*Peer` coupling, the subsequent vine Release on b_to_a
// dereferenced a freed peer (UAF). The liveness fix neutralizes the coupling at
// b_to_c.deinit() time (nulling the back-pointer in b_to_a.outbound_provides),
// so this exact ordering must now run UAF-clean: the Release is a safe no-op.
//
// This test deliberately manages b_to_c's lifetime by hand (no `defer`) so it
// can be deinited MID-FLOW, before the vine Release. Everything runs under
// std.testing.allocator, so any leak or double-free fails the test.
test "BUG #55: drop B<->C peer before Release-vine is UAF-clean and drains" {
    const allocator = std.testing.allocator;

    var link = Link.init(allocator);
    defer link.deinit();
    defer link.forwarding = false;

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    // Vat C: single peer serving B's Provide and (were it reached) A's Accept.
    var c = Peer.initDetached(allocator);
    c.disableThreadAffinity();
    defer c.deinit();

    // Vat B endpoints. b_to_c is deinited BY HAND mid-test (the dropped
    // provided-cap connection), so it is NOT registered with a `defer`.
    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    var b_to_c_alive = true;
    defer if (b_to_c_alive) b_to_c.deinit();
    var b_to_a = Peer.initDetached(allocator);
    b_to_a.disableThreadAffinity();
    defer b_to_a.deinit();

    // Vat A endpoints.
    var a_to_b = Peer.initDetached(allocator);
    a_to_b.disableThreadAffinity();
    defer a_to_b.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    b_to_c.next_question_id = 0;
    a_to_c.next_question_id = 1000;
    a_to_b.next_question_id = 2000;

    link.a_to_b = &a_to_b;
    link.b_to_a = &b_to_a;
    link.b_to_c = &b_to_c;
    link.c = &c;
    link.a_to_c = &a_to_c;

    a_to_b.setSendFrameOverride(&link, Link.aToBSend);
    b_to_a.setSendFrameOverride(&link, Link.bToASend);
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);
    c.setSendFrameOverride(&link, Link.cSend);

    const recipient_nonce = "handoff-nonce-chaos";
    try net.register(recipient_nonce, &a_to_c);
    b_to_a.attachVatNetwork(net.network());
    a_to_c.attachVatNetwork(net.network());

    // C hosts Carol; B imports her over B<->C.
    var carol = Carol{};
    _ = try c.setBootstrap(.{ .ctx = &carol, .on_call = Carol.onCall });
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;

    // B's Introducer on A<->B; A bootstraps + calls introduce() to originate.
    var introducer = Introducer{
        .b_to_c = &b_to_c,
        .b_to_a = &b_to_a,
        .carol_import_id = carol_import_id,
        .recipient_nonce = recipient_nonce,
    };
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = Introducer.onCall });
    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse
        return error.IntroducerBootstrapFailed;

    var introduce_call = IntroduceCall{};
    _ = try a_to_b.sendCall(
        introducer_import_id,
        0x1234_5678_9abc_def0,
        0,
        &introduce_call,
        null,
        IntroduceCall.onReturn,
    );

    // (1) Handoff originated: vine coupled on b_to_a, Provide held open on b_to_c.
    const provide_question_id = introducer.provide_question_id orelse return error.ProvideNotSent;
    const vine_id = introducer.vine_id orelse return error.VineNotMinted;
    const vine_import_id = introduce_call.vine_import_id orelse return error.VineNotImportedByA;
    try std.testing.expectEqual(vine_id, vine_import_id);
    try std.testing.expect(b_to_a.outbound_provides.contains(vine_id));
    // The held-open Provide question lives on b_to_c (the provided-cap peer);
    // C recorded the matching provide entry.
    try std.testing.expect(b_to_c.questions.contains(provide_question_id));
    try std.testing.expect(c.provides_by_question.contains(provide_question_id));
    // The provide_peer coupling on b_to_a records a back-link on b_to_c.
    try std.testing.expectEqual(@as(usize, 1), b_to_c.coupled_vines.items.len);
    // The coupling borrows a live pointer to b_to_c right now.
    try std.testing.expectEqual(
        @as(?*Peer, &b_to_c),
        b_to_a.outbound_provides.get(vine_id).?.provide_peer,
    );

    // (2) FORBIDDEN ORDER, step one: tear down the B<->C peer FIRST while the
    //     vine is still live and coupled on b_to_a. Stop forwarding B<->C frames
    //     so the teardown Finish/Release do not reach a half-gone peer, exactly
    //     as production teardown drops the socket. b_to_c.deinit() must NULL the
    //     coupling's provide_peer on b_to_a before freeing its own memory.
    link.forwarding = false;
    b_to_c.deinit();
    b_to_c_alive = false;
    link.forwarding = true;

    // The coupling survived the peer drop but no longer points at freed memory:
    // its provide_peer was neutralized to null.
    try std.testing.expect(b_to_a.outbound_provides.contains(vine_id));
    try std.testing.expectEqual(
        @as(?*Peer, null),
        b_to_a.outbound_provides.get(vine_id).?.provide_peer,
    );

    // (3) FORBIDDEN ORDER, step two: A Releases the vine on b_to_a. Under the old
    //     coupling this dereferenced the freed b_to_c. Now handleRelease finds a
    //     null provide_peer and skips the Finish — a safe no-op. No UAF.
    try a_to_b.releaseImport(vine_import_id, 1);

    // The vine export is gone and the coupling drained cleanly.
    try std.testing.expect(!b_to_a.exports.contains(vine_id));
    try std.testing.expect(!b_to_a.outbound_provides.contains(vine_id));

    // Teardown: release the remaining live imports so every table drains. (The
    // Accept was never sent — the provided-cap connection dropped first — so
    // there is no direct Carol import on A to release. b_to_c already deinited,
    // dropping its own Carol import on the way out.)
    try a_to_b.releaseImport(introducer_import_id, 1);
    try std.testing.expect(!a_to_b.caps.hasImport(introducer_import_id));
    // No coupling back-links linger on b_to_a either (it never anchored one).
    try std.testing.expectEqual(@as(usize, 0), b_to_a.coupled_vines.items.len);
}

// ============================================================================
// Issue #56 — the REAL P-pipeline: the introducer FORWARDS a caller's parked
// pipelined promise-calls to the capability host (instead of rejecting them at
// the vine).
//
// Topology (three vats over the same synchronous loopback as above):
//   Vat A: a_to_b (A<->B)  a_to_c (A<->C)   — holds promise P; pipelines on it
//   Vat B: b_to_a (A<->B)  b_to_c (B<->C)   — introducer; FORWARDS parked calls
//   Vat C: c (single peer)                   — hosts Counter; sees the calls
//
// Scenario (no embargo — the bare forwarding proof):
//   1. B exports a PROMISE P to A (getPromise) and A imports it.
//   2. A PIPELINES bump(k) calls on P (calls on the PROMISE itself). Unresolved,
//      they PARK at B in pending_export_promises. Carol/Counter sees nothing.
//   3. B hands P off to C via resolvePromiseExportToThirdParty. On resolution the
//      parked calls are replayed onto the handoff vine and, per issue #56,
//      FORWARDED to Counter on C — NOT rejected by the vine.
//   4. ASSERT: each parked call reached Counter and returned its correct value to
//      A; the parked calls arrived at Counter in the exact order A sent them
//      (e-order across the vine forward).
//   5. Leak-checked; every table drains; both/all peers deinit clean.
// ============================================================================

// -- bump(n): params one data word n :UInt32 @0; results one data word,
//    the Counter's running total AFTER adding n. Lets the test prove both value
//    passthrough (n travels A->B-vine->C) and arrival ORDER (the total encodes
//    the sequence in which C saw the calls).

fn readU32At0(payload: protocol.Payload) !u32 {
    const content = try payload.content.getStruct();
    return content.readU32(0);
}

const BumpCounter = struct {
    total: u32 = 0,
    seq: u32 = 0,
    /// Arrival order stamp per observed n (n -> the seq at which C saw bump(n)).
    order: [8]?u32 = [8]?u32{ null, null, null, null, null, null, null, null },

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *BumpCounter = castCtx(*BumpCounter, ctx_ptr);
        if (call.interface_id != NUMBER_INTERFACE_ID) return error.UnexpectedMethod;
        const n = try readU32At0(call.params);
        self.seq += 1;
        if (n < self.order.len) self.order[n] = self.seq;
        self.total += n;
        const RetCtx = struct {
            total: u32,
            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = castCtx(*const @This(), bctx);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                const results = try any.initStruct(1, 0);
                results.writeU32(0, bself.total);
            }
        };
        var ret_ctx = RetCtx{ .total = self.total };
        try peer.sendReturnResults(call.question_id, &ret_ctx, RetCtx.build);
    }
};

/// B's Introducer variant that mints and returns a PROMISE export (P). A imports
/// it as a promise and pipelines calls on it; those calls park at B until B
/// resolves P via resolvePromiseExportToThirdParty.
const PromiseIntroducer = struct {
    promise_export_id: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *PromiseIntroducer = castCtx(*PromiseIntroducer, ctx_ptr);
        const promise_id = try peer.addPromiseExport();
        self.promise_export_id = promise_id;
        const RetCtx = struct {
            promise_id: u32,
            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = castCtx(*const @This(), bctx);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                try any.setCapability(.{ .id = bself.promise_id });
            }
        };
        var ret_ctx = RetCtx{ .promise_id = promise_id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, RetCtx.build);
    }
};

const PromiseImportProbe = struct {
    promise_import_id: ?u32 = null,
    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *PromiseImportProbe = castCtx(*PromiseImportProbe, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedPromiseReturn;
        const payload = ret.results orelse return error.MissingPromisePayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.promise_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.PromiseNotImported,
        };
    }
};

/// A pipelined bump(n) call: one ctx drives both the params build and the return
/// observer, capturing the running total C returned (proves value passthrough).
const BumpCall = struct {
    n: u32,
    total: ?u32 = null,
    returned: bool = false,
    exception: bool = false,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *const BumpCall = castCtx(*const BumpCall, ctx_ptr);
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        const params = try any.initStruct(1, 0);
        params.writeU32(0, self.n);
    }

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *BumpCall = castCtx(*BumpCall, ctx_ptr);
        self.returned = true;
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse return error.MissingBumpPayload;
                self.total = try readU32At0(payload);
            },
            .exception => self.exception = true,
            else => return error.UnexpectedBumpReturn,
        }
    }
};

/// A pipelined call whose params carry a callback capability in pointer field 0.
/// C must receive that capability as an import, call it through B's proxy, and
/// return n + callback_result to A.
const CapParamCall = struct {
    n: u32,
    callback_export_id: u32,
    total: ?u32 = null,
    returned: bool = false,
    exception: bool = false,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *const CapParamCall = castCtx(*const CapParamCall, ctx_ptr);
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        const params = try any.initStruct(1, 1);
        params.writeU32(0, self.n);
        var cap_slot = try params.getAnyPointer(0);
        try cap_slot.setCapability(.{ .id = self.callback_export_id });
    }

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *CapParamCall = castCtx(*CapParamCall, ctx_ptr);
        self.returned = true;
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse return error.MissingCapParamPayload;
                self.total = try readU32At0(payload);
            },
            .exception => self.exception = true,
            else => return error.UnexpectedCapParamReturn,
        }
    }
};

const CapCallingCounter = struct {
    calls: u32 = 0,
    callback_import_seen: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *CapCallingCounter = castCtx(*CapCallingCounter, ctx_ptr);
        if (call.interface_id != NUMBER_INTERFACE_ID) return error.UnexpectedMethod;

        const content = try call.params.content.getStruct();
        const n = content.readU32(0);
        const cap = try content.readCapability(0);
        const resolved = try caps.resolveCapability(cap);
        self.callback_import_seen = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.CallbackNotImported,
        };

        var callback = GetNumberCall{};
        _ = try peer.sendCallResolved(
            resolved,
            NUMBER_INTERFACE_ID,
            GET_NUMBER_METHOD_ID,
            &callback,
            null,
            GetNumberCall.onReturn,
        );
        const callback_value = callback.result orelse return error.CallbackDidNotReturn;

        self.calls += 1;
        const RetCtx = struct {
            total: u32,
            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = castCtx(*const @This(), bctx);
                ret.setReleaseParamCaps(false);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                const results = try any.initStruct(1, 0);
                results.writeU32(0, bself.total);
            }
        };
        var ret_ctx = RetCtx{ .total = n + callback_value };
        try peer.sendReturnResults(call.question_id, &ret_ctx, RetCtx.build);
    }
};

/// C returns a helper capability in the forwarded results. A imports B's proxy
/// for that helper and then calls it over A↔B, which must forward to C.
const CapReturningCounter = struct {
    helper_export_id: u32,
    calls: u32 = 0,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *CapReturningCounter = castCtx(*CapReturningCounter, ctx_ptr);
        if (call.interface_id != NUMBER_INTERFACE_ID) return error.UnexpectedMethod;
        self.calls += 1;
        const RetCtx = struct {
            helper_export_id: u32,
            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = castCtx(*const @This(), bctx);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                try any.setCapability(.{ .id = bself.helper_export_id });
            }
        };
        var ret_ctx = RetCtx{ .helper_export_id = self.helper_export_id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, RetCtx.build);
    }
};

const CapResultCall = struct {
    cap_import_id: ?u32 = null,
    returned: bool = false,
    exception: bool = false,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *CapResultCall = castCtx(*CapResultCall, ctx_ptr);
        self.returned = true;
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse return error.MissingCapResultPayload;
                var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
                const cap = try payload.content.getCapability();
                const resolved = try mutable_caps.resolveCapability(cap);
                try mutable_caps.retainCapability(cap);
                self.cap_import_id = switch (resolved) {
                    .imported => |imp| imp.id,
                    else => return error.ResultCapNotImported,
                };
            },
            .exception => self.exception = true,
            else => return error.UnexpectedCapResultReturn,
        }
    }
};

test "three-party handoff: introducer FORWARDS parked pipelined P-calls to C (issue #56)" {
    const allocator = std.testing.allocator;

    var link = Link.init(allocator);
    defer link.deinit();
    defer link.forwarding = false;

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var c = Peer.initDetached(allocator);
    c.disableThreadAffinity();
    defer c.deinit();

    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var b_to_a = Peer.initDetached(allocator);
    b_to_a.disableThreadAffinity();
    defer b_to_a.deinit();

    var a_to_b = Peer.initDetached(allocator);
    a_to_b.disableThreadAffinity();
    defer a_to_b.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    b_to_c.next_question_id = 0;
    a_to_c.next_question_id = 1000;
    a_to_b.next_question_id = 2000;

    link.a_to_b = &a_to_b;
    link.b_to_a = &b_to_a;
    link.b_to_c = &b_to_c;
    link.c = &c;
    link.a_to_c = &a_to_c;

    a_to_b.setSendFrameOverride(&link, Link.aToBSend);
    b_to_a.setSendFrameOverride(&link, Link.bToASend);
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);
    c.setSendFrameOverride(&link, Link.cSend);

    const recipient_nonce = "handoff-forward-nonce-1";
    try net.register(recipient_nonce, &a_to_c);
    b_to_a.attachVatNetwork(net.network());
    a_to_c.attachVatNetwork(net.network());

    // -- C hosts a Counter as its bootstrap; B imports it over B<->C. ----------
    var counter = BumpCounter{};
    _ = try c.setBootstrap(.{ .ctx = &counter, .on_call = BumpCounter.onCall });
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const counter_import_id = carol_probe.carol_import_id orelse return error.CounterBootstrapFailed;

    // -- B serves a promise-returning Introducer on A<->B; A imports promise P. -
    var introducer = PromiseIntroducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = PromiseIntroducer.onCall });

    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse return error.IntroducerBootstrapFailed;

    var promise_probe = PromiseImportProbe{};
    _ = try a_to_b.sendCall(introducer_import_id, 0x1234_5678_9abc_def0, 0, &promise_probe, null, PromiseImportProbe.onReturn);
    const promise_import_id = promise_probe.promise_import_id orelse return error.PromiseNotImportedByA;
    try std.testing.expect(a_to_b.caps.hasImport(promise_import_id));

    // -- A PIPELINES three bump(k) calls on the still-unresolved promise P. -----
    //    They park at B; Counter on C has seen NOTHING yet. The distinct n values
    //    (1, 2, 4) make the running total unambiguous and let us prove order.
    var bump1 = BumpCall{ .n = 1 };
    _ = try a_to_b.sendCall(promise_import_id, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &bump1, BumpCall.build, BumpCall.onReturn);
    var bump2 = BumpCall{ .n = 2 };
    _ = try a_to_b.sendCall(promise_import_id, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &bump2, BumpCall.build, BumpCall.onReturn);
    var bump4 = BumpCall{ .n = 4 };
    _ = try a_to_b.sendCall(promise_import_id, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &bump4, BumpCall.build, BumpCall.onReturn);

    // Parked at B, not yet forwarded: Counter is untouched and none have returned.
    try std.testing.expectEqual(@as(u32, 0), counter.seq);
    try std.testing.expect(!bump1.returned and !bump2.returned and !bump4.returned);
    try std.testing.expect(b_to_a.pending_export_promises.contains(promise_import_id));

    // -- B HANDS P off to C. On resolution the parked calls are replayed onto the
    //    vine and FORWARDED to Counter (issue #56), NOT rejected. --------------
    const b_network = b_to_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&b_to_a, recipient_nonce);
    defer introduction.deinit(allocator);
    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = counter_import_id,
        .promised_answer = null,
    };

    const handle = try b_to_a.resolvePromiseExportToThirdParty(
        promise_import_id,
        &b_to_c,
        provided_target,
        recipient,
        introduction.to_contact,
    );

    // *** PROOF: every parked pipelined call reached Counter on C and returned
    //     its correct value to A — they were FORWARDED, not rejected. ***
    try std.testing.expect(bump1.returned and bump2.returned and bump4.returned);
    try std.testing.expect(!bump1.exception and !bump2.exception and !bump4.exception);
    // Counter saw exactly the three forwarded calls.
    try std.testing.expectEqual(@as(u32, 3), counter.seq);
    try std.testing.expectEqual(@as(u32, 7), counter.total); // 1 + 2 + 4

    // *** PROOF: e-order — the forwarded parked calls reached C in the exact
    //     order A sent them (1, then 2, then 4). ***
    try std.testing.expectEqual(@as(u32, 1), counter.order[1].?);
    try std.testing.expectEqual(@as(u32, 2), counter.order[2].?);
    try std.testing.expectEqual(@as(u32, 3), counter.order[4].?);
    // The running totals A observed encode that same order: 1, then 1+2=3, then
    // 1+2+4=7 — a value passthrough proof that also pins the arrival sequence.
    try std.testing.expectEqual(@as(u32, 1), bump1.total.?);
    try std.testing.expectEqual(@as(u32, 3), bump2.total.?);
    try std.testing.expectEqual(@as(u32, 7), bump4.total.?);

    // The parked table drained; the vine coupling is in place with the forwarding
    // target recorded.
    try std.testing.expect(!b_to_a.pending_export_promises.contains(promise_import_id));
    const vine_id = handle.vine_id;
    try std.testing.expect(b_to_a.outbound_provides.contains(vine_id));
    try std.testing.expectEqual(
        @as(?u32, counter_import_id),
        b_to_a.outbound_provides.get(vine_id).?.provided_import_id,
    );

    // -- Teardown: A releases the vine (drives B's Provide Finish), then every
    //    remaining import so all tables drain. --------------------------------
    // A imported the vine when the thirdPartyHosted Resolve arrived; release it.
    try std.testing.expect(a_to_b.caps.hasImport(vine_id));
    try a_to_b.releaseImport(vine_id, 1);
    try std.testing.expect(!b_to_a.exports.contains(vine_id));
    try std.testing.expect(!b_to_a.outbound_provides.contains(vine_id));
    // B's Provide on C was Finished; C's provision tables drained.
    try std.testing.expectEqual(@as(usize, 0), c.provides_by_question.count());
    try std.testing.expectEqual(@as(usize, 0), c.provides_by_key.count());

    try b_to_c.releaseImport(counter_import_id, 1);
    try a_to_b.releaseImport(introducer_import_id, 1);
    try std.testing.expect(!b_to_c.caps.hasImport(counter_import_id));
    try std.testing.expect(!a_to_b.caps.hasImport(introducer_import_id));
    // No coupling back-links linger.
    try std.testing.expectEqual(@as(usize, 0), b_to_c.coupled_vines.items.len);
}

test "three-party handoff: forwarded parked call remaps cap-bearing params" {
    const allocator = std.testing.allocator;

    var link = Link.init(allocator);
    defer link.deinit();
    defer link.forwarding = false;

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var c = Peer.initDetached(allocator);
    c.disableThreadAffinity();
    defer c.deinit();

    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var b_to_a = Peer.initDetached(allocator);
    b_to_a.disableThreadAffinity();
    defer b_to_a.deinit();

    var a_to_b = Peer.initDetached(allocator);
    a_to_b.disableThreadAffinity();
    defer a_to_b.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    b_to_c.next_question_id = 0;
    a_to_c.next_question_id = 1000;
    a_to_b.next_question_id = 2000;

    link.a_to_b = &a_to_b;
    link.b_to_a = &b_to_a;
    link.b_to_c = &b_to_c;
    link.c = &c;
    link.a_to_c = &a_to_c;

    a_to_b.setSendFrameOverride(&link, Link.aToBSend);
    b_to_a.setSendFrameOverride(&link, Link.bToASend);
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);
    c.setSendFrameOverride(&link, Link.cSend);

    const recipient_nonce = "handoff-forward-cap-param-nonce";
    try net.register(recipient_nonce, &a_to_c);
    b_to_a.attachVatNetwork(net.network());
    a_to_c.attachVatNetwork(net.network());

    var counter = CapCallingCounter{};
    _ = try c.setBootstrap(.{ .ctx = &counter, .on_call = CapCallingCounter.onCall });
    var counter_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&counter_probe, CarolImportProbe.onReturn);
    const counter_import_id = counter_probe.carol_import_id orelse return error.CounterBootstrapFailed;

    var introducer = PromiseIntroducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = PromiseIntroducer.onCall });
    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse return error.IntroducerBootstrapFailed;

    var promise_probe = PromiseImportProbe{};
    _ = try a_to_b.sendCall(introducer_import_id, 0x1234_5678_9abc_def0, 0, &promise_probe, null, PromiseImportProbe.onReturn);
    const promise_import_id = promise_probe.promise_import_id orelse return error.PromiseNotImportedByA;

    var alice_callback = Carol{};
    const callback_export_id = try a_to_b.addExport(.{ .ctx = &alice_callback, .on_call = Carol.onCall });

    var call_with_cap = CapParamCall{ .n = 8, .callback_export_id = callback_export_id };
    _ = try a_to_b.sendCall(
        promise_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &call_with_cap,
        CapParamCall.build,
        CapParamCall.onReturn,
    );
    try std.testing.expect(!call_with_cap.returned);
    try std.testing.expect(b_to_a.pending_export_promises.contains(promise_import_id));

    const b_network = b_to_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&b_to_a, recipient_nonce);
    defer introduction.deinit(allocator);
    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = counter_import_id,
        .promised_answer = null,
    };
    const handle = try b_to_a.resolvePromiseExportToThirdParty(
        promise_import_id,
        &b_to_c,
        provided_target,
        recipient,
        introduction.to_contact,
    );

    try std.testing.expect(call_with_cap.returned);
    try std.testing.expect(!call_with_cap.exception);
    try std.testing.expectEqual(@as(u32, 50), call_with_cap.total.?); // 8 + A callback's 42
    try std.testing.expectEqual(@as(u32, 1), counter.calls);
    try std.testing.expect(counter.callback_import_seen != null);
    try std.testing.expectEqual(@as(u32, 1), alice_callback.get_number_calls);

    const vine_id = handle.vine_id;
    try a_to_b.releaseImport(vine_id, 1);
    try b_to_c.releaseImport(counter_import_id, 1);
    try a_to_b.releaseImport(introducer_import_id, 1);

    try std.testing.expect(!b_to_a.exports.contains(vine_id));
    try std.testing.expect(!b_to_a.outbound_provides.contains(vine_id));
    try std.testing.expect(!a_to_b.exports.contains(callback_export_id));
    try std.testing.expect(!b_to_c.caps.hasImport(counter_import_id));
    try std.testing.expect(!a_to_b.caps.hasImport(introducer_import_id));
    try std.testing.expectEqual(@as(usize, 0), b_to_c.coupled_vines.items.len);
}

test "three-party handoff: forwarded parked call remaps cap-bearing results" {
    const allocator = std.testing.allocator;

    var link = Link.init(allocator);
    defer link.deinit();
    defer link.forwarding = false;

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var c = Peer.initDetached(allocator);
    c.disableThreadAffinity();
    defer c.deinit();

    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var b_to_a = Peer.initDetached(allocator);
    b_to_a.disableThreadAffinity();
    defer b_to_a.deinit();

    var a_to_b = Peer.initDetached(allocator);
    a_to_b.disableThreadAffinity();
    defer a_to_b.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    b_to_c.next_question_id = 0;
    a_to_c.next_question_id = 1000;
    a_to_b.next_question_id = 2000;

    link.a_to_b = &a_to_b;
    link.b_to_a = &b_to_a;
    link.b_to_c = &b_to_c;
    link.c = &c;
    link.a_to_c = &a_to_c;

    a_to_b.setSendFrameOverride(&link, Link.aToBSend);
    b_to_a.setSendFrameOverride(&link, Link.bToASend);
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);
    c.setSendFrameOverride(&link, Link.cSend);

    const recipient_nonce = "handoff-forward-cap-result-nonce";
    try net.register(recipient_nonce, &a_to_c);
    b_to_a.attachVatNetwork(net.network());
    a_to_c.attachVatNetwork(net.network());

    var helper = Carol{};
    const helper_export_id = try c.addExport(.{ .ctx = &helper, .on_call = Carol.onCall });
    var cap_returner = CapReturningCounter{ .helper_export_id = helper_export_id };
    _ = try c.setBootstrap(.{ .ctx = &cap_returner, .on_call = CapReturningCounter.onCall });
    var counter_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&counter_probe, CarolImportProbe.onReturn);
    const counter_import_id = counter_probe.carol_import_id orelse return error.CounterBootstrapFailed;

    var introducer = PromiseIntroducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = PromiseIntroducer.onCall });
    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse return error.IntroducerBootstrapFailed;

    var promise_probe = PromiseImportProbe{};
    _ = try a_to_b.sendCall(introducer_import_id, 0x1234_5678_9abc_def0, 0, &promise_probe, null, PromiseImportProbe.onReturn);
    const promise_import_id = promise_probe.promise_import_id orelse return error.PromiseNotImportedByA;

    var cap_result = CapResultCall{};
    _ = try a_to_b.sendCall(
        promise_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &cap_result,
        null,
        CapResultCall.onReturn,
    );
    try std.testing.expect(!cap_result.returned);
    try std.testing.expect(b_to_a.pending_export_promises.contains(promise_import_id));

    const b_network = b_to_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&b_to_a, recipient_nonce);
    defer introduction.deinit(allocator);
    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = counter_import_id,
        .promised_answer = null,
    };
    const handle = try b_to_a.resolvePromiseExportToThirdParty(
        promise_import_id,
        &b_to_c,
        provided_target,
        recipient,
        introduction.to_contact,
    );

    try std.testing.expect(cap_result.returned);
    try std.testing.expect(!cap_result.exception);
    const returned_cap_import_id = cap_result.cap_import_id orelse return error.ResultCapNotImportedByA;
    try std.testing.expect(a_to_b.caps.hasImport(returned_cap_import_id));
    try std.testing.expectEqual(@as(u32, 1), cap_returner.calls);

    var get_number = GetNumberCall{};
    _ = try a_to_b.sendCall(
        returned_cap_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &get_number,
        null,
        GetNumberCall.onReturn,
    );
    try std.testing.expectEqual(@as(u32, 42), get_number.result.?);
    try std.testing.expectEqual(@as(u32, 1), helper.get_number_calls);

    try a_to_b.releaseImport(returned_cap_import_id, 1);
    try std.testing.expect(!a_to_b.caps.hasImport(returned_cap_import_id));

    const vine_id = handle.vine_id;
    try a_to_b.releaseImport(vine_id, 1);
    try b_to_c.releaseImport(counter_import_id, 1);
    try a_to_b.releaseImport(introducer_import_id, 1);

    try std.testing.expect(!b_to_a.exports.contains(vine_id));
    try std.testing.expect(!b_to_a.outbound_provides.contains(vine_id));
    try std.testing.expect(!b_to_c.caps.hasImport(counter_import_id));
    try std.testing.expect(!a_to_b.caps.hasImport(introducer_import_id));
    try std.testing.expectEqual(@as(usize, 0), b_to_c.coupled_vines.items.len);
}

test "three-party handoff: cross-peer result proxy survives source peer teardown" {
    const allocator = std.testing.allocator;

    var link = Link.init(allocator);
    defer link.deinit();
    defer link.forwarding = false;

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var c = Peer.initDetached(allocator);
    c.disableThreadAffinity();
    defer c.deinit();

    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    var b_to_c_alive = true;
    defer if (b_to_c_alive) b_to_c.deinit();
    var b_to_a = Peer.initDetached(allocator);
    b_to_a.disableThreadAffinity();
    defer b_to_a.deinit();

    var a_to_b = Peer.initDetached(allocator);
    a_to_b.disableThreadAffinity();
    defer a_to_b.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    b_to_c.next_question_id = 0;
    a_to_c.next_question_id = 1000;
    a_to_b.next_question_id = 2000;

    link.a_to_b = &a_to_b;
    link.b_to_a = &b_to_a;
    link.b_to_c = &b_to_c;
    link.c = &c;
    link.a_to_c = &a_to_c;

    a_to_b.setSendFrameOverride(&link, Link.aToBSend);
    b_to_a.setSendFrameOverride(&link, Link.bToASend);
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);
    c.setSendFrameOverride(&link, Link.cSend);

    const recipient_nonce = "handoff-proxy-source-deinit-nonce";
    try net.register(recipient_nonce, &a_to_c);
    b_to_a.attachVatNetwork(net.network());
    a_to_c.attachVatNetwork(net.network());

    var helper = Carol{};
    const helper_export_id = try c.addExport(.{ .ctx = &helper, .on_call = Carol.onCall });
    var cap_returner = CapReturningCounter{ .helper_export_id = helper_export_id };
    _ = try c.setBootstrap(.{ .ctx = &cap_returner, .on_call = CapReturningCounter.onCall });
    var counter_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&counter_probe, CarolImportProbe.onReturn);
    const counter_import_id = counter_probe.carol_import_id orelse return error.CounterBootstrapFailed;

    var introducer = PromiseIntroducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = PromiseIntroducer.onCall });
    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse return error.IntroducerBootstrapFailed;

    var promise_probe = PromiseImportProbe{};
    _ = try a_to_b.sendCall(introducer_import_id, 0x1234_5678_9abc_def0, 0, &promise_probe, null, PromiseImportProbe.onReturn);
    const promise_import_id = promise_probe.promise_import_id orelse return error.PromiseNotImportedByA;

    var cap_result = CapResultCall{};
    const cap_result_question_id = try a_to_b.sendCall(
        promise_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &cap_result,
        null,
        CapResultCall.onReturn,
    );
    try std.testing.expect(!cap_result.returned);
    try std.testing.expect(b_to_a.pending_export_promises.contains(promise_import_id));

    const b_network = b_to_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&b_to_a, recipient_nonce);
    defer introduction.deinit(allocator);
    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = counter_import_id,
        .promised_answer = null,
    };
    const handle = try b_to_a.resolvePromiseExportToThirdParty(
        promise_import_id,
        &b_to_c,
        provided_target,
        recipient,
        introduction.to_contact,
    );

    try std.testing.expect(cap_result.returned);
    try std.testing.expect(!cap_result.exception);
    const returned_cap_import_id = cap_result.cap_import_id orelse return error.ResultCapNotImportedByA;
    try std.testing.expect(a_to_b.caps.hasImport(returned_cap_import_id));
    try std.testing.expect(b_to_a.exports.contains(returned_cap_import_id));
    try std.testing.expectEqual(@as(usize, 1), b_to_c.cross_peer_proxy_links.items.len);
    try std.testing.expectEqual(@as(u32, 1), cap_returner.calls);

    var before_drop = GetNumberCall{};
    _ = try a_to_b.sendCall(
        returned_cap_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &before_drop,
        null,
        GetNumberCall.onReturn,
    );
    try std.testing.expectEqual(@as(u32, 42), before_drop.result.?);
    try std.testing.expectEqual(@as(u32, 1), helper.get_number_calls);

    link.forwarding = false;
    b_to_c.deinit();
    b_to_c_alive = false;
    link.b_to_c = null;
    link.forwarding = true;

    var after_drop = GetNumberResultOrExceptionCall{};
    _ = try a_to_b.sendCall(
        returned_cap_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &after_drop,
        null,
        GetNumberResultOrExceptionCall.onReturn,
    );
    try std.testing.expect(after_drop.returned);
    try std.testing.expect(after_drop.exception);
    try std.testing.expectEqual(@as(?u32, null), after_drop.result);
    try std.testing.expectEqual(@as(u32, 1), helper.get_number_calls);

    try a_to_b.releaseImport(returned_cap_import_id, 1);
    try std.testing.expect(!a_to_b.caps.hasImport(returned_cap_import_id));
    try a_to_b.sendFinishForHost(cap_result_question_id, false, false);
    try std.testing.expect(!b_to_a.exports.contains(returned_cap_import_id));

    const vine_id = handle.vine_id;
    try a_to_b.releaseImport(vine_id, 1);
    try a_to_b.releaseImport(introducer_import_id, 1);

    try std.testing.expect(!b_to_a.exports.contains(vine_id));
    try std.testing.expect(!b_to_a.outbound_provides.contains(vine_id));
    try std.testing.expect(!a_to_b.caps.hasImport(introducer_import_id));

    link.forwarding = false;
}

test "three-party handoff: cross-peer result proxy deregisters when owner peer dies first" {
    const allocator = std.testing.allocator;

    var link = Link.init(allocator);
    defer link.deinit();
    defer link.forwarding = false;

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var c = Peer.initDetached(allocator);
    c.disableThreadAffinity();
    defer c.deinit();

    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var b_to_a = Peer.initDetached(allocator);
    b_to_a.disableThreadAffinity();
    var b_to_a_alive = true;
    defer if (b_to_a_alive) b_to_a.deinit();

    var a_to_b = Peer.initDetached(allocator);
    a_to_b.disableThreadAffinity();
    defer a_to_b.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    b_to_c.next_question_id = 0;
    a_to_c.next_question_id = 1000;
    a_to_b.next_question_id = 2000;

    link.a_to_b = &a_to_b;
    link.b_to_a = &b_to_a;
    link.b_to_c = &b_to_c;
    link.c = &c;
    link.a_to_c = &a_to_c;

    a_to_b.setSendFrameOverride(&link, Link.aToBSend);
    b_to_a.setSendFrameOverride(&link, Link.bToASend);
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);
    c.setSendFrameOverride(&link, Link.cSend);

    const recipient_nonce = "handoff-proxy-owner-deinit-nonce";
    try net.register(recipient_nonce, &a_to_c);
    b_to_a.attachVatNetwork(net.network());
    a_to_c.attachVatNetwork(net.network());

    var helper = Carol{};
    const helper_export_id = try c.addExport(.{ .ctx = &helper, .on_call = Carol.onCall });
    var cap_returner = CapReturningCounter{ .helper_export_id = helper_export_id };
    _ = try c.setBootstrap(.{ .ctx = &cap_returner, .on_call = CapReturningCounter.onCall });
    var counter_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&counter_probe, CarolImportProbe.onReturn);
    const counter_import_id = counter_probe.carol_import_id orelse return error.CounterBootstrapFailed;

    var introducer = PromiseIntroducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = PromiseIntroducer.onCall });
    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse return error.IntroducerBootstrapFailed;

    var promise_probe = PromiseImportProbe{};
    _ = try a_to_b.sendCall(introducer_import_id, 0x1234_5678_9abc_def0, 0, &promise_probe, null, PromiseImportProbe.onReturn);
    const promise_import_id = promise_probe.promise_import_id orelse return error.PromiseNotImportedByA;

    var cap_result = CapResultCall{};
    _ = try a_to_b.sendCall(
        promise_import_id,
        NUMBER_INTERFACE_ID,
        GET_NUMBER_METHOD_ID,
        &cap_result,
        null,
        CapResultCall.onReturn,
    );
    try std.testing.expect(!cap_result.returned);
    try std.testing.expect(b_to_a.pending_export_promises.contains(promise_import_id));

    const b_network = b_to_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&b_to_a, recipient_nonce);
    defer introduction.deinit(allocator);
    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = counter_import_id,
        .promised_answer = null,
    };
    const handle = try b_to_a.resolvePromiseExportToThirdParty(
        promise_import_id,
        &b_to_c,
        provided_target,
        recipient,
        introduction.to_contact,
    );
    _ = handle;

    try std.testing.expect(cap_result.returned);
    try std.testing.expect(!cap_result.exception);
    const returned_cap_import_id = cap_result.cap_import_id orelse return error.ResultCapNotImportedByA;
    try std.testing.expect(a_to_b.caps.hasImport(returned_cap_import_id));
    try std.testing.expect(b_to_a.exports.contains(returned_cap_import_id));
    try std.testing.expectEqual(@as(usize, 1), b_to_c.cross_peer_proxy_links.items.len);
    try std.testing.expectEqual(@as(u32, 1), cap_returner.calls);

    link.forwarding = false;
    b_to_a.deinit();
    b_to_a_alive = false;
    link.b_to_a = null;

    try std.testing.expectEqual(@as(usize, 0), b_to_c.cross_peer_proxy_links.items.len);

    try b_to_c.releaseImport(counter_import_id, 1);
    try std.testing.expect(!b_to_c.caps.hasImport(counter_import_id));
}

// ============================================================================
// Issue #56 — OWNERSHIP regression: a forwarded parked call whose cross-peer
// send FAILS in post-callback work (AFTER VatC already returned and VatB
// already relayed the result to VatA) must NOT double-answer VatA's question
// nor double-free the relay ctx.
//
// This reproduces the exact hazard the ownership review flagged: in the
// synchronous loopback, `forwardVineReturn` runs nested inside
// `provide_peer.sendCall`, answers VatA, and frees the relay ctx; THEN
// `sendCall`'s own trailing work — sending the forwarded question's auto-Finish
// to VatC — can still fail. We inject that failure deterministically by making
// the B<->C link reject that Finish frame. The catch arm must recognize (via the
// settled flag) that VatA was already answered + the ctx already freed, and
// swallow the trailing error rather than send a SECOND (exception) Return or
// re-free the ctx.
//
// PROOF: VatA receives EXACTLY ONE Return for the pipelined call (its results),
// never a duplicate; the call still observes its correct value; no leak, no
// double-free (the test completing without a panic is the latter). Before the
// fix, the catch arm sent a second Return here (count == 2) and could double-free.
// ============================================================================
test "issue #56: forwarded parked call survives a post-callback send failure without double-answering VatA" {
    const allocator = std.testing.allocator;

    var link = Link.init(allocator);
    defer link.deinit();
    defer link.forwarding = false;

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var c = Peer.initDetached(allocator);
    c.disableThreadAffinity();
    defer c.deinit();

    var b_to_c = Peer.initDetached(allocator);
    b_to_c.disableThreadAffinity();
    defer b_to_c.deinit();
    var b_to_a = Peer.initDetached(allocator);
    b_to_a.disableThreadAffinity();
    defer b_to_a.deinit();

    var a_to_b = Peer.initDetached(allocator);
    a_to_b.disableThreadAffinity();
    defer a_to_b.deinit();
    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();

    b_to_c.next_question_id = 0;
    a_to_c.next_question_id = 1000;
    a_to_b.next_question_id = 2000;

    link.a_to_b = &a_to_b;
    link.b_to_a = &b_to_a;
    link.b_to_c = &b_to_c;
    link.c = &c;
    link.a_to_c = &a_to_c;

    a_to_b.setSendFrameOverride(&link, Link.aToBSend);
    b_to_a.setSendFrameOverride(&link, Link.bToASend);
    b_to_c.setSendFrameOverride(&link, Link.bToCSend);
    a_to_c.setSendFrameOverride(&link, Link.aToCSend);
    c.setSendFrameOverride(&link, Link.cSend);

    const recipient_nonce = "handoff-forward-fault-nonce-1";
    try net.register(recipient_nonce, &a_to_c);
    b_to_a.attachVatNetwork(net.network());
    a_to_c.attachVatNetwork(net.network());

    // C hosts a Counter; B imports it over B<->C.
    var counter = BumpCounter{};
    _ = try c.setBootstrap(.{ .ctx = &counter, .on_call = BumpCounter.onCall });
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const counter_import_id = carol_probe.carol_import_id orelse return error.CounterBootstrapFailed;

    // B serves a promise-returning Introducer; A imports promise P.
    var introducer = PromiseIntroducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = PromiseIntroducer.onCall });
    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse return error.IntroducerBootstrapFailed;

    var promise_probe = PromiseImportProbe{};
    _ = try a_to_b.sendCall(introducer_import_id, 0x1234_5678_9abc_def0, 0, &promise_probe, null, PromiseImportProbe.onReturn);
    const promise_import_id = promise_probe.promise_import_id orelse return error.PromiseNotImportedByA;

    // A pipelines a single bump(1) on the still-unresolved promise; it parks at B.
    var bump1 = BumpCall{ .n = 1 };
    _ = try a_to_b.sendCall(promise_import_id, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &bump1, BumpCall.build, BumpCall.onReturn);
    try std.testing.expect(!bump1.returned);
    try std.testing.expect(b_to_a.pending_export_promises.contains(promise_import_id));

    // *** Arm the fault: reject the forwarded question's Finish to C (fails
    //     sendCall's post-callback work), and count Returns B->A. ***
    link.fail_next_bToC_finish = true;
    link.count_bToA_returns = true;
    link.bToA_return_count = 0;

    const b_network = b_to_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&b_to_a, recipient_nonce);
    defer introduction.deinit(allocator);
    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = counter_import_id,
        .promised_answer = null,
    };

    // B hands P off to C. The parked bump(1) is forwarded to Counter; VatC
    // returns; VatB relays the result to VatA; THEN the forwarded question's
    // auto-Finish to C is rejected by the injected fault — failing sendCall
    // AFTER VatA was already answered. resolve() still completes (the forward's
    // error is reported non-fatally by the replay loop).
    const handle = try b_to_a.resolvePromiseExportToThirdParty(
        promise_import_id,
        &b_to_c,
        provided_target,
        recipient,
        introduction.to_contact,
    );

    // The injected Finish failure fired.
    try std.testing.expect(!link.fail_next_bToC_finish);

    // *** The parked call still reached Counter and returned its correct value. ***
    try std.testing.expect(bump1.returned);
    try std.testing.expect(!bump1.exception);
    try std.testing.expectEqual(@as(u32, 1), bump1.total.?);
    try std.testing.expectEqual(@as(u32, 1), counter.seq);

    // *** THE REGRESSION ASSERTION: VatA received EXACTLY ONE Return for the
    //     forwarded call — no duplicate (exception) Return from the mishandled
    //     post-callback failure. Before the ownership fix this was 2. ***
    try std.testing.expectEqual(@as(u32, 1), link.bToA_return_count);
    link.count_bToA_returns = false;

    // The vine coupling recorded the forwarding target and the parked table drained.
    const vine_id = handle.vine_id;
    try std.testing.expect(!b_to_a.pending_export_promises.contains(promise_import_id));
    try std.testing.expectEqual(
        @as(?u32, counter_import_id),
        b_to_a.outbound_provides.get(vine_id).?.provided_import_id,
    );

    // Teardown: release the vine (drives B's Provide Finish) then every import,
    // so all tables drain and std.testing.allocator sees no leak.
    try a_to_b.releaseImport(vine_id, 1);
    try std.testing.expect(!b_to_a.exports.contains(vine_id));
    try std.testing.expect(!b_to_a.outbound_provides.contains(vine_id));

    try b_to_c.releaseImport(counter_import_id, 1);
    try a_to_b.releaseImport(introducer_import_id, 1);
    try std.testing.expect(!b_to_c.caps.hasImport(counter_import_id));
    try std.testing.expect(!a_to_b.caps.hasImport(introducer_import_id));
    try std.testing.expectEqual(@as(usize, 0), b_to_c.coupled_vines.items.len);
}
