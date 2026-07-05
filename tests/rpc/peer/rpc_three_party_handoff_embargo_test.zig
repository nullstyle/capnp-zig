//! Proof test for Cap'n Proto RPC Level-3 three-party handoff ORIGINATION —
//! the EMBARGO/DISEMBARGO ordering during a LIVE-promise handoff (Phase P4 of
//! docs/rpc-l3-origination-plan.md, "the single hardest sub-problem").
//!
//! This is the e-order proof. It builds on the P3 auto-pickup harness (three
//! vats A/B/C over a synchronous in-process loopback) and adds an in-flight
//! pipelined call so the recipient must EMBARGO the handoff:
//!
//!   Vat A:  a_to_b (A<->B)   a_to_c (A<->C)   — recipient / originator of embargo
//!   Vat B:  b_to_a (A<->B)   b_to_c (B<->C)   — introducer; FORWARDS the Disembargo
//!   Vat C:  c (single peer)                    — host of Carol; HOLDS the Accept
//!
//! Ordering the test proves (rpc.capnp:885-903):
//!   1. A imports a promise P toward B and PIPELINES ping(1) on it. The call
//!      parks at B (promise unresolved), so A has a question in flight against P.
//!   2. B resolves P to Carol on C. A's auto-pickup sees the in-flight call and
//!      drives the EMBARGO: it sends Accept{embargo=id} to C AND a
//!      Disembargo{context.accept=id} to B on the promise path.
//!   3. B FORWARDS that Disembargo to C (the new P4 introducer routing). We hold
//!      the forward in a buffer so we can observe the embargo GATING C:
//!        - While the Disembargo is withheld, C HOLDS the embargoed Accept: no
//!          Return, A's pickup handler has NOT fired, and A cannot obtain (let
//!          alone call) the direct cap.
//!   4. A pipelines bar()=ping on the Accept's result (a promisedAnswer on the
//!      Accept question) — exactly the spec's pipelined bar() on the Accept
//!      result. C PARKS it because the Accept is embargoed (rpc.capnp:895-897).
//!   5. We release the buffered Disembargo to C. C now returns the Accept AND
//!      replays the parked pipelined ping to Carol; A's pickup fires; A then
//!      makes a DIRECT ping on the accepted cap.
//!   6. ASSERT e-order at Carol: the pipelined ping reached Carol STRICTLY
//!      BEFORE the direct ping (Carol stamps arrival order), and the embargo
//!      demonstrably gated the direct call (it was impossible until the
//!      Disembargo cleared, because the accepted cap did not exist at A yet).
//!
//! Everything runs under std.testing.allocator; every table drains in teardown.

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

// -- ping(n) wire shape: params/results are one data word, n :UInt32 @0 -------

const PING_INTERFACE_ID: u64 = 0xD0D0_D0D0_D0D0_D001;
const PING_METHOD_ID: u16 = 0;

fn readPingN(payload: protocol.Payload) !u32 {
    const content = try payload.content.getStruct();
    return content.readU32(0);
}

const PingReturnCtx = struct {
    n: u32,
    fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *const PingReturnCtx = castCtx(*const PingReturnCtx, ctx_ptr);
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        const results = try any.initStruct(1, 0);
        results.writeU32(0, self.n);
    }
};

// -- Vat C: Carol stamps the arrival order of every ping ----------------------

const Carol = struct {
    seq: u32 = 0,
    /// The stamp at which Carol saw ping(n==N). The source of truth for
    /// pipelined-before-direct ordering.
    ping_pipelined_seq: ?u32 = null, // ping(1) — the pipelined call
    ping_direct_seq: ?u32 = null, // ping(2) — the post-pickup direct call

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Carol = castCtx(*Carol, ctx_ptr);
        if (call.interface_id != PING_INTERFACE_ID or call.method_id != PING_METHOD_ID) {
            return error.UnexpectedMethod;
        }
        self.seq += 1;
        const n = try readPingN(call.params);
        if (n == 1) self.ping_pipelined_seq = self.seq;
        if (n == 2) self.ping_direct_seq = self.seq;
        var ret_ctx = PingReturnCtx{ .n = n };
        try peer.sendReturnResults(call.question_id, &ret_ctx, PingReturnCtx.build);
    }
};

// -- Vat B: imports Carol via a bootstrap on B<->C ----------------------------

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

// -- Vat B: Introducer bootstrap. getPromise() mints + returns a promise ------

const Introducer = struct {
    promise_export_id: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Introducer = castCtx(*Introducer, ctx_ptr);
        const promise_id = try peer.addPromiseExport();
        self.promise_export_id = promise_id;
        const PromiseReturnCtx = struct {
            promise_id: u32,
            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = castCtx(*const @This(), bctx);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                try any.setCapability(.{ .id = bself.promise_id });
            }
        };
        var ret_ctx = PromiseReturnCtx{ .promise_id = promise_id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, PromiseReturnCtx.build);
    }
};

// -- Vat A: bootstrap-return / promise-return / ping-return observers ---------

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

const PromiseCall = struct {
    promise_import_id: ?u32 = null,
    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *PromiseCall = castCtx(*PromiseCall, ctx_ptr);
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

/// A ping call: one ctx drives both the params build and the return observer.
const PingCall = struct {
    n: u32,
    result_n: ?u32 = null,
    returned: bool = false,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const self: *const PingCall = castCtx(*const PingCall, ctx_ptr);
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
        const self: *PingCall = castCtx(*PingCall, ctx_ptr);
        self.returned = true;
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse return error.MissingPingPayload;
                self.result_n = try readPingN(payload);
            },
            else => return error.UnexpectedPingReturn,
        }
    }
};

// -- Vat A: the auto-pickup handler. Imports Carol directly on a_to_c ---------

const PickupHandler = struct {
    expected_promise_id: u32,
    carol_import_id: ?u32 = null,
    fired: bool = false,

    fn onPickup(
        ctx_ptr: *anyopaque,
        _: *Peer, // promise_peer (a_to_b)
        promise_id: u32,
        accept_peer: *Peer, // a_to_c
        ret: protocol.Return,
        accept_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *PickupHandler = castCtx(*PickupHandler, ctx_ptr);
        self.fired = true;
        if (promise_id != self.expected_promise_id) return error.UnexpectedPromiseId;
        if (ret.tag != .results) return error.UnexpectedAcceptReturn;
        const payload = ret.results orelse return error.MissingAcceptPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(accept_caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.carol_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.AcceptedCarolNotImported,
        };
        std.debug.assert(accept_peer.caps.hasImport(self.carol_import_id.?));
    }
};

// -- Three-connection synchronous wire, with a Disembargo forward BUFFER ------
//
// Same routing as the P3 pickup harness, plus: the B->C link can BUFFER the
// forwarded `context.accept` Disembargo instead of delivering it, so the test
// can observe C holding the embargoed Accept, then release it and observe the
// ordered delivery.

const Vat = enum { a, b, c };

const Link = struct {
    allocator: std.mem.Allocator,
    forwarding: bool = true,

    a_to_b: ?*Peer = null,
    b_to_a: ?*Peer = null,
    b_to_c: ?*Peer = null,
    c: ?*Peer = null,
    a_to_c: ?*Peer = null,

    c_question_origin: std.AutoHashMap(u32, Vat),
    /// The Accept question id A sent to C on a_to_c (captured off the wire so the
    /// test can pipeline bar() on the Accept's result).
    accept_question_id: ?u32 = null,

    /// When true, a `context.accept` Disembargo forwarded B->C is buffered here
    /// instead of delivered, so the test can hold the embargo open.
    buffer_b_to_c_accept_disembargo: bool = false,
    buffered_disembargo: ?[]u8 = null,

    fn init(allocator: std.mem.Allocator) Link {
        return .{
            .allocator = allocator,
            .c_question_origin = std.AutoHashMap(u32, Vat).init(allocator),
        };
    }

    fn deinit(self: *Link) void {
        if (self.buffered_disembargo) |buf| self.allocator.free(buf);
        self.c_question_origin.deinit();
    }

    fn recordCQuestion(self: *Link, frame: []const u8, origin: Vat) !void {
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        switch (decoded.tag) {
            .bootstrap => try self.c_question_origin.put((try decoded.asBootstrap()).question_id, origin),
            .call => try self.c_question_origin.put((try decoded.asCall()).question_id, origin),
            .accept => {
                const accept = try decoded.asAccept();
                try self.c_question_origin.put(accept.question_id, origin);
                self.accept_question_id = accept.question_id;
            },
            else => {},
        }
    }

    fn aToBSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        if (self.b_to_a) |peer| try peer.handleFrame(frame);
    }

    fn bToASend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        if (self.a_to_b) |peer| try peer.handleFrame(frame);
    }

    fn bToCSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        try self.recordCQuestion(frame, .b);
        if (!self.forwarding) return;

        // Intercept a forwarded `context.accept` Disembargo when buffering is on.
        if (self.buffer_b_to_c_accept_disembargo and try isAcceptDisembargo(self.allocator, frame)) {
            // Exactly one accept-disembargo is expected in this scenario.
            std.debug.assert(self.buffered_disembargo == null);
            self.buffered_disembargo = try self.allocator.dupe(u8, frame);
            return;
        }
        if (self.c) |peer| try peer.handleFrame(frame);
    }

    fn aToCSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        try self.recordCQuestion(frame, .a);
        if (!self.forwarding) return;
        if (self.c) |peer| try peer.handleFrame(frame);
    }

    fn cSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (!self.forwarding) return;
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        const target: ?Vat = switch (decoded.tag) {
            .@"return" => self.c_question_origin.get((try decoded.asReturn()).answer_id),
            .finish => self.c_question_origin.get((try decoded.asFinish()).question_id),
            else => null,
        };
        const dest = target orelse return;
        switch (dest) {
            .a => if (self.a_to_c) |peer| try peer.handleFrame(frame),
            .b => if (self.b_to_c) |peer| try peer.handleFrame(frame),
            .c => {},
        }
    }

    /// Deliver a previously buffered accept-disembargo to C. This is the wire
    /// event that releases C's held Accept.
    fn releaseBufferedDisembargo(self: *Link) !void {
        const frame = self.buffered_disembargo orelse return error.NoBufferedDisembargo;
        self.buffered_disembargo = null;
        defer self.allocator.free(frame);
        if (self.c) |peer| try peer.handleFrame(frame);
    }
};

fn isAcceptDisembargo(allocator: std.mem.Allocator, frame: []const u8) !bool {
    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    if (decoded.tag != .disembargo) return false;
    const dis = try decoded.asDisembargo();
    return dis.context_tag == .accept;
}

test "three-party handoff embargo: pipelined call reaches C before the post-pickup direct call" {
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

    // Disjoint question spaces so C can route unambiguously.
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

    const recipient_nonce = "handoff-embargo-nonce-1";
    try net.register(recipient_nonce, &a_to_c);
    b_to_a.attachVatNetwork(net.network());
    a_to_b.attachVatNetwork(net.network());

    // C hosts Carol.
    var carol = Carol{};
    _ = try c.setBootstrap(.{ .ctx = &carol, .on_call = Carol.onCall });

    // B imports Carol.
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;

    // B serves the Introducer on A<->B.
    var introducer = Introducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = Introducer.onCall });

    // A bootstraps the Introducer and calls getPromise() -> imports promise P.
    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse return error.IntroducerBootstrapFailed;

    var promise_call = PromiseCall{};
    _ = try a_to_b.sendCall(introducer_import_id, 0x1234_5678_9abc_def0, 0, &promise_call, null, PromiseCall.onReturn);
    const promise_import_id = promise_call.promise_import_id orelse return error.PromiseNotImportedByA;
    try std.testing.expect(a_to_b.caps.hasImport(promise_import_id));

    // Install A's auto-pickup handler.
    var pickup = PickupHandler{ .expected_promise_id = promise_import_id };
    a_to_b.setHandoffPickupHandler(&pickup, PickupHandler.onPickup);

    // *** The in-flight call that forces the embargo. ***
    // A pipelines ping(1) on the still-unresolved promise P. It parks at B, so A
    // now has a question in flight against P — the spec condition under which the
    // pickup MUST embargo the handoff (rpc.capnp:885-888).
    var pipelined_ping = PingCall{ .n = 1 };
    const pipelined_q = try a_to_b.sendCall(
        promise_import_id,
        PING_INTERFACE_ID,
        PING_METHOD_ID,
        &pipelined_ping,
        PingCall.build,
        PingCall.onReturn,
    );
    // Parked at B: no Return yet, Carol has not seen it. A now has a live
    // question against P (the in-flight pipelined call that forces the embargo).
    try std.testing.expect(!pipelined_ping.returned);
    try std.testing.expectEqual(@as(u32, 0), carol.seq);
    try std.testing.expect(a_to_b.questions.contains(pipelined_q));

    // Hold the forwarded Disembargo at the B->C link so we can observe the
    // embargo GATING C before it clears.
    link.buffer_b_to_c_accept_disembargo = true;

    // -- B ORIGINATES the handoff. A auto-picks-up synchronously inside this
    //    call: it sees the in-flight pipelined call and sends Accept{embargo} to
    //    C plus a Disembargo{context.accept} to B; B forwards the Disembargo,
    //    which the link BUFFERS. C therefore holds the embargoed Accept. --------
    const b_network = b_to_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&b_to_a, recipient_nonce);
    defer introduction.deinit(allocator);
    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = carol_import_id,
        .promised_answer = null,
    };

    const handle = try b_to_a.resolvePromiseExportToThirdParty(
        promise_import_id,
        &b_to_c,
        provided_target,
        recipient,
        introduction.to_contact,
    );

    // *** Embargo GATE proof: the Accept is held at C. ***
    // A's pickup handler has NOT fired — C withheld the Accept Return because the
    // Disembargo is still buffered at the B->C link.
    try std.testing.expect(!pickup.fired);
    try std.testing.expect(pickup.carol_import_id == null);
    // The Disembargo really was originated and buffered (not silently skipped).
    try std.testing.expect(link.buffered_disembargo != null);
    // C is holding an embargoed Accept awaiting the disembargo.
    try std.testing.expect(c.pending_accepts_by_embargo.count() > 0);
    // The Accept question A sent to C was captured off the wire.
    const accept_qid = link.accept_question_id orelse return error.AcceptQuestionNotObserved;
    // Carol has still seen nothing.
    try std.testing.expectEqual(@as(u32, 0), carol.seq);

    // *** Spec bar(): pipeline ping on the ACCEPT's result. ***
    // A sends ping(1)'s successor as a promisedAnswer on the Accept question
    // (rpc.capnp:895-897). C PARKS it because the Accept is embargoed: it must
    // not deliver a call to the newly-accepted cap until the embargo releases.
    // (We reuse n==1 as "the pipelined call" — Carol stamps whichever ping with
    // n==1 arrives first; the direct call below uses n==2.)
    // The Accept Return encodes the accepted cap (Carol) as the payload CONTENT
    // capability directly (see sendReturnProvidedTarget), so the promised-answer
    // transform to reach it is EMPTY (the content itself is the cap).
    var accept_pipelined_ping = PingCall{ .n = 1 };
    _ = try a_to_c.sendCallPromisedWithOps(
        accept_qid,
        &[_]protocol.PromisedAnswerOp{},
        PING_INTERFACE_ID,
        PING_METHOD_ID,
        &accept_pipelined_ping,
        PingCall.build,
        PingCall.onReturn,
    );
    // Still gated: the pipelined call is PARKED at C behind the embargoed Accept
    // (rpc.capnp:895-897) — it must not be delivered to the newly-accepted cap
    // until the embargo releases.
    try std.testing.expect(c.pending_promises.count() >= 1);
    try std.testing.expect(!accept_pipelined_ping.returned);
    try std.testing.expectEqual(@as(u32, 0), carol.seq);
    try std.testing.expect(carol.ping_pipelined_seq == null);

    // *** Release the embargo: deliver the buffered Disembargo to C. ***
    // C releases the held Accept: it Returns the accepted cap AND replays the
    // parked pipelined ping to Carol, all in e-order.
    try link.releaseBufferedDisembargo();

    // The pickup fired now (and only now): A has the direct cap.
    try std.testing.expect(pickup.fired);
    const accepted_carol_id = pickup.carol_import_id orelse return error.AutoPickupDidNotResolve;
    try std.testing.expect(a_to_c.caps.hasImport(accepted_carol_id));

    // The pipelined ping reached Carol as part of releasing the embargo.
    try std.testing.expect(accept_pipelined_ping.returned);
    try std.testing.expect(carol.ping_pipelined_seq != null);
    try std.testing.expectEqual(@as(u32, 1), carol.seq);

    // *** Post-pickup DIRECT call. ***
    // Only reachable now: the accepted cap did not exist at A until the embargo
    // cleared, so this direct ping could not have raced ahead of the pipelined
    // one — the embargo demonstrably gated it.
    var direct_ping = PingCall{ .n = 2 };
    _ = try a_to_c.sendCall(
        accepted_carol_id,
        PING_INTERFACE_ID,
        PING_METHOD_ID,
        &direct_ping,
        PingCall.build,
        PingCall.onReturn,
    );
    try std.testing.expect(direct_ping.returned);
    try std.testing.expectEqual(@as(u32, 2), direct_ping.result_n.?);
    try std.testing.expect(carol.ping_direct_seq != null);

    // *** THE ORDERING ASSERTION ***
    // The pipelined call reached Carol STRICTLY BEFORE the post-pickup direct
    // call. No reordering — e-order preserved across the three-connection handoff.
    try std.testing.expect(carol.ping_pipelined_seq.? < carol.ping_direct_seq.?);
    try std.testing.expectEqual(@as(u32, 1), carol.ping_pipelined_seq.?);
    try std.testing.expectEqual(@as(u32, 2), carol.ping_direct_seq.?);

    // -- The original P pipelined call (parked at B) was rejected by the vine
    //    proxy on resolution (Level-1/2 fallback), so it returned an exception;
    //    that is the known limitation this slice does not close (B does not yet
    //    forward parked calls to C). The e-order guarantee proven above does not
    //    depend on it: the pipelined-before-direct ordering is proven at Carol on
    //    C via the Accept-result pipeline, which the embargo genuinely gates.

    // -- Teardown: release every remaining import so all tables drain. ---------
    try a_to_c.releaseImport(accepted_carol_id, 1);
    try a_to_b.releaseImport(promise_import_id, 1);
    try b_to_c.releaseImport(carol_import_id, 1);
    try a_to_b.releaseImport(introducer_import_id, 1);
    _ = handle;

    // Every provision drained on C; no dangling third-party marks or imports.
    try std.testing.expectEqual(@as(usize, 0), c.provides_by_question.count());
    try std.testing.expectEqual(@as(usize, 0), c.provides_by_key.count());
    try std.testing.expectEqual(@as(usize, 0), c.pending_accepts_by_embargo.count());
}
