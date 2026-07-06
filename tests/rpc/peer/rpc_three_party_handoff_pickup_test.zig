//! Proof test for Cap'n Proto RPC Level-3 three-party handoff ORIGINATION —
//! the AUTOMATIC resolve->handoff->pickup slice (Phase P3 of
//! docs/rpc-l3-origination-plan.md). Builds on the P0+P1+P2 Provide/Accept
//! machinery but proves the recipient side runs UNATTENDED: no manual
//! `sendAccept` appears anywhere in the test body — the runtime connects to the
//! third vat, Accepts, and hands the direct cap to A's pickup handler.
//! No embargo/disembargo (P4): the handoff carries no in-flight-promise ordering.
//!
//! Three vats — A (recipient), B (introducer), C (host of the provided cap) —
//! are wired over a synchronous in-process loopback, one `Peer` endpoint per
//! connection (a `Peer` binds to exactly one remote):
//!
//!   Vat A:  a_to_b  (A<->B)   a_to_c  (A<->C)
//!   Vat B:  b_to_a  (A<->B)   b_to_c  (B<->C)
//!   Vat C:  c        (single peer; serves B's Provide AND A's auto-Accept)
//!
//! Scenario:
//!   1. C hosts Carol, a `getNumber()` cap returning 42.
//!   2. B imports Carol via a bootstrap on B<->C.
//!   3. B's bootstrap (on A<->B) serves `getPromise()`: it mints a PROMISE
//!      export and returns it, so A imports a promise cap toward B.
//!   4. B originates the handoff: `resolvePromiseExportToThirdParty(...)`
//!      resolves that promise export to Carol on C. This sends a `Provide` to C
//!      (held open) and a `Resolve{ thirdPartyHosted{ contact, vineId } }` to A.
//!   5. AUTO-PICKUP: A's `handleResolve` sees the thirdPartyHosted descriptor,
//!      consults its attached VatNetwork (connectToIntroduced -> a_to_c), sends
//!      an `Accept` to C on a_to_c WITHOUT any test code doing so, and — when C
//!      Returns Carol — invokes A's pickup handler with the DIRECT cap. A retains
//!      Carol on a_to_c and the runtime releases the vine.
//!   6. A calls getNumber() on its DIRECT import of Carol -> 42 (proves a real
//!      working cap, not a vine proxy).
//!   7. The vine release drove B's Provide Finish; every table drains under
//!      std.testing.allocator (any leak fails the test).

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
// `getPromise()` mints a PROMISE export and returns it as the result cap, so A
// imports a promise toward B. The test later drives B to resolve that promise to
// C's cap via `resolvePromiseExportToThirdParty`.

const Introducer = struct {
    promise_export_id: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Introducer = castCtx(*Introducer, ctx_ptr);
        // The call arrives on b_to_a (A<->B); `peer` is b_to_a.
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

// -- Vat A: the getPromise() Return — imports the promise cap toward B --------

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

// -- Vat A: the AUTO-PICKUP handler ------------------------------------------
//
// Invoked by the runtime when the auto-Accept's Return arrives. Imports Carol
// DIRECTLY on the third-vat peer (a_to_c) and records her id. No test code sends
// the Accept — this handler is the only place the accepted cap surfaces.

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
        // Sanity: the accepted cap lives on the third-vat connection.
        std.debug.assert(accept_peer.caps.hasImport(self.carol_import_id.?));
    }
};

const FailingPickupHandler = struct {
    expected_promise_id: u32,
    accepted_import_id: ?u32 = null,
    fired: bool = false,

    fn onPickup(
        ctx_ptr: *anyopaque,
        _: *Peer,
        promise_id: u32,
        accept_peer: *Peer,
        ret: protocol.Return,
        accept_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *FailingPickupHandler = castCtx(*FailingPickupHandler, ctx_ptr);
        self.fired = true;
        if (promise_id != self.expected_promise_id) return error.UnexpectedPromiseId;
        if (ret.tag != .results) return error.UnexpectedAcceptReturn;
        const payload = ret.results orelse return error.MissingAcceptPayload;
        const cap = try payload.content.getCapability();
        const resolved = try accept_caps.resolveCapability(cap);
        self.accepted_import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.AcceptedCarolNotImported,
        };
        try std.testing.expect(accept_peer.caps.hasImport(self.accepted_import_id.?));
        // Deliberately do NOT retain the cap. The runtime must release this
        // unretained Accept result even though the app callback fails.
        return error.IntentionalPickupFailure;
    }
};

// -- Three-connection synchronous wire ---------------------------------------
//
// Identical routing to the Provide+Accept proof: A<->B and B<->C forward to the
// paired endpoint; Vat C's single peer routes each Return/Finish back to the vat
// that owns the question it answers (A and B use disjoint id ranges).

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
    fail_finish_question_id: ?u32 = null,
    finish_failures_remaining: u32 = 0,
    finish_failures: u32 = 0,

    fn init(allocator: std.mem.Allocator) Link {
        return .{
            .allocator = allocator,
            .c_question_origin = std.AutoHashMap(u32, Vat).init(allocator),
        };
    }

    fn maybeFailFinish(self: *Link, frame: []const u8) !bool {
        if (self.finish_failures_remaining == 0) return false;
        const fail_question_id = self.fail_finish_question_id orelse return false;
        var decoded = protocol.DecodedMessage.init(self.allocator, frame) catch return false;
        defer decoded.deinit();
        if (decoded.tag != .finish) return false;
        const finish = try decoded.asFinish();
        if (finish.question_id != fail_question_id) return false;
        self.finish_failures_remaining -= 1;
        self.finish_failures += 1;
        return true;
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
        if (self.c) |peer| try peer.handleFrame(frame);
    }

    fn aToCSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *Link = castCtx(*Link, ctx);
        if (try self.maybeFailFinish(frame)) return error.OutOfMemory;
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
        const dest = target orelse return; // unrouted control frame: drop safely
        switch (dest) {
            .a => if (self.a_to_c) |peer| try peer.handleFrame(frame),
            .b => if (self.b_to_c) |peer| try peer.handleFrame(frame),
            .c => {},
        }
    }
};

test "three-party handoff origination: automatic resolve->pickup hands C's cap to A directly" {
    const allocator = std.testing.allocator;

    var link = Link.init(allocator);
    defer link.deinit();
    // Stop forwarding before any peer.deinit (LIFO) so teardown frames are not
    // delivered into a half-torn-down peer.
    defer link.forwarding = false;

    // Loopback vat network: nonce -> the peer A uses to reach C (a_to_c).
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    // -- Vat C: a single peer serving both B's Provide and A's auto-Accept. ---
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
    link.fail_finish_question_id = 1000;
    link.finish_failures_remaining = 1;

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
    const recipient_nonce = "handoff-pickup-nonce-1";
    try net.register(recipient_nonce, &a_to_c);

    // B originates on b_to_a; A auto-picks-up on a_to_b (which consults the
    // network to reach a_to_c).
    b_to_a.attachVatNetwork(net.network());
    a_to_b.attachVatNetwork(net.network());

    // -- C hosts Carol as its bootstrap. --------------------------------------
    var carol = Carol{};
    const carol_export_id = try c.setBootstrap(.{ .ctx = &carol, .on_call = Carol.onCall });

    // -- (1) B imports Carol via a bootstrap on B<->C. ------------------------
    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;
    try std.testing.expectEqual(carol_export_id, carol_import_id);

    // -- B's Introducer bootstrap service on the A<->B connection. ------------
    var introducer = Introducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = Introducer.onCall });

    // -- (2) A bootstraps the Introducer. -------------------------------------
    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse
        return error.IntroducerBootstrapFailed;

    // -- (3) A calls getPromise(): B mints + returns a promise export. --------
    var promise_call = PromiseCall{};
    _ = try a_to_b.sendCall(
        introducer_import_id,
        0x1234_5678_9abc_def0, // Introducer interface id (unused by handler)
        0, // getPromise method id
        &promise_call,
        null,
        PromiseCall.onReturn,
    );
    const promise_export_id = introducer.promise_export_id orelse return error.PromiseNotMinted;
    const promise_import_id = promise_call.promise_import_id orelse return error.PromiseNotImportedByA;
    // A's import id equals B's promise export id (senderPromise descriptor).
    try std.testing.expectEqual(promise_export_id, promise_import_id);
    try harness.expectImport(&a_to_b, promise_import_id);

    // -- Install A's AUTO-PICKUP handler on the promise-holding peer. ---------
    var pickup = PickupHandler{ .expected_promise_id = promise_import_id };
    a_to_b.setHandoffPickupHandler(&pickup, PickupHandler.onPickup);

    // -- (4) B ORIGINATES the handoff by resolving the promise export to C. ---
    // Mint the introduction tokens naming A as the recipient.
    const b_network = b_to_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&b_to_a, recipient_nonce);
    defer introduction.deinit(allocator);

    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    // Provide's target names Carol on C (importedCap == Carol's id on B<->C).
    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = carol_import_id,
        .promised_answer = null,
    };

    // This single call sends the Provide (to C, held open) AND the
    // thirdPartyHosted Resolve (to A). A auto-picks-up synchronously inside it:
    // connects to C, Accepts, imports Carol, releases the vine.
    const handle = try b_to_a.resolvePromiseExportToThirdParty(
        promise_import_id, // == promise_export_id on b_to_a
        &b_to_c,
        provided_target,
        recipient,
        introduction.to_contact,
    );

    // The pickup handler fired WITHOUT any manual sendAccept in this test.
    try std.testing.expect(pickup.fired);
    const accepted_carol_id = pickup.carol_import_id orelse return error.AutoPickupDidNotResolve;
    try harness.expectImport(&a_to_c, accepted_carol_id);
    try std.testing.expectEqual(@as(u32, 1), link.finish_failures);
    try std.testing.expect(!c.resolved_answers.contains(1000));

    // The vine was minted, handed to A, released by the runtime after pickup, and
    // its release Finished B's held-open Provide — so the vine export is gone and
    // C's provision tables have fully drained.
    try std.testing.expect(!b_to_a.exports.contains(handle.vine_id));
    try harness.expectNoOutboundProvide(&b_to_a, handle.vine_id);
    try std.testing.expect(!c.provides_by_question.contains(handle.question_id));
    try harness.expectNoProvideState(&c);
    // A holds no resolved-import entry for the promise (it was fulfilled off-peer
    // via the direct pickup, not via the vine proxy fallback).
    try std.testing.expect(!a_to_b.resolved_imports.contains(promise_import_id));
    // The vine import A briefly held is gone (released to drive the Finish).
    try harness.expectNoImport(&a_to_b, handle.vine_id);

    // -- (5) A calls getNumber() on its DIRECT import of Carol -> 42. ---------
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

    // -- Teardown: release every remaining import so all tables drain. --------
    // A's direct Carol import (retained by the pickup handler).
    try a_to_c.releaseImport(accepted_carol_id, 1);
    // A's promise import toward B (retained by PromiseCall). Its resolution
    // target (the vine) was already cleared on vine teardown, so this is clean.
    try a_to_b.releaseImport(promise_import_id, 1);
    // B's Carol import (retained by CarolImportProbe).
    try b_to_c.releaseImport(carol_import_id, 1);
    // A's Introducer import (retained by IntroducerProbe). B's Introducer
    // bootstrap export persists by design.
    try a_to_b.releaseImport(introducer_import_id, 1);

    // Everything drains: no vine, no provisions, no dangling imports.
    try harness.expectNoImport(&a_to_c, accepted_carol_id);
    try harness.expectNoImport(&a_to_b, promise_import_id);
    try harness.expectNoImport(&b_to_c, carol_import_id);
    try harness.expectNoImport(&a_to_b, introducer_import_id);
    // C holds no lingering imports and no third-party-hosted marks.
    try std.testing.expectEqual(@as(u32, 0), @as(u32, @intCast(c.caps.imports.count())));
}

test "three-party handoff auto-pickup callback failure releases vine and accepted cap" {
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

    const recipient_nonce = "handoff-pickup-fail-nonce-1";
    try net.register(recipient_nonce, &a_to_c);
    b_to_a.attachVatNetwork(net.network());
    a_to_b.attachVatNetwork(net.network());

    var carol = Carol{};
    _ = try c.setBootstrap(.{ .ctx = &carol, .on_call = Carol.onCall });

    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;

    var introducer = Introducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = Introducer.onCall });

    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse
        return error.IntroducerBootstrapFailed;

    var promise_call = PromiseCall{};
    _ = try a_to_b.sendCall(
        introducer_import_id,
        0x1234_5678_9abc_def0,
        0,
        &promise_call,
        null,
        PromiseCall.onReturn,
    );
    const promise_import_id = promise_call.promise_import_id orelse return error.PromiseNotImportedByA;

    var pickup = FailingPickupHandler{ .expected_promise_id = promise_import_id };
    a_to_b.setHandoffPickupHandler(&pickup, FailingPickupHandler.onPickup);

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

    try std.testing.expect(pickup.fired);
    const accepted_import_id = pickup.accepted_import_id orelse return error.PickupDidNotSeeAcceptedCap;

    // The app callback failed after observing the Accept Return, but it did not
    // retain the direct cap. The runtime must release that unretained import,
    // release the vine, Finish the held-open Provide, and not restore the freed
    // pickup ctx as a still-live question.
    try harness.expectNoImport(&a_to_c, accepted_import_id);
    try harness.expectNoImport(&a_to_b, handle.vine_id);
    try std.testing.expect(!a_to_c.questions.contains(1000));
    try std.testing.expect(!b_to_a.exports.contains(handle.vine_id));
    try harness.expectNoOutboundProvide(&b_to_a, handle.vine_id);
    try std.testing.expect(!b_to_c.questions.contains(handle.question_id));
    try std.testing.expect(!c.provides_by_question.contains(handle.question_id));
    try harness.expectNoProvideState(&c);

    // Teardown of the long-lived imports that the test intentionally retained.
    try a_to_b.releaseImport(promise_import_id, 1);
    try b_to_c.releaseImport(carol_import_id, 1);
    try a_to_b.releaseImport(introducer_import_id, 1);
    try harness.expectNoImport(&a_to_b, promise_import_id);
    try harness.expectNoImport(&b_to_c, carol_import_id);
    try harness.expectNoImport(&a_to_b, introducer_import_id);
    try std.testing.expectEqual(@as(u32, 0), @as(u32, @intCast(c.caps.imports.count())));
}

// -- Fallback proof: a recipient WITHOUT the auto-pickup seams keeps the ------
//    Level-1/2 proxy-via-vine behavior unchanged. ------------------------------

const FallbackIntroducer = struct {
    promise_export_id: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *FallbackIntroducer = castCtx(*FallbackIntroducer, ctx_ptr);
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

const FallbackPromiseCall = struct {
    promise_import_id: ?u32 = null,
    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *FallbackPromiseCall = castCtx(*FallbackPromiseCall, ctx_ptr);
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

test "three-party handoff: recipient without vat_network falls back to the vine proxy" {
    const allocator = std.testing.allocator;

    var link = Link.init(allocator);
    defer link.deinit();
    defer link.forwarding = false;

    // Loopback network is attached to B (the originator) only. A has NONE, so it
    // must take the Level-1/2 proxy-via-vine fallback on the inbound Resolve.
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

    const recipient_nonce = "handoff-fallback-nonce-1";
    try net.register(recipient_nonce, &a_to_c);
    // ONLY B gets the network; A deliberately gets none (and no pickup handler).
    b_to_a.attachVatNetwork(net.network());

    var carol = Carol{};
    _ = try c.setBootstrap(.{ .ctx = &carol, .on_call = Carol.onCall });

    var carol_probe = CarolImportProbe{};
    _ = try b_to_c.sendBootstrap(&carol_probe, CarolImportProbe.onReturn);
    const carol_import_id = carol_probe.carol_import_id orelse return error.CarolBootstrapFailed;

    var introducer = FallbackIntroducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = FallbackIntroducer.onCall });

    var introducer_probe = IntroducerProbe{};
    _ = try a_to_b.sendBootstrap(&introducer_probe, IntroducerProbe.onReturn);
    const introducer_import_id = introducer_probe.introducer_import_id orelse
        return error.IntroducerBootstrapFailed;

    var promise_call = FallbackPromiseCall{};
    _ = try a_to_b.sendCall(introducer_import_id, 0x1234_5678_9abc_def0, 0, &promise_call, null, FallbackPromiseCall.onReturn);
    const promise_import_id = promise_call.promise_import_id orelse return error.PromiseNotImportedByA;

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

    // A had no vat_network/pickup handler: the promise resolved to the VINE
    // import (Level-1/2 proxy fallback), NOT an auto-pickup. No Accept was sent,
    // so C's provision stays held open and the vine import is live on A.
    const resolved = a_to_b.resolved_imports.get(promise_import_id) orelse return error.PromiseNotResolved;
    try std.testing.expect(resolved.cap != null);
    switch (resolved.cap.?) {
        .imported => |cap| try std.testing.expectEqual(handle.vine_id, cap.id),
        else => return error.ExpectedVineImport,
    }
    try harness.expectImport(&a_to_b, handle.vine_id);
    // Provision still held open on C (no Accept, no Finish).
    try std.testing.expect(c.provides_by_question.contains(handle.question_id));
    // The vine export is still live on B (A has not released it).
    try std.testing.expect(b_to_a.exports.contains(handle.vine_id));

    // Releasing the vine drives B's Provide Finish, exactly as in the P0-P2 path.
    // Two references land on the vine import in this fallback: the promise's
    // resolved-import entry and the raw import ref. releaseResolvedImport (fired
    // when the promise import is released in teardown) drops the resolved-import
    // reference; here we drop the wire ref, which drives B's Finish.
    try a_to_b.releaseImport(handle.vine_id, 1);
    try std.testing.expect(!b_to_a.exports.contains(handle.vine_id));
    try std.testing.expect(!c.provides_by_question.contains(handle.question_id));
    try harness.expectNoOutboundProvide(&b_to_a, handle.vine_id);

    // Teardown.
    try a_to_b.releaseImport(promise_import_id, 1);
    try b_to_c.releaseImport(carol_import_id, 1);
    try a_to_b.releaseImport(introducer_import_id, 1);
    try std.testing.expectEqual(@as(u32, 0), @as(u32, @intCast(c.caps.imports.count())));
}
