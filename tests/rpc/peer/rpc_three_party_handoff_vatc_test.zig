//! L3 of the VatC hosting plan: a vat whose Provide and Accept arrive on
//! DIFFERENT peers can finally serve the handoff.
//!
//! The real deployment topology (rpc.capnp:882 vs :887): the introducer's
//! `Provide` lands on the introducer<->host connection while the recipient's
//! `Accept` lands on the recipient<->host connection — different `Peer`
//! objects of one vat. Per-peer provide tables cannot match them; before this
//! landing a real two-peer VatC answered every such Accept with
//! `"unknown provision"` (kept below as a permanent no-index witness). The
//! `ProvisionIndex` gives the vat's peers a shared rendezvous of refcounted,
//! connection-independent provision objects; the accepted capability is served
//! as a proxy export on the accept peer, pinned to the owner's export by the
//! Release-immune handoff ref class.
//!
//! Topology (six real peers, three synchronous pairwise links — no question-id
//! offsets, no routing maps):
//!   Vat A: a_to_b, a_to_c      Vat B: b_to_a, b_to_c      Vat C: c_to_b, c_to_a

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const vat_network = capnpc.rpc.vat.network;
const rpc_time = capnpc.rpc.time;
const message = capnpc.message;
const Peer = peer_impl.Peer;
const ProvisionIndex = peer_impl.ProvisionIndex;
const harness = @import("three_party_handoff_harness.zig");

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

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

// -- Vat C: Carol (NOT the bootstrap — she holds a real Return-carried wire
// -- ref, so destroy paths have no bootstrap early-return to hide behind). ---

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

/// Bootstrap service returning a pre-existing export id in its results.
const CapReturner = struct {
    export_id: u32,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *CapReturner = castCtx(*CapReturner, ctx_ptr);
        const ReturnCtx = struct {
            id: u32,
            fn build(bctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const bself: *const @This() = castCtx(*const @This(), bctx);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                try any.setCapability(.{ .id = bself.id });
            }
        };
        var ret_ctx = ReturnCtx{ .id = self.export_id };
        try peer.sendReturnResults(call.question_id, &ret_ctx, ReturnCtx.build);
    }
};

/// Return observer that resolves + retains a single returned cap.
const CapImportProbe = struct {
    import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *CapImportProbe = castCtx(*CapImportProbe, ctx_ptr);
        if (ret.tag != .results) return error.UnexpectedReturn;
        const payload = ret.results orelse return error.MissingPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.import_id = switch (resolved) {
            .imported => |imp| imp.id,
            else => return error.CapNotImported,
        };
    }
};

// -- Vat B: Introducer bootstrap (getPromise mints + returns a promise) ------

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

/// A call whose Return we tolerate either way (used for the pipelined call
/// that forces an embargo).
const TolerantCall = struct {
    returned: bool = false,
    ret_tag: ?protocol.ReturnTag = null,
    result_n: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *TolerantCall = castCtx(*TolerantCall, ctx_ptr);
        self.returned = true;
        self.ret_tag = ret.tag;
        if (ret.tag == .results) {
            const payload = ret.results orelse return error.MissingPayload;
            self.result_n = try readNumberN(payload);
        }
    }
};

// -- Vat A: the MODIFIED pickup handler --------------------------------------
//
// `onHandoffAcceptReturn` swallows this callback's error, so every observable
// must be captured into fields: the Return tag, a DUPED exception reason
// (Return memory is only valid during the callback), and — because under a
// synchronous transport the whole handoff completes inside the origination
// call — the mid-flight VatC state, probed HERE, before the vine release
// finishes the Provide.

const VatcPickupHandler = struct {
    allocator: std.mem.Allocator,
    expected_promise_id: u32,
    /// Optional mid-flight probes (set when the test wants them).
    c_owner: ?*Peer = null,
    c_accept: ?*Peer = null,
    carol_export_id: ?u32 = null,

    /// When set, `carol_calls_at_pickup` records how many calls Carol had
    /// answered by pickup time (e-order: parked pre-resolution calls must
    /// land BEFORE the release that fires the pickup).
    carol_probe: ?*const Carol = null,

    fired: bool = false,
    ret_tag: ?protocol.ReturnTag = null,
    exception_reason: ?[]u8 = null,
    carol_import_id: ?u32 = null,
    carol_calls_at_pickup: ?u32 = null,
    /// Mid-flight witnesses (valid when c_owner/c_accept were set).
    owner_provisions_at_pickup: ?usize = null,
    accept_provisions_at_pickup: ?usize = null,
    carol_handoff_refs_at_pickup: ?u32 = null,
    owner_legacy_provides_at_pickup: ?usize = null,
    accept_legacy_keys_at_pickup: ?usize = null,

    fn deinit(self: *VatcPickupHandler) void {
        if (self.exception_reason) |r| self.allocator.free(r);
        self.exception_reason = null;
    }

    fn onPickup(
        ctx_ptr: *anyopaque,
        _: *Peer,
        promise_id: u32,
        accept_peer: *Peer,
        ret: protocol.Return,
        accept_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *VatcPickupHandler = castCtx(*VatcPickupHandler, ctx_ptr);
        self.fired = true;
        self.ret_tag = ret.tag;
        if (promise_id != self.expected_promise_id) return error.UnexpectedPromiseId;
        if (self.c_owner) |c| self.owner_provisions_at_pickup = c.provisions_by_question.count();
        if (self.c_accept) |c| self.accept_provisions_at_pickup = c.provisions_by_question.count();
        if (self.c_owner) |c| self.owner_legacy_provides_at_pickup = c.provides_by_question.count();
        if (self.c_accept) |c| self.accept_legacy_keys_at_pickup = c.provides_by_key.count();
        if (self.carol_probe) |carol| self.carol_calls_at_pickup = carol.get_number_calls;
        if (self.c_owner) |c| {
            if (self.carol_export_id) |id| {
                if (c.exports.get(id)) |entry| self.carol_handoff_refs_at_pickup = entry.handoff_ref_count;
            }
        }
        switch (ret.tag) {
            .results => {
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
            },
            .exception => {
                if (ret.exception) |ex| {
                    self.exception_reason = try self.allocator.dupe(u8, ex.reason);
                }
            },
            else => {},
        }
    }
};

// -- Three pairwise synchronous links ----------------------------------------

const PairLink = struct {
    forwarding: bool = true,
    left: ?*Peer = null,
    right: ?*Peer = null,
    /// When set, an inbound-to-`right` Accept frame is buffered instead of
    /// delivered (holds the handoff open across the left->right hop).
    buffer_accepts_to_right: bool = false,
    buffered_accept: ?[]u8 = null,
    /// Same for a context.accept Disembargo (holds the embargo open).
    buffer_disembargos_to_right: bool = false,
    buffered_disembargo: ?[]u8 = null,
    allocator: std.mem.Allocator = std.testing.allocator,

    fn deinitBuffered(self: *PairLink) void {
        if (self.buffered_accept) |f| self.allocator.free(f);
        self.buffered_accept = null;
        if (self.buffered_disembargo) |f| self.allocator.free(f);
        self.buffered_disembargo = null;
    }

    fn leftSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *PairLink = castCtx(*PairLink, ctx);
        if (!self.forwarding) return;
        if (self.buffer_accepts_to_right or self.buffer_disembargos_to_right) {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (self.buffer_accepts_to_right and decoded.tag == .accept) {
                std.debug.assert(self.buffered_accept == null);
                self.buffered_accept = try self.allocator.dupe(u8, frame);
                return;
            }
            if (self.buffer_disembargos_to_right and decoded.tag == .disembargo) {
                const dis = try decoded.asDisembargo();
                if (dis.context_tag == .accept) {
                    std.debug.assert(self.buffered_disembargo == null);
                    self.buffered_disembargo = try self.allocator.dupe(u8, frame);
                    return;
                }
            }
        }
        if (self.right) |peer| try peer.handleFrame(frame);
    }

    fn rightSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *PairLink = castCtx(*PairLink, ctx);
        if (!self.forwarding) return;
        if (self.left) |peer| try peer.handleFrame(frame);
    }

    fn releaseBufferedAccept(self: *PairLink) !void {
        const frame = self.buffered_accept orelse return error.NoBufferedAccept;
        self.buffered_accept = null;
        defer self.allocator.free(frame);
        if (self.right) |peer| try peer.handleFrame(frame);
    }

    fn releaseBufferedDisembargo(self: *PairLink) !void {
        const frame = self.buffered_disembargo orelse return error.NoBufferedDisembargo;
        self.buffered_disembargo = null;
        defer self.allocator.free(frame);
        if (self.right) |peer| try peer.handleFrame(frame);
    }
};

// -- The six-peer fixture -----------------------------------------------------

const SixPeers = struct {
    a_to_b: Peer,
    a_to_c: Peer,
    b_to_a: Peer,
    b_to_c: Peer,
    c_to_b: Peer,
    c_to_a: Peer,
    ab: PairLink,
    bc: PairLink,
    ac: PairLink,

    fn init(self: *SixPeers, allocator: std.mem.Allocator) void {
        self.ab = .{};
        self.bc = .{};
        self.ac = .{};
        self.a_to_b = Peer.initDetached(allocator);
        self.a_to_c = Peer.initDetached(allocator);
        self.b_to_a = Peer.initDetached(allocator);
        self.b_to_c = Peer.initDetached(allocator);
        self.c_to_b = Peer.initDetached(allocator);
        self.c_to_a = Peer.initDetached(allocator);
        inline for (.{ &self.a_to_b, &self.a_to_c, &self.b_to_a, &self.b_to_c, &self.c_to_b, &self.c_to_a }) |p| {
            p.disableThreadAffinity();
        }
        self.ab.left = &self.a_to_b;
        self.ab.right = &self.b_to_a;
        self.bc.left = &self.b_to_c;
        self.bc.right = &self.c_to_b;
        self.ac.left = &self.a_to_c;
        self.ac.right = &self.c_to_a;
        self.a_to_b.setSendFrameOverride(&self.ab, PairLink.leftSend);
        self.b_to_a.setSendFrameOverride(&self.ab, PairLink.rightSend);
        self.b_to_c.setSendFrameOverride(&self.bc, PairLink.leftSend);
        self.c_to_b.setSendFrameOverride(&self.bc, PairLink.rightSend);
        self.a_to_c.setSendFrameOverride(&self.ac, PairLink.leftSend);
        self.c_to_a.setSendFrameOverride(&self.ac, PairLink.rightSend);
    }

    fn deinitPeers(self: *SixPeers) void {
        self.ab.forwarding = false;
        self.bc.forwarding = false;
        self.ac.forwarding = false;
        self.ab.deinitBuffered();
        self.bc.deinitBuffered();
        self.ac.deinitBuffered();
        self.a_to_b.deinit();
        self.a_to_c.deinit();
        self.b_to_a.deinit();
        self.b_to_c.deinit();
        self.c_to_b.deinit();
        self.c_to_a.deinit();
    }
};

/// Wire-honest grant: install `returner` as `host`'s bootstrap and have
/// `remote` import the cap it returns (bootstrap, then a call whose Return
/// carries the target — granting one wire reference).
var last_bootstrap_question_id: u32 = 0;

fn grantCapVia(host: *Peer, remote: *Peer, returner: *CapReturner) !u32 {
    _ = try host.setBootstrap(.{ .ctx = returner, .on_call = CapReturner.onCall });
    var boot_probe = CapImportProbe{};
    last_bootstrap_question_id = try remote.sendBootstrap(&boot_probe, CapImportProbe.onReturn);
    const boot_import = boot_probe.import_id orelse return error.ReturnerNotGranted;
    var probe = CapImportProbe{};
    _ = try remote.sendCall(boot_import, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &probe, null, CapImportProbe.onReturn);
    return probe.import_id orelse return error.CapNotGranted;
}

/// Everything up to (but not including) the origination: Carol exported on
/// c_to_b + granted to B; Introducer on b_to_a; A holds the promise import;
/// the pickup handler installed.
const HandoffSetup = struct {
    carol: Carol = .{},
    carol_returner: CapReturner = undefined,
    carol_export_id: u32 = 0,
    carol_import_b: u32 = 0,
    introducer: Introducer = .{},
    introducer_import: u32 = 0,
    promise_import: u32 = 0,
    provide_question_id: u32 = 0,

    fn run(self: *HandoffSetup, peers: *SixPeers, net: anytype, nonce: []const u8) !void {
        try net.register(nonce, &peers.a_to_c);
        peers.b_to_a.attachVatNetwork(net.network());
        peers.a_to_b.attachVatNetwork(net.network());

        self.carol_export_id = try peers.c_to_b.addExport(.{ .ctx = &self.carol, .on_call = Carol.onCall });
        self.carol_returner = .{ .export_id = self.carol_export_id };
        self.carol_import_b = try grantCapVia(&peers.c_to_b, &peers.b_to_c, &self.carol_returner);

        _ = try peers.b_to_a.setBootstrap(.{ .ctx = &self.introducer, .on_call = Introducer.onCall });
        var iprobe = CapImportProbe{};
        _ = try peers.a_to_b.sendBootstrap(&iprobe, CapImportProbe.onReturn);
        self.introducer_import = iprobe.import_id orelse return error.IntroducerBootstrapFailed;

        var pprobe = CapImportProbe{};
        _ = try peers.a_to_b.sendCall(self.introducer_import, 0x1234_5678_9abc_def0, 0, &pprobe, null, CapImportProbe.onReturn);
        self.promise_import = pprobe.import_id orelse return error.PromiseNotImportedByA;
    }

    fn originate(self: *HandoffSetup, peers: *SixPeers, nonce: []const u8) !void {
        const allocator = std.testing.allocator;
        const b_network = peers.b_to_a.vat_network orelse return error.NoVatNetworkOnB;
        var introduction = try b_network.mintIntroduction(&peers.b_to_a, nonce);
        defer introduction.deinit(allocator);
        var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
        defer await_msg.deinit();
        const recipient = try await_msg.getRootAnyPointer();
        const provided_target = protocol.MessageTarget{
            .tag = .importedCap,
            .imported_cap = self.carol_import_b,
            .promised_answer = null,
        };
        const handle = try peers.b_to_a.resolvePromiseExportToThirdParty(
            self.promise_import,
            &peers.b_to_c,
            provided_target,
            recipient,
            introduction.to_contact,
        );
        self.provide_question_id = handle.question_id;
    }

    fn teardownImports(self: *HandoffSetup, peers: *SixPeers) !void {
        try peers.b_to_c.releaseImport(self.carol_import_b, 1);
        try peers.a_to_b.releaseImport(self.promise_import, 1);
        try peers.a_to_b.releaseImport(self.introducer_import, 1);
    }
};

// ============================================================================
// The headline: cross-peer serve with the index attached.
// ============================================================================

test "L3: a two-peer VatC serves an Accept for a Provide received on a sibling peer" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer peers.deinitPeers();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try peers.c_to_b.attachProvisionIndex(&index);
    try peers.c_to_a.attachProvisionIndex(&index);

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l3-headline");

    var pickup = VatcPickupHandler{
        .allocator = allocator,
        .expected_promise_id = setup.promise_import,
        .c_owner = &peers.c_to_b,
        .c_accept = &peers.c_to_a,
        .carol_export_id = setup.carol_export_id,
    };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);

    try setup.originate(&peers, "vatc-l3-headline");

    // Lead assertion (prints the divergence): the Accept resolved.
    try std.testing.expect(pickup.fired);
    try std.testing.expectEqualStrings("results", @tagName(pickup.ret_tag.?));
    const accepted = pickup.carol_import_id orelse return error.NoAcceptedCap;
    try harness.expectImport(&peers.a_to_c, accepted);

    // Mid-flight witnesses (captured INSIDE the pickup, before the vine
    // release finished the Provide): the provision was owned by the SIBLING
    // (c_to_b) — the split-brain the index exists to bridge — and Carol's
    // export carried BOTH handoff pins (provision + proxy).
    try std.testing.expectEqual(@as(?usize, 1), pickup.owner_provisions_at_pickup);
    try std.testing.expectEqual(@as(?usize, 0), pickup.accept_provisions_at_pickup);
    try std.testing.expectEqual(@as(?u32, 2), pickup.carol_handoff_refs_at_pickup);

    // The handoff completed: the vine release finished the Provide, closing
    // the provision (its pin dropped; the proxy's pin remains).
    try std.testing.expectEqual(@as(usize, 0), peers.c_to_b.provisions_by_question.count());
    try std.testing.expectEqual(@as(usize, 0), index.by_key.count());
    {
        const entry = peers.c_to_b.exports.get(setup.carol_export_id) orelse return error.CarolGone;
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_ref_count);
    }

    // The callee-identity proof: a direct call on the accepted cap reaches the
    // REAL Carol (an import-exists check alone would pass against a decoy).
    var call = TolerantCall{};
    _ = try peers.a_to_c.sendCall(accepted, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &call, null, TolerantCall.onReturn);
    try std.testing.expectEqual(@as(?u32, 42), call.result_n);
    try std.testing.expectEqual(@as(u32, 1), setup.carol.get_number_calls);

    // F2 probe: drop EVERY non-pin ref (B's wire ref via releaseImport), then
    // call again — the accepted cap must still reach Carol on the proxy pin.
    try peers.b_to_c.releaseImport(setup.carol_import_b, 1);
    var call2 = TolerantCall{};
    _ = try peers.a_to_c.sendCall(accepted, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &call2, null, TolerantCall.onReturn);
    try std.testing.expectEqual(@as(?u32, 42), call2.result_n);
    try std.testing.expectEqual(@as(u32, 2), setup.carol.get_number_calls);

    // Drain: releasing the accepted import destroys the proxy; its ctx deinit
    // releases the pin; Carol's export (all counters zero, wire-granted) dies.
    try peers.a_to_c.releaseImport(accepted, 1);
    try std.testing.expect(!peers.c_to_b.exports.contains(setup.carol_export_id));
    try std.testing.expectEqual(@as(usize, 0), peers.c_to_b.cross_peer_proxy_links.items.len);
    try peers.a_to_b.releaseImport(setup.promise_import, 1);
    try peers.a_to_b.releaseImport(setup.introducer_import, 1);
    try harness.expectNoProvideState(&peers.c_to_b);
    try harness.expectNoProvideState(&peers.c_to_a);
    try std.testing.expectEqual(@as(usize, 0), peers.c_to_a.cross_peer_pending_accepts.count());
}

// ============================================================================
// The permanent no-index witness: exactly the failure this landing fixes.
// ============================================================================

test "L3 witness: without a provision index, the two-peer VatC still answers unknown provision" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer peers.deinitPeers();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l3-witness");

    var pickup = VatcPickupHandler{
        .allocator = allocator,
        .expected_promise_id = setup.promise_import,
        .c_owner = &peers.c_to_b,
        .c_accept = &peers.c_to_a,
    };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);

    try setup.originate(&peers, "vatc-l3-witness");

    // The split-brain, stated plainly: at pickup time (before the vine
    // release finished the Provide) the Provide sat in c_to_b's per-peer
    // table while c_to_a's Accept could only see its own — the pinned string.
    try std.testing.expect(pickup.fired);
    try std.testing.expectEqualStrings("exception", @tagName(pickup.ret_tag.?));
    try std.testing.expectEqualStrings("unknown provision", pickup.exception_reason.?);
    try std.testing.expectEqual(@as(?usize, 1), pickup.owner_legacy_provides_at_pickup);
    try std.testing.expectEqual(@as(?usize, 0), pickup.accept_legacy_keys_at_pickup);

    // The runtime still releases the vine after the failed pickup, Finishing
    // the Provide — everything drains.
    try setup.teardownImports(&peers);
    try harness.expectNoProvideState(&peers.c_to_b);
    try harness.expectNoProvideState(&peers.c_to_a);
}

// ============================================================================
// Cross-peer embargoed accepts fail closed (visibly) until the embargo landing.
// ============================================================================

test "L4: a cross-peer embargoed handoff releases via the forwarded spec-form Disembargo" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer peers.deinitPeers();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try peers.c_to_b.attachProvisionIndex(&index);
    try peers.c_to_a.attachProvisionIndex(&index);

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l3-embargoed");

    var pickup = VatcPickupHandler{
        .allocator = allocator,
        .expected_promise_id = setup.promise_import,
        .carol_probe = &setup.carol,
    };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);

    // An in-flight pipelined call on the promise forces the embargo.
    var pipelined = TolerantCall{};
    _ = try peers.a_to_b.sendCall(setup.promise_import, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &pipelined, null, TolerantCall.onReturn);
    try std.testing.expect(!pipelined.returned);

    try setup.originate(&peers, "vatc-l3-embargoed");

    // The full chain worked: Accept queued in the provision store on c_to_a,
    // the recipient's Disembargo stashed at B until the parked-call replay,
    // then forwarded in SPEC FORM to c_to_b, whose host arm resolved the
    // Provide question -> provision -> its own embargo slot and released the
    // withheld Return on the SIBLING peer.
    try std.testing.expect(pickup.fired);
    try std.testing.expectEqualStrings("results", @tagName(pickup.ret_tag.?));
    const accepted = pickup.carol_import_id orelse return error.NoAcceptedCap;

    // E-ORDER: the parked pre-resolution call reached Carol BEFORE the
    // release fired the pickup (rpc.capnp:898-903).
    try std.testing.expectEqual(@as(?u32, 1), pickup.carol_calls_at_pickup);
    try std.testing.expect(pipelined.returned);
    try std.testing.expectEqual(@as(?u32, 42), pipelined.result_n);

    // The accepted capability reaches the real Carol.
    var call = TolerantCall{};
    _ = try peers.a_to_c.sendCall(accepted, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &call, null, TolerantCall.onReturn);
    try std.testing.expectEqual(@as(?u32, 42), call.result_n);
    try std.testing.expectEqual(@as(u32, 2), setup.carol.get_number_calls);

    // Nothing queued anywhere; counters back to zero.
    try std.testing.expectEqual(@as(usize, 0), peers.c_to_a.cross_peer_pending_accepts.count());
    try std.testing.expectEqual(@as(usize, 0), index.queued_accept_count);
    try std.testing.expectEqual(@as(usize, 0), index.queued_accept_bytes);

    try peers.a_to_c.releaseImport(accepted, 1);
    try setup.teardownImports(&peers);
    try harness.expectNoProvideState(&peers.c_to_b);
    try harness.expectNoProvideState(&peers.c_to_a);
    try std.testing.expectEqual(@as(usize, 0), index.by_key.count());
}

// ============================================================================
// Attach/detach ceremony.
// ============================================================================

test "L3 attach ceremony: dedupe, dirty-peer rejection, detach preconditions, index-first death" {
    const allocator = std.testing.allocator;

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();

    var p = Peer.initDetached(allocator);
    p.disableThreadAffinity();
    var q = Peer.initDetached(allocator);
    q.disableThreadAffinity();

    try p.attachProvisionIndex(&index);
    // M-9: double attach is an error, not a duplicate back-link.
    try std.testing.expectError(error.PeerAlreadyAttachedToProvisionIndex, p.attachProvisionIndex(&index));
    try std.testing.expectEqual(@as(usize, 1), index.attached_peers.items.len);

    // Clean detach + re-attach round-trips.
    try p.detachProvisionIndex();
    try std.testing.expect(p.provision_index == null);
    try p.attachProvisionIndex(&index);

    try q.attachProvisionIndex(&index);
    try std.testing.expectEqual(@as(usize, 2), index.attached_peers.items.len);

    // Peer-first death: p removes itself; index deinit must not write through
    // the freed peer.
    p.deinit();
    try std.testing.expectEqual(@as(usize, 1), index.attached_peers.items.len);
    index.deinit();
    try std.testing.expect(q.provision_index == null);
    q.deinit();
}

test "L3 attach ceremony: a peer with pre-existing handoff state is rejected" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer peers.deinitPeers();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    // Run a full NO-INDEX handoff so c_to_b holds provide state mid-flight?
    // Simpler and sufficient: after the (failed, no-index) handoff drains, the
    // maps are empty and attach succeeds; the dirty rejection is probed
    // mid-flight via the buffered-accept window below.
    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l3-dirty");

    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);

    // Hold the Accept at the A->C hop so the Provide stays live on c_to_b.
    peers.ac.buffer_accepts_to_right = true;
    try setup.originate(&peers, "vatc-l3-dirty");
    try std.testing.expectEqual(@as(usize, 1), peers.c_to_b.provides_by_question.count());

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try std.testing.expectError(error.PeerAlreadyHasHandoffState, peers.c_to_b.attachProvisionIndex(&index));

    // Release the held Accept; the (no-index) handoff fails with the pinned
    // string and everything drains.
    try peers.ac.releaseBufferedAccept();
    try std.testing.expectEqualStrings("unknown provision", pickup.exception_reason.?);
    try setup.teardownImports(&peers);
}

// ============================================================================
// Vat-wide duplicate-recipient rejection (delta (b) of index mode).
// ============================================================================

test "L3: a second Provide with the same recipient token on a SIBLING peer is rejected vat-wide" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer peers.deinitPeers();

    // An extra B<->C connection so the duplicate arrives on a DIFFERENT peer
    // of vat C (per-peer duplicate detection cannot see it).
    var b2_to_c = Peer.initDetached(allocator);
    b2_to_c.disableThreadAffinity();
    var c_to_b2 = Peer.initDetached(allocator);
    c_to_b2.disableThreadAffinity();
    var bc2 = PairLink{};
    bc2.left = &b2_to_c;
    bc2.right = &c_to_b2;
    b2_to_c.setSendFrameOverride(&bc2, PairLink.leftSend);
    c_to_b2.setSendFrameOverride(&bc2, PairLink.rightSend);
    defer {
        bc2.forwarding = false;
        b2_to_c.deinit();
        c_to_b2.deinit();
    }

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try peers.c_to_b.attachProvisionIndex(&index);
    try peers.c_to_a.attachProvisionIndex(&index);
    try c_to_b2.attachProvisionIndex(&index);

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l3-dup");

    // Carol2 granted to B2 over the second connection.
    var carol2 = Carol{};
    const carol2_id = try c_to_b2.addExport(.{ .ctx = &carol2, .on_call = Carol.onCall });
    var returner2 = CapReturner{ .export_id = carol2_id };
    const carol2_import = try grantCapVia(&c_to_b2, &b2_to_c, &returner2);

    // First Provide (via the normal origination) claims the token vat-wide.
    peers.ac.buffer_accepts_to_right = true; // hold the handoff open
    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);
    try setup.originate(&peers, "vatc-l3-dup");
    try std.testing.expectEqual(@as(usize, 1), index.by_key.count());

    // Second Provide with the SAME recipient token, on the sibling peer.
    const b_network = peers.b_to_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&peers.b_to_a, "vatc-l3-dup");
    defer introduction.deinit(allocator);
    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();
    const dup_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = carol2_import,
        .promised_answer = null,
    };
    try std.testing.expectError(
        error.DuplicateProvideRecipient,
        b2_to_c.sendProvide(dup_target, recipient, &peers.b_to_a, introduction.to_contact),
    );
    // The duplicate was rolled back on its peer; the original survives.
    try std.testing.expectEqual(@as(usize, 0), c_to_b2.provides_by_question.count());
    try std.testing.expectEqual(@as(usize, 1), index.by_key.count());

    // Complete the held handoff and drain.
    try peers.ac.releaseBufferedAccept();
    try std.testing.expectEqualStrings("results", @tagName(pickup.ret_tag.?));
    const accepted = pickup.carol_import_id orelse return error.NoAcceptedCap;
    try peers.a_to_c.releaseImport(accepted, 1);
    try b2_to_c.releaseImport(carol2_import, 1);
    try setup.teardownImports(&peers);
    try std.testing.expectEqual(@as(usize, 0), index.by_key.count());
}

// ============================================================================
// The Provide-time pin window: [Provide, serve) — the V2-M6 ordering.
// ============================================================================

test "L3: the Provide-time pin keeps the target alive between Provide and Accept" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer peers.deinitPeers();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try peers.c_to_b.attachProvisionIndex(&index);
    try peers.c_to_a.attachProvisionIndex(&index);

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l3-pinwin");

    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);

    // Hold the Accept so the handoff pauses BETWEEN Provide and serve.
    peers.ac.buffer_accepts_to_right = true;
    try setup.originate(&peers, "vatc-l3-pinwin");

    // The Provide-time pin is live and load-bearing: B drops its wire ref
    // NOW, and Carol's export must survive on the provision pin alone.
    {
        const entry = peers.c_to_b.exports.get(setup.carol_export_id) orelse return error.CarolGone;
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_ref_count);
    }
    try peers.b_to_c.releaseImport(setup.carol_import_b, 1);
    {
        const entry = peers.c_to_b.exports.get(setup.carol_export_id) orelse return error.CarolDestroyedDespiteProvisionPin;
        try std.testing.expectEqual(@as(u32, 0), entry.ref_count);
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_ref_count);
    }

    // Serve: the accepted cap reaches the REAL Carol.
    try peers.ac.releaseBufferedAccept();
    try std.testing.expectEqualStrings("results", @tagName(pickup.ret_tag.?));
    const accepted = pickup.carol_import_id orelse return error.NoAcceptedCap;
    var call = TolerantCall{};
    _ = try peers.a_to_c.sendCall(accepted, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &call, null, TolerantCall.onReturn);
    try std.testing.expectEqual(@as(?u32, 42), call.result_n);
    try std.testing.expectEqual(@as(u32, 1), setup.carol.get_number_calls);

    try peers.a_to_c.releaseImport(accepted, 1);
    try std.testing.expect(!peers.c_to_b.exports.contains(setup.carol_export_id));
    try peers.a_to_b.releaseImport(setup.promise_import, 1);
    try peers.a_to_b.releaseImport(setup.introducer_import, 1);
    try harness.expectNoProvideState(&peers.c_to_b);
    try harness.expectNoProvideState(&peers.c_to_a);
}

// ============================================================================
// Teardown x3 (non-embargoed variants), each with an explicit observable.
// ============================================================================

test "L3 teardown: owner peer first — accepted cap degrades loudly, no UAF" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer {
        peers.ab.forwarding = false;
        peers.bc.forwarding = false;
        peers.ac.forwarding = false;
        peers.a_to_b.deinit();
        peers.a_to_c.deinit();
        peers.b_to_a.deinit();
        peers.b_to_c.deinit();
        peers.c_to_a.deinit();
        // c_to_b deinited mid-test.
    }

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try peers.c_to_b.attachProvisionIndex(&index);
    try peers.c_to_a.attachProvisionIndex(&index);

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l3-tdown-owner");
    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);
    try setup.originate(&peers, "vatc-l3-tdown-owner");
    const accepted = pickup.carol_import_id orelse return error.NoAcceptedCap;

    // Owner dies first (its transport is severed like a real disconnect).
    peers.bc.forwarding = false;
    peers.c_to_b.deinit();

    // The accepted cap degrades to the pinned dead-cap exception — loud, not
    // a hang, not a UAF.
    var call = TolerantCall{};
    _ = try peers.a_to_c.sendCall(accepted, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &call, null, TolerantCall.onReturn);
    try std.testing.expectEqualStrings("exception", @tagName(call.ret_tag.?));

    try peers.a_to_c.releaseImport(accepted, 1);
    try peers.a_to_b.releaseImport(setup.promise_import, 1);
    try peers.a_to_b.releaseImport(setup.introducer_import, 1);
}

test "L3 teardown: accept peer first — proxy sweep releases the pin on the live owner" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer {
        peers.ab.forwarding = false;
        peers.bc.forwarding = false;
        peers.ac.forwarding = false;
        peers.a_to_b.deinit();
        peers.a_to_c.deinit();
        peers.b_to_a.deinit();
        peers.b_to_c.deinit();
        peers.c_to_b.deinit();
        // c_to_a deinited mid-test.
    }

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try peers.c_to_b.attachProvisionIndex(&index);
    try peers.c_to_a.attachProvisionIndex(&index);

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l3-tdown-holder");
    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);
    try setup.originate(&peers, "vatc-l3-tdown-holder");
    try std.testing.expectEqualStrings("results", @tagName(pickup.ret_tag.?));

    {
        const entry = peers.c_to_b.exports.get(setup.carol_export_id) orelse return error.CarolGone;
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_ref_count);
    }

    // The accept peer dies first: its export sweep runs the proxy ctx deinit,
    // which releases the pin on the still-live owner.
    peers.ac.forwarding = false;
    peers.c_to_a.deinit();
    {
        const entry = peers.c_to_b.exports.get(setup.carol_export_id) orelse return error.CarolGoneAfterHolderDeath;
        try std.testing.expectEqual(@as(u32, 0), entry.handoff_ref_count);
    }

    try setup.teardownImports(&peers);
}

test "L3 teardown: index first — matched handoffs keep working; new Accepts degrade loudly" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer peers.deinitPeers();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    try peers.c_to_b.attachProvisionIndex(&index);
    try peers.c_to_a.attachProvisionIndex(&index);

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l3-tdown-index");
    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);
    try setup.originate(&peers, "vatc-l3-tdown-index");
    try std.testing.expectEqualStrings("results", @tagName(pickup.ret_tag.?));
    const accepted = pickup.carol_import_id orelse return error.NoAcceptedCap;

    // Index dies (application tears it down early).
    index.deinit();
    try std.testing.expect(peers.c_to_b.provision_index == null);
    try std.testing.expect(peers.c_to_a.provision_index == null);

    // F6: the already-served handoff is UNAFFECTED — the accepted capability
    // still reaches the real Carol.
    var call = TolerantCall{};
    _ = try peers.a_to_c.sendCall(accepted, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &call, null, TolerantCall.onReturn);
    try std.testing.expectEqual(@as(?u32, 42), call.result_n);

    try peers.a_to_c.releaseImport(accepted, 1);
    try setup.teardownImports(&peers);
    try harness.expectNoProvideState(&peers.c_to_b);
    try harness.expectNoProvideState(&peers.c_to_a);
}

// ============================================================================
// Single-peer degeneration: index attached, Provide and Accept on ONE peer.
// The behavior deltas of index mode are exactly (a) the Provide-time pin and
// (b) vat-wide duplicate rejection (tested above); everything else matches
// the legacy path — same identity serve, no proxy minted.
// ============================================================================

const Vat = enum { a, b, c };

const SingleCLink = struct {
    allocator: std.mem.Allocator,
    forwarding: bool = true,
    a_to_b: ?*Peer = null,
    b_to_a: ?*Peer = null,
    b_to_c: ?*Peer = null,
    c: ?*Peer = null,
    a_to_c: ?*Peer = null,
    c_question_origin: std.AutoHashMap(u32, Vat),

    fn init(allocator: std.mem.Allocator) SingleCLink {
        return .{
            .allocator = allocator,
            .c_question_origin = std.AutoHashMap(u32, Vat).init(allocator),
        };
    }

    fn deinit(self: *SingleCLink) void {
        self.c_question_origin.deinit();
    }

    fn recordCQuestion(self: *SingleCLink, frame: []const u8, origin: Vat) !void {
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
        const self: *SingleCLink = castCtx(*SingleCLink, ctx);
        if (!self.forwarding) return;
        if (self.b_to_a) |peer| try peer.handleFrame(frame);
    }

    fn bToASend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *SingleCLink = castCtx(*SingleCLink, ctx);
        if (!self.forwarding) return;
        if (self.a_to_b) |peer| try peer.handleFrame(frame);
    }

    fn bToCSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *SingleCLink = castCtx(*SingleCLink, ctx);
        try self.recordCQuestion(frame, .b);
        if (!self.forwarding) return;
        if (self.c) |peer| try peer.handleFrame(frame);
    }

    fn aToCSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *SingleCLink = castCtx(*SingleCLink, ctx);
        try self.recordCQuestion(frame, .a);
        if (!self.forwarding) return;
        if (self.c) |peer| try peer.handleFrame(frame);
    }

    fn cSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *SingleCLink = castCtx(*SingleCLink, ctx);
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
};

test "L3 degeneration: index attached, single-peer VatC — identity serve, no proxy, pin is the only delta" {
    const allocator = std.testing.allocator;

    var link = SingleCLink.init(allocator);
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

    a_to_b.setSendFrameOverride(&link, SingleCLink.aToBSend);
    b_to_a.setSendFrameOverride(&link, SingleCLink.bToASend);
    b_to_c.setSendFrameOverride(&link, SingleCLink.bToCSend);
    a_to_c.setSendFrameOverride(&link, SingleCLink.aToCSend);
    c.setSendFrameOverride(&link, SingleCLink.cSend);

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try c.attachProvisionIndex(&index);

    const nonce = "vatc-l3-degenerate";
    try net.register(nonce, &a_to_c);
    b_to_a.attachVatNetwork(net.network());
    a_to_b.attachVatNetwork(net.network());

    // Carol exported on c and granted to B (not the bootstrap: the pin delta
    // is only observable on a destroyable entry).
    var carol = Carol{};
    const carol_id = try c.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    var returner = CapReturner{ .export_id = carol_id };
    const carol_import_b = try grantCapVia(&c, &b_to_c, &returner);

    var introducer = Introducer{};
    _ = try b_to_a.setBootstrap(.{ .ctx = &introducer, .on_call = Introducer.onCall });
    var iprobe = CapImportProbe{};
    _ = try a_to_b.sendBootstrap(&iprobe, CapImportProbe.onReturn);
    const introducer_import = iprobe.import_id orelse return error.IntroducerBootstrapFailed;
    var pprobe = CapImportProbe{};
    _ = try a_to_b.sendCall(introducer_import, 0x1234_5678_9abc_def0, 0, &pprobe, null, CapImportProbe.onReturn);
    const promise_import = pprobe.import_id orelse return error.PromiseNotImportedByA;

    var pickup = VatcPickupHandler{
        .allocator = allocator,
        .expected_promise_id = promise_import,
        .c_owner = &c,
        .c_accept = &c,
        .carol_export_id = carol_id,
    };
    defer pickup.deinit();
    a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);

    const b_network = b_to_a.vat_network orelse return error.NoVatNetworkOnB;
    var introduction = try b_network.mintIntroduction(&b_to_a, nonce);
    defer introduction.deinit(allocator);
    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();
    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = carol_import_b,
        .promised_answer = null,
    };
    _ = try b_to_a.resolvePromiseExportToThirdParty(
        promise_import,
        &b_to_c,
        provided_target,
        recipient,
        introduction.to_contact,
    );

    // Identity serve: results, the ORIGINAL export id (no proxy — the legacy
    // origin-tagged re-emission), the real Carol answers.
    try std.testing.expectEqualStrings("results", @tagName(pickup.ret_tag.?));
    const accepted = pickup.carol_import_id orelse return error.NoAcceptedCap;
    try std.testing.expectEqual(carol_id, accepted);
    try std.testing.expectEqual(@as(usize, 0), c.cross_peer_proxy_links.items.len);
    // Delta (a), captured mid-flight: the provision pin was live.
    try std.testing.expectEqual(@as(?u32, 1), pickup.carol_handoff_refs_at_pickup);
    // Post-handoff: the provision closed at Finish; the pin dropped.
    {
        const entry = c.exports.get(carol_id) orelse return error.CarolGone;
        try std.testing.expectEqual(@as(u32, 0), entry.handoff_ref_count);
    }

    var call = TolerantCall{};
    _ = try a_to_c.sendCall(accepted, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &call, null, TolerantCall.onReturn);
    try std.testing.expectEqual(@as(?u32, 42), call.result_n);
    try std.testing.expectEqual(@as(u32, 1), carol.get_number_calls);

    try a_to_c.releaseImport(accepted, 1);
    try b_to_c.releaseImport(carol_import_b, 1);
    try a_to_b.releaseImport(promise_import, 1);
    try a_to_b.releaseImport(introducer_import, 1);
    try harness.expectNoProvideState(&c);
    try std.testing.expectEqual(@as(usize, 0), index.by_key.count());
}

// ============================================================================
// M-7: Provide+Finish naming a never-emitted export must not destroy it.
// ============================================================================

fn buildProvideFrame(
    allocator: std.mem.Allocator,
    question_id: u32,
    target: protocol.MessageTarget,
    recipient: message.AnyPointerReader,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildProvide(question_id, target, recipient);
    return builder.finish();
}

fn buildFinishFrame(allocator: std.mem.Allocator, question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildFinish(question_id, false, false);
    return builder.finish();
}

test "L3 M-7: Provide+Finish naming an app-held never-emitted export leaves it intact" {
    const allocator = std.testing.allocator;

    var p = Peer.initDetached(allocator);
    p.disableThreadAffinity();
    defer p.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try p.attachProvisionIndex(&index);

    // An app-held export the wire has NEVER seen.
    var carol = Carol{};
    const carol_id = try p.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });

    // A (hostile) introducer names it in a Provide — the id space is p's
    // export table, so a guessed id is enough.
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    var dummy = Peer.initDetached(allocator);
    dummy.disableThreadAffinity();
    defer dummy.deinit();
    try net.register("m7-token", &dummy);
    dummy.attachVatNetwork(net.network());
    const d_network = dummy.vat_network orelse return error.NoVatNetwork;
    var introduction = try d_network.mintIntroduction(&dummy, "m7-token");
    defer introduction.deinit(allocator);
    var await_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const provide_frame = try buildProvideFrame(allocator, 777, .{
        .tag = .importedCap,
        .imported_cap = carol_id,
        .promised_answer = null,
    }, recipient);
    defer allocator.free(provide_frame);
    try p.handleFrame(provide_frame);

    // Pinned at Provide.
    try std.testing.expectEqual(@as(usize, 1), p.provisions_by_question.count());
    {
        const entry = p.exports.get(carol_id) orelse return error.CarolGone;
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_ref_count);
        try std.testing.expect(!entry.wire_ref_granted);
    }

    // Finish unpins — and the never-wire-granted entry MUST survive, or
    // Provide+Finish becomes a remote destroy primitive for app-held ids.
    const finish_frame = try buildFinishFrame(allocator, 777);
    defer allocator.free(finish_frame);
    try p.handleFrame(finish_frame);

    const entry = p.exports.get(carol_id) orelse return error.AppExportDestroyedByProvideFinish;
    try std.testing.expectEqual(@as(u32, 0), entry.handoff_ref_count);
    try std.testing.expectEqual(@as(usize, 0), p.provisions_by_question.count());
    try std.testing.expectEqual(@as(usize, 0), index.by_key.count());
}

// ============================================================================
// OOM discipline: every allocation in the Provide-registration path rolls
// back to zero residue (two-pass FailingAllocator sweep with state probes).
// ============================================================================

test "L3 OOM: a failed Provide registration leaves zero residue (index, maps, pins)" {
    const base = std.testing.allocator;

    var window_start: usize = 0;
    var window_end: usize = 0;

    // Pass 1: record the allocation window of handleFrame(provide).
    {
        var failing = std.testing.FailingAllocator.init(base, .{});
        const alloc = failing.allocator();
        var p = Peer.initDetached(alloc);
        p.disableThreadAffinity();
        defer p.deinit();
        var index = ProvisionIndex.init(alloc, .{});
        index.disableThreadAffinity();
        defer index.deinit();
        try p.attachProvisionIndex(&index);
        var carol = Carol{};
        const carol_id = try p.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });

        var net = vat_network.LoopbackVatNetwork(Peer).init(alloc);
        defer net.deinit();
        var dummy = Peer.initDetached(alloc);
        dummy.disableThreadAffinity();
        defer dummy.deinit();
        try net.register("oom-token", &dummy);
        dummy.attachVatNetwork(net.network());
        const d_network = dummy.vat_network orelse return error.NoVatNetwork;
        var introduction = try d_network.mintIntroduction(&dummy, "oom-token");
        defer introduction.deinit(alloc);
        var await_msg = try message.Message.initUnvalidated(alloc, introduction.to_await);
        defer await_msg.deinit();
        const recipient = try await_msg.getRootAnyPointer();
        const provide_frame = try buildProvideFrame(alloc, 5, .{
            .tag = .importedCap,
            .imported_cap = carol_id,
            .promised_answer = null,
        }, recipient);
        defer alloc.free(provide_frame);

        window_start = failing.alloc_index;
        try p.handleFrame(provide_frame);
        window_end = failing.alloc_index;
    }
    try std.testing.expect(window_end > window_start);

    // Pass 2: inject one failure at every index inside the window.
    var fail_index = window_start;
    while (fail_index < window_end) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(base, .{ .fail_index = fail_index });
        const alloc = failing.allocator();
        var p = Peer.initDetached(alloc);
        p.disableThreadAffinity();
        defer p.deinit();
        var index = ProvisionIndex.init(alloc, .{});
        index.disableThreadAffinity();
        defer index.deinit();
        try p.attachProvisionIndex(&index);
        var carol = Carol{};
        const carol_id = try p.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });

        var net = vat_network.LoopbackVatNetwork(Peer).init(alloc);
        defer net.deinit();
        var dummy = Peer.initDetached(alloc);
        dummy.disableThreadAffinity();
        defer dummy.deinit();
        try net.register("oom-token", &dummy);
        dummy.attachVatNetwork(net.network());
        const d_network = dummy.vat_network orelse return error.NoVatNetwork;
        var introduction = try d_network.mintIntroduction(&dummy, "oom-token");
        defer introduction.deinit(alloc);
        var await_msg = try message.Message.initUnvalidated(alloc, introduction.to_await);
        defer await_msg.deinit();
        const recipient = try await_msg.getRootAnyPointer();
        const provide_frame = try buildProvideFrame(alloc, 5, .{
            .tag = .importedCap,
            .imported_cap = carol_id,
            .promised_answer = null,
        }, recipient);
        defer alloc.free(provide_frame);

        if (p.handleFrame(provide_frame)) |_| {
            // Allocation pattern shifted; this index no longer fails inside
            // the provide path. The success state drains at deinit.
        } else |_| {
            // THE PROBES: no half-registered residue anywhere.
            try std.testing.expectEqual(@as(usize, 0), index.by_key.count());
            try std.testing.expectEqual(@as(usize, 0), p.provisions_by_question.count());
            try std.testing.expectEqual(@as(usize, 0), p.provides_by_question.count());
            try std.testing.expectEqual(@as(usize, 0), p.provides_by_key.count());
            const entry = p.exports.get(carol_id) orelse return error.CarolDestroyedByOom;
            try std.testing.expectEqual(@as(u32, 0), entry.handoff_ref_count);
        }
    }
}

// ============================================================================
// Allocator-ownership proof: peers and index on DIFFERENT allocators, with a
// promisedAnswer-target Provide (the clone that must use the INDEX allocator).
// ============================================================================

test "L3 ownership: provision internals live on the index allocator (promised-target Provide)" {
    var peer_gpa = std.heap.DebugAllocator(.{}).init;
    var index_gpa = std.heap.DebugAllocator(.{}).init;

    {
        var p = Peer.initDetached(peer_gpa.allocator());
        p.disableThreadAffinity();
        defer p.deinit();
        const Sink = struct {
            fn send(_: *anyopaque, _: []const u8) anyerror!void {}
        };
        var sink: u8 = 0;
        p.setSendFrameOverride(&sink, Sink.send);
        var index = ProvisionIndex.init(index_gpa.allocator(), .{});
        index.disableThreadAffinity();
        defer index.deinit();
        try p.attachProvisionIndex(&index);

        // Give p a RESOLVED inbound answer to name: a bootstrap question.
        var carol = Carol{};
        _ = try p.setBootstrap(.{ .ctx = &carol, .on_call = Carol.onCall });
        var boot_builder = protocol.MessageBuilder.init(peer_gpa.allocator());
        try boot_builder.buildBootstrap(3);
        const boot_frame = try boot_builder.finish();
        boot_builder.deinit();
        defer peer_gpa.allocator().free(boot_frame);
        try p.handleFrame(boot_frame);

        var net = vat_network.LoopbackVatNetwork(Peer).init(peer_gpa.allocator());
        defer net.deinit();
        var dummy = Peer.initDetached(peer_gpa.allocator());
        dummy.disableThreadAffinity();
        defer dummy.deinit();
        try net.register("own-token", &dummy);
        dummy.attachVatNetwork(net.network());
        const d_network = dummy.vat_network orelse return error.NoVatNetwork;
        var introduction = try d_network.mintIntroduction(&dummy, "own-token");
        defer introduction.deinit(peer_gpa.allocator());
        var await_msg = try message.Message.initUnvalidated(peer_gpa.allocator(), introduction.to_await);
        defer await_msg.deinit();
        const recipient = try await_msg.getRootAnyPointer();

        // Provide targeting the resolved answer (promisedAnswer form): the
        // stored target is ops-based and its provision copy MUST be cloned
        // with the index allocator, or teardown is a cross-allocator free.
        const provide_frame = try buildProvideFrame(peer_gpa.allocator(), 9, .{
            .tag = .promisedAnswer,
            .imported_cap = null,
            .promised_answer = .{ .question_id = 3, .transform = .{ .list = null } },
        }, recipient);
        defer peer_gpa.allocator().free(provide_frame);
        try p.handleFrame(provide_frame);
        try std.testing.expectEqual(@as(usize, 1), p.provisions_by_question.count());

        const finish_frame = try buildFinishFrame(peer_gpa.allocator(), 9);
        defer peer_gpa.allocator().free(finish_frame);
        try p.handleFrame(finish_frame);
        try std.testing.expectEqual(@as(usize, 0), index.by_key.count());
    }

    // Both allocators must report every byte returned to the right owner.
    try std.testing.expectEqual(std.heap.Check.ok, peer_gpa.deinit());
    try std.testing.expectEqual(std.heap.Check.ok, index_gpa.deinit());
}

// ============================================================================
// F-3 composition: Finish-of-Provide while an embargoed accept is queued,
// over sync links — the eager-fail exception Return triggers the recipient's
// nested auto-Finish of the accept answer MID-FAN-OUT. The `.closed`
// ownership token and the record-before-send ordering make it a clean miss.
// ============================================================================

test "L4: Finish while an embargoed accept is queued fails it eagerly; the nested auto-Finish is a clean miss" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer peers.deinitPeers();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try peers.c_to_b.attachProvisionIndex(&index);
    try peers.c_to_a.attachProvisionIndex(&index);

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l4-finish");
    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);

    // Force the embargo AND hold the forwarded Disembargo at the B->C hop so
    // the accept stays queued.
    var pipelined = TolerantCall{};
    _ = try peers.a_to_b.sendCall(setup.promise_import, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &pipelined, null, TolerantCall.onReturn);
    peers.bc.buffer_disembargos_to_right = true;
    try setup.originate(&peers, "vatc-l4-finish");

    // Queued: the accept is withheld on c_to_a, keyed in the provision.
    try std.testing.expect(!pickup.fired);
    try std.testing.expectEqual(@as(usize, 1), peers.c_to_a.cross_peer_pending_accepts.count());
    try std.testing.expectEqual(@as(usize, 1), index.queued_accept_count);

    // The introducer Finishes the Provide (frame-injected — the wire event).
    // The close fans out an exception Return to A over the LIVE a<->c link;
    // A's runtime auto-Finishes the accept answer NESTED inside that
    // delivery; the routed clear wrapper's fetchRemove MISSES (the fail
    // helper removed the record before sending) — no panic, no double
    // release, no UAF.
    const finish_frame = try buildFinishFrame(allocator, setup.provide_question_id);
    defer allocator.free(finish_frame);
    try peers.c_to_b.handleFrame(finish_frame);

    try std.testing.expect(pickup.fired);
    try std.testing.expectEqualStrings("exception", @tagName(pickup.ret_tag.?));
    try std.testing.expectEqualStrings("provision finished before disembargo", pickup.exception_reason.?);

    // Zero residue: no queued accepts, no provision, pin released.
    try std.testing.expectEqual(@as(usize, 0), peers.c_to_a.cross_peer_pending_accepts.count());
    try std.testing.expectEqual(@as(usize, 0), peers.c_to_b.provisions_by_question.count());
    try std.testing.expectEqual(@as(usize, 0), index.by_key.count());
    try std.testing.expectEqual(@as(usize, 0), index.queued_accept_count);
    {
        const entry = peers.c_to_b.exports.get(setup.carol_export_id) orelse return error.CarolGone;
        try std.testing.expectEqual(@as(u32, 0), entry.handoff_ref_count);
    }

    // The buffered Disembargo arrives LATE: the provision is gone, the spec
    // form misses in provisions_by_question — a benign drop, not a wedge.
    try peers.bc.releaseBufferedDisembargo();

    try setup.teardownImports(&peers);
    try harness.expectNoProvideState(&peers.c_to_b);
    try harness.expectNoProvideState(&peers.c_to_a);
}

// ============================================================================
// Frame-driven embargo semantics on one attached peer: G12 (per-provision
// scoping with colliding bytes), the Disembargo-before-Accept race,
// duplicate embargo ids, id reuse after completion, and the queue budget.
// ============================================================================

const FrameCapture = struct {
    allocator: std.mem.Allocator,
    returns: std.ArrayList(Entry) = .empty,

    /// An exception reason is DUPED on capture: `ret.exception.?.reason`
    /// borrows the decoded frame, which dies at the end of `send`.
    const Entry = struct {
        answer_id: u32,
        tag: protocol.ReturnTag,
        reason: ?[]u8 = null,
        /// For a results Return whose payload content is a single capability:
        /// the cap-table descriptor's id (e.g. the minted proxy export id).
        /// Null for exceptions and cap-less results.
        cap_id: ?u32 = null,
    };

    fn deinit(self: *FrameCapture) void {
        for (self.returns.items) |r| {
            if (r.reason) |bytes| self.allocator.free(bytes);
        }
        self.returns.deinit(self.allocator);
    }

    fn send(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *FrameCapture = castCtx(*FrameCapture, ctx);
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        if (decoded.tag == .@"return") {
            const ret = try decoded.asReturn();
            const reason: ?[]u8 = if (ret.tag == .exception) blk: {
                const ex = ret.exception orelse break :blk null;
                break :blk try self.allocator.dupe(u8, ex.reason);
            } else null;
            errdefer if (reason) |bytes| self.allocator.free(bytes);
            const cap_id: ?u32 = if (ret.tag == .results) blk: {
                const payload = ret.results orelse break :blk null;
                const cap = payload.content.getCapability() catch break :blk null;
                const cap_list = payload.cap_table orelse break :blk null;
                if (cap.id >= cap_list.len()) break :blk null;
                const desc_reader = cap_list.get(cap.id) catch break :blk null;
                const desc = protocol.CapDescriptor.fromReader(desc_reader) catch break :blk null;
                break :blk desc.id;
            } else null;
            try self.returns.append(self.allocator, .{
                .answer_id = ret.answer_id,
                .tag = ret.tag,
                .reason = reason,
                .cap_id = cap_id,
            });
        }
    }

    fn returnFor(self: *const FrameCapture, answer_id: u32) ?protocol.ReturnTag {
        for (self.returns.items) |r| {
            if (r.answer_id == answer_id) return r.tag;
        }
        return null;
    }

    /// The captured exception reason for `answer_id` — null when that answer
    /// never returned, or returned results.
    fn reasonFor(self: *const FrameCapture, answer_id: u32) ?[]const u8 {
        for (self.returns.items) |r| {
            if (r.answer_id == answer_id) return r.reason;
        }
        return null;
    }

    /// The captured result cap id for `answer_id` (see `Entry.cap_id`).
    fn capIdFor(self: *const FrameCapture, answer_id: u32) ?u32 {
        for (self.returns.items) |r| {
            if (r.answer_id == answer_id) return r.cap_id;
        }
        return null;
    }
};

/// One attached peer with a captured wire, N provisions injected by frames.
const FrameHost = struct {
    peer: Peer,
    capture: FrameCapture,
    index: ProvisionIndex,
    net: vat_network.LoopbackVatNetwork(Peer),
    dummy: Peer,

    fn init(self: *FrameHost, allocator: std.mem.Allocator, limits: peer_impl.ProvisionIndexLimits) !void {
        self.capture = .{ .allocator = allocator };
        self.peer = Peer.initDetached(allocator);
        self.peer.disableThreadAffinity();
        self.peer.setSendFrameOverride(&self.capture, FrameCapture.send);
        self.index = ProvisionIndex.init(allocator, limits);
        self.index.disableThreadAffinity();
        try self.peer.attachProvisionIndex(&self.index);
        self.net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
        self.dummy = Peer.initDetached(allocator);
        self.dummy.disableThreadAffinity();
        try self.net.register("frame-host", &self.dummy);
        self.dummy.attachVatNetwork(self.net.network());
    }

    fn deinitAll(self: *FrameHost) void {
        self.dummy.deinit();
        self.net.deinit();
        self.peer.deinit();
        self.index.deinit();
        self.capture.deinit();
    }

    /// Mint a fresh recipient token, returning its serialized bytes (owned).
    fn mintToken(self: *FrameHost, allocator: std.mem.Allocator, nonce: []const u8) ![]u8 {
        try self.net.register(nonce, &self.dummy);
        const d_network = self.dummy.vat_network orelse return error.NoVatNetwork;
        var introduction = try d_network.mintIntroduction(&self.dummy, nonce);
        defer introduction.deinit(allocator);
        return allocator.dupe(u8, introduction.to_await);
    }

    /// Inject Provide{qid, importedCap{export_id}, token}.
    fn injectProvide(self: *FrameHost, allocator: std.mem.Allocator, qid: u32, export_id: u32, token: []const u8) !void {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const recipient = try token_msg.getRootAnyPointer();
        const frame = try buildProvideFrame(allocator, qid, .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        }, recipient);
        defer allocator.free(frame);
        try self.peer.handleFrame(frame);
    }

    /// Inject Accept{qid, token, embargo}.
    fn injectAccept(self: *FrameHost, allocator: std.mem.Allocator, qid: u32, token: []const u8, embargo: ?[]const u8) !void {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const provision = try token_msg.getRootAnyPointer();
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildAccept(qid, provision, embargo);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try self.peer.handleFrame(frame);
    }

    /// Inject the spec-form accept-Disembargo naming a Provide question.
    fn injectAcceptDisembargo(self: *FrameHost, allocator: std.mem.Allocator, provide_qid: u32, embargo: []const u8) !void {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildDisembargoAccept(.{
            .tag = .promisedAnswer,
            .imported_cap = null,
            .promised_answer = .{ .question_id = provide_qid, .transform = .{ .list = null } },
        }, embargo);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try self.peer.handleFrame(frame);
    }
};

test "L4 G12: colliding embargo bytes across two provisions release independently (per-provision scoping)" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var carol1 = Carol{};
    const carol1_id = try host.peer.addExport(.{ .ctx = &carol1, .on_call = Carol.onCall });
    var carol2 = Carol{};
    const carol2_id = try host.peer.addExport(.{ .ctx = &carol2, .on_call = Carol.onCall });

    const token1 = try host.mintToken(allocator, "g12-token-1");
    defer allocator.free(token1);
    const token2 = try host.mintToken(allocator, "g12-token-2");
    defer allocator.free(token2);

    try host.injectProvide(allocator, 10, carol1_id, token1);
    try host.injectProvide(allocator, 11, carol2_id, token2);

    // Two same-peer embargoed Accepts with EQUAL bytes — the collision that
    // co-drained in a byte-keyed store.
    const e = "\x00\x00\x00\x00\x00\x00\x00\x00";
    try host.injectAccept(allocator, 100, token1, e);
    try host.injectAccept(allocator, 101, token2, e);
    try std.testing.expectEqual(@as(usize, 2), host.peer.cross_peer_pending_accepts.count());
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(100));
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(101));

    // Disembargo naming provision #1 releases ONLY answer 100.
    try host.injectAcceptDisembargo(allocator, 10, e);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(100));
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(101));
    try std.testing.expectEqual(@as(usize, 1), host.peer.cross_peer_pending_accepts.count());

    // Disembargo naming provision #2 releases the other.
    try host.injectAcceptDisembargo(allocator, 11, e);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(101));
    try std.testing.expectEqual(@as(usize, 0), host.peer.cross_peer_pending_accepts.count());
}

test "L4 mixed: same-peer and cross-peer accepts with colliding bytes stay independent" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    // A sibling holder peer attached to the same index.
    var holder_capture = FrameCapture{ .allocator = allocator };
    defer holder_capture.deinit();
    var holder = Peer.initDetached(allocator);
    holder.disableThreadAffinity();
    holder.setSendFrameOverride(&holder_capture, FrameCapture.send);
    defer holder.deinit();
    try holder.attachProvisionIndex(&host.index);

    var carol1 = Carol{};
    const carol1_id = try host.peer.addExport(.{ .ctx = &carol1, .on_call = Carol.onCall });
    var carol2 = Carol{};
    const carol2_id = try host.peer.addExport(.{ .ctx = &carol2, .on_call = Carol.onCall });

    const token_x = try host.mintToken(allocator, "mixed-x");
    defer allocator.free(token_x);
    const token_y = try host.mintToken(allocator, "mixed-y");
    defer allocator.free(token_y);
    try host.injectProvide(allocator, 20, carol1_id, token_x);
    try host.injectProvide(allocator, 21, carol2_id, token_y);

    const e = "same-bytes";
    // X: same-peer embargoed accept (on the owner itself).
    try host.injectAccept(allocator, 200, token_x, e);
    // Y: CROSS-peer embargoed accept (arrives on the sibling holder).
    {
        var token_msg = try message.Message.initUnvalidated(allocator, token_y);
        defer token_msg.deinit();
        const provision = try token_msg.getRootAnyPointer();
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildAccept(300, provision, e);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try holder.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(usize, 1), host.peer.cross_peer_pending_accepts.count());
    try std.testing.expectEqual(@as(usize, 1), holder.cross_peer_pending_accepts.count());

    // Disembargo naming Y (the cross-peer one): only answer 300 releases,
    // on the HOLDER's wire.
    try host.injectAcceptDisembargo(allocator, 21, e);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), holder_capture.returnFor(300));
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(200));
    try std.testing.expectEqual(@as(usize, 1), host.peer.cross_peer_pending_accepts.count());

    // Disembargo naming X releases the same-peer one.
    try host.injectAcceptDisembargo(allocator, 20, e);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(200));
    try std.testing.expectEqual(@as(usize, 0), host.peer.cross_peer_pending_accepts.count());
}

test "L4 race: a Disembargo arriving before its Accept leaves a tombstone; the Accept serves immediately" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var carol = Carol{};
    const carol_id = try host.peer.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    const token = try host.mintToken(allocator, "race-token");
    defer allocator.free(token);
    try host.injectProvide(allocator, 30, carol_id, token);

    const prov = host.peer.provisions_by_question.get(30) orelse return error.NoProvision;

    // Disembargo FIRST (two independent connections make this real).
    try host.injectAcceptDisembargo(allocator, 30, "early");
    try std.testing.expectEqual(@as(usize, 1), prov.embargoes.count());

    // The Accept then serves immediately and consumes the tombstone.
    try host.injectAccept(allocator, 310, token, "early");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(310));
    try std.testing.expectEqual(@as(usize, 0), prov.embargoes.count());
    try std.testing.expectEqual(@as(usize, 0), host.peer.cross_peer_pending_accepts.count());
}

test "L4 duplicates: a second Accept reusing live embargo bytes is rejected; consumed bytes are reusable" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var carol = Carol{};
    const carol_id = try host.peer.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    const token = try host.mintToken(allocator, "dup-token");
    defer allocator.free(token);
    try host.injectProvide(allocator, 40, carol_id, token);

    try host.injectAccept(allocator, 400, token, "ebytes");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(400));

    // Duplicate while the entry lives: rejected, the original untouched.
    try host.injectAccept(allocator, 401, token, "ebytes");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), host.capture.returnFor(401));
    try std.testing.expectEqual(@as(usize, 1), host.peer.cross_peer_pending_accepts.count());

    // Release consumes the entry...
    try host.injectAcceptDisembargo(allocator, 40, "ebytes");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(400));

    // ...and the bytes become reusable by a NEW embargoed Accept (C++ parity).
    try host.injectAccept(allocator, 402, token, "ebytes");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(402));
    try host.injectAcceptDisembargo(allocator, 40, "ebytes");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(402));
}

test "L4 budget: queued-accept exhaustion is a distinguishable exception, not a silent drop" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{ .max_queued_accepts = 1 });
    defer host.deinitAll();

    var carol = Carol{};
    const carol_id = try host.peer.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    const token = try host.mintToken(allocator, "budget-token");
    defer allocator.free(token);
    try host.injectProvide(allocator, 50, carol_id, token);

    try host.injectAccept(allocator, 500, token, "e1");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(500));
    try host.injectAccept(allocator, 501, token, "e2");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), host.capture.returnFor(501));

    try host.injectAcceptDisembargo(allocator, 50, "e1");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(500));
}

// ============================================================================
// Owner-first teardown with a QUEUED embargoed accept (the §10.1 wedge test):
// the deinit drain fails the accept LOUDLY across the still-live sibling.
// ============================================================================

test "L4 teardown: owner deinit with a queued embargoed accept fails it loudly (no wedge)" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer {
        peers.ab.forwarding = false;
        peers.bc.forwarding = false;
        peers.ac.forwarding = false;
        peers.ab.deinitBuffered();
        peers.bc.deinitBuffered();
        peers.ac.deinitBuffered();
        peers.a_to_b.deinit();
        peers.a_to_c.deinit();
        peers.b_to_a.deinit();
        peers.b_to_c.deinit();
        peers.c_to_a.deinit();
        // c_to_b deinited mid-test.
    }

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try peers.c_to_b.attachProvisionIndex(&index);
    try peers.c_to_a.attachProvisionIndex(&index);

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l4-tdown");
    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);

    var pipelined = TolerantCall{};
    _ = try peers.a_to_b.sendCall(setup.promise_import, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &pipelined, null, TolerantCall.onReturn);
    peers.bc.buffer_disembargos_to_right = true;
    try setup.originate(&peers, "vatc-l4-tdown");
    try std.testing.expect(!pickup.fired);
    try std.testing.expectEqual(@as(usize, 1), peers.c_to_a.cross_peer_pending_accepts.count());

    // The owner dies with the accept queued. Its B-side link is severed (a
    // real disconnect), but the SIBLING peers stay live: the deinit drain
    // must deliver the exception Return through c_to_a -> A.
    peers.bc.forwarding = false;
    peers.c_to_b.deinit();

    try std.testing.expect(pickup.fired);
    try std.testing.expectEqualStrings("exception", @tagName(pickup.ret_tag.?));
    try std.testing.expectEqualStrings("provision lost: provider connection closed", pickup.exception_reason.?);
    try std.testing.expectEqual(@as(usize, 0), peers.c_to_a.cross_peer_pending_accepts.count());
    try std.testing.expectEqual(@as(usize, 0), index.by_key.count());

    try peers.a_to_b.releaseImport(setup.promise_import, 1);
    try peers.a_to_b.releaseImport(setup.introducer_import, 1);
}

// ============================================================================
// L5: Accept-before-Provide parks (order-independent rendezvous), and the
// Provide adopts the awaiting provision and drains its parked accepts.
// ============================================================================

test "L5: an early Accept parks; the Provide adopts and serves it (both orders, both embargo modes)" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var carol = Carol{};
    const carol_id = try host.peer.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    const token = try host.mintToken(allocator, "l5-token");
    defer allocator.free(token);

    // Non-embargoed early Accept: parked, NO Return yet.
    try host.injectAccept(allocator, 600, token, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(600));
    try std.testing.expectEqual(@as(usize, 1), host.index.parked_accept_count);
    try std.testing.expectEqual(@as(usize, 1), host.peer.cross_peer_pending_accepts.count());

    // Embargoed early Accept parks alongside it on the SAME awaiting provision.
    try host.injectAccept(allocator, 601, token, "early-e");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(601));
    try std.testing.expectEqual(@as(usize, 2), host.index.parked_accept_count);
    try std.testing.expectEqual(@as(usize, 1), host.index.by_key.count());

    // The Provide arrives: adoption serves the plain accept immediately and
    // moves the embargoed one into the provision's embargo map.
    try host.injectProvide(allocator, 60, carol_id, token);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(600));
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(601));
    try std.testing.expectEqual(@as(usize, 0), host.index.parked_accept_count);
    try std.testing.expectEqual(@as(usize, 1), host.index.queued_accept_count);

    // Its Disembargo releases it like any queued accept.
    try host.injectAcceptDisembargo(allocator, 60, "early-e");
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(601));
    try std.testing.expectEqual(@as(usize, 0), host.index.queued_accept_count);
    try std.testing.expectEqual(@as(usize, 0), host.peer.cross_peer_pending_accepts.count());
}

test "L5 budget: park exhaustion is a distinguishable exception" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{ .max_parked_accepts = 1 });
    defer host.deinitAll();

    const token = try host.mintToken(allocator, "l5-budget");
    defer allocator.free(token);

    try host.injectAccept(allocator, 610, token, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(610));
    try host.injectAccept(allocator, 611, token, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), host.capture.returnFor(611));
}

test "L5 V2-M5: cancelling the last parked accept unindexes and destroys the awaiting provision" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    const token = try host.mintToken(allocator, "l5-m5");
    defer allocator.free(token);

    try host.injectAccept(allocator, 620, token, "zz");
    try std.testing.expectEqual(@as(usize, 1), host.index.by_key.count());

    // The acceptor cancels (Finish on the accept answer): the record clears,
    // the awaiting provision loses its last parked accept and must die —
    // not squat on the budget as an unadoptable zombie.
    const finish_frame = try buildFinishFrame(allocator, 620);
    defer allocator.free(finish_frame);
    try host.peer.handleFrame(finish_frame);

    try std.testing.expectEqual(@as(usize, 0), host.index.by_key.count());
    try std.testing.expectEqual(@as(usize, 0), host.index.provision_count);
    try std.testing.expectEqual(@as(usize, 0), host.peer.cross_peer_pending_accepts.count());

    // The token is fully reusable afterwards.
    var carol = Carol{};
    const carol_id = try host.peer.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    try host.injectProvide(allocator, 62, carol_id, token);
    try host.injectAccept(allocator, 621, token, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(621));
}

// ============================================================================
// L9: a TTL for parked accepts. Parking is UNAUTHENTICATED — the recipient
// token is arbitrary bytes and needs no prior Provide — so without a time
// bound one stranger connection holds vat-wide park slots until `Peer.deinit`.
// ============================================================================

test "L9 characterization (no TTL): a stranger's parked accept starves an unrelated legit token" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    // One park slot makes the vat-wide starvation observable in two frames;
    // the production default (1024) is the same hole with more arithmetic.
    try host.init(allocator, .{ .max_parked_accepts = 1 });
    defer host.deinitAll();

    const stranger = try host.mintToken(allocator, "l9-stranger-a");
    defer allocator.free(stranger);
    const legit = try host.mintToken(allocator, "l9-legit-a");
    defer allocator.free(legit);

    // No Provide, no bootstrap, no authentication: an arbitrary token parks.
    try host.injectAccept(allocator, 900, stranger, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(900));
    try std.testing.expectEqual(@as(usize, 1), host.index.parked_accept_count);

    // Arbitrarily long afterwards — the park has NO time bound at all, and the
    // only release point is `Peer.deinit` — an unrelated legitimate token
    // cannot park. The budgets are vat-wide, so this starves a SIBLING peer's
    // legitimate rendezvous just as effectively.
    try host.injectAccept(allocator, 901, legit, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), host.capture.returnFor(901));
    try std.testing.expectEqualStrings(
        "provision park budget exhausted",
        host.capture.reasonFor(901).?,
    );
    try std.testing.expectEqual(@as(usize, 1), host.index.parked_accept_count);
    // The squatter is untouched: still parked, still unreturned.
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(900));
    try std.testing.expectEqual(@as(usize, 1), host.index.by_key.count());
}

test "L9: an expired parked accept is evicted at the next park attempt; the legit token gets the slot" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{ .max_parked_accepts = 1, .park_ttl_ms = 30_000 });
    defer host.deinitAll();

    // The clock is INDEX-owned, never a peer's: the connection whose accepts
    // are being timed out must not be able to steer their expiry.
    var tc = rpc_time.TestClock{};
    host.index.setClock(tc.clock());

    const stranger = try host.mintToken(allocator, "l9-stranger-b");
    defer allocator.free(stranger);
    const legit = try host.mintToken(allocator, "l9-legit-b");
    defer allocator.free(legit);

    // Park WITH an embargo so the byte counter is exercised too.
    const embargo = "l9-embargo";
    try host.injectAccept(allocator, 910, stranger, embargo);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(910));
    try std.testing.expectEqual(@as(usize, 1), host.index.parked_accept_count);
    try std.testing.expectEqual(@as(usize, embargo.len), host.index.parked_accept_bytes);
    try std.testing.expectEqual(@as(usize, 1), host.index.by_key.count());

    tc.advanceMs(31_000);

    try host.injectAccept(allocator, 911, legit, null);

    // (1) The squatter was evicted LOUDLY — an exception Return, never a
    //     silent drop.
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), host.capture.returnFor(910));
    try std.testing.expectEqualStrings("parked accept expired", host.capture.reasonFor(910).?);
    // (2) The legitimate accept got the slot: it PARKED (no Return at all).
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(911));
    // (3) The count moved to the legitimate entry...
    try std.testing.expectEqual(@as(usize, 1), host.index.parked_accept_count);
    // (4) ...and so did the BYTES. This is the assertion a sweep that leans on
    //     `failPendingAccept` alone gets wrong: that helper adjusts only the
    //     QUEUED counters, so an eviction would leak the parked byte budget
    //     upward forever and permanently shrink the bound the TTL protects.
    try std.testing.expectEqual(@as(usize, 0), host.index.parked_accept_bytes);
    // The squatter's now-unadoptable awaiting provision was unindexed; only
    // the legitimate one remains.
    try std.testing.expectEqual(@as(usize, 1), host.index.by_key.count());
    try std.testing.expectEqual(@as(usize, 1), host.index.provision_count);
    try std.testing.expectEqual(@as(usize, 1), host.peer.cross_peer_pending_accepts.count());
    // Teardown under std.testing.allocator proves the eviction freed both
    // owners' allocations (index-owned parked embargo, peer-owned key dupe).
}

test "L9: the sweep evicts only what expired — a younger sibling survives and is still adopted" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{ .park_ttl_ms = 30_000 });
    defer host.deinitAll();

    var tc = rpc_time.TestClock{};
    host.index.setClock(tc.clock());

    const token = try host.mintToken(allocator, "l9-partial");
    defer allocator.free(token);
    const other = try host.mintToken(allocator, "l9-partial-other");
    defer allocator.free(other);

    // t=0: the old entry, embargoed (deadline 30s).
    try host.injectAccept(allocator, 920, token, "e-old");
    // t=20s: a younger sibling on the SAME provision (deadline 50s).
    tc.advanceMs(20_000);
    try host.injectAccept(allocator, 921, token, null);
    try std.testing.expectEqual(@as(usize, 2), host.index.parked_accept_count);
    try std.testing.expectEqual(@as(usize, 1), host.index.by_key.count());

    // t=31s: an unrelated Accept drives the sweep. Only the old entry is past
    // its deadline, so only it is evicted — from the MIDDLE of a live parked
    // list, leaving its provision indexed and still adoptable.
    tc.advanceMs(11_000);
    try host.injectAccept(allocator, 922, other, null);

    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), host.capture.returnFor(920));
    try std.testing.expectEqualStrings("parked accept expired", host.capture.reasonFor(920).?);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(921));
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(922));
    try std.testing.expectEqual(@as(usize, 2), host.index.parked_accept_count);
    try std.testing.expectEqual(@as(usize, 0), host.index.parked_accept_bytes);
    try std.testing.expectEqual(@as(usize, 2), host.index.by_key.count());

    // The survivor is still a live rendezvous: the Provide adopts and serves it.
    var carol = Carol{};
    const carol_id = try host.peer.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    try host.injectProvide(allocator, 92, carol_id, token);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(921));
    try std.testing.expectEqual(@as(usize, 1), host.index.parked_accept_count);
}

test "L5 F-2: index deinit fails parked accepts; the acceptor's follow-up Finish is a clean miss" {
    const allocator = std.testing.allocator;

    var capture = FrameCapture{ .allocator = allocator };
    defer capture.deinit();
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, FrameCapture.send);

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    var dummy = Peer.initDetached(allocator);
    dummy.disableThreadAffinity();
    defer dummy.deinit();
    try net.register("l5-f2", &dummy);
    dummy.attachVatNetwork(net.network());

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    try peer.attachProvisionIndex(&index);

    const d_network = dummy.vat_network orelse return error.NoVatNetwork;
    var introduction = try d_network.mintIntroduction(&dummy, "l5-f2");
    defer introduction.deinit(allocator);
    var token_msg = try message.Message.initUnvalidated(allocator, introduction.to_await);
    defer token_msg.deinit();
    const provision_ptr = try token_msg.getRootAnyPointer();

    var builder = protocol.MessageBuilder.init(allocator);
    try builder.buildAccept(630, provision_ptr, "f2-bytes");
    const accept_frame = try builder.finish();
    builder.deinit();
    defer allocator.free(accept_frame);
    try peer.handleFrame(accept_frame);
    try std.testing.expectEqual(@as(usize, 1), peer.cross_peer_pending_accepts.count());

    // Index dies with the accept parked: the accept fails LOUDLY (delivered
    // exception Return) and the holder record is removed BEFORE the send —
    // so the natural follow-ups touch nothing freed.
    index.deinit();
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), capture.returnFor(630));
    try std.testing.expectEqual(@as(usize, 0), peer.cross_peer_pending_accepts.count());

    // Follow-up 1: the acceptor Finishes the failed answer — clean miss.
    const finish_frame = try buildFinishFrame(allocator, 630);
    defer allocator.free(finish_frame);
    try peer.handleFrame(finish_frame);
    // Follow-up 2: peer deinit (in the test's defers) — no UAF, no leak,
    // under std.testing.allocator.
}

// ============================================================================
// L6: promised-answer provide targets serve cross-peer via OWNER-SIDE
// re-resolution — the stored inbound-answer id is consumed only on the peer
// whose answer table it names; no id ever crosses a connection.
// ============================================================================

test "L6: a promisedAnswer-TARGET Provide (settled answer cap) resolves at Provide time and serves cross-peer" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    // A sibling holder attached to the same index.
    var holder_capture = FrameCapture{ .allocator = allocator };
    defer holder_capture.deinit();
    var holder = Peer.initDetached(allocator);
    holder.disableThreadAffinity();
    holder.setSendFrameOverride(&holder_capture, FrameCapture.send);
    defer holder.deinit();
    try holder.attachProvisionIndex(&host.index);

    // Give the owner a RESOLVED inbound answer whose content is a cap:
    // a bootstrap question (its Return carries the bootstrap export).
    var carol = Carol{};
    _ = try host.peer.setBootstrap(.{ .ctx = &carol, .on_call = Carol.onCall });
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildBootstrap(7);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try host.peer.handleFrame(frame);
    }

    // Provide whose target is promisedAnswer{7} (empty transform = the
    // bootstrap cap itself). Stored ops-based on the owner.
    const token = try host.mintToken(allocator, "l6-token");
    defer allocator.free(token);
    {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const recipient = try token_msg.getRootAnyPointer();
        const frame = blk: {
            var builder = protocol.MessageBuilder.init(allocator);
            defer builder.deinit();
            try builder.buildProvide(70, .{
                .tag = .promisedAnswer,
                .imported_cap = null,
                .promised_answer = .{ .question_id = 7, .transform = .{ .list = null } },
            }, recipient);
            break :blk try builder.finish();
        };
        defer allocator.free(frame);
        try host.peer.handleFrame(frame);
    }

    // Cross-peer Accept on the sibling: the owner re-resolves its own answer
    // to the bootstrap export, pins it, and mints a proxy on the holder.
    {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const provision = try token_msg.getRootAnyPointer();
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildAccept(700, provision, null);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try holder.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), holder_capture.returnFor(700));
    try std.testing.expectEqual(@as(usize, 1), host.peer.cross_peer_proxy_links.items.len);

    // The single-peer promised path stays byte-identical (receiverAnswer
    // re-emission): a same-peer Accept still serves through the legacy arm.
    {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const provision = try token_msg.getRootAnyPointer();
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildAccept(701, provision, null);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try host.peer.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(701));
}

/// Manually assemble an `.active` provision with a stored `.promised` target
/// (on the wire this shape arises only when the Provide's promisedAnswer
/// target resolves to a PROMISE-valued cap — rare; the serve arm must handle
/// it regardless). Takes the index + owner refs exactly like registration.
/// `owner` names the peer whose answer space the stored ops belong to; it is
/// usually `&host.peer`, but the L10 tests below own a peer with a live
/// remote so it can hold real IMPORTS.
fn installPromisedProvision(
    host: *FrameHost,
    owner: *Peer,
    token: []const u8,
    qid: u32,
    answer_qid: u32,
) !*ProvisionIndex.Provision {
    const idx = &host.index;
    const prov = try idx.allocator.create(ProvisionIndex.Provision);
    prov.* = .{
        .allocator = idx.allocator,
        .recipient_key = try idx.allocator.dupe(u8, token),
        .embargoes = std.StringHashMap(ProvisionIndex.ProvisionEmbargo).init(idx.allocator),
        .state = .active,
        .owner = owner,
        .provide_question_id = qid,
        .target = .{ .promised = try cap_table.OwnedPromisedAnswer.fromQuestionAndOps(idx.allocator, answer_qid, &.{}) },
        .indexed = true,
    };
    try idx.by_key.put(prov.recipient_key, prov);
    try owner.provisions_by_question.put(qid, prov);
    prov.retain(); // index +1
    prov.retain(); // owner +1
    idx.provision_count += 1;
    idx.provision_key_bytes += prov.recipient_key.len;
    return prov;
}

test "L6: a stored .promised target serves cross-peer by owner-side ops re-resolution" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var holder_capture = FrameCapture{ .allocator = allocator };
    defer holder_capture.deinit();
    var holder = Peer.initDetached(allocator);
    holder.disableThreadAffinity();
    holder.setSendFrameOverride(&holder_capture, FrameCapture.send);
    defer holder.deinit();
    try holder.attachProvisionIndex(&host.index);

    // A resolved inbound answer on the OWNER whose content is a cap
    // (bootstrap answers persist for the connection lifetime).
    var carol = Carol{};
    _ = try host.peer.setBootstrap(.{ .ctx = &carol, .on_call = Carol.onCall });
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildBootstrap(7);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try host.peer.handleFrame(frame);
    }

    const token = try host.mintToken(allocator, "l6-ops");
    defer allocator.free(token);
    _ = try installPromisedProvision(&host, &host.peer, token, 70, 7);

    // Cross-peer Accept: the OWNER re-resolves its own answer via the stored
    // ops (the id never crosses a connection), pins the resulting export, and
    // mints the proxy on the holder.
    {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const provision = try token_msg.getRootAnyPointer();
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildAccept(750, provision, null);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try holder.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), holder_capture.returnFor(750));
    try std.testing.expectEqual(@as(usize, 1), host.peer.cross_peer_proxy_links.items.len);

    // Owner Finish drains the provision.
    {
        const frame = try buildFinishFrame(allocator, 70);
        defer allocator.free(frame);
        try host.peer.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(usize, 0), host.index.by_key.count());
}

test "L6 fail-closed: a stored .promised target naming a vanished answer serves an exception, not a wedge" {
    const allocator = std.testing.allocator;
    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var holder_capture = FrameCapture{ .allocator = allocator };
    defer holder_capture.deinit();
    var holder = Peer.initDetached(allocator);
    holder.disableThreadAffinity();
    holder.setSendFrameOverride(&holder_capture, FrameCapture.send);
    defer holder.deinit();
    try holder.attachProvisionIndex(&host.index);

    const token = try host.mintToken(allocator, "l6-gone");
    defer allocator.free(token);
    // Answer 99 does not exist (Finished/never-was) — the stored ops dangle.
    _ = try installPromisedProvision(&host, &host.peer, token, 80, 99);

    {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const provision = try token_msg.getRootAnyPointer();
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildAccept(800, provision, null);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try holder.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), holder_capture.returnFor(800));

    {
        const frame = try buildFinishFrame(allocator, 80);
        defer allocator.free(frame);
        try host.peer.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(usize, 0), host.index.by_key.count());
}

// NOTE: a six-peer end-to-end with a promisedAnswer PROVIDED TARGET is not
// expressible through the public origination API today: sendCall and
// sendBootstrap auto-Finish their questions, clearing the resolved answer the
// Provide would have to name before resolvePromiseExportToThirdParty could
// run. The frame-driven tests above cover the cross-peer promised serve; a
// suppress-auto-finish call variant is future DX work (documented in
// docs/supported-surface.md).

// ============================================================================
// L7: accept-embargo ids come from the installed entropy source (16 bytes on
// the wire), with the legacy 8-byte counter as the no-source fallback (every
// earlier embargo test in this file runs the fallback and still passes).
// ============================================================================

const TestEntropy = struct {
    next: u8 = 0,

    fn fill(ctx: *anyopaque, buf: []u8) void {
        const self: *TestEntropy = castCtx(*TestEntropy, ctx);
        for (buf) |*b| {
            b.* = self.next;
            self.next +%= 1;
        }
    }

    fn source(self: *TestEntropy) peer_impl.EntropySource {
        return .{ .ctx = self, .fill = TestEntropy.fill };
    }
};

test "L7: a minted accept-embargo id is the entropy source's 16-byte stream on the wire" {
    const allocator = std.testing.allocator;
    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer peers.deinitPeers();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try peers.c_to_b.attachProvisionIndex(&index);
    try peers.c_to_a.attachProvisionIndex(&index);

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l7");
    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);

    // The RECIPIENT's promise peer mints the embargo id — install the
    // deterministic source there.
    var entropy = TestEntropy{};
    peers.a_to_b.setEntropySource(entropy.source());

    // Force the embargo and hold the Accept so its frame can be inspected.
    var pipelined = TolerantCall{};
    _ = try peers.a_to_b.sendCall(setup.promise_import, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &pipelined, null, TolerantCall.onReturn);
    peers.ac.buffer_accepts_to_right = true;
    try setup.originate(&peers, "vatc-l7");

    // THE WIRE ASSERTION: the Accept's embargo field is the 16-byte stream.
    {
        const frame = peers.ac.buffered_accept orelse return error.NoBufferedAccept;
        var decoded = try protocol.DecodedMessage.init(allocator, frame);
        defer decoded.deinit();
        const accept = try decoded.asAccept();
        const embargo = accept.embargo orelse return error.NoEmbargoOnAccept;
        try std.testing.expectEqual(@as(usize, 16), embargo.len);
        var expected: [16]u8 = undefined;
        for (&expected, 0..) |*b, i| b.* = @intCast(i);
        try std.testing.expectEqualSlices(u8, &expected, embargo);
    }

    // The handoff still completes end-to-end with the random id.
    try peers.ac.releaseBufferedAccept();
    try std.testing.expectEqualStrings("results", @tagName(pickup.ret_tag.?));
    const accepted = pickup.carol_import_id orelse return error.NoAcceptedCap;
    try peers.a_to_c.releaseImport(accepted, 1);
    try setup.teardownImports(&peers);
    try harness.expectNoProvideState(&peers.c_to_b);
    try harness.expectNoProvideState(&peers.c_to_a);
}

// ============================================================================
// L8: the Vat facade — one object per vat: enroll a peer per connection.
// ============================================================================

test "L8 Vat: entropy is mandatory (seed or io), enrollment wires index + entropy, handoff works" {
    const allocator = std.testing.allocator;

    // Neither seed nor io: refused.
    try std.testing.expectError(error.EntropyUnavailable, peer_impl.Vat.init(allocator, .{}));

    var peers: SixPeers = undefined;
    peers.init(allocator);
    defer peers.deinitPeers();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    var vat = try peer_impl.Vat.init(allocator, .{ .seed = @as([32]u8, @splat(7)) });
    vat.disableThreadAffinity();
    defer vat.deinit();
    try vat.enroll(&peers.c_to_b);
    try vat.enroll(&peers.c_to_a);
    // Double-enroll refused.
    try std.testing.expectError(error.PeerAlreadyAttachedToProvisionIndex, vat.enroll(&peers.c_to_b));

    var setup = HandoffSetup{};
    try setup.run(&peers, &net, "vatc-l8");
    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);
    try setup.originate(&peers, "vatc-l8");

    try std.testing.expectEqualStrings("results", @tagName(pickup.ret_tag.?));
    const accepted = pickup.carol_import_id orelse return error.NoAcceptedCap;
    var call = TolerantCall{};
    _ = try peers.a_to_c.sendCall(accepted, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &call, null, TolerantCall.onReturn);
    try std.testing.expectEqual(@as(?u32, 42), call.result_n);

    // Deterministic seed => deterministic stream: two fresh Vats with the
    // same seed produce identical bytes (the L7 wire test pins how a minted
    // id reaches the frame; this pins the seed seam).
    var vat2 = try peer_impl.Vat.init(allocator, .{ .seed = @as([32]u8, @splat(9)) });
    vat2.disableThreadAffinity();
    defer vat2.deinit();
    var vat3 = try peer_impl.Vat.init(allocator, .{ .seed = @as([32]u8, @splat(9)) });
    vat3.disableThreadAffinity();
    defer vat3.deinit();
    var a: [16]u8 = undefined;
    var b: [16]u8 = undefined;
    vat2.rng.random().bytes(&a);
    vat3.rng.random().bytes(&b);
    try std.testing.expectEqualSlices(u8, &a, &b);

    try peers.a_to_c.releaseImport(accepted, 1);
    try setup.teardownImports(&peers);
    try harness.expectNoProvideState(&peers.c_to_b);
    try harness.expectNoProvideState(&peers.c_to_a);
}

// ============================================================================
// L17: a cross-peer serve whose provide target is a capability the host only
// IMPORTS (`receiverHosted`) is SERVED — the receiverHosted lift.
//
// Before this landing both sites refused with
// `CrossPeerReceiverHostedTargetUnsupported` (the old L10 fail-closed pins).
// The lift serves them through the same proxy machinery as exported targets:
// the proxy minted on the accept peer forwards to the OWNER's *import* via
// the forwarded-vine call path, and the import is kept alive by a handoff
// pin (`handoff_pin_count`) that also WITHHOLDS outbound Release frames into
// `deferred_release` — a `receiverHosted` descriptor grants no transferable
// wire reference, so the withheld count is the only thing keeping the
// introducer-side export alive across the [Provide, serve) window.
//
// TWO independent sites serve, discriminated exactly like the old pins:
//   1. the stored `.local` target whose `origin_code` decodes to
//      `receiverHosted` — pinned at PROVIDE registration
//      (`target_import_pinned`), plus a serve-time pin owned by the proxy;
//   2. the OWNER-SIDE re-resolution of a stored `.promised` target landing on
//      `.imported` — no Provide-time pin exists (the answer's cap was still a
//      promise then); the serve-time pin alone covers [serve, proxy-destroy),
//      and the [Provide, serve) window is covered by the stored answer's own
//      liveness (a vanished target still fails closed — see the witness
//      below).
// ============================================================================

/// The residual fail-closed reason: a re-resolved target whose import has
/// died serves an exception with this exact name (the lift does not resurrect
/// dead capabilities). Spelled as a literal, never derived from the error, so
/// a rename surfaces here as a RED mismatch instead of quietly tracking.
const target_unavailable_reason = "CrossPeerProvisionTargetUnavailable";

/// Inject Call{qid, importedCap{target}, getNumber} on `peer` — how a
/// capture-only "recipient" exercises a served proxy export.
fn injectNumberCall(peer: *Peer, allocator: std.mem.Allocator, qid: u32, target_id: u32) !void {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call_builder = try builder.beginCall(qid, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID);
    try call_builder.setTargetImportedCap(target_id);
    call_builder.setSendResultsToCaller();
    _ = try call_builder.payloadTyped();
    const frame = try builder.finish();
    defer allocator.free(frame);
    try peer.handleFrame(frame);
}

/// Inject Finish{qid, releaseResultCaps=false} on `peer` — the recipient
/// finishing its Accept question. Releases the ANSWER-held ref the recorded
/// Return holds on its result caps (the proxy export) while keeping the wire
/// reference the Return granted.
fn injectFinish(peer: *Peer, allocator: std.mem.Allocator, qid: u32) !void {
    const frame = try buildFinishFrame(allocator, qid);
    defer allocator.free(frame);
    try peer.handleFrame(frame);
}

/// Inject Release{id, count} on `peer` — the recipient dropping its wire
/// reference on a served proxy export.
fn injectRelease(peer: *Peer, allocator: std.mem.Allocator, id: u32, count: u32) !void {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildRelease(id, count);
    const frame = try builder.finish();
    defer allocator.free(frame);
    try peer.handleFrame(frame);
}

/// Inject the spec-form accept-Disembargo naming a Provide question on `peer`
/// (the OWNER connection — where the introducer's forwarded Disembargo
/// arrives).
fn injectAcceptDisembargoOn(peer: *Peer, allocator: std.mem.Allocator, provide_qid: u32, embargo: []const u8) !void {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildDisembargoAccept(.{
        .tag = .promisedAnswer,
        .imported_cap = null,
        .promised_answer = .{ .question_id = provide_qid, .transform = .{ .list = null } },
    }, embargo);
    const frame = try builder.finish();
    defer allocator.free(frame);
    try peer.handleFrame(frame);
}

/// A VatC whose OWNER peer has a live remote, so it can hold genuine IMPORTS
/// — the precondition both L10 sites need, and one a capture-only peer cannot
/// reach (an import only ever arrives on an inbound frame carrying a cap).
///
/// Pairs with a `FrameHost`, which supplies the shared `ProvisionIndex`, the
/// recipient-token mint, and the ACCEPT peer whose Return is captured.
/// `remote` stands in for the vat that actually HOSTS the provided capability.
const ImportOwnerVat = struct {
    owner: Peer,
    remote: Peer,
    link: PairLink,
    carol: Carol = .{},
    carol_returner: CapReturner = undefined,
    carol_export_on_remote: u32 = 0,
    /// The owner's IMPORT of Carol — a capability the owner does NOT host.
    carol_import: u32 = 0,

    fn init(self: *ImportOwnerVat, allocator: std.mem.Allocator, index: *ProvisionIndex) !void {
        // The caller declares this `undefined` (the file's fixture idiom), so
        // the defaulted fields must be written explicitly — a defaulted field
        // of an `undefined` struct is still undefined.
        self.carol = .{};
        self.link = .{};
        self.owner = Peer.initDetached(allocator);
        self.owner.disableThreadAffinity();
        self.remote = Peer.initDetached(allocator);
        self.remote.disableThreadAffinity();
        self.link.left = &self.remote;
        self.link.right = &self.owner;
        self.remote.setSendFrameOverride(&self.link, PairLink.leftSend);
        self.owner.setSendFrameOverride(&self.link, PairLink.rightSend);
        try self.owner.attachProvisionIndex(index);

        // Wire-honest grant: Carol is exported by the REMOTE and reaches the
        // owner as a Return-carried cap, so the owner holds a real import with
        // a real wire reference — not a hand-poked table entry.
        self.carol_export_on_remote = try self.remote.addExport(.{ .ctx = &self.carol, .on_call = Carol.onCall });
        self.carol_returner = .{ .export_id = self.carol_export_on_remote };
        self.carol_import = try grantCapVia(&self.remote, &self.owner, &self.carol_returner);
    }

    fn deinitAll(self: *ImportOwnerVat) void {
        self.link.forwarding = false;
        self.link.deinitBuffered();
        self.owner.deinit();
        self.remote.deinit();
    }
};

/// Stage the site-1 shape: put `.imported` behind an EXPORT id.
/// `resolvePromiseExportToImport` is the lever: the owner exports a promise
/// (the remote imports it), then resolves that promise to the capability the
/// owner only imports. From then on a Provide naming the promise export
/// resolves — through `resolveProvideImportedCapForPeer`'s is_promise arm —
/// to `.imported`, and `makeProvideTarget` stores `.local{receiverHosted}`.
/// The Provide is landed on the OWNER with question id `provide_qid`.
const Site1Staging = struct {
    promise_import: u32,
    introducer_import: u32,
};

fn stageSite1Provide(
    vat: *ImportOwnerVat,
    allocator: std.mem.Allocator,
    introducer: *Introducer,
    token: []const u8,
    provide_qid: u32,
) !Site1Staging {
    _ = try vat.owner.setBootstrap(.{ .ctx = introducer, .on_call = Introducer.onCall });
    var iprobe = CapImportProbe{};
    _ = try vat.remote.sendBootstrap(&iprobe, CapImportProbe.onReturn);
    const introducer_import = iprobe.import_id orelse return error.IntroducerBootstrapFailed;
    var pprobe = CapImportProbe{};
    _ = try vat.remote.sendCall(introducer_import, 0x1234_5678_9abc_def0, 0, &pprobe, null, CapImportProbe.onReturn);
    const promise_import = pprobe.import_id orelse return error.PromiseNotImported;
    const promise_export = introducer.promise_export_id orelse return error.PromiseNotMinted;
    try vat.owner.resolvePromiseExportToImport(promise_export, vat.carol_import);

    // The Provide lands on the OWNER naming that promise export (the remote
    // legitimately holds it as an import, so this is the real wire shape).
    {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const recipient = try token_msg.getRootAnyPointer();
        const frame = try buildProvideFrame(allocator, provide_qid, .{
            .tag = .importedCap,
            .imported_cap = promise_export,
            .promised_answer = null,
        }, recipient);
        defer allocator.free(frame);
        try vat.owner.handleFrame(frame);
    }
    return .{ .promise_import = promise_import, .introducer_import = introducer_import };
}

test "L17 site 1: a cross-peer Accept whose provide target resolved to an IMPORT is served via the pinned import" {
    const allocator = std.testing.allocator;

    // Declared FIRST so its deinit runs LAST: the owner must leave the index
    // before the index dies.
    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var vat: ImportOwnerVat = undefined;
    try vat.init(allocator, &host.index);
    defer vat.deinitAll();

    var introducer = Introducer{};
    const token = try host.mintToken(allocator, "l17-local-receiverhosted");
    defer allocator.free(token);
    const staged = try stageSite1Provide(&vat, allocator, &introducer, token, 90);
    try std.testing.expectEqual(@as(usize, 1), vat.owner.provisions_by_question.count());

    // Registration took the Provide-time IMPORT pin (site 1 only).
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGone;
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_pin_count);
    }

    // The Accept lands on the SIBLING — the cross-peer arm, un-embargoed, so
    // it goes straight to `serveProvisionOnPeer`.
    try host.injectAccept(allocator, 900, token, null);

    // THE FLIP: served, not refused — a results Return carrying a
    // senderHosted proxy minted on the accept peer.
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(900));
    const proxy_id = host.capture.capIdFor(900) orelse return error.NoProxyCap;
    try std.testing.expectEqual(@as(usize, 1), vat.owner.cross_peer_proxy_links.items.len);

    // The proxy ctx holds its own serve-time pin on top of the provision's.
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGone;
        try std.testing.expectEqual(@as(u32, 2), entry.handoff_pin_count);
    }

    // The accepted capability WORKS: a call on the proxy forwards over the
    // owner's live link and reaches the remote's real Carol.
    try injectNumberCall(&host.peer, allocator, 901, proxy_id);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(901));
    try std.testing.expectEqual(@as(u32, 1), vat.carol.get_number_calls);

    // The owner's Finish drains the provision and drops ITS pin; the served
    // capability survives on the proxy's pin (the accepted cap must outlive
    // the provision's Finish).
    {
        const frame = try buildFinishFrame(allocator, 90);
        defer allocator.free(frame);
        try vat.owner.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(usize, 0), host.index.by_key.count());
    try std.testing.expectEqual(@as(usize, 0), host.index.provision_count);
    try harness.expectNoProvideState(&vat.owner);
    try harness.expectNoProvideState(&host.peer);
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGoneAfterFinish;
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_pin_count);
    }
    try injectNumberCall(&host.peer, allocator, 902, proxy_id);
    try std.testing.expectEqual(@as(u32, 2), vat.carol.get_number_calls);

    // The recipient finishes its Accept question (dropping the answer-held
    // ref on the proxy), then Releases its wire ref: the proxy dies and the
    // ctx deinit drops the last pin. Nothing was withheld in this test (the
    // owner's own wire ref never drained while pinned), so no deferred
    // Release goes out and the entry survives on its live wire ref.
    try injectFinish(&host.peer, allocator, 900);
    try injectRelease(&host.peer, allocator, proxy_id, 1);
    try std.testing.expect(!host.peer.exports.contains(proxy_id));
    try std.testing.expectEqual(@as(usize, 0), vat.owner.cross_peer_proxy_links.items.len);
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGoneAfterProxyRelease;
        try std.testing.expectEqual(@as(u32, 0), entry.handoff_pin_count);
        try std.testing.expectEqual(@as(u32, 0), entry.deferred_release);
        try std.testing.expectEqual(@as(u32, 1), entry.ref_count);
    }

    // Wire references home. Promise FIRST: destroying the promise export is
    // what drops its LOCAL pin on the import (a receiverHosted resolution
    // sends no wire Release). Leak-free teardown under std.testing.allocator
    // — both peers of the vat plus the index — is the other half.
    try vat.remote.releaseImport(staged.promise_import, 1);
    try vat.remote.releaseImport(staged.introducer_import, 1);
    try vat.owner.releaseImport(vat.carol_import, 1);
    try harness.expectNoImport(&vat.owner, vat.carol_import);
    try std.testing.expect(!vat.remote.exports.contains(vat.carol_export_on_remote));
}

test "L17 site 1 embargoed: an embargoed Accept of a receiverHosted target completes at Disembargo time" {
    const allocator = std.testing.allocator;

    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var vat: ImportOwnerVat = undefined;
    try vat.init(allocator, &host.index);
    defer vat.deinitAll();

    var introducer = Introducer{};
    const token = try host.mintToken(allocator, "l17-embargoed-receiverhosted");
    defer allocator.free(token);
    const staged = try stageSite1Provide(&vat, allocator, &introducer, token, 93);

    // The Accept arrives WITH an embargo before its Disembargo: it must be
    // QUEUED in the provision store, not served and not refused.
    const embargo = "\x11\x22\x33\x44\x55\x66\x77\x88";
    try host.injectAccept(allocator, 930, token, embargo);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, null), host.capture.returnFor(930));
    try std.testing.expectEqual(@as(usize, 1), host.peer.cross_peer_pending_accepts.count());
    try std.testing.expectEqual(@as(usize, 1), host.index.queued_accept_count);

    // The introducer's forwarded accept-Disembargo lands on the OWNER
    // connection: the embargo releases and the SAME serve arm runs — the
    // target gate sits inside `serveProvisionOnPeer`, which the
    // embargo-release fan-out calls, so a receiverHosted target must
    // complete here exactly like an un-embargoed one.
    try injectAcceptDisembargoOn(&vat.owner, allocator, 93, embargo);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(930));
    const proxy_id = host.capture.capIdFor(930) orelse return error.NoProxyCap;
    try std.testing.expectEqual(@as(usize, 0), host.peer.cross_peer_pending_accepts.count());
    try std.testing.expectEqual(@as(usize, 0), host.index.queued_accept_count);

    try injectNumberCall(&host.peer, allocator, 931, proxy_id);
    try std.testing.expectEqual(@as(u32, 1), vat.carol.get_number_calls);

    // Drain: Finish the Provide (owner side), finish the Accept question and
    // release the proxy (recipient side), send the refs home.
    {
        const frame = try buildFinishFrame(allocator, 93);
        defer allocator.free(frame);
        try vat.owner.handleFrame(frame);
    }
    try injectFinish(&host.peer, allocator, 930);
    try injectRelease(&host.peer, allocator, proxy_id, 1);
    try harness.expectNoProvideState(&vat.owner);
    try harness.expectNoProvideState(&host.peer);
    try vat.remote.releaseImport(staged.promise_import, 1);
    try vat.remote.releaseImport(staged.introducer_import, 1);
    try vat.owner.releaseImport(vat.carol_import, 1);
    try harness.expectNoImport(&vat.owner, vat.carol_import);
}

test "L17 V2-M6: wire refs draining between Provide and Accept cannot kill the serve (deferred-Release pin)" {
    const allocator = std.testing.allocator;

    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var vat: ImportOwnerVat = undefined;
    try vat.init(allocator, &host.index);
    defer vat.deinitAll();

    var introducer = Introducer{};
    const token = try host.mintToken(allocator, "l17-v2m6-drain-window");
    defer allocator.free(token);
    const staged = try stageSite1Provide(&vat, allocator, &introducer, token, 92);

    // BETWEEN the Provide and the Accept, every wire reference on the target
    // drains: first the promise export dies (dropping its local promise pin
    // on the import), then the owner-side holder releases its own retained
    // wire ref — the discriminating ordering (a post-serve drain would pass
    // even without the Provide-time pin).
    try vat.remote.releaseImport(staged.promise_import, 1);
    try vat.owner.releaseImport(vat.carol_import, 1);

    // THE PROBE: the entry is alive on the Provide-time pin ALONE
    // (ref_count == 0), the drained count was WITHHELD (deferred, not sent),
    // and therefore the introducer-side export is still alive too.
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportDiedDespitePin;
        try std.testing.expectEqual(@as(u32, 0), entry.ref_count);
        try std.testing.expectEqual(@as(u32, 0), entry.promise_ref_count);
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_pin_count);
        try std.testing.expectEqual(@as(u32, 1), entry.deferred_release);
    }
    try std.testing.expect(vat.remote.exports.contains(vat.carol_export_on_remote));

    // THE SERVE — asserted HERE, not at some post-Finish call: the
    // Provide-time pin is only load-bearing over the [Provide, serve) window.
    try host.injectAccept(allocator, 920, token, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(920));
    const proxy_id = host.capture.capIdFor(920) orelse return error.NoProxyCap;

    // ...and the served capability actually works.
    try injectNumberCall(&host.peer, allocator, 921, proxy_id);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(921));
    try std.testing.expectEqual(@as(u32, 1), vat.carol.get_number_calls);

    // WIRE TOTALS: Finish + proxy Release drop both pins; the LAST unpin
    // emits the single deferred Release(1); the introducer-side export dies —
    // granted == released across the whole handoff, just later.
    {
        const frame = try buildFinishFrame(allocator, 92);
        defer allocator.free(frame);
        try vat.owner.handleFrame(frame);
    }
    try std.testing.expect(vat.remote.exports.contains(vat.carol_export_on_remote));
    try injectFinish(&host.peer, allocator, 920);
    try injectRelease(&host.peer, allocator, proxy_id, 1);
    try harness.expectNoImport(&vat.owner, vat.carol_import);
    try std.testing.expect(!vat.remote.exports.contains(vat.carol_export_on_remote));

    try vat.remote.releaseImport(staged.introducer_import, 1);
    try harness.expectNoProvideState(&vat.owner);
    try harness.expectNoProvideState(&host.peer);
}

// The recipient does not wait for the Accept's Return before using the
// capability — it PIPELINES a Call on the Accept question (rpc.c++ always
// does; the e2e pipelined-provide scenarios hit exactly this). When the
// Accept is refused, that Call arrives AFTER the refusal already went out, so
// the queued-call drain in sendReturnException ran before there was anything
// to drain. Without the failed_answers record the Call parked in
// pending_promises forever and the C++ recipient hung on a Return that never
// came. Spec rule: every Call gets exactly one Return; a call pipelined on a
// failed answer gets (a copy of) that answer's exception.
test "a Call pipelined on a refused Accept gets the refusal exception, not silence" {
    const allocator = std.testing.allocator;

    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var vat: ImportOwnerVat = undefined;
    try vat.init(allocator, &host.index);
    defer vat.deinitAll();

    // A refusal that SURVIVES the receiverHosted lift. This test originally
    // staged a receiverHosted target, which the lift now serves; the
    // vanished-import shape is the refusal that remains — a site-2 `.promised`
    // target whose import dies before the Accept, since site 2 takes no
    // Provide-time pin. What is under test is the broken-pipeline rule, not
    // which particular refusal produced it.
    var deferring = DeferringService{};
    const token = try host.mintToken(allocator, "pipelined-on-refusal");
    defer allocator.free(token);
    // The probes live HERE, not inside the staging helper: the staged call's
    // Return is dropped in flight, so its question is still outstanding when
    // `vat.deinitAll()` cancels it THROUGH this ctx pointer. Helper locals
    // would be dead stack by then — a segfault on amd64 under ReleaseSafe.
    var bprobe = CapImportProbe{};
    var call_probe = TolerantCall{};
    const service_import = try stageSite2Provision(&vat, &host, &deferring, &bprobe, &call_probe, token, 90);

    try vat.owner.releaseImport(vat.carol_import, 1);
    try harness.expectNoImport(&vat.owner, vat.carol_import);

    try host.injectAccept(allocator, 900, token, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), host.capture.returnFor(900));

    // The pipelined Call lands after the refusal: Call{qid=901,
    // target=promisedAnswer{900}} — the exact frame order the C++ recipient
    // produces (Accept, then Call, with the Return in between on our side).
    {
        var call_builder = protocol.MessageBuilder.init(allocator);
        defer call_builder.deinit();
        var call = try call_builder.beginCall(901, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID);
        try call.setTargetPromisedAnswer(900);
        _ = try call.initCapTableTyped(0);
        const call_frame = try call_builder.finish();
        defer allocator.free(call_frame);
        try host.peer.handleFrame(call_frame);
    }

    // THE PIN: the pipelined call is ANSWERED — with a copy of the Accept's
    // own refusal, not parked in pending_promises waiting forever.
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), host.capture.returnFor(901));
    try std.testing.expectEqualStrings(target_unavailable_reason, host.capture.reasonFor(901).?);
    try std.testing.expect(!host.peer.pending_promises.contains(900));

    // Failed closed all the way down: no proxy, and Carol was never reached
    // through the refused pipeline either.
    try harness.expectNoCrossPeerProxyLinks(&vat.owner);
    try harness.expectNoCrossPeerProxyLinks(&host.peer);
    try std.testing.expectEqual(@as(u32, 0), vat.carol.get_number_calls);

    {
        const frame = try buildFinishFrame(allocator, 90);
        defer allocator.free(frame);
        try vat.owner.handleFrame(frame);
    }
    try harness.expectNoProvideState(&vat.owner);
    try harness.expectNoProvideState(&host.peer);
    try vat.remote.releaseImport(service_import, 1);
}

/// A Return whose single result cap is one the ANSWERING peer only IMPORTS,
/// emitted origin-tagged as `receiverHosted` — the same encode path
/// `sendReturnProvidedTarget` takes for a `.local` receiverHosted target.
const ReflectedCapReturnCtx = struct {
    import_id: u32,

    fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
        const self: *const ReflectedCapReturnCtx = castCtx(*const ReflectedCapReturnCtx, ctx_ptr);
        var payload = try ret.payloadTyped();
        var any = try payload.initContent();
        try any.setCapabilityOriginTagged(
            cap_table.descriptors.originCodeForTag(.receiverHosted),
            self.import_id,
        );
    }
};

/// A handler that answers NOTHING: it records the inbound question id so the
/// test can send the Return by hand, later, on its own terms.
const DeferringService = struct {
    question_id: ?u32 = null,

    fn onCall(
        ctx_ptr: *anyopaque,
        _: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *DeferringService = castCtx(*DeferringService, ctx_ptr);
        self.question_id = call.question_id;
    }
};

/// Stage the site-2 shape: give the OWNER a live resolved answer whose single
/// result cap is the capability it merely IMPORTS, then install an `.active`
/// provision whose stored target is `.promised` ops naming that answer — the
/// state the serve arm re-resolves at Accept time. Returns the remote's
/// import of the deferring service (for teardown).
/// `bprobe` and `call_probe` are supplied BY THE CALLER, and that is
/// load-bearing rather than stylistic. The staged call's Return is
/// deliberately dropped in flight, so its question stays outstanding until
/// `vat.remote.deinit()` — where `forceCancelAllQuestions` delivers a local
/// exception THROUGH the question's stored `ctx` pointer. Locals of this
/// helper would be dead stack by then: `deliverLocalException` writes into a
/// vanished frame, which on amd64 (Linux and Windows, ReleaseSafe) is a
/// segfault and on arm64 macOS merely scribbles unnoticed. The probes must
/// outlive the peer, so they live in the test's frame.
fn stageSite2Provision(
    vat: *ImportOwnerVat,
    host: *FrameHost,
    deferring: *DeferringService,
    bprobe: *CapImportProbe,
    call_probe: *TolerantCall,
    token: []const u8,
    provide_qid: u32,
) !u32 {
    _ = try vat.owner.setBootstrap(.{ .ctx = deferring, .on_call = DeferringService.onCall });
    _ = try vat.remote.sendBootstrap(bprobe, CapImportProbe.onReturn);
    const service_import = bprobe.import_id orelse return error.ServiceBootstrapFailed;
    _ = try vat.remote.sendCall(service_import, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, call_probe, null, TolerantCall.onReturn);
    const answer_qid = deferring.question_id orelse return error.CallNotDeferred;
    try std.testing.expect(!call_probe.returned);

    // The Return goes out with the link severed. The remote never sees it, so
    // it never auto-Finishes the question — and the owner's `resolved_answers`
    // entry, the one the stored ops name, stays live. (`sendCall` and
    // `sendBootstrap` auto-Finish on Return; see the L6 note above.)
    vat.link.forwarding = false;
    var ret_ctx = ReflectedCapReturnCtx{ .import_id = vat.carol_import };
    try vat.owner.sendReturnResults(answer_qid, &ret_ctx, ReflectedCapReturnCtx.build);
    vat.link.forwarding = true;

    // The stored `.promised` shape, assembled the way the L6 tests assemble
    // it: on the wire it arises only when a Provide's promisedAnswer target
    // resolves to a CHAINED promise, which no public API can stage. The state
    // itself is exactly what the serve arm reads — ops naming an answer that
    // lives on the owner.
    _ = try installPromisedProvision(host, &vat.owner, token, provide_qid, answer_qid);
    return service_import;
}

test "L17 site 2: a stored .promised target re-resolving to an IMPORT is served with a serve-time pin" {
    const allocator = std.testing.allocator;

    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var vat: ImportOwnerVat = undefined;
    try vat.init(allocator, &host.index);
    defer vat.deinitAll();

    var deferring = DeferringService{};
    const token = try host.mintToken(allocator, "l17-promised-receiverhosted");
    defer allocator.free(token);
    // Declared here, not inside the helper: they back an outstanding question
    // that `vat.deinitAll()` cancels, so they must outlive it.
    var bprobe = CapImportProbe{};
    var call_probe = TolerantCall{};
    const service_import = try stageSite2Provision(&vat, &host, &deferring, &bprobe, &call_probe, token, 91);

    // SITE 2 takes NO Provide-time pin: the target was `.promised` at
    // registration (there was no import to pin yet), so the entry carries
    // only its wire ref here.
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGone;
        try std.testing.expectEqual(@as(u32, 0), entry.handoff_pin_count);
    }

    // Cross-peer Accept on the sibling: the owner re-resolves its own answer
    // through the stored ops, lands on `.imported`, and SERVES it.
    try host.injectAccept(allocator, 910, token, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(910));
    const proxy_id = host.capture.capIdFor(910) orelse return error.NoProxyCap;
    try std.testing.expectEqual(@as(usize, 1), vat.owner.cross_peer_proxy_links.items.len);

    // The serve-time pin (owned by the proxy ctx) is the ONLY pin.
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGone;
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_pin_count);
    }

    // The accepted capability reaches the far side.
    try injectNumberCall(&host.peer, allocator, 911, proxy_id);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(911));
    try std.testing.expectEqual(@as(u32, 1), vat.carol.get_number_calls);

    // Finish drains the provision; the serve-time pin is untouched (the
    // provision never held an import pin for site 2).
    {
        const frame = try buildFinishFrame(allocator, 91);
        defer allocator.free(frame);
        try vat.owner.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(usize, 0), host.index.by_key.count());
    try harness.expectNoProvideState(&vat.owner);
    try harness.expectNoProvideState(&host.peer);
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGoneAfterFinish;
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_pin_count);
    }
    try injectNumberCall(&host.peer, allocator, 912, proxy_id);
    try std.testing.expectEqual(@as(u32, 2), vat.carol.get_number_calls);

    // The recipient finishes its Accept question, then its Release sweeps
    // the proxy and its pin.
    try injectFinish(&host.peer, allocator, 910);
    try injectRelease(&host.peer, allocator, proxy_id, 1);
    try std.testing.expect(!host.peer.exports.contains(proxy_id));
    try std.testing.expectEqual(@as(usize, 0), vat.owner.cross_peer_proxy_links.items.len);
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGoneAfterProxyRelease;
        try std.testing.expectEqual(@as(u32, 0), entry.handoff_pin_count);
    }

    // The owner's answer stays outstanding (its Return was dropped in
    // flight); teardown under std.testing.allocator proves the whole vat —
    // both peers, the index, the live answer, the import — drains leak-free.
    try vat.owner.releaseImport(vat.carol_import, 1);
    try harness.expectNoImport(&vat.owner, vat.carol_import);
    try vat.remote.releaseImport(service_import, 1);
}

test "L17 witness: a stored .promised target whose re-resolved import has DIED still fails closed, by name" {
    const allocator = std.testing.allocator;

    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    var vat: ImportOwnerVat = undefined;
    try vat.init(allocator, &host.index);
    defer vat.deinitAll();

    var deferring = DeferringService{};
    const token = try host.mintToken(allocator, "l17-vanished-import");
    defer allocator.free(token);
    var bprobe = CapImportProbe{};
    var call_probe = TolerantCall{};
    const service_import = try stageSite2Provision(&vat, &host, &deferring, &bprobe, &call_probe, token, 94);

    // The target import dies for real BEFORE the Accept: site 2 has no
    // Provide-time pin (by design — the answer's cap was still a promise at
    // registration), so nothing holds the import once its own wire ref goes
    // home. The lift serves live targets; it does not resurrect dead ones.
    try vat.owner.releaseImport(vat.carol_import, 1);
    try harness.expectNoImport(&vat.owner, vat.carol_import);

    try host.injectAccept(allocator, 940, token, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), host.capture.returnFor(940));
    try std.testing.expectEqualStrings(target_unavailable_reason, host.capture.reasonFor(940).?);

    // Fail closed, and not wedged: no proxy, no pin residue, and the
    // provision still drains normally on the owner's Finish.
    try harness.expectNoCrossPeerProxyLinks(&vat.owner);
    try harness.expectNoCrossPeerProxyLinks(&host.peer);
    try std.testing.expectEqual(@as(u32, 0), vat.carol.get_number_calls);
    {
        const frame = try buildFinishFrame(allocator, 94);
        defer allocator.free(frame);
        try vat.owner.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(usize, 0), host.index.by_key.count());
    try harness.expectNoProvideState(&vat.owner);
    try harness.expectNoProvideState(&host.peer);
    try vat.remote.releaseImport(service_import, 1);
}

// ============================================================================
// L17 teardown order: the new pin state must stay leak-free whichever side
// dies first (extends the L3 teardown trio with the import-pin cases).
// ============================================================================

test "L17 teardown: owner-first — the proxy's abandoned import pin cannot be released post-mortem (no UAF)" {
    const allocator = std.testing.allocator;

    var host: FrameHost = undefined;
    try host.init(allocator, .{});
    defer host.deinitAll();

    // Hand-rolled ImportOwnerVat guts: the owner is deinited MID-TEST, so the
    // fixture's own deinitAll (which would deinit it again) cannot be used.
    var link = PairLink{};
    var carol = Carol{};
    var owner = Peer.initDetached(allocator);
    owner.disableThreadAffinity();
    var remote = Peer.initDetached(allocator);
    remote.disableThreadAffinity();
    defer {
        link.forwarding = false;
        link.deinitBuffered();
        remote.deinit();
        // owner deinited mid-test.
    }
    link.left = &remote;
    link.right = &owner;
    remote.setSendFrameOverride(&link, PairLink.leftSend);
    owner.setSendFrameOverride(&link, PairLink.rightSend);
    try owner.attachProvisionIndex(&host.index);

    const carol_export_on_remote = try remote.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    var carol_returner = CapReturner{ .export_id = carol_export_on_remote };
    const carol_import = try grantCapVia(&remote, &owner, &carol_returner);

    var introducer = Introducer{};
    _ = try owner.setBootstrap(.{ .ctx = &introducer, .on_call = Introducer.onCall });
    var iprobe = CapImportProbe{};
    _ = try remote.sendBootstrap(&iprobe, CapImportProbe.onReturn);
    const introducer_import = iprobe.import_id orelse return error.IntroducerBootstrapFailed;
    var pprobe = CapImportProbe{};
    _ = try remote.sendCall(introducer_import, 0x1234_5678_9abc_def0, 0, &pprobe, null, CapImportProbe.onReturn);
    _ = pprobe.import_id orelse return error.PromiseNotImported;
    const promise_export = introducer.promise_export_id orelse return error.PromiseNotMinted;
    try owner.resolvePromiseExportToImport(promise_export, carol_import);

    const token = try host.mintToken(allocator, "l17-tdown-owner");
    defer allocator.free(token);
    {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const recipient = try token_msg.getRootAnyPointer();
        const frame = try buildProvideFrame(allocator, 95, .{
            .tag = .importedCap,
            .imported_cap = promise_export,
            .promised_answer = null,
        }, recipient);
        defer allocator.free(frame);
        try owner.handleFrame(frame);
    }
    try host.injectAccept(allocator, 950, token, null);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), host.capture.returnFor(950));
    const proxy_id = host.capture.capIdFor(950) orelse return error.NoProxyCap;

    // The OWNER dies first (transport severed like a real disconnect). Its
    // neutralize step must ABANDON the proxy's import pin — the import table
    // dies with the peer.
    link.forwarding = false;
    owner.deinit();

    // The proxy degrades LOUDLY (dead-source exception), not a UAF.
    try injectNumberCall(&host.peer, allocator, 951, proxy_id);
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), host.capture.returnFor(951));
    try std.testing.expectEqual(@as(u32, 0), carol.get_number_calls);

    // Destroying the proxy AFTER the owner's death must not touch the dead
    // owner's tables (the abandoned pin is null): leak-free under
    // std.testing.allocator is the other half of the assertion.
    try injectFinish(&host.peer, allocator, 950);
    try injectRelease(&host.peer, allocator, proxy_id, 1);
    try std.testing.expect(!host.peer.exports.contains(proxy_id));
}

test "L17 teardown: accept-peer-first — the proxy sweep releases the serve-time import pin on the live owner" {
    const allocator = std.testing.allocator;

    // Hand-rolled FrameHost guts: the accept peer is deinited MID-TEST.
    var capture = FrameCapture{ .allocator = allocator };
    defer capture.deinit();
    var accept_peer = Peer.initDetached(allocator);
    accept_peer.disableThreadAffinity();
    accept_peer.setSendFrameOverride(&capture, FrameCapture.send);
    var index = ProvisionIndex.init(allocator, .{});
    index.disableThreadAffinity();
    defer index.deinit();
    try accept_peer.attachProvisionIndex(&index);
    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    var dummy = Peer.initDetached(allocator);
    dummy.disableThreadAffinity();
    defer dummy.deinit();
    try net.register("l17-tdown-holder", &dummy);
    dummy.attachVatNetwork(net.network());

    var vat: ImportOwnerVat = undefined;
    try vat.init(allocator, &index);
    defer vat.deinitAll();

    var introducer = Introducer{};
    const token = blk: {
        const d_network = dummy.vat_network orelse return error.NoVatNetwork;
        var introduction = try d_network.mintIntroduction(&dummy, "l17-tdown-holder");
        defer introduction.deinit(allocator);
        break :blk try allocator.dupe(u8, introduction.to_await);
    };
    defer allocator.free(token);
    const staged = try stageSite1Provide(&vat, allocator, &introducer, token, 96);

    {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const provision = try token_msg.getRootAnyPointer();
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildAccept(960, provision, null);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try accept_peer.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), capture.returnFor(960));
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGone;
        try std.testing.expectEqual(@as(u32, 2), entry.handoff_pin_count);
    }

    // The ACCEPT PEER dies first: its export sweep runs the proxy ctx deinit,
    // which releases the serve-time pin on the still-live owner — leaving
    // exactly the Provide-time pin.
    accept_peer.deinit();
    try std.testing.expectEqual(@as(usize, 0), vat.owner.cross_peer_proxy_links.items.len);
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGoneAfterHolderDeath;
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_pin_count);
    }

    // The owner's Finish drops the last pin; everything drains.
    {
        const frame = try buildFinishFrame(allocator, 96);
        defer allocator.free(frame);
        try vat.owner.handleFrame(frame);
    }
    {
        const entry = vat.owner.caps.imports.get(vat.carol_import) orelse return error.ImportGoneAfterFinish;
        try std.testing.expectEqual(@as(u32, 0), entry.handoff_pin_count);
    }
    try vat.remote.releaseImport(staged.promise_import, 1);
    try vat.remote.releaseImport(staged.introducer_import, 1);
    try vat.owner.releaseImport(vat.carol_import, 1);
    try harness.expectNoImport(&vat.owner, vat.carol_import);
}

// ============================================================================
// L17 OOM discipline: the new import-pin ladder arms roll back to zero
// residue (two-pass FailingAllocator sweeps with state probes, the same shape
// as the L3 Provide-registration sweep above).
// ============================================================================

/// Per-iteration fixture for the L17 OOM sweeps: an ImportOwnerVat staged to
/// the site-1 shape (resolved promise export -> carol_import) on `alloc`,
/// with the provide frame BUILT but not yet delivered. Every field lives in
/// the caller's frame; `deinitAll` mirrors construction order.
const L17OomFixture = struct {
    index: ProvisionIndex,
    net: vat_network.LoopbackVatNetwork(Peer),
    dummy: Peer,
    vat: ImportOwnerVat,
    introducer: Introducer,
    token: []u8,
    provide_frame: []const u8,
    allocator: std.mem.Allocator,

    fn init(self: *L17OomFixture, alloc: std.mem.Allocator, nonce: []const u8, provide_qid: u32) !void {
        self.allocator = alloc;
        self.introducer = .{};
        self.index = ProvisionIndex.init(alloc, .{});
        self.index.disableThreadAffinity();
        self.net = vat_network.LoopbackVatNetwork(Peer).init(alloc);
        self.dummy = Peer.initDetached(alloc);
        self.dummy.disableThreadAffinity();
        try self.net.register(nonce, &self.dummy);
        self.dummy.attachVatNetwork(self.net.network());
        try self.vat.init(alloc, &self.index);

        const d_network = self.dummy.vat_network orelse return error.NoVatNetwork;
        var introduction = try d_network.mintIntroduction(&self.dummy, nonce);
        defer introduction.deinit(alloc);
        self.token = try alloc.dupe(u8, introduction.to_await);

        _ = try self.vat.owner.setBootstrap(.{ .ctx = &self.introducer, .on_call = Introducer.onCall });
        var iprobe = CapImportProbe{};
        _ = try self.vat.remote.sendBootstrap(&iprobe, CapImportProbe.onReturn);
        const introducer_import = iprobe.import_id orelse return error.IntroducerBootstrapFailed;
        var pprobe = CapImportProbe{};
        _ = try self.vat.remote.sendCall(introducer_import, 0x1234_5678_9abc_def0, 0, &pprobe, null, CapImportProbe.onReturn);
        _ = pprobe.import_id orelse return error.PromiseNotImported;
        const promise_export = self.introducer.promise_export_id orelse return error.PromiseNotMinted;
        try self.vat.owner.resolvePromiseExportToImport(promise_export, self.vat.carol_import);

        var token_msg = try message.Message.initUnvalidated(alloc, self.token);
        defer token_msg.deinit();
        const recipient = try token_msg.getRootAnyPointer();
        self.provide_frame = try buildProvideFrame(alloc, provide_qid, .{
            .tag = .importedCap,
            .imported_cap = promise_export,
            .promised_answer = null,
        }, recipient);
    }

    fn deinitAll(self: *L17OomFixture) void {
        self.allocator.free(self.provide_frame);
        self.allocator.free(self.token);
        self.vat.deinitAll();
        self.dummy.deinit();
        self.net.deinit();
        self.index.deinit();
    }
};

test "L17 OOM: a failed receiverHosted Provide registration rolls the import pin back to zero residue" {
    const base = std.testing.allocator;

    var window_start: usize = 0;
    var window_end: usize = 0;

    // Pass 1: record the allocation window of handleFrame(provide).
    {
        var failing = std.testing.FailingAllocator.init(base, .{});
        const alloc = failing.allocator();
        var fx: L17OomFixture = undefined;
        try fx.init(alloc, "l17-oom-provide", 97);
        defer fx.deinitAll();

        window_start = failing.alloc_index;
        try fx.vat.owner.handleFrame(fx.provide_frame);
        window_end = failing.alloc_index;

        // The success path really took the pin (otherwise this sweep probes
        // nothing).
        const entry = fx.vat.owner.caps.imports.get(fx.vat.carol_import) orelse return error.ImportGone;
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_pin_count);
    }
    try std.testing.expect(window_end > window_start);

    // Pass 2: inject one failure at every index inside the window.
    var fail_index = window_start;
    while (fail_index < window_end) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(base, .{ .fail_index = fail_index });
        const alloc = failing.allocator();
        var fx: L17OomFixture = undefined;
        try fx.init(alloc, "l17-oom-provide", 97);
        defer fx.deinitAll();

        if (fx.vat.owner.handleFrame(fx.provide_frame)) |_| {
            // Allocation pattern shifted; this index no longer fails inside
            // the provide path. The success state drains at deinit.
        } else |_| {
            // THE PROBES: no half-registered residue anywhere, and the pin
            // ladder rolled ITS arm back — the import survives with its wire
            // ref, its promise pin, and NO handoff pin or deferred tally.
            try std.testing.expectEqual(@as(usize, 0), fx.index.by_key.count());
            try std.testing.expectEqual(@as(usize, 0), fx.vat.owner.provisions_by_question.count());
            try std.testing.expectEqual(@as(usize, 0), fx.vat.owner.provides_by_question.count());
            try std.testing.expectEqual(@as(usize, 0), fx.vat.owner.provides_by_key.count());
            const entry = fx.vat.owner.caps.imports.get(fx.vat.carol_import) orelse return error.ImportDestroyedByOom;
            try std.testing.expectEqual(@as(u32, 1), entry.ref_count);
            try std.testing.expectEqual(@as(u32, 1), entry.promise_ref_count);
            try std.testing.expectEqual(@as(u32, 0), entry.handoff_pin_count);
            try std.testing.expectEqual(@as(u32, 0), entry.deferred_release);
        }
    }
}

test "L17 OOM: a failed receiverHosted serve rolls the serve-time pin back (Provide-time pin intact)" {
    const base = std.testing.allocator;

    var window_start: usize = 0;
    var window_end: usize = 0;

    // Pass 1: register the provide successfully, then record the allocation
    // window of the ACCEPT (the serve path: pin + proxy mint + Return).
    {
        var failing = std.testing.FailingAllocator.init(base, .{});
        const alloc = failing.allocator();
        var fx: L17OomFixture = undefined;
        try fx.init(alloc, "l17-oom-serve", 98);
        defer fx.deinitAll();
        var capture = FrameCapture{ .allocator = alloc };
        defer capture.deinit();
        var accept_peer = Peer.initDetached(alloc);
        accept_peer.disableThreadAffinity();
        defer accept_peer.deinit();
        accept_peer.setSendFrameOverride(&capture, FrameCapture.send);
        try accept_peer.attachProvisionIndex(&fx.index);

        try fx.vat.owner.handleFrame(fx.provide_frame);

        var token_msg = try message.Message.initUnvalidated(alloc, fx.token);
        defer token_msg.deinit();
        const provision = try token_msg.getRootAnyPointer();
        var builder = protocol.MessageBuilder.init(alloc);
        defer builder.deinit();
        try builder.buildAccept(980, provision, null);
        const accept_frame = try builder.finish();
        defer alloc.free(accept_frame);

        window_start = failing.alloc_index;
        try accept_peer.handleFrame(accept_frame);
        window_end = failing.alloc_index;

        // The serve really happened (this is also the pre-lift RED).
        try std.testing.expectEqual(@as(?protocol.ReturnTag, .results), capture.returnFor(980));
        const entry = fx.vat.owner.caps.imports.get(fx.vat.carol_import) orelse return error.ImportGone;
        try std.testing.expectEqual(@as(u32, 2), entry.handoff_pin_count);
    }
    try std.testing.expect(window_end > window_start);

    // Pass 2: inject one failure at every index inside the serve window.
    var fail_index = window_start;
    while (fail_index < window_end) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(base, .{ .fail_index = fail_index });
        const alloc = failing.allocator();
        var fx: L17OomFixture = undefined;
        try fx.init(alloc, "l17-oom-serve", 98);
        defer fx.deinitAll();
        var capture = FrameCapture{ .allocator = alloc };
        defer capture.deinit();
        var accept_peer = Peer.initDetached(alloc);
        accept_peer.disableThreadAffinity();
        defer accept_peer.deinit();
        accept_peer.setSendFrameOverride(&capture, FrameCapture.send);
        try accept_peer.attachProvisionIndex(&fx.index);

        try fx.vat.owner.handleFrame(fx.provide_frame);

        var token_msg = try message.Message.initUnvalidated(alloc, fx.token);
        defer token_msg.deinit();
        const provision = try token_msg.getRootAnyPointer();
        var builder = protocol.MessageBuilder.init(alloc);
        defer builder.deinit();
        try builder.buildAccept(980, provision, null);
        const accept_frame = try builder.finish();
        defer alloc.free(accept_frame);

        const serve_result = accept_peer.handleFrame(accept_frame);
        const served = if (serve_result) |_|
            (capture.returnFor(980) orelse protocol.ReturnTag.exception) == .results
        else |_|
            false;
        if (!served) {
            // THE PROBES: the serve-time pin rolled back (the Provide-time
            // pin alone remains), no proxy state leaked on either peer, and
            // nothing was queued behind the failure.
            const entry = fx.vat.owner.caps.imports.get(fx.vat.carol_import) orelse return error.ImportDestroyedByOom;
            try std.testing.expectEqual(@as(u32, 1), entry.handoff_pin_count);
            try std.testing.expectEqual(@as(usize, 0), fx.vat.owner.cross_peer_proxy_links.items.len);
            try std.testing.expectEqual(@as(usize, 0), accept_peer.exports.count());
            try std.testing.expectEqual(@as(usize, 0), fx.index.queued_accept_count);
        }
    }
}

// ============================================================================
// Thread-affinity contract. WorkerPool vats are unsupported (provisions.zig
// header, §"Single-threaded"), and the index enforces that by pinning itself
// to the first thread that touches it. The pin has to be taken on EVERY path
// that touches the index: attach and Provide took it from the first landing,
// the Accept path did not — so a second thread's Provide panicked loudly while
// its Accept raced the sweep and the by_key lookup in silence.
// ============================================================================

test "L3 affinity: an inbound Accept pins the index, exactly like a Provide does" {
    // `assertThreadAffinity` is a no-op outside Debug and `thread_id` is
    // literally `void` there, so the observation does not exist in the
    // ReleaseSafe/ReleaseFast lanes. The condition is comptime-known, which is
    // what keeps the body below out of those builds entirely.
    if (comptime builtin.mode != .debug) return error.SkipZigTest;

    const allocator = std.testing.allocator;

    var capture = FrameCapture{ .allocator = allocator };
    defer capture.deinit();
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, FrameCapture.send);

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();
    var dummy = Peer.initDetached(allocator);
    dummy.disableThreadAffinity();
    defer dummy.deinit();
    try net.register("affinity-accept", &dummy);
    dummy.attachVatNetwork(net.network());

    // Index affinity left ENABLED. Every other test in this file (and the
    // `FrameHost` harness itself) calls `index.disableThreadAffinity()`, on
    // which this assertion would be a silent no-op proving nothing.
    var index = ProvisionIndex.init(allocator, .{});
    defer index.deinit();

    // Attach by hand. `Peer.attachProvisionIndex` asserts affinity itself, so
    // routing through it would pin the index BEFORE the Accept ever arrived
    // and make the post-condition below vacuously true. These are its only
    // other two effects.
    try index.attached_peers.append(index.allocator, &peer);
    peer.provision_index = &index;
    try std.testing.expectEqual(@as(?std.Thread.Id, null), index.thread_id);

    const token = blk: {
        const d_network = dummy.vat_network orelse return error.NoVatNetwork;
        var introduction = try d_network.mintIntroduction(&dummy, "affinity-accept");
        defer introduction.deinit(allocator);
        break :blk try allocator.dupe(u8, introduction.to_await);
    };
    defer allocator.free(token);

    // An Accept for a token no Provide has claimed: it parks, which is the
    // shortest frame that reaches `handleAcceptWithProvisionIndex` at all.
    {
        var token_msg = try message.Message.initUnvalidated(allocator, token);
        defer token_msg.deinit();
        const provision = try token_msg.getRootAnyPointer();
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildAccept(640, provision, null);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
    // It really did run the index-mode Accept path (and mutated the index).
    try std.testing.expectEqual(@as(usize, 1), index.parked_accept_count);

    // THE PIN. Drop `idx.assertThreadAffinity()` from
    // `handleAcceptWithProvisionIndex` and this stays null: the index would
    // then accept its first-ever touch from any thread, and a later Provide
    // from the vat's real thread would be the one that panicked.
    // Compared as an OPTIONAL on purpose: with the assert ablated this reports
    // `expected ..., found null` instead of unwrapping a null and aborting.
    try std.testing.expectEqual(
        @as(?std.Thread.Id, std.Thread.getCurrentId()),
        index.thread_id,
    );
}
