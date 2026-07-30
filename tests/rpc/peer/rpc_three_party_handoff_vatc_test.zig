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
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const vat_network = capnpc.rpc.vat.network;
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

    fired: bool = false,
    ret_tag: ?protocol.ReturnTag = null,
    exception_reason: ?[]u8 = null,
    carol_import_id: ?u32 = null,
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
    allocator: std.mem.Allocator = std.testing.allocator,

    fn deinitBuffered(self: *PairLink) void {
        if (self.buffered_accept) |f| self.allocator.free(f);
        self.buffered_accept = null;
    }

    fn leftSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *PairLink = castCtx(*PairLink, ctx);
        if (!self.forwarding) return;
        if (self.buffer_accepts_to_right) {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (decoded.tag == .accept) {
                std.debug.assert(self.buffered_accept == null);
                self.buffered_accept = try self.allocator.dupe(u8, frame);
                return;
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
fn grantCapVia(host: *Peer, remote: *Peer, returner: *CapReturner) !u32 {
    _ = try host.setBootstrap(.{ .ctx = returner, .on_call = CapReturner.onCall });
    var boot_probe = CapImportProbe{};
    _ = try remote.sendBootstrap(&boot_probe, CapImportProbe.onReturn);
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
        _ = try peers.b_to_a.resolvePromiseExportToThirdParty(
            self.promise_import,
            &peers.b_to_c,
            provided_target,
            recipient,
            introduction.to_contact,
        );
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

test "L3 fail-closed: a cross-peer embargoed Accept is rejected with a pinned reason" {
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

    var pickup = VatcPickupHandler{ .allocator = allocator, .expected_promise_id = setup.promise_import };
    defer pickup.deinit();
    peers.a_to_b.setHandoffPickupHandler(&pickup, VatcPickupHandler.onPickup);

    // An in-flight pipelined call on the promise forces the embargo.
    var pipelined = TolerantCall{};
    _ = try peers.a_to_b.sendCall(setup.promise_import, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &pipelined, null, TolerantCall.onReturn);
    try std.testing.expect(!pipelined.returned);

    try setup.originate(&peers, "vatc-l3-embargoed");

    // Fail-closed, loudly — this string is the embargo landing's RED hook.
    try std.testing.expect(pickup.fired);
    try std.testing.expectEqualStrings("exception", @tagName(pickup.ret_tag.?));
    try std.testing.expectEqualStrings("cross-peer embargoed accept unsupported", pickup.exception_reason.?);

    // The parked pipelined call still completed through the vine-forwarding
    // fallback (issue #56) — partial progress, not to be mistaken for support.
    try std.testing.expect(pipelined.returned);

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
