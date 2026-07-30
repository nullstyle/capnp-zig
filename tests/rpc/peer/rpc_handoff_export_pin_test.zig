//! L2 of the VatC hosting plan: the `handoff_ref_count` export ref class and
//! the cross-peer proxy's export pin.
//!
//! The F2 failure this kills: VatC serves an Accept by minting a proxy export
//! on its recipient-facing peer (R) that forwards to a target export on its
//! introducer-facing peer (P). The introducer's Finish + the remote's wire
//! Release then drive P's export to zero wire references and destroy it — so
//! the accepted capability silently dies (`error.UnknownExport` on the next
//! call). The pin is a fourth, Release-immune ref class: an inbound Release
//! can never spend it (the wire never granted it), so a hostile over-release
//! stays a DETECTED `ReleaseCountExceeded` instead of a silent destroy — and
//! an export that was NEVER granted to the wire survives a full pin/unpin
//! cycle, so `Provide`+`Finish` can never become a remote destroy primitive
//! for app-held exports.
//!
//! Topology: four detached peers over two synchronous pair links.
//!   P <-> Q   "VatC's introducer-facing peer" and its remote (grants and
//!             releases the wire ref on Carol).
//!   R <-> A   "VatC's recipient-facing peer" and its remote (imports and
//!             calls the proxy).
//! P and R belong to the same vat (same thread); the proxy on R forwards to
//! P's export of Carol.

const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const Peer = peer_impl.Peer;
const peer_test_hooks = Peer.test_hooks;

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

const NUMBER_INTERFACE_ID: u64 = 0xD0D0_D0D0_D0D0_D002;
const NUMBER_METHOD_ID: u16 = 0;

fn readNumber(payload: protocol.Payload) !u32 {
    const content = try payload.content.getStruct();
    return content.readU32(0);
}

/// The capability being handed off: answers getNumber() with 42.
const Carol = struct {
    calls: u32 = 0,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *Carol = castCtx(*Carol, ctx_ptr);
        if (call.interface_id != NUMBER_INTERFACE_ID or call.method_id != NUMBER_METHOD_ID) {
            return error.UnexpectedMethod;
        }
        self.calls += 1;
        const ReturnCtx = struct {
            fn build(_: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                const results = try any.initStruct(1, 0);
                results.writeU32(0, 42);
            }
        };
        var unused: u8 = 0;
        try peer.sendReturnResults(call.question_id, &unused, ReturnCtx.build);
    }
};

/// Bootstrap service returning a pre-existing export id in its results — the
/// wire-honest way to grant a remote a reference on that export.
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

/// Bootstrap-return observer: resolve + retain the returned cap as an import.
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

/// getNumber() caller that tolerates exception Returns (captures tag+result).
const NumberCall = struct {
    result_n: ?u32 = null,
    ret_tag: ?protocol.ReturnTag = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *NumberCall = castCtx(*NumberCall, ctx_ptr);
        self.ret_tag = ret.tag;
        if (ret.tag == .results) {
            const payload = ret.results orelse return error.MissingPayload;
            self.result_n = try readNumber(payload);
        }
    }
};

/// Install `returner` as `host`'s bootstrap and have `remote` import the cap
/// it returns, in two wire-honest steps (bootstrap, then a call whose Return
/// carries the target cap — granting one wire reference on it).
fn grantCapVia(host: *Peer, remote: *Peer, returner: *CapReturner) !u32 {
    _ = try host.setBootstrap(.{ .ctx = returner, .on_call = CapReturner.onCall });
    var boot_probe = CapImportProbe{};
    _ = try remote.sendBootstrap(&boot_probe, CapImportProbe.onReturn);
    const boot_import = boot_probe.import_id orelse return error.ReturnerNotGranted;
    var probe = CapImportProbe{};
    _ = try remote.sendCall(boot_import, NUMBER_INTERFACE_ID, NUMBER_METHOD_ID, &probe, null, CapImportProbe.onReturn);
    return probe.import_id orelse return error.CapNotGranted;
}

const PairLink = struct {
    forwarding: bool = true,
    left: ?*Peer = null,
    right: ?*Peer = null,

    fn leftSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *PairLink = castCtx(*PairLink, ctx);
        if (!self.forwarding) return;
        if (self.right) |peer| try peer.handleFrame(frame);
    }

    fn rightSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *PairLink = castCtx(*PairLink, ctx);
        if (!self.forwarding) return;
        if (self.left) |peer| try peer.handleFrame(frame);
    }
};

const Fixture = struct {
    p: Peer,
    q: Peer,
    r: Peer,
    a: Peer,
    pq: PairLink = .{},
    ra: PairLink = .{},
    carol: Carol = .{},
    carol_id: u32 = 0,
    /// Q's import of Carol (the introducer-side wire reference).
    q_carol_import: u32 = 0,

    fn init(self: *Fixture, allocator: std.mem.Allocator) !void {
        // `self` arrives undefined: field defaults do NOT apply — set every
        // non-peer field explicitly before wiring.
        self.pq = .{};
        self.ra = .{};
        self.carol = .{};
        self.carol_id = 0;
        self.q_carol_import = 0;
        self.p = Peer.initDetached(allocator);
        self.q = Peer.initDetached(allocator);
        self.r = Peer.initDetached(allocator);
        self.a = Peer.initDetached(allocator);
        self.p.disableThreadAffinity();
        self.q.disableThreadAffinity();
        self.r.disableThreadAffinity();
        self.a.disableThreadAffinity();

        self.pq.left = &self.p;
        self.pq.right = &self.q;
        self.ra.left = &self.r;
        self.ra.right = &self.a;
        self.p.setSendFrameOverride(&self.pq, PairLink.leftSend);
        self.q.setSendFrameOverride(&self.pq, PairLink.rightSend);
        self.r.setSendFrameOverride(&self.ra, PairLink.leftSend);
        self.a.setSendFrameOverride(&self.ra, PairLink.rightSend);
    }

    fn deinitPeers(self: *Fixture) void {
        self.pq.forwarding = false;
        self.ra.forwarding = false;
        self.a.deinit();
        self.r.deinit();
        self.q.deinit();
        self.p.deinit();
    }

    /// Export Carol on P and grant Q one REAL wire reference via a
    /// Return-carried cap (bootstrap CapReturner + auto-finished question),
    /// so afterwards: ref_count == 1, answer_ref_count == 0.
    fn grantCarolToQ(self: *Fixture, returner: *CapReturner) !void {
        self.carol_id = try self.p.addExport(.{ .ctx = &self.carol, .on_call = Carol.onCall });
        returner.* = .{ .export_id = self.carol_id };
        self.q_carol_import = try grantCapVia(&self.p, &self.q, returner);
    }
};

// -- Baseline (permanent F2 witness): WITHOUT a pin, the wire Release kills --
// -- the proxied export and the accepted capability silently dies.          --

test "handoff pin baseline: without a pin, the source-side Release destroys the proxied export (F2)" {
    const allocator = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.init(allocator);
    defer fx.deinitPeers();

    var returner: CapReturner = undefined;
    try fx.grantCarolToQ(&returner);

    // Mint the proxy on R with NO export pin (today's only production shape).
    const proxy_id = try peer_test_hooks.addCrossPeerProxyExport(
        &fx.r,
        &fx.p,
        .{ .exported = .{ .id = fx.carol_id } },
        null,
        null,
    );

    // A imports the proxy through a wire-honest Return.
    var proxy_returner = CapReturner{ .export_id = proxy_id };
    const a_proxy_import = try grantCapVia(&fx.r, &fx.a, &proxy_returner);

    // The introducer-side remote drops its wire reference — the F2 trigger.
    try fx.q.releaseImport(fx.q_carol_import, 1);

    // Carol's export is GONE from P (nothing held it), and a call through the
    // still-live proxy reaches a dead capability: an exception Return, not 42.
    try std.testing.expect(!fx.p.exports.contains(fx.carol_id));
    var call = NumberCall{};
    _ = try fx.a.sendCall(a_proxy_import, NUMBER_INTERFACE_ID, NUMBER_METHOD_ID, &call, null, NumberCall.onReturn);
    try std.testing.expectEqual(protocol.ReturnTag.exception, call.ret_tag.?);
    try std.testing.expectEqual(@as(u32, 0), fx.carol.calls);

    try fx.a.releaseImport(a_proxy_import, 1);
}

// -- The feature: the proxy's export pin keeps the accepted cap alive. -------

test "handoff pin: the proxied export survives the source-side wire Release; releasing the proxy sweeps it" {
    const allocator = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.init(allocator);
    defer fx.deinitPeers();

    var returner: CapReturner = undefined;
    try fx.grantCarolToQ(&returner);

    // Probe the grant: exactly one wire ref, no answer refs (auto-finished).
    {
        const entry = fx.p.exports.get(fx.carol_id) orelse return error.CarolExportMissing;
        try std.testing.expectEqual(@as(u32, 1), entry.ref_count);
        try std.testing.expectEqual(@as(u32, 0), entry.answer_ref_count);
        try std.testing.expect(entry.wire_ref_granted);
    }

    // Take the handoff pin, then mint the proxy that owns it (ownership
    // transfers AT the call — the caller never rolls it back afterwards).
    try peer_test_hooks.noteHandoffExportRef(&fx.p, fx.carol_id);
    const proxy_id = try peer_test_hooks.addCrossPeerProxyExport(
        &fx.r,
        &fx.p,
        .{ .exported = .{ .id = fx.carol_id } },
        null,
        fx.carol_id,
    );

    var proxy_returner = CapReturner{ .export_id = proxy_id };
    const a_proxy_import = try grantCapVia(&fx.r, &fx.a, &proxy_returner);

    // The F2 trigger: the introducer-side remote drops its wire reference.
    try fx.q.releaseImport(fx.q_carol_import, 1);

    // The export SURVIVES on the pin alone: zero wire refs, one handoff ref.
    {
        const entry = fx.p.exports.get(fx.carol_id) orelse return error.CarolDestroyedDespitePin;
        try std.testing.expectEqual(@as(u32, 0), entry.ref_count);
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_ref_count);
    }

    // The accepted capability still works.
    var call = NumberCall{};
    _ = try fx.a.sendCall(a_proxy_import, NUMBER_INTERFACE_ID, NUMBER_METHOD_ID, &call, null, NumberCall.onReturn);
    try std.testing.expectEqual(protocol.ReturnTag.results, call.ret_tag.?);
    try std.testing.expectEqual(@as(?u32, 42), call.result_n);
    try std.testing.expectEqual(@as(u32, 1), fx.carol.calls);

    // Releasing the proxy (A's wire release drives R's export to zero) runs
    // the ctx deinit, which releases the pin — NOW the export is destroyed.
    try fx.a.releaseImport(a_proxy_import, 1);
    try std.testing.expect(!fx.r.exports.contains(proxy_id));
    try std.testing.expect(!fx.p.exports.contains(fx.carol_id));
    try std.testing.expectEqual(@as(usize, 0), fx.p.cross_peer_proxy_links.items.len);
}

// -- G11: the guard still compares the UN-inflated wire count. ---------------

test "handoff pin: over-release stays a detected protocol error and does not spend the pin" {
    const allocator = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.init(allocator);
    defer fx.deinitPeers();

    var returner: CapReturner = undefined;
    try fx.grantCarolToQ(&returner);
    try peer_test_hooks.noteHandoffExportRef(&fx.p, fx.carol_id);
    defer peer_test_hooks.releaseHandoffHeldExport(&fx.p, fx.carol_id);

    // Granted 1 wire ref; a Release(2) must fail the guard — the pin does not
    // inflate the wire count a remote is allowed to spend.
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildRelease(fx.carol_id, 2);
    const frame = try builder.finish();
    defer allocator.free(frame);
    try std.testing.expectError(error.ReleaseCountExceeded, fx.p.handleFrame(frame));

    // Rejected, not applied: the entry is intact with its wire ref and pin.
    const entry = fx.p.exports.get(fx.carol_id) orelse return error.CarolExportMissing;
    try std.testing.expectEqual(@as(u32, 1), entry.ref_count);
    try std.testing.expectEqual(@as(u32, 1), entry.handoff_ref_count);
}

// -- M-7: never-wire-granted exports survive a full pin/unpin cycle. ---------

test "handoff pin: an app-held never-emitted export survives pin/unpin (no remote destroy primitive)" {
    const allocator = std.testing.allocator;
    var p = Peer.initDetached(allocator);
    p.disableThreadAffinity();
    defer p.deinit();

    var carol = Carol{};
    const carol_id = try p.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });

    try peer_test_hooks.noteHandoffExportRef(&p, carol_id);
    peer_test_hooks.releaseHandoffHeldExport(&p, carol_id);

    // All counters zero, but the entry was never granted to the wire: it is
    // app-owned and MUST survive (destroying it would recycle an id the app
    // still holds).
    const entry = p.exports.get(carol_id) orelse return error.AppExportDestroyed;
    try std.testing.expectEqual(@as(u32, 0), entry.ref_count);
    try std.testing.expectEqual(@as(u32, 0), entry.handoff_ref_count);
    try std.testing.expect(!entry.wire_ref_granted);
}

// -- M-1: a failed mint releases the transferred pin exactly once. -----------

test "handoff pin: every failed mint step releases the transferred pin exactly once (OOM sweep)" {
    // Two-pass FailingAllocator sweep. Pass 1 (no failures) records the
    // allocation window of the mint; pass 2 injects one failure at every
    // index inside that window and probes the pin-count invariant — the probe
    // the verdict demands (a leak check cannot see a stolen pin).
    const base = std.testing.allocator;

    var mint_start: usize = 0;
    var mint_end: usize = 0;

    // Pass 1: count.
    {
        var failing = std.testing.FailingAllocator.init(base, .{});
        const alloc = failing.allocator();
        var p = Peer.initDetached(alloc);
        p.disableThreadAffinity();
        defer p.deinit();
        var r = Peer.initDetached(alloc);
        r.disableThreadAffinity();
        defer r.deinit();
        var carol = Carol{};
        const carol_id = try p.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
        try peer_test_hooks.noteHandoffExportRef(&p, carol_id);
        mint_start = failing.alloc_index;
        const proxy_id = try peer_test_hooks.addCrossPeerProxyExport(
            &r,
            &p,
            .{ .exported = .{ .id = carol_id } },
            null,
            carol_id,
        );
        mint_end = failing.alloc_index;
        _ = proxy_id;
        // Success path: the proxy owns the pin; peers deinit sweeps it.
    }
    try std.testing.expect(mint_end > mint_start);

    // Pass 2: inject.
    var fail_index = mint_start;
    while (fail_index < mint_end) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(base, .{ .fail_index = fail_index });
        const alloc = failing.allocator();
        var p = Peer.initDetached(alloc);
        p.disableThreadAffinity();
        defer p.deinit();
        var r = Peer.initDetached(alloc);
        r.disableThreadAffinity();
        defer r.deinit();
        var carol = Carol{};
        const carol_id = try p.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
        try peer_test_hooks.noteHandoffExportRef(&p, carol_id);
        const result = peer_test_hooks.addCrossPeerProxyExport(
            &r,
            &p,
            .{ .exported = .{ .id = carol_id } },
            null,
            carol_id,
        );
        if (result) |_| {
            // Allocation pattern shifted enough that this index no longer
            // fails inside the mint — the success path owns the pin; fine.
        } else |err| {
            try std.testing.expectEqual(@as(anyerror, error.OutOfMemory), err);
            // THE PROBE: the callee released the transferred pin exactly once
            // (0, not 1 = leaked/stolen twice would underflow-warn and stay 0
            // — the underflow log plus this being exactly-0 pins it), the
            // entry survived (app-held), and no proxy state leaked.
            const entry = p.exports.get(carol_id) orelse return error.SourceExportDestroyed;
            try std.testing.expectEqual(@as(u32, 0), entry.handoff_ref_count);
            try std.testing.expectEqual(@as(usize, 0), p.cross_peer_proxy_links.items.len);
        }
    }
}
