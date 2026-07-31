//! L17 of the VatC hosting plan: the `handoff_pin_count` IMPORT pin and its
//! deferred-Release accounting — the state that lifts `receiverHosted`
//! provide targets out of fail-closed.
//!
//! The failure this kills (V2-M6 shape, unit form): a receiverHosted provide
//! target is a capability the host merely IMPORTS from the introducer, and a
//! `receiverHosted` descriptor grants NO new wire reference — so if the
//! host's own wire refs on that import drain between Provide and Accept, an
//! eagerly-emitted `Release` lets the introducer destroy the very capability
//! the provision still has to serve. The pin (a) retains the import entry and
//! (b) WITHHOLDS every outbound Release into `deferred_release`, emitting the
//! accumulated count as ONE frame at the last unpin — so total granted ==
//! total released across the whole handoff, just later.
//!
//! Topology: one synchronous pair, P (imports Carol) <-> Q (hosts Carol),
//! with a Release-decoding tap on P's outbound side.

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

const NUMBER_INTERFACE_ID: u64 = 0xD0D0_D0D0_D0D0_D017;
const NUMBER_METHOD_ID: u16 = 0;

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
/// wire-honest way to grant the remote one wire reference on that export.
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

/// Return observer that resolves + retains a single returned cap (one wire
/// reference on P's import of it).
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

const ReleaseRecord = struct { id: u32, count: u32 };

/// Synchronous pair link that FORWARDS every frame but also decodes and
/// records the `Release` frames P (left) sends — the wire-totals ledger.
const TapLink = struct {
    forwarding: bool = true,
    left: ?*Peer = null,
    right: ?*Peer = null,
    allocator: std.mem.Allocator = std.testing.allocator,
    releases: std.ArrayList(ReleaseRecord) = .empty,

    fn deinitTap(self: *TapLink) void {
        self.releases.deinit(self.allocator);
    }

    fn leftSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *TapLink = castCtx(*TapLink, ctx);
        if (!self.forwarding) return;
        {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (decoded.tag == .release) {
                const rel = try decoded.asRelease();
                try self.releases.append(self.allocator, .{ .id = rel.id, .count = rel.reference_count });
            }
        }
        if (self.right) |peer| try peer.handleFrame(frame);
    }

    fn rightSend(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *TapLink = castCtx(*TapLink, ctx);
        if (!self.forwarding) return;
        if (self.left) |peer| try peer.handleFrame(frame);
    }

    fn releasesFor(self: *const TapLink, id: u32) usize {
        var n: usize = 0;
        for (self.releases.items) |r| {
            if (r.id == id) n += 1;
        }
        return n;
    }

    fn releasedTotalFor(self: *const TapLink, id: u32) u32 {
        var total: u32 = 0;
        for (self.releases.items) |r| {
            if (r.id == id) total += r.count;
        }
        return total;
    }
};

const Fixture = struct {
    p: Peer,
    q: Peer,
    link: TapLink,

    fn init(self: *Fixture, allocator: std.mem.Allocator) void {
        self.link = .{};
        self.p = Peer.initDetached(allocator);
        self.q = Peer.initDetached(allocator);
        self.p.disableThreadAffinity();
        self.q.disableThreadAffinity();
        self.link.left = &self.p;
        self.link.right = &self.q;
        self.p.setSendFrameOverride(&self.link, TapLink.leftSend);
        self.q.setSendFrameOverride(&self.link, TapLink.rightSend);
    }

    fn deinitPeers(self: *Fixture) void {
        self.link.forwarding = false;
        self.q.deinit();
        self.p.deinit();
        self.link.deinitTap();
    }

    /// One wire-honest grant of export `id` on Q to P: a call on Q's returner
    /// bootstrap whose Return carries the cap (the probe retains it). Each
    /// call grants exactly one wire reference on P's import.
    fn grantOnce(self: *Fixture, boot_import: u32) !u32 {
        var probe = CapImportProbe{};
        _ = try self.p.sendCall(boot_import, NUMBER_INTERFACE_ID, NUMBER_METHOD_ID, &probe, null, CapImportProbe.onReturn);
        return probe.import_id orelse return error.CapNotGranted;
    }
};

// -- The binding wire-totals shape (L17 test 1):
// --   pinned drain -> ZERO Release frames;
// --   unpin        -> exactly ONE Release(id, N);
// --   re-grant + second pin/unpin cycle -> total granted == total released.

test "handoff import pin: a pinned drain withholds every Release; the unpin emits one exact frame; totals balance across cycles" {
    const allocator = std.testing.allocator;
    var fx: Fixture = undefined;
    fx.init(allocator);
    defer fx.deinitPeers();

    var carol = Carol{};
    const carol_id = try fx.q.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    var returner = CapReturner{ .export_id = carol_id };
    _ = try fx.q.setBootstrap(.{ .ctx = &returner, .on_call = CapReturner.onCall });
    var boot_probe = CapImportProbe{};
    _ = try fx.p.sendBootstrap(&boot_probe, CapImportProbe.onReturn);
    const boot_import = boot_probe.import_id orelse return error.ReturnerNotGranted;

    // CYCLE 1: two wire refs granted, then a pinned full drain.
    const carol_import = try fx.grantOnce(boot_import);
    try std.testing.expectEqual(carol_import, try fx.grantOnce(boot_import));
    var granted_total: u32 = 2;
    {
        const entry = fx.p.caps.imports.get(carol_import) orelse return error.ImportMissing;
        try std.testing.expectEqual(@as(u32, 2), entry.ref_count);
    }

    try peer_test_hooks.noteHandoffImportPin(&fx.p, carol_import);

    // Drain ALL wire refs through the public release path while pinned.
    try fx.p.releaseImport(carol_import, 2);

    // THE WITHHOLD: zero Release frames on the wire (today one is emitted
    // eagerly here), the entry retained on the pin alone with the drained
    // count deferred, and Q's export still alive.
    try std.testing.expectEqual(@as(usize, 0), fx.link.releasesFor(carol_import));
    {
        const entry = fx.p.caps.imports.get(carol_import) orelse return error.ImportDiedDespitePin;
        try std.testing.expectEqual(@as(u32, 0), entry.ref_count);
        try std.testing.expectEqual(@as(u32, 1), entry.handoff_pin_count);
        try std.testing.expectEqual(@as(u32, 2), entry.deferred_release);
    }
    try std.testing.expect(fx.q.exports.contains(carol_id));

    // THE DEFERRED EMISSION: the unpin emits exactly one Release(id, 2); the
    // entry is removed and Q's export dies.
    try peer_test_hooks.releaseHandoffImportPin(&fx.p, carol_import);
    try std.testing.expectEqual(@as(usize, 1), fx.link.releasesFor(carol_import));
    try std.testing.expectEqual(@as(u32, 2), fx.link.releasedTotalFor(carol_import));
    try std.testing.expect(!fx.p.caps.hasImport(carol_import));
    try std.testing.expect(!fx.q.exports.contains(carol_id));

    // CYCLE 2 (RE-GRANT): a fresh export of the same Carol, one new grant,
    // pin/unpin again. The second cycle starts from a ZERO deferred tally —
    // no stale re-emission from cycle 1.
    const carol_id2 = try fx.q.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    returner.export_id = carol_id2;
    const carol_import2 = try fx.grantOnce(boot_import);
    granted_total += 1;

    try peer_test_hooks.noteHandoffImportPin(&fx.p, carol_import2);
    try fx.p.releaseImport(carol_import2, 1);
    try std.testing.expectEqual(@as(usize, 0), fx.link.releasesFor(carol_import2));
    try peer_test_hooks.releaseHandoffImportPin(&fx.p, carol_import2);
    try std.testing.expectEqual(@as(usize, 1), fx.link.releasesFor(carol_import2));
    try std.testing.expectEqual(@as(u32, 1), fx.link.releasedTotalFor(carol_import2));
    try std.testing.expect(!fx.q.exports.contains(carol_id2));

    // TOTALS: every wire reference ever granted was released exactly once.
    var released_total: u32 = 0;
    for (fx.link.releases.items) |r| {
        if (r.id == carol_import or r.id == carol_import2) released_total += r.count;
    }
    try std.testing.expectEqual(granted_total, released_total);

    try fx.p.releaseImport(boot_import, 1);
}

// -- Unpin with NO deferred tally: no frame, no stale emission. --------------

test "handoff import pin: an unpin with nothing deferred emits nothing and leaves live wire refs alone" {
    const allocator = std.testing.allocator;
    var fx: Fixture = undefined;
    fx.init(allocator);
    defer fx.deinitPeers();

    var carol = Carol{};
    const carol_id = try fx.q.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    var returner = CapReturner{ .export_id = carol_id };
    _ = try fx.q.setBootstrap(.{ .ctx = &returner, .on_call = CapReturner.onCall });
    var boot_probe = CapImportProbe{};
    _ = try fx.p.sendBootstrap(&boot_probe, CapImportProbe.onReturn);
    const boot_import = boot_probe.import_id orelse return error.ReturnerNotGranted;
    const carol_import = try fx.grantOnce(boot_import);

    try peer_test_hooks.noteHandoffImportPin(&fx.p, carol_import);
    try peer_test_hooks.releaseHandoffImportPin(&fx.p, carol_import);

    // No Release went out; the wire ref survives and still releases eagerly
    // afterwards (the pin left no residue on the entry).
    try std.testing.expectEqual(@as(usize, 0), fx.link.releasesFor(carol_import));
    {
        const entry = fx.p.caps.imports.get(carol_import) orelse return error.ImportMissing;
        try std.testing.expectEqual(@as(u32, 1), entry.ref_count);
        try std.testing.expectEqual(@as(u32, 0), entry.handoff_pin_count);
        try std.testing.expectEqual(@as(u32, 0), entry.deferred_release);
    }
    try fx.p.releaseImport(carol_import, 1);
    try std.testing.expectEqual(@as(usize, 1), fx.link.releasesFor(carol_import));
    try std.testing.expectEqual(@as(u32, 1), fx.link.releasedTotalFor(carol_import));
    try std.testing.expect(!fx.q.exports.contains(carol_id));

    try fx.p.releaseImport(boot_import, 1);
}

// -- Overlapping pins (Provide-time + serve-time): only the LAST unpin emits.

test "handoff import pin: overlapping pins defer jointly; only the last unpin emits the accumulated Release" {
    const allocator = std.testing.allocator;
    var fx: Fixture = undefined;
    fx.init(allocator);
    defer fx.deinitPeers();

    var carol = Carol{};
    const carol_id = try fx.q.addExport(.{ .ctx = &carol, .on_call = Carol.onCall });
    var returner = CapReturner{ .export_id = carol_id };
    _ = try fx.q.setBootstrap(.{ .ctx = &returner, .on_call = CapReturner.onCall });
    var boot_probe = CapImportProbe{};
    _ = try fx.p.sendBootstrap(&boot_probe, CapImportProbe.onReturn);
    const boot_import = boot_probe.import_id orelse return error.ReturnerNotGranted;
    const carol_import = try fx.grantOnce(boot_import);

    // Two pins — the shape a served provision holds (Provide-time pin plus
    // the proxy ctx's serve-time pin).
    try peer_test_hooks.noteHandoffImportPin(&fx.p, carol_import);
    try peer_test_hooks.noteHandoffImportPin(&fx.p, carol_import);
    try fx.p.releaseImport(carol_import, 1);
    try std.testing.expectEqual(@as(usize, 0), fx.link.releasesFor(carol_import));

    // First unpin (the provision's Finish): NOT the last — still withheld.
    try peer_test_hooks.releaseHandoffImportPin(&fx.p, carol_import);
    try std.testing.expectEqual(@as(usize, 0), fx.link.releasesFor(carol_import));
    try std.testing.expect(fx.q.exports.contains(carol_id));

    // Second unpin (the proxy's destruction): emits the single Release(1).
    try peer_test_hooks.releaseHandoffImportPin(&fx.p, carol_import);
    try std.testing.expectEqual(@as(usize, 1), fx.link.releasesFor(carol_import));
    try std.testing.expectEqual(@as(u32, 1), fx.link.releasedTotalFor(carol_import));
    try std.testing.expect(!fx.p.caps.hasImport(carol_import));
    try std.testing.expect(!fx.q.exports.contains(carol_id));

    try fx.p.releaseImport(boot_import, 1);
}
