//! M0 seam-spike tests for `conn.zig` / `cap_remap.zig` / `effects.zig`.
//!
//! Two `Conn`s in one process are wired back to back: a drain loop moves each
//! OUT_FRAME's bytes into the other side's `pushBytes`, and records every
//! other effect (deep copies) for the assertions.

const std = @import("std");
const capnp = @import("capnpc-zig");
const conn_mod = @import("conn.zig");
const effects = @import("effects.zig");
const cap_remap = @import("cap_remap.zig");

const message = capnp.message;
const protocol = capnp.rpc.wire.protocol;
const cap_table = capnp.rpc.caps.table;
const descriptors = capnp.rpc.caps.descriptors;

const Conn = conn_mod.Conn;
const Stats = conn_mod.Stats;
const Cap = effects.Cap;
const testing = std.testing;

test {
    _ = effects;
    _ = cap_remap;
}

const iface: u64 = 0xa1b2_c3d4_e5f6_0718;

// ---------------------------------------------------------------------------
// Recording host
// ---------------------------------------------------------------------------

const RecReturn = struct {
    qid: u32,
    kind: effects.ReturnKind,
    msg: []u8,
    caps: []Cap,
    exception_type: u16,
    reason: []u8,
};

const RecCall = struct {
    answer_id: u32,
    export_id: u32,
    host_tag: u64,
    interface_id: u64,
    method_id: u16,
    msg: []u8,
    caps: []Cap,
};

const Rec = struct {
    a: std.mem.Allocator,
    returns: std.ArrayList(RecReturn) = .empty,
    calls: std.ArrayList(RecCall) = .empty,
    dropped: std.ArrayList(effects.ExportDropped) = .empty,
    events: std.ArrayList(effects.Event) = .empty,
    /// OUT_FRAMEs kept when there is no peer to forward to.
    frames: std.ArrayList([]u8) = .empty,
    frames_seen: usize = 0,
    close_requested: usize = 0,

    fn init(a: std.mem.Allocator) Rec {
        return .{ .a = a };
    }

    fn deinit(self: *Rec) void {
        for (self.returns.items) |r| {
            self.a.free(r.msg);
            self.a.free(r.caps);
            self.a.free(r.reason);
        }
        for (self.calls.items) |c| {
            self.a.free(c.msg);
            self.a.free(c.caps);
        }
        for (self.frames.items) |f| self.a.free(f);
        self.returns.deinit(self.a);
        self.calls.deinit(self.a);
        self.dropped.deinit(self.a);
        self.events.deinit(self.a);
        self.frames.deinit(self.a);
    }

    fn record(self: *Rec, eff: *const effects.Effect) !void {
        switch (eff.*) {
            .out_frame => |bytes| try self.frames.append(self.a, try self.a.dupe(u8, bytes)),
            .close_requested => self.close_requested += 1,
            .@"return" => |r| try self.returns.append(self.a, .{
                .qid = r.qid,
                .kind = r.kind,
                .msg = try self.a.dupe(u8, r.msg),
                .caps = try self.a.dupe(Cap, r.caps),
                .exception_type = r.exception_type,
                .reason = try self.a.dupe(u8, r.reason),
            }),
            .inbound_call => |c| try self.calls.append(self.a, .{
                .answer_id = c.answer_id,
                .export_id = c.export_id,
                .host_tag = c.host_tag,
                .interface_id = c.interface_id,
                .method_id = c.method_id,
                .msg = try self.a.dupe(u8, c.msg),
                .caps = try self.a.dupe(Cap, c.caps),
            }),
            .export_dropped => |d| try self.dropped.append(self.a, d),
            .event => |e| try self.events.append(self.a, e),
        }
    }

    fn returnFor(self: *Rec, qid: u32) ?RecReturn {
        for (self.returns.items) |r| if (r.qid == qid) return r;
        return null;
    }

    fn countReturns(self: *Rec, qid: u32) usize {
        var n: usize = 0;
        for (self.returns.items) |r| {
            if (r.qid == qid) n += 1;
        }
        return n;
    }

    fn lastCall(self: *Rec) RecCall {
        return self.calls.items[self.calls.items.len - 1];
    }
};

/// Pull one effect from `src`; forward OUT_FRAMEs to `dst` (or keep them when
/// `dst` is null); record everything else. False when `src` had none.
fn drainOne(src: *Conn, rec: *Rec, dst: ?*Conn) !bool {
    const eff = (try src.nextEffect()) orelse return false;
    defer src.commitEffect();
    switch (eff.*) {
        .out_frame => |bytes| {
            rec.frames_seen += 1;
            if (dst) |d| try d.pushBytes(bytes) else try rec.record(eff);
        },
        else => try rec.record(eff),
    }
    return true;
}

/// Move frames both ways until both queues are empty.
fn pump(a: *Conn, ra: *Rec, b: *Conn, rb: *Rec) !void {
    var rounds: usize = 0;
    while (true) : (rounds += 1) {
        if (rounds > 100_000) return error.PumpRunaway;
        const pa = try drainOne(a, ra, b);
        const pb = try drainOne(b, rb, a);
        if (!pa and !pb) return;
    }
}

fn drainAll(c: *Conn, rec: *Rec) !void {
    while (try drainOne(c, rec, null)) {}
}

// ---------------------------------------------------------------------------
// Host message helpers (standalone messages, D5)
// ---------------------------------------------------------------------------

/// Root struct { u64 @0 }.
fn msgU64(a: std.mem.Allocator, v: u64) ![]const u8 {
    var mb = message.MessageBuilder.init(a);
    defer mb.deinit();
    const root = try mb.allocateStruct(1, 0);
    root.writeU64(0, v);
    return mb.toBytes();
}

/// Root struct { u64 @0 = tag, cap @ptr0 = caps[index] }.
fn msgCap(a: std.mem.Allocator, tag: u64, index: u32) ![]const u8 {
    var mb = message.MessageBuilder.init(a);
    defer mb.deinit();
    const root = try mb.allocateStruct(1, 1);
    root.writeU64(0, tag);
    try (try root.getAnyPointer(0)).setCapability(.{ .id = index });
    return mb.toBytes();
}

fn readU64(a: std.mem.Allocator, bytes: []const u8) !u64 {
    var m = try message.Message.init(a, bytes, .{});
    defer m.deinit();
    return (try m.getRootStruct()).readU64(0);
}

/// The `caps[]` entry the root struct's pointer 0 refers to.
fn rootCap(a: std.mem.Allocator, bytes: []const u8, caps: []const Cap) !Cap {
    var m = try message.Message.init(a, bytes, .{});
    defer m.deinit();
    const idx = (try (try m.getRootStruct()).readCapability(0)).id;
    if (idx >= caps.len) return error.TestCapIndexOutOfRange;
    return caps[idx];
}

// ---------------------------------------------------------------------------
// caps_roundtrip
// ---------------------------------------------------------------------------

test "caps_roundtrip: imports and exports both ways, then releases drop exports" {
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    // B's bootstrap is a host export (tag 100).
    const eb = try b.setBootstrap(100);

    // 1. A bootstraps; the RETURN's root is a capability pointer to caps[0].
    const q0 = try a.bootstrap();
    try pump(a, &ra, b, &rb);
    const r0 = ra.returnFor(q0) orelse return error.TestNoBootstrapReturn;
    try testing.expectEqual(effects.ReturnKind.results, r0.kind);
    const ib = blk: {
        var m = try message.Message.init(alloc, r0.msg, .{});
        defer m.deinit();
        const idx = (try (try m.getRootAnyPointer()).getCapability()).id;
        try testing.expect(idx < r0.caps.len);
        break :blk r0.caps[idx];
    };
    try testing.expectEqual(effects.CapKind.import, ib.kind);
    try testing.expectEqual(eb, ib.id);

    // 2. A calls B's bootstrap, passing one of A's exports (tag 200) in the
    //    params at host index 3 (0..2 are null caps). Index 3 is neither an
    //    export nor an import id on A, so a bare index cannot work.
    const ea = try a.exportCap(200);
    try testing.expect(ea != 3 and ib.id != 3);
    const p1 = try msgCap(alloc, 11, 3);
    defer alloc.free(p1);
    const none: Cap = .{ .kind = .none };
    const q1 = try a.call(ib, iface, 0, p1, &.{ none, none, none, .{ .kind = .@"export", .id = ea } }, 0);
    try pump(a, &ra, b, &rb);

    // 3. B's host gets INBOUND_CALL on its bootstrap with an IMPORT of A's cap.
    try testing.expectEqual(@as(usize, 1), rb.calls.items.len);
    const c1 = rb.calls.items[0];
    try testing.expectEqual(eb, c1.export_id);
    try testing.expectEqual(@as(u64, 100), c1.host_tag);
    try testing.expectEqual(iface, c1.interface_id);
    try testing.expectEqual(@as(u16, 0), c1.method_id);
    try testing.expectEqual(@as(u64, 11), try readU64(alloc, c1.msg));
    const ia = try rootCap(alloc, c1.msg, c1.caps);
    try testing.expectEqual(effects.CapKind.import, ia.kind);
    try testing.expectEqual(ea, ia.id);

    // 4. B calls A's cap back, and passes that same cap back in the params:
    //    A must see it as its OWN export (receiverHosted), not as an import.
    const p2 = try msgCap(alloc, 22, 0);
    defer alloc.free(p2);
    const q2 = try b.call(ia, iface, 1, p2, &.{ia}, 0);
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(@as(usize, 1), ra.calls.items.len);
    const c2 = ra.calls.items[0];
    try testing.expectEqual(ea, c2.export_id);
    try testing.expectEqual(@as(u64, 200), c2.host_tag);
    try testing.expectEqual(@as(u16, 1), c2.method_id);
    try testing.expectEqual(@as(u64, 22), try readU64(alloc, c2.msg));
    const back = try rootCap(alloc, c2.msg, c2.caps);
    try testing.expectEqual(effects.CapKind.@"export", back.kind);
    try testing.expectEqual(ea, back.id);

    // 5. A answers B's call; B gets RETURN results and finishes.
    const res42 = try msgU64(alloc, 42);
    defer alloc.free(res42);
    try a.returnResults(c2.answer_id, res42, &.{});
    try pump(a, &ra, b, &rb);
    const r2 = rb.returnFor(q2) orelse return error.TestNoReturnQ2;
    try testing.expectEqual(effects.ReturnKind.results, r2.kind);
    try testing.expectEqual(@as(u64, 42), try readU64(alloc, r2.msg));
    try b.finish(q2, false);

    // 6. B answers A's first call with a NEW export of B's (tag 300). B's
    //    export ids are 0 (bootstrap) and 1, so host index 0 names the wrong
    //    one unless it is remapped.
    const eb2 = try b.exportCap(300);
    try testing.expect(eb2 != 0);
    const res1 = try msgCap(alloc, 33, 0);
    defer alloc.free(res1);
    try b.returnResults(c1.answer_id, res1, &.{.{ .kind = .@"export", .id = eb2 }});
    try pump(a, &ra, b, &rb);
    const r1 = ra.returnFor(q1) orelse return error.TestNoReturnQ1;
    try testing.expectEqual(effects.ReturnKind.results, r1.kind);
    try testing.expectEqual(@as(u64, 33), try readU64(alloc, r1.msg));
    const ib2 = try rootCap(alloc, r1.msg, r1.caps);
    try testing.expectEqual(effects.CapKind.import, ib2.kind);
    try testing.expectEqual(eb2, ib2.id);
    try a.finish(q1, false);

    // 7. A calls the returned cap; B's host sees it as export eb2 / tag 300.
    const p3 = try msgU64(alloc, 5);
    defer alloc.free(p3);
    const q3 = try a.call(ib2, iface, 2, p3, &.{}, 0);
    try pump(a, &ra, b, &rb);
    const c3 = rb.lastCall();
    try testing.expectEqual(eb2, c3.export_id);
    try testing.expectEqual(@as(u64, 300), c3.host_tag);
    try testing.expectEqual(@as(u16, 2), c3.method_id);
    try testing.expectEqual(@as(u64, 5), try readU64(alloc, c3.msg));
    const res7 = try msgU64(alloc, 7);
    defer alloc.free(res7);
    try b.returnResults(c3.answer_id, res7, &.{});
    try pump(a, &ra, b, &rb);
    const r3 = ra.returnFor(q3) orelse return error.TestNoReturnQ3;
    try testing.expectEqual(@as(u64, 7), try readU64(alloc, r3.msg));
    try a.finish(q3, false);
    try pump(a, &ra, b, &rb);

    // Nothing dropped yet: every export still has a remote holder.
    try testing.expectEqual(@as(usize, 0), ra.dropped.items.len);
    try testing.expectEqual(@as(usize, 0), rb.dropped.items.len);

    // 8. Releases. B drops A's cap -> A's export 200 is dropped.
    try b.release(ia.id, 1);
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(@as(usize, 1), ra.dropped.items.len);
    try testing.expectEqual(ea, ra.dropped.items[0].export_id);
    try testing.expectEqual(@as(u64, 200), ra.dropped.items[0].host_tag);

    //    A drops B's cap -> B's export 300 is dropped.
    try a.release(ib2.id, 1);
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(@as(usize, 1), rb.dropped.items.len);
    try testing.expectEqual(eb2, rb.dropped.items[0].export_id);
    try testing.expectEqual(@as(u64, 300), rb.dropped.items[0].host_tag);

    //    A drops the bootstrap import: the bootstrap export is never dropped.
    try a.release(ib.id, 1);
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(@as(usize, 1), rb.dropped.items.len);

    // A second release of a spent import is a stale handle.
    try testing.expectError(error.BadId, a.release(ib2.id, 1));

    // Exactly one RETURN per question, no close requests, no stray calls.
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q0));
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q3));
    try testing.expectEqual(@as(usize, 1), rb.countReturns(q2));
    try testing.expectEqual(@as(usize, 3), ra.returns.items.len);
    try testing.expectEqual(@as(usize, 1), rb.returns.items.len);
    try testing.expectEqual(@as(usize, 1), ra.calls.items.len);
    try testing.expectEqual(@as(usize, 2), rb.calls.items.len);
    try testing.expectEqual(@as(usize, 0), ra.close_requested + rb.close_requested);
    // testing.allocator reports any leak after both deinits.
}

// ---------------------------------------------------------------------------
// cap_remap pointer walk
// ---------------------------------------------------------------------------

test "cap_remap: nested structs, struct lists and pointer lists are remapped, then encoded" {
    const alloc = testing.allocator;

    // A cap table where export 0 and import 0 BOTH exist (the colliding id
    // case a bare index cannot disambiguate) plus export 5 and import 9.
    var table = cap_table.CapTable.init(alloc);
    defer table.deinit();
    try table.noteExportAt(0);
    try table.noteExportAt(5);
    try table.noteImport(0);
    try table.noteImport(9);

    // Host message:
    //   root { ptr0: List(Struct) [ {ptr0: cap#2}, {ptr0: null} ],
    //          ptr1: List(AnyPointer) [ null, cap#1, {ptr0: cap#3} ],
    //          ptr2: struct { ptr0: struct { ptr0: cap#0 } },
    //          ptr3: cap#4 }
    //   caps = [ import 0, export 0, import 9, export 5, none ]
    const host = blk: {
        var mb = message.MessageBuilder.init(alloc);
        defer mb.deinit();
        const root = try mb.allocateStruct(0, 4);
        const sl = try (try root.getAnyPointer(0)).initStructList(2, 0, 1);
        try (try (try sl.get(0)).getAnyPointer(0)).setCapability(.{ .id = 2 });
        const pl = try (try root.getAnyPointer(1)).initPointerList(3);
        try pl.setCapability(1, .{ .id = 1 });
        const inner = try pl.initStruct(2, 0, 1);
        try (try inner.getAnyPointer(0)).setCapability(.{ .id = 3 });
        const s1 = try root.initStruct(2, 0, 1);
        const s2 = try s1.initStruct(0, 0, 1);
        try (try s2.getAnyPointer(0)).setCapability(.{ .id = 0 });
        try (try root.getAnyPointer(3)).setCapability(.{ .id = 4 });
        break :blk try mb.toBytes();
    };
    defer alloc.free(host);
    const caps = [_]Cap{
        .{ .kind = .import, .id = 0 },
        .{ .kind = .@"export", .id = 0 },
        .{ .kind = .import, .id = 9 },
        .{ .kind = .@"export", .id = 5 },
        .{ .kind = .none },
    };

    var mb = protocol.MessageBuilder.init(alloc);
    defer mb.deinit();
    var call = try mb.beginCall(1, iface, 0);
    try call.setTargetImportedCap(9);
    var payload = try call.payloadTyped();
    try cap_remap.writeHostContent(alloc, &table, &payload, host, &caps);

    // Every cap pointer in the cloned content now carries its real id space.
    // Encode exactly as Peer.sendCall does (no callbacks: pure table check).
    try cap_table.encodeCallPayloadCaps(&table, &call, null, null, null);
    const frame = try mb.finish();
    defer alloc.free(frame);

    var decoded = try protocol.DecodedMessage.init(alloc, frame);
    defer decoded.deinit();
    const c = try decoded.asCall();
    const ct = c.params.cap_table orelse return error.TestNoCapTable;
    // Four distinct caps: (receiverHosted 0), (senderHosted 0),
    // (receiverHosted 9), (senderHosted 5). The `none` cap became null.
    try testing.expectEqual(@as(u32, 4), ct.len());

    const Desc = struct { tag: protocol.CapDescriptorTag, id: u32 };
    var descs: [4]Desc = undefined;
    for (0..4) |i| {
        const d = try protocol.CapDescriptor.fromReader(try ct.get(@intCast(i)));
        descs[i] = .{ .tag = d.tag, .id = d.id orelse return error.TestMissingId };
    }
    const content = (try c.params.content.getStruct());
    // Resolve a content cap pointer through the encoded cap table.
    const resolve = struct {
        fn f(ds: []const Desc, cap: message.Capability) Desc {
            return ds[cap.id];
        }
    }.f;

    const sl = try content.readStructList(0);
    try testing.expectEqual(Desc{ .tag = .receiverHosted, .id = 9 }, resolve(&descs, try (try sl.get(0)).readCapability(0)));
    try testing.expect((try sl.get(1)).isPointerNull(0));
    const pl = try content.readPointerList(1);
    try testing.expectEqual(Desc{ .tag = .senderHosted, .id = 0 }, resolve(&descs, try pl.getCapability(1)));
    const inner = try (try content.readPointerList(1)).getStruct(2);
    try testing.expectEqual(Desc{ .tag = .senderHosted, .id = 5 }, resolve(&descs, try inner.readCapability(0)));
    const deep = try (try content.readStruct(2)).readStruct(0);
    try testing.expectEqual(Desc{ .tag = .receiverHosted, .id = 0 }, resolve(&descs, try deep.readCapability(0)));
    try testing.expect(content.isPointerNull(3));
}

test "cap_remap: bad host caps are rejected before anything is sent" {
    const alloc = testing.allocator;
    var table = cap_table.CapTable.init(alloc);
    defer table.deinit();
    try table.noteExportAt(3);

    const host = try msgCap(alloc, 0, 0);
    defer alloc.free(host);
    const cases = [_]struct { caps: []const Cap, err: anyerror }{
        .{ .caps = &.{}, .err = error.CapIndexOutOfRange },
        .{ .caps = &.{.{ .kind = .import, .id = 3 }}, .err = error.BadCapId },
        .{ .caps = &.{.{ .kind = .@"export", .id = 4 }}, .err = error.BadCapId },
    };
    for (cases) |case| {
        var mb = protocol.MessageBuilder.init(alloc);
        defer mb.deinit();
        var call = try mb.beginCall(1, iface, 0);
        var payload = try call.payloadTyped();
        try testing.expectError(case.err, cap_remap.writeHostContent(alloc, &table, &payload, host, case.caps));
    }
}

test "cap_remap: a PROMISED host cap becomes a receiverAnswer descriptor with its pipeline path" {
    const alloc = testing.allocator;
    var table = cap_table.CapTable.init(alloc);
    defer table.deinit();

    // Root { ptr0: cap#0 }; caps = [ promised question 7, path [2, 0] ].
    const host = try msgCap(alloc, 0, 0);
    defer alloc.free(host);
    const path = [_]u16{ 2, 0 };
    const caps = [_]Cap{.{ .kind = .promised, .id = 7, .ops = &path, .nops = path.len }};

    var mb = protocol.MessageBuilder.init(alloc);
    defer mb.deinit();
    var call = try mb.beginCall(1, iface, 0);
    try call.setTargetImportedCap(9);
    var payload = try call.payloadTyped();
    try cap_remap.writeHostContent(alloc, &table, &payload, host, &caps);
    try cap_table.encodeCallPayloadCaps(&table, &call, null, null, null);
    const frame = try mb.finish();
    defer alloc.free(frame);

    var decoded = try protocol.DecodedMessage.init(alloc, frame);
    defer decoded.deinit();
    const c = try decoded.asCall();
    const ct = c.params.cap_table orelse return error.TestNoCapTable;
    try testing.expectEqual(@as(u32, 1), ct.len());
    const d = try protocol.CapDescriptor.fromReader(try ct.get(0));
    try testing.expectEqual(protocol.CapDescriptorTag.receiverAnswer, d.tag);
    const promised = d.promised_answer orelse return error.TestNoPromisedAnswer;
    try testing.expectEqual(@as(u32, 7), promised.question_id);
    try testing.expectEqual(@as(usize, 2), promised.transform.len());
    try testing.expectEqual(@as(u16, 2), (try promised.transform.get(0)).pointer_index);
    try testing.expectEqual(@as(u16, 0), (try promised.transform.get(1)).pointer_index);
    // The encoder retired the table entry it consumed.
    try testing.expectEqual(@as(usize, 0), table.receiver_answers.count());
}

// ---------------------------------------------------------------------------
// Effect queue discipline
// ---------------------------------------------------------------------------

test "busy: a second nextEffect before commit is error.Busy" {
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0 });
    defer a.deinit();
    _ = try a.bootstrap();
    _ = try a.bootstrap();

    const e1 = (try a.nextEffect()) orelse return error.TestNoEffect;
    try testing.expect(e1.* == .out_frame);
    const first = e1.out_frame;
    try testing.expectError(error.Busy, a.nextEffect());
    // The borrowed payload is still intact while the second one is refused.
    try testing.expect(first.len > 0);
    a.commitEffect();
    const e2 = (try a.nextEffect()) orelse return error.TestNoEffect;
    try testing.expect(e2.* == .out_frame);
    a.commitEffect();
    try testing.expect((try a.nextEffect()) == null);
    a.commitEffect(); // commit with nothing in flight: no-op
}

// ---------------------------------------------------------------------------
// Disconnect
// ---------------------------------------------------------------------------

/// A and B connected, A holding B's bootstrap import; returns it.
fn connectPair(a: *Conn, ra: *Rec, b: *Conn, rb: *Rec) !Cap {
    _ = try b.setBootstrap(100);
    const q0 = try a.bootstrap();
    try pump(a, ra, b, rb);
    const r0 = ra.returnFor(q0) orelse return error.TestNoBootstrapReturn;
    var m = try message.Message.init(ra.a, r0.msg, .{});
    defer m.deinit();
    return r0.caps[(try (try m.getRootAnyPointer()).getCapability()).id];
}

test "disconnect: transportClosed ends every open question with exactly one RETURN{DISCONNECTED}" {
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    const ib = try connectPair(a, &ra, b, &rb);
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);
    const q1 = try a.call(ib, iface, 0, p, &.{}, 0);
    const q2 = try a.call(ib, iface, 1, p, &.{}, 0);
    try pump(a, &ra, b, &rb); // B holds both calls unanswered
    try testing.expectEqual(@as(usize, 2), rb.calls.items.len);
    const q3 = try a.call(ib, iface, 2, p, &.{}, 0); // never reaches B
    const returns_before = ra.returns.items.len;

    a.transportClosed();
    try drainAll(a, &ra);
    a.transportClosed(); // idempotent: no second terminal
    try drainAll(a, &ra);

    for ([_]u32{ q1, q2, q3 }) |qid| {
        try testing.expectEqual(@as(usize, 1), ra.countReturns(qid));
        const r = ra.returnFor(qid).?;
        try testing.expectEqual(effects.ReturnKind.disconnected, r.kind);
        try testing.expectEqual(@as(u16, 2), r.exception_type);
    }
    try testing.expectEqual(returns_before + 3, ra.returns.items.len);
    // The connection refuses new work once closed.
    try testing.expectError(error.Closed, a.call(ib, iface, 0, p, &.{}, 0));
    try testing.expectError(error.Closed, a.pushBytes(&.{ 0, 0, 0, 0 }));
}

test "disconnect: under OOM at transportClosed every question still yields one RETURN{DISCONNECTED}" {
    // claims.json #5: against capnp-zig 0.21.0, when the synthetic disconnect
    // Return could not be built (OOM), the Peer freed the question through
    // deinit_ctx and never called on_return, and this test required that
    // path. Since capnp-zig 0.23.0 (handoff H8) the Peer builds that Return
    // in a stack buffer and calls on_return anyway, so the deinit_ctx-only
    // path is no longer reachable from transportClosed. The shim must still
    // produce the terminal, with the right kind, without memory: the first
    // question below gets the one allocation left, the second none.
    var failing = std.testing.FailingAllocator.init(testing.allocator, .{});
    const fa = failing.allocator();
    const alloc = testing.allocator;

    const a = try Conn.init(fa, .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    const ib = try connectPair(a, &ra, b, &rb);
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);
    const q1 = try a.call(ib, iface, 0, p, &.{}, 0);
    const q2 = try a.call(ib, iface, 1, p, &.{}, 0);
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(@as(usize, 2), rb.calls.items.len);

    const via_return_before = a.stats.terminal_via_on_return;
    // Allow one allocation, fail the rest.
    failing.fail_index = failing.alloc_index + 1;
    failing.resize_fail_index = failing.resize_index;
    a.transportClosed();
    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);

    // Both terminals came through on_return (H8), none through deinit_ctx
    // or the shim's close sweep.
    try testing.expectEqual(via_return_before + 2, a.stats.terminal_via_on_return);
    try testing.expectEqual(@as(u64, 0), a.stats.terminal_via_deinit_ctx);
    try testing.expectEqual(@as(u64, 0), a.stats.terminal_via_close_sweep);

    try drainAll(a, &ra);
    for ([_]u32{ q1, q2 }) |qid| {
        try testing.expectEqual(@as(usize, 1), ra.countReturns(qid));
        try testing.expectEqual(effects.ReturnKind.disconnected, ra.returnFor(qid).?.kind);
        try testing.expectEqual(@as(u16, 2), ra.returnFor(qid).?.exception_type);
    }
}

test "errors: stale handles fail cleanly; a failed return can be replaced by an exception" {
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    const ib = try connectPair(a, &ra, b, &rb);
    const p = try msgCap(alloc, 1, 0);
    defer alloc.free(p);

    // A stale import in the params: refused before anything is queued, and
    // the reserved question state is freed (testing.allocator checks).
    try testing.expectError(error.BadCapId, a.call(ib, iface, 0, p, &.{.{ .kind = .import, .id = 999 }}, 0));
    try testing.expect((try a.nextEffect()) == null);
    // A stale call target.
    try testing.expectError(error.BadId, a.call(.{ .kind = .import, .id = 999 }, iface, 0, p, &.{.{ .kind = .none }}, 0));
    // Unknown answer ids.
    try testing.expectError(error.BadId, b.returnResults(12345, p, &.{}));
    try testing.expectError(error.BadId, b.returnException(12345, 0, "x"));

    const q = try a.call(ib, iface, 0, p, &.{.{ .kind = .none }}, 0);
    try pump(a, &ra, b, &rb);
    const c = rb.lastCall();
    // Results naming an export B does not have: refused, nothing sent, the
    // answer stays open...
    try testing.expectError(error.BadCapId, b.returnResults(c.answer_id, p, &.{.{ .kind = .@"export", .id = 77 }}));
    try testing.expect((try b.nextEffect()) == null);
    // ...so the host can still answer it with an exception. A REMOTE
    // exception typed `disconnected` (2) stays an EXCEPTION: only a local
    // disconnect is RETURN{DISCONNECTED}.
    try b.returnException(c.answer_id, 2, "remote app says disconnected");
    try pump(a, &ra, b, &rb);
    const r = ra.returnFor(q) orelse return error.TestNoReturn;
    try testing.expectEqual(effects.ReturnKind.exception, r.kind);
    try testing.expectEqual(@as(u16, 2), r.exception_type);
    try testing.expectEqualStrings("remote app says disconnected", r.reason);
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q));
    try a.finish(q, false);
    // The answer is closed now.
    try testing.expectError(error.BadId, b.returnException(c.answer_id, 0, "again"));
}

test "tick: a call deadline ends the question once; the late Return is absorbed" {
    const alloc = testing.allocator;
    const ms = std.time.ns_per_ms;
    const a = try Conn.init(alloc, .{ .now_ns = 0, .timeouts = .{ .default_call_timeout_ms = 100 } });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    _ = a.tick(1 * ms);
    const ib = try connectPair(a, &ra, b, &rb);
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);
    const q = try a.call(ib, iface, 0, p, &.{}, 0);
    try pump(a, &ra, b, &rb);
    const c = rb.lastCall();

    try testing.expectEqual(@as(usize, 0), a.tick(50 * ms)); // not yet
    try testing.expectEqual(@as(usize, 0), ra.countReturns(q));
    try testing.expectEqual(@as(usize, 1), a.tick(200 * ms));
    try pump(a, &ra, b, &rb); // RETURN on A; Finish reaches B
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q));
    const r = ra.returnFor(q).?;
    try testing.expectEqual(effects.ReturnKind.exception, r.kind);
    try testing.expectEqual(@as(u16, 1), r.exception_type); // overloaded
    try testing.expectEqualStrings(capnp.rpc.peer.deadline_reason, r.reason);

    // B answers late: the Peer absorbs the Return, no second terminal.
    const res = try msgU64(alloc, 9);
    defer alloc.free(res);
    try b.returnResults(c.answer_id, res, &.{});
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q));
    // Cancellation owns the answer now: the host's Finish is refused.
    if (a.finish(q, false)) |_| return error.TestFinishAfterCancelAccepted else |_| {}
}

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

fn expectAbortThenClose(rec: *Rec) !void {
    // Exactly one OUT_FRAME, and it is an Abort, followed by CLOSE_REQUESTED.
    try testing.expectEqual(@as(usize, 1), rec.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(testing.allocator, rec.frames.items[0]);
    defer decoded.deinit();
    try testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    try testing.expectEqual(@as(usize, 1), rec.close_requested);
}

test "malformed frame: bad pointer content -> the Peer's Abort OUT_FRAME, then CLOSE_REQUESTED" {
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0, .observer = true });
    defer a.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();

    // Well framed (1 segment, 1 word), but the root struct pointer points
    // 1000 words past the end of the segment.
    var frame: [16]u8 = undefined;
    std.mem.writeInt(u32, frame[0..4], 0, .little); // segment count - 1
    std.mem.writeInt(u32, frame[4..8], 1, .little); // segment 0: 1 word
    std.mem.writeInt(u64, frame[8..16], (@as(u64, 1) << 32) | (1000 << 2), .little);

    // The Peer logs this decode failure at warn; it is the expected outcome
    // here, so keep it out of the test output (the runner resets the level
    // before each test).
    testing.log_level = .err;
    try testing.expectError(error.Protocol, a.pushBytes(&frame));
    try testing.expect(a.last_error != null);
    try drainAll(a, &ra);
    try expectAbortThenClose(&ra);
    // The observer saw it as a protocol error.
    var saw_protocol_error = false;
    for (ra.events.items) |e| {
        if (e.tag == @backingInt(std.meta.Tag(capnp.rpc.events.Event).protocol_error)) saw_protocol_error = true;
    }
    try testing.expect(saw_protocol_error);
    // Further input is refused; no second close request.
    try testing.expectError(error.Closed, a.pushBytes(&frame));
    try drainAll(a, &ra);
    try testing.expectEqual(@as(usize, 1), ra.close_requested);
}

test "malformed frame: bad segment table -> the shim's Abort OUT_FRAME, then CLOSE_REQUESTED" {
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0 });
    defer a.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();

    // 1000 segments (> 512): the Framer rejects it before the Peer sees it.
    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 999, .little);
    std.mem.writeInt(u32, header[4..8], 1, .little);
    try testing.expectError(error.Protocol, a.pushBytes(&header));
    try testing.expectEqual(@as(?anyerror, error.InvalidFrame), a.last_error);
    try drainAll(a, &ra);
    try expectAbortThenClose(&ra);
}

// ---------------------------------------------------------------------------
// Inbound copies are bounded by their frame (review finding: aliasing)
// ---------------------------------------------------------------------------

/// Tracks the bytes live through it and their peak.
const PeakAllocator = struct {
    parent: std.mem.Allocator,
    live: usize = 0,
    peak: usize = 0,

    fn allocator(self: *PeakAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocFn, .resize = resizeFn, .remap = remapFn, .free = freeFn } };
    }

    fn grew(self: *PeakAllocator, old_len: usize, new_len: usize) void {
        self.live = self.live - old_len + new_len;
        self.peak = @max(self.peak, self.live);
    }

    fn allocFn(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        const p = self.parent.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.grew(0, len);
        return p;
    }

    fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        if (!self.parent.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.grew(memory.len, new_len);
        return true;
    }

    fn remapFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        const p = self.parent.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.grew(memory.len, new_len);
        return p;
    }

    fn freeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *PeakAllocator = @ptrCast(@alignCast(ctx));
        self.parent.rawFree(memory, alignment, ret_addr);
        self.live -= memory.len;
    }
};

/// Fill `content` with struct { ptr0: List(Data) } whose first elements are
/// distinct blobs of `blob_sizes` bytes, followed by `aliases` elements that
/// all point at the LAST blob. Aliasing is legal on the wire; a clone pays
/// for every alias.
fn fillAliasedDataList(alloc: std.mem.Allocator, content: message.AnyPointerBuilder, blob_sizes: []const usize, aliases: u32) !void {
    const n: u32 = @intCast(blob_sizes.len);
    const root = try content.initStruct(0, 1);
    const pl = try (try root.getAnyPointer(0)).initPointerList(n + aliases);
    for (blob_sizes, 0..) |size, i| {
        const blob = try alloc.alloc(u8, size);
        defer alloc.free(blob);
        @memset(blob, @intCast(0xA0 + i));
        try pl.setData(@intCast(i), blob);
    }
    const seg = pl.builder.segments.items[pl.segment_id].items;
    const last = pl.elements_offset + (n - 1) * 8;
    var i: u32 = n;
    while (i < n + aliases) : (i += 1) aliasNearPointer(seg, last, pl.elements_offset + @as(usize, i) * 8);
}

/// Make the near pointer at byte `dst_pos` point where the one at `src_pos`
/// points (same segment, same kind and size).
fn aliasNearPointer(seg: []u8, src_pos: usize, dst_pos: usize) void {
    const w = std.mem.readInt(u64, seg[src_pos..][0..8], .little);
    const off: i64 = @as(i32, @bitCast(@as(u32, @truncate(w)))) >> 2;
    const target: i64 = @as(i64, @intCast(src_pos / 8)) + 1 + off;
    const new_off: i32 = @intCast(target - (@as(i64, @intCast(dst_pos / 8)) + 1));
    const lo: u32 = (@as(u32, @bitCast(new_off)) << 2) | @as(u32, @truncate(w & 3));
    std.mem.writeInt(u64, seg[dst_pos..][0..8], (w & 0xFFFF_FFFF_0000_0000) | lo, .little);
}

/// A Call frame on `target_export` whose params are `fillAliasedDataList`.
fn aliasedCallFrame(alloc: std.mem.Allocator, qid: u32, target_export: u32, blob_sizes: []const usize, aliases: u32) ![]const u8 {
    var mb = protocol.MessageBuilder.init(alloc);
    defer mb.deinit();
    var call = try mb.beginCall(qid, iface, 0);
    try call.setTargetImportedCap(target_export);
    var payload = try call.payloadTyped();
    try fillAliasedDataList(alloc, try payload.initContent(), blob_sizes, aliases);
    return mb.finish();
}

/// What a Conn queued in answer to one pushed Call frame.
const CallOutcome = struct {
    inbound_call_msg_len: ?usize = null,
    /// Reason of a Return(exception) OUT_FRAME, copied (max 64 bytes).
    exception_reason: [64]u8 = undefined,
    exception_len: ?usize = null,

    fn reason(self: *const CallOutcome) ?[]const u8 {
        return if (self.exception_len) |n| self.exception_reason[0..n] else null;
    }
};

fn pushCallFrame(c: *Conn, frame: []const u8) !CallOutcome {
    var out: CallOutcome = .{};
    try c.pushBytes(frame);
    while (try c.nextEffect()) |eff| {
        defer c.commitEffect();
        switch (eff.*) {
            .inbound_call => |call| out.inbound_call_msg_len = call.msg.len,
            .out_frame => |bytes| {
                var d = try protocol.DecodedMessage.init(testing.allocator, bytes);
                defer d.deinit();
                if (d.tag != .@"return") continue;
                const ret = try d.asReturn();
                const ex = ret.exception orelse continue;
                const n = @min(ex.reason.len, out.exception_reason.len);
                @memcpy(out.exception_reason[0..n], ex.reason[0..n]);
                out.exception_len = n;
            },
            else => {},
        }
    }
    return out;
}

test "inbound copy: heavily aliased Call params are refused before the copy is materialized" {
    var peak: PeakAllocator = .{ .parent = testing.allocator };
    const b = try Conn.init(peak.allocator(), .{ .now_ns = 0 });
    defer b.deinit();
    const boot = try b.setBootstrap(7);

    // 1000 pointers to one 8 KiB blob: a ~16 KiB frame whose clone would be
    // ~8 MiB (the review's ADV1 used 8000 x 8 KiB: 72 KB -> 65.6 MB).
    const frame = try aliasedCallFrame(testing.allocator, 100, boot, &.{8192}, 999);
    defer testing.allocator.free(frame);
    const before = peak.live;
    peak.peak = peak.live;
    const out = try pushCallFrame(b, frame);
    const used = peak.peak - before;

    // Refused with an exception, and the host never sees the call.
    try testing.expect(out.inbound_call_msg_len == null);
    try testing.expectEqualStrings("PayloadCopyExceedsFrame", out.reason() orelse return error.TestNoException);
    // The memory spent on this frame stays within a small multiple of it
    // (the framer, the decoded frame, the capped clone, the reply), far
    // below the ~8 MiB an unbounded clone allocates.
    if (used > 8 * frame.len) {
        std.debug.print("peak {d} bytes for a {d}-byte frame\n", .{ used, frame.len });
        return error.TestCloneNotBounded;
    }
}

test "inbound copy: mildly aliased params are refused; plain params copy at most their frame" {
    const b = try Conn.init(testing.allocator, .{ .now_ns = 0 });
    defer b.deinit();
    const boot = try b.setBootstrap(7);

    // Control: two distinct blobs. The copy arrives and is no larger than
    // the frame (a clone without aliasing never is).
    const plain = try aliasedCallFrame(testing.allocator, 100, boot, &.{ 16384, 2048 }, 0);
    defer testing.allocator.free(plain);
    const ok = try pushCallFrame(b, plain);
    const copied = ok.inbound_call_msg_len orelse return error.TestPlainCallRefused;
    try testing.expect(copied <= plain.len);
    try testing.expect(ok.reason() == null);

    // One alias of the 2 KiB blob: the clone is ~1.1x the frame. That is
    // under the clone's memory cap (`cloneMemoryLimit`), so only the exact
    // size check can refuse it.
    const mild = try aliasedCallFrame(testing.allocator, 101, boot, &.{ 16384, 2048 }, 1);
    defer testing.allocator.free(mild);
    try testing.expect(cap_remap.cloneMemoryLimit(mild.len) > 3 * mild.len);
    const refused = try pushCallFrame(b, mild);
    try testing.expect(refused.inbound_call_msg_len == null);
    try testing.expectEqualStrings("PayloadCopyExceedsFrame", refused.reason() orelse return error.TestNoException);
}

test "inbound copy: aliased Return results end the question once, with an exception" {
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    const ib = try connectPair(a, &ra, b, &rb);
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);
    const q = try a.call(ib, iface, 0, p, &.{}, 0);
    try pump(a, &ra, b, &rb);
    const answer = rb.lastCall().answer_id;

    // The remote answers with results that alias one blob 1000 times.
    const frame = blk: {
        var mb = protocol.MessageBuilder.init(alloc);
        defer mb.deinit();
        var ret = try mb.beginReturn(answer, .results);
        var payload = try ret.payloadTyped();
        try fillAliasedDataList(alloc, try payload.initContent(), &.{8192}, 999);
        break :blk try mb.finish();
    };
    defer alloc.free(frame);
    try a.pushBytes(frame);
    try drainAll(a, &ra);

    try testing.expectEqual(@as(usize, 1), ra.countReturns(q));
    const r = ra.returnFor(q).?;
    try testing.expectEqual(effects.ReturnKind.exception, r.kind);
    try testing.expectEqualStrings(conn_mod.oversized_results_reason, r.reason);
    try testing.expectEqual(@as(usize, 0), r.msg.len);
    try a.finish(q, false);
}

// ---------------------------------------------------------------------------
// Remote Abort (review finding: an orderly close is not a protocol failure)
// ---------------------------------------------------------------------------

test "remote abort: no Abort back, CLOSE_REQUESTED, error.Closed; open questions end with its reason" {
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    const ib = try connectPair(a, &ra, b, &rb);
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);
    const q = try a.call(ib, iface, 0, p, &.{}, 0);
    try pump(a, &ra, b, &rb); // B holds the call

    const frame = blk: {
        var mb = protocol.MessageBuilder.init(alloc);
        defer mb.deinit();
        try mb.buildAbort("server shutting down");
        break :blk try mb.finish();
    };
    defer alloc.free(frame);
    testing.log_level = .err;
    try testing.expectError(error.Closed, a.pushBytes(frame));
    try testing.expectEqual(@as(?anyerror, error.RemoteAbort), a.last_error);
    try testing.expectEqualStrings("server shutting down", a.remote_abort_reason orelse return error.TestNoReason);

    // Nothing goes back (no echoed Abort), and the host is asked to close.
    const frames_before = ra.frames.items.len;
    try drainAll(a, &ra);
    try testing.expectEqual(frames_before, ra.frames.items.len);
    try testing.expectEqual(@as(usize, 1), ra.close_requested);
    try testing.expectEqual(@as(usize, 0), ra.countReturns(q));
    // Closed: no new work, no more input.
    try testing.expectError(error.Closed, a.call(ib, iface, 0, p, &.{}, 0));
    try testing.expectError(error.Closed, a.pushBytes(frame));

    // The host closes the transport: the open question ends once, with the
    // remote's reason.
    a.transportClosed();
    try drainAll(a, &ra);
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q));
    const r = ra.returnFor(q).?;
    try testing.expectEqual(effects.ReturnKind.disconnected, r.kind);
    try testing.expectEqualStrings("server shutting down", r.reason);
    try testing.expectEqual(frames_before, ra.frames.items.len);
    try testing.expectEqual(@as(usize, 1), ra.close_requested);
}

// ---------------------------------------------------------------------------
// Clock (review finding: questions sent before the first tick)
// ---------------------------------------------------------------------------

test "clock: a bootstrap sent before the first tick is timed from creation, not from 0" {
    const alloc = testing.allocator;
    const ms = std.time.ns_per_ms;
    // A host's monotonic uptime is large (here: one hour).
    const uptime: i64 = 3600 * std.time.ns_per_s;
    const a = try Conn.init(alloc, .{ .now_ns = uptime, .timeouts = .{ .default_call_timeout_ms = 5_000 } });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();
    _ = try b.setBootstrap(100);

    // Plan §5: the client bootstraps at once after connect; the first tick
    // comes 10 ms later.
    const q0 = try a.bootstrap();
    try testing.expectEqual(@as(usize, 0), a.tick(uptime + 10 * ms));
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(effects.ReturnKind.results, (ra.returnFor(q0) orelse return error.TestNoReturn).kind);
    // The deadline still works, counted from creation.
    const ib = blk: {
        var m = try message.Message.init(alloc, ra.returnFor(q0).?.msg, .{});
        defer m.deinit();
        break :blk ra.returnFor(q0).?.caps[(try (try m.getRootAnyPointer()).getCapability()).id];
    };
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);
    const q1 = try a.call(ib, iface, 0, p, &.{}, 0);
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(@as(usize, 1), a.tick(uptime + 6_000 * ms));
    try drainAll(a, &ra);
    try testing.expectEqual(effects.ReturnKind.exception, (ra.returnFor(q1) orelse return error.TestNoReturn).kind);
}

// ---------------------------------------------------------------------------
// Stats (review finding: u32 counters trapped on overflow)
// ---------------------------------------------------------------------------

test "stats: every counter saturates instead of trapping on a long-lived connection" {
    const alloc = testing.allocator;
    const max = std.math.maxInt(u64);
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);

    // on_return, exports_dropped and deinit_ctx-only terminals.
    {
        var failing = std.testing.FailingAllocator.init(alloc, .{});
        const a = try Conn.init(failing.allocator(), .{ .now_ns = 0 });
        defer a.deinit();
        const b = try Conn.init(alloc, .{ .now_ns = 0 });
        defer b.deinit();
        var ra = Rec.init(alloc);
        defer ra.deinit();
        var rb = Rec.init(alloc);
        defer rb.deinit();
        a.stats = .{ .terminal_via_on_return = max, .terminal_via_deinit_ctx = max, .exports_dropped = max };

        const ib = try connectPair(a, &ra, b, &rb); // a: on_return
        const ea = try a.exportCap(200);
        const pc = try msgCap(alloc, 1, 0);
        defer alloc.free(pc);
        const q1 = try a.call(ib, iface, 0, pc, &.{.{ .kind = .@"export", .id = ea }}, 0);
        try pump(a, &ra, b, &rb);
        const ia = try rootCap(alloc, rb.lastCall().msg, rb.lastCall().caps);
        try b.release(ia.id, 1); // a: exports_dropped
        try pump(a, &ra, b, &rb);
        try testing.expectEqual(@as(usize, 1), ra.dropped.items.len);

        // As in the deinit_ctx-only disconnect test: allow the cancel list,
        // fail every synthetic Return.
        failing.fail_index = failing.alloc_index + 1;
        failing.resize_fail_index = failing.resize_index;
        a.transportClosed(); // a: deinit_ctx
        failing.fail_index = std.math.maxInt(usize);
        failing.resize_fail_index = std.math.maxInt(usize);
        try drainAll(a, &ra);
        try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
        try testing.expectEqual(Stats{
            .terminal_via_on_return = max,
            .terminal_via_deinit_ctx = max,
            .exports_dropped = max,
        }, a.stats);
    }

    // The close sweep (no memory at all at transportClosed).
    {
        var failing = std.testing.FailingAllocator.init(alloc, .{});
        const a = try Conn.init(failing.allocator(), .{ .now_ns = 0 });
        defer a.deinit();
        const b = try Conn.init(alloc, .{ .now_ns = 0 });
        defer b.deinit();
        var ra = Rec.init(alloc);
        defer ra.deinit();
        var rb = Rec.init(alloc);
        defer rb.deinit();
        const ib = try connectPair(a, &ra, b, &rb);
        const q1 = try a.call(ib, iface, 0, p, &.{}, 0);
        try pump(a, &ra, b, &rb);
        a.stats.terminal_via_close_sweep = max;
        failing.fail_index = failing.alloc_index;
        a.transportClosed();
        failing.fail_index = std.math.maxInt(usize);
        try drainAll(a, &ra);
        try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
        try testing.expectEqual(max, a.stats.terminal_via_close_sweep);
    }
}

// ---------------------------------------------------------------------------
// transportClosed under OOM (review finding: questions left open until free)
// ---------------------------------------------------------------------------

test "disconnect: with no memory at all, transportClosed still ends every open question before free" {
    var failing = std.testing.FailingAllocator.init(testing.allocator, .{});
    const alloc = testing.allocator;
    const a = try Conn.init(failing.allocator(), .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    const ib = try connectPair(a, &ra, b, &rb);
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);
    const q1 = try a.call(ib, iface, 0, p, &.{}, 0);
    const q2 = try a.call(ib, iface, 1, p, &.{}, 0);
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(@as(usize, 2), rb.calls.items.len);

    // capnp-zig v0.20.0 cancelled nothing when its cancel list could not be
    // allocated (peer_lifecycle.zig:691 `catch break`; handoff H8), and the
    // shim's close sweep ended both questions. Since capnp-zig 0.23.0 the
    // Peer's cancel pass allocates nothing and ends both itself, through
    // on_return, so the sweep finds nothing; the shim's terminals must still
    // be DISCONNECTED with no memory at all.
    failing.fail_index = failing.alloc_index;
    a.transportClosed();
    failing.fail_index = std.math.maxInt(usize);
    try testing.expectEqual(@as(u64, 0), a.stats.terminal_via_close_sweep);

    try drainAll(a, &ra);
    for ([_]u32{ q1, q2 }) |qid| {
        try testing.expectEqual(@as(usize, 1), ra.countReturns(qid));
        const r = ra.returnFor(qid).?;
        try testing.expectEqual(effects.ReturnKind.disconnected, r.kind);
        try testing.expectEqual(@as(u16, 2), r.exception_type);
    }
    // Nothing later adds a second terminal: not a tick, not the Peer's own
    // callbacks at free (the testing allocator checks that nothing leaks or
    // is freed twice).
    _ = a.tick(1);
    try drainAll(a, &ra);
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q2));
}

test "disconnect: after transportClosed under OOM, freeing the conn adds no second terminal" {
    // Written against capnp-zig 0.21.0, whose cancel pass left the question
    // open under OOM: the shim's close sweep ended it, and Peer.deinit then
    // called back for the swept question (on_return when memory was back at
    // free, deinit_ctx when not), which must only free it. Since capnp-zig
    // 0.23.0 (handoff H8) the Peer ends the question itself, so nothing is
    // swept and nothing calls back at free; the test still pins one terminal
    // and no double free (the testing allocator) through the free.
    const alloc = testing.allocator;
    // false: memory is back at free. true: memory still fails at free.
    for ([_]bool{ false, true }) |oom_at_free| {
        var failing = std.testing.FailingAllocator.init(alloc, .{});
        const a = try Conn.init(failing.allocator(), .{ .now_ns = 0 });
        var a_alive = true;
        defer if (a_alive) a.deinit();
        const b = try Conn.init(alloc, .{ .now_ns = 0 });
        defer b.deinit();
        var ra = Rec.init(alloc);
        defer ra.deinit();
        var rb = Rec.init(alloc);
        defer rb.deinit();

        const ib = try connectPair(a, &ra, b, &rb);
        const p = try msgU64(alloc, 1);
        defer alloc.free(p);
        const q1 = try a.call(ib, iface, 0, p, &.{}, 0);
        try pump(a, &ra, b, &rb);

        failing.fail_index = failing.alloc_index;
        a.transportClosed();
        try testing.expectEqual(@as(u64, 0), a.stats.terminal_via_close_sweep);
        if (!oom_at_free) failing.fail_index = std.math.maxInt(usize);
        try drainAll(a, &ra); // the RETURN's node is freed by its commit
        try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
        try testing.expectEqual(effects.ReturnKind.disconnected, ra.returnFor(q1).?.kind);

        // A second terminal here would reuse the committed node: the testing
        // allocator reports that double free (and a leak if it is skipped).
        a_alive = false;
        a.deinit();
        failing.fail_index = std.math.maxInt(usize);
        try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
    }
}

test "cancel: with no memory at all, the host's cancel still ends the question with one RETURN{CANCELED}" {
    // Since capnp-zig 0.23.0 (handoff H8) the Peer delivers the cancel's
    // synthetic exception through on_return without the heap, so the
    // shim's reason copy is what fails. The fallback must keep the kind the
    // header promises (CANCELED), the exception type, and the reason.
    var failing = std.testing.FailingAllocator.init(testing.allocator, .{});
    const alloc = testing.allocator;
    const a = try Conn.init(failing.allocator(), .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    const ib = try connectPair(a, &ra, b, &rb);
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);
    const q1 = try a.call(ib, iface, 0, p, &.{}, 0);
    try pump(a, &ra, b, &rb);
    const c = rb.lastCall();

    failing.fail_index = failing.alloc_index;
    try a.cancel(q1);
    failing.fail_index = std.math.maxInt(usize);
    try drainAll(a, &ra);
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
    const r = ra.returnFor(q1) orelse return error.TestNoReturn;
    try testing.expectEqual(effects.ReturnKind.canceled, r.kind);
    try testing.expectEqual(@as(u16, 0), r.exception_type); // failed
    try testing.expectEqualStrings(conn_mod.cancel_reason, r.reason);

    // B answers late: absorbed, no second terminal.
    try b.returnResults(c.answer_id, p, &.{});
    _ = a.tick(1); // retries a Finish the OOM kept from going out
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
}

test "tick: with no memory at all, a deadline still ends the question with one RETURN{EXCEPTION overloaded}" {
    // As for cancel: the Peer delivers the deadline's synthetic exception
    // without the heap, and the shim's fallback must keep its kind, its type
    // (overloaded, not failed) and the Peer's reason.
    const ms = std.time.ns_per_ms;
    var failing = std.testing.FailingAllocator.init(testing.allocator, .{});
    const alloc = testing.allocator;
    const a = try Conn.init(failing.allocator(), .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    const ib = try connectPair(a, &ra, b, &rb);
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);
    const q1 = try a.call(ib, iface, 0, p, &.{}, 0);
    try pump(a, &ra, b, &rb);
    try a.setDeadline(q1, 10);

    failing.fail_index = failing.alloc_index;
    try testing.expectEqual(@as(usize, 1), a.tick(20 * ms));
    failing.fail_index = std.math.maxInt(usize);
    try drainAll(a, &ra);
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
    const r = ra.returnFor(q1) orelse return error.TestNoReturn;
    try testing.expectEqual(effects.ReturnKind.exception, r.kind);
    try testing.expectEqual(@as(u16, 1), r.exception_type); // overloaded
    try testing.expectEqualStrings(capnp.rpc.peer.deadline_reason, r.reason);
    _ = a.tick(30 * ms);
    try drainAll(a, &ra);
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
}

// ---------------------------------------------------------------------------
// Allocation-failure sweep (from the M0 review's ADV4; it found the
// transportClosed-under-OOM gap at one fail index)
// ---------------------------------------------------------------------------

/// Like `pump`, but a failed `pushBytes` (allocation failure on the far
/// side) does not stop the scenario: it goes on to the teardown checks.
fn pumpLenient(a: *Conn, ra: *Rec, b: *Conn, rb: *Rec) !void {
    var rounds: usize = 0;
    while (rounds < 10_000) : (rounds += 1) {
        const pa = try drainLenient(a, ra, b);
        const pb = try drainLenient(b, rb, a);
        if (!pa and !pb) return;
    }
    return error.PumpRunaway;
}

fn drainLenient(src: *Conn, rec: *Rec, dst: *Conn) !bool {
    const eff = (try src.nextEffect()) orelse return false;
    defer src.commitEffect();
    switch (eff.*) {
        .out_frame => |bytes| dst.pushBytes(bytes) catch {},
        else => try rec.record(eff),
    }
    return true;
}

/// Caps both ways, a cap passed back, results carrying a new export, a
/// release, then B's transport closes with B's question to A still open, and
/// A is freed with an effect in flight. Every allocation of both conns goes
/// through `fa`; the host side (`Rec`) uses the testing allocator.
fn capsBothWaysTeardown(fa: std.mem.Allocator) !void {
    const ta = testing.allocator;
    const a = try Conn.init(fa, .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(fa, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(ta);
    defer ra.deinit();
    var rb = Rec.init(ta);
    defer rb.deinit();

    _ = try b.setBootstrap(100);
    const q0 = try a.bootstrap();
    try pumpLenient(a, &ra, b, &rb);
    const r0 = ra.returnFor(q0) orelse return error.ScenarioNoBootstrap;
    if (r0.kind != .results) return error.ScenarioNoBootstrap;
    const ib = blk: {
        var m = try message.Message.init(ta, r0.msg, .{});
        defer m.deinit();
        const idx = (try (try m.getRootAnyPointer()).getCapability()).id;
        if (idx >= r0.caps.len) return error.ScenarioNoBootstrap;
        break :blk r0.caps[idx];
    };

    const ea = try a.exportCap(200);
    const p1 = try msgCap(ta, 11, 0);
    defer ta.free(p1);
    _ = try a.call(ib, iface, 0, p1, &.{.{ .kind = .@"export", .id = ea }}, 0);
    try pumpLenient(a, &ra, b, &rb);
    if (rb.calls.items.len < 1) return error.ScenarioNoCall;
    const c1 = rb.calls.items[0];
    const ia = try rootCap(ta, c1.msg, c1.caps);

    const p2 = try msgCap(ta, 22, 0);
    defer ta.free(p2);
    const q2 = try b.call(ia, iface, 1, p2, &.{ia}, 0);
    try pumpLenient(a, &ra, b, &rb);
    if (ra.calls.items.len < 1) return error.ScenarioNoCall;

    const eb2 = try b.exportCap(300);
    const r1 = try msgCap(ta, 33, 0);
    defer ta.free(r1);
    try b.returnResults(c1.answer_id, r1, &.{.{ .kind = .@"export", .id = eb2 }});
    try pumpLenient(a, &ra, b, &rb);

    // q2 (B -> A) stays unanswered; one more A -> B question opens.
    const p3 = try msgCap(ta, 44, 0);
    defer ta.free(p3);
    _ = try a.call(ib, iface, 2, p3, &.{.{ .kind = .none }}, 0);
    try pumpLenient(a, &ra, b, &rb);
    try b.release(ia.id, 1);
    try pumpLenient(a, &ra, b, &rb);

    b.transportClosed();
    try drainAll(b, &rb);
    // The invariant under test: one RETURN per question, whatever failed.
    if (rb.countReturns(q2) != 1) return error.QuestionNotEndedOnce;

    // Free A with an effect in flight and more queued.
    _ = try a.bootstrap();
    _ = try a.nextEffect();
}

test "oom sweep: every allocation failure in a caps-both-ways session fails cleanly, and open questions still end once" {
    try capsBothWaysTeardown(testing.allocator); // passes with no failures
    var i: usize = 0;
    while (true) : (i += 1) {
        if (i > 20_000) return error.TestSweepRunaway;
        var fa = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = i });
        capsBothWaysTeardown(fa.allocator()) catch |err| {
            if (err == error.QuestionNotEndedOnce) {
                std.debug.print("fail index {d}: B's open question did not end exactly once\n", .{i});
                return err;
            }
            continue; // failed cleanly (the testing allocator checks leaks)
        };
        if (!fa.has_induced_failure) break; // every allocation point covered
    }
    try testing.expect(i > 100);
}

// ---------------------------------------------------------------------------
// u32_le framing (M6, plan §2: the QUIC baseline)
// ---------------------------------------------------------------------------

test "u32_le: bootstrap round trip over length-prefixed frames, split pushes" {
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0, .framing = .u32_le });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0, .framing = .u32_le });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();

    _ = try b.setBootstrap(7);
    const q = try a.bootstrap();

    // Every OUT_FRAME of a must now start with the LE u32 length of the
    // standalone message that follows; b must accept the bytes split into
    // arbitrary chunks (a QUIC stream delivers partial reads).
    const eff = (try a.nextEffect()) orelse return error.TestNoOutFrame;
    try testing.expect(@as(std.meta.Tag(effects.Effect), .out_frame) == eff.*);
    const frame = try alloc.dupe(u8, eff.out_frame);
    defer alloc.free(frame);
    a.commitEffect();
    try testing.expect(frame.len > 4);
    const len = std.mem.readInt(u32, frame[0..4], .little);
    try testing.expectEqual(frame.len - 4, len);
    // Deliver one byte at a time: the codec must reassemble; b then
    // answers with a RETURN effect on its own queue.
    for (frame) |byte| try b.pushBytes(&.{byte});
    // b answers on the wire; pump delivers b's frame back into a (through
    // the same codec), and the RETURN effect lands on a, the asker.
    try pump(a, &ra, b, &rb);
    const ret = ra.returnFor(q) orelse return error.TestNoReturn;
    try testing.expectEqual(effects.ReturnKind.results, ret.kind);
}

test "u32_le: a zero or oversized length prefix fails framing" {
    const alloc = testing.allocator;
    {
        const a = try Conn.init(alloc, .{ .now_ns = 0, .framing = .u32_le });
        defer a.deinit();
        try testing.expectError(error.Protocol, a.pushBytes(&.{ 0, 0, 0, 0 }));
    }
    {
        const a = try Conn.init(alloc, .{ .now_ns = 0, .framing = .u32_le, .max_frame_bytes = 128 });
        defer a.deinit();
        var prefix: [4]u8 = undefined;
        std.mem.writeInt(u32, &prefix, 1_000_000, .little);
        try testing.expectError(error.Protocol, a.pushBytes(&prefix));
    }
}
