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
    finished: std.ArrayList(effects.AnswerFinished) = .empty,
    /// The kind of every recorded effect, in drain order.
    order: std.ArrayList(effects.Kind) = .empty,
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
        self.finished.deinit(self.a);
        self.order.deinit(self.a);
        self.frames.deinit(self.a);
    }

    fn record(self: *Rec, eff: *const effects.Effect) !void {
        try self.order.append(self.a, std.meta.activeTag(eff.*));
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
            .answer_finished => |f| try self.finished.append(self.a, f),
        }
    }

    fn countFinished(self: *Rec, answer_id: u32) usize {
        var n: usize = 0;
        for (self.finished.items) |f| {
            if (f.answer_id == answer_id) n += 1;
        }
        return n;
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

test "errors: after the remote's Finish, an answer that runs out of memory sends nothing and stays owed" {
    // The remote finishes call 10 before the host answers it. Then fail each
    // allocation of capnp_return_exception (then of capnp_return_results)
    // in turn. A refused answer queues no Return and keeps the answer, so
    // the host's next answer is the call's one Return.
    const alloc = testing.allocator;
    for ([_]bool{ false, true }) |with_results| {
        var k: usize = 0;
        while (true) : (k += 1) {
            var failing = std.testing.FailingAllocator.init(alloc, .{});
            const a = try Conn.init(failing.allocator(), .{ .now_ns = 0 });
            defer a.deinit();
            const b = try Conn.init(alloc, .{ .now_ns = 0 });
            defer b.deinit();
            var ra = Rec.init(alloc);
            defer ra.deinit();
            var rb = Rec.init(alloc);
            defer rb.deinit();
            _ = try connectPair(a, &ra, b, &rb);
            const ea = try a.exportCap(200);
            try pushFrame(alloc, a, plainCallFrame(alloc, 10, ea));
            try pushFrame(alloc, a, finishFrame(alloc, 10));
            try drainAll(a, &ra);
            try testing.expectEqual(@as(u32, 10), ra.lastCall().answer_id);

            const res = try msgU64(alloc, 1);
            defer alloc.free(res);
            failing.fail_index = failing.alloc_index + k;
            const answered = if (with_results)
                a.returnResults(10, res, &.{})
            else
                a.returnException(10, 0, "late");
            failing.fail_index = std.math.maxInt(usize);
            const induced = failing.has_induced_failure;
            try drainAll(a, &ra);

            if (answered) |_| {} else |err| {
                try testing.expectEqual(error.OutOfMemory, err);
                try testing.expectEqual(@as(usize, 0), try countReturnFrames(&ra, 10, null));
                try testing.expect(a.pending_answers.contains(10));
                try a.returnException(10, 0, "retry");
                try drainAll(a, &ra);
            }
            try testing.expectEqual(@as(usize, 1), try countReturnFrames(&ra, 10, null));
            try testing.expect(!a.pending_answers.contains(10));
            if (!induced) break;
        }
    }
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
// Pipelined params (handoff H9)
// ---------------------------------------------------------------------------

test "pipelined params: an answer that returned the caller's own capability is still refused" {
    // Handoff H9 makes the Peer resolve a receiverAnswer params cap whose
    // answer returned one of the CALLEE's exports (abi_test.zig's
    // pipelining test, q4). Any other returned capability stays .promised:
    // here B's answer to q1 echoes A's own export back, so B holds it as an
    // import, and B's core still refuses q2 that names it.
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
    const ea = try a.exportCap(200);
    const pc = try msgCap(alloc, 1, 0);
    defer alloc.free(pc);
    const q1 = try a.call(ib, iface, 0, pc, &.{.{ .kind = .@"export", .id = ea }}, 0);
    // Every frame B sends from here on is held in rb.frames, so q1 is still
    // open on A when q2 names it.
    while (try drainOne(a, &ra, b)) {} // q1 reaches B
    try drainAll(b, &rb);
    const c1 = rb.lastCall();
    const ia = try rootCap(alloc, c1.msg, c1.caps);
    try testing.expectEqual(effects.CapKind.import, ia.kind);
    // B returns A's capability.
    try b.returnResults(c1.answer_id, pc, &.{ia});
    try drainAll(b, &rb);
    try testing.expect(rb.frames.items.len > 0);

    const path = [_]u16{0};
    const q2 = try a.call(ib, iface, 3, pc, &.{.{ .kind = .promised, .id = q1, .ops = &path, .nops = 1 }}, 0);
    const calls_before = rb.calls.items.len;
    while (try drainOne(a, &ra, b)) {} // q2 reaches B's core, which refuses it
    try drainAll(b, &rb);
    try testing.expectEqual(calls_before, rb.calls.items.len);
    for (rb.frames.items) |f| try a.pushBytes(f);
    try pump(a, &ra, b, &rb);

    const r1 = ra.returnFor(q1) orelse return error.TestNoReturn;
    try testing.expectEqual(effects.ReturnKind.results, r1.kind);
    const back = try rootCap(alloc, r1.msg, r1.caps);
    try testing.expectEqual(effects.CapKind.@"export", back.kind);
    try testing.expectEqual(ea, back.id);
    const r2 = ra.returnFor(q2) orelse return error.TestNoReturn;
    try testing.expectEqual(effects.ReturnKind.exception, r2.kind);
    try testing.expectEqualStrings("PromisedCapUnsupported", r2.reason);
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q2));
    try a.finish(q1, false);
    try a.finish(q2, false);
    try b.release(ia.id, 1);
    try pump(a, &ra, b, &rb);
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

/// Answer A's open question `qid` with Return{awaitFromThirdParty}, as a
/// three-party-capable callee may. A's Peer parks the question outside its
/// question table until a ThirdPartyAnswer adopts it (none does here), and a
/// parked question ends only through `deinit_ctx` (its third-party sweep) or,
/// when that sweep cannot allocate, the shim's close sweep.
fn parkOnThirdPartyAwait(alloc: std.mem.Allocator, a: *Conn, qid: u32) !void {
    var completion_builder = message.MessageBuilder.init(alloc);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("native-conn-test-completion");
    const completion_bytes = try completion_builder.toBytes();
    defer alloc.free(completion_bytes);
    var completion_message = try message.Message.init(alloc, completion_bytes, .{});
    defer completion_message.deinit();
    const completion = try completion_message.getRootAnyPointer();

    var builder = protocol.MessageBuilder.init(alloc);
    defer builder.deinit();
    var ret = try builder.beginReturn(qid, .awaitFromThirdParty);
    try ret.setAcceptFromThirdParty(completion);
    const frame = try builder.finish();
    defer alloc.free(frame);
    try a.pushBytes(frame);
}

/// One `Stats` counter, each with its own scenario in `driveCounter`.
const Counter = enum { on_return, deinit_ctx, close_sweep, exports_dropped, events_dropped };

/// Run `counter`'s path once on a fresh connection A whose counter starts at
/// `start`, and return that counter after. A allocates through a
/// FailingAllocator, so a scenario can take its memory away.
fn driveCounter(counter: Counter, start: u64) !u64 {
    const alloc = testing.allocator;
    var failing = std.testing.FailingAllocator.init(alloc, .{});
    const a = try Conn.init(failing.allocator(), .{ .now_ns = 0, .observer = counter == .events_dropped });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);

    switch (counter) {
        .on_return => {
            a.stats.terminal_via_on_return = start;
            _ = try connectPair(a, &ra, b, &rb); // the bootstrap's RETURN
            return a.stats.terminal_via_on_return;
        },
        .exports_dropped => {
            const ib = try connectPair(a, &ra, b, &rb);
            const ea = try a.exportCap(200);
            const pc = try msgCap(alloc, 1, 0);
            defer alloc.free(pc);
            _ = try a.call(ib, iface, 0, pc, &.{.{ .kind = .@"export", .id = ea }}, 0);
            try pump(a, &ra, b, &rb);
            const ia = try rootCap(alloc, rb.lastCall().msg, rb.lastCall().caps);
            a.stats.exports_dropped = start;
            try b.release(ia.id, 1);
            try pump(a, &ra, b, &rb);
            try testing.expectEqual(@as(usize, 1), ra.dropped.items.len);
            return a.stats.exports_dropped;
        },
        .deinit_ctx, .close_sweep => {
            const ib = try connectPair(a, &ra, b, &rb);
            const q1 = try a.call(ib, iface, 0, p, &.{}, 0);
            try pump(a, &ra, b, &rb);
            try parkOnThirdPartyAwait(alloc, a, q1);
            try drainAll(a, &ra); // A's Finish for the intermediate answer
            try testing.expectEqual(@as(usize, 0), ra.countReturns(q1));
            const field = switch (counter) {
                .deinit_ctx => &a.stats.terminal_via_deinit_ctx,
                else => &a.stats.terminal_via_close_sweep,
            };
            field.* = start;
            // deinit_ctx: the Peer's third-party sweep collects the parked
            // question and frees it through deinit_ctx. close_sweep: with no
            // memory that sweep cannot allocate its key list and skips it,
            // so the shim's own sweep ends it (and Peer.deinit later frees
            // the context without a second terminal).
            if (counter == .close_sweep) failing.fail_index = failing.alloc_index;
            a.transportClosed();
            failing.fail_index = std.math.maxInt(usize);
            try drainAll(a, &ra);
            try testing.expectEqual(@as(usize, 1), ra.countReturns(q1));
            const r = ra.returnFor(q1) orelse return error.TestNoReturn;
            try testing.expectEqual(effects.ReturnKind.disconnected, r.kind);
            try testing.expectEqual(@as(u16, 2), r.exception_type);
            return field.*;
        },
        .events_dropped => {
            _ = try connectPair(a, &ra, b, &rb);
            a.stats.events_dropped = start;
            // The close events find no memory for their effect nodes.
            failing.fail_index = failing.alloc_index;
            a.transportClosed();
            failing.fail_index = std.math.maxInt(usize);
            return a.stats.events_dropped;
        },
    }
}

test "stats: every counter counts its own path, and saturates instead of trapping" {
    const max = std.math.maxInt(u64);
    for (std.enums.values(Counter)) |counter| {
        // From 0 the scenario reaches the counter's path (exactly once,
        // except for events: transportClosed emits more than one)...
        const from_zero = try driveCounter(counter, 0);
        if (counter == .events_dropped) {
            try testing.expect(from_zero >= 1);
        } else {
            try testing.expectEqual(@as(u64, 1), from_zero);
        }
        // ...and from the top it saturates instead of trapping.
        try testing.expectEqual(max, try driveCounter(counter, max));
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

// ---------------------------------------------------------------------------
// Answers the host owes: id reuse and the owed-answer cap
// ---------------------------------------------------------------------------

/// Fill the Peer's finished-early records with calls that never reach the
/// host: one call per id on a new promise export, each finished with
/// `requireEarlyCancellationWorkaround`, so the Peer keeps it queued and
/// keeps its record (as with a go-capnp caller).
fn fillFinishedRecords(alloc: std.mem.Allocator, c: *Conn, ids: []const u32) !void {
    const pe = try c.promiseExport();
    const before = c.peer.finished_early_answers.count();
    for (ids) |id| {
        try pushFrame(alloc, c, plainCallFrame(alloc, id, pe));
        try pushFrame(alloc, c, finishFrameWith(alloc, id, true));
    }
    try testing.expectEqual(before + ids.len, c.peer.finished_early_answers.count());
}

fn countCalls(rec: *Rec, answer_id: u32) usize {
    var n: usize = 0;
    for (rec.calls.items) |c| {
        if (c.answer_id == answer_id) n += 1;
    }
    return n;
}

/// The next effects of `c` are an exception Return for `answer_id` whose
/// reason is `reason`, then an Abort, then CLOSE_REQUESTED, and nothing more.
fn expectReturnAbortClose(c: *Conn, answer_id: u32, reason: []const u8) !void {
    {
        const eff = (try c.nextEffect()) orelse return error.TestNoEffect;
        defer c.commitEffect();
        var d = try protocol.DecodedMessage.init(testing.allocator, eff.out_frame);
        defer d.deinit();
        const ret = try d.asReturn();
        try testing.expectEqual(answer_id, ret.answer_id);
        try testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
        try testing.expectEqualStrings(reason, ret.exception.?.reason);
    }
    {
        const eff = (try c.nextEffect()) orelse return error.TestNoEffect;
        defer c.commitEffect();
        var d = try protocol.DecodedMessage.init(testing.allocator, eff.out_frame);
        defer d.deinit();
        try testing.expectEqual(protocol.MessageTag.abort, d.tag);
    }
    {
        const eff = (try c.nextEffect()) orelse return error.TestNoEffect;
        defer c.commitEffect();
        try testing.expect(eff.* == .close_requested);
    }
    try testing.expect((try c.nextEffect()) == null);
}

test "inbound: a new call that reuses an id the host still owes closes the connection" {
    // The remote finishes call 10 before the host answers it, then sends a
    // new call 10. With a record of the finished call, the Peer refuses the
    // id itself: Abort, no Return. With its records full (2 of 2, held by
    // calls queued on a promise export), the Peer accepts the id; the core
    // answers the new call with an exception, sends Abort and asks to
    // close. Either way the host sees one INBOUND_CALL 10.
    const alloc = testing.allocator;
    for ([_]bool{ false, true }) |records_full| {
        const a = try Conn.init(alloc, .{ .now_ns = 0, .limits = .{ .max_active_inbound_questions = 2 } });
        defer a.deinit();
        var ra = Rec.init(alloc);
        defer ra.deinit();
        const ea = try a.exportCap(200);
        if (records_full) try fillFinishedRecords(alloc, a, &.{ 20, 21 });
        try pushFrame(alloc, a, plainCallFrame(alloc, 10, ea));
        try pushFrame(alloc, a, finishFrame(alloc, 10));
        try drainAll(a, &ra);
        try testing.expectEqual(@as(usize, 1), countCalls(&ra, 10));

        try testing.expectError(error.Protocol, pushFrame(alloc, a, plainCallFrame(alloc, 10, ea)));
        try testing.expectEqual(@as(?anyerror, error.DuplicateQuestionId), a.last_error);
        if (records_full) {
            try expectReturnAbortClose(a, 10, "DuplicateQuestionId");
        } else {
            try drainAll(a, &ra);
            try expectAbortThenClose(&ra);
        }
        try drainAll(a, &ra);
        try testing.expectEqual(@as(usize, 1), countCalls(&ra, 10));
        try testing.expectError(error.Closed, pushFrame(alloc, a, plainCallFrame(alloc, 11, ea)));
    }
}

test "inbound: a queued call that reuses an id the host still owes closes the connection when it is delivered" {
    // As above, records full, but the new call 10 waits on a promise export
    // and reaches the core only when the host resolves the promise.
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0, .limits = .{ .max_active_inbound_questions = 2 } });
    defer a.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    const ea = try a.exportCap(200);
    try fillFinishedRecords(alloc, a, &.{ 20, 21 });
    try pushFrame(alloc, a, plainCallFrame(alloc, 10, ea));
    try pushFrame(alloc, a, finishFrame(alloc, 10));
    const pe = try a.promiseExport();
    try pushFrame(alloc, a, plainCallFrame(alloc, 10, pe));
    try drainAll(a, &ra);
    try testing.expectEqual(@as(usize, 1), countCalls(&ra, 10));
    try testing.expect(a.last_error == null);

    try a.resolvePromise(pe, .{ .kind = .@"export", .id = ea });
    try testing.expectEqual(@as(?anyerror, error.DuplicateQuestionId), a.last_error);
    {
        // The Peer's Resolve of the promise comes first.
        const eff = (try a.nextEffect()) orelse return error.TestNoEffect;
        defer a.commitEffect();
        var d = try protocol.DecodedMessage.init(alloc, eff.out_frame);
        defer d.deinit();
        try testing.expectEqual(protocol.MessageTag.resolve, d.tag);
    }
    try expectReturnAbortClose(a, 10, "DuplicateQuestionId");
    try testing.expectEqual(@as(usize, 1), countCalls(&ra, 10));
    try testing.expectError(error.Closed, pushFrame(alloc, a, plainCallFrame(alloc, 11, ea)));
}

test "inbound: a pipelined call that reuses an id the host still owes closes the connection when the host's answer releases it" {
    // Records full; the host owes 10. The new call 10 is pipelined on the
    // host's answer 30, so the Peer queues it until the host answers 30
    // with one of its exports; then the call reaches the core.
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0, .limits = .{ .max_active_inbound_questions = 2 } });
    defer a.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    const ea = try a.exportCap(200);
    try fillFinishedRecords(alloc, a, &.{ 20, 21 });
    try pushFrame(alloc, a, plainCallFrame(alloc, 10, ea));
    try pushFrame(alloc, a, finishFrame(alloc, 10));
    try pushFrame(alloc, a, plainCallFrame(alloc, 30, ea));
    {
        var mb = protocol.MessageBuilder.init(alloc);
        defer mb.deinit();
        var call = try mb.beginCall(10, iface, 4);
        try call.setTargetPromisedAnswerWithOps(30, &.{.{ .tag = .getPointerField, .pointer_index = 0 }});
        var payload = try call.payloadTyped();
        _ = try (try payload.initContent()).initStruct(0, 0);
        try pushFrame(alloc, a, mb.finish());
    }
    try drainAll(a, &ra);
    try testing.expectEqual(@as(usize, 1), countCalls(&ra, 10));
    try testing.expectEqual(@as(usize, 1), countCalls(&ra, 30));
    try testing.expect(a.last_error == null);

    const res = try msgCap(alloc, 1, 0);
    defer alloc.free(res);
    try a.returnResults(30, res, &.{.{ .kind = .@"export", .id = ea }});
    try testing.expectEqual(@as(?anyerror, error.DuplicateQuestionId), a.last_error);
    {
        // The host's Return of 30 comes first.
        const eff = (try a.nextEffect()) orelse return error.TestNoEffect;
        defer a.commitEffect();
        var d = try protocol.DecodedMessage.init(alloc, eff.out_frame);
        defer d.deinit();
        try testing.expectEqual(@as(u32, 30), (try d.asReturn()).answer_id);
    }
    try expectReturnAbortClose(a, 10, "DuplicateQuestionId");
    try testing.expectEqual(@as(usize, 1), countCalls(&ra, 10));
}

test "inbound: a call beyond max_active_inbound_questions owed answers is refused; the connection stays up" {
    // The host owes calls 10 and 11 (the remote finished both, so the Peer's
    // own active-call limit has room). Call 12 gets an exception Return and
    // never reaches the host. After the host answers 10, call 13 does.
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0, .limits = .{ .max_active_inbound_questions = 2 } });
    defer a.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    const ea = try a.exportCap(200);
    for ([_]u32{ 10, 11 }) |id| {
        try pushFrame(alloc, a, plainCallFrame(alloc, id, ea));
        try pushFrame(alloc, a, finishFrame(alloc, id));
    }
    try pushFrame(alloc, a, plainCallFrame(alloc, 12, ea));
    try drainAll(a, &ra);
    try testing.expectEqual(@as(usize, 2), ra.calls.items.len);
    try testing.expectEqual(@as(usize, 0), countCalls(&ra, 12));
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&ra, 12, .exception));
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&ra, 12, null));

    const res = try msgU64(alloc, 1);
    defer alloc.free(res);
    try a.returnResults(10, res, &.{});
    try pushFrame(alloc, a, plainCallFrame(alloc, 13, ea));
    try drainAll(a, &ra);
    try testing.expectEqual(@as(usize, 1), countCalls(&ra, 13));
    try testing.expectEqual(@as(usize, 0), ra.close_requested);
    try testing.expect(a.last_error == null);
}

// ---------------------------------------------------------------------------
// ANSWER_FINISHED (capnp-swift handoff H13)
// ---------------------------------------------------------------------------

/// A Conn that opted in, with export `ea` (tag 200), and its recording host.
const Finished = struct {
    a: *Conn,
    ra: Rec,
    ea: u32,

    fn init(alloc: std.mem.Allocator, opts: conn_mod.Options) !Finished {
        const a = try Conn.init(alloc, opts);
        errdefer a.deinit();
        return .{ .a = a, .ra = Rec.init(alloc), .ea = try a.exportCap(200) };
    }

    fn deinit(self: *Finished) void {
        self.ra.deinit();
        self.a.deinit();
    }
};

test "answer finished: the remote's Finish of an unanswered call queues one ANSWER_FINISHED; returnCanceled answers it once" {
    const alloc = testing.allocator;
    var f = try Finished.init(alloc, .{ .now_ns = 0, .answer_finished = true });
    defer f.deinit();

    try pushFrame(alloc, f.a, plainCallFrame(alloc, 10, f.ea));
    try pushFrame(alloc, f.a, finishFrame(alloc, 10));
    try drainAll(f.a, &f.ra);
    try testing.expectEqualSlices(effects.Kind, &.{ .inbound_call, .answer_finished }, f.ra.order.items);
    try testing.expectEqual(effects.AnswerFinished{ .answer_id = 10, .export_id = f.ea, .host_tag = 200 }, f.ra.finished.items[0]);
    // A second Finish of the same id changes nothing.
    try pushFrame(alloc, f.a, finishFrame(alloc, 10));
    try drainAll(f.a, &f.ra);
    try testing.expectEqual(@as(usize, 1), f.ra.finished.items.len);

    try f.a.returnCanceled(10);
    try drainAll(f.a, &f.ra);
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&f.ra, 10, .canceled));
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&f.ra, 10, null));
    // Answered: every further answer is a stale id, and nothing is sent.
    const res = try msgU64(alloc, 1);
    defer alloc.free(res);
    try testing.expectError(error.BadId, f.a.returnCanceled(10));
    try testing.expectError(error.BadId, f.a.returnResults(10, res, &.{}));
    try testing.expectError(error.BadId, f.a.returnException(10, 0, "late"));
    try drainAll(f.a, &f.ra);
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&f.ra, 10, null));
    // The remote may reuse the id now: a new call, a new answer.
    try pushFrame(alloc, f.a, plainCallFrame(alloc, 10, f.ea));
    try drainAll(f.a, &f.ra);
    try testing.expectEqual(@as(usize, 2), countCalls(&f.ra, 10));
    try f.a.returnResults(10, res, &.{});
    try drainAll(f.a, &f.ra);
    try testing.expectEqual(@as(usize, 2), try countReturnFrames(&f.ra, 10, null));
    try testing.expectEqual(@as(usize, 1), f.ra.finished.items.len);
    try testing.expectEqual(@as(usize, 0), f.ra.close_requested);
}

test "answer finished: after ANSWER_FINISHED the host may answer with results or an exception instead, once" {
    const alloc = testing.allocator;
    const res = try msgU64(alloc, 1);
    defer alloc.free(res);
    for ([_]bool{ true, false }) |with_results| {
        var f = try Finished.init(alloc, .{ .now_ns = 0, .answer_finished = true });
        defer f.deinit();
        try pushFrame(alloc, f.a, plainCallFrame(alloc, 10, f.ea));
        try pushFrame(alloc, f.a, finishFrame(alloc, 10));
        try drainAll(f.a, &f.ra);
        try testing.expectEqual(@as(usize, 1), f.ra.countFinished(10));
        if (with_results) try f.a.returnResults(10, res, &.{}) else try f.a.returnException(10, 0, "stopped");
        try testing.expectError(error.BadId, f.a.returnCanceled(10));
        try drainAll(f.a, &f.ra);
        try testing.expectEqual(@as(usize, 1), try countReturnFrames(&f.ra, 10, null));
        try testing.expectEqual(@as(usize, 0), try countReturnFrames(&f.ra, 10, .canceled));
    }
}

test "answer finished: without the opt-in no effect; returnCanceled is refused; the late answer goes out once" {
    const alloc = testing.allocator;
    var f = try Finished.init(alloc, .{ .now_ns = 0 });
    defer f.deinit();
    try pushFrame(alloc, f.a, plainCallFrame(alloc, 10, f.ea));
    try pushFrame(alloc, f.a, finishFrame(alloc, 10));
    try drainAll(f.a, &f.ra);
    try testing.expectEqualSlices(effects.Kind, &.{.inbound_call}, f.ra.order.items);
    try testing.expectError(error.Invalid, f.a.returnCanceled(10));
    const res = try msgU64(alloc, 1);
    defer alloc.free(res);
    try f.a.returnResults(10, res, &.{});
    try drainAll(f.a, &f.ra);
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&f.ra, 10, null));
    try testing.expectError(error.BadId, f.a.returnCanceled(10));
}

test "answer finished: returnCanceled before the Finish is refused, and on an unknown id is a stale id" {
    const alloc = testing.allocator;
    var f = try Finished.init(alloc, .{ .now_ns = 0, .answer_finished = true });
    defer f.deinit();
    try pushFrame(alloc, f.a, plainCallFrame(alloc, 10, f.ea));
    try drainAll(f.a, &f.ra);
    try testing.expectError(error.Invalid, f.a.returnCanceled(10));
    try testing.expectError(error.BadId, f.a.returnCanceled(11));
    try drainAll(f.a, &f.ra);
    try testing.expectEqual(@as(usize, 0), try countReturnFrames(&f.ra, 10, null));
    // Still owed: the Finish now reports it.
    try pushFrame(alloc, f.a, finishFrame(alloc, 10));
    try drainAll(f.a, &f.ra);
    try testing.expectEqual(@as(usize, 1), f.ra.countFinished(10));
    try f.a.returnCanceled(10);
}

test "answer finished: an answer given before the effect is drained makes the effect stale" {
    // The effect is queued with the Finish; the host answers before it
    // drains it. FIFO: the effect comes before that answer's Return frame,
    // so a host that applies effects in drain order sees it first and knows
    // it is stale: the id is already answered.
    const alloc = testing.allocator;
    var f = try Finished.init(alloc, .{ .now_ns = 0, .answer_finished = true });
    defer f.deinit();
    try pushFrame(alloc, f.a, plainCallFrame(alloc, 10, f.ea));
    try pushFrame(alloc, f.a, finishFrame(alloc, 10));
    const res = try msgU64(alloc, 1);
    defer alloc.free(res);
    try f.a.returnResults(10, res, &.{});
    try drainAll(f.a, &f.ra);
    try testing.expectEqualSlices(effects.Kind, &.{ .inbound_call, .answer_finished, .out_frame }, f.ra.order.items);
    try testing.expectError(error.BadId, f.a.returnCanceled(10));
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&f.ra, 10, null));
}

test "answer finished: after ANSWER_FINISHED a Call or Bootstrap that reuses the id closes the connection, with no Return" {
    // The Peer keeps a record of the Finish until the host's answer, so it
    // refuses the reused id itself, for a Bootstrap too.
    const alloc = testing.allocator;
    for ([_]bool{ false, true }) |bootstrap| {
        var f = try Finished.init(alloc, .{ .now_ns = 0, .answer_finished = true });
        defer f.deinit();
        _ = try f.a.setBootstrap(100);
        try pushFrame(alloc, f.a, plainCallFrame(alloc, 10, f.ea));
        try pushFrame(alloc, f.a, finishFrame(alloc, 10));
        try drainAll(f.a, &f.ra);
        try testing.expectEqual(@as(usize, 1), f.ra.countFinished(10));
        const reuse = if (bootstrap) blk: {
            var mb = protocol.MessageBuilder.init(alloc);
            defer mb.deinit();
            try mb.buildBootstrap(10);
            break :blk mb.finish();
        } else plainCallFrame(alloc, 10, f.ea);
        try testing.expectError(error.Protocol, pushFrame(alloc, f.a, reuse));
        try testing.expectEqual(@as(?anyerror, error.DuplicateQuestionId), f.a.last_error);
        try drainAll(f.a, &f.ra);
        try expectAbortThenClose(&f.ra);
        try testing.expectEqual(@as(usize, 1), countCalls(&f.ra, 10));
    }
}

test "answer finished: with the Peer's finished-early records full, no effect; the ordinary answer goes out" {
    // Records held by calls queued on a promise export (2 of 2). The Peer
    // cannot record call 10's Finish, so it does not report it, and the
    // core never learns of it.
    const alloc = testing.allocator;
    var f = try Finished.init(alloc, .{ .now_ns = 0, .answer_finished = true, .limits = .{ .max_active_inbound_questions = 2 } });
    defer f.deinit();
    try fillFinishedRecords(alloc, f.a, &.{ 20, 21 });
    try pushFrame(alloc, f.a, plainCallFrame(alloc, 10, f.ea));
    try pushFrame(alloc, f.a, finishFrame(alloc, 10));
    try drainAll(f.a, &f.ra);
    try testing.expectEqual(@as(usize, 1), countCalls(&f.ra, 10));
    try testing.expectEqual(@as(usize, 0), f.ra.finished.items.len);
    try testing.expectError(error.Invalid, f.a.returnCanceled(10));
    const res = try msgU64(alloc, 1);
    defer alloc.free(res);
    try f.a.returnResults(10, res, &.{});
    try drainAll(f.a, &f.ra);
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&f.ra, 10, null));
}

test "answer finished: a Finish of a held call is withheld; each end of the hold without the answer reports it once" {
    // The remote finishes Y (77) while question X waits for Y's answer: X
    // still wants it, so the host is not told. When X ends without Y's
    // answer (cancel, its deadline, the shutdown drain bound), the core
    // reports Y. Then returnCanceled answers Y with one Return.
    const alloc = testing.allocator;
    const ms = std.time.ns_per_ms;
    const Ending = enum { cancel, deadline, drain };
    for ([_]Ending{ .cancel, .deadline, .drain }) |ending| {
        var p = try TailPair.init(alloc, .{
            .now_ns = 0,
            .answer_finished = true,
            .timeouts = .{ .shutdown_drain_timeout_ms = 100 },
        });
        defer p.deinit();
        const t = try p.start(alloc);
        try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
        try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
        try drainAll(p.a, &p.ra);
        try testing.expectEqual(@as(usize, 0), p.ra.finished.items.len);
        try testing.expectError(error.Invalid, p.a.returnCanceled(tail_answer));
        try expectNoReturnYet(&p.ra, t.q);

        switch (ending) {
            .cancel => try p.a.cancel(t.q),
            .deadline => {
                try p.a.setDeadline(t.q, 10);
                try testing.expectEqual(@as(usize, 1), p.a.tick(20 * ms));
            },
            .drain => {
                p.a.shutdown();
                try testing.expectEqual(@as(usize, 1), p.a.tick(150 * ms));
            },
        }
        try drainAll(p.a, &p.ra);
        _ = try expectOneReturn(&p.ra, t.q, switch (ending) {
            .cancel => .canceled,
            .deadline => .exception,
            .drain => .disconnected,
        });
        try testing.expectEqual(@as(usize, 1), p.ra.countFinished(tail_answer));
        try testing.expectEqual(effects.AnswerFinished{ .answer_id = tail_answer, .export_id = t.ea, .host_tag = 200 }, p.ra.finished.items[0]);
        try p.a.returnCanceled(tail_answer);
        try drainAll(p.a, &p.ra);
        try testing.expectEqual(@as(usize, 1), try countReturnFrames(&p.ra, tail_answer, null));
        try testing.expectEqual(@as(usize, 1), try countReturnFrames(&p.ra, tail_answer, .canceled));
        try testing.expectEqual(@as(usize, 1), p.ra.countFinished(tail_answer));
        try testing.expectEqual(@as(usize, 1), p.ra.countReturns(t.q));
    }
}

test "answer finished: a Finish of a held call, then the host's answer: the question gets it, and nothing is reported" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0, .answer_finished = true });
    defer p.deinit();
    const t = try p.start(alloc);
    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    const res = try msgU64(alloc, 11);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .results);
    try testing.expectEqual(@as(u64, 11), try readU64(alloc, r.msg));
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&p.ra, tail_answer, null));
    try p.a.finish(t.q, false);
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 0), p.ra.finished.items.len);
    try testing.expectError(error.BadId, p.a.returnCanceled(tail_answer));
}

test "answer finished: a call already reported finished is not held; the redirect to it is not followed" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0, .answer_finished = true });
    defer p.deinit();
    const t = try p.start(alloc);
    try pushFrame(alloc, p.a, yourselfCallFrame(alloc, tail_answer, t.ea));
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), p.ra.countFinished(tail_answer));
    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, t.q, tail_answer));
    try drainAll(p.a, &p.ra);
    try expectUnfollowed(&p.ra, t.q);
    try testing.expectEqual(@as(usize, 0), p.a.held_tail_calls.count());
    try p.a.returnCanceled(tail_answer);
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&p.ra, tail_answer, null));
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&p.ra, tail_answer, .canceled));
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(t.q));
}

test "answer finished: the host's cancel of its own loopback call reports it before the RETURN CANCELED" {
    // B resolves a promise it gave A to A's own export, so A's call on that
    // import loops back to A's host. A's cancel is the call's Finish.
    const alloc = testing.allocator;
    const a = try Conn.init(alloc, .{ .now_ns = 0, .answer_finished = true });
    defer a.deinit();
    const b = try Conn.init(alloc, .{ .now_ns = 0 });
    defer b.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    var rb = Rec.init(alloc);
    defer rb.deinit();
    const ib = try connectPair(a, &ra, b, &rb);
    const ea = try a.exportCap(200);
    const pe = try b.promiseExport();
    const pc = try msgCap(alloc, 1, 0);
    defer alloc.free(pc);
    // A passes ea to B; B answers with the promise.
    const q1 = try a.call(ib, iface, 0, pc, &.{.{ .kind = .@"export", .id = ea }}, 0);
    try pump(a, &ra, b, &rb);
    const c1 = rb.lastCall();
    const ia = try rootCap(alloc, c1.msg, c1.caps);
    try b.returnResults(c1.answer_id, pc, &.{.{ .kind = .@"export", .id = pe }});
    try pump(a, &ra, b, &rb);
    const ip = try rootCap(alloc, ra.returnFor(q1).?.msg, ra.returnFor(q1).?.caps);
    try a.finish(q1, false);
    try b.resolvePromise(pe, ia);
    try pump(a, &ra, b, &rb);

    const calls_before = ra.calls.items.len;
    const p = try msgU64(alloc, 5);
    defer alloc.free(p);
    const q2 = try a.call(ip, iface, 7, p, &.{}, 0);
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(calls_before + 1, ra.calls.items.len);
    const loop = ra.lastCall();
    try testing.expectEqual(ea, loop.export_id);
    try testing.expectEqual(@as(u16, 7), loop.method_id);
    try testing.expectEqual(@as(usize, 1), rb.calls.items.len); // B saw only c1

    const order_before = ra.order.items.len;
    try a.cancel(q2);
    try drainAll(a, &ra);
    try testing.expectEqualSlices(effects.Kind, &.{ .answer_finished, .@"return" }, ra.order.items[order_before..]);
    try testing.expectEqual(loop.answer_id, ra.finished.items[0].answer_id);
    _ = try expectOneReturn(&ra, q2, .canceled);
    try a.returnCanceled(loop.answer_id);
    try pump(a, &ra, b, &rb);
    try testing.expectEqual(@as(usize, 1), ra.countReturns(q2));
    try testing.expectError(error.BadId, a.returnCanceled(loop.answer_id));
}

test "answer finished: freeing the connection while the host owes answers leaks nothing" {
    // Owed answers at free time: one never finished (its node reserved),
    // one reported and drained, one reported and still queued, and one
    // withheld by a tail-call hold. testing.allocator checks after deinit.
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0, .answer_finished = true });
    defer p.deinit();
    const t = try p.start(alloc);
    try pushFrame(alloc, p.a, plainCallFrame(alloc, 10, t.ea));
    try pushFrame(alloc, p.a, plainCallFrame(alloc, 11, t.ea));
    try pushFrame(alloc, p.a, finishFrame(alloc, 11));
    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), p.ra.countFinished(11));
    try pushFrame(alloc, p.a, plainCallFrame(alloc, 12, t.ea));
    try pushFrame(alloc, p.a, finishFrame(alloc, 12));
    try testing.expectEqual(@as(usize, 4), p.a.pending_answers.count());
}

/// The H13 lifecycle on one opted-in Conn whose every allocation goes
/// through `failing`, with the allocations of one host call failing from
/// its `k`-th on (the failure ends with that host call): a call, the
/// remote's Finish, the host's answer `answer` (or, without an
/// ANSWER_FINISHED, an exception), retried until it is accepted.
/// Checks: the host sees the call at most once, at most one ANSWER_FINISHED
/// and only for a call it saw; the wire carries exactly one Return for the
/// call while the connection is up, and at most one after it failed.
/// Returns whether the failure was induced (the sweep's end condition).
const FinishedAnswer = enum { canceled, results, exception };

fn finishedLifecycleUnderOom(k: usize, answer: FinishedAnswer) !bool {
    const alloc = testing.allocator;
    var failing = std.testing.FailingAllocator.init(alloc, .{});
    const a = try Conn.init(failing.allocator(), .{ .now_ns = 0, .answer_finished = true });
    defer a.deinit();
    var ra = Rec.init(alloc);
    defer ra.deinit();
    const ea = try a.exportCap(200);
    const res = try msgU64(alloc, 1);
    defer alloc.free(res);

    const start = failing.alloc_index;
    failing.fail_index = start + k;
    const Step = enum { call, finish, drain, answer };
    var answered = false;
    for ([_]Step{ .call, .finish, .drain, .answer }) |step| {
        switch (step) {
            .call => pushFrame(alloc, a, plainCallFrame(alloc, 10, ea)) catch {},
            .finish => pushFrame(alloc, a, finishFrame(alloc, 10)) catch {},
            .drain => try drainAll(a, &ra),
            .answer => if (a.pending_answers.contains(10)) {
                const reported = ra.countFinished(10) == 1;
                if (!reported) try testing.expectError(error.Invalid, a.returnCanceled(10));
                // Retry after a failure: the failure ends with the host call.
                var tries: usize = 0;
                while (!answered) : (tries += 1) {
                    if (tries > 2) return error.TestAnswerNeverAccepted;
                    const r = if (reported) switch (answer) {
                        .canceled => a.returnCanceled(10),
                        .results => a.returnResults(10, res, &.{}),
                        .exception => a.returnException(10, 0, "stopped"),
                    } else a.returnException(10, 0, "stopped");
                    if (r) |_| answered = true else |err| {
                        try testing.expectEqual(error.OutOfMemory, err);
                        try testing.expect(a.pending_answers.contains(10));
                        failing.fail_index = std.math.maxInt(usize);
                    }
                }
            },
        }
        if (failing.has_induced_failure) failing.fail_index = std.math.maxInt(usize);
    }
    try drainAll(a, &ra);

    const calls = countCalls(&ra, 10);
    try testing.expect(calls <= 1);
    try testing.expect(ra.countFinished(10) <= calls);
    try testing.expect(!a.pending_answers.contains(10) or a.failed);
    const returns = try countReturnFrames(&ra, 10, null);
    if (a.failed) {
        try testing.expect(returns <= 1);
    } else {
        try testing.expectEqual(@as(usize, 1), returns);
        if (calls == 1) try testing.expect(answered);
    }
    // Freeing with whatever is left: testing.allocator finds no leak.
    return failing.has_induced_failure;
}

test "answer finished: an allocation failure anywhere in the lifecycle keeps one Return per call and leaks nothing" {
    for (std.enums.values(FinishedAnswer)) |answer| {
        var k: usize = 0;
        while (try finishedLifecycleUnderOom(k, answer)) : (k += 1) {
            if (k > 10_000) return error.TestSweepRunaway;
        }
        try testing.expect(k > 10);
    }
}

// ---------------------------------------------------------------------------
// Tail calls: a Return whose takeFromOtherQuestion names a call to us
// ---------------------------------------------------------------------------

/// The remote's question id of the tail call (A's answer id).
const tail_answer: u32 = 77;

/// A tail call as the C++ reference sends it: a Call on A's export
/// `target_export` whose sendResultsTo is `yourself`...
fn yourselfCallFrame(alloc: std.mem.Allocator, answer_id: u32, target_export: u32) ![]const u8 {
    var mb = protocol.MessageBuilder.init(alloc);
    defer mb.deinit();
    var call = try mb.beginCall(answer_id, iface, 4);
    try call.setTargetImportedCap(target_export);
    call.setSendResultsToYourself();
    var payload = try call.payloadTyped();
    _ = try (try payload.initContent()).initStruct(0, 0);
    return mb.finish();
}

/// ...then the Return of A's question `qid` that says "your results are that
/// call's results".
fn tailCallReturnFrame(alloc: std.mem.Allocator, qid: u32, answer_id: u32) ![]const u8 {
    var mb = protocol.MessageBuilder.init(alloc);
    defer mb.deinit();
    var ret = try mb.beginReturn(qid, .takeFromOtherQuestion);
    ret.setReleaseParamCaps(false);
    try ret.setTakeFromOtherQuestion(answer_id);
    return mb.finish();
}

fn finishFrame(alloc: std.mem.Allocator, qid: u32) ![]const u8 {
    return finishFrameWith(alloc, qid, false);
}

/// A Finish, with `requireEarlyCancellationWorkaround` as given (the
/// schema default, and what go-capnp sends, is true).
fn finishFrameWith(alloc: std.mem.Allocator, qid: u32, require_early_cancellation: bool) ![]const u8 {
    var mb = protocol.MessageBuilder.init(alloc);
    defer mb.deinit();
    try mb.buildFinish(qid, false, require_early_cancellation);
    return mb.finish();
}

fn pushFrame(alloc: std.mem.Allocator, c: *Conn, frame: anyerror![]const u8) !void {
    const bytes = try frame;
    defer alloc.free(bytes);
    try c.pushBytes(bytes);
}

/// Return OUT_FRAMEs that `rec` kept for `answer_id`: all of them, or only
/// those with `tag`.
fn countReturnFrames(rec: *Rec, answer_id: u32, tag: ?protocol.ReturnTag) !usize {
    var n: usize = 0;
    for (rec.frames.items) |bytes| {
        var d = try protocol.DecodedMessage.init(testing.allocator, bytes);
        defer d.deinit();
        if (d.tag != .@"return") continue;
        const ret = try d.asReturn();
        if (ret.answer_id != answer_id) continue;
        if (tag) |want| {
            if (ret.tag != want) continue;
        }
        n += 1;
    }
    return n;
}

/// `qid` got exactly one RETURN, of `kind`. On a mismatch, print what it got.
fn expectOneReturn(rec: *Rec, qid: u32, kind: effects.ReturnKind) !RecReturn {
    const n = rec.countReturns(qid);
    const r = rec.returnFor(qid) orelse {
        std.debug.print("question {d}: no RETURN, want one {s}\n", .{ qid, @tagName(kind) });
        return error.TestNoReturn;
    };
    if (n != 1 or r.kind != kind) {
        std.debug.print("question {d}: {d} RETURN(s), first {s} type {d} \"{s}\"; want one {s}\n", .{
            qid, n, @tagName(r.kind), r.exception_type, r.reason, @tagName(kind),
        });
        return error.TestWrongReturn;
    }
    return r;
}

/// A and B connected: A holds B's bootstrap `boot`, exports a cap the remote
/// will tail-call (`ea`, tag 200) and has asked B question `q`, which B holds.
/// From here on each test plays the remote itself: it pushes frames into A,
/// and A's OUT_FRAMEs stay in `ra.frames`.
const TailSetup = struct { boot: Cap, ea: u32, q: u32 };

fn startTailCall(alloc: std.mem.Allocator, a: *Conn, ra: *Rec, b: *Conn, rb: *Rec) !TailSetup {
    const boot = try connectPair(a, ra, b, rb);
    const ea = try a.exportCap(200);
    const p = try msgU64(alloc, 1);
    defer alloc.free(p);
    const q = try a.call(boot, iface, 0, p, &.{}, 0);
    try pump(a, ra, b, rb);
    try testing.expectEqual(@as(usize, 1), rb.calls.items.len);
    return .{ .boot = boot, .ea = ea, .q = q };
}

/// One more question from A to the remote, its Call frame kept in `ra`.
fn nextQuestion(alloc: std.mem.Allocator, a: *Conn, ra: *Rec, t: TailSetup) !u32 {
    const p = try msgU64(alloc, 2);
    defer alloc.free(p);
    const q = try a.call(t.boot, iface, 0, p, &.{}, 0);
    try drainAll(a, ra);
    return q;
}

/// The remote tail-calls A's export with question `answer_id` and answers
/// `q` with the redirect, in one push: A's host has not seen the call yet,
/// as when the C++ reference forwards a parked call back to us. The call
/// reaches the host before the redirect is read, so `q` is held: it has no
/// RETURN until the host answers the call (docs/notes/tail-call-hold-design.md).
fn pushPendingTailCall(alloc: std.mem.Allocator, a: *Conn, ra: *Rec, t: TailSetup, q: u32, answer_id: u32) !void {
    const call = try yourselfCallFrame(alloc, answer_id, t.ea);
    defer alloc.free(call);
    const ret = try tailCallReturnFrame(alloc, q, answer_id);
    defer alloc.free(ret);
    const both = try std.mem.concat(alloc, u8, &.{ call, ret });
    defer alloc.free(both);
    try a.pushBytes(both);
    try drainAll(a, ra);
    const c = ra.lastCall();
    try testing.expectEqual(answer_id, c.answer_id);
    try testing.expectEqual(t.ea, c.export_id);
    try testing.expectEqual(@as(u64, 200), c.host_tag);
    try expectNoReturnYet(ra, q);
}

/// `qid` has no RETURN yet. On a mismatch, print what it got.
fn expectNoReturnYet(rec: *Rec, qid: u32) !void {
    const r = rec.returnFor(qid) orelse return;
    std.debug.print("question {d}: RETURN {s} type {d} \"{s}\" before the tail call was answered\n", .{
        qid, @tagName(r.kind), r.exception_type, r.reason,
    });
    return error.TestEarlyReturn;
}

/// The redirect was not followed: one RETURN EXCEPTION (`unimplemented`),
/// as for every Return variant the shim does not map.
fn expectUnfollowed(rec: *Rec, qid: u32) !void {
    const r = try expectOneReturn(rec, qid, .exception);
    try testing.expectEqual(@backingInt(protocol.ExceptionType.unimplemented), r.exception_type);
    try testing.expectEqualStrings("capnp-swift core: unsupported Return variant", r.reason);
}

/// A Call on A's export `target_export` with `sendResultsTo = caller`.
fn plainCallFrame(alloc: std.mem.Allocator, answer_id: u32, target_export: u32) ![]const u8 {
    var mb = protocol.MessageBuilder.init(alloc);
    defer mb.deinit();
    var call = try mb.beginCall(answer_id, iface, 4);
    try call.setTargetImportedCap(target_export);
    var payload = try call.payloadTyped();
    _ = try (try payload.initContent()).initStruct(0, 0);
    return mb.finish();
}

/// Finish OUT_FRAMEs that `rec` kept for question `qid`.
fn countFinishFrames(rec: *Rec, qid: u32) !usize {
    var n: usize = 0;
    for (rec.frames.items) |bytes| {
        var d = try protocol.DecodedMessage.init(testing.allocator, bytes);
        defer d.deinit();
        if (d.tag != .finish) continue;
        if ((try d.asFinish()).question_id == qid) n += 1;
    }
    return n;
}

/// A sent exactly one Return for `answer_id`, and it is
/// `resultsSentElsewhere`: rpc.capnp answers a `yourself` call that way,
/// also when it fails, and the C++ reference drops the connection on an
/// exception Return for its tail call.
fn expectOnlyResultsSentElsewhere(rec: *Rec, answer_id: u32) !void {
    try testing.expectEqual(@as(usize, 0), try countReturnFrames(rec, answer_id, .exception));
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(rec, answer_id, null));
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(rec, answer_id, .resultsSentElsewhere));
}

/// Two connections and records, freed in reverse order.
const TailPair = struct {
    a: *Conn,
    b: *Conn,
    ra: Rec,
    rb: Rec,

    fn init(alloc: std.mem.Allocator, a_opts: conn_mod.Options) !TailPair {
        const a = try Conn.init(alloc, a_opts);
        errdefer a.deinit();
        const b = try Conn.init(alloc, .{ .now_ns = a_opts.now_ns });
        return .{ .a = a, .b = b, .ra = Rec.init(alloc), .rb = Rec.init(alloc) };
    }

    fn deinit(self: *TailPair) void {
        self.rb.deinit();
        self.ra.deinit();
        self.b.deinit();
        self.a.deinit();
    }

    fn start(self: *TailPair, alloc: std.mem.Allocator) !TailSetup {
        return startTailCall(alloc, self.a, &self.ra, self.b, &self.rb);
    }
};

test "tail call: a pending cap-free answer becomes the question's one RETURN" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    // The C++ order (resolve_disembargo): the call and the redirect come
    // before the host answers the call.
    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    const res = try msgU64(alloc, 42);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);

    const r = try expectOneReturn(&p.ra, t.q, .results);
    try testing.expectEqual(@as(u64, 42), try readU64(alloc, r.msg));
    try testing.expectEqual(@as(usize, 0), r.caps.len);
    try expectOnlyResultsSentElsewhere(&p.ra, tail_answer);
    // The question is an ordinary returned question now.
    try p.a.finish(t.q, false);
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), try countFinishFrames(&p.ra, t.q));
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(t.q));
}

test "tail call: a pending answer that fails gives the question one exception RETURN" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    try p.a.returnException(tail_answer, 1, "tail target failed");
    try drainAll(p.a, &p.ra);

    const r = try expectOneReturn(&p.ra, t.q, .exception);
    try testing.expectEqual(@as(u16, 1), r.exception_type);
    try testing.expectEqualStrings("tail target failed", r.reason);
    try expectOnlyResultsSentElsewhere(&p.ra, tail_answer);
    try p.a.finish(t.q, false);
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 0), p.a.peer.loopback_result_stash.count());
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(t.q));
}

test "tail call: an answer given before the redirect arrives is delivered at once" {
    // The Peer's kept-answer path, results and exception: a guard.
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    try pushFrame(alloc, p.a, yourselfCallFrame(alloc, tail_answer, t.ea));
    try drainAll(p.a, &p.ra);
    const res = try msgU64(alloc, 43);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);
    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, t.q, tail_answer));
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .results);
    try testing.expectEqual(@as(u64, 43), try readU64(alloc, r.msg));
    try expectOnlyResultsSentElsewhere(&p.ra, tail_answer);
    try p.a.finish(t.q, false);

    const q2 = try nextQuestion(alloc, p.a, &p.ra, t);
    try pushFrame(alloc, p.a, yourselfCallFrame(alloc, tail_answer + 1, t.ea));
    try drainAll(p.a, &p.ra);
    try p.a.returnException(tail_answer + 1, 0, "failed before the redirect");
    try drainAll(p.a, &p.ra);
    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, q2, tail_answer + 1));
    try drainAll(p.a, &p.ra);
    const r2 = try expectOneReturn(&p.ra, q2, .exception);
    try testing.expectEqual(@as(u16, 0), r2.exception_type);
    try testing.expectEqualStrings("failed before the redirect", r2.reason);
    try expectOnlyResultsSentElsewhere(&p.ra, tail_answer + 1);
    try p.a.finish(q2, false);
}

test "tail call: results with capabilities give one exception RETURN, without a leak" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);
    const with_cap = try msgCap(alloc, 9, 0);
    defer alloc.free(with_cap);
    const cap = [_]Cap{.{ .kind = .@"export", .id = t.ea }};

    // Pending: the question is held, then the answer carries a capability.
    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    try p.a.returnResults(tail_answer, with_cap, &cap);
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .exception);
    try testing.expectEqual(@backingInt(protocol.ExceptionType.unimplemented), r.exception_type);
    try testing.expectEqualStrings(conn_mod.tail_call_caps_reason, r.reason);
    try expectOnlyResultsSentElsewhere(&p.ra, tail_answer);
    try p.a.finish(t.q, false);

    // Answered before the redirect arrives: the Peer keeps no such results,
    // so the redirect is not followed.
    const q2 = try nextQuestion(alloc, p.a, &p.ra, t);
    try pushFrame(alloc, p.a, yourselfCallFrame(alloc, tail_answer + 1, t.ea));
    try drainAll(p.a, &p.ra);
    try p.a.returnResults(tail_answer + 1, with_cap, &cap);
    try drainAll(p.a, &p.ra);
    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, q2, tail_answer + 1));
    try drainAll(p.a, &p.ra);
    try expectUnfollowed(&p.ra, q2);
    try p.a.finish(q2, false);
    try drainAll(p.a, &p.ra);
    // Nobody received those results, so they hold no reference on the
    // export they named: no EXPORT_DROPPED fires, and testing.allocator
    // finds no leak after both deinits.
    try testing.expectEqual(@as(usize, 0), p.ra.dropped.items.len);
}

test "tail call: results whose capabilities are all null, or unused, are cap-free" {
    // The Peer encodes a NONE entry as a null pointer and ignores an entry
    // nothing points at, so such results carry no capability, whether the
    // host answers before the redirect or after.
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);
    const null_cap = try msgCap(alloc, 9, 0);
    defer alloc.free(null_cap);
    const plain = try msgU64(alloc, 10);
    defer alloc.free(plain);
    const none = [_]Cap{.{ .kind = .none }};
    const unused = [_]Cap{.{ .kind = .@"export", .id = t.ea }};

    const Case = struct { msg: []const u8, caps: []const Cap, want: u64 };
    const cases = [_]Case{
        .{ .msg = null_cap, .caps = &none, .want = 9 },
        .{ .msg = plain, .caps = &unused, .want = 10 },
    };
    var answer: u32 = tail_answer;
    var q = t.q;
    for (cases) |c| {
        for ([_]bool{ true, false }) |redirect_first| {
            if (redirect_first) {
                try pushPendingTailCall(alloc, p.a, &p.ra, t, q, answer);
                try p.a.returnResults(answer, c.msg, c.caps);
            } else {
                try pushFrame(alloc, p.a, yourselfCallFrame(alloc, answer, t.ea));
                try drainAll(p.a, &p.ra);
                try p.a.returnResults(answer, c.msg, c.caps);
                try drainAll(p.a, &p.ra);
                try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, q, answer));
            }
            try drainAll(p.a, &p.ra);
            const r = try expectOneReturn(&p.ra, q, .results);
            try testing.expectEqual(c.want, try readU64(alloc, r.msg));
            try testing.expectEqual(@as(usize, 0), r.caps.len);
            var m = try message.Message.init(alloc, r.msg, .{});
            defer m.deinit();
            const root = try m.getRootStruct();
            if (c.msg.ptr == null_cap.ptr) try testing.expect(root.isPointerNull(0));
            try p.a.finish(q, false);
            answer += 1;
            q = try nextQuestion(alloc, p.a, &p.ra, t);
        }
    }
}

test "tail call: a later tail call that reuses the id gets its own answer, never the old one" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    const old = try msgU64(alloc, 42);
    defer alloc.free(old);
    try p.a.returnResults(tail_answer, old, &.{});
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .results);
    try testing.expectEqual(@as(u64, 42), try readU64(alloc, r.msg));
    try p.a.finish(t.q, false);
    // The remote finishes the call after our Finish of the question.
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 0), p.a.peer.loopback_result_stash.count());

    // The remote reuses the id for its next tail call (the C++ reference
    // reuses the lowest free id). The new question waits for the new call
    // and gets its 99, never the old call's 42.
    const q2 = try nextQuestion(alloc, p.a, &p.ra, t);
    try pushPendingTailCall(alloc, p.a, &p.ra, t, q2, tail_answer);
    const res = try msgU64(alloc, 99);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);
    const r2 = try expectOneReturn(&p.ra, q2, .results);
    try testing.expectEqual(@as(u64, 99), try readU64(alloc, r2.msg));
    try p.a.finish(q2, false);
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 0), p.a.peer.loopback_result_stash.count());
}

test "tail call: a capnp_return_results that fails, then capnp_return_exception, gives the question that exception" {
    // A host whose capnp_return_results fails (its results do not build)
    // answers with capnp_return_exception, in either order with the
    // redirect. The remote gets resultsSentElsewhere, once.
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();

    // Not a Cap'n Proto message: the results build fails.
    const bad_results = [_]u8{ 1, 2, 3 };

    // The C++ order: the redirect comes first. A refused answer keeps the
    // question held.
    const t = try p.start(alloc);
    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    if (p.a.returnResults(tail_answer, &bad_results, &.{})) |_| return error.TestResultsBuilt else |_| {}
    try drainAll(p.a, &p.ra);
    try expectNoReturnYet(&p.ra, t.q);
    try p.a.returnException(tail_answer, 0, "results did not build");
    try drainAll(p.a, &p.ra);
    try expectOnlyResultsSentElsewhere(&p.ra, tail_answer);
    const r = try expectOneReturn(&p.ra, t.q, .exception);
    try testing.expectEqualStrings("results did not build", r.reason);
    try p.a.finish(t.q, false);
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    try drainAll(p.a, &p.ra);

    // Answered before the redirect arrives: the redirect gets the exception.
    const q2 = try nextQuestion(alloc, p.a, &p.ra, t);
    try pushFrame(alloc, p.a, yourselfCallFrame(alloc, tail_answer + 1, t.ea));
    try drainAll(p.a, &p.ra);
    if (p.a.returnResults(tail_answer + 1, &bad_results, &.{})) |_| return error.TestResultsBuilt else |_| {}
    try p.a.returnException(tail_answer + 1, 0, "results did not build");
    try drainAll(p.a, &p.ra);
    try expectOnlyResultsSentElsewhere(&p.ra, tail_answer + 1);
    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, q2, tail_answer + 1));
    try drainAll(p.a, &p.ra);
    const r2 = try expectOneReturn(&p.ra, q2, .exception);
    try testing.expectEqualStrings("results did not build", r2.reason);
    try p.a.finish(q2, false);
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer + 1));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 0), p.a.peer.loopback_result_stash.count());
}

test "tail call: a call to a rejected promise export answers resultsSentElsewhere; the redirect gets the rejection" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    // The call parks on the promise; the host rejects the promise; then
    // the redirect names the call.
    const pe = try p.a.promiseExport();
    try pushFrame(alloc, p.a, yourselfCallFrame(alloc, tail_answer, pe));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 0), try countReturnFrames(&p.ra, tail_answer, null));
    try p.a.rejectPromise(pe, "rejected by host");
    try drainAll(p.a, &p.ra);
    try expectOnlyResultsSentElsewhere(&p.ra, tail_answer);

    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, t.q, tail_answer));
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .exception);
    try testing.expectEqualStrings("promise broken", r.reason);
    try p.a.finish(t.q, false);
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 0), p.a.peer.loopback_result_stash.count());
}

test "tail call: no hold for a redirect that names no call the host owes" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    // An id that names no call.
    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, t.q, 999));
    try drainAll(p.a, &p.ra);
    try expectUnfollowed(&p.ra, t.q);
    try p.a.finish(t.q, false);

    // A call the host owes, but with `sendResultsTo = caller`.
    const q2 = try nextQuestion(alloc, p.a, &p.ra, t);
    try pushFrame(alloc, p.a, plainCallFrame(alloc, tail_answer, t.ea));
    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, q2, tail_answer));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(tail_answer, p.ra.lastCall().answer_id);
    try expectUnfollowed(&p.ra, q2);
    const res = try msgU64(alloc, 5);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(q2));
    try p.a.finish(q2, false);

    // A `yourself` call the Peer still holds: parked on a promise export.
    const q3 = try nextQuestion(alloc, p.a, &p.ra, t);
    const pe = try p.a.promiseExport();
    try pushFrame(alloc, p.a, yourselfCallFrame(alloc, tail_answer + 1, pe));
    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, q3, tail_answer + 1));
    try drainAll(p.a, &p.ra);
    try expectUnfollowed(&p.ra, q3);
    try p.a.finish(q3, false);

    // The bootstrap question is not a `capnp_call` question.
    const qb = try p.a.bootstrap();
    try drainAll(p.a, &p.ra);
    try pushFrame(alloc, p.a, yourselfCallFrame(alloc, tail_answer + 2, t.ea));
    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, qb, tail_answer + 2));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(tail_answer + 2, p.ra.lastCall().answer_id);
    try expectUnfollowed(&p.ra, qb);
    try p.a.returnResults(tail_answer + 2, res, &.{});
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(qb));
}

test "tail call: a second redirect that names a held call is not followed; the first still gets the answer" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    const q2 = try nextQuestion(alloc, p.a, &p.ra, t);
    try pushFrame(alloc, p.a, tailCallReturnFrame(alloc, q2, tail_answer));
    try drainAll(p.a, &p.ra);
    try expectUnfollowed(&p.ra, q2);
    try expectNoReturnYet(&p.ra, t.q);

    const res = try msgU64(alloc, 7);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .results);
    try testing.expectEqual(@as(u64, 7), try readU64(alloc, r.msg));
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(q2));
}

test "tail call: capnp_cancel ends a held question once and sends its Finish" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    try p.a.cancel(t.q);
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .canceled);
    try testing.expectEqual(@backingInt(protocol.ExceptionType.failed), r.exception_type);
    try testing.expectEqualStrings(conn_mod.cancel_reason, r.reason);
    try testing.expectEqual(@as(usize, 1), try countFinishFrames(&p.ra, t.q));
    // The question is gone, as after any cancel.
    try testing.expectError(error.BadId, p.a.cancel(t.q));
    try testing.expectError(error.UnknownRetainedQuestion, p.a.finish(t.q, false));

    // The host still answers the call; the question gets nothing more.
    const res = try msgU64(alloc, 1);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(t.q));
    try expectOnlyResultsSentElsewhere(&p.ra, tail_answer);
}

test "tail call: a cancel with no memory at all still ends a held question once" {
    // As the Peer's own cancel: the question ends even when its Finish
    // cannot be sent, and its id is gone.
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
    const t = try startTailCall(alloc, a, &ra, b, &rb);

    try pushPendingTailCall(alloc, a, &ra, t, t.q, tail_answer);
    failing.fail_index = failing.alloc_index;
    try a.cancel(t.q);
    try testing.expectError(error.UnknownRetainedQuestion, a.finish(t.q, false));
    failing.fail_index = std.math.maxInt(usize);
    try drainAll(a, &ra);
    const r = try expectOneReturn(&ra, t.q, .canceled);
    try testing.expectEqualStrings(conn_mod.cancel_reason, r.reason);
    try testing.expectEqual(@as(usize, 0), try countFinishFrames(&ra, t.q));
    try testing.expectError(error.BadId, a.cancel(t.q));
}

test "tail call: a deadline ends a held question once and sends its Finish" {
    const alloc = testing.allocator;
    const ms = std.time.ns_per_ms;
    var p = try TailPair.init(alloc, .{ .now_ns = 0, .timeouts = .{ .default_call_timeout_ms = 100 } });
    defer p.deinit();
    _ = p.a.tick(1 * ms);
    const t = try p.start(alloc);

    // The default deadline, set when the question was sent.
    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    try testing.expectEqual(@as(usize, 0), p.a.tick(50 * ms));
    try drainAll(p.a, &p.ra);
    try expectNoReturnYet(&p.ra, t.q);
    try testing.expectEqual(@as(usize, 1), p.a.tick(200 * ms));
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .exception);
    try testing.expectEqual(@backingInt(protocol.ExceptionType.overloaded), r.exception_type);
    try testing.expectEqualStrings(capnp.rpc.peer.deadline_reason, r.reason);
    try testing.expectEqual(@as(usize, 1), try countFinishFrames(&p.ra, t.q));
    try testing.expectError(error.UnknownRetainedQuestion, p.a.finish(t.q, false));
    const res = try msgU64(alloc, 1);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(t.q));

    // A deadline set while the question is held.
    const q2 = try nextQuestion(alloc, p.a, &p.ra, t);
    try pushPendingTailCall(alloc, p.a, &p.ra, t, q2, tail_answer + 1);
    try p.a.setDeadline(q2, 10);
    try testing.expectEqual(@as(usize, 0), p.a.tick(205 * ms));
    try testing.expectEqual(@as(usize, 1), p.a.tick(211 * ms));
    try drainAll(p.a, &p.ra);
    const r2 = try expectOneReturn(&p.ra, q2, .exception);
    try testing.expectEqualStrings(capnp.rpc.peer.deadline_reason, r2.reason);
    try testing.expectEqual(@as(usize, 0), p.a.tick(300 * ms));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(q2));
}

test "tail call: a deadline with no memory at all still ends a held question on that tick" {
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
    const t = try startTailCall(alloc, a, &ra, b, &rb);

    try pushPendingTailCall(alloc, a, &ra, t, t.q, tail_answer);
    try a.setDeadline(t.q, 10);
    failing.fail_index = failing.alloc_index;
    try testing.expectEqual(@as(usize, 1), a.tick(20 * ms));
    try testing.expectError(error.UnknownRetainedQuestion, a.finish(t.q, false));
    failing.fail_index = std.math.maxInt(usize);
    try drainAll(a, &ra);
    const r = try expectOneReturn(&ra, t.q, .exception);
    try testing.expectEqualStrings(capnp.rpc.peer.deadline_reason, r.reason);
    try testing.expectEqual(@as(usize, 0), try countFinishFrames(&ra, t.q));
    try testing.expectEqual(@as(usize, 0), a.tick(30 * ms));
}

test "tail call: a held question's deadline emits one timeout event" {
    const alloc = testing.allocator;
    const ms = std.time.ns_per_ms;
    var p = try TailPair.init(alloc, .{ .now_ns = 0, .observer = true });
    defer p.deinit();
    const t = try p.start(alloc);
    const timeout_tag: u8 = @intCast(@backingInt(std.meta.Tag(capnp.rpc.events.Event).timeout));

    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    try p.a.setDeadline(t.q, 10);
    try testing.expectEqual(@as(usize, 1), p.a.tick(20 * ms));
    try drainAll(p.a, &p.ra);
    var timeouts: usize = 0;
    for (p.ra.events.items) |e| {
        if (e.tag == timeout_tag) timeouts += 1;
    }
    try testing.expectEqual(@as(usize, 1), timeouts);
}

test "tail call: capnp_finish of a held question is busy until its RETURN" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    try testing.expectError(error.Busy, p.a.finish(t.q, false));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 0), try countFinishFrames(&p.ra, t.q));
    const res = try msgU64(alloc, 3);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);
    _ = try expectOneReturn(&p.ra, t.q, .results);
    try p.a.finish(t.q, false);
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), try countFinishFrames(&p.ra, t.q));
}

test "tail call: transportClosed ends a held question with one RETURN DISCONNECTED" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    p.a.transportClosed();
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .disconnected);
    try testing.expectEqualStrings(capnp.rpc.peer.disconnected_reason, r.reason);
    const res = try msgU64(alloc, 1);
    defer alloc.free(res);
    p.a.returnResults(tail_answer, res, &.{}) catch {};
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(t.q));
}

test "tail call: freeing the connection with a held question leaks nothing" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);
    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    const q2 = try nextQuestion(alloc, p.a, &p.ra, t);
    try pushPendingTailCall(alloc, p.a, &p.ra, t, q2, tail_answer + 1);
    // `p.deinit` frees both held questions; testing.allocator checks.
}

test "tail call: the remote's Finish of the named call keeps the hold; the host's answer still reaches the question" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    try drainAll(p.a, &p.ra);
    try expectNoReturnYet(&p.ra, t.q);
    const res = try msgU64(alloc, 11);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .results);
    try testing.expectEqual(@as(u64, 11), try readU64(alloc, r.msg));
    try testing.expectEqual(@as(usize, 1), try countReturnFrames(&p.ra, tail_answer, null));
}

test "tail call: a new call that reuses a held call's id closes the connection; the held question ends once" {
    // The remote finishes the held call, then reuses its id before the host
    // answered. The Peer allows that only when its finished-early records
    // are full (here: 2 of 2, held by calls queued on a promise export).
    // The core never hands the host a second call with an id it owes: the
    // connection closes, and the held question ends once, at transport
    // close.
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0, .limits = .{ .max_active_inbound_questions = 2 } });
    defer p.deinit();
    const t = try p.start(alloc);

    try fillFinishedRecords(alloc, p.a, &.{ 20, 21 });
    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    try pushFrame(alloc, p.a, finishFrame(alloc, tail_answer));
    try drainAll(p.a, &p.ra);
    try expectNoReturnYet(&p.ra, t.q);

    try testing.expectError(error.Protocol, pushFrame(alloc, p.a, yourselfCallFrame(alloc, tail_answer, t.ea)));
    try testing.expectEqual(@as(?anyerror, error.DuplicateQuestionId), p.a.last_error);
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), countCalls(&p.ra, tail_answer));
    try testing.expectEqual(@as(usize, 1), p.ra.close_requested);
    try expectNoReturnYet(&p.ra, t.q);

    p.a.transportClosed();
    try drainAll(p.a, &p.ra);
    _ = try expectOneReturn(&p.ra, t.q, .disconnected);
    const res = try msgU64(alloc, 1);
    defer alloc.free(res);
    p.a.returnResults(tail_answer, res, &.{}) catch {};
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 1), p.ra.countReturns(t.q));
}

test "tail call: a graceful shutdown waits for a held question" {
    const alloc = testing.allocator;
    var p = try TailPair.init(alloc, .{ .now_ns = 0 });
    defer p.deinit();
    const t = try p.start(alloc);

    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    p.a.shutdown();
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 0), p.ra.close_requested);
    try expectNoReturnYet(&p.ra, t.q);

    const res = try msgU64(alloc, 8);
    defer alloc.free(res);
    try p.a.returnResults(tail_answer, res, &.{});
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .results);
    try testing.expectEqual(@as(u64, 8), try readU64(alloc, r.msg));
    try testing.expectEqual(@as(usize, 1), p.ra.close_requested);
}

test "tail call: the shutdown drain bound ends a held question" {
    const alloc = testing.allocator;
    const ms = std.time.ns_per_ms;
    var p = try TailPair.init(alloc, .{ .now_ns = 0, .timeouts = .{ .shutdown_drain_timeout_ms = 100 } });
    defer p.deinit();
    const t = try p.start(alloc);

    try pushPendingTailCall(alloc, p.a, &p.ra, t, t.q, tail_answer);
    p.a.shutdown();
    try testing.expectEqual(@as(usize, 0), p.a.tick(50 * ms));
    try drainAll(p.a, &p.ra);
    try testing.expectEqual(@as(usize, 0), p.ra.close_requested);
    try expectNoReturnYet(&p.ra, t.q);
    try testing.expectEqual(@as(usize, 1), p.a.tick(150 * ms));
    try drainAll(p.a, &p.ra);
    const r = try expectOneReturn(&p.ra, t.q, .disconnected);
    try testing.expectEqual(@backingInt(protocol.ExceptionType.disconnected), r.exception_type);
    try testing.expectEqualStrings(capnp.rpc.peer.shutdown_reason, r.reason);
    try testing.expectEqual(@as(usize, 1), p.ra.close_requested);
    // Ended by the core, as the Peer's drain ends its own questions.
    try testing.expectEqual(@as(usize, 1), try countFinishFrames(&p.ra, t.q));
    try testing.expectError(error.UnknownRetainedQuestion, p.a.finish(t.q, false));
}

test "tail call: one allocation failure anywhere in the answer to a held call gives the question one RETURN, no leak" {
    // For every allocation of capnp_return_results (then of
    // capnp_return_exception): fail it. A refused answer is answered again
    // with capnp_return_exception. The question gets one RETURN; after
    // capnp_return_results the remote's call gets only resultsSentElsewhere.
    const alloc = testing.allocator;
    for ([_]bool{ true, false }) |with_results| {
        var k: usize = 0;
        while (true) : (k += 1) {
            var failing = std.testing.FailingAllocator.init(testing.allocator, .{});
            const a = try Conn.init(failing.allocator(), .{ .now_ns = 0 });
            defer a.deinit();
            const b = try Conn.init(alloc, .{ .now_ns = 0 });
            defer b.deinit();
            var ra = Rec.init(alloc);
            defer ra.deinit();
            var rb = Rec.init(alloc);
            defer rb.deinit();
            const t = try startTailCall(alloc, a, &ra, b, &rb);
            try pushPendingTailCall(alloc, a, &ra, t, t.q, tail_answer);

            const res = try msgU64(alloc, 42);
            defer alloc.free(res);
            failing.fail_index = failing.alloc_index + k;
            const answered = if (with_results)
                a.returnResults(tail_answer, res, &.{})
            else
                a.returnException(tail_answer, 0, "host failure");
            failing.fail_index = std.math.maxInt(usize);
            const induced = failing.has_induced_failure;
            if (answered) |_| {} else |_| try a.returnException(tail_answer, 0, "fallback");
            try drainAll(a, &ra);

            const first = ra.returnFor(t.q) orelse return error.TestNoReturn;
            const r = try expectOneReturn(&ra, t.q, first.kind);
            switch (r.kind) {
                .results => try testing.expectEqual(@as(u64, 42), try readU64(alloc, r.msg)),
                .exception => {},
                else => return error.TestWrongReturn,
            }
            // A capnp_return_exception that fails ends the call's `yourself`
            // mark anyway (`Peer.sendReturnExceptionNoDrain`), so its retry
            // puts an exception Return on the wire. That is the Peer's rule,
            // not the hold's; only the results path is checked here.
            if (with_results) try expectOnlyResultsSentElsewhere(&ra, tail_answer);
            if (!induced) break;
        }
    }
}
