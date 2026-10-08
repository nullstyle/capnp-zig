//! `zig build test-abi`: the C ABI, exercised the way a C or Swift host uses
//! it. This file never imports `abi.zig`: it calls the `capnp_*` functions
//! through the translated header (`capnp_core_h`), linked from the host build
//! of `libcapnp_core.a` (`apple_root.zig`'s root, the C allocator), so a
//! prototype, layout or linkage mistake fails here, not in an app.
//!
//! capnp-zig is imported only to build and read standalone test messages.

const std = @import("std");
const c = @import("capnp_core_h");
const capnp = @import("capnpc-zig");

const message = capnp.message;
const protocol = capnp.rpc.wire.protocol;
const testing = std.testing;

const iface: u64 = 0x5e1f_0000_0000_0001;

// ---------------------------------------------------------------------------
// Recording host
// ---------------------------------------------------------------------------

const Ret = struct {
    qid: u32,
    kind: u8,
    exception_type: u16,
    msg: []u8,
    caps: []c.capnp_cap,
    reason: []u8,
};

const Call = struct {
    answer_id: u32,
    export_id: u32,
    host_tag: u64,
    interface_id: u64,
    method_id: u16,
    msg: []u8,
    caps: []c.capnp_cap,
};

const Dropped = struct { export_id: u32, host_tag: u64 };

const Side = struct {
    a: std.mem.Allocator,
    conn: *c.capnp_conn,
    returns: std.ArrayList(Ret) = .empty,
    calls: std.ArrayList(Call) = .empty,
    dropped: std.ArrayList(Dropped) = .empty,
    frames: std.ArrayList([]u8) = .empty,
    events: usize = 0,
    close_requested: usize = 0,

    fn init(a: std.mem.Allocator, conn: *c.capnp_conn) Side {
        return .{ .a = a, .conn = conn };
    }

    fn deinit(self: *Side) void {
        for (self.returns.items) |r| {
            self.a.free(r.msg);
            self.a.free(r.caps);
            self.a.free(r.reason);
        }
        for (self.calls.items) |ic| {
            self.a.free(ic.msg);
            self.a.free(ic.caps);
        }
        for (self.frames.items) |f| self.a.free(f);
        self.returns.deinit(self.a);
        self.calls.deinit(self.a);
        self.dropped.deinit(self.a);
        self.frames.deinit(self.a);
        c.capnp_conn_free(self.conn);
    }

    fn returnFor(self: *Side, qid: u32) ?Ret {
        for (self.returns.items) |r| if (r.qid == qid) return r;
        return null;
    }

    fn countReturns(self: *Side, qid: u32) usize {
        var n: usize = 0;
        for (self.returns.items) |r| {
            if (r.qid == qid) n += 1;
        }
        return n;
    }

    fn lastCall(self: *Side) Call {
        return self.calls.items[self.calls.items.len - 1];
    }
};

fn defaultOpts() c.capnp_conn_opts {
    var opts = std.mem.zeroes(c.capnp_conn_opts);
    opts.struct_size = @sizeOf(c.capnp_conn_opts);
    return opts;
}

fn newConn(opts: *const c.capnp_conn_opts, now_ns: i64) !*c.capnp_conn {
    var out: ?*c.capnp_conn = null;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_conn_new(opts, now_ns, &out));
    return out orelse error.TestNullConn;
}

fn newSide(a: std.mem.Allocator) !Side {
    const opts = defaultOpts();
    return Side.init(a, try newConn(&opts, 0));
}

fn freshEffect() c.capnp_effect {
    var e = std.mem.zeroes(c.capnp_effect);
    e.struct_size = @sizeOf(c.capnp_effect);
    return e;
}

fn dupe(comptime T: type, a: std.mem.Allocator, ptr: [*c]const T, len: usize) ![]T {
    if (len == 0) return try a.alloc(T, 0);
    return try a.dupe(T, ptr[0..len]);
}

/// Pull one effect from `src`; push OUT_FRAMEs into `dst` (or keep them when
/// `dst` is null); record everything else as deep copies. False when empty.
fn drainOne(src: *Side, dst: ?*Side) !bool {
    var e = freshEffect();
    const rc = c.capnp_conn_next_effect(src.conn, &e);
    if (rc == 0) return false;
    try testing.expectEqual(@as(i32, 1), rc);
    try testing.expectEqual(@as(u32, @sizeOf(c.capnp_effect)), e.struct_size);
    defer c.capnp_conn_commit_effect(src.conn);
    switch (e.kind) {
        c.CAPNP_EFFECT_OUT_FRAME => {
            try testing.expect(e.msg_len > 0);
            if (dst) |d| {
                const rc2 = c.capnp_conn_push_bytes(d.conn, e.msg, e.msg_len);
                if (rc2 != c.CAPNP_OK) return error.TestPushFailed;
            } else {
                try src.frames.append(src.a, try dupe(u8, src.a, e.msg, e.msg_len));
            }
        },
        c.CAPNP_EFFECT_CLOSE_REQUESTED => src.close_requested += 1,
        c.CAPNP_EFFECT_RETURN => try src.returns.append(src.a, .{
            .qid = e.id,
            .kind = e.return_kind,
            .exception_type = e.exception_type,
            .msg = try dupe(u8, src.a, e.msg, e.msg_len),
            .caps = try dupe(c.capnp_cap, src.a, e.caps, e.ncaps),
            .reason = try dupe(u8, src.a, @ptrCast(e.reason), e.reason_len),
        }),
        c.CAPNP_EFFECT_INBOUND_CALL => try src.calls.append(src.a, .{
            .answer_id = e.id,
            .export_id = e.export_id,
            .host_tag = e.host_tag,
            .interface_id = e.interface_id,
            .method_id = e.method_id,
            .msg = try dupe(u8, src.a, e.msg, e.msg_len),
            .caps = try dupe(c.capnp_cap, src.a, e.caps, e.ncaps),
        }),
        c.CAPNP_EFFECT_EXPORT_DROPPED => try src.dropped.append(src.a, .{ .export_id = e.export_id, .host_tag = e.host_tag }),
        c.CAPNP_EFFECT_EVENT => src.events += 1,
        else => return error.TestUnknownEffectKind,
    }
    return true;
}

/// Move frames both ways until both queues are empty.
fn pump(a: *Side, b: *Side) !void {
    var rounds: usize = 0;
    while (true) : (rounds += 1) {
        if (rounds > 100_000) return error.PumpRunaway;
        const pa = try drainOne(a, b);
        const pb = try drainOne(b, a);
        if (!pa and !pb) return;
    }
}

fn drainAll(s: *Side) !void {
    while (try drainOne(s, null)) {}
}

// ---------------------------------------------------------------------------
// Standalone host messages (plan D5)
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

/// The caps[] entry the root struct's pointer 0 names.
fn rootCap(a: std.mem.Allocator, bytes: []const u8, caps: []const c.capnp_cap) !c.capnp_cap {
    var m = try message.Message.init(a, bytes, .{});
    defer m.deinit();
    const idx = (try (try m.getRootStruct()).readCapability(0)).id;
    if (idx >= caps.len) return error.TestCapIndexOutOfRange;
    return caps[idx];
}

/// The caps[] entry a bootstrap RETURN's root capability pointer names.
fn bootstrapCap(a: std.mem.Allocator, r: Ret) !c.capnp_cap {
    var m = try message.Message.init(a, r.msg, .{});
    defer m.deinit();
    const idx = (try (try m.getRootAnyPointer()).getCapability()).id;
    if (idx >= r.caps.len) return error.TestCapIndexOutOfRange;
    return r.caps[idx];
}

fn cap(kind: u8, id: u32) c.capnp_cap {
    return .{ .kind = kind, .id = id };
}

/// A and B connected; B serves bootstrap `tag`; returns A's import of it.
fn connectPair(a: *Side, b: *Side, tag: u64) !c.capnp_cap {
    var eb: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_set_bootstrap(b.conn, tag, &eb));
    var q0: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_bootstrap(a.conn, &q0));
    try pump(a, b);
    const r0 = a.returnFor(q0) orelse return error.TestNoBootstrapReturn;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_RESULTS), r0.kind);
    const ib = try bootstrapCap(a.a, r0);
    try testing.expectEqual(@as(u8, c.CAPNP_CAP_IMPORT), ib.kind);
    try testing.expectEqual(eb, ib.id);
    return ib;
}

fn call(s: *Side, target: c.capnp_cap, method: u16, msg: []const u8, caps: []const c.capnp_cap) !u32 {
    var qid: u32 = 0;
    const rc = c.capnp_call(s.conn, target, iface, method, msg.ptr, msg.len, if (caps.len == 0) null else caps.ptr, caps.len, 0, &qid);
    if (rc != c.CAPNP_OK) {
        std.debug.print("capnp_call failed: {d}\n", .{rc});
        return error.TestCallFailed;
    }
    return qid;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "conn_new: struct_size versioning and bad arguments" {
    const a = testing.allocator;
    var out: ?*c.capnp_conn = null;
    var opts = defaultOpts();

    // NULL out / NULL opts / a struct_size below the first field.
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_conn_new(&opts, 0, null));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_conn_new(null, 0, &out));
    try testing.expect(out == null);
    opts.struct_size = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_conn_new(&opts, 0, &out));
    try testing.expect(out == null);

    // An unknown framing.
    opts = defaultOpts();
    opts.framing = 7;
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_conn_new(&opts, 0, &out));
    try testing.expect(out == null);

    // An older host whose struct ends after `observer` (8 bytes): the core
    // reads only that prefix and defaults the rest.
    opts = defaultOpts();
    opts.struct_size = 8;
    opts.max_outbound_questions = 0xFFFF_FFFF; // beyond the prefix: ignored
    var side = Side.init(a, try newConn(&opts, 0));
    defer side.deinit();
    var other = try newSide(a);
    defer other.deinit();
    _ = try connectPair(&side, &other, 1);

    // Freeing NULL is a no-op.
    c.capnp_conn_free(null);
}

test "bootstrap: the RETURN's root names an IMPORT of the remote bootstrap export" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    const ib = try connectPair(&sa, &sb, 100);
    try testing.expectEqual(@as(u8, c.CAPNP_CAP_IMPORT), ib.kind);
    try testing.expectEqual(@as(usize, 1), sa.returns.items.len);
    // Releasing the bootstrap import never drops the bootstrap export.
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_release(sa.conn, ib.id, 1));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 0), sb.dropped.items.len);
    try testing.expectEqual(@as(usize, 0), sa.close_requested + sb.close_requested);
}

test "call: params reach the export as INBOUND_CALL; return_results comes back as RETURN RESULTS" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    const ib = try connectPair(&sa, &sb, 100);

    const params = try msgU64(a, 41);
    defer a.free(params);
    const q = try call(&sa, ib, 3, params, &.{});
    try pump(&sa, &sb);

    try testing.expectEqual(@as(usize, 1), sb.calls.items.len);
    const ic = sb.calls.items[0];
    try testing.expectEqual(@as(u64, 100), ic.host_tag);
    try testing.expectEqual(iface, ic.interface_id);
    try testing.expectEqual(@as(u16, 3), ic.method_id);
    try testing.expectEqual(@as(u64, 41), try readU64(a, ic.msg));
    try testing.expectEqual(@as(usize, 0), ic.caps.len);

    const results = try msgU64(a, 42);
    defer a.free(results);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, ic.answer_id, results.ptr, results.len, null, 0));
    try pump(&sa, &sb);
    const r = sa.returnFor(q) orelse return error.TestNoReturn;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_RESULTS), r.kind);
    try testing.expectEqual(@as(u64, 42), try readU64(a, r.msg));
    try testing.expectEqual(@as(usize, 1), sa.countReturns(q));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q, 0));
    // The answer is closed: a second reply is a stale id.
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_return_results(sb.conn, ic.answer_id, results.ptr, results.len, null, 0));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 0), sa.close_requested + sb.close_requested);
}

test "caps both ways: an export in params arrives as an import, calls back, and releases drop it" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    const ib = try connectPair(&sa, &sb, 100);

    // A exports tag 200 and passes it in the params at caps[1] (caps[0] null).
    var ea: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_export(sa.conn, 200, &ea));
    const p1 = try msgCap(a, 11, 1);
    defer a.free(p1);
    const q1 = try call(&sa, ib, 0, p1, &.{ cap(c.CAPNP_CAP_NONE, 0), cap(c.CAPNP_CAP_EXPORT, ea) });
    try pump(&sa, &sb);

    // B's host sees an IMPORT of it.
    const c1 = sb.lastCall();
    try testing.expectEqual(@as(u64, 11), try readU64(a, c1.msg));
    const ia = try rootCap(a, c1.msg, c1.caps);
    try testing.expectEqual(@as(u8, c.CAPNP_CAP_IMPORT), ia.kind);
    try testing.expectEqual(ea, ia.id);

    // B calls it back, passing it back too: A sees its own EXPORT.
    const p2 = try msgCap(a, 22, 0);
    defer a.free(p2);
    const q2 = try call(&sb, ia, 1, p2, &.{ia});
    try pump(&sa, &sb);
    const c2 = sa.lastCall();
    try testing.expectEqual(ea, c2.export_id);
    try testing.expectEqual(@as(u64, 200), c2.host_tag);
    try testing.expectEqual(@as(u16, 1), c2.method_id);
    const back = try rootCap(a, c2.msg, c2.caps);
    try testing.expectEqual(@as(u8, c.CAPNP_CAP_EXPORT), back.kind);
    try testing.expectEqual(ea, back.id);

    // A answers B; B answers A with a new export (tag 300) in the results.
    const r42 = try msgU64(a, 42);
    defer a.free(r42);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sa.conn, c2.answer_id, r42.ptr, r42.len, null, 0));
    var eb2: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_export(sb.conn, 300, &eb2));
    const r1 = try msgCap(a, 33, 0);
    defer a.free(r1);
    const r1caps = [_]c.capnp_cap{cap(c.CAPNP_CAP_EXPORT, eb2)};
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, c1.answer_id, r1.ptr, r1.len, &r1caps, r1caps.len));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(u64, 42), try readU64(a, (sb.returnFor(q2) orelse return error.TestNoReturn).msg));
    const ret1 = sa.returnFor(q1) orelse return error.TestNoReturn;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_RESULTS), ret1.kind);
    const ib2 = try rootCap(a, ret1.msg, ret1.caps);
    try testing.expectEqual(@as(u8, c.CAPNP_CAP_IMPORT), ib2.kind);
    try testing.expectEqual(eb2, ib2.id);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sb.conn, q2, 0));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q1, 0));

    // A calls the new import: B's host sees export eb2 / tag 300.
    const p3 = try msgU64(a, 5);
    defer a.free(p3);
    const q3 = try call(&sa, ib2, 2, p3, &.{});
    try pump(&sa, &sb);
    const c3 = sb.lastCall();
    try testing.expectEqual(eb2, c3.export_id);
    try testing.expectEqual(@as(u64, 300), c3.host_tag);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, c3.answer_id, p3.ptr, p3.len, null, 0));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q3, 0));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 0), sa.dropped.items.len + sb.dropped.items.len);

    // Releases: B drops A's cap -> EXPORT_DROPPED on A; A drops B's -> on B.
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_release(sb.conn, ia.id, 1));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 1), sa.dropped.items.len);
    try testing.expectEqual(ea, sa.dropped.items[0].export_id);
    try testing.expectEqual(@as(u64, 200), sa.dropped.items[0].host_tag);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_release(sa.conn, ib2.id, 1));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 1), sb.dropped.items.len);
    try testing.expectEqual(@as(u64, 300), sb.dropped.items[0].host_tag);
    // A second release of a spent import is a stale handle.
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_release(sa.conn, ib2.id, 1));

    // Exactly one RETURN per question, nothing asked to close.
    for ([_]u32{ q1, q3 }) |q| try testing.expectEqual(@as(usize, 1), sa.countReturns(q));
    try testing.expectEqual(@as(usize, 1), sb.countReturns(q2));
    try testing.expectEqual(@as(usize, 0), sa.close_requested + sb.close_requested);
}

test "exceptions: return_exception becomes RETURN EXCEPTION with its type and reason" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    const ib = try connectPair(&sa, &sb, 100);
    const p = try msgU64(a, 1);
    defer a.free(p);

    const q = try call(&sa, ib, 0, p, &.{});
    try pump(&sa, &sb);
    const ic = sb.lastCall();
    const reason = "too busy right now";
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_exception(sb.conn, ic.answer_id, c.CAPNP_RETURN_EXCEPTION, reason, reason.len));
    try pump(&sa, &sb);
    const r = sa.returnFor(q) orelse return error.TestNoReturn;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_EXCEPTION), r.kind);
    try testing.expectEqual(@as(u16, 1), r.exception_type); // overloaded
    try testing.expectEqualStrings(reason, r.reason);
    try testing.expectEqual(@as(usize, 0), r.msg.len + r.caps.len);
    try testing.expectEqual(@as(usize, 1), sa.countReturns(q));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q, 0));

    // An empty reason and an unknown type.
    const q2 = try call(&sa, ib, 0, p, &.{});
    try pump(&sa, &sb);
    const ic2 = sb.lastCall();
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_return_exception(sb.conn, ic2.answer_id, 99, reason, reason.len));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_exception(sb.conn, ic2.answer_id, 0, null, 0));
    try pump(&sa, &sb);
    const r2 = sa.returnFor(q2) orelse return error.TestNoReturn;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_EXCEPTION), r2.kind);
    try testing.expectEqual(@as(u16, 0), r2.exception_type);
    try testing.expectEqual(@as(usize, 0), r2.reason.len);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q2, 0));
}

test "disconnect: transport_closed ends every open question once with RETURN DISCONNECTED" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    const ib = try connectPair(&sa, &sb, 100);
    const p = try msgU64(a, 1);
    defer a.free(p);

    const q1 = try call(&sa, ib, 0, p, &.{});
    const q2 = try call(&sa, ib, 1, p, &.{});
    try pump(&sa, &sb); // B holds both unanswered
    try testing.expectEqual(@as(usize, 2), sb.calls.items.len);
    const q3 = try call(&sa, ib, 2, p, &.{}); // never reaches B
    const before = sa.returns.items.len;

    c.capnp_conn_transport_closed(sa.conn);
    try drainAll(&sa);
    c.capnp_conn_transport_closed(sa.conn); // idempotent
    try drainAll(&sa);

    for ([_]u32{ q1, q2, q3 }) |q| {
        try testing.expectEqual(@as(usize, 1), sa.countReturns(q));
        const r = sa.returnFor(q).?;
        try testing.expectEqual(@as(u8, c.CAPNP_RETURN_DISCONNECTED), r.kind);
        try testing.expectEqual(@as(u16, 2), r.exception_type);
        try testing.expect(r.reason.len > 0);
    }
    try testing.expectEqual(before + 3, sa.returns.items.len);

    // Closed: no new work, no more input; finish and release are no-ops.
    var qid: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_E_CLOSED), c.capnp_call(sa.conn, ib, iface, 0, p.ptr, p.len, null, 0, 0, &qid));
    try testing.expectEqual(@as(i32, c.CAPNP_E_CLOSED), c.capnp_bootstrap(sa.conn, &qid));
    const zeros = [_]u8{ 0, 0, 0, 0 };
    try testing.expectEqual(@as(i32, c.CAPNP_E_CLOSED), c.capnp_conn_push_bytes(sa.conn, &zeros, zeros.len));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q1, 0));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_release(sa.conn, ib.id, 1));
    try drainAll(&sa);
    try testing.expectEqual(before + 3, sa.returns.items.len);
}

test "busy: a second next_effect before commit is CAPNP_E_BUSY; commit without next is a no-op" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var qid: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_bootstrap(sa.conn, &qid));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_bootstrap(sa.conn, &qid));

    var e1 = freshEffect();
    try testing.expectEqual(@as(i32, 1), c.capnp_conn_next_effect(sa.conn, &e1));
    try testing.expectEqual(@as(u8, c.CAPNP_EFFECT_OUT_FRAME), e1.kind);
    const first = e1.msg[0..e1.msg_len];
    var e2 = freshEffect();
    try testing.expectEqual(@as(i32, c.CAPNP_E_BUSY), c.capnp_conn_next_effect(sa.conn, &e2));
    // The borrowed frame is still intact.
    try testing.expect(first.len > 0);
    c.capnp_conn_commit_effect(sa.conn);
    try testing.expectEqual(@as(i32, 1), c.capnp_conn_next_effect(sa.conn, &e2));
    try testing.expectEqual(@as(u8, c.CAPNP_EFFECT_OUT_FRAME), e2.kind);
    c.capnp_conn_commit_effect(sa.conn);
    try testing.expectEqual(@as(i32, 0), c.capnp_conn_next_effect(sa.conn, &e2));
    c.capnp_conn_commit_effect(sa.conn); // no-op
    // NULL and a too-small struct_size.
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_conn_next_effect(sa.conn, null));
    var tiny = std.mem.zeroes(c.capnp_effect);
    tiny.struct_size = 2;
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_conn_next_effect(sa.conn, &tiny));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_conn_next_effect(null, &e2));
}

test "next_effect: an older host's smaller capnp_effect gets a prefix and the filled size" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var qid: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_bootstrap(sa.conn, &qid));

    // Only the fields through `kind` (13 bytes); everything after must stay
    // as the host left it.
    var e = std.mem.zeroes(c.capnp_effect);
    e.struct_size = 13;
    e.msg_len = 0xDEAD_BEEF;
    e.return_kind = 0x55;
    try testing.expectEqual(@as(i32, 1), c.capnp_conn_next_effect(sa.conn, &e));
    try testing.expectEqual(@as(u32, 13), e.struct_size);
    try testing.expectEqual(@as(u8, c.CAPNP_EFFECT_OUT_FRAME), e.kind);
    try testing.expectEqual(@as(u8, 0x55), e.return_kind);
    try testing.expectEqual(@as(usize, 0xDEAD_BEEF), e.msg_len);
    c.capnp_conn_commit_effect(sa.conn);
}

test "errors: stale ids and bad arguments map to CAPNP_E_BAD_ID / CAPNP_E_INVAL" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    const ib = try connectPair(&sa, &sb, 100);
    const p = try msgU64(a, 1);
    defer a.free(p);
    const pc = try msgCap(a, 1, 0);
    defer a.free(pc);
    var qid: u32 = 0;

    // NULL connection / NULL outputs / NULL message / promised target / flags.
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_bootstrap(null, &qid));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_bootstrap(sa.conn, null));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_call(sa.conn, ib, iface, 0, null, 0, null, 0, 0, &qid));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_call(sa.conn, ib, iface, 0, null, 16, null, 0, 0, &qid));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_call(sa.conn, ib, iface, 0, p.ptr, p.len, null, 0, 0, null));
    // A PROMISED target that is not an open question; one whose ops pointer
    // is missing although nops says there are some.
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_call(sa.conn, cap(c.CAPNP_CAP_PROMISED, 4242), iface, 0, p.ptr, p.len, null, 0, 0, &qid));
    var no_ops = cap(c.CAPNP_CAP_PROMISED, 0);
    no_ops.nops = 1;
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_call(sa.conn, no_ops, iface, 0, p.ptr, p.len, null, 0, 0, &qid));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_call(sa.conn, ib, iface, 0, p.ptr, p.len, null, 0, 1, &qid));
    // caps with NULL pointer but a count, and an unknown kind byte.
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_call(sa.conn, ib, iface, 0, pc.ptr, pc.len, null, 1, 0, &qid));
    const bad_kind = [_]c.capnp_cap{cap(9, 0)};
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_call(sa.conn, ib, iface, 0, pc.ptr, pc.len, &bad_kind, 1, 0, &qid));
    // A promised cap in the params that names no open question.
    const promised = [_]c.capnp_cap{cap(c.CAPNP_CAP_PROMISED, 4242)};
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_call(sa.conn, ib, iface, 0, pc.ptr, pc.len, &promised, 1, 0, &qid));
    // Stale handles.
    const stale_import = [_]c.capnp_cap{cap(c.CAPNP_CAP_IMPORT, 999)};
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_call(sa.conn, ib, iface, 0, pc.ptr, pc.len, &stale_import, 1, 0, &qid));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_call(sa.conn, cap(c.CAPNP_CAP_IMPORT, 999), iface, 0, p.ptr, p.len, null, 0, 0, &qid));
    // Index out of range: the message names caps[0] but the table is empty.
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_call(sa.conn, ib, iface, 0, pc.ptr, pc.len, null, 0, 0, &qid));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_finish(sa.conn, 12345, 0));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_release(sa.conn, 12345, 1));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_return_results(sb.conn, 12345, p.ptr, p.len, null, 0));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_return_exception(sb.conn, 12345, 0, null, 0));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_return_results(sb.conn, 12345, null, 0, null, 0));
    // Nothing was queued by any of the refused calls.
    var e = freshEffect();
    try testing.expectEqual(@as(i32, 0), c.capnp_conn_next_effect(sa.conn, &e));
    try testing.expectEqual(@as(i32, 0), c.capnp_conn_next_effect(sb.conn, &e));
    // A second bootstrap export is refused.
    var eid: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_set_bootstrap(sb.conn, 7, &eid));
    // A refused results payload leaves the answer open for an exception.
    const q = try call(&sa, ib, 0, p, &.{});
    try pump(&sa, &sb);
    const ic = sb.lastCall();
    const stale_export = [_]c.capnp_cap{cap(c.CAPNP_CAP_EXPORT, 77)};
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_return_results(sb.conn, ic.answer_id, pc.ptr, pc.len, &stale_export, 1));
    try testing.expectEqual(@as(i32, 0), c.capnp_conn_next_effect(sb.conn, &e));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_exception(sb.conn, ic.answer_id, 3, "nope", 4));
    try pump(&sa, &sb);
    const r = sa.returnFor(q) orelse return error.TestNoReturn;
    try testing.expectEqual(@as(u16, 3), r.exception_type);
    try testing.expectEqualStrings("nope", r.reason);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q, 0));
    // Finishing it again: spent.
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_finish(sa.conn, q, 0));
}

fn takeError(s: *Side, name_buf: *[64]u8, detail_buf: *[64]u8) !struct { code: i32, name: []const u8, detail: []const u8 } {
    var code: i32 = 0;
    var name: [*c]const u8 = null;
    var name_len: usize = 0;
    var detail: [*c]const u8 = null;
    var detail_len: usize = 0;
    try testing.expectEqual(@as(i32, 1), c.capnp_conn_take_error(s.conn, &code, &name, &name_len, &detail, &detail_len));
    const n = @min(name_len, name_buf.len);
    if (n > 0) @memcpy(name_buf[0..n], name[0..n]);
    const d = @min(detail_len, detail_buf.len);
    if (d > 0) @memcpy(detail_buf[0..d], detail[0..d]);
    return .{ .code = code, .name = name_buf[0..n], .detail = detail_buf[0..d] };
}

test "take_error: a malformed frame is PROTOCOL with the cause; a remote Abort is CLOSED with the reason" {
    const a = testing.allocator;
    var name_buf: [64]u8 = undefined;
    var detail_buf: [64]u8 = undefined;

    // 1. Malformed: 1000 segments.
    var sa = try newSide(a);
    defer sa.deinit();
    try testing.expectEqual(@as(i32, 0), c.capnp_conn_take_error(sa.conn, null, null, null, null, null));
    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 999, .little);
    std.mem.writeInt(u32, header[4..8], 1, .little);
    try testing.expectEqual(@as(i32, c.CAPNP_E_PROTOCOL), c.capnp_conn_push_bytes(sa.conn, &header, header.len));
    const e1 = try takeError(&sa, &name_buf, &detail_buf);
    try testing.expectEqual(@as(i32, c.CAPNP_E_PROTOCOL), e1.code);
    try testing.expectEqualStrings("InvalidFrame", e1.name);
    try testing.expectEqual(@as(usize, 0), e1.detail.len);
    // Taken: gone.
    try testing.expectEqual(@as(i32, 0), c.capnp_conn_take_error(sa.conn, null, null, null, null, null));
    // An Abort OUT_FRAME, then CLOSE_REQUESTED.
    try drainAll(&sa);
    try testing.expectEqual(@as(usize, 1), sa.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(a, sa.frames.items[0]);
    defer decoded.deinit();
    try testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    try testing.expectEqual(@as(usize, 1), sa.close_requested);
    try testing.expectEqual(@as(i32, c.CAPNP_E_CLOSED), c.capnp_conn_push_bytes(sa.conn, &header, header.len));

    // 2. Remote Abort, with an open question.
    var sc = try newSide(a);
    defer sc.deinit();
    var sd = try newSide(a);
    defer sd.deinit();
    const ib = try connectPair(&sc, &sd, 100);
    const p = try msgU64(a, 1);
    defer a.free(p);
    const q = try call(&sc, ib, 0, p, &.{});
    try pump(&sc, &sd);
    const abort = blk: {
        var mb = protocol.MessageBuilder.init(a);
        defer mb.deinit();
        try mb.buildAbort("server shutting down");
        break :blk try mb.finish();
    };
    defer a.free(abort);
    testing.log_level = .err;
    try testing.expectEqual(@as(i32, c.CAPNP_E_CLOSED), c.capnp_conn_push_bytes(sc.conn, abort.ptr, abort.len));
    const e2 = try takeError(&sc, &name_buf, &detail_buf);
    try testing.expectEqual(@as(i32, c.CAPNP_E_CLOSED), e2.code);
    try testing.expectEqualStrings("RemoteAbort", e2.name);
    try testing.expectEqualStrings("server shutting down", e2.detail);
    // No Abort goes back; the host is asked to close; the question is still
    // open until the host reports the transport closed.
    try drainAll(&sc);
    try testing.expectEqual(@as(usize, 0), sc.frames.items.len);
    try testing.expectEqual(@as(usize, 1), sc.close_requested);
    try testing.expectEqual(@as(usize, 0), sc.countReturns(q));
    c.capnp_conn_transport_closed(sc.conn);
    try drainAll(&sc);
    const r = sc.returnFor(q) orelse return error.TestNoReturn;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_DISCONNECTED), r.kind);
    try testing.expectEqualStrings("server shutting down", r.reason);
    try testing.expectEqual(@as(usize, 1), sc.countReturns(q));
}

test "tick: a call deadline ends the question with RETURN EXCEPTION and is counted" {
    const a = testing.allocator;
    const ms = std.time.ns_per_ms;
    var opts = defaultOpts();
    opts.default_call_timeout_ms = 100;
    // The host's clock is large (one hour of uptime); the bootstrap sent
    // before the first tick must not expire at once.
    const uptime: i64 = 3600 * std.time.ns_per_s;
    var sa = Side.init(a, try newConn(&opts, uptime));
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    try testing.expectEqual(@as(i32, 0), c.capnp_conn_tick(sa.conn, uptime + 10 * ms));
    const ib = try connectPair(&sa, &sb, 100);
    const p = try msgU64(a, 1);
    defer a.free(p);

    const q = try call(&sa, ib, 0, p, &.{});
    try pump(&sa, &sb);
    try testing.expectEqual(@as(i32, 0), c.capnp_conn_tick(sa.conn, uptime + 60 * ms));
    try testing.expectEqual(@as(usize, 0), sa.countReturns(q));
    try testing.expectEqual(@as(i32, 1), c.capnp_conn_tick(sa.conn, uptime + 300 * ms));
    try pump(&sa, &sb);
    const r = sa.returnFor(q) orelse return error.TestNoReturn;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_EXCEPTION), r.kind);
    try testing.expectEqual(@as(u16, 1), r.exception_type); // overloaded
    try testing.expectEqualStrings(capnp.rpc.peer.deadline_reason, r.reason);
    // A late answer is absorbed; the deadline owns the question now.
    const ic = sb.lastCall();
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, ic.answer_id, p.ptr, p.len, null, 0));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 1), sa.countReturns(q));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_finish(sa.conn, q, 0));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_conn_tick(null, 0));
}

test "version: the linked library reports the pinned core" {
    try testing.expectEqual(@as(u32, c.CAPNP_CORE_ABI_VERSION), c.capnp_core_abi_version());
    const v = std.mem.span(c.capnp_core_version());
    try testing.expect(std.mem.startsWith(u8, v, "core "));
    try testing.expect(std.mem.indexOf(u8, v, " / capnp-zig ") != null);
}

// ---------------------------------------------------------------------------
// M2: pipelining, cancel, promise exports, shutdown
// ---------------------------------------------------------------------------

fn promisedCap(qid: u32, ops: []const u16) c.capnp_cap {
    return .{ .kind = c.CAPNP_CAP_PROMISED, .id = qid, .ops = if (ops.len == 0) null else ops.ptr, .nops = @intCast(ops.len) };
}

test "pipelining: a call on a promised answer goes out at once, costs no extra round trip, and each question returns once" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    const ib = try connectPair(&sa, &sb, 100);
    const p1 = try msgU64(a, 1);
    defer a.free(p1);
    const p5 = try msgU64(a, 5);
    defer a.free(p5);

    // q1 asks for a capability (method 0); q2 calls method 2 on the
    // capability q1 WILL return (its results struct, pointer field 0); q3
    // passes that same promised capability in its params (method 3).
    const q1 = try call(&sa, ib, 0, p1, &.{});
    const path = [_]u16{0};
    const q2 = try call(&sa, promisedCap(q1, &path), 2, p5, &.{});
    const pc = try msgCap(a, 44, 0);
    defer a.free(pc);
    const q3 = try call(&sa, ib, 3, pc, &.{promisedCap(q1, &path)});

    // Every call left A before anything came back: 3 frames, no RETURN.
    try drainAll(&sa);
    try testing.expectEqual(@as(usize, 3), sa.frames.items.len);
    try testing.expectEqual(@as(usize, 1), sa.returns.items.len); // the bootstrap's
    for (sa.frames.items) |f| try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_conn_push_bytes(sb.conn, f.ptr, f.len));
    try drainAll(&sb);
    // B's host has q1 (method 0); q2 waits in B's core for q1's answer. q3
    // is refused by B's core (its params name an answer B has not produced;
    // capnp-zig delivers it unresolved, see cap_remap.copyInbound): the
    // caller sees an exception, the host never sees the call.
    try testing.expectEqual(@as(usize, 1), sb.calls.items.len);
    try testing.expectEqual(@as(u16, 0), sb.calls.items[0].method_id);
    const answer1 = sb.calls.items[0].answer_id;
    for (sb.frames.items) |f| try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_conn_push_bytes(sa.conn, f.ptr, f.len));
    try drainAll(&sa);
    const r3 = sa.returnFor(q3) orelse return error.TestNoReturn;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_EXCEPTION), r3.kind);
    try testing.expectEqualStrings("PromisedCapUnsupported", r3.reason);

    // B answers q1 with a new export (tag 300) at pointer 0.
    var eb2: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_export(sb.conn, 300, &eb2));
    const r1 = try msgCap(a, 33, 0);
    defer a.free(r1);
    const r1caps = [_]c.capnp_cap{cap(c.CAPNP_CAP_EXPORT, eb2)};
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, answer1, r1.ptr, r1.len, &r1caps, r1caps.len));
    try pump(&sa, &sb);

    // The pipelined q2 reached export 300 with its params.
    try testing.expectEqual(@as(usize, 2), sb.calls.items.len);
    const ic2 = sb.lastCall();
    try testing.expectEqual(@as(u16, 2), ic2.method_id);
    try testing.expectEqual(eb2, ic2.export_id);
    try testing.expectEqual(@as(u64, 300), ic2.host_tag);
    try testing.expectEqual(@as(u64, 5), try readU64(a, ic2.msg));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, ic2.answer_id, p1.ptr, p1.len, null, 0));
    try pump(&sa, &sb);

    for ([_]u32{ q1, q2, q3 }) |q| try testing.expectEqual(@as(usize, 1), sa.countReturns(q));
    for ([_]u32{ q1, q2 }) |q| try testing.expectEqual(@as(u8, c.CAPNP_RETURN_RESULTS), sa.returnFor(q).?.kind);
    const ib2 = try rootCap(a, sa.returnFor(q1).?.msg, sa.returnFor(q1).?.caps);
    try testing.expectEqual(@as(u8, c.CAPNP_CAP_IMPORT), ib2.kind);
    try testing.expectEqual(@as(u64, 1), try readU64(a, sa.returnFor(q2).?.msg));
    for ([_]u32{ q1, q2, q3 }) |q| try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q, 0));

    // Even once the callee has produced the answer, capnp-zig hands a
    // receiverAnswer params cap to the host unresolved (its InboundCapTable
    // never consults the answer; handoff H9), so q4 is refused the same way.
    try pump(&sa, &sb); // the three Finish frames reach B
    const base = sa.frames.items.len;
    const q1b = try call(&sa, ib, 0, p1, &.{});
    const q4 = try call(&sa, ib, 3, pc, &.{promisedCap(q1b, &path)});
    try drainAll(&sa);
    try testing.expectEqual(base + 2, sa.frames.items.len);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_conn_push_bytes(sb.conn, sa.frames.items[base].ptr, sa.frames.items[base].len));
    try drainAll(&sb);
    const r1b = try msgCap(a, 34, 0);
    defer a.free(r1b);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, sb.lastCall().answer_id, r1b.ptr, r1b.len, &r1caps, r1caps.len));
    const b_calls_before_q4 = sb.calls.items.len;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_conn_push_bytes(sb.conn, sa.frames.items[base + 1].ptr, sa.frames.items[base + 1].len));
    try pump(&sa, &sb);
    try testing.expectEqual(b_calls_before_q4, sb.calls.items.len);
    const r4 = sa.returnFor(q4) orelse return error.TestNoReturn;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_EXCEPTION), r4.kind);
    try testing.expectEqualStrings("PromisedCapUnsupported", r4.reason);
    const ib1b = try rootCap(a, sa.returnFor(q1b).?.msg, sa.returnFor(q1b).?.caps);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q1b, 0));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q4, 0));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_release(sa.conn, ib1b.id, 1));
    // After its RETURN a question is no longer a pipelining target.
    var qid: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_call(sa.conn, promisedCap(q1, &path), iface, 0, p1.ptr, p1.len, null, 0, 0, &qid));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_release(sa.conn, ib2.id, 1));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 0), sa.close_requested + sb.close_requested);
}

test "cancel: one RETURN CANCELED, a late Return is absorbed, the answer is gone on the callee" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    const ib = try connectPair(&sa, &sb, 100);
    const p = try msgU64(a, 1);
    defer a.free(p);

    const q = try call(&sa, ib, 0, p, &.{});
    try pump(&sa, &sb);
    const ic = sb.lastCall();

    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_cancel(sa.conn, q));
    try pump(&sa, &sb); // the Finish reaches B
    try testing.expectEqual(@as(usize, 1), sa.countReturns(q));
    const r = sa.returnFor(q).?;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_CANCELED), r.kind);
    try testing.expectEqualStrings("canceled by the host", r.reason);
    // Gone on A: finish and a second cancel are stale ids.
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_finish(sa.conn, q, 0));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_cancel(sa.conn, q));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_cancel(sa.conn, 777));

    // Late Finish on B: the host still holds the answer and replies; the
    // reply is absorbed (nothing new on A) and the answer is then gone.
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, ic.answer_id, p.ptr, p.len, null, 0));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 1), sa.countReturns(q));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_return_results(sb.conn, ic.answer_id, p.ptr, p.len, null, 0));

    // The connection is healthy: another call round-trips.
    const q2 = try call(&sa, ib, 1, p, &.{});
    try pump(&sa, &sb);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, sb.lastCall().answer_id, p.ptr, p.len, null, 0));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_RESULTS), (sa.returnFor(q2) orelse return error.TestNoReturn).kind);
    try testing.expectEqual(@as(usize, 0), sa.close_requested + sb.close_requested);
}

test "promise export: calls on it wait, resolve delivers them to the target, reject fails them" {
    const a = testing.allocator;
    var sa = try newSide(a);
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    const ib = try connectPair(&sa, &sb, 100);
    const p = try msgU64(a, 1);
    defer a.free(p);
    const pc = try msgCap(a, 9, 0);
    defer a.free(pc);

    // B hands A a promise in a results payload.
    var promise: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_promise_export(sb.conn, &promise));
    const q1 = try call(&sa, ib, 0, p, &.{});
    try pump(&sa, &sb);
    const caps1 = [_]c.capnp_cap{cap(c.CAPNP_CAP_EXPORT, promise)};
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, sb.lastCall().answer_id, pc.ptr, pc.len, &caps1, 1));
    try pump(&sa, &sb);
    const ip = try rootCap(a, sa.returnFor(q1).?.msg, sa.returnFor(q1).?.caps);
    try testing.expectEqual(@as(u8, c.CAPNP_CAP_IMPORT), ip.kind);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q1, 0));

    // A calls the promise: B's host sees nothing yet.
    const calls_before = sb.calls.items.len;
    const qp = try call(&sa, ip, 5, p, &.{});
    try pump(&sa, &sb);
    try testing.expectEqual(calls_before, sb.calls.items.len);
    try testing.expectEqual(@as(usize, 0), sa.countReturns(qp));

    // B resolves it to a real export (tag 400): the queued call arrives there.
    var target: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_export(sb.conn, 400, &target));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_resolve_promise(sb.conn, promise, cap(c.CAPNP_CAP_EXPORT, target)));
    try pump(&sa, &sb);
    try testing.expectEqual(calls_before + 1, sb.calls.items.len);
    const ic = sb.lastCall();
    try testing.expectEqual(@as(u64, 400), ic.host_tag);
    try testing.expectEqual(@as(u16, 5), ic.method_id);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, ic.answer_id, p.ptr, p.len, null, 0));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_RESULTS), (sa.returnFor(qp) orelse return error.TestNoReturn).kind);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, qp, 0));
    // Once only; not on a plain export; not on an unknown id.
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_resolve_promise(sb.conn, promise, cap(c.CAPNP_CAP_EXPORT, target)));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_resolve_promise(sb.conn, target, cap(c.CAPNP_CAP_EXPORT, target)));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_resolve_promise(sb.conn, 9999, cap(c.CAPNP_CAP_EXPORT, target)));
    try testing.expectEqual(@as(i32, c.CAPNP_E_INVAL), c.capnp_resolve_promise(sb.conn, promise, cap(c.CAPNP_CAP_NONE, 0)));

    // A second promise, rejected: the waiting call fails.
    var promise2: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_promise_export(sb.conn, &promise2));
    const q2 = try call(&sa, ib, 0, p, &.{});
    try pump(&sa, &sb);
    const caps2 = [_]c.capnp_cap{cap(c.CAPNP_CAP_EXPORT, promise2)};
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, sb.lastCall().answer_id, pc.ptr, pc.len, &caps2, 1));
    try pump(&sa, &sb);
    const ip2 = try rootCap(a, sa.returnFor(q2).?.msg, sa.returnFor(q2).?.caps);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, q2, 0));
    const qr = try call(&sa, ip2, 6, p, &.{});
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 0), sa.countReturns(qr));
    const reason = "nope";
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_reject_promise(sb.conn, promise2, reason, reason.len));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 1), sa.countReturns(qr));
    const r = sa.returnFor(qr).?;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_EXCEPTION), r.kind);
    try testing.expect(r.reason.len > 0);
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_finish(sa.conn, qr, 0));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_release(sa.conn, ip.id, 1));
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_release(sa.conn, ip2.id, 1));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(usize, 0), sa.close_requested + sb.close_requested);
}

test "shutdown: no new calls, open questions drain, then CLOSE_REQUESTED; the drain timeout ends stragglers" {
    const a = testing.allocator;
    const ms = std.time.ns_per_ms;
    const p = try msgU64(a, 1);
    defer a.free(p);

    // 1. Drain completes: the open question returns, then the core asks to close.
    {
        var sa = try newSide(a);
        defer sa.deinit();
        var sb = try newSide(a);
        defer sb.deinit();
        const ib = try connectPair(&sa, &sb, 100);
        const q = try call(&sa, ib, 0, p, &.{});
        try pump(&sa, &sb);

        c.capnp_conn_shutdown(sa.conn);
        c.capnp_conn_shutdown(sa.conn); // idempotent
        var qid: u32 = 0;
        try testing.expectEqual(@as(i32, c.CAPNP_E_CLOSED), c.capnp_call(sa.conn, ib, iface, 1, p.ptr, p.len, null, 0, 0, &qid));
        try testing.expectEqual(@as(i32, c.CAPNP_E_CLOSED), c.capnp_bootstrap(sa.conn, &qid));
        try pump(&sa, &sb);
        try testing.expectEqual(@as(usize, 0), sa.close_requested); // still draining
        try testing.expectEqual(@as(usize, 0), sa.countReturns(q));

        try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_return_results(sb.conn, sb.lastCall().answer_id, p.ptr, p.len, null, 0));
        try pump(&sa, &sb);
        _ = c.capnp_conn_tick(sa.conn, 10 * ms);
        try pump(&sa, &sb);
        try testing.expectEqual(@as(usize, 1), sa.countReturns(q));
        try testing.expectEqual(@as(u8, c.CAPNP_RETURN_RESULTS), sa.returnFor(q).?.kind);
        try testing.expectEqual(@as(usize, 1), sa.close_requested);
        c.capnp_conn_transport_closed(sa.conn);
        try drainAll(&sa);
        try testing.expectEqual(@as(usize, 1), sa.close_requested);
    }

    // 2. The drain timeout: the straggler ends as DISCONNECTED, then close.
    {
        var opts = defaultOpts();
        opts.shutdown_drain_timeout_ms = 100;
        var sa = Side.init(a, try newConn(&opts, 0));
        defer sa.deinit();
        var sb = try newSide(a);
        defer sb.deinit();
        _ = c.capnp_conn_tick(sa.conn, 1 * ms);
        const ib = try connectPair(&sa, &sb, 100);
        const q = try call(&sa, ib, 0, p, &.{});
        try pump(&sa, &sb);

        c.capnp_conn_shutdown(sa.conn);
        try testing.expectEqual(@as(i32, 0), c.capnp_conn_tick(sa.conn, 50 * ms));
        try drainAll(&sa);
        try testing.expectEqual(@as(usize, 0), sa.close_requested);
        try testing.expectEqual(@as(i32, 1), c.capnp_conn_tick(sa.conn, 200 * ms));
        try drainAll(&sa);
        try testing.expectEqual(@as(usize, 1), sa.countReturns(q));
        try testing.expectEqual(@as(u8, c.CAPNP_RETURN_DISCONNECTED), sa.returnFor(q).?.kind);
        try testing.expectEqual(@as(usize, 1), sa.close_requested);
    }
}

test "set_deadline: a per-question deadline fires on a tick" {
    const a = testing.allocator;
    const ms = std.time.ns_per_ms;
    var sa = try newSide(a);
    defer sa.deinit();
    var sb = try newSide(a);
    defer sb.deinit();
    _ = c.capnp_conn_tick(sa.conn, 1 * ms);
    const ib = try connectPair(&sa, &sb, 100);
    const p = try msgU64(a, 1);
    defer a.free(p);
    const q = try call(&sa, ib, 0, p, &.{});
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_set_deadline(sa.conn, q, 100));
    try testing.expectEqual(@as(i32, c.CAPNP_E_BAD_ID), c.capnp_set_deadline(sa.conn, 777, 100));
    try pump(&sa, &sb);
    try testing.expectEqual(@as(i32, 0), c.capnp_conn_tick(sa.conn, 50 * ms));
    try testing.expectEqual(@as(i32, 1), c.capnp_conn_tick(sa.conn, 150 * ms));
    try pump(&sa, &sb);
    const r = sa.returnFor(q) orelse return error.TestNoReturn;
    try testing.expectEqual(@as(u8, c.CAPNP_RETURN_EXCEPTION), r.kind);
    try testing.expectEqual(@as(u16, 1), r.exception_type);
}

// ---------------------------------------------------------------------------
// u32_le framing through the C ABI (M6, plan §2)
// ---------------------------------------------------------------------------

test "u32_le: accepted, wire frames carry the prefix, alpn exported" {
    var opts = defaultOpts();
    opts.framing = c.CAPNP_FRAMING_U32_LE;
    var raw: ?*c.capnp_conn = null;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_conn_new(&opts, 0, &raw));
    const conn = raw.?;
    defer _ = c.capnp_conn_free(conn);

    var qid: u32 = 0;
    try testing.expectEqual(@as(i32, c.CAPNP_OK), c.capnp_bootstrap(conn, &qid));

    var eff: c.capnp_effect = undefined;
    eff.struct_size = @sizeOf(c.capnp_effect);
    try testing.expectEqual(@as(i32, 1), c.capnp_conn_next_effect(conn, &eff));
    defer _ = c.capnp_conn_commit_effect(conn);
    try testing.expectEqual(@as(u8, 0), eff.kind); // OUT_FRAME
    try testing.expect(eff.msg_len > 4);
    const len = std.mem.readInt(u32, eff.msg[0..4], .little);
    try testing.expectEqual(eff.msg_len - 4, @as(usize, len));

    // The frozen QUIC baseline ALPN from the pinned package.
    const alpn = std.mem.span(c.capnp_core_quic_alpn());
    try testing.expectEqualStrings("capnp-rpc/1", alpn);
    try testing.expectEqual(c.CAPNP_FRAMING_U32_LE, @as(u8, 1));
}
