//! A bootstrap + call round trip between two in-process `Conn`s, run from the
//! shipped library by the test hook `capnp_core_debug_selftest` (abi.zig).
//!
//! Why it exists in M0 (before M1 adds the `capnp_conn_*` exports):
//! - it keeps `conn.zig` and the capnp-zig `Peer` linked into every
//!   XCFramework slice, so `scripts/check-symbols.sh` gates what the real
//!   connection core imports, not an empty archive;
//! - a Swift test calls it, which proves the Peer runs inside an app process
//!   under `apple_root.zig`'s overrides (C allocator, `std.Io.failing` debug
//!   Io, trap panic), not only under the Zig test runner.
//!
//! B sets a bootstrap export; A bootstraps it, calls it with `{u64 = 41}`, B
//! answers `{u64 = 42}`, A finishes the question and releases the import.

const std = @import("std");
const capnp = @import("capnpc-zig");
const conn_mod = @import("conn.zig");
const effects = @import("effects.zig");

const message = capnp.message;
const Conn = conn_mod.Conn;
const Cap = effects.Cap;

/// The host tag B's bootstrap export carries; INBOUND_CALL must echo it.
pub const bootstrap_tag: u64 = 0x5e1f_7e57;
pub const interface_id: u64 = 0x5e1f_7e57_0000_0001;

pub const Error = error{
    SelftestNoBootstrapReturn,
    SelftestBootstrapNotAnImport,
    SelftestNoInboundCall,
    SelftestWrongInboundCall,
    SelftestNoCallReturn,
    SelftestWrongResults,
    SelftestUnexpectedEffect,
    SelftestRunaway,
};

/// What one side's host saw while draining.
const Seen = struct {
    ret_qid: ?u32 = null,
    ret_kind: effects.ReturnKind = .exception,
    ret_value: u64 = 0,
    ret_cap: ?Cap = null,
    call_answer: ?u32 = null,
    call_tag: u64 = 0,
    call_interface: u64 = 0,
    call_value: u64 = 0,
    closes: u32 = 0,
};

pub fn run(allocator: std.mem.Allocator) !void {
    // Nothing here ticks or sets a deadline, so any fixed clock will do.
    const a = try Conn.init(allocator, .{ .now_ns = 0 });
    defer a.deinit();
    const b = try Conn.init(allocator, .{ .now_ns = 0 });
    defer b.deinit();
    var sa: Seen = .{};
    var sb: Seen = .{};

    _ = try b.setBootstrap(bootstrap_tag);

    // 1. Bootstrap: the RETURN's root is a capability pointer to an import.
    const q0 = try a.bootstrap();
    try pump(allocator, a, &sa, b, &sb);
    if (sa.ret_qid != q0 or sa.ret_kind != .results) return error.SelftestNoBootstrapReturn;
    const boot = sa.ret_cap orelse return error.SelftestBootstrapNotAnImport;
    if (boot.kind != .import) return error.SelftestBootstrapNotAnImport;

    // 2. Call it; B's host sees the call on its bootstrap tag.
    const params = try structU64(allocator, 41);
    defer allocator.free(params);
    const q1 = try a.call(boot, interface_id, 0, params, &.{}, 0);
    try pump(allocator, a, &sa, b, &sb);
    const answer = sb.call_answer orelse return error.SelftestNoInboundCall;
    if (sb.call_tag != bootstrap_tag or sb.call_interface != interface_id or sb.call_value != 41)
        return error.SelftestWrongInboundCall;

    // 3. B answers; A gets the results.
    const results = try structU64(allocator, 42);
    defer allocator.free(results);
    try b.returnResults(answer, results, &.{});
    try pump(allocator, a, &sa, b, &sb);
    if (sa.ret_qid != q1) return error.SelftestNoCallReturn;
    if (sa.ret_kind != .results or sa.ret_value != 42) return error.SelftestWrongResults;

    // 4. Finish and release; nothing may ask to close.
    try a.finish(q1, false);
    try a.release(boot.id, 1);
    try pump(allocator, a, &sa, b, &sb);
    if (sa.closes != 0 or sb.closes != 0) return error.SelftestUnexpectedEffect;
}

/// Move frames both ways until both queues are empty.
fn pump(allocator: std.mem.Allocator, a: *Conn, sa: *Seen, b: *Conn, sb: *Seen) !void {
    var rounds: usize = 0;
    while (rounds < 10_000) : (rounds += 1) {
        const pa = try drainOne(allocator, a, sa, b);
        const pb = try drainOne(allocator, b, sb, a);
        if (!pa and !pb) return;
    }
    return error.SelftestRunaway;
}

fn drainOne(allocator: std.mem.Allocator, src: *Conn, seen: *Seen, dst: *Conn) !bool {
    const eff = (try src.nextEffect()) orelse return false;
    defer src.commitEffect();
    switch (eff.*) {
        .out_frame => |bytes| try dst.pushBytes(bytes),
        .close_requested => seen.closes += 1,
        .@"return" => |r| {
            seen.ret_qid = r.qid;
            seen.ret_kind = r.kind;
            seen.ret_cap = null;
            seen.ret_value = 0;
            if (r.kind == .results) {
                var m = try message.Message.init(allocator, r.msg, .{});
                defer m.deinit();
                // The bootstrap RETURN's root is a capability pointer; a
                // call's is a struct.
                const root = try m.getRootAnyPointer();
                if (root.getCapability()) |cap| {
                    if (cap.id < r.caps.len) seen.ret_cap = r.caps[cap.id];
                } else |_| {
                    seen.ret_value = (try m.getRootStruct()).readU64(0);
                }
            }
        },
        .inbound_call => |c| {
            seen.call_answer = c.answer_id;
            seen.call_tag = c.host_tag;
            seen.call_interface = c.interface_id;
            var m = try message.Message.init(allocator, c.msg, .{});
            defer m.deinit();
            seen.call_value = (try m.getRootStruct()).readU64(0);
        },
        .export_dropped, .event => return error.SelftestUnexpectedEffect,
    }
    return true;
}

/// A standalone message whose root is `struct { u64 @0 }` (D5).
fn structU64(allocator: std.mem.Allocator, v: u64) ![]const u8 {
    var mb = message.MessageBuilder.init(allocator);
    defer mb.deinit();
    const root = try mb.allocateStruct(1, 0);
    root.writeU64(0, v);
    return mb.toBytes();
}

test "selftest: bootstrap + call round trip over two conns, no leaks" {
    try run(std.testing.allocator);
}
