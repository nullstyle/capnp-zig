//! `zig build fuzz-native-abi -- [--seconds N] [--seed S] [--steps N] [--ops a,b,...]`
//! (capnp-swift plan §8, M2 gate). `--steps` fixes the operations per session
//! and `--ops` restricts the operation kinds (indices of `randomOp`'s switch),
//! to bisect a finding.
//!
//! Drives the C ABI with random operation sequences: two connections wired
//! back to back, every `capnp_*` export called with known and with stale or
//! random handles, frames from the other side, from the framing fixtures
//! (`tests/fixtures/framing/`) and mutated at random, commits without a
//! `next`, a second `next` before `commit`, ticks, deadlines, cancels,
//! promises, shutdown, transport close, and frees with effects queued and one
//! in flight.
//!
//! This file is the root of the fuzz executable: `native.abi` takes its
//! allocator from `capnp_core_allocator` below, a live-byte counter over the
//! page allocator. Not `std.heap.DebugAllocator`: at Zig 0.17.0 it keeps the
//! slabs of emptied small-allocation buckets mapped, so a 30-minute run grew
//! past 20 GB with 0 live bytes (handoff-zig-fork-debug-allocator-slabs.md);
//! the counter is the leak check here (`CountingAllocator.live` must return
//! to its pre-session value), and the core's own tests keep
//! `std.testing.allocator`'s double-free checks.
//!
//! Invariants, checked every session:
//!   - no crash (run it in a safety-checked mode: Debug or ReleaseSafe);
//!   - every RETURN names a question this host opened and has not seen end,
//!     and after `transport_closed` + drain no question is still open
//!     (exactly one RETURN per question);
//!   - every RETURN/INBOUND_CALL payload is a well-formed standalone message
//!     no larger than its frame could be;
//!   - a second `next_effect` before `commit` is BUSY;
//!   - 0 live bytes after both connections are freed (and the
//!     DebugAllocator reports no leak at exit).
//! Exit 0 after the time budget; exit 1 on the first violation, printing the
//! seed and the step so it can be replayed with `--seed`.

const std = @import("std");
const capnp = @import("capnpc-zig");
// Through the module, not by path: src/native/ belongs to the capnp-zig
// module, and a file may belong to only one module per compilation.
const abi = capnp.native.abi;
const effects = capnp.native.effects;

const message = capnp.message;

/// capnp-zig logs every refused frame and failed Finish at debug/warn level;
/// the fuzzer provokes thousands, so only errors reach stderr.
pub const std_options: std.Options = .{ .log_level = .err };

var counting: abi.CountingAllocator = .{ .parent = std.heap.page_allocator };
/// Read by `abi.zig` (`@import("root").capnp_core_allocator`).
pub const capnp_core_allocator: std.mem.Allocator = counting.allocator();

const Cap = effects.Cap;
const conn_t = abi.capnp_conn;

const Side = struct {
    conn: ?*conn_t,
    /// Questions this host opened and has not seen a RETURN for.
    open: std.AutoHashMap(u32, void),
    /// Imports this host received (and may release).
    imports: std.ArrayList(u32),
    /// Export ids this host created (incl. promises).
    exports: std.ArrayList(u32),
    promises: std.ArrayList(u32),
    /// INBOUND_CALL answer ids not yet answered.
    answers: std.ArrayList(u32),
    /// Frames to deliver to the other side.
    outbox: std.ArrayList([]u8),
    closed: bool = false,
    returns: usize = 0,
    calls_in: usize = 0,
    close_requested: usize = 0,

    fn init(a: std.mem.Allocator, conn: *conn_t) Side {
        return .{
            .conn = conn,
            .open = std.AutoHashMap(u32, void).init(a),
            .imports = .empty,
            .exports = .empty,
            .promises = .empty,
            .answers = .empty,
            .outbox = .empty,
        };
    }

    fn deinit(self: *Side, a: std.mem.Allocator) void {
        self.open.deinit();
        self.imports.deinit(a);
        self.exports.deinit(a);
        self.promises.deinit(a);
        self.answers.deinit(a);
        for (self.outbox.items) |f| a.free(f);
        self.outbox.deinit(a);
    }
};

const Fuzz = struct {
    a: std.mem.Allocator,
    rand: std.Random,
    seeds: []const []const u8,
    seed: u64,
    /// `--steps` (null: random 8..168) and `--ops` (allowed op indices).
    fixed_steps: ?usize = null,
    op_mask: [32]bool = @splat(true),
    step: usize = 0,
    ops: usize = 0,
    sessions: usize = 0,
    frames_pushed: usize = 0,
    returns: usize = 0,
    busy_seen: usize = 0,
    protocol_fails: usize = 0,

    fn fail(self: *Fuzz, comptime what: []const u8, args: anytype) error{Violation} {
        std.debug.print("fuzz-abi: VIOLATION (seed {d}, session {d}, step {d}): " ++ what ++ "\n", .{ self.seed, self.sessions, self.step } ++ args);
        return error.Violation;
    }

    fn pick(self: *Fuzz, list: []const u32) ?u32 {
        if (list.len == 0) return null;
        return list[self.rand.uintLessThan(usize, list.len)];
    }

    /// A known id, or a random one (stale / forged), 1 time in 4.
    fn pickOrRandom(self: *Fuzz, list: []const u32) u32 {
        if (self.rand.uintLessThan(u8, 4) == 0) return self.rand.int(u32) % 64;
        return self.pick(list) orelse self.rand.int(u32) % 64;
    }

    fn randomCap(self: *Fuzz, side: *Side) Cap {
        return switch (self.rand.uintLessThan(u8, 6)) {
            0 => .{ .kind = .none },
            1, 2 => .{ .kind = .import, .id = self.pickOrRandom(side.imports.items) },
            3 => .{ .kind = .@"export", .id = self.pickOrRandom(side.exports.items) },
            4 => blk: {
                var keys: [8]u32 = undefined;
                var n: usize = 0;
                var it = side.open.keyIterator();
                while (it.next()) |k| : (n += 1) {
                    if (n == keys.len) break;
                    keys[n] = k.*;
                }
                break :blk .{ .kind = .promised, .id = self.pickOrRandom(keys[0..n]), .ops = &promised_paths[self.rand.uintLessThan(usize, promised_paths.len)], .nops = 1 };
            },
            else => blk: {
                // A kind byte C could send (incl. the invalid 4), forged
                // through memory: a Zig enum must never hold it directly.
                var cap: Cap = .{ .kind = .none, .id = self.rand.int(u32) };
                const raw: *u8 = @ptrCast(&cap.kind);
                raw.* = self.rand.int(u8) % 5;
                break :blk cap;
            },
        };
    }

    const promised_paths = [_][1]u16{ .{0}, .{1}, .{3}, .{7} };

    /// A standalone params/results message: struct { u64 } with 0..2 cap
    /// pointers naming random caps[] indices; or random bytes.
    fn randomPayload(self: *Fuzz, ncaps: usize) ![]u8 {
        if (self.rand.uintLessThan(u8, 8) == 0) {
            const len = self.rand.uintLessThan(usize, 48);
            const bytes = try self.a.alloc(u8, len);
            self.rand.bytes(bytes);
            return bytes;
        }
        var mb = message.MessageBuilder.init(self.a);
        defer mb.deinit();
        const nptr: u16 = @intCast(self.rand.uintLessThan(u8, 3));
        const root = try mb.allocateStruct(1, nptr);
        root.writeU64(0, self.rand.int(u64));
        var i: u16 = 0;
        while (i < nptr) : (i += 1) {
            const idx: u32 = if (ncaps == 0 or self.rand.uintLessThan(u8, 8) == 0) self.rand.int(u32) % 4 else @intCast(self.rand.uintLessThan(usize, ncaps));
            try (try root.getAnyPointer(i)).setCapability(.{ .id = idx });
        }
        const bytes = try mb.toBytes();
        return @constCast(bytes);
    }

    fn effectSize() u32 {
        return @sizeOf(abi.capnp_effect);
    }

    /// Pull one effect; record what the host must track. False when empty.
    fn drainOne(self: *Fuzz, side: *Side) !bool {
        const conn = side.conn orelse return false;
        var e: abi.capnp_effect = std.mem.zeroes(abi.capnp_effect);
        e.struct_size = effectSize();
        const rc = abi.capnp_conn_next_effect(conn, &e);
        if (rc == 0) return false;
        if (rc != 1) return self.fail("next_effect returned {d}", .{rc});
        defer abi.capnp_conn_commit_effect(conn);
        // One in flight: a second next is BUSY and leaves the first intact.
        if (self.rand.uintLessThan(u8, 16) == 0) {
            var e2: abi.capnp_effect = std.mem.zeroes(abi.capnp_effect);
            e2.struct_size = effectSize();
            if (abi.capnp_conn_next_effect(conn, &e2) != abi.CAPNP_E_BUSY) return self.fail("second next_effect was not BUSY", .{});
            self.busy_seen += 1;
        }
        switch (e.kind) {
            @backingInt(effects.Kind.out_frame) => {
                if (e.msg_len == 0 or e.msg == null) return self.fail("empty OUT_FRAME", .{});
                try side.outbox.append(self.a, try self.a.dupe(u8, e.msg.?[0..e.msg_len]));
            },
            @backingInt(effects.Kind.close_requested) => side.close_requested += 1,
            @backingInt(effects.Kind.@"return") => {
                if (!side.open.remove(e.id)) return self.fail("RETURN for question {d} that is not open", .{e.id});
                side.returns += 1;
                self.returns += 1;
                if (e.return_kind == @backingInt(effects.ReturnKind.results)) {
                    try self.checkPayload(e.msg, e.msg_len, e.caps, e.ncaps, side);
                } else if (e.msg_len != 0) return self.fail("non-RESULTS RETURN carries a payload", .{});
            },
            @backingInt(effects.Kind.inbound_call) => {
                try side.answers.append(self.a, e.id);
                side.calls_in += 1;
                try self.checkPayload(e.msg, e.msg_len, e.caps, e.ncaps, side);
            },
            @backingInt(effects.Kind.export_dropped) => {},
            @backingInt(effects.Kind.event) => {},
            else => return self.fail("unknown effect kind {d}", .{e.kind}),
        }
        return true;
    }

    fn checkPayload(self: *Fuzz, msg: ?[*]const u8, len: usize, caps: ?[*]const Cap, ncaps: usize, side: *Side) !void {
        if (len == 0 or msg == null) return self.fail("payload without bytes", .{});
        var m = message.Message.init(self.a, msg.?[0..len], .{}) catch return self.fail("payload is not a valid message", .{});
        defer m.deinit();
        _ = m.getRootAnyPointer() catch return self.fail("payload root unreadable", .{});
        if (ncaps > 0 and caps == null) return self.fail("caps missing", .{});
        var i: usize = 0;
        while (i < ncaps) : (i += 1) {
            const cap = caps.?[i];
            switch (cap.kind) {
                .import => try side.imports.append(self.a, cap.id),
                .@"export", .none => {},
                .promised => return self.fail("an unresolved PROMISED entry reached the host", .{}),
            }
        }
    }

    fn drainAll(self: *Fuzz, side: *Side) !void {
        var n: usize = 0;
        while (try self.drainOne(side)) : (n += 1) {
            if (n > 100_000) return self.fail("effect queue never empties", .{});
        }
    }

    /// Deliver (part of) the outbox to the other side, possibly mutated.
    fn deliver(self: *Fuzz, from: *Side, to: *Side) !void {
        const conn = to.conn orelse {
            for (from.outbox.items) |f| self.a.free(f);
            from.outbox.clearRetainingCapacity();
            return;
        };
        while (from.outbox.items.len > 0) {
            const frame = from.outbox.orderedRemove(0);
            defer self.a.free(frame);
            if (self.rand.uintLessThan(u8, 24) == 0 and frame.len > 0) {
                // Mutate a byte, or truncate.
                if (self.rand.boolean()) {
                    frame[self.rand.uintLessThan(usize, frame.len)] ^= @as(u8, 1) << @intCast(self.rand.uintLessThan(u8, 8));
                    try self.push(to, frame);
                } else {
                    try self.push(to, frame[0..self.rand.uintLessThan(usize, frame.len)]);
                }
            } else {
                try self.push(to, frame);
            }
            _ = conn;
            if (to.closed) break;
        }
    }

    fn push(self: *Fuzz, to: *Side, bytes: []const u8) !void {
        const conn = to.conn orelse return;
        self.frames_pushed += 1;
        const rc = abi.capnp_conn_push_bytes(conn, if (bytes.len == 0) null else bytes.ptr, bytes.len);
        if (rc == abi.CAPNP_E_PROTOCOL) self.protocol_fails += 1;
        if (rc == abi.CAPNP_E_PROTOCOL or rc == abi.CAPNP_E_CLOSED) to.closed = true;
        try self.drainAll(to);
    }

    fn randomOp(self: *Fuzz, side: *Side, other: *Side) !void {
        const conn = side.conn orelse return;
        self.ops += 1;
        var out: u32 = 0;
        var op = self.rand.uintLessThan(u8, 26);
        var tries: usize = 0;
        while (!self.op_mask[op]) : (tries += 1) {
            if (tries > 64) return;
            op = self.rand.uintLessThan(u8, 26);
        }
        switch (op) {
            0 => if (abi.capnp_bootstrap(conn, &out) == abi.CAPNP_OK) try side.open.put(out, {}),
            1, 2, 3, 4 => {
                const target = self.randomCap(side);
                var caps_buf: [3]Cap = undefined;
                const ncaps = self.rand.uintLessThan(usize, 4);
                for (caps_buf[0..ncaps]) |*cap| cap.* = self.randomCap(side);
                const params = try self.randomPayload(ncaps);
                defer self.a.free(params);
                const flags: u32 = if (self.rand.uintLessThan(u8, 16) == 0) 1 else 0;
                const rc = abi.capnp_call(conn, target, self.rand.int(u64), self.rand.int(u16), params.ptr, params.len, if (ncaps == 0) null else &caps_buf, ncaps, flags, &out);
                if (rc == abi.CAPNP_OK) try side.open.put(out, {});
            },
            5, 6 => {
                var keys: [16]u32 = undefined;
                var n: usize = 0;
                var it = side.open.keyIterator();
                while (it.next()) |k| : (n += 1) {
                    if (n == keys.len) break;
                    keys[n] = k.*;
                }
                _ = abi.capnp_finish(conn, self.pickOrRandom(keys[0..n]), @intFromBool(self.rand.boolean()));
            },
            7 => {
                var keys: [16]u32 = undefined;
                var n: usize = 0;
                var it = side.open.keyIterator();
                while (it.next()) |k| : (n += 1) {
                    if (n == keys.len) break;
                    keys[n] = k.*;
                }
                _ = abi.capnp_cancel(conn, self.pickOrRandom(keys[0..n]));
            },
            8 => _ = abi.capnp_set_deadline(conn, self.rand.int(u32) % 8, self.rand.int(u32) % 2000),
            9, 10 => _ = abi.capnp_release(conn, self.pickOrRandom(side.imports.items), 1 + self.rand.int(u32) % 3),
            11 => if (abi.capnp_export(conn, self.rand.int(u64), &out) == abi.CAPNP_OK) try side.exports.append(self.a, out),
            12 => _ = abi.capnp_set_bootstrap(conn, 7, &out),
            13 => if (abi.capnp_promise_export(conn, &out) == abi.CAPNP_OK) {
                try side.exports.append(self.a, out);
                try side.promises.append(self.a, out);
            },
            14 => _ = abi.capnp_resolve_promise(conn, self.pickOrRandom(side.promises.items), self.randomCap(side)),
            15 => _ = abi.capnp_reject_promise(conn, self.pickOrRandom(side.promises.items), "fuzz", 4),
            16, 17, 18 => {
                const answer = self.pickOrRandom(side.answers.items);
                if (self.rand.boolean()) {
                    var caps_buf: [2]Cap = undefined;
                    const ncaps = self.rand.uintLessThan(usize, 3);
                    for (caps_buf[0..ncaps]) |*cap| cap.* = self.randomCap(side);
                    const results = try self.randomPayload(ncaps);
                    defer self.a.free(results);
                    if (abi.capnp_return_results(conn, answer, results.ptr, results.len, if (ncaps == 0) null else &caps_buf, ncaps) == abi.CAPNP_OK) removeId(&side.answers, answer);
                } else {
                    if (abi.capnp_return_exception(conn, answer, self.rand.int(u16) % 5, "boom", 4) == abi.CAPNP_OK) removeId(&side.answers, answer);
                }
            },
            19 => _ = abi.capnp_conn_tick(conn, @intCast(self.step * 7_000_000)),
            20 => {
                // A fixture chunk, as-is or mutated.
                const seed = self.seeds[self.rand.uintLessThan(usize, self.seeds.len)];
                const copy = try self.a.dupe(u8, seed);
                defer self.a.free(copy);
                if (copy.len > 0 and self.rand.boolean()) copy[self.rand.uintLessThan(usize, copy.len)] ^= 0xff;
                try self.push(side, copy);
            },
            21 => {
                const len = self.rand.uintLessThan(usize, 64);
                const junk = try self.a.alloc(u8, len);
                defer self.a.free(junk);
                self.rand.bytes(junk);
                try self.push(side, junk);
            },
            22 => abi.capnp_conn_commit_effect(conn), // commit with nothing in flight
            23 => if (self.rand.uintLessThan(u8, 8) == 0) abi.capnp_conn_shutdown(conn),
            24 => {
                var code: i32 = 0;
                _ = abi.capnp_conn_take_error(conn, &code, null, null, null, null);
            },
            else => try self.deliver(other, side),
        }
        try self.drainAll(side);
    }

    fn removeId(list: *std.ArrayList(u32), id: u32) void {
        for (list.items, 0..) |v, i| {
            if (v == id) {
                _ = list.swapRemove(i);
                return;
            }
        }
    }

    fn session(self: *Fuzz) !void {
        self.sessions += 1;
        const live_before = counting.live;
        var opts: abi.capnp_conn_opts = std.mem.zeroes(abi.capnp_conn_opts);
        opts.struct_size = @sizeOf(abi.capnp_conn_opts);
        if (self.rand.boolean()) opts.default_call_timeout_ms = 1 + self.rand.int(u32) % 500;
        if (self.rand.boolean()) opts.shutdown_drain_timeout_ms = 1 + self.rand.int(u32) % 300;
        if (self.rand.uintLessThan(u8, 4) == 0) opts.max_outbound_questions = 1 + self.rand.int(u32) % 6;
        if (self.rand.uintLessThan(u8, 4) == 0) opts.max_retained_questions = 1 + self.rand.int(u32) % 6;
        opts.observer = @intFromBool(self.rand.boolean());

        var ca: ?*conn_t = null;
        var cb: ?*conn_t = null;
        if (abi.capnp_conn_new(&opts, 0, &ca) != abi.CAPNP_OK) return self.fail("conn_new failed", .{});
        if (abi.capnp_conn_new(&opts, 0, &cb) != abi.CAPNP_OK) return self.fail("conn_new failed", .{});
        var a = Side.init(self.a, ca.?);
        defer a.deinit(self.a);
        var b = Side.init(self.a, cb.?);
        defer b.deinit(self.a);
        var eid: u32 = 0;
        _ = abi.capnp_set_bootstrap(cb.?, 100, &eid);
        try b.exports.append(self.a, eid);
        if (self.rand.boolean()) {
            _ = abi.capnp_set_bootstrap(ca.?, 101, &eid);
            try a.exports.append(self.a, eid);
        }

        const steps = self.fixed_steps orelse 8 + self.rand.uintLessThan(usize, 160);
        var i: usize = 0;
        while (i < steps) : (i += 1) {
            self.step += 1;
            if (self.rand.boolean()) try self.randomOp(&a, &b) else try self.randomOp(&b, &a);
        }

        // Close both transports: every open question must end exactly once.
        for ([_]*Side{ &a, &b }) |s| {
            if (s.conn) |cn| {
                abi.capnp_conn_transport_closed(cn);
                try self.drainAll(s);
                if (s.open.count() != 0) return self.fail("{d} question(s) still open after transport_closed", .{s.open.count()});
                // Everything after close is a no-op or CLOSED, never a crash.
                var qid: u32 = 0;
                _ = abi.capnp_bootstrap(cn, &qid);
                _ = abi.capnp_finish(cn, 0, 0);
                _ = abi.capnp_release(cn, 0, 1);
                _ = abi.capnp_cancel(cn, 0);
                abi.capnp_conn_shutdown(cn);
                _ = abi.capnp_conn_tick(cn, 1);
            }
        }
        // Free with effects queued and one in flight.
        for ([_]*Side{ &a, &b }) |s| {
            if (s.conn) |cn| {
                _ = abi.capnp_bootstrap(cn, &eid);
                var e: abi.capnp_effect = std.mem.zeroes(abi.capnp_effect);
                e.struct_size = effectSize();
                _ = abi.capnp_conn_next_effect(cn, &e);
                abi.capnp_conn_free(cn);
                s.conn = null;
            }
        }
        if (counting.live != live_before) return self.fail("{d} live bytes after both frees (before: {d})", .{ counting.live, live_before });
    }
};

fn elapsedNs(io: std.Io, since: std.Io.Timestamp) u64 {
    const d = since.durationTo(std.Io.Timestamp.now(io, .awake)).nanoseconds;
    return if (d < 0) 0 else @intCast(d);
}

fn loadSeeds(a: std.mem.Allocator) ![]const []const u8 {
    const json = @embedFile("fuzz_seeds_json");
    var parsed = try std.json.parseFromSlice(std.json.Value, a, json, .{});
    defer parsed.deinit();
    var out: std.ArrayList([]const u8) = .empty;
    const cases = parsed.value.object.get("cases") orelse return error.BadSeeds;
    for (cases.array.items) |case| {
        const chunks = case.object.get("chunks") orelse continue;
        for (chunks.array.items) |chunk| {
            const hex = chunk.string;
            const bytes = try a.alloc(u8, hex.len / 2);
            _ = try std.fmt.hexToBytes(bytes, hex);
            try out.append(a, bytes);
        }
    }
    return out.toOwnedSlice(a);
}

pub fn main(init: std.process.Init) !void {
    // The driver's own allocator: page-backed for the same reason as above.
    const a = std.heap.page_allocator;
    var seconds: u64 = 10;
    var seed: u64 = 0;
    var seed_given = false;
    var fixed_steps: ?usize = null;
    var op_mask: [32]bool = @splat(true);
    var it = try std.process.Args.Iterator.initAllocator(init.minimal.args, a);
    defer it.deinit();
    _ = it.skip();
    while (it.next()) |arg| {
        if (std.mem.eql(u8, arg, "--seconds")) {
            seconds = try std.fmt.parseInt(u64, it.next() orelse return error.MissingArgValue, 10);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            seed = try std.fmt.parseInt(u64, it.next() orelse return error.MissingArgValue, 10);
            seed_given = true;
        } else if (std.mem.eql(u8, arg, "--steps")) {
            fixed_steps = try std.fmt.parseInt(usize, it.next() orelse return error.MissingArgValue, 10);
        } else if (std.mem.eql(u8, arg, "--ops")) {
            op_mask = @splat(false);
            var ops_it = std.mem.splitScalar(u8, it.next() orelse return error.MissingArgValue, ',');
            while (ops_it.next()) |tok| op_mask[try std.fmt.parseInt(u8, tok, 10)] = true;
        } else {
            std.debug.print("usage: fuzz-abi [--seconds N] [--seed S] [--steps N] [--ops a,b,...]\n", .{});
            return error.UnknownArgument;
        }
    }
    if (!seed_given) {
        var buf: [8]u8 = undefined;
        init.io.random(&buf);
        seed = std.mem.readInt(u64, &buf, .little);
    }
    const seeds = try loadSeeds(a);
    defer {
        for (seeds) |s| a.free(s);
        a.free(seeds);
    }

    var prng = std.Random.DefaultPrng.init(seed);
    var fuzz = Fuzz{ .a = a, .rand = prng.random(), .seeds = seeds, .seed = seed, .fixed_steps = fixed_steps, .op_mask = op_mask };
    std.debug.print("fuzz-abi: seed {d}, {d} fixture chunks, budget {d} s\n", .{ seed, seeds.len, seconds });

    const io = init.io;
    const started = std.Io.Timestamp.now(io, .awake);
    const budget_ns = seconds * std.time.ns_per_s;
    var last_report: u64 = 0;
    while (elapsedNs(io, started) < budget_ns) {
        fuzz.session() catch |err| switch (err) {
            error.Violation => std.process.exit(1),
            else => return err,
        };
        const elapsed = elapsedNs(io, started);
        if (elapsed - last_report >= 60 * std.time.ns_per_s) {
            last_report = elapsed;
            std.debug.print("fuzz-abi: {d} s, {d} sessions, {d} ops, {d} frames, {d} returns, {d} protocol failures, {d} BUSY checks\n", .{ elapsed / std.time.ns_per_s, fuzz.sessions, fuzz.ops, fuzz.frames_pushed, fuzz.returns, fuzz.protocol_fails, fuzz.busy_seen });
        }
    }
    std.debug.print("fuzz-abi: OK: {d} sessions, {d} ops, {d} frames pushed, {d} returns, {d} protocol failures, {d} BUSY checks, peak {d} bytes live, 0 live now\n", .{ fuzz.sessions, fuzz.ops, fuzz.frames_pushed, fuzz.returns, fuzz.protocol_fails, fuzz.busy_seen, counting.peak });
    if (counting.live != 0) {
        std.debug.print("fuzz-abi: FAIL: {d} live bytes at exit\n", .{counting.live});
        std.process.exit(1);
    }
}
