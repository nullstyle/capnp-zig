//! The capnp-swift C ABI (`core/include/capnp_core.h`, Clang module `CapnpCore`).
//!
//! Every symbol here is `pub export fn capnp_*` with the C calling convention.
//! `pub` is load-bearing: the header-drift test at the bottom of this file
//! walks the public decls to find the exports, so a non-`pub` export would
//! escape it.
//!
//! Rules (plan §4.1): inputs are borrowed for the call; no mutable globals
//! except the process-wide panic hook below; the core never calls into Swift
//! except through that hook.
//!
//! Surface: version/feature queries, the panic hook, two test hooks
//! (`capnp_core_debug_trap`, `capnp_core_debug_selftest`) and the connection
//! API (C ABI v1: `capnp_conn_*` incl. `shutdown`, `capnp_bootstrap`,
//! `capnp_call` on imports and promised answers, `capnp_cancel`,
//! `capnp_set_deadline`, `capnp_finish`, `capnp_release`, `capnp_export`,
//! `capnp_set_bootstrap`, `capnp_promise_export`, `capnp_resolve_promise`,
//! `capnp_reject_promise`, `capnp_return_results`, `capnp_return_exception`,
//! `capnp_core_event_name`). The logic lives in `conn.zig` (Zig API, tested
//! by `conn_test.zig`); this file is the C-type adapter, tested through the
//! header by `abi_test.zig` (`zig build test-abi`) and, with a counting
//! allocator, by its own tests below.

const std = @import("std");
const builtin = @import("builtin");
const build_info = @import("build_info");
const capnp = @import("capnpc-zig");
const selftest = @import("selftest.zig");
const conn_mod = @import("conn.zig");
const effects = @import("effects.zig");

const rpc = capnp.rpc;
const Conn = conn_mod.Conn;

/// Must equal `CAPNP_CORE_ABI_VERSION` in `capnp_core.h` (checked by a test).
pub const abi_version: u32 = 1;

/// Feature bits reported by `capnp_core_features()`. None are defined in M0.
pub const features: u64 = 0;

/// "core <core version> / capnp-zig <pinned version> / <pinned hash>" (plan
/// §4, §10: each release reports the exact capnp-zig package it pins). All
/// three parts come from `core/build.zig.zon` at build time, so bumping the
/// pin updates the string.
pub const version_string: [:0]const u8 = "core " ++ build_info.core_version ++
    " / capnp-zig " ++ build_info.capnp_zig_version ++
    " / " ++ build_info.capnp_zig_hash;

/// Host panic hook: `void (*)(const char *msg, size_t len)`. `msg` is not
/// NUL-terminated and is valid only during the call. The hook must not call
/// back into the core; after it returns the core executes `@trap`.
pub const PanicHook = *const fn (msg: [*]const u8, len: usize) callconv(.c) void;

/// The one deliberate mutable global (plan §4.1): a panic is process-wide.
var panic_hook: std.atomic.Value(?PanicHook) = .init(null);

/// Called by the root panic handler (`apple_root.zig`) before it traps.
pub fn runPanicHook(msg: []const u8) void {
    if (panic_hook.load(.acquire)) |hook| hook(msg.ptr, msg.len);
}

// ---------------------------------------------------------------------------
// Version and features
// ---------------------------------------------------------------------------

pub export fn capnp_core_abi_version() callconv(.c) u32 {
    return abi_version;
}

pub export fn capnp_core_features() callconv(.c) u64 {
    return features;
}

/// Static, NUL-terminated; never freed.
pub export fn capnp_core_version() callconv(.c) [*:0]const u8 {
    return version_string.ptr;
}

/// The QUIC baseline ALPN capnp-zig freezes ("capnp-rpc/1"), read from the
/// pinned package's QUIC module (exported even with QUIC compiled out; plan
/// §4, H3). Static, NUL-terminated; never freed.
pub export fn capnp_core_quic_alpn() callconv(.c) [*:0]const u8 {
    return rpc.transport.quic.alpn.ptr;
}

// ---------------------------------------------------------------------------
// Panic hook
// ---------------------------------------------------------------------------

/// Installs (or, with null, clears) the host panic hook. Any thread.
pub export fn capnp_core_set_panic_hook(hook: ?PanicHook) callconv(.c) void {
    panic_hook.store(hook, .release);
}

// ---------------------------------------------------------------------------
// TEST HOOK -- not part of the supported API.
// ---------------------------------------------------------------------------

/// TEST HOOK ONLY. Executes `@trap` inside `debugTrapFrame` below so
/// `scripts/check-dsym.sh` can prove a trapping Zig frame symbolicates to
/// `core/src/abi.zig:<line>` from an app's dSYM. It does not run the panic
/// hook (a trap is not a panic). Never call it from production code.
pub export fn capnp_core_debug_trap() callconv(.c) noreturn {
    debugTrapFrame();
}

/// The frame `check-dsym.sh` expects to see. `noinline` keeps it a real frame
/// in every optimize mode; the script greps this file for the marker below to
/// learn the expected line, so the two cannot drift.
noinline fn debugTrapFrame() noreturn {
    @trap(); // CAPNP_CORE_DEBUG_TRAP_LINE
}

/// TEST HOOK ONLY. Runs `selftest.zig` (a bootstrap + call round trip between
/// two in-process connections) with the C allocator. Returns 0 on success;
/// otherwise -1 and, when `failure` is non-null, stores the static,
/// NUL-terminated error name there.
///
/// Until M1 adds the `capnp_conn_*` exports, this is also what keeps
/// `conn.zig` and the capnp-zig Peer linked into the XCFramework slices.
pub export fn capnp_core_debug_selftest(failure: ?*?[*:0]const u8) callconv(.c) i32 {
    if (failure) |f| f.* = null;
    selftest.run(std.heap.c_allocator) catch |err| {
        if (failure) |f| f.* = @errorName(err);
        return -1;
    };
    return 0;
}

// ---------------------------------------------------------------------------
// Connection API (C ABI v1): C types
// ---------------------------------------------------------------------------
//
// Every type and constant here mirrors `capnp_core.h`; the header-drift test
// at the bottom checks the function shapes and struct layouts, and the
// error-code test checks the `CAPNP_*` constants.

pub const CAPNP_OK: i32 = 0;
pub const CAPNP_E_INVAL: i32 = -1;
pub const CAPNP_E_BAD_ID: i32 = -2;
pub const CAPNP_E_BUSY: i32 = -3;
pub const CAPNP_E_CLOSED: i32 = -4;
pub const CAPNP_E_LIMIT: i32 = -5;
pub const CAPNP_E_PROTOCOL: i32 = -6;
pub const CAPNP_E_NOMEM: i32 = -7;
pub const CAPNP_E_INTERNAL: i32 = -8;

pub const CAPNP_FRAMING_SEGMENT_TABLE: u8 = 0;
pub const CAPNP_FRAMING_U32_LE: u8 = 1;

/// Opaque to C. Points at a `Handle`.
pub const capnp_conn = opaque {};

pub const capnp_conn_opts = extern struct {
    struct_size: u32,
    framing: u8,
    observer: u8,
    max_frame_bytes: u32,
    default_call_timeout_ms: u32,
    shutdown_drain_timeout_ms: u32,
    max_outbound_questions: u32,
    max_retained_questions: u32,
    max_active_inbound_questions: u32,
    // M2 (appended; struct_size versioning keeps M1 hosts working):
    max_pending_queued_calls: u32,
    max_pending_queued_call_bytes: u32,
    max_resolved_answers: u32,
    max_pending_promises: u32,
    max_pending_export_promises: u32,
    max_resolved_imports: u32,
};

/// `capnp_cap` is `effects.Cap` itself (an extern struct), so host `caps[]`
/// arrays and effect cap tables cross the ABI without a copy.
pub const capnp_cap = effects.Cap;

pub const capnp_effect = extern struct {
    struct_size: u32,
    id: u32,
    export_id: u32,
    kind: u8,
    return_kind: u8,
    event_tag: u8,
    exception_type: u16,
    method_id: u16,
    host_tag: u64,
    interface_id: u64,
    msg: ?[*]const u8,
    msg_len: usize,
    caps: ?[*]const capnp_cap,
    ncaps: usize,
    reason: ?[*]const u8,
    reason_len: usize,
};

/// Default shutdown drain (plan §4: `drain default 5000`).
const default_shutdown_drain_ms: u64 = 5000;

/// The allocator behind every connection the C ABI creates: the root's
/// `capnp_core_allocator` when it declares one (`apple_root.zig`: the C
/// allocator; `fuzz_abi.zig`: a leak-checking counter), else in tests a
/// counting wrapper around `std.testing.allocator` (so a test can require 0
/// live bytes after `capnp_conn_free`), else the C allocator.
const root = @import("root");
const gpa: std.mem.Allocator = if (@hasDecl(root, "capnp_core_allocator"))
    root.capnp_core_allocator
else if (builtin.is_test)
    test_counting.allocator()
else
    std.heap.c_allocator;

/// Live-byte counter for the test build (see `gpa`).
pub const CountingAllocator = struct {
    parent: std.mem.Allocator,
    live: usize = 0,
    peak: usize = 0,
    allocs: usize = 0,

    pub fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocFn, .resize = resizeFn, .remap = remapFn, .free = freeFn } };
    }

    fn grew(self: *CountingAllocator, old_len: usize, new_len: usize) void {
        self.live = self.live - old_len + new_len;
        self.peak = @max(self.peak, self.live);
    }

    fn allocFn(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const p = self.parent.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.allocs += 1;
        self.grew(0, len);
        return p;
    }

    fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        if (!self.parent.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.grew(memory.len, new_len);
        return true;
    }

    fn remapFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const p = self.parent.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.grew(memory.len, new_len);
        return p;
    }

    fn freeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.parent.rawFree(memory, alignment, ret_addr);
        self.live -= memory.len;
    }
};

var test_counting: CountingAllocator = .{ .parent = std.testing.allocator };

/// One C-visible connection: the `Conn` plus the re-entrancy guard (plan
/// §4.1: "a debug-only atomic in-call flag traps on re-entry"). Safe builds
/// (Debug, ReleaseSafe: the shipped slices) check it; fast builds skip it.
const Handle = struct {
    conn: *Conn,
    in_call: std.atomic.Value(bool) = .init(false),

    fn enter(h: *Handle) void {
        if (comptime std.debug.runtime_safety) {
            if (h.in_call.swap(true, .acquire))
                @panic("capnp_conn: concurrent or re-entrant call on one connection");
        }
    }

    fn leave(h: *Handle) void {
        if (comptime std.debug.runtime_safety) h.in_call.store(false, .release);
    }
};

fn handleOf(c: ?*capnp_conn) ?*Handle {
    return @ptrCast(@alignCast(c orelse return null));
}

/// Zig error -> `CAPNP_E_*` (plan §4.1's map, plus the Peer's retained-question
/// and limit errors).
fn codeFor(err: anyerror) i32 {
    return switch (err) {
        error.Busy,
        error.RetainedQuestionFinishInProgress,
        error.RetainedQuestionTransferInProgress,
        => CAPNP_E_BUSY,
        error.Closed,
        error.TransportClosed,
        error.ConnectionClosing,
        error.RemoteAbort,
        error.PeerShuttingDown,
        => CAPNP_E_CLOSED,
        error.BadId,
        error.BadCapId,
        error.CapIndexOutOfRange,
        error.UnknownQuestion,
        error.UnknownExport,
        error.UnknownRetainedQuestion,
        error.RetainedQuestionAlreadyReturned,
        error.RetainedQuestionAlreadyTransferred,
        error.RetainedQuestionNoFinishNeeded,
        error.RetainedQuestionNotTransferred,
        => CAPNP_E_BAD_ID,
        error.Protocol => CAPNP_E_PROTOCOL,
        error.Unsupported,
        error.UnsupportedCapKind,
        error.Invalid,
        error.ExportIsNotPromise,
        error.PromiseAlreadyResolved,
        error.RetainedQuestionPending,
        error.RetainedLoopbackQuestion,
        => CAPNP_E_INVAL,
        error.OutOfMemory => CAPNP_E_NOMEM,
        error.PeerLimitExceeded,
        error.ReleaseCountExceeded,
        error.ValidationBudgetExceeded,
        => CAPNP_E_LIMIT,
        else => if (std.mem.endsWith(u8, @errorName(err), "Exceeded")) CAPNP_E_LIMIT else CAPNP_E_INTERNAL,
    };
}

fn ptrOrNull(comptime T: type, slice: []const T) ?[*]const T {
    return if (slice.len == 0) null else slice.ptr;
}

/// `bytes[0..len]`, or null when the C caller passed NULL with a nonzero
/// length (an empty slice for NULL + 0).
fn sliceArg(comptime T: type, ptr: ?[*]const T, len: usize) ?[]const T {
    if (ptr) |p| return p[0..len];
    return if (len == 0) &.{} else null;
}

// ---------------------------------------------------------------------------
// Connection API (C ABI v1): functions
// ---------------------------------------------------------------------------

pub export fn capnp_conn_new(opts: ?*const capnp_conn_opts, now_uptime_ns: i64, out: ?*?*capnp_conn) callconv(.c) i32 {
    const out_ptr = out orelse return CAPNP_E_INVAL;
    out_ptr.* = null;
    const given = opts orelse return CAPNP_E_INVAL;

    // struct_size versioning: read only the prefix the caller has.
    var o: capnp_conn_opts = std.mem.zeroes(capnp_conn_opts);
    if (given.struct_size < @sizeOf(u32)) return CAPNP_E_INVAL;
    const n = @min(given.struct_size, @sizeOf(capnp_conn_opts));
    @memcpy(std.mem.asBytes(&o)[0..n], @as([*]const u8, @ptrCast(given))[0..n]);
    const framing: @import("conn.zig").Framing = switch (o.framing) {
        CAPNP_FRAMING_SEGMENT_TABLE => .segment_table,
        CAPNP_FRAMING_U32_LE => .u32_le,
        else => return CAPNP_E_INVAL,
    };

    var limits: rpc.peer.PeerLimits = .{};
    if (o.max_outbound_questions != 0) limits.max_outbound_questions = o.max_outbound_questions;
    if (o.max_retained_questions != 0) limits.max_retained_questions = o.max_retained_questions;
    if (o.max_active_inbound_questions != 0) limits.max_active_inbound_questions = o.max_active_inbound_questions;
    if (o.max_pending_queued_calls != 0) limits.max_pending_queued_calls = o.max_pending_queued_calls;
    if (o.max_pending_queued_call_bytes != 0) limits.max_pending_queued_call_bytes = o.max_pending_queued_call_bytes;
    if (o.max_resolved_answers != 0) limits.max_resolved_answers = o.max_resolved_answers;
    if (o.max_pending_promises != 0) limits.max_pending_promises = o.max_pending_promises;
    if (o.max_pending_export_promises != 0) limits.max_pending_export_promises = o.max_pending_export_promises;
    if (o.max_resolved_imports != 0) limits.max_resolved_imports = o.max_resolved_imports;
    const timeouts: rpc.peer.PeerTimeouts = .{
        .default_call_timeout_ms = if (o.default_call_timeout_ms != 0) o.default_call_timeout_ms else null,
        .shutdown_drain_timeout_ms = if (o.shutdown_drain_timeout_ms != 0) o.shutdown_drain_timeout_ms else default_shutdown_drain_ms,
    };

    const h = gpa.create(Handle) catch return CAPNP_E_NOMEM;
    const conn = Conn.init(gpa, .{
        .now_ns = now_uptime_ns,
        .limits = limits,
        .timeouts = timeouts,
        .max_frame_bytes = if (o.max_frame_bytes != 0) o.max_frame_bytes else rpc.wire.framing.Framer.default_max_buffered_bytes,
        .observer = o.observer != 0,
        .framing = framing,
    }) catch |err| {
        gpa.destroy(h);
        return codeFor(err);
    };
    h.* = .{ .conn = conn };
    out_ptr.* = @ptrCast(h);
    return CAPNP_OK;
}

pub export fn capnp_conn_free(c: ?*capnp_conn) callconv(.c) void {
    const h = handleOf(c) orelse return;
    h.enter();
    h.conn.deinit();
    // No `leave`: the handle is gone. A concurrent caller would have tripped
    // `enter` first.
    gpa.destroy(h);
}

pub export fn capnp_conn_push_bytes(c: ?*capnp_conn, bytes: ?[*]const u8, len: usize) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    const data = sliceArg(u8, bytes, len) orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    h.conn.pushBytes(data) catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_conn_tick(c: ?*capnp_conn, now_uptime_ns: i64) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    const ended = h.conn.tick(now_uptime_ns);
    return @intCast(@min(ended, std.math.maxInt(i32)));
}

pub export fn capnp_conn_transport_closed(c: ?*capnp_conn) callconv(.c) void {
    const h = handleOf(c) orelse return;
    h.enter();
    defer h.leave();
    h.conn.transportClosed();
}

pub export fn capnp_conn_take_error(
    c: ?*capnp_conn,
    code: ?*i32,
    name: ?*?[*]const u8,
    name_len: ?*usize,
    detail: ?*?[*]const u8,
    detail_len: ?*usize,
) callconv(.c) i32 {
    if (code) |p| p.* = CAPNP_OK;
    if (name) |p| p.* = null;
    if (name_len) |p| p.* = 0;
    if (detail) |p| p.* = null;
    if (detail_len) |p| p.* = 0;
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    const err = h.conn.last_error orelse return 0;
    h.conn.last_error = null;
    const err_name = @errorName(err);
    // The code names the failure class; the name is the exact cause. A
    // protocol failure stores its cause (e.g. `InvalidFrame`), so the class
    // comes from the connection state, not from the error value.
    const class: i32 = if (err == error.RemoteAbort)
        CAPNP_E_CLOSED
    else if (h.conn.failed)
        CAPNP_E_PROTOCOL
    else
        codeFor(err);
    if (code) |p| p.* = class;
    if (name) |p| p.* = err_name.ptr;
    if (name_len) |p| p.* = err_name.len;
    if (err == error.RemoteAbort) {
        if (h.conn.remote_abort_reason) |reason| {
            if (detail) |p| p.* = ptrOrNull(u8, reason);
            if (detail_len) |p| p.* = reason.len;
        }
    }
    return 1;
}

pub export fn capnp_conn_next_effect(c: ?*capnp_conn, out: ?*capnp_effect) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    const o = out orelse return CAPNP_E_INVAL;
    if (o.struct_size < @sizeOf(u32)) return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    const eff = (h.conn.nextEffect() catch return CAPNP_E_BUSY) orelse return 0;

    var e: capnp_effect = std.mem.zeroes(capnp_effect);
    switch (eff.*) {
        .out_frame => |bytes| {
            e.kind = @backingInt(effects.Kind.out_frame);
            e.msg = ptrOrNull(u8, bytes);
            e.msg_len = bytes.len;
        },
        .close_requested => e.kind = @backingInt(effects.Kind.close_requested),
        .@"return" => |r| {
            e.kind = @backingInt(effects.Kind.@"return");
            e.id = r.qid;
            e.return_kind = @backingInt(r.kind);
            e.exception_type = r.exception_type;
            e.msg = ptrOrNull(u8, r.msg);
            e.msg_len = r.msg.len;
            e.caps = ptrOrNull(capnp_cap, r.caps);
            e.ncaps = r.caps.len;
            e.reason = ptrOrNull(u8, r.reason);
            e.reason_len = r.reason.len;
        },
        .inbound_call => |ic| {
            e.kind = @backingInt(effects.Kind.inbound_call);
            e.id = ic.answer_id;
            e.export_id = ic.export_id;
            e.host_tag = ic.host_tag;
            e.interface_id = ic.interface_id;
            e.method_id = ic.method_id;
            e.msg = ptrOrNull(u8, ic.msg);
            e.msg_len = ic.msg.len;
            e.caps = ptrOrNull(capnp_cap, ic.caps);
            e.ncaps = ic.caps.len;
        },
        .export_dropped => |d| {
            e.kind = @backingInt(effects.Kind.export_dropped);
            e.id = d.export_id;
            e.export_id = d.export_id;
            e.host_tag = d.host_tag;
        },
        .event => |ev| {
            e.kind = @backingInt(effects.Kind.event);
            e.event_tag = ev.tag;
            e.reason = ptrOrNull(u8, ev.err_name);
            e.reason_len = ev.err_name.len;
        },
    }
    // struct_size versioning: fill only what the caller has room for, and
    // tell it how much that was.
    const n: u32 = @intCast(@min(o.struct_size, @sizeOf(capnp_effect)));
    e.struct_size = n;
    @memcpy(@as([*]u8, @ptrCast(o))[0..n], std.mem.asBytes(&e)[0..n]);
    return 1;
}

pub export fn capnp_conn_commit_effect(c: ?*capnp_conn) callconv(.c) void {
    const h = handleOf(c) orelse return;
    h.enter();
    defer h.leave();
    h.conn.commitEffect();
}

pub export fn capnp_bootstrap(c: ?*capnp_conn, out_qid: ?*u32) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    const out = out_qid orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    out.* = h.conn.bootstrap() catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_call(
    c: ?*capnp_conn,
    target: capnp_cap,
    interface_id: u64,
    method_id: u16,
    msg: ?[*]const u8,
    msg_len: usize,
    caps: ?[*]const capnp_cap,
    ncaps: usize,
    flags: u32,
    out_qid: ?*u32,
) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    const out = out_qid orelse return CAPNP_E_INVAL;
    const params = sliceArg(u8, msg, msg_len) orelse return CAPNP_E_INVAL;
    if (params.len == 0) return CAPNP_E_INVAL; // a params message is required
    const cap_table = sliceArg(capnp_cap, caps, ncaps) orelse return CAPNP_E_INVAL;
    if (!validCapKinds(cap_table)) return CAPNP_E_INVAL;
    if (!validCap(target) or (target.kind != .import and target.kind != .promised)) return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    out.* = h.conn.call(target, interface_id, method_id, params, cap_table, flags) catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_cancel(c: ?*capnp_conn, qid: u32) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    h.conn.cancel(qid) catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_set_deadline(c: ?*capnp_conn, qid: u32, timeout_ms: u32) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    h.conn.setDeadline(qid, timeout_ms) catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_conn_shutdown(c: ?*capnp_conn) callconv(.c) void {
    const h = handleOf(c) orelse return;
    h.enter();
    defer h.leave();
    h.conn.shutdown();
}

pub export fn capnp_promise_export(c: ?*capnp_conn, out_promise_id: ?*u32) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    const out = out_promise_id orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    out.* = h.conn.promiseExport() catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_resolve_promise(c: ?*capnp_conn, promise_id: u32, to: capnp_cap) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    if (!validCap(to) or (to.kind != .import and to.kind != .@"export")) return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    h.conn.resolvePromise(promise_id, to) catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_reject_promise(c: ?*capnp_conn, promise_id: u32, reason: ?[*]const u8, reason_len: usize) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    const text = sliceArg(u8, reason, reason_len) orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    h.conn.rejectPromise(promise_id, text) catch |err| return codeFor(err);
    return CAPNP_OK;
}

/// Static, NUL-terminated name of a Peer observer event tag ("unknown" for
/// a tag this core does not know).
pub export fn capnp_core_event_name(tag: u8) callconv(.c) [*:0]const u8 {
    return conn_mod.eventName(tag).ptr;
}

pub export fn capnp_finish(c: ?*capnp_conn, qid: u32, release_result_caps: i32) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    h.conn.finish(qid, release_result_caps != 0) catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_release(c: ?*capnp_conn, import_id: u32, count: u32) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    h.conn.release(import_id, count) catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_export(c: ?*capnp_conn, host_tag: u64, out_export_id: ?*u32) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    const out = out_export_id orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    out.* = h.conn.exportCap(host_tag) catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_set_bootstrap(c: ?*capnp_conn, host_tag: u64, out_export_id: ?*u32) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    const out = out_export_id orelse return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    out.* = h.conn.setBootstrap(host_tag) catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_return_results(
    c: ?*capnp_conn,
    answer_id: u32,
    msg: ?[*]const u8,
    msg_len: usize,
    caps: ?[*]const capnp_cap,
    ncaps: usize,
) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    const results = sliceArg(u8, msg, msg_len) orelse return CAPNP_E_INVAL;
    if (results.len == 0) return CAPNP_E_INVAL;
    const cap_table = sliceArg(capnp_cap, caps, ncaps) orelse return CAPNP_E_INVAL;
    if (!validCapKinds(cap_table)) return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    h.conn.returnResults(answer_id, results, cap_table) catch |err| return codeFor(err);
    return CAPNP_OK;
}

pub export fn capnp_return_exception(
    c: ?*capnp_conn,
    answer_id: u32,
    exception_type: u16,
    reason: ?[*]const u8,
    reason_len: usize,
) callconv(.c) i32 {
    const h = handleOf(c) orelse return CAPNP_E_INVAL;
    const text = sliceArg(u8, reason, reason_len) orelse return CAPNP_E_INVAL;
    // rpc.capnp Exception.Type has four values; `ExceptionType` is exhaustive.
    if (exception_type > @backingInt(rpc.wire.protocol.ExceptionType.unimplemented)) return CAPNP_E_INVAL;
    h.enter();
    defer h.leave();
    h.conn.returnException(answer_id, exception_type, text) catch |err| return codeFor(err);
    return CAPNP_OK;
}

/// The host's `caps[]` may only hold the four known kinds (a stray byte from
/// C would otherwise reach a Zig `enum(u8)` switch), and a PROMISED entry
/// with ops must point at them.
fn validCapKinds(caps: []const capnp_cap) bool {
    for (caps) |cap| {
        if (!validCap(cap)) return false;
    }
    return true;
}

fn validCap(cap: capnp_cap) bool {
    // Read the kind as the byte C wrote, never as the enum (a safe-mode
    // load of an invalid enum value would trap before the check).
    const raw: u8 = @as(*const u8, @ptrCast(&cap.kind)).*;
    if (raw > @backingInt(effects.CapKind.promised)) return false;
    if (cap.nops != 0 and cap.ops == null) return false;
    return true;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test {
    // selftest.zig's leak-checked run (testing.allocator).
    _ = selftest;
}

test "debug selftest export: 0 and no failure name" {
    var failure: ?[*:0]const u8 = "unset";
    try testing.expectEqual(@as(i32, 0), capnp_core_debug_selftest(&failure));
    try testing.expect(failure == null);
    try testing.expectEqual(@as(i32, 0), capnp_core_debug_selftest(null));
}

test "capnp_core.h constants match the Zig values" {
    const c = @import("capnp_core_h");
    try testing.expectEqual(CAPNP_OK, @as(i32, c.CAPNP_OK));
    try testing.expectEqual(CAPNP_E_INVAL, @as(i32, c.CAPNP_E_INVAL));
    try testing.expectEqual(CAPNP_E_BAD_ID, @as(i32, c.CAPNP_E_BAD_ID));
    try testing.expectEqual(CAPNP_E_BUSY, @as(i32, c.CAPNP_E_BUSY));
    try testing.expectEqual(CAPNP_E_CLOSED, @as(i32, c.CAPNP_E_CLOSED));
    try testing.expectEqual(CAPNP_E_LIMIT, @as(i32, c.CAPNP_E_LIMIT));
    try testing.expectEqual(CAPNP_E_PROTOCOL, @as(i32, c.CAPNP_E_PROTOCOL));
    try testing.expectEqual(CAPNP_E_NOMEM, @as(i32, c.CAPNP_E_NOMEM));
    try testing.expectEqual(CAPNP_E_INTERNAL, @as(i32, c.CAPNP_E_INTERNAL));
    try testing.expectEqual(CAPNP_FRAMING_SEGMENT_TABLE, @as(u8, c.CAPNP_FRAMING_SEGMENT_TABLE));
    // Cap kinds, effect kinds and return kinds are the ordinals effects.zig uses.
    try testing.expectEqual(@backingInt(effects.CapKind.none), @as(u8, c.CAPNP_CAP_NONE));
    try testing.expectEqual(@backingInt(effects.CapKind.import), @as(u8, c.CAPNP_CAP_IMPORT));
    try testing.expectEqual(@backingInt(effects.CapKind.@"export"), @as(u8, c.CAPNP_CAP_EXPORT));
    try testing.expectEqual(@backingInt(effects.CapKind.promised), @as(u8, c.CAPNP_CAP_PROMISED));
    try testing.expectEqual(@backingInt(effects.Kind.out_frame), @as(u8, c.CAPNP_EFFECT_OUT_FRAME));
    try testing.expectEqual(@backingInt(effects.Kind.close_requested), @as(u8, c.CAPNP_EFFECT_CLOSE_REQUESTED));
    try testing.expectEqual(@backingInt(effects.Kind.@"return"), @as(u8, c.CAPNP_EFFECT_RETURN));
    try testing.expectEqual(@backingInt(effects.Kind.inbound_call), @as(u8, c.CAPNP_EFFECT_INBOUND_CALL));
    try testing.expectEqual(@backingInt(effects.Kind.export_dropped), @as(u8, c.CAPNP_EFFECT_EXPORT_DROPPED));
    try testing.expectEqual(@backingInt(effects.Kind.event), @as(u8, c.CAPNP_EFFECT_EVENT));
    try testing.expectEqual(@backingInt(effects.ReturnKind.results), @as(u8, c.CAPNP_RETURN_RESULTS));
    try testing.expectEqual(@backingInt(effects.ReturnKind.exception), @as(u8, c.CAPNP_RETURN_EXCEPTION));
    try testing.expectEqual(@backingInt(effects.ReturnKind.canceled), @as(u8, c.CAPNP_RETURN_CANCELED));
    try testing.expectEqual(@backingInt(effects.ReturnKind.disconnected), @as(u8, c.CAPNP_RETURN_DISCONNECTED));
    // The two shared structs have the header's layout (the drift test below
    // checks them again through every prototype that names them).
    try testing.expectEqual(@sizeOf(c.capnp_cap), @sizeOf(capnp_cap));
    try testing.expectEqual(@sizeOf(c.capnp_effect), @sizeOf(capnp_effect));
    try testing.expectEqual(@sizeOf(c.capnp_conn_opts), @sizeOf(capnp_conn_opts));
}

test "codeFor: the plan's error map" {
    try testing.expectEqual(CAPNP_E_BUSY, codeFor(error.Busy));
    try testing.expectEqual(CAPNP_E_CLOSED, codeFor(error.Closed));
    try testing.expectEqual(CAPNP_E_CLOSED, codeFor(error.RemoteAbort));
    try testing.expectEqual(CAPNP_E_BAD_ID, codeFor(error.BadId));
    try testing.expectEqual(CAPNP_E_BAD_ID, codeFor(error.BadCapId));
    try testing.expectEqual(CAPNP_E_BAD_ID, codeFor(error.CapIndexOutOfRange));
    try testing.expectEqual(CAPNP_E_BAD_ID, codeFor(error.UnknownRetainedQuestion));
    try testing.expectEqual(CAPNP_E_PROTOCOL, codeFor(error.Protocol));
    try testing.expectEqual(CAPNP_E_INVAL, codeFor(error.Unsupported));
    try testing.expectEqual(CAPNP_E_INVAL, codeFor(error.UnsupportedCapKind));
    try testing.expectEqual(CAPNP_E_INVAL, codeFor(error.Invalid));
    try testing.expectEqual(CAPNP_E_NOMEM, codeFor(error.OutOfMemory));
    try testing.expectEqual(CAPNP_E_LIMIT, codeFor(error.PeerLimitExceeded));
    try testing.expectEqual(CAPNP_E_LIMIT, codeFor(error.SomethingElseExceeded));
    try testing.expectEqual(CAPNP_E_INTERNAL, codeFor(error.Unexpected));
    try testing.expectEqual(CAPNP_E_BAD_ID, codeFor(error.UnknownQuestion));
    try testing.expectEqual(CAPNP_E_BAD_ID, codeFor(error.UnknownExport));
    try testing.expectEqual(CAPNP_E_INVAL, codeFor(error.PromiseAlreadyResolved));
    try testing.expectEqual(CAPNP_E_CLOSED, codeFor(error.PeerShuttingDown));
}

test "counting allocator: 0 live bytes after capnp_conn_free, through every M1/M2 export" {
    // The test build routes `gpa` through `test_counting` (see `gpa`): this
    // scenario touches every export, frees both connections with effects
    // still queued and one in flight, and requires nothing to stay live.
    if (@hasDecl(root, "capnp_core_allocator")) return error.SkipZigTest;
    const live_before = test_counting.live;
    var opts: capnp_conn_opts = std.mem.zeroes(capnp_conn_opts);
    opts.struct_size = @sizeOf(capnp_conn_opts);
    opts.shutdown_drain_timeout_ms = 100;
    var a: ?*capnp_conn = null;
    var b: ?*capnp_conn = null;
    try testing.expectEqual(CAPNP_OK, capnp_conn_new(&opts, 0, &a));
    try testing.expectEqual(CAPNP_OK, capnp_conn_new(&opts, 0, &b));
    try testing.expect(test_counting.live > live_before);

    var eb: u32 = 0;
    try testing.expectEqual(CAPNP_OK, capnp_set_bootstrap(b, 100, &eb));
    var promise: u32 = 0;
    try testing.expectEqual(CAPNP_OK, capnp_promise_export(b, &promise));
    var q0: u32 = 0;
    try testing.expectEqual(CAPNP_OK, capnp_bootstrap(a, &q0));
    try countingPump(a.?, b.?);
    // A's bootstrap import is B's bootstrap export id.
    var ea: u32 = 0;
    try testing.expectEqual(CAPNP_OK, capnp_export(a, 200, &ea));
    // Test data comes from the testing allocator directly, not from `gpa`,
    // so it does not count as the core's live bytes.
    const params = try selftestStructU64(testing.allocator, 41);
    defer testing.allocator.free(params);
    const target: capnp_cap = .{ .kind = .import, .id = eb };
    var q1: u32 = 0;
    const caps = [_]capnp_cap{.{ .kind = .@"export", .id = ea }};
    try testing.expectEqual(CAPNP_OK, capnp_call(a, target, 1, 0, params.ptr, params.len, &caps, caps.len, 0, &q1));
    const path = [_]u16{0};
    var q2: u32 = 0;
    try testing.expectEqual(CAPNP_OK, capnp_call(a, .{ .kind = .promised, .id = q1, .ops = &path, .nops = 1 }, 1, 1, params.ptr, params.len, null, 0, 0, &q2));
    try testing.expectEqual(CAPNP_OK, capnp_set_deadline(a, q2, 5_000));
    try testing.expectEqual(CAPNP_OK, capnp_cancel(a, q2));
    try countingPump(a.?, b.?);
    try testing.expectEqual(CAPNP_OK, capnp_resolve_promise(b, promise, .{ .kind = .@"export", .id = eb }));
    try testing.expectEqual(CAPNP_E_INVAL, capnp_reject_promise(b, promise, "x", 1)); // already resolved
    _ = capnp_conn_tick(a, 10);
    capnp_conn_shutdown(a);
    try countingPump(a.?, b.?);
    capnp_conn_transport_closed(a);
    // Free with queued effects and one in flight on each side.
    var e: capnp_effect = std.mem.zeroes(capnp_effect);
    e.struct_size = @sizeOf(capnp_effect);
    _ = capnp_conn_next_effect(a, &e);
    _ = capnp_conn_next_effect(b, &e);
    capnp_conn_free(a);
    capnp_conn_free(b);
    try testing.expectEqual(live_before, test_counting.live);
}

/// Moves OUT_FRAMEs between two C connections until both queues are empty,
/// committing every other effect unread.
fn countingPump(a: *capnp_conn, b: *capnp_conn) !void {
    var rounds: usize = 0;
    while (rounds < 10_000) : (rounds += 1) {
        const moved_a = try countingDrainOne(a, b);
        const moved_b = try countingDrainOne(b, a);
        if (!moved_a and !moved_b) return;
    }
    return error.PumpRunaway;
}

fn countingDrainOne(src: *capnp_conn, dst: *capnp_conn) !bool {
    var e: capnp_effect = std.mem.zeroes(capnp_effect);
    e.struct_size = @sizeOf(capnp_effect);
    const rc = capnp_conn_next_effect(src, &e);
    if (rc == 0) return false;
    try testing.expectEqual(@as(i32, 1), rc);
    defer capnp_conn_commit_effect(src);
    if (e.kind == @backingInt(effects.Kind.out_frame)) {
        _ = capnp_conn_push_bytes(dst, e.msg, e.msg_len);
    }
    return true;
}

fn selftestStructU64(allocator: std.mem.Allocator, v: u64) ![]const u8 {
    var mb = capnp.message.MessageBuilder.init(allocator);
    defer mb.deinit();
    const root_struct = try mb.allocateStruct(1, 0);
    root_struct.writeU64(0, v);
    return mb.toBytes();
}

test "event names" {
    try testing.expectEqualStrings("connection", std.mem.span(capnp_core_event_name(0)));
    try testing.expectEqualStrings("protocol_error", std.mem.span(capnp_core_event_name(4)));
    try testing.expectEqualStrings("cancel_failure", std.mem.span(capnp_core_event_name(9)));
    try testing.expectEqualStrings("unknown", std.mem.span(capnp_core_event_name(200)));
}

test "abi version, features and version string" {
    try testing.expectEqual(@as(u32, 1), capnp_core_abi_version());
    try testing.expectEqual(@as(u64, 0), capnp_core_features());
    const v = std.mem.span(capnp_core_version());
    // The exact pin (core/build.zig.zon). Bumping the pin updates this line,
    // CapnpCoreInfoTests.swift and the zon together.
    try testing.expectEqualStrings(
        "core 0.1.0 / capnp-zig 0.21.0 / capnpc_zig-0.21.0-nUduFa7PRwBrWc9CyzM8u052LMqBhHzvA52R1s407iCg",
        v,
    );
    try testing.expect(std.mem.endsWith(u8, v, " / " ++ build_info.capnp_zig_hash));
    // The pinned version is derived from the zon hash; it must look like one.
    try testing.expect(std.mem.startsWith(u8, build_info.capnp_zig_hash, "capnpc_zig-" ++ build_info.capnp_zig_version ++ "-"));
}

var test_hook_calls: usize = 0;
var test_hook_buf: [64]u8 = undefined;
var test_hook_len: usize = 0;

fn testHook(msg: [*]const u8, len: usize) callconv(.c) void {
    test_hook_calls += 1;
    test_hook_len = @min(len, test_hook_buf.len);
    @memcpy(test_hook_buf[0..test_hook_len], msg[0..test_hook_len]);
}

test "panic hook: set, run, clear" {
    defer capnp_core_set_panic_hook(null);
    test_hook_calls = 0;

    runPanicHook("no hook installed");
    try testing.expectEqual(@as(usize, 0), test_hook_calls);

    capnp_core_set_panic_hook(&testHook);
    runPanicHook("index out of bounds");
    try testing.expectEqual(@as(usize, 1), test_hook_calls);
    try testing.expectEqualStrings("index out of bounds", test_hook_buf[0..test_hook_len]);

    capnp_core_set_panic_hook(null);
    runPanicHook("cleared");
    try testing.expectEqual(@as(usize, 1), test_hook_calls);
}

// Header-drift gate: every `pub export fn capnp_*` here has a prototype in
// `capnp_core.h` with the same arity and scalar widths (via translate-c), and
// every function the header declares exists here. The header's
// `CAPNP_CORE_ABI_VERSION` must equal `abi_version`.
test "capnp_core.h matches the Zig exports" {
    const c = @import("capnp_core_h");
    const this = @This();

    try testing.expectEqual(abi_version, @as(u32, c.CAPNP_CORE_ABI_VERSION));

    var zig_names: [256][]const u8 = undefined;
    var n_zig: usize = 0;
    inline for (@typeInfo(this).@"struct".decl_names) |name| {
        if (comptime !std.mem.startsWith(u8, name, "capnp_")) continue;
        const ZigFn = @TypeOf(@field(this, name));
        if (@typeInfo(ZigFn) != .@"fn") continue;
        if (!@hasDecl(c, name)) {
            std.debug.print("export {s} has no prototype in capnp_core.h\n", .{name});
            return error.HeaderMissingPrototype;
        }
        try expectSameCShape(name, ZigFn, @TypeOf(@field(c, name)));
        if (n_zig == zig_names.len) return error.TooManyExportsForDriftTest;
        zig_names[n_zig] = name;
        n_zig += 1;
    }

    // Reverse direction: scan the header text for `capnp_xxx(` prototypes.
    const header = @embedFile("capnp_core_h_text");
    var it = HeaderFnNames{ .src = header };
    var n_header: usize = 0;
    while (it.next()) |name| {
        n_header += 1;
        for (zig_names[0..n_zig]) |zn| {
            if (std.mem.eql(u8, zn, name)) break;
        } else {
            std.debug.print("capnp_core.h declares {s} but abi.zig does not export it\n", .{name});
            return error.HeaderExtraPrototype;
        }
    }
    try testing.expectEqual(n_zig, n_header);
}

fn expectSameCShape(name: []const u8, comptime ZigFn: type, comptime CFn: type) !void {
    const z = @typeInfo(ZigFn).@"fn";
    const h = @typeInfo(CFn).@"fn";
    if (!std.meta.eql(z.attrs.@"callconv", h.attrs.@"callconv")) {
        std.debug.print("{s}: calling convention differs from capnp_core.h\n", .{name});
        return error.HeaderCallconvMismatch;
    }
    if (z.param_types.len != h.param_types.len) {
        std.debug.print("{s}: {d} params in Zig, {d} in capnp_core.h\n", .{ name, z.param_types.len, h.param_types.len });
        return error.HeaderArityMismatch;
    }
    inline for (z.param_types, h.param_types, 0..) |zp, hp, i| {
        if (comptime !sameCShape(zp.?, hp.?, 0)) {
            std.debug.print("{s}: param {d} is {s} in Zig, {s} in capnp_core.h\n", .{ name, i, @typeName(zp.?), @typeName(hp.?) });
            return error.HeaderParamMismatch;
        }
    }
    if (comptime !sameCShape(z.return_type.?, h.return_type.?, 0)) {
        std.debug.print("{s}: returns {s} in Zig, {s} in capnp_core.h\n", .{ name, @typeName(z.return_type.?), @typeName(h.return_type.?) });
        return error.HeaderReturnMismatch;
    }
}

/// C-compatibility of a Zig type `Z` and its translate-c twin `H`: ints agree
/// on size and signedness; pointers agree on what they point at (function
/// pointers by signature, structs by layout, opaque handles by opaqueness);
/// optional pointers match plain ones (C has no non-null pointers); structs
/// agree field by field and offset by offset; `noreturn` matches `void`.
fn sameCShape(comptime Z: type, comptime H: type, comptime depth: u8) bool {
    if (depth > 6) return true; // self-referential layouts: stop descending
    if (Z == noreturn) return H == void or H == noreturn;
    if (Z == void or H == void) return Z == H;
    if (comptime pointee(Z)) |zc| {
        const hc = comptime pointee(H) orelse return false;
        return samePointee(zc, hc, depth + 1);
    }
    if (comptime pointee(H) != null) return false;
    const zi = @typeInfo(Z);
    const hi = @typeInfo(H);
    return switch (zi) {
        .int => |a| hi == .int and hi.int.bits == a.bits and hi.int.signedness == a.signedness,
        .@"enum" => |e| sameCShape(e.tag_type, H, depth + 1),
        .bool => (hi == .bool or hi == .int) and @sizeOf(Z) == @sizeOf(H),
        .float => |f| hi == .float and hi.float.bits == f.bits,
        .@"struct" => sameStruct(Z, H, depth + 1),
        else => Z == H,
    };
}

fn pointee(comptime T: type) ?type {
    return switch (@typeInfo(T)) {
        .pointer => |p| p.child,
        .optional => |o| switch (@typeInfo(o.child)) {
            .pointer => |p| p.child,
            else => null,
        },
        else => null,
    };
}

fn samePointee(comptime Z: type, comptime H: type, comptime depth: u8) bool {
    const zi = @typeInfo(Z);
    const hi = @typeInfo(H);
    if (zi == .@"fn" or hi == .@"fn") {
        if (zi != .@"fn" or hi != .@"fn") return false;
        const z = zi.@"fn";
        const h = hi.@"fn";
        if (!std.meta.eql(z.attrs.@"callconv", h.attrs.@"callconv")) return false;
        if (z.param_types.len != h.param_types.len) return false;
        for (z.param_types, h.param_types) |zp, hp| {
            if (!sameCShape(zp.?, hp.?, depth + 1)) return false;
        }
        return sameCShape(z.return_type.?, h.return_type.?, depth + 1);
    }
    if (zi == .@"opaque" or hi == .@"opaque") return zi == .@"opaque" and hi == .@"opaque";
    return sameCShape(Z, H, depth + 1);
}

fn sameStruct(comptime Z: type, comptime H: type, comptime depth: u8) bool {
    if (@typeInfo(H) != .@"struct") return false;
    if (@sizeOf(Z) != @sizeOf(H) or @alignOf(Z) != @alignOf(H)) return false;
    const z = @typeInfo(Z).@"struct";
    const h = @typeInfo(H).@"struct";
    if (z.field_names.len != h.field_names.len) return false;
    for (z.field_names, h.field_names, z.field_types, h.field_types) |zn, hn, zt, ht| {
        if (@offsetOf(Z, zn) != @offsetOf(H, hn)) return false;
        if (!sameCShape(zt, ht, depth + 1)) return false;
    }
    return true;
}

/// Yields `capnp_*` identifiers that are immediately followed by `(` in the
/// header, skipping comments. Function-pointer typedefs (`(*capnp_x)(`) and
/// macros are not matched.
const HeaderFnNames = struct {
    src: []const u8,
    i: usize = 0,

    fn next(self: *HeaderFnNames) ?[]const u8 {
        const s = self.src;
        while (self.i < s.len) {
            if (std.mem.startsWith(u8, s[self.i..], "/*")) {
                const end = std.mem.indexOfPos(u8, s, self.i + 2, "*/") orelse s.len;
                self.i = @min(end + 2, s.len);
                continue;
            }
            if (std.mem.startsWith(u8, s[self.i..], "//")) {
                self.i = std.mem.indexOfScalarPos(u8, s, self.i, '\n') orelse s.len;
                continue;
            }
            const prev_is_ident = self.i > 0 and isIdent(s[self.i - 1]);
            if (!prev_is_ident and std.mem.startsWith(u8, s[self.i..], "capnp_")) {
                const start = self.i;
                while (self.i < s.len and isIdent(s[self.i])) self.i += 1;
                var j = self.i;
                while (j < s.len and (s[j] == ' ' or s[j] == '\t')) j += 1;
                if (j < s.len and s[j] == '(') return s[start..self.i];
                continue;
            }
            self.i += 1;
        }
        return null;
    }

    fn isIdent(ch: u8) bool {
        return std.ascii.isAlphanumeric(ch) or ch == '_';
    }
};
