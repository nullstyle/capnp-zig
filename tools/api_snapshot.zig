//! Public API snapshot tool — two-tier (Stable / Experimental).
//!
//! Walks the library's `pub` declaration tree at comptime and emits a
//! sorted, line-oriented description of the API surface: declaration paths
//! plus type/function signatures. Each declaration is assigned a stability
//! TIER by `tierFor` and routed to one of two files:
//!
//!   docs/api-snapshot.txt              — STABLE, the FROZEN contract. Any
//!                                        drift here fails `check-api` (RED).
//!   docs/api-snapshot-experimental.txt — EXPERIMENTAL, informational only.
//!                                        Regenerated on every run; drift here
//!                                        is expected and NEVER fails the gate.
//!
//!   zig build api-snapshot   # regenerate BOTH files
//!   zig build check-api      # fail ONLY on Stable-file drift; refresh the
//!                            # experimental file in place
//!
//! Stability tiers for individual modules live in docs/stability.md and the
//! F4 "Freeze scope" section of docs/rpc-stable-plan.md, which is authoritative
//! for the categorization below. The frozen file is the reviewed Stable public
//! surface; the two-tier split lets the Experimental surface (L3 origination,
//! VatNetwork, reflected-cap resolve, QUIC, persistence, ServerSession-as-a-
//! type, events, ...) keep evolving post-tag without a false-red gate or an
//! accidental freeze.
//!
//! Categorizer contract: `tierFor` DEFAULTS every path to Experimental. A
//! declaration is Stable ONLY when its path matches an explicit rule in
//! `stable_rules`. This makes "accidentally freezing something new" a
//! non-event: a brand-new symbol lands in the Experimental file until someone
//! deliberately adds a Stable rule for it.

const std = @import("std");
const capnpc = @import("capnpc-zig");

// Depth of the whole-tree walk. Bumped from 6 to 8 so the deepest promoted
// Stable declaration renders: `Connection.Options.default` sits at an 8-segment
// path (capnpc-zig.rpc.transport.tcp.connection.Connection.Options.default) and
// was invisible at the old depth. The frozen surface MUST be fully captured;
// the extra depth only adds already-shallow experimental leaves to the
// informational file, so it is cheap.
const max_depth = 8;

// F2 boundary assertion. This tool imports `capnpc-zig` in a NON-test build,
// so it sees exactly the surface a consumer sees. The test-only RPC clusters
// (`Peer.test_hooks` methods and the `rpc.testing` Internal facade) are gated
// behind `builtin.is_test`; from here they must be empty containers with no
// reachable decls. If a future change re-exposes them on the consumer surface,
// this block fails to compile — a self-checking Stable/Internal boundary that
// does not depend on a human reviewing the snapshot diff.
comptime {
    if (std.meta.declarations(capnpc.rpc.peer.Peer.test_hooks).len != 0) {
        @compileError("Peer.test_hooks is reachable from the consumer surface (src/lib.zig); it must be gated behind builtin.is_test");
    }
    if (std.meta.declarations(capnpc.rpc.testing).len != 0) {
        @compileError("rpc.testing is reachable from the consumer surface (src/lib.zig); it must be gated behind builtin.is_test");
    }
}

// ---------------------------------------------------------------------------
// Tier categorizer.
//
// A rule matches on the declaration PATH (the text left of the first ": " in a
// rendered line), never on the signature. Two match kinds:
//
//   .prefix — path equals the rule OR begins with `rule ++ "."`. Freezes a
//             whole subtree (a module or a type and all its members).
//   .exact  — path equals the rule exactly. Freezes ONE symbol without
//             dragging in its siblings or an enclosing container's other
//             members (used for the single ServerSession lifecycle entries).
//
// The list below is the FULL Stable contract. It follows the "Freeze scope"
// section of docs/rpc-stable-plan.md exactly; when a symbol was a judgment
// call it was defaulted to Experimental (omitted here), never frozen.
// ---------------------------------------------------------------------------

const MatchKind = enum { prefix, exact };
const Rule = struct { kind: MatchKind, path: []const u8 };

fn p(path: []const u8) Rule {
    return .{ .kind = .prefix, .path = path };
}
fn e(path: []const u8) Rule {
    return .{ .kind = .exact, .path = path };
}

// Exclusion overrides: paths that a `stable_rules` prefix would otherwise
// sweep in, but which the Freeze scope explicitly names as Experimental. These
// are checked FIRST and force Experimental regardless of any Stable prefix.
// The L3 third-party-hosted machinery lives on the otherwise-Stable `CapTable`
// under `rpc.caps.table.*`; the plan lists `markThirdPartyHosted` (the
// "thirdPartyHosted emission") as L3, so its whole get/set/clear trio is held
// out of the frozen contract.
const experimental_overrides = [_]Rule{
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.markThirdPartyHosted"),
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.getThirdPartyHosted"),
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.clearThirdPartyHosted"),
    // The cap-table's L3 third-party-hosted bookkeeping record, paired with the
    // three methods above. Excluded for consistency with the L3 arc even though
    // it sits inside the otherwise-Stable `caps.table` subtree.
    p("capnpc-zig.rpc.caps.table.lifecycle.ThirdPartyHostedRecord"),
};

// NOTE ON `rpc.wire.protocol.*` third-party encoders: symbols like
// `CapDescriptor.writeThirdPartyHosted`, `MessageBuilder.buildThirdPartyAnswer`,
// `ReturnBuilder.setAcceptFromThirdParty`, and the `ThirdPartyAnswer` /
// `ThirdPartyCapDescriptor` wire types are DELIBERATELY kept Stable. They are
// serializers/readers for message shapes fixed by the upstream Cap'n Proto RPC
// wire schema (rpc.capnp), not the L3 *origination* API. The Freeze scope
// freezes the whole wire protocol; the L3 exclusion is about the Peer /
// VatNetwork orchestration surface (sendProvide/sendAccept/ProvideHandle/...),
// which lives elsewhere and is excluded there.

const stable_rules = [_]Rule{
    // --- Serialization / wire-format / packing / schema / codegen (already
    //     Stable per docs/stability.md). Whole subtrees. ---
    p("capnpc-zig.message"),
    p("capnpc-zig.schema"),
    p("capnpc-zig.schema_validation"),
    p("capnpc-zig.reader"),
    p("capnpc-zig.request"),
    p("capnpc-zig.codegen"),

    // --- RPC wire protocol + framing (promoted). ---
    p("capnpc-zig.rpc.wire.protocol"),
    p("capnpc-zig.rpc.wire.framing"),

    // --- RPC cap table (promoted). ---
    p("capnpc-zig.rpc.caps.table"),

    // --- TCP ClientSession: the full session lifecycle is a frozen consumer
    //     entry point. Members render under the canonical `tcp.client.*` path;
    //     the `tcp.ClientSession: struct` alias line is frozen too. ---
    e("capnpc-zig.rpc.transport.tcp.ClientSession"),
    e("capnpc-zig.rpc.transport.tcp.client.ClientSession"),
    e("capnpc-zig.rpc.transport.tcp.client.ClientSession.connect"),
    e("capnpc-zig.rpc.transport.tcp.client.ClientSession.connectHost"),
    e("capnpc-zig.rpc.transport.tcp.client.ClientSession.run"),
    e("capnpc-zig.rpc.transport.tcp.client.ClientSession.close"),
    e("capnpc-zig.rpc.transport.tcp.client.ClientSession.requestStop"),
    e("capnpc-zig.rpc.transport.tcp.client.ClientSession.deinit"),
    e("capnpc-zig.rpc.transport.tcp.client.ClientSession.fromPeer"),
    e("capnpc-zig.rpc.transport.tcp.client.ClientSession.adoptOwnerThread"),
    // The top-level `connect`/`connectHost` free-function aliases are the
    // documented one-call consumer entry and share ClientSession's signature.
    e("capnpc-zig.rpc.transport.tcp.connect"),
    e("capnpc-zig.rpc.transport.tcp.connectHost"),

    // --- TCP ServerSession: ONLY `.accept` (the one accept entry point) plus
    //     the session lifecycle are frozen. The struct/API otherwise stays
    //     Experimental, so these are EXACT rules, not a subtree prefix. The
    //     `tcp.ServerSession: struct` alias line is intentionally NOT frozen
    //     (the type is not frozen); members render under `tcp.server.*`. ---
    e("capnpc-zig.rpc.transport.tcp.server.ServerSession.accept"),
    e("capnpc-zig.rpc.transport.tcp.server.ServerSession.run"),
    e("capnpc-zig.rpc.transport.tcp.server.ServerSession.close"),
    e("capnpc-zig.rpc.transport.tcp.server.ServerSession.requestStop"),
    e("capnpc-zig.rpc.transport.tcp.server.ServerSession.deinit"),
    e("capnpc-zig.rpc.transport.tcp.server.ServerSession.fromPeer"),

    // --- TCP Connection: NARROWED frozen surface. Members render under the
    //     canonical `tcp.connection.Connection.*` path. `init` (canonical),
    //     `Options` + `Options.default`, `enableWake`, run/close lifecycle,
    //     `adoptOwnerThread`, and `SocketFd`. Advanced/internal members
    //     (sendFrame, wake, context, start, isClosing, requestClose,
    //     writeQueueStats, assertThreadAffinity) stay Experimental. ---
    e("capnpc-zig.rpc.transport.tcp.Connection"),
    e("capnpc-zig.rpc.transport.tcp.connection.Connection"),
    e("capnpc-zig.rpc.transport.tcp.connection.Connection.init"),
    // `Options` as a whole subtree so `Options.default` and the field-default
    // contract are frozen together.
    p("capnpc-zig.rpc.transport.tcp.connection.Connection.Options"),
    e("capnpc-zig.rpc.transport.tcp.connection.Connection.InitError"),
    e("capnpc-zig.rpc.transport.tcp.connection.Connection.EnableWakeError"),
    e("capnpc-zig.rpc.transport.tcp.connection.Connection.enableWake"),
    e("capnpc-zig.rpc.transport.tcp.connection.Connection.run"),
    e("capnpc-zig.rpc.transport.tcp.connection.Connection.close"),
    e("capnpc-zig.rpc.transport.tcp.connection.Connection.deinit"),
    e("capnpc-zig.rpc.transport.tcp.connection.Connection.adoptOwnerThread"),
    // SocketFd: the frozen opaque socket-handle type. Both the tcp-level alias
    // and the canonical runtime definition.
    e("capnpc-zig.rpc.transport.tcp.SocketFd"),
    e("capnpc-zig.rpc.transport.tcp.runtime.SocketFd"),

    // --- Canonical two-party Peer consumer entry points (post F1/F2/F3). ---
    //     Frozen: the canonical ctor + attach, the send family, export/import
    //     management, basic two-party promise resolution, and the
    //     run/close/deinit/adoptOwnerThread lifecycle. EXCLUDED (Experimental,
    //     omitted): the entire L3 arc (sendProvide/sendAccept/third-party/
    //     handoff/VatNetwork), reflected resolvePromiseExportToImport, the
    //     F3-demoted initDetached*/attachTransport*, persistence, and every
    //     advanced/internal helper.
    e("capnpc-zig.rpc.peer.Peer"),
    e("capnpc-zig.rpc.peer.Peer.init"),
    e("capnpc-zig.rpc.peer.Peer.attachConnection"),
    e("capnpc-zig.rpc.peer.Peer.sendBootstrap"),
    e("capnpc-zig.rpc.peer.Peer.sendCall"),
    e("capnpc-zig.rpc.peer.Peer.sendCallResolved"),
    e("capnpc-zig.rpc.peer.Peer.sendCallPromised"),
    e("capnpc-zig.rpc.peer.Peer.sendCallPromisedWithOps"),
    e("capnpc-zig.rpc.peer.Peer.addExport"),
    e("capnpc-zig.rpc.peer.Peer.addPromiseExport"),
    e("capnpc-zig.rpc.peer.Peer.setBootstrap"),
    e("capnpc-zig.rpc.peer.Peer.releaseImport"),
    e("capnpc-zig.rpc.peer.Peer.resolvePromiseExportToExport"),
    e("capnpc-zig.rpc.peer.Peer.resolvePromiseExportToException"),
    e("capnpc-zig.rpc.peer.Peer.run"),
    e("capnpc-zig.rpc.peer.Peer.close"),
    e("capnpc-zig.rpc.peer.Peer.deinit"),
    e("capnpc-zig.rpc.peer.Peer.adoptOwnerThread"),

    // --- Two-party Peer public support types (siblings of `Peer`). ---
    //     CallError + the user-callback typedefs + PeerLimits. These are the
    //     frozen vocabulary the entry points above speak in. Explicitly NOT
    //     frozen from this cluster: the L3 types (ProvideHandle, Introduction,
    //     Introduced, HandoffPickupCallback), VatNetwork, persistence types
    //     (SaveResponse*/RestoreResponse*/persistence), TransportBinding and
    //     the F3-demoted transport typedefs, and other advanced helpers.
    e("capnpc-zig.rpc.peer.CallError"),
    e("capnpc-zig.rpc.peer.CallBuildFn"),
    e("capnpc-zig.rpc.peer.ReturnBuildFn"),
    e("capnpc-zig.rpc.peer.QuestionCallback"),
    e("capnpc-zig.rpc.peer.CallHandler"),
    e("capnpc-zig.rpc.peer.SaveHandler"),
    e("capnpc-zig.rpc.peer.RestoreHandler"),
    e("capnpc-zig.rpc.peer.PeerLimits"),
};

fn matchesRule(comptime path: []const u8, comptime rules: []const Rule) bool {
    inline for (rules) |rule| {
        switch (rule.kind) {
            .exact => if (std.mem.eql(u8, path, rule.path)) return true,
            .prefix => {
                if (std.mem.eql(u8, path, rule.path)) return true;
                if (path.len > rule.path.len and
                    std.mem.startsWith(u8, path, rule.path) and
                    path[rule.path.len] == '.') return true;
            },
        }
    }
    return false;
}

fn tierIsStable(comptime path: []const u8) bool {
    // Explicit Experimental overrides win over any Stable prefix.
    if (matchesRule(path, &experimental_overrides)) return false;
    return matchesRule(path, &stable_rules);
}

fn isContainer(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"enum", .@"union", .@"opaque" => true,
        else => false,
    };
}

fn containerKind(comptime T: type) []const u8 {
    return switch (@typeInfo(T)) {
        .@"struct" => "struct",
        .@"enum" => "enum",
        .@"union" => "union",
        .@"opaque" => "opaque",
        else => unreachable,
    };
}

/// Skip recursing into types that are not ours (std re-exports etc.).
fn foreignType(comptime T: type) bool {
    const name = @typeName(T);
    return std.mem.startsWith(u8, name, "std.") or
        std.mem.startsWith(u8, name, "builtin.") or
        std.mem.eql(u8, name, "std") or
        std.mem.eql(u8, name, "builtin");
}

fn contains(comptime seen: []const type, comptime T: type) bool {
    for (seen) |S| {
        if (S == T) return true;
    }
    return false;
}

/// A rendered declaration line plus the path that produced it (kept so the
/// categorizer can route lines after they are collected).
const Entry = struct { path: []const u8, line: []const u8 };

fn walk(
    comptime T: type,
    comptime path: []const u8,
    comptime depth: usize,
    comptime seen: *[]const type,
    comptime entries: *[]const Entry,
) void {
    if (depth >= max_depth) return;
    if (contains(seen.*, T)) return;
    seen.* = seen.* ++ [_]type{T};

    for (std.meta.declarations(T)) |decl_name| {
        const decl_path = path ++ "." ++ decl_name;
        const D = @field(T, decl_name);
        const DType = @TypeOf(D);

        if (DType == type) {
            if (isContainer(D)) {
                entries.* = entries.* ++ [_]Entry{.{ .path = decl_path, .line = decl_path ++ ": " ++ containerKind(D) }};
                if (!foreignType(D)) {
                    walk(D, decl_path, depth + 1, seen, entries);
                }
            } else {
                entries.* = entries.* ++ [_]Entry{.{ .path = decl_path, .line = decl_path ++ ": type = " ++ @typeName(D) }};
            }
        } else if (@typeInfo(DType) == .@"fn") {
            entries.* = entries.* ++ [_]Entry{.{ .path = decl_path, .line = decl_path ++ ": " ++ @typeName(DType) }};
        } else {
            entries.* = entries.* ++ [_]Entry{.{ .path = decl_path, .line = decl_path ++ ": const " ++ @typeName(DType) }};
        }
    }
}

const all_entries: []const Entry = blk: {
    @setEvalBranchQuota(20_000_000);
    var seen: []const type = &.{};
    var entries: []const Entry = &.{};
    walk(capnpc, "capnpc-zig", 0, &seen, &entries);
    break :blk entries;
};

/// Split the flat entry list into the two tiers at comptime.
const stable_lines: []const []const u8 = blk: {
    @setEvalBranchQuota(20_000_000);
    var stable: []const []const u8 = &.{};
    for (all_entries) |entry| {
        if (tierIsStable(entry.path)) {
            stable = stable ++ [_][]const u8{entry.line};
        }
    }
    break :blk stable;
};

const experimental_lines: []const []const u8 = blk: {
    @setEvalBranchQuota(20_000_000);
    var experimental: []const []const u8 = &.{};
    for (all_entries) |entry| {
        if (!tierIsStable(entry.path)) {
            experimental = experimental ++ [_][]const u8{entry.line};
        }
    }
    break :blk experimental;
};

fn lessThan(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

/// Copy `line` with every `__struct_<digits>` collapsed to `__struct_*`.
/// Those suffixes are compiler-assigned anonymous-type counters: they shift
/// whenever unrelated code changes and differ between targets, so keeping
/// them verbatim would make the snapshot churn without any API change.
fn normalizeLine(allocator: std.mem.Allocator, line: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    const marker = "__struct_";
    var rest = line;
    while (std.mem.indexOf(u8, rest, marker)) |idx| {
        var end = idx + marker.len;
        while (end < rest.len and std.ascii.isDigit(rest[end])) end += 1;
        try out.appendSlice(allocator, rest[0 .. idx + marker.len]);
        try out.append(allocator, '*');
        rest = rest[end..];
    }
    try out.appendSlice(allocator, rest);
    return out.toOwnedSlice(allocator);
}

fn renderSnapshot(
    allocator: std.mem.Allocator,
    lines: []const []const u8,
    header: []const u8,
) ![]u8 {
    const normalized = try allocator.alloc([]u8, lines.len);
    var normalized_count: usize = 0;
    defer {
        for (normalized[0..normalized_count]) |line| allocator.free(line);
        allocator.free(normalized);
    }
    for (lines) |line| {
        normalized[normalized_count] = try normalizeLine(allocator, line);
        normalized_count += 1;
    }
    std.mem.sort([]u8, normalized[0..normalized_count], {}, lessThan);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, header);
    for (normalized[0..normalized_count]) |line| {
        try out.appendSlice(allocator, line);
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

const stable_header =
    \\# STABLE public API snapshot — the FROZEN contract.
    \\# Generated by `zig build api-snapshot`. A diff here is a breaking API
    \\# change: `zig build check-api` fails (RED) until it is reviewed against
    \\# docs/stability.md + docs/rpc-stable-plan.md and this file is committed.
    \\# The categorizer lives in tools/api_snapshot.zig (`stable_rules`).
    \\
;

const experimental_header =
    \\# EXPERIMENTAL public API surface — informational, NOT frozen.
    \\# Generated by `zig build api-snapshot`; regenerated on every `check-api`.
    \\# Drift here is expected and does NOT fail the gate. Do not rely on any
    \\# symbol below across releases; only docs/api-snapshot.txt is a contract.
    \\
;

const stable_path_default = "docs/api-snapshot.txt";
const experimental_path_default = "docs/api-snapshot-experimental.txt";

fn writeFile(io: std.Io, path: []const u8, contents: []const u8) !void {
    var file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);
    try file.writeStreamingAll(io, contents);
}

/// Diff `rendered` against the on-disk `path`; on mismatch print a
/// first-divergence hint. Returns true when they match.
fn diffAndReport(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    rendered: []const u8,
) !bool {
    const existing = std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(16 * 1024 * 1024)) catch |err| {
        std.debug.print(
            "api-snapshot: cannot read {s} ({}); run `zig build api-snapshot` to create it\n",
            .{ path, err },
        );
        return error.ApiSnapshotMissing;
    };
    defer allocator.free(existing);

    if (std.mem.eql(u8, existing, rendered)) return true;

    var existing_it = std.mem.splitScalar(u8, existing, '\n');
    var rendered_it = std.mem.splitScalar(u8, rendered, '\n');
    var line_no: usize = 1;
    while (true) : (line_no += 1) {
        const a = existing_it.next();
        const b = rendered_it.next();
        if (a == null and b == null) break;
        const a_line = a orelse "<end of snapshot>";
        const b_line = b orelse "<end of live surface>";
        if (!std.mem.eql(u8, a_line, b_line)) {
            std.debug.print(
                "api-snapshot: drift in {s} at line {}:\n  snapshot: {s}\n  live:     {s}\n",
                .{ path, line_no, a_line, b_line },
            );
            break;
        }
    }
    return false;
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = init.io;

    var mode: enum { check, write } = .check;
    var stable_path: []const u8 = stable_path_default;
    var experimental_path: []const u8 = experimental_path_default;

    // initAllocator is the cross-platform form; plain init is a compile
    // error on Windows. The iterator stays alive for all of main because
    // --path captures a slice of its buffer.
    var iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer iter.deinit();
    _ = iter.skip();
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--write")) {
            mode = .write;
        } else if (std.mem.eql(u8, arg, "--check")) {
            mode = .check;
        } else if (std.mem.eql(u8, arg, "--path")) {
            stable_path = iter.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--experimental-path")) {
            experimental_path = iter.next() orelse return error.InvalidArgument;
        } else {
            std.debug.print("api-snapshot: unknown argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }

    const stable_rendered = try renderSnapshot(allocator, stable_lines, stable_header);
    defer allocator.free(stable_rendered);
    const experimental_rendered = try renderSnapshot(allocator, experimental_lines, experimental_header);
    defer allocator.free(experimental_rendered);

    switch (mode) {
        .write => {
            try writeFile(io, stable_path, stable_rendered);
            try writeFile(io, experimental_path, experimental_rendered);
            std.debug.print(
                "api-snapshot: wrote {} stable lines to {s}, {} experimental lines to {s}\n",
                .{ stable_lines.len, stable_path, experimental_lines.len, experimental_path },
            );
        },
        .check => {
            // The Experimental file is informational: refresh it in place so
            // it never goes stale, but its content NEVER fails the gate.
            writeFile(io, experimental_path, experimental_rendered) catch |err| {
                std.debug.print(
                    "api-snapshot: note: could not refresh {s} ({}); continuing\n",
                    .{ experimental_path, err },
                );
            };

            // The Stable file is the frozen contract: drift here is RED.
            const stable_ok = try diffAndReport(allocator, io, stable_path, stable_rendered);
            if (!stable_ok) {
                std.debug.print(
                    "api-snapshot: STABLE public API surface changed. This is a frozen contract. Review against docs/stability.md + docs/rpc-stable-plan.md, then run `zig build api-snapshot` and commit the result.\n",
                    .{},
                );
                return error.ApiSnapshotDrift;
            }
            std.debug.print(
                "api-snapshot: OK ({} stable declarations frozen; {} experimental refreshed)\n",
                .{ stable_lines.len, experimental_lines.len },
            );
        },
    }
}
