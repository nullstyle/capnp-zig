//! Public API snapshot tool — two-tier (Stable / Experimental).
//!
//! Walks the library's `pub` declaration tree at comptime and emits a
//! sorted, line-oriented description of the API surface: declaration paths
//! plus type/function signatures. Each declaration is assigned a stability
//! TIER by `tierFor` and routed to one of two files:
//!
//!   docs/api-snapshot.txt              — STABLE, the FROZEN contract. Any
//!                                        drift here fails `check-api` (RED).
//!   docs/api-snapshot-experimental.txt — EXPERIMENTAL, not frozen. Refreshed
//!                                        in place by local `check-api`; CI's
//!                                        strict mode fails when the committed
//!                                        file is stale.
//!
//!   zig build api-snapshot             # regenerate BOTH files
//!   zig build check-api                # fail ONLY on Stable-file drift;
//!                                      # refresh the experimental file
//!   zig build check-api-experimental   # CI: fail on Stable drift OR a stale
//!                                      # committed experimental file
//!                                      # (--strict-experimental)
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
    // Retained-answer handoff is Experimental even where its implementation
    // necessarily adds a convenience entry under otherwise-Stable containers.
    // Keep these exact additions out of the frozen two-party/wire contract.
    e("capnpc-zig.rpc.peer.PeerLimits.max_retained_questions"),
    // L4 Join lease controls are Experimental. The surrounding configuration
    // structs are Stable because existing two-party entry points consume them,
    // so exact exclusions prevent the broad prefixes below from accidentally
    // freezing this pilot surface.
    e("capnpc-zig.rpc.peer.PeerLimits.max_join_parts_per_join"),
    e("capnpc-zig.rpc.peer.PeerLimits.max_pending_join_records"),
    e("capnpc-zig.rpc.transport.tcp.client.ConnectOptions.join_timeout_ms"),
    e("capnpc-zig.rpc.transport.tcp.server.ServeOptions.join_timeout_ms"),
    e("capnpc-zig.rpc.wire.protocol.MessageBuilder.buildProvidePromisedAnswerWithOps"),
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.markThirdPartyHosted"),
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.getThirdPartyHosted"),
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.clearThirdPartyHosted"),
    // The cap-table's L3 third-party-hosted bookkeeping record, paired with the
    // three methods above. Excluded for consistency with the L3 arc even though
    // it sits inside the otherwise-Stable `caps.table` subtree.
    p("capnpc-zig.rpc.caps.table.lifecycle.ThirdPartyHostedRecord"),
    // The L17 receiverHosted-lift import-pin machinery (handoff pins with
    // deferred-Release accounting). Level-3 handoff surface living on the
    // otherwise-Stable `CapTable`, held out of the frozen contract exactly
    // like the thirdPartyHosted trio above.
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.noteHandoffImportPin"),
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.rollbackHandoffImportPin"),
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.deferReleaseWhilePinned"),
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.releaseHandoffImportPin"),
    e("capnpc-zig.rpc.caps.table.lifecycle.CapTable.removeImportIfFullyReleased"),
    p("capnpc-zig.rpc.caps.table.lifecycle.CapTable.HandoffImportUnpin"),
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
    // CODEGEN: entry points are frozen, INTERNALS are not. Decided
    // deliberately (2026-08-13) rather than inherited.
    //
    // This was `p("capnpc-zig.codegen")`, a blanket prefix that froze the
    // whole pub surface of generator.zig and struct_gen.zig — 54 entries,
    // most of them internal state. Because Zig's privacy is FILE-scoped, that
    // made those two files (9,029 lines, the largest un-decomposed units in
    // the tree) effectively unsplittable: moving any cluster of private
    // `Generator` methods into a sibling forces the helpers it calls back into
    // to become `pub`, and every one of those would have landed here
    // permanently. The `rpc.peer` decomposition was unaffected only because
    // Peer's surface is Experimental.
    //
    // What a consumer of this library actually depends on is the plugin
    // contract: construct a Generator, configure it, generate a file. That is
    // frozen below, by exact rule. Everything else under `codegen` — struct
    // fields, `ArrayListWriter` (not named in any frozen signature),
    // `TypeGenerator`'s helpers — is Experimental, free to move, and tracked
    // in the experimental snapshot rather than contractually pinned.
    //
    // Adding a genuine new entry point means adding a rule here on purpose.
    // That is the point: freezing should be an act, not an accident of prefix
    // breadth.
    e("capnpc-zig.codegen.Generator.init"),
    e("capnpc-zig.codegen.Generator.deinit"),
    e("capnpc-zig.codegen.Generator.generateFile"),
    e("capnpc-zig.codegen.Generator.setApiProfile"),
    e("capnpc-zig.codegen.Generator.setCodegenBudget"),
    e("capnpc-zig.codegen.Generator.setEmitSchemaManifest"),
    e("capnpc-zig.codegen.Generator.setShapeSharing"),
    e("capnpc-zig.codegen.Generator.setVerbose"),
    // Types named in those signatures, with their fields/enumerants: a
    // consumer configures a budget and selects a profile, so their shape is
    // part of the contract even though the Generator's own fields are not.
    p("capnpc-zig.codegen.Generator.ApiProfile"),
    p("capnpc-zig.codegen.Generator.CodegenBudget"),

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
    // PREFIX: a consumer cannot call the frozen `connect`/`connectHost` without
    // constructing one of these, and it relies on their defaults, so the fields
    // are part of the contract. Closing the frozen surface under its own
    // signatures (`zig build api-closure`) is what surfaced them.
    p("capnpc-zig.rpc.transport.tcp.client.ConnectOptions"),
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
    // PREFIX: `accept` is frozen and takes a `ServeOptions`, so a consumer must
    // construct one and depends on its defaults.
    p("capnpc-zig.rpc.transport.tcp.server.ServeOptions"),
    // `accept` also takes a `*Listener`, and before this there was NO Stable way
    // to obtain one — the frozen server entry point was unusable on its own
    // terms. Narrowed exactly like `Connection`: the constructor, the address
    // query and teardown are the documented consumer path (both
    // docs/getting-started-rpc.md and examples/rpc_pingpong.zig call
    // `Listener.init(allocator, io, address, .{})`). The raw-fd and
    // handle-oriented members stay Experimental.
    e("capnpc-zig.rpc.transport.tcp.runtime.Listener"),
    e("capnpc-zig.rpc.transport.tcp.Listener"),
    e("capnpc-zig.rpc.transport.tcp.runtime.Listener.init"),
    e("capnpc-zig.rpc.transport.tcp.runtime.Listener.close"),
    e("capnpc-zig.rpc.transport.tcp.runtime.Listener.getAddress"),
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
    //     deinit/adoptOwnerThread lifecycle. NOTE: `Peer` has no `run` or
    //     `close` method — rules for both sat here for releases, freezing
    //     nothing and promising a lifecycle that was never implemented (the
    //     nearest real method is the Experimental `closeAttachedTransport`).
    //     The rule-liveness assertion below now makes that class of mistake a
    //     compile error. EXCLUDED (Experimental,
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
    // PREFIX, not exact: `PeerLimits` is a config struct a consumer constructs
    // and whose defaults it relies on, so its FIELDS are part of the frozen
    // contract — removing one, or changing a default, is a consumer-visible
    // break. Contrast `Peer` itself, frozen exactly so its ~73 fields of
    // internal state stay out of the contract.
    p("capnpc-zig.rpc.peer.PeerLimits"),
    // PREFIX: `Peer.addExport` / `setBootstrap` are frozen and both take an
    // `Export`, so a consumer cannot serve anything without building one. Two
    // fields, both already-frozen types.
    p("capnpc-zig.rpc.peer.Export"),
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

/// Render a function signature, expanding any inferred error set.
///
/// `@typeName` renders an inferred error set as the self-referential expression
/// `@typeInfo(@typeInfo(@TypeOf(f)).@"fn".return_type.?).error_union.error_set`,
/// which is IDENTICAL no matter what the set contains — 325 of the frozen lines
/// rendered that way, so adding, removing, or renaming an error on nearly half
/// the Stable surface passed the gate unchanged while breaking every consumer's
/// `catch |err| switch (err)`. Expanding the set to a sorted `error{...}` list
/// makes those changes visible.
///
/// `anytype` parameters remain unpinned: they are genuinely unresolved until
/// instantiation, so a signature containing one pins only its arity. That
/// residual hole is documented in docs/supported-surface.md rather than hidden.
fn renderErrorSet(comptime E: type) []const u8 {
    const info = @typeInfo(E).error_set;
    const names = info.error_names orelse return "anyerror";

    comptime var sorted: []const []const u8 = &.{};
    for (names) |name| sorted = sorted ++ [_][]const u8{name};
    // Insertion sort: the rendered set must not depend on declaration order.
    comptime var i: usize = 1;
    inline while (i < sorted.len) : (i += 1) {
        comptime var j = i;
        inline while (j > 0 and std.mem.lessThan(u8, sorted[j], sorted[j - 1])) : (j -= 1) {
            const swapped = sorted[j - 1];
            var next: []const []const u8 = sorted[0 .. j - 1];
            next = next ++ [_][]const u8{sorted[j]} ++ [_][]const u8{swapped};
            if (j + 1 < sorted.len) next = next ++ sorted[j + 1 ..];
            sorted = next;
        }
    }

    comptime var out: []const u8 = "error{";
    for (sorted, 0..) |name, idx| {
        if (idx != 0) out = out ++ ",";
        out = out ++ name;
    }
    return out ++ "}";
}

fn renderFnType(comptime FnType: type) []const u8 {
    const fn_info = @typeInfo(FnType).@"fn";
    // A GENERIC function's inferred error set cannot be resolved here: it
    // depends on the instantiation, and asking for it is a compile error
    // ("cannot resolve inferred error set of generic function type"). Those
    // lines keep the opaque rendering and stay unpinned; the count is recorded
    // in docs/supported-surface.md so the residual hole is a known quantity
    // rather than a surprise.
    if (fn_info.is_generic) return @typeName(FnType);
    const ret = fn_info.return_type orelse return @typeName(FnType);
    switch (@typeInfo(ret)) {
        .error_union => |eu| {
            // Rebuild the signature with the expanded set. The parameter list is
            // taken verbatim from @typeName so `anytype`/`comptime` render
            // exactly as before and only the error set changes.
            const full = @typeName(FnType);
            const open = std.mem.indexOfScalar(u8, full, '(') orelse return full;
            // Match the parameter list's OWN closing paren by depth. A plain
            // lastIndexOf(')') lands inside the rendered return type — which for
            // an inferred error set is itself a paren-heavy
            // `@typeInfo(@typeInfo(@TypeOf(f)).@"fn".return_type.?)` expression —
            // and splices that fragment into the output.
            comptime var depth: usize = 0;
            comptime var close: ?usize = null;
            inline for (full[open..], open..) |ch, idx| {
                if (ch == '(') depth += 1;
                if (ch == ')') {
                    depth -= 1;
                    if (depth == 0) {
                        close = idx;
                        break;
                    }
                }
            }
            const close_idx = close orelse return full;
            const params = full[open .. close_idx + 1];
            return "fn " ++ params ++ " " ++ renderErrorSet(eu.error_set) ++ "!" ++ @typeName(eu.payload);
        },
        else => return @typeName(FnType),
    }
}

/// Render a field's default-value initializer, or "" when it has none.
///
/// The default VALUE matters, not just its presence: changing
/// `Connection.Options.read_buffer_size` from one number to another is a
/// behavior change for every consumer who relied on it, and a name-only
/// snapshot could not see it. Values are rendered for the scalar kinds where a
/// default is meaningful and comparable; anything else records that a default
/// exists without trying to spell it, which still pins presence.
fn defaultSuffix(
    comptime FieldType: type,
    comptime attrs: std.builtin.Type.Struct.FieldAttributes,
) []const u8 {
    const value = attrs.defaultValue(FieldType) orelse return "";
    return " = " ++ renderValue(FieldType, value);
}

/// Render a comptime-known default. Optionals are unwrapped rather than reported
/// as merely present: `default_call_timeout_ms: ?u64 = 30000` is a number
/// consumers depend on, and collapsing it to "<non-null default>" would let a
/// 30s → 60s change pass the gate. Aggregates render as `<default>` — their own
/// fields are pinned separately by their own snapshot lines.
fn renderValue(comptime T: type, comptime value: T) []const u8 {
    return switch (@typeInfo(T)) {
        .int, .comptime_int => std.fmt.comptimePrint("{d}", .{value}),
        .float, .comptime_float => std.fmt.comptimePrint("{d}", .{value}),
        .bool => if (value) "true" else "false",
        .@"enum" => "." ++ @tagName(value),
        .void => "{}",
        .optional => |oi| if (value) |inner| renderValue(oi.child, inner) else "null",
        else => "<default>",
    };
}

/// Emit one line per field/enumerant of a container.
///
/// The walker enumerates DECLARATIONS only, so before this every frozen struct
/// was pinned by name alone: removing a field from `PeerLimits`, reordering a
/// union, or changing a default was invisible to `check-api`. Fields render
/// under the container's path, so the existing tier rules route them — a
/// `.prefix` rule (a frozen module) sweeps its types' fields into the contract,
/// while a `.exact` rule (e.g. `Peer` itself) deliberately does not, keeping 73
/// fields of internal peer state out of the frozen surface.
fn fieldEntries(
    comptime T: type,
    comptime path: []const u8,
    comptime entries: *[]const Entry,
) void {
    switch (@typeInfo(T)) {
        .@"struct" => |info| {
            for (info.field_names, info.field_types, info.field_attrs) |name, FieldType, attrs| {
                const fpath = path ++ "." ++ name;
                entries.* = entries.* ++ [_]Entry{.{
                    .path = fpath,
                    .line = fpath ++ ": field " ++ @typeName(FieldType) ++ defaultSuffix(FieldType, attrs),
                }};
            }
        },
        .@"union" => |info| {
            for (info.field_names, info.field_types) |name, FieldType| {
                const fpath = path ++ "." ++ name;
                entries.* = entries.* ++ [_]Entry{.{
                    .path = fpath,
                    .line = fpath ++ ": variant " ++ @typeName(FieldType),
                }};
            }
        },
        .@"enum" => |info| {
            for (info.field_names, info.field_values) |name, value| {
                const fpath = path ++ "." ++ name;
                entries.* = entries.* ++ [_]Entry{.{
                    .path = fpath,
                    .line = fpath ++ ": enumerant = " ++ std.fmt.comptimePrint("{d}", .{value}),
                }};
            }
        },
        else => {},
    }
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
                    fieldEntries(D, decl_path, entries);
                    walk(D, decl_path, depth + 1, seen, entries);
                }
            } else {
                // A typedef whose value is a function (or a pointer to one) is
                // still a signature consumers code against — expand its error
                // set too, rather than leaving the opaque @typeName rendering.
                const rendered = switch (@typeInfo(D)) {
                    .@"fn" => renderFnType(D),
                    .pointer => |pi| if (@typeInfo(pi.child) == .@"fn")
                        "*const " ++ renderFnType(pi.child)
                    else
                        @typeName(D),
                    else => @typeName(D),
                };
                entries.* = entries.* ++ [_]Entry{.{ .path = decl_path, .line = decl_path ++ ": type = " ++ rendered }};
            }
        } else if (@typeInfo(DType) == .@"fn") {
            entries.* = entries.* ++ [_]Entry{.{ .path = decl_path, .line = decl_path ++ ": " ++ renderFnType(DType) }};
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

/// Every rule must name a declaration that actually exists.
///
/// A rule for a symbol that was never there (or has since been renamed) is
/// silent: it documents a contract nobody can rely on, and it would mask a typo
/// in a future promotion. This assertion makes the rules list self-validating —
/// it is what found `Peer.run`, a frozen "lifecycle entry point" that is not a
/// method on `Peer` at all.
fn ruleMatchesAnyDeclaration(comptime rule: Rule) bool {
    for (all_entries) |entry| {
        switch (rule.kind) {
            .exact => if (std.mem.eql(u8, entry.path, rule.path)) return true,
            .prefix => {
                if (std.mem.eql(u8, entry.path, rule.path)) return true;
                if (entry.path.len > rule.path.len and
                    std.mem.startsWith(u8, entry.path, rule.path) and
                    entry.path[rule.path.len] == '.') return true;
            },
        }
    }
    return false;
}

comptime {
    @setEvalBranchQuota(40_000_000);
    var dead: []const u8 = "";
    for (stable_rules) |rule| {
        if (!ruleMatchesAnyDeclaration(rule)) dead = dead ++ "\n  stable_rules: " ++ rule.path;
    }
    for (experimental_overrides) |rule| {
        if (!ruleMatchesAnyDeclaration(rule)) dead = dead ++ "\n  experimental_overrides: " ++ rule.path;
    }
    if (dead.len != 0) {
        @compileError("api_snapshot: rule(s) match no declaration — remove them or fix the path:" ++ dead);
    }
}

// ---------------------------------------------------------------------------
// Closure diagnostic: is the frozen surface closed under its own signatures?
//
// A Stable entry point whose signature mentions an Experimental type is only
// nominally frozen: the type can change shape under it at any 0.x bump while
// `check-api` stays green, because the Stable *line* never moved. Worse, when no
// Stable API can construct that type, the frozen entry point is unusable on its
// own terms.
//
// This is now a GATE (`zig build api-closure`, run in CI). It started as a
// diagnostic: the first run reported 14 violations, and each was a real API
// decision. Resolving them promoted the types a consumer cannot avoid —
// `ConnectOptions`, `ServeOptions`, `Export`, and a narrowed `Listener` (before
// which there was NO Stable way to obtain the `*Listener` that the frozen
// `ServerSession.accept` requires, so the frozen server entry point was
// unusable on its own terms). With the surface closed, gating it forces the next
// such decision to happen at review time instead of accumulating silently.
//
// KNOWN BLIND SPOT: a generic parameter (`anytype`) has no type to resolve, so
// such signatures are SKIPPED rather than cleared. See docs/supported-surface.md.
// ---------------------------------------------------------------------------

/// A container type the walk reached, plus whether any path reaching it is
/// Stable. Re-exports mean one type can sit at several paths; reachable via a
/// Stable path is what puts it in the contract.
const TypeTier = struct { ty: type, stable: bool };

fn collectTypes(
    comptime T: type,
    comptime path: []const u8,
    comptime depth: usize,
    comptime seen: *[]const type,
    comptime out: *[]const TypeTier,
) void {
    if (depth >= max_depth) return;
    if (contains(seen.*, T)) return;
    seen.* = seen.* ++ [_]type{T};

    for (std.meta.declarations(T)) |decl_name| {
        const decl_path = path ++ "." ++ decl_name;
        const D = @field(T, decl_name);
        if (@TypeOf(D) != type) continue;
        if (!isContainer(D)) continue;
        out.* = out.* ++ [_]TypeTier{.{ .ty = D, .stable = tierIsStable(decl_path) }};
        if (!foreignType(D)) collectTypes(D, decl_path, depth + 1, seen, out);
    }
}

const type_tiers: []const TypeTier = blk: {
    @setEvalBranchQuota(40_000_000);
    var seen: []const type = &.{};
    var out: []const TypeTier = &.{};
    collectTypes(capnpc, "capnpc-zig", 0, &seen, &out);
    break :blk out;
};

/// Strip the wrappers a signature puts around a nominal type.
fn peel(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .pointer => |pi| peel(pi.child),
        .optional => |oi| peel(oi.child),
        .error_union => |eu| peel(eu.payload),
        else => T,
    };
}

/// `null` when the type is not one of ours (std type, primitive, ...).
fn tierOfType(comptime T: type) ?bool {
    const P = peel(T);
    var found: ?bool = null;
    for (type_tiers) |entry| {
        if (entry.ty == P) {
            if (entry.stable) return true; // any Stable path wins
            found = false;
        }
    }
    return found;
}

const Violation = struct { decl: []const u8, offender: []const u8, role: []const u8 };

/// Walk again, this time checking each Stable function's signature. The check
/// has to happen inside the walk: that is the only place a declaration and its
/// snapshot path are both in hand.
fn collectClosure(
    comptime T: type,
    comptime path: []const u8,
    comptime depth: usize,
    comptime seen: *[]const type,
    comptime out: *[]const Violation,
) void {
    if (depth >= max_depth) return;
    if (contains(seen.*, T)) return;
    seen.* = seen.* ++ [_]type{T};

    for (std.meta.declarations(T)) |decl_name| {
        const decl_path = path ++ "." ++ decl_name;
        const D = @field(T, decl_name);
        const DType = @TypeOf(D);

        if (DType == type) {
            if (isContainer(D) and !foreignType(D)) {
                collectClosure(D, decl_path, depth + 1, seen, out);
            }
            continue;
        }
        if (@typeInfo(DType) != .@"fn") continue;
        if (!tierIsStable(decl_path)) continue;

        const fn_info = @typeInfo(DType).@"fn";
        if (fn_info.is_generic) continue;

        // A method that takes or returns its OWN enclosing type is not a
        // closure violation. `ServerSession.run(self: *ServerSession)` is the
        // frozen method of a type deliberately frozen only at `.accept` and its
        // lifecycle — the receiver is the same declaration cluster, not an
        // unfrozen dependency a consumer must obtain elsewhere.
        for (fn_info.param_types) |maybe_pt| {
            const PT = maybe_pt orelse continue;
            if (peel(PT) == T) continue;
            if (tierOfType(PT)) |is_stable| {
                if (!is_stable) out.* = out.* ++ [_]Violation{.{
                    .decl = decl_path,
                    .offender = @typeName(peel(PT)),
                    .role = "parameter",
                }};
            }
        }
        if (fn_info.return_type) |RT| {
            if (peel(RT) != T) {
                if (tierOfType(RT)) |is_stable| {
                    if (!is_stable) out.* = out.* ++ [_]Violation{.{
                        .decl = decl_path,
                        .offender = @typeName(peel(RT)),
                        .role = "return",
                    }};
                }
            }
        }
    }
}

const closure_violations: []const Violation = blk: {
    @setEvalBranchQuota(40_000_000);
    var seen: []const type = &.{};
    var out: []const Violation = &.{};
    collectClosure(capnpc, "capnpc-zig", 0, &seen, &out);
    break :blk out;
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
/// std type spellings that name the SAME logical type with a different path
/// per platform. `@typeName` reports the resolved declaration, so without this
/// the rendered surface differs by the OS that generated it and no staleness
/// gate can run in CI.
///
/// Concretely: `std.posix.sockaddr` resolves through translate-c on macOS
/// (`c.sockaddr__struct_*`, after the numeric suffix is normalized above) and
/// through `os.linux.sockaddr` on Linux. That is a property of std, not of our
/// API — `SockAddrStorage` names `std.posix.sockaddr` on every platform — so
/// the snapshot canonicalizes it rather than freezing one OS's spelling.
/// Longest/base forms come first: replacing the base rewrites the `.in` and
/// `.in6` members with it.
const platform_type_aliases = [_]struct { from: []const u8, to: []const u8 }{
    .{ .from = "c.sockaddr__struct_*", .to = "posix.sockaddr" },
    .{ .from = "os.linux.sockaddr", .to = "posix.sockaddr" },
    .{ .from = "os.darwin.sockaddr", .to = "posix.sockaddr" },
    .{ .from = "os.windows.ws2_32.sockaddr", .to = "posix.sockaddr" },
};

fn canonicalizePlatformTypes(allocator: std.mem.Allocator, line: []u8) ![]u8 {
    var current = line;
    for (platform_type_aliases) |alias| {
        if (std.mem.indexOf(u8, current, alias.from) == null) continue;
        const size = std.mem.replacementSize(u8, current, alias.from, alias.to);
        const next = try allocator.alloc(u8, size);
        _ = std.mem.replace(u8, current, alias.from, alias.to, next);
        allocator.free(current);
        current = next;
    }
    return current;
}

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
    return canonicalizePlatformTypes(allocator, try out.toOwnedSlice(allocator));
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

    var mode: enum { check, write, closure } = .check;
    var strict_experimental = false;
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
        } else if (std.mem.eql(u8, arg, "--closure")) {
            mode = .closure;
        } else if (std.mem.eql(u8, arg, "--strict-experimental")) {
            strict_experimental = true;
        } else if (std.mem.eql(u8, arg, "--path")) {
            stable_path = iter.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--experimental-path")) {
            experimental_path = iter.next() orelse return error.InvalidArgument;
        } else {
            std.debug.print("api-snapshot: unknown argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }

    if (mode == .closure) {
        if (closure_violations.len == 0) {
            std.debug.print("api-closure: OK — the frozen surface is closed under its own signatures\n", .{});
            return;
        }
        std.debug.print(
            "api-closure: {d} Stable declaration(s) mention an Experimental type.\n" ++
                "Each is an API decision: promote the type, or narrow the entry point.\n\n",
            .{closure_violations.len},
        );
        for (closure_violations) |v| {
            std.debug.print("  {s}\n    {s}: {s}\n", .{ v.decl, v.role, v.offender });
        }
        std.debug.print(
            "\nNOTE: signatures with an `anytype` parameter are skipped — there is no\n" ++
                "type to resolve until instantiation.\n",
            .{},
        );
        return error.StableSurfaceNotClosed;
    }

    const stable_rendered = try renderSnapshot(allocator, stable_lines, stable_header);
    defer allocator.free(stable_rendered);
    const experimental_rendered = try renderSnapshot(allocator, experimental_lines, experimental_header);
    defer allocator.free(experimental_rendered);

    switch (mode) {
        // Handled above, before the snapshots are rendered.
        .closure => unreachable,
        .write => {
            try writeFile(io, stable_path, stable_rendered);
            try writeFile(io, experimental_path, experimental_rendered);
            std.debug.print(
                "api-snapshot: wrote {} stable lines to {s}, {} experimental lines to {s}\n",
                .{ stable_lines.len, stable_path, experimental_lines.len, experimental_path },
            );
        },
        .check => {
            if (strict_experimental) {
                // Strict mode (CI): the Experimental file is not a frozen
                // contract, but the COMMITTED snapshot must match the tree —
                // otherwise the "informational" surface silently goes stale
                // and platform-dependent renderings slip through unnoticed.
                // Drift is RED with a refresh instruction, not a review one.
                const experimental_ok = try diffAndReport(allocator, io, experimental_path, experimental_rendered);
                if (!experimental_ok) {
                    std.debug.print(
                        "api-snapshot: EXPERIMENTAL surface drifted from the committed {s}. Not a frozen contract — refresh it: run `zig build api-snapshot` (and `-Dquic=true api-snapshot-quic`) and commit the result.\n",
                        .{experimental_path},
                    );
                    return error.ExperimentalSnapshotDrift;
                }
            } else {
                // The Experimental file is informational: refresh it in place
                // so it never goes stale, but its content does not fail the
                // default gate (CI enforces it via --strict-experimental).
                writeFile(io, experimental_path, experimental_rendered) catch |err| {
                    std.debug.print(
                        "api-snapshot: note: could not refresh {s} ({}); continuing\n",
                        .{ experimental_path, err },
                    );
                };
            }

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
                "api-snapshot: OK ({} stable declarations frozen; {} experimental {s})\n",
                .{ stable_lines.len, experimental_lines.len, if (strict_experimental) @as([]const u8, "verified") else "refreshed" },
            );
        },
    }
}
