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
//! Builder rule: a Stable builder line may not render `anyerror` anywhere:
//! not in its return set, its parameter types or its field types. Builder
//! lines are the members of builder types (a container whose name ends in
//! `Builder`) plus the free-function builder primitives in
//! `builder_free_functions`. The one tolerated exception is the list reader
//! makers' `anyerror` fn-pointer types inside reader parameter types
//! (`reader_maker_renderings`, pending a `ReadError` set). `api-snapshot` and
//! every `check-api` variant refuse to run until a violating line spells a
//! named set (`message.BuildError` / `message.CopyError`); see
//! `enforceBuilderRule`.
//!
//! Categorizer contract: `tierFor` DEFAULTS every path to Experimental. A
//! declaration is Stable ONLY when its path matches an explicit rule in
//! `stable_rules`. This makes "accidentally freezing something new" a
//! non-event: a brand-new symbol lands in the Experimental file until someone
//! deliberately adds a Stable rule for it.
//!
//! The walker, the line renderers, the tier matcher and the closure check
//! live in tools/snapshot_render.zig (the `snapshot-render` module), so other
//! snapshot gates can share them. This file holds the library's policy: the
//! rule tables, the builder rule, the file headers and the CLI.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const render = @import("snapshot-render");

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

const Rule = render.Rule;
const p = render.prefix;
const e = render.exact;

// Exclusion overrides: paths that a `stable_rules` prefix would otherwise
// sweep in, but which the Freeze scope explicitly names as Experimental. These
// are checked FIRST and force Experimental regardless of any Stable prefix.
// The L3 third-party-hosted machinery lives on the otherwise-Stable `CapTable`
// under `rpc.caps.table.*`; the plan lists `markThirdPartyHosted` (the
// "thirdPartyHosted emission") as L3, so its whole get/set/clear trio is held
// out of the frozen contract.
const experimental_overrides = [_]Rule{
    e("capnpc-zig.request.parseCodeGeneratorRequestMessage"),
    e("capnpc-zig.rpc.caps.table.payload_remap.clonePayloadWithRemappedCapsWithOptions"),
    // Generated reflection handles remain Experimental through this Stable
    // wire builder alias, matching the reflection runtime they expose.
    e("capnpc-zig.rpc.wire.protocol.PayloadBuilder.capnpSchema"),
    // Borrowed generated readers and their storage are an additive
    // Experimental API, including through this otherwise Stable alias.
    e("capnpc-zig.rpc.wire.protocol.PayloadBuilder.asReader"),
    // The framer's read cursor is implementation state (Zig has no private
    // fields). Freezing it would freeze one buffering strategy; the frozen
    // contract is `push` / `popFrame` / `bufferedBytes` / `reset`.
    e("capnpc-zig.rpc.wire.framing.Framer.consumed"),
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
    // Typed promise rejection (capnp-swift handoff H5) lands Experimental with
    // the `Peer` method that sends it. Its untyped sibling
    // `buildResolveException` stays frozen.
    e("capnpc-zig.rpc.wire.protocol.MessageBuilder.buildResolveExceptionTyped"),
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
    // CANONICAL: promoted to Stable 2026-08-26, deliberately, whole subtree.
    // Downstream consensus consumers pin their signing preimages to
    // `canonicalizeFlat`'s bytes — for them a byte change is not an API
    // break but a permanent network fork, so the module carries the same
    // freeze discipline as the wire format it walks. Frozen together with
    // the FromBuilder entry points and `Message.initFlat` (already under
    // the `message` prefix) so the whole flat-canonical surface pins in
    // one pass. Byte behavior itself is pinned by the acceptance-suite
    // ports and the `capnp convert binary:canonical` differential tests in
    // tests/serialization/canonical_test.zig.
    p("capnpc-zig.canonical"),
    // CODEGEN ABI: frozen deliberately. Every generated file looks these up
    // by name (`@hasDecl(runtime, "codegen_abi")`, then `.version`,
    // `.oldest_supported` and `.release`), so renaming or retyping one breaks
    // every binding a consumer has already generated. Values change only on a
    // reviewed ABI bump.
    p("capnpc-zig.codegen_abi"),
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

// The library's surface, as the shared walker sees it. Values are not
// rendered: the library snapshots pin a const by its type only, and their
// files predate value rendering (a generated-shape snapshot turns it on).
const snapshot = render.Snapshot(.{
    .root = capnpc,
    .root_path = "capnpc-zig",
    .max_depth = max_depth,
    .stable_rules = &stable_rules,
    .experimental_overrides = &experimental_overrides,
    .render_const_values = false,
});

const Entry = render.Entry;
const tierIsStable = snapshot.tierIsStable;

/// True when some container segment of `path` (any segment but the leaf) is a
/// builder type: `message.StructBuilder.writePointerList`,
/// `rpc.wire.protocol.MessageBuilder.buildAccept`,
/// `message.typed_list_helpers.CapabilityListBuilder.set`, ...
fn isBuilderMember(comptime path: []const u8) bool {
    const leaf_start = (std.mem.lastIndexOfScalar(u8, path, '.') orelse return false);
    var it = std.mem.splitScalar(u8, path[0..leaf_start], '.');
    while (it.next()) |segment| {
        if (std.mem.endsWith(u8, segment, "Builder")) return true;
    }
    return false;
}

/// Builder primitives that are not members of a `*Builder` container: free
/// functions that write message content for generated or wire builders. The
/// builder rule covers them like builder members. Every path must name a
/// Stable declaration; the comptime check after `stable_builder_entries`
/// enforces that, so a rename cannot drop one out of the rule silently.
const builder_free_functions = [_][]const u8{
    // Deep copy behind every generated copy setter (`message.CopyError`).
    "capnpc-zig.message.cloneAnyPointer",
    "capnpc-zig.message.cloneAnyPointerToBytes",
    // List codecs behind the generated nested-list views.
    "capnpc-zig.message.typed_list_helpers.CapabilityListCodec.init",
    "capnpc-zig.message.typed_list_helpers.CapabilityListCodec.initInSegment",
    "capnpc-zig.message.typed_list_helpers.DataListCodec.init",
    "capnpc-zig.message.typed_list_helpers.DataListCodec.initInSegment",
    "capnpc-zig.message.typed_list_helpers.RawPointerListCodec.init",
    "capnpc-zig.message.typed_list_helpers.RawPointerListCodec.initInSegment",
    "capnpc-zig.message.typed_list_helpers.RawStructNestedBuilderCodec.init",
    "capnpc-zig.message.typed_list_helpers.RawStructNestedBuilderCodec.initInSegment",
    // CapDescriptor writers the RPC wire builders call.
    "capnpc-zig.rpc.wire.protocol.CapDescriptor.writeReceiverAnswer",
    "capnpc-zig.rpc.wire.protocol.CapDescriptor.writeThirdPartyHosted",
    "capnpc-zig.rpc.wire.protocol.CapDescriptor.writeThirdPartyHostedNull",
};

fn isBuilderLine(comptime path: []const u8) bool {
    if (isBuilderMember(path)) return true;
    for (builder_free_functions) |free_fn| {
        if (std.mem.eql(u8, path, free_fn)) return true;
    }
    return false;
}

/// Reader-side debt that the builder rule tolerates. The list reader makers are
/// typed `anyerror`, and `@typeName` spells their fn-pointer types inside every
/// reader type (`AnyPointerReader`, `PointerListReader`, ...) that a builder
/// signature takes as a parameter. Narrowing them needs a `ReadError` set, a
/// separate follow-up. The rule removes exactly these renderings before it
/// looks for `anyerror`, so `anyerror` anywhere else on a builder line still
/// fails. Each rendering must still occur on some Stable builder line;
/// `enforceBuilderRule` reports a stale one, so the exemption goes away with
/// the debt.
const reader_maker_renderings = [_][]const u8{
    "@as(*const fn (u3, u32) anyerror!usize, @ptrCast(&serialization.message.listContentBytes))",
    "@as(*const fn (u64) anyerror!u32, @ptrCast(&serialization.message.decodeCapabilityPointer))",
};

const all_entries: []const Entry = snapshot.entries;

// Every rule must name a declaration that actually exists.
//
// A rule for a symbol that was never there (or has since been renamed) is
// silent: it documents a contract nobody can rely on, and it would mask a typo
// in a future promotion. This assertion makes the rules list self-validating —
// it is what found `Peer.run`, a frozen "lifecycle entry point" that is not a
// method on `Peer` at all.
comptime {
    if (snapshot.dead_rules.len != 0) {
        @compileError("api_snapshot: rule(s) match no declaration — remove them or fix the path:" ++ snapshot.dead_rules);
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

const closure_violations = snapshot.closure_violations;

/// The flat entry list split into the two tiers at comptime.
const stable_lines: []const []const u8 = snapshot.stable_lines;
const experimental_lines: []const []const u8 = snapshot.experimental_lines;

/// Stable builder lines: members of builder types plus `builder_free_functions`.
///
/// Builders are what generated `initX`/`setX` code calls, and an `anyerror`
/// anywhere on that path makes the generated signature `anyerror` too: that is
/// how `writePointerList` and the `anyerror`-typed pointer makers kept four
/// slcp entry points out of its Stable tier. Builder primitives now spell
/// `message.BuildError` / `message.CopyError`; `enforceBuilderRule` keeps a
/// frozen builder line from regressing to `anyerror` (the snapshot diff alone
/// would show it, but a reviewer refreezing the file would accept it). Reader
/// and RPC-callback lines are out of scope: several legitimately carry
/// user-callback errors.
const stable_builder_entries: []const Entry = blk: {
    @setEvalBranchQuota(20_000_000);
    var out: []const Entry = &.{};
    for (all_entries) |entry| {
        if (tierIsStable(entry.path) and isBuilderLine(entry.path)) {
            out = out ++ [_]Entry{entry};
        }
    }
    break :blk out;
};

comptime {
    @setEvalBranchQuota(20_000_000);
    var missing: []const u8 = "";
    for (builder_free_functions) |free_fn| {
        const found = for (stable_builder_entries) |entry| {
            if (std.mem.eql(u8, entry.path, free_fn)) break true;
        } else false;
        if (!found) missing = missing ++ "\n  " ++ free_fn;
    }
    if (missing.len != 0) {
        @compileError("api_snapshot: builder_free_functions entries that name no Stable declaration — fix the path or remove them:" ++ missing);
    }
}

fn isIdentifierByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

/// True when `text` holds `anyerror` as a whole token (not inside a longer
/// identifier).
fn containsAnyerrorToken(text: []const u8) bool {
    const token = "anyerror";
    var start: usize = 0;
    while (std.mem.indexOfPos(u8, text, start, token)) |idx| {
        const end = idx + token.len;
        const starts_token = idx == 0 or !isIdentifierByte(text[idx - 1]);
        const ends_token = end == text.len or !isIdentifierByte(text[end]);
        if (starts_token and ends_token) return true;
        start = end;
    }
    return false;
}

/// The builder rule. Fails when a Stable builder line renders `anyerror`
/// outside `reader_maker_renderings`, or when one of those renderings no
/// longer occurs on any Stable builder line (a stale exemption). Both
/// `--write` and `--check` run it, so the contract cannot be refrozen around
/// a violation.
fn enforceBuilderRule(allocator: std.mem.Allocator) !void {
    var exemption_hits: [reader_maker_renderings.len]usize = @splat(0);
    var violations: usize = 0;
    for (stable_builder_entries) |entry| {
        var line: []const u8 = entry.line;
        var owned: ?[]u8 = null;
        defer if (owned) |buf| allocator.free(buf);
        for (reader_maker_renderings, &exemption_hits) |rendering, *hits| {
            const count = std.mem.count(u8, line, rendering);
            if (count == 0) continue;
            hits.* += count;
            const stripped = try allocator.alloc(u8, std.mem.replacementSize(u8, line, rendering, ""));
            _ = std.mem.replace(u8, line, rendering, "", stripped);
            if (owned) |buf| allocator.free(buf);
            owned = stripped;
            line = stripped;
        }
        if (!containsAnyerrorToken(line)) continue;
        if (violations == 0) {
            std.debug.print(
                "api-snapshot: STABLE builder line(s) render `anyerror`. Builder primitives and generated\n" ++
                    "builders must spell a named set (message.BuildError / message.CopyError):\n",
                .{},
            );
        }
        violations += 1;
        std.debug.print("  {s}\n", .{entry.path});
    }

    var stale: usize = 0;
    for (reader_maker_renderings, exemption_hits) |rendering, hits| {
        if (hits != 0) continue;
        if (stale == 0) {
            std.debug.print(
                "api-snapshot: reader_maker_renderings entries that no Stable builder line contains any\n" ++
                    "more. Remove them (the reader makers they excused have changed):\n",
                .{},
            );
        }
        stale += 1;
        std.debug.print("  {s}\n", .{rendering});
    }

    if (violations != 0) return error.StableBuilderAnyerror;
    if (stale != 0) return error.StaleBuilderRuleExemption;
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
    // Report EVERY drifting line (capped), not just the first: platform-
    // render drifts are same-line substitutions scattered through the file,
    // and first-only reporting costs one full CI round trip per line. An
    // insertion/deletion makes every later line "drift"; the cap keeps that
    // cascade readable.
    var drifts: usize = 0;
    const max_reported = 25;
    while (true) : (line_no += 1) {
        const a = existing_it.next();
        const b = rendered_it.next();
        if (a == null and b == null) break;
        const a_line = a orelse "<end of snapshot>";
        const b_line = b orelse "<end of live surface>";
        if (!std.mem.eql(u8, a_line, b_line)) {
            drifts += 1;
            if (drifts <= max_reported) {
                std.debug.print(
                    "api-snapshot: drift in {s} at line {}:\n  snapshot: {s}\n  live:     {s}\n",
                    .{ path, line_no, a_line, b_line },
                );
            }
        }
    }
    if (drifts > max_reported) {
        std.debug.print(
            "api-snapshot: ... and {} more drifting lines (an insertion or deletion cascades; regenerate to resolve)\n",
            .{drifts - max_reported},
        );
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

    // Both writing and checking refuse a Stable builder line that renders
    // `anyerror`, so the contract cannot be refrozen around one.
    try enforceBuilderRule(allocator);

    const stable_rendered = try render.renderSnapshot(allocator, stable_lines, stable_header);
    defer allocator.free(stable_rendered);
    const experimental_rendered = try render.renderSnapshot(allocator, experimental_lines, experimental_header);
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
