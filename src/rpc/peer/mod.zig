const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_peer);
const events = @import("../events.zig");
const rpc_time = @import("../time.zig");
const protocol = @import("../wire/protocol.zig");
const cap_table = @import("../caps/table.zig");
const message = @import("../../serialization/message.zig");
pub const dispatch = @import("./dispatch.zig");
pub const bootstrap = @import("./bootstrap.zig");
pub const persistence = @import("./persistence.zig");
pub const finish = @import("./finish.zig");
pub const resolve = @import("./resolve.zig");
pub const disembargo = @import("./disembargo.zig");
pub const provide_accept_join = @import("./provide_accept_join.zig");
pub const third_party = @import("./third_party.zig");

const peer_dispatch = dispatch;
const peer_bootstrap = bootstrap;
const peer_finish = finish;
const peer_resolve = resolve;
const peer_disembargo = disembargo;
const peer_provide_accept_join = provide_accept_join;
const peer_third_party = third_party;
const peer_call_targets = @import("./call/peer_call_targets.zig");
const peer_call_sender = @import("./call/peer_call_sender.zig");
const payload_remap = @import("../caps/payload_remap.zig");
const pending_calls = @import("../promises/pending_calls.zig");
const peer_inbound_release = @import("./peer_inbound_release.zig");
const peer_embargo_accepts = peer_provide_accept_join.embargo_accepts;
const peer_join_state = peer_provide_accept_join.join_state;
const peer_provides_state = peer_provide_accept_join.provides_state;
const peer_provide_join_orchestration = peer_provide_accept_join.orchestration;
const peer_forward_orchestration = @import("./forward/peer_forward_orchestration.zig");
const peer_forward_return_callbacks = @import("./forward/peer_forward_return_callbacks.zig");
const peer_cap_lifecycle = @import("./peer_cap_lifecycle.zig");
const peer_outbound_control = @import("./peer_outbound_control.zig");
const peer_call_orchestration = @import("./call/peer_call_orchestration.zig");
const peer_return_orchestration = @import("./return/peer_return_orchestration.zig");
const peer_third_party_adoption = peer_third_party.adoption;
const peer_return_dispatch = @import("./return/peer_return_dispatch.zig");
const peer_third_party_returns = peer_third_party.returns;
const return_routing = @import("../promises/return_routing.zig");
const return_send = @import("../promises/return_send.zig");
const peer_transport = @import("./transport.zig");
const vat_network = @import("../vat/network.zig");
const peer_transport_callbacks = peer_transport.callbacks;
const peer_transport_state = peer_transport.state;
const peer_question_state = @import("./peer_question_state.zig");
const peer_cleanup = @import("./peer_cleanup.zig");
const peer_return_frames = @import("./return/peer_return_frames.zig");
const join_network = @import("../vat/join.zig");
const vat_provisions = @import("../vat/provisions.zig");

pub const errors = @import("./errors.zig");
pub const state = @import("./state.zig");

/// Typed failure surface of generated Response.unwrap() (see errors.zig).
/// Re-exported here so generated code and consumers can spell it
/// `rpc.peer.CallError`.
pub const CallError = errors.CallError;

/// Callback invoked to populate a `CallBuilder` before sending an outbound call.
pub const CallBuildFn = *const fn (ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void;
/// Callback invoked to populate a `ReturnBuilder` before sending a return.
pub const ReturnBuildFn = *const fn (ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void;
/// Handler invoked when an inbound call arrives for an exported capability.
pub const CallHandler = *const fn (ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void;
/// Callback invoked when a return message arrives for a previously sent question.
pub const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
/// Callback invoked when a Level-3 three-party handoff auto-pickup completes: an
/// inbound `thirdPartyHosted` Resolve for `promise_id` (on the peer holding the
/// promise, VatA↔VatB) drove a `Provide` pickup, and the `Accept` `Return`
/// carrying the direct capability just arrived on `accept_peer` (VatA↔VatC).
/// The handler imports the direct cap from `ret`/`accept_caps` on `accept_peer`
/// (retain it there — see `InboundCapTable.retainCapability`) so subsequent calls
/// go straight to the third vat. `promise_id` names the fulfilled promise import
/// on the promise-holding peer. The vine is released by the runtime immediately
/// after this returns (driving VatB's Provide `Finish`), so the handler must not
/// depend on the vine surviving. Errors are reported non-fatally.
pub const HandoffPickupCallback = *const fn (
    ctx: *anyopaque,
    promise_peer: *Peer,
    promise_id: u32,
    accept_peer: *Peer,
    ret: protocol.Return,
    accept_caps: *const cap_table.InboundCapTable,
) anyerror!void;
/// Optional override for outbound frame delivery (used in testing).
pub const SendFrameOverride = *const fn (ctx: *anyopaque, frame: []const u8) anyerror!void;

/// Exception reason delivered to every outstanding question's `on_return` when
/// the transport closes (see `onConnectionClose`). The terminal
/// disconnect-failure signal is a synthetic exception `Return` routed through
/// the normal `QuestionCallback`; it is never recorded in `resolved_answers`
/// nor replayed to pipelining, so callers observe a one-shot failure.
pub const disconnected_reason = "disconnected";

/// Exception reason for a question cancelled by deadline expiry (see
/// `checkDeadlines`). Exported so response-unwrapping code can match the
/// locally synthesized reason without duplicating the literal.
pub const deadline_reason = "deadline exceeded";

/// Exception reason for questions force-cancelled when the graceful-shutdown
/// drain bound expires (see `checkDeadlines`).
pub const shutdown_reason = "peer shutting down";

/// An exported capability: a context pointer and its call handler.
pub const Export = struct {
    ctx: *anyopaque,
    on_call: CallHandler,
};

/// Handler invoked when the remote calls `Persistent.save()` on an export
/// marked persistent via `Peer.setPersistentExport`. Returns the app-defined
/// sturdy-ref payload, allocated with `peer.allocator`; the peer embeds it
/// in the `SaveResults` Return and frees it. `seal_for` is the raw
/// `SaveParams.sealFor` pointer when the caller supplied one (sealing is
/// realm-defined; handlers are free to ignore it).
pub const SaveHandler = *const fn (ctx: *anyopaque, peer: *Peer, export_id: u32, seal_for: ?message.AnyPointerReader) anyerror![]u8;
/// What a restorer hook resolved a sturdy ref to (see `Peer.setRestorer`).
pub const RestoreOutcome = persistence.RestoreOutcome(Export);
/// Handler invoked when the remote calls the vat-level restore method on
/// the bootstrap capability. The sturdy-ref bytes are borrowed from the
/// inbound frame; copy them to retain beyond the call.
pub const RestoreHandler = *const fn (ctx: *anyopaque, peer: *Peer, sturdy_ref: []const u8) anyerror!RestoreOutcome;
/// Outcome of a `Peer.sendSave` question, delivered to its callback.
pub const SaveResponse = union(enum) {
    /// Sturdy-ref payload, borrowed from the Return frame; copy to retain.
    sturdy_ref: []const u8,
    exception: protocol.Exception,
    /// Any other Return tag (canceled, resultsSentElsewhere, ...).
    other: protocol.ReturnTag,
};
/// Callback invoked when a `Peer.sendSave` question completes.
pub const SaveResponseCallback = *const fn (ctx: *anyopaque, peer: *Peer, response: SaveResponse) anyerror!void;
/// Outcome of a `Peer.sendRestore` question, delivered to its callback.
pub const RestoreResponse = union(enum) {
    /// The restored capability (retained; typically `.imported`). Pass to
    /// `sendCall`/`sendCallResolved` to resume calling.
    cap: cap_table.ResolvedCap,
    exception: protocol.Exception,
    /// Any other Return tag (canceled, resultsSentElsewhere, ...).
    other: protocol.ReturnTag,
};
/// Callback invoked when a `Peer.sendRestore` question completes.
pub const RestoreResponseCallback = *const fn (ctx: *anyopaque, peer: *Peer, response: RestoreResponse) anyerror!void;

/// A registered save hook: context and handler always travel together so
/// dispatch never has to unwrap them independently.
const SaveHook = struct {
    ctx: *anyopaque,
    on_save: SaveHandler,
};

/// A registered restore hook (see `SaveHook`).
const RestoreHook = struct {
    ctx: *anyopaque,
    on_restore: RestoreHandler,
};

/// Persistence hooks installed on one export. The peer wraps the export's
/// original handler with a trampoline that serves `Persistent.save()` and
/// vat-level restore calls, forwarding everything else to the original.
const PersistenceState = struct {
    export_id: u32,
    original: Export,
    save: ?SaveHook = null,
    restore: ?RestoreHook = null,
};

const ExportEntry = state.ExportEntry(Export);
const ResolvedAnswer = state.ResolvedAnswer;
const PendingCall = state.PendingCall;

const ProvideTarget = state.ProvideTarget;
const ProvideEntry = state.ProvideEntry;

/// The record an accept peer keeps for each queued/parked cross-peer Accept.
/// `embargo_key` is the accept peer's OWN dupe (its allocator), so no cleanup
/// path ever hashes bytes owned by — and possibly freed with — the provision's
/// embargo map.
const CrossPeerAcceptRecord = struct {
    /// UNCOUNTED back-link (INV-REC: the pending slot's / parked entry's +1
    /// keeps the provision alive as long as this record exists).
    provision: *ProvisionIndex.Provision,
    /// Owned by THIS peer's allocator; null for a parked non-embargoed accept.
    embargo_key: ?[]u8,
    parked: bool,
};
const JoinKeyPart = state.JoinKeyPart;
const JoinPartEntry = state.JoinPartEntry;
const JoinState = state.JoinState;
const PendingJoinQuestion = state.PendingJoinQuestion;
const PendingEmbargoedAccept = state.PendingEmbargoedAccept;

const ForwardReturnMode = peer_forward_orchestration.ForwardReturnMode;

const ResolvedImport = state.ResolvedImport;

pub const PeerLimits = state.PeerLimits;
pub const PeerTimeouts = state.PeerTimeouts;

const QuestionDeinitCtxFn = state.QuestionDeinitCtxFn;
const ExportDeinitCtxFn = *const fn (std.mem.Allocator, *anyopaque) void;
const Question = state.Question(QuestionCallback);
const PendingThirdPartyAwait = state.PendingThirdPartyAwait(Question);

/// Low bound of the callee-allocated `ThirdPartyAnswer` answer-id range
/// (rpc.capnp:936-941): bit 30 set. The high bound is 2^31 (bit 31 clear).
/// Every id `Peer.sendThirdPartyAnswer` mints satisfies
/// `third_party.isThirdPartyAnswerId`.
const third_party_answer_id_base: u32 = 0x4000_0000;
const third_party_answer_id_limit: u32 = 0x8000_0000;

const ForwardCallContext = struct {
    peer: *Peer,
    payload: protocol.Payload,
    inbound_caps: cap_table.InboundCapTable,
    send_results_to: protocol.SendResultsToTag,
    send_results_to_third_party_payload: ?[]u8 = null,
    answer_id: u32,
    mode: ForwardReturnMode,

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *ForwardCallContext = @ptrCast(@alignCast(ctx_ptr));
        ctx.inbound_caps.deinit();
        if (ctx.send_results_to_third_party_payload) |payload| allocator.free(payload);
        allocator.destroy(ctx);
    }
};

const ForwardReturnBuildContext = struct {
    peer: *Peer,
    payload: protocol.Payload,
    inbound_caps: *const cap_table.InboundCapTable,
};

/// L3 parked-call FORWARDING relay state (issue #56). Threads a call that was
/// pipelined by VatA on a handed-off promise — parked at VatB, then replayed
/// onto the handoff vine — through to VatC and relays VatC's Return back to
/// complete VatA's ORIGINAL pipelined question.
///
/// It spans two peers: the forwarded call goes out on `provide_peer` (B↔C, the
/// peer field carried by the outbound question), but the relayed result must
/// complete VatA's question on `recipient_peer` (B↔A) under
/// `recipient_answer_id`. Heap-allocated because the forwarded question outlives
/// the synchronous replay in a real async transport; freed by
/// `forwardVineReturn` on the single Return, or by the forwarded question's
/// `deinit_ctx` if the B↔C peer tears down first.
const ForwardVineCallContext = struct {
    /// The peer used to forward the call to VatC (B↔C). Parameter-cap proxy
    /// exports created while building the forwarded call live on this peer.
    forward_peer: *Peer,
    /// The peer that received VatA's original pipelined call (B↔A). Its
    /// `recipient_answer_id` active-inbound question is completed with VatC's
    /// result.
    recipient_peer: *Peer,
    /// VatA's original pipelined question id on `recipient_peer` (B↔A).
    recipient_answer_id: u32,
    /// VatA's parked call params, cloned into the forwarded call sent to VatC.
    /// Read ONLY by `forwardVineParams`, which runs synchronously inside the
    /// `sendCall` that registers this ctx as the outbound question's callback —
    /// while the parked frame these readers point into is still alive. It is
    /// never touched after `sendCall` returns (the later `forwardVineReturn`
    /// reads only `recipient_peer` / `recipient_answer_id` / `settled_flag`), so
    /// the ctx may safely outlive the parked frame.
    source_params: protocol.Payload,
    /// VatA's parked call cap table. Only used synchronously by
    /// `forwardVineParams`, but stored here so capability pointers in
    /// `source_params` can be remapped into B↔C proxy exports.
    source_inbound_caps: *cap_table.InboundCapTable,
    created_param_proxy_ids: std.ArrayList(u32) = .empty,
    param_proxies_committed: bool = false,
    /// One-shot back-signal to `forwardVineCallToProvidedCap`, set true by
    /// `forwardVineReturn` the instant the return callback runs (before it frees
    /// this ctx). It points at a stack-local `bool` in that caller and is valid
    /// ONLY while the callback runs SYNCHRONOUSLY nested inside `sendCall` (the
    /// loopback case), where that frame is still live. The caller nulls it before
    /// its frame unwinds on the async hand-off path, so a later callback never
    /// writes through a dangling pointer. It lets the caller's post-`sendCall`
    /// code (including its error path) distinguish "the callback already answered
    /// VatA and freed this ctx" from "the callback never ran" — so a `sendCall`
    /// that fails in post-callback work does not double-answer VatA or double-free
    /// this ctx.
    settled_flag: ?*bool = null,

    fn rollbackParamProxies(ctx: *ForwardVineCallContext) void {
        if (ctx.param_proxies_committed) return;
        for (ctx.created_param_proxy_ids.items) |id| {
            ctx.forward_peer.destroyUnreferencedProxyExport(id);
        }
    }

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *ForwardVineCallContext = @ptrCast(@alignCast(ctx_ptr));
        ctx.rollbackParamProxies();
        ctx.created_param_proxy_ids.deinit(allocator);
        allocator.destroy(ctx);
    }
};

const CrossPeerProxyContext = struct {
    owner_peer: *Peer,
    export_id: u32 = 0,
    /// Peer whose cap table can call the original capability represented by
    /// `target`. When the proxy is released, an owned source import ref is
    /// released on this peer if one was retained.
    source_peer: ?*Peer,
    target: cap_table.ResolvedCap,
    release_source_import_id: ?u32 = null,
    /// A handoff-held pin on a SOURCE-peer export that this proxy owns and
    /// releases exactly once in deinit (ownership transfers AT the
    /// `addCrossPeerProxyExport` call, success or failure — the caller never
    /// rolls it back afterwards). Keeps the proxied export alive across the
    /// introducer's Finish + the remote's wire Releases; abandoned (nulled,
    /// never released) if the source peer dies first, same rule as
    /// `release_source_import_id`.
    release_source_export_pin_id: ?u32 = null,

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *CrossPeerProxyContext = @ptrCast(@alignCast(ctx_ptr));
        if (ctx.source_peer) |source_peer| {
            source_peer.deregisterCrossPeerProxy(ctx.owner_peer, ctx.export_id);
            if (ctx.release_source_import_id) |import_id| {
                source_peer.releaseImport(import_id, 1) catch |err| {
                    log.debug("cross-peer proxy: failed to release source import {}: {}", .{ import_id, err });
                };
            }
            if (ctx.release_source_export_pin_id) |pin_id| {
                source_peer.releaseHandoffHeldExport(pin_id);
            }
        }
        allocator.destroy(ctx);
    }

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *CrossPeerProxyContext = castCtx(*CrossPeerProxyContext, ctx_ptr);
        const source_peer = ctx.source_peer orelse {
            try peer.sendReturnException(call.question_id, "cross-peer proxy source disconnected");
            return;
        };
        try peer.forwardCrossPeerProxyCall(call, inbound_caps, source_peer, ctx.target);
    }
};

const CrossPeerProxyCallContext = struct {
    recipient_peer: *Peer,
    recipient_answer_id: u32,
    forward_peer: *Peer,
    target: cap_table.ResolvedCap,
    source_params: protocol.Payload,
    source_inbound_caps: *cap_table.InboundCapTable,
    created_param_proxy_ids: std.ArrayList(u32) = .empty,
    param_proxies_committed: bool = false,
    settled_flag: ?*bool = null,

    fn rollbackParamProxies(ctx: *CrossPeerProxyCallContext) void {
        if (ctx.param_proxies_committed) return;
        for (ctx.created_param_proxy_ids.items) |id| {
            ctx.forward_peer.destroyUnreferencedProxyExport(id);
        }
    }

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *CrossPeerProxyCallContext = @ptrCast(@alignCast(ctx_ptr));
        ctx.rollbackParamProxies();
        ctx.created_param_proxy_ids.deinit(allocator);
        allocator.destroy(ctx);
    }
};

const CrossPeerJoinRelayContext = struct {
    owner_peer: *Peer,
    owner_answer_id: u32,
    settled_flag: ?*bool = null,

    fn deinit(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
        const ctx: *CrossPeerJoinRelayContext = @ptrCast(@alignCast(ctx_ptr));
        allocator.destroy(ctx);
    }
};

const CrossPeerCapMapContext = struct {
    inbound_peer: *Peer,
    outbound_peer: *Peer,
    inbound_caps: *cap_table.InboundCapTable,
    created_proxy_ids: *std.ArrayList(u32),
    remapped_by_index: std.AutoHashMap(u32, u32),

    fn init(
        inbound_peer: *Peer,
        outbound_peer: *Peer,
        inbound_caps: *cap_table.InboundCapTable,
        created_proxy_ids: *std.ArrayList(u32),
    ) CrossPeerCapMapContext {
        return .{
            .inbound_peer = inbound_peer,
            .outbound_peer = outbound_peer,
            .inbound_caps = inbound_caps,
            .created_proxy_ids = created_proxy_ids,
            .remapped_by_index = std.AutoHashMap(u32, u32).init(outbound_peer.allocator),
        };
    }

    fn deinit(ctx: *CrossPeerCapMapContext) void {
        ctx.remapped_by_index.deinit();
    }
};

const CrossPeerReturnRelayContext = struct {
    source_peer: *Peer,
    target_peer: *Peer,
    source: protocol.Payload,
    source_inbound_caps: *cap_table.InboundCapTable,
    release_param_caps: bool = true,
    created_result_proxy_ids: std.ArrayList(u32) = .empty,
    result_proxies_committed: bool = false,

    fn rollbackResultProxies(ctx: *CrossPeerReturnRelayContext) void {
        if (ctx.result_proxies_committed) return;
        for (ctx.created_result_proxy_ids.items) |id| {
            ctx.target_peer.destroyUnreferencedProxyExport(id);
        }
    }

    fn deinit(ctx: *CrossPeerReturnRelayContext, allocator: std.mem.Allocator) void {
        ctx.rollbackResultProxies();
        ctx.created_result_proxy_ids.deinit(allocator);
    }
};

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

fn msToNs(ms: u64) i64 {
    return @as(i64, @intCast(ms)) * std.time.ns_per_ms;
}

/// A Cap'n Proto RPC peer that manages one side of a two-party connection.
///
/// `Peer` tracks exported capabilities, outstanding questions, promise
/// resolution, and three-party handoff state. It is **not** thread-safe;
/// all calls must be made from the thread that owns the associated event
/// loop (or under external synchronization). In debug builds, key entry
/// points assert thread affinity to catch violations early. Use
/// `initDetached` for environments without a real transport (WASM, unit
/// tests).
///
/// ## Peer lifecycle (state machine)
///
/// ```text
///   +------------+ attachConnection / attachTransportBinding  +----------+
///   |  Detached  | ----------------------------------------> | Attached |
///   | (no I/O)   | <---------------------------------------- | (idle)   |
///   +------------+     detachConnection / detachTransport     +----------+
///                                                                  |
///                                                           start()
///                                                                  v
///                                                             +----------+
///                                                             |  Active  |
///                                                             | (r/w)    |
///                                                             +----------+
///                                                                  |
///                                             transport close / error / deinit
///                                                                  v
///                                                             +----------+
///                                                             |  Closed  |
///                                                             +----------+
/// ```
///
/// * **Detached** -- Created via `initDetached`. No transport is wired up;
///   frames can still be injected manually via `handleFrame` and sent
///   through a `setSendFrameOverride` callback.
/// * **Attached** -- A transport (typically a `Connection`) has been bound
///   via `attachConnection`, `attachTransport`, or `attachTransportBinding`,
///   but `start()` has not been
///   called yet. No I/O occurs.
/// * **Active** -- After `start()`, the transport begins reading inbound
///   frames and the peer processes them. Outbound calls, returns, and
///   control messages flow through `sendFrame`.
/// * **Closed** -- The transport signaled a close (EOF, error, or explicit
///   `close`). The `on_close` callback fires. The peer should be
///   `deinit`-ed after this.
///
/// ## State maps
///
/// The peer maintains many hash maps to track the RPC protocol state.
/// Here is a summary of each:
///
/// | Map | Key | Value | Purpose |
/// |-----|-----|-------|---------|
/// | `exports` | export ID | `ExportEntry` | Local capabilities offered to the remote peer. Ref-counted (wire refs from Release plus answer-held refs from recorded answers); removed when both counts reach zero. |
/// | `questions` | question ID | `Question` | Outstanding outbound calls awaiting a Return. Removed when the Return arrives. |
/// | `question_param_export_refs` | question ID | export IDs (list) | Wire refs the question's Call params took on local exports (one per emitted senderHosted/senderPromise descriptor). Spent when the Return arrives with `releaseParamCaps = true` (the rpc.capnp default), dropped when it is false (the remote sends explicit Releases), freed unspent when the question dies without a wire Return. |
/// | `resolved_answers` | question ID | `ResolvedAnswer` | Cached Return frames for answered questions (used to resolve PromisedAnswer references). Holds one answer-held reference per results export so pipeline targets survive an early Release. Removed on Finish, releasing those references. |
/// | `pending_promises` | question ID | `ArrayList(PendingCall)` | Calls targeting a PromisedAnswer whose Return has not yet arrived. Replayed once the answer resolves. |
/// | `pending_export_promises` | export ID | `ArrayList(PendingCall)` | Calls targeting a promise export not yet resolved. Replayed on `resolvePromiseExportToExport`. |
/// | `forwarded_questions` | original answer ID | forwarded question ID | Maps an inbound call's answer ID to the question ID of the forwarded outbound call. |
/// | `forwarded_tail_questions` | original answer ID | forwarded question ID | Like `forwarded_questions` but for tail-call forwarding (takeFromOtherQuestion). |
/// | `provides_by_question` | question ID | `ProvideEntry` | Active Provide operations indexed by their answer question ID. |
/// | `provides_by_key` | recipient key (bytes) | question ID | Active Provide operations indexed by their recipient key for Accept lookup. |
/// | `pending_joins` | join ID | `JoinState` | In-progress Join operations collecting parts from multiple peers. |
/// | `pending_join_questions` | question ID | `PendingJoinQuestion` | Maps a Join answer's question ID back to its join ID and part number. |
/// | `pending_accepts_by_embargo` | embargo key (bytes) | `ArrayList(PendingEmbargoedAccept)` | Accept messages waiting for a disembargo before delivery. |
/// | `pending_accept_embargo_by_question` | question ID | embargo key (bytes) | Maps a question to its embargo key for cleanup on Finish. |
/// | `pending_third_party_awaits` | recipient key (bytes) | `PendingThirdPartyAwait` | Outbound third-party handoffs awaiting a ThirdPartyAnswer. |
/// | `pending_third_party_answers` | recipient key (bytes) | answer ID | Completed third-party answers awaiting adoption. |
/// | `pending_third_party_returns` | question ID | frame (bytes) | Return frames for third-party questions received before adoption. |
/// | `adopted_third_party_answers` | original question ID | adopted answer ID | Maps third-party questions to their locally adopted answer IDs. |
/// | `resolved_imports` | promise ID | `ResolvedImport` | Resolved promise imports (after a Resolve message). Tracks embargo state. |
/// | `pending_embargoes` | embargo ID | promise ID | In-flight disembargo operations, keyed by the embargo ID we allocated. |
/// | `loopback_questions` | question ID | void | Questions whose Return should be delivered locally (loopback / exported-cap calls). |
/// | `send_results_to_yourself` | answer ID | void | Inbound calls with `sendResultsTo = yourself`, meaning we send `resultsSentElsewhere`. |
/// | `send_results_to_third_party` | answer ID | optional payload | Inbound calls with `sendResultsTo = thirdParty`. Payload is the serialized recipient. |
/// | `persistent_exports` | export ID | `*PersistenceState` | Save/restore hooks for persistent exports. Removed when the hooks are cleared or the export is released. |
///
/// ## Invariants
///
/// * Question IDs are monotonically increasing (mod 2^32) and never reused
///   within a single peer lifetime.
/// * Each export ID has at most one entry in `exports`. The entry's
///   `ref_count` tracks how many times the remote peer has received the
///   capability in a message payload; its `answer_ref_count` tracks how many
///   recorded resolved answers carry it in their results. An inbound Release
///   spends only wire refs, so a still-unfinished answer keeps its pipeline
///   targets dispatchable regardless of how eagerly the remote releases.
/// * A question is removed from `questions` when its Return is fully
///   handled, but its `resolved_answers` entry persists until Finish so
///   that PromisedAnswer references can be resolved.
/// * `pending_promises` entries are drained (replayed or errored) when the
///   corresponding answer resolves, never left dangling.
/// Grouped transport callback bindings. Set/cleared atomically by
/// `Peer.attachTransportBinding` / `Peer.detachTransport`.
pub const TransportBinding = peer_transport.Binding(Peer);
pub const TransportStartFn = TransportBinding.StartFn;
pub const TransportSendFn = TransportBinding.SendFn;
pub const TransportCloseFn = TransportBinding.CloseFn;
pub const TransportIsClosingFn = TransportBinding.IsClosingFn;

/// Level-3 three-party addressing seam (see `rpc.vat.network`). Application
/// supplies one when originating `Provide`/`Accept` handoffs; the two-party core
/// runs unchanged without it.
pub const VatNetwork = vat_network.VatNetwork(Peer);
pub const Introduction = vat_network.Introduction;
pub const Introduced = vat_network.Introduced(Peer);
pub const JoinNetwork = join_network.JoinNetwork(Peer);
/// Experimental vat-wide L3 provision index (three-party HOSTING across
/// multiple connections of one vat). See src/rpc/vat/provisions.zig.
pub const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
pub const ProvisionIndexLimits = vat_provisions.ProvisionIndexLimits;
pub const Joined = join_network.Joined(Peer);

/// Handle returned by `Peer.sendProvide`: the ids the caller needs to drive the
/// paired `thirdPartyHosted` descriptor emission and to observe completion.
pub const ProvideHandle = struct {
    /// The held-open Provide question id (no Return; Finished when the recipient
    /// releases the vine).
    question_id: u32,
    /// The vine export id minted for this handoff. Marked third-party-hosted, so
    /// the next descriptor emitted for the provided cap resolves to
    /// `thirdPartyHosted{ contact, vineId = this }`.
    vine_id: u32,
};

/// Experimental Level-4 JoinResult coordinator for the compact Zig Join pilot.
///
/// This is the first high-level joiner-side helper above raw
/// `sendJoinExperimental`: it originates local compact Join key parts, collects
/// matching Zig `JoinResult` payloads through a `JoinNetwork`, sends the direct
/// follow-up `Accept`, retains the accepted capability, and Finishes the
/// JoinResult questions after the direct pickup succeeds. It remains
/// Experimental and Zig-shape-only; it does not define a stable Join key/result
/// format or a cross-implementation L4 contract.
pub const JoinCoordinator = struct {
    pub const Accepted = struct {
        peer: *Peer,
        cap: cap_table.ResolvedCap,
    };

    allocator: std.mem.Allocator,
    origin_peer: *Peer,
    join_network: JoinNetwork,
    join_id: u32,
    expected_parts: u16,
    question_ids: std.ArrayList(u32) = .empty,
    question_peers: std.ArrayList(*Peer) = .empty,
    question_finished: std.ArrayList(bool) = .empty,
    sent_parts: std.AutoHashMap(u16, u32),
    joined: std.ArrayList(Joined) = .empty,
    accept_question_id: ?u32 = null,
    accept_answer_peer: ?*Peer = null,
    accept_answer_id: ?u32 = null,
    accept_answer_finished: bool = true,
    accept_link_peer: ?*Peer = null,
    accept_send_in_progress: bool = false,
    accept_peer: ?*Peer = null,
    accepted_peer: ?*Peer = null,
    accepted_cap: ?cap_table.ResolvedCap = null,
    join_results_finished: bool = false,
    canceled: bool = false,
    mismatch_exceptions: u32 = 0,
    cancel_exceptions: u32 = 0,
    unexpected_exceptions: u32 = 0,
    accept_exceptions: u32 = 0,
    finish_failures: u32 = 0,

    pub fn init(
        allocator: std.mem.Allocator,
        origin_peer: *Peer,
        join_network_value: JoinNetwork,
        join_id: u32,
        expected_parts: u16,
    ) JoinCoordinator {
        return .{
            .allocator = allocator,
            .origin_peer = origin_peer,
            .join_network = join_network_value,
            .join_id = join_id,
            .expected_parts = expected_parts,
            .sent_parts = std.AutoHashMap(u16, u32).init(allocator),
        };
    }

    pub fn deinit(self: *@This()) void {
        self.cancelPending("join coordinator deinit") catch |err| {
            log.debug("failed to fully cancel L4 JoinCoordinator during deinit: {}", .{err});
        };
        self.clearAcceptPeerLink();
        for (self.joined.items) |*joined| joined.deinit(self.allocator);
        self.joined.deinit(self.allocator);
        self.sent_parts.deinit();
        self.question_finished.deinit(self.allocator);
        self.question_peers.deinit(self.allocator);
        self.question_ids.deinit(self.allocator);
    }

    /// Send one Join part to `peer`. The `target` must name the proxied
    /// capability being joined on that peer.
    pub fn sendPart(
        self: *@This(),
        peer: *Peer,
        target: protocol.MessageTarget,
        part_count: u16,
        part_num: u16,
    ) !u32 {
        if (self.canceled) return error.JoinCanceled;
        if (self.accept_question_id != null or self.accepted_cap != null) return error.JoinAlreadyAccepting;
        if (part_count == 0 or part_num >= part_count) return error.InvalidJoinKeyPart;
        if (self.expected_parts != 0 and part_count != self.expected_parts) return error.JoinPartCountMismatch;
        if (self.sent_parts.contains(part_num)) return error.DuplicateJoinPart;
        try self.question_ids.ensureUnusedCapacity(self.allocator, 1);
        try self.question_peers.ensureUnusedCapacity(self.allocator, 1);
        try self.question_finished.ensureUnusedCapacity(self.allocator, 1);
        try self.sent_parts.ensureUnusedCapacity(1);

        const key_bytes = try join_network.encodeJoinKeyPart(self.allocator, self.join_id, part_count, part_num);
        defer self.allocator.free(key_bytes);
        var key_msg = try message.Message.initUnvalidated(self.allocator, key_bytes);
        defer key_msg.deinit();

        const question_id = try Peer.sendJoinExperimentalWithAutoFinish(
            peer,
            target,
            try key_msg.getRootAnyPointer(),
            self,
            JoinCoordinator.onJoinReturn,
            true,
        );
        self.question_ids.appendAssumeCapacity(question_id);
        self.question_peers.appendAssumeCapacity(peer);
        self.question_finished.appendAssumeCapacity(false);
        self.sent_parts.putAssumeCapacityNoClobber(part_num, question_id);
        return question_id;
    }

    pub fn sendImportedCapPart(
        self: *@This(),
        peer: *Peer,
        target_import_id: u32,
        part_count: u16,
        part_num: u16,
    ) !u32 {
        return self.sendPart(
            peer,
            .{ .tag = .importedCap, .imported_cap = target_import_id, .promised_answer = null },
            part_count,
            part_num,
        );
    }

    /// Send the direct `Accept` once all expected JoinResults have arrived.
    /// The accepted capability is retained and can be read with `acceptedCap()`
    /// or transferred out with `takeAccepted()`.
    pub fn acceptFirst(self: *@This()) !u32 {
        if (self.accept_question_id) |question_id| return question_id;
        if (self.expected_parts == 0) return error.InvalidJoinKeyPart;
        if (self.mismatch_exceptions != 0 or
            self.cancel_exceptions != 0 or
            self.unexpected_exceptions != 0 or
            self.accept_exceptions != 0)
        {
            return error.JoinDidNotSucceed;
        }
        if (self.joined.items.len != self.expected_parts) return error.MissingJoinResults;

        const first = &self.joined.items[0];
        for (self.joined.items[1..]) |*joined| {
            if (joined.peer != first.peer or !std.mem.eql(u8, joined.provision, first.provision)) {
                self.mismatch_exceptions += 1;
                self.canceled = true;
                self.finishJoinResults() catch |err| {
                    self.finish_failures += 1;
                    log.debug("failed to finish L4 JoinResult questions after mismatch: {}", .{err});
                };
                self.clearJoinInputs();
                return error.JoinResultMismatch;
            }
        }

        var provision_msg = try message.Message.initUnvalidated(self.allocator, first.provision);
        defer provision_msg.deinit();
        const provision = try provision_msg.getRootAnyPointer();
        const direct_peer = first.peer;
        try self.registerAcceptPeerLink(direct_peer);
        self.accept_send_in_progress = true;
        defer self.accept_send_in_progress = false;
        const question_id = direct_peer.sendAcceptNoRestore(provision, null, self, JoinCoordinator.onAcceptReturn, true) catch |err| {
            if (self.accepted_cap != null or self.accept_exceptions != 0) {
                log.debug("L4 JoinCoordinator Accept settled before send returned trailing error: {}", .{err});
                const answer_id = self.accept_answer_id orelse return err;
                _ = self.finishAcceptAnswer(direct_peer, answer_id, "trailing Accept send error");
                return answer_id;
            }
            self.clearAcceptPeerLink();
            return err;
        };
        self.noteAcceptAnswerNeedsFinish(direct_peer, question_id);
        if (self.accepted_cap != null or self.accept_exceptions != 0) {
            _ = self.finishAcceptAnswer(direct_peer, question_id, "synchronous Accept");
        }
        if (self.accepted_cap == null and self.accept_exceptions == 0) {
            self.accept_question_id = question_id;
            self.accept_peer = direct_peer;
        }
        return question_id;
    }

    pub fn acceptedCap(self: *const @This()) ?cap_table.ResolvedCap {
        return self.accepted_cap;
    }

    pub fn acceptedPeer(self: *const @This()) ?*Peer {
        return self.accepted_peer;
    }

    /// Transfer ownership of the retained accepted cap to the caller. The
    /// caller must later release the returned cap on the returned peer.
    pub fn takeAccepted(self: *@This()) ?Accepted {
        self.retryAcceptAnswerFinish("takeAccepted") catch |err| {
            log.debug("failed to retry L4 JoinCoordinator Accept Finish before takeAccepted: {}", .{err});
        };
        const cap = self.accepted_cap orelse return null;
        const peer = self.accepted_peer orelse return null;
        self.accepted_cap = null;
        self.accepted_peer = null;
        self.clearAcceptPeerLinkIfDrained();
        return .{ .peer = peer, .cap = cap };
    }

    pub fn releaseAccepted(self: *@This()) !void {
        var first_err: ?anyerror = null;
        self.retryAcceptAnswerFinish("releaseAccepted") catch |err| {
            if (first_err == null) first_err = err;
        };

        const cap = self.accepted_cap orelse {
            if (first_err) |err| return err;
            return;
        };
        const peer = self.accepted_peer orelse {
            if (first_err) |err| return err;
            return error.MissingAcceptedPeer;
        };
        self.accepted_cap = null;
        self.accepted_peer = null;
        peer.releaseResolvedCap(cap) catch |err| {
            if (first_err == null) first_err = err;
        };
        self.clearAcceptPeerLinkIfDrained();
        if (first_err) |err| return err;
    }

    fn clearJoinedResults(self: *@This()) void {
        for (self.joined.items) |*joined| joined.deinit(self.allocator);
        self.joined.clearRetainingCapacity();
    }

    fn clearJoinInputs(self: *@This()) void {
        self.clearJoinedResults();
        self.sent_parts.clearRetainingCapacity();
    }

    /// Finish every JoinResult question without releasing result caps. This
    /// releases the host-side JoinResult lifetime after direct Accept succeeds.
    pub fn finishJoinResults(self: *@This()) !void {
        if (self.join_results_finished) return;
        var first_err: ?anyerror = null;
        for (self.question_ids.items, self.question_peers.items, self.question_finished.items) |question_id, peer, *finished| {
            if (finished.*) continue;
            peer.sendFinishForHost(question_id, false, false) catch |err| {
                if (first_err == null) first_err = err;
                continue;
            };
            finished.* = true;
        }
        if (first_err) |err| return err;
        self.join_results_finished = true;
    }

    fn noteAllJoinResultsFinished(self: *@This()) void {
        for (self.question_finished.items) |finished| {
            if (!finished) return;
        }
        self.join_results_finished = true;
    }

    fn cancelQuestionIndex(self: *@This(), index: usize, reason: []const u8) !void {
        if (self.question_finished.items[index]) return;
        const question_id = self.question_ids.items[index];
        const peer = self.question_peers.items[index];
        if (peer.questions.contains(question_id)) {
            peer.cancelQuestion(question_id, reason) catch |err| {
                self.question_finished.items[index] = true;
                return err;
            };
            self.question_finished.items[index] = true;
        } else if (!self.join_results_finished) {
            try peer.sendFinishForHost(question_id, false, false);
            self.question_finished.items[index] = true;
        }
    }

    pub fn cancelPending(self: *@This(), reason: []const u8) !void {
        self.canceled = true;
        var first_err: ?anyerror = null;
        for (0..self.question_ids.items.len) |index| {
            self.cancelQuestionIndex(index, reason) catch |err| {
                if (first_err == null) first_err = err;
            };
        }
        self.noteAllJoinResultsFinished();

        if (self.accept_question_id) |accept_question_id| {
            if (self.accept_peer) |peer| {
                if (peer.questions.contains(accept_question_id)) {
                    peer.cancelQuestion(accept_question_id, reason) catch |err| {
                        if (first_err == null) first_err = err;
                    };
                }
            }
        }

        self.releaseAccepted() catch |err| {
            if (first_err == null) first_err = err;
        };
        self.clearJoinInputs();
        self.clearAcceptPeerLinkIfDrained();

        if (first_err) |err| return err;
    }

    fn finishOneBestEffort(self: *@This(), peer: *Peer, question_id: u32) void {
        peer.sendFinishForHost(question_id, false, false) catch |err| {
            self.finish_failures += 1;
            log.debug("failed to finish L4 JoinResult question {}: {}", .{ question_id, err });
            return;
        };
        self.markQuestionFinished(peer, question_id);
        self.noteAllJoinResultsFinished();
    }

    fn noteSyntheticCancelReturn(self: *@This(), peer: *Peer, question_id: u32) bool {
        if (!self.canceled) return false;
        const question = peer.questions.get(question_id) orelse return false;
        if (!question.cancelled) return false;
        self.markQuestionFinished(peer, question_id);
        self.noteAllJoinResultsFinished();
        return true;
    }

    fn failTerminalJoinResult(self: *@This(), peer: *Peer, question_id: u32, reason: []const u8) void {
        if (self.noteSyntheticCancelReturn(peer, question_id)) return;
        self.finishOneBestEffort(peer, question_id);
        self.cancelPending(reason) catch |err| {
            self.finish_failures += 1;
            log.debug("failed to cancel L4 JoinCoordinator after terminal JoinResult {}: {}", .{ question_id, err });
        };
    }

    fn finishJoinResultsAfterAccept(self: *@This(), context: []const u8) void {
        self.finishJoinResults() catch |err| {
            self.finish_failures += 1;
            log.debug("failed to finish L4 JoinResult questions after {s}: {}", .{ context, err });
        };
        self.clearJoinInputs();
    }

    fn failTerminalAcceptReturn(self: *@This(), context: []const u8) void {
        self.accept_exceptions += 1;
        self.canceled = true;
        self.accept_question_id = null;
        self.accept_peer = null;
        self.finishJoinResultsAfterAccept(context);
        self.clearAcceptPeerLinkIfDrained();
    }

    fn registerAcceptPeerLink(self: *@This(), peer: *Peer) !void {
        if (self.accept_link_peer) |linked_peer| {
            if (linked_peer == peer) return;
            return error.ConflictingAcceptPeer;
        }
        try peer.registerJoinCoordinatorAccept(self);
        self.accept_link_peer = peer;
    }

    fn clearAcceptPeerLink(self: *@This()) void {
        if (self.accept_link_peer) |peer| {
            peer.deregisterJoinCoordinatorAccept(self);
            self.accept_link_peer = null;
        }
    }

    fn clearAcceptPeerLinkIfDrained(self: *@This()) void {
        if (self.accept_question_id != null) return;
        if (self.accept_peer != null) return;
        if (self.accepted_cap != null or self.accepted_peer != null) return;
        if (!self.accept_answer_finished) return;
        self.clearAcceptPeerLink();
    }

    fn neutralizeAcceptedPeer(self: *@This(), peer: *Peer) void {
        if (self.accept_answer_peer == peer and !self.accept_answer_finished) {
            if (self.accept_answer_id) |answer_id| {
                peer.sendFinishForHost(answer_id, false, false) catch |err| {
                    self.finish_failures += 1;
                    log.debug("failed to finish L4 JoinCoordinator Accept answer {} during peer deinit: {}", .{
                        answer_id,
                        err,
                    });
                };
            }
        }
        if (self.accept_link_peer == peer) self.accept_link_peer = null;
        if (self.accept_peer == peer) {
            self.accept_peer = null;
            self.accept_question_id = null;
        }
        if (self.accept_answer_peer == peer) {
            self.accept_answer_peer = null;
            self.accept_answer_finished = true;
        }
        if (self.accepted_peer == peer) {
            self.accepted_peer = null;
            self.accepted_cap = null;
        }
    }

    fn noteAcceptAnswerNeedsFinish(self: *@This(), peer: *Peer, answer_id: u32) void {
        if (self.accept_answer_id == answer_id and self.accept_answer_finished) return;
        self.accept_answer_peer = peer;
        self.accept_answer_id = answer_id;
        self.accept_answer_finished = false;
    }

    fn noteAcceptAnswerFinished(self: *@This(), peer: *Peer, answer_id: u32) void {
        self.accept_answer_peer = peer;
        self.accept_answer_id = answer_id;
        self.accept_answer_finished = true;
    }

    fn finishAcceptAnswer(self: *@This(), peer: *Peer, answer_id: u32, context: []const u8) bool {
        if (self.accept_answer_id == answer_id and self.accept_answer_finished) return true;
        var attempts: u8 = 0;
        while (attempts < 2) : (attempts += 1) {
            peer.sendFinishForHost(answer_id, false, false) catch |err| {
                self.finish_failures += 1;
                log.debug("failed to finish L4 JoinCoordinator Accept answer {} after {s}: {}", .{
                    answer_id,
                    context,
                    err,
                });
                continue;
            };
            self.noteAcceptAnswerFinished(peer, answer_id);
            self.clearAcceptPeerLinkIfDrained();
            return true;
        }
        return false;
    }

    fn retryAcceptAnswerFinish(self: *@This(), context: []const u8) !void {
        const answer_id = self.accept_answer_id orelse return;
        if (self.accept_answer_finished) return;
        const peer = self.accept_answer_peer orelse return error.MissingAcceptPeer;
        if (!self.finishAcceptAnswer(peer, answer_id, context)) return error.AcceptFinishFailed;
    }

    fn markQuestionFinished(self: *@This(), peer: *Peer, question_id: u32) void {
        for (self.question_ids.items, self.question_peers.items, self.question_finished.items) |qid, qpeer, *finished| {
            if (qid == question_id and qpeer == peer) {
                finished.* = true;
                return;
            }
        }
    }

    fn onJoinReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse {
                    self.unexpected_exceptions += 1;
                    self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                    return;
                };
                if (self.expected_parts != 0 and self.joined.items.len >= self.expected_parts) {
                    self.unexpected_exceptions += 1;
                    self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                    return;
                }
                const decoded = join_network.decodeJoinResult(payload.content) catch {
                    self.unexpected_exceptions += 1;
                    self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                    return;
                };
                if (!decoded.succeeded or decoded.join_id != self.join_id) {
                    self.mismatch_exceptions += 1;
                    self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                    return;
                }
                var joined = self.join_network.connectJoined(self.allocator, payload.content) catch {
                    self.unexpected_exceptions += 1;
                    self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                    return;
                };
                self.joined.append(self.allocator, joined) catch {
                    joined.deinit(self.allocator);
                    self.unexpected_exceptions += 1;
                    self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                    return;
                };
            },
            .exception => {
                const reason = if (ret.exception) |exception| exception.reason else "";
                if (std.mem.eql(u8, reason, "join target mismatch")) {
                    self.mismatch_exceptions += 1;
                } else if (std.mem.eql(u8, reason, "join canceled")) {
                    self.cancel_exceptions += 1;
                } else {
                    self.unexpected_exceptions += 1;
                }
                self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
            },
            else => {
                self.unexpected_exceptions += 1;
                self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
            },
        }
    }

    fn onAcceptReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.no_finish_needed) {
            self.noteAcceptAnswerFinished(peer, ret.answer_id);
        } else {
            self.noteAcceptAnswerNeedsFinish(peer, ret.answer_id);
        }
        defer if (!self.accept_send_in_progress and !ret.no_finish_needed) {
            _ = self.finishAcceptAnswer(peer, ret.answer_id, "Accept Return");
        };
        switch (ret.tag) {
            .results => {
                if (self.accepted_cap != null) {
                    self.failTerminalAcceptReturn("duplicate Accept result");
                    return;
                }
                const payload = ret.results orelse {
                    self.failTerminalAcceptReturn("malformed Accept result");
                    return;
                };
                const cap = payload.content.getCapability() catch {
                    self.failTerminalAcceptReturn("malformed Accept result");
                    return;
                };
                const resolved = caps.resolveCapability(cap) catch {
                    self.failTerminalAcceptReturn("unresolved Accept result");
                    return;
                };
                var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
                mutable_caps.retainCapability(cap) catch {
                    self.failTerminalAcceptReturn("unretained Accept result");
                    return;
                };
                self.accepted_peer = peer;
                self.accepted_cap = resolved;
                self.accept_question_id = null;
                self.accept_peer = null;
                self.finishJoinResultsAfterAccept("Accept");
                self.clearAcceptPeerLinkIfDrained();
            },
            .exception => {
                self.failTerminalAcceptReturn("Accept exception");
            },
            else => {
                self.failTerminalAcceptReturn("unexpected Accept return");
            },
        }
    }
};

/// Couples a vine export (minted on the host-of-recipient connection, VatB↔VatA)
/// to the held-open `Provide` question it anchors (on the host-of-provided-cap
/// connection, VatB↔VatC). When the recipient releases the vine — the wire
/// signal that the `Accept` completed (or was abandoned) — VatB Finishes the
/// paired Provide question, unregistering the provision on VatC.
///
/// The coupling deliberately spans two peers: the vine lives on THIS peer's cap
/// table (the B↔A connection), while the Provide question lives on a DIFFERENT
/// peer (the B↔C connection), which `provide_peer` points at.
///
/// LIVENESS: `provide_peer` is a borrowed pointer, but the coupling is made
/// resilient to arbitrary per-connection teardown order by a symmetric
/// back-registration. Every live coupling records a back-link on `provide_peer`
/// (`Peer.coupled_vines`); when the provided-cap peer (B↔C) deinits, it walks
/// those back-links and NULLs `provide_peer` in each recipient's coupling entry
/// BEFORE its own memory is freed (see `neutralizeCoupledVinesOnProvidePeer`).
/// A subsequent vine Release on the recipient then finds a null `provide_peer`
/// and skips the Finish — a correct no-op, because the provided-cap peer's own
/// `deinit` already force-cancelled (removed + Finished) the held-open Provide
/// question. So the freed pointer is removed, never read after free, regardless
/// of which peer is torn down first.
const OutboundProvide = struct {
    /// The peer on which the held-open Provide question was sent (B↔C).
    /// Borrowed. Null once that peer has deinited (it neutralizes every coupling
    /// pointing at it before freeing itself), so `finishOriginatedProvide` is
    /// never handed a dangling pointer.
    provide_peer: ?*Peer,
    /// The Provide question id to Finish when the vine is released.
    provide_question_id: u32,
    /// When the handoff was originated by resolving a promise EXPORT to the
    /// third party (`resolvePromiseExportToThirdParty`), this is that promise
    /// export id on THIS peer, and it resolved locally to the vine (so replayed
    /// pipelined calls dispatch to the vine — where, with `provided_import_id`
    /// set, they are FORWARDED to VatC per issue #56 rather than rejected). The
    /// vine's destruction on Release invalidates that resolution target, so the
    /// vine teardown in `handleRelease` nulls the promise export's `resolved` link
    /// to avoid a dangling target. Null for a bare `sendProvide` handoff (no
    /// promise export involved, e.g. the P0–P2 return-a-vine path).
    resolved_promise_export_id: ?u32 = null,
    /// L3 parked-call FORWARDING (issue #56): the import id, on `provide_peer`
    /// (B↔C), of the capability that was provided to VatC — i.e. the
    /// `provided_target.imported_cap` VatB passed to `sendProvide`. When a
    /// caller (VatA) pipelined calls on the handed-off promise and they were
    /// parked at VatB, resolving the promise replays them onto the VINE export
    /// (the Level-1/2 fallback). Rather than reject them at the vine, VatB
    /// FORWARDS each replayed call to VatC over `provide_peer`, targeting this
    /// import, and relays VatC's result back to complete VatA's original
    /// pipelined question (see `forwardVineCallToProvidedCap`). Null when the
    /// provided target is not a simple imported cap (e.g. a `promisedAnswer`
    /// target, or the P0–P2 return-a-vine path with no pipelined caller): in
    /// that case the vine keeps its rejecting behavior, since there is no
    /// unambiguous single import on `provide_peer` to forward to.
    provided_import_id: ?u32 = null,
    /// E-ORDER (rpc.capnp:898-903): true once `replayResolvedPromiseExport` has
    /// forwarded this coupling's parked pre-resolution calls (or the resolve
    /// path is past the point where any could exist). Until then a
    /// `context.accept` Disembargo from the recipient must NOT be forwarded to
    /// the host: under a synchronous transport the forward would release the
    /// host's embargoed Accept — delivering the pickup and any post-pickup
    /// direct calls — BEFORE the parked calls reach the host, inverting e-order
    /// and (worse) completing the handoff so the vine teardown strands the
    /// parked calls entirely.
    replay_flushed: bool = false,
    /// The recipient's forwarded accept-Disembargo, held while `replay_flushed`
    /// is false. Duped embargo bytes, owned by the INTRODUCER peer's allocator;
    /// freed on flush or with the coupling. Single slot: one Disembargo per
    /// handoff is the protocol shape, so a second stash while occupied is
    /// dropped with a log rather than queued.
    stashed_accept_disembargo: ?[]u8 = null,

    /// Stash the recipient's accept-Disembargo until the parked-call replay has
    /// run (see `replay_flushed`).
    fn stashAcceptDisembargo(op: *OutboundProvide, allocator: std.mem.Allocator, embargo: []const u8) !void {
        if (op.stashed_accept_disembargo != null) {
            log.warn("second accept-disembargo stashed before replay; dropping duplicate", .{});
            return;
        }
        op.stashed_accept_disembargo = try allocator.dupe(u8, embargo);
    }

    /// Free the stash slot (coupling teardown). Idempotent.
    fn deinitStash(op: *OutboundProvide, allocator: std.mem.Allocator) void {
        if (op.stashed_accept_disembargo) |stash| allocator.free(stash);
        op.stashed_accept_disembargo = null;
    }
};

/// Liveness back-link recorded on the host-of-provided-cap peer (B↔C) for each
/// coupling that peer anchors. Names the recipient peer (B↔A) whose
/// `outbound_provides[vine_id]` entry borrows a pointer back to this peer, so
/// this peer's `deinit` can find and NULL that borrowed pointer before it frees
/// itself. The reverse of the `OutboundProvide.provide_peer` edge.
const CoupledVine = struct {
    /// The host-of-recipient peer (B↔A) that owns the coupling entry.
    recipient_peer: *Peer,
    /// The vine export id keying the coupling in `recipient_peer.outbound_provides`.
    vine_id: u32,
};

/// Liveness back-link for a cross-peer capability proxy. The proxy export lives
/// on `owner_peer`, but its handler borrows a pointer to the source peer so it
/// can forward calls and release the retained source import. If the source peer
/// deinits first, it walks these links and nulls those borrowed pointers.
const CrossPeerProxyLink = struct {
    owner_peer: *Peer,
    proxy_export_id: u32,
};

const CrossPeerJoinRelay = struct {
    source_peer: ?*Peer,
    source_question_id: u32,
};

const CrossPeerJoinRelayLink = struct {
    owner_peer: *Peer,
    owner_answer_id: u32,
};

const PendingJoinResultAnswer = struct {
    accept_peer: ?*Peer,
    provision: []u8,
};

const JoinAcceptHostLink = struct {
    answer_peer: *Peer,
    answer_id: u32,
};

const JoinCoordinatorAcceptLink = struct {
    coordinator: *JoinCoordinator,
};

/// Reason sent when refusing an inbound Call whose results were redirected to a
/// third vat that this peer cannot contact.
const third_party_results_unsupported =
    "sendResultsTo.thirdParty unsupported: this vat cannot route results to a third party";

/// Reason sent to calls pipelined on an answer whose results went to a third
/// vat: this peer never observes those results, so it cannot resolve a
/// promised-answer target against them.
const results_sent_elsewhere_no_pipelining =
    "results sent elsewhere; pipelining is not supported on a redirected answer";

pub const Peer = struct {
    allocator: std.mem.Allocator,
    limits: PeerLimits,

    // -- Transport binding --------------------------------------------------

    /// Attached transport callbacks. Set/cleared atomically by
    /// `attachTransportBinding` / `detachTransport`.
    transport: TransportBinding = .{},

    /// Optional Level-3 three-party addressing seam. Null for a plain two-party
    /// peer; set via `attachVatNetwork` before originating `Provide`/`Accept`
    /// handoffs. Borrowed — its `ctx` must outlive the peer.
    vat_network: ?VatNetwork = null,
    third_party_result_policy: ThirdPartyResultPolicy = .reject,

    /// Optional Experimental Level-4 Join addressing seam. When present, inbound
    /// `Join` completion returns Zig `JoinResult` payloads and registers a
    /// one-shot direct `Accept` provision instead of returning the cap directly.
    /// Borrowed — its `ctx` must outlive the peer.
    join_network: ?JoinNetwork = null,

    /// Optional Level-3 recipient auto-pickup handler. When this peer holds a
    /// promise import and receives a `thirdPartyHosted` Resolve for it AND a
    /// `vat_network` is attached, the runtime connects to the third vat, sends an
    /// `Accept`, and — when the accepted capability's `Return` arrives — invokes
    /// this handler with the direct cap (see `HandoffPickupCallback`). Null (or a
    /// null `vat_network`) keeps the Level-1/2 proxy-via-vine fallback: the
    /// promise resolves to the vine import and no pickup is attempted. Set via
    /// `setHandoffPickupHandler`. `handoff_pickup_ctx` is borrowed and must
    /// outlive the peer.
    handoff_pickup_ctx: ?*anyopaque = null,
    on_handoff_pickup: ?HandoffPickupCallback = null,

    // -- Capability bookkeeping ---------------------------------------------

    /// Central capability table shared with cap_table helpers.
    caps: cap_table.CapTable,
    /// Exported (local) capabilities offered to the remote peer.
    exports: std.AutoHashMap(u32, ExportEntry),

    // -- Question / answer tracking -----------------------------------------

    /// Outstanding outbound calls (question ID -> callback).
    questions: std.AutoHashMap(u32, Question),
    /// Wire refs an outstanding outbound Call's params took on local exports
    /// (question ID -> export ids, one per senderHosted/senderPromise
    /// descriptor emitted; duplicates meaningful — each occurrence is one
    /// ref). Consumed by the question's inbound Return: `releaseParamCaps =
    /// true` (the rpc.capnp default, relied on by conforming peers) spends
    /// the refs through `releaseExport`; false drops the record and leaves
    /// the refs for the remote's explicit Release messages.
    question_param_export_refs: std.AutoHashMap(u32, std.ArrayList(u32)),
    /// Cached Return frames for answered questions, kept until Finish.
    resolved_answers: std.AutoHashMap(u32, ResolvedAnswer),
    /// Inbound call question IDs accepted from the remote peer until Return or Finish.
    active_inbound_questions: std.AutoHashMap(u32, void),
    /// Answer IDs whose results Return is currently being synchronously delivered
    /// and may receive a reentrant Finish before the resolved answer is committed.
    resolving_answers: std.AutoHashMap(u32, void),
    /// Count of ResolvedAnswerReservation values currently outstanding
    /// (reserved but not yet committed or rolled back). A reservation is held
    /// open across the transport send, and a synchronous transport can deliver
    /// a nested inbound Call/Bootstrap that reserves and commits a DIFFERENT
    /// answer while the outer reservation is pending. Each reserve therefore
    /// ensures map capacity for itself PLUS every outstanding reservation so
    /// the outer infallible commit can never underflow the map's reserved-slot
    /// accounting (getOrPutAssumeCapacity).
    resolved_answer_reservations: u32 = 0,
    /// Inbound question ids whose Finish arrived before their (async) Return.
    /// A late Return for one of these must NOT be recorded in resolved_answers:
    /// no further Finish will ever clear it, and a stale entry poisons legal
    /// reuse of the id (DuplicateQuestionId against a compliant peer). Bounded
    /// by max_active_inbound_questions.
    /// The bool records Finish.releaseResultCaps so the later Return sender can
    /// apply the Finish after delivery. If the sender already reserved a
    /// resolved answer, it commits first to drain queued promised calls, then
    /// immediately removes the recorded answer through normal Finish cleanup.
    finished_early_answers: std.AutoHashMap(u32, bool),

    // -- Promise queueing ---------------------------------------------------

    /// Calls blocked on an unresolved PromisedAnswer (question ID -> queue).
    pending_promises: std.AutoHashMap(u32, std.ArrayList(PendingCall)),
    /// Calls blocked on an unresolved promise export (export ID -> queue).
    pending_export_promises: std.AutoHashMap(u32, std.ArrayList(PendingCall)),

    // -- Forwarding ---------------------------------------------------------

    /// Maps an inbound answer ID to the outbound question ID it was forwarded to.
    forwarded_questions: std.AutoHashMap(u32, u32),
    /// Same as `forwarded_questions` but for tail-call (takeFromOtherQuestion) forwarding.
    forwarded_tail_questions: std.AutoHashMap(u32, u32),

    // -- Three-party handoff (provide / accept / join) ----------------------

    /// Active Provide operations keyed by answer question ID.
    provides_by_question: std.AutoHashMap(u32, ProvideEntry),
    /// Active Provide operations keyed by serialized recipient for Accept lookup.
    provides_by_key: std.StringHashMap(u32),
    /// Attached vat-wide provision index (BORROWED; back-linked via
    /// `ProvisionIndex.attached_peers`). Null = all L3 host behavior is
    /// exactly today's per-peer behavior. Set via `attachProvisionIndex`.
    provision_index: ?*ProvisionIndex = null,
    /// OWNER side of vat-wide provisions: Provide question id -> provision
    /// object (+1 ref each). Lives BESIDE `provides_by_question` so
    /// `ProvideEntry`'s shape and every generic helper that constructs it stay
    /// untouched.
    provisions_by_question: std.AutoHashMap(u32, *ProvisionIndex.Provision),
    /// HOLDER side: queued/parked cross-peer Accept answer id -> record.
    /// Records are UNCOUNTED back-links: the +1 lives in the provision's
    /// pending slot / parked entry; a record exists IFF its slot/entry exists;
    /// removing a record NEVER releases (invariant INV-REC).
    cross_peer_pending_accepts: std.AutoHashMap(u32, CrossPeerAcceptRecord),
    /// ORIGINATION (mirror of the inbound provide tables): vine export id ->
    /// the paired held-open Provide question this peer originated. Keyed by the
    /// vine export minted on THIS connection; the Provide question it anchors
    /// may live on a different peer (see `OutboundProvide`). Consulted from
    /// `handleRelease` to Finish the Provide once the recipient drops the vine.
    outbound_provides: std.AutoHashMap(u32, OutboundProvide),
    /// LIVENESS back-links for the cross-peer `outbound_provides` coupling. When
    /// THIS peer holds a held-open Provide question anchored by a vine minted on
    /// ANOTHER (recipient) peer, each such coupling records a `CoupledVine` here
    /// naming that recipient peer + vine id. On `deinit` this peer walks the list
    /// and NULLs its `provide_peer` back-pointer in every recipient's coupling
    /// entry, so a later vine Release can never dereference this freed peer.
    /// Entries are removed as soon as their coupling drains (vine Release, or the
    /// recipient peer's own deinit). Empty for a peer that originated no handoffs.
    coupled_vines: std.ArrayList(CoupledVine),
    /// Back-links from proxy exports on other peers that borrow this peer as
    /// their source. Walked at deinit to null those borrowed pointers before this
    /// peer's memory is freed.
    cross_peer_proxy_links: std.ArrayList(CrossPeerProxyLink),
    /// Back-links for Join relay questions sent through this peer. Walked at
    /// deinit to null owner-peer relay records before this peer is freed.
    cross_peer_join_relay_links: std.ArrayList(CrossPeerJoinRelayLink),
    /// In-progress Join operations collecting parts.
    pending_joins: std.AutoHashMap(u32, JoinState),
    /// Maps a Join answer's question ID to its join ID + part number.
    pending_join_questions: std.AutoHashMap(u32, PendingJoinQuestion),
    /// Cross-peer transparent-proxy Join relays keyed by upstream answer id.
    pending_join_relays: std.AutoHashMap(u32, CrossPeerJoinRelay),
    /// Accept messages waiting for a disembargo.
    pending_accepts_by_embargo: std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)),
    /// Maps question IDs to embargo keys for cleanup on Finish.
    pending_accept_embargo_by_question: std.AutoHashMap(u32, []u8),
    /// Experimental L4 JoinResult provisions waiting for a direct Accept.
    pending_join_accepts: std.StringHashMap(ProvideTarget),
    /// Experimental L4 JoinResult answer ids -> accept host and owned provision.
    pending_join_result_answers: std.AutoHashMap(u32, PendingJoinResultAnswer),
    /// Back-links from JoinResult answer records on other peers that point at
    /// this peer as the direct Accept host.
    join_accept_host_links: std.ArrayList(JoinAcceptHostLink),
    /// Back-links from Experimental JoinCoordinator instances holding an accepted
    /// cap or unfinished Accept answer through this peer.
    join_coordinator_accept_links: std.ArrayList(JoinCoordinatorAcceptLink),
    /// Outbound third-party handoffs awaiting ThirdPartyAnswer.
    pending_third_party_awaits: std.StringHashMap(PendingThirdPartyAwait),
    /// Completed third-party answers awaiting adoption.
    pending_third_party_answers: std.StringHashMap(u32),
    /// Return frames for third-party questions received before adoption.
    pending_third_party_returns: std.AutoHashMap(u32, []u8),
    /// Maps third-party questions to their adopted answer IDs.
    adopted_third_party_answers: std.AutoHashMap(u32, u32),

    // -- Resolve / embargo --------------------------------------------------

    /// Resolved promise imports (after Resolve). Tracks embargo state.
    resolved_imports: std.AutoHashMap(u32, ResolvedImport),
    /// In-flight disembargo operations (embargo ID -> promise ID).
    pending_embargoes: std.AutoHashMap(u32, u32),

    // -- Loopback / sendResultsTo routing -----------------------------------

    /// Questions whose Return should be delivered locally (calls to our own exports).
    loopback_questions: std.AutoHashMap(u32, void),
    /// Inbound calls with sendResultsTo=yourself.
    send_results_to_yourself: std.AutoHashMap(u32, void),
    /// Inbound calls with sendResultsTo=thirdParty.
    send_results_to_third_party: std.AutoHashMap(u32, ?[]u8),
    /// Results frames for answered `sendResultsTo=yourself` calls, awaiting the
    /// `takeFromOtherQuestion` redirect that names the local question they feed
    /// (the reflected-loopback / Level-1 tail-call optimization). Keyed by the
    /// answered call's answer id (the forwarder's forwarded-question id); the
    /// value is an owned Return-results frame. Consumed inline when the matching
    /// `takeFromOtherQuestion` arrives (see `handleReturn`); freed at deinit if
    /// that redirect never comes. NOT freed on the answered call's Finish — the
    /// forwarder's `Finish` for the forwarded question can arrive BEFORE the
    /// `takeFromOtherQuestion`, so freeing there would drop the results early.
    /// Only cap-free results are stashed.
    loopback_result_stash: std.AutoHashMap(u32, []u8),

    // -- Persistence (sturdy refs) -------------------------------------------

    /// Save/restore hooks for persistent exports (export ID -> state).
    persistent_exports: std.AutoHashMap(u32, *PersistenceState),
    /// Export currently serving vat-level restore calls (the wrapped
    /// bootstrap export), if a restorer hook is installed.
    restorer_export_id: ?u32 = null,
    /// Total `Persistent.save()` calls served by this peer.
    saves_served: u64 = 0,
    /// Total vat-level restore calls served by this peer.
    restores_served: u64 = 0,

    // -- Counters and scalars -----------------------------------------------

    /// Monotonically increasing question ID counter.
    next_question_id: u32 = 0,
    /// Monotonically increasing embargo ID counter.
    next_embargo_id: u32 = 0,
    /// Monotonically increasing counter for Level-3 three-party handoff
    /// `Accept` embargo ids. Distinct from `next_embargo_id`: an accept embargo
    /// is an opaque BYTE string carried in `Accept.embargo` and matched
    /// byte-for-byte by the capability host (VatC) against a `context.accept`
    /// `Disembargo` (rpc.capnp:870-903), whereas `next_embargo_id` names the
    /// numeric senderLoopback embargo namespace. Keeping them separate avoids
    /// cross-talk between the two ordering mechanisms.
    next_accept_embargo_id: u64 = 0,
    /// Next callee-allocated answer id for an outbound `ThirdPartyAnswer`
    /// (`Peer.sendThirdPartyAnswer`). Per rpc.capnp:936-941 the callee — not the
    /// caller — chooses this id, and it MUST fall in the range [2^30, 2^31):
    /// bit 30 set, bit 31 clear (see `third_party.isThirdPartyAnswerId`). The
    /// counter starts at 2^30 and is masked back into range on wrap so it never
    /// collides with the caller-allocated question-id space (low ids) or the
    /// `onlyPromisePipeline` range [2^31, 2^32).
    next_third_party_answer_id: u32 = third_party_answer_id_base,
    /// Export ID of the bootstrap capability, if set.
    bootstrap_export_id: ?u32 = null,

    // -- Send frame override (for testing) ----------------------------------

    send_frame_ctx: ?*anyopaque = null,
    send_frame_override: ?SendFrameOverride = null,

    // -- Lifecycle callbacks -------------------------------------------------

    callback_ctx: ?*anyopaque = null,
    on_error: ?*const fn (ctx: ?*anyopaque, peer: *Peer, err: anyerror) void = null,
    on_close: ?*const fn (ctx: ?*anyopaque, peer: *Peer) void = null,
    observer: ?events.Observer = null,

    // -- Time / deadlines -----------------------------------------------------

    /// Custom monotonic clock used for call deadlines and the shutdown
    /// drain bound. The clock's `ctx` pointer must outlive the peer.
    /// Tests inject `rpc.time.TestClock` here.
    clock: ?rpc_time.Clock = null,
    /// `std.Io` used as the monotonic time source when no custom `clock`
    /// is set; see `setClockIo`. Stored by value so peer moves cannot
    /// dangle it. Null (with no custom clock) disables time-based
    /// enforcement entirely.
    clock_io: ?std.Io = null,
    /// Opt-in time-domain policy; see `state.PeerTimeouts`.
    timeouts: PeerTimeouts = .{},
    /// Absolute drain bound stamped by `shutdown()` when a drain timeout is
    /// configured. Enforced by `checkDeadlines`.
    shutdown_deadline_ns: ?i64 = null,

    // -- Validation-work rate limit -----------------------------------------

    /// Token-bucket state for the per-connection validation-work budget (see
    /// PeerLimits.max_validation_words_per_second). `validation_tokens` counts
    /// available traversal-words; it starts full on the first charged frame and
    /// refills by elapsed time. Inert until a Clock is configured.
    validation_tokens: i64 = 0,
    validation_last_refill_ns: ?i64 = null,

    // -- Diagnostics --------------------------------------------------------

    /// Tag of the most recently decoded inbound message (for debugging).
    last_inbound_tag: ?protocol.MessageTag = null,
    /// Reason string from the most recent remote Abort message, if any.
    last_remote_abort_reason: ?[]u8 = null,

    // -- Graceful shutdown ---------------------------------------------------

    /// When true, no new outbound calls are accepted and the peer drains
    /// in-flight questions before invoking `shutdown_callback`.
    is_shutting_down: bool = false,
    /// True while `deinit` runs. Suppresses `completeShutdown` (which touches
    /// the transport and fires the shutdown callback) when the terminal
    /// question pass drains the last question of a mid-shutdown peer —
    /// deinit itself IS the completion, and the transport may already be
    /// gone.
    in_deinit: bool = false,
    /// Optional callback fired once all outstanding questions have been
    /// answered and the shutdown sequence completes.
    shutdown_callback: ?*const fn (peer: *Peer) void = null,

    // -- Thread-affinity check (debug only) ---------------------------------

    /// Thread ID captured at init time. In debug builds, key entry points
    /// assert that the current thread matches this value. Initialized to
    /// null and set to the real thread ID in `initDetached`.
    owner_thread_id: ?state.OwnerThreadId = null,

    /// When true, thread-affinity checks also run in release builds (they
    /// are always on in Debug). See `enableRuntimeThreadChecks`.
    runtime_thread_checks: bool = false,

    /// Disable the thread-affinity check so that any thread may call
    /// methods on this peer. This is intended for use by the WASM ABI
    /// layer where a global mutex provides synchronization and the peer
    /// may be accessed from different host threads.
    pub fn disableThreadAffinity(self: *Peer) void {
        self.owner_thread_id = null;
    }

    /// Re-capture thread affinity on the current thread. Legal only at a
    /// quiescent handoff point — no other thread may touch this peer
    /// concurrently (e.g. `ClientSession.run()` adopting a session that
    /// was connected on another thread before its run loop started).
    pub fn adoptOwnerThread(self: *Peer) void {
        self.owner_thread_id = state.initialOwnerThreadId();
    }

    /// Opt in to thread-affinity checking in release builds.
    ///
    /// `Peer` is single-threaded by contract: every method must be called
    /// from the owning thread (or under external synchronization with
    /// `disableThreadAffinity`). Debug builds always enforce this with a
    /// panic; release builds skip the check by default for performance.
    /// Deployments that can afford one thread-id read per entry point
    /// should enable this — a violated affinity contract is a data race,
    /// and panicking beats corrupting protocol state.
    pub fn enableRuntimeThreadChecks(self: *Peer, enabled: bool) void {
        self.runtime_thread_checks = enabled;
    }

    /// Assert that the caller is on the thread that created this peer.
    /// Always enforced in Debug builds; enforced in release builds only
    /// after `enableRuntimeThreadChecks(true)`. Panics on violation.
    pub fn assertThreadAffinity(self: *const Peer) void {
        state.assertThreadAffinity(self.owner_thread_id, self.runtime_thread_checks);
    }

    /// Canonical `Peer` constructor: create a peer and immediately attach it
    /// to a connection/transport. This is the single supported entry point for
    /// normal RPC use — `ClientSession.connect` and `ServerSession.accept` both
    /// build their peer this way. Prefer it unless you have a specific reason
    /// to construct a detached peer (see `initDetached`).
    ///
    /// Under the hood this is `initDetached` followed by `attachConnection`;
    /// the two-step form exists only for advanced/embedded scenarios.
    pub fn init(allocator: std.mem.Allocator, conn: anytype) Peer {
        var peer = initDetached(allocator);
        peer.attachConnection(conn);
        return peer;
    }

    /// Advanced/internal: create a peer WITHOUT an attached transport.
    ///
    /// Most callers want the canonical `init`, which attaches a connection in
    /// one step. Reach for `initDetached` only when there is no connection to
    /// attach yet — WASM, unit tests, or manual frame injection via
    /// `handleFrame` and `setSendFrameOverride`. Pair it with a later
    /// `attachConnection` (or `attachTransport*`) before driving real traffic.
    /// The provide-target type, re-exported for generics that are parameterized
    /// over the peer type (vat/provisions.zig) without importing peer state.
    pub const ProvideTargetType = ProvideTarget;

    pub fn initDetached(allocator: std.mem.Allocator) Peer {
        return initDetachedWithLimits(allocator, .{});
    }

    /// Advanced/internal: like `initDetached`, but with explicit resource
    /// limits instead of the defaults.
    ///
    /// Prefer the canonical `init` and set limits afterward via `setLimits`
    /// when you can; this variant exists to construct a detached peer with a
    /// non-default `PeerLimits` in one call. Not the normal-use entry point.
    pub fn initDetachedWithLimits(allocator: std.mem.Allocator, limits: PeerLimits) Peer {
        return .{
            .allocator = allocator,
            .limits = limits,
            .owner_thread_id = state.initialOwnerThreadId(),
            .caps = cap_table.CapTable.init(allocator),
            .exports = std.AutoHashMap(u32, ExportEntry).init(allocator),
            .questions = std.AutoHashMap(u32, Question).init(allocator),
            .question_param_export_refs = std.AutoHashMap(u32, std.ArrayList(u32)).init(allocator),
            .resolved_answers = std.AutoHashMap(u32, ResolvedAnswer).init(allocator),
            .active_inbound_questions = std.AutoHashMap(u32, void).init(allocator),
            .resolving_answers = std.AutoHashMap(u32, void).init(allocator),
            .finished_early_answers = std.AutoHashMap(u32, bool).init(allocator),
            .pending_promises = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(allocator),
            .pending_export_promises = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(allocator),
            .forwarded_questions = std.AutoHashMap(u32, u32).init(allocator),
            .forwarded_tail_questions = std.AutoHashMap(u32, u32).init(allocator),
            .provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(allocator),
            .provides_by_key = std.StringHashMap(u32).init(allocator),
            .provisions_by_question = std.AutoHashMap(u32, *ProvisionIndex.Provision).init(allocator),
            .cross_peer_pending_accepts = std.AutoHashMap(u32, CrossPeerAcceptRecord).init(allocator),
            .outbound_provides = std.AutoHashMap(u32, OutboundProvide).init(allocator),
            .coupled_vines = .empty,
            .cross_peer_proxy_links = .empty,
            .cross_peer_join_relay_links = .empty,
            .pending_joins = std.AutoHashMap(u32, JoinState).init(allocator),
            .pending_join_questions = std.AutoHashMap(u32, PendingJoinQuestion).init(allocator),
            .pending_join_relays = std.AutoHashMap(u32, CrossPeerJoinRelay).init(allocator),
            .pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)).init(allocator),
            .pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(allocator),
            .pending_join_accepts = std.StringHashMap(ProvideTarget).init(allocator),
            .pending_join_result_answers = std.AutoHashMap(u32, PendingJoinResultAnswer).init(allocator),
            .join_accept_host_links = .empty,
            .join_coordinator_accept_links = .empty,
            .pending_third_party_awaits = std.StringHashMap(PendingThirdPartyAwait).init(allocator),
            .pending_third_party_answers = std.StringHashMap(u32).init(allocator),
            .pending_third_party_returns = std.AutoHashMap(u32, []u8).init(allocator),
            .adopted_third_party_answers = std.AutoHashMap(u32, u32).init(allocator),
            .resolved_imports = std.AutoHashMap(u32, ResolvedImport).init(allocator),
            .pending_embargoes = std.AutoHashMap(u32, u32).init(allocator),
            .loopback_questions = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_yourself = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_third_party = std.AutoHashMap(u32, ?[]u8).init(allocator),
            .loopback_result_stash = std.AutoHashMap(u32, []u8).init(allocator),
            .persistent_exports = std.AutoHashMap(u32, *PersistenceState).init(allocator),
        };
    }

    /// Replace peer resource limits. Existing state is not evicted; future
    /// insertions fail until usage is back under the configured limits.
    pub fn setLimits(self: *Peer, limits: PeerLimits) void {
        self.assertThreadAffinity();
        self.limits = limits;
    }

    /// Install a custom monotonic clock used for deadlines, overriding any
    /// connection-derived time source. The clock's `ctx` must outlive the
    /// peer. Pass `null` to fall back to the connection-derived source
    /// (or disable enforcement when there is none).
    pub fn setClock(self: *Peer, clock: ?rpc_time.Clock) void {
        self.assertThreadAffinity();
        self.clock = clock;
    }

    /// Provide the `std.Io` whose monotonic clock backs deadline features.
    /// `WorkerPool` sets this on accepted peers automatically; embedders
    /// that construct peers directly should call this with the same io the
    /// transport uses. A custom `setClock` overrides this source.
    pub fn setClockIo(self: *Peer, io: ?std.Io) void {
        self.assertThreadAffinity();
        self.clock_io = io;
    }

    /// Current monotonic time in nanoseconds, or null when the peer has no
    /// time source (no custom clock and no io provided via `setClockIo`).
    /// All deadline features are inert in the null case.
    fn clockNow(self: *const Peer) ?i64 {
        if (self.clock) |clock| return clock.now();
        if (self.clock_io) |io| {
            return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
        }
        return null;
    }

    /// Replace the peer's time-domain policy. Affects questions allocated
    /// after this call; existing question deadlines are unchanged.
    pub fn setTimeouts(self: *Peer, timeouts: PeerTimeouts) void {
        self.assertThreadAffinity();
        self.timeouts = timeouts;
    }

    /// Set or replace the deadline on an outstanding question, overriding
    /// any default stamped at send time. Requires a time source.
    pub fn setQuestionDeadline(self: *Peer, question_id: u32, timeout_ms: u64) !void {
        self.assertThreadAffinity();
        const now = self.clockNow() orelse return error.NoClockConfigured;
        const entry = self.questions.getPtr(question_id) orelse return error.UnknownQuestion;
        if (entry.cancelled) return error.QuestionCancelled;
        entry.deadline_ns = now + msToNs(timeout_ms);
    }

    /// Remove the deadline from an outstanding question.
    pub fn clearQuestionDeadline(self: *Peer, question_id: u32) !void {
        self.assertThreadAffinity();
        const entry = self.questions.getPtr(question_id) orelse return error.UnknownQuestion;
        entry.deadline_ns = null;
    }

    /// Canonical transport-attach: bind a typed connection to this peer,
    /// wiring up transport callbacks. This is the single supported way to give
    /// a peer a transport — `Peer.init` calls it, and it is what
    /// `ClientSession`/`ServerSession` rely on. Prefer it whenever you have a
    /// connection object (e.g. `transport.tcp.Connection`).
    ///
    /// The lower-level `attachTransport` / `attachTransportBinding` seams exist
    /// for building brand-new transports and are not the normal-use path.
    ///
    /// Asserts that no transport is already attached. Call `detachConnection`
    /// first if you need to replace an existing transport.
    pub fn attachConnection(self: *Peer, conn: anytype) void {
        self.assertThreadAffinity();
        if (self.hasAttachedTransport()) {
            @panic("attachConnection called while a transport is already attached; call detachConnection first");
        }
        const ConnPtr = @TypeOf(conn);
        comptime {
            const info = @typeInfo(ConnPtr);
            if (info != .pointer) @compileError("attachConnection expects a pointer type");
        }

        self.attachTransportBinding(
            peer_transport_callbacks.bindingForConnection(
                Peer,
                ConnPtr,
                conn,
                Peer.handleFrame,
                onConnectionError,
                onConnectionClose,
            ),
        );

        // Connections with a tick cadence drive the deadline sweep. Note:
        // no time source is captured here — tests attach partially
        // initialized connections, so the io must be provided explicitly
        // via `setClockIo` (or `setClock`).
        const Conn = @typeInfo(ConnPtr).pointer.child;
        if (comptime @hasField(Conn, "on_tick")) {
            conn.on_tick = peer_transport_callbacks.onConnectionTickFor(Peer, ConnPtr);
        }
    }

    /// Detach the connection without closing it. The transport is unbound
    /// but the underlying connection remains open for potential reuse.
    pub fn detachConnection(self: *Peer) void {
        self.assertThreadAffinity();
        self.detachTransport();
    }

    /// Advanced: attach raw transport callbacks (start, send, close,
    /// isClosing) to this peer.
    ///
    /// Prefer the canonical `attachConnection` when you have a typed connection
    /// object — it wires these callbacks for you. Use `attachTransport` only
    /// when bridging a transport that is not a `Connection`-shaped object and
    /// you must supply each callback individually. Panics if a transport is
    /// already attached.
    pub fn attachTransport(
        self: *Peer,
        ctx: *anyopaque,
        start_fn: ?TransportStartFn,
        send_fn: ?TransportSendFn,
        close_fn: ?TransportCloseFn,
        is_closing: ?TransportIsClosingFn,
    ) void {
        self.attachTransportBinding(.init(ctx, start_fn, send_fn, close_fn, is_closing));
    }

    /// Advanced: attach a named transport binding to this peer.
    ///
    /// Prefer the canonical `attachConnection` for normal use. This is the
    /// preferred low-level boundary when *building a new transport*: adapt the
    /// transport into a `TransportBinding`, then attach the binding here.
    /// `attachConnection` and `attachTransport` are thin wrappers over it.
    pub fn attachTransportBinding(self: *Peer, binding: TransportBinding) void {
        self.assertThreadAffinity();
        if (self.hasAttachedTransport()) {
            @panic("attachTransportBinding called while a transport is already attached; call detachTransport first");
        }
        self.transport = binding;
    }

    /// Detach the transport without closing it, clearing all transport callbacks.
    pub fn detachTransport(self: *Peer) void {
        self.assertThreadAffinity();
        peer_transport_state.detachTransportForPeer(Peer, self);
    }

    /// Attach the Level-3 three-party addressing seam. Required before
    /// originating `Provide`/`Accept` handoffs (`sendProvide`/`sendAccept`); a
    /// plain two-party peer never needs one. The network's `ctx` must outlive
    /// the peer.
    /// Callee policy for an inbound `Call` carrying `sendResultsTo = thirdParty`.
    ///
    /// That field asks the callee to connect to a third vat, deliver the results
    /// there, and answer its immediate caller with `resultsSentElsewhere`. It is
    /// a Level-3 feature, a callee is not required to implement it, and neither
    /// reference implementation does: go-capnp echoes `Unimplemented` and drops
    /// the call before touching its answer table, and the C++ stack fails the
    /// requirement and aborts the connection.
    pub const ThirdPartyResultPolicy = enum {
        /// Default. Answer with a single exception `Return` and never dispatch
        /// the call.
        reject,
        /// The application performs the redirect itself: the call dispatches
        /// normally, and the handler is responsible for delivering results to
        /// the third vat and then settling this answer with
        /// `sendReturnResultsSentElsewhere`.
        application,
    };

    /// Select how inbound `sendResultsTo = thirdParty` calls are handled. Per
    /// `Peer`, not per call: in the canonical handoff topology only the callee's
    /// peer facing the introducer needs `.application`.
    pub fn setThirdPartyResultPolicy(self: *Peer, policy: ThirdPartyResultPolicy) void {
        self.assertThreadAffinity();
        self.third_party_result_policy = policy;
    }

    pub fn attachVatNetwork(self: *Peer, network: VatNetwork) void {
        self.assertThreadAffinity();
        self.vat_network = network;
    }

    /// Detach the vat network. Does not tear down any in-flight handoffs.
    pub fn detachVatNetwork(self: *Peer) void {
        self.assertThreadAffinity();
        self.vat_network = null;
    }

    /// Attach the Experimental Level-4 Join network seam. When attached, inbound
    /// Join completion uses Zig `JoinResult` payloads plus a direct follow-up
    /// `Accept`; when absent, the legacy raw Join pilot returns the cap directly.
    pub fn attachJoinNetwork(self: *Peer, network: JoinNetwork) void {
        self.assertThreadAffinity();
        self.join_network = network;
    }

    /// Detach the Experimental L4 Join network. Does not tear down in-flight
    /// JoinResult provisions.
    pub fn detachJoinNetwork(self: *Peer) void {
        self.assertThreadAffinity();
        self.join_network = null;
    }

    /// Install the Level-3 recipient auto-pickup handler. With both a
    /// `vat_network` (see `attachVatNetwork`) and this handler set, an inbound
    /// `thirdPartyHosted` Resolve for a promise this peer holds triggers an
    /// automatic `Accept` against the third vat; the handler receives the direct
    /// capability from the Accept `Return` (see `HandoffPickupCallback`). Without
    /// it, the Level-1/2 proxy-via-vine fallback is used. `ctx` is borrowed and
    /// must outlive the peer.
    pub fn setHandoffPickupHandler(self: *Peer, ctx: *anyopaque, on_pickup: HandoffPickupCallback) void {
        self.assertThreadAffinity();
        self.handoff_pickup_ctx = ctx;
        self.on_handoff_pickup = on_pickup;
    }

    /// Remove the Level-3 recipient auto-pickup handler, reverting to the
    /// Level-1/2 proxy-via-vine fallback for inbound `thirdPartyHosted` Resolves.
    pub fn clearHandoffPickupHandler(self: *Peer) void {
        self.assertThreadAffinity();
        self.handoff_pickup_ctx = null;
        self.on_handoff_pickup = null;
    }

    /// Return whether a transport is currently attached to this peer.
    pub fn hasAttachedTransport(self: *const Peer) bool {
        self.assertThreadAffinity();
        return peer_transport_state.hasAttachedTransportForPeer(Peer, self);
    }

    /// Close the attached transport, signaling the remote peer.
    pub fn closeAttachedTransport(self: *Peer) void {
        self.assertThreadAffinity();
        peer_transport_state.closeAttachedTransportForPeer(Peer, self);
    }

    /// Return whether the attached transport is currently in the process of closing.
    pub fn isAttachedTransportClosing(self: *const Peer) bool {
        self.assertThreadAffinity();
        return peer_transport_state.isAttachedTransportClosingForPeer(Peer, self);
    }

    /// Detach and return the owned connection, cast to `ConnPtr`.
    /// Returns `null` if no transport is attached or the context type does not match.
    pub fn takeAttachedConnection(self: *Peer, comptime ConnPtr: type) ?ConnPtr {
        self.assertThreadAffinity();
        return peer_transport_state.takeAttachedConnectionForPeer(
            Peer,
            ConnPtr,
            self,
            detachTransport,
        );
    }

    /// Return the attached connection cast to `ConnPtr` without detaching it.
    /// Returns `null` if no transport is attached.
    pub fn getAttachedConnection(self: *const Peer, comptime ConnPtr: type) ?ConnPtr {
        self.assertThreadAffinity();
        return peer_transport_state.getAttachedConnectionForPeer(
            Peer,
            ConnPtr,
            self,
        );
    }

    /// Release all owned state: pending calls, resolved answers, export
    /// entries, and the capability table.
    ///
    /// Outstanding questions receive a terminal synthetic "disconnected"
    /// exception Return through their callbacks BEFORE any state is torn
    /// down — deinit of a live peer must not strand callers awaiting a
    /// Return (their heap contexts previously leaked through the
    /// callback-less deinit_ctx sweep). After a transport close this pass
    /// is a no-op: `onConnectionClose` already delivered the terminals.
    pub fn deinit(self: *Peer) void {
        self.assertThreadAffinity();
        self.in_deinit = true;
        // LIVENESS (BUG #55): this peer may host held-open Provide questions that
        // recipient peers' `outbound_provides` entries borrow a `provide_peer`
        // pointer back to. `forceCancelAllQuestions` below removes and Finishes
        // those Provide questions, but the borrowed pointer to THIS peer would
        // still dangle. Neutralize it here — null every recipient's back-pointer
        // BEFORE this peer's memory is freed — so a later vine Release is a safe
        // no-op instead of a freed-peer deref. Must run before we return; running
        // it first keeps it independent of any error path below.
        // Vat-wide provision teardown, split per the canonical procedure:
        // infallible neutralize (sever the index back-link, move the owner map
        // out, mark provisions closed, clear holder records) BEFORE
        // forceCancelAllQuestions; the send-bearing drain AFTER it.
        self.detachCrossPeerAcceptsOnHolderPeer();
        var provision_teardown = self.neutralizeProvisionsOnOwnerPeer();
        self.neutralizeCoupledVinesOnProvidePeer();
        self.neutralizeCrossPeerProxiesOnSourcePeer();
        self.neutralizeCrossPeerJoinRelaysOnSourcePeer();
        self.neutralizeJoinAcceptHostLinks();
        _ = self.forceCancelAllQuestions(disconnected_reason, .disconnected);
        self.neutralizeJoinCoordinatorAcceptLinks();
        self.drainClosedProvisionsOnOwnerPeer(&provision_teardown);
        peer_cleanup.deinitPendingCallMapOwned(
            @TypeOf(self.pending_promises),
            self.allocator,
            &self.pending_promises,
        );
        peer_cleanup.deinitPendingCallMapOwned(
            @TypeOf(self.pending_export_promises),
            self.allocator,
            &self.pending_export_promises,
        );
        peer_cleanup.deinitResolvedAnswerMap(
            @TypeOf(self.resolved_answers),
            self.allocator,
            &self.resolved_answers,
        );
        // NOTE: deinit_ctx callbacks must NOT access peer maps (loopback_questions,
        // forwarded_questions, etc.) as they may be deinited after this loop.
        {
            var q_it = self.questions.valueIterator();
            while (q_it.next()) |q| {
                if (q.deinit_ctx) |deinit_ctx| deinit_ctx(self.allocator, q.ctx);
            }
        }
        // Questions parked in pending_third_party_awaits were moved out of the
        // questions map, so the loop above never sees them. Invoke their
        // deinit_ctx too (heap Save/Restore/ForwardCallContext), or a teardown
        // with in-flight three-party handoffs leaks them. The owned map keys are
        // freed by deinitOwnedStringKeyMap below.
        {
            var await_it = self.pending_third_party_awaits.valueIterator();
            while (await_it.next()) |pending_await| {
                if (pending_await.question.deinit_ctx) |deinit_ctx| {
                    deinit_ctx(self.allocator, pending_await.question.ctx);
                }
            }
        }
        self.questions.deinit();
        // forceCancelAllQuestions above already freed the param-export record
        // of every question still in the map; sweep any stragglers (there
        // should be none — records never outlive their question) so a
        // bookkeeping bug degrades to a counter leak, not a memory leak.
        {
            var rec_it = self.question_param_export_refs.valueIterator();
            while (rec_it.next()) |ids| ids.deinit(self.allocator);
        }
        self.question_param_export_refs.deinit();
        self.active_inbound_questions.deinit();
        self.resolving_answers.deinit();
        self.finished_early_answers.deinit();
        {
            var p_it = self.persistent_exports.valueIterator();
            while (p_it.next()) |st| self.allocator.destroy(st.*);
        }
        self.persistent_exports.deinit();
        {
            var e_it = self.exports.valueIterator();
            while (e_it.next()) |entry| {
                if (entry.deinit_ctx) |deinit_ctx| {
                    if (entry.handler) |handler| deinit_ctx(self.allocator, handler.ctx);
                }
            }
        }
        self.exports.deinit();
        self.forwarded_questions.deinit();
        self.forwarded_tail_questions.deinit();
        peer_cleanup.deinitProvideEntryMap(
            @TypeOf(self.provides_by_question),
            self.allocator,
            &self.provides_by_question,
        );
        self.provides_by_key.deinit();
        // The FRESH (post-neutralize) vat-provision maps: contents were moved
        // out and drained above; only the backing stores remain.
        self.provisions_by_question.deinit();
        self.cross_peer_pending_accepts.deinit();
        // OutboundProvide entries own no heap memory (borrowed peer pointer +
        // plain ids). A live entry at teardown means a handoff was still in
        // flight — the paired Provide question is torn down with its own peer's
        // `provides_by_question`. Before dropping the map, deregister each still
        // live coupling's back-link from its provide_peer (if that peer is still
        // alive), so a provider peer that outlives THIS recipient never walks a
        // back-link into freed recipient memory.
        {
            var op_it = self.outbound_provides.iterator();
            while (op_it.next()) |entry| {
                if (entry.value_ptr.provide_peer) |pp| {
                    pp.deregisterCoupledVine(self, entry.key_ptr.*);
                }
                entry.value_ptr.deinitStash(self.allocator);
            }
        }
        self.outbound_provides.deinit();
        // Symmetric back-link list (this peer as a provide_peer). Any residual
        // entries were neutralized at the top of deinit; free the backing store.
        self.coupled_vines.deinit(self.allocator);
        self.cross_peer_proxy_links.deinit(self.allocator);
        self.cross_peer_join_relay_links.deinit(self.allocator);

        peer_cleanup.deinitJoinStateMap(
            @TypeOf(self.pending_joins),
            self.allocator,
            &self.pending_joins,
        );
        self.pending_join_questions.deinit();
        {
            var relay_it = self.pending_join_relays.iterator();
            while (relay_it.next()) |entry| {
                if (entry.value_ptr.source_peer) |source_peer| {
                    source_peer.deregisterCrossPeerJoinRelay(self, entry.key_ptr.*);
                    source_peer.sendJoinRelayFinishAndNeutralize(entry.value_ptr.source_question_id, false) catch |err| {
                        log.debug("cross-peer join relay: failed to finish downstream question {} during deinit: {}", .{
                            entry.value_ptr.source_question_id,
                            err,
                        });
                        source_peer.neutralizeJoinRelayQuestion(entry.value_ptr.source_question_id);
                    };
                }
            }
            self.pending_join_relays.deinit();
        }

        peer_cleanup.deinitOwnedStringKeyListMap(
            @TypeOf(self.pending_accepts_by_embargo),
            self.allocator,
            &self.pending_accepts_by_embargo,
        );
        // Values in pending_accept_embargo_by_question are borrowed from
        // pending_accepts_by_embargo (already freed above), so just deinit.
        self.pending_accept_embargo_by_question.deinit();
        {
            var it = self.pending_join_accepts.iterator();
            while (it.next()) |entry| {
                if (self.join_network) |network| network.cancelHostJoinResult(entry.key_ptr.*);
                self.allocator.free(entry.key_ptr.*);
                entry.value_ptr.deinit(self.allocator);
            }
            self.pending_join_accepts.deinit();
        }
        {
            var it = self.pending_join_result_answers.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.accept_peer) |accept_peer| {
                    if (accept_peer != self) accept_peer.deregisterJoinAcceptHost(self, entry.key_ptr.*);
                }
                self.allocator.free(entry.value_ptr.provision);
            }
            self.pending_join_result_answers.deinit();
        }
        self.join_accept_host_links.deinit(self.allocator);
        self.join_coordinator_accept_links.deinit(self.allocator);
        peer_cleanup.deinitOwnedStringKeyMap(
            @TypeOf(self.pending_third_party_awaits),
            self.allocator,
            &self.pending_third_party_awaits,
        );
        peer_cleanup.deinitOwnedStringKeyMap(
            @TypeOf(self.pending_third_party_answers),
            self.allocator,
            &self.pending_third_party_answers,
        );
        peer_cleanup.deinitOwnedBytesMap(
            @TypeOf(self.pending_third_party_returns),
            self.allocator,
            &self.pending_third_party_returns,
        );
        self.adopted_third_party_answers.deinit();

        self.resolved_imports.deinit();
        self.pending_embargoes.deinit();
        self.loopback_questions.deinit();
        self.send_results_to_yourself.deinit();
        {
            var stash_it = self.loopback_result_stash.valueIterator();
            while (stash_it.next()) |frame| self.allocator.free(frame.*);
        }
        self.loopback_result_stash.deinit();
        peer_cleanup.deinitOptionalOwnedBytesMap(
            @TypeOf(self.send_results_to_third_party),
            self.allocator,
            &self.send_results_to_third_party,
        );
        peer_cleanup.clearOptionalOwnedBytes(self.allocator, &self.last_remote_abort_reason);
        self.releaseAllImports();
        self.caps.deinit();
    }

    /// Best-effort: send Release messages for all remaining imports so the
    /// remote peer can decrement its export ref counts.
    ///
    /// Errors are logged but not propagated because this runs during `deinit`
    /// when the transport may already be closed or in an error state.
    ///
    /// If the transport send function is null (i.e. the connection was
    /// already destroyed or never attached), we skip sending entirely --
    /// there is no peer to receive the Release messages.
    fn releaseAllImports(self: *Peer) void {
        // If neither a send-frame override nor the transport send function is
        // available, the connection is already gone -- skip sending.
        if (self.send_frame_override == null and self.transport.send == null) {
            log.debug("releaseAllImports: transport not attached, skipping release messages", .{});
            return;
        }
        var it = self.caps.imports.iterator();
        while (it.next()) |entry| {
            peer_outbound_control.sendReleaseViaSendFrame(
                Peer,
                self,
                entry.key_ptr.*,
                entry.value_ptr.ref_count,
                Peer.sendFrameControl,
            ) catch |err| {
                log.debug("releaseAllImports: failed to send release for import {}: {}", .{ entry.key_ptr.*, err });
            };
        }
    }

    /// Start the peer, registering error/close callbacks and initiating the
    /// transport (if attached). `cb_ctx` is handed back to both callbacks;
    /// it must outlive the peer.
    pub fn start(
        self: *Peer,
        cb_ctx: ?*anyopaque,
        on_error: ?*const fn (ctx: ?*anyopaque, peer: *Peer, err: anyerror) void,
        on_close: ?*const fn (ctx: ?*anyopaque, peer: *Peer) void,
    ) void {
        self.assertThreadAffinity();
        self.callback_ctx = cb_ctx;
        self.on_error = on_error;
        self.on_close = on_close;
        events.emitConnection(self.observer, .peer, .unknown, .started);
        self.transport.startIfPresent(self);
    }

    /// Install or clear a redacted RPC observer for peer-level dispatch events.
    pub fn setObserver(self: *Peer, observer: ?events.Observer) void {
        self.assertThreadAffinity();
        self.observer = observer;
    }

    /// Set a hook to intercept all outbound frames before they reach the transport.
    /// Pass `null` to clear a previously installed override.
    pub fn setSendFrameOverride(self: *Peer, ctx: ?*anyopaque, callback: ?SendFrameOverride) void {
        self.assertThreadAffinity();
        self.send_frame_ctx = ctx;
        self.send_frame_override = callback;
    }

    /// Return the message tag of the most recently processed inbound message, or `null` if none.
    pub fn getLastInboundTag(self: *const Peer) ?protocol.MessageTag {
        self.assertThreadAffinity();
        return self.last_inbound_tag;
    }

    /// Return the reason string from the last remote Abort message, or `null` if none received.
    pub fn getLastRemoteAbortReason(self: *const Peer) ?[]const u8 {
        self.assertThreadAffinity();
        return self.last_remote_abort_reason;
    }

    /// Point-in-time gauge snapshot of peer protocol state, for metrics
    /// scraping. Cheap: map counts plus one walk of the queued-call
    /// buckets and question table.
    pub const PeerStats = struct {
        /// Outstanding outbound calls, including cancelled entries that
        /// are still absorbing their late Return.
        outbound_questions: u32,
        /// Subset of `outbound_questions` that were cancelled locally.
        cancelled_questions: u32,
        /// Inbound calls accepted and not yet returned/finished.
        active_inbound_questions: u32,
        /// Local capabilities currently exported to the remote.
        exports: u32,
        /// Cached Return frames held for promise pipelining.
        resolved_answers: u32,
        /// Resolved promise imports being tracked.
        resolved_imports: u32,
        /// Calls queued behind unresolved promises.
        pending_queued_calls: usize,
        /// Bytes held by calls queued behind unresolved promises.
        pending_queued_call_bytes: usize,
        /// Exports with persistence hooks installed.
        persistent_exports: u32,
        /// Total `Persistent.save()` calls served since peer creation.
        saves_served: u64,
        /// Total vat-level restore calls served since peer creation.
        restores_served: u64,
    };

    pub fn stats(self: *const Peer) PeerStats {
        self.assertThreadAffinity();
        var cancelled: u32 = 0;
        var it = self.questions.valueIterator();
        while (it.next()) |q| {
            if (q.cancelled) cancelled += 1;
        }
        const queued = self.pendingQueuedCallStats();
        return .{
            .outbound_questions = self.questions.count(),
            .cancelled_questions = cancelled,
            .active_inbound_questions = self.active_inbound_questions.count(),
            .exports = self.exports.count(),
            .resolved_answers = self.resolved_answers.count(),
            .resolved_imports = self.resolved_imports.count(),
            .pending_queued_calls = queued.calls,
            .pending_queued_call_bytes = queued.bytes,
            .persistent_exports = self.persistent_exports.count(),
            .saves_served = self.saves_served,
            .restores_served = self.restores_served,
        };
    }

    const PendingQueuedCallStats = struct {
        calls: usize = 0,
        bytes: usize = 0,
    };

    fn saturatingAdd(a: usize, b: usize) usize {
        return std.math.add(usize, a, b) catch std.math.maxInt(usize);
    }

    fn ensureCountLimit(found_existing: bool, current_count: usize, max_count: usize) !void {
        if (!found_existing and current_count >= max_count) return error.PeerLimitExceeded;
    }

    fn ensureByteLimit(current_bytes: usize, added_bytes: usize, max_bytes: usize) !void {
        if (current_bytes > max_bytes) return error.PeerLimitExceeded;
        if (added_bytes > max_bytes - current_bytes) return error.PeerLimitExceeded;
    }

    fn addPendingQueuedCallStats(totals: *PendingQueuedCallStats, list: *const std.ArrayList(PendingCall)) void {
        for (list.items) |pending| {
            totals.calls = saturatingAdd(totals.calls, 1);
            totals.bytes = saturatingAdd(totals.bytes, pending.frame.len);
        }
    }

    fn pendingQueuedCallStats(self: *const Peer) PendingQueuedCallStats {
        var totals = PendingQueuedCallStats{};
        var pending_it = self.pending_promises.valueIterator();
        while (pending_it.next()) |list| addPendingQueuedCallStats(&totals, list);
        var pending_export_it = self.pending_export_promises.valueIterator();
        while (pending_export_it.next()) |list| addPendingQueuedCallStats(&totals, list);
        return totals;
    }

    fn ensurePendingQueuedCallBudget(
        self: *const Peer,
        pending_map: *const std.AutoHashMap(u32, std.ArrayList(PendingCall)),
        key: u32,
        frame_len: usize,
        max_buckets: usize,
    ) !void {
        try ensureCountLimit(pending_map.contains(key), pending_map.count(), max_buckets);

        const totals = self.pendingQueuedCallStats();
        if (totals.calls >= self.limits.max_pending_queued_calls) return error.PeerLimitExceeded;
        try ensureByteLimit(totals.bytes, frame_len, self.limits.max_pending_queued_call_bytes);
    }

    fn optionalPayloadBytes(payload: ?[]u8) usize {
        return if (payload) |bytes| bytes.len else 0;
    }

    fn sendResultsToThirdPartyBytesExcluding(self: *const Peer, answer_id: u32) usize {
        var total: usize = 0;
        var it = self.send_results_to_third_party.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* == answer_id) continue;
            total = saturatingAdd(total, optionalPayloadBytes(entry.value_ptr.*));
        }
        return total;
    }

    fn pendingThirdPartyReturnBytesExcluding(self: *const Peer, answer_id: u32) usize {
        var total: usize = 0;
        var it = self.pending_third_party_returns.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* == answer_id) continue;
            total = saturatingAdd(total, entry.value_ptr.*.len);
        }
        return total;
    }

    fn pendingAcceptEmbargoKeyBytes(self: *const Peer) usize {
        var total: usize = 0;
        var it = self.pending_accepts_by_embargo.iterator();
        while (it.next()) |entry| {
            total = saturatingAdd(total, entry.key_ptr.*.len);
        }
        return total;
    }

    fn pendingJoinAcceptKeyBytes(self: *const Peer) usize {
        var total: usize = 0;
        var it = self.pending_join_accepts.iterator();
        while (it.next()) |entry| {
            total = saturatingAdd(total, entry.key_ptr.*.len);
        }
        return total;
    }

    fn pendingJoinResultAnswerBytesExcluding(self: *const Peer, answer_id: u32) usize {
        var total: usize = 0;
        var it = self.pending_join_result_answers.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* == answer_id) continue;
            total = saturatingAdd(total, entry.value_ptr.provision.len);
        }
        return total;
    }

    fn activeProvideKeyBytes(self: *const Peer) usize {
        var total: usize = 0;
        var it = self.provides_by_question.valueIterator();
        while (it.next()) |entry| {
            total = saturatingAdd(total, entry.recipient_key.len);
        }
        return total;
    }

    fn pendingThirdPartyCompletionBytes(self: *const Peer) usize {
        var total: usize = 0;
        var await_it = self.pending_third_party_awaits.iterator();
        while (await_it.next()) |entry| {
            total = saturatingAdd(total, entry.key_ptr.*.len);
        }
        var answer_it = self.pending_third_party_answers.iterator();
        while (answer_it.next()) |entry| {
            total = saturatingAdd(total, entry.key_ptr.*.len);
        }
        return total;
    }

    fn ensureProvideBudget(self: *Peer, question_id: u32, recipient_key: []const u8) !void {
        try ensureCountLimit(
            self.provides_by_question.contains(question_id),
            self.provides_by_question.count(),
            self.limits.max_active_provides,
        );
        try ensureCountLimit(
            self.provides_by_key.contains(recipient_key),
            self.provides_by_key.count(),
            self.limits.max_active_provides,
        );
        try ensureByteLimit(
            self.activeProvideKeyBytes(),
            recipient_key.len,
            self.limits.max_active_provide_key_bytes,
        );
    }

    fn ensureJoinBudget(self: *Peer, join_key_part: JoinKeyPart, question_id: u32) !void {
        try ensureCountLimit(
            self.pending_joins.contains(join_key_part.join_id),
            self.pending_joins.count(),
            self.limits.max_pending_joins,
        );
        try ensureCountLimit(
            self.pending_join_questions.contains(question_id),
            self.pending_join_questions.count(),
            self.limits.max_pending_join_questions,
        );
    }

    fn ensurePendingThirdPartyAwaitBudget(self: *Peer, completion_key: []const u8) !void {
        try ensureCountLimit(
            self.pending_third_party_awaits.contains(completion_key),
            self.pending_third_party_awaits.count(),
            self.limits.max_pending_third_party_awaits,
        );
        try ensureByteLimit(
            self.pendingThirdPartyCompletionBytes(),
            completion_key.len,
            self.limits.max_pending_third_party_completion_bytes,
        );
    }

    fn ensurePendingThirdPartyAnswerBudget(self: *Peer, completion_key: []const u8) !void {
        try ensureCountLimit(
            self.pending_third_party_answers.contains(completion_key),
            self.pending_third_party_answers.count(),
            self.limits.max_pending_third_party_answers,
        );
        try ensureByteLimit(
            self.pendingThirdPartyCompletionBytes(),
            completion_key.len,
            self.limits.max_pending_third_party_completion_bytes,
        );
    }

    fn ensureThirdPartyAdoptionBudget(self: *Peer, adopted_answer_id: u32) !void {
        try ensureCountLimit(false, self.questions.count(), self.limits.max_outbound_questions);
        try ensureCountLimit(
            self.adopted_third_party_answers.contains(adopted_answer_id),
            self.adopted_third_party_answers.count(),
            self.limits.max_adopted_third_party_answers,
        );
    }

    /// Register a local capability for export and return its export ID.
    pub fn addExport(self: *Peer, exported: Export) !u32 {
        return self.addExportWithDeinit(exported, null);
    }

    fn addExportWithDeinit(self: *Peer, exported: Export, deinit_ctx: ?ExportDeinitCtxFn) !u32 {
        self.assertThreadAffinity();
        const id = try self.caps.allocExportId();
        try self.caps.noteExport(id);
        errdefer self.caps.clearExport(id);
        try self.exports.put(id, .{
            .handler = exported,
            .deinit_ctx = deinit_ctx,
            .ref_count = 0,
            .is_promise = false,
            .resolved = null,
        });
        log.debug("added export id={}", .{id});
        return id;
    }

    /// Register a local capability under a caller-chosen export id, if the
    /// id is not already exported.
    ///
    /// Host-relay integrations (see `HostPeer.respondHostCallReturnFrame`)
    /// let the trusted host own the export-id space: a prebuilt Return frame
    /// may name an export this peer has never seen. Registering the id here
    /// before the frame is sent gives the peer a dispatchable entry without
    /// disturbing `allocExportId`, which already skips ids present in the
    /// cap table.
    ///
    /// Returns true when a new entry was created, false when `id` was
    /// already exported.
    pub fn ensureExportAt(self: *Peer, id: u32, exported: Export) !bool {
        self.assertThreadAffinity();
        if (self.exports.contains(id)) return false;
        try self.caps.noteExportAt(id);
        errdefer self.caps.clearExport(id);
        try self.exports.put(id, .{
            .handler = exported,
            .deinit_ctx = null,
            .ref_count = 0,
            .is_promise = false,
            .resolved = null,
        });
        log.debug("registered export at explicit id={}", .{id});
        return true;
    }

    /// Remove an export entry that carries no wire, answer-held, or
    /// promise-held references. Used by host-relay rollback paths to undo a
    /// speculative `ensureExportAt` when the frame that would have taken the
    /// first reference fails before sending. No-op when the entry is
    /// missing, still referenced, or is the bootstrap export.
    pub fn removeUnreferencedExport(self: *Peer, id: u32) void {
        self.assertThreadAffinity();
        if (self.bootstrap_export_id) |bootstrap_id| {
            if (bootstrap_id == id) return;
        }
        const entry = self.exports.getPtr(id) orelse return;
        if (entry.ref_count != 0 or
            entry.answer_ref_count != 0 or
            entry.promise_ref_count != 0 or
            entry.handoff_ref_count != 0)
        {
            return;
        }
        _ = self.exports.remove(id);
        self.caps.clearExport(id);
    }

    /// Export a promise capability that will be resolved later via
    /// `resolvePromiseExportToExport` or `resolvePromiseExportToException`.
    pub fn addPromiseExport(self: *Peer) !u32 {
        self.assertThreadAffinity();
        const id = try self.caps.allocExportId();
        try self.caps.noteExport(id);
        errdefer self.caps.clearExport(id);
        try self.exports.put(id, .{
            .handler = null,
            .ref_count = 0,
            .is_promise = true,
            .resolved = null,
        });
        errdefer _ = self.exports.remove(id);
        try self.caps.markExportPromise(id);
        return id;
    }

    /// Register a capability as this peer's bootstrap interface.
    ///
    /// Returns the export ID. The remote peer can obtain this capability
    /// by sending a Bootstrap message.
    pub fn setBootstrap(self: *Peer, exported: Export) !u32 {
        self.assertThreadAffinity();
        const id = try self.addExport(exported);
        self.bootstrap_export_id = id;
        log.debug("bootstrap set export_id={}", .{id});
        return id;
    }

    /// Send a Bootstrap request to obtain the remote peer's bootstrap capability.
    ///
    /// Returns the question ID. When the remote peer responds, `on_return`
    /// is invoked with the bootstrap capability in the return payload.
    pub fn sendBootstrap(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        const question_id = try self.allocateQuestion(ctx, on_return);
        errdefer self.removeQuestion(question_id);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        try builder.buildBootstrap(question_id);
        try self.sendBuilder(&builder);
        log.debug("sent bootstrap question_id={}", .{question_id});
        return question_id;
    }

    // -- Three-party handoff ORIGINATION (Provide / Accept) -------------------

    /// No-op Return callback for a held-open Provide question. A `Provide`
    /// receives NO `Return` (rpc.capnp:834-847); the question is registered only
    /// to reserve its id and to be Finished later (on vine release or teardown).
    /// The single case that invokes this is the shutdown drain delivering a
    /// synthetic local exception, which the held-open provision safely ignores.
    fn onProvideNoReturn(
        _: *anyopaque,
        _: *Peer,
        _: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {}

    /// Originate a three-party handoff: hand the capability named by
    /// `provided_target` (a `MessageTarget` local to the vat this peer is
    /// connected to — the HOST of the provided cap, VatC in the spec) to a third
    /// party (VatA), so VatA can pick it up directly with an `Accept`.
    ///
    /// Message direction follows rpc.capnp: the `Provide` goes to the HOST of
    /// the provided cap (this `self` peer = the VatB↔VatC connection). The vine
    /// export and the `thirdPartyHosted` descriptor, by contrast, belong on the
    /// host-of-recipient connection (`host_of_recipient` = the VatB↔VatA
    /// connection), since that is where VatA takes its wire reference and where
    /// the vine's later `Release` arrives.
    ///
    /// Steps:
    ///   1. Register a held-open Provide question on `self` (no Return; Finished
    ///      when the recipient releases the vine — see `handleRelease`).
    ///   2. Mint a vine export on `host_of_recipient` and mark it
    ///      third-party-hosted with `contact_payload` (the `ThirdPartyToContact`)
    ///      so the next descriptor emitted for the vine resolves to
    ///      `thirdPartyHosted{ id = contact, vineId }`.
    ///   3. Record the vine → Provide-question coupling on `host_of_recipient`.
    ///   4. Send the `Provide` to `self` (VatC) with `recipient` = the
    ///      `ThirdPartyToAwait`.
    ///
    /// Returns the vine id + Provide question id. The caller then drives the
    /// `thirdPartyHosted` emission by sending `host_of_recipient` a payload that
    /// carries the vine export (which is marked); the recipient picks it up and
    /// eventually `sendAccept`s on its own connection to VatC.
    pub fn sendProvide(
        self: *Peer,
        provided_target: protocol.MessageTarget,
        recipient: message.AnyPointerReader,
        host_of_recipient: *Peer,
        contact_payload: []const u8,
    ) !ProvideHandle {
        self.assertThreadAffinity();
        host_of_recipient.assertThreadAffinity();
        if (self.is_shutting_down or host_of_recipient.is_shutting_down) return error.PeerShuttingDown;

        // (1) Held-open Provide question on the host-of-provided-cap connection.
        //     ctx is unused by onProvideNoReturn; pass a valid pointer (self)
        //     rather than undefined so no dispatch path ever reads garbage.
        const question_id = try self.allocateQuestion(self, onProvideNoReturn);
        errdefer self.removeQuestion(question_id);

        // (2) Vine export on the host-of-recipient connection. ctx is unused by
        //     vineRejectingCall; pass host_of_recipient rather than undefined.
        const vine_id = try host_of_recipient.addExport(.{
            .ctx = host_of_recipient,
            .on_call = vineRejectingCall,
        });
        errdefer host_of_recipient.releaseVineExport(vine_id);
        try host_of_recipient.caps.markThirdPartyHosted(vine_id, contact_payload, vine_id);
        errdefer host_of_recipient.caps.clearThirdPartyHosted(vine_id);

        // (3) Couple the vine to the held-open Provide question so a later
        //     vine Release (post-Accept) Finishes the provision on VatC.
        try ensureCountLimit(
            host_of_recipient.outbound_provides.contains(vine_id),
            host_of_recipient.outbound_provides.count(),
            host_of_recipient.limits.max_active_provides,
        );
        // Remember how to reach the provided cap on `provide_peer` (B↔C) so a
        // parked pipelined call replayed on the vine can be FORWARDED to VatC
        // (issue #56) instead of hitting the rejecting vine. Only a simple
        // `importedCap` target has an unambiguous single import to forward to;
        // for any other target shape the vine keeps rejecting (documented on
        // OutboundProvide.provided_import_id).
        const provided_import_id: ?u32 =
            if (provided_target.tag == .importedCap) provided_target.imported_cap else null;
        try host_of_recipient.outbound_provides.put(vine_id, .{
            .provide_peer = self,
            .provide_question_id = question_id,
            .provided_import_id = provided_import_id,
        });
        errdefer _ = host_of_recipient.outbound_provides.remove(vine_id);

        // (3b) LIVENESS (BUG #55): register the reverse back-link on `self` (the
        //      host-of-provided-cap peer). If `self` deinits before the vine is
        //      released, this lets it null the borrowed pointer above so the
        //      later Release never dereferences freed memory. Must land or the
        //      whole coupling unwinds (the errdefers above roll it back).
        try self.registerCoupledVine(host_of_recipient, vine_id);
        errdefer self.deregisterCoupledVine(host_of_recipient, vine_id);

        // (4) Send the Provide to the host of the provided cap (VatC).
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildProvide(question_id, provided_target, recipient);
        try self.sendBuilder(&builder);

        log.debug("sent provide question_id={} vine_id={}", .{ question_id, vine_id });
        return .{ .question_id = question_id, .vine_id = vine_id };
    }

    /// Pick up a capability a third party provided to us: send `Accept` to the
    /// HOST of the provided cap (this `self` peer = the VatA↔VatC connection).
    /// `provision` is the `ThirdPartyCompletion` obtained from the VatNetwork,
    /// matching the `ThirdPartyToAwait` VatB placed in its `Provide`. The
    /// `on_return` callback receives the `Return` carrying the accepted cap
    /// (standard import-from-return; see the host side `sendReturnProvidedTarget`).
    ///
    /// This slice sends `embargo = null` (no in-flight-promise ordering — that
    /// is Phase 4). Returns the Accept question id.
    pub fn sendAccept(
        self: *Peer,
        provision: message.AnyPointerReader,
        embargo: ?[]const u8,
        ctx: *anyopaque,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;

        const question_id = try self.allocateQuestion(ctx, on_return);
        errdefer self.removeQuestion(question_id);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildAccept(question_id, provision, embargo);
        try self.sendBuilder(&builder);

        log.debug("sent accept question_id={}", .{question_id});
        return question_id;
    }

    fn sendAcceptNoRestore(
        self: *Peer,
        provision: message.AnyPointerReader,
        embargo: ?[]const u8,
        ctx: *anyopaque,
        on_return: QuestionCallback,
        suppress_auto_finish: bool,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;

        const question_id = try self.allocateQuestionNoRestore(ctx, on_return);
        errdefer self.removeQuestion(question_id);
        if (suppress_auto_finish) {
            const question = self.questions.getPtr(question_id) orelse return error.MissingAllocatedQuestion;
            question.suppress_auto_finish = true;
        }

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildAccept(question_id, provision, embargo);
        try self.sendBuilder(&builder);

        log.debug("sent accept question_id={}", .{question_id});
        return question_id;
    }

    /// Experimental Level-4 Join origination. This is a low-level/manual
    /// helper: callers provide the raw `Join.keyPart` AnyPointer and receive the
    /// ordinary `Return` callback for the Join question. It intentionally does
    /// not implement the full E-join workflow, public `JoinResult` processing,
    /// or direct-connection formation.
    pub fn sendJoinExperimental(
        self: *Peer,
        target: protocol.MessageTarget,
        key_part: ?message.AnyPointerReader,
        ctx: *anyopaque,
        on_return: QuestionCallback,
    ) !u32 {
        return self.sendJoinExperimentalWithAutoFinish(target, key_part, ctx, on_return, false);
    }

    fn sendJoinExperimentalWithAutoFinish(
        self: *Peer,
        target: protocol.MessageTarget,
        key_part: ?message.AnyPointerReader,
        ctx: *anyopaque,
        on_return: QuestionCallback,
        suppress_auto_finish: bool,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;

        const question_id = try self.allocateQuestion(ctx, on_return);
        errdefer self.removeQuestion(question_id);
        if (suppress_auto_finish) {
            const question = self.questions.getPtr(question_id) orelse return error.MissingAllocatedQuestion;
            question.suppress_auto_finish = true;
        }

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildJoin(question_id, target, key_part);
        try self.sendBuilder(&builder);

        log.debug("sent experimental join question_id={}", .{question_id});
        return question_id;
    }

    /// Allocate the next callee-chosen answer id for an outbound
    /// `ThirdPartyAnswer`. Per rpc.capnp:936-941 the value must have bit 30 set
    /// and bit 31 clear ([2^30, 2^31)); this is enforced by masking the counter
    /// back into range on every wrap, and asserted via
    /// `peer_third_party.isThirdPartyAnswerId`. The chosen id is NOT recorded in
    /// any local table on the callee: it names the answer on the *recipient's*
    /// (VatA's) connection, and the callee only re-uses it as the `answerId` of
    /// the follow-up `Return` (which is a plain outbound frame on the same
    /// wire). Callee and recipient are different vats, so there is no collision
    /// with the callee's own caller-allocated question ids.
    fn allocateThirdPartyAnswerId(self: *Peer) u32 {
        var id = self.next_third_party_answer_id;
        // Keep bit 30 set / bit 31 clear even across a full-range wrap.
        if (id < third_party_answer_id_base or id >= third_party_answer_id_limit) {
            id = third_party_answer_id_base;
        }
        var next = id + 1;
        if (next >= third_party_answer_id_limit) next = third_party_answer_id_base;
        self.next_third_party_answer_id = next;
        std.debug.assert(peer_third_party.isThirdPartyAnswerId(id));
        return id;
    }

    /// Callee → third party: send a `ThirdPartyAnswer` on the connection to the
    /// vat that will receive this call's results (VatA). This is the
    /// redirected-return origination for a call that arrived with
    /// `sendResultsTo = thirdParty` (rpc.capnp:494-505, 906-942): the callee
    /// (VatC) connects to the third party and adopts the call into that
    /// connection by announcing a callee-allocated `answerId`. The recipient
    /// matches `completion` (the `ThirdPartyCompletion`) against the
    /// `awaitFromThirdParty` bookkeeping it was primed with, adopts its parked
    /// question under `answer_id`, and thereafter accepts the follow-up `Return`
    /// (which the callee sends under the SAME `answer_id`) as that question's
    /// results.
    ///
    /// `completion` must serialize to bytes byte-identical to the recipient's
    /// awaited `ThirdPartyToAwait`/`ThirdPartyCompletion` — the recipient keys
    /// its await table on those bytes (see `handleThirdPartyAnswer`).
    ///
    /// Returns the callee-allocated answer id, which the caller then re-uses as
    /// the `answerId` of the follow-up results `Return` on this same peer.
    pub fn sendThirdPartyAnswer(
        self: *Peer,
        completion: message.AnyPointerReader,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;

        const answer_id = self.allocateThirdPartyAnswerId();

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildThirdPartyAnswer(answer_id, completion);
        try self.sendBuilder(&builder);

        log.debug("sent thirdPartyAnswer answer_id={}", .{answer_id});
        return answer_id;
    }

    /// Third-party (recipient) side: park an outbound question that awaits a
    /// `ThirdPartyAnswer` from the callee, keyed by the completion bytes it will
    /// present. This is the recipient-side half of the `awaitFromThirdParty`
    /// bookkeeping the receive path already reads: when the callee later sends a
    /// `ThirdPartyAnswer` whose completion matches `completion`, `handleThirdPartyAnswer`
    /// adopts this parked question under the callee-allocated answer id, and the
    /// follow-up `Return` completes it via `on_return`.
    ///
    /// In the spec's canonical proxy topology this parking happens implicitly
    /// when the recipient receives a `Return{awaitFromThirdParty}` for a call it
    /// itself made (`handleReturnAcceptFromThirdParty`). In a first-class
    /// origination the recipient (VatA) never made that call — the introducer
    /// (VatB) did — so the recipient is primed explicitly here. `completion`
    /// must serialize byte-identically to the completion the callee will send.
    ///
    /// The parked question is delivered exactly one `Return` (carrying the real
    /// results) and needs no auto-Finish of its own; adoption inserts it into
    /// the normal questions table under the adopted id, after which it follows
    /// the standard Return lifecycle.
    pub fn registerPendingThirdPartyAwait(
        self: *Peer,
        completion: message.AnyPointerReader,
        ctx: *anyopaque,
        on_return: QuestionCallback,
    ) !void {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;

        const completion_key = (try captureAnyPointerPayload(self.allocator, completion)) orelse
            return error.MissingThirdPartyPayload;
        var owns_key = true;
        errdefer if (owns_key) self.allocator.free(completion_key);

        if (self.pending_third_party_awaits.contains(completion_key)) {
            return error.DuplicateThirdPartyAwait;
        }

        // Reserve an answer-id slot's worth of question budget so the later
        // adoption into `questions` cannot overflow silently. The parked
        // question is a normal outbound question that has not yet been assigned
        // a wire id (the callee chooses it), so it is NOT inserted into
        // `questions` here — only into the await table.
        try self.ensurePendingThirdPartyAwaitBudget(completion_key);

        const question = Question{
            .ctx = ctx,
            .on_return = on_return,
            // The adopted question follows the standard Return lifecycle: when
            // the callee's follow-up results `Return` (under the callee-chosen
            // answer id) arrives, the recipient sends a `Finish` for that id
            // back to the callee, per rpc.capnp:924-926 ("the receiver ... must
            // eventually send a Finish message"). That Finish clears the
            // callee's answer-table entry for the redirected results, so
            // auto-finish must NOT be suppressed here.
        };

        try peer_third_party.putPendingAwait(
            PendingThirdPartyAwait,
            &self.pending_third_party_awaits,
            completion_key,
            .{ .question_id = 0, .question = question },
        );
        owns_key = false;
    }

    /// Call handler installed on a vine export. A vine is primarily a
    /// liveness/refcount anchor for a three-party handoff. When a caller (VatA)
    /// pipelined calls on the handed-off promise, those calls are replayed onto
    /// this vine and are FORWARDED to VatC by `maybeForwardVineCall` (issue #56)
    /// BEFORE dispatch ever reaches this handler — so this handler only runs for
    /// a call on a vine with NO forwarding coupling (`provided_import_id` unset,
    /// or `provide_peer` already torn down). That is genuine non-handoff misuse
    /// or a raced teardown; answer it with a clean exception rather than
    /// silently dropping it.
    fn vineRejectingCall(
        _: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        try peer.sendReturnException(call.question_id, "vine capability is not directly callable");
    }

    /// L3 parked-call FORWARDING pre-dispatch hook (issue #56). Runs for every
    /// call replayed onto an export. Returns `true` (call consumed) only when
    /// `export_id` is a handoff VINE with a live forwarding coupling — a
    /// `provide_peer` still attached (BUG #55 nulls it on provided-cap-peer
    /// teardown) and a known `provided_import_id`. In that case the call is
    /// forwarded cross-peer to VatC and its result is relayed back to complete
    /// VatA's original pipelined question. Returns `false` for any non-vine
    /// export and for a vine whose coupling cannot forward (no import target, or
    /// a torn-down provide_peer), letting normal export dispatch — and thus
    /// `vineRejectingCall` for the vine — run.
    fn maybeForwardVineCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        export_id: u32,
    ) anyerror!bool {
        const coupling = self.outbound_provides.get(export_id) orelse return false;
        const provide_peer = coupling.provide_peer orelse return false;
        const provided_import_id = coupling.provided_import_id orelse return false;
        if (provide_peer.is_shutting_down) return false;

        try self.forwardVineCallToProvidedCap(call, inbound_caps, provide_peer, provided_import_id);
        return true;
    }

    /// Forward one replayed pipelined call from the handoff vine (on `self`,
    /// B↔A) to the provided capability on VatC (`provide_peer`, B↔C), and relay
    /// VatC's Return back to complete VatA's original pipelined question
    /// (`call.question_id`) on `self`.
    ///
    /// E-ORDER: parked calls are replayed in the exact order VatA sent them
    /// (`pending_export_promises` is an ordered ArrayList drained front-to-back),
    /// and each is sent to VatC synchronously here before the next replay, so
    /// their arrival order at VatC matches VatA's send order. Because the forward
    /// is a normal outbound Call on `provide_peer`, it also orders correctly
    /// against any later direct calls VatA makes on the accepted cap (which only
    /// exist AFTER the embargo clears — the pipelined-before-direct guarantee).
    ///
    /// REFCOUNTS: the forwarded call takes no new ref on the vine (it targets
    /// VatC's cap directly); VatA's original question is completed exactly once
    /// (VatC returns exactly once); the relay context is freed on that Return or
    /// by the forwarded question's `deinit_ctx` on a raced peer teardown. The
    /// vine's own lifetime is unchanged — still driven solely by VatA's Release.
    ///
    /// CAP-CARRYING PAYLOADS: params and results are cloned across independent
    /// connection id spaces by minting proxy exports on the outgoing peer. Those
    /// proxies retain the source-side inbound import ref until the destination
    /// releases the proxy, so callbacks can flow through the forwarded handoff
    /// without dangling the original call/return cap table.
    fn forwardVineCallToProvidedCap(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        provide_peer: *Peer,
        provided_import_id: u32,
    ) !void {
        // One heap ctx serves BOTH callbacks `sendCall` drives with a single
        // ctx: `forwardVineParams` (synchronous build, reads `source_params`)
        // and `forwardVineReturn` (later, reads `recipient_*`).
        const relay = try self.allocator.create(ForwardVineCallContext);
        relay.* = .{
            .forward_peer = provide_peer,
            .recipient_peer = self,
            .recipient_answer_id = call.question_id,
            .source_params = call.params,
            .source_inbound_caps = @constCast(inbound_caps),
        };
        var relay_owned = true;
        errdefer if (relay_owned) ForwardVineCallContext.deinit(self.allocator, relay);

        // Set true by `forwardVineReturn` iff it runs SYNCHRONOUSLY nested inside
        // the `sendCall` below (the loopback case) — meaning the return callback
        // already answered VatA and freed `relay`. `sendCall` can still fail in
        // post-callback work AFTER that (e.g. OOM sending the forwarded question's
        // auto-Finish to VatC), so its error path must NOT then re-answer VatA or
        // re-free `relay`. This flag distinguishes "callback already settled
        // everything" from "the call never went out". It lives on this stack
        // frame; the async hand-off below nulls the ctx's back-pointer so a later
        // callback never writes through it once this frame has unwound.
        var forward_settled = false;
        relay.settled_flag = &forward_settled;

        // Send the forwarded call to VatC on the B↔C connection. The build
        // closure clones VatA's params synchronously (source readers valid for
        // the duration of this send); the return closure relays VatC's result
        // back onto VatA's question on `self`. `sendForwardedVineCall` allocates
        // the question with `restore_on_return_error = false` so a post-callback
        // error (e.g. OOM in the auto-Finish) does NOT restore a question whose
        // ctx `forwardVineReturn` already freed — critical in synchronous loopback
        // where the return is processed inside this send, before any post-send
        // code could clear that flag.
        const forwarded_question_id = provide_peer.sendForwardedVineCall(
            provided_import_id,
            call.interface_id,
            call.method_id,
            relay,
            forwardVineParams,
            forwardVineReturn,
        ) catch |err| {
            if (forward_settled) {
                // The synchronous return callback already answered VatA and freed
                // `relay` before `sendCall`'s post-callback work failed. VatA is
                // settled exactly once; swallow the trailing error.
                relay_owned = false;
                return;
            }
            // The callback never ran: the forwarded call did not go out. `relay`
            // is still ours — free it explicitly (a plain `return` would skip the
            // errdefer) and answer VatA with a clean exception rather than leaving
            // its question hung. Failing to send that exception is itself
            // best-effort (logged, not propagated): VatA's own connection settles
            // the question on teardown if this last resort also fails.
            relay_owned = false;
            ForwardVineCallContext.deinit(self.allocator, relay);
            self.sendReturnException(call.question_id, @errorName(err)) catch |send_err| {
                log.debug("forwarded pipelined call: failed to fail question {}: {}", .{ call.question_id, send_err });
            };
            return;
        };
        if (forward_settled) {
            // Synchronous loopback: the return callback already ran, answered
            // VatA, and freed `relay`. Nothing left to own or hand off.
            relay_owned = false;
            return;
        }
        // Async transport (or VatC deferred its answer): the return callback has
        // not run yet. Detach the stack-local settle flag — this frame is about
        // to unwind — so the deferred callback never writes through a dangling
        // pointer, then hand `relay` ownership to the forwarded question. Its
        // `deinit_ctx` frees it if the B↔C peer tears down before the Return
        // (which never completes VatA's question; VatA's own connection settles
        // it independently). The question is already `restore_on_return_error =
        // false` from `sendForwardedVineCall`.
        relay.param_proxies_committed = true;
        relay.settled_flag = null;
        if (provide_peer.questions.getPtr(forwarded_question_id)) |q| {
            q.deinit_ctx = ForwardVineCallContext.deinit;
        }
        relay_owned = false;
    }

    /// Build closure for the forwarded call to VatC: clone VatA's parked params
    /// into the new call's payload, remapping capability pointers into B↔C
    /// proxy exports. Runs synchronously inside `sendCall`.
    fn forwardVineParams(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *ForwardVineCallContext = castCtx(*ForwardVineCallContext, ctx_ptr);
        const payload_builder = try call_builder.payloadTyped();
        try ctx.forward_peer.clonePayloadAcrossPeers(
            call_builder.call.builder,
            payload_builder,
            ctx.source_params,
            ctx.recipient_peer,
            ctx.source_inbound_caps,
            &ctx.created_param_proxy_ids,
        );
    }

    /// Return closure for the forwarded call: relay VatC's Return back onto
    /// VatA's original pipelined question on the recipient peer (B↔A). Runs on
    /// `provide_peer` (B↔C) as the forwarded question's callback; the `peer`
    /// arg here is that B↔C peer, NOT the recipient. Completes VatA's question
    /// exactly once and frees the relay ctx.
    ///
    /// This callback deliberately NEVER propagates an error: it relays
    /// best-effort (a send failure completing VatA's question is logged, not
    /// returned) and always frees the ctx. It also sets `settled_flag` — when the
    /// caller is still on the stack (the synchronous-loopback case) — the instant
    /// it runs, BEFORE freeing the ctx. Together these keep the ctx single-freed
    /// and VatA answered exactly once even when `sendCall` ITSELF fails in
    /// post-callback work AFTER this callback already answered VatA and freed the
    /// ctx (e.g. OOM sending the forwarded question's auto-Finish): the caller's
    /// error path sees `settled_flag == true` and neither re-answers VatA nor
    /// re-frees. Swallowing here alone is not sufficient — the trailing failure
    /// originates in `sendCall`, not in this callback — so the flag is load-bearing.
    fn forwardVineReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *ForwardVineCallContext = castCtx(*ForwardVineCallContext, ctx_ptr);
        const recipient = ctx.recipient_peer;
        const answer_id = ctx.recipient_answer_id;
        const release_param_caps = ctx.created_param_proxy_ids.items.len == 0;
        // Signal the synchronous caller that the callback ran (it is about to
        // answer VatA and free this ctx) BEFORE freeing — while `settled_flag`,
        // if set, still points at that caller's live stack frame. On the async
        // hand-off path the caller has already nulled it, so this is a no-op.
        if (ctx.settled_flag) |flag| flag.* = true;
        defer ForwardVineCallContext.deinit(peer.allocator, ctx);

        relayForwardedVineReturn(recipient, answer_id, peer, ret, inbound_caps, release_param_caps) catch |err| {
            log.debug("forwarded pipelined return relay failed for question {}: {}", .{ answer_id, err });
        };
    }

    /// Complete VatA's original pipelined question (`answer_id` on `recipient`,
    /// B↔A) from VatC's forwarded Return. Results are cloned onto VatA's Return
    /// with the same cross-peer capability proxying used for forwarded params.
    /// Its errors are swallowed by `forwardVineReturn`.
    fn relayForwardedVineReturn(
        recipient: *Peer,
        answer_id: u32,
        source_peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
        release_param_caps: bool,
    ) !void {
        try relayReturnAcrossPeers(recipient, answer_id, source_peer, ret, inbound_caps, release_param_caps);
    }

    /// Destroy a freshly-minted vine export on the origination error path,
    /// before any `thirdPartyHosted` descriptor has been emitted (so it holds no
    /// wire reference and no Release is owed). Removes the export table entry and
    /// clears its cap-table identity directly — `releaseExport(_, 0)` is a no-op.
    fn releaseVineExport(self: *Peer, vine_id: u32) void {
        _ = self.exports.remove(vine_id);
        self.caps.clearExport(vine_id);
    }

    // -- Persistence (sturdy refs, RPC level 2) -------------------------------

    /// Mark an existing export persistent: when the remote calls
    /// `Persistent.save()` on it, `on_save` produces the sturdy-ref payload
    /// for the `SaveResults` Return. The save call is dispatched through the
    /// normal inbound-call path; every other interface still reaches the
    /// export's original handler. Calling this again replaces the hooks.
    pub fn setPersistentExport(self: *Peer, export_id: u32, ctx: *anyopaque, on_save: SaveHandler) !void {
        self.assertThreadAffinity();
        const st = try self.ensurePersistenceState(export_id);
        st.save = .{ .ctx = ctx, .on_save = on_save };
        log.debug("export {} marked persistent", .{export_id});
    }

    /// Remove the save hook from an export. The original call handler is
    /// restored once no persistence hooks remain on the export.
    pub fn clearPersistentExport(self: *Peer, export_id: u32) void {
        self.assertThreadAffinity();
        const st = self.persistent_exports.get(export_id) orelse return;
        st.save = null;
        self.dropPersistenceStateIfUnused(export_id, st);
    }

    /// Install the vat-level restorer hook on the bootstrap capability.
    ///
    /// Restore is vat-specific per the RPC spec; capnp-zig serves it as a
    /// call on the bootstrap export using the `Restorer` convention in
    /// `peer/persistence.zig` (`restore(sturdyRef :Data) -> (cap :Capability)`).
    /// `on_restore` maps the sturdy-ref bytes to a capability (or `.unknown`,
    /// which answers with an "unknown sturdy ref" exception). Requires
    /// `setBootstrap` to have been called; call it again after replacing the
    /// bootstrap export.
    pub fn setRestorer(self: *Peer, ctx: *anyopaque, on_restore: RestoreHandler) !void {
        self.assertThreadAffinity();
        const bootstrap_id = self.bootstrap_export_id orelse return error.BootstrapNotConfigured;
        const st = try self.ensurePersistenceState(bootstrap_id);
        st.restore = .{ .ctx = ctx, .on_restore = on_restore };
        self.restorer_export_id = bootstrap_id;
        log.debug("restorer installed on bootstrap export {}", .{bootstrap_id});
    }

    /// Remove the restorer hook installed by `setRestorer`.
    pub fn clearRestorer(self: *Peer) void {
        self.assertThreadAffinity();
        const export_id = self.restorer_export_id orelse return;
        self.restorer_export_id = null;
        const st = self.persistent_exports.get(export_id) orelse return;
        st.restore = null;
        self.dropPersistenceStateIfUnused(export_id, st);
    }

    /// Call `Persistent.save()` on an imported capability. The callback
    /// receives the sturdy-ref payload from the remote's `SaveResults`
    /// (or the exception/other Return outcome). Returns the question ID,
    /// which supports `cancelQuestion` / `setQuestionDeadline` as usual.
    pub fn sendSave(self: *Peer, target_id: u32, ctx: *anyopaque, on_response: SaveResponseCallback) !u32 {
        self.assertThreadAffinity();
        const heap = try self.allocator.create(SaveQuestionContext);
        heap.* = .{ .user_ctx = ctx, .callback = on_response };
        var heap_owned = true;
        errdefer if (heap_owned) self.allocator.destroy(heap);
        const question_id = try self.sendCall(
            target_id,
            persistence.persistent_interface_id,
            persistence.save_method_id,
            heap,
            buildSaveCallParams,
            onSaveReturn,
        );
        heap_owned = false;
        if (self.questions.getPtr(question_id)) |q| {
            q.deinit_ctx = SaveQuestionContext.deinitCtx;
            q.restore_on_return_error = false;
        }
        return question_id;
    }

    /// Call the vat-level restore method on an imported capability
    /// (normally the remote's bootstrap, obtained via `sendBootstrap`).
    /// `sturdy_ref` is copied into the outbound frame before this returns.
    /// The callback receives the restored capability, already retained;
    /// release it via `releaseImport` when done. Documented client flow:
    /// connect -> `sendBootstrap` -> `sendRestore(ref)` -> resume calling.
    pub fn sendRestore(
        self: *Peer,
        target_id: u32,
        sturdy_ref: []const u8,
        ctx: *anyopaque,
        on_response: RestoreResponseCallback,
    ) !u32 {
        self.assertThreadAffinity();
        const heap = try self.allocator.create(RestoreQuestionContext);
        heap.* = .{ .user_ctx = ctx, .callback = on_response, .sturdy_ref = sturdy_ref };
        var heap_owned = true;
        errdefer if (heap_owned) self.allocator.destroy(heap);
        const question_id = try self.sendCall(
            target_id,
            persistence.restorer_interface_id,
            persistence.restore_method_id,
            heap,
            buildRestoreCallParams,
            onRestoreReturn,
        );
        heap_owned = false;
        if (self.questions.getPtr(question_id)) |q| {
            q.deinit_ctx = RestoreQuestionContext.deinitCtx;
            q.restore_on_return_error = false;
        }
        return question_id;
    }

    /// Look up or create the persistence state for an export, swapping the
    /// export's stored handler for the persistence trampoline. The original
    /// handler keeps serving every non-persistence interface.
    fn ensurePersistenceState(self: *Peer, export_id: u32) !*PersistenceState {
        const entry = self.exports.getEntry(export_id) orelse return error.UnknownExport;
        if (self.persistent_exports.get(export_id)) |st| return st;
        const original = entry.value_ptr.handler orelse return error.ExportHasNoHandler;

        const persistent_before = self.persistent_exports.count();
        try ensureCountLimit(false, persistent_before, self.limits.max_persistent_exports);
        const st = try self.allocator.create(PersistenceState);
        errdefer self.allocator.destroy(st);
        st.* = .{ .export_id = export_id, .original = original };
        try self.persistent_exports.put(export_id, st);
        entry.value_ptr.handler = .{ .ctx = st, .on_call = persistenceOnCall };
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .persistent_exports,
            persistent_before,
            self.persistent_exports.count(),
            self.limits.max_persistent_exports,
        );
        return st;
    }

    /// Drop a persistence state once both hook sets are cleared, restoring
    /// the export's original handler.
    fn dropPersistenceStateIfUnused(self: *Peer, export_id: u32, st: *PersistenceState) void {
        if (st.save != null or st.restore != null) return;
        if (self.exports.getEntry(export_id)) |entry| {
            if (entry.value_ptr.handler) |handler| {
                if (handler.on_call == persistenceOnCall and handler.ctx == @as(*anyopaque, @ptrCast(st))) {
                    entry.value_ptr.handler = st.original;
                }
            }
        }
        _ = self.persistent_exports.remove(export_id);
        self.allocator.destroy(st);
    }

    /// Free persistence state for an export that left the exports table
    /// (remote released it). The original handler is gone with the export.
    fn dropPersistenceStateForRemovedExport(self: *Peer, export_id: u32) void {
        if (self.persistent_exports.fetchRemove(export_id)) |removed| {
            self.allocator.destroy(removed.value);
            if (self.restorer_export_id == export_id) self.restorer_export_id = null;
        }
    }

    /// Trampoline installed as the export handler for persistent exports.
    /// Serves `Persistent.save()` and vat-level restore; forwards every
    /// other interface to the export's original handler.
    fn persistenceOnCall(
        ctx: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const st: *PersistenceState = castCtx(*PersistenceState, ctx);
        if (call.interface_id == persistence.persistent_interface_id) {
            if (st.save) |hook| {
                if (call.method_id == persistence.save_method_id) {
                    return peer.servePersistentSave(st.export_id, hook, call);
                }
                return peer.sendReturnException(call.question_id, "unknown method");
            }
        } else if (call.interface_id == persistence.restorer_interface_id) {
            if (st.restore) |hook| {
                if (call.method_id == persistence.restore_method_id) {
                    return peer.serveRestore(hook, call);
                }
                return peer.sendReturnException(call.question_id, "unknown method");
            }
        }
        return st.original.on_call(st.original.ctx, peer, call, caps);
    }

    fn servePersistentSave(self: *Peer, export_id: u32, hook: SaveHook, call: protocol.Call) !void {
        const seal_for = persistence.readSealFor(call.params);
        const sturdy_ref = hook.on_save(hook.ctx, self, export_id, seal_for) catch |err| {
            if (err == error.OutOfMemory) return err;
            log.debug("save handler failed for export {}: {}", .{ export_id, err });
            return self.sendReturnException(call.question_id, @errorName(err));
        };
        defer self.allocator.free(sturdy_ref);
        if (sturdy_ref.len > self.limits.max_sturdy_ref_bytes) {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .sturdy_ref_bytes,
                sturdy_ref.len,
                self.limits.max_sturdy_ref_bytes,
                error.PeerLimitExceeded,
            );
            return self.sendReturnException(call.question_id, "sturdy ref too large");
        }
        var build_ctx = persistence.SturdyRefReturnContext{ .sturdy_ref = sturdy_ref };
        try self.sendReturnResults(call.question_id, &build_ctx, persistence.buildSaveResultsReturn);
        self.saves_served +%= 1;
    }

    fn serveRestore(self: *Peer, hook: RestoreHook, call: protocol.Call) !void {
        const sturdy_ref = persistence.readSturdyRefParam(call.params) catch |err| {
            return self.sendReturnException(call.question_id, @errorName(err));
        };
        if (sturdy_ref.len > self.limits.max_sturdy_ref_bytes) {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .sturdy_ref_bytes,
                sturdy_ref.len,
                self.limits.max_sturdy_ref_bytes,
                error.PeerLimitExceeded,
            );
            return self.sendReturnException(call.question_id, "sturdy ref too large");
        }
        const outcome = hook.on_restore(hook.ctx, self, sturdy_ref) catch |err| {
            if (err == error.OutOfMemory) return err;
            log.debug("restore handler failed: {}", .{err});
            return self.sendReturnException(call.question_id, @errorName(err));
        };
        var hosted_export_id: ?u32 = null;
        const export_id: u32 = switch (outcome) {
            .unknown => return self.sendReturnException(call.question_id, "unknown sturdy ref"),
            .existing => |id| blk: {
                if (!self.exports.contains(id)) {
                    return self.sendReturnException(call.question_id, "unknown sturdy ref");
                }
                break :blk id;
            },
            .host => |exported| blk: {
                const id = try self.addExport(exported);
                hosted_export_id = id;
                break :blk id;
            },
        };
        errdefer if (hosted_export_id) |id| self.destroyUnreferencedExport(id);
        var build_ctx = persistence.CapabilityReturnContext{ .cap_id = export_id };
        try self.sendReturnResults(call.question_id, &build_ctx, persistence.buildCapabilityReturn);
        self.restores_served +%= 1;
    }

    const SaveQuestionContext = struct {
        user_ctx: *anyopaque,
        callback: SaveResponseCallback,

        fn deinitCtx(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
            const ctx: *SaveQuestionContext = @ptrCast(@alignCast(ctx_ptr));
            allocator.destroy(ctx);
        }
    };

    const RestoreQuestionContext = struct {
        user_ctx: *anyopaque,
        callback: RestoreResponseCallback,
        /// Borrowed from the caller; valid only while `sendRestore` runs
        /// (the build callback copies it into the frame synchronously).
        sturdy_ref: []const u8,

        fn deinitCtx(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
            const ctx: *RestoreQuestionContext = @ptrCast(@alignCast(ctx_ptr));
            allocator.destroy(ctx);
        }
    };

    fn buildSaveCallParams(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        _ = ctx_ptr;
        try persistence.writeEmptySaveParams(call_builder);
    }

    fn buildRestoreCallParams(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *const RestoreQuestionContext = castCtx(*const RestoreQuestionContext, ctx_ptr);
        try persistence.writeRestoreParams(call_builder, ctx.sturdy_ref);
    }

    fn onSaveReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        _ = inbound_caps;
        const ctx: *SaveQuestionContext = castCtx(*SaveQuestionContext, ctx_ptr);
        defer peer.allocator.destroy(ctx);
        const response: SaveResponse = switch (ret.tag) {
            .results => blk: {
                const payload = ret.results orelse return error.MissingReturnPayload;
                break :blk .{ .sturdy_ref = try persistence.readSturdyRefResult(payload) };
            },
            .exception => .{ .exception = ret.exception orelse return error.MissingException },
            else => .{ .other = ret.tag },
        };
        try ctx.callback(ctx.user_ctx, peer, response);
    }

    fn onRestoreReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *RestoreQuestionContext = castCtx(*RestoreQuestionContext, ctx_ptr);
        defer peer.allocator.destroy(ctx);
        var retained_cap: ?cap_table.ResolvedCap = null;
        const response: RestoreResponse = switch (ret.tag) {
            .results => blk: {
                const payload = ret.results orelse return error.MissingReturnPayload;
                if (payload.content.isNull()) return error.MissingRestoredCapability;
                const results_struct = payload.content.getStruct() catch return error.MissingRestoredCapability;
                const field = results_struct.readAnyPointer(0) catch return error.MissingRestoredCapability;
                const cap = field.getCapability() catch return error.MissingRestoredCapability;
                var mutable_caps = inbound_caps.*;
                try mutable_caps.retainCapability(cap);
                const resolved = try inbound_caps.resolveCapability(cap);
                retained_cap = resolved;
                break :blk .{ .cap = resolved };
            },
            .exception => .{ .exception = ret.exception orelse return error.MissingException },
            else => .{ .other = ret.tag },
        };
        ctx.callback(ctx.user_ctx, peer, response) catch |err| {
            if (retained_cap) |resolved| {
                peer.releaseResolvedCap(resolved) catch |release_err| {
                    if (release_err == error.OutOfMemory) return error.OutOfMemory;
                    log.debug("failed to release restored cap after callback error: {}", .{release_err});
                };
            }
            return err;
        };
    }

    /// Begin a graceful shutdown: reject new outbound calls, wait for
    /// outstanding questions to receive their Return, then fire `on_complete`.
    ///
    /// If there are no outstanding questions the callback fires immediately.
    /// Calling `shutdown` a second time is a no-op.
    pub fn shutdown(self: *Peer, on_complete: ?*const fn (peer: *Peer) void) void {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return;
        self.is_shutting_down = true;
        self.shutdown_callback = on_complete;

        if (self.questions.count() == 0) {
            self.completeShutdown();
            return;
        }
        // Stamp the drain bound; `checkDeadlines` force-cancels stragglers
        // once it passes.
        if (self.clockNow()) |now| {
            if (self.timeouts.shutdown_drain_timeout_ms) |ms| {
                self.shutdown_deadline_ns = now + msToNs(ms);
            }
        }
    }

    /// Finish the graceful-shutdown sequence: close the transport (if any)
    /// and fire the shutdown callback. Public so cross-module question
    /// bookkeeping (return orchestration) can complete a drain; not
    /// intended for direct application use.
    pub fn completeShutdown(self: *Peer) void {
        self.assertThreadAffinity();
        if (self.transport.ctx) |transport_ctx| {
            // Close transport if attached and not already closing.
            if (self.transport.close) |close_fn| {
                if (self.transport.is_closing) |is_closing_fn| {
                    if (!is_closing_fn(transport_ctx)) {
                        close_fn(transport_ctx);
                    }
                } else {
                    close_fn(transport_ctx);
                }
            }
        }
        if (self.shutdown_callback) |cb| {
            self.shutdown_callback = null;
            cb(self);
        }
    }

    /// Cancel an outstanding outbound question.
    ///
    /// The local callback is delivered an exception Return carrying
    /// `reason` immediately, and a Finish with `releaseResultCaps` is sent
    /// to the remote. Per the Cap'n Proto RPC spec the remote still sends
    /// exactly one Return for the question (possibly `canceled`); the
    /// question entry stays in the table, marked cancelled, so that Return
    /// is absorbed silently when it arrives. Loopback questions complete
    /// locally and are removed outright.
    pub fn cancelQuestion(self: *Peer, question_id: u32, reason: []const u8) !void {
        return self.cancelQuestionTyped(question_id, reason, .failed);
    }

    /// `cancelQuestion` carrying an explicit `Exception.Type` for the locally
    /// synthesized exception the caller observes.
    pub fn cancelQuestionTyped(
        self: *Peer,
        question_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        self.assertThreadAffinity();
        const entry = self.questions.getPtr(question_id) orelse return error.UnknownQuestion;
        if (entry.cancelled) return;
        const question = entry.*;

        if (question.is_loopback) {
            _ = self.loopback_questions.remove(question_id);
            self.removeQuestion(question_id);
            try self.deliverLocalException(question, question_id, reason, ex_type);
            return;
        }

        // Delivery transfers ctx ownership to the callback; drop the
        // undelivered-cleanup hook so peer deinit cannot double-free.
        entry.cancelled = true;
        entry.deadline_ns = null;
        entry.deinit_ctx = null;

        // Tell the remote we no longer want the answer. A send failure is
        // tolerated: the local caller still observes the exception, and
        // transport teardown reconciles remote state.
        peer_outbound_control.sendFinishWithFlagsViaSendFrame(
            Peer,
            self,
            question_id,
            true,
            false,
            Peer.sendFrameControl,
        ) catch |err| {
            log.debug("cancel finish send failed for question {}: {}", .{ question_id, err });
        };

        try self.deliverLocalException(question, question_id, reason, ex_type);
    }

    /// Cancel every question whose deadline has passed, and enforce the
    /// shutdown drain bound. Returns the number of questions cancelled.
    ///
    /// Call this periodically — typically from a transport tick (see
    /// `Connection.Options.tick_interval_ms`) or a test harness. A peer
    /// without a clock returns 0 immediately.
    pub fn checkDeadlines(self: *Peer) usize {
        self.assertThreadAffinity();
        const now = self.clockNow() orelse return 0;

        var expired: std.ArrayList(u32) = .empty;
        defer expired.deinit(self.allocator);
        var it = self.questions.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.cancelled) continue;
            const deadline = kv.value_ptr.deadline_ns orelse continue;
            if (now >= deadline) expired.append(self.allocator, kv.key_ptr.*) catch break;
        }

        var cancelled: usize = 0;
        for (expired.items) |question_id| {
            events.emitTimeout(self.observer, .peer, .unknown, .call_deadline, question_id);
            self.cancelQuestionTyped(question_id, deadline_reason, .overloaded) catch |err| {
                log.debug("deadline cancel failed for question {}: {}", .{ question_id, err });
                continue;
            };
            cancelled += 1;
        }

        // Parked third-party-await questions are not in the questions map, so
        // the loop above misses them; sweep expired ones so they cannot escape
        // deadline enforcement.
        cancelled += self.sweepThirdPartyAwaits(true);

        if (self.is_shutting_down) {
            if (self.shutdown_deadline_ns) |drain_deadline| {
                if (now >= drain_deadline and self.questions.count() != 0) {
                    self.shutdown_deadline_ns = null;
                    events.emitTimeout(self.observer, .peer, .unknown, .shutdown_drain, null);
                    cancelled += self.forceCancelAllQuestions(shutdown_reason, .disconnected);
                }
            }
        }
        return cancelled;
    }

    /// Synthesize an exception Return for `question_id` and deliver it to
    /// the question's callback, exactly as if the remote had answered.
    ///
    /// Ctx ownership: once the callback runs it owns `question.ctx`
    /// (generated callbacks destroy it unconditionally). If synthesis fails
    /// before the callback could run, the ctx is freed here via
    /// `question.deinit_ctx` — callers have already removed the entry from
    /// the questions map (or dropped its cleanup hook), so nothing else can.
    fn deliverLocalException(
        self: *Peer,
        question: Question,
        question_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        var callback_ran = false;
        errdefer if (!callback_ran) {
            if (question.deinit_ctx) |deinit_ctx| deinit_ctx(self.allocator, question.ctx);
        };
        const frame = try peer_return_frames.buildReturnExceptionFrame(self.allocator, question_id, reason, ex_type);
        defer self.allocator.free(frame);
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        const ret = try decoded.asReturn();
        var inbound_caps = try cap_table.InboundCapTable.init(self.allocator, null, &self.caps);
        defer inbound_caps.deinit();
        callback_ran = true;
        question.on_return(question.ctx, self, ret, &inbound_caps) catch |err| {
            if (err == error.OutOfMemory) return error.OutOfMemory;
            log.debug("cancel exception callback error for question {}: {}", .{ question_id, err });
        };
    }

    /// Remove and cancel every outstanding question (drain-bound
    /// enforcement). Unlike `cancelQuestion` this does not keep entries
    /// for late Returns — the transport is about to close.
    fn forceCancelAllQuestions(self: *Peer, reason: []const u8, ex_type: protocol.ExceptionType) usize {
        var ids: std.ArrayList(u32) = .empty;
        defer ids.deinit(self.allocator);
        var it = self.questions.keyIterator();
        while (it.next()) |key| ids.append(self.allocator, key.*) catch break;

        var cancelled: usize = 0;
        for (ids.items) |question_id| {
            const removed = self.questions.fetchRemove(question_id) orelse continue;
            const question = removed.value;
            _ = self.loopback_questions.remove(question_id);
            // No wire Return will consume this question's param-export
            // record; free it without spending the refs (transport teardown
            // reconciles export state, as before the record existed).
            self.freeQuestionParamExports(question_id);
            if (question.cancelled) continue; // exception already delivered
            if (!question.is_loopback) {
                peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                    Peer,
                    self,
                    question_id,
                    true,
                    false,
                    Peer.sendFrameControl,
                ) catch |err| {
                    log.debug("drain finish send failed for question {}: {}", .{ question_id, err });
                };
            }
            self.deliverLocalException(question, question_id, reason, ex_type) catch |err| {
                log.debug("drain exception delivery failed for question {}: {}", .{ question_id, err });
            };
            cancelled += 1;
        }
        cancelled += self.sweepThirdPartyAwaits(false);
        if (self.is_shutting_down and !self.in_deinit and self.questions.count() == 0) {
            self.completeShutdown();
        }
        return cancelled;
    }

    /// Remove parked third-party-await questions, freeing each question's heap
    /// ctx (via deinit_ctx) and the owned map key. With `only_expired` true,
    /// only deadline-expired awaits are removed; otherwise all are. This bounds
    /// the memory a peer can pin by answering questions with awaitFromThirdParty
    /// and never completing the handoff — otherwise those contexts escape both
    /// deadline enforcement and the shutdown drain entirely.
    fn sweepThirdPartyAwaits(self: *Peer, only_expired: bool) usize {
        // Null cutoff means "sweep everything" (shutdown drain); a non-null
        // cutoff sweeps only entries whose deadline has passed.
        const cutoff: ?i64 = if (only_expired) (self.clockNow() orelse return 0) else null;

        var keys: std.ArrayList([]const u8) = .empty;
        defer keys.deinit(self.allocator);
        {
            var it = self.pending_third_party_awaits.iterator();
            while (it.next()) |kv| {
                if (cutoff) |now| {
                    const deadline = kv.value_ptr.question.deadline_ns orelse continue;
                    if (now < deadline) continue;
                }
                keys.append(self.allocator, kv.key_ptr.*) catch break;
            }
        }

        var swept: usize = 0;
        for (keys.items) |key| {
            const removed = self.pending_third_party_awaits.fetchRemove(key) orelse continue;
            if (removed.value.question.deinit_ctx) |deinit_ctx| {
                deinit_ctx(self.allocator, removed.value.question.ctx);
            }
            self.allocator.free(removed.key);
            if (only_expired) {
                events.emitTimeout(self.observer, .peer, .unknown, .call_deadline, removed.value.question_id);
            }
            swept += 1;
        }
        return swept;
    }

    /// Resolve a previously exported promise to point at a concrete export.
    pub fn resolvePromiseExportToExport(self: *Peer, promise_id: u32, export_id: u32) !void {
        self.assertThreadAffinity();
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
        if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
        if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;
        if (!self.exports.contains(export_id)) return error.UnknownExport;

        const descriptor = protocol.CapDescriptor{
            .tag = if (self.caps.isExportPromise(export_id)) .senderPromise else .senderHosted,
            .id = export_id,
            .promised_answer = null,
            .attached_fd = null,
        };

        // The Resolve's cap descriptor hands the remote a reference to the
        // target export, exactly like a call/return descriptor would: the
        // remote spends it with a wire Release once it drops the resolved cap
        // (see resolveDescriptor's noteImport in caps/inbound.zig), or — if
        // it does not implement Resolve — echoes the message back inside an
        // Unimplemented and handleUnimplementedResolve releases it there.
        // Rolled back only while the send can still fail: past a successful
        // send the remote holds the descriptor and owns the ref (matching
        // handleBootstrap's pattern).
        try self.noteExportRef(export_id);
        var rollback_wire_ref = true;
        errdefer if (rollback_wire_ref) self.rollbackExportRef(export_id);

        // Pin the resolution target for the promise export's own lifetime. Once
        // the promise routes inbound calls at `export_id` (via
        // replayResolvedPromiseExport / handleResolvedExportedCall below), an
        // inbound Release that zeroes the target's wire ref_count — or the
        // echoed-Unimplemented cleanup above, which also spends a wire ref —
        // must not destroy the target out from under the still-live promise
        // export. This promise-held reference is separate from the wire ref
        // taken above (which the remote owns and spends): it is released only
        // when the promise export is itself destroyed (see
        // finalizeExportRelease / releasePromiseHeldCap). Rolled back only
        // while the send can still fail; past a committed resolution the
        // promise owns it and its destruction releases it.
        try self.notePromiseExportRef(export_id);
        var rollback_promise_ref = true;
        errdefer if (rollback_promise_ref) self.rollbackPromiseExportRef(export_id);

        try peer_outbound_control.sendResolveCapViaSendFrame(
            Peer,
            self,
            promise_id,
            descriptor,
            Peer.sendFrame,
        );
        rollback_wire_ref = false;

        promise_entry.value_ptr.resolved = .{ .exported = .{ .id = export_id } };
        // The resolution is committed: the promise export now routes to
        // `export_id` and its eventual destruction releases this promise-held
        // ref, so keep it even if replay below fails.
        rollback_promise_ref = false;
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);
    }

    /// Resolve a previously exported promise to a capability the *remote* peer
    /// hosts (one we hold as an import). This is the "reflected capability"
    /// case: the promise resolves to a cap reached by a different path than the
    /// promise itself, so a conformant remote will run the embargo/Disembargo
    /// handshake against it. Mirror of `resolvePromiseExportToExport`, but the
    /// resolution target lives in the import table, not the export table.
    pub fn resolvePromiseExportToImport(self: *Peer, promise_id: u32, import_id: u32) !void {
        self.assertThreadAffinity();
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
        if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
        if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;
        if (!self.caps.hasImport(import_id)) return error.UnknownImport;

        // `receiverHosted` names the REMOTE's own export (our import), so — unlike
        // resolvePromiseExportToExport's senderHosted/senderPromise descriptor —
        // this does NOT hand the remote a fresh wire reference on one of OUR
        // exports. The remote already owns `import_id`; it will not spend a
        // Release against us on account of receiving it in the Resolve. So we
        // take NO outbound wire ref here.
        const descriptor = protocol.CapDescriptor{
            .tag = .receiverHosted,
            .id = import_id,
            .promised_answer = null,
            .attached_fd = null,
        };

        // Pin the resolution target (our import) for the promise export's own
        // lifetime. Once the promise routes inbound calls back out to
        // `import_id` (via replayResolvedPromiseExport / forwardResolvedCall
        // below), the host handler that originally received this import as a
        // call/return cap may drop its own wire reference before the promise
        // export is gone — an inbound path that zeroes the import's wire
        // `ref_count` must not remove the import entry out from under the
        // still-live promise export. This is a purely LOCAL pin
        // (`promise_ref_count`), NOT a `noteImport`: a `receiverHosted`
        // descriptor creates no new wire reference, so dropping this pin must
        // never send a wire Release. It is released when the promise export is
        // destroyed (see finalizeExportRelease's import-target cascade, which
        // calls releasePromiseImportRef — local, no frame). Rolled back only
        // while the send can still fail; past a committed resolution the promise
        // owns it.
        try self.caps.notePromiseImportRef(import_id);
        var rollback_import_ref = true;
        errdefer if (rollback_import_ref) {
            _ = self.caps.releasePromiseImportRef(import_id);
        };

        try peer_outbound_control.sendResolveCapViaSendFrame(
            Peer,
            self,
            promise_id,
            descriptor,
            Peer.sendFrame,
        );

        promise_entry.value_ptr.resolved = .{ .imported = .{ .id = import_id } };
        // The resolution is committed: the promise export now routes to
        // `import_id` and its eventual destruction drops this promise-held pin
        // (no wire Release), so keep it even if replay below fails.
        rollback_import_ref = false;
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);
    }

    /// Resolve a previously exported promise to a capability hosted by a THIRD
    /// vat (Level-3 three-party handoff ORIGINATION). Mirror of
    /// `resolvePromiseExportToImport`, but the resolution target lives on neither
    /// this peer's export table nor its import table — it is hosted by VatC, so
    /// the Resolve carries a `thirdPartyHosted{ id = ThirdPartyToContact, vineId }`
    /// descriptor and this vat originates the paired `Provide` to VatC.
    ///
    /// `self` is the host-of-recipient connection (VatB↔VatA), where the promise
    /// export lives, the vine is minted, and the Resolve is sent. `provide_peer`
    /// is the host-of-provided-cap connection (VatB↔VatC), where the held-open
    /// `Provide` is sent naming `provided_target` (VatC's Carol) and `recipient`
    /// (the `ThirdPartyToAwait`). `contact_payload` is the serialized
    /// `ThirdPartyToContact` VatA will redeem via its VatNetwork to reach VatC.
    ///
    /// This slice sends the Resolve with `embargo = null` semantics: there is no
    /// in-flight-promise embargo/disembargo during the handoff (Phase 4).
    ///
    /// Returns the `ProvideHandle` (Provide question id + vine id). The vine's
    /// wire reference is held by the emitted `thirdPartyHosted` descriptor; when
    /// VatA releases the vine (after picking up Carol directly), `handleRelease`
    /// Finishes the Provide on `provide_peer`.
    pub fn resolvePromiseExportToThirdParty(
        self: *Peer,
        promise_id: u32,
        provide_peer: *Peer,
        provided_target: protocol.MessageTarget,
        recipient: message.AnyPointerReader,
        contact_payload: []const u8,
    ) !ProvideHandle {
        self.assertThreadAffinity();
        provide_peer.assertThreadAffinity();
        if (self.is_shutting_down or provide_peer.is_shutting_down) return error.PeerShuttingDown;

        {
            // Validate the promise export up front, but do NOT hold the entry
            // pointer across `sendProvide`: it mints the vine into `self.exports`,
            // which can rehash and invalidate any captured entry pointer. Re-fetch
            // after origination (below) before mutating the promise entry.
            const promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
            if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
            if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;
        }

        // Originate the handoff: mint the vine on `self`, mark it
        // third-party-hosted with the contact, couple vine → held-open Provide
        // on `provide_peer`, and send the Provide to VatC. `sendProvide` rolls
        // back all of that on any failure before it returns.
        const handle = try provide_peer.sendProvide(provided_target, recipient, self, contact_payload);
        // Undo the whole origination if the Resolve emission below fails: destroy
        // the vine export + its handoff mark, drop the coupling, and Finish the
        // Provide we just sent so VatC does not leak the provision.
        var origination_owned = true;
        errdefer if (origination_owned) {
            if (self.outbound_provides.getPtr(handle.vine_id)) |op| op.deinitStash(self.allocator);
            _ = self.outbound_provides.remove(handle.vine_id);
            self.caps.clearThirdPartyHosted(handle.vine_id);
            self.releaseVineExport(handle.vine_id);
            // Drop the liveness back-link `sendProvide` registered on the
            // provide_peer (BUG #55) so unwinding leaves no stale coupling.
            provide_peer.deregisterCoupledVine(self, handle.vine_id);
            self.finishOriginatedProvide(provide_peer, handle.question_id);
        };

        // Build the Resolve's resolved descriptor: thirdPartyHosted{ id, vineId }.
        // The recipient takes a wire reference on the VINE (the handoff anchor),
        // exactly as the payload emitter does (caps/outbound.zig), so account for
        // that reference here — buildResolveCap does NOT run the outbound-cap
        // callback that would otherwise note it.
        var contact_msg = try message.Message.initUnvalidated(self.allocator, contact_payload);
        defer contact_msg.deinit();
        const contact = try contact_msg.getRootAnyPointer();
        const descriptor = protocol.CapDescriptor{
            .tag = .thirdPartyHosted,
            .id = null,
            .promised_answer = null,
            .third_party = .{ .id = contact, .vine_id = handle.vine_id },
            .attached_fd = null,
        };

        try self.noteExportRef(handle.vine_id);
        var rollback_wire_ref = true;
        errdefer if (rollback_wire_ref) self.rollbackExportRef(handle.vine_id);

        // Record the resolving promise export on the coupling BEFORE emitting the
        // Resolve. Two reasons: (1) the vine teardown clears the promise's
        // resolution target (see handleRelease); (2) — critically for Phase 4 —
        // emitting the Resolve can synchronously drive the recipient's auto-pickup
        // to send a `context.accept` Disembargo straight back to us on the promise
        // path (single-threaded loopback), and `handleAcceptDisembargo` must find
        // this coupling by `resolved_promise_export_id` to forward the Disembargo
        // on to the capability host. If we set it only after the Resolve returns,
        // a synchronous Disembargo would find no coupling and never reach VatC.
        if (self.outbound_provides.getPtr(handle.vine_id)) |op| {
            op.resolved_promise_export_id = promise_id;
        }
        errdefer if (origination_owned) {
            if (self.outbound_provides.getPtr(handle.vine_id)) |op| {
                op.resolved_promise_export_id = null;
            }
        };

        try peer_outbound_control.sendResolveCapViaSendFrame(
            Peer,
            self,
            promise_id,
            descriptor,
            Peer.sendFrame,
        );
        rollback_wire_ref = false;
        origination_owned = false;

        // The Resolve is on the wire: the recipient's synchronous auto-pickup
        // may already have sent back a `context.accept` Disembargo, which
        // `handleAcceptDisembargo` STASHED on the coupling (e-order: it must
        // not reach the host before the parked-call replay below). From here,
        // EVERY exit — including the two error returns below — must flush the
        // stash, or the recipient's embargoed Accept hangs forever.
        errdefer self.flushStashedAcceptDisembargo(handle.vine_id);

        // Re-fetch the promise entry: the vine insert above may have rehashed
        // `self.exports`, invalidating the pointer captured during validation.
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;

        // Route pipelined calls that arrive on the promise export through the
        // vine. With `provided_import_id` recorded on the coupling (an importedCap
        // provided target), the replay FORWARDS each parked call to VatC over the
        // B↔C connection (issue #56, `maybeForwardVineCall`); only a coupling that
        // cannot forward (no import target, or a torn-down provide_peer) falls
        // back to the vine's rejecting handler. NO promise-held pin is taken on
        // the vine: its lifetime is driven solely by VatA's wire Release (the
        // handoff-completion signal), which must destroy it to Finish the Provide.
        // A pin would keep the vine alive past that Release and stall the Finish.
        promise_entry.value_ptr.resolved = .{ .exported = .{ .id = handle.vine_id } };
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);

        // Parked pre-resolution calls are now on their way to the host; the
        // recipient's Disembargo (if one was stashed during the Resolve send)
        // may follow them. This also arms immediate forwarding for any later
        // Disembargo on this coupling.
        self.flushStashedAcceptDisembargo(handle.vine_id);

        log.debug("resolved promise export {} to third party via vine {}", .{ promise_id, handle.vine_id });
        return handle;
    }

    /// Resolve a previously exported promise to an exception.
    pub fn resolvePromiseExportToException(self: *Peer, promise_id: u32, reason: []const u8) !void {
        self.assertThreadAffinity();
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
        if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
        if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;

        try peer_outbound_control.sendResolveExceptionViaSendFrame(
            Peer,
            self,
            promise_id,
            reason,
            Peer.sendFrame,
        );
        promise_entry.value_ptr.resolved = .none;
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, .none);
    }

    /// Register a cleanup callback for a question's heap-allocated ctx,
    /// invoked by `Peer.deinit` if the question is still outstanding (never
    /// answered — e.g. the connection dropped without a full shutdown drain).
    /// Also clears `restore_on_return_error`, since the callback frees the ctx
    /// on the normal return path: restoring the question after a callback
    /// error would leave `deinit_ctx` pointing at freed memory. No-op if the
    /// question id is unknown. Intended for generated client stubs and other
    /// external callers that cannot reach the questions table directly; it
    /// mirrors what `sendSave`/`sendRestore` do inline.
    pub fn setQuestionDeinitCtx(self: *Peer, question_id: u32, deinit_ctx: state.QuestionDeinitCtxFn) void {
        self.assertThreadAffinity();
        if (self.questions.getPtr(question_id)) |q| {
            q.deinit_ctx = deinit_ctx;
            q.restore_on_return_error = false;
        }
    }

    /// Send an RPC call to an imported capability, returning the question ID.
    ///
    /// Error set is intentionally left open (inferred/`anyerror`). Every send
    /// path in this family synchronously invokes the caller-supplied `build`
    /// closure (`CallBuildFn`, `try build_fn(ctx, &call)` in
    /// `peer_call_sender`), whose `anyerror` propagates out. Narrowing below
    /// `anyerror` would require changing the `CallBuildFn` contract, so — like
    /// the user-callback typedefs above — this stays open. (Applies to all four
    /// `sendCall*` entry points.)
    pub fn sendCall(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        log.debug("sendCall target_id={} interface_id=0x{x} method_id={}", .{ target_id, interface_id, method_id });
        if (self.resolved_imports.get(target_id)) |entry| {
            if (!entry.embargoed and entry.cap != null) {
                return self.sendCallResolved(entry.cap.?, interface_id, method_id, ctx, build, on_return);
            }
        }

        const question_id = try peer_call_sender.sendCallToImport(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            rollbackOutboundCap,
            self,
            target_id,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            Peer.allocateQuestion,
            Peer.removeQuestion,
            Peer.recordQuestionParamExports,
            Peer.sendBuilder,
        );
        // Record the promise-import target on the question so the Level-3
        // recipient auto-pickup can observe an in-flight pipelined call against
        // this (as-yet unresolved) import — the spec condition for embargoing
        // the handoff Accept (rpc.capnp:885-888). Only calls to imports NOT yet
        // in `resolved_imports` reach here (resolved caps take the fast path
        // above), so this marks exactly the still-in-flight-promise targets. The
        // mark is dropped implicitly when the question leaves the table.
        if (self.questions.getPtr(question_id)) |q| {
            q.target_promise_import = target_id;
        }
        return question_id;
    }

    /// Send a forwarded L3 vine call to a plain import (issue #56). Mirrors the
    /// import path of `sendCall` but allocates the question with
    /// `restore_on_return_error = false` (`allocateQuestionNoRestore`): the return
    /// callback (`forwardVineReturn`) frees the relay ctx, so a restore after a
    /// post-callback error would re-reference freed memory. It deliberately skips
    /// `sendCall`'s resolved-import fast path and the `target_promise_import` mark:
    /// the forward targets B's own (non-promise, non-embargoed) import of VatC's
    /// cap and must not be treated as an in-flight promise for auto-pickup.
    fn sendForwardedVineCall(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        return peer_call_sender.sendCallToImport(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            rollbackOutboundCap,
            self,
            target_id,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            Peer.allocateQuestionNoRestore,
            Peer.removeQuestion,
            Peer.recordQuestionParamExports,
            Peer.sendBuilder,
        );
    }

    /// Send a call to a resolved (non-promise) capability. Dispatches to the
    /// appropriate path based on the resolved cap type (imported, exported, or promised).
    pub fn sendCallResolved(
        self: *Peer,
        target: cap_table.ResolvedCap,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        return switch (target) {
            .imported => |cap| peer_call_sender.sendCallToImport(
                Peer,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                onOutboundCap,
                rollbackOutboundCap,
                self,
                cap.id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                Peer.allocateQuestion,
                Peer.removeQuestion,
                Peer.recordQuestionParamExports,
                Peer.sendBuilder,
            ),
            .promised => |promised| self.sendCallPromised(promised, interface_id, method_id, ctx, build, on_return),
            .exported => |cap| blk: {
                try ensureCountLimit(false, self.loopback_questions.count(), self.limits.max_loopback_questions);
                break :blk peer_call_sender.sendCallToExport(
                    Peer,
                    Question,
                    CallBuildFn,
                    QuestionCallback,
                    self.allocator,
                    &self.caps,
                    self,
                    onOutboundCap,
                    rollbackOutboundCap,
                    self,
                    &self.questions,
                    &self.loopback_questions,
                    cap.id,
                    interface_id,
                    method_id,
                    ctx,
                    build,
                    on_return,
                    Peer.allocateQuestion,
                    Peer.handleFrame,
                );
            },
            .none => error.CapabilityUnavailable,
        };
    }

    pub fn sendCallPromised(
        self: *Peer,
        promised: protocol.PromisedAnswer,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        return peer_call_sender.sendCallPromised(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            rollbackOutboundCap,
            self,
            promised,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            Peer.allocateQuestion,
            Peer.removeQuestion,
            Peer.recordQuestionParamExports,
            Peer.sendBuilder,
        );
    }

    /// Like `sendCallPromised` but takes a question ID and a slice of
    /// `PromisedAnswerOp` directly, avoiding the need to build a reader-backed
    /// `PromisedAnswer`. Used by generated pipeline code.
    pub fn sendCallPromisedWithOps(
        self: *Peer,
        question_id_target: u32,
        ops: []const protocol.PromisedAnswerOp,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        self.assertThreadAffinity();
        if (self.is_shutting_down) return error.PeerShuttingDown;
        return peer_call_sender.sendCallPromisedWithOps(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            rollbackOutboundCap,
            self,
            question_id_target,
            ops,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            Peer.allocateQuestion,
            Peer.removeQuestion,
            Peer.recordQuestionParamExports,
            Peer.sendBuilder,
        );
    }

    fn forwardResolvedCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        target: cap_table.ResolvedCap,
        mode: ForwardReturnMode,
    ) !void {
        const ctx = try self.allocator.create(ForwardCallContext);
        ctx.* = .{
            .peer = self,
            .payload = call.params,
            .inbound_caps = try inbound_caps.clone(),
            .send_results_to = .caller,
            .send_results_to_third_party_payload = null,
            .answer_id = call.question_id,
            .mode = mode,
        };
        var ctx_owned = true;
        errdefer if (ctx_owned) ForwardCallContext.deinit(self.allocator, ctx);

        const forwarded_plan = try peer_forward_orchestration.buildForwardCallPlan(
            Peer,
            self,
            mode,
            call.send_results_to.third_party,
            peer_third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
        );
        ctx.send_results_to = forwarded_plan.send_results_to;
        ctx.send_results_to_third_party_payload = forwarded_plan.send_results_to_third_party_payload;

        const forwarded_question_id = try self.sendCallResolved(
            target,
            call.interface_id,
            call.method_id,
            ctx,
            buildForwardedCall,
            onForwardedReturn,
        );
        // Once the question is registered, its deinit_ctx handles cleanup
        // on peer.deinit(); the local errdefer must no longer fire.
        if (self.questions.getPtr(forwarded_question_id)) |q| {
            q.deinit_ctx = ForwardCallContext.deinit;
            q.restore_on_return_error = false;
        }
        ctx_owned = false;
        const completion = try peer_forward_orchestration.finishForwardResolvedCall(
            Question,
            mode,
            call.question_id,
            forwarded_question_id,
            &self.forwarded_questions,
            &self.forwarded_tail_questions,
            &self.questions,
        );
        if (completion.send_take_from_other_question) {
            try self.sendReturnTakeFromOtherQuestion(call.question_id, forwarded_question_id);
        }
    }

    /// Send the tail-Finish for a forwarded question (relayed from the upstream
    /// caller's Finish) and neutralize that forwarded question so any LATE
    /// forwarded return is absorbed cleanly rather than emitting a spurious
    /// Return to the now-finished upstream question.
    ///
    /// Both forwarding modes that hold the forwarded question open until the
    /// upstream Finish route their forwarded Finish through here
    /// (`forwarded_tail_questions`, see finishForwardResolvedCall):
    ///   - `.sent_elsewhere` (spec tail-call): the late return is a bare
    ///     `resultsSentElsewhere` marker — harmless either way.
    ///   - `.translate_to_caller` (W1 reflected loopback): the late return
    ///     carries REAL results that `onForwardedReturn` would otherwise
    ///     translate onto the upstream answer — but the upstream caller already
    ///     Finished, so that Return would be spurious. Neutralizing here frees
    ///     the ForwardCallContext, drops the forwarded_questions mapping, and
    ///     marks the question cancelled so `handleReturn` absorbs the late
    ///     return via its cancelled-question path without emitting or leaking.
    ///
    /// In the common (non-race) order the forwarded return already arrived and
    /// removed the forwarded question before this Finish relay fires, so the
    /// neutralize step is a no-op beyond sending the Finish.
    fn sendTailFinishAndNeutralize(self: *Peer, tail_question_id: u32, release_result_caps: bool) !void {
        try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
            Peer,
            self,
            tail_question_id,
            release_result_caps,
            false,
            Peer.sendFrameControl,
        );
        if (self.questions.getPtr(tail_question_id)) |question| {
            if (question.deinit_ctx) |deinit_ctx| {
                deinit_ctx(self.allocator, question.ctx);
                question.deinit_ctx = null;
            }
            question.cancelled = true;
        }
        _ = self.forwarded_questions.remove(tail_question_id);
    }

    fn neutralizeJoinRelayQuestion(self: *Peer, question_id: u32) void {
        if (self.questions.getPtr(question_id)) |question| {
            if (question.deinit_ctx) |deinit_ctx| {
                deinit_ctx(self.allocator, question.ctx);
                question.deinit_ctx = null;
            }
            question.cancelled = true;
        }
    }

    fn sendJoinRelayFinishAndNeutralize(self: *Peer, question_id: u32, release_result_caps: bool) !void {
        try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
            Peer,
            self,
            question_id,
            release_result_caps,
            false,
            Peer.sendFrameControl,
        );
        self.neutralizeJoinRelayQuestion(question_id);
    }

    fn buildForwardedCall(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *const ForwardCallContext = castCtx(*const ForwardCallContext, ctx_ptr);
        try peer_third_party.applyForwardedCallSendResults(
            Peer,
            ctx.peer,
            call_builder,
            ctx.send_results_to,
            ctx.send_results_to_third_party_payload,
            peer_third_party.setForwardedCallThirdPartyFromPayloadForPeerFn(Peer),
        );

        const payload_builder = try call_builder.payloadTyped();
        try ctx.peer.clonePayloadWithRemappedCaps(
            call_builder.call.builder,
            payload_builder,
            ctx.payload,
            &ctx.inbound_caps,
        );
    }

    fn onForwardedReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *ForwardCallContext = castCtx(*ForwardCallContext, ctx_ptr);
        defer {
            ctx.inbound_caps.deinit();
            if (ctx.send_results_to_third_party_payload) |payload| peer.allocator.free(payload);
            peer.allocator.destroy(ctx);
        }
        peer_forward_orchestration.removeForwardedQuestionForPeer(Peer, peer, ret.answer_id);
        try peer_forward_return_callbacks.handleForwardedReturnWithPeerCallbacks(
            Peer,
            cap_table.InboundCapTable,
            ForwardReturnBuildContext,
            buildForwardedReturn,
            peer,
            peer_forward_orchestration.toControlMode(ctx.mode),
            ctx.answer_id,
            ret,
            inbound_caps,
            peer_third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            peer_finish.freeOwnedFrameForPeerFn(Peer),
            ctx.send_results_to_third_party_payload,
            sendReturnTag,
            peer_forward_orchestration.lookupForwardedQuestionForPeerFn(Peer),
            sendReturnTakeFromOtherQuestion,
            sendReturnAcceptFromThirdParty,
        );
    }

    fn buildForwardedReturn(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) anyerror!void {
        const ctx: *const ForwardReturnBuildContext = castCtx(*const ForwardReturnBuildContext, ctx_ptr);
        const payload_builder = try ret_builder.payloadTyped();
        try ctx.peer.clonePayloadWithRemappedCaps(
            ret_builder.ret.builder,
            payload_builder,
            ctx.payload,
            ctx.inbound_caps,
        );
    }

    fn clonePayloadWithRemappedCaps(
        self: *Peer,
        builder: *message.MessageBuilder,
        payload_builder: protocol.PayloadBuilder,
        source: protocol.Payload,
        inbound_caps: *const cap_table.InboundCapTable,
    ) !void {
        try payload_remap.clonePayloadWithRemappedCaps(
            Peer,
            self.allocator,
            self,
            builder,
            payload_builder,
            source,
            inbound_caps,
            mapInboundCapForForward,
        );
    }

    fn mapInboundCapForForward(
        self: *Peer,
        inbound_caps: *const cap_table.InboundCapTable,
        cap_index: u32,
    ) !?payload_remap.RemappedCap {
        const entry = try inbound_caps.get(cap_index);
        // Re-encode each forwarded cap under its KNOWN origin: an inbound import
        // (a cap the far peer hosts) forwards back as receiverHosted, an inbound
        // export (one of our own) as senderHosted, and a promised answer as a
        // freshly-noted receiverAnswer. The origin travels with the id so the
        // outbound encoder never re-guesses the id space — which is ambiguous
        // when a remote-forced export/import id collision exists.
        const descriptors = cap_table.descriptors;
        return switch (entry) {
            .none => null,
            .imported => |cap| .{ .origin_code = descriptors.originCodeForTag(.receiverHosted), .cap_id = cap.id },
            // An inbound .exported cap is one of our own exports; it may be an
            // unresolved export PROMISE (senderPromise), which must be preserved
            // so the peer still expects the later Resolve. classifyCap made this
            // distinction (isExportPromise before hasExport); do the same here.
            .exported => |cap| .{ .origin_code = descriptors.originCodeForTag(self.exportedCapTag(cap.id)), .cap_id = cap.id },
            .promised => |promised| .{
                .origin_code = descriptors.originCodeForTag(.receiverAnswer),
                .cap_id = try self.caps.noteReceiverAnswer(promised),
            },
        };
    }

    /// The outbound descriptor variant for one of our own exports: senderPromise
    /// for an unresolved export promise, senderHosted otherwise. Mirrors
    /// classifyCap's isExportPromise-before-hasExport ordering so the
    /// origin-carrying forward/provide paths match the app-authored path.
    fn exportedCapTag(self: *Peer, cap_id: u32) protocol.CapDescriptorTag {
        return if (self.caps.isExportPromise(cap_id)) .senderPromise else .senderHosted;
    }

    fn addCrossPeerProxyExport(
        self: *Peer,
        source_peer: *Peer,
        target: cap_table.ResolvedCap,
        release_source_import_id: ?u32,
        release_source_export_pin_id: ?u32,
    ) !u32 {
        self.assertThreadAffinity();
        // OWNERSHIP: both source-peer leases (the retained import ref and the
        // handoff export pin) transfer to this call — on success the ctx's
        // deinit releases each exactly once; on failure the errdefer below
        // does. The caller must NEVER roll either back after invoking.
        var proxy_ctx: ?*CrossPeerProxyContext = null;
        errdefer {
            if (proxy_ctx) |ctx| {
                CrossPeerProxyContext.deinit(self.allocator, ctx);
            } else {
                if (release_source_import_id) |import_id| {
                    source_peer.releaseImport(import_id, 1) catch |err| {
                        log.debug("cross-peer proxy: failed to release source import {} after allocation failure: {}", .{ import_id, err });
                    };
                }
                if (release_source_export_pin_id) |pin_id| {
                    source_peer.releaseHandoffHeldExport(pin_id);
                }
            }
        }

        const ctx = try self.allocator.create(CrossPeerProxyContext);
        proxy_ctx = ctx;
        ctx.* = .{
            .owner_peer = self,
            .source_peer = source_peer,
            .target = target,
            .release_source_import_id = release_source_import_id,
            .release_source_export_pin_id = release_source_export_pin_id,
        };

        const id = try self.addExportWithDeinit(
            .{ .ctx = ctx, .on_call = CrossPeerProxyContext.onCall },
            CrossPeerProxyContext.deinit,
        );
        ctx.export_id = id;
        source_peer.registerCrossPeerProxy(self, id) catch |err| {
            proxy_ctx = null;
            self.destroyUnreferencedProxyExport(id);
            return err;
        };
        proxy_ctx = null;
        return id;
    }

    fn destroyUnreferencedProxyExport(self: *Peer, id: u32) void {
        self.destroyUnreferencedExport(id);
    }

    fn destroyUnreferencedExport(self: *Peer, id: u32) void {
        const entry = self.exports.get(id) orelse return;
        if (entry.ref_count != 0 or entry.answer_ref_count != 0 or entry.promise_ref_count != 0 or entry.handoff_ref_count != 0) return;

        const removed = self.exports.fetchRemove(id) orelse return;
        self.caps.clearExport(id);
        if (removed.value.deinit_ctx) |deinit_ctx| {
            if (removed.value.handler) |handler| deinit_ctx(self.allocator, handler.ctx);
        }
    }

    fn clonePayloadAcrossPeers(
        self: *Peer,
        builder: *message.MessageBuilder,
        payload_builder: protocol.PayloadBuilder,
        source: protocol.Payload,
        inbound_peer: *Peer,
        inbound_caps: *cap_table.InboundCapTable,
        created_proxy_ids: *std.ArrayList(u32),
    ) !void {
        var map_ctx = CrossPeerCapMapContext.init(inbound_peer, self, inbound_caps, created_proxy_ids);
        defer map_ctx.deinit();
        try payload_remap.clonePayloadWithRemappedCaps(
            CrossPeerCapMapContext,
            self.allocator,
            &map_ctx,
            builder,
            payload_builder,
            source,
            inbound_caps,
            mapCrossPeerInboundCap,
        );
    }

    fn mapCrossPeerInboundCap(
        ctx: *CrossPeerCapMapContext,
        _: *const cap_table.InboundCapTable,
        cap_index: u32,
    ) !?payload_remap.RemappedCap {
        if (ctx.remapped_by_index.get(cap_index)) |proxy_id| {
            return .{
                .origin_code = cap_table.descriptors.originCodeForTag(.senderHosted),
                .cap_id = proxy_id,
            };
        }

        const entry = try ctx.inbound_caps.get(cap_index);
        if (entry == .none) return null;

        const release_source_import_id: ?u32 = switch (entry) {
            .imported => |cap| blk: {
                try ctx.inbound_caps.retainIndex(cap_index);
                break :blk cap.id;
            },
            else => null,
        };

        const proxy_id = try ctx.outbound_peer.addCrossPeerProxyExport(
            ctx.inbound_peer,
            entry,
            release_source_import_id,
            null,
        );
        errdefer ctx.outbound_peer.destroyUnreferencedProxyExport(proxy_id);
        try ctx.created_proxy_ids.append(ctx.outbound_peer.allocator, proxy_id);
        try ctx.remapped_by_index.put(cap_index, proxy_id);

        return .{
            .origin_code = cap_table.descriptors.originCodeForTag(.senderHosted),
            .cap_id = proxy_id,
        };
    }

    fn forwardCrossPeerProxyCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        forward_peer: *Peer,
        target: cap_table.ResolvedCap,
    ) !void {
        if (target == .none) {
            try self.sendReturnException(call.question_id, "cross-peer proxy target unavailable");
            return;
        }

        const relay = try forward_peer.allocator.create(CrossPeerProxyCallContext);
        relay.* = .{
            .recipient_peer = self,
            .recipient_answer_id = call.question_id,
            .forward_peer = forward_peer,
            .target = target,
            .source_params = call.params,
            .source_inbound_caps = @constCast(inbound_caps),
        };
        var relay_owned = true;
        errdefer if (relay_owned) CrossPeerProxyCallContext.deinit(forward_peer.allocator, relay);

        var forward_settled = false;
        relay.settled_flag = &forward_settled;

        const forwarded_question_id = forward_peer.sendCrossPeerProxyResolvedCall(
            target,
            call.interface_id,
            call.method_id,
            relay,
            buildCrossPeerProxyCall,
            onCrossPeerProxyReturn,
        ) catch |err| {
            if (forward_settled) {
                relay_owned = false;
                return;
            }
            relay_owned = false;
            CrossPeerProxyCallContext.deinit(forward_peer.allocator, relay);
            self.sendReturnException(call.question_id, @errorName(err)) catch |send_err| {
                log.debug("cross-peer proxy: failed to fail question {}: {}", .{ call.question_id, send_err });
            };
            return;
        };

        if (forward_settled) {
            relay_owned = false;
            return;
        }
        relay.param_proxies_committed = true;
        relay.settled_flag = null;
        if (forward_peer.questions.getPtr(forwarded_question_id)) |q| {
            q.deinit_ctx = CrossPeerProxyCallContext.deinit;
            q.restore_on_return_error = false;
        }
        relay_owned = false;
    }

    fn sendCrossPeerProxyResolvedCall(
        self: *Peer,
        target: cap_table.ResolvedCap,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return switch (target) {
            .imported => |cap| self.sendForwardedVineCall(cap.id, interface_id, method_id, ctx, build, on_return),
            .exported, .promised => self.sendCallResolved(target, interface_id, method_id, ctx, build, on_return),
            .none => error.CapabilityUnavailable,
        };
    }

    fn buildCrossPeerProxyCall(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *CrossPeerProxyCallContext = castCtx(*CrossPeerProxyCallContext, ctx_ptr);
        const payload_builder = try call_builder.payloadTyped();
        try ctx.forward_peer.clonePayloadAcrossPeers(
            call_builder.call.builder,
            payload_builder,
            ctx.source_params,
            ctx.recipient_peer,
            ctx.source_inbound_caps,
            &ctx.created_param_proxy_ids,
        );
    }

    fn onCrossPeerProxyReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *CrossPeerProxyCallContext = castCtx(*CrossPeerProxyCallContext, ctx_ptr);
        const recipient = ctx.recipient_peer;
        const answer_id = ctx.recipient_answer_id;
        const release_param_caps = ctx.created_param_proxy_ids.items.len == 0;
        if (ctx.settled_flag) |flag| flag.* = true;
        defer CrossPeerProxyCallContext.deinit(peer.allocator, ctx);

        relayReturnAcrossPeers(recipient, answer_id, peer, ret, inbound_caps, release_param_caps) catch |err| {
            log.debug("cross-peer proxy return relay failed for question {}: {}", .{ answer_id, err });
        };
    }

    fn relayReturnAcrossPeers(
        recipient: *Peer,
        answer_id: u32,
        source_peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
        release_param_caps: bool,
    ) !void {
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse {
                    try recipient.sendReturnException(answer_id, "cross-peer forwarded call: missing results");
                    return;
                };
                var results_ctx = CrossPeerReturnRelayContext{
                    .source_peer = source_peer,
                    .target_peer = recipient,
                    .source = payload,
                    .source_inbound_caps = @constCast(inbound_caps),
                    .release_param_caps = release_param_caps,
                };
                defer results_ctx.deinit(recipient.allocator);
                try recipient.sendReturnResults(answer_id, &results_ctx, buildCrossPeerReturnResults);
                results_ctx.result_proxies_committed = true;
            },
            .exception => {
                const reason = if (ret.exception) |e| e.reason else "cross-peer forwarded call failed";
                // Relay the origin's type verbatim: a `disconnected` upstream
                // must not reach the caller as a generic `failed`, or the
                // caller cannot tell a retryable loss from an application error.
                const ex_type = if (ret.exception) |e| e.kind() else protocol.ExceptionType.failed;
                try recipient.sendReturnExceptionTyped(answer_id, reason, ex_type);
            },
            else => {
                try recipient.sendReturnException(answer_id, "cross-peer forwarded call: unexpected return");
            },
        }
    }

    fn buildCrossPeerReturnResults(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) anyerror!void {
        const ctx: *CrossPeerReturnRelayContext = castCtx(*CrossPeerReturnRelayContext, ctx_ptr);
        ret_builder.setReleaseParamCaps(ctx.release_param_caps);
        const payload_builder = try ret_builder.payloadTyped();
        try ctx.target_peer.clonePayloadAcrossPeers(
            ret_builder.ret.builder,
            payload_builder,
            ctx.source,
            ctx.source_peer,
            ctx.source_inbound_caps,
            &ctx.created_result_proxy_ids,
        );
    }

    /// Send a return with results for a previously received call.
    pub fn sendReturnResults(self: *Peer, answer_id: u32, ctx: *anyopaque, build: ReturnBuildFn) !void {
        self.assertThreadAffinity();
        // sendResultsTo routing is resolved in precedence order:
        // third-party handoff > local results-sent-elsewhere marker > normal results payload.
        if (self.send_results_to_third_party.contains(answer_id)) {
            // This answer's results were redirected to a third vat, so we are
            // holding results we cannot deliver. Refuse loudly rather than drop
            // them: the marker is deliberately LEFT IN PLACE, because every
            // dispatch site converts an error from here into exactly one
            // exception Return, and that path clears the routing state and frees
            // the captured payload. An application that performed the redirect
            // itself settles the answer with sendReturnResultsSentElsewhere.
            //
            // Under the default `.reject` policy this is unreachable: handleCall
            // refuses such calls before the marker is ever recorded.
            return error.ThirdPartyResultsNotRedirected;
        }

        if (self.send_results_to_yourself.remove(answer_id)) {
            try self.completeSelfLoopbackReturn(answer_id, ctx, build);
            return;
        }

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        var effects = cap_table.OutboundCapEffects.init(self.allocator, self, rollbackOutboundCap);
        defer effects.deinit();
        var effects_committed = false;
        errdefer if (!effects_committed) effects.rollback();

        var ret = try builder.beginReturn(answer_id, .results);
        try build(ctx, &ret);
        _ = try cap_table.encodeReturnPayloadCapsWithEffects(&self.caps, &ret, onOutboundCap, &effects);

        const bytes = try builder.finish();
        defer self.allocator.free(bytes);

        // Capture before delivery: sendReturnFrameWithLoopback consumes the
        // loopback marker. Do not record a resolved answer for loopback
        // answers: they are delivered locally, are never referenced by a
        // remote PromisedAnswer, and get no Finish — and their outbound-counter
        // ids would collide with the remote question-id space, so a recorded
        // entry would leak and poison DuplicateQuestionId checks.
        //
        // Finished-early answers (Finish already arrived — before this late
        // async Return, or reentrantly while this send is on the stack) still
        // reserve: the post-send commit step re-checks the tombstone map and,
        // when a tombstone is present, commits the reserved answer FIRST so
        // calls pipelined on it replay with their own Returns, then
        // immediately applies the Finish cleanup. Skipping the reservation
        // would strand those parked calls with no Return at all.
        const is_loopback = self.loopback_questions.contains(answer_id);
        const should_record = !is_loopback;

        // Reserve the record resources (count budget, map slot, frame copy)
        // BEFORE sending so recording is infallible afterward. If it could fail
        // after the frame is on the wire, the error would drive the dispatch
        // catch to send a second exception Return for this answer (audit item 7).
        var reservation: ?ResolvedAnswerReservation = null;
        errdefer if (reservation) |r| r.deinit(self);
        var resolving_answer = false;
        errdefer if (resolving_answer) {
            _ = self.resolving_answers.remove(answer_id);
        };
        if (should_record) {
            reservation = try self.reserveResolvedAnswer(answer_id, bytes);
            try self.resolving_answers.put(answer_id, {});
            resolving_answer = true;
        }

        try self.sendReturnFrameWithLoopback(answer_id, bytes);
        cap_table.commitOutboundCapEffects(&self.caps, &effects);
        effects_committed = true;
        if (resolving_answer) {
            _ = self.resolving_answers.remove(answer_id);
            resolving_answer = false;
        }

        self.commitOrRollbackResolvedAnswerAfterSend(answer_id, bytes, &reservation);
    }

    /// Complete a return for an inbound call that carried `sendResultsTo =
    /// yourself` (the reflected-loopback / Level-1 tail-call optimization).
    ///
    /// The forwarder (the peer that parked this call on a promise and later
    /// resolved that promise to a capability we host) does NOT want the results
    /// on the wire — it will name the local question that consumes them via a
    /// `takeFromOtherQuestion` Return. Two things must still happen here:
    ///
    ///   1. The user handler MUST run. For generated DIRECT handlers the handler
    ///      body lives inside `build`, so we run `build` unconditionally to
    ///      produce the results — otherwise the handler would silently never
    ///      execute on this path (a real Level-1 self-loopback correctness gap).
    ///   2. The forwarder is told `resultsSentElsewhere`.
    ///
    /// The computed results are stashed keyed by this answer id so the matching
    /// inbound `takeFromOtherQuestion` (see `handleReturn`) can deliver them
    /// inline to the caller's own question — completing the value round-trip
    /// locally without a wire round-trip through the forwarder. Results carrying
    /// capabilities cannot be re-delivered locally (their descriptors are
    /// wire-encoded for the forwarder), so those are not stashed; the caller
    /// falls back to receiving the `takeFromOtherQuestion` relay tag.
    fn completeSelfLoopbackReturn(self: *Peer, answer_id: u32, ctx: *anyopaque, build: ReturnBuildFn) !void {
        // Build the results (runs the user handler) into a standalone frame.
        // Encode caps with rollback: these results never traverse the wire to
        // the forwarder, so any outbound wire refs the encode takes must be
        // undone. The encode still classifies descriptors so we can detect
        // whether the results carry capabilities.
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        var effects = cap_table.OutboundCapEffects.init(self.allocator, self, rollbackOutboundCap);
        defer effects.deinit();

        var ret = try builder.beginReturn(answer_id, .results);
        try build(ctx, &ret);
        _ = try cap_table.encodeReturnPayloadCapsWithEffects(&self.caps, &ret, onOutboundCap, &effects);
        effects.rollback();

        const frame = try builder.finish();
        var owns_frame = true;
        defer if (owns_frame) self.allocator.free(frame);

        const has_result_caps = try selfLoopbackResultsHaveCaps(self.allocator, frame);

        // Stash BEFORE telling the forwarder: in a synchronous in-process
        // transport, the `resultsSentElsewhere` we send can re-enter us with the
        // matching `takeFromOtherQuestion` before this call returns, and that
        // handler consumes the stash.
        if (!has_result_caps and
            self.loopback_result_stash.count() < self.limits.max_loopback_result_stash)
        {
            // toBytes returned an owned copy; hand ownership to the stash.
            try self.loopback_result_stash.put(answer_id, @constCast(frame));
            owns_frame = false;
        }

        try self.sendReturnTag(answer_id, .resultsSentElsewhere);
    }

    /// True when a built Return-results frame carries any capability
    /// descriptors in its payload cap table.
    fn selfLoopbackResultsHaveCaps(allocator: std.mem.Allocator, frame: []const u8) !bool {
        var decoded = try protocol.DecodedMessage.init(allocator, frame);
        defer decoded.deinit();
        const built = try decoded.asReturn();
        const payload = built.results orelse return false;
        const table = payload.cap_table orelse return false;
        return table.len() > 0;
    }

    /// Deliver a stashed self-loopback results frame to the local question named
    /// by a `takeFromOtherQuestion` redirect. Re-enters `handleReturn` with the
    /// stashed results re-keyed to the target question so the normal dispatch,
    /// cap-release, and auto-finish paths run unchanged.
    fn deliverStashedLoopbackResults(self: *Peer, target_question_id: u32, frame: []const u8) !void {
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        if (decoded.tag != .@"return") return error.UnexpectedMessage;
        var results_ret = try decoded.asReturn();
        results_ret.answer_id = target_question_id;
        try self.handleReturn(frame, results_ret);
    }

    /// Send a pre-built return frame, tracking outbound cap refs and recording
    /// the resolved answer for later PromisedAnswer resolution.
    pub fn sendPrebuiltReturnFrame(self: *Peer, ret: protocol.Return, frame: []const u8) !void {
        self.assertThreadAffinity();
        var rollback_outbound_refs = true;
        errdefer if (rollback_outbound_refs) {
            self.rollbackOutboundReturnCapRefs(ret) catch |err| {
                log.debug("failed to roll back outbound prebuilt return refs: {}", .{err});
            };
        };
        try self.noteOutboundReturnCapRefs(ret);
        self.clearSendResultsRouting(ret.answer_id);
        // Capture before delivery consumes the loopback marker. Finished-early
        // answers still reserve (see sendReturnResults): the post-send commit
        // step re-checks the tombstone map and commits-then-cleans so calls
        // pipelined on the answer replay instead of being stranded.
        const is_loopback = self.loopback_questions.contains(ret.answer_id);

        // See sendReturnResults: loopback answers may not be recorded — no
        // Finish will clear them and the id would be poisoned for reuse.
        const should_record = ret.tag == .results and !is_loopback;

        // Reserve record resources before the send so recording is infallible
        // afterward and cannot force a second (exception) Return for this
        // answer (audit item 7). A reserve failure here rolls the outbound cap
        // refs back (via rollback_outbound_refs) since nothing was sent yet.
        var reservation: ?ResolvedAnswerReservation = null;
        errdefer if (reservation) |r| r.deinit(self);
        var resolving_answer = false;
        errdefer if (resolving_answer) {
            _ = self.resolving_answers.remove(ret.answer_id);
        };
        if (should_record) {
            reservation = try self.reserveResolvedAnswer(ret.answer_id, frame);
            try self.resolving_answers.put(ret.answer_id, {});
            resolving_answer = true;
        }

        try self.sendReturnFrameWithLoopback(ret.answer_id, frame);
        rollback_outbound_refs = false;
        if (resolving_answer) {
            _ = self.resolving_answers.remove(ret.answer_id);
            resolving_answer = false;
        }

        self.commitOrRollbackResolvedAnswerAfterSend(ret.answer_id, frame, &reservation);
    }

    /// Send a return with an exception for a previously received call.
    ///
    /// A failed answer carries no results, so any pipelined calls queued
    /// against it can never be satisfied. After sending the exception this
    /// drains those queued calls, failing each with its own Return so the
    /// exactly-one-Return-per-call invariant holds and the caller's question
    /// table can drain (a compliant peer otherwise hangs forever).
    pub fn sendReturnException(self: *Peer, answer_id: u32, reason: []const u8) !void {
        return self.sendReturnExceptionTyped(answer_id, reason, .failed);
    }

    /// `sendReturnException` carrying an explicit `Exception.Type`, the
    /// retryability signal a remote peer acts on.
    ///
    /// Pipelined children inherit the parent answer's type: they failed for the
    /// same reason.
    pub fn sendReturnExceptionTyped(
        self: *Peer,
        answer_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        self.assertThreadAffinity();
        try self.sendReturnExceptionNoDrain(answer_id, reason, ex_type);
        self.failQueuedPromisedCalls(answer_id, reason, ex_type);
    }

    /// Send an exception Return without draining queued pipelined children.
    /// Used internally where the queued-call drain must not re-enter (e.g.
    /// while iterating the live `pending_promises` map).
    fn sendReturnExceptionNoDrain(
        self: *Peer,
        answer_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        try peer_return_dispatch.sendReturnExceptionForPeer(
            Peer,
            self,
            answer_id,
            reason,
            ex_type,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
        _ = self.finished_early_answers.remove(answer_id);
    }

    /// Fail and drain every pipelined call queued against `answer_id` (and,
    /// transitively, calls pipelined on those). Each queued call is sent a
    /// Return(exception) and has its inbound caps released.
    ///
    /// Drains iteratively via an explicit worklist rather than recursing, so
    /// a hostile peer cannot exhaust the stack with a deep pipelined chain.
    /// The caller must NOT be iterating the live `pending_promises` map when
    /// invoking this (it fetchRemoves buckets); callers that resolve a
    /// detached bucket, or fail a parent answer outside iteration, are safe.
    fn failQueuedPromisedCalls(
        self: *Peer,
        answer_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) void {
        var worklist = std.ArrayList(u32).empty;
        defer worklist.deinit(self.allocator);
        // Best-effort under OOM: if we cannot even seed the worklist the
        // queued children leak rather than crash; report and bail.
        worklist.append(self.allocator, answer_id) catch |err| {
            self.reportNonfatalError(err);
            return;
        };
        while (worklist.pop()) |aid| {
            var pending = self.pending_promises.fetchRemove(aid) orelse continue;
            defer pending.value.deinit(self.allocator);
            for (pending.value.items) |*pending_call| {
                defer pending_call.caps.deinit();
                defer self.allocator.free(pending_call.frame);

                // question_id was decoded once at enqueue; a value of 0 means the
                // frame was not a decodable Call, so there is nothing to fail.
                const child_qid = pending_call.question_id;
                if (child_qid == 0) continue;

                // Non-draining send: descendants are handled by the worklist,
                // not by re-entering this drain.
                self.sendReturnExceptionNoDrain(child_qid, reason, ex_type) catch |err| {
                    self.reportNonfatalError(err);
                };
                self.releaseInboundCaps(&pending_call.caps) catch |err| {
                    self.reportNonfatalError(err);
                };
                worklist.append(self.allocator, child_qid) catch |err| {
                    // Cannot enqueue this child's descendants for draining;
                    // they leak under memory pressure. Report and continue.
                    self.reportNonfatalError(err);
                };
            }
        }
    }

    fn reportNonfatalError(self: *Peer, err: anyerror) void {
        peer_return_dispatch.reportNonfatalErrorForPeer(Peer, self, err);
    }

    fn commitOrRollbackResolvedAnswerAfterSend(
        self: *Peer,
        answer_id: u32,
        frame: []const u8,
        reservation: *?ResolvedAnswerReservation,
    ) void {
        if (self.finished_early_answers.fetchRemove(answer_id)) |finished| {
            if (reservation.*) |r| {
                self.commitReservedResolvedAnswer(answer_id, r);
                reservation.* = null;
                self.cleanupResolvedAnswerAfterEarlyFinish(answer_id, finished.value);
                return;
            }
            if (finished.value) {
                self.releaseResultCaps(frame) catch |err| self.reportNonfatalError(err);
            }
            return;
        }

        if (reservation.*) |r| {
            self.commitReservedResolvedAnswer(answer_id, r);
            reservation.* = null;
        }
    }

    fn cleanupResolvedAnswerAfterEarlyFinish(self: *Peer, answer_id: u32, release_result_caps: bool) void {
        peer_finish.handleResolvedAnswerCleanup(
            Peer,
            self,
            answer_id,
            release_result_caps,
            peer_finish.takeResolvedAnswerFrameForPeerFn(Peer),
            releaseAnswerHeldResultCaps,
            releaseResultCaps,
            peer_finish.freeOwnedFrameForPeerFn(Peer),
        ) catch |err| self.reportNonfatalError(err);
    }

    /// Send a return with an empty struct result (0 data words, 0 pointers).
    /// Used by streaming methods to auto-ack after the handler completes.
    pub fn sendReturnEmptyStruct(self: *Peer, answer_id: u32) !void {
        self.assertThreadAffinity();
        const BuildCtx = struct {
            fn build(_: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                _ = try any.initStruct(0, 0);
                _ = try ret.initCapTableTyped(0);
            }
        };
        var ctx: u8 = 0;
        try self.sendReturnResults(answer_id, &ctx, BuildCtx.build);
    }

    /// Settle an inbound call that carried `sendResultsTo = thirdParty`, after
    /// the application has delivered the results to the third vat itself.
    ///
    /// Emits `Return{resultsSentElsewhere}` — the tag the protocol mandates for
    /// a Return answering a Call whose `sendResultsTo` was not `caller`.
    /// `awaitFromThirdParty` is *not* this tag: that is what an introducer sends
    /// to the original caller on a different connection, and it is gated on that
    /// caller having set `allowThirdPartyTailCall`.
    ///
    /// Requires `setThirdPartyResultPolicy(.application)`. Errors with
    /// `error.ResultsNotRedirected` when this answer's caller did not redirect
    /// its results, so a plain caller-routed question can never be settled
    /// without them.
    ///
    /// Pipelining is not supported on a redirected answer: this vat never sees
    /// the results, so it cannot resolve a promised-answer target against them.
    /// Any calls already pipelined on this answer are failed with their own
    /// exception `Return` rather than left waiting forever.
    pub fn sendReturnResultsSentElsewhere(self: *Peer, answer_id: u32) !void {
        self.assertThreadAffinity();
        if (!self.send_results_to_third_party.contains(answer_id)) {
            return error.ResultsNotRedirected;
        }
        try self.sendReturnTag(answer_id, .resultsSentElsewhere);
        self.failQueuedPromisedCalls(answer_id, results_sent_elsewhere_no_pipelining, .failed);
    }

    fn sendReturnTag(self: *Peer, answer_id: u32, tag: protocol.ReturnTag) !void {
        try peer_return_dispatch.sendReturnTagForPeer(
            Peer,
            self,
            answer_id,
            tag,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
        _ = self.finished_early_answers.remove(answer_id);
    }

    fn sendReturnTakeFromOtherQuestion(self: *Peer, answer_id: u32, other_question_id: u32) !void {
        try peer_return_dispatch.sendReturnTakeFromOtherQuestionForPeer(
            Peer,
            self,
            answer_id,
            other_question_id,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
        _ = self.finished_early_answers.remove(answer_id);
    }

    fn sendReturnAcceptFromThirdParty(self: *Peer, answer_id: u32, await_payload: ?[]const u8) !void {
        try peer_return_dispatch.sendReturnAcceptFromThirdPartyForPeer(
            Peer,
            self,
            answer_id,
            await_payload,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
        _ = self.finished_early_answers.remove(answer_id);
    }

    fn clearSendResultsRouting(self: *Peer, answer_id: u32) void {
        return_routing.clearSendResultsRoutingForPeer(
            Peer,
            self,
            answer_id,
            clearSendResultsToThirdParty,
        );
    }

    fn sendReturnFrameWithLoopback(self: *Peer, answer_id: u32, bytes: []const u8) !void {
        try return_send.sendReturnFrameWithLoopbackForPeer(
            Peer,
            self,
            answer_id,
            bytes,
            deliverLoopbackReturn,
            sendFrameControl,
        );
        _ = self.active_inbound_questions.remove(answer_id);
    }

    fn sendReturnProvidedTarget(self: *Peer, answer_id: u32, target: *const ProvideTarget) !void {
        const BuildCtx = struct {
            peer: *Peer,
            target: *const ProvideTarget,

            fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const ctx: *const @This() = castCtx(*const @This(), ctx_ptr);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();

                const descriptors = cap_table.descriptors;
                // Write the provided cap with its known origin so the accepting
                // (third) peer receives the correct descriptor variant — never a
                // space re-derived from a colliding bare id.
                const tagged: struct { origin_code: u4, cap_id: u32 } = switch (ctx.target.*) {
                    .local => |t| .{ .origin_code = t.origin_code, .cap_id = t.cap_id },
                    .promised => |promised| .{
                        .origin_code = descriptors.originCodeForTag(.receiverAnswer),
                        .cap_id = try ctx.peer.caps.noteReceiverAnswerOps(
                            promised.question_id,
                            promised.ops,
                        ),
                    },
                };
                try any.setCapabilityOriginTagged(tagged.origin_code, tagged.cap_id);
            }
        };

        var ctx = BuildCtx{
            .peer = self,
            .target = target,
        };
        try self.sendReturnResults(answer_id, &ctx, BuildCtx.build);
    }

    // ================= L3 vat-wide provision hosting (Experimental) =========
    //
    // Everything below implements the VatC hosting design (docs: FINAL-v2 as
    // amended by its adversarial verdict). The canonical drain/teardown
    // choreography is centralized here: failPendingAccept is the ONLY place a
    // queued/parked accept's +1 is released, closeProvisionAsOwner is the ONLY
    // owner-close procedure, and every send-bearing walk operates on a
    // moved-out container, never a live map.

    /// Attach this peer to a vat-wide provision index. Preconditions: not
    /// already attached, and no pre-existing per-peer handoff state (the index
    /// must see every provision from the first one).
    pub fn attachProvisionIndex(self: *Peer, index: *ProvisionIndex) !void {
        self.assertThreadAffinity();
        index.assertThreadAffinity();
        if (self.provision_index != null) return error.PeerAlreadyAttachedToProvisionIndex;
        if (self.provides_by_question.count() != 0 or
            self.pending_accepts_by_embargo.count() != 0)
        {
            return error.PeerAlreadyHasHandoffState;
        }
        try index.attached_peers.append(index.allocator, self);
        self.provision_index = index;
    }

    /// Detach from the provision index. Symmetric precondition: no live
    /// provisions owned by this peer and no queued cross-peer accepts — a
    /// cleanly detached peer can re-attach (its handoff maps are empty).
    pub fn detachProvisionIndex(self: *Peer) !void {
        self.assertThreadAffinity();
        const idx = self.provision_index orelse return;
        if (self.provisions_by_question.count() != 0 or
            self.cross_peer_pending_accepts.count() != 0)
        {
            return error.PeerHasActiveHandoffState;
        }
        var i: usize = 0;
        while (i < idx.attached_peers.items.len) {
            if (idx.attached_peers.items[i] == self) {
                _ = idx.attached_peers.swapRemove(i);
            } else i += 1;
        }
        self.provision_index = null;
    }

    /// Allocator-parameterized clone of a provide target (the provision owns
    /// COPIES on the INDEX allocator; `Peer.cloneProvideTarget` clones on the
    /// peer allocator and stays untouched).
    fn cloneProvideTargetWith(allocator: std.mem.Allocator, target: *const ProvideTarget) !ProvideTarget {
        return switch (target.*) {
            .local => |t| .{ .local = t },
            .promised => |promised| .{ .promised = try cap_table.OwnedPromisedAnswer.fromQuestionAndOps(
                allocator,
                promised.question_id,
                promised.ops,
            ) },
        };
    }

    /// Roll back a handoff pin taken by a ladder that has NOT yet transferred
    /// ownership (plain decrement, no destroy check — the pin was never
    /// exposed). Never used after a transfer: the transferee releases.
    fn rollbackHandoffExportRef(self: *Peer, id: u32) void {
        var entry = self.exports.getEntry(id) orelse return;
        if (entry.value_ptr.handoff_ref_count == 0) return;
        entry.value_ptr.handoff_ref_count -= 1;
    }

    /// PHASE A of Provide registration into the vat index (OOM ladder: all
    /// fallible work first, one infallible tail takes refs/flags/state).
    /// Errors are rolled back by the caller (clearProvide + abort). `adopted`
    /// is reserved for Accept-before-Provide adoption (parking landing).
    fn registerProvisionForProvide(
        self: *Peer,
        idx: *ProvisionIndex,
        provide_question_id: u32,
        adopted: *?*ProvisionIndex.Provision,
    ) !void {
        idx.assertThreadAffinity();
        adopted.* = null;
        const entry = self.provides_by_question.getPtr(provide_question_id) orelse
            return error.UnknownProvision;

        if (idx.by_key.get(entry.recipient_key)) |_| {
            // Vat-wide duplicate. (An `.awaiting` hit becomes adoption when
            // Accept parking lands; until then no `.awaiting` provisions can
            // exist.)
            return error.DuplicateProvideRecipient;
        }
        if (idx.provision_count >= idx.limits.max_provisions) return error.ProvisionBudgetExceeded;
        if (idx.provision_key_bytes + entry.recipient_key.len > idx.limits.max_provision_key_bytes)
            return error.ProvisionBudgetExceeded;

        // 1. Create (index allocator).
        const prov = try idx.allocator.create(ProvisionIndex.Provision);
        errdefer idx.allocator.destroy(prov);
        prov.* = .{
            .allocator = idx.allocator,
            .recipient_key = &.{},
            .embargoes = std.StringHashMap(ProvisionIndex.ProvisionEmbargo).init(idx.allocator),
        };
        errdefer prov.embargoes.deinit();
        // 2. Own the key bytes.
        prov.recipient_key = try idx.allocator.dupe(u8, entry.recipient_key);
        errdefer idx.allocator.free(prov.recipient_key);
        // 3. Own the target copy (index allocator — freed by Provision.release
        //    with the matching allocator).
        prov.target = try cloneProvideTargetWith(idx.allocator, &entry.target);
        errdefer if (prov.target) |*t| t.deinit(idx.allocator);
        // 4. Pin a sender-hosted target export for the provision's lifetime.
        var pinned_export: ?u32 = null;
        switch (entry.target) {
            .local => |t| {
                const tag = try cap_table.descriptors.tagForOriginCode(t.origin_code);
                if (tag == .senderHosted or tag == .senderPromise) {
                    try self.noteHandoffExportRef(t.cap_id);
                    pinned_export = t.cap_id;
                }
            },
            .promised => {},
        }
        errdefer if (pinned_export) |id| self.rollbackHandoffExportRef(id);
        // 5. Index entry (key borrows prov.recipient_key — rule R3).
        try idx.by_key.put(prov.recipient_key, prov);
        errdefer _ = idx.by_key.remove(prov.recipient_key);
        // 6. Owner map entry (peer allocator) — last fallible operation.
        try self.provisions_by_question.put(provide_question_id, prov);
        // 7. INFALLIBLE TAIL: refs, flags, state, counters.
        prov.state = .active;
        prov.owner = self;
        prov.provide_question_id = provide_question_id;
        prov.target_export_pinned = pinned_export != null;
        prov.indexed = true;
        prov.retain(); // the index's +1
        prov.retain(); // the owner map's +1
        idx.provision_count += 1;
        idx.provision_key_bytes += prov.recipient_key.len;
    }

    /// Index-mode Accept path. The same-peer arm delegates to the EXACT legacy
    /// orchestration call (byte-identical Returns and error strings); only the
    /// cross-peer arm is new.
    fn handleAcceptWithProvisionIndex(self: *Peer, idx: *ProvisionIndex, accept: protocol.Accept) !void {
        const key_opt = try peer_provide_join_orchestration.captureAcceptProvisionForPeer(
            Peer,
            self,
            accept,
            peer_third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
        );
        defer if (key_opt) |bytes| self.allocator.free(bytes);
        const key = key_opt orelse
            return self.sendReturnException(accept.question_id, "unknown provision");
        const prov = idx.by_key.get(key) orelse
            return self.sendReturnException(accept.question_id, "unknown provision");

        if (prov.owner == self and prov.state == .active) {
            // Degenerate same-peer arm: the same function, arguments, and
            // hooks as the no-index path.
            return peer_provide_join_orchestration.handleAccept(
                Peer,
                ProvideEntry,
                ProvideTarget,
                self,
                accept,
                &self.provides_by_question,
                &self.provides_by_key,
                peer_provide_join_orchestration.captureAcceptProvisionForPeerFn(
                    Peer,
                    peer_third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
                ),
                peer_finish.freeOwnedFrameForPeerFn(Peer),
                Peer.queueEmbargoedAcceptRouted,
                Peer.sendReturnProvidedTarget,
                Peer.sendReturnException,
            );
        }

        // Cross-peer arm.
        prov.retain();
        defer prov.release();
        if (accept.embargo) |embargo| {
            self.queueCrossPeerEmbargoedAccept(prov, accept.question_id, embargo) catch |err|
                try self.convertQueueErrorToReturn(accept.question_id, err);
            return;
        }
        serveProvisionOnPeer(self, prov, accept.question_id) catch |err|
            try self.sendReturnException(accept.question_id, @errorName(err));
    }

    /// Convert a queue-ladder failure into the Accept's exception Return —
    /// except OOM, which re-raises out of dispatch (dropping a rendezvous
    /// marker silently would wedge the recipient; loud and terminal is the
    /// rule).
    fn convertQueueErrorToReturn(self: *Peer, answer_id: u32, err: anyerror) !void {
        switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.DuplicateEmbargoId => try self.sendReturnException(answer_id, "duplicate embargo id"),
            error.DuplicateAcceptQuestionId => try self.sendReturnException(answer_id, "duplicate accept question id"),
            error.EmbargoBudgetExceeded, error.QueuedAcceptBudgetExceeded => try self.sendReturnException(answer_id, "provision queued-accept budget exhausted"),
            error.ProvisionClosed => try self.sendReturnException(answer_id, "provision lost: provider connection closed"),
            else => try self.sendReturnException(answer_id, @errorName(err)),
        }
    }

    /// Remove one embargo entry from a provision, freeing its owned key.
    fn eraseProvisionEmbargoEntry(prov: *ProvisionIndex.Provision, key_bytes: []const u8) void {
        if (prov.embargoes.fetchRemove(key_bytes)) |kv| {
            prov.embargo_key_bytes -= kv.key.len;
            prov.allocator.free(kv.key);
        }
    }

    /// Queue one embargoed accept into the provision's embargo map (single
    /// pending slot per embargo id; find-or-create meets the
    /// Disembargo-before-Accept race). Ladder discipline: ALL fallible work
    /// first, one infallible tail takes the flags, slot, ref, and counters —
    /// an OOM unwinds to zero residue (no half-queued accept, no second
    /// Return, no unspent ref). `self` is the accept peer; it may equal the
    /// owner (same-peer accepts on attached peers route here too).
    fn queueCrossPeerEmbargoedAccept(
        self: *Peer,
        prov: *ProvisionIndex.Provision,
        answer_id: u32,
        embargo: []const u8,
    ) !void {
        if (prov.state != .active) return error.ProvisionClosed;
        const idx = self.provision_index orelse return error.ProvisionClosed;
        if (self.cross_peer_pending_accepts.contains(answer_id)) return error.DuplicateAcceptQuestionId;

        const entry_exists = prov.embargoes.contains(embargo);
        if (!entry_exists) {
            if (prov.embargoes.count() >= idx.limits.max_embargoes_per_provision) return error.EmbargoBudgetExceeded;
            if (prov.embargo_key_bytes + embargo.len > idx.limits.max_embargo_key_bytes_per_provision) return error.EmbargoBudgetExceeded;
        }
        if (idx.queued_accept_count >= idx.limits.max_queued_accepts) return error.QueuedAcceptBudgetExceeded;
        if (idx.queued_accept_bytes + embargo.len > idx.limits.max_queued_accept_bytes) return error.QueuedAcceptBudgetExceeded;

        const gop = try prov.embargoes.getOrPut(embargo);
        var created_here = false;
        if (!gop.found_existing) {
            gop.key_ptr.* = try prov.allocator.dupe(u8, embargo);
            gop.value_ptr.* = .{};
            prov.embargo_key_bytes += embargo.len;
            created_here = true;
        }
        errdefer if (created_here) eraseProvisionEmbargoEntry(prov, embargo);

        if (gop.value_ptr.used_by_accept) return error.DuplicateEmbargoId;
        if (gop.value_ptr.disembargoed) {
            // The Disembargo won the race: nothing to withhold — consume the
            // tombstone and serve immediately.
            eraseProvisionEmbargoEntry(prov, embargo);
            return serveProvisionOnPeer(self, prov, answer_id);
        }

        const r_key = try self.allocator.dupe(u8, embargo);
        errdefer self.allocator.free(r_key);
        try self.cross_peer_pending_accepts.put(answer_id, .{
            .provision = prov,
            .embargo_key = r_key,
            .parked = false,
        });

        // INFALLIBLE TAIL.
        gop.value_ptr.used_by_accept = true;
        gop.value_ptr.pending = .{ .accept_peer = self, .answer_id = answer_id };
        prov.retain(); // the pending slot's +1
        idx.queued_accept_count += 1;
        idx.queued_accept_bytes += embargo.len;
    }

    /// Same-typed replacement for the `queue_embargoed_accept` hook VALUE on
    /// index-attached peers: same-peer embargoed accepts route into the
    /// provision store (per-provision keying) instead of the byte-keyed
    /// legacy bucket — the byte-collision co-drain class dies with the dual
    /// store. A miss NEVER falls back to the legacy queue (that would
    /// silently repopulate the byte-keyed store).
    fn queueEmbargoedAcceptRouted(
        self: *Peer,
        answer_id: u32,
        provided_question_id: u32,
        embargo: []const u8,
    ) !void {
        const prov = self.provisions_by_question.get(provided_question_id) orelse {
            try self.sendReturnException(answer_id, "unknown provision");
            return;
        };
        prov.retain();
        defer prov.release();
        self.queueCrossPeerEmbargoedAccept(prov, answer_id, embargo) catch |err|
            try self.convertQueueErrorToReturn(answer_id, err);
    }

    /// Release (or pre-mark) one embargo on a provision — the host arm of a
    /// spec-form accept-Disembargo. Walks the provision's own embargo map,
    /// never any vat-wide or byte-keyed store; find-or-create leaves a
    /// tombstone when the Disembargo arrives before its Accept. Consumed
    /// entries are ERASED, so completed embargo ids are reusable.
    fn releaseProvisionEmbargo(self: *Peer, prov: *ProvisionIndex.Provision, embargo: []const u8) !void {
        if (prov.state == .closed) return;

        if (!prov.embargoes.contains(embargo)) {
            // Tombstone-create budget: only the introducer sends
            // accept-Disembargos here, so exceeding the per-provision bound is
            // that introducer misbehaving — abort loudly, never drop a
            // rendezvous marker silently.
            const over = if (self.provision_index) |idx|
                prov.embargoes.count() >= idx.limits.max_embargoes_per_provision
            else
                false;
            if (over) {
                peer_outbound_control.sendAbortViaSendFrame(Peer, self, "provision embargo budget exhausted", Peer.sendFrameControl) catch |send_err| {
                    log.debug("embargo budget abort send failed: {}", .{send_err});
                };
                return error.EmbargoBudgetExceeded;
            }
        }
        const gop = try prov.embargoes.getOrPut(embargo);
        if (!gop.found_existing) {
            gop.key_ptr.* = try prov.allocator.dupe(u8, embargo);
            gop.value_ptr.* = .{};
            prov.embargo_key_bytes += embargo.len;
        }
        gop.value_ptr.disembargoed = true;
        const pending = gop.value_ptr.pending orelse return; // tombstone kept until the Accept arrives
        gop.value_ptr.pending = null; // detach: we inherit the slot's +1
        prov.retain();
        defer prov.release();
        eraseProvisionEmbargoEntry(prov, embargo);

        // Canonical fail/serve ordering: holder record removed (and the accept
        // peer's key dupe freed) BEFORE the send.
        if (pending.accept_peer.cross_peer_pending_accepts.fetchRemove(pending.answer_id)) |kv| {
            if (kv.value.embargo_key) |k| pending.accept_peer.allocator.free(k);
        }
        if (pending.accept_peer.provision_index) |idx| {
            idx.queued_accept_count -= 1;
            idx.queued_accept_bytes -= embargo.len;
        }
        serveProvisionOnPeer(pending.accept_peer, prov, pending.answer_id) catch |err| {
            pending.accept_peer.sendReturnException(pending.answer_id, @errorName(err)) catch |e2| {
                log.debug("provision release: failed to fail accept {}: {}", .{ pending.answer_id, e2 });
            };
        };
        prov.release(); // the pending slot's +1
    }

    /// FinishOps `clear_pending_accept_question` replacement (same fn type):
    /// a Finish cancelling a queued cross-peer Accept clears the provision
    /// slot — releasing the slot's +1 ONLY when a LIVE slot was actually
    /// cleared (a record whose slot a concurrent drain already detached is a
    /// clean miss) — then always falls through to the legacy per-peer path.
    fn clearPendingAcceptQuestionRouted(peer: *Peer, question_id: u32) void {
        if (peer.cross_peer_pending_accepts.fetchRemove(question_id)) |kv| {
            const rec = kv.value;
            const prov = rec.provision; // valid: the slot's +1 pins it (INV-REC)
            var cleared_live = false;
            if (rec.parked) {
                var i: usize = 0;
                while (i < prov.parked.items.len) : (i += 1) {
                    const parked = prov.parked.items[i];
                    if (parked.accept_peer == peer and parked.answer_id == question_id) {
                        if (parked.embargo) |bytes| prov.allocator.free(bytes);
                        _ = prov.parked.swapRemove(i);
                        cleared_live = true;
                        break;
                    }
                }
            } else if (rec.embargo_key) |key| {
                if (prov.embargoes.getPtr(key)) |emb| {
                    if (emb.pending) |pending| {
                        if (pending.accept_peer == peer and pending.answer_id == question_id) {
                            emb.pending = null;
                            cleared_live = true;
                        }
                    }
                }
                if (cleared_live) {
                    if (peer.provision_index) |idx| {
                        idx.queued_accept_count -= 1;
                        idx.queued_accept_bytes -= key.len;
                    }
                    eraseProvisionEmbargoEntry(prov, key);
                }
            }
            if (rec.embargo_key) |k| peer.allocator.free(k);
            if (cleared_live) prov.release(); // the slot's/parked entry's +1
        }
        peer_embargo_accepts.clearPendingAcceptQuestionForPeer(
            Peer,
            PendingEmbargoedAccept,
            peer,
            question_id,
        );
    }

    /// Serve one accept from a provision on an arbitrary holder peer — the
    /// core of cross-peer hosting. The Return carries senderHosted{proxy}
    /// where the proxy (minted on the accept peer) forwards to the OWNER's
    /// export, holding its own handoff pin so the accepted capability
    /// outlives the provision's Finish.
    fn serveProvisionOnPeer(accept_peer: *Peer, prov: *ProvisionIndex.Provision, answer_id: u32) !void {
        const owner = prov.owner orelse
            return accept_peer.sendReturnException(answer_id, "provision lost: provider connection closed");
        if (owner == accept_peer) {
            // Degenerate arm: byte-identical to today against the owner's
            // LIVE ProvideEntry.
            const entry = owner.provides_by_question.getPtr(prov.provide_question_id) orelse
                return accept_peer.sendReturnException(answer_id, "unknown provision");
            return accept_peer.sendReturnProvidedTarget(answer_id, &entry.target);
        }

        // Cross-peer arm: target-kind gate. A null target (an `.awaiting`
        // provision that lost its owner) fails closed like an ownerless one.
        const stored_target = prov.target orelse
            return accept_peer.sendReturnException(answer_id, "provision lost: provider connection closed");
        const source: cap_table.ResolvedCap = switch (stored_target) {
            .local => |t| switch (try cap_table.descriptors.tagForOriginCode(t.origin_code)) {
                .senderHosted, .senderPromise => .{ .exported = .{ .id = t.cap_id } },
                .receiverHosted => return error.CrossPeerReceiverHostedTargetUnsupported,
                else => return error.CrossPeerProvisionTargetUnsupported,
            },
            .promised => return error.CrossPeerPromisedTargetUnsupported,
        };

        // Pin the source export for the PROXY's lifetime (survives the
        // provision's Finish). Ownership of this pin transfers AT the next
        // call: the callee's pre-ctx errdefer arm or its ctx deinit releases
        // it on ANY failure from here on — this caller performs NO pin
        // rollback and NO destroy sweep of an export the serve does not own.
        try owner.noteHandoffExportRef(source.exported.id);
        const proxy_id = try accept_peer.addCrossPeerProxyExport(
            owner,
            source,
            null,
            source.exported.id,
        );
        errdefer accept_peer.destroyUnreferencedProxyExport(proxy_id);
        try accept_peer.sendReturnSenderHostedCap(answer_id, proxy_id);
    }

    /// Single-capability Return whose payload content is a senderHosted cap
    /// pointer at index 0 — the same shape as bootstrap returns, through the
    /// same origin-tagged encode path and reserve/rollback machinery.
    fn sendReturnSenderHostedCap(self: *Peer, answer_id: u32, export_id: u32) !void {
        const BuildCtx = struct {
            export_id: u32,
            fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const ctx: *const @This() = castCtx(*const @This(), ctx_ptr);
                var payload = try ret.payloadTyped();
                var any = try payload.initContent();
                try any.setCapabilityOriginTagged(
                    cap_table.descriptors.originCodeForTag(.senderHosted),
                    ctx.export_id,
                );
            }
        };
        var ctx = BuildCtx{ .export_id = export_id };
        try self.sendReturnResults(answer_id, &ctx, BuildCtx.build);
    }

    // -- The canonical drain/teardown procedure ------------------------------

    const DrainPosture = enum { fallible, best_effort };

    fn DrainError(comptime posture: DrainPosture) type {
        return switch (posture) {
            .fallible => error{OutOfMemory},
            .best_effort => error{},
        };
    }

    /// THE one shared fail helper for a queued/parked accept. Input: a pending
    /// ALREADY detached from its slot/list by the caller. Order is law: holder
    /// record removed (and the accept peer's key dupe freed) BEFORE the send;
    /// the single release of the pending's +1 AFTER the send (via defer, so it
    /// drops exactly once even on an OOM re-raise). A reentrant Finish of the
    /// accept question racing the send finds the record already gone — a
    /// clean miss, never a double release.
    fn failPendingAccept(
        pending: ProvisionIndex.PendingAccept,
        prov: *ProvisionIndex.Provision,
        reason: []const u8,
        comptime posture: DrainPosture,
    ) DrainError(posture)!void {
        if (pending.accept_peer.cross_peer_pending_accepts.fetchRemove(pending.answer_id)) |kv| {
            if (kv.value.embargo_key) |k| {
                if (!kv.value.parked) {
                    // Queued (non-parked) accepts carry the vat-wide budget;
                    // adjust it where the record dies. Reachability is via the
                    // accept peer's own index pointer — absent after index
                    // death, an accepted (documented) counter skew.
                    if (pending.accept_peer.provision_index) |idx| {
                        idx.queued_accept_count -= 1;
                        idx.queued_accept_bytes -= k.len;
                    }
                }
                pending.accept_peer.allocator.free(k);
            }
        }
        defer prov.release();
        pending.accept_peer.sendReturnException(pending.answer_id, reason) catch |err| {
            if (posture == .fallible and err == error.OutOfMemory) return error.OutOfMemory;
            log.debug("failPendingAccept: exception send for answer {} failed: {}", .{ pending.answer_id, err });
        };
    }

    /// Drain a provision's embargo slots and parked accepts through
    /// failPendingAccept, over MOVED-OUT containers (nested frames see the
    /// fresh empty containers, and `.closed`-state guards forbid new
    /// insertions). Under `.fallible`, an OOM mid-walk moves the residue back
    /// so the owner-deinit drain can later complete it best-effort.
    fn drainProvisionEntries(
        prov: *ProvisionIndex.Provision,
        reason: []const u8,
        comptime posture: DrainPosture,
    ) DrainError(posture)!void {
        var owned_embargoes = prov.embargoes;
        prov.embargoes = std.StringHashMap(ProvisionIndex.ProvisionEmbargo).init(prov.allocator);
        prov.embargo_key_bytes = 0;
        var owned_parked = prov.parked;
        prov.parked = .empty;

        var it = owned_embargoes.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.pending) |pending| {
                entry.value_ptr.pending = null;
                failPendingAccept(pending, prov, reason, posture) catch |err| {
                    // Residue rule: restore both containers, re-raise. The
                    // fresh maps are still empty (guaranteed by the `.closed`
                    // guards), so plain moves are safe.
                    prov.embargoes.deinit();
                    prov.embargoes = owned_embargoes;
                    var parked_tmp = prov.parked;
                    parked_tmp.deinit(prov.allocator);
                    prov.parked = owned_parked;
                    return err;
                };
            }
        }
        var kit = owned_embargoes.keyIterator();
        while (kit.next()) |k| prov.allocator.free(k.*);
        owned_embargoes.deinit();

        var i: usize = 0;
        while (i < owned_parked.items.len) : (i += 1) {
            const parked = owned_parked.items[i];
            if (parked.embargo) |bytes| prov.allocator.free(bytes);
            owned_parked.items[i].embargo = null;
            failPendingAccept(
                .{ .accept_peer = parked.accept_peer, .answer_id = parked.answer_id },
                prov,
                reason,
                posture,
            ) catch |err| {
                // Residue: keep the not-yet-drained tail parked.
                var rest: std.ArrayList(ProvisionIndex.ParkedAccept) = .empty;
                rest.appendSlice(prov.allocator, owned_parked.items[i + 1 ..]) catch {
                    // Cannot even keep the tail: fail it best-effort so no
                    // accept is silently stranded, then fall through.
                    for (owned_parked.items[i + 1 ..]) |tail| {
                        if (tail.embargo) |bytes| prov.allocator.free(bytes);
                        failPendingAccept(
                            .{ .accept_peer = tail.accept_peer, .answer_id = tail.answer_id },
                            prov,
                            reason,
                            .best_effort,
                        ) catch {};
                    }
                    owned_parked.deinit(prov.allocator);
                    return err;
                };
                owned_parked.deinit(prov.allocator);
                var parked_tmp = prov.parked;
                parked_tmp.deinit(prov.allocator);
                prov.parked = rest;
                return err;
            };
        }
        owned_parked.deinit(prov.allocator);
    }

    const CloseOutcome = enum { closed_now, already_closed };

    /// THE canonical owner close. Two entry contexts: Finish (`.fallible`,
    /// `.release` the pin) and owner-deinit drain (`.best_effort`, `.abandon`
    /// the pin — the export table dies with the peer). The `.closed`
    /// transition is the OWNERSHIP TOKEN: exactly one caller observes
    /// `.closed_now` and may drop the owner map entry + its +1; every nested
    /// or duplicate entry sees `.already_closed` and must transfer NOTHING.
    fn closeProvisionAsOwner(
        self: *Peer,
        idx: ?*ProvisionIndex,
        prov: *ProvisionIndex.Provision,
        comptime unpin: enum { release, abandon },
        comptime posture: DrainPosture,
    ) DrainError(posture)!CloseOutcome {
        if (prov.state == .closed) return .already_closed;
        prov.retain();
        defer prov.release();
        prov.state = .closed;
        if (unpin == .release) {
            if (prov.target_export_pinned) {
                if (prov.target) |t| switch (t) {
                    .local => |lt| self.releaseHandoffHeldExport(lt.cap_id),
                    else => {},
                };
                prov.target_export_pinned = false;
            }
        }
        prov.owner = null;
        if (prov.indexed) {
            if (idx) |i| {
                _ = i.by_key.remove(prov.recipient_key);
                i.provision_count -= 1;
                i.provision_key_bytes -= prov.recipient_key.len;
                prov.indexed = false;
                prov.release(); // the index's +1
            }
        }
        try drainProvisionEntries(prov, switch (posture) {
            .fallible => "provision finished before disembargo",
            .best_effort => "provision lost: provider connection closed",
        }, posture);
        return .closed_now;
    }

    /// Fallible Finish pre-step, called from handleFinish BEFORE the FinishOps
    /// chain — UNGATED (the orelse-return is the gate: no-index peers have an
    /// empty map; mid-deinit the live map is empty). The `.closed` early
    /// return makes a nested duplicate Finish (the vine-release cascade) a
    /// clean no-op that transfers nothing.
    fn detachProvisionForFinish(self: *Peer, question_id: u32) !void {
        const prov = self.provisions_by_question.get(question_id) orelse return;
        if (prov.state == .closed) return;
        const outcome = try self.closeProvisionAsOwner(self.provision_index, prov, .release, .fallible);
        if (outcome != .closed_now) return;
        // Only after the close COMPLETED (no residue): drop the map entry and
        // the owner's +1.
        _ = self.provisions_by_question.remove(question_id);
        prov.release();
    }

    /// State threaded from the neutralize step of Peer.deinit to its drain
    /// phase (after forceCancelAllQuestions).
    const OwnerProvisionTeardown = struct {
        saved_idx: ?*ProvisionIndex,
        owned_provisions: std.AutoHashMap(u32, *ProvisionIndex.Provision),
    };

    /// Neutralize step (infallible; template-conformant: marks, moves, and
    /// back-link removal only — no sends, no frees of shared state). Runs for
    /// HOLDER-only peers too: the index self-removal must be unconditional.
    fn neutralizeProvisionsOnOwnerPeer(self: *Peer) OwnerProvisionTeardown {
        const saved_idx = self.provision_index;
        if (saved_idx) |idx| {
            var i: usize = 0;
            while (i < idx.attached_peers.items.len) {
                if (idx.attached_peers.items[i] == self) {
                    _ = idx.attached_peers.swapRemove(i);
                } else i += 1;
            }
            self.provision_index = null;
        }
        var owned = self.provisions_by_question;
        self.provisions_by_question = std.AutoHashMap(u32, *ProvisionIndex.Provision).init(self.allocator);
        var it = owned.valueIterator();
        while (it.next()) |prov_ptr| {
            const prov = prov_ptr.*;
            prov.state = .closed;
            prov.owner = null; // pin abandoned (the export table dies with us)
        }
        return .{ .saved_idx = saved_idx, .owned_provisions = owned };
    }

    /// Drain phase: send-bearing, runs AFTER forceCancelAllQuestions over the
    /// moved-out snapshot. Exception Returns go to LIVE sibling peers.
    fn drainClosedProvisionsOnOwnerPeer(self: *Peer, teardown: *OwnerProvisionTeardown) void {
        _ = self;
        var it = teardown.owned_provisions.valueIterator();
        while (it.next()) |prov_ptr| {
            const prov = prov_ptr.*;
            prov.retain();
            if (prov.indexed) {
                if (teardown.saved_idx) |idx| {
                    _ = idx.by_key.remove(prov.recipient_key);
                    idx.provision_count -= 1;
                    idx.provision_key_bytes -= prov.recipient_key.len;
                    prov.indexed = false;
                    prov.release(); // the index's +1
                }
            }
            drainProvisionEntries(prov, "provision lost: provider connection closed", .best_effort) catch {};
            prov.release(); // the transient guard
            prov.release(); // the owner map's +1
        }
        teardown.owned_provisions.deinit();
    }

    /// Holder-peer neutralize: clear this peer's queued/parked cross-peer
    /// accepts. Send-free and infallible, but must run BEFORE
    /// forceCancelAllQuestions (whose Finishes can nest frames serving INTO
    /// this half-dead peer). Releases the pending's +1 ONLY when a LIVE
    /// slot/parked entry was actually cleared — a record whose slot was
    /// already detached by a concurrent drain transfers nothing.
    fn detachCrossPeerAcceptsOnHolderPeer(self: *Peer) void {
        var owned = self.cross_peer_pending_accepts;
        self.cross_peer_pending_accepts = std.AutoHashMap(u32, CrossPeerAcceptRecord).init(self.allocator);
        var it = owned.iterator();
        while (it.next()) |entry| {
            const answer_id = entry.key_ptr.*;
            const rec = entry.value_ptr.*;
            const prov = rec.provision; // valid: the slot's +1 pins it (INV-REC)
            var cleared_live = false;
            if (rec.parked) {
                var i: usize = 0;
                while (i < prov.parked.items.len) : (i += 1) {
                    const parked = prov.parked.items[i];
                    if (parked.accept_peer == self and parked.answer_id == answer_id) {
                        if (parked.embargo) |bytes| prov.allocator.free(bytes);
                        _ = prov.parked.swapRemove(i);
                        cleared_live = true;
                        break;
                    }
                }
            } else if (rec.embargo_key) |key| {
                if (prov.embargoes.getPtr(key)) |emb| {
                    if (emb.pending) |pending| {
                        if (pending.accept_peer == self and pending.answer_id == answer_id) {
                            emb.pending = null;
                            cleared_live = true;
                        }
                    }
                }
                if (cleared_live) {
                    if (prov.embargoes.fetchRemove(key)) |kv| {
                        prov.embargo_key_bytes -= kv.key.len;
                        prov.allocator.free(kv.key);
                    }
                }
            }
            if (rec.embargo_key) |k| {
                if (cleared_live and !rec.parked) {
                    if (self.provision_index) |idx| {
                        idx.queued_accept_count -= 1;
                        idx.queued_accept_bytes -= k.len;
                    }
                }
                self.allocator.free(k);
            }
            if (cleared_live) prov.release(); // the slot's/parked entry's +1
        }
        owned.deinit();
    }

    fn noteOutboundReturnCapRefs(self: *Peer, ret: protocol.Return) !void {
        try return_send.noteOutboundReturnCapRefsForPeer(
            Peer,
            self,
            ret,
            noteExportRef,
        );
    }

    fn rollbackOutboundReturnCapRefs(self: *Peer, ret: protocol.Return) !void {
        try return_send.rollbackOutboundReturnCapRefsForPeer(
            Peer,
            self,
            ret,
            rollbackExportRef,
        );
    }

    fn clearSendResultsToThirdParty(self: *Peer, answer_id: u32) void {
        if (self.send_results_to_third_party.fetchRemove(answer_id)) |entry| {
            if (entry.value) |payload| self.allocator.free(payload);
        }
    }

    fn provideTargetsEqual(a: *const ProvideTarget, b: *const ProvideTarget) bool {
        return switch (a.*) {
            .local => |local| switch (b.*) {
                .local => |other_local| local.origin_code == other_local.origin_code and local.cap_id == other_local.cap_id,
                else => false,
            },
            .promised => |promised| switch (b.*) {
                .promised => |other_promised| blk: {
                    if (promised.question_id != other_promised.question_id) break :blk false;
                    if (promised.ops.len != other_promised.ops.len) break :blk false;
                    for (promised.ops, 0..) |op, idx| {
                        const other = other_promised.ops[idx];
                        if (op.tag != other.tag or op.pointer_index != other.pointer_index) break :blk false;
                    }
                    break :blk true;
                },
                else => false,
            },
        };
    }

    fn captureAnyPointerPayload(
        allocator: std.mem.Allocator,
        ptr: ?message.AnyPointerReader,
    ) !?[]u8 {
        const any = ptr orelse return null;
        if (any.isNull()) return null;

        var builder = message.MessageBuilder.init(allocator);
        defer builder.deinit();

        const root = try builder.initRootAnyPointer();
        try message.cloneAnyPointer(any, root);

        const bytes = try builder.toBytes();
        return @constCast(bytes);
    }

    fn noteSendResultsToYourself(self: *Peer, answer_id: u32) !void {
        try ensureCountLimit(
            self.send_results_to_yourself.contains(answer_id),
            self.send_results_to_yourself.count(),
            self.limits.max_send_results_to_yourself,
        );
        try return_routing.noteSendResultsToYourselfForPeer(
            Peer,
            self,
            answer_id,
            clearSendResultsToThirdParty,
        );
    }

    fn noteSendResultsToThirdParty(
        self: *Peer,
        answer_id: u32,
        ptr: ?message.AnyPointerReader,
    ) !void {
        _ = self.send_results_to_yourself.remove(answer_id);

        const payload = try captureAnyPointerPayload(self.allocator, ptr);
        errdefer if (payload) |bytes| self.allocator.free(bytes);

        try ensureCountLimit(
            self.send_results_to_third_party.contains(answer_id),
            self.send_results_to_third_party.count(),
            self.limits.max_send_results_to_third_party,
        );
        try ensureByteLimit(
            self.sendResultsToThirdPartyBytesExcluding(answer_id),
            optionalPayloadBytes(payload),
            self.limits.max_send_results_to_third_party_bytes,
        );

        const entry = try self.send_results_to_third_party.getOrPut(answer_id);
        if (entry.found_existing) {
            if (entry.value_ptr.*) |existing| self.allocator.free(existing);
        }
        entry.value_ptr.* = payload;
    }

    /// Release references to an imported capability, sending a Release message
    /// to the remote peer when the reference count drops to zero.
    ///
    /// Error set is intentionally left open (`anyerror`). The Release send path
    /// funnels through `sendFrameControl` → `sendFrame`, which synchronously
    /// invokes any installed `SendFrameOverride` (`cb(ctx, frame)` — arbitrary
    /// user code, see `setSendFrameOverride`). That callback's `anyerror`
    /// propagates out of this function, so no honest named set can be narrower
    /// than `anyerror` without changing the `SendFrameOverride` contract. This
    /// stays open for the same reason as the user-callback typedefs above.
    pub fn releaseImport(self: *Peer, import_id: u32, count: u32) anyerror!void {
        self.assertThreadAffinity();
        log.debug("releasing import id={} count={}", .{ import_id, count });
        try peer_cap_lifecycle.releaseImport(
            Peer,
            self,
            import_id,
            count,
            peer_cap_lifecycle.importRefCountForPeerFn(Peer),
            peer_cap_lifecycle.releaseImportRefForPeerFn(Peer),
            Peer.releaseResolvedImport,
            peer_outbound_control.sendReleaseViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
        );
    }

    /// Send a Release message on behalf of a host integration, bypassing the
    /// peer's own import tracking.
    pub fn sendReleaseForHost(self: *Peer, import_id: u32, count: u32) !void {
        self.assertThreadAffinity();
        try peer_outbound_control.sendReleaseViaSendFrame(
            Peer,
            self,
            import_id,
            count,
            Peer.sendFrameControl,
        );
    }

    /// Drop wire references the peer holds on an import WITHOUT sending a
    /// Release frame. Used by host integrations when ownership of the
    /// references transfers to the host (a relayed host-call Return with
    /// `releaseParamCaps = false`): the host keeps the remote capability
    /// alive and later sends its own Release, so the peer must forget its
    /// bookkeeping silently or the reference would be spent twice.
    pub fn forgetImportRefsForHost(self: *Peer, import_id: u32, count: u32) !void {
        self.assertThreadAffinity();
        try peer_cap_lifecycle.releaseImport(
            Peer,
            self,
            import_id,
            count,
            peer_cap_lifecycle.importRefCountForPeerFn(Peer),
            peer_cap_lifecycle.releaseImportRefForPeerFn(Peer),
            Peer.releaseResolvedImport,
            noopSendReleaseForForgottenImport,
        );
    }

    fn noopSendReleaseForForgottenImport(_: *Peer, _: u32, _: u32) anyerror!void {}

    /// Send a Finish message on behalf of a host integration, with explicit
    /// control over `releaseResultCaps` and `requireEarlyCancellation` flags.
    pub fn sendFinishForHost(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
        require_early_cancellation: bool,
    ) !void {
        self.assertThreadAffinity();
        try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
            Peer,
            self,
            question_id,
            release_result_caps,
            require_early_cancellation,
            Peer.sendFrameControl,
        );
    }

    fn sendBuilder(self: *Peer, builder: *protocol.MessageBuilder) !void {
        const bytes = try builder.finish();
        defer self.allocator.free(bytes);
        try self.sendFrame(bytes);
    }

    fn sendBuilderControl(self: *Peer, builder: *protocol.MessageBuilder) !void {
        const bytes = try builder.finish();
        defer self.allocator.free(bytes);
        try self.sendFrameControl(bytes);
    }

    /// Send a protocol-mandated frame (Return, Finish, Release, Resolve,
    /// Disembargo, Abort, Unimplemented).
    ///
    /// Unlike caller-initiated sends — where a full write queue surfaces
    /// `error.WriteQueueFull` / `error.WriteQueueBytesExceeded` to the
    /// caller and the connection stays healthy — dropping a control frame
    /// desynchronizes protocol state with the remote. The only safe
    /// recovery is closing the connection, so enqueue overflow here emits
    /// a peer-level backpressure event and initiates transport close.
    fn sendFrameControl(self: *Peer, frame: []const u8) !void {
        self.sendFrame(frame) catch |err| {
            switch (err) {
                error.WriteQueueFull, error.WriteQueueBytesExceeded => {
                    events.emitBackpressure(
                        self.observer,
                        .peer,
                        .unknown,
                        if (err == error.WriteQueueFull) .write_queue_items else .write_queue_bytes,
                        frame.len,
                        null,
                        err,
                    );
                    log.warn("control frame dropped by saturated write queue; closing connection", .{});
                    self.closeAttachedTransport();
                },
                else => {},
            }
            return err;
        };
    }

    fn sendFrame(self: *Peer, frame: []const u8) !void {
        if (self.send_frame_override) |cb| {
            const ctx = self.send_frame_ctx orelse {
                log.debug("send frame override missing callback context", .{});
                return error.MissingCallbackContext;
            };
            try cb(ctx, frame);
            events.emitFrame(self.observer, .peer, .unknown, .enqueued, frame.len);
            return;
        }
        self.transport.sendFrame(frame) catch |err| {
            if (err == error.TransportNotAttached) {
                log.debug("cannot send frame: transport not attached", .{});
            }
            return err;
        };
        events.emitFrame(self.observer, .peer, .unknown, .enqueued, frame.len);
    }

    fn onOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) anyerror!void {
        const peer: *Peer = castCtx(*Peer, ctx);
        switch (tag) {
            .senderHosted, .senderPromise => try peer.noteExportRef(id),
            else => {},
        }
    }

    fn rollbackOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) void {
        const peer: *Peer = castCtx(*Peer, ctx);
        switch (tag) {
            .senderHosted, .senderPromise => peer.rollbackExportRef(id),
            else => {},
        }
    }

    fn noteExportRef(self: *Peer, id: u32) !void {
        try peer_cap_lifecycle.noteExportRef(
            ExportEntry,
            &self.exports,
            id,
        );
    }

    fn rollbackExportRef(self: *Peer, id: u32) void {
        var entry = self.exports.getEntry(id) orelse return;
        if (entry.value_ptr.ref_count == 0) return;
        entry.value_ptr.ref_count -= 1;
    }

    fn noteAnswerExportRef(self: *Peer, id: u32) !void {
        try peer_cap_lifecycle.noteAnswerExportRef(
            ExportEntry,
            &self.exports,
            id,
        );
    }

    fn rollbackAnswerExportRef(self: *Peer, id: u32) void {
        var entry = self.exports.getEntry(id) orelse return;
        if (entry.value_ptr.answer_ref_count == 0) return;
        entry.value_ptr.answer_ref_count -= 1;
    }

    fn notePromiseExportRef(self: *Peer, id: u32) !void {
        try peer_cap_lifecycle.notePromiseExportRef(
            ExportEntry,
            &self.exports,
            id,
        );
    }

    /// Take one handoff-held (Release-immune) reference on an export. See the
    /// `handoff_ref_count` field doc on `ExportEntry`.
    fn noteHandoffExportRef(self: *Peer, id: u32) !void {
        try peer_cap_lifecycle.noteHandoffExportRef(
            ExportEntry,
            &self.exports,
            id,
        );
    }

    /// Release one handoff-held reference; destroys the entry only when every
    /// ref class is zero AND the entry was wire-granted at least once (so a
    /// Provide+Finish cycle can never destroy an app-held, never-emitted
    /// export). Best-effort by design: unknown id / underflow are logged.
    fn releaseHandoffHeldExport(self: *Peer, id: u32) void {
        const promise_target = self.promiseTargetOf(id);
        const import_target = self.promiseImportTargetOf(id);
        peer_cap_lifecycle.releaseHandoffHeldExport(
            Peer,
            ExportEntry,
            PendingCall,
            self,
            self.allocator,
            &self.exports,
            &self.pending_export_promises,
            self.bootstrap_export_id,
            id,
            peer_cap_lifecycle.clearExportForPeerFn(Peer),
            pending_calls.deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall),
        );
        self.finalizeExportRelease(id, promise_target, import_target);
    }

    fn rollbackPromiseExportRef(self: *Peer, id: u32) void {
        var entry = self.exports.getEntry(id) orelse return;
        if (entry.value_ptr.promise_ref_count == 0) return;
        entry.value_ptr.promise_ref_count -= 1;
    }

    /// The resolution-target export id of a promise export that has resolved to
    /// a concrete export, or null for any other export (unresolved, resolved to
    /// an exception, or a plain hosted cap). Only such a resolution takes a
    /// promise-held ref on its target (see `resolvePromiseExportToExport`), so
    /// this is exactly the set of exports that must release one when destroyed.
    fn promiseTargetOf(self: *Peer, id: u32) ?u32 {
        const entry = self.exports.getEntry(id) orelse return null;
        const resolved = entry.value_ptr.resolved orelse return null;
        return switch (resolved) {
            .exported => |cap| cap.id,
            else => null,
        };
    }

    /// The resolution-target IMPORT id of a promise export that has resolved to
    /// an imported cap (the remote's own export), or null otherwise. Only
    /// `resolvePromiseExportToImport` produces such a resolution, and it takes a
    /// promise-held ref on that import; this is exactly the set of exports that
    /// must release one when destroyed. Parallel to `promiseTargetOf`, but for
    /// the import table rather than the export table.
    fn promiseImportTargetOf(self: *Peer, id: u32) ?u32 {
        const entry = self.exports.getEntry(id) orelse return null;
        const resolved = entry.value_ptr.resolved orelse return null;
        return switch (resolved) {
            .imported => |cap| cap.id,
            else => null,
        };
    }

    /// Called after a release lowered one of export `id`'s ref classes. If that
    /// dropped `id` out of the export table, free its persistence state and —
    /// when `id` was a promise that had resolved — release the promise-held ref
    /// it pinned on its resolution target: `promise_target` for an exported
    /// target (cascading destruction down a resolution chain), or `import_target`
    /// for an imported target (dropping the promise-held import pin). At most one
    /// is set, since a resolution is either exported or imported. Both must be
    /// captured *before* the release (the entry, and its `resolved` field, is
    /// gone by the time we get here).
    fn finalizeExportRelease(self: *Peer, id: u32, promise_target: ?u32, import_target: ?u32) void {
        if (self.exports.contains(id)) return;
        self.dropPersistenceStateForRemovedExport(id);
        if (promise_target) |target_id| {
            if (target_id != id) {
                self.releasePromiseHeldCap(target_id) catch |err| {
                    log.warn("cascade promise-held release for export {} failed: {}", .{ target_id, err });
                };
            }
        }
        if (import_target) |import_id| {
            // Drop the promise-held import pin this destroyed promise export
            // took in resolvePromiseExportToImport. This is a LOCAL lease, not a
            // wire reference: releasing it never sends a Release. The import's
            // own wire references are owned and released separately by whoever
            // received it as a call/return cap.
            _ = self.caps.releasePromiseImportRef(import_id);
        }
    }

    /// Release one promise-held reference on export `id`, taken by
    /// `resolvePromiseExportToExport` when a promise export resolved to `id` as
    /// its target. Destroys the export once no wire, answer, or promise
    /// references remain, cascading if `id` was itself a resolved promise.
    fn releasePromiseHeldCap(self: *Peer, id: u32) !void {
        const promise_target = self.promiseTargetOf(id);
        const import_target = self.promiseImportTargetOf(id);
        try peer_cap_lifecycle.releasePromiseHeldExport(
            Peer,
            ExportEntry,
            PendingCall,
            self,
            self.allocator,
            &self.exports,
            &self.pending_export_promises,
            self.bootstrap_export_id,
            id,
            peer_cap_lifecycle.clearExportForPeerFn(Peer),
            pending_calls.deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall),
        );
        self.finalizeExportRelease(id, promise_target, import_target);
    }

    /// Release one answer-held reference on export `id` (the `count` from the
    /// releaseResultCaps-style frame walk is always 1 per descriptor).
    fn releaseAnswerHeldCap(self: *Peer, id: u32, count: u32) !void {
        _ = count;
        const promise_target = self.promiseTargetOf(id);
        const import_target = self.promiseImportTargetOf(id);
        try peer_cap_lifecycle.releaseAnswerHeldExport(
            Peer,
            ExportEntry,
            PendingCall,
            self,
            self.allocator,
            &self.exports,
            &self.pending_export_promises,
            self.bootstrap_export_id,
            id,
            peer_cap_lifecycle.clearExportForPeerFn(Peer),
            pending_calls.deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall),
        );
        self.finalizeExportRelease(id, promise_target, import_target);
    }

    fn releaseExport(self: *Peer, id: u32, count: u32) !void {
        const promise_target = self.promiseTargetOf(id);
        const import_target = self.promiseImportTargetOf(id);
        try peer_cap_lifecycle.releaseExport(
            Peer,
            ExportEntry,
            PendingCall,
            self,
            self.allocator,
            &self.exports,
            &self.pending_export_promises,
            self.bootstrap_export_id,
            id,
            count,
            peer_cap_lifecycle.clearExportForPeerFn(Peer),
            pending_calls.deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall),
        );
        self.finalizeExportRelease(id, promise_target, import_target);
    }

    fn releaseInboundCaps(self: *Peer, inbound: *cap_table.InboundCapTable) !void {
        try peer_inbound_release.releaseInboundCaps(
            Peer,
            self.allocator,
            self,
            inbound,
            peer_cap_lifecycle.releaseImportRefForPeerFn(Peer),
            Peer.releaseResolvedImport,
            peer_outbound_control.sendReleaseViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
        );
    }

    fn storeResolvedImport(
        self: *Peer,
        promise_id: u32,
        cap: ?cap_table.ResolvedCap,
        embargo_id: ?u32,
        embargoed: bool,
    ) !void {
        const resolved_before = self.resolved_imports.count();
        try ensureCountLimit(
            self.resolved_imports.contains(promise_id),
            resolved_before,
            self.limits.max_resolved_imports,
        );
        try peer_cap_lifecycle.storeResolvedImport(
            Peer,
            ResolvedImport,
            cap_table.ResolvedCap,
            self,
            &self.resolved_imports,
            &self.pending_embargoes,
            promise_id,
            cap,
            embargo_id,
            embargoed,
            Peer.releaseResolvedCap,
        );
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .resolved_imports,
            resolved_before,
            self.resolved_imports.count(),
            self.limits.max_resolved_imports,
        );
    }

    fn rememberPendingEmbargo(self: *Peer, embargo_id: u32, promise_id: u32) !void {
        try ensureCountLimit(
            self.pending_embargoes.contains(embargo_id),
            self.pending_embargoes.count(),
            self.limits.max_pending_embargoes,
        );
        try peer_resolve.rememberPendingEmbargoForPeer(Peer, self, embargo_id, promise_id);
    }

    fn releaseResolvedImport(self: *Peer, promise_id: u32) anyerror!void {
        try peer_cap_lifecycle.releaseResolvedImport(
            Peer,
            ResolvedImport,
            cap_table.ResolvedCap,
            self,
            &self.resolved_imports,
            &self.pending_embargoes,
            promise_id,
            Peer.releaseResolvedCap,
        );
    }

    fn bufferPendingThirdPartyReturn(self: *Peer, answer_id: u32, frame: []const u8) !void {
        try ensureCountLimit(
            self.pending_third_party_returns.contains(answer_id),
            self.pending_third_party_returns.count(),
            self.limits.max_pending_third_party_returns,
        );
        try ensureByteLimit(
            self.pendingThirdPartyReturnBytesExcluding(answer_id),
            frame.len,
            self.limits.max_pending_third_party_return_bytes,
        );
        try peer_third_party_returns.bufferPendingReturnForPeer(Peer, self, answer_id, frame);
    }

    fn handleMissingReturnQuestion(self: *Peer, frame: []const u8, answer_id: u32) !void {
        try peer_third_party.handleMissingReturnQuestion(
            Peer,
            self,
            frame,
            answer_id,
            peer_third_party.isThirdPartyAnswerId,
            peer_third_party_returns.hasPendingReturnForPeerFn(Peer),
            Peer.bufferPendingThirdPartyReturn,
        );
    }

    fn releaseResolvedCap(self: *Peer, resolved: cap_table.ResolvedCap) anyerror!void {
        switch (resolved) {
            .imported => |cap| try self.releaseImport(cap.id, 1),
            else => {},
        }
    }

    fn deliverLoopbackReturn(self: *Peer, frame: []const u8) !void {
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        if (decoded.tag != .@"return") return error.UnexpectedMessage;
        try self.handleReturn(frame, try decoded.asReturn());
    }

    fn resolvePromisedAnswer(self: *Peer, promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
        const entry = self.resolved_answers.get(promised.question_id) orelse return error.PromiseUnresolved;
        var decoded = try protocol.DecodedMessage.init(self.allocator, entry.frame);
        defer decoded.deinit();
        const ret = try decoded.asReturn();
        if (ret.tag != .results or ret.results == null) return error.PromisedAnswerMissing;
        return cap_table.resolvePromisedAnswer(ret.results.?, promised.transform);
    }

    fn releaseResultCaps(self: *Peer, frame: []const u8) !void {
        try peer_cap_lifecycle.releaseResultCaps(
            Peer,
            self,
            self.allocator,
            frame,
            Peer.releaseExport,
        );
    }

    /// Release the answer-held references a recorded resolved answer took on
    /// the exports in its results (see reserveResolvedAnswer). Same frame
    /// walk as releaseResultCaps, but spending answer references, which an
    /// inbound Release message can never touch.
    fn releaseAnswerHeldResultCaps(self: *Peer, frame: []const u8) !void {
        try peer_cap_lifecycle.releaseResultCaps(
            Peer,
            self,
            self.allocator,
            frame,
            Peer.releaseAnswerHeldCap,
        );
    }

    fn allocateQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return self.allocateQuestionWithRestore(ctx, on_return, true);
    }

    /// Allocate an outbound question whose `on_return` callback OWNS and FREES
    /// the ctx (so the question must NOT be restored on a post-callback error —
    /// a restored copy would reference a ctx the callback already freed, a UAF at
    /// teardown). Used by `sendForwardedVineCall` (issue #56): its return callback
    /// frees the relay ctx, and in synchronous loopback the return is processed
    /// INSIDE the send, so `restore_on_return_error` must be false from creation —
    /// it cannot be cleared after the send the way async paths do.
    fn allocateQuestionNoRestore(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return self.allocateQuestionWithRestore(ctx, on_return, false);
    }

    fn allocateQuestionWithRestore(
        self: *Peer,
        ctx: *anyopaque,
        on_return: QuestionCallback,
        restore_on_return_error: bool,
    ) !u32 {
        const questions_before = self.questions.count();
        try ensureCountLimit(false, questions_before, self.limits.max_outbound_questions);
        var deadline_ns: ?i64 = null;
        var started_ns: ?i64 = null;
        if (self.clockNow()) |now| {
            started_ns = now;
            if (self.timeouts.default_call_timeout_ms) |ms| {
                deadline_ns = now + msToNs(ms);
            }
        }
        const question_id = try peer_question_state.allocateQuestion(
            Question,
            &self.questions,
            &self.next_question_id,
            .{
                .ctx = ctx,
                .on_return = on_return,
                .is_loopback = false,
                .deadline_ns = deadline_ns,
                .started_ns = started_ns,
                .restore_on_return_error = restore_on_return_error,
            },
        );
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .outbound_questions,
            questions_before,
            self.questions.count(),
            self.limits.max_outbound_questions,
        );
        return question_id;
    }

    fn removeQuestion(self: *Peer, question_id: u32) void {
        _ = self.questions.remove(question_id);
        // The question is being discarded without a wire Return (send
        // rollback, loopback cancel, test drain): free any param-export
        // record without spending the refs.
        self.freeQuestionParamExports(question_id);
        if (self.is_shutting_down and !self.in_deinit and self.questions.count() == 0) {
            self.completeShutdown();
        }
    }

    fn removeQuestionAndDeinit(self: *Peer, question_id: u32) void {
        if (self.questions.fetchRemove(question_id)) |removed| {
            if (removed.value.deinit_ctx) |deinit_ctx| {
                deinit_ctx(self.allocator, removed.value.ctx);
            }
            self.freeQuestionParamExports(question_id);
        }
        if (self.is_shutting_down and !self.in_deinit and self.questions.count() == 0) {
            self.completeShutdown();
        }
    }

    /// Record, under `question_id`, the wire refs an outbound Call's params
    /// just took on our exports (the senderHosted/senderPromise entries of
    /// `effects.callback_applied`, one ref per occurrence), so the inbound
    /// Return can spend them when it carries `releaseParamCaps = true`.
    ///
    /// Called by the wire senders BEFORE the frame is sent: OOM propagates
    /// and the senders' errdefers (question removal — which frees the record
    /// — plus cap-effects rollback) undo everything, so no partial record
    /// can survive a failed send.
    fn recordQuestionParamExports(
        self: *Peer,
        question_id: u32,
        entries: []const cap_table.OutboundEntry,
    ) !void {
        var ids: std.ArrayList(u32) = .empty;
        errdefer ids.deinit(self.allocator);
        for (entries) |entry| {
            switch (entry.tag) {
                .senderHosted, .senderPromise => try ids.append(self.allocator, entry.id),
                else => {},
            }
        }
        if (ids.items.len == 0) {
            ids.deinit(self.allocator);
            return;
        }
        const slot = try self.question_param_export_refs.getOrPut(question_id);
        if (slot.found_existing) {
            // Question ids are never reused while outstanding, so a stale
            // record here is a bookkeeping bug; free it rather than leak.
            log.warn("replacing stale param-export record for question {}", .{question_id});
            slot.value_ptr.deinit(self.allocator);
        }
        slot.value_ptr.* = ids;
    }

    /// Free (without spending) the param-export record of a question that
    /// died without a wire Return — sender rollback, loopback cancel, drain,
    /// or deinit. The still-held refs reconcile at transport teardown,
    /// exactly as they did before records existed.
    fn freeQuestionParamExports(self: *Peer, question_id: u32) void {
        if (self.question_param_export_refs.fetchRemove(question_id)) |removed| {
            var ids = removed.value;
            ids.deinit(self.allocator);
        }
    }

    /// Consume the param-export record for an inbound Return (rpc.capnp
    /// `Return.releaseParamCaps`, which DEFAULTS to true): the receiver of
    /// our call implicitly dropped the refs our param caps took on our
    /// exports instead of sending explicit Release messages. Spend them
    /// through the same `releaseExport` path an inbound Release frame uses
    /// (over-release therefore surfaces as the existing
    /// `error.ReleaseCountExceeded` protocol error, never an underflow).
    /// With `releaseParamCaps = false` the record is just dropped — the
    /// remote will send explicit Releases later. Runs for every inbound
    /// Return, including ones absorbed for locally cancelled questions, so
    /// the refs are spent exactly once per question.
    fn consumeQuestionParamExports(self: *Peer, answer_id: u32, release_param_caps: bool) !void {
        const removed = self.question_param_export_refs.fetchRemove(answer_id) orelse return;
        var ids = removed.value;
        defer ids.deinit(self.allocator);
        if (!release_param_caps) return;
        // Aggregate duplicate occurrences into one count per export id,
        // mirroring how a Release frame would spend them.
        std.mem.sort(u32, ids.items, {}, std.sort.asc(u32));
        var idx: usize = 0;
        while (idx < ids.items.len) {
            const id = ids.items[idx];
            var count: u32 = 0;
            while (idx < ids.items.len and ids.items[idx] == id) : (idx += 1) {
                count += 1;
            }
            try self.releaseExport(id, count);
        }
    }

    fn onConnectionError(self: *Peer, err: anyerror) void {
        log.debug("connection error: {}", .{err});
        if (self.on_error) |cb| cb(self.callback_ctx, self, err);
    }

    fn onConnectionClose(self: *Peer) void {
        log.debug("connection closed", .{});
        events.emitClose(self.observer, .peer, .unknown, null);
        events.emitConnection(self.observer, .peer, .unknown, .closed);
        // Resolve every still-outstanding question with a synthetic
        // "disconnected" exception Return delivered through its on_return
        // callback, BEFORE the owner's on_close (which typically deinits the
        // peer). Without this a caller awaiting a Return whose transport just
        // dropped hangs forever — a liveness gap and a trivial DoS. The peer's
        // maps are still intact here, so a callback that re-enters the peer is
        // safe; the synthetic Return is delivered only to the question's
        // callback and is never cached as an answer or replayed to pipelining.
        _ = self.forceCancelAllQuestions(disconnected_reason, .disconnected);
        if (self.on_close) |cb| cb(self.callback_ctx, self);
    }

    /// Process a single inbound Cap'n Proto RPC frame.
    ///
    /// Decodes the message tag and dispatches to the appropriate handler
    /// (call, return, finish, resolve, etc.). Unknown message types trigger
    /// an Unimplemented response per the Cap'n Proto RPC spec.
    /// Charge `words` (the traversal words an inbound frame's validation walk
    /// visited) against the per-connection token bucket, refilling by elapsed
    /// time first. Bounds the aggregate validation CPU one connection can drive
    /// with amplifying frames. Inert when the rate is 0 or no Clock is set.
    fn chargeValidationBudget(self: *Peer, words: usize) !void {
        const rate = self.limits.max_validation_words_per_second;
        if (rate == 0) return;
        const now = self.clockNow() orelse return;

        const i64_max: u64 = std.math.maxInt(i64);
        const burst: i64 = @intCast(@min(@as(u64, self.limits.max_validation_burst_words), i64_max));
        if (self.validation_last_refill_ns) |last| {
            const elapsed_ns = now - last;
            if (elapsed_ns > 0) {
                const refill = @divTrunc(@as(i128, elapsed_ns) * @as(i128, @as(u64, rate)), std.time.ns_per_s);
                const refilled = @as(i128, self.validation_tokens) + refill;
                self.validation_tokens = @intCast(@min(refilled, @as(i128, burst)));
            }
        } else {
            // First charged frame: start the bucket full.
            self.validation_tokens = burst;
        }
        self.validation_last_refill_ns = now;

        const cost: i64 = @intCast(@min(@as(u64, words), i64_max));
        if (cost > self.validation_tokens) return error.ValidationBudgetExceeded;
        self.validation_tokens -= cost;
    }

    pub fn handleFrame(self: *Peer, frame: []const u8) !void {
        self.assertThreadAffinity();
        events.emitFrame(self.observer, .peer, .unknown, .received, frame.len);

        // Decode with cost accounting so the validation-work budget is charged
        // even when decoding fails. `decode_cost` holds the traversal words the
        // validating walk spent whether it succeeded, hit an unknown message
        // tag after a full walk, or aborted partway on a malformed frame.
        var decode_cost: usize = 0;
        var decoded = protocol.DecodedMessage.initCounting(self.allocator, frame, &decode_cost) catch |err| {
            // Charge the failed frame's validation work FIRST — before the
            // Unimplemented echo (which re-walks and clones the same payload)
            // and before returning — so a hostile peer cannot get unlimited
            // validation CPU, or amplify through the echo, by sending frames
            // that never dispatch.
            self.chargeValidationBudget(decode_cost) catch |budget_err| {
                log.warn("validation-work budget exceeded on undispatchable frame (words={}); aborting connection", .{decode_cost});
                events.emitProtocolError(self.observer, .peer, .unknown, budget_err, null);
                self.sendAbortForError(budget_err);
                if (!self.isAttachedTransportClosing()) self.closeAttachedTransport();
                return budget_err;
            };
            if (err == error.InvalidMessageTag) {
                log.debug("unknown message tag in frame, sending unimplemented", .{});
                events.emitProtocolError(self.observer, .peer, .unknown, err, null);
                try self.sendUnimplementedForFrame(frame);
                return;
            }
            if (err == error.OutOfMemory) {
                log.debug("failed to decode inbound frame (len={}): {}", .{ frame.len, err });
            } else {
                log.warn("failed to decode inbound frame (len={}): {}", .{ frame.len, err });
            }
            events.emitProtocolError(self.observer, .peer, .unknown, err, null);
            self.sendAbortForError(err);
            return err;
        };
        defer decoded.deinit();

        // Charge this frame's validation cost against the per-connection budget.
        // On exhaustion the peer is spending disproportionate CPU on validation
        // amplification: abort and drop the connection.
        self.chargeValidationBudget(decoded.msg.traversal_words_used) catch |err| {
            log.warn("validation-work budget exceeded (frame words={}); aborting connection", .{decoded.msg.traversal_words_used});
            events.emitProtocolError(self.observer, .peer, .unknown, err, null);
            self.sendAbortForError(err);
            if (!self.isAttachedTransportClosing()) self.closeAttachedTransport();
            return err;
        };

        self.last_inbound_tag = decoded.tag;
        log.debug("dispatching inbound {s}", .{@tagName(decoded.tag)});

        peer_dispatch.dispatchDecodedForPeer(
            Peer,
            self,
            frame,
            &decoded,
            Peer.handleUnimplemented,
            Peer.handleAbort,
            Peer.handleBootstrap,
            Peer.handleCall,
            Peer.handleReturn,
            Peer.handleFinish,
            Peer.handleRelease,
            Peer.handleResolve,
            Peer.handleDisembargo,
            Peer.handleProvide,
            Peer.handleAccept,
            Peer.handleJoin,
            Peer.handleThirdPartyAnswer,
            Peer.sendUnimplemented,
        ) catch |err| {
            if (err == error.OutOfMemory) {
                log.debug("dispatch error for {s}: {}", .{ @tagName(decoded.tag), err });
            } else {
                log.warn("dispatch error for {s}: {}", .{ @tagName(decoded.tag), err });
            }
            switch (err) {
                error.PeerLimitExceeded => events.emitResourceRejection(
                    self.observer,
                    .peer,
                    .unknown,
                    .peer_state,
                    frame.len,
                    null,
                    err,
                ),
                else => events.emitProtocolError(
                    self.observer,
                    .peer,
                    .unknown,
                    err,
                    @tagName(decoded.tag),
                ),
            }
            return err;
        };
    }

    fn sendAbortForError(self: *Peer, err: anyerror) void {
        if (err == error.OutOfMemory) return;
        peer_outbound_control.sendAbortTyped(
            Peer,
            self,
            @errorName(err),
            errors.exceptionTypeForError(err),
            Peer.sendBuilderControl,
        ) catch |abort_err| {
            log.debug("failed to send abort: {}", .{abort_err});
        };
    }

    fn sendUnimplemented(self: *Peer, original: message.AnyPointerReader) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildUnimplementedFromAnyPointer(original);
        try self.sendBuilderControl(&builder);
    }

    fn sendUnimplementedForFrame(self: *Peer, frame: []const u8) !void {
        var msg = try message.Message.initUnvalidated(self.allocator, frame);
        defer msg.deinit();
        const root = try msg.getRootAnyPointer();
        try self.sendUnimplemented(root);
    }

    fn handleUnimplemented(self: *Peer, unimplemented: protocol.Unimplemented) !void {
        if (unimplemented.message_tag == .resolve) {
            const echoed = unimplemented.resolve orelse return;
            try self.handleUnimplementedResolve(echoed);
            return;
        }
        try peer_bootstrap.handleUnimplemented(
            Peer,
            self,
            unimplemented,
            peer_bootstrap.handleUnimplementedQuestionForPeerFn(
                Peer,
                Peer.handleReturn,
            ),
            peer_outbound_control.sendAbortForPeerFn(Peer, Peer.sendBuilderControl),
        );
    }

    /// The remote echoed one of our Resolve messages back inside an
    /// Unimplemented: it does not implement Resolve (a level-0 peer), so it
    /// never picked up the resolution's cap descriptor. Release the wire
    /// reference that descriptor carried (taken in
    /// resolvePromiseExportToExport) through the same releaseExport path an
    /// inbound Release frame uses; otherwise the target export's ref would
    /// leak for the connection lifetime. Outgoing Resolve descriptors are
    /// only ever senderHosted/senderPromise; other descriptor tags carried no
    /// local export reference, and an echoed resolve-to-exception carried no
    /// cap at all, so both are no-ops. A forged echo naming an export with no
    /// spendable refs fails releaseExport's ReleaseCountExceeded guard, the
    /// same trust boundary a bogus Release frame hits.
    fn handleUnimplementedResolve(self: *Peer, echoed: protocol.Resolve) !void {
        if (echoed.tag != .cap) return;
        const descriptor = echoed.cap orelse return;
        switch (descriptor.tag) {
            .senderHosted, .senderPromise => {
                const id = descriptor.id orelse return;
                try self.releaseExport(id, 1);
            },
            else => {},
        }
    }

    fn handleAbort(self: *Peer, abort: protocol.Abort) !void {
        const stored_len = @min(abort.exception.reason.len, self.limits.max_remote_abort_reason_bytes);
        log.debug("received abort from remote (reason_len={}, stored_len={})", .{
            abort.exception.reason.len,
            stored_len,
        });
        const capped_abort = protocol.Abort{
            .exception = .{
                .reason = abort.exception.reason[0..stored_len],
                .trace = abort.exception.trace,
                .type_value = abort.exception.type_value,
            },
        };
        try peer_bootstrap.handleAbort(self.allocator, &self.last_remote_abort_reason, capped_abort);
    }

    fn handleBootstrap(self: *Peer, bootstrap_msg: protocol.Bootstrap) !void {
        try peer_bootstrap.handleBootstrap(
            Peer,
            self,
            self.allocator,
            bootstrap_msg,
            self.bootstrap_export_id,
            inboundQuestionIdInUse,
            noteExportRef,
            rollbackExportRef,
            sendReturnException,
            sendAndRecordBootstrapReturn,
        );
    }

    /// Deliver a Bootstrap results Return and record its resolved answer with
    /// the same reserve → send → commit-or-cleanup discipline as
    /// sendReturnResults: the record resources (count budget, map slot, frame
    /// copy, answer-held export refs) are reserved before the frame is sent so
    /// recording cannot fail once the frame is on the wire, and the id is
    /// marked resolving so a Finish delivered synchronously during the send
    /// leaves a finished-early tombstone for the post-send step instead of
    /// vanishing — without this, the reentrant Finish would find nothing to
    /// clean and the record afterward would strand a resolved answer (plus its
    /// answer-held export reference) that no later Finish could ever clear.
    fn sendAndRecordBootstrapReturn(self: *Peer, question_id: u32, bytes: []const u8) anyerror!void {
        var reservation: ?ResolvedAnswerReservation = try self.reserveResolvedAnswer(question_id, bytes);
        errdefer if (reservation) |r| r.deinit(self);
        try self.resolving_answers.put(question_id, {});
        var resolving_answer = true;
        errdefer if (resolving_answer) {
            _ = self.resolving_answers.remove(question_id);
        };

        try self.sendFrame(bytes);
        _ = self.resolving_answers.remove(question_id);
        resolving_answer = false;

        self.commitOrRollbackResolvedAnswerAfterSend(question_id, bytes, &reservation);
    }

    fn handleFinish(self: *Peer, finish_msg: protocol.Finish) !void {
        const qid = finish_msg.question_id;
        // Vat-wide provision close (Finish of a Provide question) — UNGATED:
        // the map lookup inside is the gate. Runs in this fallible context so
        // an OOM in the fan-out propagates out of dispatch instead of being
        // force-swallowed by the void FinishOps hook below.
        try self.detachProvisionForFinish(qid);
        const was_active = self.active_inbound_questions.remove(qid);
        const was_resolving = self.resolving_answers.contains(qid);
        self.clearPendingJoinResultAnswer(qid);
        try self.clearPendingJoinRelay(qid, true, finish_msg.release_result_caps);
        // Cancellation race: a Finish for an in-flight inbound call (still
        // active, not yet resolved), or for a synchronous Return whose frame is
        // on the wire but whose resolved answer is not committed yet, means the
        // Return sender must not later leave a resolved_answers entry that no
        // further Finish will clear. Preserve releaseResultCaps for the sender
        // so it can either roll back a late async Return or commit+cleanup a
        // synchronous Return after replaying queued promised calls.
        if ((was_active or was_resolving) and
            !self.resolved_answers.contains(qid) and
            self.finished_early_answers.count() < self.limits.max_active_inbound_questions)
        {
            self.finished_early_answers.put(qid, finish_msg.release_result_caps) catch |err| self.reportNonfatalError(err);
        }
        if (!finish_msg.require_early_cancellation) {
            // Default behavior: if Finish arrives before a promised-target call is
            // deliverable, cancel the queued call immediately.
            _ = try self.cancelQueuedPendingQuestion(finish_msg.question_id);
        }
        const ops = peer_finish.FinishOps(Peer){
            .remove_send_results_to_yourself = peer_forward_orchestration.removeSendResultsToYourselfForPeerFn(Peer),
            .clear_send_results_to_third_party = clearSendResultsToThirdParty,
            .clear_provide = peer_provides_state.clearProvideForPeerFn(
                Peer,
                ProvideEntry,
                ProvideTarget,
                ProvideTarget.deinit,
            ),
            .clear_pending_join_question = peer_join_state.clearPendingJoinQuestionForPeerFn(
                Peer,
                JoinState,
                PendingJoinQuestion,
                ProvideTarget,
                ProvideTarget.deinit,
                JoinState.deinit,
            ),
            .clear_pending_accept_question = Peer.clearPendingAcceptQuestionRouted,
            .take_forwarded_tail_question = peer_forward_orchestration.takeForwardedTailQuestionForPeerFn(Peer),
            // The tail Finish is only ever sent for a forwarded question held open
            // by finishForwardResolvedCall; neutralize that question here so a late
            // forwarded return is absorbed rather than re-emitted (see
            // sendTailFinishAndNeutralize / W1).
            .send_finish = Peer.sendTailFinishAndNeutralize,
            .take_resolved_answer_frame = peer_finish.takeResolvedAnswerFrameForPeerFn(Peer),
            .release_answer_caps_for_frame = releaseAnswerHeldResultCaps,
            .release_caps_for_frame = releaseResultCaps,
            .free_frame = peer_finish.freeOwnedFrameForPeerFn(Peer),
        };
        try peer_finish.handleFinishWithOps(
            Peer,
            self,
            finish_msg.question_id,
            finish_msg.release_result_caps,
            ops,
        );
    }

    fn cancelQueuedPendingQuestionInMap(
        self: *Peer,
        pending_map: *std.AutoHashMap(u32, std.ArrayList(PendingCall)),
        question_id: u32,
    ) !bool {
        var canceled = false;
        var empty_keys = std.ArrayList(u32).empty;
        defer empty_keys.deinit(self.allocator);

        var pending_it = pending_map.iterator();
        while (pending_it.next()) |entry| {
            const pending_list = entry.value_ptr;
            var idx: usize = 0;
            while (idx < pending_list.items.len) {
                // Match on the id decoded once at enqueue — no per-scan re-parse.
                if (pending_list.items[idx].question_id != question_id) {
                    idx += 1;
                    continue;
                }

                // orderedRemove, not swapRemove: the surviving queued calls
                // must keep send order so replay honors Cap'n Proto's E-order
                // guarantee (calls to one target delivered in send order).
                var pending_call = pending_list.orderedRemove(idx);
                defer {
                    pending_call.caps.deinit();
                    self.allocator.free(pending_call.frame);
                }
                try self.releaseInboundCaps(&pending_call.caps);
                // Spec: every Call receives exactly one Return, even when
                // cancelled by a Finish. Send Return(canceled) so the caller's
                // question entry can drain (a compliant peer keeps it reserved
                // until this arrives) and shutdown can complete. Non-draining
                // by construction (sendReturnTag does not touch
                // pending_promises), so it is safe while iterating that map.
                self.sendReturnTag(question_id, .canceled) catch |err| {
                    self.reportNonfatalError(err);
                };
                canceled = true;
            }

            if (pending_list.items.len == 0) {
                try empty_keys.append(self.allocator, entry.key_ptr.*);
            }
        }

        for (empty_keys.items) |key| {
            if (pending_map.fetchRemove(key)) |removed| {
                var pending_list = removed.value;
                pending_list.deinit(self.allocator);
            }
        }
        return canceled;
    }

    fn cancelQueuedPendingQuestion(self: *Peer, question_id: u32) !bool {
        const canceled_promised = try self.cancelQueuedPendingQuestionInMap(&self.pending_promises, question_id);
        const canceled_export = try self.cancelQueuedPendingQuestionInMap(&self.pending_export_promises, question_id);
        const canceled = canceled_promised or canceled_export;
        if (canceled) {
            // The cancelled call's answer will never be produced, so any calls
            // pipelined ON it (queued under pending_promises[question_id], and
            // transitively) can never be satisfied. Fail-drain them, each with
            // its own exception Return, so the exactly-one-Return-per-call
            // invariant holds and a stale orphan bucket cannot later replay
            // against an unrelated answer if the remote reuses the id. Run here,
            // outside the pending-map iteration above, since the drain
            // fetchRemoves buckets. Mirrors sendReturnException's drain.
            self.failQueuedPromisedCalls(question_id, "pipelined on a cancelled call", .failed);
        }
        return canceled;
    }

    fn handleRelease(self: *Peer, release: protocol.Release) !void {
        // ORIGINATION: is this Release dropping a vine we minted for a handoff?
        // Snapshot the coupling BEFORE releasing the export, then check whether
        // the release fully dropped the vine (destroyed the export). The vine's
        // Release is the spec's signal that the Accept completed (or was
        // abandoned); once the vine is gone we Finish the paired Provide on the
        // host-of-provided-cap connection, unregistering the provision.
        const coupled = self.outbound_provides.get(release.id);

        try peer_cap_lifecycle.handleRelease(Peer, self, release, releaseExport);

        if (coupled) |entry| {
            if (!self.exports.contains(release.id)) {
                // Vine fully released. Drop the coupling and its handoff mark,
                // then Finish the held-open Provide question on its own peer.
                // Free the stash from the LIVE entry (not the pre-release
                // snapshot): nested delivery during the release may have
                // stashed or flushed since the snapshot was taken.
                if (self.outbound_provides.getPtr(release.id)) |op| op.deinitStash(self.allocator);
                _ = self.outbound_provides.remove(release.id);
                self.caps.clearThirdPartyHosted(release.id);
                // If this vine anchored a promise-export resolution
                // (`resolvePromiseExportToThirdParty`), that promise resolved
                // locally to the now-destroyed vine. Clear the stale target so a
                // later Release of the promise export does not cascade into a
                // vanished vine (benign no-op, but avoids a dangling resolution).
                if (entry.resolved_promise_export_id) |promise_id| {
                    if (self.exports.getEntry(promise_id)) |pe| {
                        if (pe.value_ptr.resolved) |resolved| {
                            if (resolved == .exported and resolved.exported.id == release.id) {
                                pe.value_ptr.resolved = null;
                            }
                        }
                    }
                }
                // LIVENESS (BUG #55): if `provide_peer` is null the
                // host-of-provided-cap peer already deinited — it removed and
                // Finished the held-open Provide question itself and neutralized
                // this coupling — so Finishing is a safe no-op we skip. Otherwise
                // Finish the question and drop the reverse back-link on that peer.
                if (entry.provide_peer) |provide_peer| {
                    provide_peer.deregisterCoupledVine(self, release.id);
                    self.finishOriginatedProvide(provide_peer, entry.provide_question_id);
                }
            }
        }
    }

    /// Finish a Provide question this peer originated, on the peer that owns it
    /// (the host-of-provided-cap connection). Removes the held-open question and
    /// sends the wire `Finish` so the host unregisters the provision. Best
    /// effort: a failed Finish is logged, not propagated — the vine is already
    /// gone and the handoff is over on our side.
    fn finishOriginatedProvide(_: *Peer, provide_peer: *Peer, provide_question_id: u32) void {
        provide_peer.removeQuestion(provide_question_id);
        provide_peer.sendFinishForHost(provide_question_id, false, false) catch |err| {
            log.debug("provide finish send failed for question {}: {}", .{ provide_question_id, err });
        };
    }

    /// LIVENESS (BUG #55). Record a back-link on the host-of-provided-cap peer
    /// (`self` == the peer that owns the held-open Provide question) naming the
    /// recipient peer whose `outbound_provides[vine_id]` entry borrows a pointer
    /// back to `self`. Called when a coupling is created; the paired
    /// `deregisterCoupledVine` drops it when the coupling drains. On `self`'s
    /// `deinit`, `neutralizeCoupledVinesOnProvidePeer` walks these links and
    /// nulls the borrowed pointer so it is never dereferenced after free.
    ///
    /// Returns `error.OutOfMemory` if the back-link cannot be recorded; the
    /// caller must then unwind the coupling rather than leave a half-registered
    /// (untracked, thus un-neutralizable) borrow.
    fn registerCoupledVine(self: *Peer, recipient_peer: *Peer, vine_id: u32) !void {
        try self.coupled_vines.append(self.allocator, .{
            .recipient_peer = recipient_peer,
            .vine_id = vine_id,
        });
    }

    /// LIVENESS (BUG #55). Drop the back-link recorded by `registerCoupledVine`
    /// for `(recipient_peer, vine_id)`. Called when the coupling drains normally
    /// (vine Release on the recipient) or when the recipient peer deinits while
    /// this provide_peer is still alive. Idempotent: a missing link is a no-op
    /// (e.g. already removed, or never registered on a rollback path).
    fn deregisterCoupledVine(self: *Peer, recipient_peer: *Peer, vine_id: u32) void {
        var i: usize = 0;
        while (i < self.coupled_vines.items.len) {
            const link = self.coupled_vines.items[i];
            if (link.recipient_peer == recipient_peer and link.vine_id == vine_id) {
                _ = self.coupled_vines.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    /// LIVENESS (BUG #55). Called from `deinit` on the host-of-provided-cap peer.
    /// For every coupling anchored here, null the borrowed `provide_peer` pointer
    /// in the recipient peer's `outbound_provides` entry so a subsequent vine
    /// Release finds no provide_peer and skips the Finish (a correct no-op — this
    /// peer's own `forceCancelAllQuestions` already removed and Finished the
    /// held-open Provide question). This runs BEFORE this peer's memory is freed,
    /// which is exactly what makes the borrow liveness-safe under arbitrary
    /// per-connection teardown order.
    fn neutralizeCoupledVinesOnProvidePeer(self: *Peer) void {
        for (self.coupled_vines.items) |link| {
            if (link.recipient_peer.outbound_provides.getPtr(link.vine_id)) |op| {
                // Only clear the edge that points back at THIS peer; a recipient
                // could in principle re-key the same vine id to another provide
                // peer, though the id space makes that vanishingly unlikely.
                if (op.provide_peer == self) op.provide_peer = null;
            }
        }
        self.coupled_vines.clearRetainingCapacity();
    }

    fn registerCrossPeerProxy(self: *Peer, owner_peer: *Peer, proxy_export_id: u32) !void {
        try self.cross_peer_proxy_links.append(self.allocator, .{
            .owner_peer = owner_peer,
            .proxy_export_id = proxy_export_id,
        });
    }

    fn deregisterCrossPeerProxy(self: *Peer, owner_peer: *Peer, proxy_export_id: u32) void {
        var i: usize = 0;
        while (i < self.cross_peer_proxy_links.items.len) {
            const link = self.cross_peer_proxy_links.items[i];
            if (link.owner_peer == owner_peer and link.proxy_export_id == proxy_export_id) {
                _ = self.cross_peer_proxy_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    fn neutralizeCrossPeerProxiesOnSourcePeer(self: *Peer) void {
        for (self.cross_peer_proxy_links.items) |link| {
            if (link.owner_peer.exports.getPtr(link.proxy_export_id)) |entry| {
                if (entry.handler) |handler| {
                    const proxy_ctx = castCtx(*CrossPeerProxyContext, handler.ctx);
                    if (proxy_ctx.source_peer == self) {
                        proxy_ctx.source_peer = null;
                        proxy_ctx.release_source_import_id = null;
                        // Abandon (never release): the pinned export lives on
                        // this dying peer; its entry is freed with the peer.
                        proxy_ctx.release_source_export_pin_id = null;
                    }
                }
            }
        }
        self.cross_peer_proxy_links.clearRetainingCapacity();
    }

    fn registerCrossPeerJoinRelay(self: *Peer, owner_peer: *Peer, owner_answer_id: u32) !void {
        try self.cross_peer_join_relay_links.append(self.allocator, .{
            .owner_peer = owner_peer,
            .owner_answer_id = owner_answer_id,
        });
    }

    fn deregisterCrossPeerJoinRelay(self: *Peer, owner_peer: *Peer, owner_answer_id: u32) void {
        var i: usize = 0;
        while (i < self.cross_peer_join_relay_links.items.len) {
            const link = self.cross_peer_join_relay_links.items[i];
            if (link.owner_peer == owner_peer and link.owner_answer_id == owner_answer_id) {
                _ = self.cross_peer_join_relay_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    fn neutralizeCrossPeerJoinRelaysOnSourcePeer(self: *Peer) void {
        for (self.cross_peer_join_relay_links.items) |link| {
            if (link.owner_peer.pending_join_relays.getPtr(link.owner_answer_id)) |relay| {
                if (relay.source_peer == self) relay.source_peer = null;
            }
        }
        self.cross_peer_join_relay_links.clearRetainingCapacity();
    }

    fn registerJoinAcceptHost(self: *Peer, answer_peer: *Peer, answer_id: u32) !void {
        try self.join_accept_host_links.append(self.allocator, .{
            .answer_peer = answer_peer,
            .answer_id = answer_id,
        });
    }

    fn deregisterJoinAcceptHost(self: *Peer, answer_peer: *Peer, answer_id: u32) void {
        var i: usize = 0;
        while (i < self.join_accept_host_links.items.len) {
            const link = self.join_accept_host_links.items[i];
            if (link.answer_peer == answer_peer and link.answer_id == answer_id) {
                _ = self.join_accept_host_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    fn neutralizeJoinAcceptHostLinks(self: *Peer) void {
        for (self.join_accept_host_links.items) |link| {
            if (link.answer_peer.pending_join_result_answers.getPtr(link.answer_id)) |answer| {
                if (answer.accept_peer == self) answer.accept_peer = null;
            }
        }
        self.join_accept_host_links.clearRetainingCapacity();
    }

    fn registerJoinCoordinatorAccept(self: *Peer, coordinator: *JoinCoordinator) !void {
        for (self.join_coordinator_accept_links.items) |link| {
            if (link.coordinator == coordinator) return;
        }
        try self.join_coordinator_accept_links.append(self.allocator, .{
            .coordinator = coordinator,
        });
    }

    fn deregisterJoinCoordinatorAccept(self: *Peer, coordinator: *JoinCoordinator) void {
        var i: usize = 0;
        while (i < self.join_coordinator_accept_links.items.len) {
            const link = self.join_coordinator_accept_links.items[i];
            if (link.coordinator == coordinator) {
                _ = self.join_coordinator_accept_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    fn neutralizeJoinCoordinatorAcceptLinks(self: *Peer) void {
        for (self.join_coordinator_accept_links.items) |link| {
            link.coordinator.neutralizeAcceptedPeer(self);
        }
        self.join_coordinator_accept_links.clearRetainingCapacity();
    }

    fn handleResolve(self: *Peer, resolve_msg: protocol.Resolve) !void {
        // Level-3 recipient auto-pickup: an inbound `thirdPartyHosted` Resolve for
        // a promise we hold, when a VatNetwork + pickup handler are configured,
        // is picked up directly from the third vat instead of proxied through the
        // vine. This branch is fully isolated from the two-party/reflected-
        // loopback resolve path below — it only fires for the thirdPartyHosted
        // descriptor tag with both L3 seams attached, and returns before the
        // generic ops run. Every other resolve (cap/exception, incl. the fragile
        // reflected-loopback `receiverHosted`/`senderHosted` cases) is unchanged.
        if (resolve_msg.tag == .cap) {
            if (resolve_msg.cap) |descriptor| {
                if (descriptor.tag == .thirdPartyHosted and
                    self.vat_network != null and
                    self.on_handoff_pickup != null and
                    self.caps.imports.contains(resolve_msg.promise_id))
                {
                    if (try self.tryAutoPickupThirdParty(resolve_msg.promise_id, descriptor)) {
                        return;
                    }
                    // tryAutoPickup returned false: not actionable (e.g. the
                    // network could not resolve the contact). Fall through to the
                    // Level-1/2 vine fallback so the promise still resolves.
                }
            }
        }

        const ops = peer_resolve.ResolveOps(Peer){
            .has_known_promise = peer_resolve.hasKnownResolvePromiseForPeerFn(Peer),
            .resolve_cap_descriptor = peer_resolve.resolveCapDescriptorForPeerFn(Peer),
            .release_resolved_cap = releaseResolvedCap,
            .alloc_embargo_id = peer_resolve.allocateEmbargoIdForPeerFn(Peer),
            .remember_pending_embargo = Peer.rememberPendingEmbargo,
            .forget_pending_embargo = peer_resolve.forgetPendingEmbargoForPeerFn(Peer),
            .send_disembargo_sender_loopback = peer_outbound_control.sendDisembargoSenderLoopbackViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            .store_resolved_import = storeResolvedImport,
        };
        try peer_resolve.handleResolveWithOps(Peer, self, resolve_msg, ops);
    }

    /// Heap context threaded through the auto-pickup `Accept` question. Owns a
    /// small deferred-release list for failed pickup callbacks; freed by
    /// `onHandoffAcceptReturn` on the normal async path, by the synchronous
    /// sender after nested delivery settles, or by its `deinit_ctx` if the
    /// accept peer tears down first.
    const HandoffPickupContext = struct {
        allocator: std.mem.Allocator,
        /// The peer holding the promise import (VatA↔VatB). Borrowed.
        promise_peer: *Peer,
        /// The promise import id being fulfilled by this handoff.
        promise_id: u32,
        /// The vine import id on `promise_peer`, released after pickup to drive
        /// VatB's Provide Finish.
        vine_id: u32,
        /// Application pickup handler + its ctx, copied from the promise peer.
        user_ctx: *anyopaque,
        user_cb: HandoffPickupCallback,
        /// Stack flag owned by `tryAutoPickupThirdParty` while the Accept send
        /// is in progress. The callback sets it if synchronous loopback has
        /// already freed this ctx and released the vine before sendAccept
        /// returns (or returns an error).
        settled_flag: ?*bool = null,
        /// Captured from the Accept Return so synchronous loopback can delay
        /// the Finish until the host has committed the resolved answer.
        accept_answer_id: ?u32 = null,
        accept_no_finish_needed: bool = false,
        /// Imports from a failed pickup callback that the app did not retain.
        /// In synchronous loopback, these are released by the sender after the
        /// host has committed the Return's export refs; in async delivery, the
        /// callback releases them before freeing this ctx.
        deferred_failed_imports: std.ArrayList(u32) = .empty,

        fn deinitCtx(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
            _ = allocator;
            const ctx: *HandoffPickupContext = @ptrCast(@alignCast(ctx_ptr));
            ctx.deinitSelf();
        }

        fn deinitSelf(ctx: *HandoffPickupContext) void {
            ctx.deferred_failed_imports.deinit(ctx.allocator);
            ctx.allocator.destroy(ctx);
        }

        fn retainFailedUnretainedImports(
            ctx: *HandoffPickupContext,
            accept_caps: *const cap_table.InboundCapTable,
        ) !void {
            var mutable_caps: *cap_table.InboundCapTable = @constCast(accept_caps);
            var idx: u32 = 0;
            while (idx < mutable_caps.len()) : (idx += 1) {
                if (mutable_caps.isRetained(idx)) continue;
                const entry = try mutable_caps.get(idx);
                switch (entry) {
                    .imported => |cap| {
                        try ctx.deferred_failed_imports.append(ctx.allocator, cap.id);
                        try mutable_caps.retainIndex(idx);
                    },
                    else => {},
                }
            }
        }

        fn releaseDeferredFailedImports(ctx: *HandoffPickupContext, accept_peer: *Peer) void {
            for (ctx.deferred_failed_imports.items) |import_id| {
                accept_peer.releaseImport(import_id, 1) catch |release_err| {
                    log.debug("auto-pickup failed-handler cap release failed for promise {}: {}", .{
                        ctx.promise_id,
                        release_err,
                    });
                };
            }
            ctx.deferred_failed_imports.clearRetainingCapacity();
        }

        fn finishAcceptAnswer(ctx: *HandoffPickupContext, accept_peer: *Peer, answer_id: u32, no_finish_needed: bool) void {
            if (no_finish_needed) return;
            var attempts: u8 = 0;
            while (attempts < 2) : (attempts += 1) {
                accept_peer.sendFinishForHost(answer_id, false, false) catch |err| {
                    log.debug("auto-pickup Accept Finish failed for promise {} answer {}: {}", .{
                        ctx.promise_id,
                        answer_id,
                        err,
                    });
                    continue;
                };
                return;
            }
        }
    };

    /// Attempt the Level-3 recipient auto-pickup for an inbound `thirdPartyHosted`
    /// Resolve. Returns true if the pickup was initiated (an `Accept` was sent and
    /// ownership of the vine reference transferred to the pickup flow); false if
    /// the handoff is not actionable and the caller should fall back to the
    /// Level-1/2 vine proxy. On a true return the promise import's vine reference
    /// has been noted and will be released when the Accept `Return` arrives.
    fn tryAutoPickupThirdParty(self: *Peer, promise_id: u32, descriptor: protocol.CapDescriptor) !bool {
        const third = descriptor.third_party orelse return false;
        const contact = third.id orelse return false;
        const network = self.vat_network orelse return false;
        const user_cb = self.on_handoff_pickup orelse return false;
        const user_ctx = self.handoff_pickup_ctx orelse return false;

        // Resolve the ThirdPartyToContact to a live peer connected to the third
        // vat plus the completion to present in the Accept, BEFORE noting the vine
        // import. A network that cannot place the contact returns false, leaving
        // the caller on the vine-proxy fallback (which does its own noteImport) —
        // so we must not have taken a vine ref yet on that path. Every early
        // return below this point is either an error (handled by errdefer) or
        // happens before `noteImport`.
        var introduced = network.connectToIntroduced(contact) catch |err| {
            log.debug("auto-pickup connectToIntroduced failed: {}; using vine fallback", .{err});
            return false;
        };
        defer introduced.deinit(self.allocator);
        const accept_peer = introduced.peer;
        accept_peer.assertThreadAffinity();
        if (accept_peer.is_shutting_down) return false;

        // Account for the wire reference the descriptor hands us on the vine. We
        // own it now and release it once the direct cap is in hand (below /
        // onHandoffAcceptReturn). This is the ref whose eventual Release signals
        // VatB to Finish its Provide (rpc.capnp:1232-1237). Past this point every
        // remaining failure is an error return, so the errdefer covers rollback;
        // there is no non-error `return false` that could leak the ref.
        try self.caps.noteImport(third.vine_id);
        var vine_owned = true;
        errdefer if (vine_owned) {
            self.releaseImport(third.vine_id, 1) catch |err| {
                log.debug("auto-pickup vine rollback release failed: {}", .{err});
            };
        };

        var completion_msg = try message.Message.initUnvalidated(self.allocator, introduced.completion);
        defer completion_msg.deinit();
        const provision = try completion_msg.getRootAnyPointer();

        const heap = try self.allocator.create(HandoffPickupContext);
        heap.* = .{
            .allocator = self.allocator,
            .promise_peer = self,
            .promise_id = promise_id,
            .vine_id = third.vine_id,
            .user_ctx = user_ctx,
            .user_cb = user_cb,
        };
        var heap_owned = true;
        errdefer if (heap_owned) heap.deinitSelf();
        var pickup_settled = false;
        heap.settled_flag = &pickup_settled;

        // PHASE 4 — embargo/disembargo ordering during a live-promise handoff.
        //
        // If VatA (this peer, holding the promise import) still has a pipelined
        // call in flight against the promise being resolved, the accepted cap
        // MUST be embargoed so that the pipelined call (which VatB will forward
        // to VatC on the old path) is delivered to VatC BEFORE any post-pickup
        // direct call — preserving Alice's e-order (rpc.capnp:885-903). We:
        //   (1) allocate an opaque accept-embargo byte id,
        //   (2) send `Accept{embargo=id}` to VatC (VatC holds the Return + any
        //       pipelined calls until the disembargo arrives), and
        //   (3) send `Disembargo{context.accept=id}` to VatB on the promise path;
        //       VatB forwards it to VatC behind the already-forwarded pipelined
        //       call, releasing the held Accept in e-order.
        // If NO call is in flight, keep the P3 fast path (`embargo = null`): there
        // is nothing to order the handoff against.
        var accept_embargo_buf: [ACCEPT_EMBARGO_ID_LEN]u8 = undefined;
        const embargo: ?[]const u8 = if (self.promiseImportHasInFlightCall(promise_id))
            self.nextAcceptEmbargoId(&accept_embargo_buf)
        else
            null;

        // Send the Accept on the third-vat connection. The pickup callback owns
        // and frees `heap`, so allocate the question with no restore from the
        // start: synchronous loopback can deliver the Accept Return before this
        // send call returns, and a callback/post-callback error must not restore
        // a question whose ctx has already been freed. Auto-Finish is also
        // suppressed so the callback can Finish the Accept answer after the
        // handler/vine cleanup while the context is still valid, with a bounded
        // retry for transient send failures.
        const question_id = accept_peer.sendAcceptNoRestore(provision, embargo, heap, onHandoffAcceptReturn, true) catch |err| {
            if (pickup_settled) {
                heap_owned = false;
                vine_owned = false;
                log.debug("auto-pickup Accept settled before send returned trailing error: {}", .{err});
                if (heap.accept_answer_id) |answer_id| {
                    heap.finishAcceptAnswer(accept_peer, answer_id, heap.accept_no_finish_needed);
                }
                heap.releaseDeferredFailedImports(accept_peer);
                heap.deinitSelf();
                return true;
            }
            return err;
        };
        if (pickup_settled) {
            heap_owned = false;
            vine_owned = false;
            heap.finishAcceptAnswer(accept_peer, question_id, heap.accept_no_finish_needed);
            heap.releaseDeferredFailedImports(accept_peer);
            heap.deinitSelf();
            return true;
        }
        heap.settled_flag = null;
        heap_owned = false;
        vine_owned = false; // ownership transferred to the Accept flow.
        accept_peer.setQuestionDeinitCtx(question_id, HandoffPickupContext.deinitCtx);

        // (3) Emit the paired `context.accept` Disembargo on the promise path to
        // VatB. Best-effort AFTER the Accept is safely on the wire and ownership
        // has transferred: a failed Disembargo cannot roll back the sent Accept
        // (that would desync VatC's provide table), and VatC would then simply
        // hold the embargoed Accept until a later Finish/teardown drains it. We
        // therefore log rather than propagate.
        if (embargo) |embargo_bytes| {
            self.sendHandoffAcceptDisembargo(promise_id, embargo_bytes) catch |err| {
                log.warn("auto-pickup: accept-disembargo send failed for promise {}: {}", .{ promise_id, err });
            };
        }

        log.debug("auto-pickup: sent Accept question={} for promise={} vine={} embargoed={}", .{
            question_id,
            promise_id,
            third.vine_id,
            embargo != null,
        });
        return true;
    }

    /// Length of an opaque accept-embargo byte id. A big-endian encoding of the
    /// per-peer `next_accept_embargo_id` counter: unique per handoff on this
    /// peer, which is all VatC needs to correlate the `Accept` with its
    /// `context.accept` `Disembargo`.
    const ACCEPT_EMBARGO_ID_LEN = 8;

    /// Fill `buf` with the next opaque accept-embargo byte id and return it.
    fn nextAcceptEmbargoId(self: *Peer, buf: *[ACCEPT_EMBARGO_ID_LEN]u8) []const u8 {
        const id = self.next_accept_embargo_id;
        self.next_accept_embargo_id +%= 1;
        std.mem.writeInt(u64, buf, id, .big);
        return buf[0..];
    }

    /// True if this peer has an outbound Call still in flight (Return not yet
    /// received) that targeted the promise import `promise_id`. The mark is set
    /// on the `Question` by `sendCall` (see `target_promise_import`) and cleared
    /// implicitly when the question leaves the table, so a live matching question
    /// is exactly an in-flight pipelined call against the promise.
    fn promiseImportHasInFlightCall(self: *Peer, promise_id: u32) bool {
        var it = self.questions.valueIterator();
        while (it.next()) |q| {
            if (q.cancelled) continue;
            if (q.target_promise_import == promise_id) return true;
        }
        return false;
    }

    /// Send a `Disembargo{context.accept}` on the promise path (this peer, the
    /// VatA↔VatB connection) targeting the promise import being handed off. VatB
    /// forwards it to VatC (see `handleAcceptDisembargo`/`forwardAcceptDisembargo`),
    /// which releases the matching held `Accept`. The target names the promise import
    /// (an entry in the sender's import table → the receiver's export table),
    /// matching how VatB keys its promise export.
    fn sendHandoffAcceptDisembargo(self: *Peer, promise_id: u32, embargo: []const u8) !void {
        const target = protocol.MessageTarget{
            .tag = .importedCap,
            .imported_cap = promise_id,
            .promised_answer = null,
        };
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildDisembargoAccept(target, embargo);
        try self.sendBuilder(&builder);
    }

    /// Return callback for the auto-pickup `Accept`. Runs on the third-vat peer
    /// (VatA↔VatC). Delivers the accepted direct capability to the application's
    /// pickup handler, then releases the vine on the promise peer (VatA↔VatB) —
    /// the wire signal that drives VatB's Provide Finish.
    fn onHandoffAcceptReturn(
        ctx_ptr: *anyopaque,
        accept_peer: *Peer,
        ret: protocol.Return,
        accept_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *HandoffPickupContext = castCtx(*HandoffPickupContext, ctx_ptr);
        const defer_to_sender = ctx.settled_flag != null;
        ctx.accept_answer_id = ret.answer_id;
        ctx.accept_no_finish_needed = ret.no_finish_needed;
        defer if (!defer_to_sender) ctx.deinitSelf();
        defer if (!defer_to_sender) ctx.finishAcceptAnswer(accept_peer, ret.answer_id, ret.no_finish_needed);
        const promise_peer = ctx.promise_peer;

        // Deliver the direct cap to the app FIRST (so it retains the accepted
        // import on `accept_peer` before we touch the vine), then release the
        // vine regardless of handler outcome — the pickup is complete on the wire
        // and the vine's job (holding the third-party cap alive until pickup) is
        // done. Releasing it drives VatB's Provide Finish.
        var handler_err: ?anyerror = null;
        ctx.user_cb(ctx.user_ctx, promise_peer, ctx.promise_id, accept_peer, ret, accept_caps) catch |err| {
            handler_err = err;
            log.debug("auto-pickup handler failed for promise {}: {}", .{ ctx.promise_id, err });
        };

        promise_peer.releaseImport(ctx.vine_id, 1) catch |err| {
            log.debug("auto-pickup vine release failed for promise {}: {}", .{ ctx.promise_id, err });
        };

        if (ctx.settled_flag) |flag| flag.* = true;
        if (handler_err) |err| {
            ctx.retainFailedUnretainedImports(accept_caps) catch |retain_err| {
                log.debug("auto-pickup failed-handler deferred release capture failed for promise {}: {}", .{
                    ctx.promise_id,
                    retain_err,
                });
                return retain_err;
            };
            log.debug("auto-pickup handler error cleaned up for promise {}: {}", .{ ctx.promise_id, err });
            if (!defer_to_sender) ctx.releaseDeferredFailedImports(accept_peer);
        }
    }

    fn hasKnownDisembargoTarget(self: *Peer, target: protocol.MessageTarget) bool {
        return switch (target.tag) {
            .importedCap => blk: {
                // Per the RPC spec, MessageTarget.importedCap names an entry in
                // the *sender's* import table — i.e. one of OUR exports. This
                // gate only runs on the senderLoopback path (see
                // disembargo.zig:61), and the only senderLoopback disembargo a
                // compliant peer originates targets the promise it imported from
                // us as it resolves (resolve.zig:88-94 sets
                // imported_cap = promise_id). On the wire that id is the promise
                // we EXPORTED, so it is always an entry in our export table when
                // the disembargo is legitimate — never an import. Validate
                // against exports only; accepting an import id too would over-
                // accept a target the spec never names here (supported-surface.md
                // known limitation #2).
                const cap_id = target.imported_cap orelse break :blk false;
                break :blk self.exports.contains(cap_id);
            },
            .promisedAnswer => blk: {
                const promised = target.promised_answer orelse break :blk false;
                break :blk self.resolved_answers.contains(promised.question_id) or
                    self.pending_promises.contains(promised.question_id) or
                    self.send_results_to_yourself.contains(promised.question_id) or
                    self.send_results_to_third_party.contains(promised.question_id);
            },
        };
    }

    fn handleDisembargo(self: *Peer, disembargo_msg: protocol.Disembargo) !void {
        const ops = peer_disembargo.DisembargoOps(Peer){
            .has_known_disembargo_target = Peer.hasKnownDisembargoTarget,
            .send_disembargo_receiver_loopback = peer_outbound_control.sendDisembargoReceiverLoopbackViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            .take_pending_embargo_promise = peer_disembargo.takePendingEmbargoPromiseForPeerFn(Peer),
            .clear_resolved_import_embargo = peer_disembargo.clearResolvedImportEmbargoForPeerFn(Peer),
            .release_embargoed_accepts = Peer.handleAcceptDisembargo,
        };
        try peer_disembargo.handleDisembargoWithOps(Peer, self, disembargo_msg, ops);
    }

    /// Dispatch an inbound `context.accept` Disembargo. Role is decided by state,
    /// not by the target (which is meaningless once forwarded off the promise
    /// path): a vat is the capability HOST for this embargo iff it holds a queued
    /// `Accept` under these exact embargo bytes.
    ///
    ///   * HOST (VatC): holds a pending embargoed `Accept` for `embargo`. Release
    ///     it, delivering the queued Return + parked pipelined calls in e-order
    ///     (rpc.capnp:900-903).
    ///
    ///   * INTRODUCER (VatB): does NOT hold a matching `Accept`. The Disembargo
    ///     arrived on the promise path this peer used to originate a
    ///     `Provide`+`thirdPartyHosted` Resolve; forward it to the capability host
    ///     on the connection the paired `Provide` was sent on, behind the
    ///     already-forwarded pipelined call (rpc.capnp:889-899). The `target`
    ///     names this peer's promise EXPORT (VatA's promise import id), matched
    ///     against the `resolved_promise_export_id` recorded at origination.
    ///
    /// Deciding by "do I hold this Accept?" rather than by target-matching keeps
    /// the two roles unambiguous even for a vat that is simultaneously an
    /// introducer for one handoff and a host for another. A Disembargo that
    /// matches neither role is a benign no-op (the release drains nothing and no
    /// forward target is found).
    fn handleAcceptDisembargo(self: *Peer, target: protocol.MessageTarget, embargo: []const u8) !void {
        // HOST role: we hold the queued Accept for this embargo → release it.
        if (self.pending_accepts_by_embargo.contains(embargo)) {
            try peer_embargo_accepts.releaseEmbargoedAcceptsForPeer(
                Peer,
                PendingEmbargoedAccept,
                ProvideEntry,
                ProvideTarget,
                self,
                embargo,
                Peer.sendReturnProvidedTarget,
                Peer.sendReturnException,
            );
            return;
        }

        // HOST role, spec form: the Disembargo addresses the Provide question
        // itself (promisedAnswer target in OUR answer space, no transform) —
        // gated on the per-peer LOOKUP, never the index field, so a matched
        // handoff keeps releasing after index death, and no-index peers
        // simply miss (their map is empty) and fall through.
        if (target.tag == .promisedAnswer) {
            if (target.promised_answer) |pa| {
                if (pa.transform.len() != 0) {
                    peer_outbound_control.sendAbortViaSendFrame(Peer, self, "accept disembargo must not carry a transform", Peer.sendFrameControl) catch |send_err| {
                        log.debug("disembargo transform abort send failed: {}", .{send_err});
                    };
                    return error.InvalidDisembargoTarget;
                }
                if (self.provisions_by_question.get(pa.question_id)) |prov| {
                    try self.releaseProvisionEmbargo(prov, embargo);
                    return;
                }
                // Unknown qid: fall through to the drop arm (a late/duplicate
                // Disembargo after Finish is benign).
            }
        }

        // INTRODUCER role: forward toward the host we originated the Provide on.
        if (target.tag == .importedCap) {
            if (target.imported_cap) |promise_export_id| {
                if (self.findOriginatedProvideForPromise(promise_export_id)) |op| {
                    if (op.provide_peer) |provide_peer| {
                        // E-ORDER (M-11): hold the forward until the parked
                        // pre-resolution calls have been replayed toward the
                        // host, or the released Accept overtakes them.
                        if (!op.replay_flushed) {
                            try op.stashAcceptDisembargo(self.allocator, embargo);
                            return;
                        }
                        // Copy out of the map entry before sending: the nested
                        // delivery may mutate `outbound_provides` and
                        // invalidate `op`.
                        const provide_question_id = op.provide_question_id;
                        try self.forwardAcceptDisembargo(provide_peer, provide_question_id, embargo);
                        return;
                    }
                }
            }
        }

        // Neither role: nothing to release, nothing to forward. Benign — a late
        // or duplicate Disembargo whose Accept already drained (or a target we do
        // not recognise). Dropping it silently matches the receiver-loopback arm.
        log.debug("accept-disembargo matched no held Accept and no originated provide; dropping", .{});
    }

    /// If this peer originated a three-party handoff by resolving the promise
    /// EXPORT `promise_export_id` to a third party, return that coupling.
    /// Otherwise null — this peer is not the introducer for that promise. The
    /// returned pointer aliases the `outbound_provides` map entry: copy what
    /// you need out of it before any send (nested delivery can mutate the map).
    fn findOriginatedProvideForPromise(self: *Peer, promise_export_id: u32) ?*OutboundProvide {
        var it = self.outbound_provides.valueIterator();
        while (it.next()) |op| {
            if (op.resolved_promise_export_id == promise_export_id) return op;
        }
        return null;
    }

    /// Forward a `context.accept` Disembargo to the capability host (VatC) on
    /// the connection this introducer (VatB) sent the paired `Provide` on. The
    /// recipient's original target names its promise import in the A↔B id space
    /// and is meaningless on this hop; per rpc.capnp:899 (and matching the C++
    /// reference host, which resolves the target against its answer table) the
    /// forwarded frame is REWRITTEN to address the Provide question itself: a
    /// promisedAnswer target in the B↔C answer space, with no transform.
    fn forwardAcceptDisembargo(_: *Peer, provide_peer: *Peer, provide_question_id: u32, embargo: []const u8) !void {
        provide_peer.assertThreadAffinity();
        const target = protocol.MessageTarget{
            .tag = .promisedAnswer,
            .imported_cap = null,
            .promised_answer = .{ .question_id = provide_question_id, .transform = .{ .list = null } },
        };
        var builder = protocol.MessageBuilder.init(provide_peer.allocator);
        defer builder.deinit();
        try builder.buildDisembargoAccept(target, embargo);
        try provide_peer.sendBuilder(&builder);
    }

    /// Flush (or arm) the accept-Disembargo path for a resolve-originated
    /// coupling: mark the parked-call replay as done, and if a Disembargo was
    /// stashed while the replay was pending, forward it now. Called on BOTH the
    /// success path of `resolvePromiseExportToThirdParty` (after the replay)
    /// and its post-Resolve error paths — once the Resolve is on the wire, a
    /// stashed Disembargo must never be stranded (the recipient's Accept would
    /// hang forever behind it). Best-effort: a forward failure is logged, not
    /// propagated.
    fn flushStashedAcceptDisembargo(self: *Peer, vine_id: u32) void {
        const op = self.outbound_provides.getPtr(vine_id) orelse return;
        op.replay_flushed = true;
        const stash = op.stashed_accept_disembargo orelse return;
        op.stashed_accept_disembargo = null;
        defer self.allocator.free(stash);
        const provide_peer = op.provide_peer orelse return;
        const provide_question_id = op.provide_question_id;
        self.forwardAcceptDisembargo(provide_peer, provide_question_id, stash) catch |err| {
            log.warn("failed to flush stashed accept-disembargo for vine {}: {}", .{ vine_id, err });
        };
    }

    fn makeProvideTarget(self: *Peer, resolved: cap_table.ResolvedCap) !ProvideTarget {
        const descriptors = cap_table.descriptors;
        return switch (resolved) {
            .none => error.PromisedAnswerMissing,
            .imported => |cap| .{ .local = .{ .origin_code = descriptors.originCodeForTag(.receiverHosted), .cap_id = cap.id } },
            // Preserve the export-promise sub-classification (see exportedCapTag).
            .exported => |cap| .{ .local = .{ .origin_code = descriptors.originCodeForTag(self.exportedCapTag(cap.id)), .cap_id = cap.id } },
            .promised => |promised| .{
                .promised = try cap_table.OwnedPromisedAnswer.fromPromised(self.allocator, promised),
            },
        };
    }

    fn cloneProvideTarget(self: *Peer, target: *const ProvideTarget) !ProvideTarget {
        return switch (target.*) {
            .local => |local| .{ .local = local },
            .promised => |promised| .{
                .promised = try cap_table.OwnedPromisedAnswer.fromQuestionAndOps(
                    self.allocator,
                    promised.question_id,
                    promised.ops,
                ),
            },
        };
    }

    fn putPendingJoinAcceptOwned(self: *Peer, provision: []u8, target: ProvideTarget) !void {
        try ensureCountLimit(
            self.pending_join_accepts.contains(provision),
            self.pending_join_accepts.count(),
            self.limits.max_pending_join_accepts,
        );
        try ensureByteLimit(
            self.pendingJoinAcceptKeyBytes(),
            provision.len,
            self.limits.max_pending_join_accept_bytes,
        );

        const entry = try self.pending_join_accepts.getOrPut(provision);
        if (entry.found_existing) return error.DuplicateJoinProvision;
        entry.value_ptr.* = target;
    }

    fn takePendingJoinAccept(self: *Peer, provision: []const u8) ?ProvideTarget {
        if (self.pending_join_accepts.fetchRemove(provision)) |removed| {
            if (self.join_network) |network| network.cancelHostJoinResult(removed.key);
            self.allocator.free(removed.key);
            return removed.value;
        }
        return null;
    }

    fn clearPendingJoinAccept(self: *Peer, provision: []const u8) void {
        if (self.takePendingJoinAccept(provision)) |target_value| {
            var target = target_value;
            target.deinit(self.allocator);
        }
    }

    fn rememberPendingJoinResultAnswer(
        self: *Peer,
        answer_id: u32,
        accept_peer: *Peer,
        provision: []const u8,
    ) !void {
        try ensureCountLimit(
            self.pending_join_result_answers.contains(answer_id),
            self.pending_join_result_answers.count(),
            self.limits.max_pending_join_questions,
        );
        try ensureByteLimit(
            self.pendingJoinResultAnswerBytesExcluding(answer_id),
            provision.len,
            self.limits.max_pending_join_accept_bytes,
        );

        const copy = try self.allocator.dupe(u8, provision);
        errdefer self.allocator.free(copy);
        const entry = try self.pending_join_result_answers.getOrPut(answer_id);
        if (entry.found_existing) return error.DuplicateJoinQuestionId;
        entry.value_ptr.* = .{
            .accept_peer = accept_peer,
            .provision = copy,
        };
        if (accept_peer != self) {
            errdefer {
                _ = self.pending_join_result_answers.remove(answer_id);
            }
            try accept_peer.registerJoinAcceptHost(self, answer_id);
        }
    }

    fn clearPendingJoinResultAnswer(self: *Peer, answer_id: u32) void {
        const removed = self.pending_join_result_answers.fetchRemove(answer_id) orelse return;
        defer self.allocator.free(removed.value.provision);
        if (removed.value.accept_peer) |accept_peer| {
            if (accept_peer != self) accept_peer.deregisterJoinAcceptHost(self, answer_id);
        }

        var still_live = false;
        var it = self.pending_join_result_answers.valueIterator();
        while (it.next()) |answer| {
            if (answer.accept_peer == removed.value.accept_peer and
                std.mem.eql(u8, answer.provision, removed.value.provision))
            {
                still_live = true;
                break;
            }
        }

        if (!still_live) {
            if (removed.value.accept_peer) |accept_peer| {
                accept_peer.clearPendingJoinAccept(removed.value.provision);
            }
        }
    }

    fn forgetPendingJoinResultAnswer(self: *Peer, answer_id: u32) void {
        if (self.pending_join_result_answers.fetchRemove(answer_id)) |removed| {
            if (removed.value.accept_peer) |accept_peer| {
                if (accept_peer != self) accept_peer.deregisterJoinAcceptHost(self, answer_id);
            }
            self.allocator.free(removed.value.provision);
        }
    }

    fn sendReturnJoinResultPayload(self: *Peer, answer_id: u32, result_payload: []const u8) !void {
        const BuildCtx = struct {
            allocator: std.mem.Allocator,
            result_payload: []const u8,

            fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const ctx: *const @This() = castCtx(*const @This(), ctx_ptr);
                var result_msg = try message.Message.initUnvalidated(ctx.allocator, ctx.result_payload);
                defer result_msg.deinit();
                const result = try result_msg.getRootAnyPointer();

                var payload = try ret.payloadTyped();
                const content = try payload.initContent();
                try message.cloneAnyPointer(result, content);
                _ = try ret.initCapTableTyped(0);
            }
        };

        var ctx = BuildCtx{
            .allocator = self.allocator,
            .result_payload = result_payload,
        };
        try self.sendReturnResults(answer_id, &ctx, BuildCtx.build);
    }

    fn crossPeerProxyContextForExport(self: *Peer, export_id: u32) ?*CrossPeerProxyContext {
        const entry = self.exports.getPtr(export_id) orelse return null;
        const handler = entry.handler orelse return null;
        if (handler.on_call != CrossPeerProxyContext.onCall) return null;
        return castCtx(*CrossPeerProxyContext, handler.ctx);
    }

    fn crossPeerJoinTargetForResolved(target: cap_table.ResolvedCap) !protocol.MessageTarget {
        return switch (target) {
            .imported => |cap| .{
                .tag = .importedCap,
                .imported_cap = cap.id,
                .promised_answer = null,
            },
            .exported, .promised, .none => error.UnsupportedCrossPeerJoinTarget,
        };
    }

    fn putPendingJoinRelay(
        self: *Peer,
        owner_answer_id: u32,
        source_peer: *Peer,
        source_question_id: u32,
    ) !void {
        try ensureCountLimit(
            self.pending_join_relays.contains(owner_answer_id),
            self.pending_join_relays.count(),
            self.limits.max_pending_join_questions,
        );
        const entry = try self.pending_join_relays.getOrPut(owner_answer_id);
        if (entry.found_existing) return error.DuplicateJoinQuestionId;
        entry.value_ptr.* = .{
            .source_peer = source_peer,
            .source_question_id = source_question_id,
        };
        errdefer _ = self.pending_join_relays.remove(owner_answer_id);
        try source_peer.registerCrossPeerJoinRelay(self, owner_answer_id);
    }

    fn clearPendingJoinRelay(
        self: *Peer,
        owner_answer_id: u32,
        send_downstream_finish: bool,
        release_result_caps: bool,
    ) !void {
        const relay = self.pending_join_relays.getPtr(owner_answer_id) orelse return;
        const source_peer = relay.source_peer;
        const source_question_id = relay.source_question_id;
        if (source_peer) |peer| {
            if (send_downstream_finish) {
                try peer.sendJoinRelayFinishAndNeutralize(
                    source_question_id,
                    release_result_caps,
                );
            }
            peer.deregisterCrossPeerJoinRelay(self, owner_answer_id);
        }
        _ = self.pending_join_relays.remove(owner_answer_id);
    }

    fn tryHandleCrossPeerProxyJoin(self: *Peer, join: protocol.Join) !bool {
        if (join.target.tag != .importedCap) return false;
        const export_id = join.target.imported_cap orelse return false;
        const proxy_ctx = self.crossPeerProxyContextForExport(export_id) orelse return false;
        const source_peer = proxy_ctx.source_peer orelse {
            try self.sendReturnException(join.question_id, "cross-peer proxy source disconnected");
            return true;
        };
        if (source_peer.is_shutting_down) {
            try self.sendReturnException(join.question_id, "cross-peer proxy source disconnected");
            return true;
        }
        try self.forwardCrossPeerProxyJoin(join, source_peer, proxy_ctx.target);
        return true;
    }

    fn forwardCrossPeerProxyJoin(
        self: *Peer,
        join: protocol.Join,
        source_peer: *Peer,
        source_target: cap_table.ResolvedCap,
    ) !void {
        const downstream_target = crossPeerJoinTargetForResolved(source_target) catch |err| {
            try self.sendReturnException(join.question_id, @errorName(err));
            return;
        };

        const relay = try source_peer.allocator.create(CrossPeerJoinRelayContext);
        relay.* = .{
            .owner_peer = self,
            .owner_answer_id = join.question_id,
        };
        var relay_owned = true;
        errdefer if (relay_owned) source_peer.allocator.destroy(relay);

        const source_question_id = try source_peer.allocateQuestionNoRestore(relay, onCrossPeerJoinReturn);
        var question_owned = true;
        errdefer if (question_owned) source_peer.removeQuestionAndDeinit(source_question_id);

        const source_question = source_peer.questions.getPtr(source_question_id) orelse return error.MissingAllocatedQuestion;
        source_question.suppress_auto_finish = true;
        source_question.deinit_ctx = CrossPeerJoinRelayContext.deinit;
        relay_owned = false;

        try self.putPendingJoinRelay(join.question_id, source_peer, source_question_id);
        var relay_registered = true;
        errdefer if (relay_registered) {
            self.clearPendingJoinRelay(join.question_id, false, false) catch |err| {
                log.debug("cross-peer join relay: failed to roll back relay {}: {}", .{ join.question_id, err });
            };
        };

        var relay_settled = false;
        relay.settled_flag = &relay_settled;

        var builder = protocol.MessageBuilder.init(source_peer.allocator);
        defer builder.deinit();
        try builder.buildJoin(source_question_id, downstream_target, join.key_part);
        source_peer.sendBuilder(&builder) catch |err| {
            if (relay_settled) {
                question_owned = false;
                relay_registered = false;
                return;
            }
            relay_registered = false;
            self.clearPendingJoinRelay(join.question_id, false, false) catch |clear_err| {
                log.debug("cross-peer join relay: failed to clear relay {} after send failure: {}", .{
                    join.question_id,
                    clear_err,
                });
            };
            question_owned = false;
            source_peer.removeQuestionAndDeinit(source_question_id);
            try self.sendReturnException(join.question_id, @errorName(err));
            return;
        };

        if (relay_settled) {
            question_owned = false;
            relay_registered = false;
            return;
        }
        relay.settled_flag = null;
        question_owned = false;
        relay_registered = false;
    }

    fn onCrossPeerJoinReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *CrossPeerJoinRelayContext = castCtx(*CrossPeerJoinRelayContext, ctx_ptr);
        const owner_peer = ctx.owner_peer;
        const owner_answer_id = ctx.owner_answer_id;
        if (ctx.settled_flag) |flag| flag.* = true;
        defer CrossPeerJoinRelayContext.deinit(peer.allocator, ctx);

        if (!owner_peer.pending_join_relays.contains(owner_answer_id)) return;

        switch (ret.tag) {
            .results => {
                relayReturnAcrossPeers(owner_peer, owner_answer_id, peer, ret, inbound_caps, true) catch |err| {
                    owner_peer.clearPendingJoinRelay(owner_answer_id, true, false) catch |clear_err| {
                        log.debug("cross-peer join relay: failed to finish downstream question after relay error: {}", .{clear_err});
                    };
                    owner_peer.sendReturnException(owner_answer_id, @errorName(err)) catch |send_err| {
                        log.debug("cross-peer join relay: failed to fail upstream question {}: {}", .{
                            owner_answer_id,
                            send_err,
                        });
                    };
                };
            },
            .exception => {
                const reason = if (ret.exception) |exception| exception.reason else "cross-peer join failed";
                owner_peer.sendReturnException(owner_answer_id, reason) catch |send_err| {
                    log.debug("cross-peer join relay: failed to relay exception for question {}: {}", .{
                        owner_answer_id,
                        send_err,
                    });
                };
                owner_peer.clearPendingJoinRelay(owner_answer_id, true, false) catch |clear_err| {
                    log.debug("cross-peer join relay: failed to finish exception result {}: {}", .{
                        owner_answer_id,
                        clear_err,
                    });
                };
            },
            else => {
                owner_peer.sendReturnException(owner_answer_id, "cross-peer join relay: unexpected return") catch |send_err| {
                    log.debug("cross-peer join relay: failed to fail unexpected return for question {}: {}", .{
                        owner_answer_id,
                        send_err,
                    });
                };
                owner_peer.clearPendingJoinRelay(owner_answer_id, true, false) catch |clear_err| {
                    log.debug("cross-peer join relay: failed to finish unexpected result {}: {}", .{
                        owner_answer_id,
                        clear_err,
                    });
                };
            },
        }
    }

    fn queueEmbargoedAccept(
        self: *Peer,
        answer_id: u32,
        provided_question_id: u32,
        embargo: []const u8,
    ) !void {
        try ensureCountLimit(
            self.pending_accept_embargo_by_question.contains(answer_id),
            self.pending_accept_embargo_by_question.count(),
            self.limits.max_pending_accepts,
        );
        if (!self.pending_accepts_by_embargo.contains(embargo)) {
            try ensureCountLimit(
                false,
                self.pending_accepts_by_embargo.count(),
                self.limits.max_pending_accept_embargo_buckets,
            );
            try ensureByteLimit(
                self.pendingAcceptEmbargoKeyBytes(),
                embargo.len,
                self.limits.max_pending_accept_embargo_bytes,
            );
        }

        try peer_embargo_accepts.queueEmbargoedAcceptForPeer(
            Peer,
            PendingEmbargoedAccept,
            self,
            answer_id,
            provided_question_id,
            embargo,
        );
    }

    fn handleProvide(self: *Peer, provide: protocol.Provide) !void {
        // A Provide must not reuse a question id already live as a Call /
        // Bootstrap answer or a pending Join (spec violation). Same-type
        // (provide) collisions fall through to the orchestration's specific
        // "duplicate provide question" abort below.
        if (try self.inboundAnswerQuestionIdInUse(provide.question_id) or
            self.pending_join_questions.contains(provide.question_id))
        {
            return error.DuplicateQuestionId;
        }
        try peer_provide_join_orchestration.handleProvide(
            Peer,
            ProvideEntry,
            ProvideTarget,
            self,
            self.allocator,
            provide,
            &self.provides_by_question,
            &self.provides_by_key,
            peer_provide_join_orchestration.captureProvideRecipientForPeerFn(
                Peer,
                peer_third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            peer_finish.freeOwnedFrameForPeerFn(Peer),
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            Peer.ensureProvideBudget,
            peer_provide_accept_join.resolveProvideTargetForPeerFn(
                Peer,
                peer_provide_accept_join.resolveProvideImportedCapForPeerFn(Peer),
                peer_provide_accept_join.resolveProvidePromisedAnswerForPeerFn(Peer, Peer.resolvePromisedAnswer),
            ),
            makeProvideTarget,
            ProvideTarget.deinit,
        );

        // Vat-wide registration (index mode only). On failure, roll the
        // just-stored per-peer provide back, send ONE abort with the matched
        // reason, and propagate the ORIGINAL error.
        if (self.provision_index) |idx| {
            var adopted: ?*ProvisionIndex.Provision = null;
            self.registerProvisionForProvide(idx, provide.question_id, &adopted) catch |err| {
                peer_provides_state.clearProvideForPeer(
                    Peer,
                    ProvideEntry,
                    ProvideTarget,
                    self,
                    provide.question_id,
                    ProvideTarget.deinit,
                );
                const reason: []const u8 = switch (err) {
                    error.DuplicateProvideRecipient => "duplicate provide recipient",
                    else => @errorName(err),
                };
                peer_outbound_control.sendAbortViaSendFrame(Peer, self, reason, Peer.sendFrameControl) catch |send_err| {
                    log.debug("provide registration abort send failed: {}", .{send_err});
                };
                return err;
            };
            // Adoption drain (Accept-before-Provide) arrives with parking.
            std.debug.assert(adopted == null);
        }
    }

    fn handleAccept(self: *Peer, accept: protocol.Accept) !void {
        if (try self.tryHandleJoinAccept(accept)) return;
        if (self.provision_index) |idx| return self.handleAcceptWithProvisionIndex(idx, accept);
        try peer_provide_join_orchestration.handleAccept(
            Peer,
            ProvideEntry,
            ProvideTarget,
            self,
            accept,
            &self.provides_by_question,
            &self.provides_by_key,
            peer_provide_join_orchestration.captureAcceptProvisionForPeerFn(
                Peer,
                peer_third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            peer_finish.freeOwnedFrameForPeerFn(Peer),
            Peer.queueEmbargoedAccept,
            Peer.sendReturnProvidedTarget,
            Peer.sendReturnException,
        );
    }

    fn tryHandleJoinAccept(self: *Peer, accept: protocol.Accept) !bool {
        const key = try peer_provide_join_orchestration.captureAcceptProvisionForPeer(
            Peer,
            self,
            accept,
            peer_third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
        );
        defer if (key) |bytes| self.allocator.free(bytes);
        const provision = key orelse return false;

        var target = self.takePendingJoinAccept(provision) orelse return false;
        defer target.deinit(self.allocator);

        if (accept.embargo != null) {
            try self.sendReturnException(accept.question_id, "l4 join accept embargo unsupported");
            return true;
        }

        self.sendReturnProvidedTarget(accept.question_id, &target) catch |err| {
            try self.sendReturnException(accept.question_id, @errorName(err));
        };
        return true;
    }

    fn completeJoinWithL4Runtime(self: *Peer, join_id: u32) !void {
        const network = self.join_network orelse return error.NoJoinNetwork;
        const removed = self.pending_joins.fetchRemove(join_id) orelse return;
        var join_state = removed.value;
        defer JoinState.deinit(&join_state, self.allocator);

        if (join_state.parts.count() == 0) return;

        var first_target: ?*const ProvideTarget = null;
        var all_equal = true;
        var check_it = join_state.parts.iterator();
        while (check_it.next()) |entry| {
            if (first_target) |target| {
                if (!provideTargetsEqual(target, &entry.value_ptr.target)) {
                    all_equal = false;
                    break;
                }
            } else {
                first_target = &entry.value_ptr.target;
            }
        }

        defer {
            var cleanup_it = join_state.parts.iterator();
            while (cleanup_it.next()) |entry| {
                _ = self.pending_join_questions.remove(entry.value_ptr.question_id);
            }
        }

        if (!all_equal) {
            var mismatch_it = join_state.parts.iterator();
            while (mismatch_it.next()) |entry| {
                try self.sendReturnException(entry.value_ptr.question_id, "join target mismatch");
            }
            return;
        }

        const target = first_target orelse return;
        const hosted = network.hostJoinResult(self.allocator, self, join_id) catch |err| {
            var err_it = join_state.parts.iterator();
            while (err_it.next()) |entry| {
                try self.sendReturnException(entry.value_ptr.question_id, @errorName(err));
            }
            return;
        };
        const accept_peer = hosted.accept_peer;
        var provision_registered = true;
        defer if (provision_registered) network.cancelHostJoinResult(hosted.provision);
        defer self.allocator.free(hosted.provision);
        defer self.allocator.free(hosted.result);

        const accept_provision = try accept_peer.allocator.dupe(u8, hosted.provision);
        var accept_provision_owned = true;
        defer if (accept_provision_owned) accept_peer.allocator.free(accept_provision);

        var target_copy = accept_peer.cloneProvideTarget(target) catch |err| {
            var err_it = join_state.parts.iterator();
            while (err_it.next()) |entry| {
                try self.sendReturnException(entry.value_ptr.question_id, @errorName(err));
            }
            return;
        };
        var target_owned = true;
        defer if (target_owned) target_copy.deinit(accept_peer.allocator);

        accept_peer.putPendingJoinAcceptOwned(accept_provision, target_copy) catch |err| {
            var err_it = join_state.parts.iterator();
            while (err_it.next()) |entry| {
                try self.sendReturnException(entry.value_ptr.question_id, @errorName(err));
            }
            return;
        };
        accept_provision_owned = false;
        target_owned = false;

        var sent_results: usize = 0;
        errdefer if (sent_results == 0) {
            provision_registered = false;
            accept_peer.clearPendingJoinAccept(hosted.provision);
        };
        var send_it = join_state.parts.iterator();
        while (send_it.next()) |entry| {
            self.rememberPendingJoinResultAnswer(entry.value_ptr.question_id, accept_peer, hosted.provision) catch |err| {
                try self.sendReturnException(entry.value_ptr.question_id, @errorName(err));
                continue;
            };
            self.sendReturnJoinResultPayload(entry.value_ptr.question_id, hosted.result) catch |err| {
                self.forgetPendingJoinResultAnswer(entry.value_ptr.question_id);
                try self.sendReturnException(entry.value_ptr.question_id, @errorName(err));
                continue;
            };
            sent_results += 1;
            provision_registered = false;
        }

        if (sent_results == 0) {
            provision_registered = false;
            accept_peer.clearPendingJoinAccept(hosted.provision);
        }
    }

    fn handleJoin(self: *Peer, join: protocol.Join) !void {
        // A Join must not reuse a question id already live as a Call /
        // Bootstrap answer or a Provide (spec violation). Same-type (join)
        // collisions fall through to the orchestration's specific "duplicate
        // join question" abort below.
        if (try self.inboundAnswerQuestionIdInUse(join.question_id) or
            self.provides_by_question.contains(join.question_id))
        {
            return error.DuplicateQuestionId;
        }
        if (try self.tryHandleCrossPeerProxyJoin(join)) return;
        try peer_provide_join_orchestration.handleJoin(
            Peer,
            JoinKeyPart,
            JoinState,
            PendingJoinQuestion,
            ProvideTarget,
            self,
            self.allocator,
            join,
            &self.pending_joins,
            &self.pending_join_questions,
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            peer_provide_accept_join.resolveProvideTargetForPeerFn(
                Peer,
                peer_provide_accept_join.resolveProvideImportedCapForPeerFn(Peer),
                peer_provide_accept_join.resolveProvidePromisedAnswerForPeerFn(Peer, Peer.resolvePromisedAnswer),
            ),
            makeProvideTarget,
            ProvideTarget.deinit,
            JoinState.init,
            JoinState.deinit,
            Peer.ensureJoinBudget,
            if (self.join_network != null)
                Peer.completeJoinWithL4Runtime
            else
                peer_join_state.completeJoinForPeerFn(
                    Peer,
                    JoinState,
                    PendingJoinQuestion,
                    ProvideTarget,
                    provideTargetsEqual,
                    Peer.sendReturnProvidedTarget,
                    Peer.sendReturnException,
                    JoinState.deinit,
                ),
            Peer.sendReturnException,
        );
    }

    fn handleThirdPartyAnswer(self: *Peer, third_party_answer: protocol.ThirdPartyAnswer) !void {
        try peer_third_party_adoption.handleThirdPartyAnswer(
            Peer,
            PendingThirdPartyAwait,
            self.allocator,
            self,
            third_party_answer,
            &self.pending_third_party_awaits,
            &self.pending_third_party_answers,
            peer_third_party_adoption.captureThirdPartyCompletionForPeerFn(
                Peer,
                peer_third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            peer_finish.freeOwnedFrameForPeerFn(Peer),
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            Peer.ensurePendingThirdPartyAnswerBudget,
            peer_third_party_adoption.adoptPendingAwaitEntryForPeerFn(
                Peer,
                Question,
                PendingThirdPartyAwait,
                adoptThirdPartyAnswer,
            ),
        );
    }

    fn pendingMapHasQueuedQuestionId(
        self: *Peer,
        pending_map: *std.AutoHashMap(u32, std.ArrayList(PendingCall)),
        question_id: u32,
    ) bool {
        _ = self;
        var pending_it = pending_map.valueIterator();
        while (pending_it.next()) |pending_list| {
            // Compare the id decoded once at enqueue — no per-scan re-parse.
            for (pending_list.items) |pending_call| {
                if (pending_call.question_id == question_id) return true;
            }
        }
        return false;
    }

    fn hasQueuedPendingQuestionId(self: *Peer, question_id: u32) bool {
        return self.pendingMapHasQueuedQuestionId(&self.pending_promises, question_id) or
            self.pendingMapHasQueuedQuestionId(&self.pending_export_promises, question_id);
    }

    /// True when `question_id` is already consumed by an inbound Call or
    /// Bootstrap answer (active, resolved, resolving, finished-early, forwarded,
    /// or queued for a promised target). This is the shared answer namespace;
    /// it deliberately excludes the provide/join question tables so their
    /// handlers can report their own specific errors for same-type collisions.
    ///
    /// A finished-early tombstone keeps the id reserved until the late Return
    /// drains it: a compliant caller may not reuse a question id before it has
    /// received that Return (which consumes the tombstone), so only a violator
    /// is rejected here. Without this, a reused id's Return sender would
    /// fetchRemove the STALE tombstone — skipping the record and applying the
    /// old Finish's releaseResultCaps flag to the new answer's result caps, a
    /// remote-forceable premature export release.
    fn inboundAnswerQuestionIdInUse(self: *Peer, question_id: u32) !bool {
        return self.resolved_answers.contains(question_id) or
            self.active_inbound_questions.contains(question_id) or
            self.resolving_answers.contains(question_id) or
            self.finished_early_answers.contains(question_id) or
            self.send_results_to_yourself.contains(question_id) or
            self.send_results_to_third_party.contains(question_id) or
            self.forwarded_questions.contains(question_id) or
            self.forwarded_tail_questions.contains(question_id) or
            self.pending_join_relays.contains(question_id) or
            self.hasQueuedPendingQuestionId(question_id);
    }

    /// True when `question_id` is already consumed by any inbound question:
    /// the shared Call/Bootstrap answer namespace plus in-flight Provide and
    /// Join questions. Used by handlers that own no question table of their own
    /// (Call, Bootstrap) so a spec-violating reuse across message types is
    /// rejected before a second Return can be emitted on the id.
    fn inboundQuestionIdInUse(self: *Peer, question_id: u32) !bool {
        return (try self.inboundAnswerQuestionIdInUse(question_id)) or
            self.provides_by_question.contains(question_id) or
            self.pending_join_questions.contains(question_id) or
            self.pending_join_relays.contains(question_id);
    }

    fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
        // Reject duplicate question IDs from the remote peer (spec violation).
        // Covers the shared answer namespace plus in-flight Provide/Join
        // questions so a Call can never collide with any other inbound question.
        if (try self.inboundQuestionIdInUse(call.question_id)) {
            return error.DuplicateQuestionId;
        }

        // `sendResultsTo = thirdParty` asks this vat to connect to a third vat
        // and deliver the results there. Unless the host opted in, we cannot —
        // and accepting the call only to drop its results is the one outcome the
        // protocol never permits. Refuse with a single exception Return.
        //
        // Placement is load-bearing: after the duplicate-id check, so a reused
        // id still reports DuplicateQuestionId; before the answer bookkeeping,
        // so there is nothing to unwind; and before the inbound cap table is
        // built, so no import references are taken and the exception Return's
        // default `releaseParamCaps` correctly tells the sender to drop its
        // export refs. This also covers in-process loopback calls, which are
        // delivered through handleFrame.
        if (call.send_results_to.tag == .thirdParty and
            self.third_party_result_policy == .reject)
        {
            try self.sendReturnException(call.question_id, third_party_results_unsupported);
            return;
        }

        const inbound_before = self.active_inbound_questions.count();
        try ensureCountLimit(false, inbound_before, self.limits.max_active_inbound_questions);
        try self.active_inbound_questions.put(call.question_id, {});
        errdefer _ = self.active_inbound_questions.remove(call.question_id);
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .active_inbound_questions,
            inbound_before,
            self.active_inbound_questions.count(),
            self.limits.max_active_inbound_questions,
        );

        peer_call_orchestration.handleCallForPeer(
            Peer,
            self,
            frame,
            call,
            peer_call_orchestration.handleCallImportedTargetForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                Peer.queuePromiseExportCall,
                Peer.releaseInboundCaps,
                peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
                peer_third_party.noteCallSendResultsForPeerFn(
                    Peer,
                    Peer.noteSendResultsToYourself,
                    Peer.noteSendResultsToThirdParty,
                ),
                Peer.sendReturnException,
                Peer.handleResolvedCall,
            ),
            peer_call_orchestration.handleCallPromisedTargetForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                Peer.resolvePromisedAnswer,
                peer_call_targets.hasUnresolvedPromiseExportForPeerFn(Peer),
                Peer.queuePromisedCall,
                Peer.queuePromiseExportCall,
                Peer.sendReturnException,
                Peer.handleResolvedCall,
                Peer.releaseInboundCaps,
                peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
            ),
        ) catch |err| {
            log.debug("call routing error for question {}: {}", .{ call.question_id, err });
            try self.sendReturnException(call.question_id, @errorName(err));
        };
    }

    fn handleResolvedCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        resolved: cap_table.ResolvedCap,
    ) !void {
        try peer_forward_orchestration.handleResolvedCall(
            Peer,
            cap_table.InboundCapTable,
            self,
            call,
            inbound_caps,
            resolved,
            peer_call_orchestration.handleResolvedExportedCallForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                peer_third_party.noteCallSendResultsForPeerFn(
                    Peer,
                    Peer.noteSendResultsToYourself,
                    Peer.noteSendResultsToThirdParty,
                ),
                Peer.handleResolvedCall,
                Peer.sendReturnException,
                Peer.maybeForwardVineCall,
            ),
            peer_forward_orchestration.forwardResolvedCallForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                Peer.forwardResolvedCall,
            ),
            Peer.sendReturnException,
        );
    }

    /// Pre-reserved resources for recording a resolved answer, obtained from
    /// `reserveResolvedAnswer` before the Return frame is sent so that
    /// `commitReservedResolvedAnswer` (run after the frame is on the wire)
    /// cannot fail. See `reserveResolvedAnswer`.
    const ResolvedAnswerReservation = struct {
        frame_copy: []u8,
        /// Exports on which the reservation took the answer-held reference
        /// (one entry per results cap descriptor). deinit rolls the
        /// references back; commit transfers them to the recorded answer,
        /// which releases them at Finish via the stored frame's cap table.
        held_export_ids: []u32,

        fn deinit(self: ResolvedAnswerReservation, peer: *Peer) void {
            peer.resolved_answer_reservations -= 1;
            var idx = self.held_export_ids.len;
            while (idx > 0) {
                idx -= 1;
                peer.rollbackAnswerExportRef(self.held_export_ids[idx]);
            }
            peer.allocator.free(self.held_export_ids);
            peer.allocator.free(self.frame_copy);
        }
    };

    /// Reserve everything needed to record a resolved answer BEFORE the Return
    /// frame is sent: the resolved-answers count budget, one unused map slot,
    /// the frame copy, and the answer-held references on every export in the
    /// frame's results cap table. This must precede the send so that recording
    /// — which happens only once the frame is already on the wire — is
    /// infallible.
    ///
    /// If the record step could fail after the send, the propagating error would
    /// drive the call-dispatch catch to emit a SECOND (exception) Return for the
    /// same answer_id: two Returns for one call, a remote-forceable protocol
    /// violation (a peer fills resolved_answers to max_resolved_answers with
    /// Finish-less calls, then any later successful call double-Returns). Audit
    /// 2026-07-03 item 7.
    ///
    /// The answer-held references keep the answer's pipeline targets alive
    /// until its Finish: without them the export dies as soon as the remote
    /// Releases the caps it imported from this Return — legal even before
    /// Finish — and a pipelined call on the answer can no longer dispatch.
    fn reserveResolvedAnswer(self: *Peer, question_id: u32, frame: []const u8) !ResolvedAnswerReservation {
        try ensureCountLimit(
            self.resolved_answers.contains(question_id),
            self.resolved_answers.count() + self.resolved_answer_reservations,
            self.limits.max_resolved_answers,
        );
        // Reserve a map slot so the post-send getOrPutAssumeCapacity cannot
        // OOM. Ensure one slot per OUTSTANDING reservation as well: a nested
        // reserve→commit for a different answer id (a synchronous transport
        // delivering a Call/Bootstrap while an outer send is on the stack)
        // would otherwise consume the single slot the outer reservation's
        // ensure counted on, and the outer infallible commit would underflow
        // the map's reserved-slot accounting.
        try self.resolved_answers.ensureUnusedCapacity(1 + self.resolved_answer_reservations);
        const frame_copy = try self.allocator.dupe(u8, frame);
        errdefer self.allocator.free(frame_copy);
        const held_export_ids = try peer_cap_lifecycle.noteAnswerHeldResultCaps(
            Peer,
            self,
            self.allocator,
            frame,
            Peer.noteAnswerExportRef,
            rollbackAnswerExportRef,
        );
        self.resolved_answer_reservations += 1;
        return .{ .frame_copy = frame_copy, .held_export_ids = held_export_ids };
    }

    /// Record a resolved answer using a reservation obtained (before the send)
    /// from `reserveResolvedAnswer`. Infallible: the map slot, frame copy, and
    /// answer-held export references are already reserved, so this only stores
    /// into the map. Takes ownership of the reservation: `frame_copy` moves
    /// into the map and the answer-held references now belong to the recorded
    /// answer (released at Finish by re-walking the stored frame, so the id
    /// list is no longer needed).
    fn commitReservedResolvedAnswer(
        self: *Peer,
        question_id: u32,
        reservation: ResolvedAnswerReservation,
    ) void {
        self.resolved_answer_reservations -= 1;
        self.allocator.free(reservation.held_export_ids);
        pending_calls.recordResolvedAnswerAssumeCapacity(
            Peer,
            ResolvedAnswer,
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            self,
            question_id,
            reservation.frame_copy,
            &self.resolved_answers,
            &self.pending_promises,
            Peer.resolvePromisedAnswer,
            Peer.sendReturnException,
            Peer.handleResolvedCall,
            Peer.releaseInboundCaps,
            peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
        );
    }

    fn recordResolvedAnswer(self: *Peer, question_id: u32, frame: []u8) !void {
        try ensureCountLimit(
            self.resolved_answers.contains(question_id),
            self.resolved_answers.count(),
            self.limits.max_resolved_answers,
        );
        try pending_calls.recordResolvedAnswer(
            Peer,
            ResolvedAnswer,
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            self,
            question_id,
            frame,
            &self.resolved_answers,
            &self.pending_promises,
            Peer.resolvePromisedAnswer,
            Peer.sendReturnException,
            Peer.handleResolvedCall,
            Peer.releaseInboundCaps,
            peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
        );
    }

    fn queuePromisedCall(self: *Peer, question_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
        try self.ensurePendingQueuedCallBudget(
            &self.pending_promises,
            question_id,
            frame.len,
            self.limits.max_pending_promises,
        );
        const stats_before = self.pendingQueuedCallStats();
        try pending_calls.queuePendingCall(
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            &self.pending_promises,
            question_id,
            frame,
            inbound_caps,
        );
        self.emitQueuedCallPressure(stats_before, frame.len);
    }

    fn queuePromiseExportCall(self: *Peer, export_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
        try self.ensurePendingQueuedCallBudget(
            &self.pending_export_promises,
            export_id,
            frame.len,
            self.limits.max_pending_export_promises,
        );
        const stats_before = self.pendingQueuedCallStats();
        try pending_calls.queuePendingCall(
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            &self.pending_export_promises,
            export_id,
            frame,
            inbound_caps,
        );
        self.emitQueuedCallPressure(stats_before, frame.len);
    }

    /// Emit queued-call pressure crossings after a successful enqueue.
    fn emitQueuedCallPressure(self: *Peer, before: PendingQueuedCallStats, frame_len: usize) void {
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .queued_calls,
            before.calls,
            before.calls + 1,
            self.limits.max_pending_queued_calls,
        );
        events.emitPressureCrossing(
            self.observer,
            .peer,
            .unknown,
            .queued_call_bytes,
            before.bytes,
            before.bytes + frame_len,
            self.limits.max_pending_queued_call_bytes,
        );
    }

    fn replayResolvedPromiseExport(self: *Peer, export_id: u32, resolved: cap_table.ResolvedCap) !void {
        try pending_calls.replayResolvedPromiseExport(
            Peer,
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            self,
            export_id,
            resolved,
            &self.pending_export_promises,
            Peer.handleResolvedCall,
            Peer.sendReturnException,
            Peer.releaseInboundCaps,
            peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
        );
    }

    fn adoptThirdPartyAnswer(
        self: *Peer,
        question_id: u32,
        adopted_answer_id: u32,
        question: Question,
    ) anyerror!void {
        try peer_third_party_adoption.adoptThirdPartyAnswer(
            Peer,
            Question,
            self,
            question_id,
            adopted_answer_id,
            question,
            &self.questions,
            &self.adopted_third_party_answers,
            &self.pending_third_party_returns,
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            Peer.ensureThirdPartyAdoptionBudget,
            peer_finish.freeOwnedFrameForPeerFn(Peer),
            peer_third_party_returns.handlePendingReturnFrameForPeerFn(
                Peer,
                Peer.handleReturn,
            ),
        );
    }

    fn handleReturn(self: *Peer, frame: []const u8, ret: protocol.Return) anyerror!void {
        // The wire effect of a Return on our sent param caps is independent
        // of local dispatch: the moment the remote sent this frame it either
        // dropped the refs (releaseParamCaps=true, the default) or committed
        // to explicit Releases (false). Consume the record first so every
        // Return path — regular, exception, third-party, and the
        // cancelled-question absorb below — settles the refs exactly once.
        try self.consumeQuestionParamExports(ret.answer_id, ret.release_param_caps);

        // `takeFromOtherQuestion` names a call WE answered with
        // `sendResultsTo=yourself` whose results should become this question's
        // results (reflected-loopback / Level-1 tail-call). If we stashed those
        // results (cap-free; see completeSelfLoopbackReturn), deliver them inline
        // to this question instead of surfacing the bare relay tag, so the caller
        // receives the real value without a wire round-trip through the
        // forwarder. Falls through to the normal path when nothing was stashed
        // (results carried caps, the answer is still pending, or the stash
        // overflowed) — the caller then sees `takeFromOtherQuestion` as before.
        if (ret.tag == .takeFromOtherQuestion) {
            if (ret.take_from_other_question) |source_answer_id| {
                if (self.questions.contains(ret.answer_id)) {
                    if (self.loopback_result_stash.fetchRemove(source_answer_id)) |entry| {
                        defer self.allocator.free(entry.value);
                        try self.deliverStashedLoopbackResults(ret.answer_id, entry.value);
                        return;
                    }
                }
            }
        }

        var latency_started_ns: ?i64 = null;
        if (self.questions.get(ret.answer_id)) |question| {
            if (question.cancelled) {
                // Cancelled locally: the caller already saw an exception and
                // a Finish with releaseResultCaps is on the wire, so the
                // remote releases any caps this Return carries. Per spec the
                // remote sends exactly one Return per question — absorb it
                // without importing caps or re-dispatching.
                log.debug("absorbing return for cancelled question {}", .{ret.answer_id});
                self.removeQuestion(ret.answer_id);
                return;
            }
            // awaitFromThirdParty is not call completion; results arrive
            // later through the third party, so no latency sample.
            if (ret.tag != .awaitFromThirdParty) latency_started_ns = question.started_ns;
        }
        defer if (latency_started_ns) |started| {
            if (self.clockNow()) |now| {
                const elapsed = now - started;
                if (elapsed >= 0 and !self.questions.contains(ret.answer_id)) {
                    events.emitCallLatency(self.observer, .peer, .unknown, ret.answer_id, @intCast(elapsed));
                }
            }
        };
        try peer_return_orchestration.handleReturn(
            Peer,
            Question,
            cap_table.InboundCapTable,
            self,
            frame,
            ret,
            peer_return_orchestration.getQuestionForPeerFn(Peer, Question),
            peer_return_orchestration.removeQuestionForReturnForPeerFn(Peer),
            peer_return_orchestration.restoreQuestionForReturnForPeerFn(Peer, Question),
            peer_return_orchestration.completeQuestionRemovalForPeerFn(Peer),
            Peer.handleMissingReturnQuestion,
            peer_return_orchestration.initInboundCapsForPeerFn(Peer, cap_table.InboundCapTable),
            peer_return_orchestration.deinitInboundCapsForTypeFn(cap_table.InboundCapTable),
            peer_third_party_adoption.handleReturnAcceptFromThirdPartyForPeerFn(
                Peer,
                Question,
                PendingThirdPartyAwait,
                cap_table.InboundCapTable,
                peer_third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
                peer_finish.freeOwnedFrameForPeerFn(Peer),
                peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
                Peer.ensurePendingThirdPartyAwaitBudget,
                adoptThirdPartyAnswer,
            ),
            peer_return_dispatch.maybeSendAutoFinishForPeerFn(
                Peer,
                Question,
                peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            ),
            peer_return_orchestration.handleReturnRegularForPeerFn(
                Peer,
                Question,
                cap_table.InboundCapTable,
                Peer.releaseInboundCaps,
                peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
            ),
        );
    }

    /// Exposed for integration tests that exercise internal Peer methods.
    /// Not part of the public API — gated behind `builtin.is_test` so it is
    /// absent from the frozen consumer surface (`src/lib.zig`) and the
    /// generated `docs/api-snapshot.txt`, and unreachable from application
    /// code. Test builds (`is_test == true`) still see the full facade.
    pub const test_hooks = if (builtin.is_test) struct {
        pub const ForwardCallContextType = ForwardCallContext;

        pub fn sendFrame(self: *Peer, frame: []const u8) !void {
            return Peer.sendFrame(self, frame);
        }

        pub fn removeQuestion(self: *Peer, question_id: u32) void {
            Peer.removeQuestion(self, question_id);
        }

        pub fn removeQuestionAndDeinit(self: *Peer, question_id: u32) void {
            Peer.removeQuestionAndDeinit(self, question_id);
        }

        pub fn sendJoinExperimentalRetainedResult(
            self: *Peer,
            target: protocol.MessageTarget,
            key_part: ?message.AnyPointerReader,
            ctx: *anyopaque,
            on_return: QuestionCallback,
        ) !u32 {
            return Peer.sendJoinExperimentalWithAutoFinish(self, target, key_part, ctx, on_return, true);
        }

        pub fn addCrossPeerProxyExport(
            self: *Peer,
            source_peer: *Peer,
            target: cap_table.ResolvedCap,
            release_source_import_id: ?u32,
            release_source_export_pin_id: ?u32,
        ) !u32 {
            return Peer.addCrossPeerProxyExport(self, source_peer, target, release_source_import_id, release_source_export_pin_id);
        }

        /// Take a handoff-held (Release-immune) pin on one of this peer's
        /// exports — the lease a caller passes to `addCrossPeerProxyExport` as
        /// `release_source_export_pin_id`.
        pub fn noteHandoffExportRef(self: *Peer, id: u32) !void {
            return Peer.noteHandoffExportRef(self, id);
        }

        /// Release a handoff-held pin directly (tests that never hand it to a
        /// proxy).
        pub fn releaseHandoffHeldExport(self: *Peer, id: u32) void {
            Peer.releaseHandoffHeldExport(self, id);
        }

        pub fn releaseVineExport(self: *Peer, vine_id: u32) void {
            Peer.releaseVineExport(self, vine_id);
        }

        pub fn onConnectionError(self: *Peer, err: anyerror) void {
            Peer.onConnectionError(self, err);
        }

        pub fn onConnectionClose(self: *Peer) void {
            Peer.onConnectionClose(self);
        }

        pub fn collectReleaseCounts(
            self: *Peer,
            inbound: *cap_table.InboundCapTable,
        ) !std.AutoHashMap(u32, u32) {
            const release_import = peer_cap_lifecycle.releaseImportRefForPeerFn(Peer);
            var releases = std.AutoHashMap(u32, u32).init(self.allocator);
            errdefer releases.deinit();
            var idx: u32 = 0;
            while (idx < inbound.len()) : (idx += 1) {
                if (inbound.isRetained(idx)) continue;
                const entry = try inbound.get(idx);
                switch (entry) {
                    .imported => |cap| {
                        const removed = release_import(self, cap.id);
                        if (removed) {
                            try Peer.releaseResolvedImport(self, cap.id);
                        }
                        const slot = try releases.getOrPut(cap.id);
                        if (!slot.found_existing) {
                            slot.value_ptr.* = 1;
                        } else {
                            slot.value_ptr.* +%= 1;
                        }
                    },
                    else => {},
                }
            }
            return releases;
        }

        pub fn clonePayloadWithRemappedCaps(
            self: *Peer,
            builder: *message.MessageBuilder,
            payload_builder: message.StructBuilder,
            source: protocol.Payload,
            inbound_caps: *const cap_table.InboundCapTable,
        ) !void {
            return Peer.clonePayloadWithRemappedCaps(
                self,
                builder,
                protocol.PayloadBuilder.wrap(payload_builder),
                source,
                inbound_caps,
            );
        }

        pub fn makeProvideTarget(self: *Peer, resolved: cap_table.ResolvedCap) !state.ProvideTarget {
            return Peer.makeProvideTarget(self, resolved);
        }

        pub fn sendReturnProvidedTarget(self: *Peer, answer_id: u32, target: *const state.ProvideTarget) !void {
            return Peer.sendReturnProvidedTarget(self, answer_id, target);
        }

        pub fn onForwardedReturn(
            ctx_ptr: *anyopaque,
            self: *Peer,
            ret: protocol.Return,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            return Peer.onForwardedReturn(ctx_ptr, self, ret, inbound_caps);
        }

        pub fn handleResolvedCall(
            self: *Peer,
            call: protocol.Call,
            inbound_caps: *const cap_table.InboundCapTable,
            resolved: cap_table.ResolvedCap,
        ) !void {
            return Peer.handleResolvedCall(self, call, inbound_caps, resolved);
        }

        pub fn handleFinish(self: *Peer, finish_msg: protocol.Finish) !void {
            return Peer.handleFinish(self, finish_msg);
        }

        pub fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
            return Peer.handleCall(self, frame, call);
        }

        pub fn queuePromisedCall(
            self: *Peer,
            question_id: u32,
            frame: []const u8,
            inbound_caps: cap_table.InboundCapTable,
        ) !void {
            return Peer.queuePromisedCall(self, question_id, frame, inbound_caps);
        }

        pub fn rememberPendingEmbargo(self: *Peer, embargo_id: u32, promise_id: u32) !void {
            return Peer.rememberPendingEmbargo(self, embargo_id, promise_id);
        }

        pub fn storeResolvedImport(
            self: *Peer,
            promise_id: u32,
            cap: ?cap_table.ResolvedCap,
            embargo_id: ?u32,
            embargoed: bool,
        ) !void {
            return Peer.storeResolvedImport(self, promise_id, cap, embargo_id, embargoed);
        }

        pub fn bufferPendingThirdPartyReturn(self: *Peer, answer_id: u32, frame: []const u8) !void {
            return Peer.bufferPendingThirdPartyReturn(self, answer_id, frame);
        }

        pub fn queueEmbargoedAccept(
            self: *Peer,
            answer_id: u32,
            provided_question_id: u32,
            embargo: []const u8,
        ) !void {
            return Peer.queueEmbargoedAccept(self, answer_id, provided_question_id, embargo);
        }

        pub fn ensureProvideBudget(self: *Peer, question_id: u32, recipient_key: []const u8) !void {
            return Peer.ensureProvideBudget(self, question_id, recipient_key);
        }

        pub fn ensureJoinBudget(
            self: *Peer,
            join_id: u32,
            part_count: u16,
            part_num: u16,
            question_id: u32,
        ) !void {
            return Peer.ensureJoinBudget(
                self,
                .{
                    .join_id = join_id,
                    .part_count = part_count,
                    .part_num = part_num,
                },
                question_id,
            );
        }

        pub fn ensurePendingThirdPartyAwaitBudget(self: *Peer, completion_key: []const u8) !void {
            return Peer.ensurePendingThirdPartyAwaitBudget(self, completion_key);
        }

        pub fn ensurePendingThirdPartyAnswerBudget(self: *Peer, completion_key: []const u8) !void {
            return Peer.ensurePendingThirdPartyAnswerBudget(self, completion_key);
        }

        pub fn ensureThirdPartyAdoptionBudget(self: *Peer, adopted_answer_id: u32) !void {
            return Peer.ensureThirdPartyAdoptionBudget(self, adopted_answer_id);
        }
    } else struct {};
};
