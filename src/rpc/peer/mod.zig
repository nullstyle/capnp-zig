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

const peer_call_targets = @import("./call/peer_call_targets.zig");
const peer_call_sender = @import("./call/peer_call_sender.zig");
const payload_remap = @import("../caps/payload_remap.zig");
const pending_calls = @import("../promises/pending_calls.zig");
const peer_inbound_release = @import("./peer_inbound_release.zig");
const peer_forward_orchestration = @import("./forward/peer_forward_orchestration.zig");
const peer_forward_return_callbacks = @import("./forward/peer_forward_return_callbacks.zig");
const peer_cap_lifecycle = @import("./peer_cap_lifecycle.zig");
const peer_outbound_control = @import("./peer_outbound_control.zig");
const peer_call_orchestration = @import("./call/peer_call_orchestration.zig");
const peer_return_orchestration = @import("./return/peer_return_orchestration.zig");
const peer_return_dispatch = @import("./return/peer_return_dispatch.zig");
const return_routing = @import("../promises/return_routing.zig");
const return_send = @import("../promises/return_send.zig");
const peer_transport = @import("./transport.zig");
const vat_network = @import("../vat/network.zig");
const peer_question_state = @import("./peer_question_state.zig");
const retained_question_state = @import("./retained_questions.zig");
const provide_forward_target = @import("./provide/forward_target.zig");
const peer_cleanup = @import("./peer_cleanup.zig");
const peer_return_frames = @import("./return/peer_return_frames.zig");
const join_network = @import("../vat/join.zig");
const vat_provisions = @import("../vat/provisions.zig");
const peer_provision_hosting = @import("./provision/peer_provision_hosting.zig");
const peer_provision_drain = @import("./provision/peer_provision_drain.zig");
const peer_return_send = @import("./return/peer_return_send.zig");
const peer_export_release = @import("./peer_export_release.zig");
const peer_call_send = @import("./call/peer_call_send.zig");
const peer_call_inbound = @import("./call/peer_call_inbound.zig");
const peer_join_accept = @import("./join/peer_join_accept.zig");
const peer_join_relay = @import("./join/peer_join_relay.zig");
const peer_provide_origination = @import("./provide/peer_provide_origination.zig");
const peer_provide_inbound = @import("./provide/peer_provide_inbound.zig");
const peer_cross_peer_proxy = @import("./third_party/peer_cross_peer_proxy.zig");
const peer_third_party_routes = @import("./third_party/peer_third_party_routes.zig");
const peer_promise_exports = @import("./peer_promise_exports.zig");
const peer_resolve_inbound = @import("./peer_resolve_inbound.zig");
const peer_persistence_hooks = @import("./peer_persistence_hooks.zig");
const peer_question_alloc = @import("./peer_question_alloc.zig");
const peer_context_types = @import("./peer_context_types.zig");
const peer_lifecycle = @import("./peer_lifecycle.zig");
const vat_host = @import("../vat/host.zig");
const promises_promised_answer = @import("../promises/promised_answer.zig");

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

const SaveHook = peer_context_types.SaveHookOf(Peer);

const RestoreHook = peer_context_types.RestoreHookOf(Peer);

const PersistenceState = peer_context_types.PersistenceStateOf(Peer);

const ExportEntry = state.ExportEntry(Export);
const ResolvedAnswer = state.ResolvedAnswer;
const FailedAnswer = state.FailedAnswer;
const PendingCall = state.PendingCall;

const ProvideTarget = state.ProvideTarget;
const ProvideEntry = state.ProvideEntry;

const CrossPeerAcceptRecord = peer_context_types.CrossPeerAcceptRecordOf(Peer);
const JoinKeyPart = state.JoinKeyPart;
const JoinPartEntry = state.JoinPartEntry;
const JoinState = state.JoinState;
const PendingJoinQuestion = state.PendingJoinQuestion;
const PendingEmbargoedAccept = state.PendingEmbargoedAccept;

const ForwardReturnMode = peer_forward_orchestration.ForwardReturnMode;

const ResolvedImport = state.ResolvedImport;

pub const PeerLimits = state.PeerLimits;
pub const PeerTimeouts = state.PeerTimeouts;
pub const ResultLifetime = retained_question_state.ResultLifetime;
pub const CallOptions = retained_question_state.CallOptions;

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

const ForwardCallContext = peer_context_types.ForwardCallContextOf(Peer);

const ForwardReturnBuildContext = peer_context_types.ForwardReturnBuildContextOf(Peer);

const ForwardVineCallContext = peer_context_types.ForwardVineCallContextOf(Peer);

const CrossPeerProxyContext = peer_context_types.CrossPeerProxyContextOf(Peer);

const CrossPeerProxyCallContext = peer_context_types.CrossPeerProxyCallContextOf(Peer);

const CrossPeerJoinRelayContext = peer_context_types.CrossPeerJoinRelayContextOf(Peer);

const CrossPeerCapMapContext = peer_context_types.CrossPeerCapMapContextOf(Peer);

const CrossPeerReturnRelayContext = peer_context_types.CrossPeerReturnRelayContextOf(Peer);

const AutomaticThirdPartyRoute = peer_context_types.AutomaticThirdPartyRouteOf(Peer);

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
/// | `failed_answers` | question ID | `FailedAnswer` | The exception already returned for a failed answer (results-only `resolved_answers` never records it). Lets a call pipelined on that answer arriving after the Return be failed with a copy of the same exception instead of queueing forever. Removed on Finish. |
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
/// Experimental multi-connection vat facade (owns the provision index + the
/// accept-embargo CSPRNG; one `enroll(peer)` per connection).
pub const Vat = vat_host.Vat(Peer);

/// Experimental source of random bytes for accept-embargo ids (rpc.capnp
/// requires "globally-unique ... chosen at random with enough entropy").
/// Mirrors the `rpc_time.Clock` seam: ctx + fn pointer, trivially fakeable in
/// tests. Installed by the tcp session constructors from a
/// `randomSecure`-seeded CSPRNG; peers without one keep the legacy per-peer
/// counter (correctness never depends on this — the host keys embargoes per
/// provision — it is spec compliance and cross-host hygiene).
pub const EntropySource = struct {
    ctx: *anyopaque,
    fill: *const fn (ctx: *anyopaque, buf: []u8) void,

    pub fn fromCsprng(rng: *std.Random.DefaultCsprng) EntropySource {
        const F = struct {
            fn fill(ctx: *anyopaque, buf: []u8) void {
                const r: *std.Random.DefaultCsprng = @ptrCast(@alignCast(ctx));
                r.random().bytes(buf);
            }
        };
        return .{ .ctx = rng, .fill = F.fill };
    }
};

/// Seed a CSPRNG from the OS entropy syscall — FAIL CLOSED on
/// `error.EntropyUnavailable`: never fall back to `Io.random`'s
/// pointer+timestamp emergency seed (collision-resistant but guessable).
pub fn seedEntropyCsprng(io: std.Io) std.Io.RandomSecureError!std.Random.DefaultCsprng {
    var seed: [std.Random.DefaultCsprng.secret_seed_length]u8 = undefined;
    try io.randomSecure(&seed);
    return std.Random.DefaultCsprng.init(seed);
}
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
pub const JoinCoordinator = @import("./join/join_coordinator.zig").JoinCoordinator(Peer);

const OutboundProvide = peer_context_types.OutboundProvideOf(Peer);

const ProvideOriginationTarget = peer_context_types.ProvideOriginationTargetOf(Peer);

const CoupledVine = peer_context_types.CoupledVineOf(Peer);

const CrossPeerProxyLink = peer_context_types.CrossPeerProxyLinkOf(Peer);

const CrossPeerJoinRelay = peer_context_types.CrossPeerJoinRelayOf(Peer);

const CrossPeerJoinRelayLink = peer_context_types.CrossPeerJoinRelayLinkOf(Peer);

const HostedJoin = peer_context_types.HostedJoinOf(Peer);

const PendingJoinAccept = peer_context_types.PendingJoinAcceptOf(Peer);

const CompletingJoinAnswer = peer_context_types.CompletingJoinAnswerOf(Peer);

const PendingJoinResultAnswer = peer_context_types.PendingJoinResultAnswerOf(Peer);

const JoinAcceptHostLink = peer_context_types.JoinAcceptHostLinkOf(Peer);

const JoinCoordinatorAcceptLink = peer_context_types.JoinCoordinatorAcceptLinkOf(Peer);

const JoinCoordinatorResultLink = peer_context_types.JoinCoordinatorResultLinkOf(Peer);

/// Reason sent when refusing an inbound Call whose results were redirected to a
/// third vat that this peer cannot contact.
/// Reason sent to calls pipelined on an answer whose results went to a third
/// vat: this peer never observes those results, so it cannot resolve a
/// promised-answer target against them.
const automatic_third_party_target_canceled =
    "automatic third-party result recipient canceled before delivery";

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
    /// Outbound calls whose remote answers are caller-owned after Return.
    /// Registration happens with question allocation, before a synchronous
    /// transport can deliver the Return callback.
    retained_questions: retained_question_state.Registry,
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
    /// Inbound call question IDs accepted from the remote peer until Return or
    /// Finish. The value is that answer's `owes_param_cap_releases`: TRUE when
    /// the Call's params carried `senderHosted`/`senderPromise` descriptors, so
    /// this vat took wire references on the caller's capabilities and settles
    /// them with explicit `Release` frames (the post-dispatch auto-release, or
    /// an application that kept a param cap and drops it later).
    ///
    /// It is the sole input to `returnReleasesParamCaps`, i.e. to the answering
    /// Return's `releaseParamCaps` flag: rpc.capnp forbids sending separate
    /// `Release` messages once that flag is true, so an answer that owes
    /// Releases must say `false`. Entries are created in `handleCall` and
    /// removed by the Return (`sendReturnFrameWithLoopback`) or the Finish,
    /// which is exactly the window in which the flag can be read.
    active_inbound_questions: std.AutoHashMap(u32, bool),
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
    /// Inbound answers that already returned an EXCEPTION, kept until Finish
    /// (the mirror of `resolved_answers`, which records results only). A call
    /// pipelined on such an answer that arrives AFTER the exception Return
    /// would otherwise queue in `pending_promises` forever — the failed
    /// answer can never replay it; this record answers it with a copy of the
    /// same exception instead. Bounded by max_active_inbound_questions,
    /// best-effort under pressure (a skipped record degrades to the old
    /// queue-forever behavior for that answer, never a crash).
    failed_answers: std.AutoHashMap(u32, FailedAnswer),

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
    /// Optional random source for accept-embargo ids (BORROWED ctx; must
    /// outlive the peer). Null = legacy 8-byte per-peer counter ids.
    entropy: ?EntropySource = null,
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
    /// Peer-local subset of vat-wide parked-Accept accounting. These gauges
    /// enforce fair admission across sibling connections and are updated only
    /// through `ProvisionIndex.commit/refundParkAdmission`.
    parked_accept_count: usize = 0,
    parked_accept_bytes: usize = 0,
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
    /// Async forwarded-vine contexts that borrow this peer as the recipient.
    /// Recipient close/deinit nulls every borrow before this peer can die; the
    /// forwarding peer's question owns and eventually frees each context.
    forward_vine_relay_links: std.ArrayList(*ForwardVineCallContext),
    /// Auto-pickup contexts owned by Accept questions on third-vat peers that
    /// borrow this peer for the promise/vine side of the handoff.
    handoff_pickup_links: std.ArrayList(*HandoffPickupContext),
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
    /// Completing L4 answer IDs reserved across host callbacks and fan-out.
    completing_join_answers: std.AutoHashMap(u32, CompletingJoinAnswer),
    /// Cross-peer transparent-proxy Join relays keyed by upstream answer id.
    pending_join_relays: std.AutoHashMap(u32, CrossPeerJoinRelay),
    /// Accept messages waiting for a disembargo.
    pending_accepts_by_embargo: std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)),
    /// Maps question IDs to embargo keys for cleanup on Finish.
    pending_accept_embargo_by_question: std.AutoHashMap(u32, []u8),
    /// Canonically-owned completed JoinResult handoffs. The key is also the
    /// value so map membership is an O(1) ownership/refund guard.
    hosted_joins: std.AutoHashMap(*HostedJoin, void),
    /// Experimental L4 JoinResult provisions waiting for a direct Accept. Keys
    /// borrow `HostedJoin.provision`; values own the cloned ProvideTarget.
    pending_join_accepts: std.StringHashMap(PendingJoinAccept),
    /// Experimental L4 JoinResult answer ids -> borrowed canonical owner.
    pending_join_result_answers: std.AutoHashMap(u32, PendingJoinResultAnswer),
    /// Back-links for canonical HostedJoins whose direct Accept lives here.
    join_accept_host_links: std.ArrayList(JoinAcceptHostLink),
    /// Back-links from Experimental JoinCoordinator instances holding an accepted
    /// cap or unfinished Accept answer through this peer.
    join_coordinator_accept_links: std.ArrayList(JoinCoordinatorAcceptLink),
    /// Back-links from JoinCoordinator result records that retain this peer
    /// after the outbound question Return has already been consumed.
    join_coordinator_result_links: std.ArrayList(JoinCoordinatorResultLink),
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
    /// Automatic `.vat_network` redirects owned by this peer, keyed by the
    /// original caller-facing answer id.
    automatic_third_party_routes: std.AutoHashMap(u32, *AutomaticThirdPartyRoute),
    /// Borrowed backlinks for automatic redirects whose synthetic result
    /// answer lives on this peer, keyed by that synthetic answer id.
    incoming_automatic_third_party_routes: std.AutoHashMap(u32, *AutomaticThirdPartyRoute),
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
    /// Descending cursor for REFLECTED (loopback) question ids — see
    /// `allocateLoopbackQuestion`. Loopback Call frames are fed back into this
    /// peer's own `handleFrame`, so their id also lands in the inbound answer
    /// namespace, which the REMOTE owns. Every implementation hands out wire
    /// question ids ascending from 0, so drawing loopback ids from the top of
    /// the space keeps the two apart; nothing carrying one of these ids is ever
    /// written to a socket.
    next_loopback_question_id: u32 = std.math.maxInt(u32),
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
    /// Terminal transport-close notification is delivered at most once.
    /// `detachTransport` deliberately does not set this bit: detaching a live
    /// transport is non-terminal and permits rebinding.
    transport_close_notified: bool = false,
    /// Typed close cause captured from the transport the instant its close
    /// callback fired, BEFORE pending questions are cancelled — so a
    /// question callback receiving the synthetic "disconnected" exception
    /// can already read it (`lastDisconnectCause`). Transports that expose
    /// no `closeCause()` capability leave it `.unknown`; the synthetic
    /// exception's reason text never varies with it. Reset on transport
    /// attach so a rebound peer does not carry a stale certificate.
    last_disconnect_cause: events.DisconnectCause = .unknown,
    /// Reentrant `deinit()` from a synchronous automatic third-party send or
    /// automatic Call dispatch is deferred until the outer operation unwinds.
    /// Both the source and result peers take this guard while their maps are
    /// borrowed; dispatch depth covers the handler interval between route
    /// publication and terminal result delivery.
    automatic_third_party_operation_depth: u32 = 0,
    automatic_third_party_deinit_deferred: bool = false,
    /// A terminal close observed while an automatic route operation borrows
    /// either peer is completed when the outermost operation unwinds. Unlike
    /// `detachTransport`, this remains terminal and still notifies the user
    /// exactly once.
    automatic_third_party_close_deferred: bool = false,
    /// Encloses only inbound `.vat_network` Call dispatch, so a source deinit
    /// requested from a nested result send runs after the handler and dispatch
    /// error path have stopped borrowing the peer. Ordinary RPC frames retain
    /// their established deinit semantics.
    automatic_third_party_dispatch_depth: u32 = 0,
    /// Keeps Join-owned maps and canonical cross-peer HostedJoin borrows live
    /// across synchronous Return/Finish/network callbacks. Close/deinit use
    /// the same deferred lifecycle path as automatic redirected results.
    join_operation_depth: u32 = 0,
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
    /// Earliest deadline across partial buckets, relays, and hosted Accepts in
    /// this peer's clock domain. The inbound hot path reads only this field.
    next_join_deadline_ns: ?i64 = null,
    join_sweep_in_progress: bool = false,
    join_clock_sample_in_progress: bool = false,
    /// One record replaces each detached complete Join bucket until canonical
    /// HostedJoin ownership is published or the completion is failed.
    completing_join_records: usize = 0,
    /// O(1) counted subset of `completing_join_answers`; terminal settlement
    /// tombstones intentionally remain invisible to gauges/pressure admission.
    completing_join_answer_records: usize = 0,
    /// Captured JoinNetwork callbacks in flight. Not an operability gauge, but
    /// attach/detach treats every borrow as dependent state even after the
    /// source bucket has been detached.
    join_network_borrows: usize = 0,
    /// Provision bytes charged exactly once by canonical HostedJoin owners.
    join_accept_bytes: usize = 0,
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
    /// Prevent nested transport delivery from recursively retrying the same
    /// deferred Finish batch.
    finish_maintenance_in_progress: bool = false,
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
            .retained_questions = retained_question_state.Registry.init(allocator),
            .question_param_export_refs = std.AutoHashMap(u32, std.ArrayList(u32)).init(allocator),
            .resolved_answers = std.AutoHashMap(u32, ResolvedAnswer).init(allocator),
            .active_inbound_questions = std.AutoHashMap(u32, bool).init(allocator),
            .resolving_answers = std.AutoHashMap(u32, void).init(allocator),
            .finished_early_answers = std.AutoHashMap(u32, bool).init(allocator),
            .failed_answers = std.AutoHashMap(u32, FailedAnswer).init(allocator),
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
            .forward_vine_relay_links = .empty,
            .handoff_pickup_links = .empty,
            .cross_peer_proxy_links = .empty,
            .cross_peer_join_relay_links = .empty,
            .pending_joins = std.AutoHashMap(u32, JoinState).init(allocator),
            .pending_join_questions = std.AutoHashMap(u32, PendingJoinQuestion).init(allocator),
            .completing_join_answers = std.AutoHashMap(u32, CompletingJoinAnswer).init(allocator),
            .pending_join_relays = std.AutoHashMap(u32, CrossPeerJoinRelay).init(allocator),
            .pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)).init(allocator),
            .pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(allocator),
            .hosted_joins = std.AutoHashMap(*HostedJoin, void).init(allocator),
            .pending_join_accepts = std.StringHashMap(PendingJoinAccept).init(allocator),
            .pending_join_result_answers = std.AutoHashMap(u32, PendingJoinResultAnswer).init(allocator),
            .join_accept_host_links = .empty,
            .join_coordinator_accept_links = .empty,
            .join_coordinator_result_links = .empty,
            .pending_third_party_awaits = std.StringHashMap(PendingThirdPartyAwait).init(allocator),
            .pending_third_party_answers = std.StringHashMap(u32).init(allocator),
            .pending_third_party_returns = std.AutoHashMap(u32, []u8).init(allocator),
            .adopted_third_party_answers = std.AutoHashMap(u32, u32).init(allocator),
            .resolved_imports = std.AutoHashMap(u32, ResolvedImport).init(allocator),
            .pending_embargoes = std.AutoHashMap(u32, u32).init(allocator),
            .loopback_questions = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_yourself = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_third_party = std.AutoHashMap(u32, ?[]u8).init(allocator),
            .automatic_third_party_routes = std.AutoHashMap(u32, *AutomaticThirdPartyRoute).init(allocator),
            .incoming_automatic_third_party_routes = std.AutoHashMap(u32, *AutomaticThirdPartyRoute).init(allocator),
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
    pub fn clockNow(self: *const Peer) ?i64 {
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

    pub fn newJoinDeadline(self: *Peer) !?i64 {
        const ttl_ms = self.timeouts.join_timeout_ms orelse return null;
        // A nested Join must never turn a configured TTL into an implicit
        // opt-out merely because an application Clock callback re-entered the
        // peer. The nested admission is refused generically by its caller.
        if (self.join_clock_sample_in_progress) return error.JoinClockReentrant;
        self.join_clock_sample_in_progress = true;
        defer self.join_clock_sample_in_progress = false;
        const now = self.clockNow() orelse return null;
        const sum = @as(i128, now) + @as(i128, ttl_ms) * std.time.ns_per_ms;
        return @intCast(std.math.clamp(sum, std.math.minInt(i64), std.math.maxInt(i64)));
    }

    pub fn noteJoinDeadline(self: *Peer, deadline_ns: ?i64) void {
        const deadline = deadline_ns orelse return;
        if (self.next_join_deadline_ns) |next| {
            if (deadline >= next) return;
        }
        self.next_join_deadline_ns = deadline;
    }

    pub fn refreshNextJoinDeadline(self: *Peer) void {
        var next: ?i64 = null;
        var join_it = self.pending_joins.valueIterator();
        while (join_it.next()) |join_state| noteEarliestDeadline(&next, join_state.deadline_ns);
        var relay_it = self.pending_join_relays.valueIterator();
        while (relay_it.next()) |relay| noteEarliestDeadline(&next, relay.deadline_ns);
        var accept_it = self.pending_join_accepts.valueIterator();
        while (accept_it.next()) |accept| noteEarliestDeadline(&next, accept.hosted.deadline_ns);
        self.next_join_deadline_ns = next;
    }

    fn noteEarliestDeadline(next: *?i64, candidate: ?i64) void {
        const deadline = candidate orelse return;
        if (next.*) |current| {
            if (deadline >= current) return;
        }
        next.* = deadline;
    }

    fn sampleJoinSweepNow(self: *Peer) ?i64 {
        if (self.join_sweep_in_progress or self.join_clock_sample_in_progress) return null;
        const deadline = self.next_join_deadline_ns orelse return null;
        self.join_clock_sample_in_progress = true;
        const now_opt = self.clockNow();
        self.join_clock_sample_in_progress = false;
        const now = now_opt orelse return null;
        if (now < deadline) return null;
        return now;
    }

    /// Detach every due L4 Join phase. The earliest-deadline cache makes the
    /// common inbound-frame/tick path O(1); a due pass is allocation-free and
    /// detaches records before any Return, Finish, or network callback can
    /// re-enter. Returns the number of phase records detached (buckets, relays,
    /// or hosted Accept provisions), not the number of outbound calls canceled.
    pub fn sweepExpiredJoins(self: *Peer) usize {
        self.assertThreadAffinity();
        self.enterJoinOperation();
        defer self.leaveJoinOperation();
        const now = self.sampleJoinSweepNow() orelse return 0;
        self.join_sweep_in_progress = true;
        defer {
            self.join_sweep_in_progress = false;
            self.refreshNextJoinDeadline();
        }

        var detached: usize = 0;
        while (true) {
            var expired_id: ?u32 = null;
            var it = self.pending_joins.iterator();
            while (it.next()) |entry| {
                const deadline = entry.value_ptr.deadline_ns orelse continue;
                if (now >= deadline) {
                    expired_id = entry.key_ptr.*;
                    break;
                }
            }
            const join_id = expired_id orelse break;
            const removed = self.pending_joins.fetchRemove(join_id) orelse continue;
            var join_state = removed.value;
            // Admission pre-reserved this allocation-free settlement table.
            // Exchange the bucket+part footprint for one transition record
            // plus one tombstone per answer before the first observer or send.
            var part_it = join_state.parts.iterator();
            while (part_it.next()) |entry| {
                self.putCompletingJoinAnswerAssumeCapacity(entry.value_ptr.question_id, false);
                _ = self.pending_join_questions.remove(entry.value_ptr.question_id);
            }
            part_it = join_state.parts.iterator();
            while (part_it.next()) |entry| {
                events.emitJoinTimeout(self.observer, entry.value_ptr.question_id);
                self.sendReturnException(entry.value_ptr.question_id, "join unavailable") catch |err| {
                    log.debug("expired Join exception send failed for answer {}: {}", .{ entry.value_ptr.question_id, err });
                };
            }
            part_it = join_state.parts.iterator();
            while (part_it.next()) |entry| {
                _ = self.removeCompletingJoinAnswer(entry.value_ptr.question_id);
                _ = self.finished_early_answers.remove(entry.value_ptr.question_id);
            }
            JoinState.deinit(&join_state, self.allocator);
            detached += 1;
        }

        while (true) {
            var expired_id: ?u32 = null;
            var it = self.pending_join_relays.iterator();
            while (it.next()) |entry| {
                const deadline = entry.value_ptr.deadline_ns orelse continue;
                if (now >= deadline) {
                    expired_id = entry.key_ptr.*;
                    break;
                }
            }
            const answer_id = expired_id orelse break;
            if (self.retirePendingJoinRelayTerminal(answer_id, true, true)) detached += 1;
        }

        while (true) {
            var expired: ?*HostedJoin = null;
            var it = self.pending_join_accepts.valueIterator();
            while (it.next()) |accept| {
                const deadline = accept.hosted.deadline_ns orelse continue;
                if (now >= deadline) {
                    expired = accept.hosted;
                    break;
                }
            }
            const hosted = expired orelse break;
            const owner = hosted.owner_peer;
            const observer = owner.observer;
            const timeout_answer_id = hosted.timeout_answer_id;
            if (owner != self) owner.enterJoinOperation();
            defer if (owner != self) owner.leaveJoinOperation();
            // Canonical maps, counters, and backlinks retire before observer or
            // network callbacks. The operation guard keeps `owner` alive while
            // the captured observer is notified after retirement.
            owner.cancelHostedJoin(hosted);
            if (timeout_answer_id) |answer_id| events.emitJoinTimeout(observer, answer_id);
            detached += 1;
        }
        return detached;
    }

    /// Set or replace the deadline on an outstanding question, overriding
    /// any default stamped at send time. Requires a time source.
    pub fn setQuestionDeadline(self: *Peer, question_id: u32, timeout_ms: u64) !void {
        self.assertThreadAffinity();
        const now = self.clockNow() orelse return error.NoClockConfigured;
        const logical_question_id = if (self.retained_questions.contains(question_id))
            question_id
        else
            self.retained_questions.logicalQuestionIdForWire(question_id) orelse question_id;
        const wire_answer_id = if (self.retained_questions.get(logical_question_id)) |retained|
            retained.wire_answer_id
        else
            question_id;
        const entry = self.questions.getPtr(wire_answer_id) orelse return error.UnknownQuestion;
        if (entry.cancelled) return error.QuestionCancelled;
        entry.deadline_ns = now + msToNs(timeout_ms);
    }

    /// Remove the deadline from an outstanding question.
    pub fn clearQuestionDeadline(self: *Peer, question_id: u32) !void {
        self.assertThreadAffinity();
        const wire_answer_id = if (self.retained_questions.get(question_id)) |retained|
            retained.wire_answer_id
        else
            question_id;
        const entry = self.questions.getPtr(wire_answer_id) orelse return error.UnknownQuestion;
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
            peer_transport.callbacks.bindingForConnection(
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
            conn.on_tick = peer_transport.callbacks.onConnectionTickFor(Peer, ConnPtr);
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
        self.last_disconnect_cause = .unknown;
    }

    /// Typed close cause of the most recent transport death ("death
    /// certificate"). `.unknown` until a transport that can distinguish
    /// causes (QUIC) closes; valid inside question callbacks cancelled by
    /// the disconnect and inside `on_close`. `.stateless_reset` proves
    /// the remote endpoint lost its connection state (crash-restart) —
    /// the signal a warm-restore layer keys on.
    pub fn lastDisconnectCause(self: *const Peer) events.DisconnectCause {
        return self.last_disconnect_cause;
    }

    /// Detach the transport without closing it, clearing all transport callbacks.
    pub fn detachTransport(self: *Peer) void {
        self.assertThreadAffinity();
        peer_transport.state.detachTransportForPeer(Peer, self);
    }

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
        /// Experimental automatic redirect. The peer resolves the call's
        /// `ThirdPartyToContact` through its attached `VatNetwork`, registers a
        /// synthetic answer on the introduced peer, and delivers the handler's
        /// Return there before answering the immediate caller with
        /// `resultsSentElsewhere`.
        vat_network,
    };

    /// Select how inbound `sendResultsTo = thirdParty` calls are handled. Per
    /// `Peer`, not per call: in the canonical handoff topology only the callee's
    /// peer facing the introducer needs `.application` or `.vat_network`.
    pub fn setThirdPartyResultPolicy(self: *Peer, policy: ThirdPartyResultPolicy) void {
        self.assertThreadAffinity();
        self.third_party_result_policy = policy;
    }

    /// Attach the Level-3 three-party addressing seam. Required before
    /// originating `Provide`/`Accept` handoffs (`sendProvide`/`sendAccept`) or
    /// selecting automatic `.vat_network` redirected results; a plain
    /// two-party peer never needs one. The network's `ctx` must outlive the
    /// peer.
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
    pub fn attachJoinNetwork(self: *Peer, network: JoinNetwork) !void {
        self.assertThreadAffinity();
        if (self.join_network) |current| {
            if (joinNetworksEqual(current, network)) return;
        }
        if (self.joinNetworkInUse()) return error.JoinNetworkInUse;
        self.join_network = network;
    }

    /// Detach the Experimental L4 Join network only when no partial or hosted
    /// lifecycle still depends on it. This prevents replacing callbacks while
    /// provisions remain live; HostedJoin also captures the creating network
    /// so later cleanup never consults mutable peer configuration.
    pub fn detachJoinNetwork(self: *Peer) !void {
        self.assertThreadAffinity();
        if (self.join_network == null) return;
        if (self.joinNetworkInUse()) return error.JoinNetworkInUse;
        self.join_network = null;
    }

    fn joinNetworksEqual(a: JoinNetwork, b: JoinNetwork) bool {
        return a.ctx == b.ctx and
            a.host_join_result == b.host_join_result and
            a.connect_joined == b.connect_joined and
            a.cancel_host_join_result == b.cancel_host_join_result;
    }

    fn joinNetworkInUse(self: *const Peer) bool {
        return self.join_network_borrows != 0 or
            self.pending_joins.count() != 0 or
            self.hosted_joins.count() != 0;
    }

    pub fn beginJoinNetworkBorrow(self: *Peer) !JoinNetwork {
        const network = self.join_network orelse return error.NoJoinNetwork;
        self.join_network_borrows = try std.math.add(usize, self.join_network_borrows, 1);
        return network;
    }

    pub fn endJoinNetworkBorrow(self: *Peer) void {
        std.debug.assert(self.join_network_borrows > 0);
        self.join_network_borrows -= 1;
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
        return peer_transport.state.hasAttachedTransportForPeer(Peer, self);
    }

    /// Close the attached transport, signaling the remote peer.
    pub fn closeAttachedTransport(self: *Peer) void {
        self.assertThreadAffinity();
        peer_transport.state.closeAttachedTransportForPeer(Peer, self);
    }

    /// Return whether the attached transport is currently in the process of closing.
    pub fn isAttachedTransportClosing(self: *const Peer) bool {
        self.assertThreadAffinity();
        return peer_transport.state.isAttachedTransportClosingForPeer(Peer, self);
    }

    /// Detach and return the owned connection, cast to `ConnPtr`.
    /// Returns `null` if no transport is attached or the context type does not match.
    pub fn takeAttachedConnection(self: *Peer, comptime ConnPtr: type) ?ConnPtr {
        self.assertThreadAffinity();
        return peer_transport.state.takeAttachedConnectionForPeer(
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
        return peer_transport.state.getAttachedConnectionForPeer(
            Peer,
            ConnPtr,
            self,
        );
    }

    // ================= Lifecycle: deinit (P10) ==============================
    //
    // The body — teardown order is load-bearing — lives as ONE unit in
    // peer_lifecycle.zig (Lifecycle(Peer).deinit). This frozen thunk keeps
    // the exact public signature.

    const LifecycleImpl = peer_lifecycle.Lifecycle(Peer);
    pub const disconnected_reason_text = disconnected_reason;
    pub const deadline_reason_text = deadline_reason;
    pub const shutdown_reason_text = shutdown_reason;

    /// Tear down the peer. Body (one unit, order load-bearing) in
    /// `peer_lifecycle.zig`; see the BUG #55 liveness note there.
    pub fn deinit(self: *Peer) void {
        return LifecycleImpl.deinit(self);
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
        /// Caller-owned retained answers, including retained calls still
        /// awaiting their terminal Return. Excludes answers whose ownership
        /// has transferred into another protocol lifecycle.
        retained_questions: usize,
        /// Retained source answers currently owned by a coupled protocol
        /// lifecycle and awaiting its exactly-once Finish.
        transferred_retained_questions: usize,
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
        /// Accepts parked by this connection in its vat's shared provision
        /// index while they await a matching Provide.
        parked_accepts: usize,
        /// Per-Accept attributable bytes for `parked_accepts`: normalized
        /// recipient-token bytes plus embargo bytes.
        parked_accept_bytes: usize,
        /// Aggregate live L4 Join records (buckets, parts, relays, hosted
        /// provisions, result answers, and direct Accept records).
        join_records: usize,
        /// Parts retained by incomplete inbound Join buckets.
        join_parts: usize,
        /// Provision bytes charged once by canonical HostedJoin owners.
        join_accept_bytes: usize,
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
            .retained_questions = self.retained_questions.countCallerOwned(),
            .transferred_retained_questions = self.retained_questions.countTransferred(),
            .cancelled_questions = cancelled,
            .active_inbound_questions = self.active_inbound_questions.count(),
            .exports = self.exports.count(),
            .resolved_answers = self.resolved_answers.count(),
            .resolved_imports = self.resolved_imports.count(),
            .pending_queued_calls = queued.calls,
            .pending_queued_call_bytes = queued.bytes,
            .parked_accepts = self.parked_accept_count,
            .parked_accept_bytes = self.parked_accept_bytes,
            .join_records = self.joinRecordCount(),
            .join_parts = self.pending_join_questions.count(),
            .join_accept_bytes = self.join_accept_bytes,
            .persistent_exports = self.persistent_exports.count(),
            .saves_served = self.saves_served,
            .restores_served = self.restores_served,
        };
    }

    pub const PendingQueuedCallStats = struct {
        calls: usize = 0,
        bytes: usize = 0,
    };

    pub fn saturatingAdd(a: usize, b: usize) usize {
        return std.math.add(usize, a, b) catch std.math.maxInt(usize);
    }

    pub fn joinRecordCount(self: *const Peer) usize {
        var total: usize = self.pending_joins.count();
        total = saturatingAdd(total, self.pending_join_questions.count());
        total = saturatingAdd(total, self.completing_join_answer_records);
        total = saturatingAdd(total, self.completing_join_records);
        total = saturatingAdd(total, self.pending_join_relays.count());
        total = saturatingAdd(total, self.hosted_joins.count());
        total = saturatingAdd(total, self.pending_join_accepts.count());
        total = saturatingAdd(total, self.pending_join_result_answers.count());
        return total;
    }

    pub fn ensureJoinRecordCapacity(self: *const Peer, additional: usize) !void {
        const current = self.joinRecordCount();
        if (current > self.limits.max_pending_join_records) return error.JoinRecordLimitExceeded;
        if (additional > self.limits.max_pending_join_records - current) {
            return error.JoinRecordLimitExceeded;
        }
    }

    pub fn ensureCountLimit(found_existing: bool, current_count: usize, max_count: usize) !void {
        if (!found_existing and current_count >= max_count) return error.PeerLimitExceeded;
    }

    pub fn ensureByteLimit(current_bytes: usize, added_bytes: usize, max_bytes: usize) !void {
        if (current_bytes > max_bytes) return error.PeerLimitExceeded;
        if (added_bytes > max_bytes - current_bytes) return error.PeerLimitExceeded;
    }

    pub fn joinWireReason(err: anyerror) []const u8 {
        return switch (err) {
            error.PeerLimitExceeded,
            error.JoinRecordLimitExceeded,
            error.JoinClockReentrant,
            error.DuplicateJoinProvision,
            error.UnknownDirectJoinPeer,
            => "join unavailable",
            else => @errorName(err),
        };
    }

    fn addPendingQueuedCallStats(totals: *PendingQueuedCallStats, list: *const std.ArrayList(PendingCall)) void {
        for (list.items) |pending| {
            totals.calls = saturatingAdd(totals.calls, 1);
            totals.bytes = saturatingAdd(totals.bytes, pending.frame.len);
        }
    }

    pub fn pendingQueuedCallStats(self: *const Peer) PendingQueuedCallStats {
        var totals = PendingQueuedCallStats{};
        var pending_it = self.pending_promises.valueIterator();
        while (pending_it.next()) |list| addPendingQueuedCallStats(&totals, list);
        var pending_export_it = self.pending_export_promises.valueIterator();
        while (pending_export_it.next()) |list| addPendingQueuedCallStats(&totals, list);
        return totals;
    }

    pub fn ensurePendingQueuedCallBudget(
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

    pub fn optionalPayloadBytes(payload: ?[]u8) usize {
        return if (payload) |bytes| bytes.len else 0;
    }

    pub fn sendResultsToThirdPartyBytesExcluding(self: *const Peer, answer_id: u32) usize {
        var total: usize = 0;
        var it = self.send_results_to_third_party.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* == answer_id) continue;
            total = saturatingAdd(total, optionalPayloadBytes(entry.value_ptr.*));
        }
        return total;
    }

    pub fn pendingThirdPartyReturnBytesExcluding(self: *const Peer, answer_id: u32) usize {
        var total: usize = 0;
        var it = self.pending_third_party_returns.iterator();
        while (it.next()) |entry| {
            if (entry.key_ptr.* == answer_id) continue;
            total = saturatingAdd(total, entry.value_ptr.*.len);
        }
        return total;
    }

    pub fn pendingAcceptEmbargoKeyBytes(self: *const Peer) usize {
        var total: usize = 0;
        var it = self.pending_accepts_by_embargo.iterator();
        while (it.next()) |entry| {
            total = saturatingAdd(total, entry.key_ptr.*.len);
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

    pub fn ensureProvideBudget(self: *Peer, question_id: u32, recipient_key: []const u8) !void {
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

    pub fn ensureJoinBudget(self: *Peer, join_key_part: JoinKeyPart, question_id: u32) !void {
        if (@as(usize, join_key_part.part_count) > self.limits.max_join_parts_per_join) {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .join_parts,
                join_key_part.part_count,
                self.limits.max_join_parts_per_join,
                error.PeerLimitExceeded,
            );
            return error.PeerLimitExceeded;
        }
        const additional: usize = @as(usize, 1) +
            @as(usize, @intFromBool(!self.pending_joins.contains(join_key_part.join_id)));
        self.ensureJoinRecordCapacity(additional) catch {
            events.emitResourceRejection(
                self.observer,
                .peer,
                .unknown,
                .join_records,
                saturatingAdd(self.joinRecordCount(), additional),
                self.limits.max_pending_join_records,
                error.PeerLimitExceeded,
            );
            return error.PeerLimitExceeded;
        };
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

        // Admission also reserves the allocation needed to detach every live
        // Join answer into the callback-safe completing namespace. Completion
        // and expiry must be allocation-free after the final part commits: an
        // OOM at that point cannot leave a complete raw-TTL-null bucket with no
        // future frame capable of retriggering it. The result map receives the
        // same aggregate reservation so L4 fanout can publish all steady-state
        // answer refs without allocating after `hostJoinResult` side effects.
        const answer_reservations = std.math.add(
            usize,
            self.pending_join_questions.count(),
            self.completing_join_answers.count(),
        ) catch return error.PeerLimitExceeded;
        const with_new_answer = std.math.add(usize, answer_reservations, 1) catch
            return error.PeerLimitExceeded;
        const completing_capacity = std.math.cast(u32, with_new_answer) orelse
            return error.PeerLimitExceeded;
        try self.completing_join_answers.ensureTotalCapacity(completing_capacity);
        const result_reservations = std.math.add(
            usize,
            self.pending_join_result_answers.count(),
            with_new_answer,
        ) catch return error.PeerLimitExceeded;
        const result_capacity = std.math.cast(u32, result_reservations) orelse
            return error.PeerLimitExceeded;
        try self.pending_join_result_answers.ensureTotalCapacity(result_capacity);
    }

    pub fn ensurePendingThirdPartyAwaitBudget(self: *Peer, completion_key: []const u8) !void {
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

    pub fn ensurePendingThirdPartyAnswerBudget(self: *Peer, completion_key: []const u8) !void {
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

    pub fn ensureThirdPartyAdoptionBudget(self: *Peer, adopted_answer_id: u32) !void {
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

    pub fn addExportWithDeinit(self: *Peer, exported: Export, deinit_ctx: ?ExportDeinitCtxFn) !u32 {
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

    // ================= L3 origination + inbound Provide/Accept/Join arms ====
    //
    // Bodies live in provide/peer_provide_origination.zig and
    // provide/peer_provide_inbound.zig, generic over Peer (the
    // JoinCoordinator extraction contract).

    const ProvideOriginationImpl = peer_provide_origination.ProvideOrigination(Peer);
    const ProvideInboundImpl = peer_provide_inbound.ProvideInbound(Peer);

    /// Body in `provide/peer_provide_origination.zig`.
    pub fn sendProvide(
        self: *Peer,
        provided_target: protocol.MessageTarget,
        recipient: message.AnyPointerReader,
        host_of_recipient: *Peer,
        contact_payload: []const u8,
    ) !ProvideHandle {
        return ProvideOriginationImpl.sendProvide(self, provided_target, recipient, host_of_recipient, contact_payload);
    }

    /// Body in `provide/peer_provide_origination.zig`.
    pub fn sendProvideFromRetainedAnswer(
        self: *Peer,
        retained_question_id: u32,
        ops: []const protocol.PromisedAnswerOp,
        recipient: message.AnyPointerReader,
        host_of_recipient: *Peer,
        contact_payload: []const u8,
    ) !ProvideHandle {
        return ProvideOriginationImpl.sendProvideFromRetainedAnswer(self, retained_question_id, ops, recipient, host_of_recipient, contact_payload);
    }

    /// Body in `provide/peer_provide_origination.zig`.
    pub fn sendAccept(
        self: *Peer,
        provision: message.AnyPointerReader,
        embargo: ?[]const u8,
        ctx: *anyopaque,
        on_return: QuestionCallback,
    ) !u32 {
        return ProvideOriginationImpl.sendAccept(self, provision, embargo, ctx, on_return);
    }

    /// Body in `provide/peer_provide_origination.zig`.
    pub fn sendAcceptNoRestore(
        self: *Peer,
        provision: message.AnyPointerReader,
        embargo: ?[]const u8,
        ctx: *anyopaque,
        on_return: QuestionCallback,
        suppress_auto_finish: bool,
    ) !u32 {
        return ProvideOriginationImpl.sendAcceptNoRestore(self, provision, embargo, ctx, on_return, suppress_auto_finish);
    }

    /// Body in `provide/peer_provide_origination.zig`.
    pub fn sendJoinExperimental(
        self: *Peer,
        target: protocol.MessageTarget,
        key_part: ?message.AnyPointerReader,
        ctx: *anyopaque,
        on_return: QuestionCallback,
    ) !u32 {
        return ProvideOriginationImpl.sendJoinExperimental(self, target, key_part, ctx, on_return);
    }

    /// Body in `provide/peer_provide_origination.zig`.
    fn sendJoinExperimentalWithAutoFinish(
        self: *Peer,
        target: protocol.MessageTarget,
        key_part: ?message.AnyPointerReader,
        ctx: *anyopaque,
        on_return: QuestionCallback,
        suppress_auto_finish: bool,
    ) !u32 {
        return ProvideOriginationImpl.sendJoinExperimentalWithAutoFinish(self, target, key_part, ctx, on_return, suppress_auto_finish);
    }

    /// Body in `provide/peer_provide_origination.zig`.
    pub fn allocateUnusedThirdPartyAnswerId(self: *Peer) !u32 {
        return ProvideOriginationImpl.allocateUnusedThirdPartyAnswerId(self);
    }

    /// Body in `provide/peer_provide_origination.zig`.
    pub fn sendThirdPartyAnswerWithId(
        self: *Peer,
        answer_id: u32,
        completion: message.AnyPointerReader,
    ) !void {
        return ProvideOriginationImpl.sendThirdPartyAnswerWithId(self, answer_id, completion);
    }

    /// Body in `provide/peer_provide_origination.zig`.
    pub fn sendThirdPartyAnswer(
        self: *Peer,
        completion: message.AnyPointerReader,
    ) !u32 {
        return ProvideOriginationImpl.sendThirdPartyAnswer(self, completion);
    }

    /// Body in `provide/peer_provide_origination.zig`.
    pub fn registerPendingThirdPartyAwait(
        self: *Peer,
        completion: message.AnyPointerReader,
        ctx: *anyopaque,
        on_return: QuestionCallback,
    ) !void {
        return ProvideOriginationImpl.registerPendingThirdPartyAwait(self, completion, ctx, on_return);
    }

    /// Body in `provide/peer_provide_inbound.zig`.
    fn queueEmbargoedAccept(
        self: *Peer,
        answer_id: u32,
        provided_question_id: u32,
        embargo: []const u8,
    ) !void {
        return ProvideInboundImpl.queueEmbargoedAccept(self, answer_id, provided_question_id, embargo);
    }

    /// Body in `provide/peer_provide_inbound.zig`.
    fn handleProvide(self: *Peer, provide: protocol.Provide) !void {
        return ProvideInboundImpl.handleProvide(self, provide);
    }

    /// Body in `provide/peer_provide_inbound.zig`.
    fn handleAccept(self: *Peer, accept: protocol.Accept) !void {
        return ProvideInboundImpl.handleAccept(self, accept);
    }

    /// Body in `provide/peer_provide_inbound.zig`.
    fn handleJoin(self: *Peer, join: protocol.Join) !void {
        return ProvideInboundImpl.handleJoin(self, join);
    }

    /// Body in `provide/peer_provide_inbound.zig`.
    fn handleThirdPartyAnswer(self: *Peer, third_party_answer: protocol.ThirdPartyAnswer) !void {
        return ProvideInboundImpl.handleThirdPartyAnswer(self, third_party_answer);
    }

    /// Body in `provide/peer_provide_inbound.zig`.
    fn completeJoinWithL4Runtime(self: *Peer, join_id: u32) !void {
        return ProvideInboundImpl.completeJoinWithL4Runtime(self, join_id);
    }

    /// Call handler installed on a vine export. A vine is primarily a
    /// liveness/refcount anchor for a three-party handoff. When a caller (VatA)
    /// pipelined calls on the handed-off promise, those calls are replayed onto
    /// this vine and are FORWARDED to VatC by `maybeForwardVineCall` (issue #56)
    /// BEFORE dispatch ever reaches this handler — so this handler only runs for
    /// a call on a vine with no live forwarding coupling (`provide_peer` already
    /// torn down). That is genuine non-handoff misuse
    /// or a raced teardown; answer it with a clean exception rather than
    /// silently dropping it.
    pub fn vineRejectingCall(
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
    /// teardown). The owned target may be either an import or a promised answer;
    /// in both cases the call is forwarded cross-peer to VatC and its result is
    /// relayed back to complete VatA's original pipelined question. Returns
    /// `false` for any non-vine export or a torn-down provide peer, letting
    /// normal export dispatch — and thus
    /// `vineRejectingCall` for the vine — run.
    pub fn maybeForwardVineCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        export_id: u32,
    ) anyerror!bool {
        const coupling = self.outbound_provides.get(export_id) orelse return false;
        const provide_peer = coupling.provide_peer orelse return false;
        if (provide_peer.is_shutting_down) return false;

        try self.forwardVineCallToProvidedTarget(call, inbound_caps, provide_peer, coupling.forward_target);
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
    fn forwardVineCallToProvidedTarget(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        provide_peer: *Peer,
        provided_target: provide_forward_target.ForwardTarget,
    ) !void {
        // One heap ctx serves BOTH callbacks `sendCall` drives with a single
        // ctx: `forwardVineParams` (synchronous build, reads `source_params`)
        // and `forwardVineReturn` (later, reads `recipient_*`).
        // Async ownership transfers into a question on `provide_peer`; allocate
        // the context and its proxy-id list from that same peer so Return and
        // question-teardown callbacks always free with the matching allocator.
        const relay = try provide_peer.allocator.create(ForwardVineCallContext);
        relay.* = .{
            .forward_peer = provide_peer,
            .recipient_peer = self,
            .recipient_answer_id = call.question_id,
            .source_params = call.params,
            .source_inbound_caps = @constCast(inbound_caps),
        };
        var relay_owned = true;
        errdefer if (relay_owned) ForwardVineCallContext.deinit(provide_peer.allocator, relay);
        try self.registerForwardVineRelay(relay);

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
        const forwarded_question_id = provide_peer.sendForwardedVineCallTarget(
            provided_target,
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
            relay.recipient_answer_pending = false;
            ForwardVineCallContext.deinit(provide_peer.allocator, relay);
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
            relay.recipient_answer_pending = true;
            q.deinit_ctx = ForwardVineCallContext.deinit;
        } else {
            relay_owned = false;
            ForwardVineCallContext.deinit(provide_peer.allocator, relay);
            self.sendReturnException(call.question_id, "forwarded handoff question disappeared") catch |send_err| {
                log.debug("forwarded pipelined call: failed to settle vanished question {}: {}", .{
                    call.question_id,
                    send_err,
                });
            };
            return;
        }
        relay_owned = false;
    }

    /// Build closure for the forwarded call to VatC: clone VatA's parked params
    /// into the new call's payload, remapping capability pointers into B↔C
    /// proxy exports. Runs synchronously inside `sendCall`.
    fn forwardVineParams(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *ForwardVineCallContext = castCtx(*ForwardVineCallContext, ctx_ptr);
        const recipient = ctx.recipient_peer orelse return error.ForwardVineRecipientClosed;
        const payload_builder = try call_builder.payloadTyped();
        try ctx.forward_peer.clonePayloadAcrossPeers(
            call_builder.call.builder,
            payload_builder,
            ctx.source_params,
            recipient,
            ctx.source_inbound_caps,
            &ctx.created_param_proxy_ids,
            false,
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
        if (recipient) |live_recipient| {
            live_recipient.deregisterForwardVineRelay(ctx);
        }
        ctx.recipient_peer = null;
        ctx.recipient_answer_pending = false;
        defer ForwardVineCallContext.deinit(peer.allocator, ctx);

        const live_recipient = recipient orelse return;
        relayForwardedVineReturn(live_recipient, answer_id, peer, ret, inbound_caps, release_param_caps) catch |err| {
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
    pub fn releaseVineExport(self: *Peer, vine_id: u32) void {
        _ = self.exports.remove(vine_id);
        self.caps.clearExport(vine_id);
    }

    // -- Persistence (sturdy refs, RPC level 2) -------------------------------

    // ================= Persistence hooks + question allocation ==============
    //
    // Bodies live in peer_persistence_hooks.zig and peer_question_alloc.zig,
    // generic over Peer (the JoinCoordinator extraction contract). The pinned
    // RestoreOutcome (peer/persistence.zig) is untouched.

    const PersistenceHooksImpl = peer_persistence_hooks.PersistenceHooks(Peer);
    const QuestionAllocImpl = peer_question_alloc.QuestionAlloc(Peer);
    pub const SaveHandlerFn = SaveHandler;
    pub const ExportRecord = Export;
    pub const ForwardReturnModeEnum = ForwardReturnMode;
    pub const RestoreHandlerFn = RestoreHandler;
    pub const SaveResponseCallbackFn = SaveResponseCallback;
    pub const RestoreResponseCallbackFn = RestoreResponseCallback;
    pub const SaveHookRecord = SaveHook;
    pub const RestoreHookRecord = RestoreHook;
    pub const PersistenceStateRecord = PersistenceState;
    pub const SaveResponseUnion = SaveResponse;
    pub const RestoreResponseUnion = RestoreResponse;
    pub const msToNsHelper = msToNs;

    /// Body in `peer_question_alloc.zig`.
    pub fn removeQuestionAndDeinit(self: *Peer, question_id: u32) void {
        return QuestionAllocImpl.removeQuestionAndDeinit(self, question_id);
    }

    /// Body in `peer_persistence_hooks.zig`.
    pub fn setPersistentExport(self: *Peer, export_id: u32, ctx: *anyopaque, on_save: SaveHandler) !void {
        return PersistenceHooksImpl.setPersistentExport(self, export_id, ctx, on_save);
    }

    /// Body in `peer_persistence_hooks.zig`.
    pub fn clearPersistentExport(self: *Peer, export_id: u32) void {
        return PersistenceHooksImpl.clearPersistentExport(self, export_id);
    }

    /// Body in `peer_persistence_hooks.zig`.
    pub fn setRestorer(self: *Peer, ctx: *anyopaque, on_restore: RestoreHandler) !void {
        return PersistenceHooksImpl.setRestorer(self, ctx, on_restore);
    }

    /// Body in `peer_persistence_hooks.zig`.
    pub fn clearRestorer(self: *Peer) void {
        return PersistenceHooksImpl.clearRestorer(self);
    }

    /// Body in `peer_persistence_hooks.zig`.
    pub fn sendSave(self: *Peer, target_id: u32, ctx: *anyopaque, on_response: SaveResponseCallback) !u32 {
        return PersistenceHooksImpl.sendSave(self, target_id, ctx, on_response);
    }

    /// Body in `peer_persistence_hooks.zig`.
    pub fn sendRestore(
        self: *Peer,
        target_id: u32,
        sturdy_ref: []const u8,
        ctx: *anyopaque,
        on_response: RestoreResponseCallback,
    ) !u32 {
        return PersistenceHooksImpl.sendRestore(self, target_id, sturdy_ref, ctx, on_response);
    }

    /// Pipelined restore aimed at an in-flight bootstrap question's promised
    /// answer, so bootstrap + restore enqueue back-to-back before the
    /// connection loop starts (and ride 0-RTT on a resumed QUIC dial).
    /// Body in `peer_persistence_hooks.zig`.
    pub fn sendRestorePipelined(
        self: *Peer,
        bootstrap_question_id: u32,
        sturdy_ref: []const u8,
        ctx: *anyopaque,
        on_response: RestoreResponseCallback,
    ) !u32 {
        return PersistenceHooksImpl.sendRestorePipelined(self, bootstrap_question_id, sturdy_ref, ctx, on_response);
    }

    /// Body in `peer_persistence_hooks.zig`.
    pub fn dropPersistenceStateForRemovedExport(self: *Peer, export_id: u32) void {
        return PersistenceHooksImpl.dropPersistenceStateForRemovedExport(self, export_id);
    }

    /// Body in `peer_persistence_hooks.zig`.
    fn persistenceOnCall(
        ctx: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        return PersistenceHooksImpl.persistenceOnCall(ctx, peer, call, caps);
    }

    /// Body in `peer_question_alloc.zig`.
    pub fn allocateQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return QuestionAllocImpl.allocateQuestion(self, ctx, on_return);
    }

    /// Body in `peer_question_alloc.zig`.
    pub fn allocateRetainedQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return QuestionAllocImpl.allocateRetainedQuestion(self, ctx, on_return);
    }

    /// Body in `peer_question_alloc.zig`.
    pub fn allocateQuestionNoRestore(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return QuestionAllocImpl.allocateQuestionNoRestore(self, ctx, on_return);
    }

    /// Body in `peer_question_alloc.zig`.
    pub fn allocateLoopbackQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return QuestionAllocImpl.allocateLoopbackQuestion(self, ctx, on_return);
    }

    /// Body in `peer_question_alloc.zig`.
    pub fn allocateLoopbackQuestionNoRestore(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return QuestionAllocImpl.allocateLoopbackQuestionNoRestore(self, ctx, on_return);
    }

    /// Body in `peer_question_alloc.zig`.
    pub fn allocateRetainedLoopbackQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        return QuestionAllocImpl.allocateRetainedLoopbackQuestion(self, ctx, on_return);
    }

    /// Body in `peer_question_alloc.zig`.
    pub fn removeQuestion(self: *Peer, question_id: u32) void {
        return QuestionAllocImpl.removeQuestion(self, question_id);
    }

    /// Body in `peer_question_alloc.zig`.
    pub fn recordQuestionParamExports(
        self: *Peer,
        question_id: u32,
        entries: []const cap_table.OutboundEntry,
    ) !void {
        return QuestionAllocImpl.recordQuestionParamExports(self, question_id, entries);
    }

    /// Body in `peer_question_alloc.zig`.
    pub fn freeQuestionParamExports(self: *Peer, question_id: u32) void {
        return QuestionAllocImpl.freeQuestionParamExports(self, question_id);
    }

    // ================= Lifecycle: shutdown/cancel/deadlines (P10) ===========
    // Bodies in peer_lifecycle.zig.

    /// Body in `peer_lifecycle.zig`.
    pub fn shutdown(self: *Peer, on_complete: ?*const fn (peer: *Peer) void) void {
        return LifecycleImpl.shutdown(self, on_complete);
    }

    /// Body in `peer_lifecycle.zig`.
    pub fn completeShutdown(self: *Peer) void {
        return LifecycleImpl.completeShutdown(self);
    }

    /// Body in `peer_lifecycle.zig`.
    pub fn finishRetainedQuestion(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
    ) !void {
        return LifecycleImpl.finishRetainedQuestion(self, question_id, release_result_caps);
    }

    /// Body in `peer_lifecycle.zig`.
    pub fn claimRetainedQuestionForTransfer(self: *Peer, question_id: u32) !u32 {
        return LifecycleImpl.claimRetainedQuestionForTransfer(self, question_id);
    }

    /// Body in `peer_lifecycle.zig`.
    pub fn rollbackRetainedQuestionTransfer(self: *Peer, question_id: u32) void {
        return LifecycleImpl.rollbackRetainedQuestionTransfer(self, question_id);
    }

    /// Body in `peer_lifecycle.zig`.
    pub fn commitRetainedQuestionTransfer(self: *Peer, question_id: u32) !void {
        return LifecycleImpl.commitRetainedQuestionTransfer(self, question_id);
    }

    /// Body in `peer_lifecycle.zig`.
    fn finishTransferredRetainedQuestion(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
    ) !void {
        return LifecycleImpl.finishTransferredRetainedQuestion(self, question_id, release_result_caps);
    }

    /// Body in `peer_lifecycle.zig`.
    pub fn cancelQuestion(self: *Peer, question_id: u32, reason: []const u8) !void {
        return LifecycleImpl.cancelQuestion(self, question_id, reason);
    }

    /// Body in `peer_lifecycle.zig`.
    pub fn cancelQuestionTyped(
        self: *Peer,
        question_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        return LifecycleImpl.cancelQuestionTyped(self, question_id, reason, ex_type);
    }

    /// Body in `peer_lifecycle.zig`.
    pub fn checkDeadlines(self: *Peer) usize {
        return LifecycleImpl.checkDeadlines(self);
    }

    /// Body in `peer_lifecycle.zig`.
    fn forceCancelAllQuestions(self: *Peer, reason: []const u8, ex_type: protocol.ExceptionType) usize {
        return LifecycleImpl.forceCancelAllQuestions(self, reason, ex_type);
    }

    /// Remove parked third-party-await questions, freeing each question's heap
    /// ctx (via deinit_ctx) and the owned map key. With `only_expired` true,
    /// only deadline-expired awaits are removed; otherwise all are. This bounds
    /// the memory a peer can pin by answering questions with awaitFromThirdParty
    /// and never completing the handoff — otherwise those contexts escape both
    /// deadline enforcement and the shutdown drain entirely.
    pub fn sweepThirdPartyAwaits(self: *Peer, only_expired: bool) usize {
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
            _ = self.retained_questions.retire(removed.value.question_id);
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

    // ================= Promise-export resolution + inbound Resolve ==========
    //
    // Bodies live in peer_promise_exports.zig and peer_resolve_inbound.zig,
    // generic over Peer (the JoinCoordinator extraction contract). The
    // FROZEN resolvePromiseExportToExport/ToException signatures stay
    // byte-identical on Peer.

    /// Heap context threaded through the auto-pickup `Accept` question. Owns a
    /// small deferred-release list for failed pickup callbacks; freed by
    /// `onHandoffAcceptReturn` on the normal async path, by the synchronous
    /// sender after nested delivery settles, or by its `deinit_ctx` if the
    /// accept peer tears down first.
    pub const HandoffPickupContext = struct {
        allocator: std.mem.Allocator,
        /// The peer holding the promise import (VatA↔VatB). Borrowed while the
        /// symmetric `handoff_pickup_links` registration is live.
        promise_peer: ?*Peer,
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
        promise_link_registered: bool = false,
        vine_owned: bool = false,

        pub fn deinitCtx(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
            _ = allocator;
            const ctx: *HandoffPickupContext = @ptrCast(@alignCast(ctx_ptr));
            ctx.deinitSelf();
        }

        pub fn deinitSelf(ctx: *HandoffPickupContext) void {
            if (ctx.promise_peer) |promise_peer| {
                if (ctx.promise_link_registered) {
                    promise_peer.deregisterHandoffPickup(ctx);
                    ctx.promise_link_registered = false;
                }
                ctx.promise_peer = null;
                if (ctx.vine_owned) {
                    ctx.vine_owned = false;
                    promise_peer.releaseImport(ctx.vine_id, 1) catch |err| {
                        log.debug("auto-pickup teardown vine release failed for promise {}: {}", .{
                            ctx.promise_id,
                            err,
                        });
                    };
                }
            }
            ctx.deferred_failed_imports.deinit(ctx.allocator);
            ctx.allocator.destroy(ctx);
        }

        pub fn retainFailedUnretainedImports(
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

        pub fn releaseDeferredFailedImports(ctx: *HandoffPickupContext, accept_peer: *Peer) void {
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

        pub fn finishAcceptAnswer(ctx: *HandoffPickupContext, accept_peer: *Peer, answer_id: u32, no_finish_needed: bool) void {
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

    const PromiseExportsImpl = peer_promise_exports.PromiseExports(Peer);
    const ResolveInboundImpl = peer_resolve_inbound.ResolveInbound(Peer);

    /// Body in `peer_promise_exports.zig`.
    pub fn resolvePromiseExportToExport(self: *Peer, promise_id: u32, export_id: u32) !void {
        return PromiseExportsImpl.resolvePromiseExportToExport(self, promise_id, export_id);
    }

    /// Body in `peer_promise_exports.zig`.
    pub fn resolvePromiseExportToImport(self: *Peer, promise_id: u32, import_id: u32) !void {
        return PromiseExportsImpl.resolvePromiseExportToImport(self, promise_id, import_id);
    }

    /// Body in `peer_promise_exports.zig`.
    pub fn resolvePromiseExportToThirdParty(
        self: *Peer,
        promise_id: u32,
        provide_peer: *Peer,
        provided_target: protocol.MessageTarget,
        recipient: message.AnyPointerReader,
        contact_payload: []const u8,
    ) !ProvideHandle {
        return PromiseExportsImpl.resolvePromiseExportToThirdParty(self, promise_id, provide_peer, provided_target, recipient, contact_payload);
    }

    /// Body in `peer_promise_exports.zig`.
    pub fn resolvePromiseExportToThirdPartyFromRetainedAnswer(
        self: *Peer,
        promise_id: u32,
        provide_peer: *Peer,
        retained_question_id: u32,
        ops: []const protocol.PromisedAnswerOp,
        recipient: message.AnyPointerReader,
        contact_payload: []const u8,
    ) !ProvideHandle {
        return PromiseExportsImpl.resolvePromiseExportToThirdPartyFromRetainedAnswer(self, promise_id, provide_peer, retained_question_id, ops, recipient, contact_payload);
    }

    /// Body in `peer_promise_exports.zig`.
    pub fn resolvePromiseExportToException(self: *Peer, promise_id: u32, reason: []const u8) !void {
        return PromiseExportsImpl.resolvePromiseExportToException(self, promise_id, reason);
    }

    /// Body in `peer_resolve_inbound.zig`.
    fn handleResolve(self: *Peer, resolve_msg: protocol.Resolve) !void {
        return ResolveInboundImpl.handleResolve(self, resolve_msg);
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

    // ================= Outbound Call send family ============================
    //
    // Bodies live in call/peer_call_send.zig, generic over Peer. The thunks
    // below keep every caller-visible name — including the frozen sendCall
    // entry points and generated-code callers — with exact signatures.

    const CallSendImpl = peer_call_send.CallSend(Peer);

    /// Body in `call/peer_call_send.zig`.
    pub fn sendCall(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return CallSendImpl.sendCall(self, target_id, interface_id, method_id, ctx, build, on_return);
    }

    /// Body in `call/peer_call_send.zig`.
    pub fn sendCallWithOptions(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return CallSendImpl.sendCallWithOptions(self, target_id, interface_id, method_id, ctx, build, on_return, options);
    }

    /// Body in `call/peer_call_send.zig`.
    pub fn sendCallGeneratedWithOptions(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return CallSendImpl.sendCallGeneratedWithOptions(self, target_id, interface_id, method_id, ctx, build, on_return, options);
    }

    /// Body in `call/peer_call_send.zig`.
    pub fn sendForwardedVineCall(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return CallSendImpl.sendForwardedVineCall(self, target_id, interface_id, method_id, ctx, build, on_return);
    }

    /// Body in `call/peer_call_send.zig`.
    fn sendForwardedVineCallTarget(
        self: *Peer,
        target: provide_forward_target.ForwardTarget,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return CallSendImpl.sendForwardedVineCallTarget(self, target, interface_id, method_id, ctx, build, on_return);
    }

    /// Body in `call/peer_call_send.zig`.
    pub fn sendCallResolved(
        self: *Peer,
        target: cap_table.ResolvedCap,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return CallSendImpl.sendCallResolved(self, target, interface_id, method_id, ctx, build, on_return);
    }

    /// Body in `call/peer_call_send.zig`.
    pub fn sendCallResolvedWithOptions(
        self: *Peer,
        target: cap_table.ResolvedCap,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return CallSendImpl.sendCallResolvedWithOptions(self, target, interface_id, method_id, ctx, build, on_return, options);
    }

    /// Body in `call/peer_call_send.zig`.
    pub fn sendCallPromised(
        self: *Peer,
        promised: protocol.PromisedAnswer,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return CallSendImpl.sendCallPromised(self, promised, interface_id, method_id, ctx, build, on_return);
    }

    /// Body in `call/peer_call_send.zig`.
    pub fn sendCallPromisedWithOptions(
        self: *Peer,
        promised: protocol.PromisedAnswer,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return CallSendImpl.sendCallPromisedWithOptions(self, promised, interface_id, method_id, ctx, build, on_return, options);
    }

    /// Body in `call/peer_call_send.zig`.
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
        return CallSendImpl.sendCallPromisedWithOps(self, question_id_target, ops, interface_id, method_id, ctx, build, on_return);
    }

    /// Body in `call/peer_call_send.zig`.
    pub fn sendCallPromisedWithOpsWithOptions(
        self: *Peer,
        question_id_target: u32,
        ops: []const protocol.PromisedAnswerOp,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return CallSendImpl.sendCallPromisedWithOpsWithOptions(self, question_id_target, ops, interface_id, method_id, ctx, build, on_return, options);
    }

    /// Body in `call/peer_call_send.zig`.
    pub fn sendCallPromisedWithOpsGeneratedWithOptions(
        self: *Peer,
        question_id_target: u32,
        ops: []const protocol.PromisedAnswerOp,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
        options: CallOptions,
    ) !u32 {
        return CallSendImpl.sendCallPromisedWithOpsGeneratedWithOptions(self, question_id_target, ops, interface_id, method_id, ctx, build, on_return, options);
    }

    pub fn forwardResolvedCall(
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
            third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
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

    pub fn neutralizeJoinRelayQuestion(self: *Peer, question_id: u32) void {
        if (self.questions.getPtr(question_id)) |question| {
            if (question.deinit_ctx) |deinit_ctx| {
                deinit_ctx(self.allocator, question.ctx);
                question.deinit_ctx = null;
            }
            question.cancelled = true;
        }
    }

    pub fn sendJoinRelayFinishAndNeutralize(self: *Peer, question_id: u32, release_result_caps: bool) !void {
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
        try third_party.applyForwardedCallSendResults(
            Peer,
            ctx.peer,
            call_builder,
            ctx.send_results_to,
            ctx.send_results_to_third_party_payload,
            third_party.setForwardedCallThirdPartyFromPayloadForPeerFn(Peer),
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
            third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            finish.freeOwnedFrameForPeerFn(Peer),
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

    // ================= Cross-peer proxy + automatic third-party routes ======
    //
    // Bodies live in third_party/peer_cross_peer_proxy.zig and
    // third_party/peer_third_party_routes.zig, generic over Peer (the
    // JoinCoordinator extraction contract).

    const CrossPeerProxyImpl = peer_cross_peer_proxy.CrossPeerProxy(Peer);
    const ThirdPartyRoutesImpl = peer_third_party_routes.ThirdPartyRoutes(Peer);
    pub const AutomaticThirdPartyRouteRecord = AutomaticThirdPartyRoute;
    pub const automatic_third_party_target_canceled_reason = automatic_third_party_target_canceled;

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn provideTargetsEqual(a: *const ProvideTarget, b: *const ProvideTarget) bool {
        return ThirdPartyRoutesImpl.provideTargetsEqual(a, b);
    }
    pub const CrossPeerCapMapCtx = CrossPeerCapMapContext;
    pub const CrossPeerProxyCallCtx = CrossPeerProxyCallContext;
    pub const CrossPeerReturnRelayCtx = CrossPeerReturnRelayContext;

    pub const JoinOperationGuards = struct {
        peers: [3]*Peer = undefined,
        len: usize = 0,

        pub fn add(self: *@This(), peer: *Peer) void {
            for (self.peers[0..self.len]) |existing| {
                if (existing == peer) return;
            }
            std.debug.assert(self.len < self.peers.len);
            self.peers[self.len] = peer;
            self.len += 1;
        }

        pub fn enter(self: *@This()) void {
            for (self.peers[0..self.len]) |peer| peer.enterJoinOperation();
        }

        pub fn leave(self: *@This()) void {
            var i = self.len;
            while (i != 0) {
                i -= 1;
                self.peers[i].leaveJoinOperation();
            }
            self.len = 0;
        }
    };

    /// A Finish can own an automatic route, target one, or (when numeric
    /// answer-id spaces collide) do both at once. Keep exactly the peers whose
    /// route maps/backlinks are borrowed alive across callback-bearing cleanup,
    /// without widening deferred lifecycle behavior to ordinary Finish paths.
    pub const AutomaticThirdPartyFinishGuards = struct {
        peers: [3]*Peer = undefined,
        len: usize = 0,

        pub fn add(self: *@This(), peer: *Peer) void {
            for (self.peers[0..self.len]) |existing| {
                if (existing == peer) return;
            }
            std.debug.assert(self.len < self.peers.len);
            self.peers[self.len] = peer;
            self.len += 1;
        }

        fn enter(self: *@This()) void {
            for (self.peers[0..self.len]) |peer| peer.enterAutomaticThirdPartyOperation();
        }

        fn leave(self: *@This()) void {
            var index = self.len;
            while (index != 0) {
                index -= 1;
                self.peers[index].leaveAutomaticThirdPartyOperation();
            }
            self.len = 0;
        }
    };

    /// Body in `third_party/peer_cross_peer_proxy.zig`.
    fn exportedCapTag(self: *Peer, cap_id: u32) protocol.CapDescriptorTag {
        return CrossPeerProxyImpl.exportedCapTag(self, cap_id);
    }

    /// Body in `third_party/peer_cross_peer_proxy.zig`.
    pub fn addCrossPeerProxyExport(
        self: *Peer,
        source_peer: *Peer,
        target: cap_table.ResolvedCap,
        release_source_import_id: ?u32,
        release_source_export_pin_id: ?u32,
        release_source_import_pin_id: ?u32,
    ) !u32 {
        return CrossPeerProxyImpl.addCrossPeerProxyExport(self, source_peer, target, release_source_import_id, release_source_export_pin_id, release_source_import_pin_id);
    }

    /// Body in `third_party/peer_cross_peer_proxy.zig`.
    pub fn destroyUnreferencedProxyExport(self: *Peer, id: u32) void {
        return CrossPeerProxyImpl.destroyUnreferencedProxyExport(self, id);
    }

    /// Body in `third_party/peer_cross_peer_proxy.zig`.
    pub fn destroyUnreferencedExport(self: *Peer, id: u32) void {
        return CrossPeerProxyImpl.destroyUnreferencedExport(self, id);
    }

    /// Body in `third_party/peer_cross_peer_proxy.zig`.
    pub fn clonePayloadAcrossPeers(
        self: *Peer,
        builder: *message.MessageBuilder,
        payload_builder: protocol.PayloadBuilder,
        source: protocol.Payload,
        inbound_peer: *Peer,
        inbound_caps: *cap_table.InboundCapTable,
        created_proxy_ids: *std.ArrayList(u32),
        pin_source_caps: bool,
    ) !void {
        return CrossPeerProxyImpl.clonePayloadAcrossPeers(self, builder, payload_builder, source, inbound_peer, inbound_caps, created_proxy_ids, pin_source_caps);
    }

    /// Body in `third_party/peer_cross_peer_proxy.zig`.
    pub fn forwardCrossPeerProxyCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        forward_peer: *Peer,
        target: cap_table.ResolvedCap,
    ) !void {
        return CrossPeerProxyImpl.forwardCrossPeerProxyCall(self, call, inbound_caps, forward_peer, target);
    }

    /// Body in `third_party/peer_cross_peer_proxy.zig`.
    pub fn relayReturnAcrossPeers(
        recipient: *Peer,
        answer_id: u32,
        source_peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
        release_param_caps: bool,
    ) !void {
        return CrossPeerProxyImpl.relayReturnAcrossPeers(recipient, answer_id, source_peer, ret, inbound_caps, release_param_caps);
    }

    /// Body in `third_party/peer_cross_peer_proxy.zig`.
    pub fn buildCrossPeerReturnResults(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) anyerror!void {
        return CrossPeerProxyImpl.buildCrossPeerReturnResults(ctx_ptr, ret_builder);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn enterAutomaticThirdPartyOperation(self: *Peer) void {
        return ThirdPartyRoutesImpl.enterAutomaticThirdPartyOperation(self);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn completeDeferredAutomaticThirdPartyLifecycle(self: *Peer) void {
        return ThirdPartyRoutesImpl.completeDeferredAutomaticThirdPartyLifecycle(self);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn leaveAutomaticThirdPartyOperation(self: *Peer) void {
        return ThirdPartyRoutesImpl.leaveAutomaticThirdPartyOperation(self);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn enterJoinOperation(self: *Peer) void {
        return ThirdPartyRoutesImpl.enterJoinOperation(self);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn leaveJoinOperation(self: *Peer) void {
        return ThirdPartyRoutesImpl.leaveJoinOperation(self);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn clearSendResultsToThirdParty(self: *Peer, answer_id: u32) void {
        return ThirdPartyRoutesImpl.clearSendResultsToThirdParty(self, answer_id);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn noteAutomaticThirdPartyTargetFinish(self: *Peer, answer_id: u32) bool {
        return ThirdPartyRoutesImpl.noteAutomaticThirdPartyTargetFinish(self, answer_id);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn neutralizeAutomaticThirdPartyRoutesOnTargetPeer(self: *Peer) void {
        return ThirdPartyRoutesImpl.neutralizeAutomaticThirdPartyRoutesOnTargetPeer(self);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn neutralizeAutomaticThirdPartyRoutesOnSourcePeer(self: *Peer) void {
        return ThirdPartyRoutesImpl.neutralizeAutomaticThirdPartyRoutesOnSourcePeer(self);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn captureAnyPointerPayload(
        allocator: std.mem.Allocator,
        ptr: ?message.AnyPointerReader,
    ) !?[]u8 {
        return ThirdPartyRoutesImpl.captureAnyPointerPayload(allocator, ptr);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn noteSendResultsToYourself(self: *Peer, answer_id: u32) !void {
        return ThirdPartyRoutesImpl.noteSendResultsToYourself(self, answer_id);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn noteSendResultsToThirdParty(
        self: *Peer,
        answer_id: u32,
        ptr: ?message.AnyPointerReader,
    ) !void {
        return ThirdPartyRoutesImpl.noteSendResultsToThirdParty(self, answer_id, ptr);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn sendAutomaticThirdPartyResults(
        self: *Peer,
        route: *AutomaticThirdPartyRoute,
        ctx: *anyopaque,
        build: ReturnBuildFn,
    ) !void {
        return ThirdPartyRoutesImpl.sendAutomaticThirdPartyResults(self, route, ctx, build);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn sendAutomaticThirdPartyException(
        self: *Peer,
        route: *AutomaticThirdPartyRoute,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        return ThirdPartyRoutesImpl.sendAutomaticThirdPartyException(self, route, reason, ex_type);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    pub fn finalizeAutomaticThirdPartyRoute(
        self: *Peer,
        route: *AutomaticThirdPartyRoute,
        fail_target_reason: ?[]const u8,
    ) void {
        return ThirdPartyRoutesImpl.finalizeAutomaticThirdPartyRoute(self, route, fail_target_reason);
    }

    /// Body in `third_party/peer_third_party_routes.zig`.
    fn clearSendResultsToThirdPartyPayload(self: *Peer, answer_id: u32) void {
        return ThirdPartyRoutesImpl.clearSendResultsToThirdPartyPayload(self, answer_id);
    }

    // ================= Outbound Return send family ==========================
    //
    // Bodies live in return/peer_return_send.zig, generic over Peer (the
    // JoinCoordinator extraction contract). The thunks below keep every
    // caller-visible name, signature, and hook fn-type on Peer itself.

    const ReturnSendImpl = peer_return_send.ReturnSend(Peer);

    /// Send a Return carrying results for a previously received call. `build`
    /// writes the payload; routing precedence (third-party handoff >
    /// results-sent-elsewhere marker > self-loopback > wire) is applied here.
    /// Body in `return/peer_return_send.zig`.
    pub fn sendReturnResults(self: *Peer, answer_id: u32, ctx: *anyopaque, build: ReturnBuildFn) !void {
        return ReturnSendImpl.sendReturnResults(self, answer_id, ctx, build);
    }

    /// Deliver a stashed self-loopback results frame to the local question
    /// named by a `takeFromOtherQuestion` redirect; body in
    /// `return/peer_return_send.zig`.
    fn deliverStashedLoopbackResults(self: *Peer, target_question_id: u32, frame: []const u8) !void {
        return ReturnSendImpl.deliverStashedLoopbackResults(self, target_question_id, frame);
    }

    /// Send a pre-built return frame, tracking outbound cap refs and recording
    /// the resolved answer; body in `return/peer_return_send.zig`.
    pub fn sendPrebuiltReturnFrame(self: *Peer, ret: protocol.Return, frame: []const u8) !void {
        return ReturnSendImpl.sendPrebuiltReturnFrame(self, ret, frame);
    }

    /// Send a Return carrying an exception (type `.failed`); drains queued
    /// pipelined children. Body in `return/peer_return_send.zig`.
    pub fn sendReturnException(self: *Peer, answer_id: u32, reason: []const u8) !void {
        return ReturnSendImpl.sendReturnException(self, answer_id, reason);
    }

    /// `sendReturnException` carrying an explicit `Exception.Type`; body in
    /// `return/peer_return_send.zig`.
    pub fn sendReturnExceptionTyped(
        self: *Peer,
        answer_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        return ReturnSendImpl.sendReturnExceptionTyped(self, answer_id, reason, ex_type);
    }

    /// Send an exception Return without draining queued pipelined children;
    /// body in `return/peer_return_send.zig`.
    pub fn sendReturnExceptionNoDrain(
        self: *Peer,
        answer_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) !void {
        return ReturnSendImpl.sendReturnExceptionNoDrain(self, answer_id, reason, ex_type);
    }

    /// Planner hook (`planPromisedTarget`): the recorded exception for an
    /// already-failed inbound answer, if any; body in
    /// `return/peer_return_send.zig`.
    pub fn lookupFailedAnswer(self: *Peer, answer_id: u32) ?peer_call_targets.FailedAnswerView {
        return ReturnSendImpl.lookupFailedAnswer(self, answer_id);
    }

    /// Fail and drain every pipelined call queued against `answer_id`; body in
    /// `return/peer_return_send.zig`.
    pub fn failQueuedPromisedCalls(
        self: *Peer,
        answer_id: u32,
        reason: []const u8,
        ex_type: protocol.ExceptionType,
    ) void {
        ReturnSendImpl.failQueuedPromisedCalls(self, answer_id, reason, ex_type);
    }

    fn reportNonfatalError(self: *Peer, err: anyerror) void {
        ReturnSendImpl.reportNonfatalError(self, err);
    }

    /// Post-send commit/rollback of a reserved resolved answer; body in
    /// `return/peer_return_send.zig`.
    fn commitOrRollbackResolvedAnswerAfterSend(
        self: *Peer,
        answer_id: u32,
        frame: []const u8,
        reservation: *?ResolvedAnswerReservation,
    ) void {
        ReturnSendImpl.commitOrRollbackResolvedAnswerAfterSend(self, answer_id, frame, reservation);
    }

    /// Send a return with an empty struct result (streaming auto-ack); body in
    /// `return/peer_return_send.zig`.
    pub fn sendReturnEmptyStruct(self: *Peer, answer_id: u32) !void {
        return ReturnSendImpl.sendReturnEmptyStruct(self, answer_id);
    }

    /// Settle an inbound `sendResultsTo = thirdParty` call after the
    /// application delivered the results itself; body in
    /// `return/peer_return_send.zig`.
    pub fn sendReturnResultsSentElsewhere(self: *Peer, answer_id: u32) !void {
        return ReturnSendImpl.sendReturnResultsSentElsewhere(self, answer_id);
    }

    fn sendReturnTag(self: *Peer, answer_id: u32, tag: protocol.ReturnTag) !void {
        return ReturnSendImpl.sendReturnTag(self, answer_id, tag);
    }

    fn sendReturnTakeFromOtherQuestion(self: *Peer, answer_id: u32, other_question_id: u32) !void {
        return ReturnSendImpl.sendReturnTakeFromOtherQuestion(self, answer_id, other_question_id);
    }

    fn sendReturnAcceptFromThirdParty(self: *Peer, answer_id: u32, await_payload: ?[]const u8) !void {
        return ReturnSendImpl.sendReturnAcceptFromThirdParty(self, answer_id, await_payload);
    }

    fn sendReturnFrameWithLoopback(self: *Peer, answer_id: u32, bytes: []const u8) !void {
        return ReturnSendImpl.sendReturnFrameWithLoopback(self, answer_id, bytes);
    }

    /// Single-capability Return carrying the provided target (origin-tagged);
    /// body in `return/peer_return_send.zig`.
    pub fn sendReturnProvidedTarget(self: *Peer, answer_id: u32, target: *const ProvideTarget) !void {
        return ReturnSendImpl.sendReturnProvidedTarget(self, answer_id, target);
    }

    // ================= L3 vat-wide provision hosting (Experimental) =========
    //
    // The VatC hosting implementation (docs: FINAL-v2 as amended by its
    // adversarial verdict) and its canonical drain/teardown procedure live in
    // provision/peer_provision_hosting.zig and
    // provision/peer_provision_drain.zig, generic over Peer (the
    // JoinCoordinator extraction contract). The thunks below keep every
    // caller-visible name, signature, hook fn-type, and doc on Peer itself.

    const ProvisionHosting = peer_provision_hosting.Hosting(Peer);
    const ProvisionDrain = peer_provision_drain.Drain(Peer);

    /// Install the random source for accept-embargo ids (see `EntropySource`).
    pub fn setEntropySource(self: *Peer, source: EntropySource) void {
        self.assertThreadAffinity();
        self.entropy = source;
    }

    /// Attach this peer to a vat-wide provision index. Preconditions: not
    /// already attached, and no pre-existing per-peer or new-L3 handoff state
    /// (the index must see every provision and pending cross-peer Accept from
    /// the first one). In particular, index-first teardown severs the borrowed
    /// pointer but deliberately leaves active provisions/queues alive; those
    /// records must drain before this peer can join a different index.
    pub fn attachProvisionIndex(self: *Peer, index: *ProvisionIndex) !void {
        return ProvisionHosting.attachProvisionIndex(self, index);
    }

    /// Detach from the provision index. Symmetric precondition: no live
    /// provisions owned by this peer and no queued cross-peer accepts — a
    /// cleanly detached peer can re-attach (its handoff maps are empty).
    pub fn detachProvisionIndex(self: *Peer) !void {
        return ProvisionHosting.detachProvisionIndex(self);
    }

    /// Roll back a handoff pin taken by a ladder that has NOT yet transferred
    /// ownership; body in `provision/peer_provision_hosting.zig`.
    pub fn rollbackHandoffExportRef(self: *Peer, id: u32) void {
        ProvisionHosting.rollbackHandoffExportRef(self, id);
    }

    /// PHASE A of Provide registration into the vat index; body in
    /// `provision/peer_provision_hosting.zig`.
    pub fn registerProvisionForProvide(
        self: *Peer,
        idx: *ProvisionIndex,
        provide_question_id: u32,
        adopted: *?*ProvisionIndex.Provision,
    ) !void {
        return ProvisionHosting.registerProvisionForProvide(self, idx, provide_question_id, adopted);
    }

    /// Index-mode Accept path; body in `provision/peer_provision_hosting.zig`.
    pub fn handleAcceptWithProvisionIndex(self: *Peer, idx: *ProvisionIndex, accept: protocol.Accept) !void {
        return ProvisionHosting.handleAcceptWithProvisionIndex(self, idx, accept);
    }

    /// L9 parked-accept TTL sweep; body in
    /// `provision/peer_provision_hosting.zig`.
    pub fn sweepExpiredParkedAcceptsForProvisionIndex(idx: *ProvisionIndex) usize {
        return ProvisionHosting.sweepExpiredParkedAcceptsForProvisionIndex(idx);
    }

    /// PHASE B of adoption: transition parked accepts a Provide just adopted;
    /// body in `provision/peer_provision_hosting.zig`.
    pub fn drainAdoptedParkedAccepts(self: *Peer, idx: *ProvisionIndex, prov: *ProvisionIndex.Provision) !void {
        return ProvisionHosting.drainAdoptedParkedAccepts(self, idx, prov);
    }

    /// Release (or pre-mark) one embargo on a provision — the host arm of a
    /// spec-form accept-Disembargo; body in
    /// `provision/peer_provision_hosting.zig`.
    fn releaseProvisionEmbargo(self: *Peer, prov: *ProvisionIndex.Provision, embargo: []const u8) !void {
        return ProvisionHosting.releaseProvisionEmbargo(self, prov, embargo);
    }

    /// FinishOps `clear_pending_accept_question` replacement (same fn type);
    /// body in `provision/peer_provision_hosting.zig`.
    fn clearPendingAcceptQuestionRouted(peer: *Peer, question_id: u32) void {
        ProvisionHosting.clearPendingAcceptQuestionRouted(peer, question_id);
    }

    // -- The canonical drain/teardown procedure ------------------------------
    // (bodies in provision/peer_provision_drain.zig)

    /// Fallible Finish pre-step, called from handleFinish BEFORE the FinishOps
    /// chain; body in `provision/peer_provision_drain.zig`.
    fn detachProvisionForFinish(self: *Peer, question_id: u32) !void {
        return ProvisionDrain.detachProvisionForFinish(self, question_id);
    }

    /// Neutralize step of `deinit` for owned provisions (callback-free); body
    /// in `provision/peer_provision_drain.zig`.
    pub fn neutralizeProvisionsOnOwnerPeer(self: *Peer) ProvisionDrain.OwnerProvisionTeardown {
        return ProvisionDrain.neutralizeProvisionsOnOwnerPeer(self);
    }

    /// Send-bearing drain phase of `deinit`, after forceCancelAllQuestions;
    /// body in `provision/peer_provision_drain.zig`.
    pub fn drainClosedProvisionsOnOwnerPeer(self: *Peer, teardown: *ProvisionDrain.OwnerProvisionTeardown) void {
        ProvisionDrain.drainClosedProvisionsOnOwnerPeer(self, teardown);
    }

    /// Holder-peer neutralize: clear this peer's queued/parked cross-peer
    /// accepts; body in `provision/peer_provision_drain.zig`.
    pub fn detachCrossPeerAcceptsOnHolderPeer(self: *Peer) void {
        ProvisionDrain.detachCrossPeerAcceptsOnHolderPeer(self);
    }

    // ================= Cap refcount / release / frame send ==================
    //
    // Bodies live in peer_export_release.zig, generic over Peer (the
    // JoinCoordinator extraction contract). The thunks below keep every
    // caller-visible name — including the FROZEN `releaseImport` — with its
    // exact signature on Peer itself.

    const ExportReleaseImpl = peer_export_release.ExportRelease(Peer);

    /// Release references to an imported capability, sending a Release message
    /// to the remote peer when the reference count drops to zero.
    ///
    /// Error set is intentionally left open (`anyerror`): the Release send
    /// path funnels through `sendFrameControl` -> `sendFrame`, whose
    /// `SendFrameOverride` callback is arbitrary user code. Body in
    /// `peer_export_release.zig`.
    pub fn releaseImport(self: *Peer, import_id: u32, count: u32) anyerror!void {
        return ExportReleaseImpl.releaseImport(self, import_id, count);
    }

    /// Body in `peer_export_release.zig`.
    pub fn sendReleaseForHost(self: *Peer, import_id: u32, count: u32) !void {
        return ExportReleaseImpl.sendReleaseForHost(self, import_id, count);
    }

    /// Body in `peer_export_release.zig`.
    pub fn forgetImportRefsForHost(self: *Peer, import_id: u32, count: u32) !void {
        return ExportReleaseImpl.forgetImportRefsForHost(self, import_id, count);
    }

    /// Body in `peer_export_release.zig`.
    pub fn sendFinishForHost(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
        require_early_cancellation: bool,
    ) !void {
        return ExportReleaseImpl.sendFinishForHost(self, question_id, release_result_caps, require_early_cancellation);
    }

    /// Body in `peer_export_release.zig`.
    pub fn sendBuilder(self: *Peer, builder: *protocol.MessageBuilder) !void {
        return ExportReleaseImpl.sendBuilder(self, builder);
    }

    /// Body in `peer_export_release.zig`.
    fn sendBuilderControl(self: *Peer, builder: *protocol.MessageBuilder) !void {
        return ExportReleaseImpl.sendBuilderControl(self, builder);
    }

    /// Body in `peer_export_release.zig`.
    pub fn sendFrameControl(self: *Peer, frame: []const u8) !void {
        return ExportReleaseImpl.sendFrameControl(self, frame);
    }

    /// Body in `peer_export_release.zig`.
    pub fn sendFrame(self: *Peer, frame: []const u8) !void {
        return ExportReleaseImpl.sendFrame(self, frame);
    }

    /// Body in `peer_export_release.zig`.
    pub fn onOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) anyerror!void {
        return ExportReleaseImpl.onOutboundCap(ctx, tag, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn rollbackOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) void {
        return ExportReleaseImpl.rollbackOutboundCap(ctx, tag, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn noteExportRef(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.noteExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn rollbackExportRef(self: *Peer, id: u32) void {
        return ExportReleaseImpl.rollbackExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn noteAnswerExportRef(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.noteAnswerExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn rollbackAnswerExportRef(self: *Peer, id: u32) void {
        return ExportReleaseImpl.rollbackAnswerExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn notePromiseExportRef(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.notePromiseExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn noteHandoffExportRef(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.noteHandoffExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseHandoffHeldExport(self: *Peer, id: u32) void {
        return ExportReleaseImpl.releaseHandoffHeldExport(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn noteHandoffImportPin(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.noteHandoffImportPin(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn rollbackHandoffImportPin(self: *Peer, id: u32) void {
        return ExportReleaseImpl.rollbackHandoffImportPin(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseHandoffImportPin(self: *Peer, id: u32) anyerror!void {
        return ExportReleaseImpl.releaseHandoffImportPin(self, id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn rollbackPromiseExportRef(self: *Peer, id: u32) void {
        return ExportReleaseImpl.rollbackPromiseExportRef(self, id);
    }

    /// Body in `peer_export_release.zig`.
    fn finalizeExportRelease(self: *Peer, id: u32, promise_target: ?u32, import_target: ?u32) void {
        return ExportReleaseImpl.finalizeExportRelease(self, id, promise_target, import_target);
    }

    /// Body in `peer_export_release.zig`.
    fn releasePromiseHeldCap(self: *Peer, id: u32) !void {
        return ExportReleaseImpl.releasePromiseHeldCap(self, id);
    }

    /// Body in `peer_export_release.zig`.
    fn releaseExport(self: *Peer, id: u32, count: u32) !void {
        return ExportReleaseImpl.releaseExport(self, id, count);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseInboundCaps(self: *Peer, inbound: *cap_table.InboundCapTable) !void {
        return ExportReleaseImpl.releaseInboundCaps(self, inbound);
    }

    /// Body in `peer_export_release.zig`.
    pub fn storeResolvedImport(
        self: *Peer,
        promise_id: u32,
        cap: ?cap_table.ResolvedCap,
        embargo_id: ?u32,
        embargoed: bool,
    ) !void {
        return ExportReleaseImpl.storeResolvedImport(self, promise_id, cap, embargo_id, embargoed);
    }

    /// Body in `peer_export_release.zig`.
    pub fn rememberPendingEmbargo(self: *Peer, embargo_id: u32, promise_id: u32) !void {
        return ExportReleaseImpl.rememberPendingEmbargo(self, embargo_id, promise_id);
    }

    /// Body in `peer_export_release.zig`.
    fn releaseResolvedImport(self: *Peer, promise_id: u32) anyerror!void {
        return ExportReleaseImpl.releaseResolvedImport(self, promise_id);
    }

    /// Body in `peer_export_release.zig`.
    fn bufferPendingThirdPartyReturn(self: *Peer, answer_id: u32, frame: []const u8) !void {
        return ExportReleaseImpl.bufferPendingThirdPartyReturn(self, answer_id, frame);
    }

    /// Body in `peer_export_release.zig`.
    fn handleMissingReturnQuestion(self: *Peer, frame: []const u8, answer_id: u32) !void {
        return ExportReleaseImpl.handleMissingReturnQuestion(self, frame, answer_id);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseResolvedCap(self: *Peer, resolved: cap_table.ResolvedCap) anyerror!void {
        return ExportReleaseImpl.releaseResolvedCap(self, resolved);
    }

    /// Body in `peer_export_release.zig`.
    pub fn deliverLoopbackReturn(self: *Peer, frame: []const u8) !void {
        return ExportReleaseImpl.deliverLoopbackReturn(self, frame);
    }

    /// Body in `peer_export_release.zig`.
    pub fn resolveProvidePromisedOps(self: *Peer, question_id: u32, ops: []const protocol.PromisedAnswerOp) !cap_table.ResolvedCap {
        return ExportReleaseImpl.resolveProvidePromisedOps(self, question_id, ops);
    }

    /// Body in `peer_export_release.zig`.
    pub fn resolvePromisedAnswer(self: *Peer, promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
        return ExportReleaseImpl.resolvePromisedAnswer(self, promised);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseResultCaps(self: *Peer, frame: []const u8) !void {
        return ExportReleaseImpl.releaseResultCaps(self, frame);
    }

    /// Body in `peer_export_release.zig`.
    pub fn releaseAnswerHeldResultCaps(self: *Peer, frame: []const u8) !void {
        return ExportReleaseImpl.releaseAnswerHeldResultCaps(self, frame);
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

    /// The `releaseParamCaps` flag for the Return that answers `answer_id`.
    ///
    /// rpc.capnp: with `releaseParamCaps = true` the caller drops the wire refs
    /// its param capabilities took, and "the sender must not send separate
    /// `Release` messages for them". This vat DOES send them — the
    /// post-dispatch auto-release for params the handler ignored, and the
    /// application's own `releaseImport` for ones it kept — so any answer that
    /// took param import refs must say `false`, exactly as the C++ reference
    /// does on every Return it sends. Claiming `true` alongside those frames
    /// releases the caller's export twice; a compliant peer answers that with
    /// "Tried to release invalid export ID" and drops the connection.
    ///
    /// Returns the schema default `true` for every answer with no
    /// `active_inbound_questions` record: Bootstrap/Provide/Accept/Join Returns
    /// (whose messages have no params cap table at all), the `sendResultsTo =
    /// thirdParty` refusal issued before the record exists, and any answer whose
    /// Call params carried no ref-granting descriptor. In all of those the
    /// caller recorded no param exports, so the flag is a no-op either way.
    pub fn returnReleasesParamCaps(self: *Peer, answer_id: u32) bool {
        return !(self.active_inbound_questions.get(answer_id) orelse false);
    }

    fn onConnectionError(self: *Peer, err: anyerror) void {
        log.debug("connection error: {}", .{err});
        if (self.on_error) |cb| cb(self.callback_ctx, self, err);
    }

    /// Deliver terminal transport-close lifecycle exactly once.
    ///
    /// Bound transports reach this path through `onConnectionClose`; manual
    /// integrations such as `HostPeer` call it when their socket reaches EOF
    /// or reset. Holder-side parked and embargo-queued Accept reservations are
    /// detached before the user callback. Provider-owned active provisions
    /// deliberately remain live until Finish or peer deinit, preserving the
    /// disconnect-after-Provide direct-pickup contract.
    pub fn notifyTransportClosed(self: *Peer) void {
        self.assertThreadAffinity();
        if (self.transport_close_notified) return;
        self.transport_close_notified = true;
        if (self.automatic_third_party_operation_depth != 0 or
            self.automatic_third_party_dispatch_depth != 0 or
            self.join_operation_depth != 0)
        {
            self.automatic_third_party_close_deferred = true;
            return;
        }
        self.finishTransportClosedNotification();
    }

    pub fn finishTransportClosedNotification(self: *Peer) void {
        const guards_automatic_routes = self.automatic_third_party_routes.count() != 0 or
            self.incoming_automatic_third_party_routes.count() != 0;
        if (guards_automatic_routes) self.enterAutomaticThirdPartyOperation();
        defer if (guards_automatic_routes) self.leaveAutomaticThirdPartyOperation();

        log.debug("connection closed", .{});
        self.neutralizeAutomaticThirdPartyRoutesOnTargetPeer();
        self.neutralizeAutomaticThirdPartyRoutesOnSourcePeer();
        self.detachCrossPeerAcceptsOnHolderPeer();
        self.drainOutboundProvidesOnRecipientPeer();
        self.neutralizeForwardVineRelaysOnRecipientPeer();
        self.neutralizeHandoffPickupsOnPromisePeer();
        self.drainIncompleteJoinsOnTransportClose();
        self.detachJoinResultAnswersOnTransportClose();
        // This peer's role as a distinct direct-Accept host is terminal on
        // transport close. Canonical JoinResults owned by this result-path
        // peer are deliberately untouched, preserving direct pickup through a
        // still-live sibling Accept host.
        self.cancelJoinAcceptHostLinks();
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
        // Completed/transferred retained answers are no longer reachable once
        // the transport is gone. Drop their local ownership records before the
        // user close callback; repeated close and later deinit remain no-ops.
        self.retained_questions.clearRetainingCapacity();
        if (self.on_close) |cb| cb(self.callback_ctx, self);
    }

    fn onConnectionClose(self: *Peer) void {
        self.notifyTransportClosed();
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
        if (self.transport_close_notified) return error.TransportClosed;

        return self.handleFrameImpl(frame);
    }

    /// Dispatch an in-process call to one of this peer's local exports. Local
    /// capabilities deliberately remain callable after transport close (the
    /// L3 disconnect-after-Provide contract), while public/transport ingress
    /// is rejected by `handleFrame` once close has been notified.
    pub fn handleLoopbackFrame(self: *Peer, frame: []const u8) !void {
        self.assertThreadAffinity();
        return self.handleFrameImpl(frame);
    }

    fn handleFrameImpl(self: *Peer, frame: []const u8) !void {
        self.enterJoinOperation();
        defer self.leaveJoinOperation();
        // Drive vat-wide expiry before decoding or validation. A connection
        // that stays busy — including one sending malformed traffic — must not
        // suppress reclamation merely by avoiding Accept frames or idle ticks.
        if (self.provision_index) |idx| _ = idx.sweepExpiredParkedAccepts();
        _ = self.sweepExpiredJoins();

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

        const automatic_third_party_dispatch = if (decoded.tag == .call and
            self.third_party_result_policy == .vat_network)
        blk: {
            const call = decoded.asCall() catch break :blk false;
            break :blk call.send_results_to.tag == .thirdParty;
        } else false;
        if (automatic_third_party_dispatch) self.automatic_third_party_dispatch_depth += 1;
        defer if (automatic_third_party_dispatch) {
            std.debug.assert(self.automatic_third_party_dispatch_depth > 0);
            self.automatic_third_party_dispatch_depth -= 1;
            self.completeDeferredAutomaticThirdPartyLifecycle();
        };

        dispatch.dispatchDecodedForPeer(
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
        try bootstrap.handleUnimplemented(
            Peer,
            self,
            unimplemented,
            bootstrap.handleUnimplementedQuestionForPeerFn(
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
        try bootstrap.handleAbort(self.allocator, &self.last_remote_abort_reason, capped_abort);
    }

    fn handleBootstrap(self: *Peer, bootstrap_msg: protocol.Bootstrap) !void {
        try bootstrap.handleBootstrap(
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
        // Automatic route retirement can synchronously settle the opposite
        // peer and invoke arbitrary callbacks. Snapshot every borrowed peer
        // before either map is mutated, deduplicating same-peer and answer-id
        // collision cases, and guard only this automatic-route Finish path.
        const owned_automatic_route = self.automatic_third_party_routes.get(qid);
        const incoming_automatic_route = self.incoming_automatic_third_party_routes.get(qid);
        var automatic_guards = AutomaticThirdPartyFinishGuards{};
        if (owned_automatic_route != null or incoming_automatic_route != null) {
            automatic_guards.add(self);
            if (owned_automatic_route) |route| {
                if (route.target_peer) |target| automatic_guards.add(target);
            }
            if (incoming_automatic_route) |route| {
                if (route.source_peer) |source| automatic_guards.add(source);
            }
            automatic_guards.enter();
        }
        defer automatic_guards.leave();
        const canceled_automatic_target = self.noteAutomaticThirdPartyTargetFinish(qid);
        // Vat-wide provision close (Finish of a Provide question) — UNGATED:
        // the map lookup inside is the gate. Runs in this fallible context so
        // an OOM in the fan-out propagates out of dispatch instead of being
        // force-swallowed by the void FinishOps hook below.
        try self.detachProvisionForFinish(qid);
        const was_active = self.active_inbound_questions.remove(qid);
        const was_resolving = self.resolving_answers.contains(qid);
        // The failed-answer record lives exactly as long as resolved_answers
        // entries do: until the remote finishes the question.
        if (self.failed_answers.fetchRemove(qid)) |failed| {
            self.allocator.free(failed.value.reason);
        }
        const finished_completing_join = self.finishCompletingJoinAnswer(qid, finish_msg.release_result_caps);
        if (!finished_completing_join) self.clearPendingJoinResultAnswer(qid);
        try self.clearPendingJoinRelay(qid, true, finish_msg.release_result_caps);
        // Cancellation race: a Finish for an in-flight inbound call (still
        // active, not yet resolved), or for a synchronous Return whose frame is
        // on the wire but whose resolved answer is not committed yet, means the
        // Return sender must not later leave a resolved_answers entry that no
        // further Finish will clear. Preserve releaseResultCaps for the sender
        // so it can either roll back a late async Return or commit+cleanup a
        // synchronous Return after replaying queued promised calls.
        if ((was_active or was_resolving or finished_completing_join) and
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
        const ops = finish.FinishOps(Peer){
            .remove_send_results_to_yourself = peer_forward_orchestration.removeSendResultsToYourselfForPeerFn(Peer),
            .clear_send_results_to_third_party = clearSendResultsToThirdParty,
            .clear_provide = provide_accept_join.provides_state.clearProvideForPeerFn(
                Peer,
                ProvideEntry,
                ProvideTarget,
                ProvideTarget.deinit,
            ),
            .clear_pending_join_question = provide_accept_join.join_state.clearPendingJoinQuestionForPeerFn(
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
            .take_resolved_answer_frame = finish.takeResolvedAnswerFrameForPeerFn(Peer),
            .release_answer_caps_for_frame = releaseAnswerHeldResultCaps,
            .release_caps_for_frame = releaseResultCaps,
            .free_frame = finish.freeOwnedFrameForPeerFn(Peer),
        };
        const cleared_partial_join = self.pending_join_questions.contains(qid);
        try finish.handleFinishWithOps(
            Peer,
            self,
            finish_msg.question_id,
            finish_msg.release_result_caps,
            ops,
        );
        if (cleared_partial_join) self.refreshNextJoinDeadline();
        // No Return will follow for a synthetic answer canceled before result
        // delivery, so its ordinary finished-early tombstone must not reserve
        // the high-range id forever.
        if (canceled_automatic_target) {
            _ = self.finished_early_answers.remove(qid);
        }
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
                if (self.outbound_provides.fetchRemove(release.id)) |removed| {
                    var op = removed.value;
                    op.deinit(self.allocator);
                }
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
                    self.finishOriginatedProvide(
                        provide_peer,
                        entry.provide_question_id,
                        entry.retained_source_question_id,
                    );
                }
            }
        }
    }

    /// Finish a Provide question this peer originated, on the peer that owns it
    /// (the host-of-provided-cap connection). Removes the held-open question and
    /// sends the wire `Finish` so the host unregisters the provision. Both the
    /// Provide and a transferred retained source answer publish durable finish
    /// requests before either frame is sent; transport failures stay queued for
    /// `retryDeferredFinishes` rather than losing the lifetime owner when the
    /// vine coupling disappears.
    fn requestOriginatedProvideFinish(
        provide_peer: *Peer,
        provide_question_id: u32,
        retained_source_question_id: ?u32,
    ) void {
        if (provide_peer.questions.getPtr(provide_question_id)) |question| {
            question.finish_on_maintenance = true;
            question.finish_release_result_caps = false;
        }
        if (retained_source_question_id) |source_question_id| {
            provide_peer.retained_questions.requestTransferredFinish(source_question_id) catch |err| {
                log.debug("retained source cleanup request failed for question {}: {}", .{ source_question_id, err });
            };
        }
    }

    pub fn finishOriginatedProvide(
        _: *Peer,
        provide_peer: *Peer,
        provide_question_id: u32,
        retained_source_question_id: ?u32,
    ) void {
        requestOriginatedProvideFinish(
            provide_peer,
            provide_question_id,
            retained_source_question_id,
        );
        provide_peer.retryDeferredFinishes();
    }

    /// Recipient-side terminal cleanup for vine couplings. Move the entire map
    /// out first, publish every Provide/source-answer Finish request without
    /// callbacks, then retry each distinct provider only after this peer owns no
    /// borrowed coupling state. Thus a synchronous send callback can close or
    /// deinit the recipient without stranding a later entry.
    pub fn drainOutboundProvidesOnRecipientPeer(self: *Peer) void {
        if (self.outbound_provides.count() == 0) return;

        const allocator = self.allocator;
        var moved = self.outbound_provides;
        self.outbound_provides = std.AutoHashMap(u32, OutboundProvide).init(allocator);
        defer moved.deinit();

        var retry_peers: std.ArrayList(*Peer) = .empty;
        defer retry_peers.deinit(allocator);

        var it = moved.iterator();
        while (it.next()) |entry| {
            const vine_id = entry.key_ptr.*;
            const op = entry.value_ptr;
            if (op.provide_peer) |provide_peer| {
                provide_peer.deregisterCoupledVine(self, vine_id);
                requestOriginatedProvideFinish(
                    provide_peer,
                    op.provide_question_id,
                    op.retained_source_question_id,
                );
                if (provide_peer != self and
                    std.mem.indexOfScalar(*Peer, retry_peers.items, provide_peer) == null)
                {
                    retry_peers.append(allocator, provide_peer) catch {};
                }
            }
            if (op.resolved_promise_export_id) |promise_id| {
                if (self.exports.getEntry(promise_id)) |promise_entry| {
                    if (promise_entry.value_ptr.resolved) |resolved| {
                        if (resolved == .exported and resolved.exported.id == vine_id) {
                            promise_entry.value_ptr.resolved = null;
                        }
                    }
                }
            }
            self.caps.clearThirdPartyHosted(vine_id);
            self.releaseVineExport(vine_id);
            op.deinit(allocator);
        }

        for (retry_peers.items) |provide_peer| {
            provide_peer.retryDeferredFinishes();
        }
    }

    /// Retry protocol lifetimes which have ended locally but whose Finish
    /// frame was rejected by the transport. State is published before any
    /// send, and the guard makes synchronous nested delivery observe a single
    /// owner rather than recursively finishing the same question.
    pub fn retryDeferredFinishes(self: *Peer) void {
        if (self.finish_maintenance_in_progress) return;
        self.finish_maintenance_in_progress = true;
        defer self.finish_maintenance_in_progress = false;

        var provide_ids: std.ArrayList(u32) = .empty;
        defer provide_ids.deinit(self.allocator);
        var question_it = self.questions.iterator();
        while (question_it.next()) |entry| {
            if (!entry.value_ptr.finish_on_maintenance) continue;
            provide_ids.append(self.allocator, entry.key_ptr.*) catch break;
        }
        for (provide_ids.items) |question_id| {
            const question = self.questions.get(question_id) orelse continue;
            if (!question.finish_on_maintenance) continue;
            peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                Peer,
                self,
                question_id,
                question.finish_release_result_caps,
                false,
                Peer.sendFrameControl,
            ) catch |err| {
                log.debug("deferred provide finish failed for question {}: {}", .{ question_id, err });
                continue;
            };
            self.removeQuestion(question_id);
        }

        var retained_ids: std.ArrayList(u32) = .empty;
        defer retained_ids.deinit(self.allocator);
        var retained_it = self.retained_questions.entries.iterator();
        while (retained_it.next()) |entry| {
            if (!entry.value_ptr.finish_requested or entry.value_ptr.state != .transferred) continue;
            retained_ids.append(self.allocator, entry.key_ptr.*) catch break;
        }
        for (retained_ids.items) |question_id| {
            self.finishTransferredRetainedQuestion(question_id, false) catch |err| {
                log.debug("deferred retained source finish failed for question {}: {}", .{ question_id, err });
            };
        }
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
    pub fn registerCoupledVine(self: *Peer, recipient_peer: *Peer, vine_id: u32) !void {
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
    pub fn deregisterCoupledVine(self: *Peer, recipient_peer: *Peer, vine_id: u32) void {
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
    pub fn neutralizeCoupledVinesOnProvidePeer(self: *Peer) void {
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

    fn registerForwardVineRelay(self: *Peer, relay: *ForwardVineCallContext) !void {
        try self.forward_vine_relay_links.append(self.allocator, relay);
        relay.recipient_link_registered = true;
    }

    pub fn deregisterForwardVineRelay(self: *Peer, relay: *ForwardVineCallContext) void {
        if (!relay.recipient_link_registered) return;
        var i: usize = 0;
        while (i < self.forward_vine_relay_links.items.len) : (i += 1) {
            if (self.forward_vine_relay_links.items[i] == relay) {
                _ = self.forward_vine_relay_links.swapRemove(i);
                relay.recipient_link_registered = false;
                return;
            }
        }
        relay.recipient_link_registered = false;
    }

    /// Null every async forwarded-vine context that borrows this recipient
    /// before transport-close callbacks or peer teardown can free it. The
    /// forwarding question still owns the context and will free it later, but
    /// its Return/deinit path can no longer dereference this peer.
    pub fn neutralizeForwardVineRelaysOnRecipientPeer(self: *Peer) void {
        for (self.forward_vine_relay_links.items) |relay| {
            if (relay.recipient_peer == self) {
                relay.recipient_peer = null;
                relay.recipient_link_registered = false;
                relay.recipient_answer_pending = false;
            }
        }
        self.forward_vine_relay_links.clearRetainingCapacity();
    }

    pub fn registerHandoffPickup(self: *Peer, ctx: *HandoffPickupContext) !void {
        try self.handoff_pickup_links.append(self.allocator, ctx);
        ctx.promise_link_registered = true;
    }

    pub fn deregisterHandoffPickup(self: *Peer, ctx: *HandoffPickupContext) void {
        if (!ctx.promise_link_registered) return;
        var i: usize = 0;
        while (i < self.handoff_pickup_links.items.len) : (i += 1) {
            if (self.handoff_pickup_links.items[i] == ctx) {
                _ = self.handoff_pickup_links.swapRemove(i);
                ctx.promise_link_registered = false;
                return;
            }
        }
        ctx.promise_link_registered = false;
    }

    /// The Accept question on the third-vat peer owns each context. Promise
    /// peer teardown abandons its local vine ref (the cap table is dying) and
    /// nulls the callback borrow; the eventual Accept Return/teardown then only
    /// finishes and frees its own side.
    pub fn neutralizeHandoffPickupsOnPromisePeer(self: *Peer) void {
        for (self.handoff_pickup_links.items) |ctx| {
            if (ctx.promise_peer == self) {
                ctx.promise_peer = null;
                ctx.promise_link_registered = false;
                ctx.vine_owned = false;
            }
        }
        self.handoff_pickup_links.clearRetainingCapacity();
    }

    pub fn registerCrossPeerProxy(self: *Peer, owner_peer: *Peer, proxy_export_id: u32) !void {
        try self.cross_peer_proxy_links.append(self.allocator, .{
            .owner_peer = owner_peer,
            .proxy_export_id = proxy_export_id,
        });
    }

    pub fn deregisterCrossPeerProxy(self: *Peer, owner_peer: *Peer, proxy_export_id: u32) void {
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

    pub fn neutralizeCrossPeerProxiesOnSourcePeer(self: *Peer) void {
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
                        // Same rule for the import pin: the source peer's
                        // import table is going away with it.
                        proxy_ctx.release_source_import_pin_id = null;
                    }
                }
            }
        }
        self.cross_peer_proxy_links.clearRetainingCapacity();
    }

    pub fn registerCrossPeerJoinRelay(self: *Peer, owner_peer: *Peer, owner_answer_id: u32) !void {
        try self.cross_peer_join_relay_links.append(self.allocator, .{
            .owner_peer = owner_peer,
            .owner_answer_id = owner_answer_id,
        });
    }

    pub fn deregisterCrossPeerJoinRelay(self: *Peer, owner_peer: *Peer, owner_answer_id: u32) void {
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

    pub fn neutralizeCrossPeerJoinRelaysOnSourcePeer(self: *Peer) void {
        for (self.cross_peer_join_relay_links.items) |link| {
            if (link.owner_peer.pending_join_relays.getPtr(link.owner_answer_id)) |relay| {
                if (relay.source_peer == self) relay.source_peer = null;
            }
        }
        self.cross_peer_join_relay_links.clearRetainingCapacity();
    }

    pub fn registerJoinAcceptHost(self: *Peer, hosted: *HostedJoin) !void {
        for (self.join_accept_host_links.items) |link| {
            if (link.hosted == hosted) return;
        }
        try self.join_accept_host_links.append(self.allocator, .{ .hosted = hosted });
    }

    pub fn deregisterJoinAcceptHost(self: *Peer, hosted: *HostedJoin) void {
        var i: usize = 0;
        while (i < self.join_accept_host_links.items.len) {
            const link = self.join_accept_host_links.items[i];
            if (link.hosted == hosted) {
                _ = self.join_accept_host_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    pub fn cancelJoinAcceptHostLinks(self: *Peer) void {
        while (self.join_accept_host_links.items.len != 0) {
            const hosted = self.join_accept_host_links.items[0].hosted;
            hosted.owner_peer.cancelHostedJoin(hosted);
        }
    }

    fn drainIncompleteJoinsOnTransportClose(self: *Peer) void {
        while (self.pending_joins.count() != 0) {
            var it = self.pending_joins.keyIterator();
            const join_id = (it.next() orelse break).*;
            const removed = self.pending_joins.fetchRemove(join_id) orelse continue;
            var join_state = removed.value;
            var part_it = join_state.parts.valueIterator();
            while (part_it.next()) |part| _ = self.pending_join_questions.remove(part.question_id);
            JoinState.deinit(&join_state, self.allocator);
        }
        while (self.pending_join_relays.count() != 0) {
            var it = self.pending_join_relays.keyIterator();
            const answer_id = (it.next() orelse break).*;
            _ = self.retirePendingJoinRelayTerminal(answer_id, false, false);
        }
        self.refreshNextJoinDeadline();
    }

    /// Transport close retires the result-path answer bookkeeping without
    /// interpreting it as explicit Finish. A committed lease hosted by a
    /// distinct live Accept peer therefore remains pickable, while the closed
    /// result peer exposes no stale answer records to its user close callback.
    fn detachJoinResultAnswersOnTransportClose(self: *Peer) void {
        while (self.pending_join_result_answers.count() != 0) {
            var it = self.pending_join_result_answers.iterator();
            const entry = it.next() orelse break;
            const answer_id = entry.key_ptr.*;
            const hosted = entry.value_ptr.hosted;
            _ = self.pending_join_result_answers.remove(answer_id);
            std.debug.assert(hosted.owner_peer == self);
            std.debug.assert(hosted.result_refs > 0);
            hosted.result_refs -= 1;
            // `accept_live` retains a committed distinct-host lease. If the
            // Accept was already consumed/cancelled, dropping the final result
            // reference can retire the canonical object immediately.
            self.maybeDestroyHostedJoin(hosted);
        }
    }

    pub fn registerJoinCoordinatorAccept(self: *Peer, coordinator: *JoinCoordinator) !void {
        for (self.join_coordinator_accept_links.items) |link| {
            if (link.coordinator == coordinator) return;
        }
        try self.join_coordinator_accept_links.append(self.allocator, .{
            .coordinator = coordinator,
        });
    }

    pub fn deregisterJoinCoordinatorAccept(self: *Peer, coordinator: *JoinCoordinator) void {
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

    fn registerJoinCoordinatorResult(self: *Peer, coordinator: *JoinCoordinator, question_id: u32) !void {
        for (self.join_coordinator_result_links.items) |link| {
            if (link.coordinator == coordinator and link.question_id == question_id) return;
        }
        try self.join_coordinator_result_links.append(self.allocator, .{
            .coordinator = coordinator,
            .question_id = question_id,
        });
    }

    pub fn deregisterJoinCoordinatorResult(self: *Peer, coordinator: *JoinCoordinator, question_id: u32) void {
        var i: usize = 0;
        while (i < self.join_coordinator_result_links.items.len) {
            const link = self.join_coordinator_result_links.items[i];
            if (link.coordinator == coordinator and link.question_id == question_id) {
                _ = self.join_coordinator_result_links.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    pub fn neutralizeJoinCoordinatorResultLinks(self: *Peer) void {
        while (self.join_coordinator_result_links.items.len != 0) {
            const link = self.join_coordinator_result_links.pop() orelse break;
            link.coordinator.neutralizeResultPeer(self, link.question_id);
        }
    }

    pub fn neutralizeJoinCoordinatorAcceptLinks(self: *Peer) void {
        for (self.join_coordinator_accept_links.items) |link| {
            link.coordinator.neutralizeAcceptedPeer(self);
        }
        self.join_coordinator_accept_links.clearRetainingCapacity();
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
        const ops = disembargo.DisembargoOps(Peer){
            .has_known_disembargo_target = Peer.hasKnownDisembargoTarget,
            .send_disembargo_receiver_loopback = peer_outbound_control.sendDisembargoReceiverLoopbackViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            .take_pending_embargo_promise = disembargo.takePendingEmbargoPromiseForPeerFn(Peer),
            .clear_resolved_import_embargo = disembargo.clearResolvedImportEmbargoForPeerFn(Peer),
            .release_embargoed_accepts = Peer.handleAcceptDisembargo,
        };
        try disembargo.handleDisembargoWithOps(Peer, self, disembargo_msg, ops);
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
            try provide_accept_join.embargo_accepts.releaseEmbargoedAcceptsForPeer(
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
    pub fn flushStashedAcceptDisembargo(self: *Peer, vine_id: u32) void {
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

    pub fn makeProvideTarget(self: *Peer, resolved: cap_table.ResolvedCap) !ProvideTarget {
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

    pub fn cloneProvideTarget(self: *Peer, target: *const ProvideTarget) !ProvideTarget {
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

    // ================= L4 hosted-Join accept/completion + cross-peer relay ==
    //
    // Bodies live in join/peer_join_accept.zig and join/peer_join_relay.zig,
    // generic over Peer (the JoinCoordinator extraction contract).

    const JoinAcceptImpl = peer_join_accept.JoinAccept(Peer);
    const JoinRelayImpl = peer_join_relay.JoinRelay(Peer);

    /// Type aliases consumed by the extracted join/relay (and later) sibling
    /// namespaces: the underlying structs are file-level and
    /// Peer-parameterized, so siblings reach them through Peer itself.
    pub const HostedJoinRecord = HostedJoin;
    pub const ProvideHandleRecord = ProvideHandle;
    /// Third-party answer-id space bounds, re-exported for the extracted
    /// origination namespace (single source of truth stays file-level).
    pub const third_party_answer_id_base_value = third_party_answer_id_base;
    pub const third_party_answer_id_limit_value = third_party_answer_id_limit;
    pub const ProvideOriginationTargetRecord = ProvideOriginationTarget;
    pub const CompletingJoinAnswerRecord = CompletingJoinAnswer;
    pub const CrossPeerJoinRelayRecord = CrossPeerJoinRelay;
    pub const CrossPeerProxyCtx = CrossPeerProxyContext;
    pub const CrossPeerJoinRelayCtx = CrossPeerJoinRelayContext;

    /// Body in `join/peer_join_accept.zig`.
    pub fn putPendingJoinAcceptOwned(self: *Peer, hosted: *HostedJoin, target: ProvideTarget) !void {
        return JoinAcceptImpl.putPendingJoinAcceptOwned(self, hosted, target);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn takePendingJoinAccept(self: *Peer, provision: []const u8) ?ProvideTarget {
        return JoinAcceptImpl.takePendingJoinAccept(self, provision);
    }

    /// Body in `join/peer_join_accept.zig`.
    fn clearPendingJoinResultAnswer(self: *Peer, answer_id: u32) void {
        return JoinAcceptImpl.clearPendingJoinResultAnswer(self, answer_id);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn dropCompletingJoinResultRef(self: *Peer, completing: *CompletingJoinAnswer) void {
        return JoinAcceptImpl.dropCompletingJoinResultRef(self, completing);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn putCompletingJoinAnswerAssumeCapacity(
        self: *Peer,
        answer_id: u32,
        counts_as_join_record: bool,
    ) void {
        return JoinAcceptImpl.putCompletingJoinAnswerAssumeCapacity(self, answer_id, counts_as_join_record);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn retireCompletingJoinAnswerAccounting(self: *Peer, completing: *CompletingJoinAnswer) void {
        return JoinAcceptImpl.retireCompletingJoinAnswerAccounting(self, completing);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn removeCompletingJoinAnswer(self: *Peer, answer_id: u32) bool {
        return JoinAcceptImpl.removeCompletingJoinAnswer(self, answer_id);
    }

    /// Body in `join/peer_join_accept.zig`.
    fn finishCompletingJoinAnswer(self: *Peer, answer_id: u32, release_result_caps: bool) bool {
        return JoinAcceptImpl.finishCompletingJoinAnswer(self, answer_id, release_result_caps);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn retireCompletingJoinTransitionAccounting(
        self: *Peer,
        join_state: *JoinState,
        transition_live: *bool,
    ) void {
        return JoinAcceptImpl.retireCompletingJoinTransitionAccounting(self, join_state, transition_live);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn completeJoinLegacy(self: *Peer, join_id: u32) !void {
        return JoinAcceptImpl.completeJoinLegacy(self, join_id);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn ownHostedJoinFromCompletion(self: *Peer, hosted: *HostedJoin) !void {
        return JoinAcceptImpl.ownHostedJoinFromCompletion(self, hosted);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn maybeDestroyHostedJoin(self: *Peer, hosted: *HostedJoin) void {
        return JoinAcceptImpl.maybeDestroyHostedJoin(self, hosted);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn cancelHostedJoin(self: *Peer, hosted: *HostedJoin) void {
        return JoinAcceptImpl.cancelHostedJoin(self, hosted);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn cancelAllHostedJoins(self: *Peer) void {
        return JoinAcceptImpl.cancelAllHostedJoins(self);
    }

    /// Body in `join/peer_join_accept.zig`.
    pub fn sendReturnJoinResultPayload(self: *Peer, answer_id: u32, result_payload: []const u8) !void {
        return JoinAcceptImpl.sendReturnJoinResultPayload(self, answer_id, result_payload);
    }

    /// Body in `join/peer_join_relay.zig`.
    fn clearPendingJoinRelay(
        self: *Peer,
        owner_answer_id: u32,
        send_downstream_finish: bool,
        release_result_caps: bool,
    ) !void {
        return JoinRelayImpl.clearPendingJoinRelay(self, owner_answer_id, send_downstream_finish, release_result_caps);
    }

    /// Body in `join/peer_join_relay.zig`.
    fn retirePendingJoinRelayTerminal(
        self: *Peer,
        owner_answer_id: u32,
        emit_timeout: bool,
        send_upstream_exception: bool,
    ) bool {
        return JoinRelayImpl.retirePendingJoinRelayTerminal(self, owner_answer_id, emit_timeout, send_upstream_exception);
    }

    /// Body in `join/peer_join_relay.zig`.
    pub fn tryHandleCrossPeerProxyJoin(self: *Peer, join: protocol.Join) !bool {
        return JoinRelayImpl.tryHandleCrossPeerProxyJoin(self, join);
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
    /// Bootstrap answer (active, resolved, failed, resolving, finished-early,
    /// forwarded, or queued for a promised target). This is the shared answer
    /// namespace;
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
    pub fn inboundAnswerQuestionIdInUse(self: *Peer, question_id: u32) !bool {
        return self.resolved_answers.contains(question_id) or
            self.failed_answers.contains(question_id) or
            self.active_inbound_questions.contains(question_id) or
            self.resolving_answers.contains(question_id) or
            self.finished_early_answers.contains(question_id) or
            self.send_results_to_yourself.contains(question_id) or
            self.send_results_to_third_party.contains(question_id) or
            self.forwarded_questions.contains(question_id) or
            self.forwarded_tail_questions.contains(question_id) or
            self.completing_join_answers.contains(question_id) or
            self.pending_join_result_answers.contains(question_id) or
            self.pending_join_relays.contains(question_id) or
            self.pending_accept_embargo_by_question.contains(question_id) or
            self.cross_peer_pending_accepts.contains(question_id) or
            self.hasQueuedPendingQuestionId(question_id);
    }

    /// True when `question_id` is already consumed by any inbound question:
    /// the shared Call/Bootstrap answer namespace plus in-flight Provide and
    /// Join questions. Used by handlers that own no question table of their own
    /// (Call, Bootstrap) so a spec-violating reuse across message types is
    /// rejected before a second Return can be emitted on the id.
    pub fn inboundQuestionIdInUse(self: *Peer, question_id: u32) !bool {
        return (try self.inboundAnswerQuestionIdInUse(question_id)) or
            self.provides_by_question.contains(question_id) or
            self.pending_join_questions.contains(question_id) or
            self.pending_join_relays.contains(question_id);
    }

    /// Pre-reserved resources for recording a resolved answer, obtained from
    /// `reserveResolvedAnswer` before the Return frame is sent so that
    /// `commitReservedResolvedAnswer` (run after the frame is on the wire)
    /// cannot fail. See `reserveResolvedAnswer`.
    pub const ResolvedAnswerReservation = struct {
        frame_copy: []u8,
        /// Exports on which the reservation took the answer-held reference
        /// (one entry per results cap descriptor). deinit rolls the
        /// references back; commit transfers them to the recorded answer,
        /// which releases them at Finish via the stored frame's cap table.
        held_export_ids: []u32,

        pub fn deinit(self: ResolvedAnswerReservation, peer: *Peer) void {
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

    // ================= Inbound Call path / resolved answers =================
    //
    // Bodies live in call/peer_call_inbound.zig, generic over Peer.

    const CallInboundImpl = peer_call_inbound.CallInbound(Peer);

    /// Body in `call/peer_call_inbound.zig`.
    fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
        return CallInboundImpl.handleCall(self, frame, call);
    }

    /// Body in `call/peer_call_inbound.zig`.
    fn handleResolvedCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        resolved: cap_table.ResolvedCap,
    ) !void {
        return CallInboundImpl.handleResolvedCall(self, call, inbound_caps, resolved);
    }

    /// Body in `call/peer_call_inbound.zig`.
    pub fn reserveResolvedAnswer(self: *Peer, question_id: u32, frame: []const u8) !ResolvedAnswerReservation {
        return CallInboundImpl.reserveResolvedAnswer(self, question_id, frame);
    }

    /// Body in `call/peer_call_inbound.zig`.
    pub fn commitReservedResolvedAnswer(
        self: *Peer,
        question_id: u32,
        reservation: ResolvedAnswerReservation,
    ) void {
        return CallInboundImpl.commitReservedResolvedAnswer(self, question_id, reservation);
    }

    /// Body in `call/peer_call_inbound.zig`.
    fn queuePromisedCall(self: *Peer, question_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
        return CallInboundImpl.queuePromisedCall(self, question_id, frame, inbound_caps);
    }

    /// Body in `call/peer_call_inbound.zig`.
    pub fn replayResolvedPromiseExport(self: *Peer, export_id: u32, resolved: cap_table.ResolvedCap) !void {
        return CallInboundImpl.replayResolvedPromiseExport(self, export_id, resolved);
    }

    /// Body in `call/peer_call_inbound.zig`.
    pub fn adoptThirdPartyAnswer(
        self: *Peer,
        question_id: u32,
        adopted_answer_id: u32,
        question: Question,
    ) anyerror!void {
        return CallInboundImpl.adoptThirdPartyAnswer(self, question_id, adopted_answer_id, question);
    }

    pub fn handleReturn(self: *Peer, frame: []const u8, ret: protocol.Return) anyerror!void {
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
        var retained_return_id: ?u32 = null;
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
            if (ret.tag != .awaitFromThirdParty) {
                latency_started_ns = question.started_ns;
                const original_id = self.adopted_third_party_answers.get(ret.answer_id) orelse ret.answer_id;
                if (self.retained_questions.contains(original_id)) {
                    self.retained_questions.noteReturn(
                        original_id,
                        ret.no_finish_needed,
                        question.is_loopback,
                    );
                    retained_return_id = original_id;
                }
            }
        }
        defer if (latency_started_ns) |started| {
            if (self.clockNow()) |now| {
                const elapsed = now - started;
                if (elapsed >= 0 and !self.questions.contains(ret.answer_id)) {
                    events.emitCallLatency(self.observer, .peer, .unknown, ret.answer_id, @intCast(elapsed));
                }
            }
        };
        peer_return_orchestration.handleReturn(
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
            third_party.adoption.handleReturnAcceptFromThirdPartyForPeerFn(
                Peer,
                Question,
                PendingThirdPartyAwait,
                cap_table.InboundCapTable,
                third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
                finish.freeOwnedFrameForPeerFn(Peer),
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
        ) catch |err| {
            if (retained_return_id) |question_id| {
                // Failure before question removal (notably inbound-cap-table
                // OOM) means the Return was not callback-visible; restore the
                // pending state. Once removed, retained callbacks are at-most
                // once and the answer remains explicitly finishable.
                if (self.questions.contains(ret.answer_id)) {
                    self.retained_questions.rollbackReturn(question_id);
                } else if (ret.no_finish_needed) {
                    _ = self.retained_questions.completeNoFinishNeeded(question_id);
                }
            }
            return err;
        };
        if (retained_return_id) |question_id| {
            if (ret.no_finish_needed) {
                _ = self.retained_questions.completeNoFinishNeeded(question_id);
            }
        }
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

        pub fn noteSendResultsToThirdParty(
            self: *Peer,
            answer_id: u32,
            recipient: ?message.AnyPointerReader,
        ) !void {
            return Peer.noteSendResultsToThirdParty(self, answer_id, recipient);
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
            return Peer.addCrossPeerProxyExport(self, source_peer, target, release_source_import_id, release_source_export_pin_id, null);
        }

        /// Mint a cross-peer proxy whose ctx owns a handoff IMPORT pin on the
        /// source peer (the receiverHosted serve shape).
        pub fn addCrossPeerProxyExportPinnedImport(
            self: *Peer,
            source_peer: *Peer,
            target: cap_table.ResolvedCap,
            release_source_import_id: ?u32,
            release_source_import_pin_id: ?u32,
        ) !u32 {
            return Peer.addCrossPeerProxyExport(self, source_peer, target, release_source_import_id, null, release_source_import_pin_id);
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

        /// Take a handoff pin on one of this peer's IMPORTS — the
        /// receiverHosted lift's Provide-time / serve-time lease.
        pub fn noteHandoffImportPin(self: *Peer, id: u32) !void {
            return Peer.noteHandoffImportPin(self, id);
        }

        /// Release a handoff import pin directly (tests that never hand it to
        /// a proxy); the last unpin emits the withheld deferred Release.
        pub fn releaseHandoffImportPin(self: *Peer, id: u32) !void {
            return Peer.releaseHandoffImportPin(self, id);
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
