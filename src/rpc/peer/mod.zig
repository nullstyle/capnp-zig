const std = @import("std");
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
const peer_transport_callbacks = peer_transport.callbacks;
const peer_transport_state = peer_transport.state;
const peer_question_state = @import("./peer_question_state.zig");
const peer_cleanup = @import("./peer_cleanup.zig");
const peer_return_frames = @import("./return/peer_return_frames.zig");

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
const Question = state.Question(QuestionCallback);
const PendingThirdPartyAwait = state.PendingThirdPartyAwait(Question);

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

pub const Peer = struct {
    allocator: std.mem.Allocator,
    limits: PeerLimits,

    // -- Transport binding --------------------------------------------------

    /// Attached transport callbacks. Set/cleared atomically by
    /// `attachTransportBinding` / `detachTransport`.
    transport: TransportBinding = .{},

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
    /// Inbound question ids whose Finish arrived before their (async) Return.
    /// A late Return for one of these must NOT be recorded in resolved_answers:
    /// no further Finish will ever clear it, and a stale entry poisons legal
    /// reuse of the id (DuplicateQuestionId against a compliant peer). Bounded
    /// by max_active_inbound_questions.
    finished_early_answers: std.AutoHashMap(u32, void),

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
    /// In-progress Join operations collecting parts.
    pending_joins: std.AutoHashMap(u32, JoinState),
    /// Maps a Join answer's question ID to its join ID + part number.
    pending_join_questions: std.AutoHashMap(u32, PendingJoinQuestion),
    /// Accept messages waiting for a disembargo.
    pending_accepts_by_embargo: std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)),
    /// Maps question IDs to embargo keys for cleanup on Finish.
    pending_accept_embargo_by_question: std.AutoHashMap(u32, []u8),
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

    /// Create a peer and immediately attach it to a connection/transport.
    pub fn init(allocator: std.mem.Allocator, conn: anytype) Peer {
        var peer = initDetached(allocator);
        peer.attachConnection(conn);
        return peer;
    }

    /// Create a peer without an attached transport.
    ///
    /// Useful for WASM, unit tests, or manual frame injection via
    /// `handleFrame` and `setSendFrameOverride`.
    pub fn initDetached(allocator: std.mem.Allocator) Peer {
        return initDetachedWithLimits(allocator, .{});
    }

    /// Create a peer without an attached transport and with explicit limits.
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
            .finished_early_answers = std.AutoHashMap(u32, void).init(allocator),
            .pending_promises = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(allocator),
            .pending_export_promises = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(allocator),
            .forwarded_questions = std.AutoHashMap(u32, u32).init(allocator),
            .forwarded_tail_questions = std.AutoHashMap(u32, u32).init(allocator),
            .provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(allocator),
            .provides_by_key = std.StringHashMap(u32).init(allocator),
            .pending_joins = std.AutoHashMap(u32, JoinState).init(allocator),
            .pending_join_questions = std.AutoHashMap(u32, PendingJoinQuestion).init(allocator),
            .pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)).init(allocator),
            .pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(allocator),
            .pending_third_party_awaits = std.StringHashMap(PendingThirdPartyAwait).init(allocator),
            .pending_third_party_answers = std.StringHashMap(u32).init(allocator),
            .pending_third_party_returns = std.AutoHashMap(u32, []u8).init(allocator),
            .adopted_third_party_answers = std.AutoHashMap(u32, u32).init(allocator),
            .resolved_imports = std.AutoHashMap(u32, ResolvedImport).init(allocator),
            .pending_embargoes = std.AutoHashMap(u32, u32).init(allocator),
            .loopback_questions = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_yourself = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_third_party = std.AutoHashMap(u32, ?[]u8).init(allocator),
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

    /// Bind a typed connection to this peer, wiring up transport callbacks.
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

    /// Attach raw transport callbacks (send, close, isClosing) to this peer.
    ///
    /// Unlike `attachConnection`, this does not wrap a typed connection object;
    /// the caller provides each callback individually. Panics if a transport
    /// is already attached.
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

    /// Attach a named transport binding to this peer.
    ///
    /// This is the preferred boundary for new transports: adapt the transport
    /// into a `TransportBinding`, then attach the binding here.
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
        _ = self.forceCancelAllQuestions(disconnected_reason);
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
        self.finished_early_answers.deinit();
        {
            var p_it = self.persistent_exports.valueIterator();
            while (p_it.next()) |st| self.allocator.destroy(st.*);
        }
        self.persistent_exports.deinit();
        self.exports.deinit();
        self.forwarded_questions.deinit();
        self.forwarded_tail_questions.deinit();
        peer_cleanup.deinitProvideEntryMap(
            @TypeOf(self.provides_by_question),
            self.allocator,
            &self.provides_by_question,
        );
        self.provides_by_key.deinit();

        peer_cleanup.deinitJoinStateMap(
            @TypeOf(self.pending_joins),
            self.allocator,
            &self.pending_joins,
        );
        self.pending_join_questions.deinit();

        peer_cleanup.deinitOwnedStringKeyListMap(
            @TypeOf(self.pending_accepts_by_embargo),
            self.allocator,
            &self.pending_accepts_by_embargo,
        );
        // Values in pending_accept_embargo_by_question are borrowed from
        // pending_accepts_by_embargo (already freed above), so just deinit.
        self.pending_accept_embargo_by_question.deinit();
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
        self.assertThreadAffinity();
        const id = try self.caps.allocExportId();
        try self.caps.noteExport(id);
        errdefer self.caps.clearExport(id);
        try self.exports.put(id, .{
            .handler = exported,
            .ref_count = 0,
            .is_promise = false,
            .resolved = null,
        });
        log.debug("added export id={}", .{id});
        return id;
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
        const export_id: u32 = switch (outcome) {
            .unknown => return self.sendReturnException(call.question_id, "unknown sturdy ref"),
            .existing => |id| blk: {
                if (!self.exports.contains(id)) {
                    return self.sendReturnException(call.question_id, "unknown sturdy ref");
                }
                break :blk id;
            },
            .host => |exported| try self.addExport(exported),
        };
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
        const response: RestoreResponse = switch (ret.tag) {
            .results => blk: {
                const payload = ret.results orelse return error.MissingReturnPayload;
                if (payload.content.isNull()) return error.MissingRestoredCapability;
                const results_struct = payload.content.getStruct() catch return error.MissingRestoredCapability;
                const field = results_struct.readAnyPointer(0) catch return error.MissingRestoredCapability;
                const cap = field.getCapability() catch return error.MissingRestoredCapability;
                var mutable_caps = inbound_caps.*;
                try mutable_caps.retainCapability(cap);
                break :blk .{ .cap = try inbound_caps.resolveCapability(cap) };
            },
            .exception => .{ .exception = ret.exception orelse return error.MissingException },
            else => .{ .other = ret.tag },
        };
        try ctx.callback(ctx.user_ctx, peer, response);
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
        self.assertThreadAffinity();
        const entry = self.questions.getPtr(question_id) orelse return error.UnknownQuestion;
        if (entry.cancelled) return;
        const question = entry.*;

        if (question.is_loopback) {
            _ = self.loopback_questions.remove(question_id);
            self.removeQuestion(question_id);
            try self.deliverLocalException(question, question_id, reason);
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

        try self.deliverLocalException(question, question_id, reason);
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
            self.cancelQuestion(question_id, deadline_reason) catch |err| {
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
                    cancelled += self.forceCancelAllQuestions(shutdown_reason);
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
    fn deliverLocalException(self: *Peer, question: Question, question_id: u32, reason: []const u8) !void {
        var callback_ran = false;
        errdefer if (!callback_ran) {
            if (question.deinit_ctx) |deinit_ctx| deinit_ctx(self.allocator, question.ctx);
        };
        const frame = try peer_return_frames.buildReturnExceptionFrame(self.allocator, question_id, reason);
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
    fn forceCancelAllQuestions(self: *Peer, reason: []const u8) usize {
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
            self.deliverLocalException(question, question_id, reason) catch |err| {
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
            Peer.allocateQuestion,
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

    /// Send a return with results for a previously received call.
    pub fn sendReturnResults(self: *Peer, answer_id: u32, ctx: *anyopaque, build: ReturnBuildFn) !void {
        self.assertThreadAffinity();
        // sendResultsTo routing is resolved in precedence order:
        // third-party handoff > local results-sent-elsewhere marker > normal results payload.
        if (self.send_results_to_third_party.fetchRemove(answer_id)) |entry| {
            _ = self.send_results_to_yourself.remove(answer_id);
            defer if (entry.value) |payload| self.allocator.free(payload);
            try self.sendReturnAcceptFromThirdParty(answer_id, entry.value);
            return;
        }

        if (self.send_results_to_yourself.remove(answer_id)) {
            try self.sendReturnTag(answer_id, .resultsSentElsewhere);
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
        // loopback_questions and finished_early_answers entries for this id.
        const is_loopback = self.loopback_questions.contains(answer_id);
        const is_finished_early = self.finished_early_answers.contains(answer_id);

        // Do not record a resolved answer when it can never be cleared by a
        // Finish: (1) loopback answers are delivered locally, are never
        // referenced by a remote PromisedAnswer, and get no Finish — and their
        // outbound-counter ids would collide with the remote question-id space;
        // (2) finished-early answers already had their Finish arrive before
        // this (late, async) Return. In both cases a recorded entry would leak
        // and poison DuplicateQuestionId checks against a compliant peer.
        const should_record = !is_loopback and !is_finished_early;

        // Reserve the record resources (count budget, map slot, frame copy)
        // BEFORE sending so recording is infallible afterward. If it could fail
        // after the frame is on the wire, the error would drive the dispatch
        // catch to send a second exception Return for this answer (audit item 7).
        var reservation: ?ResolvedAnswerReservation = null;
        errdefer if (reservation) |r| r.deinit(self);
        if (should_record) reservation = try self.reserveResolvedAnswer(answer_id, bytes);

        try self.sendReturnFrameWithLoopback(answer_id, bytes);
        cap_table.commitOutboundCapEffects(&self.caps, &effects);
        effects_committed = true;

        if (reservation) |r| {
            self.commitReservedResolvedAnswer(answer_id, r);
            reservation = null;
        }
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
        // Capture before delivery consumes the loopback / finished-early entry.
        const is_loopback = self.loopback_questions.contains(ret.answer_id);
        const is_finished_early = self.finished_early_answers.contains(ret.answer_id);

        // See sendReturnResults: neither loopback nor finished-early answers
        // may be recorded — no Finish will clear them and the id would be
        // poisoned for reuse.
        const should_record = ret.tag == .results and !is_loopback and !is_finished_early;

        // Reserve record resources before the send so recording is infallible
        // afterward and cannot force a second (exception) Return for this
        // answer (audit item 7). A reserve failure here rolls the outbound cap
        // refs back (via rollback_outbound_refs) since nothing was sent yet.
        var reservation: ?ResolvedAnswerReservation = null;
        errdefer if (reservation) |r| r.deinit(self);
        if (should_record) reservation = try self.reserveResolvedAnswer(ret.answer_id, frame);

        try self.sendReturnFrameWithLoopback(ret.answer_id, frame);
        rollback_outbound_refs = false;

        if (reservation) |r| {
            self.commitReservedResolvedAnswer(ret.answer_id, r);
            reservation = null;
        }
    }

    /// Send a return with an exception for a previously received call.
    ///
    /// A failed answer carries no results, so any pipelined calls queued
    /// against it can never be satisfied. After sending the exception this
    /// drains those queued calls, failing each with its own Return so the
    /// exactly-one-Return-per-call invariant holds and the caller's question
    /// table can drain (a compliant peer otherwise hangs forever).
    pub fn sendReturnException(self: *Peer, answer_id: u32, reason: []const u8) !void {
        self.assertThreadAffinity();
        try self.sendReturnExceptionNoDrain(answer_id, reason);
        self.failQueuedPromisedCalls(answer_id, reason);
    }

    /// Send an exception Return without draining queued pipelined children.
    /// Used internally where the queued-call drain must not re-enter (e.g.
    /// while iterating the live `pending_promises` map).
    fn sendReturnExceptionNoDrain(self: *Peer, answer_id: u32, reason: []const u8) !void {
        try peer_return_dispatch.sendReturnExceptionForPeer(
            Peer,
            self,
            answer_id,
            reason,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
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
    fn failQueuedPromisedCalls(self: *Peer, answer_id: u32, reason: []const u8) void {
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
                self.sendReturnExceptionNoDrain(child_qid, reason) catch |err| {
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

    fn sendReturnTag(self: *Peer, answer_id: u32, tag: protocol.ReturnTag) !void {
        try peer_return_dispatch.sendReturnTagForPeer(
            Peer,
            self,
            answer_id,
            tag,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
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
        // Any terminal Return for this answer discharges a finished-early
        // tombstone (see finished_early_answers / handleFinish).
        _ = self.finished_early_answers.remove(answer_id);
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
        _ = self.forceCancelAllQuestions(disconnected_reason);
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
        var decoded = protocol.DecodedMessage.init(self.allocator, frame) catch |err| {
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
        peer_outbound_control.sendAbort(
            Peer,
            self,
            @errorName(err),
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
            noteAnswerExportRef,
            rollbackAnswerExportRef,
            sendReturnException,
            sendFrame,
            recordResolvedAnswer,
        );
    }

    fn handleFinish(self: *Peer, finish_msg: protocol.Finish) !void {
        const qid = finish_msg.question_id;
        const was_active = self.active_inbound_questions.remove(qid);
        // Cancellation race: a Finish for an in-flight inbound call (still
        // active, not yet resolved) means an async handler will answer later.
        // Tombstone the id so that late Return is not recorded in
        // resolved_answers (nothing would ever clear it, and it would poison
        // legal reuse of the id). Bounded by max_active_inbound_questions;
        // best-effort under OOM. Synchronous handlers already removed the
        // active entry before this Finish, so was_active is false for them.
        if (was_active and
            !self.resolved_answers.contains(qid) and
            self.finished_early_answers.count() < self.limits.max_active_inbound_questions)
        {
            self.finished_early_answers.put(qid, {}) catch |err| self.reportNonfatalError(err);
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
            .clear_pending_accept_question = peer_embargo_accepts.clearPendingAcceptQuestionForPeerFn(
                Peer,
                PendingEmbargoedAccept,
            ),
            .take_forwarded_tail_question = peer_forward_orchestration.takeForwardedTailQuestionForPeerFn(Peer),
            .send_finish = peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
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
        return canceled_promised or canceled_export;
    }

    fn handleRelease(self: *Peer, release: protocol.Release) !void {
        try peer_cap_lifecycle.handleRelease(Peer, self, release, releaseExport);
    }

    fn handleResolve(self: *Peer, resolve_msg: protocol.Resolve) !void {
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

    fn hasKnownDisembargoTarget(self: *Peer, target: protocol.MessageTarget) bool {
        return switch (target.tag) {
            .importedCap => blk: {
                // Per the RPC spec, MessageTarget.importedCap names an entry
                // in the *sender's* import table — i.e. one of our exports. A
                // senderLoopback disembargo issued after we resolve a promise
                // export targets that export, so validate against the export
                // table. (Also accept a matching import id for robustness
                // against either addressing convention.)
                const cap_id = target.imported_cap orelse break :blk false;
                break :blk self.exports.contains(cap_id) or self.caps.imports.contains(cap_id);
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
            .release_embargoed_accepts = peer_embargo_accepts.releaseEmbargoedAcceptsForPeerFn(
                Peer,
                PendingEmbargoedAccept,
                ProvideEntry,
                ProvideTarget,
                Peer.sendReturnProvidedTarget,
                Peer.sendReturnException,
            ),
        };
        try peer_disembargo.handleDisembargoWithOps(Peer, self, disembargo_msg, ops);
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
    }

    fn handleAccept(self: *Peer, accept: protocol.Accept) !void {
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
            Peer.ensureJoinBudget,
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
    /// Bootstrap answer (active, resolved, forwarded, or queued for a promised
    /// target). This is the shared answer namespace; it deliberately excludes
    /// the provide/join question tables so their handlers can report their own
    /// specific errors for same-type collisions.
    fn inboundAnswerQuestionIdInUse(self: *Peer, question_id: u32) !bool {
        return self.resolved_answers.contains(question_id) or
            self.active_inbound_questions.contains(question_id) or
            self.send_results_to_yourself.contains(question_id) or
            self.send_results_to_third_party.contains(question_id) or
            self.forwarded_questions.contains(question_id) or
            self.forwarded_tail_questions.contains(question_id) or
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
            self.pending_join_questions.contains(question_id);
    }

    fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
        // Reject duplicate question IDs from the remote peer (spec violation).
        // Covers the shared answer namespace plus in-flight Provide/Join
        // questions so a Call can never collide with any other inbound question.
        if (try self.inboundQuestionIdInUse(call.question_id)) {
            return error.DuplicateQuestionId;
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
            self.resolved_answers.count(),
            self.limits.max_resolved_answers,
        );
        // Reserve a map slot so the post-send getOrPutAssumeCapacity cannot OOM.
        try self.resolved_answers.ensureUnusedCapacity(1);
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
    /// Not part of the public API.
    pub const test_hooks = struct {
        pub const ForwardCallContextType = ForwardCallContext;

        pub fn sendFrame(self: *Peer, frame: []const u8) !void {
            return Peer.sendFrame(self, frame);
        }

        pub fn removeQuestion(self: *Peer, question_id: u32) void {
            Peer.removeQuestion(self, question_id);
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
    };
};
