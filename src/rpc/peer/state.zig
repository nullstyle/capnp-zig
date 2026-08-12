const std = @import("std");
const builtin = @import("builtin");
const state_types = @import("./peer_state_types.zig");
const cap_table = @import("../caps/table.zig");
const protocol = @import("../wire/protocol.zig");

/// Tunable hard limits for peer-owned protocol state.
///
/// The defaults are intentionally conservative and are enforced by the peer
/// before inserting into unbounded maps or queues. Keeping these limits in a
/// separate module makes the hardening surface easy to find without pulling in
/// the full RPC dispatch implementation.
pub const PeerLimits = struct {
    max_outbound_questions: usize = 4096,
    /// Completed retained answers keep remote question state open until an
    /// explicit Finish or ownership transfer. Bound them independently from
    /// in-flight outbound questions so a caller cannot accumulate them after
    /// their callbacks have run.
    max_retained_questions: usize = 1024,
    max_active_inbound_questions: usize = 4096,
    max_resolved_answers: usize = 4096,
    max_pending_promises: usize = 4096,
    max_pending_export_promises: usize = 4096,
    max_pending_queued_calls: usize = 8192,
    max_pending_queued_call_bytes: usize = 16 * 1024 * 1024,
    /// Matches the capability table's hard cap (`caps.lifecycle.max_table_size`)
    /// so promise-heavy workloads do not hit a lower hidden wall first.
    max_resolved_imports: usize = 10_000,
    max_pending_embargoes: usize = 4096,
    max_loopback_questions: usize = 4096,
    max_send_results_to_yourself: usize = 4096,
    /// Bound on stashed self-loopback results frames awaiting their
    /// `takeFromOtherQuestion` redirect. Overflow degrades gracefully (the call
    /// falls back to the `takeFromOtherQuestion` relay) rather than failing.
    max_loopback_result_stash: usize = 4096,
    max_send_results_to_third_party: usize = 4096,
    max_send_results_to_third_party_bytes: usize = 1024 * 1024,
    max_pending_third_party_returns: usize = 4096,
    max_pending_third_party_return_bytes: usize = 16 * 1024 * 1024,
    max_pending_accepts: usize = 4096,
    max_pending_accept_embargo_buckets: usize = 4096,
    max_pending_accept_embargo_bytes: usize = 1024 * 1024,
    max_active_provides: usize = 4096,
    max_active_provide_key_bytes: usize = 1024 * 1024,
    max_pending_joins: usize = 4096,
    max_pending_join_questions: usize = 4096,
    max_pending_join_accepts: usize = 4096,
    max_pending_join_accept_bytes: usize = 1024 * 1024,
    /// Maximum number of key parts declared by one inbound Join. This is
    /// checked before resolving the target or allocating a part record.
    max_join_parts_per_join: usize = 64,
    /// Aggregate bound across partial Join buckets, their parts, cross-peer
    /// relays, hosted JoinResult provisions, result-answer references, and
    /// direct-Accept records. The older per-table limits remain compatibility
    /// sub-bounds; this is the final peer-wide admission ceiling.
    max_pending_join_records: usize = 4096,
    max_pending_third_party_awaits: usize = 4096,
    max_pending_third_party_answers: usize = 4096,
    max_pending_third_party_completion_bytes: usize = 1024 * 1024,
    max_adopted_third_party_answers: usize = 4096,
    max_remote_abort_reason_bytes: usize = 4096,
    max_persistent_exports: usize = 4096,
    /// Bound on a single sturdy-ref payload, applied both to save-handler
    /// output and to inbound restore parameters.
    max_sturdy_ref_bytes: usize = 64 * 1024,
    /// Cumulative per-connection validation-work rate limit: a token bucket over
    /// the traversal words each inbound frame's validation walk visits. Bounds
    /// the aggregate CPU one connection can spend on validation amplification
    /// (many frames each just under the per-frame traversal cap, or small frames
    /// with large shared-DAG visit counts). Generous by default so legitimate
    /// traffic is never throttled; set the rate to 0 to disable. Enforced only
    /// when a Clock is configured (see `Peer.setClock`).
    max_validation_words_per_second: usize = 128 * 1024 * 1024,
    max_validation_burst_words: usize = 256 * 1024 * 1024,
};

/// Time-domain policy for a peer. All fields are opt-in: a null field
/// disables that bound. Enforcement additionally requires a `Clock`
/// (see `Peer.setClock`); without one, configured timeouts stay inert.
pub const PeerTimeouts = struct {
    /// Default deadline applied to every outbound question at send time.
    /// Individual questions can override via `Peer.setQuestionDeadline`.
    default_call_timeout_ms: ?u64 = null,
    /// Bound on the graceful-shutdown drain. When the drain exceeds this,
    /// remaining in-flight questions are force-cancelled and the shutdown
    /// completes.
    shutdown_drain_timeout_ms: ?u64 = null,
    /// Lease applied to inbound Join phases. A partial bucket is stamped by
    /// its first part; later parts never extend it. Cross-peer relays and
    /// hosted direct-Accept provisions receive fresh phase-local deadlines.
    join_timeout_ms: ?u64 = null,
};

const ExportDeinitCtxFn = *const fn (std.mem.Allocator, *anyopaque) void;

pub fn ExportEntry(comptime ExportType: type) type {
    return struct {
        handler: ?ExportType = null,
        deinit_ctx: ?ExportDeinitCtxFn = null,
        /// Wire references: one per cap descriptor sent to the remote peer,
        /// released by inbound Release messages (or Finish.releaseResultCaps).
        ref_count: u32,
        /// Answer-held references: one per results cap descriptor of each
        /// recorded resolved answer, held from Return until that answer's
        /// Finish so pipeline targets stay dispatchable even after the remote
        /// Releases its own wire references. Only Finish releases these; a
        /// Release message cannot touch them.
        answer_ref_count: u32 = 0,
        /// Promise-held references: taken on a resolution-target export when a
        /// promise export resolves to it (`resolvePromiseExportToExport`). The
        /// promise export routes inbound calls at this target id, so it must
        /// stay dispatchable even after the remote Releases its own wire
        /// references to it. Released only when the promise export that took it
        /// is itself destroyed; a Release message cannot touch these.
        promise_ref_count: u32 = 0,
        /// Handoff-held references (L3 three-party hosting): one per live
        /// provision whose pinned target is this export, plus one per live
        /// cross-peer provision proxy forwarding to it. Like answer_ref_count
        /// and promise_ref_count, a purely local lease: an inbound Release can
        /// never touch it, and it never corresponds to an outbound wire
        /// reference — so a hostile over-release stays a DETECTED
        /// ReleaseCountExceeded rather than a silent destroy of a pinned
        /// export.
        handoff_ref_count: u32 = 0,
        /// Set the first time a wire reference is granted on this entry
        /// (`noteExportRef`). An entry that was NEVER wire-referenced is
        /// app-owned — today no wire message can destroy such an entry, and
        /// the handoff unpin path must not become the first one that can:
        /// `releaseHandoffHeldExport` destroys only entries that were granted
        /// to the wire at least once.
        wire_ref_granted: bool = false,
        is_promise: bool = false,
        resolved: ?cap_table.ResolvedCap = null,
    };
}

pub const ResolvedAnswer = struct {
    frame: []u8,
};

/// The exception a Return already delivered for an inbound answer, kept until
/// that answer's Finish. Only results Returns are recorded in
/// `resolved_answers`, so without this record a call pipelined on a FAILED
/// answer that arrives after its exception Return would queue in
/// `pending_promises` forever — the failed answer can never replay it. The
/// record lets the peer answer such a call with a copy of the same exception,
/// the broken-pipeline behavior of the C++ reference.
pub const FailedAnswer = struct {
    /// Owned copy of the exception reason.
    reason: []u8,
    ex_type: protocol.ExceptionType,
};

pub const PendingCall = struct {
    frame: []u8,
    caps: cap_table.InboundCapTable,
    /// The queued call's own question id, decoded once at enqueue so the
    /// per-inbound-message duplicate-id and cancellation scans never re-run a
    /// validating parse over every stored frame (an amplification vector).
    question_id: ?u32,
};

pub const ResolvedImport = struct {
    cap: ?cap_table.ResolvedCap,
    embargo_id: ?u32 = null,
    embargoed: bool = false,
};

pub const QuestionDeinitCtxFn = *const fn (std.mem.Allocator, *anyopaque) void;

pub fn Question(comptime QuestionCallbackType: type) type {
    return struct {
        ctx: *anyopaque,
        on_return: QuestionCallbackType,
        deinit_ctx: ?QuestionDeinitCtxFn = null,
        is_loopback: bool = false,
        suppress_auto_finish: bool = false,
        restore_on_return_error: bool = true,
        /// Absolute monotonic deadline (clock nanoseconds) after which the
        /// question is cancelled. Null means no deadline.
        deadline_ns: ?i64 = null,
        /// Monotonic send timestamp, stamped when the peer has a clock.
        /// Used to emit `call_latency` events on Return dispatch.
        started_ns: ?i64 = null,
        /// Set when the question was cancelled locally (deadline expiry or
        /// explicit cancel). The local callback has already been delivered
        /// an exception and a Finish has been sent; the entry stays in the
        /// table only to absorb the remote's guaranteed Return silently.
        cancelled: bool = false,
        /// A protocol lifecycle has ended locally and requested a wire Finish,
        /// but the transport rejected the last send. Maintenance retries the
        /// Finish before removing this question, so ownership is never lost.
        finish_on_maintenance: bool = false,
        finish_release_result_caps: bool = false,
        /// When this outbound Call targeted an UNRESOLVED promise import, this
        /// records that promise import id. Used by the Level-3 recipient
        /// auto-pickup (`tryAutoPickupThirdParty`) to detect whether a
        /// pipelined call is still in flight against the promise being handed
        /// off — the spec condition (`rpc.capnp:885-888`) under which the
        /// pickup must embargo the `Accept` and emit a `context.accept`
        /// `Disembargo` to preserve e-order. The question's mere presence in
        /// the table (with this field set) IS the in-flight signal; it is
        /// removed exactly once when the Return arrives, so no separate
        /// per-return decrement is needed. Null for calls to resolved caps,
        /// exports, or promised answers.
        target_promise_import: ?u32 = null,
    };
}

pub fn PendingThirdPartyAwait(comptime QuestionType: type) type {
    return struct {
        question_id: u32,
        question: QuestionType,
    };
}

/// Owner-thread identity captured at peer init. Stored as `u64` — not
/// `std.Thread.Id` — because the alias's integer width varies by target
/// (u32 on linux/windows, u64 on darwin) and `@typeName` renders the
/// resolved integer, which would make the api-snapshot gates
/// platform-dependent. `std.Thread.getCurrentId()` widens losslessly on
/// assignment and comparison.
pub const OwnerThreadId = struct {
    value: u64,
};

pub fn initialOwnerThreadId() ?OwnerThreadId {
    if (comptime builtin.target.os.tag == .freestanding) return null;
    return .{ .value = std.Thread.getCurrentId() };
}

/// Pure decision: should an affinity check fire for this owner/current
/// pair under the given mode? Extracted so the policy is testable without
/// panicking. Checks always fire in Debug; in release modes they fire only
/// when the embedder opted in to runtime checks.
pub fn threadAffinityViolation(
    owner_thread_id: ?OwnerThreadId,
    current: OwnerThreadId,
    runtime_checks: bool,
    mode: std.builtin.OptimizeMode,
) bool {
    if (mode != .debug and !runtime_checks) return false;
    const owner = owner_thread_id orelse return false;
    return current.value != owner.value;
}

pub fn assertThreadAffinity(owner_thread_id: ?OwnerThreadId, runtime_checks: bool) void {
    if (comptime builtin.target.os.tag == .freestanding) return;
    // Skip the getCurrentId() read entirely when no check can fire.
    if (comptime builtin.mode != .debug) {
        if (!runtime_checks) return;
    }
    if (threadAffinityViolation(owner_thread_id, .{ .value = std.Thread.getCurrentId() }, runtime_checks, builtin.mode)) {
        @panic("Peer method called from wrong thread: Peer is not thread-safe, all calls must be on the owner thread");
    }
}

pub const ProvideTarget = state_types.ProvideTarget;
pub const ProvideEntry = state_types.ProvideEntry;
pub const JoinKeyPart = state_types.JoinKeyPart;
pub const JoinPartEntry = state_types.JoinPartEntry;
pub const JoinState = state_types.JoinState;
pub const PendingJoinQuestion = state_types.PendingJoinQuestion;
pub const PendingEmbargoedAccept = state_types.PendingEmbargoedAccept;
