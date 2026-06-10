const std = @import("std");
const builtin = @import("builtin");
const state_types = @import("./peer_state_types.zig");
const cap_table = @import("../caps/table.zig");

/// Tunable hard limits for peer-owned protocol state.
///
/// The defaults are intentionally conservative and are enforced by the peer
/// before inserting into unbounded maps or queues. Keeping these limits in a
/// separate module makes the hardening surface easy to find without pulling in
/// the full RPC dispatch implementation.
pub const PeerLimits = struct {
    max_outbound_questions: usize = 4096,
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
    max_pending_third_party_awaits: usize = 4096,
    max_pending_third_party_answers: usize = 4096,
    max_pending_third_party_completion_bytes: usize = 1024 * 1024,
    max_adopted_third_party_answers: usize = 4096,
    max_remote_abort_reason_bytes: usize = 4096,
    max_persistent_exports: usize = 4096,
    /// Bound on a single sturdy-ref payload, applied both to save-handler
    /// output and to inbound restore parameters.
    max_sturdy_ref_bytes: usize = 64 * 1024,
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
};

pub fn ExportEntry(comptime ExportType: type) type {
    return struct {
        handler: ?ExportType = null,
        ref_count: u32,
        is_promise: bool = false,
        resolved: ?cap_table.ResolvedCap = null,
    };
}

pub const ResolvedAnswer = struct {
    frame: []u8,
};

pub const PendingCall = struct {
    frame: []u8,
    caps: cap_table.InboundCapTable,
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
    };
}

pub fn PendingThirdPartyAwait(comptime QuestionType: type) type {
    return struct {
        question_id: u32,
        question: QuestionType,
    };
}

pub fn initialOwnerThreadId() ?std.Thread.Id {
    if (comptime builtin.target.os.tag == .freestanding) return null;
    return std.Thread.getCurrentId();
}

/// Pure decision: should an affinity check fire for this owner/current
/// pair under the given mode? Extracted so the policy is testable without
/// panicking. Checks always fire in Debug; in release modes they fire only
/// when the embedder opted in to runtime checks.
pub fn threadAffinityViolation(
    owner_thread_id: ?std.Thread.Id,
    current: std.Thread.Id,
    runtime_checks: bool,
    mode: std.builtin.OptimizeMode,
) bool {
    if (mode != .Debug and !runtime_checks) return false;
    const owner = owner_thread_id orelse return false;
    return current != owner;
}

pub fn assertThreadAffinity(owner_thread_id: ?std.Thread.Id, runtime_checks: bool) void {
    if (comptime builtin.target.os.tag == .freestanding) return;
    // Skip the getCurrentId() read entirely when no check can fire.
    if (comptime builtin.mode != .Debug) {
        if (!runtime_checks) return;
    }
    if (threadAffinityViolation(owner_thread_id, std.Thread.getCurrentId(), runtime_checks, builtin.mode)) {
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
