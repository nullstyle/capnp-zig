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
    max_resolved_imports: usize = 4096,
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

pub fn assertThreadAffinity(owner_thread_id: ?std.Thread.Id) void {
    if (comptime builtin.target.os.tag == .freestanding) return;
    if (builtin.mode == .Debug) {
        const owner = owner_thread_id orelse return;
        const current = std.Thread.getCurrentId();
        if (current != owner) {
            @panic("Peer method called from wrong thread: Peer is not thread-safe, all calls must be on the owner thread");
        }
    }
}

pub const ProvideTarget = state_types.ProvideTarget;
pub const ProvideEntry = state_types.ProvideEntry;
pub const JoinKeyPart = state_types.JoinKeyPart;
pub const JoinPartEntry = state_types.JoinPartEntry;
pub const JoinState = state_types.JoinState;
pub const PendingJoinQuestion = state_types.PendingJoinQuestion;
pub const PendingEmbargoedAccept = state_types.PendingEmbargoedAccept;
