/// Bundles the 11 callback parameters of handleFinish into a single operations struct.
/// Groups: question-state cleanup (5), tail-finish forwarding (2), answer-frame cleanup (3).
pub fn FinishOps(comptime PeerType: type) type {
    return struct {
        // Question-state cleanup
        remove_send_results_to_yourself: *const fn (*PeerType, u32) void,
        clear_send_results_to_third_party: *const fn (*PeerType, u32) void,
        clear_provide: *const fn (*PeerType, u32) void,
        clear_pending_join_question: *const fn (*PeerType, u32) void,
        clear_pending_accept_question: *const fn (*PeerType, u32) void,
        // Tail-finish forwarding
        take_forwarded_tail_question: *const fn (*PeerType, u32) ?u32,
        send_finish: *const fn (*PeerType, u32, bool) anyerror!void,
        // Answer-frame cleanup
        take_resolved_answer_frame: *const fn (*PeerType, u32) ?[]u8,
        release_caps_for_frame: *const fn (*PeerType, []const u8) anyerror!void,
        free_frame: *const fn (*PeerType, []u8) void,
    };
}

pub fn clearFinishQuestionState(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    remove_send_results_to_yourself: *const fn (*PeerType, u32) void,
    clear_send_results_to_third_party: *const fn (*PeerType, u32) void,
    clear_provide: *const fn (*PeerType, u32) void,
    clear_pending_join_question: *const fn (*PeerType, u32) void,
    clear_pending_accept_question: *const fn (*PeerType, u32) void,
) void {
    remove_send_results_to_yourself(peer, question_id);
    clear_send_results_to_third_party(peer, question_id);
    clear_provide(peer, question_id);
    clear_pending_join_question(peer, question_id);
    clear_pending_accept_question(peer, question_id);
}

pub fn forwardTailFinishIfNeeded(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    take_forwarded_tail_question: *const fn (*PeerType, u32) ?u32,
    send_finish: *const fn (*PeerType, u32, bool) anyerror!void,
) !void {
    if (take_forwarded_tail_question(peer, question_id)) |tail_question_id| {
        try send_finish(peer, tail_question_id, false);
    }
}

pub fn handleResolvedAnswerCleanup(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    release_result_caps: bool,
    take_resolved_answer_frame: *const fn (*PeerType, u32) ?[]u8,
    release_caps_for_frame: *const fn (*PeerType, []const u8) anyerror!void,
    free_frame: *const fn (*PeerType, []u8) void,
) !void {
    if (take_resolved_answer_frame(peer, question_id)) |frame| {
        defer free_frame(peer, frame);
        if (release_result_caps) {
            try release_caps_for_frame(peer, frame);
        }
    }
}

pub fn handleFinish(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    release_result_caps: bool,
    remove_send_results_to_yourself: *const fn (*PeerType, u32) void,
    clear_send_results_to_third_party: *const fn (*PeerType, u32) void,
    clear_provide: *const fn (*PeerType, u32) void,
    clear_pending_join_question: *const fn (*PeerType, u32) void,
    clear_pending_accept_question: *const fn (*PeerType, u32) void,
    take_forwarded_tail_question: *const fn (*PeerType, u32) ?u32,
    send_finish: *const fn (*PeerType, u32, bool) anyerror!void,
    take_resolved_answer_frame: *const fn (*PeerType, u32) ?[]u8,
    release_caps_for_frame: *const fn (*PeerType, []const u8) anyerror!void,
    free_frame: *const fn (*PeerType, []u8) void,
) !void {
    const ops = FinishOps(PeerType){
        .remove_send_results_to_yourself = remove_send_results_to_yourself,
        .clear_send_results_to_third_party = clear_send_results_to_third_party,
        .clear_provide = clear_provide,
        .clear_pending_join_question = clear_pending_join_question,
        .clear_pending_accept_question = clear_pending_accept_question,
        .take_forwarded_tail_question = take_forwarded_tail_question,
        .send_finish = send_finish,
        .take_resolved_answer_frame = take_resolved_answer_frame,
        .release_caps_for_frame = release_caps_for_frame,
        .free_frame = free_frame,
    };
    try handleFinishWithOps(PeerType, peer, question_id, release_result_caps, ops);
}

/// handleFinish variant that accepts a bundled FinishOps instead of 11 individual callbacks.
pub fn handleFinishWithOps(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    release_result_caps: bool,
    ops: FinishOps(PeerType),
) !void {
    clearFinishQuestionState(
        PeerType,
        peer,
        question_id,
        ops.remove_send_results_to_yourself,
        ops.clear_send_results_to_third_party,
        ops.clear_provide,
        ops.clear_pending_join_question,
        ops.clear_pending_accept_question,
    );
    try forwardTailFinishIfNeeded(
        PeerType,
        peer,
        question_id,
        ops.take_forwarded_tail_question,
        ops.send_finish,
    );
    try handleResolvedAnswerCleanup(
        PeerType,
        peer,
        question_id,
        release_result_caps,
        ops.take_resolved_answer_frame,
        ops.release_caps_for_frame,
        ops.free_frame,
    );
}

pub fn takeResolvedAnswerFrameForPeer(comptime PeerType: type, peer: *PeerType, question_id: u32) ?[]u8 {
    if (peer.resolved_answers.fetchRemove(question_id)) |entry| {
        return entry.value.frame;
    }
    return null;
}

pub fn takeResolvedAnswerFrameForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) ?[]u8 {
    return struct {
        fn call(peer: *PeerType, question_id: u32) ?[]u8 {
            return takeResolvedAnswerFrameForPeer(PeerType, peer, question_id);
        }
    }.call;
}

pub fn freeOwnedFrameForPeer(comptime PeerType: type, peer: *PeerType, frame: []u8) void {
    peer.allocator.free(frame);
}

pub fn freeOwnedFrameForPeerFn(comptime PeerType: type) *const fn (*PeerType, []u8) void {
    return struct {
        fn call(peer: *PeerType, frame: []u8) void {
            freeOwnedFrameForPeer(PeerType, peer, frame);
        }
    }.call;
}
