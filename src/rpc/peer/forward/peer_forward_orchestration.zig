const std = @import("std");
const cap_table = @import("../../caps/table.zig");
const message = @import("../../../serialization/message.zig");
const peer_third_party = @import("../third_party.zig");
const protocol = @import("../../wire/protocol.zig");

pub const ForwardResolvedMode = enum {
    translate_to_caller,
    sent_elsewhere,
    propagate_results_sent_elsewhere,
    propagate_accept_from_third_party,
};

pub const ForwardReturnMode = enum {
    translate_to_caller,
    sent_elsewhere,
    propagate_results_sent_elsewhere,
    propagate_accept_from_third_party,
};

pub fn toControlMode(mode: ForwardReturnMode) peer_third_party.ForwardedReturnMode {
    return switch (mode) {
        .translate_to_caller => .translate_to_caller,
        .sent_elsewhere => .sent_elsewhere,
        .propagate_results_sent_elsewhere => .propagate_results_sent_elsewhere,
        .propagate_accept_from_third_party => .propagate_accept_from_third_party,
    };
}

pub fn fromResolvedMode(mode: ForwardResolvedMode) ForwardReturnMode {
    return switch (mode) {
        .translate_to_caller => .translate_to_caller,
        .sent_elsewhere => .sent_elsewhere,
        .propagate_results_sent_elsewhere => .propagate_results_sent_elsewhere,
        .propagate_accept_from_third_party => .propagate_accept_from_third_party,
    };
}

/// Pick the forward mode for a call being relayed to a resolved promise target.
///
/// `resolved` selects the topology and `tag` is the ORIGINAL call's
/// `sendResultsTo` discriminant:
///   - `.imported` is the REFLECTED-LOOPBACK case (W1): the promise resolved to
///     a capability the CALLER hosts, so the relayed call travels back out the
///     same link it came in on. For a plain `.caller`-originated pipelined call
///     we forward it as a normal `.caller` call and translate the real results
///     straight back to the original question (`.translate_to_caller`). The
///     spec-canonical `yourself` + `takeFromOtherQuestion` tail-call is
///     protocol-correct and cheaper, but two reference peers cannot follow it:
///     go-capnp rejects an inbound `sendResultsTo != caller` call outright
///     (Unimplemented) and cannot parse a `takeFromOtherQuestion`/
///     `resultsSentElsewhere` return at all, and capnp-rpc only completes the
///     take-from path under narrow conditions. `.translate_to_caller` emits only
///     plain `.caller` calls and `.results` returns, which every reference impl
///     consumes, at the cost of routing the results back through this vat. A
///     nested tail-call being reflected (original tag already `.yourself`/
///     `.thirdParty`) keeps its propagate mode so its own tail-call semantics
///     survive.
///   - `.promised` (a promise resolving to another promised-answer target) keeps
///     the tag-driven mapping unchanged — that is a different (non-reflected)
///     topology and is out of W1's scope.
fn forwardModeForResolved(resolved: cap_table.ResolvedCap, tag: protocol.SendResultsToTag) ForwardResolvedMode {
    if (resolved == .imported and tag == .caller) return .translate_to_caller;
    return forwardModeForSendResults(tag);
}

fn forwardModeForSendResults(tag: protocol.SendResultsToTag) ForwardResolvedMode {
    return switch (tag) {
        .caller => .sent_elsewhere,
        .yourself => .propagate_results_sent_elsewhere,
        .thirdParty => .propagate_accept_from_third_party,
    };
}

pub fn handleResolvedCall(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    call: protocol.Call,
    inbound_caps: *const InboundCapsType,
    resolved: cap_table.ResolvedCap,
    handle_exported: *const fn (*PeerType, protocol.Call, *const InboundCapsType, u32) anyerror!void,
    forward_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap, ForwardResolvedMode) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    /// Runs before a call whose target resolved to no capability, or whose
    /// forward failed before the forwarded Call went out, is answered with an
    /// exception: the Peer marks a `sendResultsTo = yourself` call, so that
    /// Return is `resultsSentElsewhere`
    /// (`peer_call_orchestration.markYourselfCallForException`).
    mark_yourself_call: *const fn (*PeerType, protocol.Call) void,
) !void {
    switch (resolved) {
        .exported => |cap| {
            try handle_exported(peer, call, inbound_caps, cap.id);
        },
        .imported, .promised => {
            const mode = forwardModeForResolved(resolved, call.send_results_to.tag);
            forward_resolved_call(peer, call, inbound_caps, resolved, mode) catch |err| {
                mark_yourself_call(peer, call);
                try send_return_exception(peer, call.question_id, @errorName(err));
            };
        },
        .none => {
            mark_yourself_call(peer, call);
            try send_return_exception(peer, call.question_id, "promised answer missing");
        },
    }
}

pub const ForwardCallPlan = struct {
    send_results_to: protocol.SendResultsToTag,
    send_results_to_third_party_payload: ?[]u8 = null,
};

pub fn buildForwardCallPlan(
    comptime PeerType: type,
    peer: *PeerType,
    mode: ForwardReturnMode,
    third_party: ?message.AnyPointerReader,
    capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
) !ForwardCallPlan {
    const destination = try peer_third_party.buildForwardedCallDestination(
        PeerType,
        peer,
        toControlMode(mode),
        third_party,
        capture_payload,
    );

    return .{
        .send_results_to = destination.sendResultsToTag(),
        .send_results_to_third_party_payload = destination.thirdPartyPayload(),
    };
}

/// Record the bookkeeping of a forwarded call when its question id is
/// allocated, BEFORE its Call is sent (`Peer.forwardResolvedCall`): on a
/// synchronous transport the forwarded Return, and the upstream caller's
/// Finish after it, can arrive inside that send.
///
/// Both the spec-canonical tail-call (`.sent_elsewhere`) and the W1
/// reflected-loopback translate mode (`.translate_to_caller`) hold the
/// forwarded outbound question OPEN until the upstream caller sends its Finish
/// for the original question: register the tail mapping so `handleFinish`
/// relays a single Finish to the forwarded question, and suppress the
/// forwarded question's own auto-finish so it is not finished twice (once on
/// its own return, once on the relayed upstream Finish). rpc.capnp
/// `Call.sendResultsTo.yourself` asks exactly this of `.sent_elsewhere`: the
/// callee keeps the forwarded call's answer for our `takeFromOtherQuestion`
/// until that Finish. The two modes differ only in what is returned on the
/// upstream question: `.sent_elsewhere` emits the eager
/// `takeFromOtherQuestion` redirect after the send; `.translate_to_caller`
/// emits nothing then and later translates the real results back (see
/// peer_forwarded_return_logic + onForwardedReturn). Routing the forwarded
/// Finish through the upstream Finish (rather than auto-finishing on the
/// forwarded return) also cancels the forwarded question cleanly when the
/// caller Finishes before the forwarded return arrives (ordering race).
///
/// `upstream_finished`: the caller's Finish of the upstream question came
/// before this forward. That happens when go-capnp's
/// `requireEarlyCancellationWorkaround` (the schema default) kept a call
/// queued past its Finish, and its target resolved later to a capability
/// on the same connection. No upstream Finish is left to relay, so neither
/// mode registers the tail mapping or suppresses the auto-finish: the
/// forwarded question gets its Finish once its own Return is relayed (the
/// auto-finish after `onForwardedReturn`). Held for an upstream Finish that
/// already came, it was never finished, and the tail mapping kept the
/// upstream id in use, so the caller's legal reuse of that id closed the
/// connection.
///
/// `forwarded_questions` holds a forward only while it owes the upstream
/// question its Return (the relay in `onForwardedReturn`). A tail call
/// (`.sent_elsewhere`) owes none: the upstream question gets
/// `takeFromOtherQuestion` right after the send. So a tail call is not
/// recorded there. Before, it stayed there until the forwarded Return, and
/// the upstream id counted as in use after its Return and Finish: the
/// caller's legal reuse of the id closed the connection.
///
/// On failure nothing stays registered. A send that fails after this undoes
/// it with `unregisterForwardedQuestion`.
pub fn registerForwardedQuestion(
    comptime QuestionType: type,
    mode: ForwardReturnMode,
    upstream_finished: bool,
    upstream_question_id: u32,
    forwarded_question_id: u32,
    forwarded_questions: *std.AutoHashMap(u32, u32),
    forwarded_tail_questions: *std.AutoHashMap(u32, u32),
    questions: *std.AutoHashMap(u32, QuestionType),
) !void {
    const relays_return = mode != .sent_elsewhere;
    if (relays_return) try forwarded_questions.put(forwarded_question_id, upstream_question_id);
    errdefer if (relays_return) {
        _ = forwarded_questions.remove(forwarded_question_id);
    };

    if (upstream_finished) return;
    if (mode == .sent_elsewhere or mode == .translate_to_caller) {
        try forwarded_tail_questions.put(upstream_question_id, forwarded_question_id);
        if (questions.getEntry(forwarded_question_id)) |question| {
            question.value_ptr.suppress_auto_finish = true;
        }
    }
}

/// Undo `registerForwardedQuestion` for a forwarded call whose Call was not
/// sent.
pub fn unregisterForwardedQuestion(
    upstream_question_id: u32,
    forwarded_question_id: u32,
    forwarded_questions: *std.AutoHashMap(u32, u32),
    forwarded_tail_questions: *std.AutoHashMap(u32, u32),
) void {
    _ = forwarded_questions.remove(forwarded_question_id);
    if (forwarded_tail_questions.get(upstream_question_id)) |tail| {
        if (tail == forwarded_question_id) _ = forwarded_tail_questions.remove(upstream_question_id);
    }
}

pub fn lookupForwardedQuestionForPeer(comptime PeerType: type, peer: *PeerType, local_question_id: u32) ?u32 {
    return peer.forwarded_questions.get(local_question_id);
}

pub fn lookupForwardedQuestionForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) ?u32 {
    return struct {
        fn call(peer: *PeerType, local_question_id: u32) ?u32 {
            return lookupForwardedQuestionForPeer(PeerType, peer, local_question_id);
        }
    }.call;
}

pub fn removeForwardedQuestionForPeer(comptime PeerType: type, peer: *PeerType, local_question_id: u32) void {
    _ = peer.forwarded_questions.remove(local_question_id);
}

pub fn takeForwardedTailQuestionForPeer(comptime PeerType: type, peer: *PeerType, question_id: u32) ?u32 {
    if (peer.forwarded_tail_questions.fetchRemove(question_id)) |tail| {
        return tail.value;
    }
    return null;
}

pub fn takeForwardedTailQuestionForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) ?u32 {
    return struct {
        fn call(peer: *PeerType, question_id: u32) ?u32 {
            return takeForwardedTailQuestionForPeer(PeerType, peer, question_id);
        }
    }.call;
}

pub fn removeSendResultsToYourselfForPeer(comptime PeerType: type, peer: *PeerType, answer_id: u32) void {
    _ = peer.send_results_to_yourself.remove(answer_id);
}

pub fn removeSendResultsToYourselfForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) void {
    return struct {
        fn call(peer: *PeerType, answer_id: u32) void {
            removeSendResultsToYourselfForPeer(PeerType, peer, answer_id);
        }
    }.call;
}

pub fn forwardResolvedCallForPeerFn(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    comptime forward_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap, ForwardReturnMode) anyerror!void,
) *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap, ForwardResolvedMode) anyerror!void {
    return struct {
        fn call(
            peer: *PeerType,
            call_msg: protocol.Call,
            inbound_caps: *const InboundCapsType,
            resolved: cap_table.ResolvedCap,
            mode: ForwardResolvedMode,
        ) anyerror!void {
            try forward_resolved_call(
                peer,
                call_msg,
                inbound_caps,
                resolved,
                fromResolvedMode(mode),
            );
        }
    }.call;
}

test "peer_forward_orchestration buildForwardCallPlan maps modes to sendResultsTo tags" {
    const State = struct {
        capture_calls: usize = 0,
        payload_to_return: ?[]u8 = null,
    };

    const Hooks = struct {
        fn capture(state: *State, ptr: ?message.AnyPointerReader) !?[]u8 {
            _ = ptr;
            state.capture_calls += 1;
            return state.payload_to_return;
        }
    };

    {
        var state = State{};
        const plan = try buildForwardCallPlan(
            State,
            &state,
            .translate_to_caller,
            null,
            Hooks.capture,
        );
        try std.testing.expectEqual(protocol.SendResultsToTag.caller, plan.send_results_to);
        try std.testing.expectEqual(@as(?[]u8, null), plan.send_results_to_third_party_payload);
        try std.testing.expectEqual(@as(usize, 0), state.capture_calls);
    }

    {
        var state = State{};
        const plan = try buildForwardCallPlan(
            State,
            &state,
            .sent_elsewhere,
            null,
            Hooks.capture,
        );
        try std.testing.expectEqual(protocol.SendResultsToTag.yourself, plan.send_results_to);
        try std.testing.expectEqual(@as(?[]u8, null), plan.send_results_to_third_party_payload);
        try std.testing.expectEqual(@as(usize, 0), state.capture_calls);
    }

    {
        var state = State{};
        const plan = try buildForwardCallPlan(
            State,
            &state,
            .propagate_results_sent_elsewhere,
            null,
            Hooks.capture,
        );
        try std.testing.expectEqual(protocol.SendResultsToTag.yourself, plan.send_results_to);
        try std.testing.expectEqual(@as(?[]u8, null), plan.send_results_to_third_party_payload);
        try std.testing.expectEqual(@as(usize, 0), state.capture_calls);
    }

    {
        // `capture_payload` returns an OWNED, mutable payload (`?[]u8`), so the
        // fake hook needs a mutable backing buffer — a string literal is a
        // `*const [N:0]u8` and will not coerce to `[]u8`. A stack array is
        // enough: `buildForwardCallPlan` only threads the slice into the plan,
        // it never writes to or frees it.
        var payload_buf = "third-party-destination".*;
        var state = State{ .payload_to_return = &payload_buf };
        const plan = try buildForwardCallPlan(
            State,
            &state,
            .propagate_accept_from_third_party,
            null,
            Hooks.capture,
        );
        try std.testing.expectEqual(protocol.SendResultsToTag.thirdParty, plan.send_results_to);
        try std.testing.expectEqualStrings(
            "third-party-destination",
            plan.send_results_to_third_party_payload orelse return error.MissingThirdPartyPayload,
        );
        try std.testing.expectEqual(@as(usize, 1), state.capture_calls);
    }
}

test "peer_forward_orchestration registerForwardedQuestion records forwarding and tail state" {
    const Question = struct {
        suppress_auto_finish: bool = false,
    };

    var forwarded_questions = std.AutoHashMap(u32, u32).init(std.testing.allocator);
    defer forwarded_questions.deinit();

    var forwarded_tail_questions = std.AutoHashMap(u32, u32).init(std.testing.allocator);
    defer forwarded_tail_questions.deinit();

    var questions = std.AutoHashMap(u32, Question).init(std.testing.allocator);
    defer questions.deinit();

    // A non-tail mode (propagate-results-sent-elsewhere): records only the
    // forwarded_questions mapping, no tail state.
    try questions.put(20, .{});
    try registerForwardedQuestion(
        Question,
        .propagate_results_sent_elsewhere,
        false,
        10,
        20,
        &forwarded_questions,
        &forwarded_tail_questions,
        &questions,
    );

    try std.testing.expectEqual(@as(u32, 10), forwarded_questions.get(20).?);
    try std.testing.expectEqual(@as(usize, 0), forwarded_tail_questions.count());
    try std.testing.expect(!questions.get(20).?.suppress_auto_finish);

    // The spec-canonical tail-call mode: tail mapping + suppressed
    // auto-finish. It relays no Return, so it has no forwarded_questions
    // mapping.
    try questions.put(21, .{});
    try registerForwardedQuestion(
        Question,
        .sent_elsewhere,
        false,
        11,
        21,
        &forwarded_questions,
        &forwarded_tail_questions,
        &questions,
    );

    try std.testing.expect(!forwarded_questions.contains(21));
    try std.testing.expectEqual(@as(u32, 21), forwarded_tail_questions.get(11).?);
    try std.testing.expect(questions.get(21).?.suppress_auto_finish);

    // W1 reflected-loopback translate mode: tail mapping + suppressed auto-finish
    // (so the forwarded question is finished exactly once, via the relayed
    // upstream Finish).
    try questions.put(22, .{});
    try registerForwardedQuestion(
        Question,
        .translate_to_caller,
        false,
        12,
        22,
        &forwarded_questions,
        &forwarded_tail_questions,
        &questions,
    );

    try std.testing.expectEqual(@as(u32, 12), forwarded_questions.get(22).?);
    try std.testing.expectEqual(@as(u32, 22), forwarded_tail_questions.get(12).?);
    try std.testing.expect(questions.get(22).?.suppress_auto_finish);

    // An upstream question whose Finish already came: neither holding mode
    // registers the tail mapping or suppresses the auto-finish, so the
    // forwarded question is finished on its own Return.
    for ([_]ForwardReturnMode{ .sent_elsewhere, .translate_to_caller }, [_]u32{ 23, 24 }) |mode, forwarded| {
        try questions.put(forwarded, .{});
        try registerForwardedQuestion(
            Question,
            mode,
            true,
            forwarded - 10,
            forwarded,
            &forwarded_questions,
            &forwarded_tail_questions,
            &questions,
        );
        try std.testing.expectEqual(mode != .sent_elsewhere, forwarded_questions.contains(forwarded));
        try std.testing.expect(!forwarded_tail_questions.contains(forwarded - 10));
        try std.testing.expect(!questions.get(forwarded).?.suppress_auto_finish);
    }

    // A send that failed: the registration is undone, and only its own.
    unregisterForwardedQuestion(11, 21, &forwarded_questions, &forwarded_tail_questions);
    try std.testing.expect(!forwarded_tail_questions.contains(11));
    try std.testing.expectEqual(@as(u32, 12), forwarded_questions.get(22).?);
    try std.testing.expectEqual(@as(u32, 22), forwarded_tail_questions.get(12).?);
    unregisterForwardedQuestion(12, 99, &forwarded_questions, &forwarded_tail_questions);
    try std.testing.expectEqual(@as(u32, 22), forwarded_tail_questions.get(12).?);
}

test "peer_forward_orchestration peer-map helpers lookup/remove/take/remove-send-results" {
    const FakePeer = struct {
        forwarded_questions: std.AutoHashMap(u32, u32),
        forwarded_tail_questions: std.AutoHashMap(u32, u32),
        send_results_to_yourself: std.AutoHashMap(u32, void),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .forwarded_questions = std.AutoHashMap(u32, u32).init(allocator),
                .forwarded_tail_questions = std.AutoHashMap(u32, u32).init(allocator),
                .send_results_to_yourself = std.AutoHashMap(u32, void).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.forwarded_questions.deinit();
            self.forwarded_tail_questions.deinit();
            self.send_results_to_yourself.deinit();
        }
    };

    var peer = FakePeer.init(std.testing.allocator);
    defer peer.deinit();

    try peer.forwarded_questions.put(30, 300);
    try peer.forwarded_tail_questions.put(40, 400);
    try peer.send_results_to_yourself.put(50, {});

    try std.testing.expectEqual(@as(?u32, 300), lookupForwardedQuestionForPeer(FakePeer, &peer, 30));

    removeForwardedQuestionForPeer(FakePeer, &peer, 30);
    try std.testing.expectEqual(@as(?u32, null), lookupForwardedQuestionForPeer(FakePeer, &peer, 30));

    const tail = takeForwardedTailQuestionForPeer(FakePeer, &peer, 40) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 400), tail);
    try std.testing.expectEqual(@as(?u32, null), takeForwardedTailQuestionForPeer(FakePeer, &peer, 40));

    removeSendResultsToYourselfForPeer(FakePeer, &peer, 50);
    try std.testing.expectEqual(@as(usize, 0), peer.send_results_to_yourself.count());
}

test "peer_forward_orchestration forwardResolvedCallForPeerFn maps control mode and forwards call" {
    const InboundCaps = struct {};
    const State = struct {
        calls: usize = 0,
        last_mode: ?ForwardReturnMode = null,

        fn forwardResolvedCall(
            self: *@This(),
            call_msg: protocol.Call,
            inbound_caps: *const InboundCaps,
            resolved: cap_table.ResolvedCap,
            mode: ForwardReturnMode,
        ) !void {
            _ = call_msg;
            _ = inbound_caps;
            _ = resolved;
            self.calls += 1;
            self.last_mode = mode;
        }
    };

    const call_msg = protocol.Call{
        .question_id = 5,
        .target = .{
            .tag = .importedCap,
            .imported_cap = 1,
            .promised_answer = null,
        },
        .interface_id = 0,
        .method_id = 0,
        .params = .{
            .content = undefined,
            .cap_table = null,
        },
        .send_results_to = .{
            .tag = .caller,
            .third_party = null,
        },
        // `protocol.Call` gained the three spec flag bits; none of them are
        // defaulted. This test only exercises the mode mapping performed by
        // `forwardResolvedCallForPeerFn` — the call message is passed through
        // untouched (the fake sink discards it) — so the flags stay at the
        // neutral "plain caller call, no tail permission, ordinary pipelining"
        // values, matching the `.caller` `send_results_to` above.
        .allow_third_party_tail = false,
        .no_promise_pipelining = false,
        .only_promise_pipeline = false,
    };
    const inbound = InboundCaps{};

    var state = State{};
    const forward = forwardResolvedCallForPeerFn(
        State,
        InboundCaps,
        State.forwardResolvedCall,
    );

    try forward(&state, call_msg, &inbound, .none, .sent_elsewhere);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(ForwardReturnMode.sent_elsewhere, state.last_mode.?);

    try forward(&state, call_msg, &inbound, .none, .propagate_results_sent_elsewhere);
    try std.testing.expectEqual(@as(usize, 2), state.calls);
    try std.testing.expectEqual(ForwardReturnMode.propagate_results_sent_elsewhere, state.last_mode.?);

    try forward(&state, call_msg, &inbound, .none, .propagate_accept_from_third_party);
    try std.testing.expectEqual(@as(usize, 3), state.calls);
    try std.testing.expectEqual(ForwardReturnMode.propagate_accept_from_third_party, state.last_mode.?);
}
