const std = @import("std");
const cap_table = @import("../../../level0/cap_table.zig");
const message = @import("../../../../serialization/message.zig");
const peer_control = @import("../peer_control.zig");
const protocol = @import("../../../level0/protocol.zig");

pub const ForwardReturnMode = enum {
    translate_to_caller,
    sent_elsewhere,
    propagate_results_sent_elsewhere,
    propagate_accept_from_third_party,
};

pub fn toControlMode(mode: ForwardReturnMode) peer_control.ForwardedReturnMode {
    return switch (mode) {
        .translate_to_caller => .translate_to_caller,
        .sent_elsewhere => .sent_elsewhere,
        .propagate_results_sent_elsewhere => .propagate_results_sent_elsewhere,
        .propagate_accept_from_third_party => .propagate_accept_from_third_party,
    };
}

pub fn fromControlResolvedMode(mode: peer_control.ForwardResolvedMode) ForwardReturnMode {
    return switch (mode) {
        .sent_elsewhere => .sent_elsewhere,
        .propagate_results_sent_elsewhere => .propagate_results_sent_elsewhere,
        .propagate_accept_from_third_party => .propagate_accept_from_third_party,
    };
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
    const destination = try peer_control.buildForwardedCallDestination(
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

pub const ForwardResolvedCompletion = struct {
    send_take_from_other_question: bool = false,
};

pub fn finishForwardResolvedCall(
    comptime QuestionType: type,
    mode: ForwardReturnMode,
    upstream_question_id: u32,
    forwarded_question_id: u32,
    forwarded_questions: *std.AutoHashMap(u32, u32),
    forwarded_tail_questions: *std.AutoHashMap(u32, u32),
    questions: *std.AutoHashMap(u32, QuestionType),
) !ForwardResolvedCompletion {
    try forwarded_questions.put(forwarded_question_id, upstream_question_id);

    if (mode == .sent_elsewhere) {
        try forwarded_tail_questions.put(upstream_question_id, forwarded_question_id);
        if (questions.getEntry(forwarded_question_id)) |question| {
            question.value_ptr.suppress_auto_finish = true;
        }
        return .{ .send_take_from_other_question = true };
    }
    return .{};
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
) *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap, peer_control.ForwardResolvedMode) anyerror!void {
    return struct {
        fn call(
            peer: *PeerType,
            call_msg: protocol.Call,
            inbound_caps: *const InboundCapsType,
            resolved: cap_table.ResolvedCap,
            mode: peer_control.ForwardResolvedMode,
        ) anyerror!void {
            try forward_resolved_call(
                peer,
                call_msg,
                inbound_caps,
                resolved,
                fromControlResolvedMode(mode),
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
        var state = State{ .payload_to_return = "third-party-destination" };
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

test "peer_forward_orchestration finishForwardResolvedCall records forwarding and tail state" {
    const Question = struct {
        suppress_auto_finish: bool = false,
    };

    var forwarded_questions = std.AutoHashMap(u32, u32).init(std.testing.allocator);
    defer forwarded_questions.deinit();

    var forwarded_tail_questions = std.AutoHashMap(u32, u32).init(std.testing.allocator);
    defer forwarded_tail_questions.deinit();

    var questions = std.AutoHashMap(u32, Question).init(std.testing.allocator);
    defer questions.deinit();

    try questions.put(20, .{});
    const completion_normal = try finishForwardResolvedCall(
        Question,
        .translate_to_caller,
        10,
        20,
        &forwarded_questions,
        &forwarded_tail_questions,
        &questions,
    );

    try std.testing.expect(!completion_normal.send_take_from_other_question);
    try std.testing.expectEqual(@as(u32, 10), forwarded_questions.get(20).?);
    try std.testing.expectEqual(@as(usize, 0), forwarded_tail_questions.count());
    try std.testing.expect(!questions.get(20).?.suppress_auto_finish);

    try questions.put(21, .{});
    const completion_tail = try finishForwardResolvedCall(
        Question,
        .sent_elsewhere,
        11,
        21,
        &forwarded_questions,
        &forwarded_tail_questions,
        &questions,
    );

    try std.testing.expect(completion_tail.send_take_from_other_question);
    try std.testing.expectEqual(@as(u32, 11), forwarded_questions.get(21).?);
    try std.testing.expectEqual(@as(u32, 21), forwarded_tail_questions.get(11).?);
    try std.testing.expect(questions.get(21).?.suppress_auto_finish);
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
