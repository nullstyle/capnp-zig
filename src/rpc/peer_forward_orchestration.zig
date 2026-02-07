const std = @import("std");
const message = @import("../message.zig");
const peer_control = @import("peer_control.zig");
const protocol = @import("protocol.zig");

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

pub fn finishForwardResolvedCall(
    comptime PeerType: type,
    peer: *PeerType,
    mode: ForwardReturnMode,
    upstream_question_id: u32,
    forwarded_question_id: u32,
    remember_forwarded_question: *const fn (*PeerType, u32, u32) anyerror!void,
    remember_forwarded_tail_question: *const fn (*PeerType, u32, u32) anyerror!void,
    suppress_auto_finish: *const fn (*PeerType, u32) void,
    send_take_from_other_question: *const fn (*PeerType, u32, u32) anyerror!void,
) !void {
    try remember_forwarded_question(peer, forwarded_question_id, upstream_question_id);

    if (mode == .sent_elsewhere) {
        try remember_forwarded_tail_question(peer, upstream_question_id, forwarded_question_id);
        suppress_auto_finish(peer, forwarded_question_id);
        try send_take_from_other_question(peer, upstream_question_id, forwarded_question_id);
    }
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
        try std.testing.expectEqual(protocol.SendResultsToTag.third_party, plan.send_results_to);
        try std.testing.expectEqualStrings(
            "third-party-destination",
            plan.send_results_to_third_party_payload orelse return error.MissingThirdPartyPayload,
        );
        try std.testing.expectEqual(@as(usize, 1), state.capture_calls);
    }
}

test "peer_forward_orchestration finishForwardResolvedCall records forwarding and tail state" {
    const State = struct {
        forwarded_question_calls: usize = 0,
        forwarded_question_local: u32 = 0,
        forwarded_question_upstream: u32 = 0,
        tail_calls: usize = 0,
        tail_upstream: u32 = 0,
        tail_forwarded: u32 = 0,
        suppress_calls: usize = 0,
        suppressed_question_id: u32 = 0,
        take_calls: usize = 0,
        take_answer_id: u32 = 0,
        take_other_id: u32 = 0,
    };

    const Hooks = struct {
        fn rememberForwardedQuestion(state: *State, local_question_id: u32, upstream_question_id: u32) !void {
            state.forwarded_question_calls += 1;
            state.forwarded_question_local = local_question_id;
            state.forwarded_question_upstream = upstream_question_id;
        }

        fn rememberTail(state: *State, upstream_question_id: u32, forwarded_question_id: u32) !void {
            state.tail_calls += 1;
            state.tail_upstream = upstream_question_id;
            state.tail_forwarded = forwarded_question_id;
        }

        fn suppress(state: *State, forwarded_question_id: u32) void {
            state.suppress_calls += 1;
            state.suppressed_question_id = forwarded_question_id;
        }

        fn sendTake(state: *State, answer_id: u32, other_question_id: u32) !void {
            state.take_calls += 1;
            state.take_answer_id = answer_id;
            state.take_other_id = other_question_id;
        }
    };

    {
        var state = State{};
        try finishForwardResolvedCall(
            State,
            &state,
            .translate_to_caller,
            10,
            20,
            Hooks.rememberForwardedQuestion,
            Hooks.rememberTail,
            Hooks.suppress,
            Hooks.sendTake,
        );

        try std.testing.expectEqual(@as(usize, 1), state.forwarded_question_calls);
        try std.testing.expectEqual(@as(u32, 20), state.forwarded_question_local);
        try std.testing.expectEqual(@as(u32, 10), state.forwarded_question_upstream);
        try std.testing.expectEqual(@as(usize, 0), state.tail_calls);
        try std.testing.expectEqual(@as(usize, 0), state.suppress_calls);
        try std.testing.expectEqual(@as(usize, 0), state.take_calls);
    }

    {
        var state = State{};
        try finishForwardResolvedCall(
            State,
            &state,
            .sent_elsewhere,
            11,
            21,
            Hooks.rememberForwardedQuestion,
            Hooks.rememberTail,
            Hooks.suppress,
            Hooks.sendTake,
        );

        try std.testing.expectEqual(@as(usize, 1), state.forwarded_question_calls);
        try std.testing.expectEqual(@as(usize, 1), state.tail_calls);
        try std.testing.expectEqual(@as(u32, 11), state.tail_upstream);
        try std.testing.expectEqual(@as(u32, 21), state.tail_forwarded);
        try std.testing.expectEqual(@as(usize, 1), state.suppress_calls);
        try std.testing.expectEqual(@as(u32, 21), state.suppressed_question_id);
        try std.testing.expectEqual(@as(usize, 1), state.take_calls);
        try std.testing.expectEqual(@as(u32, 11), state.take_answer_id);
        try std.testing.expectEqual(@as(u32, 21), state.take_other_id);
    }
}
