const std = @import("std");
const protocol = @import("protocol.zig");

pub fn takeAdoptedAnswerOriginal(
    adopted_answers: *std.AutoHashMap(u32, u32),
    answer_id: u32,
) ?u32 {
    if (adopted_answers.fetchRemove(answer_id)) |original| {
        return original.value;
    }
    return null;
}

pub fn dispatchQuestionReturn(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    question: QuestionType,
    ret: protocol.Return,
    inbound_caps: *const InboundCapsType,
    question_return_fn: *const fn (QuestionType, *PeerType, protocol.Return, *const InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
) void {
    question_return_fn(question, peer, ret, inbound_caps) catch |err| {
        report_nonfatal_error(peer, err);
    };
}

pub fn releaseInboundCaps(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    inbound_caps: *const InboundCapsType,
    release_fn: *const fn (*PeerType, *InboundCapsType) anyerror!void,
) !void {
    var mutable_caps = inbound_caps.*;
    try release_fn(peer, &mutable_caps);
}

test "peer_return_dispatch takeAdoptedAnswerOriginal removes and returns mapped answer" {
    var adopted = std.AutoHashMap(u32, u32).init(std.testing.allocator);
    defer adopted.deinit();
    try adopted.put(900, 42);

    const original = takeAdoptedAnswerOriginal(&adopted, 900) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 42), original);
    try std.testing.expect(!adopted.contains(900));
    try std.testing.expect(takeAdoptedAnswerOriginal(&adopted, 900) == null);
}

test "peer_return_dispatch dispatchQuestionReturn reports callback failures" {
    const Question = struct { marker: u32 };
    const InboundCaps = struct { marker: u32 };
    const State = struct {
        callback_calls: usize = 0,
        report_calls: usize = 0,
        saw_marker: u32 = 0,
    };

    const Hooks = struct {
        fn onReturn(
            question: Question,
            state: *State,
            ret: protocol.Return,
            inbound_caps: *const InboundCaps,
        ) !void {
            _ = ret;
            state.callback_calls += 1;
            state.saw_marker = question.marker + inbound_caps.marker;
            return error.TestExpectedError;
        }

        fn report(state: *State, err: anyerror) void {
            std.testing.expectEqual(error.TestExpectedError, err) catch unreachable;
            state.report_calls += 1;
        }
    };

    const ret = protocol.Return{
        .answer_id = 1,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .canceled,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };

    var state = State{};
    const question = Question{ .marker = 7 };
    const inbound = InboundCaps{ .marker = 5 };
    dispatchQuestionReturn(
        State,
        Question,
        InboundCaps,
        &state,
        question,
        ret,
        &inbound,
        Hooks.onReturn,
        Hooks.report,
    );

    try std.testing.expectEqual(@as(usize, 1), state.callback_calls);
    try std.testing.expectEqual(@as(usize, 1), state.report_calls);
    try std.testing.expectEqual(@as(u32, 12), state.saw_marker);
}

test "peer_return_dispatch releaseInboundCaps passes mutable copy to release callback" {
    const InboundCaps = struct { value: u32 };
    const State = struct {
        release_calls: usize = 0,
        observed_value: u32 = 0,
    };
    const Hooks = struct {
        fn release(state: *State, inbound_caps: *InboundCaps) !void {
            state.release_calls += 1;
            state.observed_value = inbound_caps.value;
            inbound_caps.value = 999;
        }
    };

    var state = State{};
    const inbound = InboundCaps{ .value = 123 };
    try releaseInboundCaps(
        State,
        InboundCaps,
        &state,
        &inbound,
        Hooks.release,
    );

    try std.testing.expectEqual(@as(usize, 1), state.release_calls);
    try std.testing.expectEqual(@as(u32, 123), state.observed_value);
    try std.testing.expectEqual(@as(u32, 123), inbound.value);
}
