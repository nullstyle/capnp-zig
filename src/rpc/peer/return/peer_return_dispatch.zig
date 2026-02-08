const std = @import("std");
const message = @import("../../../message.zig");
const protocol = @import("../../protocol.zig");
const peer_return_frames = @import("peer_return_frames.zig");

pub fn takeAdoptedAnswerOriginal(
    adopted_answers: *std.AutoHashMap(u32, u32),
    answer_id: u32,
) ?u32 {
    if (adopted_answers.fetchRemove(answer_id)) |original| {
        return original.value;
    }
    return null;
}

pub fn takeAdoptedAnswerOriginalForPeer(comptime PeerType: type, peer: *PeerType, answer_id: u32) ?u32 {
    return takeAdoptedAnswerOriginal(&peer.adopted_third_party_answers, answer_id);
}

pub fn takeAdoptedAnswerOriginalForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) ?u32 {
    return struct {
        fn call(peer: *PeerType, answer_id: u32) ?u32 {
            return takeAdoptedAnswerOriginalForPeer(PeerType, peer, answer_id);
        }
    }.call;
}

pub fn reportNonfatalErrorForPeer(comptime PeerType: type, peer: *PeerType, err: anyerror) void {
    if (peer.on_error) |cb| cb(peer, err);
}

pub fn reportNonfatalErrorForPeerFn(comptime PeerType: type) *const fn (*PeerType, anyerror) void {
    return struct {
        fn call(peer: *PeerType, err: anyerror) void {
            reportNonfatalErrorForPeer(PeerType, peer, err);
        }
    }.call;
}

pub fn invokeQuestionReturnForPeer(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime InboundCapsType: type,
    question: QuestionType,
    peer: *PeerType,
    ret: protocol.Return,
    inbound_caps: *const InboundCapsType,
) anyerror!void {
    try question.on_return(question.ctx, peer, ret, inbound_caps);
}

pub fn invokeQuestionReturnForPeerFn(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime InboundCapsType: type,
) *const fn (QuestionType, *PeerType, protocol.Return, *const InboundCapsType) anyerror!void {
    return struct {
        fn call(
            question: QuestionType,
            peer: *PeerType,
            ret: protocol.Return,
            inbound_caps: *const InboundCapsType,
        ) anyerror!void {
            try invokeQuestionReturnForPeer(
                PeerType,
                QuestionType,
                InboundCapsType,
                question,
                peer,
                ret,
                inbound_caps,
            );
        }
    }.call;
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

pub fn dispatchQuestionReturnForPeer(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    question: QuestionType,
    ret: protocol.Return,
    inbound_caps: *const InboundCapsType,
) void {
    dispatchQuestionReturn(
        PeerType,
        QuestionType,
        InboundCapsType,
        peer,
        question,
        ret,
        inbound_caps,
        invokeQuestionReturnForPeerFn(PeerType, QuestionType, InboundCapsType),
        reportNonfatalErrorForPeerFn(PeerType),
    );
}

pub fn dispatchQuestionReturnForPeerFn(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime InboundCapsType: type,
) *const fn (*PeerType, QuestionType, protocol.Return, *const InboundCapsType) void {
    return struct {
        fn call(
            peer: *PeerType,
            question: QuestionType,
            ret: protocol.Return,
            inbound_caps: *const InboundCapsType,
        ) void {
            dispatchQuestionReturnForPeer(
                PeerType,
                QuestionType,
                InboundCapsType,
                peer,
                question,
                ret,
                inbound_caps,
            );
        }
    }.call;
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

pub fn releaseInboundCapsForPeerFn(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    comptime release_fn: *const fn (*PeerType, *InboundCapsType) anyerror!void,
) *const fn (*PeerType, *const InboundCapsType) anyerror!void {
    return struct {
        fn call(peer: *PeerType, inbound_caps: *const InboundCapsType) anyerror!void {
            try releaseInboundCaps(
                PeerType,
                InboundCapsType,
                peer,
                inbound_caps,
                release_fn,
            );
        }
    }.call;
}

pub fn maybeSendAutoFinish(
    comptime PeerType: type,
    comptime QuestionType: type,
    peer: *PeerType,
    question: QuestionType,
    answer_id: u32,
    no_finish_needed: bool,
    send_finish: *const fn (*PeerType, u32, bool) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
) void {
    if (!question.is_loopback and !question.suppress_auto_finish and !no_finish_needed) {
        send_finish(peer, answer_id, false) catch |err| {
            report_nonfatal_error(peer, err);
        };
    }
}

pub fn maybeSendAutoFinishForPeerFn(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime send_finish: *const fn (*PeerType, u32, bool) anyerror!void,
) *const fn (*PeerType, QuestionType, u32, bool) void {
    return struct {
        fn call(peer: *PeerType, question: QuestionType, answer_id: u32, no_finish_needed: bool) void {
            maybeSendAutoFinish(
                PeerType,
                QuestionType,
                peer,
                question,
                answer_id,
                no_finish_needed,
                send_finish,
                reportNonfatalErrorForPeerFn(PeerType),
            );
        }
    }.call;
}

pub fn sendReturnTagForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
    tag: protocol.ReturnTag,
    clear_send_results_routing: *const fn (*PeerType, u32) void,
    send_return_frame_with_loopback: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    clear_send_results_routing(peer, answer_id);
    const bytes = try peer_return_frames.buildReturnTagFrame(peer.allocator, answer_id, tag);
    defer peer.allocator.free(bytes);
    try send_return_frame_with_loopback(peer, answer_id, bytes);
}

pub fn sendReturnExceptionForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
    reason: []const u8,
    clear_send_results_routing: *const fn (*PeerType, u32) void,
    send_return_frame_with_loopback: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    clear_send_results_routing(peer, answer_id);
    const bytes = try peer_return_frames.buildReturnExceptionFrame(peer.allocator, answer_id, reason);
    defer peer.allocator.free(bytes);
    try send_return_frame_with_loopback(peer, answer_id, bytes);
}

pub fn sendReturnTakeFromOtherQuestionForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
    other_question_id: u32,
    clear_send_results_routing: *const fn (*PeerType, u32) void,
    send_return_frame_with_loopback: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    clear_send_results_routing(peer, answer_id);
    const bytes = try peer_return_frames.buildReturnTakeFromOtherQuestionFrame(
        peer.allocator,
        answer_id,
        other_question_id,
    );
    defer peer.allocator.free(bytes);
    try send_return_frame_with_loopback(peer, answer_id, bytes);
}

pub fn sendReturnAcceptFromThirdPartyForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
    await_payload: ?[]const u8,
    clear_send_results_routing: *const fn (*PeerType, u32) void,
    send_return_frame_with_loopback: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    clear_send_results_routing(peer, answer_id);
    const bytes = try peer_return_frames.buildReturnAcceptFromThirdPartyFrame(
        peer.allocator,
        answer_id,
        await_payload,
    );
    defer peer.allocator.free(bytes);
    try send_return_frame_with_loopback(peer, answer_id, bytes);
}

test "peer_return_dispatch sendReturnTagForPeer clears routing and sends encoded tag" {
    const State = struct {
        allocator: std.mem.Allocator,
        clear_calls: usize = 0,
        send_calls: usize = 0,
        clear_answer_id: u32 = 0,
        sent_answer_id: u32 = 0,
        sent_tag: protocol.ReturnTag = .results,
    };

    const Hooks = struct {
        fn clear(state: *State, answer_id: u32) void {
            state.clear_calls += 1;
            state.clear_answer_id = answer_id;
        }

        fn send(state: *State, answer_id: u32, frame: []const u8) !void {
            state.send_calls += 1;
            state.sent_answer_id = answer_id;
            var decoded = try protocol.DecodedMessage.init(state.allocator, frame);
            defer decoded.deinit();
            const ret = try decoded.asReturn();
            state.sent_tag = ret.tag;
            try std.testing.expectEqual(@as(usize, 1), state.clear_calls);
        }
    };

    var state = State{
        .allocator = std.testing.allocator,
    };

    try sendReturnTagForPeer(
        State,
        &state,
        44,
        .results_sent_elsewhere,
        Hooks.clear,
        Hooks.send,
    );

    try std.testing.expectEqual(@as(usize, 1), state.clear_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_calls);
    try std.testing.expectEqual(@as(u32, 44), state.clear_answer_id);
    try std.testing.expectEqual(@as(u32, 44), state.sent_answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results_sent_elsewhere, state.sent_tag);
}

test "peer_return_dispatch sendReturnAcceptFromThirdPartyForPeer sends await payload" {
    const State = struct {
        allocator: std.mem.Allocator,
        clear_calls: usize = 0,
        send_calls: usize = 0,
        await_text: []const u8 = "",
    };

    const Hooks = struct {
        fn clear(state: *State, answer_id: u32) void {
            _ = answer_id;
            state.clear_calls += 1;
        }

        fn send(state: *State, answer_id: u32, frame: []const u8) !void {
            _ = answer_id;
            state.send_calls += 1;
            var decoded = try protocol.DecodedMessage.init(state.allocator, frame);
            defer decoded.deinit();
            const ret = try decoded.asReturn();
            try std.testing.expectEqual(protocol.ReturnTag.accept_from_third_party, ret.tag);
            const await_ptr = ret.accept_from_third_party orelse return error.MissingThirdPartyPayload;
            state.await_text = try await_ptr.getText();
        }
    };

    var await_builder = message.MessageBuilder.init(std.testing.allocator);
    defer await_builder.deinit();
    const await_root = try await_builder.initRootAnyPointer();
    try await_root.setText("loopback-await");
    const await_payload = try await_builder.toBytes();
    defer std.testing.allocator.free(await_payload);

    var state = State{
        .allocator = std.testing.allocator,
    };
    try sendReturnAcceptFromThirdPartyForPeer(
        State,
        &state,
        45,
        await_payload,
        Hooks.clear,
        Hooks.send,
    );

    try std.testing.expectEqual(@as(usize, 1), state.clear_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_calls);
    try std.testing.expectEqualStrings("loopback-await", state.await_text);
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

test "peer_return_dispatch dispatchQuestionReturnForPeerFn calls question callback and reports error" {
    const InboundCaps = struct { marker: u32 };
    const PeerState = struct {
        callback_calls: usize = 0,
        report_calls: usize = 0,
        saw_marker: u32 = 0,
        on_error: ?*const fn (peer: *@This(), err: anyerror) void = onError,

        fn onError(peer: *@This(), err: anyerror) void {
            std.testing.expectEqual(error.TestExpectedError, err) catch unreachable;
            peer.report_calls += 1;
        }
    };
    const Question = struct {
        ctx: *anyopaque,
        on_return: *const fn (*anyopaque, *PeerState, protocol.Return, *const InboundCaps) anyerror!void,
    };
    const Hooks = struct {
        fn onReturn(
            ctx: *anyopaque,
            peer: *PeerState,
            ret: protocol.Return,
            inbound_caps: *const InboundCaps,
        ) anyerror!void {
            _ = ret;
            const marker: *u32 = @ptrCast(@alignCast(ctx));
            peer.callback_calls += 1;
            peer.saw_marker = marker.* + inbound_caps.marker;
            return error.TestExpectedError;
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

    var marker: u32 = 7;
    var peer = PeerState{};
    const question = Question{
        .ctx = &marker,
        .on_return = Hooks.onReturn,
    };
    const inbound = InboundCaps{ .marker = 5 };

    const dispatch = dispatchQuestionReturnForPeerFn(PeerState, Question, InboundCaps);
    dispatch(&peer, question, ret, &inbound);

    try std.testing.expectEqual(@as(usize, 1), peer.callback_calls);
    try std.testing.expectEqual(@as(usize, 1), peer.report_calls);
    try std.testing.expectEqual(@as(u32, 12), peer.saw_marker);
}

test "peer_return_dispatch releaseInboundCapsForPeerFn passes mutable copy to peer method" {
    const InboundCaps = struct { value: u32 };
    const PeerState = struct {
        release_calls: usize = 0,
        observed_value: u32 = 0,

        fn releaseInboundCaps(self: *@This(), inbound_caps: *InboundCaps) !void {
            self.release_calls += 1;
            self.observed_value = inbound_caps.value;
            inbound_caps.value = 999;
        }
    };

    var peer = PeerState{};
    const inbound = InboundCaps{ .value = 123 };
    const release = releaseInboundCapsForPeerFn(PeerState, InboundCaps, PeerState.releaseInboundCaps);
    try release(&peer, &inbound);

    try std.testing.expectEqual(@as(usize, 1), peer.release_calls);
    try std.testing.expectEqual(@as(u32, 123), peer.observed_value);
    try std.testing.expectEqual(@as(u32, 123), inbound.value);
}

test "peer_return_dispatch maybeSendAutoFinishForPeerFn routes send-finish errors to on_error" {
    const Question = struct {
        is_loopback: bool = false,
        suppress_auto_finish: bool = false,
    };
    const PeerState = struct {
        finish_calls: usize = 0,
        error_calls: usize = 0,
        fail_finish: bool = false,
        on_error: ?*const fn (peer: *@This(), err: anyerror) void = onError,

        fn onError(peer: *@This(), err: anyerror) void {
            std.testing.expectEqual(error.TestExpectedError, err) catch unreachable;
            peer.error_calls += 1;
        }

        fn sendFinish(self: *@This(), answer_id: u32, release_result_caps: bool) !void {
            _ = answer_id;
            _ = release_result_caps;
            self.finish_calls += 1;
            if (self.fail_finish) return error.TestExpectedError;
        }
    };

    var peer = PeerState{};
    const maybe_finish = maybeSendAutoFinishForPeerFn(PeerState, Question, PeerState.sendFinish);

    maybe_finish(&peer, .{}, 10, false);
    try std.testing.expectEqual(@as(usize, 1), peer.finish_calls);
    try std.testing.expectEqual(@as(usize, 0), peer.error_calls);

    peer.fail_finish = true;
    maybe_finish(&peer, .{}, 11, false);
    try std.testing.expectEqual(@as(usize, 2), peer.finish_calls);
    try std.testing.expectEqual(@as(usize, 1), peer.error_calls);

    maybe_finish(&peer, .{ .is_loopback = true }, 12, false);
    maybe_finish(&peer, .{ .suppress_auto_finish = true }, 13, false);
    maybe_finish(&peer, .{}, 14, true);
    try std.testing.expectEqual(@as(usize, 2), peer.finish_calls);
}
