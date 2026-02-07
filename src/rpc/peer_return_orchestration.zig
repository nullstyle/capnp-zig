const std = @import("std");
const message = @import("../message.zig");
const protocol = @import("protocol.zig");

pub fn handleReturn(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    frame: []const u8,
    ret: protocol.Return,
    fetch_remove_question: *const fn (*PeerType, u32) ?QuestionType,
    handle_missing_return_question: *const fn (*PeerType, []const u8, u32) anyerror!void,
    init_inbound_caps: *const fn (*PeerType, protocol.Return) anyerror!InboundCapsType,
    deinit_inbound_caps: *const fn (*InboundCapsType) void,
    handle_return_accept_from_third_party: *const fn (*PeerType, u32, QuestionType, ?message.AnyPointerReader, *const InboundCapsType) anyerror!void,
    maybe_send_auto_finish: *const fn (*PeerType, QuestionType, u32, bool) void,
    handle_return_regular: *const fn (*PeerType, QuestionType, protocol.Return, *const InboundCapsType) void,
) anyerror!void {
    const question = fetch_remove_question(peer, ret.answer_id) orelse {
        try handle_missing_return_question(peer, frame, ret.answer_id);
        return;
    };

    var inbound_caps = try init_inbound_caps(peer, ret);
    defer deinit_inbound_caps(&inbound_caps);

    if (ret.tag == .accept_from_third_party) {
        try handle_return_accept_from_third_party(
            peer,
            ret.answer_id,
            question,
            ret.accept_from_third_party,
            &inbound_caps,
        );
        maybe_send_auto_finish(peer, question, ret.answer_id, ret.no_finish_needed);
        return;
    }

    handle_return_regular(peer, question, ret, &inbound_caps);
}

test "peer_return_orchestration handles missing question via callback" {
    const Question = struct { marker: u32 };
    const State = struct {
        missing_calls: usize = 0,
        init_calls: usize = 0,
    };
    const Inbound = struct { state: *State };
    const Hooks = struct {
        fn fetchQuestion(state: *State, answer_id: u32) ?Question {
            _ = state;
            _ = answer_id;
            return null;
        }

        fn handleMissing(state: *State, frame: []const u8, answer_id: u32) !void {
            _ = frame;
            _ = answer_id;
            state.missing_calls += 1;
        }

        fn initInbound(state: *State, ret: protocol.Return) !Inbound {
            state.init_calls += 1;
            _ = ret;
            return error.TestUnexpectedResult;
        }

        fn deinitInbound(inbound: *Inbound) void {
            _ = inbound;
        }

        fn handleAccept(state: *State, answer_id: u32, question: Question, await_ptr: ?message.AnyPointerReader, inbound: *const Inbound) !void {
            _ = state;
            _ = answer_id;
            _ = question;
            _ = await_ptr;
            _ = inbound;
            return error.TestUnexpectedResult;
        }

        fn maybeFinish(state: *State, question: Question, answer_id: u32, no_finish_needed: bool) void {
            _ = state;
            _ = question;
            _ = answer_id;
            _ = no_finish_needed;
        }

        fn handleRegular(state: *State, question: Question, ret: protocol.Return, inbound: *const Inbound) void {
            _ = state;
            _ = question;
            _ = ret;
            _ = inbound;
            unreachable;
        }
    };

    var state = State{};
    const ret = protocol.Return{
        .answer_id = 3,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .canceled,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try handleReturn(
        State,
        Question,
        Inbound,
        &state,
        &.{},
        ret,
        Hooks.fetchQuestion,
        Hooks.handleMissing,
        Hooks.initInbound,
        Hooks.deinitInbound,
        Hooks.handleAccept,
        Hooks.maybeFinish,
        Hooks.handleRegular,
    );
    try std.testing.expectEqual(@as(usize, 1), state.missing_calls);
    try std.testing.expectEqual(@as(usize, 0), state.init_calls);
}

test "peer_return_orchestration regular path invokes regular handler and deinit" {
    const Question = struct { marker: u32 };
    const State = struct {
        fetch_calls: usize = 0,
        missing_calls: usize = 0,
        init_calls: usize = 0,
        deinit_calls: usize = 0,
        accept_calls: usize = 0,
        finish_calls: usize = 0,
        regular_calls: usize = 0,
    };
    const Inbound = struct { state: *State };
    const Hooks = struct {
        fn fetchQuestion(state: *State, answer_id: u32) ?Question {
            _ = answer_id;
            state.fetch_calls += 1;
            return .{ .marker = 9 };
        }

        fn handleMissing(state: *State, frame: []const u8, answer_id: u32) !void {
            state.missing_calls += 1;
            _ = frame;
            _ = answer_id;
            return error.TestUnexpectedResult;
        }

        fn initInbound(state: *State, ret: protocol.Return) !Inbound {
            _ = ret;
            state.init_calls += 1;
            return .{ .state = state };
        }

        fn deinitInbound(inbound: *Inbound) void {
            inbound.state.deinit_calls += 1;
        }

        fn handleAccept(state: *State, answer_id: u32, question: Question, await_ptr: ?message.AnyPointerReader, inbound: *const Inbound) !void {
            _ = answer_id;
            _ = inbound;
            _ = await_ptr;
            _ = question;
            state.accept_calls += 1;
            return error.TestUnexpectedResult;
        }

        fn maybeFinish(state: *State, question: Question, answer_id: u32, no_finish_needed: bool) void {
            _ = question;
            _ = answer_id;
            _ = no_finish_needed;
            state.finish_calls += 1;
        }

        fn handleRegular(state: *State, question: Question, ret: protocol.Return, inbound: *const Inbound) void {
            _ = inbound;
            std.testing.expectEqual(@as(u32, 9), question.marker) catch unreachable;
            std.testing.expectEqual(protocol.ReturnTag.canceled, ret.tag) catch unreachable;
            std.testing.expectEqual(@as(u32, 3), ret.answer_id) catch unreachable;
            state.regular_calls += 1;
        }
    };

    var state = State{};
    const ret = protocol.Return{
        .answer_id = 3,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .canceled,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try handleReturn(
        State,
        Question,
        Inbound,
        &state,
        &.{},
        ret,
        Hooks.fetchQuestion,
        Hooks.handleMissing,
        Hooks.initInbound,
        Hooks.deinitInbound,
        Hooks.handleAccept,
        Hooks.maybeFinish,
        Hooks.handleRegular,
    );
    try std.testing.expectEqual(@as(usize, 1), state.fetch_calls);
    try std.testing.expectEqual(@as(usize, 0), state.missing_calls);
    try std.testing.expectEqual(@as(usize, 1), state.init_calls);
    try std.testing.expectEqual(@as(usize, 1), state.deinit_calls);
    try std.testing.expectEqual(@as(usize, 0), state.accept_calls);
    try std.testing.expectEqual(@as(usize, 0), state.finish_calls);
    try std.testing.expectEqual(@as(usize, 1), state.regular_calls);
}

test "peer_return_orchestration accept path invokes accept handler and maybe-finish" {
    const Question = struct { marker: u32 };
    const State = struct {
        fetch_calls: usize = 0,
        init_calls: usize = 0,
        deinit_calls: usize = 0,
        accept_calls: usize = 0,
        finish_calls: usize = 0,
        regular_calls: usize = 0,
    };
    const Inbound = struct { state: *State };
    const Hooks = struct {
        fn fetchQuestion(state: *State, answer_id: u32) ?Question {
            _ = answer_id;
            state.fetch_calls += 1;
            return .{ .marker = 9 };
        }

        fn handleMissing(state: *State, frame: []const u8, answer_id: u32) !void {
            _ = state;
            _ = frame;
            _ = answer_id;
            return error.TestUnexpectedResult;
        }

        fn initInbound(state: *State, ret: protocol.Return) !Inbound {
            _ = ret;
            state.init_calls += 1;
            return .{ .state = state };
        }

        fn deinitInbound(inbound: *Inbound) void {
            inbound.state.deinit_calls += 1;
        }

        fn handleAccept(state: *State, answer_id: u32, question: Question, await_ptr: ?message.AnyPointerReader, inbound: *const Inbound) !void {
            _ = await_ptr;
            _ = inbound;
            try std.testing.expectEqual(@as(u32, 3), answer_id);
            try std.testing.expectEqual(@as(u32, 9), question.marker);
            state.accept_calls += 1;
        }

        fn maybeFinish(state: *State, question: Question, answer_id: u32, no_finish_needed: bool) void {
            std.testing.expectEqual(@as(u32, 9), question.marker) catch unreachable;
            std.testing.expectEqual(@as(u32, 3), answer_id) catch unreachable;
            std.testing.expect(no_finish_needed) catch unreachable;
            state.finish_calls += 1;
        }

        fn handleRegular(state: *State, question: Question, ret: protocol.Return, inbound: *const Inbound) void {
            _ = question;
            _ = ret;
            _ = inbound;
            state.regular_calls += 1;
        }
    };

    var state = State{};
    const ret = protocol.Return{
        .answer_id = 3,
        .release_param_caps = false,
        .no_finish_needed = true,
        .tag = .accept_from_third_party,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try handleReturn(
        State,
        Question,
        Inbound,
        &state,
        &.{},
        ret,
        Hooks.fetchQuestion,
        Hooks.handleMissing,
        Hooks.initInbound,
        Hooks.deinitInbound,
        Hooks.handleAccept,
        Hooks.maybeFinish,
        Hooks.handleRegular,
    );
    try std.testing.expectEqual(@as(usize, 1), state.fetch_calls);
    try std.testing.expectEqual(@as(usize, 1), state.init_calls);
    try std.testing.expectEqual(@as(usize, 1), state.deinit_calls);
    try std.testing.expectEqual(@as(usize, 1), state.accept_calls);
    try std.testing.expectEqual(@as(usize, 1), state.finish_calls);
    try std.testing.expectEqual(@as(usize, 0), state.regular_calls);
}
