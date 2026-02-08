const std = @import("std");
const message = @import("../../../message.zig");
const protocol = @import("../../protocol.zig");
const peer_control = @import("../peer_control.zig");
const peer_return_dispatch = @import("peer_return_dispatch.zig");
const peer_third_party_returns = @import("../third_party/peer_third_party_returns.zig");

pub fn fetchRemoveQuestionForPeer(
    comptime PeerType: type,
    comptime QuestionType: type,
    peer: *PeerType,
    answer_id: u32,
) ?QuestionType {
    const entry = peer.questions.fetchRemove(answer_id) orelse return null;
    return entry.value;
}

pub fn fetchRemoveQuestionForPeerFn(
    comptime PeerType: type,
    comptime QuestionType: type,
) *const fn (*PeerType, u32) ?QuestionType {
    return struct {
        fn call(peer: *PeerType, answer_id: u32) ?QuestionType {
            return fetchRemoveQuestionForPeer(PeerType, QuestionType, peer, answer_id);
        }
    }.call;
}

pub fn initInboundCapsForPeer(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    ret: protocol.Return,
) !InboundCapsType {
    const cap_list = if (ret.tag == .results and ret.results != null) ret.results.?.cap_table else null;
    return InboundCapsType.init(peer.allocator, cap_list, &peer.caps);
}

pub fn initInboundCapsForPeerFn(
    comptime PeerType: type,
    comptime InboundCapsType: type,
) *const fn (*PeerType, protocol.Return) anyerror!InboundCapsType {
    return struct {
        fn call(peer: *PeerType, ret: protocol.Return) anyerror!InboundCapsType {
            return try initInboundCapsForPeer(PeerType, InboundCapsType, peer, ret);
        }
    }.call;
}

pub fn deinitInboundCapsForType(comptime InboundCapsType: type, inbound_caps: *InboundCapsType) void {
    inbound_caps.deinit();
}

pub fn deinitInboundCapsForTypeFn(comptime InboundCapsType: type) *const fn (*InboundCapsType) void {
    return struct {
        fn call(inbound_caps: *InboundCapsType) void {
            deinitInboundCapsForType(InboundCapsType, inbound_caps);
        }
    }.call;
}

pub fn handleMissingReturnQuestionForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    frame: []const u8,
    answer_id: u32,
) !void {
    try peer_control.handleMissingReturnQuestion(
        PeerType,
        peer,
        frame,
        answer_id,
        peer_control.isThirdPartyAnswerId,
        peer_third_party_returns.hasPendingReturnForPeerFn(PeerType),
        peer_third_party_returns.bufferPendingReturnForPeerFn(PeerType),
    );
}

pub fn handleMissingReturnQuestionForPeerFn(comptime PeerType: type) *const fn (*PeerType, []const u8, u32) anyerror!void {
    return struct {
        fn call(peer: *PeerType, frame: []const u8, answer_id: u32) anyerror!void {
            try handleMissingReturnQuestionForPeer(PeerType, peer, frame, answer_id);
        }
    }.call;
}

pub fn handleReturnRegularForPeer(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime InboundCapsType: type,
    comptime release_inbound_caps_mut: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    comptime send_finish: *const fn (*PeerType, u32, bool) anyerror!void,
    peer: *PeerType,
    question: QuestionType,
    ret: protocol.Return,
    inbound_caps: *const InboundCapsType,
) void {
    peer_control.handleReturnRegular(
        PeerType,
        QuestionType,
        InboundCapsType,
        peer,
        question,
        ret,
        inbound_caps,
        peer_return_dispatch.takeAdoptedAnswerOriginalForPeerFn(PeerType),
        peer_return_dispatch.dispatchQuestionReturnForPeerFn(PeerType, QuestionType, InboundCapsType),
        peer_return_dispatch.releaseInboundCapsForPeerFn(PeerType, InboundCapsType, release_inbound_caps_mut),
        peer_return_dispatch.reportNonfatalErrorForPeerFn(PeerType),
        peer_return_dispatch.maybeSendAutoFinishForPeerFn(PeerType, QuestionType, send_finish),
    );
}

pub fn handleReturnRegularForPeerFn(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime InboundCapsType: type,
    comptime release_inbound_caps_mut: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    comptime send_finish: *const fn (*PeerType, u32, bool) anyerror!void,
) *const fn (*PeerType, QuestionType, protocol.Return, *const InboundCapsType) void {
    return struct {
        fn call(
            peer: *PeerType,
            question: QuestionType,
            ret: protocol.Return,
            inbound_caps: *const InboundCapsType,
        ) void {
            handleReturnRegularForPeer(
                PeerType,
                QuestionType,
                InboundCapsType,
                release_inbound_caps_mut,
                send_finish,
                peer,
                question,
                ret,
                inbound_caps,
            );
        }
    }.call;
}

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

test "peer_return_orchestration fetchRemoveQuestionForPeerFn removes and returns question" {
    const Question = struct { marker: u32 };
    const FakePeer = struct {
        questions: std.AutoHashMap(u32, Question),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .questions = std.AutoHashMap(u32, Question).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.questions.deinit();
        }
    };

    var peer = FakePeer.init(std.testing.allocator);
    defer peer.deinit();
    try peer.questions.put(44, .{ .marker = 9 });

    const fetch = fetchRemoveQuestionForPeerFn(FakePeer, Question);
    const question = fetch(&peer, 44) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 9), question.marker);
    try std.testing.expectEqual(@as(?Question, null), fetch(&peer, 44));
}

test "peer_return_orchestration inbound caps helper factories initialize and deinitialize" {
    const cap_table = @import("../../cap_table.zig");

    const FakePeer = struct {
        allocator: std.mem.Allocator,
        caps: cap_table.CapTable,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .caps = cap_table.CapTable.init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.caps.deinit();
        }
    };

    var peer = FakePeer.init(std.testing.allocator);
    defer peer.deinit();

    const init_inbound = initInboundCapsForPeerFn(FakePeer, cap_table.InboundCapTable);
    const deinit_inbound = deinitInboundCapsForTypeFn(cap_table.InboundCapTable);

    const ret = protocol.Return{
        .answer_id = 12,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .canceled,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };

    var inbound = try init_inbound(&peer, ret);
    deinit_inbound(&inbound);
}

test "peer_return_orchestration handleMissingReturnQuestionForPeerFn routes third-party buffering and errors" {
    const FakePeer = struct {
        allocator: std.mem.Allocator,
        pending_third_party_returns: std.AutoHashMap(u32, []u8),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .pending_third_party_returns = std.AutoHashMap(u32, []u8).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            var it = self.pending_third_party_returns.valueIterator();
            while (it.next()) |frame| self.allocator.free(frame.*);
            self.pending_third_party_returns.deinit();
        }
    };

    var peer = FakePeer.init(std.testing.allocator);
    defer peer.deinit();
    const handle_missing = handleMissingReturnQuestionForPeerFn(FakePeer);

    const frame = [_]u8{ 1, 2, 3 };
    const third_party_answer_id: u32 = 0x4000_0005;
    try handle_missing(&peer, frame[0..], third_party_answer_id);
    try std.testing.expect(peer.pending_third_party_returns.contains(third_party_answer_id));

    try std.testing.expectError(
        error.DuplicateThirdPartyReturn,
        handle_missing(&peer, frame[0..], third_party_answer_id),
    );

    try std.testing.expectError(
        error.UnknownQuestion,
        handle_missing(&peer, frame[0..], 7),
    );
}

test "peer_return_orchestration handleReturnRegularForPeerFn applies adopted answer id and dispatches callback" {
    const InboundCaps = struct {};
    const PeerState = struct {
        adopted_third_party_answers: std.AutoHashMap(u32, u32),
        callback_calls: usize = 0,
        saw_answer_id: u32 = 0,
        finish_calls: usize = 0,
        release_calls: usize = 0,
        on_error: ?*const fn (peer: *@This(), err: anyerror) void = onError,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .adopted_third_party_answers = std.AutoHashMap(u32, u32).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.adopted_third_party_answers.deinit();
        }

        fn onError(peer: *@This(), err: anyerror) void {
            _ = peer;
            _ = err;
            unreachable;
        }

        fn sendFinish(self: *@This(), answer_id: u32, release_result_caps: bool) !void {
            _ = answer_id;
            _ = release_result_caps;
            self.finish_calls += 1;
        }

        fn releaseInboundCaps(self: *@This(), inbound_caps: *InboundCaps) !void {
            _ = inbound_caps;
            self.release_calls += 1;
        }
    };
    const Question = struct {
        ctx: *anyopaque,
        on_return: *const fn (*anyopaque, *PeerState, protocol.Return, *const InboundCaps) anyerror!void,
        is_loopback: bool = false,
        suppress_auto_finish: bool = false,
    };
    const Hooks = struct {
        fn onReturn(
            ctx: *anyopaque,
            peer: *PeerState,
            ret: protocol.Return,
            inbound_caps: *const InboundCaps,
        ) anyerror!void {
            _ = ctx;
            _ = inbound_caps;
            peer.callback_calls += 1;
            peer.saw_answer_id = ret.answer_id;
        }
    };

    var peer = PeerState.init(std.testing.allocator);
    defer peer.deinit();
    try peer.adopted_third_party_answers.put(77, 42);

    var marker: u32 = 0;
    const question = Question{
        .ctx = &marker,
        .on_return = Hooks.onReturn,
    };
    const ret = protocol.Return{
        .answer_id = 77,
        .release_param_caps = false,
        .no_finish_needed = true,
        .tag = .canceled,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    const inbound = InboundCaps{};

    const handle_regular = handleReturnRegularForPeerFn(
        PeerState,
        Question,
        InboundCaps,
        PeerState.releaseInboundCaps,
        PeerState.sendFinish,
    );
    handle_regular(&peer, question, ret, &inbound);

    try std.testing.expectEqual(@as(usize, 1), peer.callback_calls);
    try std.testing.expectEqual(@as(u32, 42), peer.saw_answer_id);
    try std.testing.expectEqual(@as(usize, 0), peer.release_calls);
    try std.testing.expectEqual(@as(usize, 0), peer.finish_calls);
    try std.testing.expect(!peer.adopted_third_party_answers.contains(77));
}
