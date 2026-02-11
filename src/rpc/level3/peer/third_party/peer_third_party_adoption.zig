const std = @import("std");
const message = @import("../../../../serialization/message.zig");
const protocol = @import("../../../level0/protocol.zig");
const peer_third_party_pending = @import("peer_third_party_pending.zig");
const peer_third_party_returns = @import("peer_third_party_returns.zig");

pub fn isThirdPartyAnswerId(answer_id: u32) bool {
    return (answer_id & 0x4000_0000) != 0 and (answer_id & 0x8000_0000) == 0;
}

pub fn adoptThirdPartyAnswer(
    comptime PeerType: type,
    comptime QuestionType: type,
    peer: *PeerType,
    question_id: u32,
    adopted_answer_id: u32,
    question: QuestionType,
    questions: *std.AutoHashMap(u32, QuestionType),
    adopted_answers: *std.AutoHashMap(u32, u32),
    pending_returns: *std.AutoHashMap(u32, []u8),
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    free_frame: *const fn (*PeerType, []u8) void,
    handle_pending_return_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    if (!isThirdPartyAnswerId(adopted_answer_id)) {
        try send_abort(peer, "invalid thirdPartyAnswer answerId");
        return error.InvalidThirdPartyAnswerId;
    }
    if (questions.contains(adopted_answer_id) or adopted_answers.contains(adopted_answer_id)) {
        try send_abort(peer, "duplicate thirdPartyAnswer answerId");
        return error.DuplicateThirdPartyAnswerId;
    }

    try questions.put(adopted_answer_id, question);
    errdefer _ = questions.remove(adopted_answer_id);

    try adopted_answers.put(adopted_answer_id, question_id);
    errdefer _ = adopted_answers.remove(adopted_answer_id);

    if (peer_third_party_returns.takePendingReturnFrame(pending_returns, adopted_answer_id)) |pending_frame| {
        defer free_frame(peer, pending_frame);
        try handle_pending_return_frame(peer, pending_frame);
    }
}

pub fn handleThirdPartyAnswer(
    comptime PeerType: type,
    comptime PendingAwaitType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    third_party_answer: protocol.ThirdPartyAnswer,
    pending_awaits: *std.StringHashMap(PendingAwaitType),
    pending_answers: *std.StringHashMap(u32),
    capture_completion: *const fn (*PeerType, protocol.ThirdPartyAnswer) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    adopt_pending_await: *const fn (*PeerType, u32, PendingAwaitType) anyerror!void,
) !void {
    if (!isThirdPartyAnswerId(third_party_answer.answer_id)) {
        try send_abort(peer, "invalid thirdPartyAnswer answerId");
        return error.InvalidThirdPartyAnswerId;
    }

    const completion_payload = try capture_completion(peer, third_party_answer);
    const completion_key = completion_payload orelse {
        try send_abort(peer, "thirdPartyAnswer missing completion");
        return error.MissingThirdPartyPayload;
    };
    var owns_completion_key = true;
    errdefer if (owns_completion_key) free_payload(peer, completion_key);

    if (try peer_third_party_pending.adoptPendingAwait(
        PeerType,
        PendingAwaitType,
        allocator,
        pending_awaits,
        peer,
        completion_key,
        third_party_answer.answer_id,
        adopt_pending_await,
    )) {
        owns_completion_key = false;
        free_payload(peer, completion_key);
        return;
    }

    if (pending_answers.get(completion_key)) |existing_id| {
        if (existing_id == third_party_answer.answer_id) {
            owns_completion_key = false;
            free_payload(peer, completion_key);
            return;
        }
        try send_abort(peer, "conflicting thirdPartyAnswer completion");
        return error.ConflictingThirdPartyAnswer;
    }

    try peer_third_party_pending.putPendingAnswer(
        pending_answers,
        completion_key,
        third_party_answer.answer_id,
    );
    owns_completion_key = false;
}

pub fn captureThirdPartyCompletionForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    third_party_answer: protocol.ThirdPartyAnswer,
    capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
) !?[]u8 {
    return capture_payload(peer, third_party_answer.completion);
}

pub fn captureThirdPartyCompletionForPeerFn(
    comptime PeerType: type,
    comptime capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
) *const fn (*PeerType, protocol.ThirdPartyAnswer) anyerror!?[]u8 {
    return struct {
        fn call(peer: *PeerType, third_party_answer: protocol.ThirdPartyAnswer) anyerror!?[]u8 {
            return try captureThirdPartyCompletionForPeer(PeerType, peer, third_party_answer, capture_payload);
        }
    }.call;
}

pub fn adoptPendingAwaitEntryForPeer(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime PendingAwaitType: type,
    peer: *PeerType,
    adopted_answer_id: u32,
    pending_await: PendingAwaitType,
    adopt_third_party_answer: *const fn (*PeerType, u32, u32, QuestionType) anyerror!void,
) !void {
    try adopt_third_party_answer(
        peer,
        pending_await.question_id,
        adopted_answer_id,
        pending_await.question,
    );
}

pub fn adoptPendingAwaitEntryForPeerFn(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime PendingAwaitType: type,
    comptime adopt_third_party_answer: *const fn (*PeerType, u32, u32, QuestionType) anyerror!void,
) *const fn (*PeerType, u32, PendingAwaitType) anyerror!void {
    return struct {
        fn call(peer: *PeerType, adopted_answer_id: u32, pending_await: PendingAwaitType) anyerror!void {
            try adoptPendingAwaitEntryForPeer(
                PeerType,
                QuestionType,
                PendingAwaitType,
                peer,
                adopted_answer_id,
                pending_await,
                adopt_third_party_answer,
            );
        }
    }.call;
}

pub fn handleReturnAcceptFromThirdParty(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime PendingAwaitType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    answer_id: u32,
    question: QuestionType,
    accept_from_third_party: ?message.AnyPointerReader,
    pending_awaits: *std.StringHashMap(PendingAwaitType),
    pending_answers: *std.StringHashMap(u32),
    capture_completion_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    adopt_third_party_answer: *const fn (*PeerType, u32, u32, QuestionType) anyerror!void,
    make_pending_await: *const fn (u32, QuestionType) PendingAwaitType,
) !void {
    const completion_payload = try capture_completion_payload(peer, accept_from_third_party);
    const completion_key = completion_payload orelse return error.MissingThirdPartyPayload;
    var owns_completion_key = true;
    errdefer if (owns_completion_key) free_payload(peer, completion_key);

    if (pending_awaits.contains(completion_key)) {
        try send_abort(peer, "duplicate awaitFromThirdParty completion");
        return error.DuplicateThirdPartyAwait;
    }

    if (peer_third_party_pending.takePendingAnswerId(allocator, pending_answers, completion_key)) |pending_answer_id| {
        free_payload(peer, completion_key);
        owns_completion_key = false;
        try adopt_third_party_answer(peer, answer_id, pending_answer_id, question);
    } else {
        const pending_await = make_pending_await(answer_id, question);
        try peer_third_party_pending.putPendingAwait(
            PendingAwaitType,
            pending_awaits,
            completion_key,
            pending_await,
        );
        owns_completion_key = false;
    }
}

pub fn handleReturnAcceptFromThirdPartyForPeerFn(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime PendingAwaitType: type,
    comptime InboundCapsType: type,
    comptime capture_completion_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
    comptime free_payload: *const fn (*PeerType, []u8) void,
    comptime send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    comptime adopt_third_party_answer: *const fn (*PeerType, u32, u32, QuestionType) anyerror!void,
) *const fn (*PeerType, u32, QuestionType, ?message.AnyPointerReader, *const InboundCapsType) anyerror!void {
    return struct {
        fn makePendingAwait(question_id: u32, question: QuestionType) PendingAwaitType {
            return .{
                .question_id = question_id,
                .question = question,
            };
        }

        fn call(
            peer: *PeerType,
            answer_id: u32,
            question: QuestionType,
            accept_from_third_party: ?message.AnyPointerReader,
            inbound_caps: *const InboundCapsType,
        ) anyerror!void {
            _ = inbound_caps;
            try handleReturnAcceptFromThirdParty(
                PeerType,
                QuestionType,
                PendingAwaitType,
                peer.allocator,
                peer,
                answer_id,
                question,
                accept_from_third_party,
                &peer.pending_third_party_awaits,
                &peer.pending_third_party_answers,
                capture_completion_payload,
                free_payload,
                send_abort,
                adopt_third_party_answer,
                makePendingAwait,
            );
        }
    }.call;
}

test "peer_third_party_adoption adoptThirdPartyAnswer records adoption and replays pending return" {
    const Question = struct { marker: u32 };
    const State = struct {
        abort_calls: usize = 0,
        free_calls: usize = 0,
        replay_calls: usize = 0,
    };
    const Hooks = struct {
        fn sendAbort(state: *State, reason: []const u8) !void {
            _ = reason;
            state.abort_calls += 1;
            return error.TestUnexpectedResult;
        }

        fn freeFrame(state: *State, frame: []u8) void {
            state.free_calls += 1;
            std.testing.allocator.free(frame);
        }

        fn replay(state: *State, frame: []const u8) !void {
            state.replay_calls += 1;
            try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3 }, frame);
        }
    };

    var questions = std.AutoHashMap(u32, Question).init(std.testing.allocator);
    defer questions.deinit();

    var adopted_answers = std.AutoHashMap(u32, u32).init(std.testing.allocator);
    defer adopted_answers.deinit();

    var pending_returns = std.AutoHashMap(u32, []u8).init(std.testing.allocator);
    defer {
        var it = pending_returns.valueIterator();
        while (it.next()) |frame| std.testing.allocator.free(frame.*);
        pending_returns.deinit();
    }

    const adopted_answer_id: u32 = 0x4000_0011;
    const frame = try std.testing.allocator.dupe(u8, &[_]u8{ 1, 2, 3 });
    try pending_returns.put(adopted_answer_id, frame);

    var state = State{};
    try adoptThirdPartyAnswer(
        State,
        Question,
        &state,
        77,
        adopted_answer_id,
        .{ .marker = 5 },
        &questions,
        &adopted_answers,
        &pending_returns,
        Hooks.sendAbort,
        Hooks.freeFrame,
        Hooks.replay,
    );

    try std.testing.expectEqual(@as(usize, 0), state.abort_calls);
    try std.testing.expectEqual(@as(usize, 1), state.free_calls);
    try std.testing.expectEqual(@as(usize, 1), state.replay_calls);
    try std.testing.expectEqual(@as(usize, 0), pending_returns.count());
    try std.testing.expectEqual(@as(u32, 77), adopted_answers.get(adopted_answer_id).?);
    try std.testing.expectEqual(@as(u32, 5), questions.get(adopted_answer_id).?.marker);
}

test "peer_third_party_adoption handleReturnAcceptFromThirdParty adopts pending answer id" {
    const PendingAwait = struct {
        question_id: u32,
        question: u32,
    };
    const State = struct {
        adopted_question_id: u32 = 0,
        adopted_answer_id: u32 = 0,
        adopted_question: u32 = 0,
    };
    const Hooks = struct {
        fn capture(_: *State, await_ptr: ?message.AnyPointerReader) !?[]u8 {
            _ = await_ptr;
            return try std.testing.allocator.dupe(u8, "completion-key");
        }

        fn freePayload(_: *State, payload: []u8) void {
            std.testing.allocator.free(payload);
        }

        fn sendAbort(_: *State, reason: []const u8) !void {
            _ = reason;
            return error.TestUnexpectedResult;
        }

        fn adopt(state: *State, question_id: u32, adopted_answer_id: u32, question: u32) !void {
            state.adopted_question_id = question_id;
            state.adopted_answer_id = adopted_answer_id;
            state.adopted_question = question;
        }

        fn makePending(question_id: u32, question: u32) PendingAwait {
            return .{ .question_id = question_id, .question = question };
        }
    };

    var pending_awaits = std.StringHashMap(PendingAwait).init(std.testing.allocator);
    defer {
        var it = pending_awaits.iterator();
        while (it.next()) |entry| std.testing.allocator.free(entry.key_ptr.*);
        pending_awaits.deinit();
    }

    var pending_answers = std.StringHashMap(u32).init(std.testing.allocator);
    defer {
        var it = pending_answers.iterator();
        while (it.next()) |entry| std.testing.allocator.free(entry.key_ptr.*);
        pending_answers.deinit();
    }

    const answer_key = try std.testing.allocator.dupe(u8, "completion-key");
    try pending_answers.put(answer_key, 0x4000_0022);

    var state = State{};
    try handleReturnAcceptFromThirdParty(
        State,
        u32,
        PendingAwait,
        std.testing.allocator,
        &state,
        55,
        99,
        null,
        &pending_awaits,
        &pending_answers,
        Hooks.capture,
        Hooks.freePayload,
        Hooks.sendAbort,
        Hooks.adopt,
        Hooks.makePending,
    );

    try std.testing.expectEqual(@as(usize, 0), pending_answers.count());
    try std.testing.expectEqual(@as(usize, 0), pending_awaits.count());
    try std.testing.expectEqual(@as(u32, 55), state.adopted_question_id);
    try std.testing.expectEqual(@as(u32, 0x4000_0022), state.adopted_answer_id);
    try std.testing.expectEqual(@as(u32, 99), state.adopted_question);
}

test "peer_third_party_adoption handleThirdPartyAnswer adopts pending await" {
    const PendingAwait = struct {
        question_id: u32,
        question: u32,
    };
    const State = struct {
        adopted_answer_id: u32 = 0,
        adopted_question_id: u32 = 0,
        adopted_question: u32 = 0,
    };
    const Hooks = struct {
        fn capture(_: *State, third_party_answer: protocol.ThirdPartyAnswer) !?[]u8 {
            _ = third_party_answer;
            return try std.testing.allocator.dupe(u8, "completion-key");
        }

        fn freePayload(_: *State, payload: []u8) void {
            std.testing.allocator.free(payload);
        }

        fn sendAbort(_: *State, reason: []const u8) !void {
            _ = reason;
            return error.TestUnexpectedResult;
        }

        fn adopt(state: *State, adopted_answer_id: u32, pending: PendingAwait) !void {
            state.adopted_answer_id = adopted_answer_id;
            state.adopted_question_id = pending.question_id;
            state.adopted_question = pending.question;
        }
    };

    var pending_awaits = std.StringHashMap(PendingAwait).init(std.testing.allocator);
    defer {
        var it = pending_awaits.iterator();
        while (it.next()) |entry| std.testing.allocator.free(entry.key_ptr.*);
        pending_awaits.deinit();
    }

    var pending_answers = std.StringHashMap(u32).init(std.testing.allocator);
    defer {
        var it = pending_answers.iterator();
        while (it.next()) |entry| std.testing.allocator.free(entry.key_ptr.*);
        pending_answers.deinit();
    }

    const await_key = try std.testing.allocator.dupe(u8, "completion-key");
    try pending_awaits.put(await_key, .{
        .question_id = 7,
        .question = 13,
    });

    var state = State{};
    try handleThirdPartyAnswer(
        State,
        PendingAwait,
        std.testing.allocator,
        &state,
        .{
            .answer_id = 0x4000_0042,
            .completion = null,
        },
        &pending_awaits,
        &pending_answers,
        Hooks.capture,
        Hooks.freePayload,
        Hooks.sendAbort,
        Hooks.adopt,
    );

    try std.testing.expectEqual(@as(usize, 0), pending_awaits.count());
    try std.testing.expectEqual(@as(usize, 0), pending_answers.count());
    try std.testing.expectEqual(@as(u32, 0x4000_0042), state.adopted_answer_id);
    try std.testing.expectEqual(@as(u32, 7), state.adopted_question_id);
    try std.testing.expectEqual(@as(u32, 13), state.adopted_question);
}

test "peer_third_party_adoption captureThirdPartyCompletionForPeerFn forwards completion pointer payload" {
    const FakePeer = struct {
        capture_calls: usize = 0,
        saw_non_null_completion: bool = false,

        fn capturePayload(self: *@This(), maybe_ptr: ?message.AnyPointerReader) !?[]u8 {
            self.capture_calls += 1;
            if (maybe_ptr) |ptr| {
                self.saw_non_null_completion = !ptr.isNull();
            }
            return try std.testing.allocator.dupe(u8, "captured");
        }
    };

    var peer = FakePeer{};
    const capture = captureThirdPartyCompletionForPeerFn(FakePeer, FakePeer.capturePayload);

    var completion_builder = message.MessageBuilder.init(std.testing.allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("done");
    const completion_payload = try completion_builder.toBytes();
    defer std.testing.allocator.free(completion_payload);

    var completion_msg = try message.Message.init(std.testing.allocator, completion_payload);
    defer completion_msg.deinit();
    const completion_ptr = try completion_msg.getRootAnyPointer();

    const captured = try capture(&peer, .{
        .answer_id = 0x4000_0077,
        .completion = completion_ptr,
    });
    defer std.testing.allocator.free(captured.?);

    try std.testing.expectEqual(@as(usize, 1), peer.capture_calls);
    try std.testing.expect(peer.saw_non_null_completion);
    try std.testing.expectEqualStrings("captured", captured.?);
}

test "peer_third_party_adoption adoptPendingAwaitEntryForPeerFn maps pending await fields to adopter callback" {
    const PendingAwait = struct {
        question_id: u32,
        question: u32,
    };
    const FakePeer = struct {
        adopted_question_id: u32 = 0,
        adopted_answer_id: u32 = 0,
        adopted_question: u32 = 0,

        fn adopt(self: *@This(), question_id: u32, adopted_answer_id: u32, question: u32) !void {
            self.adopted_question_id = question_id;
            self.adopted_answer_id = adopted_answer_id;
            self.adopted_question = question;
        }
    };

    var peer = FakePeer{};
    const adopt_pending = adoptPendingAwaitEntryForPeerFn(
        FakePeer,
        u32,
        PendingAwait,
        FakePeer.adopt,
    );

    try adopt_pending(&peer, 0x4000_0099, .{
        .question_id = 55,
        .question = 88,
    });

    try std.testing.expectEqual(@as(u32, 55), peer.adopted_question_id);
    try std.testing.expectEqual(@as(u32, 0x4000_0099), peer.adopted_answer_id);
    try std.testing.expectEqual(@as(u32, 88), peer.adopted_question);
}

test "peer_third_party_adoption handleReturnAcceptFromThirdPartyForPeerFn adopts pending answer id" {
    const PendingAwait = struct {
        question_id: u32,
        question: u32,
    };
    const InboundCaps = struct {};
    const FakePeer = struct {
        allocator: std.mem.Allocator,
        pending_third_party_awaits: std.StringHashMap(PendingAwait),
        pending_third_party_answers: std.StringHashMap(u32),
        adopted_question_id: u32 = 0,
        adopted_answer_id: u32 = 0,
        adopted_question: u32 = 0,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .pending_third_party_awaits = std.StringHashMap(PendingAwait).init(allocator),
                .pending_third_party_answers = std.StringHashMap(u32).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            var await_it = self.pending_third_party_awaits.iterator();
            while (await_it.next()) |entry| self.allocator.free(entry.key_ptr.*);
            self.pending_third_party_awaits.deinit();

            var answer_it = self.pending_third_party_answers.iterator();
            while (answer_it.next()) |entry| self.allocator.free(entry.key_ptr.*);
            self.pending_third_party_answers.deinit();
        }

        fn captureCompletionPayload(
            self: *@This(),
            await_ptr: ?message.AnyPointerReader,
        ) !?[]u8 {
            _ = await_ptr;
            return try self.allocator.dupe(u8, "completion-key");
        }

        fn freePayload(self: *@This(), payload: []u8) void {
            self.allocator.free(payload);
        }

        fn sendAbort(self: *@This(), reason: []const u8) !void {
            _ = self;
            _ = reason;
            return error.TestUnexpectedResult;
        }

        fn adoptThirdPartyAnswer(
            self: *@This(),
            question_id: u32,
            adopted_answer_id: u32,
            question: u32,
        ) !void {
            self.adopted_question_id = question_id;
            self.adopted_answer_id = adopted_answer_id;
            self.adopted_question = question;
        }
    };

    var peer = FakePeer.init(std.testing.allocator);
    defer peer.deinit();

    const key = try std.testing.allocator.dupe(u8, "completion-key");
    try peer.pending_third_party_answers.put(key, 0x4000_00AA);

    const handle_accept = handleReturnAcceptFromThirdPartyForPeerFn(
        FakePeer,
        u32,
        PendingAwait,
        InboundCaps,
        FakePeer.captureCompletionPayload,
        FakePeer.freePayload,
        FakePeer.sendAbort,
        FakePeer.adoptThirdPartyAnswer,
    );

    const inbound = InboundCaps{};
    try handle_accept(&peer, 55, 99, null, &inbound);

    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
    try std.testing.expectEqual(@as(u32, 55), peer.adopted_question_id);
    try std.testing.expectEqual(@as(u32, 0x4000_00AA), peer.adopted_answer_id);
    try std.testing.expectEqual(@as(u32, 99), peer.adopted_question);
}
