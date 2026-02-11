const std = @import("std");

pub fn adoptPendingAwait(
    comptime PeerType: type,
    comptime PendingAwaitType: type,
    allocator: std.mem.Allocator,
    pending_awaits: *std.StringHashMap(PendingAwaitType),
    peer: *PeerType,
    completion_key: []const u8,
    adopted_answer_id: u32,
    adopt_pending_entry: *const fn (*PeerType, u32, PendingAwaitType) anyerror!void,
) !bool {
    if (pending_awaits.fetchRemove(completion_key)) |await_entry| {
        defer allocator.free(await_entry.key);
        try adopt_pending_entry(peer, adopted_answer_id, await_entry.value);
        return true;
    }
    return false;
}

pub fn takePendingAnswerId(
    allocator: std.mem.Allocator,
    pending_answers: *std.StringHashMap(u32),
    completion_key: []const u8,
) ?u32 {
    if (pending_answers.fetchRemove(completion_key)) |pending_answer| {
        allocator.free(pending_answer.key);
        return pending_answer.value;
    }
    return null;
}

pub fn putPendingAnswer(
    pending_answers: *std.StringHashMap(u32),
    completion_key: []u8,
    answer_id: u32,
) !void {
    try pending_answers.put(completion_key, answer_id);
}

pub fn putPendingAwait(
    comptime PendingAwaitType: type,
    pending_awaits: *std.StringHashMap(PendingAwaitType),
    completion_key: []u8,
    pending_await: PendingAwaitType,
) !void {
    try pending_awaits.put(completion_key, pending_await);
}

test "peer_third_party_pending adoptPendingAwait removes entry and invokes adopter" {
    const PendingAwait = struct {
        question_id: u32,
        question: u32,
    };
    const State = struct {
        called: bool = false,
        adopted_answer_id: u32 = 0,
        question_id: u32 = 0,
        question: u32 = 0,
    };
    const Hooks = struct {
        fn adopt(state: *State, adopted_answer_id: u32, pending: PendingAwait) !void {
            state.called = true;
            state.adopted_answer_id = adopted_answer_id;
            state.question_id = pending.question_id;
            state.question = pending.question;
        }
    };

    var pending = std.StringHashMap(PendingAwait).init(std.testing.allocator);
    defer {
        var it = pending.iterator();
        while (it.next()) |entry| {
            std.testing.allocator.free(entry.key_ptr.*);
        }
        pending.deinit();
    }

    const key = try std.testing.allocator.dupe(u8, "completion-key");
    try putPendingAwait(PendingAwait, &pending, key, .{
        .question_id = 44,
        .question = 99,
    });

    var state = State{};
    const adopted = try adoptPendingAwait(
        State,
        PendingAwait,
        std.testing.allocator,
        &pending,
        &state,
        "completion-key",
        777,
        Hooks.adopt,
    );

    try std.testing.expect(adopted);
    try std.testing.expect(state.called);
    try std.testing.expectEqual(@as(u32, 777), state.adopted_answer_id);
    try std.testing.expectEqual(@as(u32, 44), state.question_id);
    try std.testing.expectEqual(@as(u32, 99), state.question);
    try std.testing.expectEqual(@as(usize, 0), pending.count());
}

test "peer_third_party_pending adoptPendingAwait returns false when key is absent" {
    const PendingAwait = struct {
        question_id: u32,
        question: u32,
    };
    const State = struct {
        called: bool = false,
    };
    const Hooks = struct {
        fn adopt(state: *State, adopted_answer_id: u32, pending: PendingAwait) !void {
            _ = adopted_answer_id;
            _ = pending;
            state.called = true;
        }
    };

    var pending = std.StringHashMap(PendingAwait).init(std.testing.allocator);
    defer pending.deinit();

    var state = State{};
    const adopted = try adoptPendingAwait(
        State,
        PendingAwait,
        std.testing.allocator,
        &pending,
        &state,
        "missing-key",
        12,
        Hooks.adopt,
    );
    try std.testing.expect(!adopted);
    try std.testing.expect(!state.called);
}

test "peer_third_party_pending takePendingAnswerId removes and returns answer id" {
    var pending_answers = std.StringHashMap(u32).init(std.testing.allocator);
    defer {
        var it = pending_answers.iterator();
        while (it.next()) |entry| {
            std.testing.allocator.free(entry.key_ptr.*);
        }
        pending_answers.deinit();
    }

    const key = try std.testing.allocator.dupe(u8, "completion-key");
    try putPendingAnswer(&pending_answers, key, 5150);

    const answer_id = takePendingAnswerId(
        std.testing.allocator,
        &pending_answers,
        "completion-key",
    ) orelse return error.TestExpectedEqual;

    try std.testing.expectEqual(@as(u32, 5150), answer_id);
    try std.testing.expectEqual(@as(usize, 0), pending_answers.count());
}
