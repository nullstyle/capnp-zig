const std = @import("std");
const message = @import("../message.zig");

pub const InsertOutcome = enum {
    inserted,
    inserted_ready,
    part_count_mismatch,
    duplicate_part,
};

pub fn parseJoinKeyPart(
    comptime JoinKeyPartType: type,
    join_key_part: ?message.AnyPointerReader,
) !JoinKeyPartType {
    // join.key_part is a struct payload: [join_id:u32, part_count:u16, part_num:u16].
    const key_part_ptr = join_key_part orelse return error.MissingJoinKeyPart;
    if (key_part_ptr.isNull()) return error.MissingJoinKeyPart;

    const key_struct = key_part_ptr.getStruct() catch return error.InvalidJoinKeyPart;
    const part_count = key_struct.readU16(4);
    const part_num = key_struct.readU16(6);

    if (part_count == 0 or part_num >= part_count) return error.InvalidJoinKeyPart;
    return .{
        .join_id = key_struct.readU32(0),
        .part_count = part_count,
        .part_num = part_num,
    };
}

pub fn insertJoinPart(
    comptime JoinKeyPartType: type,
    comptime JoinStateType: type,
    comptime PendingJoinQuestionType: type,
    comptime ProvideTargetType: type,
    allocator: std.mem.Allocator,
    pending_joins: *std.AutoHashMap(u32, JoinStateType),
    pending_join_questions: *std.AutoHashMap(u32, PendingJoinQuestionType),
    join_key_part: JoinKeyPartType,
    question_id: u32,
    target: ProvideTargetType,
    init_join_state: *const fn (std.mem.Allocator, u16) JoinStateType,
) !InsertOutcome {
    const join_entry = try pending_joins.getOrPut(join_key_part.join_id);
    if (!join_entry.found_existing) {
        join_entry.value_ptr.* = init_join_state(allocator, join_key_part.part_count);
    } else if (join_entry.value_ptr.part_count != join_key_part.part_count) {
        return .part_count_mismatch;
    }

    if (join_entry.value_ptr.parts.contains(join_key_part.part_num)) {
        return .duplicate_part;
    }

    try pending_join_questions.put(question_id, .{
        .join_id = join_key_part.join_id,
        .part_num = join_key_part.part_num,
    });
    errdefer _ = pending_join_questions.remove(question_id);

    try join_entry.value_ptr.parts.put(join_key_part.part_num, .{
        .question_id = question_id,
        .target = target,
    });
    errdefer _ = join_entry.value_ptr.parts.fetchRemove(join_key_part.part_num);

    if (join_entry.value_ptr.parts.count() == join_key_part.part_count) {
        // Caller can immediately complete once all parts are present.
        return .inserted_ready;
    }
    return .inserted;
}

pub fn clearPendingJoinQuestion(
    comptime JoinStateType: type,
    comptime PendingJoinQuestionType: type,
    comptime ProvideTargetType: type,
    allocator: std.mem.Allocator,
    pending_joins: *std.AutoHashMap(u32, JoinStateType),
    pending_join_questions: *std.AutoHashMap(u32, PendingJoinQuestionType),
    question_id: u32,
    deinit_target: *const fn (*ProvideTargetType, std.mem.Allocator) void,
    deinit_join_state: *const fn (*JoinStateType, std.mem.Allocator) void,
) void {
    const pending_question = pending_join_questions.fetchRemove(question_id) orelse return;
    const key = pending_question.value;

    var remove_state = false;
    if (pending_joins.getPtr(key.join_id)) |join_state| {
        if (join_state.parts.fetchRemove(key.part_num)) |removed_part| {
            var target = removed_part.value.target;
            deinit_target(&target, allocator);
        }
        remove_state = join_state.parts.count() == 0;
    }

    if (remove_state) {
        if (pending_joins.fetchRemove(key.join_id)) |removed_state| {
            var state = removed_state.value;
            deinit_join_state(&state, allocator);
        }
    }
}

pub fn completeJoin(
    comptime PeerType: type,
    comptime JoinStateType: type,
    comptime PendingJoinQuestionType: type,
    comptime ProvideTargetType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    pending_joins: *std.AutoHashMap(u32, JoinStateType),
    pending_join_questions: *std.AutoHashMap(u32, PendingJoinQuestionType),
    join_id: u32,
    targets_equal: *const fn (*const ProvideTargetType, *const ProvideTargetType) bool,
    send_return_provided_target: *const fn (*PeerType, u32, *const ProvideTargetType) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    deinit_join_state: *const fn (*JoinStateType, std.mem.Allocator) void,
) !void {
    const removed = pending_joins.fetchRemove(join_id) orelse return;
    var join_state = removed.value;
    defer deinit_join_state(&join_state, allocator);

    if (join_state.parts.count() == 0) return;

    var first_target: ?*const ProvideTargetType = null;
    var all_equal = true;

    var part_it = join_state.parts.iterator();
    while (part_it.next()) |entry| {
        if (first_target) |target| {
            if (!targets_equal(target, &entry.value_ptr.target)) {
                all_equal = false;
                break;
            }
        } else {
            first_target = &entry.value_ptr.target;
        }
    }

    var send_it = join_state.parts.iterator();
    while (send_it.next()) |entry| {
        _ = pending_join_questions.remove(entry.value_ptr.question_id);

        if (all_equal) {
            const target = first_target orelse &entry.value_ptr.target;
            send_return_provided_target(peer, entry.value_ptr.question_id, target) catch |err| {
                // Preserve join fanout: per-question fallback is an exception return.
                try send_return_exception(peer, entry.value_ptr.question_id, @errorName(err));
            };
        } else {
            try send_return_exception(peer, entry.value_ptr.question_id, "join target mismatch");
        }
    }
}

const TestTarget = struct {
    id: u32,
};

const TestJoinPartEntry = struct {
    question_id: u32,
    target: TestTarget,
};

const TestJoinState = struct {
    part_count: u16,
    parts: std.AutoHashMap(u16, TestJoinPartEntry),

    fn init(allocator: std.mem.Allocator, part_count: u16) @This() {
        return .{
            .part_count = part_count,
            .parts = std.AutoHashMap(u16, TestJoinPartEntry).init(allocator),
        };
    }

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        self.parts.deinit();
    }
};

const TestPendingJoinQuestion = struct {
    join_id: u32,
    part_num: u16,
};

const TestJoinKeyPart = struct {
    join_id: u32,
    part_count: u16,
    part_num: u16,
};

const SentProvided = struct {
    question_id: u32,
    target_id: u32,
};

const SentException = struct {
    question_id: u32,
    reason: []u8,
};

const SendState = struct {
    allocator: std.mem.Allocator,
    fail_provided: bool = false,
    provided: std.ArrayList(SentProvided),
    exceptions: std.ArrayList(SentException),

    fn init(allocator: std.mem.Allocator) @This() {
        return .{
            .allocator = allocator,
            .provided = std.ArrayList(SentProvided){},
            .exceptions = std.ArrayList(SentException){},
        };
    }

    fn deinit(self: *@This()) void {
        for (self.exceptions.items) |entry| self.allocator.free(entry.reason);
        self.provided.deinit(self.allocator);
        self.exceptions.deinit(self.allocator);
    }
};

fn initTestJoinState(allocator: std.mem.Allocator, part_count: u16) TestJoinState {
    return TestJoinState.init(allocator, part_count);
}

fn deinitTestJoinState(state: *TestJoinState, allocator: std.mem.Allocator) void {
    state.deinit(allocator);
}

fn deinitTestTarget(target: *TestTarget, allocator: std.mem.Allocator) void {
    _ = target;
    _ = allocator;
}

fn testTargetsEqual(a: *const TestTarget, b: *const TestTarget) bool {
    return a.id == b.id;
}

fn sendProvided(state: *SendState, question_id: u32, target: *const TestTarget) !void {
    if (state.fail_provided) return error.TestExpectedError;
    try state.provided.append(state.allocator, .{
        .question_id = question_id,
        .target_id = target.id,
    });
}

fn sendException(state: *SendState, question_id: u32, reason: []const u8) !void {
    const reason_copy = try state.allocator.dupe(u8, reason);
    errdefer state.allocator.free(reason_copy);
    try state.exceptions.append(state.allocator, .{
        .question_id = question_id,
        .reason = reason_copy,
    });
}

fn cleanupJoinMaps(
    allocator: std.mem.Allocator,
    pending_joins: *std.AutoHashMap(u32, TestJoinState),
    pending_join_questions: *std.AutoHashMap(u32, TestPendingJoinQuestion),
) void {
    var it = pending_joins.valueIterator();
    while (it.next()) |join_state| {
        join_state.deinit(allocator);
    }
    pending_joins.deinit();
    pending_join_questions.deinit();
}

fn containsQuestion(questions: []const SentProvided, question_id: u32) bool {
    for (questions) |entry| {
        if (entry.question_id == question_id) return true;
    }
    return false;
}

fn containsException(exceptions: []const SentException, question_id: u32, reason: []const u8) bool {
    for (exceptions) |entry| {
        if (entry.question_id == question_id and std.mem.eql(u8, entry.reason, reason)) return true;
    }
    return false;
}

test "peer_join_state insertJoinPart handles duplicate and part-count mismatch outcomes" {
    var pending_joins = std.AutoHashMap(u32, TestJoinState).init(std.testing.allocator);
    var pending_join_questions = std.AutoHashMap(u32, TestPendingJoinQuestion).init(std.testing.allocator);
    defer cleanupJoinMaps(std.testing.allocator, &pending_joins, &pending_join_questions);

    const first = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 1, .part_count = 2, .part_num = 0 },
        100,
        .{ .id = 7 },
        initTestJoinState,
    );
    try std.testing.expectEqual(InsertOutcome.inserted, first);

    const duplicate = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 1, .part_count = 2, .part_num = 0 },
        101,
        .{ .id = 7 },
        initTestJoinState,
    );
    try std.testing.expectEqual(InsertOutcome.duplicate_part, duplicate);

    const ready = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 1, .part_count = 2, .part_num = 1 },
        102,
        .{ .id = 7 },
        initTestJoinState,
    );
    try std.testing.expectEqual(InsertOutcome.inserted_ready, ready);

    _ = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 2, .part_count = 2, .part_num = 0 },
        200,
        .{ .id = 9 },
        initTestJoinState,
    );

    const mismatch = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 2, .part_count = 3, .part_num = 1 },
        201,
        .{ .id = 9 },
        initTestJoinState,
    );
    try std.testing.expectEqual(InsertOutcome.part_count_mismatch, mismatch);
}

test "peer_join_state clearPendingJoinQuestion removes join parts and empty join state" {
    var pending_joins = std.AutoHashMap(u32, TestJoinState).init(std.testing.allocator);
    var pending_join_questions = std.AutoHashMap(u32, TestPendingJoinQuestion).init(std.testing.allocator);
    defer cleanupJoinMaps(std.testing.allocator, &pending_joins, &pending_join_questions);

    _ = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 5, .part_count = 2, .part_num = 0 },
        500,
        .{ .id = 11 },
        initTestJoinState,
    );
    _ = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 5, .part_count = 2, .part_num = 1 },
        501,
        .{ .id = 11 },
        initTestJoinState,
    );

    clearPendingJoinQuestion(
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        500,
        deinitTestTarget,
        deinitTestJoinState,
    );

    try std.testing.expectEqual(@as(usize, 1), pending_join_questions.count());
    try std.testing.expect(pending_joins.contains(5));
    try std.testing.expectEqual(@as(usize, 1), pending_joins.getPtr(5).?.parts.count());

    clearPendingJoinQuestion(
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        501,
        deinitTestTarget,
        deinitTestJoinState,
    );

    try std.testing.expectEqual(@as(usize, 0), pending_join_questions.count());
    try std.testing.expect(!pending_joins.contains(5));
}

test "peer_join_state completeJoin fans out provided target when all parts match" {
    var pending_joins = std.AutoHashMap(u32, TestJoinState).init(std.testing.allocator);
    var pending_join_questions = std.AutoHashMap(u32, TestPendingJoinQuestion).init(std.testing.allocator);
    defer cleanupJoinMaps(std.testing.allocator, &pending_joins, &pending_join_questions);

    _ = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 9, .part_count = 2, .part_num = 0 },
        900,
        .{ .id = 22 },
        initTestJoinState,
    );
    _ = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 9, .part_count = 2, .part_num = 1 },
        901,
        .{ .id = 22 },
        initTestJoinState,
    );

    var state = SendState.init(std.testing.allocator);
    defer state.deinit();

    try completeJoin(
        SendState,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        &state,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        9,
        testTargetsEqual,
        sendProvided,
        sendException,
        deinitTestJoinState,
    );

    try std.testing.expectEqual(@as(usize, 0), pending_join_questions.count());
    try std.testing.expect(!pending_joins.contains(9));
    try std.testing.expectEqual(@as(usize, 2), state.provided.items.len);
    try std.testing.expectEqual(@as(usize, 0), state.exceptions.items.len);
    try std.testing.expect(containsQuestion(state.provided.items, 900));
    try std.testing.expect(containsQuestion(state.provided.items, 901));
}

test "peer_join_state completeJoin sends mismatch exceptions for all join questions" {
    var pending_joins = std.AutoHashMap(u32, TestJoinState).init(std.testing.allocator);
    var pending_join_questions = std.AutoHashMap(u32, TestPendingJoinQuestion).init(std.testing.allocator);
    defer cleanupJoinMaps(std.testing.allocator, &pending_joins, &pending_join_questions);

    _ = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 10, .part_count = 2, .part_num = 0 },
        1000,
        .{ .id = 31 },
        initTestJoinState,
    );
    _ = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 10, .part_count = 2, .part_num = 1 },
        1001,
        .{ .id = 32 },
        initTestJoinState,
    );

    var state = SendState.init(std.testing.allocator);
    defer state.deinit();

    try completeJoin(
        SendState,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        &state,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        10,
        testTargetsEqual,
        sendProvided,
        sendException,
        deinitTestJoinState,
    );

    try std.testing.expectEqual(@as(usize, 0), state.provided.items.len);
    try std.testing.expectEqual(@as(usize, 2), state.exceptions.items.len);
    try std.testing.expect(containsException(state.exceptions.items, 1000, "join target mismatch"));
    try std.testing.expect(containsException(state.exceptions.items, 1001, "join target mismatch"));
}

test "peer_join_state completeJoin converts send-target failures to return exceptions" {
    var pending_joins = std.AutoHashMap(u32, TestJoinState).init(std.testing.allocator);
    var pending_join_questions = std.AutoHashMap(u32, TestPendingJoinQuestion).init(std.testing.allocator);
    defer cleanupJoinMaps(std.testing.allocator, &pending_joins, &pending_join_questions);

    _ = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 11, .part_count = 2, .part_num = 0 },
        1100,
        .{ .id = 44 },
        initTestJoinState,
    );
    _ = try insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 11, .part_count = 2, .part_num = 1 },
        1101,
        .{ .id = 44 },
        initTestJoinState,
    );

    var state = SendState.init(std.testing.allocator);
    defer state.deinit();
    state.fail_provided = true;

    try completeJoin(
        SendState,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        &state,
        std.testing.allocator,
        &pending_joins,
        &pending_join_questions,
        11,
        testTargetsEqual,
        sendProvided,
        sendException,
        deinitTestJoinState,
    );

    try std.testing.expectEqual(@as(usize, 0), state.provided.items.len);
    try std.testing.expectEqual(@as(usize, 2), state.exceptions.items.len);
    try std.testing.expect(containsException(state.exceptions.items, 1100, "TestExpectedError"));
    try std.testing.expect(containsException(state.exceptions.items, 1101, "TestExpectedError"));
}
