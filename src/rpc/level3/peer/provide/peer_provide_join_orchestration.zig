const std = @import("std");
const cap_table = @import("../../../level0/cap_table.zig");
const peer_join_state = @import("peer_join_state.zig");
const peer_provides_state = @import("peer_provides_state.zig");
const protocol = @import("../../../level0/protocol.zig");
const message = @import("../../../../serialization/message.zig");

pub fn captureProvideRecipientForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    provide: protocol.Provide,
    capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
) !?[]u8 {
    return capture_payload(peer, provide.recipient);
}

pub fn captureProvideRecipientForPeerFn(
    comptime PeerType: type,
    comptime capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
) *const fn (*PeerType, protocol.Provide) anyerror!?[]u8 {
    return struct {
        fn call(peer: *PeerType, provide: protocol.Provide) anyerror!?[]u8 {
            return try captureProvideRecipientForPeer(PeerType, peer, provide, capture_payload);
        }
    }.call;
}

pub fn captureAcceptProvisionForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    accept: protocol.Accept,
    capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
) !?[]u8 {
    return capture_payload(peer, accept.provision);
}

pub fn captureAcceptProvisionForPeerFn(
    comptime PeerType: type,
    comptime capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
) *const fn (*PeerType, protocol.Accept) anyerror!?[]u8 {
    return struct {
        fn call(peer: *PeerType, accept: protocol.Accept) anyerror!?[]u8 {
            return try captureAcceptProvisionForPeer(PeerType, peer, accept, capture_payload);
        }
    }.call;
}

pub fn handleProvide(
    comptime PeerType: type,
    comptime ProvideEntryType: type,
    comptime ProvideTargetType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    provide: protocol.Provide,
    provides_by_question: *std.AutoHashMap(u32, ProvideEntryType),
    provides_by_key: *std.StringHashMap(u32),
    capture_recipient: *const fn (*PeerType, protocol.Provide) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    resolve_target: *const fn (*PeerType, protocol.MessageTarget) anyerror!cap_table.ResolvedCap,
    make_target: *const fn (*PeerType, cap_table.ResolvedCap) anyerror!ProvideTargetType,
    deinit_target: *const fn (*ProvideTargetType, std.mem.Allocator) void,
) !void {
    const key = try capture_recipient(peer, provide);
    const key_bytes = key orelse {
        try send_abort(peer, "provide missing recipient");
        return error.MissingThirdPartyPayload;
    };
    var key_owned = true;
    errdefer if (key_owned) free_payload(peer, key_bytes);

    if (peer_provides_state.hasProvideQuestion(ProvideEntryType, provides_by_question, provide.question_id)) {
        try send_abort(peer, "duplicate provide question");
        return error.DuplicateProvideQuestionId;
    }
    if (peer_provides_state.hasProvideRecipient(provides_by_key, key_bytes)) {
        try send_abort(peer, "duplicate provide recipient");
        return error.DuplicateProvideRecipient;
    }

    const resolved = resolve_target(peer, provide.target) catch |err| {
        try send_abort(peer, @errorName(err));
        return err;
    };
    var target = try make_target(peer, resolved);
    errdefer deinit_target(&target, allocator);

    try peer_provides_state.putProvideByQuestion(
        ProvideEntryType,
        ProvideTargetType,
        provides_by_question,
        provide.question_id,
        key_bytes,
        target,
    );
    // Ownership of key_bytes has transferred to provides_by_question.
    // clearProvide will free it on error from here on.
    key_owned = false;
    errdefer peer_provides_state.clearProvide(
        ProvideEntryType,
        ProvideTargetType,
        allocator,
        provides_by_question,
        provides_by_key,
        provide.question_id,
        deinit_target,
    );

    try peer_provides_state.putProvideByKey(
        provides_by_key,
        key_bytes,
        provide.question_id,
    );
}

pub fn handleAccept(
    comptime PeerType: type,
    comptime ProvideEntryType: type,
    comptime ProvideTargetType: type,
    peer: *PeerType,
    accept: protocol.Accept,
    provides_by_question: *std.AutoHashMap(u32, ProvideEntryType),
    provides_by_key: *std.StringHashMap(u32),
    capture_provision: *const fn (*PeerType, protocol.Accept) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    queue_embargoed_accept: *const fn (*PeerType, u32, u32, []const u8) anyerror!void,
    send_return_provided_target: *const fn (*PeerType, u32, *const ProvideTargetType) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    const key = try capture_provision(peer, accept);
    defer if (key) |bytes| free_payload(peer, bytes);
    const key_bytes = key orelse {
        try send_return_exception(peer, accept.question_id, "unknown provision");
        return;
    };

    const provided_question_id = peer_provides_state.getProvidedQuestion(provides_by_key, key_bytes) orelse {
        try send_return_exception(peer, accept.question_id, "unknown provision");
        return;
    };
    const target = peer_provides_state.getProvidedTarget(
        ProvideEntryType,
        ProvideTargetType,
        provides_by_question,
        provided_question_id,
    ) orelse {
        try send_return_exception(peer, accept.question_id, "unknown provision");
        return;
    };

    if (accept.embargo) |embargo| {
        try queue_embargoed_accept(peer, accept.question_id, provided_question_id, embargo);
        return;
    }

    send_return_provided_target(peer, accept.question_id, target) catch |err| {
        try send_return_exception(peer, accept.question_id, @errorName(err));
    };
}

pub fn handleJoin(
    comptime PeerType: type,
    comptime JoinKeyPartType: type,
    comptime JoinStateType: type,
    comptime PendingJoinQuestionType: type,
    comptime ProvideTargetType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    join: protocol.Join,
    pending_joins: *std.AutoHashMap(u32, JoinStateType),
    pending_join_questions: *std.AutoHashMap(u32, PendingJoinQuestionType),
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    resolve_target: *const fn (*PeerType, protocol.MessageTarget) anyerror!cap_table.ResolvedCap,
    make_target: *const fn (*PeerType, cap_table.ResolvedCap) anyerror!ProvideTargetType,
    deinit_target: *const fn (*ProvideTargetType, std.mem.Allocator) void,
    init_join_state: *const fn (std.mem.Allocator, u16) JoinStateType,
    complete_join: *const fn (*PeerType, u32) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    if (pending_join_questions.contains(join.question_id)) {
        try send_abort(peer, "duplicate join question");
        return error.DuplicateJoinQuestionId;
    }

    const join_key_part = peer_join_state.parseJoinKeyPart(JoinKeyPartType, join.key_part) catch |err| {
        try send_return_exception(peer, join.question_id, @errorName(err));
        return;
    };

    const resolved = resolve_target(peer, join.target) catch |err| {
        try send_return_exception(peer, join.question_id, @errorName(err));
        return;
    };

    var target = make_target(peer, resolved) catch |err| {
        try send_return_exception(peer, join.question_id, @errorName(err));
        return;
    };

    const outcome = peer_join_state.insertJoinPart(
        JoinKeyPartType,
        JoinStateType,
        PendingJoinQuestionType,
        ProvideTargetType,
        allocator,
        pending_joins,
        pending_join_questions,
        join_key_part,
        join.question_id,
        target,
        init_join_state,
    ) catch |err| {
        deinit_target(&target, allocator);
        return err;
    };

    switch (outcome) {
        .inserted => {},
        .inserted_ready => {
            try complete_join(peer, join_key_part.join_id);
        },
        .part_count_mismatch => {
            deinit_target(&target, allocator);
            try send_return_exception(peer, join.question_id, "join partCount mismatch");
        },
        .duplicate_part => {
            deinit_target(&target, allocator);
            try send_return_exception(peer, join.question_id, "duplicate join part");
        },
    }
}

test "peer_provide_join_orchestration handleProvide rejects duplicate question id" {
    const ProvideTarget = struct { id: u32 };
    const ProvideEntry = struct {
        recipient_key: []u8,
        target: ProvideTarget,
    };
    const State = struct {
        abort_calls: usize = 0,
    };
    const Hooks = struct {
        fn capture(_: *State, provide: protocol.Provide) !?[]u8 {
            _ = provide;
            return try std.testing.allocator.dupe(u8, "recipient-key");
        }

        fn freePayload(_: *State, payload: []u8) void {
            std.testing.allocator.free(payload);
        }

        fn sendAbort(state: *State, reason: []const u8) !void {
            _ = reason;
            state.abort_calls += 1;
        }

        fn resolveTarget(_: *State, target: protocol.MessageTarget) !cap_table.ResolvedCap {
            _ = target;
            return .{ .exported = .{ .id = 1 } };
        }

        fn makeTarget(_: *State, resolved: cap_table.ResolvedCap) !ProvideTarget {
            _ = resolved;
            return .{ .id = 1 };
        }

        fn deinitTarget(target: *ProvideTarget, allocator: std.mem.Allocator) void {
            _ = target;
            _ = allocator;
        }
    };

    var provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(std.testing.allocator);
    defer {
        var it = provides_by_question.valueIterator();
        while (it.next()) |entry| std.testing.allocator.free(entry.recipient_key);
        provides_by_question.deinit();
    }
    var provides_by_key = std.StringHashMap(u32).init(std.testing.allocator);
    defer provides_by_key.deinit();

    const existing_key = try std.testing.allocator.dupe(u8, "existing");
    try provides_by_question.put(9, .{
        .recipient_key = existing_key,
        .target = .{ .id = 2 },
    });

    var state = State{};
    try std.testing.expectError(
        error.DuplicateProvideQuestionId,
        handleProvide(
            State,
            ProvideEntry,
            ProvideTarget,
            &state,
            std.testing.allocator,
            .{
                .question_id = 9,
                .target = .{
                    .tag = .importedCap,
                    .imported_cap = 1,
                    .promised_answer = null,
                },
                .recipient = null,
            },
            &provides_by_question,
            &provides_by_key,
            Hooks.capture,
            Hooks.freePayload,
            Hooks.sendAbort,
            Hooks.resolveTarget,
            Hooks.makeTarget,
            Hooks.deinitTarget,
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), state.abort_calls);
}

test "peer_provide_join_orchestration handleAccept reports unknown provision" {
    const ProvideTarget = struct { id: u32 };
    const ProvideEntry = struct {
        recipient_key: []u8,
        target: ProvideTarget,
    };
    const State = struct {
        exception_calls: usize = 0,
        last_question_id: u32 = 0,
        last_reason: ?[]u8 = null,
    };
    const Hooks = struct {
        fn capture(_: *State, accept: protocol.Accept) !?[]u8 {
            _ = accept;
            return try std.testing.allocator.dupe(u8, "unknown-key");
        }

        fn freePayload(_: *State, payload: []u8) void {
            std.testing.allocator.free(payload);
        }

        fn queueEmbargoed(_: *State, answer_id: u32, provided_question_id: u32, embargo: []const u8) !void {
            _ = answer_id;
            _ = provided_question_id;
            _ = embargo;
            return error.TestUnexpectedResult;
        }

        fn sendProvided(_: *State, answer_id: u32, target: *const ProvideTarget) !void {
            _ = answer_id;
            _ = target;
            return error.TestUnexpectedResult;
        }

        fn sendException(state: *State, question_id: u32, reason: []const u8) !void {
            state.exception_calls += 1;
            state.last_question_id = question_id;
            state.last_reason = try std.testing.allocator.dupe(u8, reason);
        }
    };

    var provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(std.testing.allocator);
    defer provides_by_question.deinit();
    var provides_by_key = std.StringHashMap(u32).init(std.testing.allocator);
    defer provides_by_key.deinit();

    var state = State{};
    defer if (state.last_reason) |reason| std.testing.allocator.free(reason);

    try handleAccept(
        State,
        ProvideEntry,
        ProvideTarget,
        &state,
        .{
            .question_id = 77,
            .embargo = null,
            .provision = null,
        },
        &provides_by_question,
        &provides_by_key,
        Hooks.capture,
        Hooks.freePayload,
        Hooks.queueEmbargoed,
        Hooks.sendProvided,
        Hooks.sendException,
    );

    try std.testing.expectEqual(@as(usize, 1), state.exception_calls);
    try std.testing.expectEqual(@as(u32, 77), state.last_question_id);
    const reason = state.last_reason orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("unknown provision", reason);
}

test "peer_provide_join_orchestration handleJoin rejects duplicate join question id" {
    const JoinKeyPart = struct {
        join_id: u32,
        part_count: u16,
        part_num: u16,
    };
    const JoinPartEntry = struct {
        question_id: u32,
        target: u32,
    };
    const JoinState = struct {
        part_count: u16,
        parts: std.AutoHashMap(u16, JoinPartEntry),

        fn init(allocator: std.mem.Allocator, part_count: u16) @This() {
            return .{
                .part_count = part_count,
                .parts = std.AutoHashMap(u16, JoinPartEntry).init(allocator),
            };
        }

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            _ = allocator;
            self.parts.deinit();
        }
    };
    const PendingJoinQuestion = struct {
        join_id: u32,
        part_num: u16,
    };
    const State = struct {
        abort_calls: usize = 0,
    };
    const Hooks = struct {
        fn sendAbort(state: *State, reason: []const u8) !void {
            _ = reason;
            state.abort_calls += 1;
        }

        fn resolveTarget(_: *State, target: protocol.MessageTarget) !cap_table.ResolvedCap {
            _ = target;
            return error.TestUnexpectedResult;
        }

        fn makeTarget(_: *State, resolved: cap_table.ResolvedCap) !u32 {
            _ = resolved;
            return error.TestUnexpectedResult;
        }

        fn deinitTarget(target: *u32, allocator: std.mem.Allocator) void {
            _ = target;
            _ = allocator;
        }

        fn initJoinState(allocator: std.mem.Allocator, part_count: u16) JoinState {
            return JoinState.init(allocator, part_count);
        }

        fn completeJoin(_: *State, join_id: u32) !void {
            _ = join_id;
            return error.TestUnexpectedResult;
        }

        fn sendReturnException(_: *State, question_id: u32, reason: []const u8) !void {
            _ = question_id;
            _ = reason;
            return error.TestUnexpectedResult;
        }
    };

    var pending_joins = std.AutoHashMap(u32, JoinState).init(std.testing.allocator);
    defer {
        var it = pending_joins.valueIterator();
        while (it.next()) |join_state| join_state.deinit(std.testing.allocator);
        pending_joins.deinit();
    }
    var pending_join_questions = std.AutoHashMap(u32, PendingJoinQuestion).init(std.testing.allocator);
    defer pending_join_questions.deinit();
    try pending_join_questions.put(5, .{ .join_id = 1, .part_num = 0 });

    var state = State{};
    try std.testing.expectError(
        error.DuplicateJoinQuestionId,
        handleJoin(
            State,
            JoinKeyPart,
            JoinState,
            PendingJoinQuestion,
            u32,
            &state,
            std.testing.allocator,
            .{
                .question_id = 5,
                .target = .{
                    .tag = .importedCap,
                    .imported_cap = 1,
                    .promised_answer = null,
                },
                .key_part = null,
            },
            &pending_joins,
            &pending_join_questions,
            Hooks.sendAbort,
            Hooks.resolveTarget,
            Hooks.makeTarget,
            Hooks.deinitTarget,
            Hooks.initJoinState,
            Hooks.completeJoin,
            Hooks.sendReturnException,
        ),
    );
    try std.testing.expectEqual(@as(usize, 1), state.abort_calls);
}

test "peer_provide_join_orchestration capture recipient/provision helper factories use expected pointer fields" {
    const State = struct {
        capture_calls: usize = 0,
        saw_non_null: bool = false,

        fn capture(self: *@This(), ptr: ?message.AnyPointerReader) !?[]u8 {
            self.capture_calls += 1;
            if (ptr) |p| self.saw_non_null = !p.isNull();
            return try std.testing.allocator.dupe(u8, "captured");
        }
    };

    var msg_builder = message.MessageBuilder.init(std.testing.allocator);
    defer msg_builder.deinit();
    const root = try msg_builder.initRootAnyPointer();
    try root.setText("hello");
    const payload = try msg_builder.toBytes();
    defer std.testing.allocator.free(payload);

    var msg = try message.Message.init(std.testing.allocator, payload);
    defer msg.deinit();
    const ptr = try msg.getRootAnyPointer();

    var state = State{};
    const capture_recipient = captureProvideRecipientForPeerFn(State, State.capture);
    const capture_provision = captureAcceptProvisionForPeerFn(State, State.capture);

    const recipient_key = try capture_recipient(&state, .{
        .question_id = 1,
        .target = .{
            .tag = .importedCap,
            .imported_cap = 1,
            .promised_answer = null,
        },
        .recipient = ptr,
    });
    defer std.testing.allocator.free(recipient_key.?);

    const provision_key = try capture_provision(&state, .{
        .question_id = 2,
        .embargo = null,
        .provision = ptr,
    });
    defer std.testing.allocator.free(provision_key.?);

    try std.testing.expectEqual(@as(usize, 2), state.capture_calls);
    try std.testing.expect(state.saw_non_null);
    try std.testing.expectEqualStrings("captured", recipient_key.?);
    try std.testing.expectEqualStrings("captured", provision_key.?);
}
