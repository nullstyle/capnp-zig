const std = @import("std");
const cap_table = @import("../../cap_table.zig");
const protocol = @import("../../protocol.zig");

pub fn sendCallToImport(
    comptime PeerType: type,
    comptime CallBuildFnType: type,
    comptime QuestionCallbackType: type,
    allocator: std.mem.Allocator,
    caps: *cap_table.CapTable,
    outbound_ctx: ?*anyopaque,
    on_outbound_cap: ?cap_table.CapEntryCallback,
    peer: *PeerType,
    target_id: u32,
    interface_id: u64,
    method_id: u16,
    ctx: *anyopaque,
    build: ?CallBuildFnType,
    on_return: QuestionCallbackType,
    allocate_question: *const fn (*PeerType, *anyopaque, QuestionCallbackType) anyerror!u32,
    remove_question: *const fn (*PeerType, u32) void,
    send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) !u32 {
    const question_id = try allocate_question(peer, ctx, on_return);
    errdefer remove_question(peer, question_id);

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(question_id, interface_id, method_id);
    try call.setTargetImportedCap(target_id);

    if (build) |build_fn| {
        try build_fn(ctx, &call);
    }

    try cap_table.encodeCallPayloadCaps(caps, &call, outbound_ctx, on_outbound_cap);
    try send_builder(peer, &builder);
    return question_id;
}

pub fn sendCallToExport(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime CallBuildFnType: type,
    comptime QuestionCallbackType: type,
    allocator: std.mem.Allocator,
    caps: *cap_table.CapTable,
    outbound_ctx: ?*anyopaque,
    on_outbound_cap: ?cap_table.CapEntryCallback,
    peer: *PeerType,
    questions: *std.AutoHashMap(u32, QuestionType),
    loopback_questions: *std.AutoHashMap(u32, void),
    export_id: u32,
    interface_id: u64,
    method_id: u16,
    ctx: *anyopaque,
    build: ?CallBuildFnType,
    on_return: QuestionCallbackType,
    allocate_question: *const fn (*PeerType, *anyopaque, QuestionCallbackType) anyerror!u32,
    handle_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !u32 {
    const question_id = try allocate_question(peer, ctx, on_return);
    errdefer _ = questions.remove(question_id);
    if (questions.getEntry(question_id)) |question| {
        question.value_ptr.is_loopback = true;
    }

    try loopback_questions.put(question_id, {});
    errdefer _ = loopback_questions.remove(question_id);

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(question_id, interface_id, method_id);
    try call.setTargetImportedCap(export_id);

    if (build) |build_fn| {
        try build_fn(ctx, &call);
    }

    try cap_table.encodeCallPayloadCaps(caps, &call, outbound_ctx, on_outbound_cap);
    const bytes = try builder.finish();
    defer allocator.free(bytes);
    try handle_frame(peer, bytes);
    return question_id;
}

pub fn sendCallPromised(
    comptime PeerType: type,
    comptime CallBuildFnType: type,
    comptime QuestionCallbackType: type,
    allocator: std.mem.Allocator,
    caps: *cap_table.CapTable,
    outbound_ctx: ?*anyopaque,
    on_outbound_cap: ?cap_table.CapEntryCallback,
    peer: *PeerType,
    promised: protocol.PromisedAnswer,
    interface_id: u64,
    method_id: u16,
    ctx: *anyopaque,
    build: ?CallBuildFnType,
    on_return: QuestionCallbackType,
    allocate_question: *const fn (*PeerType, *anyopaque, QuestionCallbackType) anyerror!u32,
    remove_question: *const fn (*PeerType, u32) void,
    send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) !u32 {
    const question_id = try allocate_question(peer, ctx, on_return);
    errdefer remove_question(peer, question_id);

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(question_id, interface_id, method_id);
    try call.setTargetPromisedAnswerFrom(promised);

    if (build) |build_fn| {
        try build_fn(ctx, &call);
    }

    try cap_table.encodeCallPayloadCaps(caps, &call, outbound_ctx, on_outbound_cap);
    try send_builder(peer, &builder);
    return question_id;
}

test "peer_call_sender sendCallToImport allocates question and encodes imported target" {
    const State = struct {
        allocator: std.mem.Allocator,
        caps: cap_table.CapTable,
        next_question_id: u32 = 41,
        send_calls: usize = 0,
        sent_question_id: u32 = 0,
        sent_target_id: u32 = 0,

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

    const CallBuildFn = *const fn (*anyopaque, *protocol.CallBuilder) anyerror!void;
    const QuestionCallback = *const fn (*anyopaque, *State, protocol.Return, *const cap_table.InboundCapTable) anyerror!void;

    const Hooks = struct {
        fn allocateQuestion(state: *State, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
            _ = ctx;
            _ = on_return;
            const id = state.next_question_id;
            state.next_question_id +%= 1;
            return id;
        }

        fn sendBuilder(state: *State, builder: *protocol.MessageBuilder) !void {
            const frame = try builder.finish();
            defer state.allocator.free(frame);

            state.send_calls += 1;
            var decoded = try protocol.DecodedMessage.init(state.allocator, frame);
            defer decoded.deinit();
            const call = try decoded.asCall();
            state.sent_question_id = call.question_id;
            state.sent_target_id = call.target.imported_cap orelse return error.MissingCallTarget;
        }

        fn onReturn(_: *anyopaque, _: *State, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}

        fn removeQuestion(_: *State, _: u32) void {}
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();
    var ctx: u8 = 0;
    const question_id = try sendCallToImport(
        State,
        CallBuildFn,
        QuestionCallback,
        state.allocator,
        &state.caps,
        null,
        null,
        &state,
        77,
        0xABCD,
        5,
        &ctx,
        null,
        Hooks.onReturn,
        Hooks.allocateQuestion,
        Hooks.removeQuestion,
        Hooks.sendBuilder,
    );

    try std.testing.expectEqual(@as(u32, 41), question_id);
    try std.testing.expectEqual(@as(usize, 1), state.send_calls);
    try std.testing.expectEqual(@as(u32, 41), state.sent_question_id);
    try std.testing.expectEqual(@as(u32, 77), state.sent_target_id);
}

test "peer_call_sender sendCallToExport marks loopback question and dispatches frame locally" {
    const Question = struct {
        is_loopback: bool = false,
    };
    const State = struct {
        allocator: std.mem.Allocator,
        caps: cap_table.CapTable,
        questions: std.AutoHashMap(u32, Question),
        loopback_questions: std.AutoHashMap(u32, void),
        next_question_id: u32 = 91,
        handled_calls: usize = 0,
        handled_question_id: u32 = 0,
        handled_target_id: u32 = 0,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .caps = cap_table.CapTable.init(allocator),
                .questions = std.AutoHashMap(u32, Question).init(allocator),
                .loopback_questions = std.AutoHashMap(u32, void).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.questions.deinit();
            self.loopback_questions.deinit();
            self.caps.deinit();
        }
    };

    const CallBuildFn = *const fn (*anyopaque, *protocol.CallBuilder) anyerror!void;
    const QuestionCallback = *const fn (*anyopaque, *State, protocol.Return, *const cap_table.InboundCapTable) anyerror!void;

    const Hooks = struct {
        fn allocateQuestion(state: *State, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
            _ = ctx;
            _ = on_return;
            const id = state.next_question_id;
            state.next_question_id +%= 1;
            try state.questions.put(id, .{});
            return id;
        }

        fn handleFrame(state: *State, frame: []const u8) !void {
            state.handled_calls += 1;
            var decoded = try protocol.DecodedMessage.init(state.allocator, frame);
            defer decoded.deinit();
            const call = try decoded.asCall();
            state.handled_question_id = call.question_id;
            state.handled_target_id = call.target.imported_cap orelse return error.MissingCallTarget;
        }

        fn onReturn(_: *anyopaque, _: *State, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();
    var ctx: u8 = 0;
    const question_id = try sendCallToExport(
        State,
        Question,
        CallBuildFn,
        QuestionCallback,
        state.allocator,
        &state.caps,
        null,
        null,
        &state,
        &state.questions,
        &state.loopback_questions,
        44,
        0x1111,
        2,
        &ctx,
        null,
        Hooks.onReturn,
        Hooks.allocateQuestion,
        Hooks.handleFrame,
    );

    try std.testing.expectEqual(@as(u32, 91), question_id);
    try std.testing.expectEqual(@as(usize, 1), state.handled_calls);
    try std.testing.expectEqual(@as(u32, 91), state.handled_question_id);
    try std.testing.expectEqual(@as(u32, 44), state.handled_target_id);
    try std.testing.expect(state.loopback_questions.contains(question_id));
    try std.testing.expect(state.questions.get(question_id).?.is_loopback);
}
