const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const message = capnpc.message;
const Peer = capnpc.rpc.peer.Peer;
const peer_test_hooks = Peer.test_hooks;
const join_state = capnpc.rpc.testing.peer_provide_accept_join.join_state;
const harness = @import("three_party_handoff_harness.zig");

const ReturnCapture = struct {
    allocator: std.mem.Allocator,
    frames: std.ArrayList([]u8) = .empty,

    fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        const copy = try self.allocator.dupe(u8, frame);
        errdefer self.allocator.free(copy);
        try self.frames.append(self.allocator, copy);
    }

    fn deinit(self: *@This()) void {
        for (self.frames.items) |frame| self.allocator.free(frame);
        self.frames.deinit(self.allocator);
    }

    fn countReturns(self: *@This(), answer_id: u32, tag: protocol.ReturnTag) usize {
        var count: usize = 0;
        for (self.frames.items) |frame| {
            var decoded = protocol.DecodedMessage.init(self.allocator, frame) catch continue;
            defer decoded.deinit();
            if (decoded.tag != .@"return") continue;
            const ret = decoded.asReturn() catch continue;
            if (ret.answer_id == answer_id and ret.tag == tag) count += 1;
        }
        return count;
    }

    fn expectException(self: *@This(), answer_id: u32, reason: []const u8) !void {
        for (self.frames.items) |frame| {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (decoded.tag != .@"return") continue;
            const ret = try decoded.asReturn();
            if (ret.answer_id != answer_id or ret.tag != .exception) continue;
            const exception = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings(reason, exception.reason);
            return;
        }
        return error.MissingReturn;
    }

    fn expectProvidedDescriptor(
        self: *@This(),
        answer_id: u32,
        tag: protocol.CapDescriptorTag,
        cap_id: u32,
    ) !void {
        for (self.frames.items) |frame| {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (decoded.tag != .@"return") continue;
            const ret = try decoded.asReturn();
            if (ret.answer_id != answer_id or ret.tag != .results) continue;
            const payload = ret.results orelse return error.MissingResults;
            const cap_list = payload.cap_table orelse return error.MissingCapTable;
            const desc = try protocol.CapDescriptor.fromReader(try cap_list.get(0));
            try std.testing.expectEqual(tag, desc.tag);
            try std.testing.expectEqual(cap_id, desc.id orelse return error.MissingCapId);
            return;
        }
        return error.MissingReturn;
    }
};

const ResultsFailCapture = struct {
    capture: ReturnCapture,
    failed_results: usize = 0,

    fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        var decoded = protocol.DecodedMessage.init(self.capture.allocator, frame) catch {
            try ReturnCapture.onFrame(&self.capture, frame);
            return;
        };
        defer decoded.deinit();
        if (decoded.tag == .@"return") {
            const ret = decoded.asReturn() catch {
                try ReturnCapture.onFrame(&self.capture, frame);
                return;
            };
            if (ret.tag == .results) {
                self.failed_results += 1;
                return error.TestExpectedError;
            }
        }
        try ReturnCapture.onFrame(&self.capture, frame);
    }
};

fn noopCall(_: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
    try peer.sendReturnEmptyStruct(call.question_id);
}

fn addNoopExport(peer: *Peer) !u32 {
    return peer.addExport(.{ .ctx = peer, .on_call = noopCall });
}

fn buildJoinFrame(
    allocator: std.mem.Allocator,
    question_id: u32,
    target_export_id: u32,
    join_id: u32,
    part_count: u16,
    part_num: u16,
) ![]const u8 {
    var key_builder = message.MessageBuilder.init(allocator);
    defer key_builder.deinit();
    var key_root = try key_builder.initRootAnyPointer();
    const key = try key_root.initStruct(1, 0);
    key.writeU32(0, join_id);
    key.writeU16(4, part_count);
    key.writeU16(6, part_num);

    const key_bytes = try key_builder.toBytes();
    defer allocator.free(key_bytes);
    var key_msg = try message.Message.initUnvalidated(allocator, key_bytes);
    defer key_msg.deinit();
    const key_ptr = try key_msg.getRootAnyPointer();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildJoin(
        question_id,
        .{
            .tag = .importedCap,
            .imported_cap = target_export_id,
            .promised_answer = null,
        },
        key_ptr,
    );
    return builder.finish();
}

test "L4 Join waits for all matching parts, returns provided targets, and drains state" {
    const allocator = std.testing.allocator;

    var capture = ReturnCapture{ .allocator = allocator };
    defer capture.deinit();

    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const export_id = try addNoopExport(&peer);

    const first = try buildJoinFrame(allocator, 100, export_id, 77, 2, 0);
    defer allocator.free(first);
    try peer.handleFrame(first);
    try std.testing.expectEqual(@as(usize, 1), peer.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 1), peer.pending_join_questions.count());
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    const second = try buildJoinFrame(allocator, 101, export_id, 77, 2, 1);
    defer allocator.free(second);
    try peer.handleFrame(second);

    try harness.expectNoJoinState(&peer);
    try capture.expectProvidedDescriptor(100, .senderHosted, export_id);
    try capture.expectProvidedDescriptor(101, .senderHosted, export_id);
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(100, .results));
    try std.testing.expectEqual(@as(usize, 1), capture.countReturns(101, .results));
}

test "L4 Join target mismatch sends exceptions for every part and drains state" {
    const allocator = std.testing.allocator;

    var capture = ReturnCapture{ .allocator = allocator };
    defer capture.deinit();

    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const export_a = try addNoopExport(&peer);
    const export_b = try addNoopExport(&peer);

    const first = try buildJoinFrame(allocator, 110, export_a, 88, 2, 0);
    defer allocator.free(first);
    try peer.handleFrame(first);

    const second = try buildJoinFrame(allocator, 111, export_b, 88, 2, 1);
    defer allocator.free(second);
    try peer.handleFrame(second);

    try harness.expectNoJoinState(&peer);
    try capture.expectException(110, "join target mismatch");
    try capture.expectException(111, "join target mismatch");
    try std.testing.expectEqual(@as(usize, 0), capture.countReturns(110, .results));
    try std.testing.expectEqual(@as(usize, 0), capture.countReturns(111, .results));
}

test "Finish before Join completion releases the pending part and empty join bucket" {
    const allocator = std.testing.allocator;

    var capture = ReturnCapture{ .allocator = allocator };
    defer capture.deinit();

    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const export_id = try addNoopExport(&peer);
    const first = try buildJoinFrame(allocator, 120, export_id, 99, 2, 0);
    defer allocator.free(first);
    try peer.handleFrame(first);
    try std.testing.expectEqual(@as(usize, 1), peer.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 1), peer.pending_join_questions.count());

    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = 120,
        .release_result_caps = true,
        .require_early_cancellation = false,
    });

    try harness.expectNoJoinState(&peer);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);
}

test "duplicate Join part reports an exception without replacing the original part" {
    const allocator = std.testing.allocator;

    var capture = ReturnCapture{ .allocator = allocator };
    defer capture.deinit();

    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    const export_id = try addNoopExport(&peer);
    const first = try buildJoinFrame(allocator, 130, export_id, 100, 2, 0);
    defer allocator.free(first);
    try peer.handleFrame(first);

    const duplicate = try buildJoinFrame(allocator, 131, export_id, 100, 2, 0);
    defer allocator.free(duplicate);
    try peer.handleFrame(duplicate);

    try std.testing.expectEqual(@as(usize, 1), peer.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 1), peer.pending_join_questions.count());
    try capture.expectException(131, "duplicate join part");

    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = 130,
        .release_result_caps = true,
        .require_early_cancellation = false,
    });
    try harness.expectNoJoinState(&peer);
}

test "Join provided-target send failure falls back to exceptions and drains state" {
    const allocator = std.testing.allocator;

    var fail_capture = ResultsFailCapture{
        .capture = .{ .allocator = allocator },
    };
    defer fail_capture.capture.deinit();

    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    peer.setSendFrameOverride(&fail_capture, ResultsFailCapture.onFrame);

    const export_id = try addNoopExport(&peer);

    const first = try buildJoinFrame(allocator, 140, export_id, 101, 2, 0);
    defer allocator.free(first);
    try peer.handleFrame(first);

    const second = try buildJoinFrame(allocator, 141, export_id, 101, 2, 1);
    defer allocator.free(second);
    try peer.handleFrame(second);

    try harness.expectNoJoinState(&peer);
    try std.testing.expectEqual(@as(usize, 2), fail_capture.failed_results);
    try fail_capture.capture.expectException(140, "TestExpectedError");
    try fail_capture.capture.expectException(141, "TestExpectedError");
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

fn initTestJoinState(allocator: std.mem.Allocator, part_count: u16) TestJoinState {
    return TestJoinState.init(allocator, part_count);
}

fn cleanupJoinMaps(
    allocator: std.mem.Allocator,
    pending_joins: *std.AutoHashMap(u32, TestJoinState),
    pending_join_questions: *std.AutoHashMap(u32, TestPendingJoinQuestion),
) void {
    var it = pending_joins.valueIterator();
    while (it.next()) |state| state.deinit(allocator);
    pending_joins.deinit();
    pending_join_questions.deinit();
}

fn insertJoinPartOomImpl(allocator: std.mem.Allocator) !void {
    var pending_joins = std.AutoHashMap(u32, TestJoinState).init(allocator);
    var pending_join_questions = std.AutoHashMap(u32, TestPendingJoinQuestion).init(allocator);
    defer cleanupJoinMaps(allocator, &pending_joins, &pending_join_questions);

    const result = join_state.insertJoinPart(
        TestJoinKeyPart,
        TestJoinState,
        TestPendingJoinQuestion,
        TestTarget,
        allocator,
        &pending_joins,
        &pending_join_questions,
        .{ .join_id = 1, .part_count = 1, .part_num = 0 },
        200,
        .{ .id = 7 },
        initTestJoinState,
        TestJoinState.deinit,
    );

    if (result) |outcome| {
        try std.testing.expectEqual(join_state.InsertOutcome.inserted_ready, outcome);
        try std.testing.expectEqual(@as(usize, 1), pending_joins.count());
        try std.testing.expectEqual(@as(usize, 1), pending_join_questions.count());
    } else |err| {
        try std.testing.expectEqual(@as(usize, 0), pending_joins.count());
        try std.testing.expectEqual(@as(usize, 0), pending_join_questions.count());
        return err;
    }
}

test "Join part insertion rolls back a fresh join bucket under OOM injection" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, insertJoinPartOomImpl, .{});
}
