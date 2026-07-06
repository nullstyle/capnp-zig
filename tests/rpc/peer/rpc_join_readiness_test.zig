const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const message = capnpc.message;
const Peer = capnpc.rpc.peer.Peer;
const peer_test_hooks = Peer.test_hooks;
const join_state = capnpc.rpc.testing.peer_provide_accept_join.join_state;
const harness = @import("three_party_handoff_harness.zig");

const NUMBER_INTERFACE_ID: u64 = 0x4c34_4a4f_494e_0001;
const GET_NUMBER_METHOD_ID: u16 = 0;

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

const JoinKeyPartPointer = struct {
    bytes: []const u8,
    msg: message.Message,

    fn init(
        allocator: std.mem.Allocator,
        join_id: u32,
        part_count: u16,
        part_num: u16,
    ) !JoinKeyPartPointer {
        var builder = message.MessageBuilder.init(allocator);
        defer builder.deinit();
        const root = try builder.initRootAnyPointer();
        const key_part_struct = try root.initStruct(1, 0);
        key_part_struct.writeU32(0, join_id);
        key_part_struct.writeU16(4, part_count);
        key_part_struct.writeU16(6, part_num);
        const bytes = try builder.toBytes();
        errdefer allocator.free(bytes);
        const msg = try message.Message.init(allocator, bytes, .{});
        return .{ .bytes = bytes, .msg = msg };
    }

    fn deinit(self: *JoinKeyPartPointer, allocator: std.mem.Allocator) void {
        self.msg.deinit();
        allocator.free(self.bytes);
    }

    fn any(self: *JoinKeyPartPointer) !message.AnyPointerReader {
        return self.msg.getRootAnyPointer();
    }
};

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

const ZigJoinLink = struct {
    client: *Peer,
    server: *Peer,
    forwarding: bool = true,

    fn clientToServer(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (!self.forwarding) return;
        try self.server.handleFrame(frame);
    }

    fn serverToClient(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (!self.forwarding) return;
        try self.client.handleFrame(frame);
    }
};

const NumberService = struct {
    value: u32,
    calls: u32 = 0,

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (call.interface_id != NUMBER_INTERFACE_ID or call.method_id != GET_NUMBER_METHOD_ID) {
            return error.UnexpectedMethod;
        }
        self.calls += 1;

        const BuildCtx = struct {
            n: u32,

            fn build(build_ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const build_ctx: *const @This() = @ptrCast(@alignCast(build_ctx_ptr));
                var payload = try ret.payloadTyped();
                const any = try payload.initContent();
                const results = try any.initStruct(1, 0);
                results.writeU32(0, build_ctx.n);
            }
        };
        var build_ctx = BuildCtx{ .n = self.value };
        try peer.sendReturnResults(call.question_id, &build_ctx, BuildCtx.build);
    }
};

const BootstrapCapCallback = struct {
    import_id: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.UnexpectedBootstrapReturn;
        const payload = ret.results orelse return error.MissingBootstrapPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.import_id = switch (resolved) {
            .imported => |imported| imported.id,
            else => return error.ExpectedImportedCap,
        };
    }
};

const JoinCapCallback = struct {
    import_id: ?u32 = null,
    saw_join_mismatch_exception: bool = false,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse return error.MissingJoinPayload;
                var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
                const cap = try payload.content.getCapability();
                const resolved = try mutable_caps.resolveCapability(cap);
                try mutable_caps.retainCapability(cap);
                self.import_id = switch (resolved) {
                    .imported => |imported| imported.id,
                    else => return error.ExpectedImportedCap,
                };
            },
            .exception => {
                const exception = ret.exception orelse return error.MissingJoinException;
                self.saw_join_mismatch_exception = std.mem.eql(u8, exception.reason, "join target mismatch");
            },
            else => return error.UnexpectedJoinReturn,
        }
    }
};

const NumberCallCallback = struct {
    result: ?u32 = null,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.UnexpectedNumberReturn;
        const payload = ret.results orelse return error.MissingNumberPayload;
        const content = try payload.content.getStruct();
        self.result = content.readU32(0);
    }
};

test "sendJoinExperimental originates Zig-to-Zig Join and returned cap is callable" {
    const allocator = std.testing.allocator;

    var client = Peer.initDetached(allocator);
    client.disableThreadAffinity();
    defer client.deinit();
    var server = Peer.initDetached(allocator);
    server.disableThreadAffinity();
    defer server.deinit();

    var link = ZigJoinLink{ .client = &client, .server = &server };
    defer link.forwarding = false;
    client.setSendFrameOverride(&link, ZigJoinLink.clientToServer);
    server.setSendFrameOverride(&link, ZigJoinLink.serverToClient);

    var number = NumberService{ .value = 4242 };
    const export_id = try server.setBootstrap(.{ .ctx = &number, .on_call = NumberService.onCall });

    var bootstrap = BootstrapCapCallback{};
    _ = try client.sendBootstrap(&bootstrap, BootstrapCapCallback.onReturn);
    const target_import_id = bootstrap.import_id orelse return error.MissingBootstrapImport;
    try std.testing.expectEqual(export_id, target_import_id);

    var key0 = try JoinKeyPartPointer.init(allocator, 0x4a01, 2, 0);
    defer key0.deinit(allocator);
    var key1 = try JoinKeyPartPointer.init(allocator, 0x4a01, 2, 1);
    defer key1.deinit(allocator);

    var join0 = JoinCapCallback{};
    const join0_qid = try client.sendJoinExperimental(
        .{ .tag = .importedCap, .imported_cap = target_import_id, .promised_answer = null },
        try key0.any(),
        &join0,
        JoinCapCallback.onReturn,
    );
    try std.testing.expect(client.questions.contains(join0_qid));
    try std.testing.expectEqual(@as(usize, 1), server.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 1), server.pending_join_questions.count());
    try std.testing.expect(join0.import_id == null);

    var join1 = JoinCapCallback{};
    _ = try client.sendJoinExperimental(
        .{ .tag = .importedCap, .imported_cap = target_import_id, .promised_answer = null },
        try key1.any(),
        &join1,
        JoinCapCallback.onReturn,
    );

    try harness.expectNoJoinState(&server);
    try std.testing.expect(!client.questions.contains(join0_qid));
    const joined_import_id = join0.import_id orelse return error.MissingFirstJoinImport;
    const joined_import_id_2 = join1.import_id orelse return error.MissingSecondJoinImport;
    try std.testing.expectEqual(target_import_id, joined_import_id);
    try std.testing.expectEqual(target_import_id, joined_import_id_2);

    var number_call = NumberCallCallback{};
    _ = try client.sendCall(joined_import_id, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &number_call, null, NumberCallCallback.onReturn);
    try std.testing.expectEqual(@as(u32, 4242), number_call.result orelse return error.MissingNumberResult);
    try std.testing.expectEqual(@as(u32, 1), number.calls);

    try client.releaseImport(joined_import_id, 1);
    try client.releaseImport(joined_import_id_2, 1);
    try client.releaseImport(target_import_id, 1);
    try std.testing.expectEqual(@as(usize, 0), client.questions.count());
    try harness.expectNoJoinState(&client);
}

test "sendJoinExperimental mismatch Return is delivered as exception and drains state" {
    const allocator = std.testing.allocator;

    var client = Peer.initDetached(allocator);
    client.disableThreadAffinity();
    defer client.deinit();
    var server = Peer.initDetached(allocator);
    server.disableThreadAffinity();
    defer server.deinit();

    var link = ZigJoinLink{ .client = &client, .server = &server };
    defer link.forwarding = false;
    client.setSendFrameOverride(&link, ZigJoinLink.clientToServer);
    server.setSendFrameOverride(&link, ZigJoinLink.serverToClient);

    const export_a = try addNoopExport(&server);
    const export_b = try addNoopExport(&server);

    var key0 = try JoinKeyPartPointer.init(allocator, 0x4a02, 2, 0);
    defer key0.deinit(allocator);
    var key1 = try JoinKeyPartPointer.init(allocator, 0x4a02, 2, 1);
    defer key1.deinit(allocator);

    var join0 = JoinCapCallback{};
    var join1 = JoinCapCallback{};
    _ = try client.sendJoinExperimental(
        .{ .tag = .importedCap, .imported_cap = export_a, .promised_answer = null },
        try key0.any(),
        &join0,
        JoinCapCallback.onReturn,
    );
    _ = try client.sendJoinExperimental(
        .{ .tag = .importedCap, .imported_cap = export_b, .promised_answer = null },
        try key1.any(),
        &join1,
        JoinCapCallback.onReturn,
    );

    try harness.expectNoJoinState(&server);
    try std.testing.expect(join0.saw_join_mismatch_exception);
    try std.testing.expect(join1.saw_join_mismatch_exception);
    try std.testing.expectEqual(@as(usize, 0), client.questions.count());
}

fn sendJoinExperimentalOomImpl(allocator: std.mem.Allocator) !void {
    var capture = ReturnCapture{ .allocator = allocator };
    defer capture.deinit();

    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    var key = try JoinKeyPartPointer.init(allocator, 0x4a03, 1, 0);
    defer key.deinit(allocator);

    var join = JoinCapCallback{};
    const result = peer.sendJoinExperimental(
        .{ .tag = .importedCap, .imported_cap = 7, .promised_answer = null },
        try key.any(),
        &join,
        JoinCapCallback.onReturn,
    );

    if (result) |question_id| {
        try std.testing.expect(peer.questions.contains(question_id));
        peer_test_hooks.removeQuestion(&peer, question_id);
        try std.testing.expectEqual(@as(usize, 0), peer.questions.count());
    } else |err| {
        try std.testing.expectEqual(@as(usize, 0), peer.questions.count());
        return err;
    }
}

test "sendJoinExperimental rolls back the outbound question under OOM injection" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sendJoinExperimentalOomImpl, .{});
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
