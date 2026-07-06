const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const message = capnpc.message;
const Peer = capnpc.rpc.peer.Peer;
const peer_test_hooks = Peer.test_hooks;
const join_state = capnpc.rpc.testing.peer_provide_accept_join.join_state;
const vat_join = capnpc.rpc.vat.join;
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

    fn expectFinish(self: *@This(), question_id: u32, release_result_caps: bool) !void {
        for (self.frames.items) |frame| {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (decoded.tag != .finish) continue;
            const finish = try decoded.asFinish();
            if (finish.question_id != question_id) continue;
            try std.testing.expectEqual(release_result_caps, finish.release_result_caps);
            return;
        }
        return error.MissingFinish;
    }

    fn countTag(self: *@This(), tag: protocol.MessageTag) usize {
        var count: usize = 0;
        for (self.frames.items) |frame| {
            var decoded = protocol.DecodedMessage.init(self.allocator, frame) catch continue;
            defer decoded.deinit();
            if (decoded.tag == tag) count += 1;
        }
        return count;
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

const FailingSend = struct {
    frames: usize = 0,

    fn onFrame(ctx_ptr: *anyopaque, _: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        self.frames += 1;
        return error.TestExpectedError;
    }
};

const DiscardSend = struct {
    frames: usize = 0,

    fn onFrame(ctx_ptr: *anyopaque, _: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        self.frames += 1;
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

fn buildFinishFrame(allocator: std.mem.Allocator, question_id: u32, release_result_caps: bool) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildFinish(question_id, release_result_caps, false);
    return builder.finish();
}

fn buildReturnResultsFrame(allocator: std.mem.Allocator, answer_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .results);
    var payload = try ret.payloadTyped();
    _ = try payload.initContent();
    _ = try ret.initCapTableTyped(0);
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

const TestJoinCoordinator = struct {
    allocator: std.mem.Allocator,
    peer: *Peer,
    join_id: u32,
    result_imports: std.ArrayList(u32) = .empty,
    question_ids: std.ArrayList(u32) = .empty,
    mismatch_exceptions: u32 = 0,
    cancel_exceptions: u32 = 0,
    unexpected_exceptions: u32 = 0,
    callback_failures: u32 = 0,
    fail_after_next_retain: bool = false,

    fn init(allocator: std.mem.Allocator, peer: *Peer, join_id: u32) TestJoinCoordinator {
        return .{
            .allocator = allocator,
            .peer = peer,
            .join_id = join_id,
        };
    }

    fn deinit(self: *@This()) void {
        self.result_imports.deinit(self.allocator);
        self.question_ids.deinit(self.allocator);
    }

    fn sendPart(
        self: *@This(),
        target_import_id: u32,
        part_count: u16,
        part_num: u16,
    ) !u32 {
        try self.question_ids.ensureUnusedCapacity(self.allocator, 1);
        var key = try JoinKeyPartPointer.init(self.allocator, self.join_id, part_count, part_num);
        defer key.deinit(self.allocator);

        const question_id = try self.peer.sendJoinExperimental(
            .{ .tag = .importedCap, .imported_cap = target_import_id, .promised_answer = null },
            try key.any(),
            self,
            TestJoinCoordinator.onReturn,
        );
        self.question_ids.appendAssumeCapacity(question_id);
        return question_id;
    }

    fn selectedCap(self: *const @This()) !u32 {
        if (self.result_imports.items.len == 0) return error.MissingJoinedCap;
        if (self.mismatch_exceptions != 0 or self.cancel_exceptions != 0 or self.unexpected_exceptions != 0) {
            return error.JoinDidNotSucceed;
        }
        const selected = self.result_imports.items[0];
        for (self.result_imports.items[1..]) |import_id| {
            if (import_id != selected) return error.JoinResultMismatch;
        }
        return selected;
    }

    fn releaseRetained(self: *@This()) !void {
        for (self.result_imports.items) |import_id| {
            try self.peer.releaseImport(import_id, 1);
        }
        self.result_imports.clearRetainingCapacity();
    }

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
                const import_id = switch (resolved) {
                    .imported => |imported| imported.id,
                    else => return error.ExpectedImportedCap,
                };
                try self.result_imports.ensureUnusedCapacity(self.allocator, 1);
                try mutable_caps.retainCapability(cap);
                self.result_imports.appendAssumeCapacity(import_id);
                if (self.fail_after_next_retain) {
                    self.fail_after_next_retain = false;
                    self.callback_failures += 1;
                    return error.TestExpectedError;
                }
            },
            .exception => {
                const exception = ret.exception orelse return error.MissingJoinException;
                if (std.mem.eql(u8, exception.reason, "join target mismatch")) {
                    self.mismatch_exceptions += 1;
                } else if (std.mem.eql(u8, exception.reason, "join canceled")) {
                    self.cancel_exceptions += 1;
                } else {
                    self.unexpected_exceptions += 1;
                }
            },
            else => return error.UnexpectedJoinReturn,
        }
    }
};

const TestL4RuntimeCoordinator = struct {
    allocator: std.mem.Allocator,
    join_network: capnpc.rpc.peer.JoinNetwork,
    join_id: u32,
    expected_results: usize,
    joined: std.ArrayList(vat_join.Joined(Peer)) = .empty,
    accept_import_id: ?u32 = null,
    accept_sent: bool = false,
    mismatch_exceptions: u32 = 0,
    unexpected_exceptions: u32 = 0,

    fn init(
        allocator: std.mem.Allocator,
        join_network_value: capnpc.rpc.peer.JoinNetwork,
        join_id: u32,
        expected_results: usize,
    ) TestL4RuntimeCoordinator {
        return .{
            .allocator = allocator,
            .join_network = join_network_value,
            .join_id = join_id,
            .expected_results = expected_results,
        };
    }

    fn deinit(self: *@This()) void {
        for (self.joined.items) |*joined| joined.deinit(self.allocator);
        self.joined.deinit(self.allocator);
    }

    fn sendPart(self: *@This(), peer: *Peer, target_import_id: u32, part_count: u16, part_num: u16) !u32 {
        var key = try JoinKeyPartPointer.init(self.allocator, self.join_id, part_count, part_num);
        defer key.deinit(self.allocator);
        return try Peer.test_hooks.sendJoinExperimentalRetainedResult(
            peer,
            .{ .tag = .importedCap, .imported_cap = target_import_id, .promised_answer = null },
            try key.any(),
            self,
            TestL4RuntimeCoordinator.onJoinReturn,
        );
    }

    fn acceptFirst(self: *@This()) !void {
        if (self.accept_sent) return;
        if (self.joined.items.len != self.expected_results) return error.MissingJoinResults;
        const first = &self.joined.items[0];
        for (self.joined.items[1..]) |*joined| {
            if (joined.peer != first.peer or !std.mem.eql(u8, joined.provision, first.provision)) {
                return error.JoinResultMismatch;
            }
        }

        var provision_msg = try message.Message.initUnvalidated(self.allocator, first.provision);
        defer provision_msg.deinit();
        const provision = try provision_msg.getRootAnyPointer();
        _ = try first.peer.sendAccept(provision, null, self, TestL4RuntimeCoordinator.onAcceptReturn);
        self.accept_sent = true;
    }

    fn releaseAccepted(self: *@This(), peer: *Peer) !void {
        if (self.accept_import_id) |import_id| {
            try peer.releaseImport(import_id, 1);
            self.accept_import_id = null;
        }
    }

    fn onJoinReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        switch (ret.tag) {
            .results => {
                const payload = ret.results orelse return error.MissingJoinPayload;
                const decoded = try vat_join.decodeJoinResult(payload.content);
                if (!decoded.succeeded or decoded.join_id != self.join_id) return error.JoinResultMismatch;
                const joined = try self.join_network.connectJoined(payload.content);
                errdefer {
                    var rollback = joined;
                    rollback.deinit(self.allocator);
                }
                try self.joined.append(self.allocator, joined);
            },
            .exception => {
                const exception = ret.exception orelse return error.MissingJoinException;
                if (std.mem.eql(u8, exception.reason, "join target mismatch")) {
                    self.mismatch_exceptions += 1;
                } else {
                    self.unexpected_exceptions += 1;
                }
            },
            else => return error.UnexpectedJoinReturn,
        }
    }

    fn onAcceptReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.UnexpectedAcceptReturn;
        const payload = ret.results orelse return error.MissingAcceptPayload;
        var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
        const cap = try payload.content.getCapability();
        const resolved = try mutable_caps.resolveCapability(cap);
        try mutable_caps.retainCapability(cap);
        self.accept_import_id = switch (resolved) {
            .imported => |imported| imported.id,
            else => return error.ExpectedImportedCap,
        };
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

test "L4 JoinResult runtime resolves direct Accept and invokes joined cap" {
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

    var join_net = vat_join.LoopbackJoinNetwork(Peer).init(allocator);
    defer join_net.deinit();
    try join_net.registerDirectPeer(&server, &client);
    server.attachJoinNetwork(join_net.network());

    var number = NumberService{ .value = 9001 };
    const export_id = try server.setBootstrap(.{ .ctx = &number, .on_call = NumberService.onCall });

    var bootstrap = BootstrapCapCallback{};
    _ = try client.sendBootstrap(&bootstrap, BootstrapCapCallback.onReturn);
    const target_import_id = bootstrap.import_id orelse return error.MissingBootstrapImport;
    try std.testing.expectEqual(export_id, target_import_id);

    var coordinator = TestL4RuntimeCoordinator.init(allocator, join_net.network(), 0x4b01, 2);
    defer coordinator.deinit();

    const q0 = try coordinator.sendPart(&client, target_import_id, 2, 0);
    try std.testing.expectEqual(@as(usize, 1), server.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 1), server.pending_join_questions.count());
    try std.testing.expectEqual(@as(usize, 0), server.pending_join_accepts.count());
    try std.testing.expectEqual(@as(usize, 0), coordinator.joined.items.len);

    const q1 = try coordinator.sendPart(&client, target_import_id, 2, 1);
    try std.testing.expectEqual(@as(usize, 0), server.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 0), server.pending_join_questions.count());
    try std.testing.expectEqual(@as(usize, 1), server.pending_join_accepts.count());
    try std.testing.expectEqual(@as(usize, 2), server.pending_join_result_answers.count());
    try std.testing.expectEqual(@as(usize, 1), join_net.registry.count());
    try std.testing.expectEqual(@as(usize, 2), coordinator.joined.items.len);
    try std.testing.expect(coordinator.accept_import_id == null);

    try coordinator.acceptFirst();
    try std.testing.expectEqual(@as(usize, 0), server.pending_join_accepts.count());
    try std.testing.expectEqual(@as(usize, 0), join_net.registry.count());
    const joined_import_id = coordinator.accept_import_id orelse return error.MissingAcceptedJoinCap;
    try std.testing.expectEqual(target_import_id, joined_import_id);

    var number_call = NumberCallCallback{};
    _ = try client.sendCall(joined_import_id, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &number_call, null, NumberCallCallback.onReturn);
    try std.testing.expectEqual(@as(u32, 9001), number_call.result orelse return error.MissingNumberResult);
    try std.testing.expectEqual(@as(u32, 1), number.calls);

    try client.sendFinishForHost(q0, false, false);
    try client.sendFinishForHost(q1, false, false);

    try coordinator.releaseAccepted(&client);
    try client.releaseImport(target_import_id, 1);
    try std.testing.expectEqual(@as(usize, 0), client.questions.count());
    try harness.expectNoJoinState(&client);
    try harness.expectNoJoinState(&server);
}

test "L4 Join relays through transparent proxy exports and accepts direct cap" {
    const allocator = std.testing.allocator;

    var a_to_b = Peer.initDetached(allocator);
    a_to_b.disableThreadAffinity();
    defer a_to_b.deinit();
    var b_to_a = Peer.initDetached(allocator);
    b_to_a.disableThreadAffinity();
    defer b_to_a.deinit();

    var a_to_c = Peer.initDetached(allocator);
    a_to_c.disableThreadAffinity();
    defer a_to_c.deinit();
    var c_to_a = Peer.initDetached(allocator);
    c_to_a.disableThreadAffinity();
    defer c_to_a.deinit();

    var b_to_d = Peer.initDetached(allocator);
    b_to_d.disableThreadAffinity();
    defer b_to_d.deinit();
    var d_to_b = Peer.initDetached(allocator);
    d_to_b.disableThreadAffinity();
    defer d_to_b.deinit();

    var a_to_d = Peer.initDetached(allocator);
    a_to_d.disableThreadAffinity();
    defer a_to_d.deinit();
    var d_to_a = Peer.initDetached(allocator);
    d_to_a.disableThreadAffinity();
    defer d_to_a.deinit();

    var ab_link = ZigJoinLink{ .client = &a_to_b, .server = &b_to_a };
    defer ab_link.forwarding = false;
    a_to_b.setSendFrameOverride(&ab_link, ZigJoinLink.clientToServer);
    b_to_a.setSendFrameOverride(&ab_link, ZigJoinLink.serverToClient);

    var ac_link = ZigJoinLink{ .client = &a_to_c, .server = &c_to_a };
    defer ac_link.forwarding = false;
    a_to_c.setSendFrameOverride(&ac_link, ZigJoinLink.clientToServer);
    c_to_a.setSendFrameOverride(&ac_link, ZigJoinLink.serverToClient);

    var bd_link = ZigJoinLink{ .client = &b_to_d, .server = &d_to_b };
    defer bd_link.forwarding = false;
    b_to_d.setSendFrameOverride(&bd_link, ZigJoinLink.clientToServer);
    d_to_b.setSendFrameOverride(&bd_link, ZigJoinLink.serverToClient);

    var ad_link = ZigJoinLink{ .client = &a_to_d, .server = &d_to_a };
    defer ad_link.forwarding = false;
    a_to_d.setSendFrameOverride(&ad_link, ZigJoinLink.clientToServer);
    d_to_a.setSendFrameOverride(&ad_link, ZigJoinLink.serverToClient);

    var join_net = vat_join.LoopbackJoinNetwork(Peer).init(allocator);
    defer join_net.deinit();
    try join_net.registerDirectPeerWithAcceptHost(&d_to_b, &a_to_d, &d_to_a);
    d_to_b.attachJoinNetwork(join_net.network());

    var number = NumberService{ .value = 0x4444 };
    const d_via_b_export = try d_to_b.addExport(.{ .ctx = &number, .on_call = NumberService.onCall });
    const d_via_a_export = try d_to_a.addExport(.{ .ctx = &number, .on_call = NumberService.onCall });
    try std.testing.expectEqual(d_via_b_export, d_via_a_export);

    const b_proxy_export = try peer_test_hooks.addCrossPeerProxyExport(
        &b_to_a,
        &b_to_d,
        .{ .imported = .{ .id = d_via_b_export } },
        null,
    );
    const c_proxy_export = try peer_test_hooks.addCrossPeerProxyExport(
        &c_to_a,
        &b_to_d,
        .{ .imported = .{ .id = d_via_b_export } },
        null,
    );

    var coordinator = TestL4RuntimeCoordinator.init(allocator, join_net.network(), 0x4c01, 2);
    defer coordinator.deinit();

    const q0 = try coordinator.sendPart(&a_to_b, b_proxy_export, 2, 0);
    try std.testing.expectEqual(@as(usize, 1), b_to_a.pending_join_relays.count());
    try std.testing.expectEqual(@as(usize, 1), b_to_d.cross_peer_join_relay_links.items.len);
    try std.testing.expectEqual(@as(usize, 1), d_to_b.pending_joins.count());

    const q1 = try coordinator.sendPart(&a_to_c, c_proxy_export, 2, 1);
    try std.testing.expectEqual(@as(usize, 1), b_to_a.pending_join_relays.count());
    try std.testing.expectEqual(@as(usize, 1), c_to_a.pending_join_relays.count());
    try std.testing.expectEqual(@as(usize, 2), b_to_d.cross_peer_join_relay_links.items.len);
    try std.testing.expectEqual(@as(usize, 0), d_to_b.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 2), d_to_b.pending_join_result_answers.count());
    try std.testing.expectEqual(@as(usize, 1), d_to_a.pending_join_accepts.count());
    try std.testing.expectEqual(@as(usize, 2), d_to_a.join_accept_host_links.items.len);
    try std.testing.expectEqual(@as(usize, 2), coordinator.joined.items.len);

    try coordinator.acceptFirst();
    try std.testing.expectEqual(@as(usize, 0), d_to_a.pending_join_accepts.count());
    try std.testing.expectEqual(@as(usize, 2), d_to_b.pending_join_result_answers.count());
    const accepted_import_id = coordinator.accept_import_id orelse return error.MissingAcceptedJoinCap;

    var number_call = NumberCallCallback{};
    _ = try a_to_d.sendCall(accepted_import_id, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &number_call, null, NumberCallCallback.onReturn);
    try std.testing.expectEqual(@as(u32, 0x4444), number_call.result orelse return error.MissingNumberResult);
    try std.testing.expectEqual(@as(u32, 1), number.calls);

    try a_to_b.sendFinishForHost(q0, false, false);
    try std.testing.expectEqual(@as(usize, 0), b_to_a.pending_join_relays.count());
    try std.testing.expectEqual(@as(usize, 1), c_to_a.pending_join_relays.count());
    try std.testing.expectEqual(@as(usize, 1), b_to_d.cross_peer_join_relay_links.items.len);
    try std.testing.expectEqual(@as(usize, 1), d_to_b.pending_join_result_answers.count());

    try a_to_c.sendFinishForHost(q1, false, false);
    try coordinator.releaseAccepted(&a_to_d);
    try harness.expectNoJoinState(&a_to_b);
    try harness.expectNoJoinState(&a_to_c);
    try harness.expectNoJoinState(&b_to_a);
    try harness.expectNoJoinState(&c_to_a);
    try harness.expectNoJoinState(&b_to_d);
    try harness.expectNoJoinState(&d_to_b);
    try harness.expectNoJoinState(&a_to_d);
    try harness.expectNoJoinState(&d_to_a);
}

test "L4 Join proxy relay sends downstream Finish when upstream finishes first" {
    const allocator = std.testing.allocator;

    var upstream_capture = ReturnCapture{ .allocator = allocator };
    defer upstream_capture.deinit();
    var source_capture = ReturnCapture{ .allocator = allocator };
    defer source_capture.deinit();

    var owner = Peer.initDetached(allocator);
    owner.disableThreadAffinity();
    defer owner.deinit();
    owner.setSendFrameOverride(&upstream_capture, ReturnCapture.onFrame);

    var source = Peer.initDetached(allocator);
    source.disableThreadAffinity();
    defer source.deinit();
    source.setSendFrameOverride(&source_capture, ReturnCapture.onFrame);

    const proxy_export = try peer_test_hooks.addCrossPeerProxyExport(
        &owner,
        &source,
        .{ .imported = .{ .id = 123 } },
        null,
    );

    const join_frame = try buildJoinFrame(allocator, 42, proxy_export, 0x4c02, 1, 0);
    defer allocator.free(join_frame);
    try owner.handleFrame(join_frame);

    const relay = owner.pending_join_relays.get(42) orelse return error.MissingJoinRelay;
    const downstream_qid = relay.source_question_id;
    try std.testing.expect(source.questions.contains(downstream_qid));
    try std.testing.expectEqual(@as(usize, 1), source.cross_peer_join_relay_links.items.len);
    try std.testing.expectEqual(@as(usize, 1), source_capture.countTag(.join));

    const finish_frame = try buildFinishFrame(allocator, 42, true);
    defer allocator.free(finish_frame);
    try owner.handleFrame(finish_frame);

    try std.testing.expectEqual(@as(usize, 0), owner.pending_join_relays.count());
    try std.testing.expectEqual(@as(usize, 0), source.cross_peer_join_relay_links.items.len);
    try source_capture.expectFinish(downstream_qid, true);
    try std.testing.expect(source.questions.contains(downstream_qid));

    const late_return = try buildReturnResultsFrame(allocator, downstream_qid);
    defer allocator.free(late_return);
    try source.handleFrame(late_return);
    try std.testing.expect(!source.questions.contains(downstream_qid));
    try std.testing.expectEqual(@as(usize, 0), upstream_capture.frames.items.len);
    try harness.expectNoJoinState(&owner);
    try harness.expectNoJoinState(&source);
}

test "L4 Join proxy relay fails cleanly when source peer is unavailable" {
    const allocator = std.testing.allocator;

    var capture = ReturnCapture{ .allocator = allocator };
    defer capture.deinit();

    var owner = Peer.initDetached(allocator);
    owner.disableThreadAffinity();
    defer owner.deinit();
    owner.setSendFrameOverride(&capture, ReturnCapture.onFrame);

    var source = Peer.initDetached(allocator);
    source.disableThreadAffinity();
    const proxy_export = try peer_test_hooks.addCrossPeerProxyExport(
        &owner,
        &source,
        .{ .imported = .{ .id = 321 } },
        null,
    );
    source.deinit();

    const join_frame = try buildJoinFrame(allocator, 43, proxy_export, 0x4c03, 1, 0);
    defer allocator.free(join_frame);
    try owner.handleFrame(join_frame);

    try capture.expectException(43, "cross-peer proxy source disconnected");
    try harness.expectNoJoinState(&owner);
    try std.testing.expectEqual(@as(usize, 0), owner.cross_peer_proxy_links.items.len);
}

test "L4 Join proxy relay downstream send failure drains relay state" {
    const allocator = std.testing.allocator;

    var upstream_capture = ReturnCapture{ .allocator = allocator };
    defer upstream_capture.deinit();
    var failing_send = FailingSend{};

    var owner = Peer.initDetached(allocator);
    owner.disableThreadAffinity();
    defer owner.deinit();
    owner.setSendFrameOverride(&upstream_capture, ReturnCapture.onFrame);

    var source = Peer.initDetached(allocator);
    source.disableThreadAffinity();
    defer source.deinit();
    source.setSendFrameOverride(&failing_send, FailingSend.onFrame);

    const proxy_export = try peer_test_hooks.addCrossPeerProxyExport(
        &owner,
        &source,
        .{ .imported = .{ .id = 444 } },
        null,
    );

    const join_frame = try buildJoinFrame(allocator, 44, proxy_export, 0x4c04, 1, 0);
    defer allocator.free(join_frame);
    try owner.handleFrame(join_frame);

    try upstream_capture.expectException(44, "TestExpectedError");
    try std.testing.expectEqual(@as(usize, 1), failing_send.frames);
    try std.testing.expectEqual(@as(usize, 0), owner.pending_join_relays.count());
    try std.testing.expectEqual(@as(usize, 0), source.cross_peer_join_relay_links.items.len);
    try std.testing.expectEqual(@as(usize, 0), source.questions.count());
    try harness.expectNoJoinState(&owner);
    try harness.expectNoJoinState(&source);
}

test "L4 Join proxy relay owner teardown finishes downstream question" {
    const allocator = std.testing.allocator;

    var source_capture = ReturnCapture{ .allocator = allocator };
    defer source_capture.deinit();

    var owner = Peer.initDetached(allocator);
    owner.disableThreadAffinity();

    var source = Peer.initDetached(allocator);
    source.disableThreadAffinity();
    defer source.deinit();
    source.setSendFrameOverride(&source_capture, ReturnCapture.onFrame);

    const proxy_export = try peer_test_hooks.addCrossPeerProxyExport(
        &owner,
        &source,
        .{ .imported = .{ .id = 777 } },
        null,
    );

    const join_frame = try buildJoinFrame(allocator, 46, proxy_export, 0x4c06, 1, 0);
    defer allocator.free(join_frame);
    try owner.handleFrame(join_frame);
    const downstream_qid = (owner.pending_join_relays.get(46) orelse return error.MissingJoinRelay).source_question_id;
    try std.testing.expectEqual(@as(usize, 1), source.cross_peer_join_relay_links.items.len);

    owner.deinit();
    try std.testing.expectEqual(@as(usize, 0), source.cross_peer_join_relay_links.items.len);
    try source_capture.expectFinish(downstream_qid, false);

    const late_return = try buildReturnResultsFrame(allocator, downstream_qid);
    defer allocator.free(late_return);
    try source.handleFrame(late_return);
    try std.testing.expect(!source.questions.contains(downstream_qid));
    try harness.expectNoJoinState(&source);
}

test "L4 Join proxy relay source teardown neutralizes owner backlink" {
    const allocator = std.testing.allocator;

    var owner_capture = ReturnCapture{ .allocator = allocator };
    defer owner_capture.deinit();
    var source_send = DiscardSend{};

    var owner = Peer.initDetached(allocator);
    owner.disableThreadAffinity();
    defer owner.deinit();
    owner.setSendFrameOverride(&owner_capture, ReturnCapture.onFrame);

    var source = Peer.initDetached(allocator);
    source.disableThreadAffinity();
    source.setSendFrameOverride(&source_send, DiscardSend.onFrame);

    const proxy_export = try peer_test_hooks.addCrossPeerProxyExport(
        &owner,
        &source,
        .{ .imported = .{ .id = 778 } },
        null,
    );

    const join_frame = try buildJoinFrame(allocator, 47, proxy_export, 0x4c07, 1, 0);
    defer allocator.free(join_frame);
    try owner.handleFrame(join_frame);
    try std.testing.expectEqual(@as(usize, 1), owner.pending_join_relays.count());
    try std.testing.expectEqual(@as(usize, 1), source.cross_peer_join_relay_links.items.len);

    source.deinit();
    try std.testing.expectEqual(@as(usize, 0), owner.pending_join_relays.count());
    try owner_capture.expectException(47, capnpc.rpc.peer.disconnected_reason);
    try harness.expectNoJoinState(&owner);
}

test "L4 Join proxy relay propagates downstream target mismatch" {
    const allocator = std.testing.allocator;

    var owner_capture = ReturnCapture{ .allocator = allocator };
    defer owner_capture.deinit();

    var owner = Peer.initDetached(allocator);
    owner.disableThreadAffinity();
    defer owner.deinit();
    owner.setSendFrameOverride(&owner_capture, ReturnCapture.onFrame);

    var source = Peer.initDetached(allocator);
    source.disableThreadAffinity();
    defer source.deinit();
    var host = Peer.initDetached(allocator);
    host.disableThreadAffinity();
    defer host.deinit();

    var link = ZigJoinLink{ .client = &source, .server = &host };
    defer link.forwarding = false;
    source.setSendFrameOverride(&link, ZigJoinLink.clientToServer);
    host.setSendFrameOverride(&link, ZigJoinLink.serverToClient);

    const export_a = try addNoopExport(&host);
    const export_b = try addNoopExport(&host);
    const proxy_a = try peer_test_hooks.addCrossPeerProxyExport(
        &owner,
        &source,
        .{ .imported = .{ .id = export_a } },
        null,
    );
    const proxy_b = try peer_test_hooks.addCrossPeerProxyExport(
        &owner,
        &source,
        .{ .imported = .{ .id = export_b } },
        null,
    );

    const first = try buildJoinFrame(allocator, 48, proxy_a, 0x4c08, 2, 0);
    defer allocator.free(first);
    try owner.handleFrame(first);
    try std.testing.expectEqual(@as(usize, 1), owner.pending_join_relays.count());

    const second = try buildJoinFrame(allocator, 49, proxy_b, 0x4c08, 2, 1);
    defer allocator.free(second);
    try owner.handleFrame(second);

    try owner_capture.expectException(48, "join target mismatch");
    try owner_capture.expectException(49, "join target mismatch");
    try harness.expectNoJoinState(&owner);
    try harness.expectNoJoinState(&source);
    try harness.expectNoJoinState(&host);
}

fn crossPeerJoinRelayOomImpl(allocator: std.mem.Allocator) !void {
    var owner_send = DiscardSend{};
    var source_send = DiscardSend{};

    var owner = Peer.initDetached(allocator);
    owner.disableThreadAffinity();
    defer owner.deinit();
    owner.setSendFrameOverride(&owner_send, DiscardSend.onFrame);

    var source = Peer.initDetached(allocator);
    source.disableThreadAffinity();
    defer source.deinit();
    source.setSendFrameOverride(&source_send, DiscardSend.onFrame);

    const proxy_export = try peer_test_hooks.addCrossPeerProxyExport(
        &owner,
        &source,
        .{ .imported = .{ .id = 555 } },
        null,
    );

    const join_frame = try buildJoinFrame(allocator, 45, proxy_export, 0x4c05, 1, 0);
    defer allocator.free(join_frame);

    const handled = owner.handleFrame(join_frame);
    if (handled) {
        if (owner.pending_join_relays.get(45)) |relay| {
            const downstream_qid = relay.source_question_id;
            const finish_frame = try buildFinishFrame(allocator, 45, false);
            defer allocator.free(finish_frame);
            try owner.handleFrame(finish_frame);

            const late_return = try buildReturnResultsFrame(allocator, downstream_qid);
            defer allocator.free(late_return);
            try source.handleFrame(late_return);
        }
        try harness.expectNoJoinState(&owner);
        try harness.expectNoJoinState(&source);
    } else |err| {
        try harness.expectNoJoinState(&owner);
        try harness.expectNoJoinState(&source);
        return err;
    }
}

test "L4 Join proxy relay rolls back state under OOM injection" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, crossPeerJoinRelayOomImpl, .{});
}

test "L4 JoinResult send failure drains pending direct Accept state" {
    const allocator = std.testing.allocator;

    var fail_capture = ResultsFailCapture{
        .capture = .{ .allocator = allocator },
    };
    defer fail_capture.capture.deinit();

    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    peer.setSendFrameOverride(&fail_capture, ResultsFailCapture.onFrame);

    var join_net = vat_join.LoopbackJoinNetwork(Peer).init(allocator);
    defer join_net.deinit();
    try join_net.registerDirectPeer(&peer, &peer);
    peer.attachJoinNetwork(join_net.network());

    const export_id = try addNoopExport(&peer);

    const first = try buildJoinFrame(allocator, 150, export_id, 0x4b02, 2, 0);
    defer allocator.free(first);
    try peer.handleFrame(first);
    try std.testing.expectEqual(@as(usize, 1), peer.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_join_accepts.count());

    const second = try buildJoinFrame(allocator, 151, export_id, 0x4b02, 2, 1);
    defer allocator.free(second);
    try peer.handleFrame(second);

    try harness.expectNoJoinState(&peer);
    try std.testing.expectEqual(@as(usize, 0), peer.pending_join_accepts.count());
    try std.testing.expectEqual(@as(usize, 0), join_net.registry.count());
    try std.testing.expectEqual(@as(usize, 2), fail_capture.failed_results);
    try fail_capture.capture.expectException(150, "TestExpectedError");
    try fail_capture.capture.expectException(151, "TestExpectedError");
}

test "L4 JoinResult Finish before direct Accept drains pending provision" {
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

    var join_net = vat_join.LoopbackJoinNetwork(Peer).init(allocator);
    defer join_net.deinit();
    try join_net.registerDirectPeer(&server, &client);
    server.attachJoinNetwork(join_net.network());

    const export_id = try addNoopExport(&server);
    var coordinator = TestL4RuntimeCoordinator.init(allocator, join_net.network(), 0x4b03, 2);
    defer coordinator.deinit();

    const q0 = try coordinator.sendPart(&client, export_id, 2, 0);
    const q1 = try coordinator.sendPart(&client, export_id, 2, 1);

    try std.testing.expectEqual(@as(usize, 0), server.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 0), server.pending_join_questions.count());
    try std.testing.expectEqual(@as(usize, 1), server.pending_join_accepts.count());
    try std.testing.expectEqual(@as(usize, 2), server.pending_join_result_answers.count());
    try std.testing.expectEqual(@as(usize, 1), join_net.registry.count());

    try client.sendFinishForHost(q0, false, false);
    try std.testing.expectEqual(@as(usize, 1), server.pending_join_accepts.count());
    try std.testing.expectEqual(@as(usize, 1), server.pending_join_result_answers.count());
    try std.testing.expectEqual(@as(usize, 1), join_net.registry.count());

    try client.sendFinishForHost(q1, false, false);
    try harness.expectNoJoinState(&server);
    try std.testing.expectEqual(@as(usize, 0), join_net.registry.count());
    try std.testing.expectEqual(@as(usize, 0), client.questions.count());
}

test "L4 JoinResult peer deinit cancels pending direct Accept provision" {
    const allocator = std.testing.allocator;

    var capture = ReturnCapture{ .allocator = allocator };
    defer capture.deinit();

    var join_net = vat_join.LoopbackJoinNetwork(Peer).init(allocator);
    defer join_net.deinit();

    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    peer.setSendFrameOverride(&capture, ReturnCapture.onFrame);
    peer.attachJoinNetwork(join_net.network());
    try join_net.registerDirectPeer(&peer, &peer);

    const export_id = try addNoopExport(&peer);

    const first = try buildJoinFrame(allocator, 170, export_id, 0x4b04, 2, 0);
    defer allocator.free(first);
    try peer.handleFrame(first);

    const second = try buildJoinFrame(allocator, 171, export_id, 0x4b04, 2, 1);
    defer allocator.free(second);
    try peer.handleFrame(second);

    try std.testing.expectEqual(@as(usize, 1), peer.pending_join_accepts.count());
    try std.testing.expectEqual(@as(usize, 2), peer.pending_join_result_answers.count());
    try std.testing.expectEqual(@as(usize, 1), join_net.registry.count());

    peer.deinit();
    try std.testing.expectEqual(@as(usize, 0), join_net.registry.count());
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

test "test-local JoinCoordinator selects a callable cap and releases retained imports" {
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

    var number = NumberService{ .value = 5150 };
    const export_id = try server.setBootstrap(.{ .ctx = &number, .on_call = NumberService.onCall });

    var bootstrap = BootstrapCapCallback{};
    _ = try client.sendBootstrap(&bootstrap, BootstrapCapCallback.onReturn);
    const target_import_id = bootstrap.import_id orelse return error.MissingBootstrapImport;
    try std.testing.expectEqual(export_id, target_import_id);

    var coordinator = TestJoinCoordinator.init(allocator, &client, 0x4a10);
    defer coordinator.deinit();
    _ = try coordinator.sendPart(target_import_id, 2, 0);
    _ = try coordinator.sendPart(target_import_id, 2, 1);

    try harness.expectNoJoinState(&server);
    try std.testing.expectEqual(@as(usize, 2), coordinator.result_imports.items.len);
    const selected = try coordinator.selectedCap();
    try std.testing.expectEqual(target_import_id, selected);
    try std.testing.expect(client.caps.hasImport(selected));

    var number_call = NumberCallCallback{};
    _ = try client.sendCall(selected, NUMBER_INTERFACE_ID, GET_NUMBER_METHOD_ID, &number_call, null, NumberCallCallback.onReturn);
    try std.testing.expectEqual(@as(u32, 5150), number_call.result orelse return error.MissingNumberResult);
    try std.testing.expectEqual(@as(u32, 1), number.calls);

    try coordinator.releaseRetained();
    try std.testing.expect(client.caps.hasImport(target_import_id));
    try client.releaseImport(target_import_id, 1);
    try std.testing.expect(!client.caps.hasImport(target_import_id));
    try std.testing.expectEqual(@as(usize, 0), client.questions.count());
}

test "test-local JoinCoordinator records mismatch without retaining joined caps" {
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

    var coordinator = TestJoinCoordinator.init(allocator, &client, 0x4a11);
    defer coordinator.deinit();
    _ = try coordinator.sendPart(export_a, 2, 0);
    _ = try coordinator.sendPart(export_b, 2, 1);

    try harness.expectNoJoinState(&server);
    try std.testing.expectEqual(@as(usize, 0), coordinator.result_imports.items.len);
    try std.testing.expectEqual(@as(u32, 2), coordinator.mismatch_exceptions);
    try std.testing.expectEqual(@as(usize, 0), client.questions.count());
}

test "test-local JoinCoordinator cancel drains remote partial Join state" {
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

    const export_id = try addNoopExport(&server);
    var coordinator = TestJoinCoordinator.init(allocator, &client, 0x4a12);
    defer coordinator.deinit();
    const question_id = try coordinator.sendPart(export_id, 2, 0);

    try std.testing.expectEqual(@as(usize, 1), server.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 1), server.pending_join_questions.count());

    try client.cancelQuestion(question_id, "join canceled");

    try harness.expectNoJoinState(&server);
    try std.testing.expectEqual(@as(u32, 1), coordinator.cancel_exceptions);
    try std.testing.expectEqual(@as(usize, 0), coordinator.result_imports.items.len);
    const canceled = client.questions.get(question_id) orelse return error.MissingCanceledQuestion;
    try std.testing.expect(canceled.cancelled);
    peer_test_hooks.removeQuestion(&client, question_id);
    try std.testing.expectEqual(@as(usize, 0), client.questions.count());
}

test "test-local JoinCoordinator callback failure after retention leaves retained cap releasable" {
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

    const export_id = try addNoopExport(&server);
    var coordinator = TestJoinCoordinator.init(allocator, &client, 0x4a13);
    defer coordinator.deinit();
    coordinator.fail_after_next_retain = true;

    _ = try coordinator.sendPart(export_id, 1, 0);

    try harness.expectNoJoinState(&server);
    try std.testing.expectEqual(@as(usize, 1), coordinator.result_imports.items.len);
    try std.testing.expectEqual(@as(u32, 1), coordinator.callback_failures);
    try std.testing.expectEqual(@as(u32, 0), coordinator.unexpected_exceptions);
    try std.testing.expectEqual(@as(usize, 0), client.questions.count());
    const retained = coordinator.result_imports.items[0];
    try std.testing.expect(client.caps.hasImport(retained));
    try coordinator.releaseRetained();
    try std.testing.expect(!client.caps.hasImport(retained));
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
