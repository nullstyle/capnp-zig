const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.protocol;
const peer_impl = capnpc.rpc.peer;
const peer_dispatch = capnpc.rpc._internal.peer_dispatch;
const cap_table = capnpc.rpc.cap_table;
const Connection = capnpc.rpc.connection.Connection;
const Peer = peer_impl.Peer;

const Capture = struct {
    allocator: std.mem.Allocator,
    frames: std.ArrayList([]u8),

    fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
        const copy = try ctx.allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, copy, frame);
        try ctx.frames.append(ctx.allocator, copy);
    }

    fn deinit(self: *@This()) void {
        for (self.frames.items) |frame| self.allocator.free(frame);
        self.frames.deinit(self.allocator);
    }
};

const NoopHandler = struct {
    fn onCall(
        ctx: *anyopaque,
        called_peer: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        _ = ctx;
        _ = called_peer;
        _ = call;
        _ = inbound_caps;
    }
};

const TextPointer = struct {
    bytes: []const u8,
    msg: message.Message,

    fn init(allocator: std.mem.Allocator, text: []const u8) !TextPointer {
        var builder = message.MessageBuilder.init(allocator);
        defer builder.deinit();
        const root = try builder.initRootAnyPointer();
        try root.setText(text);
        const bytes = try builder.toBytes();
        errdefer allocator.free(bytes);
        const msg = try message.Message.init(allocator, bytes);
        return .{
            .bytes = bytes,
            .msg = msg,
        };
    }

    fn deinit(self: *TextPointer, allocator: std.mem.Allocator) void {
        self.msg.deinit();
        allocator.free(self.bytes);
    }

    fn any(self: *TextPointer) !message.AnyPointerReader {
        return self.msg.getRootAnyPointer();
    }
};

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
        var key_part_struct = try root.initStruct(1, 0);
        key_part_struct.writeU32(0, join_id);
        key_part_struct.writeU16(4, part_count);
        key_part_struct.writeU16(6, part_num);
        const bytes = try builder.toBytes();
        errdefer allocator.free(bytes);
        const msg = try message.Message.init(allocator, bytes);
        return .{
            .bytes = bytes,
            .msg = msg,
        };
    }

    fn deinit(self: *JoinKeyPartPointer, allocator: std.mem.Allocator) void {
        self.msg.deinit();
        allocator.free(self.bytes);
    }

    fn any(self: *JoinKeyPartPointer) !message.AnyPointerReader {
        return self.msg.getRootAnyPointer();
    }
};

test "peer sendCallResolved rejects unavailable capability" {
    const allocator = std.testing.allocator;

    const Callback = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var ctx: u8 = 0;
    try std.testing.expectError(
        error.CapabilityUnavailable,
        peer.sendCallResolved(.none, 0x12_34_56_78, 9, &ctx, null, Callback.onReturn),
    );
}

test "peer sendReleaseForHost emits release frame" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    try peer.sendReleaseForHost(77, 3);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.release, decoded.tag);
    const release = try decoded.asRelease();
    try std.testing.expectEqual(@as(u32, 77), release.id);
    try std.testing.expectEqual(@as(u32, 3), release.reference_count);
}

test "peer sendFinishForHost emits finish frame with explicit flags" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    try peer.sendFinishForHost(91, true, true);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, decoded.tag);
    const finish = try decoded.asFinish();
    try std.testing.expectEqual(@as(u32, 91), finish.question_id);
    try std.testing.expect(finish.release_result_caps);
    try std.testing.expect(finish.require_early_cancellation);
}

test "peer handleFrame abort updates last inbound tag and abort reason" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildAbort("fatal-route-test");
    const frame = try builder.finish();
    defer allocator.free(frame);

    try std.testing.expectError(error.RemoteAbort, peer.handleFrame(frame));
    try std.testing.expectEqual(protocol.MessageTag.abort, peer.getLastInboundTag().?);
    try std.testing.expectEqualStrings("fatal-route-test", peer.getLastRemoteAbortReason().?);
}

test "peer dispatch route maps message tags" {
    try std.testing.expectEqual(peer_dispatch.InboundRoute.call, peer_dispatch.route(.call));
    try std.testing.expectEqual(peer_dispatch.InboundRoute.@"return", peer_dispatch.route(.@"return"));
    try std.testing.expectEqual(peer_dispatch.InboundRoute.thirdPartyAnswer, peer_dispatch.route(.thirdPartyAnswer));
    try std.testing.expectEqual(peer_dispatch.InboundRoute.unknown, peer_dispatch.route(.obsoleteSave));
    try std.testing.expectEqual(peer_dispatch.InboundRoute.unknown, peer_dispatch.route(.obsoleteDelete));
}

test "peer provide+accept returns provided capability" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient = try TextPointer.init(allocator, "recipient-A");
    defer recipient.deinit(allocator);

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        100,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try recipient.any(),
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(101, try recipient.any(), null);
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);
    try peer.handleFrame(accept_frame);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 101), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, descriptor.tag);
    try std.testing.expectEqual(export_id, descriptor.id.?);
}

test "peer finish clears stored provide entry" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient = try TextPointer.init(allocator, "recipient-B");
    defer recipient.deinit(allocator);

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        200,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try recipient.any(),
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    var finish_builder = protocol.MessageBuilder.init(allocator);
    defer finish_builder.deinit();
    try finish_builder.buildFinish(200, false, false);
    const finish_frame = try finish_builder.finish();
    defer allocator.free(finish_frame);
    try peer.handleFrame(finish_frame);

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(201, try recipient.any(), null);
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);
    try peer.handleFrame(accept_frame);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 201), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("unknown provision", ex.reason);
}

test "peer accept with embargo waits for disembargo.accept" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient = try TextPointer.init(allocator, "recipient-embargo");
    defer recipient.deinit(allocator);

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        210,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try recipient.any(),
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(211, try recipient.any(), "accept-embargo");
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);
    try peer.handleFrame(accept_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var disembargo_builder = protocol.MessageBuilder.init(allocator);
    defer disembargo_builder.deinit();
    try disembargo_builder.buildDisembargoAccept(
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        "accept-embargo",
    );
    const disembargo_frame = try disembargo_builder.finish();
    defer allocator.free(disembargo_frame);
    try peer.handleFrame(disembargo_frame);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 211), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, descriptor.tag);
    try std.testing.expectEqual(export_id, descriptor.id.?);
}

test "peer finish cancels pending embargoed accept" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient = try TextPointer.init(allocator, "recipient-embargo-finish");
    defer recipient.deinit(allocator);

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        220,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try recipient.any(),
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(221, try recipient.any(), "accept-embargo-finish");
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);
    try peer.handleFrame(accept_frame);

    var finish_builder = protocol.MessageBuilder.init(allocator);
    defer finish_builder.deinit();
    try finish_builder.buildFinish(221, false, false);
    const finish_frame = try finish_builder.finish();
    defer allocator.free(finish_frame);
    try peer.handleFrame(finish_frame);

    var disembargo_builder = protocol.MessageBuilder.init(allocator);
    defer disembargo_builder.deinit();
    try disembargo_builder.buildDisembargoAccept(
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        "accept-embargo-finish",
    );
    const disembargo_frame = try disembargo_builder.finish();
    defer allocator.free(disembargo_frame);
    try peer.handleFrame(disembargo_frame);

    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);
}

test "peer pipelined call to embargoed accept answer waits for disembargo.accept" {
    const allocator = std.testing.allocator;

    const OrderedHandler = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = inbound_caps;
            const called: *bool = @ptrCast(@alignCast(ctx));
            called.* = true;
            try called_peer.sendReturnException(call.question_id, "ordered");
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_called = false;
    const export_id = try peer.addExport(.{
        .ctx = &handler_called,
        .on_call = OrderedHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient = try TextPointer.init(allocator, "recipient-embargo-pipeline");
    defer recipient.deinit(allocator);

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        230,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try recipient.any(),
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(231, try recipient.any(), "accept-embargo-pipeline");
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);
    try peer.handleFrame(accept_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var pipelined_builder = protocol.MessageBuilder.init(allocator);
    defer pipelined_builder.deinit();
    var pipelined_call = try pipelined_builder.beginCall(232, 0x1234, 1);
    try pipelined_call.setTargetPromisedAnswerFrom(.{
        .question_id = 231,
        .transform = .{ .list = null },
    });
    _ = try pipelined_call.initCapTableTyped(0);

    const pipelined_frame = try pipelined_builder.finish();
    defer allocator.free(pipelined_frame);
    try peer.handleFrame(pipelined_frame);

    try std.testing.expect(!handler_called);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var disembargo_builder = protocol.MessageBuilder.init(allocator);
    defer disembargo_builder.deinit();
    try disembargo_builder.buildDisembargoAccept(
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        "accept-embargo-pipeline",
    );
    const disembargo_frame = try disembargo_builder.finish();
    defer allocator.free(disembargo_frame);
    try peer.handleFrame(disembargo_frame);

    try std.testing.expect(handler_called);
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var accept_ret = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer accept_ret.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", accept_ret.tag);
    const accept_return = try accept_ret.asReturn();
    try std.testing.expectEqual(@as(u32, 231), accept_return.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, accept_return.tag);

    var pipelined_ret = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer pipelined_ret.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", pipelined_ret.tag);
    const replayed_return = try pipelined_ret.asReturn();
    try std.testing.expectEqual(@as(u32, 232), replayed_return.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, replayed_return.tag);
    const ex = replayed_return.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("ordered", ex.reason);
}

test "peer handleFrame embargoed accept + promised calls preserve ordering under stress" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: usize = 0,
    };
    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = inbound_caps;
            const state: *ServerCtx = @ptrCast(@alignCast(ctx));
            state.called += 1;
            try called_peer.sendReturnException(call.question_id, "stress-ordered");
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient = try TextPointer.init(allocator, "stress-accept-recipient");
    defer recipient.deinit(allocator);

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        1200,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try recipient.any(),
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    const embargo = "stress-accept-embargo";
    const rounds: u32 = 64;
    var round: u32 = 0;
    while (round < rounds) : (round += 1) {
        const frame_start = capture.frames.items.len;
        const accept_qid: u32 = 1300 + round * 2;
        const call_qid: u32 = accept_qid + 1;

        var accept_builder = protocol.MessageBuilder.init(allocator);
        defer accept_builder.deinit();
        try accept_builder.buildAccept(accept_qid, try recipient.any(), embargo);
        const accept_frame = try accept_builder.finish();
        defer allocator.free(accept_frame);
        try peer.handleFrame(accept_frame);

        var pipelined_builder = protocol.MessageBuilder.init(allocator);
        defer pipelined_builder.deinit();
        var pipelined_call = try pipelined_builder.beginCall(call_qid, 0x1234, 1);
        try pipelined_call.setTargetPromisedAnswer(accept_qid);
        _ = try pipelined_call.initCapTableTyped(0);

        const pipelined_frame = try pipelined_builder.finish();
        defer allocator.free(pipelined_frame);
        try peer.handleFrame(pipelined_frame);

        try std.testing.expect(peer.pending_promises.contains(accept_qid));
        try std.testing.expectEqual(frame_start, capture.frames.items.len);
        try std.testing.expectEqual(round, @as(u32, @intCast(server_ctx.called)));

        var disembargo_builder = protocol.MessageBuilder.init(allocator);
        defer disembargo_builder.deinit();
        try disembargo_builder.buildDisembargoAccept(
            .{
                .tag = .importedCap,
                .imported_cap = export_id,
                .promised_answer = null,
            },
            embargo,
        );
        const disembargo_frame = try disembargo_builder.finish();
        defer allocator.free(disembargo_frame);
        try peer.handleFrame(disembargo_frame);

        try std.testing.expectEqual(frame_start + 2, capture.frames.items.len);
        try std.testing.expect(!peer.pending_promises.contains(accept_qid));
        try std.testing.expectEqual(round + 1, @as(u32, @intCast(server_ctx.called)));

        var accept_ret = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start]);
        defer accept_ret.deinit();
        try std.testing.expectEqual(protocol.MessageTag.@"return", accept_ret.tag);
        const accept_return = try accept_ret.asReturn();
        try std.testing.expectEqual(accept_qid, accept_return.answer_id);
        try std.testing.expectEqual(protocol.ReturnTag.results, accept_return.tag);

        var pipelined_ret = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start + 1]);
        defer pipelined_ret.deinit();
        try std.testing.expectEqual(protocol.MessageTag.@"return", pipelined_ret.tag);
        const replayed_return = try pipelined_ret.asReturn();
        try std.testing.expectEqual(call_qid, replayed_return.answer_id);
        try std.testing.expectEqual(protocol.ReturnTag.exception, replayed_return.tag);
        const ex = replayed_return.exception orelse return error.MissingException;
        try std.testing.expectEqualStrings("stress-ordered", ex.reason);

        var accept_finish_builder = protocol.MessageBuilder.init(allocator);
        defer accept_finish_builder.deinit();
        try accept_finish_builder.buildFinish(accept_qid, false, false);
        const accept_finish_frame = try accept_finish_builder.finish();
        defer allocator.free(accept_finish_frame);
        try peer.handleFrame(accept_finish_frame);

        var call_finish_builder = protocol.MessageBuilder.init(allocator);
        defer call_finish_builder.deinit();
        try call_finish_builder.buildFinish(call_qid, false, false);
        const call_finish_frame = try call_finish_builder.finish();
        defer allocator.free(call_finish_frame);
        try peer.handleFrame(call_finish_frame);
    }

    try std.testing.expectEqual(rounds, @as(u32, @intCast(server_ctx.called)));
    try std.testing.expectEqual(@as(usize, 0), peer.pending_accepts_by_embargo.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_accept_embargo_by_question.count());
}

test "peer duplicate provide recipient sends abort" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient = try TextPointer.init(allocator, "recipient-C");
    defer recipient.deinit(allocator);

    var first_builder = protocol.MessageBuilder.init(allocator);
    defer first_builder.deinit();
    try first_builder.buildProvide(
        300,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try recipient.any(),
    );
    const first_frame = try first_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);

    var duplicate_builder = protocol.MessageBuilder.init(allocator);
    defer duplicate_builder.deinit();
    try duplicate_builder.buildProvide(
        301,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try recipient.any(),
    );
    const duplicate_frame = try duplicate_builder.finish();
    defer allocator.free(duplicate_frame);
    try std.testing.expectError(error.DuplicateProvideRecipient, peer.handleFrame(duplicate_frame));

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings("duplicate provide recipient", abort.exception.reason);
}

test "peer duplicate provide question sends abort" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var first_recipient = try TextPointer.init(allocator, "recipient-question-1");
    defer first_recipient.deinit(allocator);

    var second_recipient = try TextPointer.init(allocator, "recipient-question-2");
    defer second_recipient.deinit(allocator);

    var first_builder = protocol.MessageBuilder.init(allocator);
    defer first_builder.deinit();
    try first_builder.buildProvide(
        302,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try first_recipient.any(),
    );
    const first_frame = try first_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);

    var duplicate_builder = protocol.MessageBuilder.init(allocator);
    defer duplicate_builder.deinit();
    try duplicate_builder.buildProvide(
        302,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try second_recipient.any(),
    );
    const duplicate_frame = try duplicate_builder.finish();
    defer allocator.free(duplicate_frame);
    try std.testing.expectError(error.DuplicateProvideQuestionId, peer.handleFrame(duplicate_frame));

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings("duplicate provide question", abort.exception.reason);
}

test "peer join returns provided capability" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var key_part_builder = message.MessageBuilder.init(allocator);
    defer key_part_builder.deinit();
    const key_part_root = try key_part_builder.initRootAnyPointer();
    var key_part_struct = try key_part_root.initStruct(1, 0);
    key_part_struct.writeU32(0, 0xD4);
    key_part_struct.writeU16(4, 1);
    key_part_struct.writeU16(6, 0);
    const key_part_bytes = try key_part_builder.toBytes();
    defer allocator.free(key_part_bytes);
    var key_part_msg = try message.Message.init(allocator, key_part_bytes);
    defer key_part_msg.deinit();
    const key_part_ptr = try key_part_msg.getRootAnyPointer();

    var join_builder = protocol.MessageBuilder.init(allocator);
    defer join_builder.deinit();
    try join_builder.buildJoin(
        400,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        key_part_ptr,
    );
    const join_frame = try join_builder.finish();
    defer allocator.free(join_frame);
    try peer.handleFrame(join_frame);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 400), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, descriptor.tag);
    try std.testing.expectEqual(export_id, descriptor.id.?);
}

test "peer handleFrame join aggregates parts and returns capability for each part" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var key0 = try JoinKeyPartPointer.init(allocator, 0xB2, 2, 0);
    defer key0.deinit(allocator);

    var join0_builder = protocol.MessageBuilder.init(allocator);
    defer join0_builder.deinit();
    try join0_builder.buildJoin(
        910,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try key0.any(),
    );
    const join0_frame = try join0_builder.finish();
    defer allocator.free(join0_frame);
    try peer.handleFrame(join0_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var key1 = try JoinKeyPartPointer.init(allocator, 0xB2, 2, 1);
    defer key1.deinit(allocator);

    var join1_builder = protocol.MessageBuilder.init(allocator);
    defer join1_builder.deinit();
    try join1_builder.buildJoin(
        911,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try key1.any(),
    );
    const join1_frame = try join1_builder.finish();
    defer allocator.free(join1_frame);
    try peer.handleFrame(join1_frame);
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var seen_910 = false;
    var seen_911 = false;
    for (capture.frames.items) |out_frame| {
        var out_decoded = try protocol.DecodedMessage.init(allocator, out_frame);
        defer out_decoded.deinit();
        try std.testing.expectEqual(protocol.MessageTag.@"return", out_decoded.tag);
        const ret = try out_decoded.asReturn();
        try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);

        if (ret.answer_id == 910) {
            seen_910 = true;
        } else if (ret.answer_id == 911) {
            seen_911 = true;
        } else {
            return error.UnexpectedQuestionId;
        }

        const payload = ret.results orelse return error.MissingPayload;
        const cap = try payload.content.getCapability();
        const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
        const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
        try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, descriptor.tag);
        try std.testing.expectEqual(export_id, descriptor.id.?);
    }
    try std.testing.expect(seen_910);
    try std.testing.expect(seen_911);
}

test "peer duplicate join question sends abort" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var first_key = try JoinKeyPartPointer.init(allocator, 0xD5, 2, 0);
    defer first_key.deinit(allocator);
    var first_builder = protocol.MessageBuilder.init(allocator);
    defer first_builder.deinit();
    try first_builder.buildJoin(
        410,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try first_key.any(),
    );
    const first_frame = try first_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var second_key = try JoinKeyPartPointer.init(allocator, 0xD5, 2, 1);
    defer second_key.deinit(allocator);
    var duplicate_builder = protocol.MessageBuilder.init(allocator);
    defer duplicate_builder.deinit();
    try duplicate_builder.buildJoin(
        410,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try second_key.any(),
    );
    const duplicate_frame = try duplicate_builder.finish();
    defer allocator.free(duplicate_frame);
    try std.testing.expectError(error.DuplicateJoinQuestionId, peer.handleFrame(duplicate_frame));

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings("duplicate join question", abort.exception.reason);
}

test "peer join part count mismatch returns exception" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var first_key = try JoinKeyPartPointer.init(allocator, 0xE1, 2, 0);
    defer first_key.deinit(allocator);
    var first_builder = protocol.MessageBuilder.init(allocator);
    defer first_builder.deinit();
    try first_builder.buildJoin(
        420,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try first_key.any(),
    );
    const first_frame = try first_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var mismatch_key = try JoinKeyPartPointer.init(allocator, 0xE1, 3, 1);
    defer mismatch_key.deinit(allocator);
    var mismatch_builder = protocol.MessageBuilder.init(allocator);
    defer mismatch_builder.deinit();
    try mismatch_builder.buildJoin(
        421,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try mismatch_key.any(),
    );
    const mismatch_frame = try mismatch_builder.finish();
    defer allocator.free(mismatch_frame);
    try peer.handleFrame(mismatch_frame);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 421), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("join partCount mismatch", ex.reason);
}

test "peer duplicate join part returns exception" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var first_key = try JoinKeyPartPointer.init(allocator, 0xE2, 2, 0);
    defer first_key.deinit(allocator);
    var first_builder = protocol.MessageBuilder.init(allocator);
    defer first_builder.deinit();
    try first_builder.buildJoin(
        430,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try first_key.any(),
    );
    const first_frame = try first_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var duplicate_key = try JoinKeyPartPointer.init(allocator, 0xE2, 2, 0);
    defer duplicate_key.deinit(allocator);
    var duplicate_builder = protocol.MessageBuilder.init(allocator);
    defer duplicate_builder.deinit();
    try duplicate_builder.buildJoin(
        431,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        try duplicate_key.any(),
    );
    const duplicate_frame = try duplicate_builder.finish();
    defer allocator.free(duplicate_frame);
    try peer.handleFrame(duplicate_frame);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 431), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("duplicate join part", ex.reason);
}

test "peer provide with unresolved promise target sends abort" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    const promise_export_id = try peer.addPromiseExport();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient = try TextPointer.init(allocator, "recipient-promise-unresolved");
    defer recipient.deinit(allocator);

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        701,
        .{
            .tag = .importedCap,
            .imported_cap = promise_export_id,
            .promised_answer = null,
        },
        try recipient.any(),
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);

    try std.testing.expectError(error.PromiseUnresolved, peer.handleFrame(provide_frame));
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings("PromiseUnresolved", abort.exception.reason);
}

test "peer thirdPartyAnswer rejects invalid answer id" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildThirdPartyAnswer(500, null);
    const frame = try builder.finish();
    defer allocator.free(frame);

    try std.testing.expectError(error.InvalidThirdPartyAnswerId, peer.handleFrame(frame));
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings("invalid thirdPartyAnswer answerId", abort.exception.reason);
}

test "peer thirdPartyAnswer conflicting completion sends abort" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var completion = try TextPointer.init(allocator, "third-party-conflict");
    defer completion.deinit(allocator);

    var first_builder = protocol.MessageBuilder.init(allocator);
    defer first_builder.deinit();
    try first_builder.buildThirdPartyAnswer(0x4000_0100, try completion.any());
    const first_frame = try first_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var conflict_builder = protocol.MessageBuilder.init(allocator);
    defer conflict_builder.deinit();
    try conflict_builder.buildThirdPartyAnswer(0x4000_0101, try completion.any());
    const conflict_frame = try conflict_builder.finish();
    defer allocator.free(conflict_frame);
    try std.testing.expectError(error.ConflictingThirdPartyAnswer, peer.handleFrame(conflict_frame));

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings("conflicting thirdPartyAnswer completion", abort.exception.reason);
}

test "peer thirdPartyAnswer duplicate completion with same answer id is ignored" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var completion = try TextPointer.init(allocator, "third-party-duplicate");
    defer completion.deinit(allocator);

    var first_builder = protocol.MessageBuilder.init(allocator);
    defer first_builder.deinit();
    try first_builder.buildThirdPartyAnswer(0x4000_0200, try completion.any());
    const first_frame = try first_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);

    var duplicate_builder = protocol.MessageBuilder.init(allocator);
    defer duplicate_builder.deinit();
    try duplicate_builder.buildThirdPartyAnswer(0x4000_0200, try completion.any());
    const duplicate_frame = try duplicate_builder.finish();
    defer allocator.free(duplicate_frame);
    try peer.handleFrame(duplicate_frame);

    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);
}

test "peer duplicate third-party return before await errors" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var ret_builder = protocol.MessageBuilder.init(allocator);
    defer ret_builder.deinit();
    var ret = try ret_builder.beginReturn(0x4000_0400, .exception);
    try ret.setException("early-third-party-return");
    const ret_frame = try ret_builder.finish();
    defer allocator.free(ret_frame);

    try peer.handleFrame(ret_frame);
    try std.testing.expectError(error.DuplicateThirdPartyReturn, peer.handleFrame(ret_frame));
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);
}

test "peer duplicate awaitFromThirdParty completion sends abort" {
    const allocator = std.testing.allocator;

    const Callback = struct {
        fn onReturn(
            _: *anyopaque,
            _: *Peer,
            _: protocol.Return,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {}
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var callback_ctx_1: u8 = 0;
    const answer_id_1 = try peer.sendCallResolved(
        .{ .imported = .{ .id = 1 } },
        0xC0DE,
        1,
        &callback_ctx_1,
        null,
        Callback.onReturn,
    );

    var callback_ctx_2: u8 = 0;
    const answer_id_2 = try peer.sendCallResolved(
        .{ .imported = .{ .id = 1 } },
        0xC0DE,
        2,
        &callback_ctx_2,
        null,
        Callback.onReturn,
    );

    var completion = try TextPointer.init(allocator, "duplicate-await-completion");
    defer completion.deinit(allocator);

    var first_builder = protocol.MessageBuilder.init(allocator);
    defer first_builder.deinit();
    var first = try first_builder.beginReturn(answer_id_1, .awaitFromThirdParty);
    try first.setAcceptFromThirdParty(try completion.any());
    const first_frame = try first_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);

    var duplicate_builder = protocol.MessageBuilder.init(allocator);
    defer duplicate_builder.deinit();
    var duplicate = try duplicate_builder.beginReturn(answer_id_2, .awaitFromThirdParty);
    try duplicate.setAcceptFromThirdParty(try completion.any());
    const duplicate_frame = try duplicate_builder.finish();
    defer allocator.free(duplicate_frame);
    try std.testing.expectError(error.DuplicateThirdPartyAwait, peer.handleFrame(duplicate_frame));

    try std.testing.expect(capture.frames.items.len >= 4);
    const last_idx = capture.frames.items.len - 1;
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[last_idx]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings("duplicate awaitFromThirdParty completion", abort.exception.reason);
}

test "peer attachConnection sets hasAttachedTransport to true" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    // Before attaching, no transport is present.
    try std.testing.expect(!peer.hasAttachedTransport());

    var conn: Connection = undefined;
    peer.attachConnection(&conn);

    // After attaching, the transport is present — this is the condition
    // that would trigger the @panic guard on a second attachConnection call.
    try std.testing.expect(peer.hasAttachedTransport());
}

test "peer attachTransport sets hasAttachedTransport to true" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    try std.testing.expect(!peer.hasAttachedTransport());

    const DummyTransport = struct {
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
        fn start(_: *anyopaque, _: *Peer) void {}
        fn close(_: *anyopaque) void {}
        fn isClosing(_: *anyopaque) bool {
            return false;
        }
    };

    var ctx: u8 = 0;
    peer.attachTransport(
        @ptrCast(&ctx),
        DummyTransport.start,
        DummyTransport.send,
        DummyTransport.close,
        DummyTransport.isClosing,
    );

    // After attaching, the transport is present — this is the condition
    // that would trigger the @panic guard on a second attachTransport call.
    try std.testing.expect(peer.hasAttachedTransport());
}

test "peer detachConnection then re-attachConnection succeeds" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var conn1: Connection = undefined;
    peer.attachConnection(&conn1);
    try std.testing.expect(peer.hasAttachedTransport());

    // Detach the connection.
    peer.detachConnection();
    try std.testing.expect(!peer.hasAttachedTransport());

    // Re-attach a different connection — this must succeed because the
    // transport was properly detached first. Without detach, this would
    // trigger the @panic in attachConnection.
    var conn2: Connection = undefined;
    peer.attachConnection(&conn2);
    try std.testing.expect(peer.hasAttachedTransport());
}

test "peer detachTransport then re-attachTransport succeeds" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const DummyTransport = struct {
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
        fn start(_: *anyopaque, _: *Peer) void {}
        fn close(_: *anyopaque) void {}
        fn isClosing(_: *anyopaque) bool {
            return false;
        }
    };

    var ctx1: u8 = 0;
    peer.attachTransport(
        @ptrCast(&ctx1),
        DummyTransport.start,
        DummyTransport.send,
        DummyTransport.close,
        DummyTransport.isClosing,
    );
    try std.testing.expect(peer.hasAttachedTransport());

    // Detach the transport.
    peer.detachTransport();
    try std.testing.expect(!peer.hasAttachedTransport());

    // Re-attach a different transport — this must succeed because the
    // transport was properly detached first. Without detach, this would
    // trigger the @panic in attachTransport.
    var ctx2: u8 = 0;
    peer.attachTransport(
        @ptrCast(&ctx2),
        DummyTransport.start,
        DummyTransport.send,
        DummyTransport.close,
        DummyTransport.isClosing,
    );
    try std.testing.expect(peer.hasAttachedTransport());
}

test "peer attachConnection after attachTransport triggers hasAttachedTransport guard" {
    // Verify that mixing attachConnection and attachTransport correctly
    // detects the double-attach condition. The @panic cannot be caught
    // in-process, so we verify the precondition that triggers it.
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const DummyTransport = struct {
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
        fn start(_: *anyopaque, _: *Peer) void {}
        fn close(_: *anyopaque) void {}
        fn isClosing(_: *anyopaque) bool {
            return false;
        }
    };

    var ctx: u8 = 0;
    peer.attachTransport(
        @ptrCast(&ctx),
        DummyTransport.start,
        DummyTransport.send,
        DummyTransport.close,
        DummyTransport.isClosing,
    );

    // The transport is attached; attempting attachConnection now would
    // hit the @panic guard. We verify the guard condition holds.
    try std.testing.expect(peer.hasAttachedTransport());

    // Detach first, then attachConnection succeeds.
    peer.detachTransport();
    try std.testing.expect(!peer.hasAttachedTransport());

    var conn: Connection = undefined;
    peer.attachConnection(&conn);
    try std.testing.expect(peer.hasAttachedTransport());
}

test "peer init attaches connection immediately" {
    // Peer.init calls attachConnection internally. Verify the transport
    // is attached right away, meaning a second attach would panic.
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    try std.testing.expect(peer.hasAttachedTransport());
}

test {
    _ = @import("rpc_peer_control_from_peer_control_zig_test.zig");
}
