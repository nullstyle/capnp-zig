const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.protocol;
const peer_impl = capnpc.rpc.peer;
const peer_dispatch = capnpc.rpc.peer_dispatch;
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
    try std.testing.expectEqual(peer_dispatch.InboundRoute.return_, peer_dispatch.route(.return_));
    try std.testing.expectEqual(peer_dispatch.InboundRoute.third_party_answer, peer_dispatch.route(.third_party_answer));
    try std.testing.expectEqual(peer_dispatch.InboundRoute.unknown, peer_dispatch.route(.obsolete_save));
    try std.testing.expectEqual(peer_dispatch.InboundRoute.unknown, peer_dispatch.route(.obsolete_delete));
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
            .tag = .imported_cap,
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
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 101), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, descriptor.tag);
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
            .tag = .imported_cap,
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
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 211), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, descriptor.tag);
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
    try pipelined_call.setEmptyCapTable();
    const pipelined_frame = try pipelined_builder.finish();
    defer allocator.free(pipelined_frame);
    try peer.handleFrame(pipelined_frame);

    try std.testing.expect(!handler_called);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var disembargo_builder = protocol.MessageBuilder.init(allocator);
    defer disembargo_builder.deinit();
    try disembargo_builder.buildDisembargoAccept(
        .{
            .tag = .imported_cap,
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
    try std.testing.expectEqual(protocol.MessageTag.return_, accept_ret.tag);
    const accept_return = try accept_ret.asReturn();
    try std.testing.expectEqual(@as(u32, 231), accept_return.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, accept_return.tag);

    var pipelined_ret = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer pipelined_ret.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, pipelined_ret.tag);
    const replayed_return = try pipelined_ret.asReturn();
    try std.testing.expectEqual(@as(u32, 232), replayed_return.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, replayed_return.tag);
    const ex = replayed_return.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("ordered", ex.reason);
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 400), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, descriptor.tag);
    try std.testing.expectEqual(export_id, descriptor.id.?);
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
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
            .tag = .imported_cap,
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
            .tag = .imported_cap,
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
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
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
            .tag = .imported_cap,
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
    var first = try first_builder.beginReturn(answer_id_1, .accept_from_third_party);
    try first.setAcceptFromThirdParty(try completion.any());
    const first_frame = try first_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);

    var duplicate_builder = protocol.MessageBuilder.init(allocator);
    defer duplicate_builder.deinit();
    var duplicate = try duplicate_builder.beginReturn(answer_id_2, .accept_from_third_party);
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
