const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.protocol;
const peer_impl = capnpc.rpc.peer;
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
