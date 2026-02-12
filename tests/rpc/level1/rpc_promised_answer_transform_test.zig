const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.protocol;
const cap_table = capnpc.rpc.cap_table;
const message = capnpc.message;

test "promised answer getPointerField resolves to cap table entry" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(1, .results);
    var any_payload = try ret.payloadTyped();
    var any = try any_payload.initContent();

    var result_struct = try any.initStruct(0, 1);
    var field_any = try result_struct.getAnyPointer(0);
    try field_any.setCapability(.{ .id = 0 });

    var cap_list = try ret.initCapTableTyped(1);

    const entry = try cap_list.get(0);
    protocol.CapDescriptor.writeSenderHosted(entry, 42);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const ret_decoded = try decoded.asReturn();
    const payload = ret_decoded.results orelse return error.MissingPayload;

    var transform_builder = message.MessageBuilder.init(allocator);
    defer transform_builder.deinit();

    var root = try transform_builder.allocateStruct(0, 1);
    var list = try root.writeStructList(0, 1, 1, 0);
    const op = try list.get(0);
    op.writeUnionDiscriminant(0, @intFromEnum(protocol.PromisedAnswerOpTag.getPointerField));
    op.writeU16(2, 0);

    const transform_bytes = try transform_builder.toBytes();
    defer allocator.free(transform_bytes);

    var transform_msg = try message.Message.init(allocator, transform_bytes);
    defer transform_msg.deinit();

    const transform_root = try transform_msg.getRootStruct();
    const transform_list = try transform_root.readStructList(0);

    const transform = protocol.PromisedAnswerTransform{ .list = transform_list };
    const resolved = try cap_table.resolvePromisedAnswer(payload, transform);
    switch (resolved) {
        .exported => |cap| try std.testing.expectEqual(@as(u32, 42), cap.id),
        else => return error.UnexpectedCapType,
    }
}

test "promised answer getPointerField resolves senderPromise to exported cap" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(1, .results);
    var any_payload = try ret.payloadTyped();
    var any = try any_payload.initContent();

    var result_struct = try any.initStruct(0, 1);
    var field_any = try result_struct.getAnyPointer(0);
    try field_any.setCapability(.{ .id = 0 });

    var cap_list = try ret.initCapTableTyped(1);

    const entry = try cap_list.get(0);
    protocol.CapDescriptor.writeSenderPromise(entry, 77);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const ret_decoded = try decoded.asReturn();
    const payload = ret_decoded.results orelse return error.MissingPayload;

    var transform_builder = message.MessageBuilder.init(allocator);
    defer transform_builder.deinit();

    var root = try transform_builder.allocateStruct(0, 1);
    var list = try root.writeStructList(0, 1, 1, 0);
    const op = try list.get(0);
    op.writeUnionDiscriminant(0, @intFromEnum(protocol.PromisedAnswerOpTag.getPointerField));
    op.writeU16(2, 0);

    const transform_bytes = try transform_builder.toBytes();
    defer allocator.free(transform_bytes);

    var transform_msg = try message.Message.init(allocator, transform_bytes);
    defer transform_msg.deinit();

    const transform_root = try transform_msg.getRootStruct();
    const transform_list = try transform_root.readStructList(0);
    const transform = protocol.PromisedAnswerTransform{ .list = transform_list };

    const resolved = try cap_table.resolvePromisedAnswer(payload, transform);
    switch (resolved) {
        .exported => |cap| try std.testing.expectEqual(@as(u32, 77), cap.id),
        else => return error.UnexpectedCapType,
    }
}

test "promised answer getPointerField resolves receiverAnswer to promised cap" {
    const allocator = std.testing.allocator;

    const receiver_ops = [_]protocol.PromisedAnswerOp{
        .{ .tag = .getPointerField, .pointer_index = 2 },
    };

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(1, .results);
    var any_payload = try ret.payloadTyped();
    var any = try any_payload.initContent();

    var result_struct = try any.initStruct(0, 1);
    var field_any = try result_struct.getAnyPointer(0);
    try field_any.setCapability(.{ .id = 0 });

    var cap_list = try ret.initCapTableTyped(1);

    const entry = try cap_list.get(0);
    try protocol.CapDescriptor.writeReceiverAnswer(entry, 88, &receiver_ops);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const ret_decoded = try decoded.asReturn();
    const payload = ret_decoded.results orelse return error.MissingPayload;

    var transform_builder = message.MessageBuilder.init(allocator);
    defer transform_builder.deinit();

    var root = try transform_builder.allocateStruct(0, 1);
    var list = try root.writeStructList(0, 1, 1, 0);
    const op = try list.get(0);
    op.writeUnionDiscriminant(0, @intFromEnum(protocol.PromisedAnswerOpTag.getPointerField));
    op.writeU16(2, 0);

    const transform_bytes = try transform_builder.toBytes();
    defer allocator.free(transform_bytes);

    var transform_msg = try message.Message.init(allocator, transform_bytes);
    defer transform_msg.deinit();

    const transform_root = try transform_msg.getRootStruct();
    const transform_list = try transform_root.readStructList(0);
    const transform = protocol.PromisedAnswerTransform{ .list = transform_list };

    const resolved = try cap_table.resolvePromisedAnswer(payload, transform);
    switch (resolved) {
        .promised => |promised| {
            try std.testing.expectEqual(@as(u32, 88), promised.question_id);
            try std.testing.expectEqual(@as(u32, 1), promised.transform.len());
            const resolved_op = try promised.transform.get(0);
            try std.testing.expectEqual(protocol.PromisedAnswerOpTag.getPointerField, resolved_op.tag);
            try std.testing.expectEqual(@as(u16, 2), resolved_op.pointer_index);
        },
        else => return error.UnexpectedCapType,
    }
}
