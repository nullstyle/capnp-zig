const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.protocol;
const cap_table = capnpc.rpc.cap_table;

test "encode outbound cap table rewrites capability pointers" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(1, 0x1234, 0);
    try call.setTargetImportedCap(1);
    const any = try call.getParamsAnyPointer();
    try any.setCapability(.{ .id = 42 });

    try cap_table.encodeCallPayloadCaps(&caps, &call, null, null);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const call_decoded = try decoded.asCall();
    const payload = call_decoded.params;
    const cap = try payload.content.getCapability();
    try std.testing.expectEqual(@as(u32, 0), cap.id);

    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const desc = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(0));
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, desc.tag);
    try std.testing.expectEqual(@as(u32, 42), desc.id.?);
}

test "encode outbound cap table marks promised export as senderPromise" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    try caps.markExportPromise(42);

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(2, 0x1234, 0);
    try call.setTargetImportedCap(1);
    const any = try call.getParamsAnyPointer();
    try any.setCapability(.{ .id = 42 });

    try cap_table.encodeCallPayloadCaps(&caps, &call, null, null);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const call_decoded = try decoded.asCall();
    const payload = call_decoded.params;
    const cap = try payload.content.getCapability();
    try std.testing.expectEqual(@as(u32, 0), cap.id);

    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const desc = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(0));
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_promise, desc.tag);
    try std.testing.expectEqual(@as(u32, 42), desc.id.?);
}

test "noteReceiverAnswer copies promised-answer transform ops" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    const ops = [_]protocol.PromisedAnswerOp{
        .{ .tag = .noop, .pointer_index = 0 },
        .{ .tag = .get_pointer_field, .pointer_index = 7 },
    };

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(44, 0x1234, 0);
    try call.setTargetPromisedAnswerWithOps(99, &ops);
    try call.setEmptyCapTable();

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const call_decoded = try decoded.asCall();
    const promised = call_decoded.target.promised_answer orelse return error.MissingPromisedAnswer;

    const id = try caps.noteReceiverAnswer(promised);
    const stored = caps.getReceiverAnswer(id) orelse return error.MissingStoredPromisedAnswer;

    try std.testing.expectEqual(@as(u32, 99), stored.question_id);
    try std.testing.expectEqual(@as(usize, ops.len), stored.ops.len);
    try std.testing.expectEqual(protocol.PromisedAnswerOpTag.noop, stored.ops[0].tag);
    try std.testing.expectEqual(protocol.PromisedAnswerOpTag.get_pointer_field, stored.ops[1].tag);
    try std.testing.expectEqual(@as(u16, 7), stored.ops[1].pointer_index);
}

fn noteReceiverAnswerOpsOomImpl(allocator: std.mem.Allocator) !void {
    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    const ops = [_]protocol.PromisedAnswerOp{
        .{ .tag = .get_pointer_field, .pointer_index = 3 },
    };
    _ = try caps.noteReceiverAnswerOps(99, &ops);
}

test "noteReceiverAnswerOps propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, noteReceiverAnswerOpsOomImpl, .{});
}

fn encodeCallPayloadCapsOomImpl(allocator: std.mem.Allocator) !void {
    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(9, 0x4321, 4);
    try call.setTargetImportedCap(1);
    const any = try call.getParamsAnyPointer();
    try any.setCapability(.{ .id = 7 });

    try cap_table.encodeCallPayloadCaps(&caps, &call, null, null);
    const bytes = try builder.finish();
    defer allocator.free(bytes);
}

test "encodeCallPayloadCaps propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, encodeCallPayloadCapsOomImpl, .{});
}
