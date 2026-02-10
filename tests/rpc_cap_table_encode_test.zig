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

test "encode outbound cap table rewrites capability pointer lists in struct payloads" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(7, .results);
    const results_struct = try ret.initResultsStruct(0, 1);
    var workers = try results_struct.writePointerList(0, 3);
    try workers.setCapability(0, .{ .id = 40 });
    try workers.setCapability(1, .{ .id = 41 });
    try workers.setCapability(2, .{ .id = 42 });

    _ = try cap_table.encodeReturnPayloadCaps(&caps, &ret, null, null);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const ret_decoded = try decoded.asReturn();
    const payload = ret_decoded.results orelse return error.MissingPayload;

    const payload_struct = try payload.content.getStruct();
    const workers_reader = try payload_struct.readPointerList(0);
    try std.testing.expectEqual(@as(u32, 0), (try workers_reader.getCapability(0)).id);
    try std.testing.expectEqual(@as(u32, 1), (try workers_reader.getCapability(1)).id);
    try std.testing.expectEqual(@as(u32, 2), (try workers_reader.getCapability(2)).id);

    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    try std.testing.expectEqual(@as(u32, 3), cap_table_reader.len());

    const desc0 = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(0));
    const desc1 = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(1));
    const desc2 = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(2));

    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, desc0.tag);
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, desc1.tag);
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, desc2.tag);
    try std.testing.expectEqual(@as(u32, 40), desc0.id.?);
    try std.testing.expectEqual(@as(u32, 41), desc1.id.?);
    try std.testing.expectEqual(@as(u32, 42), desc2.id.?);
}

test "encode outbound cap table rewrites large capability lists in struct payloads" {
    const allocator = std.testing.allocator;
    const width: u32 = 64;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(8, .results);
    const results_struct = try ret.initResultsStruct(0, 1);
    var workers = try results_struct.writePointerList(0, width);

    var i: u32 = 0;
    while (i < width) : (i += 1) {
        try workers.setCapability(i, .{ .id = 1000 + i });
    }

    _ = try cap_table.encodeReturnPayloadCaps(&caps, &ret, null, null);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const ret_decoded = try decoded.asReturn();
    const payload = ret_decoded.results orelse return error.MissingPayload;
    const payload_struct = try payload.content.getStruct();
    const workers_reader = try payload_struct.readPointerList(0);
    try std.testing.expectEqual(width, workers_reader.len());

    i = 0;
    while (i < width) : (i += 1) {
        const cap = try workers_reader.getCapability(i);
        try std.testing.expectEqual(i, cap.id);
    }

    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    try std.testing.expectEqual(width, cap_table_reader.len());

    i = 0;
    while (i < width) : (i += 1) {
        const desc = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(i));
        try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, desc.tag);
        try std.testing.expectEqual(1000 + i, desc.id.?);
    }
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

test "noteReceiverAnswerOps copies transform ops without aliasing source slice" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var ops = [_]protocol.PromisedAnswerOp{
        .{ .tag = .noop, .pointer_index = 0 },
        .{ .tag = .get_pointer_field, .pointer_index = 3 },
    };

    const id = try caps.noteReceiverAnswerOps(77, &ops);
    ops[1].pointer_index = 42;
    ops[1].tag = .noop;

    const stored = caps.getReceiverAnswer(id) orelse return error.MissingStoredPromisedAnswer;
    try std.testing.expectEqual(@as(u32, 77), stored.question_id);
    try std.testing.expectEqual(@as(usize, 2), stored.ops.len);
    try std.testing.expectEqual(protocol.PromisedAnswerOpTag.noop, stored.ops[0].tag);
    try std.testing.expectEqual(protocol.PromisedAnswerOpTag.get_pointer_field, stored.ops[1].tag);
    try std.testing.expectEqual(@as(u16, 3), stored.ops[1].pointer_index);
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
