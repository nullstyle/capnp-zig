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
