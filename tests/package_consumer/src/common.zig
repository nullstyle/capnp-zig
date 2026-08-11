const std = @import("std");
const capnpc = @import("capnpc-zig");

// Release sentinels: each package root must carry the new sprint surface, not
// merely compile an older baseline. This file is imported by all three clean-
// room consumers.
comptime {
    _ = capnpc.message.PointerListReader.isNull;
    _ = capnpc.message.PointerListBuilder.initTextList;
    _ = capnpc.message.typed_list_helpers.NestedListReader;
    _ = capnpc.rpc.peer.CallOptions;
}

pub fn exerciseSerialization() !void {
    var builder = capnpc.message.MessageBuilder.init(std.heap.page_allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(1, 1);
    root.writeU32(0, 0xdecafbad);
    try root.writeText(0, "consumer");

    const bytes = try builder.toBytes();
    defer std.heap.page_allocator.free(bytes);

    var parsed = try capnpc.message.Message.init(std.heap.page_allocator, bytes, .{});
    defer parsed.deinit();
    const reader = try parsed.getRootStruct();
    if (reader.readU32(0) != 0xdecafbad) return error.ConsumerRoundTripFailed;
    if (!std.mem.eql(u8, try reader.readText(0), "consumer")) return error.ConsumerRoundTripFailed;
}
