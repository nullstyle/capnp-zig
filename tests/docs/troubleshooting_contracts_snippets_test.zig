const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const rpc = capnpc.rpc;
const testing = std.testing;

test "troubleshooting reader lifetime snippet copies borrowed text before message deinit" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var player = try builder.allocateStruct(1, 1);
    player.writeU32(0, 7);
    try player.writeText(0, "Ada");

    const packed_bytes = try builder.toPackedBytes();
    defer testing.allocator.free(packed_bytes);

    var owned_name: []u8 = undefined;
    {
        var msg = try message.Message.initPacked(testing.allocator, packed_bytes, .{});
        defer msg.deinit();

        const reader = try msg.getRootStruct();
        try testing.expectEqual(@as(u32, 7), reader.readU32(0));

        const borrowed_name = try reader.readText(0);
        owned_name = try testing.allocator.dupe(u8, borrowed_name);
    }
    defer testing.allocator.free(owned_name);

    try testing.expectEqualStrings("Ada", owned_name);
}

test "troubleshooting validation snippet uses limits before reading untrusted data" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    _ = try builder.allocateStruct(1, 0);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    const validation_options = message.Message.ValidationOptions{
        .traversal_limit_words = 16,
        .nesting_limit = 8,
        .segment_count_limit = 1,
    };

    var msg = try message.Message.init(testing.allocator, bytes, validation_options);
    defer msg.deinit();

    const reader = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 0), reader.readU32(8));
    try testing.expectError(error.OutOfBounds, reader.readU32Strict(8));
}

test "api contracts snippet imports only domain-shaped RPC facade names" {
    comptime {
        _ = rpc.wire.framing.Framer;
        _ = rpc.wire.protocol.MessageTag;
        _ = rpc.caps.table.CapTable;
        _ = rpc.promises.pipeline.OwnedPromisedAnswer;
        _ = rpc.events.Observer;
        _ = rpc.peer.Peer;
        _ = rpc.transport.binding.Binding(rpc.peer.Peer);
        _ = rpc.transport.tcp.Transport;
        _ = rpc.transport.quic.enabled;
        _ = rpc.integration.host_peer.HostPeer;
        _ = rpc.generated.rpc.Message;
    }
}
