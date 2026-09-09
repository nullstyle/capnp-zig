const std = @import("std");
const capnpc = @import("capnpc-zig");
const Root = @import("generated.zig").Root;
test "older generic list upgrade" {
    var message = capnpc.message.MessageBuilder.init(std.testing.allocator);
    defer message.deinit();
    var raw = try message.allocateStruct(0, 1);
    var original = try raw.writeStructList(0, 1, 1, 0);
    var old = try original.get(0);
    old.writeU64(0, 0xfeedbeef12345678);
    const root = Root.Builder.wrap(raw);
    var boxes = try root.brands().getBoxes();
    var first = try boxes.get(0);
    try first.setValue("upgraded");
    const bytes = try message.toBytes();
    defer std.testing.allocator.free(bytes);
    var parsed = try capnpc.message.Message.init(std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    const reader = Root.Reader.wrap(try parsed.getRootStruct());
    try std.testing.expectEqualStrings("upgraded", try (try (try reader.brands().getBoxes()).get(0)).getValue());
    const stored = try (try (try parsed.getRootStruct()).readStructList(0)).get(0);
    try std.testing.expectEqual(@as(u64, 0xfeedbeef12345678), stored.readU64(0));
}
