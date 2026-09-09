const std = @import("std");
const capnpc = @import("capnpc-zig");
const Root = @import("generated.zig").Root;
test "typed generic list round trip" {
    var message = capnpc.message.MessageBuilder.init(std.testing.allocator);
    defer message.deinit();
    const root = Root.Builder.wrap(try message.allocateStruct(0, 1));
    var boxes = try root.brands().initBoxes(2);
    var first = try boxes.get(0);
    try first.setValue("alpha");
    var second = try boxes.get(1);
    try second.setValue("beta");
    const bytes = try message.toBytes();
    defer std.testing.allocator.free(bytes);
    var parsed = try capnpc.message.Message.init(std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    const reader = Root.Reader.wrap(try parsed.getRootStruct());
    const values = try reader.brands().getBoxes();
    try std.testing.expectEqual(@as(u32, 2), values.len());
    try std.testing.expectEqualStrings("alpha", try (try values.get(0)).getValue());
    try std.testing.expectEqualStrings("beta", try (try values.get(1)).getValue());
}
