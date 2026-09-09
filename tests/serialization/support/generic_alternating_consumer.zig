const std = @import("std");
const capnpc = @import("capnpc-zig");
const Root = @import("generated.zig").Root;
test "alternating Text and Data applications" {
    var message = capnpc.message.MessageBuilder.init(std.testing.allocator);
    defer message.deinit();
    const root = Root.Builder.wrap(try message.allocateStruct(0, 1));
    var first = try root.brands().initHead();
    try first.setValue("text");
    var second = try first.initNext();
    try second.setValue(&.{ 0xff, 0xfe });
    var third = try second.initNext();
    try third.setValue("text again");
    const bytes = try message.toBytes();
    defer std.testing.allocator.free(bytes);
    var parsed = try capnpc.message.Message.init(std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    const reader = Root.Reader.wrap(try parsed.getRootStruct());
    const head = try reader.brands().getHead();
    try std.testing.expectEqualStrings("text", try head.getValue());
    const data_link = try head.getNext();
    try std.testing.expectEqualSlices(u8, &.{ 0xff, 0xfe }, try data_link.getValue());
    try std.testing.expectEqualStrings("text again", try (try data_link.getNext()).getValue());
}
