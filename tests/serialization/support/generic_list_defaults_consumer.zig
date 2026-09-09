const std = @import("std");
const capnpc = @import("capnpc-zig");
const Root = @import("generated.zig").Root;
test "defaults and union selection" {
    var message = capnpc.message.MessageBuilder.init(std.testing.allocator);
    defer message.deinit();
    const root = Root.Builder.wrap(try message.allocateStruct(1, 2));
    const original_bytes = try message.toBytes();
    defer std.testing.allocator.free(original_bytes);
    var original = try capnpc.message.Message.init(std.testing.allocator, original_bytes, .{});
    defer original.deinit();
    const original_reader = Root.Reader.wrap(try original.getRootStruct());
    try std.testing.expectEqualStrings("seed", try (try (try original_reader.brands().getBoxes()).get(0)).getValue());
    try std.testing.expectError(error.WrongUnionMember, root.brands().getChoices());
    var boxes = try root.brands().getBoxes();
    var first = try boxes.get(0);
    try first.setValue("edited");
    var choices = try root.brands().initChoices(1);
    var chosen = try choices.get(0);
    try chosen.setValue("chosen");
    const bytes = try message.toBytes();
    defer std.testing.allocator.free(bytes);
    var parsed = try capnpc.message.Message.init(std.testing.allocator, bytes, .{});
    defer parsed.deinit();
    const reader = Root.Reader.wrap(try parsed.getRootStruct());
    try std.testing.expectEqualStrings("edited", try (try (try reader.brands().getBoxes()).get(0)).getValue());
    try std.testing.expectEqualStrings("chosen", try (try (try reader.brands().getChoices()).get(0)).getValue());
    try std.testing.expectEqualStrings("seed", try (try (try original_reader.brands().getBoxes()).get(0)).getValue());
}
