const std = @import("std");
const capnpc = @import("capnpc-zig");
const generated = @import("generated").helpers;

test "WhichTag schema name permits union setters, discriminants, and guarded reads" {
    const allocator = std.testing.allocator;
    var message = capnpc.message.MessageBuilder.init(allocator);
    defer message.deinit();
    var value = try generated.WhichTag.Builder.init(&message);
    try value.setNone({});
    try value.setNumber(123456789);

    const bytes = try message.toBytes();
    defer allocator.free(bytes);
    var decoded = try capnpc.message.Message.init(allocator, bytes, .{});
    defer decoded.deinit();
    const reader = try generated.WhichTag.Reader.init(&decoded);
    try std.testing.expectEqual(generated.WhichTag.WhichTag.number, try reader.which());
    try std.testing.expectEqual(@as(u16, 1), reader.whichOrdinal());
    try std.testing.expectEqual(@as(u32, 123456789), try reader.getNumber());
    try std.testing.expectError(error.WrongUnionMember, reader.getNone());
}

test "EnumOrdinals schema name permits Reader and Builder ordinal views" {
    const allocator = std.testing.allocator;
    var message = capnpc.message.MessageBuilder.init(allocator);
    defer message.deinit();
    var value = try generated.EnumOrdinals.Builder.init(&message);
    try value.setValue(.Active);
    {
        const bytes = try message.toBytes();
        defer allocator.free(bytes);
        var decoded = try capnpc.message.Message.init(allocator, bytes, .{});
        defer decoded.deinit();
        const reader = try generated.EnumOrdinals.Reader.init(&decoded);
        try std.testing.expectEqual(generated.EnumOrdinals.State.Active, try reader.getValue());
        try std.testing.expectEqual(@as(u16, 1), try reader.enumOrdinals().getValue());
    }
    try value.enumOrdinals().setValue(444);

    const bytes = try message.toBytes();
    defer allocator.free(bytes);
    var decoded = try capnpc.message.Message.init(allocator, bytes, .{});
    defer decoded.deinit();
    const reader = try generated.EnumOrdinals.Reader.init(&decoded);
    try std.testing.expectEqual(@as(u16, 444), try reader.enumOrdinals().getValue());
    try std.testing.expectError(error.InvalidEnumValue, reader.getValue());
}

test "NestedLists schema name permits typed nested UInt32 list views" {
    const allocator = std.testing.allocator;
    var message = capnpc.message.MessageBuilder.init(allocator);
    defer message.deinit();
    const value = try generated.NestedLists.Builder.init(&message);
    const rows = try value.nestedLists().initValues(2);
    const numbers = try rows.init(0, 3);
    try numbers.set(0, 0);
    try numbers.set(1, 123456789);
    try numbers.set(2, std.math.maxInt(u32));
    _ = try rows.init(1, 0);

    const bytes = try message.toBytes();
    defer allocator.free(bytes);
    var decoded = try capnpc.message.Message.init(allocator, bytes, .{});
    defer decoded.deinit();
    const reader = try generated.NestedLists.Reader.init(&decoded);
    const result = try reader.nestedLists().getValues();
    try std.testing.expectEqual(@as(u32, 2), result.len());
    const first = try result.get(0);
    try std.testing.expectEqual(@as(u32, 3), first.len());
    try std.testing.expectEqual(@as(u32, 0), try first.get(0));
    try std.testing.expectEqual(@as(u32, 123456789), try first.get(1));
    try std.testing.expectEqual(std.math.maxInt(u32), try first.get(2));
    try std.testing.expectEqual(@as(u32, 0), (try result.get(1)).len());
}

test "PointerKinds schema name permits constrained AnyStruct views" {
    const allocator = std.testing.allocator;
    var message = capnpc.message.MessageBuilder.init(allocator);
    defer message.deinit();
    const value = try generated.PointerKinds.Builder.init(&message);
    const slot = try value.pointerKinds().initValue();
    var payload = generated.Payload.Builder.wrap(try slot.init(1, 1));
    try payload.setNumber(42);
    try payload.setText("initial");
    var reopened = generated.Payload.Builder.wrap(try (try value.pointerKinds().getValue()).get());
    try reopened.setText("helper name 🦀");

    const bytes = try message.toBytes();
    defer allocator.free(bytes);
    var decoded = try capnpc.message.Message.init(allocator, bytes, .{});
    defer decoded.deinit();
    const reader = try generated.PointerKinds.Reader.init(&decoded);
    const result = generated.Payload.Reader.wrap(try reader.pointerKinds().getValue());
    try std.testing.expectEqual(@as(u32, 42), try result.getNumber());
    try std.testing.expectEqualStrings("helper name 🦀", try result.getText());
}

test "EnumOrdinals group remains distinct from the parent's ordinal view" {
    const allocator = std.testing.allocator;
    var message = capnpc.message.MessageBuilder.init(allocator);
    defer message.deinit();
    var value = try generated.GroupViews.Builder.init(&message);
    try value.enumOrdinals().setValue(1);
    var group = value.getEnumOrdinals();
    try group.setNumber(123456789);

    const bytes = try message.toBytes();
    defer allocator.free(bytes);
    var decoded = try capnpc.message.Message.init(allocator, bytes, .{});
    defer decoded.deinit();
    const reader = try generated.GroupViews.Reader.init(&decoded);
    try std.testing.expectEqual(generated.GroupViews.State.One, try reader.getValue());
    try std.testing.expectEqual(@as(u16, 1), try reader.enumOrdinals().getValue());
    try std.testing.expectEqual(@as(u32, 123456789), try reader.getEnumOrdinals().getNumber());
}
