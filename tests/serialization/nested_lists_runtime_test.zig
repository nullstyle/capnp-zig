const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;
const generated = @import("generated/nested_lists_runtime.zig");

test "checked recursive nested-list fixture reads and writes typed values" {
    var builder = message.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();

    var root = try generated.NestedListDemo.Builder.init(&builder);
    var rows = try root.nestedLists().initNumbersInSegment(3, 1);
    var first = try rows.initInSegment(0, 2, 2);
    try first.set(0, 10);
    try first.set(1, 20);
    try rows.setNull(1);
    _ = try rows.init(2, 0);

    var deep = try root.nestedLists().initDeepText(1);
    var middle = try deep.init(0, 1);
    var texts = try middle.init(0, 1);
    try texts.set(0, "nested");

    var records = try root.nestedLists().initRecords(1);
    var record_row = try records.init(0, 1);
    var record = try record_row.get(0);
    try record.setValue(99);

    var enums = try root.nestedLists().initEnumRows(1);
    var enum_row = try enums.init(0, 2);
    try enum_row.set(0, .Green);
    try enum_row.setOrdinal(1, 77);

    var grouped = root.getGrouped();
    var bool_rows = try grouped.nestedLists().initGroupRows(1);
    var bool_row = try bool_rows.init(0, 1);
    try bool_row.set(0, true);

    var choice_rows = try root.nestedLists().initChoiceRows(1);
    var choice_row = try choice_rows.init(0, 1);
    try choice_row.set(0, 123);

    const raw_builder: message.PointerListBuilder = try root.initNumbers(0);
    _ = raw_builder;
    rows = try root.nestedLists().initNumbersInSegment(3, 1);
    first = try rows.initInSegment(0, 2, 2);
    try first.set(0, 10);
    try first.set(1, 20);
    try rows.setNull(1);
    _ = try rows.init(2, 0);

    const bytes = try builder.toBytes();
    defer std.testing.allocator.free(bytes);
    var msg = try message.Message.initUnvalidated(std.testing.allocator, bytes);
    defer msg.deinit();

    const reader = try generated.NestedListDemo.Reader.init(&msg);
    const raw_reader: message.PointerListReader = try reader.getNumbers();
    _ = raw_reader;

    const read_rows = try reader.nestedLists().getNumbers();
    try std.testing.expectEqual(@as(u16, 20), try (try read_rows.get(0)).get(1));
    try std.testing.expect(try read_rows.isNull(1));
    try std.testing.expectEqual(@as(u32, 0), (try read_rows.get(1)).len());
    try std.testing.expect(!try read_rows.isNull(2));
    try std.testing.expectEqual(@as(u32, 0), (try read_rows.get(2)).len());

    const read_deep = try reader.nestedLists().getDeepText();
    try std.testing.expectEqualStrings("nested", try (try (try read_deep.get(0)).get(0)).get(0));
    try std.testing.expectEqual(@as(u16, 99), try (try (try (try reader.nestedLists().getRecords()).get(0)).get(0)).getValue());

    const read_enum = try (try reader.nestedLists().getEnumRows()).get(0);
    try std.testing.expectEqual(generated.Shade.Green, try read_enum.get(0));
    try std.testing.expectEqual(@as(u16, 77), try read_enum.getOrdinal(1));
    try std.testing.expectError(error.InvalidEnumValue, read_enum.get(1));

    const read_bool = try (try reader.getGrouped().nestedLists().getGroupRows()).get(0);
    try std.testing.expect(try read_bool.get(0));
    try std.testing.expectEqual(@as(u32, 123), try (try (try reader.nestedLists().getChoiceRows()).get(0)).get(0));

    const defaults = try reader.nestedLists().getDefaultRows();
    try std.testing.expectEqual(@as(u16, 5), try (try defaults.get(0)).get(1));
    try std.testing.expectEqual(@as(u32, 0), (try defaults.get(1)).len());
}

test "recursive nested-list union view rejects inactive stale pointer" {
    var builder = message.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    var root = try generated.NestedListDemo.Builder.init(&builder);
    var choice_rows = try root.nestedLists().initChoiceRows(1);
    _ = try choice_rows.init(0, 1);
    try root.setNone({});

    const bytes = try builder.toBytes();
    defer std.testing.allocator.free(bytes);
    var msg = try message.Message.initUnvalidated(std.testing.allocator, bytes);
    defer msg.deinit();
    const reader = try generated.NestedListDemo.Reader.init(&msg);
    try std.testing.expectError(error.WrongUnionMember, reader.nestedLists().getChoiceRows());
}

test "recursive nested-list union view preserves unknown-tag errors" {
    var builder = message.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    var raw_root = try builder.allocateStruct(1, 18);
    raw_root.writeU16(0, 77);

    const bytes = try builder.toBytes();
    defer std.testing.allocator.free(bytes);
    var msg = try message.Message.initUnvalidated(std.testing.allocator, bytes);
    defer msg.deinit();
    const reader = generated.NestedListDemo.Reader.wrap(try msg.getRootStruct());
    try std.testing.expectError(error.InvalidEnumValue, reader.nestedLists().getChoiceRows());
}

test "recursive nested-list defaults survive an old pointer layout" {
    var builder = message.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    const old_root = try builder.allocateStruct(1, 9);
    const root = generated.NestedListDemo.Builder.wrap(old_root);
    try std.testing.expect(!root.hasDefaultRows());

    const bytes = try builder.toBytes();
    defer std.testing.allocator.free(bytes);
    var msg = try message.Message.initUnvalidated(std.testing.allocator, bytes);
    defer msg.deinit();
    const reader = generated.NestedListDemo.Reader.wrap(try msg.getRootStruct());
    try std.testing.expect(!reader.hasDefaultRows());
    const defaults = try reader.nestedLists().getDefaultRows();
    try std.testing.expectEqual(@as(u16, 4), try (try defaults.get(0)).get(0));
}
