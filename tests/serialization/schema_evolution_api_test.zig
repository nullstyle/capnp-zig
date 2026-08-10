const std = @import("std");
const capnpc = @import("capnpc-zig");
const v1 = @import("schema-evolution-v1");
const v2 = @import("schema-evolution-v2");

const message = capnpc.message;
const testing = std.testing;

test "unknown enum ordinals survive an older generated reader and builder" {
    var source_builder = message.MessageBuilder.init(testing.allocator);
    defer source_builder.deinit();

    var source = try v2.Evolution.Builder.init(&source_builder);
    try source.enumOrdinals().setStatus(2);
    var source_statuses = try source.initStatuses(2);
    try source_statuses.setOrdinal(0, 2);
    try source_statuses.set(1, .Ready);

    const source_bytes = try source_builder.toBytes();
    defer testing.allocator.free(source_bytes);

    var source_message = try message.Message.init(testing.allocator, source_bytes, .{});
    defer source_message.deinit();

    const raw_source = try source_message.getRootStruct();
    try testing.expectEqual(@as(u16, 2 ^ 1), raw_source.readU16(0));

    const older = try v1.Evolution.Reader.init(&source_message);
    try testing.expectError(error.InvalidEnumValue, older.getStatus());
    const forwarded_status = try older.enumOrdinals().getStatus();
    try testing.expectEqual(@as(u16, 2), forwarded_status);

    const older_statuses = try older.getStatuses();
    try testing.expectError(error.InvalidEnumValue, older_statuses.get(0));
    const forwarded_list_status = try older_statuses.getOrdinal(0);
    try testing.expectEqual(@as(u16, 2), forwarded_list_status);
    try testing.expectEqual(v1.Status.Ready, try older_statuses.get(1));

    var relay_builder = message.MessageBuilder.init(testing.allocator);
    defer relay_builder.deinit();

    var relay = try v1.Evolution.Builder.init(&relay_builder);
    try relay.enumOrdinals().setStatus(forwarded_status);
    var relay_statuses = try relay.initStatuses(1);
    try relay_statuses.setOrdinal(0, forwarded_list_status);

    const relay_bytes = try relay_builder.toBytes();
    defer testing.allocator.free(relay_bytes);

    var relay_message = try message.Message.init(testing.allocator, relay_bytes, .{});
    defer relay_message.deinit();

    const newer = try v2.Evolution.Reader.init(&relay_message);
    try testing.expectEqual(v2.Status.Future, try newer.getStatus());
    try testing.expectEqual(v2.Status.Future, try (try newer.getStatuses()).get(0));
}

test "enum ordinal views apply defaults in structs and union groups" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root = try v2.Evolution.Builder.init(&builder);
    var details = root.initDetails();
    try details.enumOrdinals().setState(2);
    try details.setNote("");
    try testing.expect(details.hasNote());

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const reader = try v2.Evolution.Reader.init(&msg);
    // An untouched non-zero default is returned as its logical ordinal.
    try testing.expectEqual(@as(u16, 1), try reader.enumOrdinals().getStatus());
    const read_details = try reader.getDetails();
    try testing.expectEqual(@as(u16, 2), try read_details.enumOrdinals().getState());
    try testing.expectEqual(v2.Status.Future, try read_details.getState());
    try testing.expect(read_details.hasNote());
}

test "whichOrdinal observes a union arm unknown to the older schema" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root = try v2.Evolution.Builder.init(&builder);
    try root.setFutureLabel("new arm");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const older = try v1.Evolution.Reader.init(&msg);
    try testing.expectEqual(@as(u16, 3), older.whichOrdinal());
    try testing.expectError(error.InvalidEnumValue, older.which());
    // `futureLabel` shares label's pointer slot. The unknown arm must be
    // rejected before that nonzero pointer is treated as label presence.
    try testing.expect(!older.hasLabel());

    const newer = try v2.Evolution.Reader.init(&msg);
    try testing.expectEqual(v2.Evolution.WhichTag.futureLabel, try newer.which());
    try testing.expect(newer.hasFutureLabel());
    try testing.expectEqualStrings("new arm", try newer.getFutureLabel());
}

test "generated pointer presence distinguishes absent and explicit empty values" {
    try testing.expect(!@hasDecl(v1.Evolution.Reader, "hasStatus"));
    try testing.expect(!@hasDecl(v1.Evolution.Builder, "hasStatus"));
    try testing.expect(!@hasDecl(v1.Evolution.Reader, "hasDetails"));
    try testing.expect(@hasDecl(v1.Evolution.Details.Reader, "hasNote"));

    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root = try v1.Evolution.Builder.init(&builder);
    try testing.expect(!root.hasStatuses());
    try testing.expect(!root.hasText());
    try testing.expect(!root.hasData());
    try testing.expect(!root.hasChild());
    try testing.expect(!root.hasChildren());
    try testing.expect(!root.hasAny());
    try testing.expect(!root.hasService());
    try testing.expect(!root.hasDefaultText());
    try testing.expect(!root.hasEmpty());
    try testing.expect(!root.hasLabel());

    const absent_bytes = try builder.toBytes();
    defer testing.allocator.free(absent_bytes);
    var absent_message = try message.Message.init(testing.allocator, absent_bytes, .{});
    defer absent_message.deinit();
    const absent_reader = try v1.Evolution.Reader.init(&absent_message);
    try testing.expect(!absent_reader.hasDefaultText());
    try testing.expectEqualStrings("fallback", try absent_reader.getDefaultText());

    _ = try root.initStatuses(0);
    try root.setText("");
    try root.setData("");
    _ = try root.initChild();
    _ = try root.initChildren(0);
    try root.setAnyText("");
    try root.setServiceCapability(.{ .id = 7 });
    try root.setDefaultText("fallback");
    _ = try root.initEmpty();
    try root.setLabel("");

    try testing.expect(root.hasStatuses());
    try testing.expect(root.hasText());
    try testing.expect(root.hasData());
    try testing.expect(root.hasChild());
    try testing.expect(root.hasChildren());
    try testing.expect(root.hasAny());
    try testing.expect(root.hasService());
    try testing.expect(root.hasDefaultText());
    try testing.expect(root.hasEmpty());
    try testing.expect(root.hasLabel());

    // Selecting the void arm deliberately leaves label's shared pointer word
    // untouched. Presence must check the discriminant first.
    try root.setNone({});
    try testing.expect(!root.hasLabel());

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const reader = try v1.Evolution.Reader.init(&msg);
    try testing.expect(reader.hasStatuses());
    try testing.expect(reader.hasText());
    try testing.expect(reader.hasData());
    try testing.expect(reader.hasChild());
    try testing.expect(reader.hasChildren());
    try testing.expect(reader.hasAny());
    try testing.expect(reader.hasService());
    try testing.expect(reader.hasEmpty());
    try testing.expect(reader.hasDefaultText());
    try testing.expectEqualStrings("fallback", try reader.getDefaultText());
    try testing.expect(!reader.hasLabel());
}

test "presence recognizes far pointers and does not validate pointer kind" {
    {
        var builder = message.MessageBuilder.init(testing.allocator);
        defer builder.deinit();

        var root = try v1.Evolution.Builder.init(&builder);
        const target_segment = try builder.createSegment();
        try root._builder.writeTextInSegment(1, "far", target_segment);
        try testing.expect(root.hasText());

        const bytes = try builder.toBytes();
        defer testing.allocator.free(bytes);

        var msg = try message.Message.init(testing.allocator, bytes, .{});
        defer msg.deinit();

        const reader = try v1.Evolution.Reader.init(&msg);
        try testing.expect(reader.hasText());
        try testing.expectEqualStrings("far", try reader.getText());
    }

    {
        var builder = message.MessageBuilder.init(testing.allocator);
        defer builder.deinit();

        var root = try v1.Evolution.Builder.init(&builder);
        // Store a valid, nonzero capability pointer in the Text slot. Presence
        // is structural; resolving the slot as Text still rejects its type.
        var wrong_kind = try root._builder.getAnyPointer(1);
        try wrong_kind.setCapability(.{ .id = 9 });
        try testing.expect(root.hasText());

        const bytes = try builder.toBytes();
        defer testing.allocator.free(bytes);

        var msg = try message.Message.init(testing.allocator, bytes, .{});
        defer msg.deinit();

        const reader = try v1.Evolution.Reader.init(&msg);
        try testing.expect(reader.hasText());
        try testing.expectError(error.InvalidPointer, reader.getText());
    }
}

test "old layouts stay absent and initialized empty structs stay present" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root = try v1.Evolution.Builder.init(&builder);
    _ = try root.initEmpty();

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const newer = try v2.Evolution.Reader.init(&msg);
    try testing.expect(newer.hasEmpty());
    _ = try newer.getEmpty();
    try testing.expect(!newer.hasAddedText());
    try testing.expectEqualStrings("", try newer.getAddedText());
}
