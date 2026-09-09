const std = @import("std");
const capnp = @import("capnpc-zig");
const message = capnp.message;
const reflection = capnp.reflection;
const helpers = capnp.generated_helpers;

fn structSchema(registry: reflection.Registry, name: []const u8) !reflection.StructSchema {
    for (registry.nodes()) |node| {
        if (std.mem.eql(u8, node.display_name, name)) return (try registry.get(node.id)).asStruct();
    }
    return error.SchemaNotFound;
}

fn reader(schema: reflection.StructSchema, builder: message.StructBuilder, storage: *helpers.ReaderStorage) !reflection.DynamicStruct.Reader {
    try storage.bind(builder.builder);
    return .{ .schema = schema, .reader = try storage.reader(builder) };
}

const unknown_text: [65536]u8 = @splat('x');

fn replaceWithAllocationFailures(allocator: std.mem.Allocator, schema: reflection.StructSchema) !void {
    const entry_schema = try (try (try schema.field("single")).type()).asStruct();
    var source = message.MessageBuilder.init(allocator);
    defer source.deinit();
    const physical = try source.allocateStruct(3, 3);
    physical.writeU64(0, 71);
    physical.writeU64(16, 0x1234abcd);
    try physical.writeText(0, "known source");
    try physical.writeText(2, &unknown_text);
    var source_storage = helpers.ReaderStorage.init(allocator);
    defer source_storage.deinit();
    const source_reader = try reader(entry_schema, physical, &source_storage);
    var destination = message.MessageBuilder.init(allocator);
    defer destination.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &destination);
    const old = try root.initStruct("single");
    try old.set("value", .{ .uint64 = 99 });
    try old.set("label", .{ .text = "retained on failure" });
    var destination_storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer destination_storage.deinit();
    root.set("single", .{ .@"struct" = source_reader }) catch |err| {
        const after = (try (try reader(schema, root.builder, &destination_storage)).get("single")).@"struct";
        try std.testing.expectEqual(@as(u64, 99), (try after.get("value")).uint64);
        try std.testing.expectEqualStrings("retained on failure", (try after.get("label")).text);
        return err;
    };
    const after = (try (try reader(schema, root.builder, &destination_storage)).get("single")).@"struct";
    try std.testing.expectEqual(@as(u64, 71), (try after.get("value")).uint64);
    try std.testing.expectEqual(@as(u64, 0x1234abcd), after.reader.readU64(16));
    try std.testing.expectEqualStrings(&unknown_text, try after.reader.readTextStrict(2));
}

test "dynamic struct replacement preserves the original value at every allocation failure" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, replaceWithAllocationFailures, .{try structSchema(registry, "reflection.capnp:Evolution")});
}

fn growWithAllocationFailures(allocator: std.mem.Allocator, schema: reflection.StructSchema) !void {
    var destination = message.MessageBuilder.init(allocator);
    defer destination.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &destination);
    const old = try root.builder.initStruct(3, 1, 1);
    old.writeU64(0, 99);
    try old.writeText(0, &unknown_text);
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    _ = root.getStruct("single") catch |err| {
        const after = (try (try reader(schema, root.builder, &storage)).get("single")).@"struct";
        try std.testing.expectEqual(@as(u16, 1), after.reader.data_size);
        try std.testing.expectEqual(@as(u16, 1), after.reader.pointer_count);
        try std.testing.expectEqual(@as(u64, 99), (try after.get("value")).uint64);
        try std.testing.expectEqualStrings(&unknown_text, (try after.get("label")).text);
        return err;
    };
    const after = (try (try reader(schema, root.builder, &storage)).get("single")).@"struct";
    try std.testing.expectEqual(@as(u16, 2), after.reader.data_size);
    try std.testing.expectEqual(@as(u16, 2), after.reader.pointer_count);
    try std.testing.expectEqualStrings(&unknown_text, (try after.get("label")).text);
}

test "dynamic struct growth preserves old layout and reachable values at every allocation failure" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, growWithAllocationFailures, .{try structSchema(registry, "reflection.capnp:Evolution")});
}

fn groupWithAllocationFailures(allocator: std.mem.Allocator, schema: reflection.StructSchema, original_discriminant: u16) !void {
    var source = message.MessageBuilder.init(allocator);
    defer source.deinit();
    const source_root = try reflection.DynamicStruct.Builder.init(schema, &source);
    const source_group = try source_root.initGroup("selected");
    try source_group.set("name", .{ .text = "source selected" });
    try source_group.set("payload", .{ .data = &unknown_text });
    var source_storage = helpers.ReaderStorage.init(allocator);
    defer source_storage.deinit();
    const source_reader = try reader(source_group.schema, source_group.builder, &source_storage);

    var destination = message.MessageBuilder.init(allocator);
    defer destination.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &destination);
    const old = try root.initGroup("selected");
    try old.set("name", .{ .text = "original selected" });
    try old.set("payload", .{ .data = "original payload" });
    try root.set("high", .{ .uint64 = 991 });
    // An inactive arm can still own reachable storage. A failed replacement
    // must preserve both that storage and the containing union's selection.
    try root.builder.writeU16Strict(@as(usize, schema.proto().discriminant_offset) * 2, original_discriminant);
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    root.set("selected", .{ .@"struct" = source_reader }) catch |err| {
        const after = try reader(schema, root.builder, &storage);
        try std.testing.expectEqual(@as(?u16, original_discriminant), after.whichDiscriminant());
        if (original_discriminant == 0) try std.testing.expectEqualStrings("none", (try after.which()).?.proto().name) else try std.testing.expect((try after.which()) == null);
        try std.testing.expectEqual(@as(u64, 991), (try after.get("high")).uint64);
        const group = reflection.DynamicStruct.Reader{ .schema = old.schema, .reader = after.reader };
        try std.testing.expectEqualStrings("original selected", (try group.get("name")).text);
        try std.testing.expectEqualStrings("original payload", (try group.get("payload")).data);
        return err;
    };
    const after = try reader(schema, root.builder, &storage);
    try std.testing.expectEqualStrings("selected", (try after.which()).?.proto().name);
    try std.testing.expectEqual(@as(u64, 991), (try after.get("high")).uint64);
    const selected = (try after.get("selected")).@"struct";
    try std.testing.expectEqualStrings("source selected", (try selected.get("name")).text);
    try std.testing.expectEqualStrings(&unknown_text, (try selected.get("payload")).data);
}

test "dynamic group copy preserves original storage and union selection at every allocation failure" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    for ([_]u16{ 0, 234 }) |ordinal| try std.testing.checkAllAllocationFailures(std.testing.allocator, groupWithAllocationFailures, .{ try structSchema(registry, "values.capnp:Values"), ordinal });
}

fn listElementWithAllocationFailures(allocator: std.mem.Allocator, schema: reflection.StructSchema) !void {
    var source = message.MessageBuilder.init(allocator);
    defer source.deinit();
    const physical = try source.allocateStruct(3, 3);
    physical.writeU64(0, 71);
    physical.writeU64(16, 0xcafe);
    try physical.writeText(0, "source label");
    try physical.writeText(2, &unknown_text);
    var source_storage = helpers.ReaderStorage.init(allocator);
    defer source_storage.deinit();
    const entry_schema = try (try (try schema.field("single")).type()).asStruct();
    const source_reader = try reader(entry_schema, physical, &source_storage);
    var destination = message.MessageBuilder.init(allocator);
    defer destination.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &destination);
    const physical_list = try root.builder.writeStructList(0, 2, 3, 3);
    for (0..2) |i| {
        const old = try physical_list.get(@intCast(i));
        old.writeU64(0, 90 + i);
        old.writeU64(16, 0xbeef + i);
        try old.writeText(0, "original label");
        try old.writeText(2, "original unknown");
    }
    const list = try root.getList("records");
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    list.set(1, .{ .@"struct" = source_reader }) catch |err| {
        const after = (try (try reader(schema, root.builder, &storage)).get("records")).list;
        for (0..2) |i| {
            const old = (try after.get(@intCast(i))).@"struct";
            try std.testing.expectEqual(@as(u64, 90 + i), (try old.get("value")).uint64);
            try std.testing.expectEqual(@as(u64, 0xbeef + i), old.reader.readU64(16));
            try std.testing.expectEqualStrings("original label", (try old.get("label")).text);
            try std.testing.expectEqualStrings("original unknown", try old.reader.readTextStrict(2));
        }
        return err;
    };
    const after = (try (try reader(schema, root.builder, &storage)).get("records")).list;
    try std.testing.expectEqual(@as(u64, 90), (try (try after.get(0)).@"struct".get("value")).uint64);
    const replaced = (try after.get(1)).@"struct";
    try std.testing.expectEqual(@as(u64, 71), (try replaced.get("value")).uint64);
    try std.testing.expectEqual(@as(u64, 0xcafe), replaced.reader.readU64(16));
    try std.testing.expectEqualStrings(&unknown_text, try replaced.reader.readTextStrict(2));
}

test "dynamic same-size list element replacement rolls back every allocation failure" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, listElementWithAllocationFailures, .{try structSchema(registry, "reflection.capnp:Evolution")});
}

test "dynamic list pointer replacement supports a reader of its own builder" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    const schema = try structSchema(registry, "values.capnp:Values");
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &arena);
    const list = try root.initList("numbers", 8192);
    try list.set(0, .{ .uint64 = 41 });
    try list.set(8191, .{ .uint64 = 99 });
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const source = try reader(schema, root.builder, &storage);
    try root.set("numbers", try source.get("numbers"));
    const after = (try (try reader(schema, root.builder, &storage)).get("numbers")).list;
    try std.testing.expectEqual(@as(u32, 8192), try after.len());
    try std.testing.expectEqual(@as(u64, 41), (try after.get(0)).uint64);
    try std.testing.expectEqual(@as(u64, 99), (try after.get(8191)).uint64);
}

test "dynamic Text and Data replacement support slices borrowed from their own builder" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    const schema = try structSchema(registry, "values.capnp:Values");
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &arena);
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    for ([_][]const u8{ "text", "raw" }) |name| {
        const value: reflection.Value = if (std.mem.eql(u8, name, "text")) .{ .text = &unknown_text } else .{ .data = &unknown_text };
        try root.set(name, value);
        const source = try reader(schema, root.builder, &storage);
        try root.set(name, try source.get(name));
        const after = try (try reader(schema, root.builder, &storage)).get(name);
        try std.testing.expectEqualStrings(&unknown_text, if (after == .text) after.text else after.data);
    }
}

test "dynamic Builder scalar and presence queries reuse defaults and union rules without allocation" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    const schema = try structSchema(registry, "values.capnp:Values");
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &arena);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    arena.allocator = failing.allocator();
    defer arena.allocator = std.testing.allocator;
    try std.testing.expectEqual(std.math.maxInt(u64), (try root.getScalar("high")).uint64);
    try std.testing.expectEqual(std.math.minInt(i64), (try root.getScalarField(try schema.field("low"))).int64);
    try std.testing.expect(try root.has("high"));
    try std.testing.expect(!(try root.hasNonDefault("high")));
    try std.testing.expect(!(try root.has("record")));
    try std.testing.expect(try root.has("details"));
    try std.testing.expect(!(try root.has("selected")));
    try std.testing.expectEqualStrings("none", (try root.which()).?.proto().name);
    try std.testing.expectEqual(@as(?u16, 0), root.whichDiscriminant());
    try std.testing.expectError(error.TypeMismatch, root.getScalar("text"));
    try std.testing.expectError(error.InactiveUnionField, root.getScalar("selected"));
    try root.set("high", .{ .uint64 = 42 });
    try std.testing.expectEqual(@as(u64, 42), (try root.getScalar("high")).uint64);
    try std.testing.expect(try root.hasNonDefault("high"));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    arena.allocator = std.testing.allocator;
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try std.testing.expectEqualStrings("constant é", (try (try (try root.asReader(&storage)).get("record")).@"struct".get("label")).text);
    const selected = try root.initGroup("selected");
    try selected.set("name", .{ .text = "rebound" });
    try std.testing.expectEqualStrings("selected", (try root.which()).?.proto().name);
    const after = try root.asReader(&storage);
    try std.testing.expectEqualStrings("rebound", (try (try after.get("selected")).@"struct".get("name")).text);
    try std.testing.expect(try root.has("selected"));
    // Unknown future discriminants are visible without inventing a field.
    try root.builder.writeU16Strict(@as(usize, schema.proto().discriminant_offset) * 2, 234);
    try std.testing.expectEqual(@as(?u16, 234), root.whichDiscriminant());
    try std.testing.expect((try root.which()) == null);
    try std.testing.expect(!(try root.has("selected")));
}

test "dynamic list Builder scalar reads and reader views observe mutation and growth" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    const schema = try structSchema(registry, "reflection.capnp:Scalars");
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &arena);
    const list = try root.initList("signed", 2);
    try list.set(1, .{ .int16 = -819 });
    try std.testing.expectEqual(@as(i16, -819), (try list.getScalar(1)).int16);
    try std.testing.expectError(error.IndexOutOfBounds, list.getScalar(2));
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try std.testing.expectEqual(@as(i16, -819), (try (try list.asReader(&storage)).get(1)).int16);
    try list.set(1, .{ .int16 = 135 });
    try std.testing.expectEqual(@as(i16, 135), (try (try list.asReader(&storage)).get(1)).int16);
    const texts = try root.initList("texts", 1);
    try std.testing.expectError(error.TypeMismatch, texts.getScalar(0));
}

test "dynamic copy options reject expanded work before publication and propagate to child builders" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    const schema = try structSchema(registry, "reflection.capnp:Evolution");
    var source = message.MessageBuilder.init(std.testing.allocator);
    defer source.deinit();
    const physical = try source.allocateStruct(2, 2);
    physical.writeU64(0, 71);
    try physical.writeText(0, "copied label");
    var source_storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer source_storage.deinit();
    const entry_schema = try (try (try schema.field("single")).type()).asStruct();
    const source_reader = try reader(entry_schema, physical, &source_storage);
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var root = try reflection.DynamicStruct.Builder.init(schema, &arena);
    try (try root.initStruct("single")).set("value", .{ .uint64 = 99 });
    const list = try root.initList("records", 1);
    try (try list.getStruct(0)).set("value", .{ .uint64 = 98 });
    root.copy_options = .{ .max_work = 1 };
    try std.testing.expectError(error.CopyWorkLimitExceeded, root.set("single", .{ .@"struct" = source_reader }));
    try std.testing.expectEqual(@as(u64, 99), (try (try root.getStruct("single")).getScalar("value")).uint64);
    const restricted_list = try root.getList("records");
    try std.testing.expectEqual(@as(usize, 1), restricted_list.copy_options.max_work);
    try std.testing.expectError(error.CopyWorkLimitExceeded, restricted_list.set(0, .{ .@"struct" = source_reader }));
    try std.testing.expectEqual(@as(u64, 98), (try (try restricted_list.getStruct(0)).getScalar("value")).uint64);
}

test "dynamic self and overlapping copies preserve known values and unknown physical fields" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    const values_schema = try structSchema(registry, "values.capnp:Values");
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    const info = values_schema.proto();
    const physical = try arena.allocateStruct(info.data_word_count + 1, info.pointer_count + 1);
    const root = reflection.DynamicStruct.Builder{ .schema = values_schema, .builder = physical };
    physical.writeU64(@as(usize, info.data_word_count) * 8, 0xfeedface);
    try physical.writeText(info.pointer_count, "unknown parent pointer");
    try root.set("high", .{ .uint64 = 191 });
    const group = try root.initGroup("selected");
    try group.set("name", .{ .text = &unknown_text });
    try group.set("payload", .{ .data = "overlapping payload" });
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try root.set("selected", .{ .@"struct" = try group.asReader(&storage) });
    const after = try root.asReader(&storage);
    try std.testing.expectEqual(@as(u64, 0xfeedface), after.reader.readU64(@as(usize, info.data_word_count) * 8));
    try std.testing.expectEqualStrings("unknown parent pointer", try after.reader.readTextStrict(info.pointer_count));
    try std.testing.expectEqual(@as(u64, 191), (try after.get("high")).uint64);
    try std.testing.expectEqualStrings(&unknown_text, (try (try after.get("selected")).@"struct".get("name")).text);
    try std.testing.expectEqualStrings("overlapping payload", (try (try after.get("selected")).@"struct".get("payload")).data);

    const schema = try structSchema(registry, "reflection.capnp:Evolution");
    var evolution_arena = message.MessageBuilder.init(std.testing.allocator);
    defer evolution_arena.deinit();
    const evolution = try reflection.DynamicStruct.Builder.init(schema, &evolution_arena);
    const old = try evolution.builder.writeStructList(0, 2, 1, 1);
    (try old.get(0)).writeU64(0, 71);
    try (try old.get(0)).writeText(0, &unknown_text);
    (try old.get(1)).writeU64(0, 91);
    try (try old.get(1)).writeText(0, "sibling label");
    const list = try evolution.getList("records");
    // The source aliases the old list which replacement must widen. Reacquire
    // elements afterward; previously acquired views expire on list growth.
    const from = try (try list.asReader(&storage)).get(0);
    try list.set(1, from);
    const copied = try list.getStruct(1);
    try std.testing.expectEqual(@as(u64, 71), (try copied.getScalar("value")).uint64);
    try std.testing.expectEqualStrings(&unknown_text, (try (try copied.asReader(&storage)).get("label")).text);
    try list.set(1, .{ .@"struct" = try copied.asReader(&storage) });
    try std.testing.expectEqualStrings(&unknown_text, (try (try (try list.getStruct(1)).asReader(&storage)).get("label")).text);
}

test "dynamic copy allocation limits preserve a group's union and an existing field" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    const schema = try structSchema(registry, "values.capnp:Values");
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var root = try reflection.DynamicStruct.Builder.init(schema, &arena);
    try root.set("text", .{ .text = "retained" });
    const group = try root.initGroup("selected");
    try group.set("name", .{ .text = "original group" });
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    const source = try group.asReader(&storage);
    root.copy_options.max_allocation_bytes = 0;
    try std.testing.expectError(error.CopyAllocationLimitExceeded, root.set("text", .{ .text = "rejected" }));
    try std.testing.expectError(error.CopyAllocationLimitExceeded, root.set("selected", .{ .@"struct" = source }));
    const after = try root.asReader(&storage);
    try std.testing.expectEqualStrings("retained", (try after.get("text")).text);
    try std.testing.expectEqualStrings("selected", (try after.which()).?.proto().name);
    try std.testing.expectEqualStrings("original group", (try (try after.get("selected")).@"struct".get("name")).text);
}

test "dynamic group copying preserves pointer absence as well as logical defaults" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    const schema = try structSchema(registry, "values.capnp:Values");
    var source = message.MessageBuilder.init(std.testing.allocator);
    defer source.deinit();
    const source_root = try reflection.DynamicStruct.Builder.init(schema, &source);
    const source_group = try source_root.initGroup("selected");
    var source_storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer source_storage.deinit();
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &arena);
    try (try root.initGroup("selected")).set("name", .{ .text = "replaced" });
    try root.set("selected", .{ .@"struct" = try source_group.asReader(&source_storage) });
    const copied = try root.getStruct("selected");
    try std.testing.expect(!(try copied.has("name")));
    try std.testing.expect(!(try copied.has("payload")));
}

test "dynamic null struct reopening observes copy growth limits before publishing storage" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    const schema = try structSchema(registry, "reflection.capnp:Evolution");
    var arena = message.MessageBuilder.init(std.testing.allocator);
    defer arena.deinit();
    var root = try reflection.DynamicStruct.Builder.init(schema, &arena);
    root.copy_options.max_output_words = 0;
    try std.testing.expectError(error.CopyOutputLimitExceeded, root.getStruct("single"));
    try std.testing.expect(!(try root.has("single")));
}

test "dynamic list growth charges aggregate work and widened output at their exact boundaries" {
    const registry = try reflection.Registry.init(std.testing.allocator, @embedFile("request.bin"));
    defer registry.deinit();
    const schema = try structSchema(registry, "reflection.capnp:Evolution");
    const Case = struct { work: usize, words: usize, expected_error: ?anyerror };
    for ([_]Case{
        .{ .work = 6, .words = 9, .expected_error = error.CopyWorkLimitExceeded },
        .{ .work = 7, .words = 8, .expected_error = error.CopyOutputLimitExceeded },
        .{ .work = 7, .words = 9, .expected_error = null },
        .{ .work = 8, .words = 10, .expected_error = null },
    }) |case| {
        var arena = message.MessageBuilder.init(std.testing.allocator);
        defer arena.deinit();
        var root = try reflection.DynamicStruct.Builder.init(schema, &arena);
        const physical = try root.builder.writeStructList(0, 2, 1, 1);
        (try physical.get(0)).writeU64(0, 10);
        (try physical.get(1)).writeU64(0, 11);
        root.copy_options = .{ .max_work = case.work, .max_output_words = case.words };
        const list = try root.getList("records");
        if (case.expected_error) |expected_error| {
            try std.testing.expectError(expected_error, list.getStruct(0));
        } else _ = try list.getStruct(0);
        var storage = helpers.ReaderStorage.init(std.testing.allocator);
        defer storage.deinit();
        const after = try list.asReader(&storage);
        try std.testing.expectEqual(@as(u64, 10), (try (try after.get(0)).@"struct".get("value")).uint64);
        try std.testing.expectEqual(@as(u64, 11), (try (try after.get(1)).@"struct".get("value")).uint64);
        const layout = try after.reader.getStructList();
        try std.testing.expectEqual(@as(u16, if (case.expected_error == null) 2 else 1), layout.data_words);
        try std.testing.expectEqual(@as(u16, if (case.expected_error == null) 2 else 1), layout.pointer_words);
    }
}
