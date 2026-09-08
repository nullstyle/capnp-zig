const std = @import("std");
const output = @import("output.zig");
const capnpc = @import("capnpc-zig");
const reflection = capnpc.reflection;
const message = capnpc.message;
const generated = @import("generated");
const values = generated.values;
const brands = generated.brands;
const scalars = generated.scalars;
const expect = std.testing.expect;
const equal = std.testing.expectEqual;
const strings = std.testing.expectEqualStrings;
const expectError = std.testing.expectError;

fn write(init: std.process.Init, path: []const u8, bytes: []const u8) !void {
    try output.write(init, path, bytes);
}

fn checkMetadata(registry: reflection.Registry) !void {
    const node = try values.Values.capnpSchema.resolve(registry);
    try equal(values.Values.capnpSchema.id, node.id());
    try equal(node.id(), (try node.raw()).readU64(0));
    try equal(capnpc.schema.NodeKind.@"struct", node.kind());
    try expectError(error.SchemaNotFound, registry.get(0));
    try expectError(error.TypeMismatch, node.asEnum());
    const value = try node.asStruct();
    try expectError(error.FieldNotFound, value.field("missing"));
    const high = try value.field("high");
    try equal(@as(?u16, 4), try high.explicitOrdinal());
    try expect(try high.hadExplicitDefault());
    try equal(std.math.maxInt(u64), high.proto().slot.?.default_value.?.uint64);
    const tagged = try value.field("tagged");
    try strings("field annotation", tagged.proto().annotations[0].value.text);
    try equal(capnpc.schema.NodeKind.annotation, (try registry.get(tagged.proto().annotations[0].id)).kind());
    const details = try (try value.field("details")).groupSchema();
    try expect(details.proto().is_group);
    const mode = try (try (try details.field("mode")).type()).asEnum();
    try equal(@as(u16, 1), try mode.ordinal("archived"));
    try strings("active", mode.name(0).?);
    try expect(mode.name(60000) == null);
    try expectError(error.EnumerantNotFound, mode.ordinal("missing"));

    const branded = try (try brands.Brands.capnpSchema.resolve(registry)).asStruct();
    const text_box = try (try (try branded.field("text")).type()).asStruct();
    try expect((try (try (try text_box.field("value")).type()).proto()) == .text);
    const pair = try (try (try branded.field("pair")).type()).asStruct();
    try expect((try (try (try pair.field("first")).type()).proto()) == .text);
    try expect((try (try (try pair.field("second")).type()).proto()) == .data);
    const unbound = try (try (try branded.field("unbound")).type()).asStruct();
    try expect(try (try (try unbound.field("value")).type()).isUnbound());
    const lookup = try (try scalars.Lookup.capnpSchema.resolve(registry)).asInterface();
    const method = try lookup.method("find");
    try expect((try (try (try (try method.params()).field("name")).type()).proto()) == .text);
    const result = try (try (try (try method.results()).field("value")).type()).asStruct();
    try equal(scalars.Scalars.capnpSchema.id, result.schema.id());
}

fn checkValueDefaults(reader: reflection.DynamicStruct.Reader) !void {
    try expect(try reader.has("high"));
    try expect(try reader.has("details"));
    try expect(try reader.has("none"));
    try expect(!(try reader.has("text")));
    try expect(!(try reader.has("record")));
    try expect(!(try reader.has("selected")));
    try expect(!(try reader.hasNonDefault("low")));
    try expect(!(try reader.hasNonDefault("none")));
    try expect(try reader.hasNonDefault("details"));
    try equal(std.math.minInt(i64), (try reader.get("low")).int64);
    try strings("Embedded text: café, Tromsø, 🦀.\nSecond line.\n", (try reader.get("text")).text);
    try strings("NUL:\x00; café 🦀\n", (try reader.get("inlineText")).text);
    const raw = (try reader.get("raw")).data;
    try equal(@as(usize, 256), raw.len);
    for (raw, 0..) |byte, index| try equal(@as(u8, @intCast(index)), byte);
    const numbers = (try reader.get("numbers")).list;
    try equal(@as(u32, 3), try numbers.len());
    try equal(std.math.maxInt(u64), (try numbers.get(2)).uint64);
    const record = (try reader.get("record")).@"struct";
    try strings("constant é", (try record.get("label")).text);
    try equal(@as(u16, 0), (try record.get("mode")).@"enum".ordinal);
    const records = (try reader.get("records")).list;
    const second = (try records.get(1)).@"struct";
    try strings("default 🦀", (try second.get("label")).text);
    try equal(@as(u16, 0), (try second.get("mode")).@"enum".ordinal);
    const details = (try reader.get("details")).@"struct";
    try expect((try details.get("enabled")).bool);
    try equal(@as(u16, 1), (try details.get("mode")).@"enum".ordinal);
    try strings("none", (try reader.which()).?.proto().name);
    try expectError(error.InactiveUnionField, reader.get("selected"));
    try expectError(error.FieldNotFound, reader.get("absent"));
}

fn valuesRoundtrip(init: std.process.Init, registry: reflection.Registry) !void {
    const schema = try (try values.Values.capnpSchema.resolve(registry)).asStruct();
    var builder = message.MessageBuilder.init(init.gpa);
    defer builder.deinit();
    var typed = try values.Values.Builder.init(&builder);
    try typed.setHigh(13);
    {
        const initial = try builder.toBytes();
        defer init.gpa.free(initial);
        var decoded = try message.Message.init(init.gpa, initial, .{});
        defer decoded.deinit();
        const reader = try reflection.DynamicStruct.Reader.init(schema, &decoded);
        try checkValueDefaults(reader);
        try equal(@as(u64, 13), (try reader.get("high")).uint64);
        try expect(try reader.has("high"));
        try expect(try reader.hasNonDefault("high"));
        try expect(!(try reader.has("record")));
    }
    var dynamic = reflection.DynamicStruct.Builder{ .schema = schema, .builder = typed._builder };
    try expectError(error.TypeMismatch, dynamic.set("high", .{ .int64 = 42 }));
    try dynamic.set("high", .{ .uint64 = 42 });
    try dynamic.set("text", .{ .text = "dynamic café" });
    try dynamic.set("inlineData", .{ .data = &.{ 0, 128, 255 } });
    var record = try dynamic.getStruct("record");
    try record.set("label", .{ .text = "mutable default" });
    var numbers = try dynamic.getList("numbers");
    try numbers.set(0, .{ .uint64 = 7 });
    var records = try dynamic.initList("records", 2);
    var first = try records.initStruct(0);
    try first.set("label", .{ .text = "list first" });
    var second = try records.initStruct(1);
    try second.set("label", .{ .text = "list second" });
    var details = try dynamic.initGroup("details");
    try details.set("enabled", .{ .bool = false });
    const mode_schema = try (try (try details.schema.field("mode")).type()).asEnum();
    try details.set("mode", .{ .@"enum" = .{ .schema = mode_schema, .ordinal = 60000 } });
    var selected = try dynamic.initGroup("selected");
    try selected.set("name", .{ .text = "discarded union" });
    try dynamic.clear("selected");
    selected = try dynamic.initGroup("selected");
    try selected.set("name", .{ .text = "dynamic union" });
    try selected.set("payload", .{ .data = &.{ 0, 128, 255 } });

    const bytes = try builder.toBytes();
    defer init.gpa.free(bytes);
    var decoded = try message.Message.init(init.gpa, bytes, .{});
    defer decoded.deinit();
    const typed_reader = try values.Values.Reader.init(&decoded);
    try equal(@as(u64, 42), try typed_reader.getHigh());
    try strings("dynamic café", try typed_reader.getText());
    try strings("mutable default", try (try typed_reader.getRecord()).getLabel());
    try equal(@as(u64, 7), try (try typed_reader.getNumbers()).get(0));
    try strings("list second", try (try (try typed_reader.getRecords()).get(1)).getLabel());
    try expect(!(try typed_reader.getDetails().getEnabled()));
    try strings("dynamic union", try (try typed_reader.getSelected()).getName());
    const dynamic_reader = try reflection.DynamicStruct.Reader.init(schema, &decoded);
    try strings("selected", (try dynamic_reader.which()).?.proto().name);
    try expectError(error.InactiveUnionField, dynamic_reader.get("none"));
    try equal(@as(u16, 60000), (try (try dynamic_reader.get("details")).@"struct".get("mode")).@"enum".ordinal);
    try write(init, "values.bin", bytes);

    // Reopening mutable defaults must never alter the registry's shared defaults.
    var fresh = message.MessageBuilder.init(init.gpa);
    defer fresh.deinit();
    _ = try values.Values.Builder.init(&fresh);
    const fresh_bytes = try fresh.toBytes();
    defer init.gpa.free(fresh_bytes);
    var fresh_message = try message.Message.init(init.gpa, fresh_bytes, .{});
    defer fresh_message.deinit();
    try checkValueDefaults(try reflection.DynamicStruct.Reader.init(schema, &fresh_message));

    // A message from an older schema can hold a physically smaller child.
    // Reopening it must grow storage before writes to newly known fields.
    var old = message.MessageBuilder.init(init.gpa);
    defer old.deinit();
    var old_root = try reflection.DynamicStruct.Builder.init(schema, &old);
    const record_slot = (try schema.field("record")).proto().slot.?.offset;
    _ = try old_root.builder.initStruct(record_slot, 0, 0);
    var grown = try old_root.getStruct("record");
    try grown.set("label", .{ .text = "grown record" });
    const grown_bytes = try old.toBytes();
    defer init.gpa.free(grown_bytes);
    var grown_message = try message.Message.init(init.gpa, grown_bytes, .{});
    defer grown_message.deinit();
    const grown_value = try values.Values.Reader.init(&grown_message);
    try strings("grown record", try (try grown_value.getRecord()).getLabel());
}

fn brandsRoundtrip(init: std.process.Init, registry: reflection.Registry) !void {
    const schema = try (try brands.Brands.capnpSchema.resolve(registry)).asStruct();
    var builder = message.MessageBuilder.init(init.gpa);
    defer builder.deinit();
    const typed = try brands.Brands.Builder.init(&builder);
    const bytes = try builder.toBytes();
    defer init.gpa.free(bytes);
    var decoded = try message.Message.init(init.gpa, bytes, .{});
    defer decoded.deinit();
    const reader = try reflection.DynamicStruct.Reader.init(schema, &decoded);
    const text = (try reader.get("text")).@"struct";
    try strings("branded 🦀", (try text.get("value")).text);
    var dynamic = reflection.DynamicStruct.Builder{ .schema = schema, .builder = typed._builder };
    // Box(Text) and Box(Box(Record)) share a node ID, but have incompatible brands.
    try expectError(error.TypeMismatch, dynamic.set("nested", .{ .@"struct" = text }));
    const nested = (try (try (try reader.get("nested")).@"struct".get("value")).@"struct".get("value")).@"struct";
    try strings("constant é", (try nested.get("label")).text);
    const pair = (try reader.get("pair")).@"struct";
    try strings("outer", (try pair.get("first")).text);
    try std.testing.expectEqualSlices(u8, &.{ 0, 128, 255 }, (try pair.get("second")).data);
}

fn scalarRoundtrip(init: std.process.Init, registry: reflection.Registry) !void {
    const schema = try (try scalars.Scalars.capnpSchema.resolve(registry)).asStruct();
    var builder = message.MessageBuilder.init(init.gpa);
    defer builder.deinit();
    var dynamic = try reflection.DynamicStruct.Builder.init(schema, &builder);
    // Defaults use XOR encoding for every scalar width, including floating point.
    {
        const bytes = try builder.toBytes();
        defer init.gpa.free(bytes);
        var decoded = try message.Message.init(init.gpa, bytes, .{});
        defer decoded.deinit();
        const reader = try reflection.DynamicStruct.Reader.init(schema, &decoded);
        try expect((try reader.get("boolean")).bool);
        try equal(@as(i8, -12), (try reader.get("signed8")).int8);
        try equal(@as(i16, -1234), (try reader.get("signed16")).int16);
        try equal(@as(i32, -123456), (try reader.get("signed32")).int32);
        try equal(@as(i64, -123456789012345), (try reader.get("signed64")).int64);
        try equal(@as(u8, 240), (try reader.get("unsigned8")).uint8);
        try equal(@as(u16, 60000), (try reader.get("unsigned16")).uint16);
        try equal(@as(u32, 4000000000), (try reader.get("unsigned32")).uint32);
        try equal(@as(u64, 18000000000000000000), (try reader.get("unsigned64")).uint64);
        try equal(@as(f32, 1.25), (try reader.get("real32")).float32);
        try equal(@as(f64, -2.5), (try reader.get("real64")).float64);
    }
    try dynamic.set("nothing", .{ .void = {} });
    try dynamic.set("boolean", .{ .bool = false });
    try dynamic.set("signed8", .{ .int8 = std.math.minInt(i8) });
    try dynamic.set("signed16", .{ .int16 = std.math.minInt(i16) });
    try dynamic.set("signed32", .{ .int32 = std.math.minInt(i32) });
    try dynamic.set("signed64", .{ .int64 = std.math.minInt(i64) });
    try dynamic.set("unsigned8", .{ .uint8 = std.math.maxInt(u8) });
    try dynamic.set("unsigned16", .{ .uint16 = std.math.maxInt(u16) });
    try dynamic.set("unsigned32", .{ .uint32 = std.math.maxInt(u32) });
    try dynamic.set("unsigned64", .{ .uint64 = std.math.maxInt(u64) });
    try dynamic.set("real32", .{ .float32 = -3.5 });
    try dynamic.set("real64", .{ .float64 = 9.25 });
    var bits = try dynamic.initList("booleans", 9);
    try bits.set(0, .{ .bool = true });
    try bits.set(8, .{ .bool = true });
    try expectError(error.IndexOutOfBounds, bits.set(9, .{ .bool = true }));
    var signed = try dynamic.initList("signed", 2);
    try signed.set(0, .{ .int16 = std.math.minInt(i16) });
    try signed.set(1, .{ .int16 = std.math.maxInt(i16) });
    var reals = try dynamic.initList("reals", 1);
    try reals.set(0, .{ .float64 = 1.5 });
    var texts = try dynamic.initList("texts", 2);
    try texts.set(0, .{ .text = "first" });
    try texts.set(1, .{ .text = "second 🦀" });
    var blobs = try dynamic.initList("blobs", 1);
    try blobs.set(0, .{ .data = &.{ 0, 128, 255 } });
    var nested = try dynamic.initList("nested", 1);
    var inner = try nested.initList(0, 2);
    try inner.set(0, .{ .uint32 = 7 });
    try inner.set(1, .{ .uint32 = std.math.maxInt(u32) });
    var choices = try dynamic.initList("choices", 2);
    const enum_schema = try (try (try (try schema.field("choices")).type()).listElement()).asEnum();
    try choices.set(0, .{ .@"enum" = .{ .schema = enum_schema, .ordinal = 1 } });
    try choices.set(1, .{ .@"enum" = .{ .schema = enum_schema, .ordinal = 60000 } });
    const bytes = try builder.toBytes();
    defer init.gpa.free(bytes);
    var decoded = try message.Message.init(init.gpa, bytes, .{});
    defer decoded.deinit();
    const typed = try scalars.Scalars.Reader.init(&decoded);
    try equal(std.math.minInt(i64), try typed.getSigned64());
    try equal(std.math.maxInt(u64), try typed.getUnsigned64());
    try strings("second 🦀", try (try typed.getTexts()).get(1));
    const reader = try reflection.DynamicStruct.Reader.init(schema, &decoded);
    try expect(!(try reader.get("boolean")).bool);
    try equal(std.math.minInt(i8), (try reader.get("signed8")).int8);
    try equal(std.math.minInt(i16), (try reader.get("signed16")).int16);
    try equal(std.math.minInt(i32), (try reader.get("signed32")).int32);
    try equal(std.math.minInt(i64), (try reader.get("signed64")).int64);
    try equal(std.math.maxInt(u8), (try reader.get("unsigned8")).uint8);
    try equal(std.math.maxInt(u16), (try reader.get("unsigned16")).uint16);
    try equal(std.math.maxInt(u32), (try reader.get("unsigned32")).uint32);
    try equal(std.math.maxInt(u64), (try reader.get("unsigned64")).uint64);
    try equal(@as(f32, -3.5), (try reader.get("real32")).float32);
    try equal(@as(f64, 9.25), (try reader.get("real64")).float64);
    try expect((try (try reader.get("booleans")).list.get(8)).bool);
    try equal(std.math.minInt(i16), (try (try reader.get("signed")).list.get(0)).int16);
    try equal(@as(f64, 1.5), (try (try reader.get("reals")).list.get(0)).float64);
    try strings("second 🦀", (try (try reader.get("texts")).list.get(1)).text);
    try std.testing.expectEqualSlices(u8, &.{ 0, 128, 255 }, (try (try reader.get("blobs")).list.get(0)).data);
    try equal(@as(u16, 60000), (try (try reader.get("choices")).list.get(1)).@"enum".ordinal);
    try equal(std.math.maxInt(u32), (try (try (try reader.get("nested")).list.get(0)).list.get(1)).uint32);
    try expectError(error.IndexOutOfBounds, (try reader.get("booleans")).list.get(9));
    try write(init, "scalars.bin", bytes);
}

fn invalidDescriptors(allocator: std.mem.Allocator) !void {
    try expectError(error.TruncatedMessage, reflection.Registry.init(allocator, &.{}));
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(0, 4);
    const nodes = try root.writeStructList(0, 2, 5, 6);
    var first = try nodes.get(0);
    var second = try nodes.get(1);
    first.writeU64(0, 0x9123456789abcdef);
    second.writeU64(0, 0x9123456789abcdef);
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    try expectError(error.DuplicateSchemaId, reflection.Registry.init(allocator, bytes));
}

fn constrainedPointers(allocator: std.mem.Allocator, registry: reflection.Registry) !void {
    const schema = try (try brands.Pointers.capnpSchema.resolve(registry)).asStruct();
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var dynamic = try reflection.DynamicStruct.Builder.init(schema, &builder);
    const slot = (try schema.field("structure")).proto().slot.?.offset;
    // A valid Text pointer is nevertheless invalid for AnyStruct.
    try dynamic.builder.writeText(slot, "wrong pointer kind");
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var decoded = try message.Message.init(allocator, bytes, .{});
    defer decoded.deinit();
    const reader = try reflection.DynamicStruct.Reader.init(schema, &decoded);
    try expectError(error.TypeMismatch, reader.get("structure"));
    const pointer = try reader.reader.readAnyPointer(slot);
    try expectError(error.TypeMismatch, dynamic.set("structure", .{ .any_pointer = pointer }));
}

fn capabilities(allocator: std.mem.Allocator, registry: reflection.Registry) !void {
    const schema = try (try scalars.Scalars.capnpSchema.resolve(registry)).asStruct();
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var dynamic = try reflection.DynamicStruct.Builder.init(schema, &builder);
    const cases = [_]?message.Capability{ null, .{ .id = 0 }, null };
    for (cases) |capability| {
        try dynamic.set("service", .{ .capability = capability });
        const bytes = try builder.toBytes();
        defer allocator.free(bytes);
        var decoded = try message.Message.init(allocator, bytes, .{});
        defer decoded.deinit();
        const reader = try reflection.DynamicStruct.Reader.init(schema, &decoded);
        const actual = (try reader.get("service")).capability;
        try equal(capability != null, try reader.has("service"));
        if (capability) |expected| {
            try equal(expected.id, actual.?.id);
        } else {
            try expect(actual == null);
        }
    }
}

pub fn main(init: std.process.Init) !void {
    const destination = try output.configure(init);
    defer if (destination) |path| init.gpa.free(path);
    const registry = try values.Values.capnpSchema.load(init.gpa);
    defer registry.deinit();
    try checkMetadata(registry);
    try valuesRoundtrip(init, registry);
    try brandsRoundtrip(init, registry);
    try scalarRoundtrip(init, registry);
    try invalidDescriptors(init.gpa);
    try constrainedPointers(init.gpa, registry);
    try capabilities(init.gpa, registry);
    try @import("list_evolution_test.zig").run(init, registry);
    try @import("list_failure_test.zig").run(init, registry);
    try @import("generic_list_test.zig").run(init, registry);
    try write(init, "schema.bin", registry.encodedRequest());
}
