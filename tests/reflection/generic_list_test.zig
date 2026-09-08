const std = @import("std");
const capnpc = @import("capnpc-zig");
const reflection = capnpc.reflection;
const message = capnpc.message;
const schema = capnpc.schema;
const generated = @import("generated");
const equal = std.testing.expectEqual;
const strings = std.testing.expectEqualStrings;
const expectError = std.testing.expectError;

// The reference compiler currently rejects List(T) as List(AnyPointer), but the
// metadata API accepts this synthetic shape and schema_validation specifies its
// pointer-list encoding. Clone the real Box(T) descriptor to exercise that same
// contract without pretending this is compiler-produced schema coverage.
fn listRegistry(allocator: std.mem.Allocator, original: reflection.Registry, box_id: schema.Id, depth: u8) !reflection.Registry {
    var input = try message.Message.init(allocator, original.encodedRequest(), .{});
    defer input.deinit();
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.initRootAnyPointer();
    try message.cloneAnyPointer(try input.getRootAnyPointer(), root);
    const nodes = try (try (try root.getStruct()).getAnyPointer(0)).getStructList();
    var box_index: u32 = undefined;
    for (original.nodes(), 0..) |node, index| {
        if (node.id == box_id) {
            box_index = @intCast(index);
            break;
        }
    } else return error.MissingTestSchema;
    const fields = try (try (try nodes.get(box_index)).getAnyPointer(3)).getStructList();
    const field = try fields.get(0);
    const original_field = try (try (try original.get(box_id)).asStruct()).field("value");
    const parameter = try (try original_field.raw()).readAnyPointer(2);
    var type_pointer = try field.getAnyPointer(2);
    for (0..depth) |_| {
        const list_type = try type_pointer.initStruct(3, 1);
        list_type.writeU16(0, 14); // schema.Type.list
        type_pointer = try list_type.getAnyPointer(0);
    }
    try message.cloneAnyPointer(parameter, type_pointer);
    const default = try (try field.getAnyPointer(3)).initStruct(2, 1);
    default.writeU16(0, 14); // schema.Value.list, null default
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    return reflection.Registry.init(allocator, bytes);
}

fn checkPointerEncoding(reader: message.AnyPointerReader) !void {
    // These small messages use a direct list pointer, not a far landing pad.
    try equal(@as(u64, 1), reader.pointer_word & 3);
    try equal(@as(u64, 6), (reader.pointer_word >> 32) & 7);
}

fn roundtrip(allocator: std.mem.Allocator, registry: reflection.Registry, box: reflection.StructSchema, brand: schema.Brand, direct: reflection.StructSchema) !void {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try reflection.DynamicStruct.Builder.init(box, &builder);
    const list = try root.initList("value", 3);
    try equal(@as(u32, 3), try list.len());
    const first = try list.getStruct(0); // Allocate a null element.
    try first.set("label", .{ .text = "first" });
    _ = try list.initStruct(1);
    // Widen a pointer-list element without changing any sibling's dimensions.
    const raw_list = try list.pointer.getPointerList();
    const old = try raw_list.initStruct(2, 0, 1);
    try old.writeText(0, "preserved while growing");
    const grown = try list.getStruct(2);
    try equal(@as(u16, 1), grown.builder.data_size);
    const mode = try (try (try grown.schema.field("mode")).type()).asEnum();
    try grown.set("mode", .{ .@"enum" = .{ .schema = mode, .ordinal = 0 } });
    // A replacement may carry additional, unknown storage. Keep all of it.
    var source_builder = message.MessageBuilder.init(allocator);
    defer source_builder.deinit();
    const source_root = try source_builder.allocateStruct(2, 2);
    source_root.writeU64(8, 0xfeed);
    try source_root.writeText(0, "replacement");
    try source_root.writeText(1, "unknown pointer");
    const source_bytes = try source_builder.toBytes();
    defer allocator.free(source_bytes);
    var source_message = try message.Message.init(allocator, source_bytes, .{});
    defer source_message.deinit();
    const source = try reflection.DynamicStruct.Reader.init(grown.schema, &source_message);
    try list.set(1, .{ .@"struct" = source });
    const reset = try list.initStruct(0);
    try reset.set("label", .{ .text = "reset first" });
    try expectError(error.IndexOutOfBounds, list.getStruct(3));
    try expectError(error.IndexOutOfBounds, list.initStruct(3));
    try expectError(error.TypeMismatch, list.set(0, .{ .text = "wrong type" }));
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var decoded = try message.Message.init(allocator, bytes, .{});
    defer decoded.deinit();
    try capnpc.schema_validation.validateMessageWithBrand(&decoded, @constCast(registry.nodes()), box.schema.node, brand, .{});
    const raw = try (try decoded.getRootStruct()).readAnyPointer(0);
    try checkPointerEncoding(raw);
    const reader = try reflection.DynamicStruct.Reader.init(box, &decoded);
    const items = (try reader.get("value")).list;
    try equal(@as(u32, 3), try items.len());
    try strings("reset first", (try (try items.get(0)).@"struct".get("label")).text);
    const second = (try items.get(1)).@"struct";
    try strings("replacement", (try second.get("label")).text);
    try equal(@as(u64, 0xfeed), second.reader.readU64(8));
    try strings("unknown pointer", try second.reader.readTextStrict(1));
    const third = (try items.get(2)).@"struct";
    try strings("preserved while growing", (try third.get("label")).text);
    try equal(@as(u16, 0), (try third.get("mode")).@"enum".ordinal);
    // A direct List(Record) has the same resolved element type but a different
    // wire layout. Raw copying between the two must be rejected.
    var direct_builder = message.MessageBuilder.init(allocator);
    defer direct_builder.deinit();
    const direct_root = try reflection.DynamicStruct.Builder.init(direct, &direct_builder);
    try expectError(error.TypeMismatch, direct_root.set("records", .{ .list = items }));
    _ = try direct_root.initList("records", 1);
    const direct_bytes = try direct_builder.toBytes();
    defer allocator.free(direct_bytes);
    var direct_message = try message.Message.init(allocator, direct_bytes, .{});
    defer direct_message.deinit();
    const direct_reader = try reflection.DynamicStruct.Reader.init(direct, &direct_message);
    try expectError(error.TypeMismatch, root.set("value", try direct_reader.get("records")));
}

fn nested(allocator: std.mem.Allocator, registry: reflection.Registry, box: reflection.StructSchema, brand: schema.Brand) !void {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try reflection.DynamicStruct.Builder.init(box, &builder);
    const outer = try root.initList("value", 2);
    const inner = try outer.initList(0, 1);
    try (try inner.initStruct(0)).set("label", .{ .text = "nested" });
    try equal(@as(u32, 1), try (try outer.getList(0)).len());
    try equal(@as(u32, 0), try (try outer.getList(1)).len());
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var decoded = try message.Message.init(allocator, bytes, .{});
    defer decoded.deinit();
    try capnpc.schema_validation.validateMessageWithBrand(&decoded, @constCast(registry.nodes()), box.schema.node, brand, .{});
    const reader = try reflection.DynamicStruct.Reader.init(box, &decoded);
    const lists = (try reader.get("value")).list;
    try checkPointerEncoding(lists.reader.raw());
    const items = (try lists.get(0)).list;
    try checkPointerEncoding(items.reader.raw());
    try strings("nested", (try (try items.get(0)).@"struct".get("label")).text);
    try equal(@as(u32, 0), try (try lists.get(1)).list.len());
}

pub fn run(init: std.process.Init, original: reflection.Registry) !void {
    const box_id = generated.brands.Box.capnpSchema.id;
    const direct = try (try generated.values.Values.capnpSchema.resolve(original)).asStruct();
    const record = try (try (try direct.field("record")).type()).asStruct();
    var expression: schema.TypeExpression = .{ .type = .{ .@"struct" = .{ .type_id = record.schema.id() } }, .metadata = .{ .named = .{} } };
    var bindings = [_]schema.Brand.Binding{.{ .type = &expression }};
    var scopes = [_]schema.Brand.Scope{.{ .scope_id = box_id, .binding = .{ .bind = &bindings } }};
    const brand: schema.Brand = .{ .scopes = &scopes };
    const single = try listRegistry(init.gpa, original, box_id, 1);
    defer single.deinit();
    try roundtrip(init.gpa, single, try (try single.get(box_id)).asStructWithBrand(brand), brand, direct);
    const double = try listRegistry(init.gpa, original, box_id, 2);
    defer double.deinit();
    try nested(init.gpa, double, try (try double.get(box_id)).asStructWithBrand(brand), brand);
}
