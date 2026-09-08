const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;
const reflection = capnpc.reflection;

const node_id: u64 = 0xb74f6a4081665fad;
const node_name = "registry-test.capnp:Record";

const Descriptor = struct {
    data_words: u16 = 1,
    pointer_words: u16 = 0,
    type_discriminant: u16 = 9, // schema::Type.uint64
    slot_offset: u32 = 0,
    discriminant_count: u16 = 0,
    discriminant_offset: u32 = 0,
    field_discriminant: u16 = 0xffff,
    default_uint64: ?u64 = null,
};

/// Construct valid wire data describing one struct and one slot. The layout
/// values are intentionally configurable so malformed schemas pass wire
/// validation and exercise Registry's semantic validation during loading.
fn makeRequest(allocator: std.mem.Allocator, options: Descriptor) ![]u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root_pointer = try builder.initRootAnyPointer();
    var root = try root_pointer.initStruct(0, 1);
    var nodes = try root.writeStructList(0, 1, 5, 6);
    var node = try nodes.get(0);
    node.writeU64(0, node_id);
    node.writeU32(8, "registry-test.capnp:".len);
    node.writeU16(12, 1); // schema::Node.struct
    node.writeU16(14, options.data_words);
    node.writeU16(24, options.pointer_words);
    node.writeU16(26, 7); // inlineComposite
    node.writeU16(30, options.discriminant_count);
    node.writeU32(32, options.discriminant_offset);
    try node.writeText(0, node_name);
    var fields = try node.writeStructList(3, 1, 3, 4);
    var field = try fields.get(0);
    field.writeU16(2, options.field_discriminant ^ @as(u16, 0xffff));
    field.writeU32(4, options.slot_offset);
    try field.writeText(0, "value");
    var typ = try field.initStruct(2, 1, 0);
    typ.writeU16(0, options.type_discriminant);
    if (options.default_uint64) |default| {
        var value = try field.initStruct(3, 2, 1);
        value.writeU16(0, 9); // schema::Value.uint64
        value.writeU64(8, default);
        field.writeBool(16, 0, true);
    }
    return @constCast(try builder.toBytes());
}

fn expectInvalid(options: Descriptor) !void {
    const allocator = std.testing.allocator;
    const bytes = try makeRequest(allocator, options);
    defer allocator.free(bytes);
    // Establish that failures below are schema errors, not malformed wire data.
    var valid_wire = try message.Message.init(allocator, bytes, .{});
    defer valid_wire.deinit();
    try std.testing.expectError(error.InvalidSchema, reflection.Registry.init(allocator, bytes));
}

test "registry rejects scalar offsets before wasm32 arithmetic can overflow" {
    for ([_]u16{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 }) |kind| {
        try expectInvalid(.{ .type_discriminant = kind, .slot_offset = std.math.maxInt(u32) });
    }
    try expectInvalid(.{ .slot_offset = 1 }); // uint64 just beyond one data word
}

test "registry rejects pointer indexes outside the declared pointer section" {
    try expectInvalid(.{ .type_discriminant = 12, .pointer_words = 1, .slot_offset = 1 });
    try expectInvalid(.{ .type_discriminant = 12, .pointer_words = 1, .slot_offset = std.math.maxInt(u32) });
}

test "registry rejects union discriminant storage outside the data section" {
    try expectInvalid(.{ .discriminant_count = 2, .discriminant_offset = 4 });
    try expectInvalid(.{ .discriminant_count = 2, .discriminant_offset = std.math.maxInt(u32) });
    try expectInvalid(.{ .data_words = 0, .type_discriminant = 0, .discriminant_count = 2 });
}

test "registry rejects field discriminants outside the declared union" {
    try expectInvalid(.{ .discriminant_count = 2, .field_discriminant = 2 });
    try expectInvalid(.{ .discriminant_count = 0, .field_discriminant = 0 });
}

test "registry owns parsed and raw descriptors after input mutation and release" {
    const allocator = std.testing.allocator;
    const registry = blk: {
        const input = try makeRequest(allocator, .{ .default_uint64 = 99 });
        defer allocator.free(input);
        const result = try reflection.Registry.init(allocator, input);
        @memset(input, 0xdd);
        break :blk result;
    };
    defer registry.deinit();
    const node = try registry.get(node_id);
    try std.testing.expectEqual(node_id, node.proto().id);
    try std.testing.expectEqualStrings(node_name, node.displayName());
    const raw = try node.raw();
    try std.testing.expectEqual(node_id, raw.readU64(0));
    try std.testing.expectEqualStrings(node_name, try raw.readTextStrict(0));
    const field = try (try node.asStruct()).field("value");
    try std.testing.expectEqual(@as(u64, 99), field.proto().slot.?.default_value.?.uint64);
    try std.testing.expect(try field.hadExplicitDefault());
    try std.testing.expectEqualStrings("value", try (try field.raw()).readTextStrict(0));
}

test "registry permits valid schema defaults when reading an evolved smaller message" {
    const allocator = std.testing.allocator;
    const descriptor = try makeRequest(allocator, .{ .data_words = 2, .slot_offset = 1, .default_uint64 = 99 });
    defer allocator.free(descriptor);
    const registry = try reflection.Registry.init(allocator, descriptor);
    defer registry.deinit();
    const struct_schema = try (try registry.get(node_id)).asStruct();
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    _ = try builder.allocateStruct(0, 0);
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var decoded = try message.Message.init(allocator, bytes, .{});
    defer decoded.deinit();
    const reader = try reflection.DynamicStruct.Reader.init(struct_schema, &decoded);
    try std.testing.expectEqual(@as(u64, 99), (try reader.get("value")).uint64);
}
