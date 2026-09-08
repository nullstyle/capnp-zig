//! Lossless schema metadata for generated modules. Copy the compiler's raw
//! Nodes rather than rebuilding them from the generator's reduced schema model.
const std = @import("std");
const message = @import("../serialization/message.zig");
const canonical = @import("../serialization/canonical.zig");

const NodeEntry = struct {
    id: u64,
    index: u32,

    fn lessThan(_: void, a: NodeEntry, b: NodeEntry) bool {
        return a.id < b.id;
    }
};

/// Build a standard CodeGeneratorRequest containing only its Nodes, sorted by
/// ID. Preserve every data word and pointer, including fields this generator
/// does not understand. Requested filenames, source comments, and compiler
/// version are not part of the runtime type graph.
pub fn encodeRequest(allocator: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    var source = try message.Message.init(allocator, bytes, .{});
    defer source.deinit();
    const source_root = try source.getRootStruct();
    const source_nodes = try source_root.readStructList(0);
    const order = try allocator.alloc(NodeEntry, source_nodes.len());
    defer allocator.free(order);
    for (order, 0..) |*entry, index| {
        const node = try source_nodes.get(@intCast(index));
        entry.* = .{ .id = node.readU64(0), .index = @intCast(index) };
    }
    std.mem.sort(NodeEntry, order, {}, NodeEntry.lessThan);
    for (order, 0..) |entry, index| {
        if (index > 0 and order[index - 1].id == entry.id) return error.DuplicateSchemaNode;
    }

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root_pointer = try builder.initRootAnyPointer();
    var root = try root_pointer.initStruct(0, 1);
    var nodes = try root.writeStructList(0, source_nodes.len(), source_nodes.data_words, source_nodes.pointer_words);
    for (order, 0..) |entry, index| {
        const src = try source_nodes.get(entry.index);
        var dst = try nodes.get(@intCast(index));
        const data = src.getDataSection();
        @memcpy(builder.segments.items[dst.segment_id].items[dst.offset..][0..data.len], data);
        for (0..src.pointer_count) |pointer_index| {
            try message.cloneAnyPointer(try src.readAnyPointer(pointer_index), try dst.getAnyPointer(@intCast(pointer_index)));
        }
    }
    return canonical.canonicalizeFromBuilder(allocator, &builder);
}

pub fn writeSchemaRef(writer: anytype, id: u64, indent: []const u8) !void {
    try writer.print("{s}pub const capnpSchema = capnpc.reflection.SchemaRef{{ .id = 0x{x}, .encoded_request = _capnp_file.CAPNP_SCHEMA_REQUEST }};\n", .{ indent, id });
}

test "reflection metadata sorts Nodes and preserves unknown data and pointers" {
    const allocator = std.testing.allocator;
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root_pointer = try builder.initRootAnyPointer();
    var root = try root_pointer.initStruct(0, 4);
    var nodes = try root.writeStructList(0, 2, 9, 8);
    var high = try nodes.get(0);
    high.writeU64(0, 22);
    high.writeU64(64, 0xabcdef);
    try high.writeText(7, "unknown pointer field");
    var low = try nodes.get(1);
    low.writeU64(0, 11);
    try low.writeText(0, "low");
    try root.writeText(1, "host-dependent requested files");
    try root.writeText(2, "compiler version");
    try root.writeText(3, "source comments");
    const source = try builder.toBytes();
    defer allocator.free(source);
    const encoded = try encodeRequest(allocator, source);
    defer allocator.free(encoded);
    var result = try message.Message.init(allocator, encoded, .{});
    defer result.deinit();
    const result_root = try result.getRootStruct();
    try std.testing.expectEqual(@as(u16, 1), result_root.pointer_count);
    const result_nodes = try result_root.readStructList(0);
    try std.testing.expectEqual(@as(u32, 2), result_nodes.len());
    try std.testing.expectEqual(@as(u64, 11), (try result_nodes.get(0)).readU64(0));
    const second = try result_nodes.get(1);
    try std.testing.expectEqual(@as(u64, 22), second.readU64(0));
    try std.testing.expectEqual(@as(u64, 0xabcdef), second.readU64(64));
    try std.testing.expectEqualStrings("unknown pointer field", try second.readText(7));
    const encoded_again = try encodeRequest(allocator, encoded);
    defer allocator.free(encoded_again);
    try std.testing.expectEqualSlices(u8, encoded, encoded_again);
}

test "reflection metadata rejects duplicate Node IDs" {
    const allocator = std.testing.allocator;
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root_pointer = try builder.initRootAnyPointer();
    var root = try root_pointer.initStruct(0, 1);
    _ = try root.writeStructList(0, 2, 1, 0);
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    try std.testing.expectError(error.DuplicateSchemaNode, encodeRequest(allocator, bytes));
}
