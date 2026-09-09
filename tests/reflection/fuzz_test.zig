//! Structured reflection fuzzing keeps valid schemas and operations in reach.
const std = @import("std");
const capnp = @import("capnpc-zig");
const generated = @import("generated");
const message = capnp.message;
const reflection = capnp.reflection;
const descriptor = @embedFile("request.bin");

fn fuzzRegistry(_: void, smith: *std.testing.Smith) !void {
    var bytes: [descriptor.len]u8 = descriptor.*;
    const edits = smith.valueRangeAtMost(u32, 1, 8);
    for (0..edits) |_| {
        const index = smith.valueRangeAtMost(u32, 0, bytes.len - 1);
        bytes[index] = smith.value(u8);
    }
    const registry = reflection.Registry.initWithOptions(std.testing.allocator, &bytes, .{
        .max_input_bytes = descriptor.len,
        .max_memory_bytes = 2 * 1024 * 1024,
        .max_nodes = 128,
        .validation = .{ .traversal_limit_words = 32768, .inline_composite_element_limit = 4096, .nesting_limit = 32 },
    }) catch return;
    defer registry.deinit();
    for (registry.nodes()) |node| {
        const resolved = try registry.get(node.id);
        try std.testing.expectEqual(node.id, (try resolved.raw()).readU64(0));
    }
}

fn fuzzMutation(registry: reflection.Registry, smith: *std.testing.Smith) !void {
    const schema = try (try generated.scalars.Evolution.capnpSchema.resolve(registry)).asStruct();
    var builder = message.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    const physical = try builder.allocateStruct(schema.proto().data_word_count + 1, schema.proto().pointer_count + 1);
    const marker = smith.value(u64);
    physical.writeU64(@as(usize, schema.proto().data_word_count) * 8, marker);
    try physical.writeText(schema.proto().pointer_count, "unknown newer field");
    const root = reflection.DynamicStruct.Builder{ .schema = schema, .builder = physical };
    const child = try root.initStruct("single");
    var expected: u64 = 0;
    const length = smith.valueRangeAtMost(u32, 1, 12);
    for (0..length) |_| {
        switch (smith.valueRangeAtMost(u8, 0, 2)) {
            0 => {
                expected = smith.value(u64);
                try child.set("value", .{ .uint64 = expected });
            },
            1 => {
                expected = 0;
                try child.clear("value");
            },
            2 => try child.set("label", .{ .text = "copied after mutation" }),
            else => unreachable,
        }
    }
    var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try storage.bind(&builder);
    const before = reflection.DynamicStruct.Reader{ .schema = schema, .reader = try storage.reader(physical) };
    try root.set("single", try before.get("single")); // borrowed self-copy
    const bytes = try builder.toBytes();
    defer std.testing.allocator.free(bytes);
    var decoded = try message.Message.init(std.testing.allocator, bytes, .{});
    defer decoded.deinit();
    const typed = try generated.scalars.Evolution.Reader.init(&decoded);
    try std.testing.expectEqual(expected, try (try typed.getSingle()).getValue());
    const dynamic = try reflection.DynamicStruct.Reader.init(schema, &decoded);
    try std.testing.expectEqual(expected, (try (try dynamic.get("single")).@"struct".get("value")).uint64);
    const unknown = try decoded.getRootStruct();
    try std.testing.expectEqual(marker, unknown.readU64(@as(usize, schema.proto().data_word_count) * 8));
    try std.testing.expectEqualStrings("unknown newer field", try unknown.readTextStrict(schema.proto().pointer_count));
}

test "fuzz: bounded reflection registry loading" {
    try std.testing.fuzz({}, fuzzRegistry, .{});
}

test "fuzz: dynamic mutation agrees with generated readers" {
    const registry = try reflection.Registry.init(std.testing.allocator, descriptor);
    defer registry.deinit();
    try std.testing.fuzz(registry, fuzzMutation, .{});
}
