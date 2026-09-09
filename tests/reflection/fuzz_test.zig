//! Structured reflection fuzzing keeps valid schemas and operations in reach.
const std = @import("std");
const capnp = @import("capnpc-zig");
const generated = @import("generated");
const message = capnp.message;
const reflection = capnp.reflection;
const descriptor = @embedFile("request.bin");
const double_far = @import("double_far_fixture.zig");

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

fn fuzzDoubleFarCopy(registry: reflection.Registry, smith: *std.testing.Smith) !void {
    const schema = try (try generated.brands.Pointers.capnpSchema.resolve(registry)).asStruct();
    const prefix = smith.valueRangeAtMost(u32, 0, 3);
    const values = [3]u64{ smith.value(u64), smith.value(u64), smith.value(u64) };
    // Both encodings are valid for every input; empty structs have a zero tag
    // in the landing pad but must retain non-null presence after copying.
    for ([_]bool{ false, true }) |empty| {
        const frame = double_far.make(.{ .prefix_words = prefix, .values = values, .empty = empty });
        var source = try message.Message.init(std.testing.allocator, frame.bytes[0..frame.len], .{});
        defer source.deinit();
        const pointer = try source.getRootAnyPointer();
        try std.testing.expect(!pointer.isNull());
        for ([_]bool{ false, true }) |dynamic| {
            var destination = message.MessageBuilder.init(std.testing.allocator);
            defer destination.deinit();
            if (dynamic) {
                const root = try reflection.DynamicStruct.Builder.init(schema, &destination);
                try root.set("any", .{ .any_pointer = pointer });
            } else {
                var root = try generated.brands.Pointers.Builder.init(&destination);
                try root.setAny(pointer);
            }
            var storage = capnp.generated_helpers.ReaderStorage.init(std.testing.allocator);
            defer storage.deinit();
            try storage.bind(&destination);
            const typed = try generated.brands.Pointers.Reader.init(&storage.message_view);
            try std.testing.expect(typed.hasAny());
            const copied = try (try typed.getAny()).getStruct();
            try std.testing.expectEqual(@as(u16, if (empty) 0 else 2), copied.data_size);
            try std.testing.expectEqual(@as(u16, if (empty) 0 else 2), copied.pointer_count);
            if (!empty) {
                try std.testing.expectEqual(values[0], copied.readU64(0));
                try std.testing.expectEqual(values[1], copied.readU64(8));
                try std.testing.expectEqualStrings("hello", try copied.readTextStrict(0));
                try std.testing.expectEqual(values[2], (try copied.readStruct(1)).readU64(0));
            }
        }
    }
}

test "fuzz: bounded reflection registry loading" {
    try std.testing.fuzz({}, fuzzRegistry, .{});
}

test "fuzz: dynamic mutation agrees with generated readers" {
    const registry = try reflection.Registry.init(std.testing.allocator, descriptor);
    defer registry.deinit();
    try std.testing.fuzz(registry, fuzzMutation, .{});
}

test "fuzz: double-far struct copies preserve content and presence" {
    const registry = try reflection.Registry.init(std.testing.allocator, descriptor);
    defer registry.deinit();
    try std.testing.fuzz(registry, fuzzDoubleFarCopy, .{});
}
