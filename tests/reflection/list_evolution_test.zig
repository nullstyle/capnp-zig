const std = @import("std");
const output = @import("output.zig");
const capnpc = @import("capnpc-zig");
const reflection = capnpc.reflection;
const message = capnpc.message;
const generated = @import("generated").scalars;
const equal = std.testing.expectEqual;
const strings = std.testing.expectEqualStrings;
const expectError = std.testing.expectError;

const Encoding = enum { composite, byte, u16, u32, u64, pointer, void };
const Operation = enum { get, init, copy };
const Case = struct {
    name: []const u8,
    encoding: Encoding = .composite,
    data_words: u16 = 1,
    pointer_words: u16 = 1,
    count: u32 = 3,
    operation: Operation = .get,
    far: bool = false,
};
const cases = [_]Case{
    .{ .name = "inline-small" },
    .{ .name = "far-inline", .far = true },
    .{ .name = "inline-init", .operation = .init },
    .{ .name = "inline-extra-data", .data_words = 3 },
    .{ .name = "inline-extra-pointers", .pointer_words = 3 },
    .{ .name = "inline-zero-width", .data_words = 0, .pointer_words = 0 },
    .{ .name = "byte", .encoding = .byte },
    .{ .name = "u16", .encoding = .u16 },
    .{ .name = "u32", .encoding = .u32 },
    .{ .name = "u64", .encoding = .u64 },
    .{ .name = "pointer", .encoding = .pointer },
    .{ .name = "void", .encoding = .void },
    .{ .name = "empty-inline", .count = 0 },
    .{ .name = "empty-byte", .encoding = .byte, .count = 0 },
    .{ .name = "copy-large", .operation = .copy },
};
const labels = [_][]const u8{ "first", "middle", "last" };
const unknown_labels = [_][]const u8{ "unknown first", "unknown middle", "unknown last" };

fn originalValue(case: Case, index: u32) u64 {
    return switch (case.encoding) {
        .composite => if (case.data_words > 0) (@as(u64, index) + 1) * 10 else 0,
        .byte => ([_]u8{ 0x12, 0x80, 0xff })[index],
        .u16 => ([_]u16{ 0x1234, 0x8000, 0xffff })[index],
        .u32 => ([_]u32{ 0x12345678, 0x80000000, 0xffffffff })[index],
        .u64 => ([_]u64{ 0x123456789abcdef0, 0x8000000000000000, 0xffffffffffffffff })[index],
        .pointer, .void => 0,
    };
}

fn originalLabel(case: Case, index: u32) []const u8 {
    return if (case.encoding == .pointer or (case.encoding == .composite and case.pointer_words > 0)) labels[index] else "";
}

fn seed(root: reflection.DynamicStruct.Builder, case: Case) !void {
    const pointer = try root.builder.getAnyPointer(0);
    switch (case.encoding) {
        .composite => {
            const list = if (case.far) blk: {
                const landing = try pointer.builder.createSegment();
                const content = try pointer.builder.createSegment();
                break :blk try root.builder.writeStructListInSegments(0, case.count, case.data_words, case.pointer_words, landing, content);
            } else try pointer.initStructList(case.count, case.data_words, case.pointer_words);
            for (0..case.count) |i| {
                const index: u32 = @intCast(i);
                var child = try list.get(index);
                if (case.data_words > 0) child.writeU64(0, originalValue(case, index));
                if (case.data_words > 1) child.writeU64(8, 100 + index);
                if (case.data_words > 2) child.writeU64(16, 0xfeed0000 + index);
                if (case.pointer_words > 0) try child.writeText(0, labels[index]);
                if (case.pointer_words > 1) try child.writeText(1, "prior note");
                if (case.pointer_words > 2) try child.writeText(2, unknown_labels[index]);
            }
        },
        .byte => {
            const list = try pointer.initU8List(case.count);
            for (0..case.count) |i| try list.set(@intCast(i), @intCast(originalValue(case, @intCast(i))));
        },
        .u16 => {
            const list = try pointer.initU16List(case.count);
            for (0..case.count) |i| try list.set(@intCast(i), @intCast(originalValue(case, @intCast(i))));
        },
        .u32 => {
            const list = try pointer.initU32List(case.count);
            for (0..case.count) |i| try list.set(@intCast(i), @intCast(originalValue(case, @intCast(i))));
        },
        .u64 => {
            const list = try pointer.initU64List(case.count);
            for (0..case.count) |i| try list.set(@intCast(i), originalValue(case, @intCast(i)));
        },
        .pointer => {
            const list = try pointer.initPointerList(case.count);
            for (0..case.count) |i| try list.setText(@intCast(i), labels[i]);
        },
        .void => _ = try pointer.initVoidList(case.count),
    }
}

fn write(init: std.process.Init, name: []const u8, bytes: []const u8) !void {
    const filename = try std.fmt.allocPrint(init.gpa, "evolution-{s}.bin", .{name});
    defer init.gpa.free(filename);
    try output.write(init, filename, bytes);
}

fn check(case: Case, schema: reflection.StructSchema, decoded: *message.Message) !void {
    const root = try reflection.DynamicStruct.Reader.init(schema, decoded);
    try strings("root survives", (try root.get("marker")).text);
    if (case.encoding == .byte and case.count > 0) {
        const single = (try root.get("single")).@"struct";
        try equal(@as(u64, 0x12), (try single.get("value")).uint64);
        try std.testing.expect(single.reader.data_size >= 1);
    }
    const list = (try root.get("records")).list;
    try equal(case.count, try list.len());
    if (case.count == 0) return;
    const raw = try root.reader.readStructList(0);
    for (0..case.count) |i| {
        const index: u32 = @intCast(i);
        const child = (try list.get(index)).@"struct";
        const copied = index == 1 and case.operation == .copy;
        const initialized = index == 1 and case.operation == .init;
        const value: u64 = if (copied) 91 else if (initialized) 77 else originalValue(case, index);
        const extra: u64 = if (copied) 92 else if (initialized) 88 else if (index == 1) 222 else if (case.encoding == .composite and case.data_words > 1) 100 + index else 0;
        const label = if (copied) "copied" else if (initialized) "reset" else originalLabel(case, index);
        const note = if (copied) "copied note" else if (initialized) "initialized" else if (index == 1) "grown" else if (case.encoding == .composite and case.pointer_words > 1) "prior note" else "";
        try equal(value, (try child.get("value")).uint64);
        try equal(extra, (try child.get("extra")).uint64);
        try strings(label, (try child.get("label")).text);
        try strings(note, (try child.get("note")).text);
        const raw_child = try raw.get(index);
        const unknown_data: u64 = if (copied) 0xa55a else if (initialized) 0 else if (case.encoding == .composite and case.data_words > 2) 0xfeed0000 + index else 0;
        try equal(unknown_data, raw_child.readU64(16));
        const unknown_pointer = if (copied) "copied unknown" else if (initialized) "" else if (case.encoding == .composite and case.pointer_words > 2) unknown_labels[index] else "";
        try strings(unknown_pointer, try raw_child.readTextStrict(2));
    }
}

fn runCase(init: std.process.Init, schema: reflection.StructSchema, case: Case) !void {
    var builder = message.MessageBuilder.init(init.gpa);
    defer builder.deinit();
    var root = try reflection.DynamicStruct.Builder.init(schema, &builder);
    try root.set("marker", .{ .text = "root survives" });
    try seed(root, case);
    const list = try root.getList("records");
    const before = try builder.toBytes();
    defer init.gpa.free(before);
    try equal(case.count, try list.len());
    try expectError(error.IndexOutOfBounds, list.getStruct(case.count));
    try expectError(error.IndexOutOfBounds, list.initStruct(case.count));
    try expectError(error.IndexOutOfBounds, list.set(case.count, .{ .uint64 = 0 }));
    const unchanged = try builder.toBytes();
    defer init.gpa.free(unchanged);
    try std.testing.expectEqualSlices(u8, before, unchanged);
    if (case.encoding == .byte and case.count > 0) {
        // The first byte is a valid old representation of a struct's first
        // field. Copying that virtual struct must round storage up to a word.
        var source = try message.Message.init(init.gpa, before, .{});
        defer source.deinit();
        const source_root = try reflection.DynamicStruct.Reader.init(schema, &source);
        const first = try (try source_root.get("records")).list.get(0);
        try root.set("single", first);
    }
    if (case.count > 0) {
        switch (case.operation) {
            .get => {
                var child = try list.getStruct(1);
                try child.set("extra", .{ .uint64 = 222 });
                try child.set("note", .{ .text = "grown" });
            },
            .init => {
                var child = try list.initStruct(1);
                try child.set("value", .{ .uint64 = 77 });
                try child.set("extra", .{ .uint64 = 88 });
                try child.set("label", .{ .text = "reset" });
                try child.set("note", .{ .text = "initialized" });
            },
            .copy => {
                const element_schema = try (try (try (try schema.field("records")).type()).listElement()).asStruct();
                var source = message.MessageBuilder.init(init.gpa);
                defer source.deinit();
                var source_root = try source.allocateStruct(3, 3);
                source_root.writeU64(0, 91);
                source_root.writeU64(8, 92);
                source_root.writeU64(16, 0xa55a);
                try source_root.writeText(0, "copied");
                try source_root.writeText(1, "copied note");
                try source_root.writeText(2, "copied unknown");
                const source_bytes = try source.toBytes();
                defer init.gpa.free(source_bytes);
                var decoded_source = try message.Message.init(init.gpa, source_bytes, .{});
                defer decoded_source.deinit();
                try list.set(1, .{ .@"struct" = try reflection.DynamicStruct.Reader.init(element_schema, &decoded_source) });
            },
        }
        // This existing list handle must follow its updated parent pointer.
        try equal(case.count, try list.len());
        _ = try list.getStruct(2);
    }
    const bytes = try builder.toBytes();
    defer init.gpa.free(bytes);
    var decoded = try message.Message.init(init.gpa, bytes, .{});
    defer decoded.deinit();
    try check(case, schema, &decoded);
    try write(init, case.name, bytes);
}

fn booleanRejection(init: std.process.Init, schema: reflection.StructSchema, count: u32) !void {
    var builder = message.MessageBuilder.init(init.gpa);
    defer builder.deinit();
    var root = try reflection.DynamicStruct.Builder.init(schema, &builder);
    try root.set("marker", .{ .text = "root survives" });
    const pointer = try root.builder.getAnyPointer(0);
    const bits = try pointer.initBoolList(count);
    if (count > 0) try bits.set(0, true);
    const word = std.mem.readInt(u64, builder.segments.items[pointer.segment_id].items[pointer.pointer_pos..][0..8], .little);
    try std.testing.expect(word != 0);
    try equal(@as(u3, 1), @as(u3, @truncate(word >> 32)));
    const before = try builder.toBytes();
    defer init.gpa.free(before);
    const list = try root.getList("records");
    try expectError(error.TypeMismatch, list.getStruct(0));
    try expectError(error.TypeMismatch, list.initStruct(0));
    const after = try builder.toBytes();
    defer init.gpa.free(after);
    try std.testing.expectEqualSlices(u8, before, after);
    try write(init, if (count == 0) "empty-boolean-rejected" else "boolean-rejected", after);
}

fn nestedList(init: std.process.Init, schema: reflection.StructSchema) !void {
    var builder = message.MessageBuilder.init(init.gpa);
    defer builder.deinit();
    var root = try reflection.DynamicStruct.Builder.init(schema, &builder);
    const outer = try root.builder.writePointerList(2, 1);
    const old = try outer.initU16List(0, 3);
    try old.set(0, 12);
    try old.set(1, 34);
    try old.set(2, 56);
    const list = try (try root.getList("nested")).getList(0);
    try equal(@as(u32, 3), try list.len());
    var middle = try list.getStruct(1);
    try middle.set("extra", .{ .uint64 = 222 });
    try middle.set("note", .{ .text = "nested growth" });
    const bytes = try builder.toBytes();
    defer init.gpa.free(bytes);
    var decoded = try message.Message.init(init.gpa, bytes, .{});
    defer decoded.deinit();
    const reader = try reflection.DynamicStruct.Reader.init(schema, &decoded);
    const result = (try (try reader.get("nested")).list.get(0)).list;
    for ([_]u64{ 12, 34, 56 }, 0..) |value, i| try equal(value, (try (try result.get(@intCast(i))).@"struct".get("value")).uint64);
    try strings("nested growth", (try (try result.get(1)).@"struct".get("note")).text);
    try write(init, "nested", bytes);
}

pub fn run(init: std.process.Init, registry: reflection.Registry) !void {
    const schema = try (try generated.Evolution.capnpSchema.resolve(registry)).asStruct();
    for (cases) |case| try runCase(init, schema, case);
    try booleanRejection(init, schema, 3);
    try booleanRejection(init, schema, 0);
    try nestedList(init, schema);
}
