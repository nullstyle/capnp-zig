const std = @import("std");
const capnpc = @import("capnpc-zig");
const reflection = capnpc.reflection;
const message = capnpc.message;
const generated = @import("generated").scalars;
const equal = std.testing.expectEqual;
const strings = std.testing.expectEqualStrings;
const expectError = std.testing.expectError;

// A single-far pointer to a segment that these small messages cannot contain.
const invalid_far: u64 = (@as(u64, 0xfffffffe) << 32) | 2;
const labels = [_][]const u8{ "first intact", "second intact", "third intact" };

fn pointerWord(pointer: message.AnyPointerBuilder) u64 {
    const segment = pointer.builder.segments.items[pointer.segment_id].items;
    return std.mem.readInt(u64, segment[pointer.pointer_pos..][0..8], .little);
}

fn corrupt(pointer: message.AnyPointerBuilder) void {
    const segment = pointer.builder.segments.items[pointer.segment_id].items;
    std.mem.writeInt(u64, segment[pointer.pointer_pos..][0..8], invalid_far, .little);
}

fn allocatedBytes(builder: *const message.MessageBuilder) usize {
    var count: usize = 0;
    for (builder.segments.items) |segment| count += segment.items.len;
    return count;
}

fn seed(root: reflection.DynamicStruct.Builder, data_words: u16, pointer_words: u16) !message.StructListBuilder {
    try root.set("marker", .{ .text = "parent intact" });
    const list = try (try root.builder.getAnyPointer(0)).initStructList(3, data_words, pointer_words);
    for (0..3) |index| {
        const entry = try list.get(@intCast(index));
        entry.writeU64(0, 10 + index);
        if (data_words > 1) entry.writeU64(8, 20 + index);
        try entry.writeText(0, labels[index]);
        if (pointer_words > 1) try entry.writeText(1, "note intact");
    }
    return list;
}

fn checkOriginal(
    allocator: std.mem.Allocator,
    builder: *message.MessageBuilder,
    data_words: u16,
    pointer_words: u16,
    corrupt_last: bool,
) !void {
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    // The first case intentionally starts with one malformed reachable pointer.
    // Check the rest of that original graph without traversing the bad target.
    var decoded = try message.Message.initUnvalidated(allocator, bytes);
    defer decoded.deinit();
    if (!corrupt_last) try decoded.validate(.{});
    const root = try decoded.getRootStruct();
    try strings("parent intact", try root.readTextStrict(1));
    const list = try root.readStructList(0);
    try equal(@as(u32, 3), list.len());
    try equal(data_words, list.data_words);
    try equal(pointer_words, list.pointer_words);
    for (0..3) |index| {
        const entry = try list.get(@intCast(index));
        try equal(@as(u64, 10 + index), entry.readU64(0));
        if (data_words > 1) try equal(@as(u64, 20 + index), entry.readU64(8));
        if (corrupt_last and index == 2) {
            try equal(invalid_far, (try entry.readAnyPointer(0)).pointer_word);
        } else {
            try strings(labels[index], try entry.readTextStrict(0));
        }
        if (pointer_words > 1) try strings("note intact", try entry.readTextStrict(1));
    }
}

fn copyingOriginalFails(allocator: std.mem.Allocator, schema: reflection.StructSchema) !void {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &builder);
    const old_list = try seed(root, 1, 1);
    corrupt(try (try old_list.get(2)).getAnyPointer(0));
    const parent = try root.builder.getAnyPointer(0);
    const original_pointer = pointerWord(parent);
    const bytes_before = allocatedBytes(&builder);
    const list = try root.getList("records");

    // Entry needs two data and two pointer words. Widening copies the first
    // two healthy siblings, then encounters the malformed last sibling.
    try expectError(error.InvalidSegmentId, list.getStruct(0));
    try std.testing.expect(allocatedBytes(&builder) > bytes_before);
    try equal(original_pointer, pointerWord(parent));
    try checkOriginal(allocator, &builder, 1, 1, true);
}

fn copyingReplacementFails(allocator: std.mem.Allocator, schema: reflection.StructSchema) !void {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try reflection.DynamicStruct.Builder.init(schema, &builder);
    _ = try seed(root, 2, 2);
    const parent = try root.builder.getAnyPointer(0);
    const original_pointer = pointerWord(parent);
    const bytes_before = allocatedBytes(&builder);
    const list = try root.getList("records");

    var source_builder = message.MessageBuilder.init(allocator);
    defer source_builder.deinit();
    const source_root = try source_builder.allocateStruct(3, 3);
    source_root.writeU64(0, 91);
    source_root.writeU64(8, 92);
    source_root.writeU64(16, 0xfeed);
    try source_root.writeText(0, "replacement label");
    try source_root.writeText(1, "replacement note");
    // An unknown extra pointer must be retained, so cloning must encounter
    // this invalid target after allocating the replacement list and text.
    corrupt(try source_root.getAnyPointer(2));
    const source_bytes = try source_builder.toBytes();
    defer allocator.free(source_bytes);
    var source_message = try message.Message.initUnvalidated(allocator, source_bytes);
    defer source_message.deinit();
    const element_schema = try (try (try (try schema.field("records")).type()).listElement()).asStruct();
    const source = try reflection.DynamicStruct.Reader.init(element_schema, &source_message);

    try expectError(error.InvalidSegmentId, list.set(1, .{ .@"struct" = source }));
    try std.testing.expect(allocatedBytes(&builder) > bytes_before);
    try equal(original_pointer, pointerWord(parent));
    try checkOriginal(allocator, &builder, 2, 2, false);
}

pub fn run(init: std.process.Init, registry: reflection.Registry) !void {
    const schema = try (try generated.Evolution.capnpSchema.resolve(registry)).asStruct();
    try copyingOriginalFails(init.gpa, schema);
    try copyingReplacementFails(init.gpa, schema);
}
