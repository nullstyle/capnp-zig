const std = @import("std");
const testing = std.testing;
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

fn mutableCopy(bytes: []const u8) ![]u8 {
    const out = try testing.allocator.alloc(u8, bytes.len);
    @memcpy(out, bytes);
    return out;
}

fn setRootPointer0ElementCount(framed: []u8, element_count: u32) !void {
    // These tests build a single-segment root struct with 0 data words and
    // 1 pointer word, so pointer field 0 is at framed byte offset 16.
    const pointer_offset = 16;
    if (framed.len < pointer_offset + 8) return error.TestSetupFailed;
    var pointer_word = std.mem.readInt(u64, framed[pointer_offset..][0..8], .little);
    const element_count_mask = @as(u64, 0x1fff_ffff) << 35;
    pointer_word = (pointer_word & ~element_count_mask) | (@as(u64, element_count) << 35);
    std.mem.writeInt(u64, framed[pointer_offset..][0..8], pointer_word, .little);
}

fn packedZeroRunOverMessageLimit() ![]u8 {
    const decoded_bytes_per_run = 256 * 8;
    const runs = message.Message.max_packed_unpacked_bytes / decoded_bytes_per_run + 1;
    const packed_bytes = try testing.allocator.alloc(u8, runs * 2);
    for (0..runs) |i| {
        packed_bytes[i * 2] = 0x00;
        packed_bytes[i * 2 + 1] = 0xff;
    }
    return packed_bytes;
}

test "MessageBuilder: create empty message" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    // Should have at least header
    try testing.expect(bytes.len >= 8);
}

test "MessageBuilder and Message: round trip simple struct" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Create a simple struct with 1 data word, 0 pointers
    var struct_builder = try builder.allocateStruct(1, 0);
    struct_builder.writeU32(0, 42);
    struct_builder.writeU32(4, 100);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    // Read it back
    var msg = try message.Message.initUnvalidated(testing.allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 42), root.readU32(0));
    try testing.expectEqual(@as(u32, 100), root.readU32(4));
}

test "MessageBuilder and Message: bool fields" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(1, 0);
    struct_builder.writeBool(0, 0, true);
    struct_builder.writeBool(0, 1, false);
    struct_builder.writeBool(0, 2, true);
    struct_builder.writeBool(0, 7, true);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.initUnvalidated(testing.allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(true, root.readBool(0, 0));
    try testing.expectEqual(false, root.readBool(0, 1));
    try testing.expectEqual(true, root.readBool(0, 2));
    try testing.expectEqual(false, root.readBool(0, 3));
    try testing.expectEqual(true, root.readBool(0, 7));
}

test "MessageBuilder and Message: all integer types" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(2, 0);
    struct_builder.writeU8(0, 255);
    struct_builder.writeU16(2, 65535);
    struct_builder.writeU32(4, 4294967295);
    struct_builder.writeU64(8, 18446744073709551615);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u8, 255), root.readU8(0));
    try testing.expectEqual(@as(u16, 65535), root.readU16(2));
    try testing.expectEqual(@as(u32, 4294967295), root.readU32(4));
    try testing.expectEqual(@as(u64, 18446744073709551615), root.readU64(8));
}

test "StructReader: extreme offsets and pointer indexes do not overflow" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(1, 1);
    struct_builder.writeU64(0, 1234);
    try struct_builder.writeText(0, "safe");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const max = std.math.maxInt(usize);

    try testing.expectEqual(@as(u64, 0), root.readU64(max));
    try testing.expectEqual(@as(u32, 0), root.readU32(max));
    try testing.expectEqual(@as(u16, 0), root.readU16(max));
    try testing.expectEqual(@as(u8, 0), root.readU8(max));
    try testing.expectEqual(false, root.readBool(max, 0));
    try testing.expectError(error.OutOfBounds, root.readU64Strict(max));
    try testing.expectError(error.OutOfBounds, root.readU32Strict(max));
    try testing.expectError(error.OutOfBounds, root.readU16Strict(max));
    try testing.expectError(error.OutOfBounds, root.readU8Strict(max));
    try testing.expectError(error.OutOfBounds, root.readBoolStrict(max, 0));

    try testing.expect(root.isPointerNull(max));
    try testing.expectEqualStrings("", try root.readText(max));
    try testing.expectError(error.OutOfBounds, root.readAnyPointer(max));
    try testing.expectError(error.OutOfBounds, root.readStruct(max));
    try testing.expectError(error.OutOfBounds, root.readStructList(max));
    try testing.expectError(error.OutOfBounds, root.readTextList(max));
    try testing.expectError(error.OutOfBounds, root.readPointerList(max));
    try testing.expectError(error.OutOfBounds, root.readData(max));
}

test "MessageBuilder and Message: text field" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    try struct_builder.writeText(0, "Hello, Cap'n Proto!");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const text = try root.readText(0);
    try testing.expectEqualStrings("Hello, Cap'n Proto!", text);
}

test "MessageBuilder and Message: empty text field" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    try struct_builder.writeText(0, "");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const text = try root.readText(0);
    try testing.expectEqualStrings("", text);
}

test "MessageBuilder and Message: multiple text fields" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 3);
    try struct_builder.writeText(0, "First");
    try struct_builder.writeText(1, "Second");
    try struct_builder.writeText(2, "Third");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqualStrings("First", try root.readText(0));
    try testing.expectEqualStrings("Second", try root.readText(1));
    try testing.expectEqualStrings("Third", try root.readText(2));
}

test "MessageBuilder and Message: mixed data and pointer fields" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(1, 2);
    struct_builder.writeU32(0, 42);
    struct_builder.writeU32(4, 100);
    try struct_builder.writeText(0, "Hello");
    try struct_builder.writeText(1, "World");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 42), root.readU32(0));
    try testing.expectEqual(@as(u32, 100), root.readU32(4));
    try testing.expectEqualStrings("Hello", try root.readText(0));
    try testing.expectEqualStrings("World", try root.readText(1));
}

test "Message: handle truncated message" {
    const allocator = testing.allocator;

    // Create a valid message first
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.allocateStruct(1, 0);
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    // Try to parse truncated version
    if (bytes.len > 4) {
        const truncated = bytes[0 .. bytes.len - 4];
        const result = message.Message.init(allocator, truncated, .{});
        try testing.expectError(error.TruncatedMessage, result);
    }
}

test "Message: validate traversal and nesting limits" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(1, 1);
    struct_builder.writeU64(0, 42);
    try struct_builder.writeText(0, "hello");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    try msg.validate(.{});
    try testing.expectError(error.TraversalLimitExceeded, msg.validate(.{ .traversal_limit_words = 1, .nesting_limit = 64 }));
    try testing.expectError(error.NestingLimitExceeded, msg.validate(.{ .nesting_limit = 0 }));
}

test "Message: segment count decode limit is enforced" {
    // segment_count_minus_one = 512 => 513 segments, beyond Message.max_segment_count (512)
    const bytes = [_]u8{ 0x00, 0x02, 0x00, 0x00 };
    try testing.expectError(error.SegmentCountLimitExceeded, message.Message.init(testing.allocator, &bytes, .{}));
}

test "Message: validate enforces segment count limit option" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    _ = try builder.allocateStruct(0, 0);
    _ = try builder.createSegment();

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    try msg.validate(.{ .segment_count_limit = 2 });
    try testing.expectError(error.SegmentCountLimitExceeded, msg.validate(.{ .segment_count_limit = 1 }));
}

test "Message: traversal limit boundary conditions" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(1, 0);
    root_builder.writeU64(0, 123);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    try msg.validate(.{ .traversal_limit_words = 1 });
    try testing.expectError(error.TraversalLimitExceeded, msg.validate(.{ .traversal_limit_words = 0 }));
}

test "Message: nesting limit boundary conditions" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var child = try root_builder.initStruct(0, 0, 1);
    var grandchild = try child.initStruct(0, 1, 0);
    grandchild.writeU32(0, 99);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    try msg.validate(.{ .nesting_limit = 3 });
    try testing.expectError(error.NestingLimitExceeded, msg.validate(.{ .nesting_limit = 2 }));
}

test "StructReader: out of bounds access returns zero" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(1, 0);
    struct_builder.writeU32(0, 42);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();

    // Reading beyond the data section should return 0
    try testing.expectEqual(@as(u32, 0), root.readU32(100));
    try testing.expectEqual(@as(u64, 0), root.readU64(200));
}

fn encodeOffsetWords(offset_words: i32) u32 {
    if (offset_words < 0) {
        const base: i64 = 1 << 30;
        return @as(u32, @intCast(base + offset_words));
    }
    return @as(u32, @intCast(offset_words));
}

fn makeStructPointer(offset_words: i32, data_words: u16, pointer_words: u16) u64 {
    var pointer: u64 = 0;
    pointer |= @as(u64, encodeOffsetWords(offset_words)) << 2;
    pointer |= @as(u64, data_words) << 32;
    pointer |= @as(u64, pointer_words) << 48;
    return pointer;
}

fn makeListPointer(offset_words: i32, element_size: u3, element_count: u32) u64 {
    var pointer: u64 = 1; // list pointer
    pointer |= @as(u64, encodeOffsetWords(offset_words)) << 2;
    pointer |= @as(u64, element_size) << 32;
    pointer |= @as(u64, element_count) << 35;
    return pointer;
}

fn makeFarPointer(landing_pad_is_double: bool, landing_pad_offset_words: u32, segment_id: u32) u64 {
    var pointer: u64 = 2; // far pointer
    if (landing_pad_is_double) {
        pointer |= @as(u64, 1) << 2;
    }
    pointer |= @as(u64, landing_pad_offset_words) << 3;
    pointer |= @as(u64, segment_id) << 32;
    return pointer;
}

test "Message: negative list pointer offset" {
    const allocator = testing.allocator;

    var segment: [3 * 8]u8 = @splat(0);

    // Text data at word 1 (offset 8): "hi\0"
    segment[8] = 'h';
    segment[9] = 'i';
    segment[10] = 0;

    // Root pointer at word 0 -> struct at word 2 (offset = 1)
    const root_ptr = makeStructPointer(1, 0, 1);
    std.mem.writeInt(u64, segment[0..8], root_ptr, .little);

    // List pointer at word 2 -> text at word 1 (offset = -2)
    const list_ptr = makeListPointer(-2, 2, 3);
    std.mem.writeInt(u64, segment[16..24], list_ptr, .little);

    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 0, .little); // segment count - 1
    std.mem.writeInt(u32, header[4..8], 3, .little); // segment size in words
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqualStrings("hi", try root.readText(0));
}

test "Message: zero-width inline-composite list consumes element budget" {
    const allocator = testing.allocator;
    const element_count = message.Message.max_total_words + 1;

    var segment: [3 * 8]u8 = @splat(0);
    std.mem.writeInt(u64, segment[0..8], makeStructPointer(0, 0, 1), .little);
    std.mem.writeInt(u64, segment[8..16], makeListPointer(0, 7, 0), .little);
    std.mem.writeInt(u64, segment[16..24], makeStructPointer(@intCast(element_count), 0, 0), .little);

    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 0, .little);
    std.mem.writeInt(u32, header[4..8], 3, .little);
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    try testing.expectError(error.TraversalLimitExceeded, message.Message.init(allocator, bytes, .{}));
}

test "Message: init caps total segment words including unreferenced segments" {
    const allocator = testing.allocator;

    var segment0: [8]u8 = @splat(0);
    std.mem.writeInt(u64, segment0[0..8], makeStructPointer(0, 0, 0), .little);
    var unreferenced_segment: [4 * 8]u8 = @splat(0);

    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [16]u8 = @splat(0);
    std.mem.writeInt(u32, header[0..4], 1, .little); // two segments
    std.mem.writeInt(u32, header[4..8], 1, .little); // root segment
    std.mem.writeInt(u32, header[8..12], 4, .little); // unreferenced segment
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment0);
    try framed.appendSlice(allocator, &unreferenced_segment);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    try testing.expectError(error.MessageTooLarge, message.Message.init(allocator, bytes, .{
        .total_segment_words_limit = 4,
    }));

    var msg = try message.Message.init(allocator, bytes, .{
        .total_segment_words_limit = 5,
    });
    defer msg.deinit();
}

test "Message: far pointer root struct in another segment" {
    const allocator = testing.allocator;

    var segment0: [1 * 8]u8 = @splat(0);
    var segment1: [2 * 8]u8 = @splat(0);

    const far_ptr = makeFarPointer(false, 0, 1);
    std.mem.writeInt(u64, segment0[0..8], far_ptr, .little);

    const struct_ptr = makeStructPointer(0, 1, 0);
    std.mem.writeInt(u64, segment1[0..8], struct_ptr, .little);
    std.mem.writeInt(u32, segment1[8..12], 123, .little);

    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [16]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 1, .little); // segment count - 1
    std.mem.writeInt(u32, header[4..8], 1, .little); // segment0 size in words
    std.mem.writeInt(u32, header[8..12], 2, .little); // segment1 size in words
    std.mem.writeInt(u32, header[12..16], 0, .little); // padding
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment0);
    try framed.appendSlice(allocator, &segment1);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 123), root.readU32(0));
}

test "Message: far pointer list in another segment" {
    const allocator = testing.allocator;

    var segment0: [2 * 8]u8 = @splat(0);
    var segment1: [2 * 8]u8 = @splat(0);

    const root_ptr = makeStructPointer(0, 0, 1);
    std.mem.writeInt(u64, segment0[0..8], root_ptr, .little);

    const far_ptr = makeFarPointer(false, 0, 1);
    std.mem.writeInt(u64, segment0[8..16], far_ptr, .little);

    const list_ptr = makeListPointer(0, 2, 3);
    std.mem.writeInt(u64, segment1[0..8], list_ptr, .little);
    segment1[8] = 'h';
    segment1[9] = 'i';
    segment1[10] = 0;

    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [16]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 1, .little); // segment count - 1
    std.mem.writeInt(u32, header[4..8], 2, .little); // segment0 size in words
    std.mem.writeInt(u32, header[8..12], 2, .little); // segment1 size in words
    std.mem.writeInt(u32, header[12..16], 0, .little); // padding
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment0);
    try framed.appendSlice(allocator, &segment1);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqualStrings("hi", try root.readText(0));
}

test "Message: double-far pointer root struct" {
    const allocator = testing.allocator;

    var segment0: [1 * 8]u8 = @splat(0);
    var segment1: [3 * 8]u8 = @splat(0);

    const far_ptr = makeFarPointer(true, 0, 1);
    std.mem.writeInt(u64, segment0[0..8], far_ptr, .little);

    const content_far = makeFarPointer(false, 2, 1);
    std.mem.writeInt(u64, segment1[0..8], content_far, .little);

    const tag = makeStructPointer(0, 1, 0);
    std.mem.writeInt(u64, segment1[8..16], tag, .little);

    std.mem.writeInt(u32, segment1[16..20], 77, .little);

    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [16]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 1, .little); // segment count - 1
    std.mem.writeInt(u32, header[4..8], 1, .little); // segment0 size in words
    std.mem.writeInt(u32, header[8..12], 3, .little); // segment1 size in words
    std.mem.writeInt(u32, header[12..16], 0, .little); // padding
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment0);
    try framed.appendSlice(allocator, &segment1);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 77), root.readU32(0));
}

test "MessageBuilder: writeText across segments emits far pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    const segment_id = try builder.createSegment();

    try struct_builder.writeTextInSegment(0, "segment", segment_id);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqualStrings("segment", try root.readText(0));
}

test "MessageBuilder: inline composite list in same segment" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    var list_builder = try struct_builder.writeStructList(0, 2, 1, 0);

    var first = try list_builder.get(0);
    first.writeU32(0, 10);
    var second = try list_builder.get(1);
    second.writeU32(0, 20);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqual(@as(u32, 10), (try list_reader.get(0)).readU32(0));
    try testing.expectEqual(@as(u32, 20), (try list_reader.get(1)).readU32(0));
}

test "MessageBuilder: inline composite list with far pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    const target_segment = try builder.createSegment();
    var list_builder = try struct_builder.writeStructListInSegment(0, 2, 1, 0, target_segment);

    var first = try list_builder.get(0);
    first.writeU32(0, 111);
    var second = try list_builder.get(1);
    second.writeU32(0, 222);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqual(@as(u32, 111), (try list_reader.get(0)).readU32(0));
    try testing.expectEqual(@as(u32, 222), (try list_reader.get(1)).readU32(0));
}

test "MessageBuilder: inline composite list with double-far pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    const landing_segment = try builder.createSegment();
    const content_segment = try builder.createSegment();

    var list_builder = try struct_builder.writeStructListInSegments(0, 2, 1, 0, landing_segment, content_segment);

    var first = try list_builder.get(0);
    first.writeU32(0, 7);
    var second = try list_builder.get(1);
    second.writeU32(0, 9);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqual(@as(u32, 7), (try list_reader.get(0)).readU32(0));
    try testing.expectEqual(@as(u32, 9), (try list_reader.get(1)).readU32(0));
}

test "MessageBuilder: text list in same segment" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    var list_builder = try struct_builder.writeTextList(0, 2);

    try list_builder.set(0, "alpha");
    try list_builder.set(1, "beta");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readTextList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqualStrings("alpha", try list_reader.get(0));
    try testing.expectEqualStrings("beta", try list_reader.get(1));
}

test "MessageBuilder: text list with far pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    const list_segment = try builder.createSegment();
    var list_builder = try struct_builder.writeTextListInSegment(0, 2, list_segment);

    try list_builder.set(0, "one");
    try list_builder.set(1, "two");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readTextList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqualStrings("one", try list_reader.get(0));
    try testing.expectEqualStrings("two", try list_reader.get(1));
}

test "MessageBuilder: text list with double-far pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    const landing_segment = try builder.createSegment();
    const content_segment = try builder.createSegment();
    var list_builder = try struct_builder.writeTextListInSegments(0, 2, landing_segment, content_segment);

    try list_builder.set(0, "left");
    try list_builder.set(1, "right");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readTextList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqualStrings("left", try list_reader.get(0));
    try testing.expectEqualStrings("right", try list_reader.get(1));
}

test "Message: cloneAnyPointer clones text list behind far pointer" {
    var src_builder = message.MessageBuilder.init(testing.allocator);
    defer src_builder.deinit();

    var src_struct = try src_builder.allocateStruct(0, 1);
    const list_segment = try src_builder.createSegment();
    var src_list = try src_struct.writeTextListInSegment(0, 2, list_segment);
    try src_list.set(0, "north");
    try src_list.set(1, "south");

    const src_bytes = try src_builder.toBytes();
    defer testing.allocator.free(src_bytes);

    var src_msg = try message.Message.init(testing.allocator, src_bytes, .{});
    defer src_msg.deinit();

    const src_root = try src_msg.getRootStruct();
    const src_any = try src_root.readAnyPointer(0);
    try testing.expectEqual(@as(u2, 2), @as(u2, @truncate(src_any.pointer_word & 0x3)));
    try testing.expectEqual(@as(u1, 0), @as(u1, @truncate((src_any.pointer_word >> 2) & 0x1)));

    var dest_builder = message.MessageBuilder.init(testing.allocator);
    defer dest_builder.deinit();
    const dest_root = try dest_builder.initRootAnyPointer();
    try message.cloneAnyPointer(src_any, dest_root);

    const dest_bytes = try dest_builder.toBytes();
    defer testing.allocator.free(dest_bytes);

    var dest_msg = try message.Message.init(testing.allocator, dest_bytes, .{});
    defer dest_msg.deinit();

    const dest_any = try dest_msg.getRootAnyPointer();
    const dest_list = try dest_any.getPointerList();
    try testing.expectEqual(@as(u32, 2), dest_list.len());
    try testing.expectEqualStrings("north", try dest_list.getText(0));
    try testing.expectEqualStrings("south", try dest_list.getText(1));
}

test "Message: cloneAnyPointer clones text list behind double-far pointer" {
    var src_builder = message.MessageBuilder.init(testing.allocator);
    defer src_builder.deinit();

    var src_struct = try src_builder.allocateStruct(0, 1);
    const landing_segment = try src_builder.createSegment();
    const content_segment = try src_builder.createSegment();
    var src_list = try src_struct.writeTextListInSegments(0, 2, landing_segment, content_segment);
    try src_list.set(0, "left");
    try src_list.set(1, "right");

    const src_bytes = try src_builder.toBytes();
    defer testing.allocator.free(src_bytes);

    var src_msg = try message.Message.init(testing.allocator, src_bytes, .{});
    defer src_msg.deinit();

    const src_root = try src_msg.getRootStruct();
    const src_any = try src_root.readAnyPointer(0);
    try testing.expectEqual(@as(u2, 2), @as(u2, @truncate(src_any.pointer_word & 0x3)));
    try testing.expectEqual(@as(u1, 1), @as(u1, @truncate((src_any.pointer_word >> 2) & 0x1)));

    var dest_builder = message.MessageBuilder.init(testing.allocator);
    defer dest_builder.deinit();
    const dest_root = try dest_builder.initRootAnyPointer();
    try message.cloneAnyPointer(src_any, dest_root);

    const dest_bytes = try dest_builder.toBytes();
    defer testing.allocator.free(dest_bytes);

    var dest_msg = try message.Message.init(testing.allocator, dest_bytes, .{});
    defer dest_msg.deinit();

    const dest_any = try dest_msg.getRootAnyPointer();
    const dest_list = try dest_any.getPointerList();
    try testing.expectEqual(@as(u32, 2), dest_list.len());
    try testing.expectEqualStrings("left", try dest_list.getText(0));
    try testing.expectEqualStrings("right", try dest_list.getText(1));
}

test "MessageBuilder: text list elements stored in other segment" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    var list_builder = try struct_builder.writeTextList(0, 2);
    const text_segment = try builder.createSegment();

    try list_builder.setInSegment(0, "east", text_segment);
    try list_builder.setInSegment(1, "west", text_segment);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readTextList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqualStrings("east", try list_reader.get(0));
    try testing.expectEqualStrings("west", try list_reader.get(1));
}

test "MessageBuilder: pointer list with struct and text" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writePointerList(0, 2);

    var struct_builder = try list_builder.initStruct(0, 1, 0);
    struct_builder.writeU32(0, 555);

    try list_builder.setText(1, "ptr");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readPointerList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqual(@as(u32, 555), (try list_reader.getStruct(0)).readU32(0));
    try testing.expectEqualStrings("ptr", try list_reader.getText(1));
}

test "MessageBuilder: primitive lists and list-of-lists" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 8);
    try root_builder.writeData(0, &[_]u8{ 1, 2, 3 });

    var u16s = try root_builder.writeU16List(1, 2);
    try u16s.set(0, 10);
    try u16s.set(1, 20);

    var u32s = try root_builder.writeU32List(2, 1);
    try u32s.set(0, 3000);

    var u64s = try root_builder.writeU64List(3, 1);
    try u64s.set(0, 4000);

    var bools = try root_builder.writeBoolList(4, 4);
    try bools.set(0, true);
    try bools.set(1, false);
    try bools.set(2, true);
    try bools.set(3, false);

    var f32s = try root_builder.writeF32List(5, 2);
    try f32s.set(0, 1.25);
    try f32s.set(1, -2.5);

    var f64s = try root_builder.writeF64List(6, 1);
    try f64s.set(0, 3.5);

    var list_of_lists = try root_builder.writePointerList(7, 2);
    var list0 = try list_of_lists.initU16List(0, 2);
    try list0.set(0, 7);
    try list0.set(1, 8);
    var list1 = try list_of_lists.initU16List(1, 3);
    try list1.set(0, 9);
    try list1.set(1, 10);
    try list1.set(2, 11);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3 }, try root.readData(0));

    const u16s_reader = try root.readU16List(1);
    try testing.expectEqual(@as(u32, 2), u16s_reader.len());
    try testing.expectEqual(@as(u16, 10), try u16s_reader.get(0));
    try testing.expectEqual(@as(u16, 20), try u16s_reader.get(1));

    const u32s_reader = try root.readU32List(2);
    try testing.expectEqual(@as(u32, 1), u32s_reader.len());
    try testing.expectEqual(@as(u32, 3000), try u32s_reader.get(0));

    const u64s_reader = try root.readU64List(3);
    try testing.expectEqual(@as(u32, 1), u64s_reader.len());
    try testing.expectEqual(@as(u64, 4000), try u64s_reader.get(0));

    const bools_reader = try root.readBoolList(4);
    try testing.expectEqual(@as(u32, 4), bools_reader.len());
    try testing.expectEqual(true, try bools_reader.get(0));
    try testing.expectEqual(false, try bools_reader.get(1));
    try testing.expectEqual(true, try bools_reader.get(2));
    try testing.expectEqual(false, try bools_reader.get(3));

    const f32s_reader = try root.readF32List(5);
    try testing.expectApproxEqAbs(@as(f32, 1.25), try f32s_reader.get(0), 0.0001);
    try testing.expectApproxEqAbs(@as(f32, -2.5), try f32s_reader.get(1), 0.0001);

    const f64s_reader = try root.readF64List(6);
    try testing.expectApproxEqAbs(@as(f64, 3.5), try f64s_reader.get(0), 0.0001);

    const list_reader = try root.readPointerList(7);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    const r0 = try list_reader.getU16List(0);
    try testing.expectEqual(@as(u32, 2), r0.len());
    try testing.expectEqual(@as(u16, 7), try r0.get(0));
    try testing.expectEqual(@as(u16, 8), try r0.get(1));
    const r1 = try list_reader.getU16List(1);
    try testing.expectEqual(@as(u32, 3), r1.len());
    try testing.expectEqual(@as(u16, 9), try r1.get(0));
    try testing.expectEqual(@as(u16, 10), try r1.get(1));
    try testing.expectEqual(@as(u16, 11), try r1.get(2));
}

test "MessageBuilder: primitive list with far pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    const list_segment = try builder.createSegment();
    var u16s = try root_builder.writeU16ListInSegment(0, 3, list_segment);
    try u16s.set(0, 101);
    try u16s.set(1, 202);
    try u16s.set(2, 303);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const u16s_reader = try root.readU16List(0);
    try testing.expectEqual(@as(u32, 3), u16s_reader.len());
    try testing.expectEqual(@as(u16, 101), try u16s_reader.get(0));
    try testing.expectEqual(@as(u16, 202), try u16s_reader.get(1));
    try testing.expectEqual(@as(u16, 303), try u16s_reader.get(2));
}

test "MessageBuilder and Message: capability pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    var any = try struct_builder.getAnyPointer(0);
    try any.setCapability(.{ .id = 42 });

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.initUnvalidated(testing.allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const any_reader = try root.readAnyPointer(0);
    const cap = try any_reader.getCapability();
    try testing.expectEqual(@as(u32, 42), cap.id);

    const cap2 = try root.readCapability(0);
    try testing.expectEqual(@as(u32, 42), cap2.id);
}

test "MessageBuilder and Message: capability pointer list" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    var list = try struct_builder.writePointerList(0, 2);
    try list.setCapability(0, .{ .id = 1 });
    try list.setCapability(1, .{ .id = 7 });

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.initUnvalidated(testing.allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readPointerList(0);
    const cap0 = try list_reader.getCapability(0);
    const cap1 = try list_reader.getCapability(1);
    try testing.expectEqual(@as(u32, 1), cap0.id);
    try testing.expectEqual(@as(u32, 7), cap1.id);
}

test "Message validation rejects invalid capability pointer encoding" {
    var bytes: [16]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 0, .little);
    std.mem.writeInt(u32, bytes[4..8], 1, .little);
    std.mem.writeInt(u64, bytes[8..16], (@as(u64, 1) << 2) | 3, .little);

    try testing.expectError(error.InvalidPointer, message.Message.init(testing.allocator, &bytes, .{}));
}

test "MessageBuilder: packed bytes roundtrip" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(1, 1);
    root_builder.writeU32(0, 4242);
    try root_builder.writeText(0, "packed");

    const packed_bytes = try builder.toPackedBytes();
    defer testing.allocator.free(packed_bytes);

    var msg = try message.Message.initPacked(testing.allocator, packed_bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 4242), root.readU32(0));
    try testing.expectEqualStrings("packed", try root.readText(0));
}

test "Message: initPacked caps unpacked expansion" {
    const packed_bytes = try packedZeroRunOverMessageLimit();
    defer testing.allocator.free(packed_bytes);

    try testing.expectError(error.MessageTooLarge, message.Message.initPackedUnvalidated(testing.allocator, packed_bytes));
    try testing.expectError(error.MessageTooLarge, message.Message.initPacked(testing.allocator, packed_bytes, .{}));
}

test "Message: initPacked rejects oversized segment word claims" {
    const oversized_words: u32 = @as(u32, @intCast(message.Message.max_total_words + 1));
    var packed_bytes: [10]u8 = @splat(0);
    packed_bytes[0] = 0xff;
    std.mem.writeInt(u64, packed_bytes[1..9], @as(u64, oversized_words) << 32, .little);
    packed_bytes[9] = 0;

    try testing.expectError(error.MessageTooLarge, message.Message.initPackedUnvalidated(testing.allocator, &packed_bytes));
    try testing.expectError(error.MessageTooLarge, message.Message.initPacked(testing.allocator, &packed_bytes, .{}));
}

test "AnyPointer: set and read text" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var any = try root_builder.getAnyPointer(0);
    try any.setText("any");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const any_reader = try root.readAnyPointer(0);
    try testing.expectEqualStrings("any", try any_reader.getText());
}

test "Message: malformed segment count header reports InvalidSegmentCount" {
    const bytes = [_]u8{
        0xff, 0xff, 0xff, 0xff, // segment_count_minus_one (overflow)
        0x00, 0x00, 0x00, 0x00, // first segment size (unused)
    };
    try testing.expectError(error.InvalidSegmentCount, message.Message.init(testing.allocator, &bytes, .{}));
}

test "MessageBuilder: struct list rejects oversized element count" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(0, 1);
    const too_many: u32 = @as(u32, @intCast(std.math.maxInt(i32))) + 1;
    try testing.expectError(error.ElementCountTooLarge, root.writeStructList(0, too_many, 1, 0));
}

test "Message: invalid double-far landing pointer reports InvalidFarPointer" {
    var bytes: [40]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 1, .little); // 2 segments total
    std.mem.writeInt(u32, bytes[4..8], 1, .little); // segment 0: root pointer word
    std.mem.writeInt(u32, bytes[8..12], 2, .little); // segment 1: double-far landing pad (2 words)
    // bytes[12..16] padding word left as zero

    const root_double_far: u64 = 2 | (@as(u64, 1) << 2) | (@as(u64, 1) << 32);
    std.mem.writeInt(u64, bytes[16..24], root_double_far, .little);

    // landing first word intentionally not a far pointer -> InvalidFarPointer
    std.mem.writeInt(u64, bytes[24..32], 0, .little);
    std.mem.writeInt(u64, bytes[32..40], 0, .little);

    var msg = try message.Message.initUnvalidated(testing.allocator, &bytes);
    defer msg.deinit();

    try testing.expectError(error.InvalidFarPointer, msg.getRootStruct());
}

test "Message: inline composite overflow in expected words is rejected" {
    var bytes: [32]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 0, .little); // 1 segment
    std.mem.writeInt(u32, bytes[4..8], 3, .little); // 3 words

    // Root pointer: inline composite list with 1 word payload.
    const root_pointer: u64 = 1 | (@as(u64, 7) << 32) | (@as(u64, 1) << 35);
    std.mem.writeInt(u64, bytes[8..16], root_pointer, .little);

    // Tag word: element_count=65536, data_words=1, pointer_words=65535.
    // This overflows u32 multiplication if arithmetic is unchecked.
    const tag_word: u64 = (@as(u64, 65_536) << 2) | (@as(u64, 1) << 32) | (@as(u64, 65_535) << 48);
    std.mem.writeInt(u64, bytes[16..24], tag_word, .little);

    var msg = try message.Message.initUnvalidated(testing.allocator, &bytes);
    defer msg.deinit();

    const root = try msg.getRootAnyPointer();
    try testing.expectError(error.InvalidInlineCompositePointer, root.getInlineCompositeList());
}

test "Message: double-far inline composite overflow is rejected" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    const landing_segment = try builder.createSegment();
    const content_segment = try builder.createSegment();

    _ = try root_builder.writeStructListInSegments(0, 2, 1, 0, landing_segment, content_segment);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    const mutated = try testing.allocator.alloc(u8, bytes.len);
    defer testing.allocator.free(mutated);
    std.mem.copyForwards(u8, mutated, bytes);

    try testing.expectEqual(@as(u32, 1), landing_segment);
    const segment_count = std.mem.readInt(u32, mutated[0..4], .little) + 1;
    try testing.expectEqual(@as(u32, 3), segment_count);

    const padding_words: usize = if (segment_count % 2 == 0) 1 else 0;
    const header_bytes = (1 + @as(usize, segment_count) + padding_words) * 4;
    const segment0_words = std.mem.readInt(u32, mutated[4..8], .little);
    const landing_offset = header_bytes + @as(usize, segment0_words) * 8;

    // Mutate landing-pad tag to trigger words-per-element multiplication overflow.
    const tag_word: u64 = (@as(u64, 65_536) << 2) | (@as(u64, 1) << 32) | (@as(u64, 65_535) << 48);
    std.mem.writeInt(u64, mutated[landing_offset + 8 ..][0..8], tag_word, .little);

    var msg = try message.Message.initUnvalidated(testing.allocator, mutated);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectError(error.OutOfBounds, root.readStructList(0));
}

test "Message: fuzz malformed buffers do not crash decode" {
    var prng = std.Random.DefaultPrng.init(0x3E22_7AB4_BD10_9C61);
    const random = prng.random();

    var i: usize = 0;
    while (i < 1024) : (i += 1) {
        const len = random.uintLessThan(usize, 160);
        const bytes = try testing.allocator.alloc(u8, len);
        defer testing.allocator.free(bytes);
        random.bytes(bytes);

        var msg = message.Message.initUnvalidated(testing.allocator, bytes) catch continue;
        defer msg.deinit();

        _ = msg.getRootStruct() catch {};
        _ = msg.validate(.{}) catch {};
    }
}

test "Message: fuzz malformed packed buffers do not crash decode" {
    var prng = std.Random.DefaultPrng.init(0xA7C4_1E59_F032_8D6B);
    const random = prng.random();

    var i: usize = 0;
    while (i < 1024) : (i += 1) {
        const len = random.uintLessThan(usize, 160);
        const bytes = try testing.allocator.alloc(u8, len);
        defer testing.allocator.free(bytes);
        random.bytes(bytes);

        var msg = message.Message.initPackedUnvalidated(testing.allocator, bytes) catch continue;
        defer msg.deinit();

        _ = msg.getRootStruct() catch {};
        _ = msg.validate(.{}) catch {};
    }
}

test "Message: packed format adversarial edge cases do not crash" {
    // Each entry is a hand-crafted adversarial byte sequence targeting specific
    // edge cases in the packed decoder's tag-byte processing.
    const cases = [_][]const u8{
        // Zero-length input
        &[_]u8{},
        // Single byte (just a tag byte, no payload)
        &[_]u8{0x01},
        // Tag 0x00 with no count byte (truncated)
        &[_]u8{0x00},
        // Tag 0x00 with zero extra words (one all-zero word)
        &[_]u8{ 0x00, 0x00 },
        // Tag 0x00 with max count (255 extra zero words)
        &[_]u8{ 0x00, 0xFF },
        // Tag 0xFF with no literal word (truncated)
        &[_]u8{0xFF},
        // Tag 0xFF with partial literal word (truncated at 4 bytes)
        &[_]u8{ 0xFF, 0x01, 0x02, 0x03, 0x04 },
        // Tag 0xFF with full literal word but no count byte
        &[_]u8{ 0xFF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 },
        // Tag 0xFF with full literal word, count=0 (no extra literal words)
        &[_]u8{ 0xFF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00 },
        // Tag 0xFF with full literal word, count=1 but no data (truncated literal run)
        &[_]u8{ 0xFF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x01 },
        // Tag 0xFF with full literal word, count=1 but only partial run data
        &[_]u8{ 0xFF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x01, 0xAA, 0xBB },
        // Tag 0xFF with full literal word, count=255 (huge literal run, truncated)
        &[_]u8{ 0xFF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0xFF },
        // Regular tag with all bits set (0xFE) but insufficient payload bytes
        &[_]u8{ 0xFE, 0x01, 0x02 },
        // Regular tag with one bit set but no payload byte
        &[_]u8{0x80},
        // Multiple tags in sequence: zero word then truncated regular tag
        &[_]u8{ 0x00, 0x00, 0x01 },
        // Two zero-word tags back to back
        &[_]u8{ 0x00, 0x00, 0x00, 0x00 },
        // All 0xFF bytes (tag=0xFF, literal word=all-FF, count=0xFF, then truncated run)
        &[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF },
        // Minimal valid packed message attempt: a zero word that could be a header
        &[_]u8{ 0x00, 0x03 },
    };

    for (cases) |packed_bytes| {
        var msg = message.Message.initPackedUnvalidated(testing.allocator, packed_bytes) catch continue;
        defer msg.deinit();

        _ = msg.getRootStruct() catch {};
        _ = msg.validate(.{}) catch {};
    }
}

test "readTextStrict: valid UTF-8 passes" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    try struct_builder.writeText(0, "hello");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const text = try root.readTextStrict(0);
    try testing.expectEqualStrings("hello", text);
}

test "readTextStrict: empty text passes" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    try struct_builder.writeText(0, "");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const text = try root.readTextStrict(0);
    try testing.expectEqualStrings("", text);
}

test "readTextStrict: null pointer returns empty string" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    _ = try builder.allocateStruct(0, 1);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const text = try root.readTextStrict(0);
    try testing.expectEqualStrings("", text);
}

test "readTextStrict: missing trailing NUL is rejected without breaking readText" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    try struct_builder.writeText(0, "abc");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    const mutated = try mutableCopy(bytes);
    defer testing.allocator.free(mutated);
    try setRootPointer0ElementCount(mutated, 3);

    var msg = try message.Message.init(testing.allocator, mutated, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqualStrings("abc", try root.readText(0));
    try testing.expectError(error.InvalidTextPointer, root.readTextStrict(0));
}

test "readTextStrict: invalid UTF-8 with trailing NUL is rejected" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    try struct_builder.writeText(0, "bad");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    const mutated = try mutableCopy(bytes);
    defer testing.allocator.free(mutated);
    const pos = std.mem.indexOf(u8, mutated, "bad") orelse return error.TestSetupFailed;
    mutated[pos] = 0xff;

    var msg = try message.Message.init(testing.allocator, mutated, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const raw = try root.readText(0);
    try testing.expectEqual(@as(u8, 0xff), raw[0]);
    try testing.expectError(error.InvalidUtf8, root.readTextStrict(0));
}

test "AnyPointer.getTextStrict: missing trailing NUL is rejected" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var any = try root_builder.getAnyPointer(0);
    try any.setText("any");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    const mutated = try mutableCopy(bytes);
    defer testing.allocator.free(mutated);
    try setRootPointer0ElementCount(mutated, 3);

    var msg = try message.Message.init(testing.allocator, mutated, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const any_reader = try root.readAnyPointer(0);
    try testing.expectEqualStrings("any", try any_reader.getText());
    try testing.expectError(error.InvalidTextPointer, any_reader.getTextStrict());
}

test "TextListReader.getStrict: valid UTF-8 succeeds" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    var list_builder = try struct_builder.writeTextList(0, 3);

    try list_builder.set(0, "hello");
    try list_builder.set(1, "\xc3\xa9\xc3\xa0"); // valid UTF-8: e-acute, a-grave
    try list_builder.set(2, "\xe2\x9c\x93"); // valid UTF-8: checkmark

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readTextList(0);
    try testing.expectEqual(@as(u32, 3), list_reader.len());
    try testing.expectEqualStrings("hello", try list_reader.getStrict(0));
    try testing.expectEqualStrings("\xc3\xa9\xc3\xa0", try list_reader.getStrict(1));
    try testing.expectEqualStrings("\xe2\x9c\x93", try list_reader.getStrict(2));
}

test "TextListReader.getStrict: invalid UTF-8 returns error" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    var list_builder = try struct_builder.writeTextList(0, 2);

    try list_builder.set(0, "good");
    try list_builder.set(1, "bad!");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    // Copy the bytes so we can mutate them to inject invalid UTF-8.
    const mutated = try testing.allocator.alloc(u8, bytes.len);
    defer testing.allocator.free(mutated);
    @memcpy(mutated, bytes);

    // Find "bad!" in the buffer and replace 'b' with 0xFF (invalid UTF-8 lead byte).
    const needle = "bad!";
    const pos = std.mem.indexOf(u8, mutated, needle) orelse return error.TestSetupFailed;
    mutated[pos] = 0xFF;

    var msg = try message.Message.init(testing.allocator, mutated, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readTextList(0);

    // Element 0 is still valid UTF-8.
    try testing.expectEqualStrings("good", try list_reader.getStrict(0));

    // Element 1 contains invalid UTF-8 and must fail.
    try testing.expectError(error.InvalidUtf8, list_reader.getStrict(1));

    // Non-strict get still works and returns the raw bytes.
    const raw = try list_reader.get(1);
    try testing.expectEqual(@as(u8, 0xFF), raw[0]);
}

test "PointerListReader.getTextStrict: valid UTF-8 succeeds" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writePointerList(0, 3);

    try list_builder.setText(0, "alpha");
    try list_builder.setText(1, "\xc3\xbc\xc3\xb6"); // valid UTF-8: u-umlaut, o-umlaut
    try list_builder.setText(2, "\xf0\x9f\x98\x80"); // valid UTF-8: grinning face emoji

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readPointerList(0);
    try testing.expectEqual(@as(u32, 3), list_reader.len());
    try testing.expectEqualStrings("alpha", try list_reader.getTextStrict(0));
    try testing.expectEqualStrings("\xc3\xbc\xc3\xb6", try list_reader.getTextStrict(1));
    try testing.expectEqualStrings("\xf0\x9f\x98\x80", try list_reader.getTextStrict(2));
}

test "PointerListReader.getTextStrict: invalid UTF-8 returns error" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writePointerList(0, 2);

    try list_builder.setText(0, "fine");
    try list_builder.setText(1, "oops");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    // Copy the bytes so we can mutate them to inject invalid UTF-8.
    const mutated = try testing.allocator.alloc(u8, bytes.len);
    defer testing.allocator.free(mutated);
    @memcpy(mutated, bytes);

    // Find "oops" in the buffer and replace first 'o' with 0xFE
    // (invalid UTF-8 byte — never valid as a start or continuation byte).
    const needle = "oops";
    const pos = std.mem.indexOf(u8, mutated, needle) orelse return error.TestSetupFailed;
    mutated[pos] = 0xFE;

    var msg = try message.Message.init(testing.allocator, mutated, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readPointerList(0);

    // Element 0 is still valid UTF-8.
    try testing.expectEqualStrings("fine", try list_reader.getTextStrict(0));

    // Element 1 contains invalid UTF-8 and must fail.
    try testing.expectError(error.InvalidUtf8, list_reader.getTextStrict(1));

    // Non-strict getText still works and returns the raw bytes.
    const raw = try list_reader.getText(1);
    try testing.expectEqual(@as(u8, 0xFE), raw[0]);
}

test "Message: far pointer to inline-composite list (raw bytes)" {
    // Layout:
    //   Segment 0 (2 words):
    //     word 0: root struct pointer -> struct at word 1 (0 data, 1 pointer)
    //     word 1: far pointer (single) -> segment 1, offset 0
    //   Segment 1 (6 words):
    //     word 0: list pointer (element_size=7, offset=0, word_count=4)
    //     word 1: tag word (struct pointer: element_count=2, data_words=1, pointer_words=1)
    //     word 2: element 0 data (u32 = 42)
    //     word 3: element 0 pointer (null)
    //     word 4: element 1 data (u32 = 99)
    //     word 5: element 1 pointer (null)
    const allocator = testing.allocator;

    var segment0: [2 * 8]u8 = @splat(0);
    var segment1: [6 * 8]u8 = @splat(0);

    // Word 0: root struct pointer -> struct at offset 0 (i.e. word 1), 0 data words, 1 pointer word
    const root_ptr = makeStructPointer(0, 0, 1);
    std.mem.writeInt(u64, segment0[0..8], root_ptr, .little);

    // Word 1: far pointer -> segment 1, word offset 0, single (not double)
    const far_ptr = makeFarPointer(false, 0, 1);
    std.mem.writeInt(u64, segment0[8..16], far_ptr, .little);

    // Segment 1, word 0: list pointer, element_size=7 (inline composite), offset=0, word_count=4
    // After far pointer resolution, resolveInlineCompositeList is called recursively
    // with segment_id=1, pointer_pos=0, and this list pointer word.
    // tag_pos = pointer_pos + 8 + offset*8 = 0 + 8 + 0*8 = 8 (word 1 of segment 1)
    const list_ptr = makeListPointer(0, 7, 4);
    std.mem.writeInt(u64, segment1[0..8], list_ptr, .little);

    // Segment 1, word 1: tag word = struct pointer with element_count=2, data_words=1, pointer_words=1
    const tag_word = makeStructPointer(2, 1, 1);
    std.mem.writeInt(u64, segment1[8..16], tag_word, .little);

    // Segment 1, word 2: element 0, data word (u32 = 42)
    std.mem.writeInt(u32, segment1[16..20], 42, .little);

    // Segment 1, word 3: element 0, pointer word (null = 0)
    // Segment 1, word 4: element 1, data word (u32 = 99)
    std.mem.writeInt(u32, segment1[32..36], 99, .little);

    // Segment 1, word 5: element 1, pointer word (null = 0)

    // Frame the message: 2 segments
    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [16]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 1, .little); // segment_count - 1 = 1 (2 segments)
    std.mem.writeInt(u32, header[4..8], 2, .little); // segment 0 size: 2 words
    std.mem.writeInt(u32, header[8..12], 6, .little); // segment 1 size: 6 words
    std.mem.writeInt(u32, header[12..16], 0, .little); // padding
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment0);
    try framed.appendSlice(allocator, &segment1);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqual(@as(u32, 42), (try list_reader.get(0)).readU32(0));
    try testing.expectEqual(@as(u32, 99), (try list_reader.get(1)).readU32(0));
}

test "Message: double-far pointer Layout A inline-composite list (raw bytes)" {
    // Layout A: the landing pad's second word is a struct-pointer tag (type 0).
    // This is the layout our builder produces.
    //
    //   Segment 0 (2 words):
    //     word 0: root struct pointer -> struct at word 1 (0 data, 1 pointer)
    //     word 1: double-far pointer -> segment 1, offset 0
    //   Segment 1 (2 words = landing pad):
    //     word 0: far pointer (single) -> segment 2, offset 0
    //     word 1: struct pointer tag (element_count=2, data_words=1, pointer_words=0)
    //   Segment 2 (2 words = list content):
    //     word 0: element 0 data (u32 = 10)
    //     word 1: element 1 data (u32 = 20)
    const allocator = testing.allocator;

    var segment0: [2 * 8]u8 = @splat(0);
    var segment1: [2 * 8]u8 = @splat(0);
    var segment2: [2 * 8]u8 = @splat(0);

    // Segment 0, word 0: root struct pointer -> word 1, 0 data, 1 pointer
    std.mem.writeInt(u64, segment0[0..8], makeStructPointer(0, 0, 1), .little);
    // Segment 0, word 1: double-far pointer -> segment 1, word 0
    std.mem.writeInt(u64, segment0[8..16], makeFarPointer(true, 0, 1), .little);

    // Segment 1, word 0: far pointer (single) -> segment 2, word 0
    std.mem.writeInt(u64, segment1[0..8], makeFarPointer(false, 0, 2), .little);
    // Segment 1, word 1: tag = struct pointer, element_count=2, data_words=1, pointer_words=0
    // This is Layout A: second word type = 0 (struct pointer)
    std.mem.writeInt(u64, segment1[8..16], makeStructPointer(2, 1, 0), .little);

    // Segment 2, word 0: element 0 data
    std.mem.writeInt(u32, segment2[0..4], 10, .little);
    // Segment 2, word 1: element 1 data
    std.mem.writeInt(u32, segment2[8..12], 20, .little);

    // Frame the message: 3 segments
    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [16]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 2, .little); // segment_count - 1 = 2 (3 segments)
    std.mem.writeInt(u32, header[4..8], 2, .little); // segment 0: 2 words
    std.mem.writeInt(u32, header[8..12], 2, .little); // segment 1: 2 words
    std.mem.writeInt(u32, header[12..16], 2, .little); // segment 2: 2 words
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment0);
    try framed.appendSlice(allocator, &segment1);
    try framed.appendSlice(allocator, &segment2);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqual(@as(u32, 10), (try list_reader.get(0)).readU32(0));
    try testing.expectEqual(@as(u32, 20), (try list_reader.get(1)).readU32(0));
}

test "Message: double-far pointer Layout B inline-composite list (raw bytes)" {
    // Layout B: the landing pad's second word is a list pointer (type 1).
    // This is the layout used by the C++ reference implementation.
    //
    //   Segment 0 (2 words):
    //     word 0: root struct pointer -> struct at word 1 (0 data, 1 pointer)
    //     word 1: double-far pointer -> segment 1, offset 0
    //   Segment 1 (2 words = landing pad):
    //     word 0: far pointer (single) -> segment 2, offset 0
    //     word 1: list pointer (element_size=7, word_count=4) -- type=1
    //   Segment 2 (5 words = tag + list content):
    //     word 0: tag word = struct pointer (element_count=2, data_words=1, pointer_words=1)
    //     word 1: element 0 data (u32 = 55)
    //     word 2: element 0 pointer (null)
    //     word 3: element 1 data (u32 = 66)
    //     word 4: element 1 pointer (null)
    const allocator = testing.allocator;

    var segment0: [2 * 8]u8 = @splat(0);
    var segment1: [2 * 8]u8 = @splat(0);
    var segment2: [5 * 8]u8 = @splat(0);

    // Segment 0, word 0: root struct pointer -> word 1, 0 data, 1 pointer
    std.mem.writeInt(u64, segment0[0..8], makeStructPointer(0, 0, 1), .little);
    // Segment 0, word 1: double-far pointer -> segment 1, word 0
    std.mem.writeInt(u64, segment0[8..16], makeFarPointer(true, 0, 1), .little);

    // Segment 1, word 0: far pointer (single) -> segment 2, word 0
    std.mem.writeInt(u64, segment1[0..8], makeFarPointer(false, 0, 2), .little);
    // Segment 1, word 1: list pointer (element_size=7 = inline composite, word_count=4)
    // This is Layout B: second word type = 1 (list pointer)
    // The offset field in this list pointer is ignored; word_count=4.
    std.mem.writeInt(u64, segment1[8..16], makeListPointer(0, 7, 4), .little);

    // Segment 2, word 0: tag word = struct pointer, element_count=2, data_words=1, pointer_words=1
    std.mem.writeInt(u64, segment2[0..8], makeStructPointer(2, 1, 1), .little);
    // Segment 2, word 1: element 0 data
    std.mem.writeInt(u32, segment2[8..12], 55, .little);
    // Segment 2, word 2: element 0 pointer (null = 0, already zeroed)
    // Segment 2, word 3: element 1 data
    std.mem.writeInt(u32, segment2[24..28], 66, .little);
    // Segment 2, word 4: element 1 pointer (null = 0, already zeroed)

    // Frame the message: 3 segments (odd count -> padding word in header)
    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [16]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 2, .little); // segment_count - 1 = 2 (3 segments)
    std.mem.writeInt(u32, header[4..8], 2, .little); // segment 0: 2 words
    std.mem.writeInt(u32, header[8..12], 2, .little); // segment 1: 2 words
    std.mem.writeInt(u32, header[12..16], 5, .little); // segment 2: 5 words
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment0);
    try framed.appendSlice(allocator, &segment1);
    try framed.appendSlice(allocator, &segment2);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());

    const elem0 = try list_reader.get(0);
    try testing.expectEqual(@as(u32, 55), elem0.readU32(0));

    const elem1 = try list_reader.get(1);
    try testing.expectEqual(@as(u32, 66), elem1.readU32(0));
}

test "Message: double-far pointer Layout B with multi-word struct elements" {
    // Verifies Layout B with structs having 2 data words and 0 pointers.
    //
    //   Segment 0 (2 words): root struct + double-far pointer
    //   Segment 1 (2 words): landing pad (far ptr to seg 2 + list pointer)
    //   Segment 2 (5 words): tag word + 2 elements x 2 data words
    const allocator = testing.allocator;

    var segment0: [2 * 8]u8 = @splat(0);
    var segment1: [2 * 8]u8 = @splat(0);
    var segment2: [5 * 8]u8 = @splat(0);

    // Segment 0: root struct (0 data, 1 ptr) + double-far -> seg 1
    std.mem.writeInt(u64, segment0[0..8], makeStructPointer(0, 0, 1), .little);
    std.mem.writeInt(u64, segment0[8..16], makeFarPointer(true, 0, 1), .little);

    // Segment 1 landing pad: far -> seg 2 word 0, list pointer (size=7, word_count=4)
    std.mem.writeInt(u64, segment1[0..8], makeFarPointer(false, 0, 2), .little);
    std.mem.writeInt(u64, segment1[8..16], makeListPointer(0, 7, 4), .little);

    // Segment 2: tag (2 elements, 2 data words, 0 pointer words)
    std.mem.writeInt(u64, segment2[0..8], makeStructPointer(2, 2, 0), .little);
    // Element 0: two data words
    std.mem.writeInt(u32, segment2[8..12], 100, .little);
    std.mem.writeInt(u32, segment2[12..16], 1, .little);
    std.mem.writeInt(u32, segment2[16..20], 200, .little);
    // Element 1: two data words
    std.mem.writeInt(u32, segment2[24..28], 300, .little);
    std.mem.writeInt(u32, segment2[28..32], 3, .little);
    std.mem.writeInt(u32, segment2[32..36], 400, .little);

    // Frame
    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [16]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 2, .little);
    std.mem.writeInt(u32, header[4..8], 2, .little);
    std.mem.writeInt(u32, header[8..12], 2, .little);
    std.mem.writeInt(u32, header[12..16], 5, .little);
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment0);
    try framed.appendSlice(allocator, &segment1);
    try framed.appendSlice(allocator, &segment2);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());

    const elem0 = try list_reader.get(0);
    try testing.expectEqual(@as(u32, 100), elem0.readU32(0));
    try testing.expectEqual(@as(u32, 1), elem0.readU32(4));
    try testing.expectEqual(@as(u32, 200), elem0.readU32(8));

    const elem1 = try list_reader.get(1);
    try testing.expectEqual(@as(u32, 300), elem1.readU32(0));
    try testing.expectEqual(@as(u32, 3), elem1.readU32(4));
    try testing.expectEqual(@as(u32, 400), elem1.readU32(8));
}

test "Message: far pointer inline-composite list at nonzero offset in target segment" {
    // Tests the far pointer path where the landing pad is not at the start
    // of the target segment (landing_pad_offset_words > 0).
    //
    //   Segment 0 (2 words): root struct + far pointer -> segment 1 word 1
    //   Segment 1 (7 words):
    //     word 0: padding / unused
    //     word 1: list pointer (element_size=7, offset=0, word_count=4)
    //     word 2: tag (element_count=2, data_words=1, pointer_words=1)
    //     words 3-6: 2 elements x (1 data + 1 pointer)
    const allocator = testing.allocator;

    var segment0: [2 * 8]u8 = @splat(0);
    var segment1: [7 * 8]u8 = @splat(0);

    // Root struct -> word 1, 0 data, 1 pointer
    std.mem.writeInt(u64, segment0[0..8], makeStructPointer(0, 0, 1), .little);
    // Far pointer -> segment 1, word offset 1 (not 0!)
    std.mem.writeInt(u64, segment0[8..16], makeFarPointer(false, 1, 1), .little);

    // Segment 1, word 0: unused padding (0xDEADBEEF as marker)
    std.mem.writeInt(u64, segment1[0..8], 0xDEADBEEFDEADBEEF, .little);

    // Segment 1, word 1: list pointer at the landing pad position
    std.mem.writeInt(u64, segment1[8..16], makeListPointer(0, 7, 4), .little);

    // Segment 1, word 2: tag word
    std.mem.writeInt(u64, segment1[16..24], makeStructPointer(2, 1, 1), .little);

    // Element 0: data=77, pointer=null
    std.mem.writeInt(u32, segment1[24..28], 77, .little);
    // word 4: element 0 pointer (null)

    // Element 1: data=88, pointer=null
    std.mem.writeInt(u32, segment1[40..44], 88, .little);
    // word 6: element 1 pointer (null)

    // Frame
    var framed = std.ArrayList(u8).empty;
    defer framed.deinit(allocator);

    var header: [16]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 1, .little);
    std.mem.writeInt(u32, header[4..8], 2, .little);
    std.mem.writeInt(u32, header[8..12], 7, .little);
    std.mem.writeInt(u32, header[12..16], 0, .little); // padding
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment0);
    try framed.appendSlice(allocator, &segment1);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqual(@as(u32, 77), (try list_reader.get(0)).readU32(0));
    try testing.expectEqual(@as(u32, 88), (try list_reader.get(1)).readU32(0));
}

// --- 29-bit list-pointer count boundary (adversarial-input hardening) ---
//
// The Cap'n Proto list-pointer count field is only 29 bits. A count of
// `2^29` or more cannot be encoded and would be silently truncated to
// `count mod 2^29` by the builder, producing a valid-looking pointer whose
// count is wrong (silent data corruption / truncation). The builder must
// instead reject such counts. Void lists exercise the exact count boundary
// with zero content allocation, so we can test `2^29 - 1` accept vs `2^29`
// reject without allocating gigabytes.

test "MessageBuilder: void list accepts the max 29-bit element count without truncation" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    const max_count = message.MAX_LIST_ELEMENT_COUNT; // (1 << 29) - 1
    _ = try struct_builder.writeVoidList(0, max_count);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    // A Void list allocates no content words, but validation deliberately
    // charges one traversal word per element: a zero-width element is readable
    // as a struct under the list-upgrade rule, so leaving it free would let a
    // one-word pointer synthesize 2^29 elements at no cost. Raise the limit
    // past the count so this test keeps measuring what it is about — that the
    // 29-bit count survives a round trip untruncated.
    var msg = try message.Message.init(testing.allocator, bytes, .{
        .traversal_limit_words = @as(usize, max_count) + 16,
    });
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const void_list = try root.readVoidList(0);
    // The decoded count must be exactly what we wrote — not truncated.
    try testing.expectEqual(max_count, void_list.element_count);
}

test "Message: a huge void list is charged one traversal word per element" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    _ = try struct_builder.writeVoidList(0, 4096);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    // The list occupies zero content words, so under a per-element charge of
    // zero this would validate comfortably inside a 1024-word budget.
    try testing.expectError(
        error.TraversalLimitExceeded,
        message.Message.init(testing.allocator, bytes, .{ .traversal_limit_words = 1024 }),
    );

    // With room for the elements it validates normally.
    var msg = try message.Message.init(testing.allocator, bytes, .{ .traversal_limit_words = 8192 });
    defer msg.deinit();
}

test "MessageBuilder: void list rejects a count that overflows the 29-bit field" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    const over_limit: u32 = 1 << 29; // one past MAX_LIST_ELEMENT_COUNT
    try testing.expectError(error.ListTooLarge, struct_builder.writeVoidList(0, over_limit));
}

test "MessageBuilder: u8 list rejects an oversized count before allocating content" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    // A u8 list of 2^29 elements would allocate 512 MiB if the guard ran after
    // allocation; the guard fires first, so this returns without allocating.
    const over_limit: u32 = 1 << 29;
    try testing.expectError(error.ListTooLarge, struct_builder.writeU8List(0, over_limit));
}

test "MessageBuilder: writeData rejects a blob at the 29-bit list-count boundary" {
    // Sanity-check the shared guard rejects exactly at 2^29 without needing a
    // real 512 MiB slice: the constant defines the wire limit and the guard in
    // writeListPointer trips on any count > MAX_LIST_ELEMENT_COUNT.
    try testing.expectEqual(@as(u32, (1 << 29) - 1), message.MAX_LIST_ELEMENT_COUNT);

    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    // Reuse the void-list path to drive the exact boundary count cheaply; it
    // funnels through the same makeListPointer guard as writeData/writeText.
    try testing.expectError(error.ListTooLarge, struct_builder.writeVoidList(0, 1 << 29));
    // And a count exactly at the limit is accepted by the guard.
    _ = try struct_builder.writeVoidList(0, message.MAX_LIST_ELEMENT_COUNT);
}

test "MessageBuilder: inline-composite struct list rejects a body-word count over the 29-bit limit" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    const any = try builder.initRootAnyPointer();
    // Each element is 1 data word + 0 pointer words, so total_words == count.
    // A count of 2^29 makes the inline-composite list pointer's word count
    // overflow the 29-bit field; the guard must reject before allocating.
    const over_limit: u32 = 1 << 29;
    try testing.expectError(error.ListTooLarge, any.initStructList(over_limit, 1, 0));
}

test "Message.validate exposes the frozen MessageValidationError contract" {
    // The public validation error set is a frozen v0.2.0 contract. Pin it by
    // exact type equality against a locally reconstructed set: Zig error sets
    // are order-independent and interned, so this equality holds iff the
    // membership is identical — a silent widening OR narrowing breaks the test.
    // (The validator's internal call tree already returns this exact type, so a
    // newly-reachable error also fails the build until deliberately added here.)
    const Expected = error{
        EmptyMessage,
        SegmentCountLimitExceeded,
        TruncatedMessage,
        InvalidSegmentId,
        OutOfBounds,
        InvalidPointer,
        InvalidFarPointer,
        InvalidInlineCompositePointer,
        ListTooLarge,
        NestingLimitExceeded,
        TraversalLimitExceeded,
    };
    try testing.expect(message.Message.MessageValidationError == Expected);

    // validate/validateCounted return the narrowed set, not anyerror.
    const validate_err = @typeInfo(@typeInfo(@TypeOf(message.Message.validate)).@"fn".return_type.?).error_union.error_set;
    try testing.expect(validate_err == message.Message.MessageValidationError);
    const counted_err = @typeInfo(@typeInfo(@TypeOf(message.Message.validateCounted)).@"fn".return_type.?).error_union.error_set;
    try testing.expect(counted_err == message.Message.MessageValidationError);
}

// --- Struct-list "upgrade" decoding ---
//
// Cap'n Proto requires a reader to decode a list of any element size except
// C = 1 (one bit) as a struct list, synthesizing a struct whose data section is
// the element itself. That rule is what makes evolving a `List(UInt32)` field
// into a `List(SomeStruct)` backward compatible: a peer that performed the
// evolution can still read data written under the old schema, and vice versa.
//
// capnp-zig previously rejected every such list as `InvalidInlineCompositePointer`
// — i.e. reported a legal, spec-conformant message as corrupt.

test "struct-list upgrade: List(UInt32) reads as List(Struct)" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeU32List(0, 3);
    try list_builder.set(0, 0xAABBCCDD);
    try list_builder.set(1, 1);
    try list_builder.set(2, 0xFFFFFFFF);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 3), list.len());

    const expected = [_]u32{ 0xAABBCCDD, 1, 0xFFFFFFFF };
    for (expected, 0..) |want, i| {
        const element = try list.get(@intCast(i));
        try testing.expectEqual(want, element.readU32(0));
        try testing.expectEqual(@as(u16, @truncate(want)), element.readU16(0));
        // The decisive assertion: a read past the element's true 4-byte width
        // must yield the field default, NOT the next element's bytes. A naive
        // implementation that rounds the data section up to one word returns
        // `expected[i + 1]` here.
        try testing.expectEqual(@as(u32, 0), element.readU32(4));
        try testing.expectEqual(@as(u16, 0), element.pointer_count);
        try testing.expectEqual(@as(usize, 4), element.getDataSection().len);
    }
    try testing.expectError(error.IndexOutOfBounds, list.get(3));
}

test "struct-list upgrade: List(UInt8) keeps a one-byte stride" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeU8List(0, 5);
    for (0..5) |i| try list_builder.set(@intCast(i), @intCast(0x10 + i));

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const list = try (try msg.getRootStruct()).readStructList(0);
    try testing.expectEqual(@as(u32, 5), list.len());
    for (0..5) |i| {
        const element = try list.get(@intCast(i));
        // Stride is 1: element i is byte i, not byte 8*i.
        try testing.expectEqual(@as(u8, @intCast(0x10 + i)), element.readU8(0));
        try testing.expectEqual(@as(u8, 0), element.readU8(1));
        try testing.expectEqual(@as(usize, 1), element.getDataSection().len);
    }
}

test "struct-list upgrade: List(UInt16) keeps a two-byte stride" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeU16List(0, 4);
    for (0..4) |i| try list_builder.set(@intCast(i), @intCast(0x1000 + i));

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const list = try (try msg.getRootStruct()).readStructList(0);
    for (0..4) |i| {
        const element = try list.get(@intCast(i));
        try testing.expectEqual(@as(u16, @intCast(0x1000 + i)), element.readU16(0));
        // Out of a 2-byte data section: default, not the neighbour.
        try testing.expectEqual(@as(u32, 0), element.readU32(0));
        try testing.expectEqual(@as(usize, 2), element.getDataSection().len);
    }
}

test "struct-list upgrade: List(UInt64) takes the existing whole-word path" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeU64List(0, 3);
    for (0..3) |i| try list_builder.set(@intCast(i), 0xDEADBEEF00000000 + i);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const list = try (try msg.getRootStruct()).readStructList(0);
    try testing.expectEqual(@as(u16, 1), list.data_words);
    try testing.expectEqual(@as(u8, 0), list.sub_word_data_bytes);
    for (0..3) |i| {
        const element = try list.get(@intCast(i));
        try testing.expectEqual(@as(u64, 0xDEADBEEF00000000 + i), element.readU64(0));
        try testing.expectEqual(@as(usize, 8), element.getDataSection().len);
    }
}

test "struct-list upgrade: List(Void) yields zero-width structs" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    _ = try root_builder.writeVoidList(0, 7);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const list = try (try msg.getRootStruct()).readStructList(0);
    try testing.expectEqual(@as(u32, 7), list.len());
    for (0..7) |i| {
        const element = try list.get(@intCast(i));
        try testing.expectEqual(@as(u64, 0), element.readU64(0));
        try testing.expectEqual(@as(usize, 0), element.getDataSection().len);
    }
}

test "struct-list upgrade: List(pointer) yields one-pointer structs" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeTextList(0, 2);
    try list_builder.set(0, "alpha");
    try list_builder.set(1, "beta");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const list = try (try msg.getRootStruct()).readStructList(0);
    try testing.expectEqual(@as(u16, 0), list.data_words);
    try testing.expectEqual(@as(u16, 1), list.pointer_words);
    // Proves the synthesized pointer section is anchored at the element start.
    try testing.expectEqualStrings("alpha", try (try list.get(0)).readText(0));
    try testing.expectEqualStrings("beta", try (try list.get(1)).readText(0));
}

test "struct-list upgrade: List(Bool) is the one element size that cannot upgrade" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeBoolList(0, 8);
    try list_builder.set(0, true);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    // A distinct error, not `InvalidInlineCompositePointer`: this is a schema
    // mismatch (or a peer that violated the spec), not a corrupt message.
    try testing.expectError(error.CannotUpgradeBitList, root.readStructList(0));
}

test "struct-list upgrade: through a nested pointer list" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var outer = try root_builder.writePointerList(0, 1);
    var inner = try outer.initU32List(0, 2);
    try inner.set(0, 111);
    try inner.set(1, 222);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    // Covers PointerListReader.getStructList independently of readStructList.
    const outer_reader = try (try msg.getRootStruct()).readPointerList(0);
    const list = try outer_reader.getStructList(0);
    try testing.expectEqual(@as(u32, 2), list.len());
    try testing.expectEqual(@as(u32, 111), (try list.get(0)).readU32(0));
    try testing.expectEqual(@as(u32, 222), (try list.get(1)).readU32(0));
    try testing.expectEqual(@as(u32, 0), (try list.get(0)).readU32(4));
}

test "struct-list upgrade: inline-composite lists still decode unchanged" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 2, 1, 0);
    var first = try list_builder.get(0);
    first.writeU32(0, 7);
    var second = try list_builder.get(1);
    second.writeU32(0, 9);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const list = try (try msg.getRootStruct()).readStructList(0);
    // Regression fence: the native encoding must not acquire sub-word state.
    try testing.expectEqual(@as(u8, 0), list.sub_word_data_bytes);
    try testing.expectEqual(@as(u16, 1), list.data_words);
    try testing.expectEqual(@as(u32, 7), (try list.get(0)).readU32(0));
    try testing.expectEqual(@as(u32, 9), (try list.get(1)).readU32(0));
}

test "struct-list upgrade: a double-far inline-composite list still decodes" {
    // The layout-A regression. For a double-far struct list this builder stores
    // the struct *tag* as the landing pad's second word, and a tag word has
    // pointer type 0. An element-size probe built on `resolvePointer` sees that
    // type-0 word, concludes "not a list", and sends a perfectly valid struct
    // list down the upgrade path. This is the fixture that goes red for it —
    // an upgraded (non-composite) far list would not, since its pad's second
    // word is an ordinary list pointer.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    const landing_segment = try builder.createSegment();
    const content_segment = try builder.createSegment();
    var list_builder = try root_builder.writeStructListInSegments(0, 2, 1, 0, landing_segment, content_segment);
    var first = try list_builder.get(0);
    first.writeU32(0, 4242);
    var second = try list_builder.get(1);
    second.writeU32(0, 2424);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const list = try (try msg.getRootStruct()).readStructList(0);
    try testing.expectEqual(@as(u32, 2), list.len());
    try testing.expectEqual(@as(u8, 0), list.sub_word_data_bytes);
    try testing.expectEqual(@as(u32, 4242), (try list.get(0)).readU32(0));
    try testing.expectEqual(@as(u32, 2424), (try list.get(1)).readU32(0));
}

test "struct-list upgrade: a corrupt inline-composite tag is still rejected" {
    // The peek must not turn corruption into an upgrade attempt: a list pointer
    // that genuinely claims C = 7 keeps its strict tag validation.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    _ = try root_builder.writeStructList(0, 2, 1, 0);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    const mutated = try testing.allocator.dupe(u8, bytes);
    defer testing.allocator.free(mutated);

    // Find the C = 7 list pointer and corrupt the tag word that follows it by
    // giving the tag a non-zero pointer type.
    var word_index: usize = 0;
    var patched = false;
    while (word_index + 1 < mutated.len / 8) : (word_index += 1) {
        const word = std.mem.readInt(u64, mutated[word_index * 8 ..][0..8], .little);
        if (@as(u2, @truncate(word & 0x3)) != 1) continue;
        if (@as(u3, @truncate((word >> 32) & 0x7)) != 7) continue;
        const tag_index = word_index + 1 + @as(usize, @intCast(@max(@as(i32, 0), @as(i32, @truncate(@as(i64, @bitCast(word)) >> 2)))));
        if (tag_index >= mutated.len / 8) continue;
        const tag = std.mem.readInt(u64, mutated[tag_index * 8 ..][0..8], .little);
        std.mem.writeInt(u64, mutated[tag_index * 8 ..][0..8], tag | 0x1, .little);
        patched = true;
        break;
    }
    try testing.expect(patched);

    try testing.expectError(
        error.InvalidInlineCompositePointer,
        message.Message.init(testing.allocator, mutated, .{}),
    );
}

// --- Struct-list "downgrade" decoding (the inverse rule) ---
//
// The other half of the same compatibility guarantee. Once a peer has evolved a
// `List(UInt32)` field into a `List(SomeStruct)` it writes a correctly encoded
// struct list (element size C = 7), and a binary still running the old schema
// has to read it back as `List(UInt32)`. Both reference implementations accept
// that, so rejecting it means rejecting messages every other implementation
// reads.
//
// The decode must go through the inline-composite resolver. A C = 7 list
// pointer's D field is a WORD count, not an element count, and its content
// offset addresses the TAG word rather than the elements — so a reader built
// from the plain list resolution has the wrong length AND a base one word too
// early, and returns garbage without erroring.

test "struct-list downgrade: an inline-composite list reads back as List(UInt32)" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 3, 1, 0);
    const expected = [_]u32{ 0xAABBCCDD, 1, 0xFFFFFFFF };
    for (expected, 0..) |value, i| {
        var element = try list_builder.get(@intCast(i));
        element.writeU32(0, value);
    }

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list = try root.readU32List(0);
    // Three elements, not the six-word content size the pointer's D field
    // carries for an inline-composite list.
    try testing.expectEqual(@as(u32, 3), list.len());
    // Elements are separated by the whole struct, not by the four bytes being
    // read out of each one.
    try testing.expectEqual(@as(u32, 8), list.stride_bytes);
    for (expected, 0..) |want, i| {
        try testing.expectEqual(want, try list.get(@intCast(i)));
    }
    try testing.expectError(error.IndexOutOfBounds, list.get(3));

    // `castListReader` copies an already-resolved reader field by field; the
    // signed and float views must see the same stride, not the natural one.
    const signed = try root.readI32List(0);
    try testing.expectEqual(@as(u32, 8), signed.stride_bytes);
    for (expected, 0..) |want, i| {
        try testing.expectEqual(@as(i32, @bitCast(want)), try signed.get(@intCast(i)));
    }
    const floats = try root.readF32List(0);
    try testing.expectEqual(@as(f32, @bitCast(expected[0])), try floats.get(0));
}

test "struct-list downgrade: multi-word elements stride by the whole struct" {
    // Two data words per element, so the word count (6) and the element count
    // (3) differ: a reader that resolves this as a plain list pointer reports
    // six elements and starts one word early, at the tag.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 3, 2, 0);
    const expected = [_]u32{ 0x11111111, 0x22222222, 0x33333333 };
    for (expected, 0..) |value, i| {
        var element = try list_builder.get(@intCast(i));
        element.writeU32(0, value);
        // The element's second data word. Nothing may ever surface it as an
        // element of the downgraded list.
        element.writeU32(8, 0xDEADBEEF);
    }

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const list = try (try msg.getRootStruct()).readU32List(0);
    try testing.expectEqual(@as(u32, 3), list.len());
    try testing.expectEqual(@as(u32, 16), list.stride_bytes);
    for (expected, 0..) |want, i| {
        try testing.expectEqual(want, try list.get(@intCast(i)));
    }
}

test "struct-list downgrade: a double-far struct list reads back as List(UInt32)" {
    // The reason the element-size test is a dedicated probe and not the plain
    // list resolution: for a double-far struct list this builder stores the
    // struct *tag* as the landing pad's second word, and a tag word has pointer
    // type 0 — so `resolveListPointer` reports "not a list" for a message that
    // is entirely well formed.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    const landing_segment = try builder.createSegment();
    const content_segment = try builder.createSegment();
    var list_builder = try root_builder.writeStructListInSegments(0, 2, 1, 0, landing_segment, content_segment);
    var first = try list_builder.get(0);
    first.writeU32(0, 4242);
    var second = try list_builder.get(1);
    second.writeU32(0, 2424);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const list = try (try msg.getRootStruct()).readU32List(0);
    try testing.expectEqual(@as(u32, 2), list.len());
    try testing.expectEqual(@as(u32, 4242), try list.get(0));
    try testing.expectEqual(@as(u32, 2424), try list.get(1));
}

test "struct-list downgrade: the same list reads back at every primitive width" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 3, 1, 0);
    for (0..3) |i| {
        var element = try list_builder.get(@intCast(i));
        element.writeU64(0, 0x8899AABBCCDDEE00 + @as(u64, i));
    }

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const wide = try root.readU64List(0);
    const half = try root.readU16List(0);
    const bytes_list = try root.readU8List(0);
    for (0..3) |i| {
        try testing.expectEqual(@as(u64, 0x8899AABBCCDDEE00 + @as(u64, i)), try wide.get(@intCast(i)));
        // The low half-word / low byte of each element, one struct apart.
        try testing.expectEqual(@as(u16, @truncate(0xEE00 + i)), try half.get(@intCast(i)));
        try testing.expectEqual(@as(u8, @truncate(i)), try bytes_list.get(@intCast(i)));
    }

    // A downgraded `List(UInt8)` has no contiguous representation — its bytes
    // are separated by the rest of each struct — so the slice escape hatch has
    // to refuse rather than hand back interleaved struct bytes.
    try testing.expectError(error.InvalidPointer, bytes_list.slice());
}

test "struct-list downgrade: a struct list reads back as List(Text)" {
    // The pointer arm. An element's pointer section starts after its data
    // section, so the text pointer for element i is at `i * struct_size +
    // data_bytes` — the offset go-capnp's TextList.At omits (while its own
    // PointerList.At applies it). Verified against the C++ reference only.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 2, 1, 1);
    var first = try list_builder.get(0);
    // Data the reader must skip over to find the pointer section.
    first.writeU64(0, 0xFFFFFFFFFFFFFFFF);
    try first.writeText(0, "alpha");
    var second = try list_builder.get(1);
    second.writeU64(0, 0xFFFFFFFFFFFFFFFF);
    try second.writeText(0, "beta");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const texts = try root.readTextList(0);
    try testing.expectEqual(@as(u32, 2), texts.len());
    try testing.expectEqual(@as(u32, 16), texts.stride_bytes);
    try testing.expectEqualStrings("alpha", try texts.get(0));
    try testing.expectEqualStrings("beta", try texts.get(1));

    // Same wire bytes through the type-erased pointer list.
    const pointers = try root.readPointerList(0);
    try testing.expectEqualStrings("alpha", try pointers.getText(0));
    try testing.expectEqualStrings("beta", try pointers.getText(1));
}

test "struct-list downgrade: a pointer-only struct list is not a primitive list" {
    // The precondition that stops a struct list with no data section at all
    // from synthesizing primitive elements for free — and, just as important,
    // from returning pointer words as if they were data.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 3, 0, 1);
    for (0..3) |i| {
        var element = try list_builder.get(@intCast(i));
        try element.writeText(0, "pointer-only");
    }

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectError(error.InvalidPointer, root.readU8List(0));
    try testing.expectError(error.InvalidPointer, root.readU16List(0));
    try testing.expectError(error.InvalidPointer, root.readU32List(0));
    try testing.expectError(error.InvalidPointer, root.readU64List(0));
    // ...but the pointer arm of the same list is legitimate.
    try testing.expectEqualStrings("pointer-only", try (try root.readTextList(0)).get(2));
}

test "struct-list downgrade: a data-only struct list is not a pointer list" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 2, 1, 0);
    var first = try list_builder.get(0);
    first.writeU64(0, 0x1122334455667788);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    // Without the pointer-count precondition these would hand a data word to
    // the pointer decoder.
    try testing.expectError(error.InvalidPointer, root.readTextList(0));
    try testing.expectError(error.InvalidPointer, root.readPointerList(0));
}

test "struct-list downgrade: Bool, Text and Data stay strict" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 2, 1, 0);
    var first = try list_builder.get(0);
    first.writeU64(0, 0x1122334455667788);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    // C++ hard-fails composite-as-bit, and requires ElementSize::BYTE for both
    // blob types. Relaxing any of these would diverge from the reference, not
    // converge with it.
    try testing.expectError(error.InvalidPointer, root.readBoolList(0));
    try testing.expectError(error.InvalidPointer, root.readData(0));
    try testing.expectError(error.InvalidTextPointer, root.readText(0));
}

// ---------------------------------------------------------------------------
// Inverse list-upgrade rule: the NESTED and TYPE-ERASED readers
// ---------------------------------------------------------------------------
//
// The same decode, one level down. `PointerListReader.getU32List` reaches the
// element of a `List(List(UInt32))`, and `AnyPointerReader.getPointerList` the
// type-erased equivalent; both used to reject a struct list where a struct's
// own field accepted it. All three surfaces now share one resolver
// (`message/element_list.zig`), which is the point: a downgrade implemented
// per-surface is how one of them ends up silently wrong.
//
// No builder API produces a nested list-of-lists fixture directly, so these
// synthesize the outer `PointerListReader` over the root struct's own pointer
// section. The root struct has no data words, so its first pointer sits at
// segment offset 8. The `getStructList` assertion in the first test is what
// proves that offset addresses the list pointer and not some other word.

test "struct-list downgrade: a nested List(List(UInt32)) element reads a struct list" {
    // Two data words per element, so the pointer's word count (6) and the real
    // element count (3) differ: a reader resolved through the plain list
    // pointer would report six elements and start one word early, at the tag.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 3, 2, 0);
    const expected = [_]u32{ 0x11111111, 0x22222222, 0x33333333 };
    for (expected, 0..) |value, i| {
        var element = try list_builder.get(@intCast(i));
        element.writeU32(0, value);
        // The element's second data word. Nothing may ever surface it as an
        // element of the downgraded list.
        element.writeU32(8, 0xDEADBEEF);
    }

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const nested = message.PointerListReader{
        .message = &msg,
        .segment_id = 0,
        .elements_offset = 8,
        .element_count = 1,
    };

    const list = try nested.getU32List(0);
    // Three elements, not the six-word content size the pointer's D field
    // carries for an inline-composite list.
    try testing.expectEqual(@as(u32, 3), list.len());
    // Values before metadata, so a stride regression surfaces as wrong data
    // from element 1 onward rather than only as a wrong `stride_bytes`.
    for (expected, 0..) |want, i| {
        try testing.expectEqual(want, try list.get(@intCast(i)));
    }
    // Elements are separated by the whole struct, not by the four bytes being
    // read out of each one.
    try testing.expectEqual(@as(u32, 16), list.stride_bytes);
    try testing.expectError(error.IndexOutOfBounds, list.get(3));

    // Every width sees the same stride, including the signed view.
    const signed = try nested.getI32List(0);
    try testing.expectEqual(@as(u32, 16), signed.stride_bytes);
    try testing.expectEqual(@as(i32, 0x22222222), try signed.get(1));
    try testing.expectEqual(@as(u64, 0x11111111), try (try nested.getU64List(0)).get(0));
    try testing.expectEqual(@as(u16, 0x2222), try (try nested.getU16List(0)).get(1));
    try testing.expectEqual(@as(u8, 0x33), try (try nested.getU8List(0)).get(2));

    // The forward direction of the very same pointer is unchanged — and this
    // is what proves offset 8 addresses the list pointer.
    try testing.expectEqual(@as(u32, 0x22222222), (try (try nested.getStructList(0)).get(1)).readU32(0));
}

test "struct-list downgrade: a nested pointer list reads a struct list" {
    // The pointer arm one level down. Each element's data word is all-ones, so
    // a reader that failed to skip the data section would try to decode
    // 0xFFFF... as a pointer rather than quietly returning the wrong text.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 2, 1, 1);
    var first = try list_builder.get(0);
    first.writeU64(0, 0xFFFFFFFFFFFFFFFF);
    try first.writeText(0, "alpha");
    var second = try list_builder.get(1);
    second.writeU64(0, 0xFFFFFFFFFFFFFFFF);
    try second.writeText(0, "beta");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const nested = message.PointerListReader{
        .message = &msg,
        .segment_id = 0,
        .elements_offset = 8,
        .element_count = 1,
    };

    const pointers = try nested.getPointerList(0);
    try testing.expectEqual(@as(u32, 2), pointers.len());
    try testing.expectEqualStrings("alpha", try pointers.getText(0));
    try testing.expectEqualStrings("beta", try pointers.getText(1));
    try testing.expectEqual(@as(u32, 16), pointers.stride_bytes);
}

test "struct-list downgrade: the type-erased pointer list reads a struct list" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 2, 1, 1);
    var first = try list_builder.get(0);
    first.writeU64(0, 0xFFFFFFFFFFFFFFFF);
    try first.writeText(0, "alpha");
    var second = try list_builder.get(1);
    second.writeU64(0, 0xFFFFFFFFFFFFFFFF);
    try second.writeText(0, "beta");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const any = try (try msg.getRootStruct()).readAnyPointer(0);

    // `getList` is the raw resolution and still reports the pointer verbatim:
    // element size 7, and a D field of 4 that is a WORD count, not the two
    // elements actually present. Reading that as the list is precisely the
    // silent-wrong-data failure the downgrade must not have.
    const raw = try any.getList();
    try testing.expectEqual(@as(u3, 7), raw.element_size);
    try testing.expectEqual(@as(u32, 4), raw.element_count);

    const pointers = try any.getPointerList();
    try testing.expectEqual(@as(u32, 2), pointers.len());
    try testing.expectEqualStrings("alpha", try pointers.getText(0));
    try testing.expectEqualStrings("beta", try pointers.getText(1));
    try testing.expectEqual(@as(u32, 16), pointers.stride_bytes);
}

test "struct-list downgrade: a nested/type-erased pointer-only list is not a primitive list" {
    // The same precondition as on a struct's own fields: without the data-words
    // check a struct list carrying no data at all would synthesize primitive
    // elements for free, and hand back pointer words as if they were data.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 3, 0, 1);
    for (0..3) |i| {
        var element = try list_builder.get(@intCast(i));
        try element.writeText(0, "pointer-only");
    }

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const nested = message.PointerListReader{
        .message = &msg,
        .segment_id = 0,
        .elements_offset = 8,
        .element_count = 1,
    };

    try testing.expectError(error.InvalidPointer, nested.getU8List(0));
    try testing.expectError(error.InvalidPointer, nested.getU16List(0));
    try testing.expectError(error.InvalidPointer, nested.getU32List(0));
    try testing.expectError(error.InvalidPointer, nested.getU64List(0));

    // ...but the pointer arm of the same list is legitimate, on both surfaces.
    try testing.expectEqualStrings("pointer-only", try (try nested.getPointerList(0)).getText(2));
    const any = try (try msg.getRootStruct()).readAnyPointer(0);
    try testing.expectEqualStrings("pointer-only", try (try any.getPointerList()).getText(2));
}

test "struct-list downgrade: nested Bool and Data stay strict, data-only is not a pointer list" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 2, 1, 0);
    var first = try list_builder.get(0);
    first.writeU64(0, 0x1122334455667788);

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const nested = message.PointerListReader{
        .message = &msg,
        .segment_id = 0,
        .elements_offset = 8,
        .element_count = 1,
    };
    const any = try (try msg.getRootStruct()).readAnyPointer(0);

    // Without the pointer-count precondition these would hand a data word to
    // the pointer decoder.
    try testing.expectError(error.InvalidPointer, nested.getPointerList(0));
    try testing.expectError(error.InvalidPointer, any.getPointerList());

    // C++ hard-fails composite-as-bit and requires ElementSize::BYTE for both
    // blob types. Relaxing any of these would diverge from the reference, not
    // converge with it — one level down exactly as on a struct's own fields.
    try testing.expectError(error.InvalidPointer, nested.getBoolList(0));
    try testing.expectError(error.InvalidPointer, nested.getData(0));
    try testing.expectError(error.InvalidTextPointer, nested.getText(0));
    try testing.expectError(error.InvalidPointer, any.getData());
    try testing.expectError(error.InvalidTextPointer, any.getText());
}

test "struct-list downgrade: a struct list reads as List(Void) with the tag's element count" {
    // The last arm of the inverse list-upgrade rule. C++ accepts
    // `ElementSize::VOID` against an inline-composite list — the `case
    // ElementSize::VOID: break;` arm of readListPointer's struct-list check
    // (layout.c++) has no precondition, because a void element needs zero data
    // bits and zero pointers. The element count MUST come from the TAG word:
    // the pointer's D field is a WORD count for C = 7, so a resolver that goes
    // through the plain list path reports a three-element two-words-per-element
    // list as having six elements — silent wrong data, not an error.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var list_builder = try root_builder.writeStructList(0, 3, 2, 0);
    for (0..3) |i| {
        var element = try list_builder.get(@intCast(i));
        element.writeU32(0, @intCast(i + 1));
    }

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const void_list = try root.readVoidList(0);
    // Three elements — the tag's count, not the pointer's six-word D field.
    try testing.expectEqual(@as(u32, 3), void_list.len());
    try void_list.get(2);
    try testing.expectError(error.IndexOutOfBounds, void_list.get(3));
}

test "list upgrade: any element size reads as List(Void)" {
    // The ordinary-list half of the same C++ rule: "verify that the elements
    // are at least as large as the expected type" is vacuous for void (zero
    // data bits, zero pointers), so EVERY well-formed list satisfies a
    // List(Void) read, at its own element count. This is the forward
    // list-upgrade direction (old List(Void) schema, peer evolved the field),
    // and the count must be the actual element count for every width.
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 2);
    var u32_list = try root_builder.writeU32List(0, 4);
    for (0..4) |i| try u32_list.set(@intCast(i), @intCast(i));
    try root_builder.writeText(1, "seven");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 4), (try root.readVoidList(0)).len());
    // Text is a byte list on the wire (six bytes with the NUL); void reads it
    // at its own count exactly as C++ does.
    try testing.expectEqual(@as(u32, 6), (try root.readVoidList(1)).len());

    // Unchanged strictness elsewhere: null is still an error, and the same
    // pointer still reads as its own type.
    try testing.expectEqual(@as(u32, 2), (try root.readU32List(0)).get(2) catch unreachable);
}

test "list upgrade/downgrade: both directions of the same field agree" {
    // The forward direction (a primitive list decoded as a struct list) must
    // keep working unchanged, and the two directions must agree on the values.
    var primitive_builder = message.MessageBuilder.init(testing.allocator);
    defer primitive_builder.deinit();

    const expected = [_]u32{ 100, 200, 300 };
    var primitive_root = try primitive_builder.allocateStruct(0, 1);
    var primitive_list = try primitive_root.writeU32List(0, 3);
    for (expected, 0..) |value, i| try primitive_list.set(@intCast(i), value);

    const primitive_bytes = try primitive_builder.toBytes();
    defer testing.allocator.free(primitive_bytes);

    var primitive_msg = try message.Message.init(testing.allocator, primitive_bytes, .{});
    defer primitive_msg.deinit();
    const primitive_root_reader = try primitive_msg.getRootStruct();

    // Read as written: the natural, tightly packed stride.
    const as_written = try primitive_root_reader.readU32List(0);
    try testing.expectEqual(@as(u32, 0), as_written.stride_bytes);
    // ...and as the evolved schema sees it (the 3b9a91b upgrade rule).
    const as_structs = try primitive_root_reader.readStructList(0);
    try testing.expectEqual(@as(u32, 3), as_structs.len());
    try testing.expectEqual(@as(u8, 4), as_structs.sub_word_data_bytes);

    // The same field written by the evolved peer, read by the old binary.
    var struct_builder_msg = message.MessageBuilder.init(testing.allocator);
    defer struct_builder_msg.deinit();
    var struct_root = try struct_builder_msg.allocateStruct(0, 1);
    var struct_list = try struct_root.writeStructList(0, 3, 1, 0);
    for (expected, 0..) |value, i| {
        var element = try struct_list.get(@intCast(i));
        element.writeU32(0, value);
    }

    const struct_bytes = try struct_builder_msg.toBytes();
    defer testing.allocator.free(struct_bytes);

    var struct_msg = try message.Message.init(testing.allocator, struct_bytes, .{});
    defer struct_msg.deinit();
    const struct_root_reader = try struct_msg.getRootStruct();
    const as_primitives = try struct_root_reader.readU32List(0);
    const still_structs = try struct_root_reader.readStructList(0);

    for (expected, 0..) |want, i| {
        const index: u32 = @intCast(i);
        try testing.expectEqual(want, try as_written.get(index));
        try testing.expectEqual(want, (try as_structs.get(index)).readU32(0));
        try testing.expectEqual(want, try as_primitives.get(index));
        try testing.expectEqual(want, (try still_structs.get(index)).readU32(0));
    }
}

// ---------------------------------------------------------------------------
// MessageBuilder.writeTo / writePackedTo.
//
// These two frozen Stable functions had ZERO call sites anywhere in the tree.
// While they took `writer: anytype` that meant their bodies were never
// semantically analyzed and their behaviour was never exercised: an
// uninstantiated generic body is not type-checked. The parameter is now a
// concrete `*std.Io.Writer`, and these tests instantiate and execute both.
// ---------------------------------------------------------------------------

test "MessageBuilder.writeTo streams the same framing as toBytes and round-trips" {
    const allocator = testing.allocator;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(1, 2);
    root.writeU64(0, 0xdead_beef_0bad_f00d);
    try root.writeText(0, "writeTo round-trip");

    // A second segment forces the multi-entry segment table AND the 4-byte
    // padding word that only appears for an even segment count.
    const far_segment = try builder.createSegment();
    try root.writeTextInSegment(1, "second segment", far_segment);

    const expected = try builder.toBytes();
    defer allocator.free(expected);

    var sink = std.Io.Writer.Allocating.init(allocator);
    defer sink.deinit();
    try builder.writeTo(&sink.writer);
    try sink.writer.flush();

    // Byte-for-byte identical to the allocating serializer.
    try testing.expectEqualSlices(u8, expected, sink.written());

    // ...and the streamed bytes are a message a reader accepts.
    var msg = try message.Message.init(allocator, sink.written(), .{});
    defer msg.deinit();
    const reader = try msg.getRootStruct();
    try testing.expectEqual(@as(u64, 0xdead_beef_0bad_f00d), reader.readU64(0));
    try testing.expectEqualStrings("writeTo round-trip", try reader.readText(0));
    try testing.expectEqualStrings("second segment", try reader.readText(1));
}

test "MessageBuilder.writePackedTo streams the same framing as toPackedBytes and round-trips" {
    const allocator = testing.allocator;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(1, 1);
    root.writeU32(0, 0x0000_2a2a);
    try root.writeText(0, "writePackedTo round-trip");

    const expected = try builder.toPackedBytes();
    defer allocator.free(expected);

    var sink = std.Io.Writer.Allocating.init(allocator);
    defer sink.deinit();
    try builder.writePackedTo(&sink.writer);
    try sink.writer.flush();

    try testing.expectEqualSlices(u8, expected, sink.written());

    var msg = try message.Message.initPacked(allocator, sink.written(), .{});
    defer msg.deinit();
    const reader = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 0x0000_2a2a), reader.readU32(0));
    try testing.expectEqualStrings("writePackedTo round-trip", try reader.readText(0));
}

test "MessageBuilder.writeTo emits a well-formed frame for a builder with no segments" {
    const allocator = testing.allocator;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var sink = std.Io.Writer.Allocating.init(allocator);
    defer sink.deinit();
    try builder.writeTo(&sink.writer);
    try sink.writer.flush();

    // segment_count - 1 = 0, then a single zero-word segment size. No padding
    // word: the count is odd.
    try testing.expectEqualSlices(u8, &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 }, sink.written());
}
