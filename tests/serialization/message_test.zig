const std = @import("std");
const testing = std.testing;
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

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
    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u8, 255), root.readU8(0));
    try testing.expectEqual(@as(u16, 65535), root.readU16(2));
    try testing.expectEqual(@as(u32, 4294967295), root.readU32(4));
    try testing.expectEqual(@as(u64, 18446744073709551615), root.readU64(8));
}

test "MessageBuilder and Message: text field" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    try struct_builder.writeText(0, "Hello, Cap'n Proto!");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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
        const result = message.Message.init(allocator, truncated);
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

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    try msg.validate(.{});
    try testing.expectError(error.TraversalLimitExceeded, msg.validate(.{ .traversal_limit_words = 1, .nesting_limit = 64 }));
    try testing.expectError(error.NestingLimitExceeded, msg.validate(.{ .nesting_limit = 0 }));
}

test "Message: segment count decode limit is enforced" {
    // segment_count_minus_one = 512 => 513 segments, beyond Message.max_segment_count (512)
    const bytes = [_]u8{ 0x00, 0x02, 0x00, 0x00 };
    try testing.expectError(error.SegmentCountLimitExceeded, message.Message.init(testing.allocator, &bytes));
}

test "Message: validate enforces segment count limit option" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    _ = try builder.allocateStruct(0, 0);
    _ = try builder.createSegment();

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var segment = [_]u8{0} ** (3 * 8);

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

    var framed = std.ArrayList(u8){};
    defer framed.deinit(allocator);

    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 0, .little); // segment count - 1
    std.mem.writeInt(u32, header[4..8], 3, .little); // segment size in words
    try framed.appendSlice(allocator, &header);
    try framed.appendSlice(allocator, &segment);

    const bytes = try framed.toOwnedSlice(allocator);
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqualStrings("hi", try root.readText(0));
}

test "Message: far pointer root struct in another segment" {
    const allocator = testing.allocator;

    var segment0 = [_]u8{0} ** (1 * 8);
    var segment1 = [_]u8{0} ** (2 * 8);

    const far_ptr = makeFarPointer(false, 0, 1);
    std.mem.writeInt(u64, segment0[0..8], far_ptr, .little);

    const struct_ptr = makeStructPointer(0, 1, 0);
    std.mem.writeInt(u64, segment1[0..8], struct_ptr, .little);
    std.mem.writeInt(u32, segment1[8..12], 123, .little);

    var framed = std.ArrayList(u8){};
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

    var msg = try message.Message.init(allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 123), root.readU32(0));
}

test "Message: far pointer list in another segment" {
    const allocator = testing.allocator;

    var segment0 = [_]u8{0} ** (2 * 8);
    var segment1 = [_]u8{0} ** (2 * 8);

    const root_ptr = makeStructPointer(0, 0, 1);
    std.mem.writeInt(u64, segment0[0..8], root_ptr, .little);

    const far_ptr = makeFarPointer(false, 0, 1);
    std.mem.writeInt(u64, segment0[8..16], far_ptr, .little);

    const list_ptr = makeListPointer(0, 2, 3);
    std.mem.writeInt(u64, segment1[0..8], list_ptr, .little);
    segment1[8] = 'h';
    segment1[9] = 'i';
    segment1[10] = 0;

    var framed = std.ArrayList(u8){};
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

    var msg = try message.Message.init(allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqualStrings("hi", try root.readText(0));
}

test "Message: double-far pointer root struct" {
    const allocator = testing.allocator;

    var segment0 = [_]u8{0} ** (1 * 8);
    var segment1 = [_]u8{0} ** (3 * 8);

    const far_ptr = makeFarPointer(true, 0, 1);
    std.mem.writeInt(u64, segment0[0..8], far_ptr, .little);

    const content_far = makeFarPointer(false, 2, 1);
    std.mem.writeInt(u64, segment1[0..8], content_far, .little);

    const tag = makeStructPointer(0, 1, 0);
    std.mem.writeInt(u64, segment1[8..16], tag, .little);

    std.mem.writeInt(u32, segment1[16..20], 77, .little);

    var framed = std.ArrayList(u8){};
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

    var msg = try message.Message.init(allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var src_msg = try message.Message.init(testing.allocator, src_bytes);
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

    var dest_msg = try message.Message.init(testing.allocator, dest_bytes);
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

    var src_msg = try message.Message.init(testing.allocator, src_bytes);
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

    var dest_msg = try message.Message.init(testing.allocator, dest_bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readPointerList(0);
    const cap0 = try list_reader.getCapability(0);
    const cap1 = try list_reader.getCapability(1);
    try testing.expectEqual(@as(u32, 1), cap0.id);
    try testing.expectEqual(@as(u32, 7), cap1.id);
}

test "MessageBuilder: packed bytes roundtrip" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(1, 1);
    root_builder.writeU32(0, 4242);
    try root_builder.writeText(0, "packed");

    const packed_bytes = try builder.toPackedBytes();
    defer testing.allocator.free(packed_bytes);

    var msg = try message.Message.initPacked(testing.allocator, packed_bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u32, 4242), root.readU32(0));
    try testing.expectEqualStrings("packed", try root.readText(0));
}

test "AnyPointer: set and read text" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_builder = try builder.allocateStruct(0, 1);
    var any = try root_builder.getAnyPointer(0);
    try any.setText("any");

    const bytes = try builder.toBytes();
    defer testing.allocator.free(bytes);

    var msg = try message.Message.init(testing.allocator, bytes);
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
    try testing.expectError(error.InvalidSegmentCount, message.Message.init(testing.allocator, &bytes));
}

test "MessageBuilder: struct list rejects oversized element count" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(0, 1);
    const too_many: u32 = @as(u32, @intCast(std.math.maxInt(i32))) + 1;
    try testing.expectError(error.ElementCountTooLarge, root.writeStructList(0, too_many, 1, 0));
}

test "Message: invalid double-far landing pointer reports InvalidFarPointer" {
    var bytes: [40]u8 = [_]u8{0} ** 40;
    std.mem.writeInt(u32, bytes[0..4], 1, .little); // 2 segments total
    std.mem.writeInt(u32, bytes[4..8], 1, .little); // segment 0: root pointer word
    std.mem.writeInt(u32, bytes[8..12], 2, .little); // segment 1: double-far landing pad (2 words)
    // bytes[12..16] padding word left as zero

    const root_double_far: u64 = 2 | (@as(u64, 1) << 2) | (@as(u64, 1) << 32);
    std.mem.writeInt(u64, bytes[16..24], root_double_far, .little);

    // landing first word intentionally not a far pointer -> InvalidFarPointer
    std.mem.writeInt(u64, bytes[24..32], 0, .little);
    std.mem.writeInt(u64, bytes[32..40], 0, .little);

    var msg = try message.Message.init(testing.allocator, &bytes);
    defer msg.deinit();

    try testing.expectError(error.InvalidFarPointer, msg.getRootStruct());
}

test "Message: inline composite overflow in expected words is rejected" {
    var bytes: [32]u8 = [_]u8{0} ** 32;
    std.mem.writeInt(u32, bytes[0..4], 0, .little); // 1 segment
    std.mem.writeInt(u32, bytes[4..8], 3, .little); // 3 words

    // Root pointer: inline composite list with 1 word payload.
    const root_pointer: u64 = 1 | (@as(u64, 7) << 32) | (@as(u64, 1) << 35);
    std.mem.writeInt(u64, bytes[8..16], root_pointer, .little);

    // Tag word: element_count=65536, data_words=1, pointer_words=65535.
    // This overflows u32 multiplication if arithmetic is unchecked.
    const tag_word: u64 = (@as(u64, 65_536) << 2) | (@as(u64, 1) << 32) | (@as(u64, 65_535) << 48);
    std.mem.writeInt(u64, bytes[16..24], tag_word, .little);

    var msg = try message.Message.init(testing.allocator, &bytes);
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

    var msg = try message.Message.init(testing.allocator, mutated);
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

        var msg = message.Message.init(testing.allocator, bytes) catch continue;
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

        var msg = message.Message.initPacked(testing.allocator, bytes) catch continue;
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
        var msg = message.Message.initPacked(testing.allocator, packed_bytes) catch continue;
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const text = try root.readTextStrict(0);
    try testing.expectEqualStrings("", text);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, mutated);
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

    var msg = try message.Message.init(testing.allocator, bytes);
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

    var msg = try message.Message.init(testing.allocator, mutated);
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

    var segment0 = [_]u8{0} ** (2 * 8);
    var segment1 = [_]u8{0} ** (6 * 8);

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
    var framed = std.ArrayList(u8){};
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

    var msg = try message.Message.init(allocator, bytes);
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

    var segment0 = [_]u8{0} ** (2 * 8);
    var segment1 = [_]u8{0} ** (2 * 8);
    var segment2 = [_]u8{0} ** (2 * 8);

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
    var framed = std.ArrayList(u8){};
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

    var msg = try message.Message.init(allocator, bytes);
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

    var segment0 = [_]u8{0} ** (2 * 8);
    var segment1 = [_]u8{0} ** (2 * 8);
    var segment2 = [_]u8{0} ** (5 * 8);

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
    var framed = std.ArrayList(u8){};
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

    var msg = try message.Message.init(allocator, bytes);
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

    var segment0 = [_]u8{0} ** (2 * 8);
    var segment1 = [_]u8{0} ** (2 * 8);
    var segment2 = [_]u8{0} ** (5 * 8);

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
    var framed = std.ArrayList(u8){};
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

    var msg = try message.Message.init(allocator, bytes);
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

    var segment0 = [_]u8{0} ** (2 * 8);
    var segment1 = [_]u8{0} ** (7 * 8);

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
    var framed = std.ArrayList(u8){};
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

    var msg = try message.Message.init(allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 2), list_reader.len());
    try testing.expectEqual(@as(u32, 77), (try list_reader.get(0)).readU32(0));
    try testing.expectEqual(@as(u32, 88), (try list_reader.get(1)).readU32(0));
}
