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
