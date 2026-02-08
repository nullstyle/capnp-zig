const std = @import("std");
const testing = std.testing;
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

// ---------------------------------------------------------------------------
// Helper: build a message, serialize, deserialize, return the root StructReader.
// The caller must deinit both the returned Message and free the bytes slice.
// ---------------------------------------------------------------------------

fn roundTrip(builder: *message.MessageBuilder) !struct { msg: message.Message, bytes: []const u8 } {
    const bytes = try builder.toBytes();
    errdefer testing.allocator.free(bytes);
    const msg = try message.Message.init(testing.allocator, bytes);
    return .{ .msg = msg, .bytes = bytes };
}

// ===========================================================================
// 1. Writing and reading union discriminant values
// ===========================================================================

test "discriminant: write and read u16 discriminant at byte offset 0" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // 1 data word (8 bytes) holds discriminant (u16 at offset 0) + union payload (u32 at offset 4)
    var sb = try builder.allocateStruct(1, 0);
    sb.writeU16(0, 3); // discriminant = 3
    sb.writeU32(4, 0xDEAD);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 3), root.readU16(0));
    try testing.expectEqual(@as(u32, 0xDEAD), root.readU32(4));
}

test "discriminant: write and read discriminant at non-zero byte offset" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // 2 data words: first word is a u64 payload, second word holds discriminant at byte offset 8
    var sb = try builder.allocateStruct(2, 0);
    sb.writeU64(0, 0x1234_5678_9ABC_DEF0);
    sb.writeU16(8, 7); // discriminant at offset 8

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u64, 0x1234_5678_9ABC_DEF0), root.readU64(0));
    try testing.expectEqual(@as(u16, 7), root.readU16(8));
}

test "discriminant: writeUnionDiscriminant and readUnionDiscriminant round-trip" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(1, 0);
    sb.writeUnionDiscriminant(2, 42);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 42), root.readUnionDiscriminant(2));
}

test "discriminant: maximum u16 value" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(1, 0);
    sb.writeUnionDiscriminant(0, 0xFFFF);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 0xFFFF), root.readUnionDiscriminant(0));
}

// ===========================================================================
// 2. Switching union fields and verifying old data is zeroed
// ===========================================================================

test "switch: changing discriminant overwrites the previous value" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(2, 0);

    // First, set discriminant = 0 and write a u64 payload in word 1
    sb.writeUnionDiscriminant(0, 0);
    sb.writeU64(8, 0xAAAA_BBBB_CCCC_DDDD);

    // Now switch to discriminant = 1 and write a different payload.
    // In Cap'n Proto the generated code would zero the overlapping region;
    // here we simulate that by zeroing the union data word before writing.
    sb.writeUnionDiscriminant(0, 1);
    sb.writeU64(8, 0); // zero old data
    sb.writeU32(8, 99); // write new, smaller payload

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 1), root.readUnionDiscriminant(0));
    try testing.expectEqual(@as(u32, 99), root.readU32(8));
    // Upper 4 bytes of the word should be zero (old data cleared)
    try testing.expectEqual(@as(u32, 0), root.readU32(12));
}

test "switch: zeroing data section on variant change preserves non-union fields" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Layout: word 0 = non-union f64, word 1 = discriminant(u16) + padding,
    //         word 2 = union data (f64)
    var sb = try builder.allocateStruct(3, 0);

    // Set non-union field
    sb.writeU64(0, @as(u64, @bitCast(@as(f64, 3.14))));

    // Set circle variant: discriminant=0, union data=radius
    sb.writeU16(8, 0);
    sb.writeU64(16, @as(u64, @bitCast(@as(f64, 5.0))));

    // Switch to square variant: discriminant=1, zero union data, set side
    sb.writeU16(8, 1);
    sb.writeU64(16, 0); // clear old union data
    sb.writeU64(16, @as(u64, @bitCast(@as(f64, 7.0))));

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    // Non-union field must be intact
    try testing.expectApproxEqAbs(@as(f64, 3.14), @as(f64, @bitCast(root.readU64(0))), 0.001);
    // Discriminant updated
    try testing.expectEqual(@as(u16, 1), root.readU16(8));
    // Union data holds the new value
    try testing.expectApproxEqAbs(@as(f64, 7.0), @as(f64, @bitCast(root.readU64(16))), 0.001);
}

test "switch: pointer field is nulled when switching from pointer to data variant" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Layout: 1 data word (discriminant u16 @ 0, union u32 @ 4), 1 pointer
    var sb = try builder.allocateStruct(1, 1);

    // Set text variant (discriminant=0, pointer[0]=text)
    sb.writeU16(0, 0);
    try sb.writeText(0, "hello");

    var rt1 = try roundTrip(&builder);
    defer rt1.msg.deinit();
    defer testing.allocator.free(rt1.bytes);

    const root1 = try rt1.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 0), root1.readU16(0));
    try testing.expectEqualStrings("hello", try root1.readText(0));

    // Now build a second message where we switch from text to data variant.
    // The generated code would null the pointer, so we simulate by building
    // a struct that has discriminant=1, u32 payload, and a null pointer.
    var builder2 = message.MessageBuilder.init(testing.allocator);
    defer builder2.deinit();

    var sb2 = try builder2.allocateStruct(1, 1);
    sb2.writeU16(0, 1); // switch to data variant
    sb2.writeU32(4, 42);
    // pointer[0] is left as null (zero-initialized by allocateStruct)

    var rt2 = try roundTrip(&builder2);
    defer rt2.msg.deinit();
    defer testing.allocator.free(rt2.bytes);

    const root2 = try rt2.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 1), root2.readU16(0));
    try testing.expectEqual(@as(u32, 42), root2.readU32(4));
    try testing.expect(root2.isPointerNull(0));
}

// ===========================================================================
// 3. Reading default / unset union fields
// ===========================================================================

test "default: freshly allocated struct reads discriminant as 0" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Allocate struct but do not write any discriminant
    _ = try builder.allocateStruct(2, 0);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    // Zero-initialized data section means discriminant reads as 0
    try testing.expectEqual(@as(u16, 0), root.readUnionDiscriminant(0));
    try testing.expectEqual(@as(u16, 0), root.readUnionDiscriminant(2));
    try testing.expectEqual(@as(u16, 0), root.readUnionDiscriminant(4));
    // All data in the union payload region should be zero
    try testing.expectEqual(@as(u64, 0), root.readU64(8));
}

test "default: reading discriminant beyond data section returns 0" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Struct with only 1 data word
    var sb = try builder.allocateStruct(1, 0);
    sb.writeU64(0, 0xFFFF_FFFF_FFFF_FFFF);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    // Reading discriminant at offset 8 is beyond the 1-word data section -> returns 0
    try testing.expectEqual(@as(u16, 0), root.readUnionDiscriminant(8));
    try testing.expectEqual(@as(u16, 0), root.readUnionDiscriminant(100));
}

test "default: unset union data reads as zero for all types" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // 2 data words, 1 pointer: discriminant at offset 0, data at offset 8
    _ = try builder.allocateStruct(2, 1);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u8, 0), root.readU8(8));
    try testing.expectEqual(@as(u16, 0), root.readU16(8));
    try testing.expectEqual(@as(u32, 0), root.readU32(8));
    try testing.expectEqual(@as(u64, 0), root.readU64(8));
    try testing.expectEqual(false, root.readBool(8, 0));
    try testing.expect(root.isPointerNull(0));
}

// ===========================================================================
// 4. Edge cases: discriminant at various offsets, multi-word data sections
// ===========================================================================

test "edge: discriminant packed with other u16 fields in the same word" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // 1 data word with four u16 fields packed into it:
    //   offset 0: some field
    //   offset 2: discriminant
    //   offset 4: another field
    //   offset 6: yet another field
    var sb = try builder.allocateStruct(1, 0);
    sb.writeU16(0, 0x1111);
    sb.writeU16(2, 5); // discriminant
    sb.writeU16(4, 0x3333);
    sb.writeU16(6, 0x4444);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 0x1111), root.readU16(0));
    try testing.expectEqual(@as(u16, 5), root.readUnionDiscriminant(2));
    try testing.expectEqual(@as(u16, 0x3333), root.readU16(4));
    try testing.expectEqual(@as(u16, 0x4444), root.readU16(6));
}

test "edge: discriminant at end of a multi-word data section" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // 4 data words (32 bytes). Discriminant at byte offset 30 (last u16 of word 3).
    var sb = try builder.allocateStruct(4, 0);
    sb.writeU64(0, 0xAAAA_AAAA_AAAA_AAAA);
    sb.writeU64(8, 0xBBBB_BBBB_BBBB_BBBB);
    sb.writeU64(16, 0xCCCC_CCCC_CCCC_CCCC);
    sb.writeU16(24, 0xDDDD);
    sb.writeU16(26, 0xEEEE);
    sb.writeUnionDiscriminant(28, 12);
    sb.writeU16(30, 0xFFFF);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u64, 0xAAAA_AAAA_AAAA_AAAA), root.readU64(0));
    try testing.expectEqual(@as(u64, 0xBBBB_BBBB_BBBB_BBBB), root.readU64(8));
    try testing.expectEqual(@as(u64, 0xCCCC_CCCC_CCCC_CCCC), root.readU64(16));
    try testing.expectEqual(@as(u16, 12), root.readUnionDiscriminant(28));
    try testing.expectEqual(@as(u16, 0xFFFF), root.readU16(30));
}

test "edge: multiple sequential union writes keep only last discriminant" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(2, 0);

    // Rapidly cycle through discriminant values
    sb.writeUnionDiscriminant(0, 0);
    sb.writeU64(8, 111);
    sb.writeUnionDiscriminant(0, 1);
    sb.writeU64(8, 222);
    sb.writeUnionDiscriminant(0, 2);
    sb.writeU64(8, 333);
    sb.writeUnionDiscriminant(0, 3);
    sb.writeU64(8, 0); // clear
    sb.writeU64(8, 444);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 3), root.readUnionDiscriminant(0));
    try testing.expectEqual(@as(u64, 444), root.readU64(8));
}

test "edge: union with bool payload at bit level" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // 1 data word: discriminant at offset 0, bool payload at offset 2, bit 0
    var sb = try builder.allocateStruct(1, 0);
    sb.writeU16(0, 1); // discriminant = 1 (the "true" variant)
    sb.writeBool(2, 0, true);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 1), root.readUnionDiscriminant(0));
    try testing.expectEqual(true, root.readBool(2, 0));
}

test "edge: struct with discriminant and nested struct pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // 1 data word (discriminant), 1 pointer (nested struct)
    var sb = try builder.allocateStruct(1, 1);
    sb.writeUnionDiscriminant(0, 2);

    // Init a nested struct at pointer index 0
    var nested = try sb.initStruct(0, 1, 0);
    nested.writeU32(0, 0xBEEF);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 2), root.readUnionDiscriminant(0));
    try testing.expect(!root.isPointerNull(0));

    const nested_reader = try root.readStruct(0);
    try testing.expectEqual(@as(u32, 0xBEEF), nested_reader.readU32(0));
}

test "edge: union discriminant coexists with other data in the same word" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Simulate a struct where byte 0-1 = discriminant, byte 2 = u8, byte 4-7 = u32
    var sb = try builder.allocateStruct(1, 0);
    sb.writeUnionDiscriminant(0, 10);
    sb.writeU8(2, 0xAB);
    sb.writeU32(4, 0x12345678);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 10), root.readUnionDiscriminant(0));
    try testing.expectEqual(@as(u8, 0xAB), root.readU8(2));
    try testing.expectEqual(@as(u32, 0x12345678), root.readU32(4));
}

test "edge: discriminant survives packed encoding round-trip" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(2, 0);
    sb.writeUnionDiscriminant(0, 42);
    sb.writeU64(8, 0x0102_0304_0506_0708);

    const packed_bytes = try builder.toPackedBytes();
    defer testing.allocator.free(packed_bytes);

    var msg = try message.Message.initPacked(testing.allocator, packed_bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();
    try testing.expectEqual(@as(u16, 42), root.readUnionDiscriminant(0));
    try testing.expectEqual(@as(u64, 0x0102_0304_0506_0708), root.readU64(8));
}

test "edge: two independent unions in the same struct" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Simulate a struct with two unions:
    //   Union A: discriminant at offset 0, payload at offset 8
    //   Union B: discriminant at offset 2, payload at offset 16
    // Total: 3 data words
    var sb = try builder.allocateStruct(3, 0);

    // Set union A to variant 1 with payload
    sb.writeUnionDiscriminant(0, 1);
    sb.writeU64(8, 0xAAAA);

    // Set union B to variant 3 with payload
    sb.writeUnionDiscriminant(2, 3);
    sb.writeU64(16, 0xBBBB);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 1), root.readUnionDiscriminant(0));
    try testing.expectEqual(@as(u64, 0xAAAA), root.readU64(8));
    try testing.expectEqual(@as(u16, 3), root.readUnionDiscriminant(2));
    try testing.expectEqual(@as(u64, 0xBBBB), root.readU64(16));
}

test "edge: discriminant with text union field round-trips correctly" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // 1 data word (discriminant at 0), 1 pointer (text)
    var sb = try builder.allocateStruct(1, 1);
    sb.writeUnionDiscriminant(0, 5);
    try sb.writeText(0, "union-text-field");

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u16, 5), root.readUnionDiscriminant(0));
    try testing.expectEqualStrings("union-text-field", try root.readText(0));
}

test "edge: union in struct list element" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Root struct with one pointer (a struct list)
    var root_builder = try builder.allocateStruct(0, 1);

    // Each list element: 1 data word (discriminant + payload), 0 pointers
    var list_builder = try root_builder.writeStructList(0, 3, 1, 0);

    // Element 0: variant 0, payload 100
    var elem0 = try list_builder.get(0);
    elem0.writeUnionDiscriminant(0, 0);
    elem0.writeU32(4, 100);

    // Element 1: variant 1, payload 200
    var elem1 = try list_builder.get(1);
    elem1.writeUnionDiscriminant(0, 1);
    elem1.writeU32(4, 200);

    // Element 2: variant 2, payload 300
    var elem2 = try list_builder.get(2);
    elem2.writeUnionDiscriminant(0, 2);
    elem2.writeU32(4, 300);

    var rt = try roundTrip(&builder);
    defer rt.msg.deinit();
    defer testing.allocator.free(rt.bytes);

    const root = try rt.msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(@as(u32, 3), list_reader.len());

    const r0 = try list_reader.get(0);
    try testing.expectEqual(@as(u16, 0), r0.readUnionDiscriminant(0));
    try testing.expectEqual(@as(u32, 100), r0.readU32(4));

    const r1 = try list_reader.get(1);
    try testing.expectEqual(@as(u16, 1), r1.readUnionDiscriminant(0));
    try testing.expectEqual(@as(u32, 200), r1.readU32(4));

    const r2 = try list_reader.get(2);
    try testing.expectEqual(@as(u16, 2), r2.readUnionDiscriminant(0));
    try testing.expectEqual(@as(u32, 300), r2.readU32(4));
}
