const std = @import("std");
const testing = std.testing;
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

// ============================================================================
// Helpers
// ============================================================================

/// Build a message, serialize to bytes, deserialize, and return the root struct.
/// Caller must deinit the returned msg and free bytes.
fn roundTrip(builder: *message.MessageBuilder) !struct { bytes: []const u8, msg: message.Message } {
    const bytes = try builder.toBytes();
    errdefer testing.allocator.free(bytes);

    const msg = try message.Message.init(testing.allocator, bytes);
    return .{ .bytes = bytes, .msg = msg };
}

/// Build a message, pack, unpack, deserialize and return root struct.
/// Caller must deinit the returned msg and free packed_bytes.
fn packedRoundTrip(builder: *message.MessageBuilder) !struct { packed_bytes: []const u8, msg: message.Message } {
    const packed_bytes = try builder.toPackedBytes();
    errdefer testing.allocator.free(packed_bytes);

    const msg = try message.Message.initPacked(testing.allocator, packed_bytes);
    return .{ .packed_bytes = packed_bytes, .msg = msg };
}

// ============================================================================
// 1. Round-trip: build message with various field types, verify all match
// ============================================================================

test "fuzz round-trip: all unsigned integer types with bit patterns" {
    // Iterate through representative bit patterns for each integer width
    const u8_patterns = [_]u8{ 0, 1, 0x7F, 0x80, 0xFE, 0xFF };
    const u16_patterns = [_]u16{ 0, 1, 0x7FFF, 0x8000, 0xFFFE, 0xFFFF };
    const u32_patterns = [_]u32{ 0, 1, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFE, 0xFFFFFFFF };
    const u64_patterns = [_]u64{ 0, 1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFE, 0xFFFFFFFFFFFFFFFF };

    for (u8_patterns) |v8| {
        for (u16_patterns) |v16| {
            for (u32_patterns) |v32| {
                for (u64_patterns) |v64| {
                    var builder = message.MessageBuilder.init(testing.allocator);
                    defer builder.deinit();

                    // 2 data words = 16 bytes: u8@0, u16@2, u32@4, u64@8
                    var sb = try builder.allocateStruct(2, 0);
                    sb.writeU8(0, v8);
                    sb.writeU16(2, v16);
                    sb.writeU32(4, v32);
                    sb.writeU64(8, v64);

                    var rt = try roundTrip(&builder);
                    defer testing.allocator.free(rt.bytes);
                    defer rt.msg.deinit();

                    const root = try rt.msg.getRootStruct();
                    try testing.expectEqual(v8, root.readU8(0));
                    try testing.expectEqual(v16, root.readU16(2));
                    try testing.expectEqual(v32, root.readU32(4));
                    try testing.expectEqual(v64, root.readU64(8));
                }
            }
        }
    }
}

test "fuzz round-trip: signed integers via bitcast" {
    // Cap'n Proto stores signed ints as raw bits; we write/read as unsigned
    // and bitcast at application level. Verify the bit patterns survive.
    const i8_patterns = [_]i8{ -128, -1, 0, 1, 127 };
    const i16_patterns = [_]i16{ -32768, -1, 0, 1, 32767 };
    const i32_patterns = [_]i32{ -2147483648, -1, 0, 1, 2147483647 };
    const i64_patterns = [_]i64{ -9223372036854775808, -1, 0, 1, 9223372036854775807 };

    for (i8_patterns) |v8| {
        for (i16_patterns) |v16| {
            var builder = message.MessageBuilder.init(testing.allocator);
            defer builder.deinit();

            var sb = try builder.allocateStruct(2, 0);
            sb.writeU8(0, @bitCast(v8));
            sb.writeU16(2, @bitCast(v16));
            for (i32_patterns) |v32| {
                _ = v32;
            }
            // Write first i32 and i64 values for this combination
            sb.writeU32(4, @bitCast(i32_patterns[0]));
            sb.writeU64(8, @bitCast(i64_patterns[0]));

            var rt = try roundTrip(&builder);
            defer testing.allocator.free(rt.bytes);
            defer rt.msg.deinit();

            const root = try rt.msg.getRootStruct();
            const read_i8: i8 = @bitCast(root.readU8(0));
            const read_i16: i16 = @bitCast(root.readU16(2));
            const read_i32: i32 = @bitCast(root.readU32(4));
            const read_i64: i64 = @bitCast(root.readU64(8));

            try testing.expectEqual(v8, read_i8);
            try testing.expectEqual(v16, read_i16);
            try testing.expectEqual(i32_patterns[0], read_i32);
            try testing.expectEqual(i64_patterns[0], read_i64);
        }
    }

    // Also sweep through all i32 and i64 patterns individually
    for (i32_patterns) |v32| {
        for (i64_patterns) |v64| {
            var builder = message.MessageBuilder.init(testing.allocator);
            defer builder.deinit();

            var sb = try builder.allocateStruct(2, 0);
            sb.writeU32(0, @bitCast(v32));
            sb.writeU64(8, @bitCast(v64));

            var rt = try roundTrip(&builder);
            defer testing.allocator.free(rt.bytes);
            defer rt.msg.deinit();

            const root = try rt.msg.getRootStruct();
            const read_i32: i32 = @bitCast(root.readU32(0));
            const read_i64: i64 = @bitCast(root.readU64(8));
            try testing.expectEqual(v32, read_i32);
            try testing.expectEqual(v64, read_i64);
        }
    }
}

test "fuzz round-trip: bool fields all 8 bits in a byte" {
    // Exhaustively test all 256 combinations of 8 bools in a single byte
    var pattern: u16 = 0;
    while (pattern < 256) : (pattern += 1) {
        var builder = message.MessageBuilder.init(testing.allocator);
        defer builder.deinit();

        var sb = try builder.allocateStruct(1, 0);
        var bit: u3 = 0;
        while (true) {
            const set = ((@as(u8, @intCast(pattern)) >> bit) & 1) != 0;
            sb.writeBool(0, bit, set);
            if (bit == 7) break;
            bit += 1;
        }

        var rt = try roundTrip(&builder);
        defer testing.allocator.free(rt.bytes);
        defer rt.msg.deinit();

        const root = try rt.msg.getRootStruct();
        bit = 0;
        while (true) {
            const expected = ((@as(u8, @intCast(pattern)) >> bit) & 1) != 0;
            try testing.expectEqual(expected, root.readBool(0, bit));
            if (bit == 7) break;
            bit += 1;
        }
    }
}

// ============================================================================
// 2. Edge-case values: 0, max, min, NaN, infinity, empty strings/lists
// ============================================================================

test "fuzz edge cases: float special values via f32 list" {
    const f32_patterns = [_]f32{
        0.0,
        -0.0,
        1.0,
        -1.0,
        std.math.floatMin(f32),
        std.math.floatMax(f32),
        std.math.inf(f32),
        -std.math.inf(f32),
        std.math.nan(f32),
        // Denormalized
        @bitCast(@as(u32, 1)),
    };

    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(0, 1);
    var list = try sb.writeF32List(0, @intCast(f32_patterns.len));
    for (f32_patterns, 0..) |v, i| {
        try list.set(@intCast(i), v);
    }

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    const reader = try root.readF32List(0);
    try testing.expectEqual(@as(u32, f32_patterns.len), reader.len());

    for (f32_patterns, 0..) |expected, i| {
        const actual = try reader.get(@intCast(i));
        if (std.math.isNan(expected)) {
            try testing.expect(std.math.isNan(actual));
        } else {
            // Use bitCast comparison to correctly handle -0.0 vs 0.0
            const expected_bits: u32 = @bitCast(expected);
            const actual_bits: u32 = @bitCast(actual);
            try testing.expectEqual(expected_bits, actual_bits);
        }
    }
}

test "fuzz edge cases: float special values via f64 list" {
    const f64_patterns = [_]f64{
        0.0,
        -0.0,
        1.0,
        -1.0,
        std.math.floatMin(f64),
        std.math.floatMax(f64),
        std.math.inf(f64),
        -std.math.inf(f64),
        std.math.nan(f64),
        // Denormalized
        @bitCast(@as(u64, 1)),
    };

    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(0, 1);
    var list = try sb.writeF64List(0, @intCast(f64_patterns.len));
    for (f64_patterns, 0..) |v, i| {
        try list.set(@intCast(i), v);
    }

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    const reader = try root.readF64List(0);
    try testing.expectEqual(@as(u32, f64_patterns.len), reader.len());

    for (f64_patterns, 0..) |expected, i| {
        const actual = try reader.get(@intCast(i));
        if (std.math.isNan(expected)) {
            try testing.expect(std.math.isNan(actual));
        } else {
            const expected_bits: u64 = @bitCast(expected);
            const actual_bits: u64 = @bitCast(actual);
            try testing.expectEqual(expected_bits, actual_bits);
        }
    }
}

test "fuzz edge cases: empty string and empty data" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(0, 2);
    try sb.writeText(0, "");
    try sb.writeData(1, &[_]u8{});

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    try testing.expectEqualStrings("", try root.readText(0));
    try testing.expectEqualSlices(u8, &[_]u8{}, try root.readData(1));
}

test "fuzz edge cases: null pointer fields return defaults" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Allocate struct with pointers but don't write anything to them
    _ = try builder.allocateStruct(2, 4);

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    // Unwritten data section returns zeros
    try testing.expectEqual(@as(u64, 0), root.readU64(0));
    try testing.expectEqual(@as(u32, 0), root.readU32(0));
    try testing.expectEqual(@as(u16, 0), root.readU16(0));
    try testing.expectEqual(@as(u8, 0), root.readU8(0));
    try testing.expectEqual(false, root.readBool(0, 0));
    // Unwritten text pointer returns empty string
    try testing.expectEqualStrings("", try root.readText(0));
}

test "fuzz edge cases: empty lists" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(0, 7);
    _ = try sb.writeBoolList(0, 0);
    _ = try sb.writeU8List(1, 0);
    _ = try sb.writeU16List(2, 0);
    _ = try sb.writeU32List(3, 0);
    _ = try sb.writeU64List(4, 0);
    _ = try sb.writeF32List(5, 0);
    _ = try sb.writeF64List(6, 0);

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u32, 0), (try root.readBoolList(0)).len());
    try testing.expectEqual(@as(u32, 0), (try root.readU8List(1)).len());
    try testing.expectEqual(@as(u32, 0), (try root.readU16List(2)).len());
    try testing.expectEqual(@as(u32, 0), (try root.readU32List(3)).len());
    try testing.expectEqual(@as(u32, 0), (try root.readU64List(4)).len());
    try testing.expectEqual(@as(u32, 0), (try root.readF32List(5)).len());
    try testing.expectEqual(@as(u32, 0), (try root.readF64List(6)).len());
}

test "fuzz edge cases: void list round-trip" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(0, 1);
    const void_list = try sb.writeVoidList(0, 42);
    try testing.expectEqual(@as(u32, 42), void_list.len());

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    const reader = try root.readVoidList(0);
    try testing.expectEqual(@as(u32, 42), reader.len());
}

// ============================================================================
// 3. Nested structs (struct containing struct)
// ============================================================================

test "fuzz round-trip: nested structs three levels deep" {
    const depth_values = [_]u32{ 0, 42, 0xDEADBEEF, 0xFFFFFFFF };

    for (depth_values) |val_a| {
        for (depth_values) |val_b| {
            for (depth_values) |val_c| {
                var builder = message.MessageBuilder.init(testing.allocator);
                defer builder.deinit();

                // root: 1 data word, 1 pointer
                var root_sb = try builder.allocateStruct(1, 1);
                root_sb.writeU32(0, val_a);

                // child: 1 data word, 1 pointer
                var child = try root_sb.initStruct(0, 1, 1);
                child.writeU32(0, val_b);

                // grandchild: 1 data word, 0 pointers
                var grandchild = try child.initStruct(0, 1, 0);
                grandchild.writeU32(0, val_c);

                var rt = try roundTrip(&builder);
                defer testing.allocator.free(rt.bytes);
                defer rt.msg.deinit();

                const root = try rt.msg.getRootStruct();
                try testing.expectEqual(val_a, root.readU32(0));

                const child_r = try root.readStruct(0);
                try testing.expectEqual(val_b, child_r.readU32(0));

                const grandchild_r = try child_r.readStruct(0);
                try testing.expectEqual(val_c, grandchild_r.readU32(0));
            }
        }
    }
}

test "fuzz round-trip: nested struct with mixed data and text" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_sb = try builder.allocateStruct(1, 2);
    root_sb.writeU64(0, 0xCAFEBABE_DEADBEEF);
    try root_sb.writeText(0, "parent-text");

    var child = try root_sb.initStruct(1, 1, 1);
    child.writeU32(0, 12345);
    try child.writeText(0, "child-text");

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u64, 0xCAFEBABE_DEADBEEF), root.readU64(0));
    try testing.expectEqualStrings("parent-text", try root.readText(0));

    const child_r = try root.readStruct(1);
    try testing.expectEqual(@as(u32, 12345), child_r.readU32(0));
    try testing.expectEqualStrings("child-text", try child_r.readText(0));
}

// ============================================================================
// 4. Various list types
// ============================================================================

test "fuzz round-trip: primitive list sweep with deterministic patterns" {
    // Use a deterministic sequence to populate lists
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    const count: u32 = 16;
    var sb = try builder.allocateStruct(0, 7);

    // u8 list
    var u8_list = try sb.writeU8List(0, count);
    for (0..count) |i| {
        try u8_list.set(@intCast(i), @intCast(i * 17 % 256));
    }

    // i8 list
    var i8_list = try sb.writeI8List(1, count);
    for (0..count) |i| {
        const val: i8 = @intCast(@as(i16, @intCast(i)) * 17 - 128);
        try i8_list.set(@intCast(i), val);
    }

    // u16 list
    var u16_list = try sb.writeU16List(2, count);
    for (0..count) |i| {
        try u16_list.set(@intCast(i), @intCast(i * 4111));
    }

    // i16 list
    var i16_list = try sb.writeI16List(3, count);
    for (0..count) |i| {
        const val: i16 = @intCast(@as(i32, @intCast(i)) * 4111 - 32768);
        try i16_list.set(@intCast(i), val);
    }

    // u32 list
    var u32_list = try sb.writeU32List(4, count);
    for (0..count) |i| {
        try u32_list.set(@intCast(i), @as(u32, @intCast(i)) *% 0x10001);
    }

    // i32 list
    var i32_list = try sb.writeI32List(5, count);
    for (0..count) |i| {
        const val: i32 = @bitCast(@as(u32, @intCast(i)) *% 0x10001);
        try i32_list.set(@intCast(i), val);
    }

    // u64 list
    var u64_list = try sb.writeU64List(6, count);
    for (0..count) |i| {
        try u64_list.set(@intCast(i), @as(u64, @intCast(i)) *% 0x100000001);
    }

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();

    // Verify u8
    const u8_reader = try root.readU8List(0);
    try testing.expectEqual(count, u8_reader.len());
    for (0..count) |i| {
        try testing.expectEqual(@as(u8, @intCast(i * 17 % 256)), try u8_reader.get(@intCast(i)));
    }

    // Verify i8
    const i8_reader = try root.readI8List(1);
    try testing.expectEqual(count, i8_reader.len());
    for (0..count) |i| {
        const expected: i8 = @intCast(@as(i16, @intCast(i)) * 17 - 128);
        try testing.expectEqual(expected, try i8_reader.get(@intCast(i)));
    }

    // Verify u16
    const u16_reader = try root.readU16List(2);
    try testing.expectEqual(count, u16_reader.len());
    for (0..count) |i| {
        try testing.expectEqual(@as(u16, @intCast(i * 4111)), try u16_reader.get(@intCast(i)));
    }

    // Verify i16
    const i16_reader = try root.readI16List(3);
    try testing.expectEqual(count, i16_reader.len());
    for (0..count) |i| {
        const expected: i16 = @intCast(@as(i32, @intCast(i)) * 4111 - 32768);
        try testing.expectEqual(expected, try i16_reader.get(@intCast(i)));
    }

    // Verify u32
    const u32_reader = try root.readU32List(4);
    try testing.expectEqual(count, u32_reader.len());
    for (0..count) |i| {
        try testing.expectEqual(@as(u32, @intCast(i)) *% 0x10001, try u32_reader.get(@intCast(i)));
    }

    // Verify i32
    const i32_reader = try root.readI32List(5);
    try testing.expectEqual(count, i32_reader.len());
    for (0..count) |i| {
        const expected: i32 = @bitCast(@as(u32, @intCast(i)) *% 0x10001);
        try testing.expectEqual(expected, try i32_reader.get(@intCast(i)));
    }

    // Verify u64
    const u64_reader = try root.readU64List(6);
    try testing.expectEqual(count, u64_reader.len());
    for (0..count) |i| {
        try testing.expectEqual(@as(u64, @intCast(i)) *% 0x100000001, try u64_reader.get(@intCast(i)));
    }
}

test "fuzz round-trip: struct list with varying data" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    const count: u32 = 8;
    var sb = try builder.allocateStruct(0, 1);
    var list = try sb.writeStructList(0, count, 1, 1);

    for (0..count) |i| {
        var elem = try list.get(@intCast(i));
        elem.writeU32(0, @as(u32, @intCast(i)) *% 0xDEAD);
        const text_buf = [_]u8{ 'A' + @as(u8, @intCast(i)), 0 };
        try elem.writeText(0, text_buf[0..1]);
    }

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(count, list_reader.len());

    for (0..count) |i| {
        const elem = try list_reader.get(@intCast(i));
        try testing.expectEqual(@as(u32, @intCast(i)) *% 0xDEAD, elem.readU32(0));
        const text = try elem.readText(0);
        try testing.expectEqual(@as(u8, 'A' + @as(u8, @intCast(i))), text[0]);
        try testing.expectEqual(@as(usize, 1), text.len);
    }
}

test "fuzz round-trip: text list with varied lengths" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    const strings = [_][]const u8{
        "",
        "a",
        "ab",
        "abc",
        "Hello, World!",
        "The quick brown fox jumps over the lazy dog",
        // Long string to push toward word boundaries
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    };

    var sb = try builder.allocateStruct(0, 1);
    var list = try sb.writeTextList(0, strings.len);
    for (strings, 0..) |s, i| {
        try list.set(@intCast(i), s);
    }

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    const list_reader = try root.readTextList(0);
    try testing.expectEqual(@as(u32, strings.len), list_reader.len());

    for (strings, 0..) |expected, i| {
        try testing.expectEqualStrings(expected, try list_reader.get(@intCast(i)));
    }
}

test "fuzz round-trip: list of lists (pointer list containing u32 lists)" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    const outer_count: u32 = 5;
    var sb = try builder.allocateStruct(0, 1);
    var outer = try sb.writePointerList(0, outer_count);

    // Each inner list has (i+1) elements
    for (0..outer_count) |i| {
        const inner_count: u32 = @intCast(i + 1);
        var inner = try outer.initU32List(@intCast(i), inner_count);
        for (0..inner_count) |j| {
            try inner.set(@intCast(j), @as(u32, @intCast(i * 100 + j)));
        }
    }

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    const outer_reader = try root.readPointerList(0);
    try testing.expectEqual(outer_count, outer_reader.len());

    for (0..outer_count) |i| {
        const inner_count: u32 = @intCast(i + 1);
        const inner_reader = try outer_reader.getU32List(@intCast(i));
        try testing.expectEqual(inner_count, inner_reader.len());
        for (0..inner_count) |j| {
            try testing.expectEqual(@as(u32, @intCast(i * 100 + j)), try inner_reader.get(@intCast(j)));
        }
    }
}

test "fuzz round-trip: list of lists (nested bool lists)" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(0, 1);
    var outer = try sb.writePointerList(0, 3);

    // List 0: 8 bools alternating true/false
    var list0 = try outer.initBoolList(0, 8);
    for (0..8) |i| {
        try list0.set(@intCast(i), i % 2 == 0);
    }

    // List 1: 1 bool (true)
    var list1 = try outer.initBoolList(1, 1);
    try list1.set(0, true);

    // List 2: 0 bools (empty)
    _ = try outer.initBoolList(2, 0);

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    const outer_reader = try root.readPointerList(0);

    const reader0 = try outer_reader.getBoolList(0);
    try testing.expectEqual(@as(u32, 8), reader0.len());
    for (0..8) |i| {
        try testing.expectEqual(i % 2 == 0, try reader0.get(@intCast(i)));
    }

    const reader1 = try outer_reader.getBoolList(1);
    try testing.expectEqual(@as(u32, 1), reader1.len());
    try testing.expectEqual(true, try reader1.get(0));

    const reader2 = try outer_reader.getBoolList(2);
    try testing.expectEqual(@as(u32, 0), reader2.len());
}

// ============================================================================
// 5. Packed/unpacked round-trips
// ============================================================================

test "fuzz packed round-trip: sparse data (mostly zeros)" {
    // Sparse data exercises the 0x00 tag path in packing
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // 8 data words = 64 bytes; only write to a few locations
    var sb = try builder.allocateStruct(8, 0);
    sb.writeU64(0, 1); // first word: single bit set
    // words 1-6 left as zero
    sb.writeU64(56, 0xFFFFFFFFFFFFFFFF); // last word: all bits set

    var rt = try packedRoundTrip(&builder);
    defer testing.allocator.free(rt.packed_bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u64, 1), root.readU64(0));
    try testing.expectEqual(@as(u64, 0), root.readU64(8));
    try testing.expectEqual(@as(u64, 0), root.readU64(16));
    try testing.expectEqual(@as(u64, 0), root.readU64(24));
    try testing.expectEqual(@as(u64, 0), root.readU64(32));
    try testing.expectEqual(@as(u64, 0), root.readU64(40));
    try testing.expectEqual(@as(u64, 0), root.readU64(48));
    try testing.expectEqual(@as(u64, 0xFFFFFFFFFFFFFFFF), root.readU64(56));
}

test "fuzz packed round-trip: dense data (no zeros)" {
    // Dense data exercises the 0xFF tag path in packing
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(4, 0);
    sb.writeU64(0, 0x0101010101010101);
    sb.writeU64(8, 0x0202020202020202);
    sb.writeU64(16, 0x0303030303030303);
    sb.writeU64(24, 0x0404040404040404);

    var rt = try packedRoundTrip(&builder);
    defer testing.allocator.free(rt.packed_bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u64, 0x0101010101010101), root.readU64(0));
    try testing.expectEqual(@as(u64, 0x0202020202020202), root.readU64(8));
    try testing.expectEqual(@as(u64, 0x0303030303030303), root.readU64(16));
    try testing.expectEqual(@as(u64, 0x0404040404040404), root.readU64(24));
}

test "fuzz packed round-trip: mixed data with text" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(2, 2);
    sb.writeU64(0, 0);
    sb.writeU64(8, 0xDEADBEEFCAFEBABE);
    try sb.writeText(0, "packed-text-field");
    try sb.writeText(1, "");

    var rt = try packedRoundTrip(&builder);
    defer testing.allocator.free(rt.packed_bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u64, 0), root.readU64(0));
    try testing.expectEqual(@as(u64, 0xDEADBEEFCAFEBABE), root.readU64(8));
    try testing.expectEqualStrings("packed-text-field", try root.readText(0));
    try testing.expectEqualStrings("", try root.readText(1));
}

test "fuzz packed round-trip: packed equals unpacked after decode" {
    // Build a message with varied content, compare packed and unpacked decoding
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var sb = try builder.allocateStruct(2, 3);
    sb.writeU32(0, 42);
    sb.writeU32(4, 0);
    sb.writeU64(8, 0xABCDEF0123456789);
    try sb.writeText(0, "compare");
    try sb.writeData(1, &[_]u8{ 0xDE, 0xAD });

    var u16s = try sb.writeU16List(2, 4);
    try u16s.set(0, 0);
    try u16s.set(1, 1);
    try u16s.set(2, 0xFFFF);
    try u16s.set(3, 0x8000);

    // Get unpacked bytes
    const unpacked_bytes = try builder.toBytes();
    defer testing.allocator.free(unpacked_bytes);

    // Get packed bytes and decode
    const packed_bytes = try builder.toPackedBytes();
    defer testing.allocator.free(packed_bytes);

    var unpacked_msg = try message.Message.init(testing.allocator, unpacked_bytes);
    defer unpacked_msg.deinit();

    var packed_msg = try message.Message.initPacked(testing.allocator, packed_bytes);
    defer packed_msg.deinit();

    // Compare root structs
    const unpacked_root = try unpacked_msg.getRootStruct();
    const packed_root = try packed_msg.getRootStruct();

    try testing.expectEqual(unpacked_root.readU32(0), packed_root.readU32(0));
    try testing.expectEqual(unpacked_root.readU32(4), packed_root.readU32(4));
    try testing.expectEqual(unpacked_root.readU64(8), packed_root.readU64(8));
    try testing.expectEqualStrings(try unpacked_root.readText(0), try packed_root.readText(0));
    try testing.expectEqualSlices(u8, try unpacked_root.readData(1), try packed_root.readData(1));

    const unpacked_u16s = try unpacked_root.readU16List(2);
    const packed_u16s = try packed_root.readU16List(2);
    try testing.expectEqual(unpacked_u16s.len(), packed_u16s.len());
    for (0..unpacked_u16s.len()) |i| {
        try testing.expectEqual(try unpacked_u16s.get(@intCast(i)), try packed_u16s.get(@intCast(i)));
    }
}

test "fuzz packed round-trip: deterministic bit-pattern sweep" {
    // Create messages where each data word has exactly one byte non-zero,
    // exercising different tag byte patterns in the packer.
    var byte_pos: usize = 0;
    while (byte_pos < 8) : (byte_pos += 1) {
        var builder = message.MessageBuilder.init(testing.allocator);
        defer builder.deinit();

        var sb = try builder.allocateStruct(1, 0);
        // Write a single non-zero byte at position byte_pos
        sb.writeU8(@intCast(byte_pos), 0xAB);

        var rt = try packedRoundTrip(&builder);
        defer testing.allocator.free(rt.packed_bytes);
        defer rt.msg.deinit();

        const root = try rt.msg.getRootStruct();
        // All other bytes should be zero
        for (0..8) |pos| {
            const expected: u8 = if (pos == byte_pos) 0xAB else 0;
            try testing.expectEqual(expected, root.readU8(@intCast(pos)));
        }
    }
}

// ============================================================================
// 6. Multiple segments (large messages)
// ============================================================================

test "fuzz round-trip: struct in secondary segment via far pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_sb = try builder.allocateStruct(1, 1);
    root_sb.writeU32(0, 0xAAAA);

    // Force child struct into a new segment
    const seg1 = try builder.createSegment();
    var child = try root_sb.initStructInSegment(0, 1, 0, seg1);
    child.writeU32(0, 0xBBBB);

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    // Message should have 2 segments
    try testing.expectEqual(@as(usize, 2), rt.msg.segments.len);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u32, 0xAAAA), root.readU32(0));

    const child_r = try root.readStruct(0);
    try testing.expectEqual(@as(u32, 0xBBBB), child_r.readU32(0));
}

test "fuzz round-trip: text in secondary segment via far pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_sb = try builder.allocateStruct(0, 1);
    const seg1 = try builder.createSegment();
    try root_sb.writeTextInSegment(0, "cross-segment-text", seg1);

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    try testing.expectEqual(@as(usize, 2), rt.msg.segments.len);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqualStrings("cross-segment-text", try root.readText(0));
}

test "fuzz round-trip: multiple segments with struct list via far pointer" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_sb = try builder.allocateStruct(0, 1);
    const seg1 = try builder.createSegment();

    const count: u32 = 4;
    var list = try root_sb.writeStructListInSegment(0, count, 1, 0, seg1);
    for (0..count) |i| {
        var elem = try list.get(@intCast(i));
        elem.writeU32(0, @as(u32, @intCast(i)) * 1000 + 1);
    }

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    const list_reader = try root.readStructList(0);
    try testing.expectEqual(count, list_reader.len());

    for (0..count) |i| {
        const elem = try list_reader.get(@intCast(i));
        try testing.expectEqual(@as(u32, @intCast(i)) * 1000 + 1, elem.readU32(0));
    }
}

test "fuzz round-trip: three segments with mixed content" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Root in segment 0 with data + 3 pointers
    var root_sb = try builder.allocateStruct(1, 3);
    root_sb.writeU64(0, 0x1111111111111111);

    // Text in segment 1
    const seg1 = try builder.createSegment();
    try root_sb.writeTextInSegment(0, "seg1-text", seg1);

    // Data in segment 2
    const seg2 = try builder.createSegment();
    try root_sb.writeDataInSegment(1, &[_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05 }, seg2);

    // u16 list in segment 2 as well
    var u16s = try root_sb.writeU16ListInSegment(2, 3, seg2);
    try u16s.set(0, 100);
    try u16s.set(1, 200);
    try u16s.set(2, 300);

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    try testing.expectEqual(@as(usize, 3), rt.msg.segments.len);

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u64, 0x1111111111111111), root.readU64(0));
    try testing.expectEqualStrings("seg1-text", try root.readText(0));
    try testing.expectEqualSlices(u8, &[_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05 }, try root.readData(1));

    const u16s_reader = try root.readU16List(2);
    try testing.expectEqual(@as(u32, 3), u16s_reader.len());
    try testing.expectEqual(@as(u16, 100), try u16s_reader.get(0));
    try testing.expectEqual(@as(u16, 200), try u16s_reader.get(1));
    try testing.expectEqual(@as(u16, 300), try u16s_reader.get(2));
}

test "fuzz round-trip: packed with multiple segments" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_sb = try builder.allocateStruct(1, 1);
    root_sb.writeU32(0, 0xFACE);

    const seg1 = try builder.createSegment();
    try root_sb.writeTextInSegment(0, "packed-multi-seg", seg1);

    var rt = try packedRoundTrip(&builder);
    defer testing.allocator.free(rt.packed_bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u32, 0xFACE), root.readU32(0));
    try testing.expectEqualStrings("packed-multi-seg", try root.readText(0));
}

// ============================================================================
// 7. Deterministic "random" patterns: systematic bit-pattern sweeps
// ============================================================================

test "fuzz systematic: single-bit sweep through u64" {
    // Write a u64 with exactly one bit set and verify round-trip
    var bit: u6 = 0;
    while (true) {
        const value: u64 = @as(u64, 1) << bit;

        var builder = message.MessageBuilder.init(testing.allocator);
        defer builder.deinit();

        var sb = try builder.allocateStruct(1, 0);
        sb.writeU64(0, value);

        var rt = try roundTrip(&builder);
        defer testing.allocator.free(rt.bytes);
        defer rt.msg.deinit();

        const root = try rt.msg.getRootStruct();
        try testing.expectEqual(value, root.readU64(0));

        if (bit == 63) break;
        bit += 1;
    }
}

test "fuzz systematic: walking ones through u64 (packed)" {
    // Same as above but through the packed path
    var bit: u6 = 0;
    while (true) {
        const value: u64 = @as(u64, 1) << bit;

        var builder = message.MessageBuilder.init(testing.allocator);
        defer builder.deinit();

        var sb = try builder.allocateStruct(1, 0);
        sb.writeU64(0, value);

        var rt = try packedRoundTrip(&builder);
        defer testing.allocator.free(rt.packed_bytes);
        defer rt.msg.deinit();

        const root = try rt.msg.getRootStruct();
        try testing.expectEqual(value, root.readU64(0));

        if (bit == 63) break;
        bit += 1;
    }
}

test "fuzz systematic: alternating bit patterns" {
    const patterns = [_]u64{
        0x5555555555555555, // 0101...
        0xAAAAAAAAAAAAAAAA, // 1010...
        0x3333333333333333, // 0011...
        0xCCCCCCCCCCCCCCCC, // 1100...
        0x0F0F0F0F0F0F0F0F, // 00001111...
        0xF0F0F0F0F0F0F0F0, // 11110000...
        0x00FF00FF00FF00FF, // byte-alternating
        0xFF00FF00FF00FF00,
        0x0000FFFF0000FFFF, // word-alternating
        0xFFFF0000FFFF0000,
    };

    for (patterns) |pattern| {
        var builder = message.MessageBuilder.init(testing.allocator);
        defer builder.deinit();

        var sb = try builder.allocateStruct(1, 0);
        sb.writeU64(0, pattern);

        // Test both unpacked and packed
        var rt = try roundTrip(&builder);
        defer testing.allocator.free(rt.bytes);
        defer rt.msg.deinit();

        const root_unpacked = try rt.msg.getRootStruct();
        try testing.expectEqual(pattern, root_unpacked.readU64(0));
    }

    // Packed path for all patterns
    for (patterns) |pattern| {
        var builder = message.MessageBuilder.init(testing.allocator);
        defer builder.deinit();

        var sb = try builder.allocateStruct(1, 0);
        sb.writeU64(0, pattern);

        var rt = try packedRoundTrip(&builder);
        defer testing.allocator.free(rt.packed_bytes);
        defer rt.msg.deinit();

        const root_packed = try rt.msg.getRootStruct();
        try testing.expectEqual(pattern, root_packed.readU64(0));
    }
}

test "fuzz systematic: PRNG-driven compound message round-trip" {
    // Use a deterministic PRNG to build varied messages and verify round-trip
    var prng = std.Random.DefaultPrng.init(0x12345678_9ABCDEF0);
    const random = prng.random();

    var iteration: usize = 0;
    while (iteration < 64) : (iteration += 1) {
        var builder = message.MessageBuilder.init(testing.allocator);
        defer builder.deinit();

        // Random structure: 1-4 data words, 0-3 pointers
        const data_words: u16 = @intCast(random.intRangeAtMost(u16, 1, 4));
        const pointer_count: u16 = @intCast(random.intRangeAtMost(u16, 0, 3));

        var sb = try builder.allocateStruct(data_words, pointer_count);

        // Fill data section with random values
        var dw: u16 = 0;
        while (dw < data_words) : (dw += 1) {
            sb.writeU64(@as(usize, dw) * 8, random.int(u64));
        }

        // Record the data we wrote for verification
        const expected_data = try testing.allocator.alloc(u64, data_words);
        defer testing.allocator.free(expected_data);
        {
            // Re-read what the builder wrote by serializing
            const tmp_bytes = try builder.toBytes();
            defer testing.allocator.free(tmp_bytes);
            var tmp_msg = try message.Message.init(testing.allocator, tmp_bytes);
            defer tmp_msg.deinit();
            const tmp_root = try tmp_msg.getRootStruct();
            for (0..data_words) |w| {
                expected_data[w] = tmp_root.readU64(w * 8);
            }
        }

        // Write text to first pointer slot if available
        var expected_text: ?[]const u8 = null;
        if (pointer_count > 0) {
            const text_len = random.intRangeAtMost(usize, 0, 32);
            const text_buf = try testing.allocator.alloc(u8, text_len);
            defer testing.allocator.free(text_buf);
            for (text_buf) |*b| {
                b.* = random.intRangeAtMost(u8, 0x20, 0x7E); // printable ASCII
            }
            try sb.writeText(0, text_buf);
            expected_text = try testing.allocator.dupe(u8, text_buf);
        }
        defer if (expected_text) |t| testing.allocator.free(t);

        // Round-trip
        var rt = try roundTrip(&builder);
        defer testing.allocator.free(rt.bytes);
        defer rt.msg.deinit();

        const root = try rt.msg.getRootStruct();

        // Verify data
        for (0..data_words) |w| {
            try testing.expectEqual(expected_data[w], root.readU64(w * 8));
        }

        // Verify text
        if (expected_text) |t| {
            try testing.expectEqualStrings(t, try root.readText(0));
        }
    }
}

test "fuzz systematic: PRNG-driven compound message packed round-trip" {
    var prng = std.Random.DefaultPrng.init(0xFEDCBA98_76543210);
    const random = prng.random();

    var iteration: usize = 0;
    while (iteration < 32) : (iteration += 1) {
        var builder = message.MessageBuilder.init(testing.allocator);
        defer builder.deinit();

        const data_words: u16 = @intCast(random.intRangeAtMost(u16, 1, 4));
        var sb = try builder.allocateStruct(data_words, 1);

        // Fill with a mix of zero and non-zero words
        var dw: u16 = 0;
        while (dw < data_words) : (dw += 1) {
            if (random.boolean()) {
                sb.writeU64(@as(usize, dw) * 8, random.int(u64));
            }
            // else: leave as zero (exercises packed zero-run)
        }

        try sb.writeText(0, "packed-prng");

        // Record expected values
        const unpacked_bytes = try builder.toBytes();
        defer testing.allocator.free(unpacked_bytes);

        var unpacked_msg = try message.Message.init(testing.allocator, unpacked_bytes);
        defer unpacked_msg.deinit();
        const unpacked_root = try unpacked_msg.getRootStruct();

        const expected_values = try testing.allocator.alloc(u64, data_words);
        defer testing.allocator.free(expected_values);
        for (0..data_words) |w| {
            expected_values[w] = unpacked_root.readU64(w * 8);
        }

        // Packed round-trip
        var rt = try packedRoundTrip(&builder);
        defer testing.allocator.free(rt.packed_bytes);
        defer rt.msg.deinit();

        const packed_root = try rt.msg.getRootStruct();
        for (0..data_words) |w| {
            try testing.expectEqual(expected_values[w], packed_root.readU64(w * 8));
        }
        try testing.expectEqualStrings("packed-prng", try packed_root.readText(0));
    }
}

// ============================================================================
// 8. Complex combined scenarios
// ============================================================================

test "fuzz combined: nested struct with all list types" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Root: 2 data words, 1 pointer (to child struct)
    var root_sb = try builder.allocateStruct(2, 1);
    root_sb.writeU64(0, 0x1234);
    root_sb.writeU64(8, 0x5678);

    // Child: 0 data words, 6 pointers (for various lists)
    var child = try root_sb.initStruct(0, 0, 6);

    // Bool list
    var bools = try child.writeBoolList(0, 16);
    for (0..16) |i| {
        try bools.set(@intCast(i), i % 3 == 0);
    }

    // U32 list
    var u32s = try child.writeU32List(1, 4);
    try u32s.set(0, 0);
    try u32s.set(1, 1);
    try u32s.set(2, 0x7FFFFFFF);
    try u32s.set(3, 0xFFFFFFFF);

    // F32 list
    var f32s = try child.writeF32List(2, 3);
    try f32s.set(0, 0.0);
    try f32s.set(1, 1.5);
    try f32s.set(2, -1.5);

    // F64 list
    var f64s = try child.writeF64List(3, 2);
    try f64s.set(0, std.math.pi);
    try f64s.set(1, std.math.e);

    // Text list
    var texts = try child.writeTextList(4, 3);
    try texts.set(0, "alpha");
    try texts.set(1, "");
    try texts.set(2, "gamma");

    // Data field
    try child.writeData(5, &[_]u8{ 0xFF, 0x00, 0xAA, 0x55 });

    // Round-trip
    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u64, 0x1234), root.readU64(0));
    try testing.expectEqual(@as(u64, 0x5678), root.readU64(8));

    const child_r = try root.readStruct(0);

    // Verify bool list
    const bools_r = try child_r.readBoolList(0);
    try testing.expectEqual(@as(u32, 16), bools_r.len());
    for (0..16) |i| {
        try testing.expectEqual(i % 3 == 0, try bools_r.get(@intCast(i)));
    }

    // Verify u32 list
    const u32s_r = try child_r.readU32List(1);
    try testing.expectEqual(@as(u32, 4), u32s_r.len());
    try testing.expectEqual(@as(u32, 0), try u32s_r.get(0));
    try testing.expectEqual(@as(u32, 1), try u32s_r.get(1));
    try testing.expectEqual(@as(u32, 0x7FFFFFFF), try u32s_r.get(2));
    try testing.expectEqual(@as(u32, 0xFFFFFFFF), try u32s_r.get(3));

    // Verify f32 list
    const f32s_r = try child_r.readF32List(2);
    try testing.expectEqual(@as(u32, 3), f32s_r.len());
    try testing.expectApproxEqAbs(@as(f32, 0.0), try f32s_r.get(0), 0.0001);
    try testing.expectApproxEqAbs(@as(f32, 1.5), try f32s_r.get(1), 0.0001);
    try testing.expectApproxEqAbs(@as(f32, -1.5), try f32s_r.get(2), 0.0001);

    // Verify f64 list
    const f64s_r = try child_r.readF64List(3);
    try testing.expectEqual(@as(u32, 2), f64s_r.len());
    try testing.expectApproxEqAbs(std.math.pi, try f64s_r.get(0), 0.00001);
    try testing.expectApproxEqAbs(std.math.e, try f64s_r.get(1), 0.00001);

    // Verify text list
    const texts_r = try child_r.readTextList(4);
    try testing.expectEqual(@as(u32, 3), texts_r.len());
    try testing.expectEqualStrings("alpha", try texts_r.get(0));
    try testing.expectEqualStrings("", try texts_r.get(1));
    try testing.expectEqualStrings("gamma", try texts_r.get(2));

    // Verify data
    try testing.expectEqualSlices(u8, &[_]u8{ 0xFF, 0x00, 0xAA, 0x55 }, try child_r.readData(5));
}

test "fuzz combined: struct list within pointer list (list of lists of structs)" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_sb = try builder.allocateStruct(0, 1);
    var outer = try root_sb.writePointerList(0, 2);

    // First element: a struct with data
    var s0 = try outer.initStruct(0, 1, 1);
    s0.writeU32(0, 999);
    try s0.writeText(0, "inner-struct");

    // Second element: text
    try outer.setText(1, "plain-text");

    var rt = try roundTrip(&builder);
    defer testing.allocator.free(rt.bytes);
    defer rt.msg.deinit();

    const root = try rt.msg.getRootStruct();
    const outer_r = try root.readPointerList(0);
    try testing.expectEqual(@as(u32, 2), outer_r.len());

    const s0_r = try outer_r.getStruct(0);
    try testing.expectEqual(@as(u32, 999), s0_r.readU32(0));
    try testing.expectEqualStrings("inner-struct", try s0_r.readText(0));

    try testing.expectEqualStrings("plain-text", try outer_r.getText(1));
}

test "fuzz combined: deeply nested structs with packed round-trip" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    // Build 8 levels of nesting
    const depth = 8;
    var current = try builder.allocateStruct(1, 1);
    current.writeU32(0, 0);

    var level: usize = 1;
    while (level < depth) : (level += 1) {
        var next = try current.initStruct(0, 1, 1);
        next.writeU32(0, @intCast(level));
        current = next;
    }
    // Leaf has no pointer
    var leaf = try current.initStruct(0, 1, 0);
    leaf.writeU32(0, @intCast(depth));

    // Packed round-trip
    var rt = try packedRoundTrip(&builder);
    defer testing.allocator.free(rt.packed_bytes);
    defer rt.msg.deinit();

    // Walk the tree and verify
    var reader = try rt.msg.getRootStruct();
    try testing.expectEqual(@as(u32, 0), reader.readU32(0));

    level = 1;
    while (level < depth) : (level += 1) {
        reader = try reader.readStruct(0);
        try testing.expectEqual(@as(u32, @intCast(level)), reader.readU32(0));
    }
    // Leaf
    reader = try reader.readStruct(0);
    try testing.expectEqual(@as(u32, @intCast(depth)), reader.readU32(0));
}

test "fuzz combined: multi-segment with packed encoding" {
    var builder = message.MessageBuilder.init(testing.allocator);
    defer builder.deinit();

    var root_sb = try builder.allocateStruct(1, 2);
    root_sb.writeU64(0, 0xBEEF);

    // Put a struct list in segment 1
    const seg1 = try builder.createSegment();
    var list = try root_sb.writeStructListInSegment(0, 3, 1, 0, seg1);
    for (0..3) |i| {
        var elem = try list.get(@intCast(i));
        elem.writeU32(0, @as(u32, @intCast(i + 1)) * 111);
    }

    // Put text in segment 2
    const seg2 = try builder.createSegment();
    try root_sb.writeTextInSegment(1, "multi-seg-packed", seg2);

    // Both unpacked and packed round-trips
    var rt_unpacked = try roundTrip(&builder);
    defer testing.allocator.free(rt_unpacked.bytes);
    defer rt_unpacked.msg.deinit();

    var rt_packed = try packedRoundTrip(&builder);
    defer testing.allocator.free(rt_packed.packed_bytes);
    defer rt_packed.msg.deinit();

    // Verify both produce identical results
    const root_u = try rt_unpacked.msg.getRootStruct();
    const root_p = try rt_packed.msg.getRootStruct();

    try testing.expectEqual(root_u.readU64(0), root_p.readU64(0));

    const list_u = try root_u.readStructList(0);
    const list_p = try root_p.readStructList(0);
    try testing.expectEqual(list_u.len(), list_p.len());
    for (0..list_u.len()) |i| {
        const eu = try list_u.get(@intCast(i));
        const ep = try list_p.get(@intCast(i));
        try testing.expectEqual(eu.readU32(0), ep.readU32(0));
    }

    try testing.expectEqualStrings(try root_u.readText(1), try root_p.readText(1));
}
