const std = @import("std");
const message = @import("capnpc-zig").message;

fn word(bytes: []u8, offset: usize, value: u64) void {
    std.mem.writeInt(u64, bytes[offset..][0..8], value, .little);
}

fn cyclicStruct() [48]u8 {
    var bytes: [48]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 2, .little);
    std.mem.writeInt(u32, bytes[4..8], 1, .little);
    std.mem.writeInt(u32, bytes[8..12], 2, .little);
    std.mem.writeInt(u32, bytes[12..16], 1, .little);
    word(&bytes, 16, 0x0000000100000006); // Root -> double-far landing pad.
    word(&bytes, 24, 0x0000000200000002); // Content in segment 2.
    word(&bytes, 32, 0x0001000000000000); // Struct with one pointer.
    word(&bytes, 40, 0x00010000fffffffc); // Child points back to itself.
    return bytes;
}

fn expectRejected(bytes: []const u8, options: message.Message.ValidationOptions, expected: anyerror) !void {
    var decoded = message.Message.init(std.testing.allocator, bytes, options) catch |actual| {
        try std.testing.expectEqual(expected, actual);
        return;
    };
    defer decoded.deinit();
    return error.ExpectedValidationRejection;
}

test "double-far validation enforces independent limits on cyclic struct children" {
    const bytes = cyclicStruct();
    try expectRejected(&bytes, .{ .nesting_limit = 1 }, error.NestingLimitExceeded);
    try expectRejected(&bytes, .{ .traversal_limit_words = 2 }, error.TraversalLimitExceeded);
}

fn finiteTree() [56]u8 {
    var bytes: [56]u8 = @splat(0);
    const cycle = cyclicStruct();
    @memcpy(bytes[0..cycle.len], &cycle);
    std.mem.writeInt(u32, bytes[12..16], 2, .little);
    word(&bytes, 40, 0x0000000100000000); // Child has one data word.
    word(&bytes, 48, 42);
    return bytes;
}

test "double-far validation charges a finite struct and its child" {
    const bytes = finiteTree();
    var decoded = try message.Message.init(std.testing.allocator, &bytes, .{
        .nesting_limit = 2,
        .traversal_limit_words = 4,
    });
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 4), decoded.traversal_words_used);
    try std.testing.expectEqual(@as(u64, 42), (try (try decoded.getRootStruct()).readStruct(0)).readU64(0));
    try expectRejected(&bytes, .{ .traversal_limit_words = 3 }, error.TraversalLimitExceeded);
    try expectRejected(&bytes, .{ .nesting_limit = 1 }, error.NestingLimitExceeded);
}

test "double-far validation rejects an invalid pointer below the root" {
    var bytes = finiteTree();
    word(&bytes, 40, 0x0000000100001000); // Child lies outside segment 2.
    try expectRejected(&bytes, .{}, error.OutOfBounds);
}

test "ambiguous zero-count legacy tags receive canonical struct bounds checks" {
    var bytes = cyclicStruct();
    word(&bytes, 32, 0x0002000000000000); // Struct claims two pointer words.
    word(&bytes, 40, 0);
    try expectRejected(&bytes, .{}, error.OutOfBounds);
}

test "failed double-far validation reports work already consumed" {
    const bytes = cyclicStruct();
    var decoded = try message.Message.initUnvalidated(std.testing.allocator, &bytes);
    defer decoded.deinit();
    var consumed: usize = 0;
    try std.testing.expectError(error.NestingLimitExceeded, decoded.validateCountedInto(.{ .nesting_limit = 2 }, &consumed));
    try std.testing.expectEqual(@as(usize, 4), consumed);
}

test "zero-width struct list traversal charges are independent of pointer encoding" {
    for (0..3) |encoding| {
        var builder = message.MessageBuilder.init(std.testing.allocator);
        defer builder.deinit();
        const root = try builder.allocateStruct(0, 1);
        const landing = if (encoding == 0) 0 else try builder.createSegment();
        const content = if (encoding < 2) landing else try builder.createSegment();
        _ = try root.writeStructListInSegments(0, 32, 0, 0, landing, content);
        const bytes = try builder.toBytes();
        defer std.testing.allocator.free(bytes);
        try expectRejected(bytes, .{ .traversal_limit_words = 31 }, error.TraversalLimitExceeded);
        var decoded = try message.Message.init(std.testing.allocator, bytes, .{ .traversal_limit_words = 35 });
        defer decoded.deinit();
        try std.testing.expectEqual(@as(usize, 33) + encoding, decoded.traversal_words_used);
        try std.testing.expectEqual(@as(u32, 32), (try (try decoded.getRootStruct()).readStructList(0)).len());
    }
}
