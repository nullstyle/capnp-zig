const std = @import("std");
const message = @import("capnpc-zig").message;
const expect = std.testing.expect;
const equal = std.testing.expectEqual;

fn word(builder: *message.MessageBuilder, segment: u32, offset: usize) u64 {
    return std.mem.readInt(u64, builder.segments.items[segment].items[offset..][0..8], .little);
}

test "double-far struct-list writer emits canonical list tags for empty and aliased segments" {
    const Assignment = enum { distinct, source_landing, source_content };
    const Case = struct { assignment: Assignment, count: u32, data: u16 = 1, pointers: u16 = 0 };
    const cases = [_]Case{
        .{ .assignment = .distinct, .count = 2 },
        .{ .assignment = .source_landing, .count = 2 },
        .{ .assignment = .source_content, .count = 257 },
        .{ .assignment = .distinct, .count = 0 },
        .{ .assignment = .distinct, .count = 2, .data = 0 },
        .{ .assignment = .distinct, .count = 0, .data = 0 },
        .{ .assignment = .distinct, .count = 2, .pointers = 1 },
        .{ .assignment = .source_landing, .count = 2, .pointers = 1 },
        .{ .assignment = .source_content, .count = 2, .pointers = 1 },
    };
    for (cases) |case| {
        var builder = message.MessageBuilder.init(std.testing.allocator);
        defer builder.deinit();
        var root = try builder.allocateStruct(0, 1);
        const landing = if (case.assignment == .source_landing) 0 else try builder.createSegment();
        const content = if (case.assignment == .source_content) 0 else try builder.createSegment();
        // Sentinels force nonzero offsets and must survive aliasing/reallocation.
        const landing_sentinel = try builder.allocateStructInSegment(landing, 1, 0);
        landing_sentinel.writeU64(0, 0xfeed);
        const content_sentinel = try builder.allocateStructInSegment(content, 1, 0);
        content_sentinel.writeU64(0, 0xbeef);
        const list = try root.writeStructListInSegments(0, case.count, case.data, case.pointers, landing, content);
        for (0..case.count) |i| {
            var child = try list.get(@intCast(i));
            if (case.data > 0) child.writeU64(0, @intCast(i + 10));
            if (case.pointers > 0) try child.writeText(0, "canonical");
        }
        const pointer = try root.getAnyPointer(0);
        const source_word = word(&builder, pointer.segment_id, pointer.pointer_pos);
        try equal(@as(u64, 6), source_word & 7);
        try equal(landing, @as(u32, @truncate(source_word >> 32)));
        const landing_offset: usize = @as(usize, @intCast((source_word >> 3) & 0x1fffffff)) * 8;
        const far = word(&builder, landing, landing_offset);
        const list_tag = word(&builder, landing, landing_offset + 8);
        try equal(@as(u64, 2), far & 7);
        try equal(content, @as(u32, @truncate(far >> 32)));
        const content_offset: usize = @as(usize, @intCast((far >> 3) & 0x1fffffff)) * 8;
        try equal(@as(u64, 1), list_tag & 3);
        try equal(@as(u64, 7), (list_tag >> 32) & 7);
        try equal(@as(u64, case.count) * (case.data + case.pointers), list_tag >> 35);
        const element_tag = word(&builder, content, content_offset);
        try equal(@as(u64, 0), element_tag & 3);
        try equal(@as(u64, case.count), (element_tag >> 2) & 0x3fffffff);
        try equal(@as(u64, case.data), (element_tag >> 32) & 0xffff);
        try equal(@as(u64, case.pointers), element_tag >> 48);
        try equal(@as(u64, 0xfeed), word(&builder, landing, landing_sentinel.offset));
        try equal(@as(u64, 0xbeef), word(&builder, content, content_sentinel.offset));
        const reopened = try pointer.getStructList();
        try equal(case.count, reopened.len());
        if (case.count > 0 and case.data > 0) (try reopened.get(0)).writeU64(0, 42);
        const bytes = try builder.toBytes();
        defer std.testing.allocator.free(bytes);
        var decoded = try message.Message.init(std.testing.allocator, bytes, .{});
        defer decoded.deinit();
        const result = try (try decoded.getRootStruct()).readStructList(0);
        try equal(case.count, result.len());
        for (0..case.count) |i| {
            const child = try result.get(@intCast(i));
            if (case.data > 0) try equal(@as(u64, if (i == 0) 42 else i + 10), child.readU64(0));
            if (case.pointers > 0) try std.testing.expectEqualStrings("canonical", try child.readTextStrict(0));
        }
    }
}
