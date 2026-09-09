const std = @import("std");
const message = @import("capnpc-zig").message;

test "mutable primitive and pointer views preserve unknown struct list fields" {
    for (0..3) |encoding| {
        var builder = message.MessageBuilder.init(std.testing.allocator);
        defer builder.deinit();
        const root = try builder.allocateStruct(0, 1);
        const landing = if (encoding == 0) 0 else try builder.createSegment();
        const content = if (encoding < 2) landing else try builder.createSegment();
        const list = try root.writeStructListInSegments(0, 2, 2, 2, landing, content);
        for (0..2) |i| {
            const elem = try list.get(@intCast(i));
            try elem.writeU64Strict(0, 10 + i);
            try elem.writeU64Strict(8, 900 + i);
            try elem.writeText(0, "original");
            try elem.writeText(1, "unknown");
        }
        const pointer = try root.getAnyPointer(0);
        try (try pointer.getU32List()).set(1, 77);
        try (try pointer.getU8List()).setAll(&.{ 5, 6 });
        try (try pointer.getTextList()).set(1, "updated");
        try (try pointer.getPointerList()).setText(0, "first");
        try std.testing.expectEqual(@as(u32, 2), (try pointer.getVoidList()).len());
        try std.testing.expectError(error.InvalidPointer, pointer.getBoolList());
        const bytes = try builder.toBytes();
        defer std.testing.allocator.free(bytes);
        var decoded = try message.Message.init(std.testing.allocator, bytes, .{});
        defer decoded.deinit();
        const read = try (try decoded.getRootStruct()).readStructList(0);
        for (0..2) |i| {
            const elem = try read.get(@intCast(i));
            try std.testing.expectEqual(@as(u64, 5 + i), elem.readU64(0));
            try std.testing.expectEqual(@as(u64, 900 + i), elem.readU64(8));
            try std.testing.expectEqualStrings(if (i == 0) "first" else "updated", try elem.readTextStrict(0));
            try std.testing.expectEqualStrings("unknown", try elem.readTextStrict(1));
        }
    }
}

test "strict text list entry points reject missing NUL and invalid UTF-8" {
    for ([_][]const u8{ "missing", "\xff\x00", "" }, 0..) |payload, index| {
        var builder = message.MessageBuilder.init(std.testing.allocator);
        defer builder.deinit();
        const root = try builder.allocateStruct(0, 1);
        const list = try root.writePointerList(0, 1);
        try list.setData(0, payload);
        const bytes = try builder.toBytes();
        defer std.testing.allocator.free(bytes);
        var decoded = try message.Message.init(std.testing.allocator, bytes, .{});
        defer decoded.deinit();
        const read = try decoded.getRootStruct();
        const expected = if (index == 1) error.InvalidUtf8 else error.InvalidTextPointer;
        try std.testing.expectError(expected, (try read.readTextList(0)).getStrict(0));
        try std.testing.expectError(expected, (try read.readPointerList(0)).getTextStrict(0));
        try std.testing.expectError(expected, (try read.readTextListStrict(0)).get(0));
        try std.testing.expectError(expected, (try (try message.AnyListReader.wrap(try read.readAnyPointer(0))).getTextListStrict()).get(0));
    }
}
