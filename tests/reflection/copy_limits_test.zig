const std = @import("std");
const capnp = @import("capnpc-zig");
const message = capnp.message;
const helpers = capnp.generated_helpers;

test "bounded copy checks output and work limits without changing destination" {
    var source = message.MessageBuilder.init(std.testing.allocator);
    defer source.deinit();
    const record = try source.allocateStruct(1, 0);
    record.writeU64(0, 42);
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try storage.bind(&source);
    const reader = try storage.message_view.getRootAnyPointer();
    var dest = message.MessageBuilder.init(std.testing.allocator);
    defer dest.deinit();
    const pointer = try dest.initRootAnyPointer();
    try pointer.setText("original");
    try std.testing.expectError(error.CopyOutputLimitExceeded, helpers.setPointerWithOptions(pointer, reader, .{ .max_output_words = 0 }));
    try std.testing.expectError(error.CopyWorkLimitExceeded, helpers.setPointerWithOptions(pointer, reader, .{ .max_work = 1 }));
    var destination_storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer destination_storage.deinit();
    try destination_storage.bind(&dest);
    try std.testing.expectEqualStrings("original", try (try destination_storage.message_view.getRootAnyPointer()).getTextStrict());
    for ([_]usize{ 1, 2 }) |words| {
        try helpers.setPointerWithOptions(pointer, reader, .{ .max_output_words = words, .max_work = 2 });
        try std.testing.expectEqual(@as(u64, 42), helpers.scalarReader(try pointer.getStruct()).readU64(0));
    }
}

test "bounded copy accounts for temporary allocations before publishing" {
    var source = message.MessageBuilder.init(std.testing.allocator);
    defer source.deinit();
    const record = try source.allocateStruct(1, 1);
    record.writeU64(0, 51);
    const large: [8192]u8 = @splat('b');
    try record.writeText(0, &large);
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try storage.bind(&source);
    var dest = message.MessageBuilder.init(std.testing.allocator);
    defer dest.deinit();
    const pointer = try dest.initRootAnyPointer();
    try pointer.setText("retained");
    try std.testing.expectError(error.CopyAllocationLimitExceeded, helpers.setPointerWithOptions(pointer, try storage.message_view.getRootAnyPointer(), .{ .max_allocation_bytes = 0 }));
    var dest_storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer dest_storage.deinit();
    try dest_storage.bind(&dest);
    try std.testing.expectEqualStrings("retained", try (try dest_storage.message_view.getRootAnyPointer()).getTextStrict());
    try helpers.setPointerWithOptions(pointer, try storage.message_view.getRootAnyPointer(), .{ .max_allocation_bytes = 65536 });
    try std.testing.expectEqual(@as(u64, 51), helpers.scalarReader(try pointer.getStruct()).readU64(0));
}

test "bounded copy limits nesting and logical zero-width list work" {
    var source = message.MessageBuilder.init(std.testing.allocator);
    defer source.deinit();
    const root = try source.allocateStruct(0, 1);
    _ = try root.initStruct(0, 0, 1); // child with a null pointer
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try storage.bind(&source);
    const reader = try storage.message_view.getRootAnyPointer();
    try std.testing.expectError(error.RecursionLimitExceeded, helpers.checkPointerCopy(reader, .{ .nesting_limit = 1 }));
    try helpers.checkPointerCopy(reader, .{ .nesting_limit = 2 });
    try helpers.checkPointerCopy(reader, .{ .nesting_limit = 3 });
    _ = try (try root.getAnyPointer(0)).initVoidList(1000000);
    try storage.bind(&source);
    try std.testing.expectError(error.CopyWorkLimitExceeded, helpers.checkPointerCopy(try storage.message_view.getRootAnyPointer(), .{ .max_work = 100 }));
}

fn copyWithFailures(allocator: std.mem.Allocator, bytes: []const u8) !void {
    var source = try message.Message.init(std.testing.allocator, bytes, .{});
    defer source.deinit();
    var dest = message.MessageBuilder.init(allocator);
    defer dest.deinit();
    const pointer = try dest.initRootAnyPointer();
    try pointer.setText("original");
    helpers.setPointerWithOptions(pointer, try source.getRootAnyPointer(), .{}) catch |err| {
        var storage = helpers.ReaderStorage.init(std.testing.allocator);
        defer storage.deinit();
        try storage.bind(&dest);
        try std.testing.expectEqualStrings("original", try (try storage.message_view.getRootAnyPointer()).getTextStrict());
        return err;
    };
    try std.testing.expectEqual(@as(u64, 87), helpers.scalarReader(try pointer.getStruct()).readU64(0));
}

test "bounded copy retains old data and allocator ownership at every failure" {
    var source = message.MessageBuilder.init(std.testing.allocator);
    defer source.deinit();
    const root = try source.allocateStruct(1, 1);
    root.writeU64(0, 87);
    const text: [8192]u8 = @splat('x');
    try root.writeText(0, &text);
    const bytes = try source.toBytes();
    defer std.testing.allocator.free(bytes);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, copyWithFailures, .{bytes});
}

test "bounded copy charges shared targets per edge and rejects cyclic expansion" {
    var source = message.MessageBuilder.init(std.testing.allocator);
    defer source.deinit();
    const root = try source.allocateStruct(0, 2);
    try root.writeText(0, "abcdefgh");
    const first = try root.getAnyPointer(0);
    const second = try root.getAnyPointer(1);
    const segment = source.segments.items[first.segment_id].items;
    const word = std.mem.readInt(u64, segment[first.pointer_pos..][0..8], .little);
    // Same target from the next slot: relative offset decreases by one word.
    std.mem.writeInt(u64, segment[second.pointer_pos..][0..8], word - 4, .little);
    var storage = helpers.ReaderStorage.init(std.testing.allocator);
    defer storage.deinit();
    try storage.bind(&source);
    const reader = try storage.message_view.getRootAnyPointer();
    try std.testing.expectError(error.CopyWorkLimitExceeded, helpers.checkPointerCopy(reader, .{ .max_work = 20 }));
    try std.testing.expectError(error.CopyOutputLimitExceeded, helpers.checkPointerCopy(reader, .{ .max_output_words = 5 }));
    for ([_]usize{ 0, 1 }) |extra| try helpers.checkPointerCopy(reader, .{ .max_work = 21 + extra, .max_output_words = 6 + extra });

    var cycle = message.MessageBuilder.init(std.testing.allocator);
    defer cycle.deinit();
    const record = try cycle.allocateStruct(0, 1);
    const pointer = try record.getAnyPointer(0);
    std.mem.writeInt(u64, cycle.segments.items[pointer.segment_id].items[pointer.pointer_pos..][0..8], 0x00010000fffffffc, .little);
    try storage.bind(&cycle);
    try std.testing.expectError(error.RecursionLimitExceeded, helpers.checkPointerCopy(try storage.message_view.getRootAnyPointer(), .{ .nesting_limit = 8 }));
}
