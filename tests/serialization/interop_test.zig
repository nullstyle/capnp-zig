const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

fn readFixture(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    return try file.readToEndAlloc(allocator, std.math.maxInt(usize));
}

fn verifyWidgetRoot(root: message.StructReader) !void {
    try std.testing.expectEqual(@as(u32, 123), root.readU32(0));
    try std.testing.expectEqualStrings("widget", try root.readText(0));

    const list = try root.readStructList(1);
    try std.testing.expectEqual(@as(u32, 3), list.len());

    const p0 = try list.get(0);
    try std.testing.expectEqual(@as(u32, 1), p0.readU32(0));
    try std.testing.expectEqual(@as(u32, 10), p0.readU32(4));

    const p1 = try list.get(1);
    try std.testing.expectEqual(@as(u32, 2), p1.readU32(0));
    try std.testing.expectEqual(@as(u32, 20), p1.readU32(4));

    const p2 = try list.get(2);
    try std.testing.expectEqual(@as(u32, 3), p2.readU32(0));
    try std.testing.expectEqual(@as(u32, 30), p2.readU32(4));

    const tags = try root.readTextList(2);
    try std.testing.expectEqual(@as(u32, 2), tags.len());
    try std.testing.expectEqualStrings("alpha", try tags.get(0));
    try std.testing.expectEqualStrings("beta", try tags.get(1));

    const bytes = try root.readData(3);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3, 4, 5 }, bytes);

    const u16s = try root.readU16List(4);
    try std.testing.expectEqual(@as(u32, 3), u16s.len());
    try std.testing.expectEqual(@as(u16, 10), try u16s.get(0));
    try std.testing.expectEqual(@as(u16, 20), try u16s.get(1));
    try std.testing.expectEqual(@as(u16, 30), try u16s.get(2));

    const u32s = try root.readU32List(5);
    try std.testing.expectEqual(@as(u32, 2), u32s.len());
    try std.testing.expectEqual(@as(u32, 1000), try u32s.get(0));
    try std.testing.expectEqual(@as(u32, 2000), try u32s.get(1));

    const u64s = try root.readU64List(6);
    try std.testing.expectEqual(@as(u32, 2), u64s.len());
    try std.testing.expectEqual(@as(u64, 123456789), try u64s.get(0));
    try std.testing.expectEqual(@as(u64, 987654321), try u64s.get(1));

    const bools = try root.readBoolList(7);
    try std.testing.expectEqual(@as(u32, 5), bools.len());
    try std.testing.expectEqual(true, try bools.get(0));
    try std.testing.expectEqual(false, try bools.get(1));
    try std.testing.expectEqual(true, try bools.get(2));
    try std.testing.expectEqual(false, try bools.get(3));
    try std.testing.expectEqual(true, try bools.get(4));

    const f32s = try root.readF32List(8);
    try std.testing.expectEqual(@as(u32, 3), f32s.len());
    try std.testing.expectApproxEqAbs(@as(f32, 1.25), try f32s.get(0), 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, 2.5), try f32s.get(1), 0.0001);
    try std.testing.expectApproxEqAbs(@as(f32, -3.75), try f32s.get(2), 0.0001);

    const f64s = try root.readF64List(9);
    try std.testing.expectEqual(@as(u32, 2), f64s.len());
    try std.testing.expectApproxEqAbs(@as(f64, 1.125), try f64s.get(0), 0.0001);
    try std.testing.expectApproxEqAbs(@as(f64, -2.25), try f64s.get(1), 0.0001);

    const u16_lists = try root.readPointerList(10);
    try std.testing.expectEqual(@as(u32, 2), u16_lists.len());
    const list0 = try u16_lists.getU16List(0);
    try std.testing.expectEqual(@as(u32, 2), list0.len());
    try std.testing.expectEqual(@as(u16, 7), try list0.get(0));
    try std.testing.expectEqual(@as(u16, 8), try list0.get(1));
    const list1 = try u16_lists.getU16List(1);
    try std.testing.expectEqual(@as(u32, 3), list1.len());
    try std.testing.expectEqual(@as(u16, 9), try list1.get(0));
    try std.testing.expectEqual(@as(u16, 10), try list1.get(1));
    try std.testing.expectEqual(@as(u16, 11), try list1.get(2));
}

fn verifyWidgetMessage(bytes: []const u8) !void {
    const allocator = std.testing.allocator;
    var msg = try message.Message.init(allocator, bytes);
    defer msg.deinit();
    const root = try msg.getRootStruct();
    try verifyWidgetRoot(root);
}

test "Interop: pycapnp single-segment fixture" {
    const allocator = std.testing.allocator;
    const bytes = try readFixture(allocator, "tests/interop/fixture_single.bin");
    defer allocator.free(bytes);
    try verifyWidgetMessage(bytes);
}

test "Interop: pycapnp multi-segment fixture" {
    const allocator = std.testing.allocator;
    const bytes = try readFixture(allocator, "tests/interop/fixture_far.bin");
    defer allocator.free(bytes);
    try verifyWidgetMessage(bytes);
}

test "Interop: pycapnp packed single-segment fixture" {
    const allocator = std.testing.allocator;
    const bytes = try readFixture(allocator, "tests/interop/fixture_single_packed.bin");
    defer allocator.free(bytes);
    var msg = try message.Message.initPacked(allocator, bytes);
    defer msg.deinit();
    const root = try msg.getRootStruct();
    try verifyWidgetRoot(root);
}

test "Interop: pycapnp packed multi-segment fixture" {
    const allocator = std.testing.allocator;
    const bytes = try readFixture(allocator, "tests/interop/fixture_far_packed.bin");
    defer allocator.free(bytes);
    var msg = try message.Message.initPacked(allocator, bytes);
    defer msg.deinit();
    const root = try msg.getRootStruct();
    try verifyWidgetRoot(root);
}
