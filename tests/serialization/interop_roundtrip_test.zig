const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;
const json = std.json;

fn expectObject(value: json.Value) !json.ObjectMap {
    return switch (value) {
        .object => |obj| obj,
        else => error.InvalidJsonFixture,
    };
}

fn expectArray(value: json.Value) ![]const json.Value {
    return switch (value) {
        .array => |arr| arr.items,
        else => error.InvalidJsonFixture,
    };
}

fn expectString(value: json.Value) ![]const u8 {
    return switch (value) {
        .string => |s| s,
        else => error.InvalidJsonFixture,
    };
}

fn expectBool(value: json.Value) !bool {
    return switch (value) {
        .bool => |b| b,
        else => error.InvalidJsonFixture,
    };
}

fn expectInt(value: json.Value) !i64 {
    return switch (value) {
        .integer => |i| i,
        .float => |f| @as(i64, @intFromFloat(f)),
        else => error.InvalidJsonFixture,
    };
}

fn expectFloat(value: json.Value) !f64 {
    return switch (value) {
        .float => |f| f,
        .integer => |i| @as(f64, @floatFromInt(i)),
        else => error.InvalidJsonFixture,
    };
}

fn getField(obj: json.ObjectMap, key: []const u8) !json.Value {
    return obj.get(key) orelse error.InvalidJsonFixture;
}

fn verifyWidgetAgainstJson(root: message.StructReader, expected: json.Value) !void {
    const obj = try expectObject(expected);

    const id_value = try expectInt(try getField(obj, "id"));
    try std.testing.expectEqual(@as(u32, @intCast(id_value)), root.readU32(0));

    const name = try expectString(try getField(obj, "name"));
    try std.testing.expectEqualStrings(name, try root.readText(0));

    const points_expected = try expectArray(try getField(obj, "points"));
    const points = try root.readStructList(1);
    try std.testing.expectEqual(@as(u32, @intCast(points_expected.len)), points.len());
    for (points_expected, 0..) |point_value, idx| {
        const pair = try expectArray(point_value);
        if (pair.len != 2) return error.InvalidJsonFixture;
        const x = @as(u32, @intCast(try expectInt(pair[0])));
        const y = @as(u32, @intCast(try expectInt(pair[1])));
        const point = try points.get(@as(u32, @intCast(idx)));
        try std.testing.expectEqual(x, point.readU32(0));
        try std.testing.expectEqual(y, point.readU32(4));
    }

    const tags_expected = try expectArray(try getField(obj, "tags"));
    const tags = try root.readTextList(2);
    try std.testing.expectEqual(@as(u32, @intCast(tags_expected.len)), tags.len());
    for (tags_expected, 0..) |tag_value, idx| {
        const tag = try expectString(tag_value);
        try std.testing.expectEqualStrings(tag, try tags.get(@as(u32, @intCast(idx))));
    }

    const bytes_expected = try expectArray(try getField(obj, "bytes"));
    const bytes_actual = try root.readData(3);
    try std.testing.expectEqual(bytes_expected.len, bytes_actual.len);
    for (bytes_expected, 0..) |byte_value, idx| {
        const b = @as(u8, @intCast(try expectInt(byte_value)));
        try std.testing.expectEqual(b, bytes_actual[idx]);
    }

    const u16s_expected = try expectArray(try getField(obj, "u16s"));
    const u16s = try root.readU16List(4);
    try std.testing.expectEqual(@as(u32, @intCast(u16s_expected.len)), u16s.len());
    for (u16s_expected, 0..) |value, idx| {
        const expected_value = @as(u16, @intCast(try expectInt(value)));
        try std.testing.expectEqual(expected_value, try u16s.get(@as(u32, @intCast(idx))));
    }

    const u32s_expected = try expectArray(try getField(obj, "u32s"));
    const u32s = try root.readU32List(5);
    try std.testing.expectEqual(@as(u32, @intCast(u32s_expected.len)), u32s.len());
    for (u32s_expected, 0..) |value, idx| {
        const expected_value = @as(u32, @intCast(try expectInt(value)));
        try std.testing.expectEqual(expected_value, try u32s.get(@as(u32, @intCast(idx))));
    }

    const u64s_expected = try expectArray(try getField(obj, "u64s"));
    const u64s = try root.readU64List(6);
    try std.testing.expectEqual(@as(u32, @intCast(u64s_expected.len)), u64s.len());
    for (u64s_expected, 0..) |value, idx| {
        const expected_value = @as(u64, @intCast(try expectInt(value)));
        try std.testing.expectEqual(expected_value, try u64s.get(@as(u32, @intCast(idx))));
    }

    const bools_expected = try expectArray(try getField(obj, "bools"));
    const bools = try root.readBoolList(7);
    try std.testing.expectEqual(@as(u32, @intCast(bools_expected.len)), bools.len());
    for (bools_expected, 0..) |value, idx| {
        const expected_value = try expectBool(value);
        try std.testing.expectEqual(expected_value, try bools.get(@as(u32, @intCast(idx))));
    }

    const f32s_expected = try expectArray(try getField(obj, "f32s"));
    const f32s = try root.readF32List(8);
    try std.testing.expectEqual(@as(u32, @intCast(f32s_expected.len)), f32s.len());
    for (f32s_expected, 0..) |value, idx| {
        const expected_value = @as(f32, @floatCast(try expectFloat(value)));
        try std.testing.expectApproxEqAbs(expected_value, try f32s.get(@as(u32, @intCast(idx))), 0.0001);
    }

    const f64s_expected = try expectArray(try getField(obj, "f64s"));
    const f64s = try root.readF64List(9);
    try std.testing.expectEqual(@as(u32, @intCast(f64s_expected.len)), f64s.len());
    for (f64s_expected, 0..) |value, idx| {
        const expected_value = try expectFloat(value);
        try std.testing.expectApproxEqAbs(expected_value, try f64s.get(@as(u32, @intCast(idx))), 0.000001);
    }

    const u16_lists_expected = try expectArray(try getField(obj, "u16Lists"));
    const u16_lists = try root.readPointerList(10);
    try std.testing.expectEqual(@as(u32, @intCast(u16_lists_expected.len)), u16_lists.len());
    for (u16_lists_expected, 0..) |value, idx| {
        const inner_expected = try expectArray(value);
        const list = try u16_lists.getU16List(@as(u32, @intCast(idx)));
        try std.testing.expectEqual(@as(u32, @intCast(inner_expected.len)), list.len());
        for (inner_expected, 0..) |inner_value, inner_idx| {
            const expected_value = @as(u16, @intCast(try expectInt(inner_value)));
            try std.testing.expectEqual(expected_value, try list.get(@as(u32, @intCast(inner_idx))));
        }
    }
}

test "Interop: Zig -> pycapnp round trip" {
    const allocator = std.testing.allocator;

    const python_path = ".venv/bin/python";
    const script_path = "tests/interop/verify_pycapnp.py";

    std.fs.cwd().access(python_path, .{}) catch return error.SkipZigTest;
    std.fs.cwd().access(script_path, .{}) catch return error.SkipZigTest;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(1, 11);
    root.writeU32(0, 123);
    try root.writeText(0, "widget");

    var points = try root.writeStructList(1, 3, 1, 0);
    var p0 = try points.get(0);
    p0.writeU32(0, 1);
    p0.writeU32(4, 10);
    var p1 = try points.get(1);
    p1.writeU32(0, 2);
    p1.writeU32(4, 20);
    var p2 = try points.get(2);
    p2.writeU32(0, 3);
    p2.writeU32(4, 30);

    var tags = try root.writeTextList(2, 2);
    try tags.set(0, "alpha");
    try tags.set(1, "beta");

    try root.writeData(3, &[_]u8{ 1, 2, 3, 4, 5 });

    var u16s = try root.writeU16List(4, 3);
    try u16s.set(0, 10);
    try u16s.set(1, 20);
    try u16s.set(2, 30);

    var u32s = try root.writeU32List(5, 2);
    try u32s.set(0, 1000);
    try u32s.set(1, 2000);

    var u64s = try root.writeU64List(6, 2);
    try u64s.set(0, 123456789);
    try u64s.set(1, 987654321);

    var bools = try root.writeBoolList(7, 5);
    try bools.set(0, true);
    try bools.set(1, false);
    try bools.set(2, true);
    try bools.set(3, false);
    try bools.set(4, true);

    var f32s = try root.writeF32List(8, 3);
    try f32s.set(0, 1.25);
    try f32s.set(1, 2.5);
    try f32s.set(2, -3.75);

    var f64s = try root.writeF64List(9, 2);
    try f64s.set(0, 1.125);
    try f64s.set(1, -2.25);

    var u16_lists = try root.writePointerList(10, 2);
    var list0 = try u16_lists.initU16List(0, 2);
    try list0.set(0, 7);
    try list0.set(1, 8);
    var list1 = try u16_lists.initU16List(1, 3);
    try list1.set(0, 9);
    try list1.set(1, 10);
    try list1.set(2, 11);

    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile("zig_roundtrip.bin", .{});
    defer file.close();
    try file.writeAll(bytes);

    const abs_path = try tmp.dir.realpathAlloc(allocator, "zig_roundtrip.bin");
    defer allocator.free(abs_path);

    var child = std.process.Child.init(&[_][]const u8{
        python_path,
        script_path,
        abs_path,
    }, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;

    const term = try child.spawnAndWait();
    try std.testing.expect(term == .Exited and term.Exited == 0);
}

test "Interop: Zig -> pycapnp packed round trip" {
    const allocator = std.testing.allocator;

    const python_path = ".venv/bin/python";
    const script_path = "tests/interop/verify_pycapnp.py";

    std.fs.cwd().access(python_path, .{}) catch return error.SkipZigTest;
    std.fs.cwd().access(script_path, .{}) catch return error.SkipZigTest;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(1, 11);
    root.writeU32(0, 123);
    try root.writeText(0, "widget");

    var points = try root.writeStructList(1, 3, 1, 0);
    var p0 = try points.get(0);
    p0.writeU32(0, 1);
    p0.writeU32(4, 10);
    var p1 = try points.get(1);
    p1.writeU32(0, 2);
    p1.writeU32(4, 20);
    var p2 = try points.get(2);
    p2.writeU32(0, 3);
    p2.writeU32(4, 30);

    var tags = try root.writeTextList(2, 2);
    try tags.set(0, "alpha");
    try tags.set(1, "beta");

    try root.writeData(3, &[_]u8{ 1, 2, 3, 4, 5 });

    var u16s = try root.writeU16List(4, 3);
    try u16s.set(0, 10);
    try u16s.set(1, 20);
    try u16s.set(2, 30);

    var u32s = try root.writeU32List(5, 2);
    try u32s.set(0, 1000);
    try u32s.set(1, 2000);

    var u64s = try root.writeU64List(6, 2);
    try u64s.set(0, 123456789);
    try u64s.set(1, 987654321);

    var bools = try root.writeBoolList(7, 5);
    try bools.set(0, true);
    try bools.set(1, false);
    try bools.set(2, true);
    try bools.set(3, false);
    try bools.set(4, true);

    var f32s = try root.writeF32List(8, 3);
    try f32s.set(0, 1.25);
    try f32s.set(1, 2.5);
    try f32s.set(2, -3.75);

    var f64s = try root.writeF64List(9, 2);
    try f64s.set(0, 1.125);
    try f64s.set(1, -2.25);

    var u16_lists = try root.writePointerList(10, 2);
    var list0 = try u16_lists.initU16List(0, 2);
    try list0.set(0, 7);
    try list0.set(1, 8);
    var list1 = try u16_lists.initU16List(1, 3);
    try list1.set(0, 9);
    try list1.set(1, 10);
    try list1.set(2, 11);

    const packed_bytes = try builder.toPackedBytes();
    defer allocator.free(packed_bytes);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile("zig_roundtrip_packed.bin", .{});
    defer file.close();
    try file.writeAll(packed_bytes);

    const abs_path = try tmp.dir.realpathAlloc(allocator, "zig_roundtrip_packed.bin");
    defer allocator.free(abs_path);

    var child = std.process.Child.init(&[_][]const u8{
        python_path,
        script_path,
        "--packed",
        abs_path,
    }, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Inherit;
    child.stderr_behavior = .Inherit;

    const term = try child.spawnAndWait();
    try std.testing.expect(term == .Exited and term.Exited == 0);
}

fn runRandomFixture(use_packed: bool) !void {
    const allocator = std.testing.allocator;

    const python_path = ".venv/bin/python";
    const script_path = "tests/interop/generate_random_fixture.py";

    std.fs.cwd().access(python_path, .{}) catch return error.SkipZigTest;
    std.fs.cwd().access(script_path, .{}) catch return error.SkipZigTest;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const tmp_root = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(tmp_root);

    const seeds = [_]u32{ 1, 2, 3 };
    for (seeds) |seed| {
        const bin_name = try std.fmt.allocPrint(allocator, "fixture_{d}.bin", .{seed});
        defer allocator.free(bin_name);
        const json_name = try std.fmt.allocPrint(allocator, "fixture_{d}.json", .{seed});
        defer allocator.free(json_name);

        const bin_path = try std.fs.path.join(allocator, &.{ tmp_root, bin_name });
        defer allocator.free(bin_path);
        const json_path = try std.fs.path.join(allocator, &.{ tmp_root, json_name });
        defer allocator.free(json_path);

        var argv = std.ArrayList([]const u8){};
        defer argv.deinit(allocator);
        try argv.append(allocator, python_path);
        try argv.append(allocator, script_path);
        try argv.append(allocator, "--seed");
        const seed_str = try std.fmt.allocPrint(allocator, "{d}", .{seed});
        defer allocator.free(seed_str);
        try argv.append(allocator, seed_str);
        try argv.append(allocator, "--out-bin");
        try argv.append(allocator, bin_path);
        try argv.append(allocator, "--out-json");
        try argv.append(allocator, json_path);
        if (use_packed) {
            try argv.append(allocator, "--packed");
        }

        var child = std.process.Child.init(argv.items, allocator);
        child.stdin_behavior = .Ignore;
        child.stdout_behavior = .Inherit;
        child.stderr_behavior = .Inherit;
        const term = try child.spawnAndWait();
        try std.testing.expect(term == .Exited and term.Exited == 0);

        var bin_file = try std.fs.openFileAbsolute(bin_path, .{});
        defer bin_file.close();
        const bin_bytes = try bin_file.readToEndAlloc(allocator, std.math.maxInt(usize));
        defer allocator.free(bin_bytes);

        var json_file = try std.fs.openFileAbsolute(json_path, .{});
        defer json_file.close();
        const json_bytes = try json_file.readToEndAlloc(allocator, std.math.maxInt(usize));
        defer allocator.free(json_bytes);

        var parsed = try json.parseFromSlice(json.Value, allocator, json_bytes, .{});
        defer parsed.deinit();

        var msg = if (use_packed)
            try message.Message.initPacked(allocator, bin_bytes)
        else
            try message.Message.init(allocator, bin_bytes);
        defer msg.deinit();

        const root = try msg.getRootStruct();
        try verifyWidgetAgainstJson(root, parsed.value);
    }
}

test "Interop: pycapnp random fixture" {
    try runRandomFixture(false);
}

test "Interop: pycapnp random packed fixture" {
    try runRandomFixture(true);
}
