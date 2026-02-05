const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const schema = capnpc.schema;
const request_reader = capnpc.request;
const json = std.json;

const Fixture = struct {
    path: []const u8,
    is_packed: bool,
};

const Context = struct {
    allocator: std.mem.Allocator,
    nodes: []schema.Node,
};

fn readFileAlloc(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    return try file.readToEndAlloc(allocator, std.math.maxInt(usize));
}

fn loadJson(allocator: std.mem.Allocator, path: []const u8) !json.Parsed(json.Value) {
    const bytes = try readFileAlloc(allocator, path);
    defer allocator.free(bytes);
    return try json.parseFromSlice(json.Value, allocator, bytes, .{ .allocate = .alloc_always });
}

fn loadCodeGeneratorRequest(allocator: std.mem.Allocator) !schema.CodeGeneratorRequest {
    const argv = [_][]const u8{
        "capnp",
        "compile",
        "--no-standard-import",
        "-Itests/capnp_testdata",
        "-o-",
        "tests/capnp_testdata/test.capnp",
    };

    var child = std.process.Child.init(&argv, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    child.spawn() catch |err| {
        return switch (err) {
            error.FileNotFound => error.SkipZigTest,
            else => err,
        };
    };

    const stdout_bytes = try child.stdout.?.readToEndAlloc(allocator, 32 * 1024 * 1024);
    const stderr_bytes = try child.stderr.?.readToEndAlloc(allocator, 32 * 1024 * 1024);

    const term = try child.wait();
    switch (term) {
        .Exited => |code| {
            if (code != 0) {
                std.debug.print("capnp compile failed: {s}\n", .{stderr_bytes});
                allocator.free(stdout_bytes);
                allocator.free(stderr_bytes);
                return error.CapnpCompileFailed;
            }
        },
        else => {
            std.debug.print("capnp compile failed: unexpected termination\n", .{});
            allocator.free(stdout_bytes);
            allocator.free(stderr_bytes);
            return error.CapnpCompileFailed;
        },
    }

    allocator.free(stderr_bytes);

    const request = try request_reader.parseCodeGeneratorRequest(allocator, stdout_bytes);
    allocator.free(stdout_bytes);
    return request;
}

fn findNodeById(nodes: []schema.Node, id: schema.Id) ?*const schema.Node {
    for (nodes) |*node| {
        if (node.id == id) return node;
    }
    return null;
}

fn findStructBySuffix(nodes: []schema.Node, suffix: []const u8) ?*const schema.Node {
    for (nodes) |*node| {
        if (node.kind != .@"struct") continue;
        if (std.mem.endsWith(u8, node.display_name, suffix)) return node;
    }
    return null;
}

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

fn expectNull(value: json.Value) !void {
    switch (value) {
        .null => return,
        else => return error.InvalidJsonFixture,
    }
}

fn expectI64(value: json.Value) !i64 {
    return switch (value) {
        .integer => |i| i,
        .float => |f| @as(i64, @intFromFloat(f)),
        .string => |s| try std.fmt.parseInt(i64, s, 10),
        else => error.InvalidJsonFixture,
    };
}

fn expectU64(value: json.Value) !u64 {
    return switch (value) {
        .integer => |i| if (i < 0) return error.InvalidJsonFixture else @as(u64, @intCast(i)),
        .float => |f| if (f < 0) return error.InvalidJsonFixture else @as(u64, @intFromFloat(f)),
        .string => |s| try std.fmt.parseInt(u64, s, 10),
        else => error.InvalidJsonFixture,
    };
}

fn expectF64(value: json.Value) !f64 {
    return switch (value) {
        .float => |f| f,
        .integer => |i| @as(f64, @floatFromInt(i)),
        .string => |s| blk: {
            if (std.mem.eql(u8, s, "Infinity")) break :blk std.math.inf(f64);
            if (std.mem.eql(u8, s, "-Infinity")) break :blk -std.math.inf(f64);
            if (std.mem.eql(u8, s, "NaN")) break :blk std.math.nan(f64);
            break :blk try std.fmt.parseFloat(f64, s);
        },
        else => error.InvalidJsonFixture,
    };
}

fn expectFloatApprox(expected: f64, actual: f64, rel: f64, abs: f64) !void {
    if (std.math.isNan(expected)) {
        try std.testing.expect(std.math.isNan(actual));
        return;
    }
    if (std.math.isInf(expected)) {
        try std.testing.expect(std.math.isInf(actual));
        if (std.math.isInf(actual)) {
            try std.testing.expect(std.math.isNegativeInf(expected) == std.math.isNegativeInf(actual));
        }
        return;
    }
    const diff = @abs(actual - expected);
    const tol = @max(abs, rel * @abs(expected));
    try std.testing.expect(diff <= tol);
}

fn dataByteOffset(typ: schema.Type, offset: u32) u32 {
    return switch (typ) {
        .bool => offset / 8,
        .int8, .uint8 => offset,
        .int16, .uint16, .@"enum" => offset * 2,
        .int32, .uint32, .float32 => offset * 4,
        .int64, .uint64, .float64 => offset * 8,
        else => offset,
    };
}

fn enumName(nodes: []schema.Node, type_id: schema.Id, value: u16) ?[]const u8 {
    const node = findNodeById(nodes, type_id) orelse return null;
    const enum_info = node.enum_node orelse return null;
    if (value >= enum_info.enumerants.len) return null;
    return enum_info.enumerants[value].name;
}

fn compareDefault(reader: message.StructReader, slot: schema.FieldSlot) anyerror!void {
    const byte_offset = dataByteOffset(slot.type, slot.offset);
    switch (slot.type) {
        .void => {},
        .bool => {
            const bit_offset: u3 = @intCast(slot.offset % 8);
            try std.testing.expectEqual(false, reader.readBool(byte_offset, bit_offset));
        },
        .int8 => try std.testing.expectEqual(@as(i8, 0), @as(i8, @bitCast(reader.readU8(byte_offset)))),
        .int16 => try std.testing.expectEqual(@as(i16, 0), @as(i16, @bitCast(reader.readU16(byte_offset)))),
        .int32 => try std.testing.expectEqual(@as(i32, 0), @as(i32, @bitCast(reader.readU32(byte_offset)))),
        .int64 => try std.testing.expectEqual(@as(i64, 0), @as(i64, @bitCast(reader.readU64(byte_offset)))),
        .uint8 => try std.testing.expectEqual(@as(u8, 0), reader.readU8(byte_offset)),
        .uint16 => try std.testing.expectEqual(@as(u16, 0), reader.readU16(byte_offset)),
        .uint32 => try std.testing.expectEqual(@as(u32, 0), reader.readU32(byte_offset)),
        .uint64 => try std.testing.expectEqual(@as(u64, 0), reader.readU64(byte_offset)),
        .float32 => try std.testing.expectEqual(@as(f32, 0), @as(f32, @bitCast(reader.readU32(byte_offset)))),
        .float64 => try std.testing.expectEqual(@as(f64, 0), @as(f64, @bitCast(reader.readU64(byte_offset)))),
        .@"enum" => try std.testing.expectEqual(@as(u16, 0), reader.readU16(byte_offset)),
        .text, .data, .list, .@"struct", .interface, .any_pointer => {
            try std.testing.expect(reader.isPointerNull(slot.offset));
        },
    }
}

fn compareStruct(ctx: *const Context, node: *const schema.Node, reader: message.StructReader, expected: json.Value) anyerror!void {
    const obj = try expectObject(expected);
    const struct_info = node.struct_node orelse return error.InvalidSchema;

    for (struct_info.fields) |field| {
        if (field.slot == null and field.group == null) continue;

        const expected_value_opt = obj.get(field.name);
        if (expected_value_opt == null) {
            if (field.slot) |slot| {
                try compareDefault(reader, slot);
            }
            continue;
        }

        if (field.slot) |slot| {
            try compareSlot(ctx, reader, slot, expected_value_opt.?);
        } else if (field.group) |group| {
            const group_node = findNodeById(ctx.nodes, group.type_id) orelse return error.InvalidSchema;
            try compareStruct(ctx, group_node, reader, expected_value_opt.?);
        }
    }
}

fn compareSlot(ctx: *const Context, reader: message.StructReader, slot: schema.FieldSlot, expected: json.Value) anyerror!void {
    const byte_offset = dataByteOffset(slot.type, slot.offset);
    switch (slot.type) {
        .void => try expectNull(expected),
        .bool => {
            const bit_offset: u3 = @intCast(slot.offset % 8);
            const actual = reader.readBool(byte_offset, bit_offset);
            const expected_bool = try expectBool(expected);
            try std.testing.expectEqual(expected_bool, actual);
        },
        .int8 => {
            const actual = @as(i8, @bitCast(reader.readU8(byte_offset)));
            const expected_i = try expectI64(expected);
            try std.testing.expectEqual(@as(i8, @intCast(expected_i)), actual);
        },
        .int16 => {
            const actual = @as(i16, @bitCast(reader.readU16(byte_offset)));
            const expected_i = try expectI64(expected);
            try std.testing.expectEqual(@as(i16, @intCast(expected_i)), actual);
        },
        .int32 => {
            const actual = @as(i32, @bitCast(reader.readU32(byte_offset)));
            const expected_i = try expectI64(expected);
            try std.testing.expectEqual(@as(i32, @intCast(expected_i)), actual);
        },
        .int64 => {
            const actual = @as(i64, @bitCast(reader.readU64(byte_offset)));
            const expected_i = try expectI64(expected);
            try std.testing.expectEqual(expected_i, actual);
        },
        .uint8 => {
            const actual = reader.readU8(byte_offset);
            const expected_u = try expectU64(expected);
            try std.testing.expectEqual(@as(u8, @intCast(expected_u)), actual);
        },
        .uint16 => {
            const actual = reader.readU16(byte_offset);
            const expected_u = try expectU64(expected);
            try std.testing.expectEqual(@as(u16, @intCast(expected_u)), actual);
        },
        .uint32 => {
            const actual = reader.readU32(byte_offset);
            const expected_u = try expectU64(expected);
            try std.testing.expectEqual(@as(u32, @intCast(expected_u)), actual);
        },
        .uint64 => {
            const actual = reader.readU64(byte_offset);
            const expected_u = try expectU64(expected);
            try std.testing.expectEqual(expected_u, actual);
        },
        .float32 => {
            const actual = @as(f32, @bitCast(reader.readU32(byte_offset)));
            const expected_f = try expectF64(expected);
            try expectFloatApprox(expected_f, actual, 1e-5, 1e-6);
        },
        .float64 => {
            const actual = @as(f64, @bitCast(reader.readU64(byte_offset)));
            const expected_f = try expectF64(expected);
            try expectFloatApprox(expected_f, actual, 1e-12, 1e-9);
        },
        .text => {
            const actual = try reader.readText(slot.offset);
            const expected_text = try expectString(expected);
            try std.testing.expectEqualStrings(expected_text, actual);
        },
        .data => {
            const actual = try reader.readData(slot.offset);
            const expected_arr = try expectArray(expected);
            try std.testing.expectEqual(expected_arr.len, actual.len);
            for (expected_arr, 0..) |item, idx| {
                const b = try expectU64(item);
                try std.testing.expectEqual(@as(u8, @intCast(b)), actual[idx]);
            }
        },
        .@"enum" => |enum_info| {
            const actual = reader.readU16(byte_offset);
            const expected_name = try expectString(expected);
            const name = enumName(ctx.nodes, enum_info.type_id, actual) orelse return error.InvalidEnumValue;
            try std.testing.expectEqualStrings(expected_name, name);
        },
        .@"struct" => |struct_info| {
            const expected_obj = expected;
            const struct_node = findNodeById(ctx.nodes, struct_info.type_id) orelse return error.InvalidSchema;
            const actual = try reader.readStruct(slot.offset);
            try compareStruct(ctx, struct_node, actual, expected_obj);
        },
        .list => |list_info| {
            try compareList(ctx, reader, slot.offset, list_info.element_type.*, expected);
        },
        .interface => {
            try expectNull(expected);
            try std.testing.expect(reader.isPointerNull(slot.offset));
        },
        .any_pointer => {
            try expectNull(expected);
            try std.testing.expect(reader.isPointerNull(slot.offset));
        },
    }
}

fn compareList(ctx: *const Context, reader: message.StructReader, pointer_index: u32, element_type: schema.Type, expected: json.Value) anyerror!void {
    const expected_arr = try expectArray(expected);
    switch (element_type) {
        .void => {
            const list = try reader.readVoidList(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
        },
        .bool => {
            const list = try reader.readBoolList(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_bool = try expectBool(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try std.testing.expectEqual(expected_bool, actual);
            }
        },
        .int8 => {
            const list = try reader.readI8List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_i = try expectI64(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try std.testing.expectEqual(@as(i8, @intCast(expected_i)), actual);
            }
        },
        .int16 => {
            const list = try reader.readI16List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_i = try expectI64(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try std.testing.expectEqual(@as(i16, @intCast(expected_i)), actual);
            }
        },
        .int32 => {
            const list = try reader.readI32List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_i = try expectI64(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try std.testing.expectEqual(@as(i32, @intCast(expected_i)), actual);
            }
        },
        .int64 => {
            const list = try reader.readI64List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_i = try expectI64(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try std.testing.expectEqual(expected_i, actual);
            }
        },
        .uint8 => {
            const list = try reader.readU8List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_u = try expectU64(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try std.testing.expectEqual(@as(u8, @intCast(expected_u)), actual);
            }
        },
        .uint16 => {
            const list = try reader.readU16List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_u = try expectU64(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try std.testing.expectEqual(@as(u16, @intCast(expected_u)), actual);
            }
        },
        .uint32 => {
            const list = try reader.readU32List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_u = try expectU64(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try std.testing.expectEqual(@as(u32, @intCast(expected_u)), actual);
            }
        },
        .uint64 => {
            const list = try reader.readU64List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_u = try expectU64(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try std.testing.expectEqual(expected_u, actual);
            }
        },
        .float32 => {
            const list = try reader.readF32List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_f = try expectF64(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try expectFloatApprox(expected_f, actual, 1e-5, 1e-6);
            }
        },
        .float64 => {
            const list = try reader.readF64List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_f = try expectF64(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try expectFloatApprox(expected_f, actual, 1e-12, 1e-9);
            }
        },
        .text => {
            const list = try reader.readTextList(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_text = try expectString(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                try std.testing.expectEqualStrings(expected_text, actual);
            }
        },
        .data => {
            const list = try reader.readPointerList(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_bytes = try expectArray(item);
                const actual = try list.getData(@as(u32, @intCast(idx)));
                try std.testing.expectEqual(expected_bytes.len, actual.len);
                for (expected_bytes, 0..) |b, b_idx| {
                    const expected_u = try expectU64(b);
                    try std.testing.expectEqual(@as(u8, @intCast(expected_u)), actual[b_idx]);
                }
            }
        },
        .@"enum" => |enum_info| {
            const list = try reader.readU16List(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            for (expected_arr, 0..) |item, idx| {
                const expected_name = try expectString(item);
                const actual = try list.get(@as(u32, @intCast(idx)));
                const name = enumName(ctx.nodes, enum_info.type_id, actual) orelse return error.InvalidEnumValue;
                try std.testing.expectEqualStrings(expected_name, name);
            }
        },
        .@"struct" => |struct_info| {
            const list = try reader.readStructList(pointer_index);
            try std.testing.expectEqual(@as(u32, @intCast(expected_arr.len)), list.len());
            const struct_node = findNodeById(ctx.nodes, struct_info.type_id) orelse return error.InvalidSchema;
            for (expected_arr, 0..) |item, idx| {
                const actual = try list.get(@as(u32, @intCast(idx)));
                try compareStruct(ctx, struct_node, actual, item);
            }
        },
        .list => {
            return error.UnsupportedType;
        },
        .interface => {
            return error.UnsupportedType;
        },
        .any_pointer => {
            return error.UnsupportedType;
        },
    }
}

test "capnp testdata: TestAllTypes fixtures" {
    const allocator = std.testing.allocator;

    const request = try loadCodeGeneratorRequest(allocator);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    const root_node = findStructBySuffix(request.nodes, "TestAllTypes") orelse return error.InvalidSchema;

    var parsed = try loadJson(allocator, "tests/capnp_testdata/testdata/pretty.json");
    defer parsed.deinit();

    const ctx = Context{ .allocator = allocator, .nodes = request.nodes };

    const fixtures = [_]Fixture{
        .{ .path = "tests/capnp_testdata/testdata/binary", .is_packed = false },
        .{ .path = "tests/capnp_testdata/testdata/segmented", .is_packed = false },
        .{ .path = "tests/capnp_testdata/testdata/packed", .is_packed = true },
        .{ .path = "tests/capnp_testdata/testdata/segmented-packed", .is_packed = true },
    };

    for (fixtures) |fixture| {
        const bytes = try readFileAlloc(allocator, fixture.path);
        defer allocator.free(bytes);

        var msg = if (fixture.is_packed)
            try message.Message.initPacked(allocator, bytes)
        else
            try message.Message.init(allocator, bytes);
        defer msg.deinit();

        const root = try msg.getRootStruct();
        try compareStruct(&ctx, root_node, root, parsed.value);
    }
}
