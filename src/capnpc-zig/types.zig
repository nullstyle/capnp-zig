const std = @import("std");
const schema = @import("../serialization/schema.zig");

/// Zig keywords that must be escaped with @"..." when used as identifiers.
pub const zig_keywords = std.StaticStringMap(void).initComptime(.{
    .{ "addrspace", {} }, .{ "align", {} },       .{ "allowzero", {} },
    .{ "and", {} },       .{ "anyframe", {} },    .{ "anytype", {} },
    .{ "asm", {} },       .{ "async", {} },       .{ "await", {} },
    .{ "break", {} },     .{ "callconv", {} },    .{ "catch", {} },
    .{ "comptime", {} },  .{ "const", {} },       .{ "continue", {} },
    .{ "defer", {} },     .{ "else", {} },        .{ "enum", {} },
    .{ "errdefer", {} },  .{ "error", {} },       .{ "export", {} },
    .{ "extern", {} },    .{ "fn", {} },          .{ "for", {} },
    .{ "if", {} },        .{ "inline", {} },      .{ "linksection", {} },
    .{ "noalias", {} },   .{ "nosuspend", {} },   .{ "noinline", {} },
    .{ "opaque", {} },    .{ "or", {} },          .{ "orelse", {} },
    .{ "packed", {} },    .{ "pub", {} },         .{ "resume", {} },
    .{ "return", {} },    .{ "struct", {} },      .{ "suspend", {} },
    .{ "switch", {} },    .{ "test", {} },        .{ "threadlocal", {} },
    .{ "try", {} },       .{ "type", {} },        .{ "undefined", {} },
    .{ "union", {} },     .{ "unreachable", {} }, .{ "usingnamespace", {} },
    .{ "var", {} },       .{ "volatile", {} },    .{ "while", {} },
    .{ "true", {} },      .{ "false", {} },       .{ "null", {} },
});

/// Escape a name with @"..." if it collides with a Zig keyword.
pub fn escapeZigKeyword(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    if (zig_keywords.has(name)) {
        return std.fmt.allocPrint(allocator, "@\"{s}\"", .{name});
    }
    return allocator.dupe(u8, name);
}

fn normalizeIdentifier(allocator: std.mem.Allocator, name: []const u8, capitalize_first: bool) ![]u8 {
    var result = std.ArrayList(u8){};
    errdefer result.deinit(allocator);

    var capitalize_next = capitalize_first;
    for (name) |c| {
        if (c == '_' or c == '$') {
            capitalize_next = true;
            continue;
        }

        if (capitalize_next) {
            try result.append(allocator, std.ascii.toUpper(c));
            capitalize_next = false;
        } else {
            try result.append(allocator, c);
        }
    }

    // If all characters were separators, the result is empty which would
    // produce an invalid Zig identifier. Fall back to a stable non-discard
    // identifier.
    if (result.items.len == 0) {
        if (capitalize_first) {
            try result.appendSlice(allocator, "Unnamed");
        } else {
            try result.appendSlice(allocator, "_unnamed");
        }
    }

    return result.toOwnedSlice(allocator);
}

pub fn identToZigValueName(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    return normalizeIdentifier(allocator, name, false);
}

pub fn identToZigTypeName(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    return normalizeIdentifier(allocator, name, true);
}

pub fn normalizeAndEscapeValueIdentifier(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    const normalized = try identToZigValueName(allocator, name);
    defer allocator.free(normalized);
    return escapeZigKeyword(allocator, normalized);
}

pub fn normalizeAndEscapeTypeIdentifier(allocator: std.mem.Allocator, name: []const u8) ![]const u8 {
    const normalized = try identToZigTypeName(allocator, name);
    defer allocator.free(normalized);
    return escapeZigKeyword(allocator, normalized);
}

/// Generate Zig type code for Cap'n Proto types
pub const TypeGenerator = struct {
    allocator: std.mem.Allocator,
    node_lookup_ctx: ?*anyopaque,
    node_lookup: ?*const fn (ctx: ?*anyopaque, id: schema.Id) ?*const schema.Node,

    pub fn init(allocator: std.mem.Allocator) TypeGenerator {
        return .{
            .allocator = allocator,
            .node_lookup_ctx = null,
            .node_lookup = null,
        };
    }

    pub fn initWithLookup(
        allocator: std.mem.Allocator,
        node_lookup: *const fn (ctx: ?*anyopaque, id: schema.Id) ?*const schema.Node,
        node_lookup_ctx: ?*anyopaque,
    ) TypeGenerator {
        return .{
            .allocator = allocator,
            .node_lookup_ctx = node_lookup_ctx,
            .node_lookup = node_lookup,
        };
    }

    fn getNode(self: *const TypeGenerator, id: schema.Id) ?*const schema.Node {
        const lookup = self.node_lookup orelse return null;
        return lookup(self.node_lookup_ctx, id);
    }

    /// Convert Cap'n Proto type to Zig type string
    pub fn typeToZig(self: *TypeGenerator, typ: schema.Type) ![]const u8 {
        return switch (typ) {
            .void => try self.allocator.dupe(u8, "void"),
            .bool => try self.allocator.dupe(u8, "bool"),
            .int8 => try self.allocator.dupe(u8, "i8"),
            .int16 => try self.allocator.dupe(u8, "i16"),
            .int32 => try self.allocator.dupe(u8, "i32"),
            .int64 => try self.allocator.dupe(u8, "i64"),
            .uint8 => try self.allocator.dupe(u8, "u8"),
            .uint16 => try self.allocator.dupe(u8, "u16"),
            .uint32 => try self.allocator.dupe(u8, "u32"),
            .uint64 => try self.allocator.dupe(u8, "u64"),
            .float32 => try self.allocator.dupe(u8, "f32"),
            .float64 => try self.allocator.dupe(u8, "f64"),
            .text => try self.allocator.dupe(u8, "[]const u8"),
            .data => try self.allocator.dupe(u8, "[]const u8"),
            .list => |list_info| try self.listReaderType(list_info.element_type.*),
            .@"enum" => |enum_info| try self.enumTypeName(enum_info.type_id),
            .@"struct" => |struct_info| try self.structReaderTypeName(struct_info.type_id),
            .interface => try self.allocator.dupe(u8, "message.Capability"),
            .any_pointer => try self.allocator.dupe(u8, "message.AnyPointerReader"),
        };
    }

    fn listReaderType(self: *TypeGenerator, elem_type: schema.Type) ![]const u8 {
        return switch (elem_type) {
            .void => try self.allocator.dupe(u8, "message.VoidListReader"),
            .bool => try self.allocator.dupe(u8, "message.BoolListReader"),
            .int8 => try self.allocator.dupe(u8, "message.I8ListReader"),
            .uint8 => try self.allocator.dupe(u8, "message.U8ListReader"),
            .int16 => try self.allocator.dupe(u8, "message.I16ListReader"),
            .uint16 => try self.allocator.dupe(u8, "message.U16ListReader"),
            .int32 => try self.allocator.dupe(u8, "message.I32ListReader"),
            .uint32 => try self.allocator.dupe(u8, "message.U32ListReader"),
            .float32 => try self.allocator.dupe(u8, "message.F32ListReader"),
            .int64 => try self.allocator.dupe(u8, "message.I64ListReader"),
            .uint64 => try self.allocator.dupe(u8, "message.U64ListReader"),
            .float64 => try self.allocator.dupe(u8, "message.F64ListReader"),
            .text => try self.allocator.dupe(u8, "message.TextListReader"),
            .@"enum" => try self.allocator.dupe(u8, "message.U16ListReader"),
            .@"struct" => try self.allocator.dupe(u8, "message.StructListReader"),
            else => try self.allocator.dupe(u8, "message.PointerListReader"),
        };
    }

    fn enumTypeName(self: *TypeGenerator, id: schema.Id) ![]const u8 {
        const node = self.getNode(id) orelse return try self.allocator.dupe(u8, "u16");
        if (node.kind != .@"enum") return try self.allocator.dupe(u8, "u16");
        const simple_name = self.getSimpleName(node);
        return self.toZigTypeName(simple_name);
    }

    fn structReaderTypeName(self: *TypeGenerator, id: schema.Id) ![]const u8 {
        const node = self.getNode(id) orelse return try self.allocator.dupe(u8, "message.StructReader");
        if (node.kind != .@"struct") return try self.allocator.dupe(u8, "message.StructReader");
        const simple_name = self.getSimpleName(node);
        const struct_name = try self.toZigTypeName(simple_name);
        defer self.allocator.free(struct_name);
        return std.fmt.allocPrint(self.allocator, "{s}.Reader", .{struct_name});
    }

    fn getSimpleName(self: *TypeGenerator, node: *const schema.Node) []const u8 {
        _ = self;
        const prefix_len = node.display_name_prefix_length;
        if (prefix_len >= node.display_name.len) return node.display_name;
        return node.display_name[prefix_len..];
    }

    /// Get the size in bytes for primitive types
    pub fn typeSize(typ: schema.Type) usize {
        return switch (typ) {
            .void => 0,
            .bool => 1,
            .int8, .uint8 => 1,
            .int16, .uint16 => 2,
            .int32, .uint32, .float32 => 4,
            .int64, .uint64, .float64 => 8,
            else => 8, // Pointers are 8 bytes
        };
    }

    /// Check if type is a pointer type
    pub fn isPointer(typ: schema.Type) bool {
        return switch (typ) {
            .text, .data, .list, .@"struct", .interface, .any_pointer => true,
            else => false,
        };
    }

    /// Convert Cap'n Proto identifier to Zig identifier (camelCase)
    pub fn toZigIdentifier(self: *TypeGenerator, name: []const u8) ![]const u8 {
        return identToZigValueName(self.allocator, name);
    }

    /// Convert Cap'n Proto identifier to Zig type name (PascalCase)
    pub fn toZigTypeName(self: *TypeGenerator, name: []const u8) ![]const u8 {
        return identToZigTypeName(self.allocator, name);
    }
};

test "escapeZigKeyword escapes usingnamespace" {
    const escaped = try escapeZigKeyword(std.testing.allocator, "usingnamespace");
    defer std.testing.allocator.free(escaped);
    try std.testing.expectEqualStrings("@\"usingnamespace\"", escaped);
}

test "escapeZigKeyword escapes common keywords" {
    const alloc = std.testing.allocator;

    const keywords = [_][]const u8{ "type", "error", "return", "struct", "enum", "union", "const", "var", "fn", "if", "else", "for", "while", "true", "false", "null", "try", "switch", "break", "continue", "test", "pub" };
    for (keywords) |kw| {
        const escaped = try escapeZigKeyword(alloc, kw);
        defer alloc.free(escaped);
        const expected = try std.fmt.allocPrint(alloc, "@\"{s}\"", .{kw});
        defer alloc.free(expected);
        try std.testing.expectEqualStrings(expected, escaped);
    }
}

test "escapeZigKeyword passes through non-keywords" {
    const alloc = std.testing.allocator;

    const non_keywords = [_][]const u8{ "myField", "fooBar", "hello", "x", "data_word_count", "Type", "Error", "Return" };
    for (non_keywords) |name| {
        const result = try escapeZigKeyword(alloc, name);
        defer alloc.free(result);
        try std.testing.expectEqualStrings(name, result);
    }
}

test "identToZigValueName converts underscores to camelCase" {
    const alloc = std.testing.allocator;

    const result1 = try identToZigValueName(alloc, "my_field_name");
    defer alloc.free(result1);
    try std.testing.expectEqualStrings("myFieldName", result1);

    const result2 = try identToZigValueName(alloc, "simple");
    defer alloc.free(result2);
    try std.testing.expectEqualStrings("simple", result2);

    const result3 = try identToZigValueName(alloc, "already_camel");
    defer alloc.free(result3);
    try std.testing.expectEqualStrings("alreadyCamel", result3);
}

test "identToZigValueName returns stable name for all-separator input" {
    const alloc = std.testing.allocator;

    const result1 = try identToZigValueName(alloc, "___");
    defer alloc.free(result1);
    try std.testing.expectEqualStrings("_unnamed", result1);

    const result2 = try identToZigValueName(alloc, "$");
    defer alloc.free(result2);
    try std.testing.expectEqualStrings("_unnamed", result2);

    const result3 = try identToZigValueName(alloc, "_$_");
    defer alloc.free(result3);
    try std.testing.expectEqualStrings("_unnamed", result3);
}

test "identToZigTypeName returns stable name for all-separator input" {
    const alloc = std.testing.allocator;

    const result = try identToZigTypeName(alloc, "___");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("Unnamed", result);
}

test "identToZigValueName handles dollar signs" {
    const alloc = std.testing.allocator;

    const result = try identToZigValueName(alloc, "foo$bar");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("fooBar", result);
}

test "identToZigTypeName converts to PascalCase" {
    const alloc = std.testing.allocator;

    const result1 = try identToZigTypeName(alloc, "my_struct");
    defer alloc.free(result1);
    try std.testing.expectEqualStrings("MyStruct", result1);

    const result2 = try identToZigTypeName(alloc, "simple");
    defer alloc.free(result2);
    try std.testing.expectEqualStrings("Simple", result2);

    const result3 = try identToZigTypeName(alloc, "foo_bar_baz");
    defer alloc.free(result3);
    try std.testing.expectEqualStrings("FooBarBaz", result3);
}

test "identToZigTypeName preserves existing capitals" {
    const alloc = std.testing.allocator;

    const result = try identToZigTypeName(alloc, "myType");
    defer alloc.free(result);
    try std.testing.expectEqualStrings("MyType", result);
}

test "normalizeAndEscapeValueIdentifier normalizes and escapes" {
    const normalized = try normalizeAndEscapeValueIdentifier(std.testing.allocator, "my_value");
    defer std.testing.allocator.free(normalized);
    try std.testing.expectEqualStrings("myValue", normalized);

    const escaped = try normalizeAndEscapeValueIdentifier(std.testing.allocator, "usingnamespace");
    defer std.testing.allocator.free(escaped);
    try std.testing.expectEqualStrings("@\"usingnamespace\"", escaped);
}

test "normalizeAndEscapeTypeIdentifier normalizes and escapes" {
    const alloc = std.testing.allocator;

    const result1 = try normalizeAndEscapeTypeIdentifier(alloc, "my_type");
    defer alloc.free(result1);
    try std.testing.expectEqualStrings("MyType", result1);

    // "Type" is not a keyword (keywords are lowercase)
    const result2 = try normalizeAndEscapeTypeIdentifier(alloc, "type");
    defer alloc.free(result2);
    try std.testing.expectEqualStrings("Type", result2);
}

test "TypeGenerator.typeSize returns correct sizes" {
    try std.testing.expectEqual(@as(usize, 0), TypeGenerator.typeSize(.void));
    try std.testing.expectEqual(@as(usize, 1), TypeGenerator.typeSize(.bool));
    try std.testing.expectEqual(@as(usize, 1), TypeGenerator.typeSize(.int8));
    try std.testing.expectEqual(@as(usize, 1), TypeGenerator.typeSize(.uint8));
    try std.testing.expectEqual(@as(usize, 2), TypeGenerator.typeSize(.int16));
    try std.testing.expectEqual(@as(usize, 2), TypeGenerator.typeSize(.uint16));
    try std.testing.expectEqual(@as(usize, 4), TypeGenerator.typeSize(.int32));
    try std.testing.expectEqual(@as(usize, 4), TypeGenerator.typeSize(.uint32));
    try std.testing.expectEqual(@as(usize, 4), TypeGenerator.typeSize(.float32));
    try std.testing.expectEqual(@as(usize, 8), TypeGenerator.typeSize(.int64));
    try std.testing.expectEqual(@as(usize, 8), TypeGenerator.typeSize(.uint64));
    try std.testing.expectEqual(@as(usize, 8), TypeGenerator.typeSize(.float64));
    try std.testing.expectEqual(@as(usize, 8), TypeGenerator.typeSize(.text));
    try std.testing.expectEqual(@as(usize, 8), TypeGenerator.typeSize(.data));
    try std.testing.expectEqual(@as(usize, 8), TypeGenerator.typeSize(.any_pointer));
}

test "TypeGenerator.isPointer identifies pointer types" {
    try std.testing.expect(TypeGenerator.isPointer(.text));
    try std.testing.expect(TypeGenerator.isPointer(.data));
    try std.testing.expect(TypeGenerator.isPointer(.any_pointer));

    try std.testing.expect(!TypeGenerator.isPointer(.void));
    try std.testing.expect(!TypeGenerator.isPointer(.bool));
    try std.testing.expect(!TypeGenerator.isPointer(.int32));
    try std.testing.expect(!TypeGenerator.isPointer(.uint64));
    try std.testing.expect(!TypeGenerator.isPointer(.float64));
}

test "TypeGenerator.typeToZig maps primitive types" {
    const alloc = std.testing.allocator;
    var gen = TypeGenerator.init(alloc);

    const cases = .{
        .{ .typ = schema.Type.void, .expected = "void" },
        .{ .typ = schema.Type.bool, .expected = "bool" },
        .{ .typ = schema.Type.int8, .expected = "i8" },
        .{ .typ = schema.Type.int16, .expected = "i16" },
        .{ .typ = schema.Type.int32, .expected = "i32" },
        .{ .typ = schema.Type.int64, .expected = "i64" },
        .{ .typ = schema.Type.uint8, .expected = "u8" },
        .{ .typ = schema.Type.uint16, .expected = "u16" },
        .{ .typ = schema.Type.uint32, .expected = "u32" },
        .{ .typ = schema.Type.uint64, .expected = "u64" },
        .{ .typ = schema.Type.float32, .expected = "f32" },
        .{ .typ = schema.Type.float64, .expected = "f64" },
        .{ .typ = schema.Type.text, .expected = "[]const u8" },
        .{ .typ = schema.Type.data, .expected = "[]const u8" },
        .{ .typ = schema.Type.any_pointer, .expected = "message.AnyPointerReader" },
        .{ .typ = @as(schema.Type, .{ .interface = .{ .type_id = 0 } }), .expected = "message.Capability" },
    };

    inline for (cases) |case| {
        const result = try gen.typeToZig(case.typ);
        defer alloc.free(result);
        try std.testing.expectEqualStrings(case.expected, result);
    }
}
