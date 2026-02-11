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
    node_lookup_ctx: ?*const anyopaque,
    node_lookup: ?*const fn (ctx: ?*const anyopaque, id: schema.Id) ?*const schema.Node,

    pub fn init(allocator: std.mem.Allocator) TypeGenerator {
        return .{
            .allocator = allocator,
            .node_lookup_ctx = null,
            .node_lookup = null,
        };
    }

    pub fn initWithLookup(
        allocator: std.mem.Allocator,
        node_lookup: *const fn (ctx: ?*const anyopaque, id: schema.Id) ?*const schema.Node,
        node_lookup_ctx: ?*const anyopaque,
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

test "normalizeAndEscapeValueIdentifier normalizes and escapes" {
    const normalized = try normalizeAndEscapeValueIdentifier(std.testing.allocator, "my_value");
    defer std.testing.allocator.free(normalized);
    try std.testing.expectEqualStrings("myValue", normalized);

    const escaped = try normalizeAndEscapeValueIdentifier(std.testing.allocator, "usingnamespace");
    defer std.testing.allocator.free(escaped);
    try std.testing.expectEqualStrings("@\"usingnamespace\"", escaped);
}
