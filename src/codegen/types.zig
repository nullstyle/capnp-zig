const std = @import("std");
const schema = @import("../schema.zig");

/// Generate Zig type code for Cap'n Proto types
pub const TypeGenerator = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) TypeGenerator {
        return .{ .allocator = allocator };
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
            .list => try self.allocator.dupe(u8, "[]const u8"), // TODO: Proper list types
            .@"enum" => try self.allocator.dupe(u8, "u16"), // TODO: Proper enum types
            .@"struct" => try self.allocator.dupe(u8, "[]const u8"), // TODO: Proper struct types
            .interface => try self.allocator.dupe(u8, "void"), // TODO: Proper interface types
            .any_pointer => try self.allocator.dupe(u8, "[]const u8"),
        };
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
        var result = std.ArrayList(u8){};
        errdefer result.deinit(self.allocator);

        var capitalize_next = false;
        for (name) |c| {
            if (c == '_') {
                capitalize_next = true;
                continue;
            }

            if (capitalize_next) {
                try result.append(self.allocator, std.ascii.toUpper(c));
                capitalize_next = false;
            } else {
                try result.append(self.allocator, c);
            }
        }

        return result.toOwnedSlice(self.allocator);
    }

    /// Convert Cap'n Proto identifier to Zig type name (PascalCase)
    pub fn toZigTypeName(self: *TypeGenerator, name: []const u8) ![]const u8 {
        var result = std.ArrayList(u8){};
        errdefer result.deinit(self.allocator);

        var capitalize_next = true;
        for (name) |c| {
            if (c == '_') {
                capitalize_next = true;
                continue;
            }

            if (capitalize_next) {
                try result.append(self.allocator, std.ascii.toUpper(c));
                capitalize_next = false;
            } else {
                try result.append(self.allocator, c);
            }
        }

        return result.toOwnedSlice(self.allocator);
    }
};
