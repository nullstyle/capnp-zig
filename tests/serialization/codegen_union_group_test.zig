const std = @import("std");
const testing = std.testing;
const schema = @import("capnpc-zig").schema;
const Generator = @import("capnpc-zig").codegen.Generator;

test "Codegen: union generates WhichTag enum and which method" {
    var fields = [_]schema.Field{
        .{
            .name = "circle",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 1,
                .type = .float64,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "square",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 1,
            .slot = .{
                .offset = 1,
                .type = .float64,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "area",
            .code_order = 2,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF, // not a union field
            .slot = .{
                .offset = 0,
                .type = .float64,
                .default_value = null,
            },
            .group = null,
        },
    };

    const shape_node = schema.Node{
        .id = 2,
        .display_name = "Shape",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 3,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 2,
            .discriminant_offset = 4, // byte offset = 4 * 2 = 8
            .fields = &fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "Shape", .id = 2 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, shape_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    // Should have WhichTag enum
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const WhichTag = enum(u16)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "circle = 0,"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "square = 1,"));

    // Should have which() method
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn whichOrdinal(self: Reader) u16"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return self._reader.readUnionDiscriminant(8);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn which(self: Reader) error{InvalidEnumValue}!WhichTag"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return std.enums.fromInt(WhichTag, self.whichOrdinal()) orelse return error.InvalidEnumValue;"));

    // Non-union field should NOT appear in WhichTag
    try testing.expect(!std.mem.containsAtLeast(u8, output, 1, "area = "));
}

test "Codegen: union setter writes discriminant before value" {
    var fields = [_]schema.Field{
        .{
            .name = "idle",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = .void,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "running",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 1,
            .slot = .{
                .offset = 1,
                .type = .uint32,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "message",
            .code_order = 2,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 2,
            .slot = .{
                .offset = 0,
                .type = .text,
                .default_value = null,
            },
            .group = null,
        },
    };

    const status_node = schema.Node{
        .id = 2,
        .display_name = "Status",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 1,
            .pointer_count = 1,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 3,
            .discriminant_offset = 0, // byte offset = 0 * 2 = 0
            .fields = &fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "Status", .id = 2 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, status_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    // WhichTag should have all three variants
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "idle = 0,"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "running = 1,"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "message = 2,"));

    // Setters should write discriminant
    // For void field (idle), the setter should write discriminant 0
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setIdle(self: *Builder, value: void) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(0, 0)"));

    // For u32 field (running), the setter should write discriminant 1
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setRunning(self: *Builder, value: u32) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(0, 1)"));

    // For text field (message), the setter should write discriminant 2
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setMessage(self: *Builder, value: []const u8) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(0, 2)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn hasMessage(self: Reader) bool"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "if (self._reader.readUnionDiscriminant(0) != 2) return false;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return !self._reader.isPointerNull(0);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn hasMessage(self: Builder) bool"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "if (self._builder.readUnionDiscriminant(0) != 2) return false;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return !self._builder.isPointerNull(0);"));

    // which() method should read from correct offset
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn which(self: Reader) error{InvalidEnumValue}!WhichTag"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return std.enums.fromInt(WhichTag, self.whichOrdinal()) orelse return error.InvalidEnumValue;"));
}

test "Codegen: struct without union does not generate WhichTag" {
    var fields = [_]schema.Field{
        .{
            .name = "name",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 0,
                .type = .text,
                .default_value = null,
            },
            .group = null,
        },
    };

    const person_node = schema.Node{
        .id = 2,
        .display_name = "Person",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 0,
            .pointer_count = 1,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "Person", .id = 2 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, person_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    // Should NOT have WhichTag or which() for structs without unions
    try testing.expect(!std.mem.containsAtLeast(u8, output, 1, "WhichTag"));
    try testing.expect(!std.mem.containsAtLeast(u8, output, 1, "pub fn which"));
}

test "Codegen: group generates nested struct with shared reader/builder" {
    // Group field: the group's fields share the parent's data section
    var group_fields = [_]schema.Field{
        .{
            .name = "x",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 0,
                .type = .float32,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "y",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 1,
                .type = .float32,
                .default_value = null,
            },
            .group = null,
        },
    };

    const group_node = schema.Node{
        .id = 3,
        .display_name = "test.capnp:Point.position",
        .display_name_prefix_length = 17,
        .scope_id = 2,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 1,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = true,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &group_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var parent_fields = [_]schema.Field{
        .{
            .name = "position",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = null,
            .group = .{ .type_id = 3 },
        },
        .{
            .name = "color",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 2,
                .type = .uint32,
                .default_value = null,
            },
            .group = null,
        },
    };

    const point_node = schema.Node{
        .id = 2,
        .display_name = "Point",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 2,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &parent_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "Point", .id = 2 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, point_node, group_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    // Group type should be generated as nested struct
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const Position = struct"));

    // Group Reader should share parent reader
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getPosition(self: Reader) Position.Reader"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return .{ ._reader = self._reader }"));

    // Group Builder should share parent builder
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getPosition(self: *Builder) Position.Builder"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return .{ ._builder = self._builder }"));

    // Group should have its own field accessors
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getX(self: @This())"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getY(self: @This())"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setX(self: *@This()"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setY(self: *@This()"));

    // Regular field should still work
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getColor(self: Reader)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setColor(self: *Builder"));

    // Group node should NOT be generated as a top-level struct
    // (only one Position struct, nested inside Point)
    var count: usize = 0;
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, output, pos, "pub const Position = struct")) |idx| {
        count += 1;
        pos = idx + 1;
    }
    try testing.expectEqual(@as(usize, 1), count);
}

test "Codegen: union group field sets discriminant on init" {
    // A group that is also a union member should set the discriminant
    var group_fields = [_]schema.Field{
        .{
            .name = "width",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 1,
                .type = .float32,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "height",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 2,
                .type = .float32,
                .default_value = null,
            },
            .group = null,
        },
    };

    const rect_group_node = schema.Node{
        .id = 3,
        .display_name = "test.capnp:Shape.rectangle",
        .display_name_prefix_length = 17,
        .scope_id = 2,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 2,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = true,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &group_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var parent_fields = [_]schema.Field{
        .{
            .name = "circle",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 1,
                .type = .float32,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "rectangle",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 1,
            .slot = null,
            .group = .{ .type_id = 3 },
        },
    };

    const shape_node = schema.Node{
        .id = 2,
        .display_name = "Shape",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 2,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 2,
            .discriminant_offset = 0, // byte offset = 0 * 2 = 0
            .fields = &parent_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "Shape", .id = 2 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, shape_node, rect_group_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    // Union tag enum should include both circle and rectangle
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "circle = 0,"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "rectangle = 1,"));

    // circle setter (non-group union member) should write discriminant
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setCircle(self: *Builder"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(0, 0)"));

    // rectangle group init should write discriminant 1
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn initRectangle(self: *Builder) Rectangle.Builder"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(0, 1)"));
}

test "Codegen: group union generates WhichTag and which method" {
    var group_fields = [_]schema.Field{
        .{
            .name = "senderLoopback",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = .uint32,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "receiverLoopback",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 1,
            .slot = .{
                .offset = 0,
                .type = .uint32,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "accept",
            .code_order = 2,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 2,
            .slot = .{
                .offset = 0,
                .type = .data,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "mode",
            .code_order = 3,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 3,
            .slot = .{
                .offset = 3,
                .type = .{ .@"enum" = .{ .type_id = 999 } },
                .default_value = .{ .@"enum" = 1 },
            },
            .group = null,
        },
    };

    const context_group_node = schema.Node{
        .id = 3,
        .display_name = "test.capnp:Disembargo.context",
        .display_name_prefix_length = 22,
        .scope_id = 2,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 1,
            .pointer_count = 1,
            .preferred_list_encoding = .inline_composite,
            .is_group = true,
            .discriminant_count = 4,
            .discriminant_offset = 2, // byte offset = 2 * 2 = 4
            .fields = &group_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var parent_fields = [_]schema.Field{
        .{
            .name = "context",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = null,
            .group = .{ .type_id = 3 },
        },
    };

    const disembargo_node = schema.Node{
        .id = 2,
        .display_name = "Disembargo",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 1,
            .pointer_count = 1,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &parent_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "Disembargo", .id = 2 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, disembargo_node, context_group_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const Context = struct"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const WhichTag = enum(u16)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "senderLoopback = 0,"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "receiverLoopback = 1,"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "accept = 2,"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "mode = 3,"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn which(self: @This()) error{InvalidEnumValue}!WhichTag"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return std.enums.fromInt(WhichTag, self.whichOrdinal()) orelse return error.InvalidEnumValue;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn whichOrdinal(self: @This()) u16"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return self._reader.readUnionDiscriminant(4);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 2, "pub const EnumOrdinals = struct"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "if (self._reader.readUnionDiscriminant(4) != 3) return error.WrongUnionMember;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return self._reader.readU16(6) ^ @as(u16, 1);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "if ((try self.which()) != .mode) return error.WrongUnionMember;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(4, 3);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(6, value ^ @as(u16, 1));"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setSenderLoopback(self: *@This(), value: u32) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(4, 0)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setReceiverLoopback(self: *@This(), value: u32) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(4, 1)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setAccept(self: *@This(), value: []const u8) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(4, 2)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn hasAccept(self: @This()) bool"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "if (self._reader.readUnionDiscriminant(4) != 2) return false;"));
    try testing.expect(!std.mem.containsAtLeast(u8, output, 1, "hasContext"));
}

test "Codegen: group-typed union variant emits nested type and accessors" {
    // BUG 1: a group nested inside a union group (a group-typed union variant)
    // was silently dropped — no nested type, no getXxx/initXxx. The parent group
    // `U` is a union whose `circle` variant is itself a group.
    var circle_fields = [_]schema.Field{
        .{
            .name = "radius",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 0, .type = .float64, .default_value = null },
            .group = null,
        },
    };

    const circle_node = schema.Node{
        .id = 4,
        .display_name = "test.capnp:Shape.u.circle",
        .display_name_prefix_length = 19, // simple name "circle" (25 - 6)
        .scope_id = 3,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 1,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = true,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &circle_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    // The `u` group is a union with a group variant `circle` and a scalar
    // variant `square`.
    var u_fields = [_]schema.Field{
        .{
            .name = "circle",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = null,
            .group = .{ .type_id = 4 },
        },
        .{
            .name = "square",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 1,
            .slot = .{ .offset = 0, .type = .float64, .default_value = null },
            .group = null,
        },
    };

    const u_node = schema.Node{
        .id = 3,
        .display_name = "test.capnp:Shape.u",
        .display_name_prefix_length = 17, // simple name "u" (18 - 1)
        .scope_id = 2,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 2,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = true,
            .discriminant_count = 2,
            .discriminant_offset = 5, // byte offset = 5 * 2 = 10
            .fields = &u_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var shape_fields = [_]schema.Field{
        .{
            .name = "u",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = null,
            .group = .{ .type_id = 3 },
        },
    };

    const shape_node = schema.Node{
        .id = 2,
        .display_name = "Shape",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 2,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &shape_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "Shape", .id = 2 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, shape_node, u_node, circle_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    // The nested Circle group type must be emitted inside the U group namespace.
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const Circle = struct"));
    // Its own field accessors must exist.
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getRadius(self: @This())"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setRadius(self: *@This()"));
    // The group Reader must expose getCircle returning the nested reader.
    // As a union member (discriminant 0), the getter is guarded and fallible:
    // reading it while a sibling variant is selected returns WrongUnionMember
    // instead of reinterpreting the sibling's bits.
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getCircle(self: @This()) !Circle.Reader"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "if ((try self.which()) != .circle) return error.WrongUnionMember;"));
    // The group Builder must expose initCircle that writes the discriminant.
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn initCircle(self: *@This()) Circle.Builder"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "self._builder.writeU16(10, 0)"));
}

test "Codegen: union member getters emit a discriminant guard" {
    // BUG 3: getters for union slot fields must check the discriminant and
    // return error.WrongUnionMember, while non-union fields must not.
    var fields = [_]schema.Field{
        .{
            .name = "circle",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{ .offset = 0, .type = .float64, .default_value = null },
            .group = null,
        },
        .{
            .name = "square",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 1,
            .slot = .{ .offset = 0, .type = .float64, .default_value = null },
            .group = null,
        },
        .{
            .name = "area",
            .code_order = 2,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF, // not a union member
            .slot = .{ .offset = 2, .type = .float64, .default_value = null },
            .group = null,
        },
    };

    const shape_node = schema.Node{
        .id = 2,
        .display_name = "Shape",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 3,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 2,
            .discriminant_offset = 8, // byte offset = 16
            .fields = &fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "Shape", .id = 2 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, shape_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    // Union member getters must guard on the discriminant.
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "if ((try self.which()) != .circle) return error.WrongUnionMember;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "if ((try self.which()) != .square) return error.WrongUnionMember;"));
    // The non-union `area` getter must NOT contain a guard.
    const area_idx = std.mem.indexOf(u8, output, "pub fn getArea(self: Reader)").?;
    const after_area = output[area_idx..];
    const area_body_end = std.mem.indexOf(u8, after_area, "}").?;
    try testing.expect(std.mem.indexOf(u8, after_area[0..area_body_end], "WrongUnionMember") == null);
}
