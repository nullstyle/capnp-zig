const std = @import("std");
const testing = std.testing;
const schema = @import("capnpc-zig").schema;
const Generator = @import("capnpc-zig").codegen.Generator;

/// Compare generated code against a golden file. If the golden file does not
/// exist, write the output to disk and return an error so the developer can
/// review the snapshot before checking it in.
fn expectGolden(actual: []const u8, golden_path: []const u8) !void {
    const golden = std.fs.cwd().readFileAlloc(testing.allocator, golden_path, 1024 * 1024) catch |err| {
        if (err == error.FileNotFound) {
            // Write the golden file so the developer can review it.
            const dir = std.fs.cwd();
            const file = dir.createFile(golden_path, .{}) catch |create_err| {
                std.debug.print("Failed to create golden file '{s}': {}\n", .{ golden_path, create_err });
                return error.GoldenFileMissing;
            };
            defer file.close();
            file.writeAll(actual) catch |write_err| {
                std.debug.print("Failed to write golden file '{s}': {}\n", .{ golden_path, write_err });
                return error.GoldenFileMissing;
            };
            std.debug.print(
                "\n=== Golden file created: {s} ===\n" ++
                    "Review the file and re-run the test.\n\n",
                .{golden_path},
            );
            return error.GoldenFileMissing;
        }
        return err;
    };
    defer testing.allocator.free(golden);

    const golden_normalized = try normalizeLineEndings(testing.allocator, golden);
    defer testing.allocator.free(golden_normalized);
    const actual_normalized = try normalizeLineEndings(testing.allocator, actual);
    defer testing.allocator.free(actual_normalized);

    testing.expectEqualStrings(golden_normalized, actual_normalized) catch |err| {
        std.debug.print(
            "\n=== Golden file mismatch: {s} ===\n" ++
                "If the change is intentional, delete the golden file and re-run to regenerate.\n\n",
            .{golden_path},
        );
        return err;
    };
}

fn normalizeLineEndings(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);

    var i: usize = 0;
    while (i < input.len) : (i += 1) {
        const ch = input[i];
        if (ch == '\r') {
            if (i + 1 < input.len and input[i + 1] == '\n') {
                i += 1;
            }
            try out.append(allocator, '\n');
            continue;
        }
        try out.append(allocator, ch);
    }

    return out.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// Schema 1: simple struct with primitive + pointer fields
// Equivalent to:
//   struct Person {
//     name @0 :Text;
//     age  @1 :UInt32;
//     email @2 :Text;
//   }
// ---------------------------------------------------------------------------
test "golden: simple struct with primitives" {
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
        .{
            .name = "age",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 0,
                .type = .uint32,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "email",
            .code_order = 2,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 1,
                .type = .text,
                .default_value = null,
            },
            .group = null,
        },
    };

    const person_node = schema.Node{
        .id = 0xA1B2C3D4E5F60001,
        .display_name = "person.capnp:Person",
        .display_name_prefix_length = 13,
        .scope_id = 0xF0F0F0F0F0F0F0F0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 1,
            .pointer_count = 2,
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
        .{ .name = "Person", .id = 0xA1B2C3D4E5F60001 },
    };

    const file_node = schema.Node{
        .id = 0xF0F0F0F0F0F0F0F0,
        .display_name = "person.capnp",
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
        .id = 0xF0F0F0F0F0F0F0F0,
        .filename = "person.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try expectGolden(output, "tests/golden/simple_struct.zig");
}

// ---------------------------------------------------------------------------
// Schema 2: struct with union + enum
// Equivalent to:
//   enum Color { red @0; green @1; blue @2; }
//   struct Shape {
//     color @0 :Color;
//     union {
//       circle @1 :Float64;
//       rectangle :group {
//         width  @2 :Float32;
//         height @3 :Float32;
//       }
//     }
//   }
// ---------------------------------------------------------------------------
test "golden: struct with union, group, and enum" {
    var enumerants = [_]schema.Enumerant{
        .{ .name = "red", .code_order = 0, .annotations = &[_]schema.AnnotationUse{} },
        .{ .name = "green", .code_order = 1, .annotations = &[_]schema.AnnotationUse{} },
        .{ .name = "blue", .code_order = 2, .annotations = &[_]schema.AnnotationUse{} },
    };

    const color_node = schema.Node{
        .id = 0xBBBBBBBBBBBBBB01,
        .display_name = "shape.capnp:Color",
        .display_name_prefix_length = 12,
        .scope_id = 0xAAAAAAAAAAAAAAAA,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"enum",
        .struct_node = null,
        .enum_node = .{ .enumerants = &enumerants },
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    // Rectangle group node
    var rect_fields = [_]schema.Field{
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
        .id = 0xBBBBBBBBBBBBBB03,
        .display_name = "shape.capnp:Shape.rectangle",
        .display_name_prefix_length = 18,
        .scope_id = 0xBBBBBBBBBBBBBB02,
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
            .fields = &rect_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const color_enum_type = schema.Type{ .@"enum" = .{ .type_id = 0xBBBBBBBBBBBBBB01 } };

    // Shape node with union (circle | rectangle)
    var shape_fields = [_]schema.Field{
        .{
            .name = "color",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 0,
                .type = color_enum_type,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "circle",
            .code_order = 1,
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
            .name = "rectangle",
            .code_order = 2,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 1,
            .slot = null,
            .group = .{ .type_id = 0xBBBBBBBBBBBBBB03 },
        },
    };

    const shape_node = schema.Node{
        .id = 0xBBBBBBBBBBBBBB02,
        .display_name = "shape.capnp:Shape",
        .display_name_prefix_length = 12,
        .scope_id = 0xAAAAAAAAAAAAAAAA,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 2,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 2,
            .discriminant_offset = 1,
            .fields = &shape_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "Color", .id = 0xBBBBBBBBBBBBBB01 },
        .{ .name = "Shape", .id = 0xBBBBBBBBBBBBBB02 },
    };

    const file_node = schema.Node{
        .id = 0xAAAAAAAAAAAAAAAA,
        .display_name = "shape.capnp",
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

    const nodes = [_]schema.Node{ file_node, color_node, shape_node, rect_group_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 0xAAAAAAAAAAAAAAAA,
        .filename = "shape.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try expectGolden(output, "tests/golden/union_group_enum.zig");
}

// ---------------------------------------------------------------------------
// Schema 3: struct with all primitive types
// Equivalent to:
//   struct AllTypes {
//     boolField   @0  :Bool;
//     int8Field   @1  :Int8;
//     int16Field  @2  :Int16;
//     int32Field  @3  :Int32;
//     int64Field  @4  :Int64;
//     uint8Field  @5  :UInt8;
//     uint16Field @6  :UInt16;
//     uint32Field @7  :UInt32;
//     uint64Field @8  :UInt64;
//     float32Field @9  :Float32;
//     float64Field @10 :Float64;
//     textField   @11 :Text;
//     dataField   @12 :Data;
//   }
// ---------------------------------------------------------------------------
test "golden: struct with all primitive types" {
    var fields = [_]schema.Field{
        .{
            .name = "boolField",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 0, .type = .bool, .default_value = null },
            .group = null,
        },
        .{
            .name = "int8Field",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 1, .type = .int8, .default_value = null },
            .group = null,
        },
        .{
            .name = "int16Field",
            .code_order = 2,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 1, .type = .int16, .default_value = null },
            .group = null,
        },
        .{
            .name = "int32Field",
            .code_order = 3,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 1, .type = .int32, .default_value = null },
            .group = null,
        },
        .{
            .name = "int64Field",
            .code_order = 4,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 1, .type = .int64, .default_value = null },
            .group = null,
        },
        .{
            .name = "uint8Field",
            .code_order = 5,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 2, .type = .uint8, .default_value = null },
            .group = null,
        },
        .{
            .name = "uint16Field",
            .code_order = 6,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 2, .type = .uint16, .default_value = null },
            .group = null,
        },
        .{
            .name = "uint32Field",
            .code_order = 7,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 2, .type = .uint32, .default_value = null },
            .group = null,
        },
        .{
            .name = "uint64Field",
            .code_order = 8,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 2, .type = .uint64, .default_value = null },
            .group = null,
        },
        .{
            .name = "float32Field",
            .code_order = 9,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 3, .type = .float32, .default_value = null },
            .group = null,
        },
        .{
            .name = "float64Field",
            .code_order = 10,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 3, .type = .float64, .default_value = null },
            .group = null,
        },
        .{
            .name = "textField",
            .code_order = 11,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 0, .type = .text, .default_value = null },
            .group = null,
        },
        .{
            .name = "dataField",
            .code_order = 12,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 1, .type = .data, .default_value = null },
            .group = null,
        },
    };

    const all_types_node = schema.Node{
        .id = 0xCCCCCCCCCCCCCC01,
        .display_name = "all_types.capnp:AllTypes",
        .display_name_prefix_length = 16,
        .scope_id = 0xCCCCCCCCCCCCCC00,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 4,
            .pointer_count = 2,
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
        .{ .name = "AllTypes", .id = 0xCCCCCCCCCCCCCC01 },
    };

    const file_node = schema.Node{
        .id = 0xCCCCCCCCCCCCCC00,
        .display_name = "all_types.capnp",
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

    const nodes = [_]schema.Node{ file_node, all_types_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 0xCCCCCCCCCCCCCC00,
        .filename = "all_types.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try expectGolden(output, "tests/golden/all_primitive_types.zig");
}

// ---------------------------------------------------------------------------
// Schema 4: const + enum together
// Equivalent to:
//   const version :UInt32 = 42;
//   enum Status { active @0; inactive @1; pending @2; }
// ---------------------------------------------------------------------------
test "golden: const and enum definitions" {
    const const_node = schema.Node{
        .id = 0xDDDDDDDDDDDDDD01,
        .display_name = "defs.capnp:version",
        .display_name_prefix_length = 11,
        .scope_id = 0xDDDDDDDDDDDDDD00,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"const",
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = .{
            .type = .uint32,
            .value = .{ .uint32 = 42 },
        },
        .annotation_node = null,
    };

    var status_enumerants = [_]schema.Enumerant{
        .{ .name = "active", .code_order = 0, .annotations = &[_]schema.AnnotationUse{} },
        .{ .name = "inactive", .code_order = 1, .annotations = &[_]schema.AnnotationUse{} },
        .{ .name = "pending", .code_order = 2, .annotations = &[_]schema.AnnotationUse{} },
    };

    const status_node = schema.Node{
        .id = 0xDDDDDDDDDDDDDD02,
        .display_name = "defs.capnp:Status",
        .display_name_prefix_length = 11,
        .scope_id = 0xDDDDDDDDDDDDDD00,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"enum",
        .struct_node = null,
        .enum_node = .{ .enumerants = &status_enumerants },
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "version", .id = 0xDDDDDDDDDDDDDD01 },
        .{ .name = "Status", .id = 0xDDDDDDDDDDDDDD02 },
    };

    const file_node = schema.Node{
        .id = 0xDDDDDDDDDDDDDD00,
        .display_name = "defs.capnp",
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

    const nodes = [_]schema.Node{ file_node, const_node, status_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 0xDDDDDDDDDDDDDD00,
        .filename = "defs.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try expectGolden(output, "tests/golden/const_and_enum.zig");
}
