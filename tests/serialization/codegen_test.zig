const std = @import("std");
const testing = std.testing;

// This test file will test the generated code from example schemas
// We'll create a simple Person struct and test its serialization

test "Generated code: Person struct round trip" {
    // This test assumes we've generated code from a Person schema
    // For now, we'll manually create what the generated code would look like

    const Person = struct {
        const capnpc = @import("capnpc-zig");
        const message_mod = capnpc.message;

        pub const Reader = struct {
            _reader: message_mod.StructReader,

            pub fn init(msg: *const message_mod.Message) !Reader {
                const root = try msg.getRootStruct();
                return .{ ._reader = root };
            }

            pub fn getName(self: Reader) ![]const u8 {
                return try self._reader.readText(0);
            }

            pub fn getAge(self: Reader) u32 {
                return self._reader.readU32(0);
            }

            pub fn getEmail(self: Reader) ![]const u8 {
                return try self._reader.readText(1);
            }
        };

        pub const Builder = struct {
            _builder: message_mod.StructBuilder,

            pub fn init(msg: *message_mod.MessageBuilder) !Builder {
                const builder = try msg.allocateStruct(1, 2);
                return .{ ._builder = builder };
            }

            pub fn setName(self: *Builder, value: []const u8) !void {
                try self._builder.writeText(0, value);
            }

            pub fn setAge(self: *Builder, value: u32) void {
                self._builder.writeU32(0, value);
            }

            pub fn setEmail(self: *Builder, value: []const u8) !void {
                try self._builder.writeText(1, value);
            }
        };
    };

    // Create a Person
    var msg_builder = Person.message_mod.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var person_builder = try Person.Builder.init(&msg_builder);
    try person_builder.setName("Alice");
    person_builder.setAge(30);
    try person_builder.setEmail("alice@example.com");

    // Serialize
    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    // Deserialize
    var msg = try Person.message_mod.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const person_reader = try Person.Reader.init(&msg);

    // Verify
    const name = try person_reader.getName();
    try testing.expectEqualStrings("Alice", name);

    const age = person_reader.getAge();
    try testing.expectEqual(@as(u32, 30), age);

    const email = try person_reader.getEmail();
    try testing.expectEqualStrings("alice@example.com", email);
}

test "Generated code: struct with bool fields" {
    const Config = struct {
        const capnpc = @import("capnpc-zig");
        const message_mod = capnpc.message;

        pub const Reader = struct {
            _reader: message_mod.StructReader,

            pub fn init(msg: *const message_mod.Message) !Reader {
                const root = try msg.getRootStruct();
                return .{ ._reader = root };
            }

            pub fn getEnabled(self: Reader) bool {
                return self._reader.readBool(0, 0);
            }

            pub fn getDebugMode(self: Reader) bool {
                return self._reader.readBool(0, 1);
            }

            pub fn getVerbose(self: Reader) bool {
                return self._reader.readBool(0, 2);
            }
        };

        pub const Builder = struct {
            _builder: message_mod.StructBuilder,

            pub fn init(msg: *message_mod.MessageBuilder) !Builder {
                const builder = try msg.allocateStruct(1, 0);
                return .{ ._builder = builder };
            }

            pub fn setEnabled(self: *Builder, value: bool) void {
                self._builder.writeBool(0, 0, value);
            }

            pub fn setDebugMode(self: *Builder, value: bool) void {
                self._builder.writeBool(0, 1, value);
            }

            pub fn setVerbose(self: *Builder, value: bool) void {
                self._builder.writeBool(0, 2, value);
            }
        };
    };

    // Create a Config
    var msg_builder = Config.message_mod.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var config_builder = try Config.Builder.init(&msg_builder);
    config_builder.setEnabled(true);
    config_builder.setDebugMode(false);
    config_builder.setVerbose(true);

    // Serialize
    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    // Deserialize
    var msg = try Config.message_mod.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const config_reader = try Config.Reader.init(&msg);

    // Verify
    try testing.expectEqual(true, config_reader.getEnabled());
    try testing.expectEqual(false, config_reader.getDebugMode());
    try testing.expectEqual(true, config_reader.getVerbose());
}

test "Generated code: struct with all integer types" {
    const Numbers = struct {
        const capnpc = @import("capnpc-zig");
        const message_mod = capnpc.message;

        pub const Reader = struct {
            _reader: message_mod.StructReader,

            pub fn init(msg: *const message_mod.Message) !Reader {
                const root = try msg.getRootStruct();
                return .{ ._reader = root };
            }

            pub fn getU8Field(self: Reader) u8 {
                return self._reader.readU8(0);
            }

            pub fn getU16Field(self: Reader) u16 {
                return self._reader.readU16(2);
            }

            pub fn getU32Field(self: Reader) u32 {
                return self._reader.readU32(4);
            }

            pub fn getU64Field(self: Reader) u64 {
                return self._reader.readU64(8);
            }
        };

        pub const Builder = struct {
            _builder: message_mod.StructBuilder,

            pub fn init(msg: *message_mod.MessageBuilder) !Builder {
                const builder = try msg.allocateStruct(2, 0);
                return .{ ._builder = builder };
            }

            pub fn setU8Field(self: *Builder, value: u8) void {
                self._builder.writeU8(0, value);
            }

            pub fn setU16Field(self: *Builder, value: u16) void {
                self._builder.writeU16(2, value);
            }

            pub fn setU32Field(self: *Builder, value: u32) void {
                self._builder.writeU32(4, value);
            }

            pub fn setU64Field(self: *Builder, value: u64) void {
                self._builder.writeU64(8, value);
            }
        };
    };

    // Create Numbers
    var msg_builder = Numbers.message_mod.MessageBuilder.init(testing.allocator);
    defer msg_builder.deinit();

    var numbers_builder = try Numbers.Builder.init(&msg_builder);
    numbers_builder.setU8Field(255);
    numbers_builder.setU16Field(65535);
    numbers_builder.setU32Field(4294967295);
    numbers_builder.setU64Field(18446744073709551615);

    // Serialize
    const bytes = try msg_builder.toBytes();
    defer testing.allocator.free(bytes);

    // Deserialize
    var msg = try Numbers.message_mod.Message.init(testing.allocator, bytes);
    defer msg.deinit();

    const numbers_reader = try Numbers.Reader.init(&msg);

    // Verify
    try testing.expectEqual(@as(u8, 255), numbers_reader.getU8Field());
    try testing.expectEqual(@as(u16, 65535), numbers_reader.getU16Field());
    try testing.expectEqual(@as(u32, 4294967295), numbers_reader.getU32Field());
    try testing.expectEqual(@as(u64, 18446744073709551615), numbers_reader.getU64Field());
}

test "Codegen: list, struct, any pointer, defaults" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    const u16_type = schema.Type{ .uint16 = {} };
    const list_type = schema.Type{ .list = .{ .element_type = @constCast(&u16_type) } };
    const child_struct_type = schema.Type{ .@"struct" = .{ .type_id = 2 } };
    const any_type = schema.Type{ .any_pointer = {} };

    const default_true = schema.Value{ .bool = true };

    var parent_fields = [_]schema.Field{
        .{
            .name = "flag",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = .bool,
                .default_value = default_true,
            },
            .group = null,
        },
        .{
            .name = "numbers",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = list_type,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "child",
            .code_order = 2,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 1,
                .type = child_struct_type,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "any",
            .code_order = 3,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 2,
                .type = any_type,
                .default_value = null,
            },
            .group = null,
        },
    };

    const child_struct = schema.StructNode{
        .data_word_count = 0,
        .pointer_count = 0,
        .preferred_list_encoding = .inline_composite,
        .is_group = false,
        .discriminant_count = 0,
        .discriminant_offset = 0,
        .fields = &[_]schema.Field{},
    };

    const parent_struct = schema.StructNode{
        .data_word_count = 1,
        .pointer_count = 3,
        .preferred_list_encoding = .inline_composite,
        .is_group = false,
        .discriminant_count = 0,
        .discriminant_offset = 0,
        .fields = &parent_fields,
    };

    const child_node = schema.Node{
        .id = 2,
        .display_name = "Child",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = child_struct,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const parent_node = schema.Node{
        .id = 3,
        .display_name = "Parent",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = parent_struct,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var file_nested = [_]schema.Node.NestedNode{
        .{ .name = "Child", .id = 2 },
        .{ .name = "Parent", .id = 3 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = file_nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, child_node, parent_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "readU16List"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "readAnyPointer"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "initChild"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "!= true"));
}

test "Codegen: const and annotation output" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    const const_value = schema.Value{ .uint32 = 42 };
    const const_node = schema.Node{
        .id = 4,
        .display_name = "MyConst",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"const",
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = .{ .type = .uint32, .value = const_value },
        .annotation_node = null,
    };

    const annotation_node = schema.Node{
        .id = 5,
        .display_name = "MyAnnotation",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .annotation,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = .{
            .type = .bool,
            .targets_file = true,
            .targets_const = false,
            .targets_enum = false,
            .targets_enumerant = false,
            .targets_struct = true,
            .targets_field = true,
            .targets_union = false,
            .targets_group = false,
            .targets_interface = false,
            .targets_method = false,
            .targets_param = false,
            .targets_annotation = false,
        },
    };

    var file_nested = [_]schema.Node.NestedNode{
        .{ .name = "MyConst", .id = 4 },
        .{ .name = "MyAnnotation", .id = 5 },
    };

    const file_node = schema.Node{
        .id = 10,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = file_nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, const_node, annotation_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 10,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const MyConst"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "@as(u32, 42)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const MyAnnotation"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, ".field = true"));
}

test "Codegen: typed enum/struct consts use normalized declaration names" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    var enum_values = [_]schema.Enumerant{
        .{ .name = "first", .code_order = 0, .annotations = &[_]schema.AnnotationUse{} },
    };
    const enum_node = schema.Node{
        .id = 2,
        .display_name = "my_enum",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"enum",
        .struct_node = null,
        .enum_node = .{ .enumerants = &enum_values },
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const struct_node = schema.Node{
        .id = 3,
        .display_name = "my_struct",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 0,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &[_]schema.Field{},
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const enum_type = schema.Type{ .@"enum" = .{ .type_id = 2 } };
    const struct_type = schema.Type{ .@"struct" = .{ .type_id = 3 } };

    const enum_const_node = schema.Node{
        .id = 4,
        .display_name = "enum_const",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"const",
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = .{
            .type = enum_type,
            .value = .{ .@"enum" = 0 },
        },
        .annotation_node = null,
    };

    const struct_const_node = schema.Node{
        .id = 5,
        .display_name = "struct_const",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"const",
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = .{
            .type = struct_type,
            .value = .{
                .@"struct" = .{
                    .message_bytes = &[_]u8{
                        0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                    },
                },
            },
        },
        .annotation_node = null,
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "my_enum", .id = 2 },
        .{ .name = "my_struct", .id = 3 },
        .{ .name = "enum_const", .id = 4 },
        .{ .name = "struct_const", .id = 5 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "typed_consts.capnp",
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

    const nodes = [_]schema.Node{
        file_node,
        enum_node,
        struct_node,
        enum_const_node,
        struct_const_node,
    };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "typed_consts.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const MyEnum = enum(u16)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const MyStruct = struct"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const enumConst: MyEnum = @enumFromInt(@as(u16, 0));"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const structConst = struct {"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn get() !MyStruct.Reader {"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return MyStruct.Reader{ ._reader = value };"));
}

test "Codegen: void const emits unit literal" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    const const_node = schema.Node{
        .id = 4,
        .display_name = "UnitConst",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"const",
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = .{ .type = .void, .value = .void },
        .annotation_node = null,
    };

    var file_nested = [_]schema.Node.NestedNode{
        .{ .name = "UnitConst", .id = 4 },
    };

    const file_node = schema.Node{
        .id = 10,
        .display_name = "test.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = file_nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ file_node, const_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 10,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const UnitConst: void = {};"));
    try testing.expect(!std.mem.containsAtLeast(u8, output, 1, "Unsupported const"));
}

test "Codegen: void list and pointer helper builders" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    const void_type = schema.Type{ .void = {} };
    const list_void_type = schema.Type{ .list = .{ .element_type = @constCast(&void_type) } };
    const iface_type = schema.Type{ .interface = .{ .type_id = 2 } };
    const any_type = schema.Type{ .any_pointer = {} };

    var fields = [_]schema.Field{
        .{
            .name = "empty_list",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = list_void_type,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "service",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 1,
                .type = iface_type,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "payload",
            .code_order = 2,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 2,
                .type = any_type,
                .default_value = null,
            },
            .group = null,
        },
    };

    const iface_node = schema.Node{
        .id = 2,
        .display_name = "Service",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .interface,
        .struct_node = null,
        .enum_node = null,
        .interface_node = .{ .methods = &[_]schema.Method{}, .superclasses = &[_]schema.Id{} },
        .const_node = null,
        .annotation_node = null,
    };

    const holder_node = schema.Node{
        .id = 3,
        .display_name = "Holder",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 0,
            .pointer_count = 3,
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
        .{ .name = "Service", .id = 2 },
        .{ .name = "Holder", .id = 3 },
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

    const nodes = [_]schema.Node{ file_node, iface_node, holder_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getEmptyList(self: Reader) !message.VoidListReader"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "readVoidList(0)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getService(self: Reader) !message.Capability"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "readCapability(1)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn initEmptyList(self: *Builder, element_count: u32) !message.VoidListBuilder"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "writeVoidList(0, element_count)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn clearService(self: *Builder) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setServiceCapability(self: *Builder, cap: message.Capability) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setPayloadNull(self: *Builder) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setPayloadText(self: *Builder, value: []const u8) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setPayloadData(self: *Builder, value: []const u8) !void"));
}

test "TypeGenerator: complex type mappings are concrete" {
    const schema = @import("capnpc-zig").schema;
    const TypeGenerator = @import("capnpc-zig").codegen.TypeGenerator;
    var type_gen = TypeGenerator.init(testing.allocator);

    const void_type = schema.Type{ .void = {} };
    const list_void_type = schema.Type{ .list = .{ .element_type = @constCast(&void_type) } };
    const struct_type = schema.Type{ .@"struct" = .{ .type_id = 999 } };
    const iface_type = schema.Type{ .interface = .{ .type_id = 1234 } };
    const any_type = schema.Type{ .any_pointer = {} };

    const list_name = try type_gen.typeToZig(list_void_type);
    defer testing.allocator.free(list_name);
    try testing.expectEqualStrings("message.VoidListReader", list_name);

    const struct_name = try type_gen.typeToZig(struct_type);
    defer testing.allocator.free(struct_name);
    try testing.expectEqualStrings("message.StructReader", struct_name);

    const iface_name = try type_gen.typeToZig(iface_type);
    defer testing.allocator.free(iface_name);
    try testing.expectEqualStrings("message.Capability", iface_name);

    const any_name = try type_gen.typeToZig(any_type);
    defer testing.allocator.free(any_name);
    try testing.expectEqualStrings("message.AnyPointerReader", any_name);
}

test "Codegen: unresolved enum uses u16 setter value path" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    const unresolved_enum_type = schema.Type{ .@"enum" = .{ .type_id = 999 } };
    var fields = [_]schema.Field{
        .{
            .name = "status",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = unresolved_enum_type,
                .default_value = null,
            },
            .group = null,
        },
    };

    const holder_node = schema.Node{
        .id = 2,
        .display_name = "Holder",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 1,
            .pointer_count = 0,
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
        .{ .name = "Holder", .id = 2 },
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

    const nodes = [_]schema.Node{ file_node, holder_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn setStatus(self: *Builder, value: u16) !void"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "writeU16(0, @as(u16, value));"));
}

test "Codegen: unresolved struct getter falls back to StructReader" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    const unresolved_struct_type = schema.Type{ .@"struct" = .{ .type_id = 999 } };
    var fields = [_]schema.Field{
        .{
            .name = "child",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = unresolved_struct_type,
                .default_value = null,
            },
            .group = null,
        },
    };

    const holder_node = schema.Node{
        .id = 2,
        .display_name = "Holder",
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
        .{ .name = "Holder", .id = 2 },
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

    const nodes = [_]schema.Node{ file_node, holder_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getChild(self: Reader) !message.StructReader"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return try self._reader.readStruct(0);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn initChild(self: *Builder, data_words: u16, pointer_words: u16) !message.StructBuilder"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return try self._builder.initStruct(0, data_words, pointer_words);"));
    try testing.expect(!std.mem.containsAtLeast(u8, output, 1, "UnsupportedType"));
}

test "Codegen: unresolved struct list builder accepts explicit layout" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    const unresolved_child_type = schema.Type{ .@"struct" = .{ .type_id = 999 } };
    const list_child_type = schema.Type{ .list = .{ .element_type = @constCast(&unresolved_child_type) } };
    var fields = [_]schema.Field{
        .{
            .name = "children",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = list_child_type,
                .default_value = null,
            },
            .group = null,
        },
    };

    const holder_node = schema.Node{
        .id = 2,
        .display_name = "Holder",
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
        .{ .name = "Holder", .id = 2 },
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

    const nodes = [_]schema.Node{ file_node, holder_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn initChildren(self: *Builder, element_count: u32, data_words: u16, pointer_words: u16) !message.StructListBuilder"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return try self._builder.writeStructList(0, element_count, data_words, pointer_words);"));
    try testing.expect(!std.mem.containsAtLeast(u8, output, 1, "UnsupportedType"));
}

test "Codegen: enum list fields use typed wrappers" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    const enum_type = schema.Type{ .@"enum" = .{ .type_id = 2 } };
    const list_enum_type = schema.Type{ .list = .{ .element_type = @constCast(&enum_type) } };
    var fields = [_]schema.Field{
        .{
            .name = "colors",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = list_enum_type,
                .default_value = null,
            },
            .group = null,
        },
    };

    var enumerants = [_]schema.Enumerant{
        .{ .name = "red", .code_order = 0, .annotations = &[_]schema.AnnotationUse{} },
        .{ .name = "green", .code_order = 1, .annotations = &[_]schema.AnnotationUse{} },
    };

    const color_node = schema.Node{
        .id = 2,
        .display_name = "Color",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"enum",
        .struct_node = null,
        .enum_node = .{ .enumerants = &enumerants },
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const holder_node = schema.Node{
        .id = 3,
        .display_name = "Holder",
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
        .{ .name = "Color", .id = 2 },
        .{ .name = "Holder", .id = 3 },
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

    const nodes = [_]schema.Node{ file_node, color_node, holder_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const EnumListReader = message.typed_list_helpers.EnumListReader;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const EnumListBuilder = message.typed_list_helpers.EnumListBuilder;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getColors(self: Reader) !EnumListReader(Color)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const raw = try self._reader.readU16List(0);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return EnumListReader(Color){ ._list = raw };"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn initColors(self: *Builder, element_count: u32) !EnumListBuilder(Color)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const raw = try self._builder.writeU16List(0, element_count);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return EnumListBuilder(Color){ ._list = raw };"));
}

test "Codegen: struct list fields use typed wrappers" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    const child_type = schema.Type{ .@"struct" = .{ .type_id = 2 } };
    const list_child_type = schema.Type{ .list = .{ .element_type = @constCast(&child_type) } };
    var fields = [_]schema.Field{
        .{
            .name = "children",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = list_child_type,
                .default_value = null,
            },
            .group = null,
        },
    };

    const child_node = schema.Node{
        .id = 2,
        .display_name = "Child",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 0,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &[_]schema.Field{},
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const holder_node = schema.Node{
        .id = 3,
        .display_name = "Holder",
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
        .{ .name = "Child", .id = 2 },
        .{ .name = "Holder", .id = 3 },
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

    const nodes = [_]schema.Node{ file_node, child_node, holder_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const StructListReader = message.typed_list_helpers.StructListReader;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const StructListBuilder = message.typed_list_helpers.StructListBuilder;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getChildren(self: Reader) !StructListReader(Child)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const raw = try self._reader.readStructList(0);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return StructListReader(Child){ ._list = raw };"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn initChildren(self: *Builder, element_count: u32) !StructListBuilder(Child)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const raw = try self._builder.writeStructList(0, element_count, 0, 0);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return StructListBuilder(Child){ ._list = raw };"));
}

test "Codegen: data and capability list fields use typed wrappers" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    const data_type = schema.Type{ .data = {} };
    const list_data_type = schema.Type{ .list = .{ .element_type = @constCast(&data_type) } };
    const iface_type = schema.Type{ .interface = .{ .type_id = 2 } };
    const list_iface_type = schema.Type{ .list = .{ .element_type = @constCast(&iface_type) } };
    var fields = [_]schema.Field{
        .{
            .name = "data_items",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = list_data_type,
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "services",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 1,
                .type = list_iface_type,
                .default_value = null,
            },
            .group = null,
        },
    };

    const iface_node = schema.Node{
        .id = 2,
        .display_name = "Service",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .interface,
        .struct_node = null,
        .enum_node = null,
        .interface_node = .{ .methods = &[_]schema.Method{}, .superclasses = &[_]schema.Id{} },
        .const_node = null,
        .annotation_node = null,
    };

    const holder_node = schema.Node{
        .id = 3,
        .display_name = "Holder",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 0,
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
        .{ .name = "Service", .id = 2 },
        .{ .name = "Holder", .id = 3 },
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

    const nodes = [_]schema.Node{ file_node, iface_node, holder_node };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "test.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const DataListReader = message.typed_list_helpers.DataListReader;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const DataListBuilder = message.typed_list_helpers.DataListBuilder;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const CapabilityListReader = message.typed_list_helpers.CapabilityListReader;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "const CapabilityListBuilder = message.typed_list_helpers.CapabilityListBuilder;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getDataItems(self: Reader) !DataListReader"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return DataListReader{ ._list = raw };"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn initDataItems(self: *Builder, element_count: u32) !DataListBuilder"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return DataListBuilder{ ._list = raw };"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getServices(self: Reader) !CapabilityListReader"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return CapabilityListReader{ ._list = raw };"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn resolveServices(self: Reader, index: u32, peer: *rpc.peer.Peer, caps: *const rpc.cap_table.InboundCapTable) !Service.Client"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "var mutable_caps = caps.*;"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "try mutable_caps.retainCapability(cap);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn initServices(self: *Builder, element_count: u32) !CapabilityListBuilder"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "return CapabilityListBuilder{ ._list = raw };"));
}

test "Codegen: declaration identifiers are normalized and escaped consistently" {
    const schema = @import("capnpc-zig").schema;
    const Generator = @import("capnpc-zig").codegen.Generator;

    var enum_values = [_]schema.Enumerant{
        .{ .name = "first_value", .code_order = 0, .annotations = &[_]schema.AnnotationUse{} },
    };
    const enum_node = schema.Node{
        .id = 2,
        .display_name = "my_enum",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"enum",
        .struct_node = null,
        .enum_node = .{ .enumerants = &enum_values },
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const enum_type = schema.Type{ .@"enum" = .{ .type_id = 2 } };
    var holder_fields = [_]schema.Field{
        .{
            .name = "status",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{ .offset = 0, .type = enum_type, .default_value = null },
            .group = null,
        },
    };
    const holder_node = schema.Node{
        .id = 3,
        .display_name = "holder",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 1,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &holder_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var methods = [_]schema.Method{
        .{
            .name = "ping",
            .code_order = 0,
            .param_struct_type = 5,
            .result_struct_type = 6,
            .annotations = &[_]schema.AnnotationUse{},
        },
    };
    const interface_node = schema.Node{
        .id = 4,
        .display_name = "my_service",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .interface,
        .struct_node = null,
        .enum_node = null,
        .interface_node = .{ .methods = &methods, .superclasses = &[_]schema.Id{} },
        .const_node = null,
        .annotation_node = null,
    };

    const empty_struct = schema.StructNode{
        .data_word_count = 0,
        .pointer_count = 0,
        .preferred_list_encoding = .inline_composite,
        .is_group = false,
        .discriminant_count = 0,
        .discriminant_offset = 0,
        .fields = &[_]schema.Field{},
    };
    const ping_params_node = schema.Node{
        .id = 5,
        .display_name = "ping_params",
        .display_name_prefix_length = 0,
        .scope_id = 4,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = empty_struct,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
    const ping_results_node = schema.Node{
        .id = 6,
        .display_name = "ping_results",
        .display_name_prefix_length = 0,
        .scope_id = 4,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = empty_struct,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const keyword_const_node = schema.Node{
        .id = 7,
        .display_name = "usingnamespace",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"const",
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = .{
            .type = .{ .uint32 = {} },
            .value = .{ .uint32 = 7 },
        },
        .annotation_node = null,
    };

    const snake_const_node = schema.Node{
        .id = 8,
        .display_name = "snake_case_const",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"const",
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = .{
            .type = .{ .uint32 = {} },
            .value = .{ .uint32 = 9 },
        },
        .annotation_node = null,
    };

    const annotation_node = schema.Node{
        .id = 9,
        .display_name = "annotation_value",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .annotation,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = .{
            .type = .text,
            .targets_file = false,
            .targets_const = false,
            .targets_enum = false,
            .targets_enumerant = false,
            .targets_struct = false,
            .targets_field = false,
            .targets_union = false,
            .targets_group = false,
            .targets_interface = false,
            .targets_method = false,
            .targets_param = false,
            .targets_annotation = false,
        },
    };

    var nested = [_]schema.Node.NestedNode{
        .{ .name = "my_enum", .id = 2 },
        .{ .name = "holder", .id = 3 },
        .{ .name = "my_service", .id = 4 },
        .{ .name = "ping_params", .id = 5 },
        .{ .name = "ping_results", .id = 6 },
        .{ .name = "usingnamespace", .id = 7 },
        .{ .name = "snake_case_const", .id = 8 },
        .{ .name = "annotation_value", .id = 9 },
    };

    const file_node = schema.Node{
        .id = 1,
        .display_name = "decls.capnp",
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

    const nodes = [_]schema.Node{
        file_node,
        enum_node,
        holder_node,
        interface_node,
        ping_params_node,
        ping_results_node,
        keyword_const_node,
        snake_const_node,
        annotation_node,
    };
    var gen = try Generator.init(testing.allocator, &nodes);
    defer gen.deinit();

    const requested_file = schema.RequestedFile{
        .id = 1,
        .filename = "decls.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested_file);
    defer testing.allocator.free(output);

    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const MyEnum = enum(u16)"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getStatus(self: Reader) !MyEnum"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const MyService = struct"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const @\"usingnamespace\": u32 = @as(u32, 7);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const snakeCaseConst: u32 = @as(u32, 9);"));
    try testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const annotationValue = struct"));

    try testing.expect(!std.mem.containsAtLeast(u8, output, 1, "pub const my_enum = enum(u16)"));
    try testing.expect(!std.mem.containsAtLeast(u8, output, 1, "pub const my_service = struct"));
    try testing.expect(!std.mem.containsAtLeast(u8, output, 1, "pub const snake_case_const"));
}
