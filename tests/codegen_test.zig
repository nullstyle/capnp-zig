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
