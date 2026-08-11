const std = @import("std");
const message = @import("message.zig");
const schema = @import("schema.zig");

const NodeWhich = enum(u16) {
    file = 0,
    @"struct" = 1,
    @"enum" = 2,
    interface = 3,
    @"const" = 4,
    annotation = 5,
};

const FieldWhich = enum(u16) {
    slot = 0,
    group = 1,
};

const TypeWhich = enum(u16) {
    void = 0,
    bool = 1,
    int8 = 2,
    int16 = 3,
    int32 = 4,
    int64 = 5,
    uint8 = 6,
    uint16 = 7,
    uint32 = 8,
    uint64 = 9,
    float32 = 10,
    float64 = 11,
    text = 12,
    data = 13,
    list = 14,
    @"enum" = 15,
    @"struct" = 16,
    interface = 17,
    any_pointer = 18,
};

const AnyPointerWhich = enum(u16) {
    unconstrained = 0,
    parameter = 1,
    implicit_method_parameter = 2,
};

const UnconstrainedAnyPointerWhich = enum(u16) {
    any_kind = 0,
    @"struct" = 1,
    list = 2,
    capability = 3,
};

const BrandScopeWhich = enum(u16) {
    bind = 0,
    inherit = 1,
};

const BrandBindingWhich = enum(u16) {
    unbound = 0,
    type = 1,
};

const ValueWhich = enum(u16) {
    void = 0,
    bool = 1,
    int8 = 2,
    int16 = 3,
    int32 = 4,
    int64 = 5,
    uint8 = 6,
    uint16 = 7,
    uint32 = 8,
    uint64 = 9,
    float32 = 10,
    float64 = 11,
    text = 12,
    data = 13,
    list = 14,
    @"enum" = 15,
    @"struct" = 16,
    interface = 17,
    any_pointer = 18,
};

pub fn parseCodeGeneratorRequest(allocator: std.mem.Allocator, bytes: []const u8) !schema.CodeGeneratorRequest {
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();

    const nodes = try parseNodeList(allocator, root);
    errdefer freeNodes(allocator, nodes);

    const requested_files = try parseRequestedFiles(allocator, root);
    errdefer freeRequestedFiles(allocator, requested_files);

    var capnp_version: ?schema.CapnpVersion = null;
    const version_reader = try readOptionalStruct(root, 2);
    if (version_reader) |version| {
        capnp_version = .{
            .major = version.readU16(0),
            .minor = version.readU8(2),
            .micro = version.readU8(3),
        };
    }

    return .{
        .nodes = nodes,
        .requested_files = requested_files,
        .capnp_version = capnp_version,
    };
}

pub fn freeCodeGeneratorRequest(allocator: std.mem.Allocator, request: schema.CodeGeneratorRequest) void {
    freeNodes(allocator, request.nodes);
    freeRequestedFiles(allocator, request.requested_files);
}

fn parseNodeList(allocator: std.mem.Allocator, root: message.StructReader) ![]schema.Node {
    const list = (try readOptionalStructList(root, 0)) orelse return allocator.alloc(schema.Node, 0);
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var nodes = try allocator.alloc(schema.Node, count);
    var initialized: u32 = 0;
    errdefer {
        for (nodes[0..initialized]) |node| {
            allocator.free(node.display_name);
            freeNestedNodes(allocator, node.nested_nodes);
            freeAnnotations(allocator, node.annotations);
            freeParameters(allocator, node.parameters);
            if (node.struct_node) |sn| freeFields(allocator, sn.fields);
            if (node.enum_node) |en| freeEnumerants(allocator, en.enumerants);
            if (node.interface_node) |in_| {
                freeMethods(allocator, in_.methods);
                allocator.free(in_.superclasses);
                freeBrands(allocator, in_.superclass_brands);
                allocator.free(in_.superclass_brands);
            }
            if (node.const_node) |cn| {
                freeTypeExpression(allocator, .{ .type = cn.type, .metadata = cn.type_metadata });
                freeValue(allocator, cn.value);
            }
            if (node.annotation_node) |an| freeTypeExpression(allocator, .{ .type = an.type, .metadata = an.type_metadata });
        }
        allocator.free(nodes);
    }

    while (initialized < count) : (initialized += 1) {
        nodes[initialized] = try parseNode(allocator, try list.get(initialized));
    }

    return nodes;
}

fn parseNode(allocator: std.mem.Allocator, reader: message.StructReader) !schema.Node {
    const id = reader.readU64(0);
    const display_name = try dupText(allocator, reader, 0);
    errdefer allocator.free(display_name);
    const display_name_prefix_length = reader.readU32(8);
    const scope_id = reader.readU64(16);
    const nested_nodes = try parseNestedNodes(allocator, reader);
    errdefer freeNestedNodes(allocator, nested_nodes);
    const annotations = try parseAnnotations(allocator, reader, 2);
    errdefer freeAnnotations(allocator, annotations);
    const parameters = try parseParameters(allocator, reader, 5);
    errdefer freeParameters(allocator, parameters);

    const kind_raw = reader.readU16(12);
    const kind_tag = std.enums.fromInt(NodeWhich, kind_raw) orelse return error.InvalidNodeKind;

    var struct_node: ?schema.StructNode = null;
    var enum_node: ?schema.EnumNode = null;
    var interface_node: ?schema.InterfaceNode = null;
    var const_node: ?schema.ConstNode = null;
    var annotation_node: ?schema.AnnotationNode = null;
    errdefer if (struct_node) |sn| freeFields(allocator, sn.fields);
    errdefer if (enum_node) |en| {
        for (en.enumerants) |e| {
            allocator.free(e.name);
            freeAnnotations(allocator, e.annotations);
        }
        allocator.free(en.enumerants);
    };
    errdefer if (interface_node) |in| {
        freeMethods(allocator, in.methods);
        allocator.free(in.superclasses);
        freeBrands(allocator, in.superclass_brands);
        allocator.free(in.superclass_brands);
    };
    errdefer if (const_node) |cn| {
        freeTypeExpression(allocator, .{ .type = cn.type, .metadata = cn.type_metadata });
        freeValue(allocator, cn.value);
    };
    errdefer if (annotation_node) |an| freeTypeExpression(allocator, .{ .type = an.type, .metadata = an.type_metadata });

    switch (kind_tag) {
        .file => {},
        .@"struct" => struct_node = try parseStructNode(allocator, reader),
        .@"enum" => enum_node = try parseEnumNode(allocator, reader),
        .interface => interface_node = try parseInterfaceNode(allocator, reader),
        .@"const" => const_node = try parseConstNode(allocator, reader),
        .annotation => annotation_node = try parseAnnotationNode(allocator, reader),
    }

    return .{
        .id = id,
        .display_name = display_name,
        .display_name_prefix_length = display_name_prefix_length,
        .scope_id = scope_id,
        .nested_nodes = nested_nodes,
        .annotations = annotations,
        .kind = switch (kind_tag) {
            .file => .file,
            .@"struct" => .@"struct",
            .@"enum" => .@"enum",
            .interface => .interface,
            .@"const" => .@"const",
            .annotation => .annotation,
        },
        .struct_node = struct_node,
        .enum_node = enum_node,
        .interface_node = interface_node,
        .const_node = const_node,
        .annotation_node = annotation_node,
        .parameters = parameters,
        .is_generic = reader.readBool(36, 0),
    };
}

fn parseParameters(allocator: std.mem.Allocator, reader: message.StructReader, pointer_index: usize) ![]schema.Parameter {
    const list = (try readOptionalStructList(reader, pointer_index)) orelse return allocator.alloc(schema.Parameter, 0);
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var parameters = try allocator.alloc(schema.Parameter, count);
    var initialized: u32 = 0;
    errdefer {
        for (parameters[0..initialized]) |parameter| allocator.free(parameter.name);
        allocator.free(parameters);
    }

    while (initialized < count) : (initialized += 1) {
        parameters[initialized] = .{ .name = try dupText(allocator, try list.get(initialized), 0) };
    }
    return parameters;
}

fn parseNestedNodes(allocator: std.mem.Allocator, reader: message.StructReader) ![]schema.Node.NestedNode {
    const list = (try readOptionalStructList(reader, 1)) orelse return allocator.alloc(schema.Node.NestedNode, 0);
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var nested = try allocator.alloc(schema.Node.NestedNode, count);
    var initialized: u32 = 0;
    errdefer {
        for (nested[0..initialized]) |item| allocator.free(item.name);
        allocator.free(nested);
    }

    while (initialized < count) : (initialized += 1) {
        const item = try list.get(initialized);
        nested[initialized] = .{
            .name = try dupText(allocator, item, 0),
            .id = item.readU64(0),
        };
    }

    return nested;
}

fn parseStructNode(allocator: std.mem.Allocator, reader: message.StructReader) !schema.StructNode {
    const data_word_count = reader.readU16(14);
    const pointer_count = reader.readU16(24);
    const preferred_raw = reader.readU16(26);
    const preferred_list_encoding = std.enums.fromInt(schema.ElementSize, preferred_raw) orelse return error.InvalidElementSize;
    const is_group = reader.readBool(28, 0);
    const discriminant_count = reader.readU16(30);
    const discriminant_offset = reader.readU32(32);
    const fields = try parseFields(allocator, reader);

    return .{
        .data_word_count = data_word_count,
        .pointer_count = pointer_count,
        .preferred_list_encoding = preferred_list_encoding,
        .is_group = is_group,
        .discriminant_count = discriminant_count,
        .discriminant_offset = discriminant_offset,
        .fields = fields,
    };
}

fn parseEnumNode(allocator: std.mem.Allocator, reader: message.StructReader) !schema.EnumNode {
    const list = (try readOptionalStructList(reader, 3)) orelse return .{ .enumerants = try allocator.alloc(schema.Enumerant, 0) };
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var enumerants = try allocator.alloc(schema.Enumerant, count);
    var initialized: u32 = 0;
    errdefer {
        for (enumerants[0..initialized]) |e| {
            allocator.free(e.name);
            freeAnnotations(allocator, e.annotations);
        }
        allocator.free(enumerants);
    }

    while (initialized < count) : (initialized += 1) {
        const item = try list.get(initialized);
        const name = try dupText(allocator, item, 0);
        errdefer allocator.free(name);
        const ann = try parseAnnotations(allocator, item, 1);
        enumerants[initialized] = .{
            .name = name,
            .code_order = item.readU16(0),
            .annotations = ann,
        };
    }

    return .{ .enumerants = enumerants };
}

fn parseInterfaceNode(allocator: std.mem.Allocator, reader: message.StructReader) !schema.InterfaceNode {
    const maybe_list = try readOptionalStructList(reader, 3);
    const list = maybe_list orelse {
        const methods = try allocator.alloc(schema.Method, 0);
        errdefer allocator.free(methods);
        const superclasses = try allocator.alloc(schema.Id, 0);
        errdefer allocator.free(superclasses);
        const superclass_brands = try allocator.alloc(schema.Brand, 0);
        return .{
            .methods = methods,
            .superclasses = superclasses,
            .superclass_brands = superclass_brands,
        };
    };
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var methods = try allocator.alloc(schema.Method, count);
    var initialized: u32 = 0;
    errdefer {
        for (methods[0..initialized]) |m| {
            allocator.free(m.name);
            freeAnnotations(allocator, m.annotations);
            freeParameters(allocator, m.implicit_parameters);
            freeBrand(allocator, m.param_brand);
            freeBrand(allocator, m.result_brand);
        }
        allocator.free(methods);
    }

    while (initialized < count) : (initialized += 1) {
        const item = try list.get(initialized);
        const name = try dupText(allocator, item, 0);
        errdefer allocator.free(name);
        const ann = try parseAnnotations(allocator, item, 1);
        errdefer freeAnnotations(allocator, ann);
        const implicit_parameters = try parseParameters(allocator, item, 4);
        errdefer freeParameters(allocator, implicit_parameters);
        const param_brand = try parseOptionalBrand(allocator, item, 2, 0);
        errdefer freeBrand(allocator, param_brand);
        const result_brand = try parseOptionalBrand(allocator, item, 3, 0);
        methods[initialized] = .{
            .name = name,
            .code_order = item.readU16(0),
            .param_struct_type = item.readU64(8),
            .result_struct_type = item.readU64(16),
            .annotations = ann,
            .implicit_parameters = implicit_parameters,
            .param_brand = param_brand,
            .result_brand = result_brand,
        };
    }

    const superclass_info = try parseSuperclasses(allocator, reader);

    return .{
        .methods = methods,
        .superclasses = superclass_info.ids,
        .superclass_brands = superclass_info.brands,
    };
}

fn parseSuperclasses(allocator: std.mem.Allocator, reader: message.StructReader) !struct { ids: []schema.Id, brands: []schema.Brand } {
    const list = (try readOptionalStructList(reader, 4)) orelse {
        const ids = try allocator.alloc(schema.Id, 0);
        errdefer allocator.free(ids);
        return .{ .ids = ids, .brands = try allocator.alloc(schema.Brand, 0) };
    };
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var superclasses = try allocator.alloc(schema.Id, count);
    errdefer allocator.free(superclasses);
    var brands = try allocator.alloc(schema.Brand, count);
    var initialized: usize = 0;
    errdefer {
        freeBrands(allocator, brands[0..initialized]);
        allocator.free(brands);
    }

    while (initialized < count) : (initialized += 1) {
        const i = initialized;
        const item = try list.get(@intCast(i));
        superclasses[i] = item.readU64(0);
        brands[i] = try parseOptionalBrand(allocator, item, 0, 0);
    }

    return .{ .ids = superclasses, .brands = brands };
}

fn parseConstNode(allocator: std.mem.Allocator, reader: message.StructReader) !schema.ConstNode {
    const type_reader = try reader.readStruct(3);
    const typ = try parseType(allocator, type_reader, 0);
    errdefer freeTypeExpression(allocator, typ);

    const value_reader = try readOptionalStruct(reader, 4);
    const value = if (value_reader) |value|
        (try parseValue(allocator, value)) orelse return error.InvalidConstValue
    else
        return error.InvalidConstValue;

    return .{
        .type = typ.type,
        .value = value,
        .type_metadata = typ.metadata,
    };
}

fn parseAnnotationNode(allocator: std.mem.Allocator, reader: message.StructReader) !schema.AnnotationNode {
    const type_reader = try reader.readStruct(3);
    const typ = try parseType(allocator, type_reader, 0);

    return .{
        .type = typ.type,
        .type_metadata = typ.metadata,
        .targets_file = reader.readBool(14, 0),
        .targets_const = reader.readBool(14, 1),
        .targets_enum = reader.readBool(14, 2),
        .targets_enumerant = reader.readBool(14, 3),
        .targets_struct = reader.readBool(14, 4),
        .targets_field = reader.readBool(14, 5),
        .targets_union = reader.readBool(14, 6),
        .targets_group = reader.readBool(14, 7),
        .targets_interface = reader.readBool(15, 0),
        .targets_method = reader.readBool(15, 1),
        .targets_param = reader.readBool(15, 2),
        .targets_annotation = reader.readBool(15, 3),
    };
}

fn parseFields(allocator: std.mem.Allocator, reader: message.StructReader) ![]schema.Field {
    const list = (try readOptionalStructList(reader, 3)) orelse return allocator.alloc(schema.Field, 0);
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var fields = try allocator.alloc(schema.Field, count);
    var initialized: u32 = 0;
    errdefer {
        for (fields[0..initialized]) |field| {
            allocator.free(field.name);
            freeAnnotations(allocator, field.annotations);
            if (field.slot) |slot| {
                freeTypeExpression(allocator, .{ .type = slot.type, .metadata = slot.type_metadata });
                if (slot.default_value) |value| freeValue(allocator, value);
            }
        }
        allocator.free(fields);
    }

    while (initialized < count) : (initialized += 1) {
        fields[initialized] = try parseField(allocator, try list.get(initialized));
    }

    return fields;
}

fn parseField(allocator: std.mem.Allocator, reader: message.StructReader) !schema.Field {
    const name = try dupText(allocator, reader, 0);
    errdefer allocator.free(name);
    const code_order = reader.readU16(0);
    const annotations = try parseAnnotations(allocator, reader, 1);
    errdefer freeAnnotations(allocator, annotations);
    // schema.capnp declares Field.discriminantValue with a non-zero default
    // (0xffff), so values are stored XOR'd with that default on the wire.
    const discriminant_value = reader.readU16(2) ^ @as(u16, 0xffff);

    const which_raw = reader.readU16(8);
    const which_tag = std.enums.fromInt(FieldWhich, which_raw) orelse return error.InvalidFieldKind;

    var slot: ?schema.FieldSlot = null;
    var group: ?schema.FieldGroup = null;

    switch (which_tag) {
        .slot => {
            const offset = reader.readU32(4);
            const type_reader = try reader.readStruct(2);
            const field_type = try parseType(allocator, type_reader, 0);
            errdefer freeTypeExpression(allocator, field_type);

            const default_value_reader = try readOptionalStruct(reader, 3);

            const default_value = if (default_value_reader) |value|
                (try parseValue(allocator, value))
            else
                null;

            slot = .{
                .offset = offset,
                .type = field_type.type,
                .default_value = default_value,
                .type_metadata = field_type.metadata,
            };
        },
        .group => {
            const type_id = reader.readU64(16);
            group = .{ .type_id = type_id };
        },
    }

    return .{
        .name = name,
        .code_order = code_order,
        .annotations = annotations,
        .discriminant_value = discriminant_value,
        .slot = slot,
        .group = group,
    };
}

fn parseAnnotations(allocator: std.mem.Allocator, reader: message.StructReader, pointer_index: usize) ![]schema.AnnotationUse {
    const list = (try readOptionalStructList(reader, pointer_index)) orelse return allocator.alloc(schema.AnnotationUse, 0);
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var annotations = try allocator.alloc(schema.AnnotationUse, count);
    var initialized: u32 = 0;
    errdefer {
        for (annotations[0..initialized]) |ann| {
            freeValue(allocator, ann.value);
            freeBrand(allocator, ann.brand);
        }
        allocator.free(annotations);
    }

    while (initialized < count) : (initialized += 1) {
        const item = try list.get(initialized);
        const id = item.readU64(0);
        const value_reader = try readOptionalStruct(item, 0);
        const value = if (value_reader) |value|
            (try parseValue(allocator, value)) orelse .void
        else
            .void;
        errdefer freeValue(allocator, value);
        // Annotation.value is pointer 0 and Annotation.brand is pointer 1 in
        // schema.capnp. Preserve the latter for generic lexical scopes.
        const brand = try parseOptionalBrand(allocator, item, 1, 0);

        annotations[initialized] = .{
            .id = id,
            .value = value,
            .brand = brand,
        };
    }

    return annotations;
}

fn parseRequestedFiles(allocator: std.mem.Allocator, root: message.StructReader) ![]schema.RequestedFile {
    const list = (try readOptionalStructList(root, 1)) orelse return allocator.alloc(schema.RequestedFile, 0);
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var requested = try allocator.alloc(schema.RequestedFile, count);
    var initialized: u32 = 0;
    errdefer {
        for (requested[0..initialized]) |file| {
            allocator.free(file.filename);
            for (file.imports) |imp| allocator.free(imp.name);
            allocator.free(file.imports);
        }
        allocator.free(requested);
    }

    while (initialized < count) : (initialized += 1) {
        requested[initialized] = try parseRequestedFile(allocator, try list.get(initialized));
    }

    return requested;
}

fn parseRequestedFile(allocator: std.mem.Allocator, reader: message.StructReader) !schema.RequestedFile {
    const id = reader.readU64(0);
    const filename = try dupText(allocator, reader, 0);
    errdefer allocator.free(filename);
    const imports = try parseImports(allocator, reader);

    return .{
        .id = id,
        .filename = filename,
        .imports = imports,
    };
}

fn parseImports(allocator: std.mem.Allocator, reader: message.StructReader) ![]schema.Import {
    const list = (try readOptionalStructList(reader, 1)) orelse return allocator.alloc(schema.Import, 0);
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var imports = try allocator.alloc(schema.Import, count);
    var initialized: u32 = 0;
    errdefer {
        for (imports[0..initialized]) |imp| allocator.free(imp.name);
        allocator.free(imports);
    }

    while (initialized < count) : (initialized += 1) {
        const item = try list.get(initialized);
        imports[initialized] = .{
            .id = item.readU64(0),
            .name = try dupText(allocator, item, 0),
        };
    }

    return imports;
}

fn parseType(allocator: std.mem.Allocator, reader: message.StructReader, depth: u8) anyerror!schema.TypeExpression {
    if (depth > 64) return error.TypeNestingTooDeep;

    const which_raw = reader.readU16(0);
    const which_tag = std.enums.fromInt(TypeWhich, which_raw) orelse return error.InvalidTypeKind;

    return switch (which_tag) {
        .void => .{ .type = .void },
        .bool => .{ .type = .bool },
        .int8 => .{ .type = .int8 },
        .int16 => .{ .type = .int16 },
        .int32 => .{ .type = .int32 },
        .int64 => .{ .type = .int64 },
        .uint8 => .{ .type = .uint8 },
        .uint16 => .{ .type = .uint16 },
        .uint32 => .{ .type = .uint32 },
        .uint64 => .{ .type = .uint64 },
        .float32 => .{ .type = .float32 },
        .float64 => .{ .type = .float64 },
        .text => .{ .type = .text },
        .data => .{ .type = .data },
        .list => blk: {
            const element_reader = try reader.readStruct(0);
            const element = try parseType(allocator, element_reader, depth + 1);
            errdefer freeTypeExpression(allocator, element);
            const element_ptr = try allocator.create(schema.Type);
            errdefer allocator.destroy(element_ptr);
            const metadata_ptr = try allocator.create(schema.TypeMetadata);
            element_ptr.* = element.type;
            metadata_ptr.* = element.metadata;
            break :blk .{
                .type = .{ .list = .{ .element_type = element_ptr } },
                .metadata = .{ .list = metadata_ptr },
            };
        },
        .@"enum" => .{
            .type = .{ .@"enum" = .{ .type_id = reader.readU64(8) } },
            .metadata = .{ .named = try parseOptionalBrand(allocator, reader, 0, depth + 1) },
        },
        .@"struct" => .{
            .type = .{ .@"struct" = .{ .type_id = reader.readU64(8) } },
            .metadata = .{ .named = try parseOptionalBrand(allocator, reader, 0, depth + 1) },
        },
        .interface => .{
            .type = .{ .interface = .{ .type_id = reader.readU64(8) } },
            .metadata = .{ .named = try parseOptionalBrand(allocator, reader, 0, depth + 1) },
        },
        .any_pointer => .{
            .type = .any_pointer,
            .metadata = .{ .any_pointer = try parseAnyPointerType(reader) },
        },
    };
}

fn parseAnyPointerType(reader: message.StructReader) !schema.TypeMetadata.AnyPointer {
    const outer = std.enums.fromInt(AnyPointerWhich, reader.readU16(8)) orelse return error.InvalidAnyPointerKind;
    return switch (outer) {
        .unconstrained => switch (std.enums.fromInt(UnconstrainedAnyPointerWhich, reader.readU16(10)) orelse return error.InvalidAnyPointerKind) {
            .any_kind => .{ .unconstrained = .any_kind },
            .@"struct" => .{ .unconstrained = .@"struct" },
            .list => .{ .unconstrained = .list },
            .capability => .{ .unconstrained = .capability },
        },
        .parameter => .{ .parameter = .{
            .scope_id = reader.readU64(16),
            .parameter_index = reader.readU16(10),
        } },
        .implicit_method_parameter => .{ .implicit_method_parameter = .{
            .parameter_index = reader.readU16(10),
        } },
    };
}

fn parseOptionalBrand(
    allocator: std.mem.Allocator,
    reader: message.StructReader,
    pointer_index: usize,
    depth: u8,
) !schema.Brand {
    const brand_reader = try readOptionalStruct(reader, pointer_index);
    if (brand_reader) |brand| return parseBrand(allocator, brand, depth);
    return .{ .scopes = try allocator.alloc(schema.Brand.Scope, 0) };
}

fn parseBrand(allocator: std.mem.Allocator, reader: message.StructReader, depth: u8) !schema.Brand {
    if (depth > 64) return error.TypeNestingTooDeep;
    const list = (try readOptionalStructList(reader, 0)) orelse
        return .{ .scopes = try allocator.alloc(schema.Brand.Scope, 0) };
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var scopes = try allocator.alloc(schema.Brand.Scope, count);
    var initialized: u32 = 0;
    errdefer {
        for (scopes[0..initialized]) |scope| freeBrandScope(allocator, scope);
        allocator.free(scopes);
    }

    while (initialized < count) : (initialized += 1) {
        const item = try list.get(initialized);
        const which = std.enums.fromInt(BrandScopeWhich, item.readU16(8)) orelse return error.InvalidBrandScopeKind;
        scopes[initialized] = .{
            .scope_id = item.readU64(0),
            .binding = switch (which) {
                .inherit => .inherit,
                .bind => .{ .bind = try parseBrandBindings(allocator, item, depth + 1) },
            },
        };
    }
    return .{ .scopes = scopes };
}

fn parseBrandBindings(allocator: std.mem.Allocator, reader: message.StructReader, depth: u8) ![]schema.Brand.Binding {
    const list = (try readOptionalStructList(reader, 0)) orelse return allocator.alloc(schema.Brand.Binding, 0);
    try rejectZeroWidthStructList(list);

    const count = list.len();
    var bindings = try allocator.alloc(schema.Brand.Binding, count);
    var initialized: u32 = 0;
    errdefer {
        for (bindings[0..initialized]) |binding| freeBrandBinding(allocator, binding);
        allocator.free(bindings);
    }

    while (initialized < count) : (initialized += 1) {
        const item = try list.get(initialized);
        const which = std.enums.fromInt(BrandBindingWhich, item.readU16(0)) orelse return error.InvalidBrandBindingKind;
        bindings[initialized] = switch (which) {
            .unbound => .unbound,
            .type => blk: {
                const type_reader = (try readOptionalStruct(item, 0)) orelse return error.InvalidBrandBindingType;
                const value = try parseType(allocator, type_reader, depth);
                errdefer freeTypeExpression(allocator, value);
                const value_ptr = try allocator.create(schema.TypeExpression);
                value_ptr.* = value;
                break :blk .{ .type = value_ptr };
            },
        };
    }
    return bindings;
}

fn parseValue(allocator: std.mem.Allocator, reader: message.StructReader) !?schema.Value {
    const which_raw = reader.readU16(0);
    const which_tag = std.enums.fromInt(ValueWhich, which_raw) orelse return error.InvalidValueKind;

    return switch (which_tag) {
        .void => .void,
        .bool => .{ .bool = reader.readBool(2, 0) },
        .int8 => .{ .int8 = @bitCast(reader.readU8(2)) },
        .int16 => .{ .int16 = @bitCast(reader.readU16(2)) },
        .int32 => .{ .int32 = @bitCast(reader.readU32(4)) },
        .int64 => .{ .int64 = @bitCast(reader.readU64(8)) },
        .uint8 => .{ .uint8 = reader.readU8(2) },
        .uint16 => .{ .uint16 = reader.readU16(2) },
        .uint32 => .{ .uint32 = reader.readU32(4) },
        .uint64 => .{ .uint64 = reader.readU64(8) },
        .float32 => .{ .float32 = @bitCast(reader.readU32(4)) },
        .float64 => .{ .float64 = @bitCast(reader.readU64(8)) },
        .text => blk: {
            const text = try reader.readTextStrict(0);
            const owned = try allocator.dupe(u8, text);
            break :blk .{ .text = owned };
        },
        .data => blk: {
            const data = try readOptionalData(reader, 0);
            const owned = try allocator.dupe(u8, data);
            break :blk .{ .data = owned };
        },
        .list => blk: {
            const any = (try readOptionalAnyPointer(reader, 0)) orelse return null;
            const bytes = try message.cloneAnyPointerToBytes(allocator, any);
            break :blk .{ .list = .{ .message_bytes = bytes } };
        },
        .@"enum" => .{ .@"enum" = reader.readU16(2) },
        .@"struct" => blk: {
            const any = (try readOptionalAnyPointer(reader, 0)) orelse return null;
            const bytes = try message.cloneAnyPointerToBytes(allocator, any);
            break :blk .{ .@"struct" = .{ .message_bytes = bytes } };
        },
        .interface => .interface,
        .any_pointer => blk: {
            const any = (try readOptionalAnyPointer(reader, 0)) orelse return null;
            const bytes = try message.cloneAnyPointerToBytes(allocator, any);
            break :blk .{ .any_pointer = .{ .message_bytes = bytes } };
        },
    };
}

fn dupText(allocator: std.mem.Allocator, reader: message.StructReader, pointer_index: usize) ![]const u8 {
    const text = try reader.readTextStrict(pointer_index);
    return allocator.dupe(u8, text);
}

fn readOptionalStruct(reader: message.StructReader, pointer_index: usize) !?message.StructReader {
    return reader.readStruct(pointer_index) catch |err| switch (err) {
        error.InvalidPointer, error.OutOfBounds => if (reader.isPointerNull(pointer_index)) null else return err,
        else => return err,
    };
}

fn readOptionalStructList(reader: message.StructReader, pointer_index: usize) !?message.StructListReader {
    return reader.readStructList(pointer_index) catch |err| switch (err) {
        error.InvalidPointer,
        error.InvalidInlineCompositePointer,
        error.OutOfBounds,
        => if (reader.isPointerNull(pointer_index)) null else return err,
        else => return err,
    };
}

fn readOptionalAnyPointer(reader: message.StructReader, pointer_index: usize) !?message.AnyPointerReader {
    const any = reader.readAnyPointer(pointer_index) catch |err| switch (err) {
        error.OutOfBounds => return null,
    };
    if (any.isNull()) return null;
    return any;
}

fn readOptionalData(reader: message.StructReader, pointer_index: usize) ![]const u8 {
    return reader.readData(pointer_index) catch |err| switch (err) {
        error.InvalidPointer, error.OutOfBounds => if (reader.isPointerNull(pointer_index)) &[_]u8{} else return err,
        else => return err,
    };
}

/// Reject a struct list whose elements occupy no space at all: a huge element
/// count over zero-width elements is an amplification vector against the
/// plugin, which allocates one schema node per element.
///
/// `sub_word_data_bytes` must be part of the test. A list upgraded from element
/// size C = 2/3/4 legitimately reports `data_words == 0` and
/// `pointer_words == 0` while carrying a real 1/2/4-byte element, so without
/// this conjunct the plugin would reject valid CodeGeneratorRequests. A Void
/// (C = 0) upgrade still lands on all-zero and is still rejected, so the guard
/// keeps its full strength.
fn rejectZeroWidthStructList(list: message.StructListReader) !void {
    if (list.element_count > 0 and
        list.data_words == 0 and
        list.pointer_words == 0 and
        list.sub_word_data_bytes == 0)
    {
        return error.InvalidInlineCompositePointer;
    }
}

fn freeNodes(allocator: std.mem.Allocator, nodes: []schema.Node) void {
    for (nodes) |node| {
        allocator.free(node.display_name);
        freeNestedNodes(allocator, node.nested_nodes);
        freeAnnotations(allocator, node.annotations);
        freeParameters(allocator, node.parameters);

        if (node.struct_node) |struct_node| {
            freeFields(allocator, struct_node.fields);
        }
        if (node.enum_node) |enum_node| {
            freeEnumerants(allocator, enum_node.enumerants);
        }
        if (node.interface_node) |interface_node| {
            freeMethods(allocator, interface_node.methods);
            allocator.free(interface_node.superclasses);
            freeBrands(allocator, interface_node.superclass_brands);
            allocator.free(interface_node.superclass_brands);
        }
        if (node.const_node) |const_node| {
            freeTypeExpression(allocator, .{ .type = const_node.type, .metadata = const_node.type_metadata });
            freeValue(allocator, const_node.value);
        }
        if (node.annotation_node) |annotation_node| {
            freeTypeExpression(allocator, .{ .type = annotation_node.type, .metadata = annotation_node.type_metadata });
        }
    }

    allocator.free(nodes);
}

fn freeNestedNodes(allocator: std.mem.Allocator, nested: []schema.Node.NestedNode) void {
    for (nested) |item| {
        allocator.free(item.name);
    }
    allocator.free(nested);
}

fn freeFields(allocator: std.mem.Allocator, fields: []schema.Field) void {
    for (fields) |field| {
        allocator.free(field.name);
        freeAnnotations(allocator, field.annotations);
        if (field.slot) |slot| {
            freeTypeExpression(allocator, .{ .type = slot.type, .metadata = slot.type_metadata });
            if (slot.default_value) |value| freeValue(allocator, value);
        }
    }
    allocator.free(fields);
}

fn freeEnumerants(allocator: std.mem.Allocator, enumerants: []schema.Enumerant) void {
    for (enumerants) |enumerant| {
        allocator.free(enumerant.name);
        freeAnnotations(allocator, enumerant.annotations);
    }
    allocator.free(enumerants);
}

fn freeMethods(allocator: std.mem.Allocator, methods: []schema.Method) void {
    for (methods) |method| {
        allocator.free(method.name);
        freeAnnotations(allocator, method.annotations);
        freeParameters(allocator, method.implicit_parameters);
        freeBrand(allocator, method.param_brand);
        freeBrand(allocator, method.result_brand);
    }
    allocator.free(methods);
}

fn freeParameters(allocator: std.mem.Allocator, parameters: []schema.Parameter) void {
    for (parameters) |parameter| allocator.free(parameter.name);
    allocator.free(parameters);
}

fn freeType(allocator: std.mem.Allocator, typ: schema.Type) void {
    switch (typ) {
        .list => |info| {
            const element = info.element_type;
            freeType(allocator, element.*);
            allocator.destroy(element);
        },
        else => {},
    }
}

fn freeTypeExpression(allocator: std.mem.Allocator, expression: schema.TypeExpression) void {
    freeType(allocator, expression.type);
    freeTypeMetadata(allocator, expression.metadata);
}

fn freeTypeMetadata(allocator: std.mem.Allocator, metadata: schema.TypeMetadata) void {
    switch (metadata) {
        .none, .any_pointer => {},
        .named => |brand| freeBrand(allocator, brand),
        .list => |element| {
            freeTypeMetadata(allocator, element.*);
            allocator.destroy(element);
        },
    }
}

fn freeBrands(allocator: std.mem.Allocator, brands: []schema.Brand) void {
    for (brands) |brand| freeBrand(allocator, brand);
}

fn freeBrand(allocator: std.mem.Allocator, brand: schema.Brand) void {
    for (brand.scopes) |scope| freeBrandScope(allocator, scope);
    allocator.free(brand.scopes);
}

fn freeBrandScope(allocator: std.mem.Allocator, scope: schema.Brand.Scope) void {
    switch (scope.binding) {
        .inherit => {},
        .bind => |bindings| {
            for (bindings) |binding| freeBrandBinding(allocator, binding);
            allocator.free(bindings);
        },
    }
}

fn freeBrandBinding(allocator: std.mem.Allocator, binding: schema.Brand.Binding) void {
    switch (binding) {
        .unbound => {},
        .type => |typ| {
            freeTypeExpression(allocator, typ.*);
            allocator.destroy(typ);
        },
    }
}

fn freeValue(allocator: std.mem.Allocator, value: schema.Value) void {
    switch (value) {
        .text => |text| allocator.free(text),
        .data => |data| allocator.free(data),
        .list => |info| allocator.free(info.message_bytes),
        .@"struct" => |info| allocator.free(info.message_bytes),
        .any_pointer => |info| allocator.free(info.message_bytes),
        else => {},
    }
}

fn freeAnnotations(allocator: std.mem.Allocator, annotations: []schema.AnnotationUse) void {
    for (annotations) |annotation| {
        freeValue(allocator, annotation.value);
        freeBrand(allocator, annotation.brand);
    }
    allocator.free(annotations);
}

fn freeRequestedFiles(allocator: std.mem.Allocator, requested: []schema.RequestedFile) void {
    for (requested) |file| {
        allocator.free(file.filename);
        for (file.imports) |imp| {
            allocator.free(imp.name);
        }
        allocator.free(file.imports);
    }
    allocator.free(requested);
}
