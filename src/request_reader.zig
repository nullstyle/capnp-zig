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
    var msg = try message.Message.init(allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();

    const nodes = try parseNodeList(allocator, root);
    errdefer freeNodes(allocator, nodes);

    const requested_files = try parseRequestedFiles(allocator, root);
    errdefer freeRequestedFiles(allocator, requested_files);

    var capnp_version: ?schema.CapnpVersion = null;
    const version_reader = root.readStruct(2) catch |err| switch (err) {
        error.InvalidPointer => null,
        else => return err,
    };
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
    const list = root.readStructList(0) catch |err| switch (err) {
        error.InvalidPointer => return allocator.alloc(schema.Node, 0),
        else => return err,
    };

    const count = list.len();
    var nodes = try allocator.alloc(schema.Node, count);
    var initialized: u32 = 0;
    errdefer {
        for (nodes[0..initialized]) |node| {
            allocator.free(node.display_name);
            freeNestedNodes(allocator, node.nested_nodes);
            freeAnnotations(allocator, node.annotations);
            if (node.struct_node) |sn| freeFields(allocator, sn.fields);
            if (node.enum_node) |en| freeEnumerants(allocator, en.enumerants);
            if (node.interface_node) |in_| freeMethods(allocator, in_.methods);
            if (node.const_node) |cn| {
                freeType(allocator, cn.type);
                freeValue(allocator, cn.value);
            }
            if (node.annotation_node) |an| freeType(allocator, an.type);
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

    const kind_raw = reader.readU16(12);
    const kind_tag = std.meta.intToEnum(NodeWhich, kind_raw) catch return error.InvalidNodeKind;

    var struct_node: ?schema.StructNode = null;
    var enum_node: ?schema.EnumNode = null;
    var interface_node: ?schema.InterfaceNode = null;
    var const_node: ?schema.ConstNode = null;
    var annotation_node: ?schema.AnnotationNode = null;

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
    };
}

fn parseNestedNodes(allocator: std.mem.Allocator, reader: message.StructReader) ![]schema.Node.NestedNode {
    const list = reader.readStructList(1) catch |err| switch (err) {
        error.InvalidPointer => return allocator.alloc(schema.Node.NestedNode, 0),
        else => return err,
    };

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
    const preferred_list_encoding = std.meta.intToEnum(schema.ElementSize, preferred_raw) catch return error.InvalidElementSize;
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
    const list = reader.readStructList(3) catch |err| switch (err) {
        error.InvalidPointer => return .{ .enumerants = try allocator.alloc(schema.Enumerant, 0) },
        else => return err,
    };

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
    const list = reader.readStructList(3) catch |err| switch (err) {
        error.InvalidPointer => return .{ .methods = try allocator.alloc(schema.Method, 0) },
        else => return err,
    };

    const count = list.len();
    var methods = try allocator.alloc(schema.Method, count);
    var initialized: u32 = 0;
    errdefer {
        for (methods[0..initialized]) |m| {
            allocator.free(m.name);
            freeAnnotations(allocator, m.annotations);
        }
        allocator.free(methods);
    }

    while (initialized < count) : (initialized += 1) {
        const item = try list.get(initialized);
        const name = try dupText(allocator, item, 0);
        errdefer allocator.free(name);
        const ann = try parseAnnotations(allocator, item, 1);
        methods[initialized] = .{
            .name = name,
            .code_order = item.readU16(0),
            .param_struct_type = item.readU64(8),
            .result_struct_type = item.readU64(16),
            .annotations = ann,
        };
    }

    return .{ .methods = methods };
}

fn parseConstNode(allocator: std.mem.Allocator, reader: message.StructReader) !schema.ConstNode {
    const type_reader = try reader.readStruct(3);
    const typ = try parseType(allocator, type_reader);
    errdefer freeType(allocator, typ);

    const value_reader = reader.readStruct(4) catch |err| switch (err) {
        error.InvalidPointer => null,
        else => return err,
    };
    const value = if (value_reader) |value|
        (try parseValue(allocator, value)) orelse return error.InvalidConstValue
    else
        return error.InvalidConstValue;

    return .{
        .type = typ,
        .value = value,
    };
}

fn parseAnnotationNode(allocator: std.mem.Allocator, reader: message.StructReader) !schema.AnnotationNode {
    const type_reader = try reader.readStruct(3);
    const typ = try parseType(allocator, type_reader);

    return .{
        .type = typ,
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
    const list = reader.readStructList(3) catch |err| switch (err) {
        error.InvalidPointer => return allocator.alloc(schema.Field, 0),
        else => return err,
    };

    const count = list.len();
    var fields = try allocator.alloc(schema.Field, count);
    var initialized: u32 = 0;
    errdefer {
        for (fields[0..initialized]) |field| {
            allocator.free(field.name);
            freeAnnotations(allocator, field.annotations);
            if (field.slot) |slot| {
                freeType(allocator, slot.type);
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
    const discriminant_value = reader.readU16(2);

    const which_raw = reader.readU16(8);
    const which_tag = std.meta.intToEnum(FieldWhich, which_raw) catch return error.InvalidFieldKind;

    var slot: ?schema.FieldSlot = null;
    var group: ?schema.FieldGroup = null;

    switch (which_tag) {
        .slot => {
            const offset = reader.readU32(4);
            const type_reader = try reader.readStruct(2);
            const field_type = try parseType(allocator, type_reader);
            errdefer freeType(allocator, field_type);

            const default_value_reader = reader.readStruct(3) catch |err| switch (err) {
                error.InvalidPointer => null,
                else => return err,
            };

            const default_value = if (default_value_reader) |value|
                (try parseValue(allocator, value))
            else
                null;

            slot = .{
                .offset = offset,
                .type = field_type,
                .default_value = default_value,
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
    const list = reader.readStructList(pointer_index) catch |err| switch (err) {
        error.InvalidPointer => return allocator.alloc(schema.AnnotationUse, 0),
        else => return err,
    };

    const count = list.len();
    var annotations = try allocator.alloc(schema.AnnotationUse, count);
    var initialized: u32 = 0;
    errdefer {
        for (annotations[0..initialized]) |ann| freeValue(allocator, ann.value);
        allocator.free(annotations);
    }

    while (initialized < count) : (initialized += 1) {
        const item = try list.get(initialized);
        const id = item.readU64(0);
        const value_reader = item.readStruct(0) catch |err| switch (err) {
            error.InvalidPointer => null,
            else => return err,
        };
        const value = if (value_reader) |value|
            (try parseValue(allocator, value)) orelse .void
        else
            .void;

        annotations[initialized] = .{
            .id = id,
            .value = value,
        };
    }

    return annotations;
}

fn parseRequestedFiles(allocator: std.mem.Allocator, root: message.StructReader) ![]schema.RequestedFile {
    const list = root.readStructList(1) catch |err| switch (err) {
        error.InvalidPointer => return allocator.alloc(schema.RequestedFile, 0),
        else => return err,
    };

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
    const list = reader.readStructList(1) catch |err| switch (err) {
        error.InvalidPointer => return allocator.alloc(schema.Import, 0),
        else => return err,
    };

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

fn parseType(allocator: std.mem.Allocator, reader: message.StructReader) !schema.Type {
    const which_raw = reader.readU16(0);
    const which_tag = std.meta.intToEnum(TypeWhich, which_raw) catch return error.InvalidTypeKind;

    return switch (which_tag) {
        .void => .void,
        .bool => .bool,
        .int8 => .int8,
        .int16 => .int16,
        .int32 => .int32,
        .int64 => .int64,
        .uint8 => .uint8,
        .uint16 => .uint16,
        .uint32 => .uint32,
        .uint64 => .uint64,
        .float32 => .float32,
        .float64 => .float64,
        .text => .text,
        .data => .data,
        .list => blk: {
            const element_reader = try reader.readStruct(0);
            const element_type = try parseType(allocator, element_reader);
            errdefer freeType(allocator, element_type);
            const element_ptr = try allocator.create(schema.Type);
            element_ptr.* = element_type;
            break :blk .{ .list = .{ .element_type = element_ptr } };
        },
        .@"enum" => .{ .@"enum" = .{ .type_id = reader.readU64(8) } },
        .@"struct" => .{ .@"struct" = .{ .type_id = reader.readU64(8) } },
        .interface => .{ .interface = .{ .type_id = reader.readU64(8) } },
        .any_pointer => .any_pointer,
    };
}

fn parseValue(allocator: std.mem.Allocator, reader: message.StructReader) !?schema.Value {
    const which_raw = reader.readU16(0);
    const which_tag = std.meta.intToEnum(ValueWhich, which_raw) catch return null;

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
            const text = reader.readText(0) catch "";
            const owned = try allocator.dupe(u8, text);
            break :blk .{ .text = owned };
        },
        .data => blk: {
            const data = reader.readData(0) catch &[_]u8{};
            const owned = try allocator.dupe(u8, data);
            break :blk .{ .data = owned };
        },
        .list => blk: {
            const any = try reader.readAnyPointer(0);
            if (any.pointer_word == 0) return null;
            const bytes = try message.cloneAnyPointerToBytes(allocator, any);
            break :blk .{ .list = .{ .message_bytes = bytes } };
        },
        .@"enum" => .{ .@"enum" = reader.readU16(2) },
        .@"struct" => blk: {
            const any = try reader.readAnyPointer(0);
            if (any.pointer_word == 0) return null;
            const bytes = try message.cloneAnyPointerToBytes(allocator, any);
            break :blk .{ .@"struct" = .{ .message_bytes = bytes } };
        },
        .interface => .interface,
        .any_pointer => blk: {
            const any = try reader.readAnyPointer(0);
            if (any.pointer_word == 0) return null;
            const bytes = try message.cloneAnyPointerToBytes(allocator, any);
            break :blk .{ .any_pointer = .{ .message_bytes = bytes } };
        },
    };
}

fn dupText(allocator: std.mem.Allocator, reader: message.StructReader, pointer_index: usize) ![]const u8 {
    const text = try reader.readText(pointer_index);
    return allocator.dupe(u8, text);
}

fn freeNodes(allocator: std.mem.Allocator, nodes: []schema.Node) void {
    for (nodes) |node| {
        allocator.free(node.display_name);
        freeNestedNodes(allocator, node.nested_nodes);
        freeAnnotations(allocator, node.annotations);

        if (node.struct_node) |struct_node| {
            freeFields(allocator, struct_node.fields);
        }
        if (node.enum_node) |enum_node| {
            freeEnumerants(allocator, enum_node.enumerants);
        }
        if (node.interface_node) |interface_node| {
            freeMethods(allocator, interface_node.methods);
        }
        if (node.const_node) |const_node| {
            freeType(allocator, const_node.type);
            freeValue(allocator, const_node.value);
        }
        if (node.annotation_node) |annotation_node| {
            freeType(allocator, annotation_node.type);
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
            freeType(allocator, slot.type);
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
    }
    allocator.free(methods);
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
