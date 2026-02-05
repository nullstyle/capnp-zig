const std = @import("std");
const schema = @import("../schema.zig");
const TypeGenerator = @import("types.zig").TypeGenerator;

pub const StructGenerator = struct {
    allocator: std.mem.Allocator,
    type_gen: TypeGenerator,
    node_lookup_ctx: ?*const anyopaque,
    node_lookup: ?*const fn (ctx: ?*const anyopaque, id: schema.Id) ?*const schema.Node,

    pub fn init(allocator: std.mem.Allocator) StructGenerator {
        return .{
            .allocator = allocator,
            .type_gen = TypeGenerator.init(allocator),
            .node_lookup_ctx = null,
            .node_lookup = null,
        };
    }

    pub fn initWithLookup(
        allocator: std.mem.Allocator,
        node_lookup: *const fn (ctx: ?*const anyopaque, id: schema.Id) ?*const schema.Node,
        node_lookup_ctx: ?*const anyopaque,
    ) StructGenerator {
        return .{
            .allocator = allocator,
            .type_gen = TypeGenerator.init(allocator),
            .node_lookup_ctx = node_lookup_ctx,
            .node_lookup = node_lookup,
        };
    }

    fn getNode(self: *const StructGenerator, id: schema.Id) ?*const schema.Node {
        const lookup = self.node_lookup orelse return null;
        return lookup(self.node_lookup_ctx, id);
    }

    pub fn generate(self: *StructGenerator, node: *const schema.Node, writer: anytype) !void {
        const struct_info = node.struct_node orelse return error.InvalidStructNode;
        const name = self.getSimpleName(node);

        const data_word_count = struct_info.data_word_count;
        const pointer_count = struct_info.pointer_count;

        try writer.print("pub const {s} = struct {{\n", .{name});
        try writer.writeAll("    const message = @import(\"message.zig\");\n\n");

        // Generate Reader
        try self.generateReader(struct_info, data_word_count, pointer_count, writer);

        // Generate Builder
        try self.generateBuilder(struct_info, data_word_count, pointer_count, writer);

        try writer.writeAll("};\n\n");
    }

    fn generateReader(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        data_word_count: u16,
        pointer_count: u16,
        writer: anytype,
    ) !void {
        _ = data_word_count;
        _ = pointer_count;
        try writer.writeAll("    pub const Reader = struct {\n");
        try writer.writeAll("        _reader: message.StructReader,\n\n");

        try self.generatePointerDefaults(struct_info, writer);

        try writer.writeAll("        pub fn init(msg: *const message.Message) !Reader {\n");
        try writer.writeAll("            const root = try msg.getRootStruct();\n");
        try writer.writeAll("            return .{ ._reader = root };\n");
        try writer.writeAll("        }\n\n");

        // Generate field getters
        for (struct_info.fields) |field| {
            try self.generateFieldGetter(field, writer);
        }

        try writer.writeAll("    };\n\n");
    }

    fn generateFieldGetter(self: *StructGenerator, field: schema.Field, writer: anytype) !void {
        const slot = field.slot orelse return;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);

        const zig_type = try self.readerTypeString(slot.type);
        defer self.allocator.free(zig_type);

        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        try writer.print("        pub fn get{s}(self: Reader) !{s} {{\n", .{
            cap_name,
            zig_type,
        });

        switch (slot.type) {
            .void => try writer.writeAll("            return {};\n"),
            .bool => {
                const byte_offset = self.dataByteOffset(slot.type, slot.offset);
                const bit_offset = @as(u3, @truncate(slot.offset % 8));
                if (slot.default_value) |default_value| {
                    const default_bool = self.defaultBool(default_value);
                    try writer.print("            return self._reader.readBool({}, {}) != {s};\n", .{
                        byte_offset,
                        bit_offset,
                        if (default_bool) "true" else "false",
                    });
                } else {
                    try writer.print("            return self._reader.readBool({}, {});\n", .{ byte_offset, bit_offset });
                }
            },
            .int8,
            .uint8,
            .int16,
            .uint16,
            .int32,
            .uint32,
            .int64,
            .uint64,
            .float32,
            .float64,
            => {
                const read_fn = self.readFnForType(slot.type);
                const byte_offset = self.dataByteOffset(slot.type, slot.offset);
                if (slot.default_value) |default_value| {
                    if (try self.defaultLiteral(slot.type, default_value)) |literal| {
                        defer self.allocator.free(literal);
                        try writer.print("            const raw = self._reader.{s}({});\n", .{ read_fn, byte_offset });
                        try writer.print("            const value = raw ^ {s};\n", .{literal});
                        if (self.isUnsigned(slot.type)) {
                            try writer.writeAll("            return value;\n");
                        } else {
                            try writer.writeAll("            return @bitCast(value);\n");
                        }
                    } else {
                        try self.writeNumericGetterWithoutDefault(slot.type, byte_offset, writer);
                    }
                } else {
                    try self.writeNumericGetterWithoutDefault(slot.type, byte_offset, writer);
                }
            },
            .@"enum" => |enum_info| {
                const byte_offset = self.dataByteOffset(slot.type, slot.offset);
                const enum_name = self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);

                if (slot.default_value) |default_value| {
                    if (try self.defaultLiteral(.uint16, default_value)) |literal| {
                        defer self.allocator.free(literal);
                        try writer.print("            const raw = self._reader.readU16({}) ^ {s};\n", .{ byte_offset, literal });
                        if (enum_name) |_| {
                            try writer.writeAll("            return @enumFromInt(raw);\n");
                        } else {
                            try writer.writeAll("            return raw;\n");
                        }
                    } else {
                        if (enum_name) |_| {
                            try writer.print("            return @enumFromInt(self._reader.readU16({}));\n", .{byte_offset});
                        } else {
                            try writer.print("            return self._reader.readU16({});\n", .{byte_offset});
                        }
                    }
                } else {
                    if (enum_name) |_| {
                        try writer.print("            return @enumFromInt(self._reader.readU16({}));\n", .{byte_offset});
                    } else {
                        try writer.print("            return self._reader.readU16({});\n", .{byte_offset});
                    }
                }
            },
            .text => {
                if (slot.default_value) |default_value| {
                    const text = self.defaultText(default_value) orelse "";
                    try writer.print(
                        "            if (self._reader.isPointerNull({})) return \"{f}\";\n",
                        .{ slot.offset, std.zig.fmtString(text) },
                    );
                }
                try writer.print("            return try self._reader.readText({});\n", .{slot.offset});
            },
            .data => {
                if (slot.default_value) |default_value| {
                    if (self.defaultData(default_value)) |data| {
                        try writer.print("            if (self._reader.isPointerNull({})) return ", .{slot.offset});
                        try self.writeByteArrayLiteral(writer, data);
                        try writer.writeAll(";\n");
                    }
                }
                try writer.print("            return try self._reader.readData({});\n", .{slot.offset});
            },
            .list => |list_info| {
                const method = self.listReaderMethod(list_info.element_type.*);
                if (slot.default_value) |default_value| {
                    if (self.defaultPointerBytes(default_value)) |bytes| {
                        const const_name = try self.defaultConstName(field.name);
                        defer self.allocator.free(const_name);
                        try writer.print("            if (self._reader.isPointerNull({})) return try {s}();\n", .{ slot.offset, const_name });
                        _ = bytes;
                    }
                }
                try writer.print("            return try self._reader.{s}({});\n", .{ method, slot.offset });
            },
            .@"struct" => |struct_info| {
                const struct_name = self.structTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                if (struct_name) |name| {
                    if (slot.default_value) |default_value| {
                        if (self.defaultPointerBytes(default_value)) |bytes| {
                            const const_name = try self.defaultConstName(field.name);
                            defer self.allocator.free(const_name);
                            try writer.print("            if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                            try writer.print("                const value = try {s}();\n", .{const_name});
                            try writer.print("                return {s}.Reader{{ ._reader = value }};\n", .{name});
                            try writer.writeAll("            }\n");
                            _ = bytes;
                        }
                    }
                    try writer.print("            const value = try self._reader.readStruct({});\n", .{slot.offset});
                    try writer.print("            return {s}.Reader{{ ._reader = value }};\n", .{name});
                } else {
                    try writer.writeAll("            return error.UnsupportedType;\n");
                }
            },
            .any_pointer => {
                if (slot.default_value) |default_value| {
                    if (self.defaultPointerBytes(default_value)) |bytes| {
                        const const_name = try self.defaultConstName(field.name);
                        defer self.allocator.free(const_name);
                        try writer.print("            if (self._reader.isPointerNull({})) return try {s}();\n", .{ slot.offset, const_name });
                        _ = bytes;
                    }
                }
                try writer.print("            return try self._reader.readAnyPointer({});\n", .{slot.offset});
            },
            .interface => {
                if (slot.default_value) |default_value| {
                    if (self.defaultPointerBytes(default_value)) |bytes| {
                        const const_name = try self.defaultConstName(field.name);
                        defer self.allocator.free(const_name);
                        try writer.print("            if (self._reader.isPointerNull({})) return try {s}();\n", .{ slot.offset, const_name });
                        _ = bytes;
                    }
                }
                try writer.print("            return try self._reader.readAnyPointer({});\n", .{slot.offset});
            },
        }

        try writer.writeAll("        }\n\n");
    }

    fn generatePointerDefaults(self: *StructGenerator, struct_info: schema.StructNode, writer: anytype) !void {
        var emitted: bool = false;
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            const value = slot.default_value orelse continue;
            const bytes = self.defaultPointerBytes(value) orelse continue;

            const const_name = try self.defaultConstName(field.name);
            defer self.allocator.free(const_name);

            const return_type = try self.defaultPointerReturnType(slot.type);
            defer self.allocator.free(return_type);

            try writer.print("        const {s}_bytes = ", .{const_name});
            try self.writeByteArrayInitializer(writer, bytes);
            try writer.writeAll(";\n");
            try writer.print("        const {s}_segments = [_][]const u8{{ {s}_bytes[0..] }};\n", .{ const_name, const_name });
            try writer.print(
                "        const {s}_message = message.Message{{ .allocator = std.heap.page_allocator, .segments = {s}_segments[0..], .backing_data = null, .segments_owned = false }};\n\n",
                .{ const_name, const_name },
            );

            try writer.print("        fn {s}() !{s} {{\n", .{ const_name, return_type });
            switch (slot.type) {
                .list => |list_info| {
                    const elem_type = list_info.element_type.*;
                    try writer.print("            const root = try {s}_message.getRootAnyPointer();\n", .{const_name});
                    if (elem_type == .@"struct") {
                        try writer.writeAll("            const list = try root.getInlineCompositeList();\n");
                        try writer.writeAll("            return message.StructListReader{\n");
                        try writer.print("                .message = &{s}_message,\n", .{const_name});
                        try writer.writeAll("                .segment_id = list.segment_id,\n");
                        try writer.writeAll("                .elements_offset = list.elements_offset,\n");
                        try writer.writeAll("                .element_count = list.element_count,\n");
                        try writer.writeAll("                .data_words = list.data_words,\n");
                        try writer.writeAll("                .pointer_words = list.pointer_words,\n");
                        try writer.writeAll("            };\n");
                    } else {
                        const element_size = try self.listElementSize(elem_type);
                        try writer.writeAll("            const list = try root.getList();\n");
                        try writer.print("            if (list.element_size != {}) return error.InvalidPointer;\n", .{element_size});
                        try writer.writeAll("            return " );
                        try writer.print("{s}{{\n", .{return_type});
                        try writer.print("                .message = &{s}_message,\n", .{const_name});
                        try writer.writeAll("                .segment_id = list.segment_id,\n");
                        try writer.writeAll("                .elements_offset = list.content_offset,\n");
                        try writer.writeAll("                .element_count = list.element_count,\n");
                        try writer.writeAll("            };\n");
                    }
                },
                .@"struct" => {
                    try writer.print("            return try {s}_message.getRootStruct();\n", .{const_name});
                },
                .any_pointer => {
                    try writer.print("            return try {s}_message.getRootAnyPointer();\n", .{const_name});
                },
                else => {
                    try writer.writeAll("            return error.UnsupportedType;\n");
                },
            }
            try writer.writeAll("        }\n\n");
            emitted = true;
        }

        if (emitted) {
            try writer.writeAll("\n");
        }
    }

    fn generateBuilder(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        data_word_count: u16,
        pointer_count: u16,
        writer: anytype,
    ) !void {
        try writer.writeAll("    pub const Builder = struct {\n");
        try writer.writeAll("        _builder: message.StructBuilder,\n\n");

        try writer.print("        pub fn init(msg: *message.MessageBuilder) !Builder {{\n", .{});
        try writer.print("            const builder = try msg.allocateStruct({}, {});\n", .{ data_word_count, pointer_count });
        try writer.writeAll("            return .{ ._builder = builder };\n");
        try writer.writeAll("        }\n\n");

        // Generate field setters
        for (struct_info.fields) |field| {
            try self.generateFieldSetter(field, writer);
        }

        try writer.writeAll("    };\n");
    }

    fn generateFieldSetter(self: *StructGenerator, field: schema.Field, writer: anytype) !void {
        const slot = field.slot orelse return;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        switch (slot.type) {
            .list => |list_info| {
                const builder_type = try self.listBuilderTypeString(list_info.element_type.*);
                defer self.allocator.free(builder_type);
                try writer.print("        pub fn init{s}(self: *Builder, element_count: u32) !{s} {{\n", .{
                    cap_name,
                    builder_type,
                });

                switch (list_info.element_type.*) {
                    .bool => try writer.print("            return try self._builder.writeBoolList({}, element_count);\n", .{slot.offset}),
                    .int8 => try writer.print("            return try self._builder.writeI8List({}, element_count);\n", .{slot.offset}),
                    .uint8 => try writer.print("            return try self._builder.writeU8List({}, element_count);\n", .{slot.offset}),
                    .int16 => try writer.print("            return try self._builder.writeI16List({}, element_count);\n", .{slot.offset}),
                    .uint16 => try writer.print("            return try self._builder.writeU16List({}, element_count);\n", .{slot.offset}),
                    .int32 => try writer.print("            return try self._builder.writeI32List({}, element_count);\n", .{slot.offset}),
                    .uint32 => try writer.print("            return try self._builder.writeU32List({}, element_count);\n", .{slot.offset}),
                    .float32 => try writer.print("            return try self._builder.writeF32List({}, element_count);\n", .{slot.offset}),
                    .int64 => try writer.print("            return try self._builder.writeI64List({}, element_count);\n", .{slot.offset}),
                    .uint64 => try writer.print("            return try self._builder.writeU64List({}, element_count);\n", .{slot.offset}),
                    .float64 => try writer.print("            return try self._builder.writeF64List({}, element_count);\n", .{slot.offset}),
                    .text => try writer.print("            return try self._builder.writeTextList({}, element_count);\n", .{slot.offset}),
                    .@"enum" => try writer.print("            return try self._builder.writeU16List({}, element_count);\n", .{slot.offset}),
                    .@"struct" => |struct_info| {
                        if (self.structLayout(struct_info.type_id)) |layout| {
                            try writer.print("            return try self._builder.writeStructList({}, element_count, {}, {});\n", .{
                                slot.offset,
                                layout.data_words,
                                layout.pointer_words,
                            });
                        } else {
                            try writer.writeAll("            return error.UnsupportedType;\n");
                        }
                    },
                    else => try writer.print("            return try self._builder.writePointerList({}, element_count);\n", .{slot.offset}),
                }

                try writer.writeAll("        }\n\n");
                return;
            },
            .@"struct" => |struct_info| {
                const struct_name = self.structTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                if (struct_name) |name| {
                    if (self.structLayout(struct_info.type_id)) |layout| {
                        try writer.print("        pub fn init{s}(self: *Builder) !{s}.Builder {{\n", .{ cap_name, name });
                        try writer.print("            const builder = try self._builder.initStruct({}, {}, {});\n", .{
                            slot.offset,
                            layout.data_words,
                            layout.pointer_words,
                        });
                        try writer.print("            return {s}.Builder{{ ._builder = builder }};\n", .{name});
                        try writer.writeAll("        }\n\n");
                        return;
                    }
                }

                try writer.print("        pub fn init{s}(self: *Builder) !message.StructBuilder {{\n", .{cap_name});
                try writer.writeAll("            return error.UnsupportedType;\n");
                try writer.writeAll("        }\n\n");
                return;
            },
            .any_pointer => {
                try writer.print("        pub fn init{s}(self: *Builder) !message.AnyPointerBuilder {{\n", .{cap_name});
                try writer.print("            return try self._builder.getAnyPointer({});\n", .{slot.offset});
                try writer.writeAll("        }\n\n");
                return;
            },
            .interface => {
                try writer.print("        pub fn init{s}(self: *Builder) !message.AnyPointerBuilder {{\n", .{cap_name});
                try writer.print("            return try self._builder.getAnyPointer({});\n", .{slot.offset});
                try writer.writeAll("        }\n\n");
                return;
            },
            else => {},
        }

        const zig_type = try self.writerTypeString(slot.type);
        defer self.allocator.free(zig_type);

        try writer.print("        pub fn set{s}(self: *Builder, value: {s}) !void {{\n", .{
            cap_name,
            zig_type,
        });

        switch (slot.type) {
            .void => try writer.writeAll("            _ = value;\n"),
            .bool => {
                const byte_offset = slot.offset / 8;
                const bit_offset = @as(u3, @truncate(slot.offset % 8));
                try writer.print("            self._builder.writeBool({}, {}, value);\n", .{ byte_offset, bit_offset });
            },
            .int8, .uint8 => try writer.print("            self._builder.writeU8({}, @bitCast(value));\n", .{self.dataByteOffset(slot.type, slot.offset)}),
            .int16, .uint16 => try writer.print("            self._builder.writeU16({}, @bitCast(value));\n", .{self.dataByteOffset(slot.type, slot.offset)}),
            .int32, .uint32, .float32 => try writer.print("            self._builder.writeU32({}, @bitCast(value));\n", .{self.dataByteOffset(slot.type, slot.offset)}),
            .int64, .uint64, .float64 => try writer.print("            self._builder.writeU64({}, @bitCast(value));\n", .{self.dataByteOffset(slot.type, slot.offset)}),
            .@"enum" => {
                const byte_offset = self.dataByteOffset(slot.type, slot.offset);
                try writer.print("            self._builder.writeU16({}, @intFromEnum(value));\n", .{byte_offset});
            },
            .text => try writer.print("            try self._builder.writeText({}, value);\n", .{slot.offset}),
            .data => try writer.print("            try self._builder.writeData({}, value);\n", .{slot.offset}),
            else => try writer.writeAll("            return error.UnsupportedType;\n"),
        }

        try writer.writeAll("        }\n\n");
    }

    fn getSimpleName(self: *StructGenerator, node: *const schema.Node) []const u8 {
        _ = self;
        const prefix_len = node.display_name_prefix_length;
        if (prefix_len >= node.display_name.len) return node.display_name;
        return node.display_name[prefix_len..];
    }

    fn capitalizeFirst(self: *StructGenerator, name: []const u8) ![]const u8 {
        if (name.len == 0) return try self.allocator.dupe(u8, name);
        var result = try self.allocator.alloc(u8, name.len);
        result[0] = std.ascii.toUpper(name[0]);
        @memcpy(result[1..], name[1..]);
        return result;
    }

    fn readerTypeString(self: *StructGenerator, typ: schema.Type) ![]const u8 {
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
            .any_pointer => try self.allocator.dupe(u8, "message.AnyPointerReader"),
            .interface => try self.allocator.dupe(u8, "message.AnyPointerReader"),
            .@"enum" => |enum_info| blk: {
                if (self.enumTypeName(enum_info.type_id)) |name| break :blk name;
                break :blk try self.allocator.dupe(u8, "u16");
            },
            .@"struct" => |struct_info| blk: {
                if (self.structTypeName(struct_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "{s}.Reader", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.StructReader");
            },
            .list => |list_info| try self.listReaderTypeString(list_info.element_type.*),
        };
    }

    fn writerTypeString(self: *StructGenerator, typ: schema.Type) ![]const u8 {
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
            .@"enum" => |enum_info| blk: {
                if (self.enumTypeName(enum_info.type_id)) |name| break :blk name;
                break :blk try self.allocator.dupe(u8, "u16");
            },
            else => try self.allocator.dupe(u8, "void"),
        };
    }

    fn listReaderMethod(self: *StructGenerator, elem_type: schema.Type) []const u8 {
        _ = self;
        return switch (elem_type) {
            .bool => "readBoolList",
            .int8 => "readI8List",
            .uint8 => "readU8List",
            .int16 => "readI16List",
            .uint16 => "readU16List",
            .int32 => "readI32List",
            .uint32 => "readU32List",
            .float32 => "readF32List",
            .int64 => "readI64List",
            .uint64 => "readU64List",
            .float64 => "readF64List",
            .text => "readTextList",
            .@"struct" => "readStructList",
            .@"enum" => "readU16List",
            else => "readPointerList",
        };
    }

    fn listReaderTypeString(self: *StructGenerator, elem_type: schema.Type) ![]const u8 {
        return switch (elem_type) {
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
            .@"struct" => try self.allocator.dupe(u8, "message.StructListReader"),
            .@"enum" => try self.allocator.dupe(u8, "message.U16ListReader"),
            else => try self.allocator.dupe(u8, "message.PointerListReader"),
        };
    }

    fn listBuilderTypeString(self: *StructGenerator, elem_type: schema.Type) ![]const u8 {
        return switch (elem_type) {
            .bool => try self.allocator.dupe(u8, "message.BoolListBuilder"),
            .int8 => try self.allocator.dupe(u8, "message.I8ListBuilder"),
            .uint8 => try self.allocator.dupe(u8, "message.U8ListBuilder"),
            .int16 => try self.allocator.dupe(u8, "message.I16ListBuilder"),
            .uint16 => try self.allocator.dupe(u8, "message.U16ListBuilder"),
            .int32 => try self.allocator.dupe(u8, "message.I32ListBuilder"),
            .uint32 => try self.allocator.dupe(u8, "message.U32ListBuilder"),
            .float32 => try self.allocator.dupe(u8, "message.F32ListBuilder"),
            .int64 => try self.allocator.dupe(u8, "message.I64ListBuilder"),
            .uint64 => try self.allocator.dupe(u8, "message.U64ListBuilder"),
            .float64 => try self.allocator.dupe(u8, "message.F64ListBuilder"),
            .text => try self.allocator.dupe(u8, "message.TextListBuilder"),
            .@"struct" => try self.allocator.dupe(u8, "message.StructListBuilder"),
            .@"enum" => try self.allocator.dupe(u8, "message.U16ListBuilder"),
            else => try self.allocator.dupe(u8, "message.PointerListBuilder"),
        };
    }

    fn structTypeName(self: *StructGenerator, id: schema.Id) ?[]const u8 {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .@"struct") return null;
        const name = self.getSimpleName(node);
        return self.allocator.dupe(u8, name) catch null;
    }

    fn enumTypeName(self: *StructGenerator, id: schema.Id) ?[]const u8 {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .@"enum") return null;
        const name = self.getSimpleName(node);
        return self.allocator.dupe(u8, name) catch null;
    }

    fn structLayout(self: *StructGenerator, id: schema.Id) ?struct { data_words: u16, pointer_words: u16 } {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .@"struct") return null;
        const info = node.struct_node orelse return null;
        return .{ .data_words = info.data_word_count, .pointer_words = info.pointer_count };
    }

    fn defaultBool(self: *StructGenerator, default_value: schema.Value) bool {
        _ = self;
        return switch (default_value) {
            .bool => |value| value,
            else => false,
        };
    }

    fn defaultLiteral(self: *StructGenerator, typ: schema.Type, default_value: schema.Value) !?[]u8 {
        const bits = self.defaultBits(typ, default_value) orelse return null;
        const width = self.bitWidth(typ) orelse return null;
        const literal = try std.fmt.allocPrint(self.allocator, "@as(u{d}, {d})", .{ width, bits });
        return @as(?[]u8, literal);
    }

    fn defaultBits(self: *StructGenerator, typ: schema.Type, default_value: schema.Value) ?u64 {
        _ = self;
        return switch (typ) {
            .int8 => if (default_value == .int8) @as(u64, @intCast(@as(u8, @bitCast(default_value.int8)))) else null,
            .uint8 => if (default_value == .uint8) @as(u64, default_value.uint8) else null,
            .int16 => if (default_value == .int16) @as(u64, @intCast(@as(u16, @bitCast(default_value.int16)))) else null,
            .uint16 => if (default_value == .uint16)
                @as(u64, default_value.uint16)
            else if (default_value == .@"enum")
                @as(u64, default_value.@"enum")
            else
                null,
            .int32 => if (default_value == .int32) @as(u64, @intCast(@as(u32, @bitCast(default_value.int32)))) else null,
            .uint32 => if (default_value == .uint32) @as(u64, default_value.uint32) else null,
            .int64 => if (default_value == .int64) @as(u64, @bitCast(default_value.int64)) else null,
            .uint64 => if (default_value == .uint64) default_value.uint64 else null,
            .float32 => if (default_value == .float32) @as(u64, @intCast(@as(u32, @bitCast(default_value.float32)))) else null,
            .float64 => if (default_value == .float64) @as(u64, @bitCast(default_value.float64)) else null,
            .@"enum" => if (default_value == .@"enum") @as(u64, default_value.@"enum") else null,
            else => null,
        };
    }

    fn defaultText(self: *StructGenerator, default_value: schema.Value) ?[]const u8 {
        _ = self;
        return switch (default_value) {
            .text => |text| text,
            else => null,
        };
    }

    fn defaultData(self: *StructGenerator, default_value: schema.Value) ?[]const u8 {
        _ = self;
        return switch (default_value) {
            .data => |data| data,
            else => null,
        };
    }

    fn defaultPointerBytes(self: *StructGenerator, default_value: schema.Value) ?[]const u8 {
        _ = self;
        return switch (default_value) {
            .list => |info| info.message_bytes,
            .@"struct" => |info| info.message_bytes,
            .any_pointer => |info| info.message_bytes,
            else => null,
        };
    }

    fn defaultConstName(self: *StructGenerator, field_name: []const u8) ![]const u8 {
        const zig_name = try self.type_gen.toZigIdentifier(field_name);
        defer self.allocator.free(zig_name);
        return std.fmt.allocPrint(self.allocator, "_default_{s}", .{zig_name});
    }

    fn defaultPointerReturnType(self: *StructGenerator, typ: schema.Type) ![]const u8 {
        return switch (typ) {
            .list => |list_info| try self.listReaderTypeString(list_info.element_type.*),
            .@"struct" => try self.allocator.dupe(u8, "message.StructReader"),
            .any_pointer => try self.allocator.dupe(u8, "message.AnyPointerReader"),
            .interface => try self.allocator.dupe(u8, "message.AnyPointerReader"),
            else => try self.allocator.dupe(u8, "void"),
        };
    }

    fn listElementSize(self: *StructGenerator, elem_type: schema.Type) !u3 {
        _ = self;
        return switch (elem_type) {
            .void => 0,
            .bool => 1,
            .int8, .uint8 => 2,
            .int16, .uint16, .@"enum" => 3,
            .int32, .uint32, .float32 => 4,
            .int64, .uint64, .float64 => 5,
            .text, .data, .list, .@"struct", .any_pointer, .interface => 6,
        };
    }

    fn writeByteArrayInitializer(self: *StructGenerator, writer: anytype, data: []const u8) !void {
        _ = self;
        try writer.writeAll("[_]u8{");
        for (data, 0..) |byte, i| {
            if (i != 0) try writer.writeAll(", ");
            try writer.print("0x{X:0>2}", .{byte});
        }
        try writer.writeAll("}");
    }

    fn writeByteArrayLiteral(self: *StructGenerator, writer: anytype, data: []const u8) !void {
        _ = self;
        try writer.writeAll("&[_]u8{");
        for (data, 0..) |byte, i| {
            if (i != 0) try writer.writeAll(", ");
            try writer.print("0x{X:0>2}", .{byte});
        }
        try writer.writeAll("}");
    }

    fn bitWidth(self: *StructGenerator, typ: schema.Type) ?u8 {
        _ = self;
        return switch (typ) {
            .int8, .uint8 => 8,
            .int16, .uint16, .@"enum" => 16,
            .int32, .uint32, .float32 => 32,
            .int64, .uint64, .float64 => 64,
            else => null,
        };
    }

    fn isUnsigned(self: *StructGenerator, typ: schema.Type) bool {
        _ = self;
        return switch (typ) {
            .uint8, .uint16, .uint32, .uint64 => true,
            else => false,
        };
    }

    fn dataByteOffset(self: *StructGenerator, typ: schema.Type, offset: u32) u32 {
        _ = self;
        return switch (typ) {
            .bool => offset / 8,
            .int8, .uint8 => offset,
            .int16, .uint16, .@"enum" => offset * 2,
            .int32, .uint32, .float32 => offset * 4,
            .int64, .uint64, .float64 => offset * 8,
            else => offset,
        };
    }

    fn readFnForType(self: *StructGenerator, typ: schema.Type) []const u8 {
        _ = self;
        return switch (typ) {
            .int8, .uint8 => "readU8",
            .int16, .uint16, .@"enum" => "readU16",
            .int32, .uint32, .float32 => "readU32",
            .int64, .uint64, .float64 => "readU64",
            else => "readU64",
        };
    }

    fn writeNumericGetterWithoutDefault(self: *StructGenerator, typ: schema.Type, byte_offset: u32, writer: anytype) !void {
        const read_fn = self.readFnForType(typ);
        if (self.isUnsigned(typ)) {
            try writer.print("            return self._reader.{s}({});\n", .{ read_fn, byte_offset });
        } else {
            try writer.print("            return @bitCast(self._reader.{s}({}));\n", .{ read_fn, byte_offset });
        }
    }
};
