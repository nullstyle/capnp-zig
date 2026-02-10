const std = @import("std");
const schema = @import("../schema.zig");
const types = @import("types.zig");
const TypeGenerator = types.TypeGenerator;

/// Generates Zig source code for a single Cap'n Proto struct node, emitting
/// a `Reader` type (zero-copy field accessors) and a `Builder` type (field
/// writers), plus list helpers, union tag enums, and nested group types.
pub const StructGenerator = struct {
    allocator: std.mem.Allocator,
    type_gen: TypeGenerator,
    node_lookup_ctx: ?*const anyopaque,
    node_lookup: ?*const fn (ctx: ?*const anyopaque, id: schema.Id) ?*const schema.Node,
    /// Optional callback returning the import module prefix for cross-file types.
    /// Returns null for types in the current file.
    type_prefix_fn: ?*const fn (ctx: ?*const anyopaque, id: schema.Id) ?[]const u8 = null,

    /// Create a standalone struct generator (no cross-node lookup support).
    pub fn init(allocator: std.mem.Allocator) StructGenerator {
        return .{
            .allocator = allocator,
            .type_gen = TypeGenerator.init(allocator),
            .node_lookup_ctx = null,
            .node_lookup = null,
        };
    }

    /// Create a struct generator with cross-node lookup for resolving
    /// type references to other schema nodes.
    pub fn initWithLookup(
        allocator: std.mem.Allocator,
        node_lookup: *const fn (ctx: ?*const anyopaque, id: schema.Id) ?*const schema.Node,
        node_lookup_ctx: ?*const anyopaque,
    ) StructGenerator {
        return .{
            .allocator = allocator,
            .type_gen = TypeGenerator.initWithLookup(allocator, node_lookup, node_lookup_ctx),
            .node_lookup_ctx = node_lookup_ctx,
            .node_lookup = node_lookup,
        };
    }

    fn getNode(self: *const StructGenerator, id: schema.Id) ?*const schema.Node {
        const lookup = self.node_lookup orelse return null;
        return lookup(self.node_lookup_ctx, id);
    }

    /// Emit the complete Zig type definition for a struct node, including
    /// Reader, Builder, union tag enum, group types, and list helpers.
    pub fn generate(self: *StructGenerator, node: *const schema.Node, writer: anytype) !void {
        const struct_info = node.struct_node orelse return error.InvalidStructNode;
        const name = try self.allocTypeName(node);
        defer self.allocator.free(name);

        const data_word_count = struct_info.data_word_count;
        const pointer_count = struct_info.pointer_count;

        try writer.print("pub const {s} = struct {{\n", .{name});
        try self.generateListHelpers(writer);

        // Generate union tag enum if this struct has a union
        if (struct_info.discriminant_count > 0) {
            try self.generateWhichTag(struct_info, writer);
        }

        // Generate nested group structs
        try self.generateGroupTypes(struct_info, writer);

        // Generate Reader
        try self.generateReader(struct_info, data_word_count, pointer_count, writer);

        // Generate Builder
        try self.generateBuilder(struct_info, data_word_count, pointer_count, writer);

        try writer.writeAll("};\n\n");
    }

    fn generateWhichTag(self: *StructGenerator, struct_info: schema.StructNode, writer: anytype) !void {
        try writer.writeAll("    pub const WhichTag = enum(u16) {\n");
        for (struct_info.fields) |field| {
            if (field.discriminant_value == 0xFFFF) continue;
            const zig_name = try self.type_gen.toZigIdentifier(field.name);
            defer self.allocator.free(zig_name);
            const escaped_name = try types.escapeZigKeyword(self.allocator, zig_name);
            defer self.allocator.free(escaped_name);
            try writer.print("        {s} = {},\n", .{ escaped_name, field.discriminant_value });
        }
        try writer.writeAll("    };\n\n");
    }

    fn generateGroupTypes(self: *StructGenerator, struct_info: schema.StructNode, writer: anytype) !void {
        for (struct_info.fields) |field| {
            const group = field.group orelse continue;
            const group_node = self.getNode(group.type_id) orelse continue;
            const group_struct_info = group_node.struct_node orelse continue;
            const group_name = try self.allocTypeName(group_node);
            defer self.allocator.free(group_name);

            try writer.print("    pub const {s} = struct {{\n", .{group_name});

            // Generate group Reader
            try self.writeGroupWrapStruct(writer, "Reader", "_reader", "message.StructReader", "reader");
            for (group_struct_info.fields) |group_field| {
                try self.generateGroupFieldGetter(group_field, writer);
            }
            try writer.writeAll("        };\n\n");

            // Generate group Builder
            try self.writeGroupWrapStruct(writer, "Builder", "_builder", "message.StructBuilder", "builder");
            for (group_struct_info.fields) |group_field| {
                try self.generateGroupFieldSetter(group_field, struct_info, writer);
            }
            try writer.writeAll("        };\n");

            try writer.writeAll("    };\n\n");
        }
    }

    fn generateListHelpers(self: *StructGenerator, writer: anytype) !void {
        _ = self;
        try writer.writeAll("    fn EnumListReader(comptime EnumType: type) type {\n");
        try writer.writeAll("        return struct {\n");
        try writer.writeAll("            _list: message.U16ListReader,\n\n");
        try writer.writeAll("            pub fn len(self: @This()) u32 {\n");
        try writer.writeAll("                return self._list.len();\n");
        try writer.writeAll("            }\n\n");
        try writer.writeAll("            pub fn get(self: @This(), index: u32) !EnumType {\n");
        try writer.writeAll("                return std.meta.intToEnum(EnumType, try self._list.get(index)) catch return error.InvalidEnumValue;\n");
        try writer.writeAll("            }\n\n");
        try writer.writeAll("            pub fn raw(self: @This()) message.U16ListReader {\n");
        try writer.writeAll("                return self._list;\n");
        try writer.writeAll("            }\n");
        try writer.writeAll("        };\n");
        try writer.writeAll("    }\n\n");
        try writer.writeAll("    fn EnumListBuilder(comptime EnumType: type) type {\n");
        try writer.writeAll("        return struct {\n");
        try writer.writeAll("            _list: message.U16ListBuilder,\n\n");
        try writer.writeAll("            pub fn len(self: @This()) u32 {\n");
        try writer.writeAll("                return self._list.len();\n");
        try writer.writeAll("            }\n\n");
        try writer.writeAll("            pub fn set(self: @This(), index: u32, value: EnumType) !void {\n");
        try writer.writeAll("                try self._list.set(index, @intFromEnum(value));\n");
        try writer.writeAll("            }\n\n");
        try writer.writeAll("            pub fn raw(self: @This()) message.U16ListBuilder {\n");
        try writer.writeAll("                return self._list;\n");
        try writer.writeAll("            }\n");
        try writer.writeAll("        };\n");
        try writer.writeAll("    }\n\n");
        try writer.writeAll("    fn StructListReader(comptime StructType: type) type {\n");
        try writer.writeAll("        return struct {\n");
        try writer.writeAll("            _list: message.StructListReader,\n\n");
        try writer.writeAll("            pub fn len(self: @This()) u32 {\n");
        try writer.writeAll("                return self._list.len();\n");
        try writer.writeAll("            }\n\n");
        try writer.writeAll("            pub fn get(self: @This(), index: u32) !StructType.Reader {\n");
        try writer.writeAll("                const item = try self._list.get(index);\n");
        try writer.writeAll("                return StructType.Reader.wrap(item);\n");
        try writer.writeAll("            }\n\n");
        try writer.writeAll("            pub fn raw(self: @This()) message.StructListReader {\n");
        try writer.writeAll("                return self._list;\n");
        try writer.writeAll("            }\n");
        try writer.writeAll("        };\n");
        try writer.writeAll("    }\n\n");
        try writer.writeAll("    fn StructListBuilder(comptime StructType: type) type {\n");
        try writer.writeAll("        return struct {\n");
        try writer.writeAll("            _list: message.StructListBuilder,\n\n");
        try writer.writeAll("            pub fn len(self: @This()) u32 {\n");
        try writer.writeAll("                return self._list.len();\n");
        try writer.writeAll("            }\n\n");
        try writer.writeAll("            pub fn get(self: @This(), index: u32) !StructType.Builder {\n");
        try writer.writeAll("                const item = try self._list.get(index);\n");
        try writer.writeAll("                return StructType.Builder.wrap(item);\n");
        try writer.writeAll("            }\n\n");
        try writer.writeAll("            pub fn raw(self: @This()) message.StructListBuilder {\n");
        try writer.writeAll("                return self._list;\n");
        try writer.writeAll("            }\n");
        try writer.writeAll("        };\n");
        try writer.writeAll("    }\n\n");
        try writer.writeAll("    const DataListReader = struct {\n");
        try writer.writeAll("        _list: message.PointerListReader,\n\n");
        try writer.writeAll("        pub fn len(self: @This()) u32 {\n");
        try writer.writeAll("            return self._list.len();\n");
        try writer.writeAll("        }\n\n");
        try writer.writeAll("        pub fn get(self: @This(), index: u32) ![]const u8 {\n");
        try writer.writeAll("            return try self._list.getData(index);\n");
        try writer.writeAll("        }\n\n");
        try writer.writeAll("        pub fn raw(self: @This()) message.PointerListReader {\n");
        try writer.writeAll("            return self._list;\n");
        try writer.writeAll("        }\n");
        try writer.writeAll("    };\n\n");
        try writer.writeAll("    const DataListBuilder = struct {\n");
        try writer.writeAll("        _list: message.PointerListBuilder,\n\n");
        try writer.writeAll("        pub fn len(self: @This()) u32 {\n");
        try writer.writeAll("            return self._list.len();\n");
        try writer.writeAll("        }\n\n");
        try writer.writeAll("        pub fn set(self: @This(), index: u32, value: []const u8) !void {\n");
        try writer.writeAll("            try self._list.setData(index, value);\n");
        try writer.writeAll("        }\n\n");
        try writer.writeAll("        pub fn setNull(self: @This(), index: u32) !void {\n");
        try writer.writeAll("            try self._list.setNull(index);\n");
        try writer.writeAll("        }\n\n");
        try writer.writeAll("        pub fn raw(self: @This()) message.PointerListBuilder {\n");
        try writer.writeAll("            return self._list;\n");
        try writer.writeAll("        }\n");
        try writer.writeAll("    };\n\n");
        try writer.writeAll("    const CapabilityListReader = struct {\n");
        try writer.writeAll("        _list: message.PointerListReader,\n\n");
        try writer.writeAll("        pub fn len(self: @This()) u32 {\n");
        try writer.writeAll("            return self._list.len();\n");
        try writer.writeAll("        }\n\n");
        try writer.writeAll("        pub fn get(self: @This(), index: u32) !message.Capability {\n");
        try writer.writeAll("            return try self._list.getCapability(index);\n");
        try writer.writeAll("        }\n\n");
        try writer.writeAll("        pub fn raw(self: @This()) message.PointerListReader {\n");
        try writer.writeAll("            return self._list;\n");
        try writer.writeAll("        }\n");
        try writer.writeAll("    };\n\n");
        try writer.writeAll("    const CapabilityListBuilder = struct {\n");
        try writer.writeAll("        _list: message.PointerListBuilder,\n\n");
        try writer.writeAll("        pub fn len(self: @This()) u32 {\n");
        try writer.writeAll("            return self._list.len();\n");
        try writer.writeAll("        }\n\n");
        try writer.writeAll("        pub fn set(self: @This(), index: u32, cap: message.Capability) !void {\n");
        try writer.writeAll("            try self._list.setCapability(index, cap);\n");
        try writer.writeAll("        }\n\n");
        try writer.writeAll("        pub fn setNull(self: @This(), index: u32) !void {\n");
        try writer.writeAll("            try self._list.setNull(index);\n");
        try writer.writeAll("        }\n\n");
        try writer.writeAll("        pub fn raw(self: @This()) message.PointerListBuilder {\n");
        try writer.writeAll("            return self._list;\n");
        try writer.writeAll("        }\n");
        try writer.writeAll("    };\n\n");
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

        try writer.writeAll("        pub fn wrap(reader: message.StructReader) Reader {\n");
        try writer.writeAll("            return .{ ._reader = reader };\n");
        try writer.writeAll("        }\n\n");

        // Generate which() method if this struct has a union
        if (struct_info.discriminant_count > 0) {
            const disc_byte_offset = struct_info.discriminant_offset * 2;
            try writer.print("        pub fn which(self: Reader) error{{InvalidEnumValue}}!WhichTag {{\n", .{});
            try writer.print("            return std.meta.intToEnum(WhichTag, self._reader.readU16({})) catch return error.InvalidEnumValue;\n", .{disc_byte_offset});
            try writer.writeAll("        }\n\n");
        }

        // Generate field getters
        for (struct_info.fields) |field| {
            if (field.group != null) {
                try self.generateGroupFieldAccessor(field, writer);
            } else {
                try self.generateFieldGetter(field, writer);
            }
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
                        if (enum_name) |en| {
                            try writer.print("            return std.meta.intToEnum({s}, raw) catch return error.InvalidEnumValue;\n", .{en});
                        } else {
                            try writer.writeAll("            return raw;\n");
                        }
                    } else {
                        if (enum_name) |en| {
                            try writer.print("            return std.meta.intToEnum({s}, self._reader.readU16({})) catch return error.InvalidEnumValue;\n", .{ en, byte_offset });
                        } else {
                            try writer.print("            return self._reader.readU16({});\n", .{byte_offset});
                        }
                    }
                } else {
                    if (enum_name) |en| {
                        try writer.print("            return std.meta.intToEnum({s}, self._reader.readU16({})) catch return error.InvalidEnumValue;\n", .{ en, byte_offset });
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
                if (list_info.element_type.* == .@"enum") {
                    const enum_info = list_info.element_type.@"enum";
                    const enum_name = self.enumTypeName(enum_info.type_id);
                    defer if (enum_name) |name| self.allocator.free(name);
                    if (slot.default_value) |default_value| {
                        if (self.defaultPointerBytes(default_value)) |bytes| {
                            const const_name = try self.defaultConstName(field.name);
                            defer self.allocator.free(const_name);
                            if (enum_name) |name| {
                                try writer.print("            if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                                try writer.print("                const raw = try {s}();\n", .{const_name});
                                try writer.print("                return EnumListReader({s}){{ ._list = raw }};\n", .{name});
                                try writer.writeAll("            }\n");
                            } else {
                                try writer.print("            if (self._reader.isPointerNull({})) return try {s}();\n", .{
                                    slot.offset,
                                    const_name,
                                });
                            }
                            _ = bytes;
                        }
                    }
                    if (enum_name) |name| {
                        try writer.print("            const raw = try self._reader.readU16List({});\n", .{slot.offset});
                        try writer.print("            return EnumListReader({s}){{ ._list = raw }};\n", .{name});
                    } else {
                        try writer.print("            return try self._reader.readU16List({});\n", .{slot.offset});
                    }
                    try writer.writeAll("        }\n\n");
                    return;
                }
                if (list_info.element_type.* == .data) {
                    if (slot.default_value) |default_value| {
                        if (self.defaultPointerBytes(default_value)) |bytes| {
                            const const_name = try self.defaultConstName(field.name);
                            defer self.allocator.free(const_name);
                            try writer.print("            if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                            try writer.print("                const raw = try {s}();\n", .{const_name});
                            try writer.writeAll("                return DataListReader{ ._list = raw };\n");
                            try writer.writeAll("            }\n");
                            _ = bytes;
                        }
                    }
                    try writer.print("            const raw = try self._reader.readPointerList({});\n", .{slot.offset});
                    try writer.writeAll("            return DataListReader{ ._list = raw };\n");
                    try writer.writeAll("        }\n\n");
                    return;
                }
                if (list_info.element_type.* == .interface) {
                    if (slot.default_value) |default_value| {
                        if (self.defaultPointerBytes(default_value)) |bytes| {
                            const const_name = try self.defaultConstName(field.name);
                            defer self.allocator.free(const_name);
                            try writer.print("            if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                            try writer.print("                const raw = try {s}();\n", .{const_name});
                            try writer.writeAll("                return CapabilityListReader{ ._list = raw };\n");
                            try writer.writeAll("            }\n");
                            _ = bytes;
                        }
                    }
                    try writer.print("            const raw = try self._reader.readPointerList({});\n", .{slot.offset});
                    try writer.writeAll("            return CapabilityListReader{ ._list = raw };\n");
                    try writer.writeAll("        }\n\n");

                    // Generate typed resolve helper for List(Interface) fields
                    if (self.interfaceTypeName(list_info.element_type.interface.type_id)) |iface_name| {
                        defer self.allocator.free(iface_name);
                        try writer.print("        pub fn resolve{s}(self: Reader, index: u32, peer: *rpc.peer.Peer, caps: *const rpc.cap_table.InboundCapTable) !{s}.Client {{\n", .{ cap_name, iface_name });
                        try writer.print("            const raw_list = try self._reader.readPointerList({});\n", .{slot.offset});
                        try writer.writeAll("            const cap = try raw_list.getCapability(index);\n");
                        try writer.writeAll("            var mutable_caps = caps.*;\n");
                        try writer.writeAll("            try mutable_caps.retainCapability(cap);\n");
                        try writer.writeAll("            const resolved = try caps.resolveCapability(cap);\n");
                        try writer.writeAll("            switch (resolved) {\n");
                        try writer.print("                .imported => |imported| return {s}.Client.init(peer, imported.id),\n", .{iface_name});
                        try writer.writeAll("                else => return error.UnexpectedCapabilityType,\n");
                        try writer.writeAll("            }\n");
                        try writer.writeAll("        }\n\n");
                    }

                    return;
                }
                if (list_info.element_type.* == .@"struct") {
                    const struct_info = list_info.element_type.@"struct";
                    const struct_name = self.structTypeName(struct_info.type_id);
                    defer if (struct_name) |name| self.allocator.free(name);
                    if (slot.default_value) |default_value| {
                        if (self.defaultPointerBytes(default_value)) |bytes| {
                            const const_name = try self.defaultConstName(field.name);
                            defer self.allocator.free(const_name);
                            if (struct_name) |name| {
                                try writer.print("            if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                                try writer.print("                const raw = try {s}();\n", .{const_name});
                                try writer.print("                return StructListReader({s}){{ ._list = raw }};\n", .{name});
                                try writer.writeAll("            }\n");
                            } else {
                                try writer.print("            if (self._reader.isPointerNull({})) return try {s}();\n", .{
                                    slot.offset,
                                    const_name,
                                });
                            }
                            _ = bytes;
                        }
                    }
                    if (struct_name) |name| {
                        try writer.print("            const raw = try self._reader.readStructList({});\n", .{slot.offset});
                        try writer.print("            return StructListReader({s}){{ ._list = raw }};\n", .{name});
                    } else {
                        try writer.print("            return try self._reader.readStructList({});\n", .{slot.offset});
                    }
                    try writer.writeAll("        }\n\n");
                    return;
                }

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
                    try writer.print("            return try self._reader.readStruct({});\n", .{slot.offset});
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
                try writer.print("            return try self._reader.readCapability({});\n", .{slot.offset});
            },
        }

        try writer.writeAll("        }\n\n");

        // Generate typed resolve helper for interface fields
        if (slot.type == .interface) {
            const iface_name = self.interfaceTypeName(slot.type.interface.type_id) orelse return;
            defer self.allocator.free(iface_name);
            try writer.print("        pub fn resolve{s}(self: Reader, peer: *rpc.peer.Peer, caps: *const rpc.cap_table.InboundCapTable) !{s}.Client {{\n", .{ cap_name, iface_name });
            try writer.print("            const cap = try self._reader.readCapability({});\n", .{slot.offset});
            try writer.writeAll("            var mutable_caps = caps.*;\n");
            try writer.writeAll("            try mutable_caps.retainCapability(cap);\n");
            try writer.writeAll("            const resolved = try caps.resolveCapability(cap);\n");
            try writer.writeAll("            switch (resolved) {\n");
            try writer.print("                .imported => |imported| return {s}.Client.init(peer, imported.id),\n", .{iface_name});
            try writer.writeAll("                else => return error.UnexpectedCapabilityType,\n");
            try writer.writeAll("            }\n");
            try writer.writeAll("        }\n\n");
        }
    }

    fn generateGroupFieldAccessor(self: *StructGenerator, field: schema.Field, writer: anytype) !void {
        const group = field.group orelse return;
        const group_node = self.getNode(group.type_id) orelse return;
        const group_name = try self.allocTypeName(group_node);
        defer self.allocator.free(group_name);

        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        try writer.print("        pub fn get{s}(self: Reader) {s}.Reader {{\n", .{ cap_name, group_name });
        try writer.writeAll("            return .{ ._reader = self._reader };\n");
        try writer.writeAll("        }\n\n");
    }

    fn generateGroupBuilderAccessor(self: *StructGenerator, field: schema.Field, struct_info: schema.StructNode, writer: anytype) !void {
        const group = field.group orelse return;
        const group_node = self.getNode(group.type_id) orelse return;
        const group_name = try self.allocTypeName(group_node);
        defer self.allocator.free(group_name);

        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        if (field.discriminant_value != 0xFFFF and struct_info.discriminant_count > 0) {
            const disc_byte_offset = struct_info.discriminant_offset * 2;
            try writer.print("        pub fn init{s}(self: *Builder) {s}.Builder {{\n", .{ cap_name, group_name });
            try writer.print("            self._builder.writeU16({}, {});\n", .{ disc_byte_offset, field.discriminant_value });
            try writer.writeAll("            return .{ ._builder = self._builder };\n");
            try writer.writeAll("        }\n\n");
        } else {
            try writer.print("        pub fn get{s}(self: *Builder) {s}.Builder {{\n", .{ cap_name, group_name });
            try writer.writeAll("            return .{ ._builder = self._builder };\n");
            try writer.writeAll("        }\n\n");
        }
    }

    /// Generate field getter for a group's internal field (used inside group Reader)
    fn generateGroupFieldGetter(self: *StructGenerator, field: schema.Field, writer: anytype) !void {
        const slot = field.slot orelse return;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);

        const zig_type = try self.readerTypeString(slot.type);
        defer self.allocator.free(zig_type);

        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        try writer.print("            pub fn get{s}(self: Reader) !{s} {{\n", .{ cap_name, zig_type });

        switch (slot.type) {
            .void => try writer.writeAll("                return {};\n"),
            .bool => {
                const byte_offset = self.dataByteOffset(slot.type, slot.offset);
                const bit_offset = @as(u3, @truncate(slot.offset % 8));
                if (slot.default_value) |default_value| {
                    const default_bool = self.defaultBool(default_value);
                    try writer.print("                return self._reader.readBool({}, {}) != {s};\n", .{
                        byte_offset,
                        bit_offset,
                        if (default_bool) "true" else "false",
                    });
                } else {
                    try writer.print("                return self._reader.readBool({}, {});\n", .{ byte_offset, bit_offset });
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
                        try writer.print("                const raw = self._reader.{s}({});\n", .{ read_fn, byte_offset });
                        try writer.print("                const value = raw ^ {s};\n", .{literal});
                        if (self.isUnsigned(slot.type)) {
                            try writer.writeAll("                return value;\n");
                        } else {
                            try writer.writeAll("                return @bitCast(value);\n");
                        }
                    } else {
                        try self.writeNumericGroupGetterWithoutDefault(slot.type, byte_offset, writer);
                    }
                } else {
                    try self.writeNumericGroupGetterWithoutDefault(slot.type, byte_offset, writer);
                }
            },
            .@"enum" => |enum_info| {
                const byte_offset = self.dataByteOffset(slot.type, slot.offset);
                const enum_name = self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);
                if (slot.default_value) |default_value| {
                    if (try self.defaultLiteral(.uint16, default_value)) |literal| {
                        defer self.allocator.free(literal);
                        try writer.print("                const raw = self._reader.readU16({}) ^ {s};\n", .{ byte_offset, literal });
                        if (enum_name) |en| {
                            try writer.print("                return std.meta.intToEnum({s}, raw) catch return error.InvalidEnumValue;\n", .{en});
                        } else {
                            try writer.writeAll("                return raw;\n");
                        }
                    } else {
                        if (enum_name) |en| {
                            try writer.print("                return std.meta.intToEnum({s}, self._reader.readU16({})) catch return error.InvalidEnumValue;\n", .{ en, byte_offset });
                        } else {
                            try writer.print("                return self._reader.readU16({});\n", .{byte_offset});
                        }
                    }
                } else {
                    if (enum_name) |en| {
                        try writer.print("                return std.meta.intToEnum({s}, self._reader.readU16({})) catch return error.InvalidEnumValue;\n", .{ en, byte_offset });
                    } else {
                        try writer.print("                return self._reader.readU16({});\n", .{byte_offset});
                    }
                }
            },
            .text => {
                if (slot.default_value) |default_value| {
                    const text = self.defaultText(default_value) orelse "";
                    try writer.print(
                        "                if (self._reader.isPointerNull({})) return \"{f}\";\n",
                        .{ slot.offset, std.zig.fmtString(text) },
                    );
                }
                try writer.print("                return try self._reader.readText({});\n", .{slot.offset});
            },
            .data => {
                if (slot.default_value) |default_value| {
                    if (self.defaultData(default_value)) |data| {
                        try writer.print("                if (self._reader.isPointerNull({})) return ", .{slot.offset});
                        try self.writeByteArrayLiteral(writer, data);
                        try writer.writeAll(";\n");
                    }
                }
                try writer.print("                return try self._reader.readData({});\n", .{slot.offset});
            },
            .@"struct" => |struct_info| {
                const struct_name = self.structTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                if (struct_name) |name| {
                    try writer.print("                const value = try self._reader.readStruct({});\n", .{slot.offset});
                    try writer.print("                return {s}.Reader{{ ._reader = value }};\n", .{name});
                } else {
                    try writer.print("                return try self._reader.readStruct({});\n", .{slot.offset});
                }
            },
            .list => |list_info| {
                const method = self.listReaderMethod(list_info.element_type.*);
                try writer.print("                return try self._reader.{s}({});\n", .{ method, slot.offset });
            },
            .any_pointer => {
                try writer.print("                return try self._reader.readAnyPointer({});\n", .{slot.offset});
            },
            .interface => {
                try writer.print("                return try self._reader.readCapability({});\n", .{slot.offset});
            },
        }

        try writer.writeAll("            }\n\n");

        // Generate typed resolve helper for interface fields in groups
        if (slot.type == .interface) {
            const iface_name = self.interfaceTypeName(slot.type.interface.type_id) orelse return;
            defer self.allocator.free(iface_name);
            try writer.print("            pub fn resolve{s}(self: Reader, peer: *rpc.peer.Peer, caps: *const rpc.cap_table.InboundCapTable) !{s}.Client {{\n", .{ cap_name, iface_name });
            try writer.print("                const cap = try self._reader.readCapability({});\n", .{slot.offset});
            try writer.writeAll("                var mutable_caps = caps.*;\n");
            try writer.writeAll("                try mutable_caps.retainCapability(cap);\n");
            try writer.writeAll("                const resolved = try caps.resolveCapability(cap);\n");
            try writer.writeAll("                switch (resolved) {\n");
            try writer.print("                    .imported => |imported| return {s}.Client.init(peer, imported.id),\n", .{iface_name});
            try writer.writeAll("                    else => return error.UnexpectedCapabilityType,\n");
            try writer.writeAll("                }\n");
            try writer.writeAll("            }\n\n");
        }
    }

    /// Generate field setter for a group's internal field (used inside group Builder)
    fn generateGroupFieldSetter(self: *StructGenerator, field: schema.Field, parent_struct_info: schema.StructNode, writer: anytype) !void {
        _ = parent_struct_info;
        const slot = field.slot orelse return;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        switch (slot.type) {
            .list => |list_info| {
                const unresolved_struct_layout = switch (list_info.element_type.*) {
                    .@"struct" => |si| self.structLayout(si.type_id) == null,
                    else => false,
                };
                const builder_type = try self.listBuilderTypeString(list_info.element_type.*);
                defer self.allocator.free(builder_type);
                if (unresolved_struct_layout) {
                    try writer.print("            pub fn init{s}(self: *Builder, element_count: u32, data_words: u16, pointer_words: u16) !{s} {{\n", .{ cap_name, builder_type });
                } else {
                    try writer.print("            pub fn init{s}(self: *Builder, element_count: u32) !{s} {{\n", .{ cap_name, builder_type });
                }
                try self.writeGroupListSetterBody(list_info.element_type.*, slot.offset, writer);
                try writer.writeAll("            }\n\n");
                return;
            },
            .@"struct" => |struct_info| {
                const struct_name = self.structTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                if (self.structLayout(struct_info.type_id)) |layout| {
                    if (struct_name) |name| {
                        try writer.print("            pub fn init{s}(self: *Builder) !{s}.Builder {{\n", .{ cap_name, name });
                        try writer.print("                const builder = try self._builder.initStruct({}, {}, {});\n", .{ slot.offset, layout.data_words, layout.pointer_words });
                        try writer.print("                return {s}.Builder{{ ._builder = builder }};\n", .{name});
                    } else {
                        try writer.print("            pub fn init{s}(self: *Builder) !message.StructBuilder {{\n", .{cap_name});
                        try writer.print("                return try self._builder.initStruct({}, {}, {});\n", .{ slot.offset, layout.data_words, layout.pointer_words });
                    }
                } else {
                    try writer.print("            pub fn init{s}(self: *Builder, data_words: u16, pointer_words: u16) !message.StructBuilder {{\n", .{cap_name});
                    try writer.print("                return try self._builder.initStruct({}, data_words, pointer_words);\n", .{slot.offset});
                }
                try writer.writeAll("            }\n\n");
                return;
            },
            .any_pointer => {
                try writer.print("            pub fn init{s}(self: *Builder) !message.AnyPointerBuilder {{\n", .{cap_name});
                try writer.print("                return try self._builder.getAnyPointer({});\n", .{slot.offset});
                try writer.writeAll("            }\n\n");
                return;
            },
            .interface => {
                try writer.print("            pub fn init{s}(self: *Builder) !message.AnyPointerBuilder {{\n", .{cap_name});
                try writer.print("                return try self._builder.getAnyPointer({});\n", .{slot.offset});
                try writer.writeAll("            }\n\n");

                // Typed helpers for group interface fields
                if (self.interfaceTypeName(slot.type.interface.type_id)) |iface_name| {
                    defer self.allocator.free(iface_name);
                    try writer.print("            pub fn set{s}Capability(self: *Builder, cap: message.Capability) !void {{\n", .{cap_name});
                    try writer.print("                var any = try self._builder.getAnyPointer({});\n", .{slot.offset});
                    try writer.writeAll("                try any.setCapability(cap);\n");
                    try writer.writeAll("            }\n\n");

                    try writer.print("            pub fn set{s}Server(self: *Builder, peer: *rpc.peer.Peer, server: *{s}.Server) !void {{\n", .{ cap_name, iface_name });
                    try writer.print("                const cap_id = try {s}.exportServer(peer, server);\n", .{iface_name});
                    try writer.print("                var any = try self._builder.getAnyPointer({});\n", .{slot.offset});
                    try writer.writeAll("                try any.setCapability(.{ .id = cap_id });\n");
                    try writer.writeAll("            }\n\n");

                    try writer.print("            pub fn set{s}Client(self: *Builder, client: {s}.Client) !void {{\n", .{ cap_name, iface_name });
                    try writer.print("                var any = try self._builder.getAnyPointer({});\n", .{slot.offset});
                    try writer.writeAll("                try any.setCapability(.{ .id = client.cap_id });\n");
                    try writer.writeAll("            }\n\n");
                }

                return;
            },
            else => {},
        }

        const zig_type = try self.writerTypeString(slot.type);
        defer self.allocator.free(zig_type);

        try writer.print("            pub fn set{s}(self: *Builder, value: {s}) !void {{\n", .{ cap_name, zig_type });

        switch (slot.type) {
            .void => try writer.writeAll("                _ = value;\n"),
            .bool => {
                const byte_offset = slot.offset / 8;
                const bit_offset = @as(u3, @truncate(slot.offset % 8));
                if (slot.default_value) |default_value| {
                    const default_bool = self.defaultBool(default_value);
                    try writer.print("                self._builder.writeBool({}, {}, value != {s});\n", .{
                        byte_offset,
                        bit_offset,
                        if (default_bool) "true" else "false",
                    });
                } else {
                    try writer.print("                self._builder.writeBool({}, {}, value);\n", .{ byte_offset, bit_offset });
                }
            },
            .int8, .uint8 => try self.writeNumericGroupSetterBody(slot, "writeU8", "u8", writer),
            .int16, .uint16 => try self.writeNumericGroupSetterBody(slot, "writeU16", "u16", writer),
            .int32, .uint32, .float32 => try self.writeNumericGroupSetterBody(slot, "writeU32", "u32", writer),
            .int64, .uint64, .float64 => try self.writeNumericGroupSetterBody(slot, "writeU64", "u64", writer),
            .@"enum" => |enum_info| {
                const byte_offset = self.dataByteOffset(slot.type, slot.offset);
                const enum_name = self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);
                const raw_expr = if (enum_name != null) "@as(u16, @intFromEnum(value))" else "@as(u16, value)";
                if (slot.default_value) |default_value| {
                    if (try self.defaultLiteral(.uint16, default_value)) |literal| {
                        defer self.allocator.free(literal);
                        try writer.print("                const raw = {s};\n", .{raw_expr});
                        try writer.print("                const stored = raw ^ {s};\n", .{literal});
                        try writer.print("                self._builder.writeU16({}, stored);\n", .{byte_offset});
                    } else {
                        try writer.print("                self._builder.writeU16({}, {s});\n", .{ byte_offset, raw_expr });
                    }
                } else {
                    try writer.print("                self._builder.writeU16({}, {s});\n", .{ byte_offset, raw_expr });
                }
            },
            .text => try writer.print("                try self._builder.writeText({}, value);\n", .{slot.offset}),
            .data => try writer.print("                try self._builder.writeData({}, value);\n", .{slot.offset}),
            else => {},
        }

        try writer.writeAll("            }\n\n");
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
                        try writer.writeAll("            return ");
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
                .interface => {
                    try writer.print("            const root = try {s}_message.getRootAnyPointer();\n", .{const_name});
                    try writer.writeAll("            return try root.getCapability();\n");
                },
                else => return error.InvalidDefaultPointerType,
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

        try writer.writeAll("        pub fn wrap(builder: message.StructBuilder) Builder {\n");
        try writer.writeAll("            return .{ ._builder = builder };\n");
        try writer.writeAll("        }\n\n");

        // Generate field setters
        for (struct_info.fields) |field| {
            if (field.group != null) {
                try self.generateGroupBuilderAccessor(field, struct_info, writer);
            } else {
                try self.generateFieldSetter(field, struct_info, writer);
            }
        }

        try writer.writeAll("    };\n");
    }

    fn generateFieldSetter(self: *StructGenerator, field: schema.Field, parent_struct_info: schema.StructNode, writer: anytype) !void {
        const slot = field.slot orelse return;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        switch (slot.type) {
            .list => |list_info| {
                const unresolved_struct_layout = switch (list_info.element_type.*) {
                    .@"struct" => |struct_info| self.structLayout(struct_info.type_id) == null,
                    else => false,
                };
                const builder_type = try self.listBuilderTypeString(list_info.element_type.*);
                defer self.allocator.free(builder_type);
                if (unresolved_struct_layout) {
                    try writer.print("        pub fn init{s}(self: *Builder, element_count: u32, data_words: u16, pointer_words: u16) !{s} {{\n", .{
                        cap_name,
                        builder_type,
                    });
                } else {
                    try writer.print("        pub fn init{s}(self: *Builder, element_count: u32) !{s} {{\n", .{
                        cap_name,
                        builder_type,
                    });
                }

                try self.writeUnionDiscriminant(field, parent_struct_info, writer);

                switch (list_info.element_type.*) {
                    .void => try writer.print("            return try self._builder.writeVoidList({}, element_count);\n", .{slot.offset}),
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
                    .data => {
                        try writer.print("            const raw = try self._builder.writePointerList({}, element_count);\n", .{slot.offset});
                        try writer.writeAll("            return DataListBuilder{ ._list = raw };\n");
                    },
                    .interface => {
                        try writer.print("            const raw = try self._builder.writePointerList({}, element_count);\n", .{slot.offset});
                        try writer.writeAll("            return CapabilityListBuilder{ ._list = raw };\n");
                    },
                    .@"enum" => |enum_info| {
                        const enum_name = self.enumTypeName(enum_info.type_id);
                        defer if (enum_name) |name| self.allocator.free(name);
                        if (enum_name) |name| {
                            try writer.print("            const raw = try self._builder.writeU16List({}, element_count);\n", .{slot.offset});
                            try writer.print("            return EnumListBuilder({s}){{ ._list = raw }};\n", .{name});
                        } else {
                            try writer.print("            return try self._builder.writeU16List({}, element_count);\n", .{slot.offset});
                        }
                    },
                    .@"struct" => |struct_info| {
                        const struct_name = self.structTypeName(struct_info.type_id);
                        defer if (struct_name) |name| self.allocator.free(name);
                        if (self.structLayout(struct_info.type_id)) |layout| {
                            if (struct_name) |name| {
                                try writer.print("            const raw = try self._builder.writeStructList({}, element_count, {}, {});\n", .{
                                    slot.offset,
                                    layout.data_words,
                                    layout.pointer_words,
                                });
                                try writer.print("            return StructListBuilder({s}){{ ._list = raw }};\n", .{name});
                            } else {
                                try writer.print("            return try self._builder.writeStructList({}, element_count, {}, {});\n", .{
                                    slot.offset,
                                    layout.data_words,
                                    layout.pointer_words,
                                });
                            }
                        } else {
                            try writer.print(
                                "            return try self._builder.writeStructList({}, element_count, data_words, pointer_words);\n",
                                .{slot.offset},
                            );
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
                if (self.structLayout(struct_info.type_id)) |layout| {
                    if (struct_name) |name| {
                        try writer.print("        pub fn init{s}(self: *Builder) !{s}.Builder {{\n", .{ cap_name, name });
                        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                        try writer.print("            const builder = try self._builder.initStruct({}, {}, {});\n", .{
                            slot.offset,
                            layout.data_words,
                            layout.pointer_words,
                        });
                        try writer.print("            return {s}.Builder{{ ._builder = builder }};\n", .{name});
                        try writer.writeAll("        }\n\n");
                    } else {
                        try writer.print("        pub fn init{s}(self: *Builder) !message.StructBuilder {{\n", .{cap_name});
                        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                        try writer.print("            return try self._builder.initStruct({}, {}, {});\n", .{
                            slot.offset,
                            layout.data_words,
                            layout.pointer_words,
                        });
                        try writer.writeAll("        }\n\n");
                    }
                } else {
                    try writer.print("        pub fn init{s}(self: *Builder, data_words: u16, pointer_words: u16) !message.StructBuilder {{\n", .{cap_name});
                    try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                    try writer.print("            return try self._builder.initStruct({}, data_words, pointer_words);\n", .{slot.offset});
                    try writer.writeAll("        }\n\n");
                }
                return;
            },
            .any_pointer => {
                try self.writeAnyPointerMethod(cap_name, field, parent_struct_info, slot.offset, false, writer);
                return;
            },
            .interface => {
                try self.writeAnyPointerMethod(cap_name, field, parent_struct_info, slot.offset, true, writer);
                try self.writeInterfaceCapabilityHelpers(cap_name, slot.type.interface.type_id, field, parent_struct_info, slot.offset, writer);
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

        // Write union discriminant if this is a union field
        try self.writeUnionDiscriminant(field, parent_struct_info, writer);

        switch (slot.type) {
            .void => try writer.writeAll("            _ = value;\n"),
            .bool => {
                const byte_offset = slot.offset / 8;
                const bit_offset = @as(u3, @truncate(slot.offset % 8));
                if (slot.default_value) |default_value| {
                    const default_bool = self.defaultBool(default_value);
                    try writer.print("            self._builder.writeBool({}, {}, value != {s});\n", .{
                        byte_offset,
                        bit_offset,
                        if (default_bool) "true" else "false",
                    });
                } else {
                    try writer.print("            self._builder.writeBool({}, {}, value);\n", .{ byte_offset, bit_offset });
                }
            },
            .int8, .uint8 => try self.writeNumericSetterBody(slot, "writeU8", "u8", writer),
            .int16, .uint16 => try self.writeNumericSetterBody(slot, "writeU16", "u16", writer),
            .int32, .uint32, .float32 => try self.writeNumericSetterBody(slot, "writeU32", "u32", writer),
            .int64, .uint64, .float64 => try self.writeNumericSetterBody(slot, "writeU64", "u64", writer),
            .@"enum" => |enum_info| {
                const byte_offset = self.dataByteOffset(slot.type, slot.offset);
                const enum_name = self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);
                const raw_expr = if (enum_name != null) "@as(u16, @intFromEnum(value))" else "@as(u16, value)";
                if (slot.default_value) |default_value| {
                    if (try self.defaultLiteral(.uint16, default_value)) |literal| {
                        defer self.allocator.free(literal);
                        try writer.print("            const raw = {s};\n", .{raw_expr});
                        try writer.print("            const stored = raw ^ {s};\n", .{literal});
                        try writer.print("            self._builder.writeU16({}, stored);\n", .{byte_offset});
                    } else {
                        try writer.print("            self._builder.writeU16({}, {s});\n", .{ byte_offset, raw_expr });
                    }
                } else {
                    try writer.print("            self._builder.writeU16({}, {s});\n", .{ byte_offset, raw_expr });
                }
            },
            .text => try writer.print("            try self._builder.writeText({}, value);\n", .{slot.offset}),
            .data => try writer.print("            try self._builder.writeData({}, value);\n", .{slot.offset}),
            else => return error.InvalidFieldSetterType,
        }

        try writer.writeAll("        }\n\n");
    }

    fn getSimpleName(self: *StructGenerator, node: *const schema.Node) []const u8 {
        _ = self;
        const prefix_len = node.display_name_prefix_length;
        if (prefix_len >= node.display_name.len) return node.display_name;
        return node.display_name[prefix_len..];
    }

    fn allocTypeName(self: *StructGenerator, node: *const schema.Node) ![]u8 {
        const name = self.getSimpleName(node);
        return types.identToZigTypeName(self.allocator, name);
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
            .interface => try self.allocator.dupe(u8, "message.Capability"),
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
            .list => |list_info| try self.listFieldReaderTypeString(list_info.element_type.*),
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
            .void => "readVoidList",
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
            .@"struct" => try self.allocator.dupe(u8, "message.StructListReader"),
            .@"enum" => try self.allocator.dupe(u8, "message.U16ListReader"),
            else => try self.allocator.dupe(u8, "message.PointerListReader"),
        };
    }

    fn listFieldReaderTypeString(self: *StructGenerator, elem_type: schema.Type) ![]const u8 {
        return switch (elem_type) {
            .data => try self.allocator.dupe(u8, "DataListReader"),
            .interface => try self.allocator.dupe(u8, "CapabilityListReader"),
            .@"enum" => |enum_info| blk: {
                if (self.enumTypeName(enum_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "EnumListReader({s})", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.U16ListReader");
            },
            .@"struct" => |struct_info| blk: {
                if (self.structTypeName(struct_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "StructListReader({s})", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.StructListReader");
            },
            else => try self.listReaderTypeString(elem_type),
        };
    }

    fn listBuilderTypeString(self: *StructGenerator, elem_type: schema.Type) ![]const u8 {
        return switch (elem_type) {
            .void => try self.allocator.dupe(u8, "message.VoidListBuilder"),
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
            .data => try self.allocator.dupe(u8, "DataListBuilder"),
            .interface => try self.allocator.dupe(u8, "CapabilityListBuilder"),
            .@"enum" => |enum_info| blk: {
                if (self.enumTypeName(enum_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "EnumListBuilder({s})", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.U16ListBuilder");
            },
            .@"struct" => |struct_info| blk: {
                if (self.structTypeName(struct_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "StructListBuilder({s})", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.StructListBuilder");
            },
            else => try self.allocator.dupe(u8, "message.PointerListBuilder"),
        };
    }

    fn structTypeName(self: *StructGenerator, id: schema.Id) ?[]const u8 {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .@"struct") return null;
        return self.qualifiedTypeName(node, id) catch null;
    }

    fn enumTypeName(self: *StructGenerator, id: schema.Id) ?[]const u8 {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .@"enum") return null;
        return self.qualifiedTypeName(node, id) catch null;
    }

    fn interfaceTypeName(self: *StructGenerator, id: schema.Id) ?[]const u8 {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .interface) return null;
        return self.qualifiedTypeName(node, id) catch null;
    }

    /// Return the type name, qualified with import module prefix for cross-file types.
    fn qualifiedTypeName(self: *StructGenerator, node: *const schema.Node, id: schema.Id) ![]const u8 {
        const bare_name = try self.allocTypeName(node);
        if (self.type_prefix_fn) |prefix_fn| {
            if (prefix_fn(self.node_lookup_ctx, id)) |prefix| {
                defer self.allocator.free(bare_name);
                return std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ prefix, bare_name });
            }
        }
        return bare_name;
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
            .interface => try self.allocator.dupe(u8, "message.Capability"),
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

    fn writeNumericGroupGetterWithoutDefault(self: *StructGenerator, typ: schema.Type, byte_offset: u32, writer: anytype) !void {
        const read_fn = self.readFnForType(typ);
        if (self.isUnsigned(typ)) {
            try writer.print("                return self._reader.{s}({});\n", .{ read_fn, byte_offset });
        } else {
            try writer.print("                return @bitCast(self._reader.{s}({}));\n", .{ read_fn, byte_offset });
        }
    }

    /// Emit the opening boilerplate for a group's inner Reader or Builder struct,
    /// including the wrapped field and its `wrap` constructor.
    fn writeGroupWrapStruct(
        self: *StructGenerator,
        writer: anytype,
        comptime type_name: []const u8,
        comptime field_name: []const u8,
        comptime field_type: []const u8,
        comptime param_name: []const u8,
    ) !void {
        _ = self;
        try writer.writeAll("        pub const " ++ type_name ++ " = struct {\n");
        try writer.writeAll("            " ++ field_name ++ ": " ++ field_type ++ ",\n\n");
        try writer.writeAll("            pub fn wrap(" ++ param_name ++ ": " ++ field_type ++ ") " ++ type_name ++ " {\n");
        try writer.writeAll("                return .{ ." ++ field_name ++ " = " ++ param_name ++ " };\n");
        try writer.writeAll("            }\n\n");
    }

    /// Emit the union discriminant write if the field is a union member.
    fn writeUnionDiscriminant(
        self: *StructGenerator,
        field: schema.Field,
        parent_struct_info: schema.StructNode,
        writer: anytype,
    ) !void {
        _ = self;
        if (field.discriminant_value != 0xFFFF and parent_struct_info.discriminant_count > 0) {
            const disc_byte_offset = parent_struct_info.discriminant_offset * 2;
            try writer.print("            self._builder.writeU16({}, {});\n", .{ disc_byte_offset, field.discriminant_value });
        }
    }

    /// Emit an init method and optional setter/clear methods for an AnyPointer or
    /// interface field on a Builder.
    fn writeAnyPointerMethod(
        self: *StructGenerator,
        cap_name: []const u8,
        field: schema.Field,
        parent_struct_info: schema.StructNode,
        slot_offset: u32,
        is_interface: bool,
        writer: anytype,
    ) !void {
        // init method
        try writer.print("        pub fn init{s}(self: *Builder) !message.AnyPointerBuilder {{\n", .{cap_name});
        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
        try writer.print("            return try self._builder.getAnyPointer({});\n", .{slot_offset});
        try writer.writeAll("        }\n\n");

        if (is_interface) {
            // clear method
            try writer.print("        pub fn clear{s}(self: *Builder) !void {{\n", .{cap_name});
            try self.writeUnionDiscriminant(field, parent_struct_info, writer);
            try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
            try writer.writeAll("            try any.setNull();\n");
            try writer.writeAll("        }\n\n");
        } else {
            // setNull method
            try writer.print("        pub fn set{s}Null(self: *Builder) !void {{\n", .{cap_name});
            try self.writeUnionDiscriminant(field, parent_struct_info, writer);
            try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
            try writer.writeAll("            try any.setNull();\n");
            try writer.writeAll("        }\n\n");

            // setText method
            try writer.print("        pub fn set{s}Text(self: *Builder, value: []const u8) !void {{\n", .{cap_name});
            try self.writeUnionDiscriminant(field, parent_struct_info, writer);
            try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
            try writer.writeAll("            try any.setText(value);\n");
            try writer.writeAll("        }\n\n");

            // setData method
            try writer.print("        pub fn set{s}Data(self: *Builder, value: []const u8) !void {{\n", .{cap_name});
            try self.writeUnionDiscriminant(field, parent_struct_info, writer);
            try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
            try writer.writeAll("            try any.setData(value);\n");
            try writer.writeAll("        }\n\n");
        }

        // setCapability method
        try writer.print("        pub fn set{s}Capability(self: *Builder, cap: message.Capability) !void {{\n", .{cap_name});
        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
        try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
        try writer.writeAll("            try any.setCapability(cap);\n");
        try writer.writeAll("        }\n\n");
    }

    /// Emit typed helper methods for an interface-typed Builder field:
    /// setXxxServer (exports a server and writes the capability pointer) and
    /// setXxxClient (writes an existing client's capability pointer).
    fn writeInterfaceCapabilityHelpers(
        self: *StructGenerator,
        cap_name: []const u8,
        type_id: schema.Id,
        field: schema.Field,
        parent_struct_info: schema.StructNode,
        slot_offset: u32,
        writer: anytype,
    ) !void {
        const iface_name = self.interfaceTypeName(type_id) orelse return;
        defer self.allocator.free(iface_name);

        // setXxxServer: exports a server and writes the capability pointer
        try writer.print("        pub fn set{s}Server(self: *Builder, peer: *rpc.peer.Peer, server: *{s}.Server) !void {{\n", .{ cap_name, iface_name });
        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
        try writer.print("            const cap_id = try {s}.exportServer(peer, server);\n", .{iface_name});
        try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
        try writer.writeAll("            try any.setCapability(.{ .id = cap_id });\n");
        try writer.writeAll("        }\n\n");

        // setXxxClient: writes an existing client's capability pointer
        try writer.print("        pub fn set{s}Client(self: *Builder, client: {s}.Client) !void {{\n", .{ cap_name, iface_name });
        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
        try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
        try writer.writeAll("            try any.setCapability(.{ .id = client.cap_id });\n");
        try writer.writeAll("        }\n\n");
    }

    /// Emit a numeric setter body with optional XOR-default handling.
    fn writeNumericSetterBody(
        self: *StructGenerator,
        slot: schema.FieldSlot,
        write_fn: []const u8,
        cast_width: []const u8,
        writer: anytype,
    ) !void {
        const byte_offset = self.dataByteOffset(slot.type, slot.offset);
        if (slot.default_value) |default_value| {
            if (try self.defaultLiteral(slot.type, default_value)) |literal| {
                defer self.allocator.free(literal);
                try writer.print("            const stored = @as({s}, @bitCast(value)) ^ {s};\n", .{ cast_width, literal });
                try writer.print("            self._builder.{s}({}, stored);\n", .{ write_fn, byte_offset });
                return;
            }
        }
        try writer.print("            self._builder.{s}({}, @bitCast(value));\n", .{ write_fn, byte_offset });
    }

    /// Emit a numeric setter body for group fields (4-level indent) with optional XOR-default handling.
    fn writeGroupListSetterBody(self: *StructGenerator, element_type: schema.Type, slot_offset: u32, writer: anytype) !void {
        switch (element_type) {
            .void => try writer.print("                return try self._builder.writeVoidList({}, element_count);\n", .{slot_offset}),
            .bool => try writer.print("                return try self._builder.writeBoolList({}, element_count);\n", .{slot_offset}),
            .int8 => try writer.print("                return try self._builder.writeI8List({}, element_count);\n", .{slot_offset}),
            .uint8 => try writer.print("                return try self._builder.writeU8List({}, element_count);\n", .{slot_offset}),
            .int16 => try writer.print("                return try self._builder.writeI16List({}, element_count);\n", .{slot_offset}),
            .uint16 => try writer.print("                return try self._builder.writeU16List({}, element_count);\n", .{slot_offset}),
            .int32 => try writer.print("                return try self._builder.writeI32List({}, element_count);\n", .{slot_offset}),
            .uint32 => try writer.print("                return try self._builder.writeU32List({}, element_count);\n", .{slot_offset}),
            .float32 => try writer.print("                return try self._builder.writeF32List({}, element_count);\n", .{slot_offset}),
            .int64 => try writer.print("                return try self._builder.writeI64List({}, element_count);\n", .{slot_offset}),
            .uint64 => try writer.print("                return try self._builder.writeU64List({}, element_count);\n", .{slot_offset}),
            .float64 => try writer.print("                return try self._builder.writeF64List({}, element_count);\n", .{slot_offset}),
            .text => try writer.print("                return try self._builder.writeTextList({}, element_count);\n", .{slot_offset}),
            .data => {
                try writer.print("                const raw = try self._builder.writePointerList({}, element_count);\n", .{slot_offset});
                try writer.writeAll("                return DataListBuilder{ ._list = raw };\n");
            },
            .interface => {
                try writer.print("                const raw = try self._builder.writePointerList({}, element_count);\n", .{slot_offset});
                try writer.writeAll("                return CapabilityListBuilder{ ._list = raw };\n");
            },
            .@"enum" => |enum_info| {
                const enum_name = self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);
                if (enum_name) |name| {
                    try writer.print("                const raw = try self._builder.writeU16List({}, element_count);\n", .{slot_offset});
                    try writer.print("                return EnumListBuilder({s}){{ ._list = raw }};\n", .{name});
                } else {
                    try writer.print("                return try self._builder.writeU16List({}, element_count);\n", .{slot_offset});
                }
            },
            .@"struct" => |struct_info| {
                const struct_name = self.structTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                if (self.structLayout(struct_info.type_id)) |layout| {
                    if (struct_name) |name| {
                        try writer.print("                const raw = try self._builder.writeStructList({}, element_count, {}, {});\n", .{ slot_offset, layout.data_words, layout.pointer_words });
                        try writer.print("                return StructListBuilder({s}){{ ._list = raw }};\n", .{name});
                    } else {
                        try writer.print("                return try self._builder.writeStructList({}, element_count, {}, {});\n", .{ slot_offset, layout.data_words, layout.pointer_words });
                    }
                } else {
                    try writer.print("                return try self._builder.writeStructList({}, element_count, data_words, pointer_words);\n", .{slot_offset});
                }
            },
            else => try writer.print("                return try self._builder.writePointerList({}, element_count);\n", .{slot_offset}),
        }
    }

    fn writeNumericGroupSetterBody(
        self: *StructGenerator,
        slot: schema.FieldSlot,
        write_fn: []const u8,
        cast_width: []const u8,
        writer: anytype,
    ) !void {
        const byte_offset = self.dataByteOffset(slot.type, slot.offset);
        if (slot.default_value) |default_value| {
            if (try self.defaultLiteral(slot.type, default_value)) |literal| {
                defer self.allocator.free(literal);
                try writer.print("                const stored = @as({s}, @bitCast(value)) ^ {s};\n", .{ cast_width, literal });
                try writer.print("                self._builder.{s}({}, stored);\n", .{ write_fn, byte_offset });
                return;
            }
        }
        try writer.print("                self._builder.{s}({}, @bitCast(value));\n", .{ write_fn, byte_offset });
    }
};
