const std = @import("std");
const schema = @import("../serialization/schema.zig");
const type_resolver = @import("../serialization/type_resolver.zig");
const brand_fidelity = @import("brand_fidelity.zig");
const types = @import("types.zig");
const TypeGenerator = types.TypeGenerator;
const ArrayListWriter = @import("generator.zig").ArrayListWriter;

/// Generates Zig source code for a single Cap'n Proto struct node, emitting
/// a `Reader` type (zero-copy field accessors) and a `Builder` type (field
/// writers), plus list helpers, union tag enums, and nested group types.
pub const StructGenerator = struct {
    pub const ApiProfile = enum {
        full,
        compact,
    };

    allocator: std.mem.Allocator,
    type_gen: TypeGenerator,
    node_lookup_ctx: ?*anyopaque,
    node_lookup: ?*const fn (ctx: ?*anyopaque, id: schema.Id) ?*const schema.Node,
    /// Optional callback returning the import module prefix for cross-file types.
    /// Returns null for types in the current file.
    type_prefix_fn: ?*const fn (ctx: ?*anyopaque, id: schema.Id) std.mem.Allocator.Error!?[]const u8 = null,
    /// Optional callback returning the parent-scope dotted path for a nested type
    /// (e.g. "Outer1" for Outer1.Inner), or null for a file-scoped type. The
    /// returned slice is owned and freed by the caller (qualifiedTypeName).
    parent_path_fn: ?*const fn (ctx: ?*anyopaque, id: schema.Id) std.mem.Allocator.Error!?[]const u8 = null,
    /// How this struct names its OWN Reader/Builder/WhichTag in self-references.
    /// For a file-scoped struct these are the bare "Reader"/"Builder"/"WhichTag"
    /// (byte-identical to the pre-nesting output). For a struct nested inside
    /// another struct, the enclosing struct also declares Reader/Builder/WhichTag,
    /// so a bare reference is ambiguous — self-references then use `@This()`
    /// (for the enclosing Reader/Builder) and `<Name>.WhichTag`. Set in generate().
    reader_ref: []const u8 = "Reader",
    builder_ref: []const u8 = "Builder",
    whichtag_ref: []const u8 = "WhichTag",
    whichtag_ref_owned: bool = false,
    /// Declaring node whose field application is currently being inspected.
    /// Keeping the caller frame lets the shared resolver distinguish a valid
    /// unbound lexical parameter from a malformed unrelated scope.
    brand_owner: ?*const schema.Node = null,
    /// Controls generated convenience API surface for Reader/Builder wrappers.
    api_profile: ApiProfile = .full,
    max_brand_specializations: usize = 4096,
    in_brand_emission: bool = false,

    const ListHelperUsage = struct {
        enum_list: bool = false,
        struct_list: bool = false,
        data_list: bool = false,
        capability_list: bool = false,

        fn any(self: @This()) bool {
            return self.enum_list or self.struct_list or self.data_list or self.capability_list;
        }
    };

    /// Create a standalone struct generator (no cross-node lookup support).
    pub fn init(allocator: std.mem.Allocator) StructGenerator {
        return .{
            .allocator = allocator,
            .type_gen = TypeGenerator.init(allocator),
            .node_lookup_ctx = null,
            .node_lookup = null,
            .api_profile = .full,
        };
    }

    /// Create a struct generator with cross-node lookup for resolving
    /// type references to other schema nodes.
    pub fn initWithLookup(
        allocator: std.mem.Allocator,
        node_lookup: *const fn (ctx: ?*anyopaque, id: schema.Id) ?*const schema.Node,
        node_lookup_ctx: ?*anyopaque,
    ) StructGenerator {
        return .{
            .allocator = allocator,
            .type_gen = TypeGenerator.initWithLookup(allocator, node_lookup, node_lookup_ctx),
            .node_lookup_ctx = node_lookup_ctx,
            .node_lookup = node_lookup,
            .api_profile = .full,
        };
    }

    pub fn setApiProfile(self: *StructGenerator, profile: ApiProfile) void {
        self.api_profile = profile;
    }

    fn getNode(self: *const StructGenerator, id: schema.Id) ?*const schema.Node {
        const lookup = self.node_lookup orelse return null;
        return lookup(self.node_lookup_ctx, id);
    }

    fn brandOwnerHasGenericScope(self: *const StructGenerator) bool {
        var node = self.brand_owner;
        var depth: usize = 0;
        while (node) |current| {
            if (depth >= type_resolver.max_resolution_depth) return true;
            depth += 1;
            if (current.is_generic or current.parameters.len != 0) return true;
            if (current.scope_id == 0) return false;
            node = self.getNode(current.scope_id);
        }
        return false;
    }

    fn groupAccessorTypeName(self: *StructGenerator, group_node: *const schema.Node) ![]const u8 {
        if (self.brandOwnerHasGenericScope()) {
            return (try self.brandStructTypeName(group_node.id)) orelse error.InvalidStructNode;
        }
        return self.allocTypeName(group_node);
    }

    /// Convert a discriminant_offset (u32, in units of u16) to a byte offset.
    /// Returns an error if the multiplication overflows, which indicates a
    /// malicious or corrupt schema.
    fn discriminantByteOffset(discriminant_offset: u32) error{InvalidDiscriminantOffset}!u32 {
        return std.math.mul(u32, discriminant_offset, 2) catch return error.InvalidDiscriminantOffset;
    }

    /// Emit the complete Zig type definition for a struct node, including
    /// Reader, Builder, union tag enum, group types, and list helpers.
    /// `nested_children`, when non-null, is the already-rendered (canonical
    /// file-indent) text of this struct's nested named types; it is reindented
    /// one level and spliced inside the struct body, just before the closing
    /// brace, so nested types live under their parent instead of at file scope.
    ///
    /// `self_qualify` is set when this struct is nested inside ANOTHER struct,
    /// whose Reader/Builder/WhichTag would otherwise shadow this struct's own —
    /// in that case self-references are disambiguated (`@This()` and
    /// `<Name>.WhichTag`). File-scoped structs keep bare references.
    pub fn generate(self: *StructGenerator, node: *const schema.Node, writer: anytype, nested_children: ?[]const u8, self_qualify: bool) !void {
        const previous_brand_owner = self.brand_owner;
        self.brand_owner = node;
        defer self.brand_owner = previous_brand_owner;
        const struct_info = node.struct_node orelse return error.InvalidStructNode;
        const name = try self.allocTypeName(node);
        defer self.allocator.free(name);

        if (self_qualify) {
            self.reader_ref = "@This()";
            self.builder_ref = "@This()";
            self.whichtag_ref = try std.fmt.allocPrint(self.allocator, "{s}.WhichTag", .{name});
            self.whichtag_ref_owned = true;
        }
        defer if (self.whichtag_ref_owned) {
            self.allocator.free(self.whichtag_ref);
            self.whichtag_ref = "WhichTag";
            self.whichtag_ref_owned = false;
        };

        const list_helper_usage = try self.collectListHelperUsage(struct_info);

        const data_word_count = struct_info.data_word_count;
        const pointer_count = struct_info.pointer_count;

        try writer.print("pub const {s} = struct {{\n", .{name});
        try self.generateListHelpers(list_helper_usage, writer);

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

        // Splice nested named types (structs/enums/consts declared inside this
        // one) reindented one level, before the closing brace.
        if (nested_children) |children| {
            try writeReindented(writer, children, 4);
        }

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
            try self.renderGroupBlock(group_node, writer);
        }
    }

    /// Render a single group's Zig type block (nested type declarations, Reader,
    /// and Builder) at the canonical indent used for a struct's direct group
    /// members. Nested group types are emitted recursively into the parent
    /// group's namespace, re-indented so a group nested arbitrarily deep still
    /// produces well-formed source. `writer` receives the finished block.
    fn renderGroupBlock(self: *StructGenerator, group_node: *const schema.Node, writer: anytype) !void {
        const previous_brand_owner = self.brand_owner;
        self.brand_owner = group_node;
        defer self.brand_owner = previous_brand_owner;
        const group_struct_info = group_node.struct_node orelse return;
        const group_name = try self.allocTypeName(group_node);
        defer self.allocator.free(group_name);
        const group_type_name = (try self.structTypeName(group_node.id)) orelse return error.InvalidStructNode;
        defer self.allocator.free(group_type_name);

        try writer.print("    pub const {s} = struct {{\n", .{group_name});

        if (group_struct_info.discriminant_count > 0) {
            try writer.writeAll("        pub const WhichTag = enum(u16) {\n");
            for (group_struct_info.fields) |group_field| {
                if (group_field.discriminant_value == 0xFFFF) continue;
                const zig_name = try self.type_gen.toZigIdentifier(group_field.name);
                defer self.allocator.free(zig_name);
                const escaped_name = try types.escapeZigKeyword(self.allocator, zig_name);
                defer self.allocator.free(escaped_name);
                try writer.print("            {s} = {},\n", .{ escaped_name, group_field.discriminant_value });
            }
            try writer.writeAll("        };\n\n");
        }

        // Emit nested group types (groups declared inside this group, including
        // group-typed union variants) as siblings of this group's Reader/Builder.
        // Each nested block is rendered at the canonical indent, then shifted one
        // level deeper to sit inside this group's namespace.
        for (group_struct_info.fields) |group_field| {
            const nested_group = group_field.group orelse continue;
            const nested_node = self.getNode(nested_group.type_id) orelse continue;
            try self.renderNestedGroupBlock(nested_node, writer);
        }

        // Generate group Reader
        try self.writeGroupWrapStruct(writer, "Reader", "_reader", "message.StructReader", "reader");
        try self.generatePointerDefaults(group_struct_info, "            ", "                ", writer);
        try self.generateEnumOrdinalsReaderView(
            group_struct_info,
            "            ",
            "                ",
            "                    ",
            writer,
        );
        try self.generateNestedListsReaderView(
            group_struct_info,
            "            ",
            "                ",
            "                    ",
            writer,
        );
        try self.generateBrandsReaderView(
            group_struct_info,
            "            ",
            "                ",
            "                    ",
            writer,
        );
        try self.generatePointerKindsReaderView(
            group_struct_info,
            "            ",
            "                ",
            "                    ",
            writer,
        );
        if (group_struct_info.discriminant_count > 0) {
            const disc_byte_offset = try discriminantByteOffset(group_struct_info.discriminant_offset);
            try writer.writeAll("            pub fn whichOrdinal(self: @This()) u16 {\n");
            try writer.print("                return self._reader.readUnionDiscriminant({});\n", .{disc_byte_offset});
            try writer.writeAll("            }\n\n");
            try writer.print("            pub fn which(self: @This()) error{{InvalidEnumValue}}!{s}.WhichTag {{\n", .{group_type_name});
            try writer.print("                return std.enums.fromInt({s}.WhichTag, self.whichOrdinal()) orelse return error.InvalidEnumValue;\n", .{group_type_name});
            try writer.writeAll("            }\n\n");
        }
        for (group_struct_info.fields) |group_field| {
            if (group_field.group != null) {
                try self.generateGroupNestedReaderAccessor(group_field, group_struct_info, writer);
            } else {
                try self.generateGroupFieldGetter(group_field, group_struct_info, writer);
            }
        }
        try writer.writeAll("        };\n\n");

        // Generate group Builder
        try self.writeGroupWrapStruct(writer, "Builder", "_builder", "message.StructBuilder", "builder");
        try self.generateEnumOrdinalsBuilderView(
            group_struct_info,
            "            ",
            "                ",
            "                    ",
            writer,
        );
        try self.generateNestedListsBuilderView(
            group_struct_info,
            "            ",
            "                ",
            "                    ",
            writer,
        );
        try self.generateBrandsBuilderView(
            group_struct_info,
            "            ",
            "                ",
            "                    ",
            writer,
        );
        try self.generatePointerKindsBuilderView(
            group_struct_info,
            "            ",
            "                ",
            "                    ",
            writer,
        );
        for (group_struct_info.fields) |group_field| {
            if (group_field.group != null) {
                try self.generateGroupNestedBuilderAccessor(group_field, group_struct_info, writer);
            } else {
                try self.generateGroupFieldSetter(group_field, group_struct_info, writer);
            }
        }
        try writer.writeAll("        };\n");

        try writer.writeAll("    };\n\n");
    }

    /// Render a nested group's block into a scratch buffer at canonical indent,
    /// then emit it into `writer` shifted 4 spaces deeper so it nests correctly
    /// inside its parent group struct.
    /// Error set for group-block rendering. Declared explicitly so the mutual
    /// recursion between `renderGroupBlock` and `renderNestedGroupBlock` does not
    /// form an inferred-error-set dependency loop.
    const GroupRenderError = std.mem.Allocator.Error || error{
        CodegenBudgetExceeded,
        InvalidDiscriminantOffset,
        InvalidFieldOffset,
        InvalidStructNode,
        InvalidDefaultPointerType,
    };

    fn renderNestedGroupBlock(self: *StructGenerator, nested_node: *const schema.Node, writer: anytype) GroupRenderError!void {
        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(self.allocator);
        const buf_writer = ArrayListWriter{ .list = &buf, .allocator = self.allocator };
        try self.renderGroupBlock(nested_node, buf_writer);
        try writeReindented(writer, buf.items, 4);
    }

    /// Copy `block` to `writer`, prefixing every non-empty line with `spaces`
    /// additional spaces. Blank lines are left untouched so trailing newlines do
    /// not accumulate whitespace.
    fn writeReindented(writer: anytype, block: []const u8, spaces: usize) !void {
        var it = std.mem.splitScalar(u8, block, '\n');
        var first = true;
        while (it.next()) |line| {
            if (!first) try writer.writeByte('\n');
            first = false;
            if (line.len == 0) continue;
            var i: usize = 0;
            while (i < spaces) : (i += 1) try writer.writeByte(' ');
            try writer.writeAll(line);
        }
    }

    /// Emit a group-typed field accessor inside a group's Reader. Mirrors the
    /// top-level `generateGroupFieldAccessor` but at the group's inner indent
    /// with a `@This()` receiver.
    fn generateGroupNestedReaderAccessor(self: *StructGenerator, field: schema.Field, parent_struct_info: schema.StructNode, writer: anytype) !void {
        const group = field.group orelse return;
        const group_node = self.getNode(group.type_id) orelse return;
        const group_name = try self.groupAccessorTypeName(group_node);
        defer self.allocator.free(group_name);

        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        // Guard union-member groups on the discriminant (see
        // generateGroupFieldAccessor), at the group's inner indent.
        const is_union_member = field.discriminant_value != 0xFFFF and parent_struct_info.discriminant_count > 0;
        const bang = if (is_union_member) "!" else "";
        try writer.print("            pub fn get{s}(self: @This()) {s}{s}.Reader {{\n", .{ cap_name, bang, group_name });
        try self.writeUnionMemberGuard(field, parent_struct_info, "                ", writer);
        try writer.writeAll("                return .{ ._reader = self._reader };\n");
        try writer.writeAll("            }\n\n");
    }

    /// Emit a group-typed field accessor inside a group's Builder. Mirrors the
    /// top-level `generateGroupBuilderAccessor`: union-member groups get an
    /// `initXxx` that zeroes their slots and writes the discriminant; plain
    /// groups get a `getXxx`.
    fn generateGroupNestedBuilderAccessor(self: *StructGenerator, field: schema.Field, parent_struct_info: schema.StructNode, writer: anytype) !void {
        const group = field.group orelse return;
        const group_node = self.getNode(group.type_id) orelse return;
        const group_name = try self.groupAccessorTypeName(group_node);
        defer self.allocator.free(group_name);

        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        if (field.discriminant_value != 0xFFFF and parent_struct_info.discriminant_count > 0) {
            const disc_byte_offset = try discriminantByteOffset(parent_struct_info.discriminant_offset);
            try writer.print("            pub fn init{s}(self: *@This()) {s}.Builder {{\n", .{ cap_name, group_name });
            // Zero the group's own slots first (capnp init<group>() semantics),
            // then write the discriminant so an overlapping data word does not
            // clobber it. Uses the deeper builder indent.
            try self.writeGroupSlotZeroingIndented(group_node, "                ", writer);
            try writer.print("                self._builder.writeU16({}, {});\n", .{ disc_byte_offset, field.discriminant_value });
            try writer.writeAll("                return .{ ._builder = self._builder };\n");
            try writer.writeAll("            }\n\n");
        } else {
            try writer.print("            pub fn get{s}(self: *@This()) {s}.Builder {{\n", .{ cap_name, group_name });
            try writer.writeAll("                return .{ ._builder = self._builder };\n");
            try writer.writeAll("            }\n\n");
        }
    }

    fn collectListHelperUsage(self: *StructGenerator, struct_info: schema.StructNode) !ListHelperUsage {
        var usage = ListHelperUsage{};
        try self.collectListHelperUsageFromFields(struct_info.fields, &usage);
        return usage;
    }

    fn collectListHelperUsageFromFields(
        self: *StructGenerator,
        fields: []const schema.Field,
        usage: *ListHelperUsage,
    ) !void {
        for (fields) |field| {
            if (field.group) |group| {
                const group_node = self.getNode(group.type_id) orelse continue;
                const group_struct_info = group_node.struct_node orelse continue;
                try self.collectListHelperUsageFromFields(group_struct_info.fields, usage);
            }

            const slot = field.slot orelse continue;
            if (slot.type != .list) continue;
            try self.markListHelperUsage(slot.type.list.element_type.*, usage);
        }
    }

    fn markListHelperUsage(
        self: *StructGenerator,
        element_type: schema.Type,
        usage: *ListHelperUsage,
    ) !void {
        switch (element_type) {
            .@"enum" => |enum_info| {
                const enum_name = try self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);
                if (enum_name != null) usage.enum_list = true;
            },
            .@"struct" => |struct_info| {
                const struct_name = try self.structTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                if (struct_name != null) usage.struct_list = true;
            },
            .data => usage.data_list = true,
            .interface => usage.capability_list = true,
            else => {},
        }
    }

    fn generateListHelpers(self: *StructGenerator, usage: ListHelperUsage, writer: anytype) !void {
        _ = self;
        if (usage.enum_list) {
            try writer.writeAll("    const EnumListReader = message.typed_list_helpers.EnumListReader;\n");
            try writer.writeAll("    const EnumListBuilder = message.typed_list_helpers.EnumListBuilder;\n");
        }
        if (usage.struct_list) {
            try writer.writeAll("    const StructListReader = message.typed_list_helpers.StructListReader;\n");
            try writer.writeAll("    const StructListBuilder = message.typed_list_helpers.StructListBuilder;\n");
        }
        if (usage.data_list) {
            try writer.writeAll("    const DataListReader = message.typed_list_helpers.DataListReader;\n");
            try writer.writeAll("    const DataListBuilder = message.typed_list_helpers.DataListBuilder;\n");
        }
        if (usage.capability_list) {
            try writer.writeAll("    const CapabilityListReader = message.typed_list_helpers.CapabilityListReader;\n");
            try writer.writeAll("    const CapabilityListBuilder = message.typed_list_helpers.CapabilityListBuilder;\n");
        }
        if (usage.any()) {
            try writer.writeAll("\n");
        }
    }

    fn hasDirectEnumSlot(struct_info: schema.StructNode) bool {
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (slot.type == .@"enum") return true;
        }
        return false;
    }

    fn hasDirectNestedListSlot(struct_info: schema.StructNode) bool {
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (slot.type != .list) continue;
            if (slot.type.list.element_type.* == .list) return true;
        }
        return false;
    }

    const ConcreteBrand = struct {
        target: *const schema.Node,
        target_info: schema.StructNode,
        brand: schema.Brand,
        resolver: type_resolver.Resolver,
    };

    const BoundExpression = struct {
        expression: schema.TypeExpression,
        resolver: type_resolver.Resolver,
        context_depth: u8,
        erased_parameter: bool,
    };
    const BrandRenderError = std.mem.Allocator.Error || error{
        CodegenBudgetExceeded,
        InvalidDiscriminantOffset,
        InvalidStructNode,
    };

    const BrandListInfo = struct {
        reader_type: []const u8,
        builder_type: []const u8,
        get_method: []const u8,
        init_method: []const u8,
    };

    fn brandListInfo(element_type: schema.Type) ?BrandListInfo {
        return switch (element_type) {
            .void => .{ .reader_type = "message.VoidListReader", .builder_type = "message.VoidListBuilder", .get_method = "getVoidList", .init_method = "initVoidList" },
            .bool => .{ .reader_type = "message.BoolListReader", .builder_type = "message.BoolListBuilder", .get_method = "getBoolList", .init_method = "initBoolList" },
            .int8 => .{ .reader_type = "message.I8ListReader", .builder_type = "message.I8ListBuilder", .get_method = "getI8List", .init_method = "initI8List" },
            .uint8 => .{ .reader_type = "message.U8ListReader", .builder_type = "message.U8ListBuilder", .get_method = "getU8List", .init_method = "initU8List" },
            .int16 => .{ .reader_type = "message.I16ListReader", .builder_type = "message.I16ListBuilder", .get_method = "getI16List", .init_method = "initI16List" },
            .uint16 => .{ .reader_type = "message.U16ListReader", .builder_type = "message.U16ListBuilder", .get_method = "getU16List", .init_method = "initU16List" },
            .int32 => .{ .reader_type = "message.I32ListReader", .builder_type = "message.I32ListBuilder", .get_method = "getI32List", .init_method = "initI32List" },
            .uint32 => .{ .reader_type = "message.U32ListReader", .builder_type = "message.U32ListBuilder", .get_method = "getU32List", .init_method = "initU32List" },
            .float32 => .{ .reader_type = "message.F32ListReader", .builder_type = "message.F32ListBuilder", .get_method = "getF32List", .init_method = "initF32List" },
            .int64 => .{ .reader_type = "message.I64ListReader", .builder_type = "message.I64ListBuilder", .get_method = "getI64List", .init_method = "initI64List" },
            .uint64 => .{ .reader_type = "message.U64ListReader", .builder_type = "message.U64ListBuilder", .get_method = "getU64List", .init_method = "initU64List" },
            .float64 => .{ .reader_type = "message.F64ListReader", .builder_type = "message.F64ListBuilder", .get_method = "getF64List", .init_method = "initF64List" },
            else => null,
        };
    }

    fn concreteBrand(self: *StructGenerator, slot: schema.FieldSlot) !?ConcreteBrand {
        if (slot.type != .@"struct") return null;
        const brand = switch (slot.type_metadata) {
            .named => |value| value,
            else => return null,
        };
        const target_id = slot.type.@"struct".type_id;
        const target = self.getNode(target_id) orelse return null;
        if (target.kind != .@"struct") return null;
        const target_info = target.struct_node orelse return null;
        if (target_info.is_group) return null;
        if (!target.is_generic) return null;
        const lookup = self.node_lookup orelse return null;
        const owner = self.brand_owner orelse return error.InvalidStructNode;
        const caller = type_resolver.Resolver.initWithLookup(owner, .{}, lookup, self.node_lookup_ctx) catch return error.InvalidStructNode;
        const resolver = caller.enterNamed(target.id, brand, caller.contextDepth()) catch return error.InvalidStructNode;
        const inspection = brand_fidelity.inspectApplication(
            target,
            &resolver,
            lookup,
            self.node_lookup_ctx,
            self.max_brand_specializations,
        ) catch return error.InvalidStructNode;
        if (inspection == null) return null;
        return .{ .target = target, .target_info = target_info, .brand = brand, .resolver = resolver };
    }

    fn brandBindingForField(self: *StructGenerator, brand: *const ConcreteBrand, field: schema.Field) !?BoundExpression {
        const slot = field.slot orelse return null;
        const cursor = brand.resolver.cursor(.{ .type = slot.type, .metadata = slot.type_metadata });
        brand.resolver.validateExpression(cursor) catch return error.InvalidStructNode;
        const resolution = brand.resolver.resolve(cursor) catch return error.InvalidStructNode;
        if (resolution.unbound) return null;
        const erased_parameter = slot.type == .any_pointer and switch (slot.type_metadata) {
            .any_pointer => |metadata| metadata == .parameter,
            else => false,
        };
        if (!erased_parameter) {
            if (slot.type != .@"struct") return null;
            const node = self.getNode(slot.type.@"struct".type_id) orelse return error.InvalidStructNode;
            if (!node.is_generic) return null;
        }
        return .{
            .expression = resolution.cursor.expression,
            .resolver = brand.resolver,
            .context_depth = resolution.cursor.context_depth,
            .erased_parameter = erased_parameter,
        };
    }

    fn nestedConcreteBrand(self: *StructGenerator, bound: BoundExpression) !?ConcreteBrand {
        if (bound.expression.type != .@"struct") return null;
        const type_id = bound.expression.type.@"struct".type_id;
        const target = self.getNode(type_id) orelse return error.InvalidStructNode;
        if (target.kind != .@"struct" or !target.is_generic) return null;
        const target_info = target.struct_node orelse return error.InvalidStructNode;
        if (target_info.is_group) return null;
        const brand = type_resolver.Resolver.namedBrand(bound.expression) catch return error.InvalidStructNode;
        const resolver = bound.resolver.enterNamed(type_id, brand, bound.context_depth) catch return error.InvalidStructNode;
        const lookup = self.node_lookup orelse return null;
        const inspection = brand_fidelity.inspectApplication(
            target,
            &resolver,
            lookup,
            self.node_lookup_ctx,
            self.max_brand_specializations,
        ) catch return error.InvalidStructNode;
        if (inspection == null) return null;
        return .{ .target = target, .target_info = target_info, .brand = brand, .resolver = resolver };
    }

    fn listConcreteBrandTerminal(self: *StructGenerator, bound: BoundExpression) !?ConcreteBrand {
        if (bound.expression.type != .list) return null;
        var cursor = type_resolver.Cursor{
            .expression = bound.expression,
            .context_depth = bound.context_depth,
        };
        var depth: usize = 0;
        while (cursor.expression.type == .list) {
            if (depth >= type_resolver.max_resolution_depth) return error.InvalidStructNode;
            depth += 1;
            const element = type_resolver.Resolver.listElement(cursor) catch return error.InvalidStructNode;
            bound.resolver.validateExpression(element) catch return error.InvalidStructNode;
            const resolution = bound.resolver.resolve(element) catch return error.InvalidStructNode;
            if (resolution.unbound) return null;
            cursor = resolution.cursor;
        }
        return self.nestedConcreteBrand(.{
            .expression = cursor.expression,
            .resolver = bound.resolver,
            .context_depth = cursor.context_depth,
            .erased_parameter = false,
        });
    }

    fn brandStructTypeName(self: *StructGenerator, id: schema.Id) !?[]const u8 {
        const name = (try self.structTypeName(id)) orelse return null;
        defer self.allocator.free(name);
        if (self.type_prefix_fn) |prefix_fn| {
            if (try prefix_fn(self.node_lookup_ctx, id) != null) {
                return try self.allocator.dupe(u8, name);
            }
        }
        return try std.fmt.allocPrint(self.allocator, "_capnp_file.{s}", .{name});
    }

    fn brandEnumTypeName(self: *StructGenerator, id: schema.Id) !?[]const u8 {
        const name = (try self.enumTypeName(id)) orelse return null;
        defer self.allocator.free(name);
        if (self.type_prefix_fn) |prefix_fn| {
            if (try prefix_fn(self.node_lookup_ctx, id) != null) {
                return try self.allocator.dupe(u8, name);
            }
        }
        return try std.fmt.allocPrint(self.allocator, "_capnp_file.{s}", .{name});
    }

    fn hasDirectConcreteBrandSlot(self: *StructGenerator, struct_info: schema.StructNode) !bool {
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (try self.concreteBrand(slot) != null) return true;
        }
        return false;
    }

    fn resolvedCursor(bound: BoundExpression) !type_resolver.Cursor {
        const initial = type_resolver.Cursor{
            .expression = bound.expression,
            .context_depth = bound.context_depth,
        };
        bound.resolver.validateExpression(initial) catch return error.InvalidStructNode;
        const resolution = bound.resolver.resolve(initial) catch return error.InvalidStructNode;
        if (resolution.unbound) return error.InvalidStructNode;
        return resolution.cursor;
    }

    fn resolvedListElement(bound: BoundExpression) !type_resolver.Cursor {
        const list_cursor = try resolvedCursor(bound);
        const element = type_resolver.Resolver.listElement(list_cursor) catch return error.InvalidStructNode;
        bound.resolver.validateExpression(element) catch return error.InvalidStructNode;
        const resolution = bound.resolver.resolve(element) catch return error.InvalidStructNode;
        if (resolution.unbound) return error.InvalidStructNode;
        return resolution.cursor;
    }

    fn boundFromCursor(bound: BoundExpression, cursor: type_resolver.Cursor) BoundExpression {
        return .{
            .expression = cursor.expression,
            .resolver = bound.resolver,
            .context_depth = cursor.context_depth,
            .erased_parameter = false,
        };
    }

    fn brandReaderTypeString(self: *StructGenerator, bound: BoundExpression) ![]const u8 {
        const cursor = try resolvedCursor(bound);
        return switch (cursor.expression.type) {
            .text, .data => self.allocator.dupe(u8, "[]const u8"),
            .@"struct" => |info| blk: {
                const name = (try self.brandStructTypeName(info.type_id)) orelse return error.InvalidStructNode;
                defer self.allocator.free(name);
                break :blk std.fmt.allocPrint(self.allocator, "{s}.Reader", .{name});
            },
            .list => self.brandListReaderTypeString(boundFromCursor(bound, cursor)),
            .interface => self.allocator.dupe(u8, "message.Capability"),
            .any_pointer => blk: {
                const metadata = switch (cursor.expression.metadata) {
                    .any_pointer => |value| value,
                    else => return error.InvalidStructNode,
                };
                const name: []const u8 = switch (metadata) {
                    .unconstrained => |kind| switch (kind) {
                        .any_kind => "message.AnyPointerReader",
                        .@"struct" => "message.StructReader",
                        .list => "message.AnyListReader",
                        .capability => "message.Capability",
                    },
                    else => return error.InvalidStructNode,
                };
                break :blk self.allocator.dupe(u8, name);
            },
            else => error.InvalidStructNode,
        };
    }

    fn brandListReaderTypeString(self: *StructGenerator, bound: BoundExpression) ![]const u8 {
        const element = try resolvedListElement(bound);
        return switch (element.expression.type) {
            .data => self.allocator.dupe(u8, "message.typed_list_helpers.DataListReader"),
            .interface => self.allocator.dupe(u8, "message.typed_list_helpers.CapabilityListReader"),
            .@"enum" => |info| blk: {
                const name = (try self.brandEnumTypeName(info.type_id)) orelse return error.InvalidStructNode;
                defer self.allocator.free(name);
                break :blk std.fmt.allocPrint(self.allocator, "message.typed_list_helpers.EnumListReader({s})", .{name});
            },
            .@"struct" => |info| blk: {
                const name = (try self.brandStructTypeName(info.type_id)) orelse return error.InvalidStructNode;
                defer self.allocator.free(name);
                break :blk std.fmt.allocPrint(self.allocator, "message.typed_list_helpers.StructListReader({s})", .{name});
            },
            .list => self.resolvedNestedListTypeString(boundFromCursor(bound, element), false),
            else => self.listReaderTypeString(element.expression.type),
        };
    }

    fn brandListBuilderTypeString(self: *StructGenerator, bound: BoundExpression) ![]const u8 {
        const element = try resolvedListElement(bound);
        return switch (element.expression.type) {
            .data => self.allocator.dupe(u8, "message.typed_list_helpers.DataListBuilder"),
            .interface => self.allocator.dupe(u8, "message.typed_list_helpers.CapabilityListBuilder"),
            .@"enum" => |info| blk: {
                const name = (try self.brandEnumTypeName(info.type_id)) orelse return error.InvalidStructNode;
                defer self.allocator.free(name);
                break :blk std.fmt.allocPrint(self.allocator, "message.typed_list_helpers.EnumListBuilder({s})", .{name});
            },
            .@"struct" => |info| blk: {
                const name = (try self.brandStructTypeName(info.type_id)) orelse return error.InvalidStructNode;
                defer self.allocator.free(name);
                break :blk std.fmt.allocPrint(self.allocator, "message.typed_list_helpers.StructListBuilder({s})", .{name});
            },
            .list => self.resolvedNestedListTypeString(boundFromCursor(bound, element), true),
            else => self.listBuilderTypeString(element.expression.type),
        };
    }

    fn writeResolvedNestedCodec(
        self: *StructGenerator,
        bound: BoundExpression,
        comptime builder: bool,
        writer: anytype,
    ) BrandRenderError!void {
        const element = try resolvedListElement(bound);
        if (element.expression.type == .list) {
            try writer.print(
                "message.typed_list_helpers.Nested{s}Codec(",
                .{if (builder) "Builder" else "Reader"},
            );
            try self.writeResolvedNestedCodec(boundFromCursor(bound, element), builder, writer);
            try writer.writeByte(')');
            return;
        }
        try self.writeNestedBaseCodec(element.expression.type, writer);
    }

    fn resolvedNestedListTypeString(
        self: *StructGenerator,
        bound: BoundExpression,
        comptime builder: bool,
    ) ![]const u8 {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);
        const writer = ArrayListWriter{ .list = &out, .allocator = self.allocator };
        try writer.print(
            "message.typed_list_helpers.NestedList{s}(",
            .{if (builder) "Builder" else "Reader"},
        );
        try self.writeResolvedNestedCodec(bound, builder, writer);
        try writer.writeByte(')');
        return out.toOwnedSlice(self.allocator);
    }

    fn brandedListDepth(bound: BoundExpression) !usize {
        var cursor = type_resolver.Cursor{
            .expression = bound.expression,
            .context_depth = bound.context_depth,
        };
        var depth: usize = 0;
        while (cursor.expression.type == .list) {
            if (depth >= type_resolver.max_resolution_depth) return error.InvalidStructNode;
            depth += 1;
            const element = type_resolver.Resolver.listElement(cursor) catch return error.InvalidStructNode;
            bound.resolver.validateExpression(element) catch return error.InvalidStructNode;
            const resolution = bound.resolver.resolve(element) catch return error.InvalidStructNode;
            if (resolution.unbound) return error.InvalidStructNode;
            cursor = resolution.cursor;
        }
        if (depth == 0 or cursor.expression.type != .@"struct") return error.InvalidStructNode;
        return depth;
    }

    fn brandedApplicationListTypeString(
        self: *StructGenerator,
        bound: BoundExpression,
        application_ref: []const u8,
        application: *const ConcreteBrand,
        comptime builder: bool,
    ) ![]const u8 {
        const depth = try brandedListDepth(bound);
        if (depth == 1) {
            return std.fmt.allocPrint(
                self.allocator,
                "message.typed_list_helpers.StructList{s}({s})",
                .{ if (builder) "Builder" else "Reader", application_ref },
            );
        }
        const layout = self.structLayout(application.target.id) orelse return error.InvalidStructNode;
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);
        const writer = ArrayListWriter{ .list = &out, .allocator = self.allocator };
        try writer.print(
            "message.typed_list_helpers.NestedList{s}(",
            .{if (builder) "Builder" else "Reader"},
        );
        var nested_depth: usize = 2;
        while (nested_depth < depth) : (nested_depth += 1) {
            try writer.print(
                "message.typed_list_helpers.Nested{s}Codec(",
                .{if (builder) "Builder" else "Reader"},
            );
        }
        try writer.print(
            "message.typed_list_helpers.StructListCodec({s}, {}, {})",
            .{ application_ref, layout.data_words, layout.pointer_words },
        );
        nested_depth = 2;
        while (nested_depth < depth) : (nested_depth += 1) try writer.writeByte(')');
        try writer.writeByte(')');
        return out.toOwnedSlice(self.allocator);
    }

    fn writeBrandListReaderReturn(
        self: *StructGenerator,
        bound: BoundExpression,
        body_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        _ = self;
        const element = try resolvedListElement(bound);
        const element_type = element.expression.type;
        try writer.print("{s}const list = try message.AnyListReader.wrap(pointer);\n", .{body_indent});
        switch (element_type) {
            .data => try writer.print("{s}return .{{ ._list = try list.getPointerList() }};\n", .{body_indent}),
            .interface => try writer.print("{s}return .{{ ._list = try list.getPointerList() }};\n", .{body_indent}),
            .@"enum" => try writer.print("{s}return .{{ ._list = try list.getU16List() }};\n", .{body_indent}),
            .@"struct" => try writer.print("{s}return .{{ ._list = try list.getStructList() }};\n", .{body_indent}),
            .list => try writer.print("{s}return .{{ ._list = try list.getPointerList() }};\n", .{body_indent}),
            .any_pointer => try writer.print("{s}return try list.getPointerList();\n", .{body_indent}),
            .text => try writer.print("{s}return try list.getTextList();\n", .{body_indent}),
            else => {
                const info = brandListInfo(element_type) orelse return error.InvalidStructNode;
                try writer.print("{s}return try list.{s}();\n", .{ body_indent, info.get_method });
            },
        }
    }

    fn writeBrandListBuilderGetReturn(
        self: *StructGenerator,
        bound: BoundExpression,
        body_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        _ = self;
        const element = try resolvedListElement(bound);
        const element_type = element.expression.type;
        try writer.print("{s}const list = try message.AnyListBuilder.wrap(pointer);\n", .{body_indent});
        switch (element_type) {
            .data, .interface, .list => try writer.print("{s}return .{{ ._list = try list.getPointerList() }};\n", .{body_indent}),
            .@"enum" => try writer.print("{s}return .{{ ._list = try list.getU16List() }};\n", .{body_indent}),
            .@"struct" => try writer.print("{s}return .{{ ._list = try list.getStructList() }};\n", .{body_indent}),
            .any_pointer => try writer.print("{s}return try list.getPointerList();\n", .{body_indent}),
            .text => try writer.print("{s}return try list.getTextList();\n", .{body_indent}),
            else => {
                const info = brandListInfo(element_type) orelse return error.InvalidStructNode;
                try writer.print("{s}return try list.{s}();\n", .{ body_indent, info.get_method });
            },
        }
    }

    fn writeBrandListBuilderInitReturn(
        self: *StructGenerator,
        bound: BoundExpression,
        body_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        const element = try resolvedListElement(bound);
        const element_type = element.expression.type;
        switch (element_type) {
            .data, .interface, .list => try writer.print("{s}return .{{ ._list = try pointer.initPointerList(element_count) }};\n", .{body_indent}),
            .@"enum" => try writer.print("{s}return .{{ ._list = try pointer.initU16List(element_count) }};\n", .{body_indent}),
            .@"struct" => |info| {
                const layout = self.structLayout(info.type_id) orelse return error.InvalidStructNode;
                try writer.print("{s}return .{{ ._list = try pointer.initStructList(element_count, {}, {}) }};\n", .{ body_indent, layout.data_words, layout.pointer_words });
            },
            .any_pointer => try writer.print("{s}return try pointer.initPointerList(element_count);\n", .{body_indent}),
            .text => {
                try writer.print("{s}const list_raw = try pointer.initPointerList(element_count);\n", .{body_indent});
                try writer.print("{s}return .{{ .builder = list_raw.builder, .segment_id = list_raw.segment_id, .elements_offset = list_raw.elements_offset, .element_count = list_raw.element_count }};\n", .{body_indent});
            },
            else => {
                const info = brandListInfo(element_type) orelse return error.InvalidStructNode;
                try writer.print("{s}return try pointer.{s}(element_count);\n", .{ body_indent, info.init_method });
            },
        }
    }

    fn generateBrandReaderParameterMethod(
        self: *StructGenerator,
        field: schema.Field,
        bound: BoundExpression,
        decl_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        const expression = (try resolvedCursor(bound)).expression;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);
        const nested = try self.nestedConcreteBrand(bound);
        const list_application = try self.listConcreteBrandTerminal(bound);
        if (nested) |application| {
            try self.generateNestedBrandReaderWrapper(cap_name, &application, decl_indent, body_indent, writer);
        }
        // Keep the synthesized adapter inside the field's own namespace. A
        // `${Field}Element` sibling is a valid schema name and must not collide
        // with this implementation detail.
        const list_application_name: ?[]const u8 = if (list_application != null) cap_name else null;
        const list_application_ref = if (list_application_name) |name|
            try std.fmt.allocPrint(self.allocator, "@This().{s}", .{name})
        else
            null;
        defer if (list_application_ref) |name| self.allocator.free(name);
        if (list_application) |application| {
            const application_name = list_application_name orelse return error.InvalidStructNode;
            try self.generateBrandedListApplicationAdapter(
                application_name,
                &application,
                decl_indent,
                body_indent,
                writer,
            );
        }
        const return_type = if (nested != null)
            try std.fmt.allocPrint(self.allocator, "@This().{s}", .{cap_name})
        else if (list_application) |*application| blk: {
            const application_ref = list_application_ref orelse return error.InvalidStructNode;
            break :blk try self.brandedApplicationListTypeString(
                bound,
                application_ref,
                application,
                false,
            );
        } else try self.brandReaderTypeString(bound);
        defer self.allocator.free(return_type);

        try writer.print("{s}pub fn get{s}(self: @This()) !{s} {{\n", .{ decl_indent, cap_name, return_type });
        try writer.print("{s}const pointer = try self._reader.get{s}();\n", .{ body_indent, cap_name });
        switch (expression.type) {
            .text => try writer.print("{s}return try pointer.getText();\n", .{body_indent}),
            .data => try writer.print("{s}return try pointer.getData();\n", .{body_indent}),
            .@"struct" => |info| {
                const name = (try self.brandStructTypeName(info.type_id)) orelse return error.InvalidStructNode;
                defer self.allocator.free(name);
                if (nested != null) {
                    if (bound.erased_parameter) {
                        try writer.print("{s}return .{{ ._reader = {s}.Reader.wrap(try pointer.getStruct()) }};\n", .{ body_indent, name });
                    } else {
                        try writer.print("{s}return .{{ ._reader = pointer }};\n", .{body_indent});
                    }
                } else {
                    try writer.print("{s}return {s}.Reader.wrap(try pointer.getStruct());\n", .{ body_indent, name });
                }
            },
            .list => try self.writeBrandListReaderReturn(bound, body_indent, writer),
            .interface => try writer.print("{s}return try pointer.getCapability();\n", .{body_indent}),
            .any_pointer => switch (expression.metadata.any_pointer) {
                .unconstrained => |kind| switch (kind) {
                    .any_kind => try writer.print("{s}return pointer;\n", .{body_indent}),
                    .@"struct" => try writer.print("{s}return try pointer.getStruct();\n", .{body_indent}),
                    .list => try writer.print("{s}return try message.AnyListReader.wrap(pointer);\n", .{body_indent}),
                    .capability => try writer.print("{s}return try pointer.getCapability();\n", .{body_indent}),
                },
                else => return error.InvalidStructNode,
            },
            else => return error.InvalidStructNode,
        }
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    fn generateNestedBrandReaderWrapper(
        self: *StructGenerator,
        name: []const u8,
        application: *const ConcreteBrand,
        decl_indent: []const u8,
        member_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        const target_name = (try self.brandStructTypeName(application.target.id)) orelse return error.InvalidStructNode;
        defer self.allocator.free(target_name);
        const method_body_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{member_indent});
        defer self.allocator.free(method_body_indent);

        try writer.print("{s}pub const {s} = struct {{\n", .{ decl_indent, name });
        try writer.print("{s}_reader: {s}.Reader,\n\n", .{ member_indent, target_name });
        try writer.print("{s}pub fn raw(self: @This()) {s}.Reader {{\n", .{ member_indent, target_name });
        try writer.print("{s}return self._reader;\n", .{method_body_indent});
        try writer.print("{s}}}\n\n", .{member_indent});
        try self.generateBrandReaderApplicationFields(
            application,
            application.target_info,
            member_indent,
            method_body_indent,
            writer,
        );
        try writer.print("{s}}};\n\n", .{decl_indent});
    }

    fn generateBrandReaderApplicationFields(
        self: *StructGenerator,
        application: *const ConcreteBrand,
        info: schema.StructNode,
        decl_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        for (info.fields) |field| {
            if (field.group != null) {
                try self.generateBrandReaderGroup(application, field, info, decl_indent, body_indent, writer);
                continue;
            }
            const bound = (try self.brandBindingForField(application, field)) orelse continue;
            try self.generateBrandReaderParameterMethod(field, bound, decl_indent, body_indent, writer);
        }
    }

    fn generateBrandReaderGroup(
        self: *StructGenerator,
        application: *const ConcreteBrand,
        field: schema.Field,
        parent_info: schema.StructNode,
        decl_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        const group = field.group orelse return;
        const group_node = self.getNode(group.type_id) orelse return error.InvalidStructNode;
        const group_info = group_node.struct_node orelse return error.InvalidStructNode;
        if (!group_info.is_group) return error.InvalidStructNode;
        const group_name = (try self.brandStructTypeName(group.type_id)) orelse return error.InvalidStructNode;
        defer self.allocator.free(group_name);
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);
        const member_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{decl_indent});
        defer self.allocator.free(member_indent);
        const nested_body_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{member_indent});
        defer self.allocator.free(nested_body_indent);

        try writer.print("{s}pub const {s} = struct {{\n", .{ decl_indent, cap_name });
        try writer.print("{s}_reader: {s}.Reader,\n\n", .{ member_indent, group_name });
        try writer.print("{s}pub fn raw(self: @This()) {s}.Reader {{\n", .{ member_indent, group_name });
        try writer.print("{s}return self._reader;\n", .{nested_body_indent});
        try writer.print("{s}}}\n\n", .{member_indent});
        try self.generateBrandReaderApplicationFields(
            application,
            group_info,
            member_indent,
            nested_body_indent,
            writer,
        );
        try writer.print("{s}}};\n\n", .{decl_indent});

        try writer.print("{s}pub fn get{s}(self: @This()) !@This().{s} {{\n", .{ decl_indent, cap_name, cap_name });
        if (field.discriminant_value != 0xffff and parent_info.discriminant_count > 0) {
            try writer.print("{s}return .{{ ._reader = try self._reader.get{s}() }};\n", .{ body_indent, cap_name });
        } else {
            try writer.print("{s}return .{{ ._reader = self._reader.get{s}() }};\n", .{ body_indent, cap_name });
        }
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    fn writeBrandBuilderFieldGuard(
        field: schema.Field,
        target_info: schema.StructNode,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        if (field.discriminant_value == 0xFFFF or target_info.discriminant_count == 0) return;
        const disc_byte_offset = try discriminantByteOffset(target_info.discriminant_offset);
        try writer.print(
            "{s}if (self._builder._builder.readUnionDiscriminant({}) != {}) return error.WrongUnionMember;\n",
            .{ body_indent, disc_byte_offset, field.discriminant_value },
        );
    }

    fn brandBuilderDefaultName(self: *StructGenerator, field: schema.Field) !?[]const u8 {
        const slot = field.slot orelse return null;
        const value = slot.default_value orelse return null;
        const bytes = self.defaultPointerBytes(value) orelse return null;
        _ = bytes;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const name = try std.fmt.allocPrint(self.allocator, "_brand_default_{s}", .{zig_name});
        return @as(?[]const u8, name);
    }

    fn generateBrandBuilderDefault(
        self: *StructGenerator,
        field: schema.Field,
        default_name: []const u8,
        decl_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        const slot = field.slot orelse return error.InvalidStructNode;
        const value = slot.default_value orelse return error.InvalidStructNode;
        const bytes = self.defaultPointerBytes(value) orelse return error.InvalidStructNode;
        try writer.print("{s}const {s}_bytes = ", .{ decl_indent, default_name });
        try self.writeByteArrayInitializer(writer, bytes);
        try writer.writeAll(";\n");
        try writer.print("{s}const {s}_segments = [_][]const u8{{ {s}_bytes[0..] }};\n", .{ decl_indent, default_name, default_name });
        try writer.print(
            "{s}const {s}_message = message.Message{{ .allocator = std.heap.page_allocator, .segments = {s}_segments[0..], .backing_data = null, .segments_owned = false }};\n\n",
            .{ decl_indent, default_name, default_name },
        );
    }

    fn writeBrandBuilderDefaultMaterialization(
        slot: schema.FieldSlot,
        default_name: ?[]const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        const name = default_name orelse return;
        try writer.print("{s}if (self._builder._builder.isPointerNull({})) {{\n", .{ body_indent, slot.offset });
        try writer.print("{s}    const source = try @This().{s}_message.getRootAnyPointer();\n", .{ body_indent, name });
        try writer.print("{s}    const destination = try self._builder._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
        try writer.print("{s}    try message.cloneAnyPointer(source, destination);\n", .{body_indent});
        try writer.print("{s}}}\n", .{body_indent});
    }

    fn generateBrandBuilderParameterMethods(
        self: *StructGenerator,
        field: schema.Field,
        target_info: schema.StructNode,
        bound: BoundExpression,
        decl_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        const expression = (try resolvedCursor(bound)).expression;
        const slot = field.slot orelse return;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);
        const default_name = try self.brandBuilderDefaultName(field);
        defer if (default_name) |name| self.allocator.free(name);
        const nested = try self.nestedConcreteBrand(bound);
        const list_application = try self.listConcreteBrandTerminal(bound);
        if (nested) |application| {
            try self.generateNestedBrandBuilderWrapper(cap_name, &application, decl_indent, body_indent, writer);
        }
        const list_application_name: ?[]const u8 = if (list_application != null) cap_name else null;
        const list_application_ref = if (list_application_name) |name|
            try std.fmt.allocPrint(self.allocator, "@This().{s}", .{name})
        else
            null;
        defer if (list_application_ref) |name| self.allocator.free(name);
        if (list_application) |application| {
            const application_name = list_application_name orelse return error.InvalidStructNode;
            try self.generateBrandedListApplicationAdapter(
                application_name,
                &application,
                decl_indent,
                body_indent,
                writer,
            );
        }
        if (default_name) |name| try self.generateBrandBuilderDefault(field, name, decl_indent, writer);

        switch (expression.type) {
            .text, .data => {
                try writer.print("{s}pub fn set{s}(self: *@This(), value: []const u8) !void {{\n", .{ decl_indent, cap_name });
                try writer.print("{s}const pointer = try self._builder.init{s}();\n", .{ body_indent, cap_name });
                try writer.print("{s}return pointer.set{s}(value);\n", .{ body_indent, if (expression.type == .text) "Text" else "Data" });
                try writer.print("{s}}}\n\n", .{decl_indent});
            },
            .@"struct" => |info| {
                const name = (try self.brandStructTypeName(info.type_id)) orelse return error.InvalidStructNode;
                defer self.allocator.free(name);
                const layout = self.structLayout(info.type_id) orelse return error.InvalidStructNode;
                const return_type = if (nested != null)
                    try std.fmt.allocPrint(self.allocator, "@This().{s}", .{cap_name})
                else
                    try std.fmt.allocPrint(self.allocator, "{s}.Builder", .{name});
                defer self.allocator.free(return_type);

                try writer.print("{s}pub fn get{s}(self: *@This()) !{s} {{\n", .{ decl_indent, cap_name, return_type });
                try writeBrandBuilderFieldGuard(field, target_info, body_indent, writer);
                try writeBrandBuilderDefaultMaterialization(slot, default_name, body_indent, writer);
                if (default_name == null) {
                    try writer.print("{s}if (self._builder._builder.isPointerNull({})) {{\n", .{ body_indent, slot.offset });
                    try writer.print("{s}    const destination = try self._builder._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
                    try writer.print("{s}    _ = try destination.initStruct({}, {});\n", .{ body_indent, layout.data_words, layout.pointer_words });
                    try writer.print("{s}}}\n", .{body_indent});
                }
                try writer.print("{s}const pointer = try self._builder._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
                if (nested != null) {
                    try writer.print("{s}return .{{ ._builder = {s}.Builder.wrap(try pointer.getStruct()) }};\n", .{ body_indent, name });
                } else {
                    try writer.print("{s}return {s}.Builder.wrap(try pointer.getStruct());\n", .{ body_indent, name });
                }
                try writer.print("{s}}}\n\n", .{decl_indent});

                try writer.print("{s}pub fn init{s}(self: *@This()) !{s} {{\n", .{ decl_indent, cap_name, return_type });
                if (nested != null) {
                    if (bound.erased_parameter) {
                        try writer.print("{s}const pointer = try self._builder.init{s}();\n", .{ body_indent, cap_name });
                        try writer.print("{s}return .{{ ._builder = {s}.Builder.wrap(try pointer.initStruct({}, {})) }};\n", .{ body_indent, name, layout.data_words, layout.pointer_words });
                    } else {
                        try writer.print("{s}return .{{ ._builder = try self._builder.init{s}() }};\n", .{ body_indent, cap_name });
                    }
                } else {
                    try writer.print("{s}const pointer = try self._builder.init{s}();\n", .{ body_indent, cap_name });
                    try writer.print("{s}return {s}.Builder.wrap(try pointer.initStruct({}, {}));\n", .{ body_indent, name, layout.data_words, layout.pointer_words });
                }
                try writer.print("{s}}}\n\n", .{decl_indent});
            },
            .list => {
                const builder_type = if (list_application) |*application| blk: {
                    const application_ref = list_application_ref orelse return error.InvalidStructNode;
                    break :blk try self.brandedApplicationListTypeString(
                        bound,
                        application_ref,
                        application,
                        true,
                    );
                } else try self.brandListBuilderTypeString(bound);
                defer self.allocator.free(builder_type);
                try writer.print("{s}pub fn get{s}(self: *@This()) !{s} {{\n", .{ decl_indent, cap_name, builder_type });
                try writeBrandBuilderFieldGuard(field, target_info, body_indent, writer);
                try writeBrandBuilderDefaultMaterialization(slot, default_name, body_indent, writer);
                try writer.print("{s}const pointer = try self._builder._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
                try self.writeBrandListBuilderGetReturn(bound, body_indent, writer);
                try writer.print("{s}}}\n\n", .{decl_indent});

                try writer.print("{s}pub fn init{s}(self: *@This(), element_count: u32) !{s} {{\n", .{ decl_indent, cap_name, builder_type });
                try writer.print("{s}const pointer = try self._builder.init{s}();\n", .{ body_indent, cap_name });
                try self.writeBrandListBuilderInitReturn(bound, body_indent, writer);
                try writer.print("{s}}}\n\n", .{decl_indent});
            },
            .interface => {
                try writer.print("{s}pub fn get{s}(self: *@This()) !message.Capability {{\n", .{ decl_indent, cap_name });
                try writeBrandBuilderFieldGuard(field, target_info, body_indent, writer);
                try writeBrandBuilderDefaultMaterialization(slot, default_name, body_indent, writer);
                try writer.print("{s}const pointer = try self._builder._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
                try writer.print("{s}return try pointer.getCapability();\n", .{body_indent});
                try writer.print("{s}}}\n\n", .{decl_indent});
                try writer.print("{s}pub fn set{s}(self: *@This(), value: message.Capability) !void {{\n", .{ decl_indent, cap_name });
                try writer.print("{s}const pointer = try self._builder.init{s}();\n", .{ body_indent, cap_name });
                try writer.print("{s}return pointer.setCapability(value);\n", .{body_indent});
                try writer.print("{s}}}\n\n", .{decl_indent});
            },
            .any_pointer => switch (expression.metadata.any_pointer) {
                .unconstrained => |kind| switch (kind) {
                    .any_kind => {
                        try writer.print("{s}pub fn get{s}(self: *@This()) !message.AnyPointerBuilder {{\n", .{ decl_indent, cap_name });
                        try writeBrandBuilderFieldGuard(field, target_info, body_indent, writer);
                        try writeBrandBuilderDefaultMaterialization(slot, default_name, body_indent, writer);
                        try writer.print("{s}return self._builder._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
                        try writer.print("{s}}}\n\n", .{decl_indent});
                    },
                    .@"struct", .list => {
                        const wrapper = if (kind == .@"struct") "message.AnyStructBuilder" else "message.AnyListBuilder";
                        try writer.print("{s}pub fn get{s}(self: *@This()) !{s} {{\n", .{ decl_indent, cap_name, wrapper });
                        try writeBrandBuilderFieldGuard(field, target_info, body_indent, writer);
                        try writeBrandBuilderDefaultMaterialization(slot, default_name, body_indent, writer);
                        try writer.print("{s}const pointer = try self._builder._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
                        try writer.print("{s}return try {s}.wrap(pointer);\n", .{ body_indent, wrapper });
                        try writer.print("{s}}}\n\n", .{decl_indent});
                    },
                    .capability => {
                        try writer.print("{s}pub fn get{s}(self: *@This()) !message.Capability {{\n", .{ decl_indent, cap_name });
                        try writeBrandBuilderFieldGuard(field, target_info, body_indent, writer);
                        try writeBrandBuilderDefaultMaterialization(slot, default_name, body_indent, writer);
                        try writer.print("{s}const pointer = try self._builder._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
                        try writer.print("{s}return try pointer.getCapability();\n", .{body_indent});
                        try writer.print("{s}}}\n\n", .{decl_indent});
                        try writer.print("{s}pub fn set{s}(self: *@This(), value: message.Capability) !void {{\n", .{ decl_indent, cap_name });
                        try writer.print("{s}const pointer = try self._builder.init{s}();\n", .{ body_indent, cap_name });
                        try writer.print("{s}return pointer.setCapability(value);\n", .{body_indent});
                        try writer.print("{s}}}\n\n", .{decl_indent});
                    },
                },
                else => return error.InvalidStructNode,
            },
            else => return error.InvalidStructNode,
        }
    }

    fn generateNestedBrandBuilderWrapper(
        self: *StructGenerator,
        name: []const u8,
        application: *const ConcreteBrand,
        decl_indent: []const u8,
        member_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        const target_name = (try self.brandStructTypeName(application.target.id)) orelse return error.InvalidStructNode;
        defer self.allocator.free(target_name);
        const method_body_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{member_indent});
        defer self.allocator.free(method_body_indent);

        try writer.print("{s}pub const {s} = struct {{\n", .{ decl_indent, name });
        try writer.print("{s}_builder: {s}.Builder,\n\n", .{ member_indent, target_name });
        try writer.print("{s}pub fn raw(self: @This()) {s}.Builder {{\n", .{ member_indent, target_name });
        try writer.print("{s}return self._builder;\n", .{method_body_indent});
        try writer.print("{s}}}\n\n", .{member_indent});
        try self.generateBrandBuilderApplicationFields(
            application,
            application.target_info,
            member_indent,
            method_body_indent,
            writer,
        );
        try writer.print("{s}}};\n\n", .{decl_indent});
    }

    fn generateBrandBuilderApplicationFields(
        self: *StructGenerator,
        application: *const ConcreteBrand,
        info: schema.StructNode,
        decl_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        for (info.fields) |field| {
            if (field.group != null) {
                try self.generateBrandBuilderGroup(application, field, info, decl_indent, body_indent, writer);
                continue;
            }
            const bound = (try self.brandBindingForField(application, field)) orelse continue;
            try self.generateBrandBuilderParameterMethods(field, info, bound, decl_indent, body_indent, writer);
        }
    }

    fn generateBrandBuilderGroup(
        self: *StructGenerator,
        application: *const ConcreteBrand,
        field: schema.Field,
        parent_info: schema.StructNode,
        decl_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        const group = field.group orelse return;
        const group_node = self.getNode(group.type_id) orelse return error.InvalidStructNode;
        const group_info = group_node.struct_node orelse return error.InvalidStructNode;
        if (!group_info.is_group) return error.InvalidStructNode;
        const group_name = (try self.brandStructTypeName(group.type_id)) orelse return error.InvalidStructNode;
        defer self.allocator.free(group_name);
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);
        const member_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{decl_indent});
        defer self.allocator.free(member_indent);
        const nested_body_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{member_indent});
        defer self.allocator.free(nested_body_indent);

        try writer.print("{s}pub const {s} = struct {{\n", .{ decl_indent, cap_name });
        try writer.print("{s}_builder: {s}.Builder,\n\n", .{ member_indent, group_name });
        try writer.print("{s}pub fn raw(self: @This()) {s}.Builder {{\n", .{ member_indent, group_name });
        try writer.print("{s}return self._builder;\n", .{nested_body_indent});
        try writer.print("{s}}}\n\n", .{member_indent});
        try self.generateBrandBuilderApplicationFields(
            application,
            group_info,
            member_indent,
            nested_body_indent,
            writer,
        );
        try writer.print("{s}}};\n\n", .{decl_indent});

        if (field.discriminant_value != 0xffff and parent_info.discriminant_count > 0) {
            const disc_byte_offset = try discriminantByteOffset(parent_info.discriminant_offset);
            try writer.print("{s}pub fn get{s}(self: *@This()) !@This().{s} {{\n", .{ decl_indent, cap_name, cap_name });
            try writer.print(
                "{s}if (self._builder._builder.readUnionDiscriminant({}) != {}) return error.WrongUnionMember;\n",
                .{ body_indent, disc_byte_offset, field.discriminant_value },
            );
            try writer.print("{s}return .{{ ._builder = {s}.Builder.wrap(self._builder._builder) }};\n", .{ body_indent, group_name });
            try writer.print("{s}}}\n\n", .{decl_indent});
            try writer.print("{s}pub fn init{s}(self: *@This()) !@This().{s} {{\n", .{ decl_indent, cap_name, cap_name });
            try writer.print("{s}return .{{ ._builder = self._builder.init{s}() }};\n", .{ body_indent, cap_name });
            try writer.print("{s}}}\n\n", .{decl_indent});
        } else {
            try writer.print("{s}pub fn get{s}(self: *@This()) !@This().{s} {{\n", .{ decl_indent, cap_name, cap_name });
            try writer.print("{s}return .{{ ._builder = self._builder.get{s}() }};\n", .{ body_indent, cap_name });
            try writer.print("{s}}}\n\n", .{decl_indent});
        }
    }

    /// A generic struct used as a list terminal needs the shape expected by
    /// `StructListReader/Builder`: a namespace containing application-aware
    /// Reader and Builder wrappers. The wrappers recursively expose any
    /// concrete brands used by the terminal application's own fields.
    fn generateBrandedListApplicationAdapter(
        self: *StructGenerator,
        name: []const u8,
        application: *const ConcreteBrand,
        decl_indent: []const u8,
        member_indent: []const u8,
        writer: anytype,
    ) BrandRenderError!void {
        const target_name = (try self.brandStructTypeName(application.target.id)) orelse return error.InvalidStructNode;
        defer self.allocator.free(target_name);
        const nested_member_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{member_indent});
        defer self.allocator.free(nested_member_indent);
        const nested_body_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{nested_member_indent});
        defer self.allocator.free(nested_body_indent);

        try writer.print("{s}pub const {s} = struct {{\n", .{ decl_indent, name });

        try writer.print("{s}pub const Reader = struct {{\n", .{member_indent});
        try writer.print("{s}_reader: {s}.Reader,\n\n", .{ nested_member_indent, target_name });
        try writer.print("{s}pub fn wrap(reader: message.StructReader) @This() {{\n", .{nested_member_indent});
        try writer.print("{s}return .{{ ._reader = {s}.Reader.wrap(reader) }};\n", .{ nested_body_indent, target_name });
        try writer.print("{s}}}\n\n", .{nested_member_indent});
        try writer.print("{s}pub fn raw(self: @This()) {s}.Reader {{\n", .{ nested_member_indent, target_name });
        try writer.print("{s}return self._reader;\n", .{nested_body_indent});
        try writer.print("{s}}}\n\n", .{nested_member_indent});
        try self.generateBrandReaderApplicationFields(
            application,
            application.target_info,
            nested_member_indent,
            nested_body_indent,
            writer,
        );
        try writer.print("{s}}};\n\n", .{member_indent});

        try writer.print("{s}pub const Builder = struct {{\n", .{member_indent});
        try writer.print("{s}_builder: {s}.Builder,\n\n", .{ nested_member_indent, target_name });
        try writer.print("{s}pub fn wrap(builder: message.StructBuilder) @This() {{\n", .{nested_member_indent});
        try writer.print("{s}return .{{ ._builder = {s}.Builder.wrap(builder) }};\n", .{ nested_body_indent, target_name });
        try writer.print("{s}}}\n\n", .{nested_member_indent});
        try writer.print("{s}pub fn raw(self: @This()) {s}.Builder {{\n", .{ nested_member_indent, target_name });
        try writer.print("{s}return self._builder;\n", .{nested_body_indent});
        try writer.print("{s}}}\n\n", .{nested_member_indent});
        try self.generateBrandBuilderApplicationFields(
            application,
            application.target_info,
            nested_member_indent,
            nested_body_indent,
            writer,
        );
        try writer.print("{s}}};\n", .{member_indent});
        try writer.print("{s}}};\n\n", .{decl_indent});
    }

    fn generateBrandsReaderView(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        decl_indent: []const u8,
        member_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        if (!try self.hasDirectConcreteBrandSlot(struct_info)) return;
        const previous_brand_emission = self.in_brand_emission;
        self.in_brand_emission = true;
        defer self.in_brand_emission = previous_brand_emission;
        const wrapper_body_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{body_indent});
        defer self.allocator.free(wrapper_body_indent);

        try writer.print("{s}pub const Brands = struct {{\n", .{decl_indent});
        try writer.print("{s}_reader: message.StructReader,\n\n", .{member_indent});
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            const brand = (try self.concreteBrand(slot)) orelse continue;
            const target_info = brand.target_info;
            const target_name = (try self.brandStructTypeName(brand.target.id)) orelse continue;
            defer self.allocator.free(target_name);
            const zig_name = try self.type_gen.toZigIdentifier(field.name);
            defer self.allocator.free(zig_name);
            const cap_name = try self.capitalizeFirst(zig_name);
            defer self.allocator.free(cap_name);

            try writer.print("{s}pub const {s} = struct {{\n", .{ member_indent, cap_name });
            try writer.print("{s}_reader: {s}.Reader,\n\n", .{ body_indent, target_name });
            try writer.print("{s}pub fn raw(self: @This()) {s}.Reader {{\n", .{ body_indent, target_name });
            try writer.print("{s}return self._reader;\n", .{wrapper_body_indent});
            try writer.print("{s}}}\n\n", .{body_indent});
            try self.generateBrandReaderApplicationFields(
                &brand,
                target_info,
                body_indent,
                wrapper_body_indent,
                writer,
            );
            try writer.print("{s}}};\n\n", .{member_indent});

            try writer.print("{s}pub fn get{s}(self: @This()) !@This().{s} {{\n", .{ member_indent, cap_name, cap_name });
            try self.writeNestedListUnionGuard(field, struct_info, body_indent, writer);
            if (try self.pointerDefaultConstName(field, slot)) |const_name| {
                defer self.allocator.free(const_name);
                try writer.print("{s}if (self._reader.isPointerNull({})) {{\n", .{ body_indent, slot.offset });
                try writer.print("{s}    const raw = try {s}();\n", .{ body_indent, const_name });
                try writer.print("{s}    return .{{ ._reader = {s}.Reader.wrap(raw) }};\n", .{ body_indent, target_name });
                try writer.print("{s}}}\n", .{body_indent});
            } else {
                try writer.print("{s}if (self._reader.isPointerNull({})) return .{{ ._reader = {s}.Reader.wrap(self._reader.emptyStruct()) }};\n", .{ body_indent, slot.offset, target_name });
            }
            try writer.print("{s}return .{{ ._reader = {s}.Reader.wrap(try self._reader.readStruct({})) }};\n", .{ body_indent, target_name, slot.offset });
            try writer.print("{s}}}\n\n", .{member_indent});
        }
        try writer.print("{s}}};\n\n", .{decl_indent});
        try writer.print("{s}pub fn brands(self: @This()) Brands {{\n", .{decl_indent});
        try writer.print("{s}return .{{ ._reader = self._reader }};\n", .{member_indent});
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    fn generateBrandsBuilderView(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        decl_indent: []const u8,
        member_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        if (!try self.hasDirectConcreteBrandSlot(struct_info)) return;
        const previous_brand_emission = self.in_brand_emission;
        self.in_brand_emission = true;
        defer self.in_brand_emission = previous_brand_emission;
        const wrapper_body_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{body_indent});
        defer self.allocator.free(wrapper_body_indent);

        try writer.print("{s}pub const Brands = struct {{\n", .{decl_indent});
        try writer.print("{s}_builder: message.StructBuilder,\n\n", .{member_indent});
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            const brand = (try self.concreteBrand(slot)) orelse continue;
            const target_info = brand.target_info;
            const target_name = (try self.brandStructTypeName(brand.target.id)) orelse continue;
            defer self.allocator.free(target_name);
            const target_layout = self.structLayout(brand.target.id) orelse continue;
            const zig_name = try self.type_gen.toZigIdentifier(field.name);
            defer self.allocator.free(zig_name);
            const cap_name = try self.capitalizeFirst(zig_name);
            defer self.allocator.free(cap_name);

            if (try self.pointerDefaultConstName(field, slot)) |const_name| {
                defer self.allocator.free(const_name);
                const default_value = slot.default_value orelse return error.InvalidStructNode;
                const default_bytes = self.defaultPointerBytes(default_value) orelse return error.InvalidStructNode;
                try writer.print("{s}const {s}_bytes = ", .{ member_indent, const_name });
                try self.writeByteArrayInitializer(writer, default_bytes);
                try writer.writeAll(";\n");
                try writer.print("{s}const {s}_segments = [_][]const u8{{ {s}_bytes[0..] }};\n", .{ member_indent, const_name, const_name });
                try writer.print(
                    "{s}const {s}_message = message.Message{{ .allocator = std.heap.page_allocator, .segments = {s}_segments[0..], .backing_data = null, .segments_owned = false }};\n\n",
                    .{ member_indent, const_name, const_name },
                );
            }

            try writer.print("{s}pub const {s} = struct {{\n", .{ member_indent, cap_name });
            try writer.print("{s}_builder: {s}.Builder,\n\n", .{ body_indent, target_name });
            try writer.print("{s}pub fn raw(self: @This()) {s}.Builder {{\n", .{ body_indent, target_name });
            try writer.print("{s}return self._builder;\n", .{wrapper_body_indent});
            try writer.print("{s}}}\n\n", .{body_indent});
            try self.generateBrandBuilderApplicationFields(
                &brand,
                target_info,
                body_indent,
                wrapper_body_indent,
                writer,
            );
            try writer.print("{s}}};\n\n", .{member_indent});

            try writer.print("{s}pub fn get{s}(self: @This()) !@This().{s} {{\n", .{ member_indent, cap_name, cap_name });
            if (field.discriminant_value != 0xFFFF and struct_info.discriminant_count > 0) {
                const disc_byte_offset = try discriminantByteOffset(struct_info.discriminant_offset);
                try writer.print(
                    "{s}if (self._builder.readUnionDiscriminant({}) != {}) return error.WrongUnionMember;\n",
                    .{ body_indent, disc_byte_offset, field.discriminant_value },
                );
            }
            if (try self.pointerDefaultConstName(field, slot)) |const_name| {
                defer self.allocator.free(const_name);
                try writer.print("{s}if (self._builder.isPointerNull({})) {{\n", .{ body_indent, slot.offset });
                try writer.print("{s}    const source = try @This().{s}_message.getRootAnyPointer();\n", .{ body_indent, const_name });
                try writer.print("{s}    const destination = try self._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
                try writer.print("{s}    try message.cloneAnyPointer(source, destination);\n", .{body_indent});
                try writer.print("{s}}}\n", .{body_indent});
            } else {
                try writer.print("{s}if (self._builder.isPointerNull({})) {{\n", .{ body_indent, slot.offset });
                try writer.print("{s}    _ = try self._builder.initStruct({}, {}, {});\n", .{ body_indent, slot.offset, target_layout.data_words, target_layout.pointer_words });
                try writer.print("{s}}}\n", .{body_indent});
            }
            try writer.print("{s}const raw = try self._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
            try writer.print("{s}return .{{ ._builder = {s}.Builder.wrap(try raw.getStruct()) }};\n", .{ body_indent, target_name });
            try writer.print("{s}}}\n\n", .{member_indent});

            try writer.print("{s}pub fn init{s}(self: @This()) !@This().{s} {{\n", .{ member_indent, cap_name, cap_name });
            try self.writeOrdinalUnionDiscriminant(field, struct_info, body_indent, writer);
            try writer.print("{s}const raw = try self._builder.initStruct({}, {}, {});\n", .{ body_indent, slot.offset, target_layout.data_words, target_layout.pointer_words });
            try writer.print("{s}return .{{ ._builder = {s}.Builder.wrap(raw) }};\n", .{ body_indent, target_name });
            try writer.print("{s}}}\n\n", .{member_indent});
        }
        try writer.print("{s}}};\n\n", .{decl_indent});
        try writer.print("{s}pub fn brands(self: @This()) Brands {{\n", .{decl_indent});
        try writer.print("{s}return .{{ ._builder = self._builder }};\n", .{member_indent});
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    const PointerKind = enum {
        @"struct",
        list,
        capability,
    };

    fn pointerKind(slot: schema.FieldSlot) ?PointerKind {
        if (slot.type != .any_pointer) return null;
        return switch (slot.type_metadata) {
            .any_pointer => |metadata| switch (metadata) {
                .unconstrained => |kind| switch (kind) {
                    .any_kind => null,
                    .@"struct" => .@"struct",
                    .list => .list,
                    .capability => .capability,
                },
                else => null,
            },
            else => null,
        };
    }

    fn hasDirectPointerKindSlot(struct_info: schema.StructNode) bool {
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (pointerKind(slot) != null) return true;
        }
        return false;
    }

    fn generatePointerKindsReaderView(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        decl_indent: []const u8,
        member_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        if (!hasDirectPointerKindSlot(struct_info)) return;

        try writer.print("{s}pub const PointerKinds = struct {{\n", .{decl_indent});
        try writer.print("{s}_reader: message.StructReader,\n\n", .{member_indent});
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            const kind = pointerKind(slot) orelse continue;
            const zig_name = try self.type_gen.toZigIdentifier(field.name);
            defer self.allocator.free(zig_name);
            const cap_name = try self.capitalizeFirst(zig_name);
            defer self.allocator.free(cap_name);
            const return_type: []const u8 = switch (kind) {
                .@"struct" => "message.StructReader",
                .list => "message.AnyListReader",
                .capability => "message.Capability",
            };

            try writer.print("{s}pub fn get{s}(self: @This()) !{s} {{\n", .{ member_indent, cap_name, return_type });
            try self.writeNestedListUnionGuard(field, struct_info, body_indent, writer);
            if (try self.pointerDefaultConstName(field, slot)) |const_name| {
                defer self.allocator.free(const_name);
                try writer.print("{s}if (self._reader.isPointerNull({})) {{\n", .{ body_indent, slot.offset });
                try writer.print("{s}    const value = try {s}();\n", .{ body_indent, const_name });
                switch (kind) {
                    .@"struct" => try writer.print("{s}    return try value.getStruct();\n", .{body_indent}),
                    .list => try writer.print("{s}    return try message.AnyListReader.wrap(value);\n", .{body_indent}),
                    .capability => try writer.print("{s}    return try value.getCapability();\n", .{body_indent}),
                }
                try writer.print("{s}}}\n", .{body_indent});
            } else if (kind == .@"struct") {
                try writer.print("{s}if (self._reader.isPointerNull({})) return self._reader.emptyStruct();\n", .{ body_indent, slot.offset });
            }

            switch (kind) {
                .@"struct" => try writer.print("{s}return try self._reader.readStruct({});\n", .{ body_indent, slot.offset }),
                .list => {
                    try writer.print("{s}const value = try self._reader.readAnyPointer({});\n", .{ body_indent, slot.offset });
                    try writer.print("{s}return try message.AnyListReader.wrap(value);\n", .{body_indent});
                },
                .capability => try writer.print("{s}return try self._reader.readCapability({});\n", .{ body_indent, slot.offset }),
            }
            try writer.print("{s}}}\n\n", .{member_indent});
        }
        try writer.print("{s}}};\n\n", .{decl_indent});
        try writer.print("{s}pub fn pointerKinds(self: @This()) PointerKinds {{\n", .{decl_indent});
        try writer.print("{s}return .{{ ._reader = self._reader }};\n", .{member_indent});
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    fn generatePointerKindsBuilderView(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        decl_indent: []const u8,
        member_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        if (!hasDirectPointerKindSlot(struct_info)) return;

        try writer.print("{s}pub const PointerKinds = struct {{\n", .{decl_indent});
        try writer.print("{s}_builder: message.StructBuilder,\n\n", .{member_indent});
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            const kind = pointerKind(slot) orelse continue;
            const zig_name = try self.type_gen.toZigIdentifier(field.name);
            defer self.allocator.free(zig_name);
            const cap_name = try self.capitalizeFirst(zig_name);
            defer self.allocator.free(cap_name);

            const builder_type: []const u8 = switch (kind) {
                .@"struct" => "message.AnyStructBuilder",
                .list => "message.AnyListBuilder",
                .capability => "message.CapabilityBuilder",
            };
            try writer.print("{s}pub fn get{s}(self: @This()) !{s} {{\n", .{ member_indent, cap_name, builder_type });
            if (field.discriminant_value != 0xFFFF and struct_info.discriminant_count > 0) {
                const disc_byte_offset = try discriminantByteOffset(struct_info.discriminant_offset);
                try writer.print(
                    "{s}if (self._builder.readUnionDiscriminant({}) != {}) return error.WrongUnionMember;\n",
                    .{ body_indent, disc_byte_offset, field.discriminant_value },
                );
            }
            try writer.print("{s}const slot_builder = try self._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
            try writer.print("{s}return try {s}.wrap(slot_builder);\n", .{ body_indent, builder_type });
            try writer.print("{s}}}\n\n", .{member_indent});

            switch (kind) {
                .@"struct" => try writer.print("{s}pub fn init{s}(self: @This()) !message.AnyStructBuilder {{\n", .{ member_indent, cap_name }),
                .list => try writer.print("{s}pub fn init{s}(self: @This()) !message.AnyListBuilder {{\n", .{ member_indent, cap_name }),
                .capability => try writer.print("{s}pub fn set{s}(self: @This(), value: message.Capability) !void {{\n", .{ member_indent, cap_name }),
            }
            try self.writeOrdinalUnionDiscriminant(field, struct_info, body_indent, writer);
            try writer.print("{s}const slot_builder = try self._builder.getAnyPointer({});\n", .{ body_indent, slot.offset });
            switch (kind) {
                .@"struct" => try writer.print("{s}return .{{ ._builder = slot_builder }};\n", .{body_indent}),
                .list => try writer.print("{s}return .{{ ._builder = slot_builder }};\n", .{body_indent}),
                .capability => try writer.print("{s}return slot_builder.setCapability(value);\n", .{body_indent}),
            }
            try writer.print("{s}}}\n\n", .{member_indent});
        }
        try writer.print("{s}}};\n\n", .{decl_indent});
        try writer.print("{s}pub fn pointerKinds(self: @This()) PointerKinds {{\n", .{decl_indent});
        try writer.print("{s}return .{{ ._builder = self._builder }};\n", .{member_indent});
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    fn isNestedListSlot(field: schema.Field) bool {
        const slot = field.slot orelse return false;
        return slot.type == .list and slot.type.list.element_type.* == .list;
    }

    fn isUnknownStructTerminalList(self: *StructGenerator, list_type: schema.Type) bool {
        if (list_type != .list) return false;
        const element_type = list_type.list.element_type.*;
        if (element_type != .@"struct") return false;
        const info = element_type.@"struct";
        const node = self.getNode(info.type_id) orelse return true;
        return node.kind != .@"struct" or self.structLayout(info.type_id) == null;
    }

    fn writeScalarListCodec(kind: schema.Type, writer: anytype) !void {
        const name: []const u8 = switch (kind) {
            .void => "void",
            .bool => "bool",
            .int8 => "int8",
            .uint8 => "uint8",
            .int16 => "int16",
            .uint16 => "uint16",
            .int32 => "int32",
            .uint32 => "uint32",
            .float32 => "float32",
            .int64 => "int64",
            .uint64 => "uint64",
            .float64 => "float64",
            .text => "text",
            else => return error.InvalidStructNode,
        };
        try writer.print("message.typed_list_helpers.ScalarListCodec(.{s})", .{name});
    }

    fn writeNestedBaseCodec(self: *StructGenerator, element_type: schema.Type, writer: anytype) !void {
        switch (element_type) {
            .void,
            .bool,
            .int8,
            .uint8,
            .int16,
            .uint16,
            .int32,
            .uint32,
            .float32,
            .int64,
            .uint64,
            .float64,
            .text,
            => try writeScalarListCodec(element_type, writer),
            .data => try writer.writeAll("message.typed_list_helpers.DataListCodec"),
            .interface => try writer.writeAll("message.typed_list_helpers.CapabilityListCodec"),
            .any_pointer => try writer.writeAll("message.typed_list_helpers.RawPointerListCodec"),
            .@"enum" => |enum_info| {
                if (try self.brandEnumTypeName(enum_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    try writer.print("message.typed_list_helpers.EnumListCodec({s})", .{name});
                } else {
                    try writer.writeAll("message.typed_list_helpers.ScalarListCodec(.uint16)");
                }
            },
            .@"struct" => |struct_info| {
                const struct_name = try self.brandStructTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                if (struct_name) |name| {
                    if (self.structLayout(struct_info.type_id)) |layout| {
                        try writer.print(
                            "message.typed_list_helpers.StructListCodec({s}, {}, {})",
                            .{ name, layout.data_words, layout.pointer_words },
                        );
                        return;
                    }
                }
                try writer.writeAll("message.typed_list_helpers.RawStructListReaderCodec");
            },
            else => try writer.writeAll("message.typed_list_helpers.RawPointerListCodec"),
        }
    }

    /// Write the codec for a list value nested inside another list. `list_type`
    /// is always the schema type of that inner list.
    fn writeNestedReaderCodec(self: *StructGenerator, list_type: schema.Type, writer: anytype) !void {
        if (list_type != .list) return error.InvalidStructNode;
        const element_type = list_type.list.element_type.*;
        if (element_type != .list) return self.writeNestedBaseCodec(element_type, writer);

        try writer.writeAll("message.typed_list_helpers.NestedReaderCodec(");
        try self.writeNestedReaderCodec(element_type, writer);
        try writer.writeAll(")");
    }

    fn writeNestedBuilderCodec(self: *StructGenerator, list_type: schema.Type, writer: anytype) !void {
        if (list_type != .list) return error.InvalidStructNode;
        const element_type = list_type.list.element_type.*;
        if (element_type != .list) return self.writeNestedBaseCodec(element_type, writer);

        if (self.isUnknownStructTerminalList(element_type)) {
            try writer.writeAll("message.typed_list_helpers.RawStructNestedBuilderCodec");
            return;
        }

        try writer.writeAll("message.typed_list_helpers.NestedBuilderCodec(");
        try self.writeNestedBuilderCodec(element_type, writer);
        try writer.writeAll(")");
    }

    fn nestedListReaderTypeString(self: *StructGenerator, inner_list_type: schema.Type) ![]const u8 {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);
        const out_writer = ArrayListWriter{ .list = &out, .allocator = self.allocator };
        try out_writer.writeAll("message.typed_list_helpers.NestedListReader(");
        try self.writeNestedReaderCodec(inner_list_type, out_writer);
        try out_writer.writeAll(")");
        return out.toOwnedSlice(self.allocator);
    }

    fn nestedListBuilderTypeString(self: *StructGenerator, inner_list_type: schema.Type) ![]const u8 {
        if (self.isUnknownStructTerminalList(inner_list_type)) {
            return self.allocator.dupe(u8, "message.typed_list_helpers.RawStructNestedListBuilder");
        }

        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);
        const out_writer = ArrayListWriter{ .list = &out, .allocator = self.allocator };
        try out_writer.writeAll("message.typed_list_helpers.NestedListBuilder(");
        try self.writeNestedBuilderCodec(inner_list_type, out_writer);
        try out_writer.writeAll(")");
        return out.toOwnedSlice(self.allocator);
    }

    fn generateNestedListsReaderView(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        decl_indent: []const u8,
        member_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        if (!hasDirectNestedListSlot(struct_info)) return;

        try writer.print("{s}pub const NestedLists = struct {{\n", .{decl_indent});
        try writer.print("{s}_reader: message.StructReader,\n\n", .{member_indent});
        for (struct_info.fields) |field| {
            if (!isNestedListSlot(field)) continue;
            const slot = field.slot orelse continue;

            const zig_name = try self.type_gen.toZigIdentifier(field.name);
            defer self.allocator.free(zig_name);
            const cap_name = try self.capitalizeFirst(zig_name);
            defer self.allocator.free(cap_name);
            const return_type = try self.nestedListReaderTypeString(slot.type.list.element_type.*);
            defer self.allocator.free(return_type);

            try writer.print("{s}pub fn get{s}(self: @This()) !{s} {{\n", .{ member_indent, cap_name, return_type });
            try self.writeNestedListUnionGuard(field, struct_info, body_indent, writer);
            if (try self.pointerDefaultConstName(field, slot)) |const_name| {
                defer self.allocator.free(const_name);
                try writer.print("{s}if (self._reader.isPointerNull({})) {{\n", .{ body_indent, slot.offset });
                try writer.print("{s}    const raw = try {s}();\n", .{ body_indent, const_name });
                try writer.print("{s}    return .{{ ._list = raw }};\n", .{body_indent});
                try writer.print("{s}}}\n", .{body_indent});
            } else {
                try writer.print(
                    "{s}if (self._reader.isPointerNull({})) return .{{ ._list = self._reader.emptyList(message.PointerListReader) }};\n",
                    .{ body_indent, slot.offset },
                );
            }
            try writer.print("{s}const raw = try self._reader.readPointerList({});\n", .{ body_indent, slot.offset });
            try writer.print("{s}return .{{ ._list = raw }};\n", .{body_indent});
            try writer.print("{s}}}\n\n", .{member_indent});
        }
        try writer.print("{s}}};\n\n", .{decl_indent});
        try writer.print("{s}pub fn nestedLists(self: @This()) NestedLists {{\n", .{decl_indent});
        try writer.print("{s}return .{{ ._reader = self._reader }};\n", .{member_indent});
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    /// Preserve the legacy typed getter's union error distinction inside the
    /// nested-list view, which stores only the raw StructReader and therefore
    /// cannot call the enclosing Reader's `which()` method directly.
    fn writeNestedListUnionGuard(
        self: *StructGenerator,
        field: schema.Field,
        parent_struct_info: schema.StructNode,
        indent: []const u8,
        writer: anytype,
    ) !void {
        _ = self;
        if (field.discriminant_value == 0xFFFF or parent_struct_info.discriminant_count == 0) return;
        const disc_byte_offset = try discriminantByteOffset(parent_struct_info.discriminant_offset);
        try writer.print(
            "{s}const union_ordinal = self._reader.readUnionDiscriminant({});\n",
            .{ indent, disc_byte_offset },
        );
        try writer.print("{s}if (union_ordinal != {}) {{\n", .{ indent, field.discriminant_value });
        try writer.print("{s}    switch (union_ordinal) {{\n", .{indent});
        for (parent_struct_info.fields) |union_field| {
            if (union_field.discriminant_value == 0xFFFF) continue;
            try writer.print(
                "{s}        {} => return error.WrongUnionMember,\n",
                .{ indent, union_field.discriminant_value },
            );
        }
        try writer.print("{s}        else => return error.InvalidEnumValue,\n", .{indent});
        try writer.print("{s}    }}\n", .{indent});
        try writer.print("{s}}}\n", .{indent});
    }

    fn generateNestedListsBuilderView(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        decl_indent: []const u8,
        member_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        if (!hasDirectNestedListSlot(struct_info)) return;

        try writer.print("{s}pub const NestedLists = struct {{\n", .{decl_indent});
        try writer.print("{s}_builder: message.StructBuilder,\n\n", .{member_indent});
        for (struct_info.fields) |field| {
            if (!isNestedListSlot(field)) continue;
            const slot = field.slot orelse continue;

            const zig_name = try self.type_gen.toZigIdentifier(field.name);
            defer self.allocator.free(zig_name);
            const cap_name = try self.capitalizeFirst(zig_name);
            defer self.allocator.free(cap_name);
            const return_type = try self.nestedListBuilderTypeString(slot.type.list.element_type.*);
            defer self.allocator.free(return_type);

            try writer.print(
                "{s}pub fn init{s}(self: @This(), element_count: u32) !{s} {{\n",
                .{ member_indent, cap_name, return_type },
            );
            try self.writeOrdinalUnionDiscriminant(field, struct_info, body_indent, writer);
            try writer.print("{s}const raw = try self._builder.writePointerList({}, element_count);\n", .{ body_indent, slot.offset });
            try writer.print("{s}return .{{ ._list = raw }};\n", .{body_indent});
            try writer.print("{s}}}\n\n", .{member_indent});

            try writer.print(
                "{s}pub fn init{s}InSegment(self: @This(), element_count: u32, target_segment_id: u32) !{s} {{\n",
                .{ member_indent, cap_name, return_type },
            );
            try self.writeOrdinalUnionDiscriminant(field, struct_info, body_indent, writer);
            try writer.print(
                "{s}const raw = try self._builder.writePointerListInSegment({}, element_count, target_segment_id);\n",
                .{ body_indent, slot.offset },
            );
            try writer.print("{s}return .{{ ._list = raw }};\n", .{body_indent});
            try writer.print("{s}}}\n\n", .{member_indent});
        }
        try writer.print("{s}}};\n\n", .{decl_indent});
        try writer.print("{s}pub fn nestedLists(self: @This()) NestedLists {{\n", .{decl_indent});
        try writer.print("{s}return .{{ ._builder = self._builder }};\n", .{member_indent});
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    fn isPointerSlotType(typ: schema.Type) bool {
        return switch (typ) {
            .text, .data, .list, .@"struct", .any_pointer, .interface => true,
            else => false,
        };
    }

    /// Emit the raw-enum reader view nested in a generated Reader. The view is
    /// intentionally separate from typed getters: typed access remains
    /// exhaustive, while this path can preserve ordinals introduced by a newer
    /// schema version.
    fn generateEnumOrdinalsReaderView(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        decl_indent: []const u8,
        member_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        if (!hasDirectEnumSlot(struct_info)) return;

        try writer.print("{s}pub const EnumOrdinals = struct {{\n", .{decl_indent});
        try writer.print("{s}_reader: message.StructReader,\n\n", .{member_indent});
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (slot.type != .@"enum") continue;

            const zig_name = try self.type_gen.toZigIdentifier(field.name);
            defer self.allocator.free(zig_name);
            const cap_name = try self.capitalizeFirst(zig_name);
            defer self.allocator.free(cap_name);

            try writer.print("{s}pub fn get{s}(self: @This()) !u16 {{\n", .{ member_indent, cap_name });
            try self.writeOrdinalUnionGuard(field, struct_info, "_reader", body_indent, writer);
            try self.writeEnumOrdinalGetterBody(slot, body_indent, writer);
            try writer.print("{s}}}\n\n", .{member_indent});
        }
        try writer.print("{s}}};\n\n", .{decl_indent});
        try writer.print("{s}pub fn enumOrdinals(self: @This()) EnumOrdinals {{\n", .{decl_indent});
        try writer.print("{s}return .{{ ._reader = self._reader }};\n", .{member_indent});
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    /// Emit the raw-enum builder view nested in a generated Builder.
    fn generateEnumOrdinalsBuilderView(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        decl_indent: []const u8,
        member_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        if (!hasDirectEnumSlot(struct_info)) return;

        try writer.print("{s}pub const EnumOrdinals = struct {{\n", .{decl_indent});
        try writer.print("{s}_builder: message.StructBuilder,\n\n", .{member_indent});
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (slot.type != .@"enum") continue;

            const zig_name = try self.type_gen.toZigIdentifier(field.name);
            defer self.allocator.free(zig_name);
            const cap_name = try self.capitalizeFirst(zig_name);
            defer self.allocator.free(cap_name);

            try writer.print("{s}pub fn set{s}(self: @This(), value: u16) !void {{\n", .{ member_indent, cap_name });
            try self.writeOrdinalUnionDiscriminant(field, struct_info, body_indent, writer);
            try self.writeEnumOrdinalSetterBody(slot, body_indent, writer);
            try writer.print("{s}}}\n\n", .{member_indent});
        }
        try writer.print("{s}}};\n\n", .{decl_indent});
        try writer.print("{s}pub fn enumOrdinals(self: @This()) EnumOrdinals {{\n", .{decl_indent});
        try writer.print("{s}return .{{ ._builder = self._builder }};\n", .{member_indent});
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    fn writeEnumOrdinalGetterBody(self: *StructGenerator, slot: schema.FieldSlot, indent: []const u8, writer: anytype) !void {
        const byte_offset = try self.dataByteOffset(slot.type, slot.offset);
        if (slot.default_value) |default_value| {
            if (!self.hasZeroDefaultBits(.uint16, default_value)) {
                if (try self.defaultLiteral(.uint16, default_value)) |literal| {
                    defer self.allocator.free(literal);
                    try writer.print("{s}return self._reader.readU16({}) ^ {s};\n", .{ indent, byte_offset, literal });
                    return;
                }
            }
        }
        try writer.print("{s}return self._reader.readU16({});\n", .{ indent, byte_offset });
    }

    fn writeEnumOrdinalSetterBody(self: *StructGenerator, slot: schema.FieldSlot, indent: []const u8, writer: anytype) !void {
        const byte_offset = try self.dataByteOffset(slot.type, slot.offset);
        if (slot.default_value) |default_value| {
            if (!self.hasZeroDefaultBits(.uint16, default_value)) {
                if (try self.defaultLiteral(.uint16, default_value)) |literal| {
                    defer self.allocator.free(literal);
                    try writer.print("{s}self._builder.writeU16({}, value ^ {s});\n", .{ indent, byte_offset, literal });
                    return;
                }
            }
        }
        try writer.print("{s}self._builder.writeU16({}, value);\n", .{ indent, byte_offset });
    }

    fn writeOrdinalUnionGuard(
        self: *StructGenerator,
        field: schema.Field,
        parent_struct_info: schema.StructNode,
        comptime storage_field: []const u8,
        indent: []const u8,
        writer: anytype,
    ) !void {
        _ = self;
        if (field.discriminant_value == 0xFFFF or parent_struct_info.discriminant_count == 0) return;
        const disc_byte_offset = try discriminantByteOffset(parent_struct_info.discriminant_offset);
        try writer.print(
            "{s}if (self.{s}.readUnionDiscriminant({}) != {}) return error.WrongUnionMember;\n",
            .{ indent, storage_field, disc_byte_offset, field.discriminant_value },
        );
    }

    fn writeOrdinalUnionDiscriminant(
        self: *StructGenerator,
        field: schema.Field,
        parent_struct_info: schema.StructNode,
        indent: []const u8,
        writer: anytype,
    ) !void {
        _ = self;
        if (field.discriminant_value == 0xFFFF or parent_struct_info.discriminant_count == 0) return;
        const disc_byte_offset = try discriminantByteOffset(parent_struct_info.discriminant_offset);
        try writer.print("{s}self._builder.writeU16({}, {});\n", .{ indent, disc_byte_offset, field.discriminant_value });
    }

    fn generatePointerPresenceReader(
        self: *StructGenerator,
        field: schema.Field,
        parent_struct_info: schema.StructNode,
        receiver_type: []const u8,
        decl_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        const slot = field.slot orelse return;
        if (!isPointerSlotType(slot.type)) return;

        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        try writer.print("{s}pub fn has{s}(self: {s}) bool {{\n", .{ decl_indent, cap_name, receiver_type });
        if (field.discriminant_value != 0xFFFF and parent_struct_info.discriminant_count > 0) {
            const disc_byte_offset = try discriminantByteOffset(parent_struct_info.discriminant_offset);
            try writer.print(
                "{s}if (self._reader.readUnionDiscriminant({}) != {}) return false;\n",
                .{ body_indent, disc_byte_offset, field.discriminant_value },
            );
        }
        try writer.print("{s}return !self._reader.isPointerNull({});\n", .{ body_indent, slot.offset });
        try writer.print("{s}}}\n\n", .{decl_indent});
    }

    fn generatePointerPresenceBuilder(
        self: *StructGenerator,
        field: schema.Field,
        parent_struct_info: schema.StructNode,
        receiver_type: []const u8,
        decl_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        const slot = field.slot orelse return;
        if (!isPointerSlotType(slot.type)) return;

        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        try writer.print("{s}pub fn has{s}(self: {s}) bool {{\n", .{ decl_indent, cap_name, receiver_type });
        if (field.discriminant_value != 0xFFFF and parent_struct_info.discriminant_count > 0) {
            const disc_byte_offset = try discriminantByteOffset(parent_struct_info.discriminant_offset);
            try writer.print(
                "{s}if (self._builder.readUnionDiscriminant({}) != {}) return false;\n",
                .{ body_indent, disc_byte_offset, field.discriminant_value },
            );
        }
        try writer.print("{s}return !self._builder.isPointerNull({});\n", .{ body_indent, slot.offset });
        try writer.print("{s}}}\n\n", .{decl_indent});
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

        try self.generatePointerDefaults(struct_info, "        ", "            ", writer);

        if (self.api_profile == .full) {
            try writer.print("        pub fn init(msg: *const message.Message) !{s} {{\n", .{self.reader_ref});
            try writer.writeAll("            const root = try msg.getRootStruct();\n");
            try writer.writeAll("            return .{ ._reader = root };\n");
            try writer.writeAll("        }\n\n");
        }

        try writer.print("        pub fn wrap(reader: message.StructReader) {s} {{\n", .{self.reader_ref});
        try writer.writeAll("            return .{ ._reader = reader };\n");
        try writer.writeAll("        }\n\n");

        try self.generateEnumOrdinalsReaderView(
            struct_info,
            "        ",
            "            ",
            "                ",
            writer,
        );
        try self.generateNestedListsReaderView(
            struct_info,
            "        ",
            "            ",
            "                ",
            writer,
        );
        try self.generateBrandsReaderView(
            struct_info,
            "        ",
            "            ",
            "                ",
            writer,
        );
        try self.generatePointerKindsReaderView(
            struct_info,
            "        ",
            "            ",
            "                ",
            writer,
        );

        // Generate which() method if this struct has a union
        if (struct_info.discriminant_count > 0) {
            const disc_byte_offset = try discriminantByteOffset(struct_info.discriminant_offset);
            try writer.print("        pub fn whichOrdinal(self: {s}) u16 {{\n", .{self.reader_ref});
            try writer.print("            return self._reader.readUnionDiscriminant({});\n", .{disc_byte_offset});
            try writer.writeAll("        }\n\n");
            try writer.print("        pub fn which(self: {s}) error{{InvalidEnumValue}}!{s} {{\n", .{ self.reader_ref, self.whichtag_ref });
            try writer.print("            return std.enums.fromInt({s}, self.whichOrdinal()) orelse return error.InvalidEnumValue;\n", .{self.whichtag_ref});
            try writer.writeAll("        }\n\n");
        }

        // Generate field getters
        for (struct_info.fields) |field| {
            if (field.group != null) {
                try self.generateGroupFieldAccessor(field, struct_info, writer);
            } else {
                try self.generateFieldGetter(field, struct_info, writer);
            }
        }

        try writer.writeAll("    };\n\n");
    }

    /// Emit a discriminant guard at the top of a union member's getter so that
    /// reading a variant that is not currently selected returns
    /// `error.WrongUnionMember` instead of reinterpreting another variant's
    /// bits. `receiver` is `Reader` for a struct's own getters or `@This()` for
    /// a group's inner getters; `indent` is the getter body indentation.
    fn writeUnionMemberGuard(
        self: *StructGenerator,
        field: schema.Field,
        parent_struct_info: schema.StructNode,
        indent: []const u8,
        writer: anytype,
    ) !void {
        if (field.discriminant_value == 0xFFFF or parent_struct_info.discriminant_count == 0) return;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const escaped_name = try types.escapeZigKeyword(self.allocator, zig_name);
        defer self.allocator.free(escaped_name);
        try writer.print("{s}if ((try self.which()) != .{s}) return error.WrongUnionMember;\n", .{ indent, escaped_name });
    }

    fn generateFieldGetter(self: *StructGenerator, field: schema.Field, parent_struct_info: schema.StructNode, writer: anytype) !void {
        const slot = field.slot orelse return;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);

        const zig_type = try self.readerTypeString(slot.type);
        defer self.allocator.free(zig_type);

        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        const is_union_member = field.discriminant_value != 0xFFFF and parent_struct_info.discriminant_count > 0;

        try self.generatePointerPresenceReader(
            field,
            parent_struct_info,
            self.reader_ref,
            "        ",
            "            ",
            writer,
        );

        try writer.print("        pub fn get{s}(self: {s}) !{s} {{\n", .{
            cap_name,
            self.reader_ref,
            zig_type,
        });

        // Preserve the typed accessor's historical error distinction: an
        // unknown union tag fails through `which()` with InvalidEnumValue,
        // while a different known arm returns WrongUnionMember. The ordinal
        // view performs its own raw guard because it must not call typed
        // `which()` for a schema-new tag.
        try self.writeUnionMemberGuard(field, parent_struct_info, "            ", writer);

        switch (slot.type) {
            .void => {
                // The guard already consumes `self` for union members; only
                // discard it when no guard was emitted.
                if (!is_union_member) try writer.writeAll("            _ = self;\n");
                try writer.writeAll("            return {};\n");
            },
            .bool => {
                const byte_offset = try self.dataByteOffset(slot.type, slot.offset);
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
                const byte_offset = try self.dataByteOffset(slot.type, slot.offset);
                if (slot.default_value) |default_value| {
                    if (self.hasZeroDefaultBits(slot.type, default_value)) {
                        try self.writeNumericGetterWithoutDefault(slot.type, byte_offset, "            ", writer);
                    } else if (try self.defaultLiteral(slot.type, default_value)) |literal| {
                        defer self.allocator.free(literal);
                        try writer.print("            const raw = self._reader.{s}({});\n", .{ read_fn, byte_offset });
                        try writer.print("            const value = raw ^ {s};\n", .{literal});
                        if (self.isUnsigned(slot.type)) {
                            try writer.writeAll("            return value;\n");
                        } else {
                            try writer.writeAll("            return @bitCast(value);\n");
                        }
                    } else {
                        try self.writeNumericGetterWithoutDefault(slot.type, byte_offset, "            ", writer);
                    }
                } else {
                    try self.writeNumericGetterWithoutDefault(slot.type, byte_offset, "            ", writer);
                }
            },
            .@"enum" => |enum_info| {
                const enum_name = try self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);
                try writer.print("            const ordinal = try self.enumOrdinals().get{s}();\n", .{cap_name});
                if (enum_name) |en| {
                    try writer.print("            return std.enums.fromInt({s}, ordinal) orelse return error.InvalidEnumValue;\n", .{en});
                } else {
                    try writer.writeAll("            return ordinal;\n");
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
                // A null data pointer reads back as an empty slice (spec: null
                // pointer means default). Guard unconditionally so unset /
                // newly-added data fields return "" instead of erroring.
                const data_default: ?[]const u8 = if (slot.default_value) |dv| self.defaultData(dv) else null;
                if (data_default) |data| {
                    try writer.print("            if (self._reader.isPointerNull({})) return ", .{slot.offset});
                    try self.writeByteArrayLiteral(writer, data);
                    try writer.writeAll(";\n");
                } else {
                    try writer.print("            if (self._reader.isPointerNull({})) return &[_]u8{{}};\n", .{slot.offset});
                }
                try writer.print("            return try self._reader.readData({});\n", .{slot.offset});
            },
            .list => |list_info| {
                if (list_info.element_type.* == .@"enum") {
                    const enum_info = list_info.element_type.@"enum";
                    const enum_name = try self.enumTypeName(enum_info.type_id);
                    defer if (enum_name) |name| self.allocator.free(name);
                    // A null list pointer reads back as an empty list (spec: null
                    // pointer means default). Guard unconditionally so unset /
                    // newly-added list fields return an empty reader.
                    if (try self.pointerDefaultConstName(field, slot)) |const_name| {
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
                    } else {
                        if (enum_name) |name| {
                            try writer.print("            if (self._reader.isPointerNull({})) return EnumListReader({s}){{ ._list = self._reader.emptyList(message.U16ListReader) }};\n", .{ slot.offset, name });
                        } else {
                            try writer.print("            if (self._reader.isPointerNull({})) return self._reader.emptyList(message.U16ListReader);\n", .{slot.offset});
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
                    // Null list pointer -> empty list (spec: null means default).
                    if (try self.pointerDefaultConstName(field, slot)) |const_name| {
                        defer self.allocator.free(const_name);
                        try writer.print("            if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                        try writer.print("                const raw = try {s}();\n", .{const_name});
                        try writer.writeAll("                return DataListReader{ ._list = raw };\n");
                        try writer.writeAll("            }\n");
                    } else {
                        try writer.print("            if (self._reader.isPointerNull({})) return DataListReader{{ ._list = self._reader.emptyList(message.PointerListReader) }};\n", .{slot.offset});
                    }
                    try writer.print("            const raw = try self._reader.readPointerList({});\n", .{slot.offset});
                    try writer.writeAll("            return DataListReader{ ._list = raw };\n");
                    try writer.writeAll("        }\n\n");
                    return;
                }
                if (list_info.element_type.* == .interface) {
                    // Null list pointer -> empty list (spec: null means default).
                    if (try self.pointerDefaultConstName(field, slot)) |const_name| {
                        defer self.allocator.free(const_name);
                        try writer.print("            if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                        try writer.print("                const raw = try {s}();\n", .{const_name});
                        try writer.writeAll("                return CapabilityListReader{ ._list = raw };\n");
                        try writer.writeAll("            }\n");
                    } else {
                        try writer.print("            if (self._reader.isPointerNull({})) return CapabilityListReader{{ ._list = self._reader.emptyList(message.PointerListReader) }};\n", .{slot.offset});
                    }
                    try writer.print("            const raw = try self._reader.readPointerList({});\n", .{slot.offset});
                    try writer.writeAll("            return CapabilityListReader{ ._list = raw };\n");
                    try writer.writeAll("        }\n\n");

                    // Generate typed resolve helper for List(Interface) fields
                    if (try self.interfaceTypeName(list_info.element_type.interface.type_id)) |iface_name| {
                        defer self.allocator.free(iface_name);
                        try writer.print("        pub fn resolve{s}(self: {s}, index: u32, peer: *rpc.peer.Peer, caps: *const rpc.caps.table.InboundCapTable) !{s}.Client {{\n", .{ cap_name, self.reader_ref, iface_name });
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
                    const struct_name = try self.structTypeName(struct_info.type_id);
                    defer if (struct_name) |name| self.allocator.free(name);
                    // Null list pointer -> empty list (spec: null means default).
                    if (try self.pointerDefaultConstName(field, slot)) |const_name| {
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
                    } else {
                        if (struct_name) |name| {
                            try writer.print("            if (self._reader.isPointerNull({})) return StructListReader({s}){{ ._list = self._reader.emptyStructList() }};\n", .{ slot.offset, name });
                        } else {
                            try writer.print("            if (self._reader.isPointerNull({})) return self._reader.emptyStructList();\n", .{slot.offset});
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
                // Null list pointer -> empty list (spec: null means default).
                if (try self.pointerDefaultConstName(field, slot)) |const_name| {
                    defer self.allocator.free(const_name);
                    try writer.print("            if (self._reader.isPointerNull({})) return try {s}();\n", .{ slot.offset, const_name });
                } else {
                    const reader_type = try self.listReaderTypeString(list_info.element_type.*);
                    defer self.allocator.free(reader_type);
                    try writer.print("            if (self._reader.isPointerNull({})) return self._reader.emptyList({s});\n", .{ slot.offset, reader_type });
                }
                try writer.print("            return try self._reader.{s}({});\n", .{ method, slot.offset });
            },
            .@"struct" => |struct_info| {
                const struct_name = try self.structTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                // A null struct pointer reads back as an all-defaults struct (spec:
                // null pointer means default). Guard unconditionally so unset /
                // newly-added struct fields return an empty reader instead of
                // erroring, unless an explicit non-null default is configured.
                if (struct_name) |name| {
                    if (try self.pointerDefaultConstName(field, slot)) |const_name| {
                        defer self.allocator.free(const_name);
                        try writer.print("            if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                        try writer.print("                const value = try {s}();\n", .{const_name});
                        try writer.print("                return {s}.Reader{{ ._reader = value }};\n", .{name});
                        try writer.writeAll("            }\n");
                    } else {
                        try writer.print("            if (self._reader.isPointerNull({})) return {s}.Reader{{ ._reader = self._reader.emptyStruct() }};\n", .{ slot.offset, name });
                    }
                    try writer.print("            const value = try self._reader.readStruct({});\n", .{slot.offset});
                    try writer.print("            return {s}.Reader{{ ._reader = value }};\n", .{name});
                } else {
                    if (try self.pointerDefaultConstName(field, slot)) |const_name| {
                        defer self.allocator.free(const_name);
                        try writer.print("            if (self._reader.isPointerNull({})) return try {s}();\n", .{ slot.offset, const_name });
                    } else {
                        try writer.print("            if (self._reader.isPointerNull({})) return self._reader.emptyStruct();\n", .{slot.offset});
                    }
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
            const iface_name = try self.interfaceTypeName(slot.type.interface.type_id) orelse return;
            defer self.allocator.free(iface_name);
            try writer.print("        pub fn resolve{s}(self: {s}, peer: *rpc.peer.Peer, caps: *const rpc.caps.table.InboundCapTable) !{s}.Client {{\n", .{ cap_name, self.reader_ref, iface_name });
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

    fn generateGroupFieldAccessor(self: *StructGenerator, field: schema.Field, parent_struct_info: schema.StructNode, writer: anytype) !void {
        const group = field.group orelse return;
        const group_node = self.getNode(group.type_id) orelse return;
        const group_name = try self.groupAccessorTypeName(group_node);
        defer self.allocator.free(group_name);

        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        // A group that is a union member must guard on the discriminant like a
        // slot member does — otherwise reading a non-selected variant silently
        // reinterprets the sibling variant's bits. The guard makes the getter
        // fallible (`!Group.Reader`); plain (non-union) groups keep the
        // infallible signature.
        const is_union_member = field.discriminant_value != 0xFFFF and parent_struct_info.discriminant_count > 0;
        const bang = if (is_union_member) "!" else "";
        try writer.print("        pub fn get{s}(self: {s}) {s}{s}.Reader {{\n", .{ cap_name, self.reader_ref, bang, group_name });
        try self.writeUnionMemberGuard(field, parent_struct_info, "            ", writer);
        try writer.writeAll("            return .{ ._reader = self._reader };\n");
        try writer.writeAll("        }\n\n");
    }

    fn generateGroupBuilderAccessor(self: *StructGenerator, field: schema.Field, struct_info: schema.StructNode, writer: anytype) !void {
        const group = field.group orelse return;
        const group_node = self.getNode(group.type_id) orelse return;
        const group_name = try self.groupAccessorTypeName(group_node);
        defer self.allocator.free(group_name);

        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        if (field.discriminant_value != 0xFFFF and struct_info.discriminant_count > 0) {
            const disc_byte_offset = try discriminantByteOffset(struct_info.discriminant_offset);
            try writer.print("        pub fn init{s}(self: *{s}) {s}.Builder {{\n", .{ cap_name, self.builder_ref, group_name });
            // Union group members share the union's data/pointer space with the
            // other variants. capnp's init<group>() zeroes the group's own slots
            // so its fields read back as their defaults rather than stale bits
            // left over from a previously-selected variant. Zero first, then write
            // the discriminant so a group data word overlapping the discriminant
            // does not clobber it.
            try self.writeGroupSlotZeroing(group_node, writer);
            try writer.print("            self._builder.writeU16({}, {});\n", .{ disc_byte_offset, field.discriminant_value });
            try writer.writeAll("            return .{ ._builder = self._builder };\n");
            try writer.writeAll("        }\n\n");
        } else {
            try writer.print("        pub fn get{s}(self: *{s}) {s}.Builder {{\n", .{ cap_name, self.builder_ref, group_name });
            try writer.writeAll("            return .{ ._builder = self._builder };\n");
            try writer.writeAll("        }\n\n");
        }
    }

    /// Byte size of a scalar/data field type, used to compute which data words
    /// a group field occupies. Pointer-typed fields live in the pointer section
    /// and return null (handled separately).
    fn scalarByteSize(typ: schema.Type) ?u32 {
        return switch (typ) {
            .bool => 1, // packed; the containing byte's word is zeroed
            .int8, .uint8 => 1,
            .int16, .uint16, .@"enum" => 2,
            .int32, .uint32, .float32 => 4,
            .int64, .uint64, .float64 => 8,
            .void => 0,
            else => null,
        };
    }

    /// Recursively collect the data-word indices and pointer indices that a
    /// group (including any nested groups) occupies within the enclosing
    /// struct's storage. Used to zero a union group's own slots on init.
    fn collectGroupSlots(
        self: *StructGenerator,
        group_node: *const schema.Node,
        data_words: *std.ArrayList(u32),
        pointer_indices: *std.ArrayList(u32),
    ) !void {
        const group_struct = group_node.struct_node orelse return;
        for (group_struct.fields) |gfield| {
            if (gfield.group) |nested| {
                if (self.getNode(nested.type_id)) |nested_node| {
                    try self.collectGroupSlots(nested_node, data_words, pointer_indices);
                }
                continue;
            }
            const slot = gfield.slot orelse continue;
            if (scalarByteSize(slot.type)) |size| {
                if (size == 0) continue; // void occupies no storage
                const byte_offset = try self.dataByteOffset(slot.type, slot.offset);
                const first_word = byte_offset / 8;
                const last_word = (byte_offset + size - 1) / 8;
                var w = first_word;
                while (w <= last_word) : (w += 1) try self.appendUnique(data_words, w);
            } else {
                // Pointer-typed field (struct/list/text/data/interface/any): the
                // slot offset is a pointer index.
                try self.appendUnique(pointer_indices, slot.offset);
            }
        }
    }

    fn appendUnique(self: *StructGenerator, list: *std.ArrayList(u32), value: u32) !void {
        for (list.items) |existing| {
            if (existing == value) return;
        }
        try list.append(self.allocator, value);
    }

    /// Emit statements zeroing a union group's own data words and nulling its
    /// pointer slots, mirroring capnp's `init<group>()` semantics.
    fn writeGroupSlotZeroing(self: *StructGenerator, group_node: *const schema.Node, writer: anytype) !void {
        try self.writeGroupSlotZeroingIndented(group_node, "            ", writer);
    }

    /// Indent-parameterized variant of `writeGroupSlotZeroing`, used when the
    /// init accessor is emitted deeper in the type tree (e.g. a union-group
    /// variant nested inside another group).
    fn writeGroupSlotZeroingIndented(self: *StructGenerator, group_node: *const schema.Node, indent: []const u8, writer: anytype) !void {
        var data_words = std.ArrayList(u32).empty;
        defer data_words.deinit(self.allocator);
        var pointer_indices = std.ArrayList(u32).empty;
        defer pointer_indices.deinit(self.allocator);

        try self.collectGroupSlots(group_node, &data_words, &pointer_indices);

        std.mem.sort(u32, data_words.items, {}, std.sort.asc(u32));
        std.mem.sort(u32, pointer_indices.items, {}, std.sort.asc(u32));

        for (data_words.items) |word| {
            try writer.print("{s}self._builder.writeU64({}, 0);\n", .{ indent, word * 8 });
        }
        for (pointer_indices.items) |idx| {
            try writer.print("{s}self._builder.clearPointer({});\n", .{ indent, idx });
        }
    }

    /// Generate field getter for a group's internal field (used inside group Reader)
    fn generateGroupFieldGetter(self: *StructGenerator, field: schema.Field, parent_struct_info: schema.StructNode, writer: anytype) !void {
        const slot = field.slot orelse return;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);

        const zig_type = try self.readerTypeString(slot.type);
        defer self.allocator.free(zig_type);

        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        const is_union_member = field.discriminant_value != 0xFFFF and parent_struct_info.discriminant_count > 0;

        try self.generatePointerPresenceReader(
            field,
            parent_struct_info,
            "@This()",
            "            ",
            "                ",
            writer,
        );

        try writer.print("            pub fn get{s}(self: @This()) !{s} {{\n", .{ cap_name, zig_type });

        // Keep typed union errors backward-compatible; see the top-level
        // getter's matching guard above.
        try self.writeUnionMemberGuard(field, parent_struct_info, "                ", writer);

        switch (slot.type) {
            .void => {
                if (!is_union_member) try writer.writeAll("                _ = self;\n");
                try writer.writeAll("                return {};\n");
            },
            .bool => {
                const byte_offset = try self.dataByteOffset(slot.type, slot.offset);
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
                const byte_offset = try self.dataByteOffset(slot.type, slot.offset);
                if (slot.default_value) |default_value| {
                    if (self.hasZeroDefaultBits(slot.type, default_value)) {
                        try self.writeNumericGetterWithoutDefault(slot.type, byte_offset, "                ", writer);
                    } else if (try self.defaultLiteral(slot.type, default_value)) |literal| {
                        defer self.allocator.free(literal);
                        try writer.print("                const raw = self._reader.{s}({});\n", .{ read_fn, byte_offset });
                        try writer.print("                const value = raw ^ {s};\n", .{literal});
                        if (self.isUnsigned(slot.type)) {
                            try writer.writeAll("                return value;\n");
                        } else {
                            try writer.writeAll("                return @bitCast(value);\n");
                        }
                    } else {
                        try self.writeNumericGetterWithoutDefault(slot.type, byte_offset, "                ", writer);
                    }
                } else {
                    try self.writeNumericGetterWithoutDefault(slot.type, byte_offset, "                ", writer);
                }
            },
            .@"enum" => |enum_info| {
                const enum_name = try self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);
                try writer.print("                const ordinal = try self.enumOrdinals().get{s}();\n", .{cap_name});
                if (enum_name) |en| {
                    try writer.print("                return std.enums.fromInt({s}, ordinal) orelse return error.InvalidEnumValue;\n", .{en});
                } else {
                    try writer.writeAll("                return ordinal;\n");
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
                const struct_name = try self.structTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                if (struct_name) |name| {
                    if (slot.default_value) |default_value| {
                        if (self.defaultPointerBytes(default_value)) |bytes| {
                            const const_name = try self.defaultConstName(field.name);
                            defer self.allocator.free(const_name);
                            try writer.print("                if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                            try writer.print("                    const value = try {s}();\n", .{const_name});
                            try writer.print("                    return {s}.Reader{{ ._reader = value }};\n", .{name});
                            try writer.writeAll("                }\n");
                            _ = bytes;
                        }
                    }
                    try writer.print("                const value = try self._reader.readStruct({});\n", .{slot.offset});
                    try writer.print("                return {s}.Reader{{ ._reader = value }};\n", .{name});
                } else {
                    try writer.print("                return try self._reader.readStruct({});\n", .{slot.offset});
                }
            },
            .list => |list_info| {
                if (list_info.element_type.* == .@"enum") {
                    const enum_info = list_info.element_type.@"enum";
                    const enum_name = try self.enumTypeName(enum_info.type_id);
                    defer if (enum_name) |name| self.allocator.free(name);
                    if (slot.default_value) |default_value| {
                        if (self.defaultPointerBytes(default_value)) |bytes| {
                            const const_name = try self.defaultConstName(field.name);
                            defer self.allocator.free(const_name);
                            if (enum_name) |name| {
                                try writer.print("                if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                                try writer.print("                    const raw = try {s}();\n", .{const_name});
                                try writer.print("                    return EnumListReader({s}){{ ._list = raw }};\n", .{name});
                                try writer.writeAll("                }\n");
                            } else {
                                try writer.print("                if (self._reader.isPointerNull({})) return try {s}();\n", .{
                                    slot.offset,
                                    const_name,
                                });
                            }
                            _ = bytes;
                        }
                    }
                    if (enum_name) |name| {
                        try writer.print("                const raw = try self._reader.readU16List({});\n", .{slot.offset});
                        try writer.print("                return EnumListReader({s}){{ ._list = raw }};\n", .{name});
                    } else {
                        try writer.print("                return try self._reader.readU16List({});\n", .{slot.offset});
                    }
                    try writer.writeAll("            }\n\n");
                    return;
                }
                if (list_info.element_type.* == .data) {
                    if (slot.default_value) |default_value| {
                        if (self.defaultPointerBytes(default_value)) |bytes| {
                            const const_name = try self.defaultConstName(field.name);
                            defer self.allocator.free(const_name);
                            try writer.print("                if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                            try writer.print("                    const raw = try {s}();\n", .{const_name});
                            try writer.writeAll("                    return DataListReader{ ._list = raw };\n");
                            try writer.writeAll("                }\n");
                            _ = bytes;
                        }
                    }
                    try writer.print("                const raw = try self._reader.readPointerList({});\n", .{slot.offset});
                    try writer.writeAll("                return DataListReader{ ._list = raw };\n");
                    try writer.writeAll("            }\n\n");
                    return;
                }
                if (list_info.element_type.* == .interface) {
                    if (slot.default_value) |default_value| {
                        if (self.defaultPointerBytes(default_value)) |bytes| {
                            const const_name = try self.defaultConstName(field.name);
                            defer self.allocator.free(const_name);
                            try writer.print("                if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                            try writer.print("                    const raw = try {s}();\n", .{const_name});
                            try writer.writeAll("                    return CapabilityListReader{ ._list = raw };\n");
                            try writer.writeAll("                }\n");
                            _ = bytes;
                        }
                    }
                    try writer.print("                const raw = try self._reader.readPointerList({});\n", .{slot.offset});
                    try writer.writeAll("                return CapabilityListReader{ ._list = raw };\n");
                    try writer.writeAll("            }\n\n");
                    return;
                }
                if (list_info.element_type.* == .@"struct") {
                    const si = list_info.element_type.@"struct";
                    const struct_name = try self.structTypeName(si.type_id);
                    defer if (struct_name) |name| self.allocator.free(name);
                    if (slot.default_value) |default_value| {
                        if (self.defaultPointerBytes(default_value)) |bytes| {
                            const const_name = try self.defaultConstName(field.name);
                            defer self.allocator.free(const_name);
                            if (struct_name) |name| {
                                try writer.print("                if (self._reader.isPointerNull({})) {{\n", .{slot.offset});
                                try writer.print("                    const raw = try {s}();\n", .{const_name});
                                try writer.print("                    return StructListReader({s}){{ ._list = raw }};\n", .{name});
                                try writer.writeAll("                }\n");
                            } else {
                                try writer.print("                if (self._reader.isPointerNull({})) return try {s}();\n", .{
                                    slot.offset,
                                    const_name,
                                });
                            }
                            _ = bytes;
                        }
                    }
                    if (struct_name) |name| {
                        try writer.print("                const raw = try self._reader.readStructList({});\n", .{slot.offset});
                        try writer.print("                return StructListReader({s}){{ ._list = raw }};\n", .{name});
                    } else {
                        try writer.print("                return try self._reader.readStructList({});\n", .{slot.offset});
                    }
                    try writer.writeAll("            }\n\n");
                    return;
                }

                // Primitive list types (void, bool, integers, floats, text)
                const method = self.listReaderMethod(list_info.element_type.*);
                if (slot.default_value) |default_value| {
                    if (self.defaultPointerBytes(default_value)) |bytes| {
                        const const_name = try self.defaultConstName(field.name);
                        defer self.allocator.free(const_name);
                        try writer.print("                if (self._reader.isPointerNull({})) return try {s}();\n", .{ slot.offset, const_name });
                        _ = bytes;
                    }
                }
                try writer.print("                return try self._reader.{s}({});\n", .{ method, slot.offset });
            },
            .any_pointer => {
                if (slot.default_value) |default_value| {
                    if (self.defaultPointerBytes(default_value)) |bytes| {
                        const const_name = try self.defaultConstName(field.name);
                        defer self.allocator.free(const_name);
                        try writer.print("                if (self._reader.isPointerNull({})) return try {s}();\n", .{ slot.offset, const_name });
                        _ = bytes;
                    }
                }
                try writer.print("                return try self._reader.readAnyPointer({});\n", .{slot.offset});
            },
            .interface => {
                try writer.print("                return try self._reader.readCapability({});\n", .{slot.offset});
            },
        }

        try writer.writeAll("            }\n\n");

        // Generate typed resolve helper for interface fields in groups
        if (slot.type == .interface) {
            const iface_name = try self.interfaceTypeName(slot.type.interface.type_id) orelse return;
            defer self.allocator.free(iface_name);
            try writer.print("            pub fn resolve{s}(self: @This(), peer: *rpc.peer.Peer, caps: *const rpc.caps.table.InboundCapTable) !{s}.Client {{\n", .{ cap_name, iface_name });
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
        const slot = field.slot orelse return;
        const zig_name = try self.type_gen.toZigIdentifier(field.name);
        defer self.allocator.free(zig_name);
        const cap_name = try self.capitalizeFirst(zig_name);
        defer self.allocator.free(cap_name);

        try self.generatePointerPresenceBuilder(
            field,
            parent_struct_info,
            "@This()",
            "            ",
            "                ",
            writer,
        );

        switch (slot.type) {
            .list => |list_info| {
                const unresolved_struct_layout = switch (list_info.element_type.*) {
                    .@"struct" => |si| self.structLayout(si.type_id) == null,
                    else => false,
                };
                const builder_type = try self.listBuilderTypeString(list_info.element_type.*);
                defer self.allocator.free(builder_type);
                if (unresolved_struct_layout) {
                    try writer.print("            pub fn init{s}(self: *@This(), element_count: u32, data_words: u16, pointer_words: u16) !{s} {{\n", .{ cap_name, builder_type });
                } else {
                    try writer.print("            pub fn init{s}(self: *@This(), element_count: u32) !{s} {{\n", .{ cap_name, builder_type });
                }
                try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                try self.writeGroupListSetterBody(list_info.element_type.*, slot.offset, writer);
                try writer.writeAll("            }\n\n");
                return;
            },
            .@"struct" => |struct_info| {
                const struct_name = try self.structTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                if (self.structLayout(struct_info.type_id)) |layout| {
                    if (struct_name) |name| {
                        try writer.print("            pub fn init{s}(self: *@This()) !{s}.Builder {{\n", .{ cap_name, name });
                        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                        try writer.print("                const builder = try self._builder.initStruct({}, {}, {});\n", .{ slot.offset, layout.data_words, layout.pointer_words });
                        try writer.print("                return {s}.Builder{{ ._builder = builder }};\n", .{name});
                    } else {
                        try writer.print("            pub fn init{s}(self: *@This()) !message.StructBuilder {{\n", .{cap_name});
                        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                        try writer.print("                return try self._builder.initStruct({}, {}, {});\n", .{ slot.offset, layout.data_words, layout.pointer_words });
                    }
                } else {
                    try writer.print("            pub fn init{s}(self: *@This(), data_words: u16, pointer_words: u16) !message.StructBuilder {{\n", .{cap_name});
                    try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                    try writer.print("                return try self._builder.initStruct({}, data_words, pointer_words);\n", .{slot.offset});
                }
                try writer.writeAll("            }\n\n");
                return;
            },
            .any_pointer => {
                try writer.print("            pub fn init{s}(self: *@This()) !message.AnyPointerBuilder {{\n", .{cap_name});
                try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                try writer.print("                return try self._builder.getAnyPointer({});\n", .{slot.offset});
                try writer.writeAll("            }\n\n");
                return;
            },
            .interface => {
                try writer.print("            pub fn init{s}(self: *@This()) !message.AnyPointerBuilder {{\n", .{cap_name});
                try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                try writer.print("                return try self._builder.getAnyPointer({});\n", .{slot.offset});
                try writer.writeAll("            }\n\n");

                // Typed helpers for group interface fields
                if (try self.interfaceTypeName(slot.type.interface.type_id)) |iface_name| {
                    defer self.allocator.free(iface_name);
                    try writer.print("            pub fn set{s}Capability(self: *@This(), cap: message.Capability) !void {{\n", .{cap_name});
                    try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                    try writer.print("                var any = try self._builder.getAnyPointer({});\n", .{slot.offset});
                    try writer.writeAll("                try any.setCapability(cap);\n");
                    try writer.writeAll("            }\n\n");

                    try writer.print("            pub fn set{s}Server(self: *@This(), peer: *rpc.peer.Peer, server: *{s}.Server) !void {{\n", .{ cap_name, iface_name });
                    try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                    try writer.print("                const cap_id = try {s}.exportServer(peer, server);\n", .{iface_name});
                    try writer.print("                var any = try self._builder.getAnyPointer({});\n", .{slot.offset});
                    try writer.writeAll("                try any.setCapability(.{ .id = cap_id });\n");
                    try writer.writeAll("            }\n\n");

                    try writer.print("            pub fn set{s}Client(self: *@This(), client: {s}.Client) !void {{\n", .{ cap_name, iface_name });
                    try self.writeUnionDiscriminant(field, parent_struct_info, writer);
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

        try writer.print("            pub fn set{s}(self: *@This(), value: {s}) !void {{\n", .{ cap_name, zig_type });
        if (slot.type != .@"enum") {
            try self.writeUnionDiscriminant(field, parent_struct_info, writer);
        }

        switch (slot.type) {
            .void => {
                if (field.discriminant_value == 0xFFFF or parent_struct_info.discriminant_count == 0) {
                    try writer.writeAll("                _ = self;\n");
                }
                try writer.writeAll("                _ = value;\n");
            },
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
            .int8, .uint8 => try self.writeNumericSetterBody(slot, "writeU8", "u8", "                ", writer),
            .int16, .uint16 => try self.writeNumericSetterBody(slot, "writeU16", "u16", "                ", writer),
            .int32, .uint32, .float32 => try self.writeNumericSetterBody(slot, "writeU32", "u32", "                ", writer),
            .int64, .uint64, .float64 => try self.writeNumericSetterBody(slot, "writeU64", "u64", "                ", writer),
            .@"enum" => |enum_info| {
                const enum_name = try self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);
                const raw_expr = if (enum_name != null) "@as(u16, @intFromEnum(value))" else "@as(u16, value)";
                try writer.print("                return self.enumOrdinals().set{s}({s});\n", .{ cap_name, raw_expr });
            },
            .text => try writer.print("                try self._builder.writeText({}, value);\n", .{slot.offset}),
            .data => try writer.print("                try self._builder.writeData({}, value);\n", .{slot.offset}),
            else => {},
        }

        try writer.writeAll("            }\n\n");
    }

    fn generatePointerDefaults(
        self: *StructGenerator,
        struct_info: schema.StructNode,
        decl_indent: []const u8,
        body_indent: []const u8,
        writer: anytype,
    ) !void {
        const value_indent = try std.fmt.allocPrint(self.allocator, "{s}    ", .{body_indent});
        defer self.allocator.free(value_indent);
        var emitted: bool = false;
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            const value = slot.default_value orelse continue;
            const bytes = self.defaultPointerBytes(value) orelse continue;

            const const_name = try self.defaultConstName(field.name);
            defer self.allocator.free(const_name);

            const return_type = try self.defaultPointerReturnType(slot.type);
            defer self.allocator.free(return_type);

            try writer.print("{s}const {s}_bytes = ", .{ decl_indent, const_name });
            try self.writeByteArrayInitializer(writer, bytes);
            try writer.writeAll(";\n");
            try writer.print("{s}const {s}_segments = [_][]const u8{{ {s}_bytes[0..] }};\n", .{ decl_indent, const_name, const_name });
            try writer.print(
                "{s}const {s}_message = message.Message{{ .allocator = std.heap.page_allocator, .segments = {s}_segments[0..], .backing_data = null, .segments_owned = false }};\n\n",
                .{ decl_indent, const_name, const_name },
            );

            try writer.print("{s}fn {s}() !{s} {{\n", .{ decl_indent, const_name, return_type });
            switch (slot.type) {
                .list => |list_info| {
                    const elem_type = list_info.element_type.*;
                    try writer.print("{s}const root = try {s}_message.getRootAnyPointer();\n", .{ body_indent, const_name });
                    if (elem_type == .@"struct") {
                        try writer.print("{s}const list = try root.getInlineCompositeList();\n", .{body_indent});
                        try writer.print("{s}return message.StructListReader{{\n", .{body_indent});
                        try writer.print("{s}.message = &{s}_message,\n", .{ value_indent, const_name });
                        try writer.print("{s}.segment_id = list.segment_id,\n", .{value_indent});
                        try writer.print("{s}.elements_offset = list.elements_offset,\n", .{value_indent});
                        try writer.print("{s}.element_count = list.element_count,\n", .{value_indent});
                        try writer.print("{s}.data_words = list.data_words,\n", .{value_indent});
                        try writer.print("{s}.pointer_words = list.pointer_words,\n", .{value_indent});
                        try writer.print("{s}}};\n", .{body_indent});
                    } else {
                        const element_size = try self.listElementSize(elem_type);
                        try writer.print("{s}const list = try root.getList();\n", .{body_indent});
                        try writer.print("{s}if (list.element_size != {}) return error.InvalidPointer;\n", .{ body_indent, element_size });
                        if (elem_type == .void) {
                            try writer.print("{s}return .{{ .element_count = list.element_count }};\n", .{body_indent});
                        } else {
                            try writer.print("{s}return ", .{body_indent});
                            try writer.print("{s}{{\n", .{return_type});
                            try writer.print("{s}.message = &{s}_message,\n", .{ value_indent, const_name });
                            try writer.print("{s}.segment_id = list.segment_id,\n", .{value_indent});
                            try writer.print("{s}.elements_offset = list.content_offset,\n", .{value_indent});
                            try writer.print("{s}.element_count = list.element_count,\n", .{value_indent});
                            try writer.print("{s}}};\n", .{body_indent});
                        }
                    }
                },
                .@"struct" => {
                    try writer.print("{s}return try {s}_message.getRootStruct();\n", .{ body_indent, const_name });
                },
                .any_pointer => {
                    try writer.print("{s}return try {s}_message.getRootAnyPointer();\n", .{ body_indent, const_name });
                },
                .interface => {
                    try writer.print("{s}const root = try {s}_message.getRootAnyPointer();\n", .{ body_indent, const_name });
                    try writer.print("{s}return try root.getCapability();\n", .{body_indent});
                },
                else => return error.InvalidDefaultPointerType,
            }
            try writer.print("{s}}}\n\n", .{decl_indent});
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

        if (self.api_profile == .full) {
            try writer.print("        pub fn init(msg: *message.MessageBuilder) !{s} {{\n", .{self.builder_ref});
            try writer.print("            const builder = try msg.allocateStruct({}, {});\n", .{ data_word_count, pointer_count });
            try writer.writeAll("            return .{ ._builder = builder };\n");
            try writer.writeAll("        }\n\n");
        }

        try writer.print("        pub fn wrap(builder: message.StructBuilder) {s} {{\n", .{self.builder_ref});
        try writer.writeAll("            return .{ ._builder = builder };\n");
        try writer.writeAll("        }\n\n");

        try self.generateEnumOrdinalsBuilderView(
            struct_info,
            "        ",
            "            ",
            "                ",
            writer,
        );
        try self.generateNestedListsBuilderView(
            struct_info,
            "        ",
            "            ",
            "                ",
            writer,
        );
        try self.generateBrandsBuilderView(
            struct_info,
            "        ",
            "            ",
            "                ",
            writer,
        );
        try self.generatePointerKindsBuilderView(
            struct_info,
            "        ",
            "            ",
            "                ",
            writer,
        );

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

        try self.generatePointerPresenceBuilder(
            field,
            parent_struct_info,
            self.builder_ref,
            "        ",
            "            ",
            writer,
        );

        switch (slot.type) {
            .list => |list_info| {
                const unresolved_struct_layout = switch (list_info.element_type.*) {
                    .@"struct" => |struct_info| self.structLayout(struct_info.type_id) == null,
                    else => false,
                };
                const builder_type = try self.listBuilderTypeString(list_info.element_type.*);
                defer self.allocator.free(builder_type);
                if (unresolved_struct_layout) {
                    try writer.print("        pub fn init{s}(self: *{s}, element_count: u32, data_words: u16, pointer_words: u16) !{s} {{\n", .{
                        cap_name,
                        self.builder_ref,
                        builder_type,
                    });
                } else {
                    try writer.print("        pub fn init{s}(self: *{s}, element_count: u32) !{s} {{\n", .{
                        cap_name,
                        self.builder_ref,
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
                        const enum_name = try self.enumTypeName(enum_info.type_id);
                        defer if (enum_name) |name| self.allocator.free(name);
                        if (enum_name) |name| {
                            try writer.print("            const raw = try self._builder.writeU16List({}, element_count);\n", .{slot.offset});
                            try writer.print("            return EnumListBuilder({s}){{ ._list = raw }};\n", .{name});
                        } else {
                            try writer.print("            return try self._builder.writeU16List({}, element_count);\n", .{slot.offset});
                        }
                    },
                    .@"struct" => |struct_info| {
                        const struct_name = try self.structTypeName(struct_info.type_id);
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
                const struct_name = try self.structTypeName(struct_info.type_id);
                defer if (struct_name) |name| self.allocator.free(name);
                if (self.structLayout(struct_info.type_id)) |layout| {
                    if (struct_name) |name| {
                        try writer.print("        pub fn init{s}(self: *{s}) !{s}.Builder {{\n", .{ cap_name, self.builder_ref, name });
                        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                        try writer.print("            const builder = try self._builder.initStruct({}, {}, {});\n", .{
                            slot.offset,
                            layout.data_words,
                            layout.pointer_words,
                        });
                        try writer.print("            return {s}.Builder{{ ._builder = builder }};\n", .{name});
                        try writer.writeAll("        }\n\n");
                    } else {
                        try writer.print("        pub fn init{s}(self: *{s}) !message.StructBuilder {{\n", .{ cap_name, self.builder_ref });
                        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
                        try writer.print("            return try self._builder.initStruct({}, {}, {});\n", .{
                            slot.offset,
                            layout.data_words,
                            layout.pointer_words,
                        });
                        try writer.writeAll("        }\n\n");
                    }
                } else {
                    try writer.print("        pub fn init{s}(self: *{s}, data_words: u16, pointer_words: u16) !message.StructBuilder {{\n", .{ cap_name, self.builder_ref });
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

        try writer.print("        pub fn set{s}(self: *{s}, value: {s}) !void {{\n", .{
            cap_name,
            self.builder_ref,
            zig_type,
        });

        // Write union discriminant if this is a union field
        if (slot.type != .@"enum") {
            try self.writeUnionDiscriminant(field, parent_struct_info, writer);
        }

        switch (slot.type) {
            .void => {
                if (field.discriminant_value == 0xFFFF or parent_struct_info.discriminant_count == 0) {
                    try writer.writeAll("            _ = self;\n");
                }
                try writer.writeAll("            _ = value;\n");
            },
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
            .int8, .uint8 => try self.writeNumericSetterBody(slot, "writeU8", "u8", "            ", writer),
            .int16, .uint16 => try self.writeNumericSetterBody(slot, "writeU16", "u16", "            ", writer),
            .int32, .uint32, .float32 => try self.writeNumericSetterBody(slot, "writeU32", "u32", "            ", writer),
            .int64, .uint64, .float64 => try self.writeNumericSetterBody(slot, "writeU64", "u64", "            ", writer),
            .@"enum" => |enum_info| {
                const enum_name = try self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);
                const raw_expr = if (enum_name != null) "@as(u16, @intFromEnum(value))" else "@as(u16, value)";
                try writer.print("            return self.enumOrdinals().set{s}({s});\n", .{ cap_name, raw_expr });
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

    // PascalCase normalization prevents keyword collision (all Zig keywords are lowercase)
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
        if (types.primitiveTypeToZig(typ)) |name| return try self.allocator.dupe(u8, name);
        return switch (typ) {
            .any_pointer => try self.allocator.dupe(u8, "message.AnyPointerReader"),
            .interface => try self.allocator.dupe(u8, "message.Capability"),
            .@"enum" => |enum_info| blk: {
                if (try self.enumTypeName(enum_info.type_id)) |name| break :blk name;
                break :blk try self.allocator.dupe(u8, "u16");
            },
            .@"struct" => |struct_info| blk: {
                if (try self.structTypeName(struct_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "{s}.Reader", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.StructReader");
            },
            .list => |list_info| try self.listFieldReaderTypeString(list_info.element_type.*),
            else => try self.allocator.dupe(u8, "void"),
        };
    }

    fn writerTypeString(self: *StructGenerator, typ: schema.Type) ![]const u8 {
        if (types.primitiveTypeToZig(typ)) |name| return try self.allocator.dupe(u8, name);
        return switch (typ) {
            .@"enum" => |enum_info| blk: {
                if (try self.enumTypeName(enum_info.type_id)) |name| break :blk name;
                break :blk try self.allocator.dupe(u8, "u16");
            },
            else => try self.allocator.dupe(u8, "void"),
        };
    }

    const ListPrimitiveInfo = struct {
        method: []const u8,
        reader_type: []const u8,
        builder_type: []const u8,
    };

    fn listPrimitiveInfo(elem_type: schema.Type) ?ListPrimitiveInfo {
        return switch (elem_type) {
            .void => .{ .method = "readVoidList", .reader_type = "message.VoidListReader", .builder_type = "message.VoidListBuilder" },
            .bool => .{ .method = "readBoolList", .reader_type = "message.BoolListReader", .builder_type = "message.BoolListBuilder" },
            .int8 => .{ .method = "readI8List", .reader_type = "message.I8ListReader", .builder_type = "message.I8ListBuilder" },
            .uint8 => .{ .method = "readU8List", .reader_type = "message.U8ListReader", .builder_type = "message.U8ListBuilder" },
            .int16 => .{ .method = "readI16List", .reader_type = "message.I16ListReader", .builder_type = "message.I16ListBuilder" },
            .uint16 => .{ .method = "readU16List", .reader_type = "message.U16ListReader", .builder_type = "message.U16ListBuilder" },
            .int32 => .{ .method = "readI32List", .reader_type = "message.I32ListReader", .builder_type = "message.I32ListBuilder" },
            .uint32 => .{ .method = "readU32List", .reader_type = "message.U32ListReader", .builder_type = "message.U32ListBuilder" },
            .float32 => .{ .method = "readF32List", .reader_type = "message.F32ListReader", .builder_type = "message.F32ListBuilder" },
            .int64 => .{ .method = "readI64List", .reader_type = "message.I64ListReader", .builder_type = "message.I64ListBuilder" },
            .uint64 => .{ .method = "readU64List", .reader_type = "message.U64ListReader", .builder_type = "message.U64ListBuilder" },
            .float64 => .{ .method = "readF64List", .reader_type = "message.F64ListReader", .builder_type = "message.F64ListBuilder" },
            .text => .{ .method = "readTextList", .reader_type = "message.TextListReader", .builder_type = "message.TextListBuilder" },
            else => null,
        };
    }

    fn listReaderMethod(self: *StructGenerator, elem_type: schema.Type) []const u8 {
        _ = self;
        if (listPrimitiveInfo(elem_type)) |info| return info.method;
        return switch (elem_type) {
            .@"struct" => "readStructList",
            .@"enum" => "readU16List",
            else => "readPointerList",
        };
    }

    fn listReaderTypeString(self: *StructGenerator, elem_type: schema.Type) ![]const u8 {
        if (listPrimitiveInfo(elem_type)) |info| return try self.allocator.dupe(u8, info.reader_type);
        return switch (elem_type) {
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
                if (try self.enumTypeName(enum_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "EnumListReader({s})", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.U16ListReader");
            },
            .@"struct" => |struct_info| blk: {
                if (try self.structTypeName(struct_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "StructListReader({s})", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.StructListReader");
            },
            else => try self.listReaderTypeString(elem_type),
        };
    }

    fn listBuilderTypeString(self: *StructGenerator, elem_type: schema.Type) ![]const u8 {
        if (listPrimitiveInfo(elem_type)) |info| return try self.allocator.dupe(u8, info.builder_type);
        return switch (elem_type) {
            .data => try self.allocator.dupe(u8, "DataListBuilder"),
            .interface => try self.allocator.dupe(u8, "CapabilityListBuilder"),
            .@"enum" => |enum_info| blk: {
                if (try self.enumTypeName(enum_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "EnumListBuilder({s})", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.U16ListBuilder");
            },
            .@"struct" => |struct_info| blk: {
                if (try self.structTypeName(struct_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "StructListBuilder({s})", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.StructListBuilder");
            },
            else => try self.allocator.dupe(u8, "message.PointerListBuilder"),
        };
    }

    fn structTypeName(self: *StructGenerator, id: schema.Id) !?[]const u8 {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .@"struct") return null;
        return try self.qualifiedTypeName(node, id);
    }

    fn enumTypeName(self: *StructGenerator, id: schema.Id) !?[]const u8 {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .@"enum") return null;
        return try self.qualifiedTypeName(node, id);
    }

    fn interfaceTypeName(self: *StructGenerator, id: schema.Id) !?[]const u8 {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .interface) return null;
        return try self.qualifiedTypeName(node, id);
    }

    /// Return the type name, qualified with import module prefix for cross-file types.
    fn qualifiedTypeName(self: *StructGenerator, node: *const schema.Node, id: schema.Id) ![]const u8 {
        const bare_name = try self.allocTypeName(node);
        defer self.allocator.free(bare_name);

        // Owned parent-scope path (e.g. "Outer1"), or null for a file-scoped type.
        var parent: ?[]const u8 = null;
        defer if (parent) |p| self.allocator.free(p);
        if (self.parent_path_fn) |ppf| parent = try ppf(self.node_lookup_ctx, id);

        // Borrowed cross-file module prefix (not freed here).
        var module: ?[]const u8 = null;
        if (self.type_prefix_fn) |prefix_fn| module = try prefix_fn(self.node_lookup_ctx, id);

        // A concrete Brands view declares field-named wrapper types. When a
        // field has the same PascalCase name as its schema target (the natural
        // `box :Box(Text)` spelling), a bare type reference in the surrounding
        // Reader/Builder becomes ambiguous. Anchor that same-file reference at
        // the generated file namespace; imported module paths are already
        // unambiguous and must be preserved as-is.
        const qualify_at_file_root = self.in_brand_emission and module == null and try self.currentFieldShadowsTypeName(bare_name);

        if (parent == null and module == null and !qualify_at_file_root) return self.allocator.dupe(u8, bare_name);

        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);
        if (module) |m| {
            try out.appendSlice(self.allocator, m);
        } else if (qualify_at_file_root) {
            try out.appendSlice(self.allocator, "_capnp_file");
        }
        if (parent) |p| {
            if (out.items.len > 0) try out.append(self.allocator, '.');
            try out.appendSlice(self.allocator, p);
        }
        try out.append(self.allocator, '.');
        try out.appendSlice(self.allocator, bare_name);
        return out.toOwnedSlice(self.allocator);
    }

    fn currentFieldShadowsTypeName(self: *StructGenerator, type_name: []const u8) !bool {
        const owner = self.brand_owner orelse return false;
        const info = owner.struct_node orelse return false;
        for (info.fields) |field| {
            if (field.slot == null and field.group == null) continue;
            const zig_name = try self.type_gen.toZigIdentifier(field.name);
            defer self.allocator.free(zig_name);
            const cap_name = try self.capitalizeFirst(zig_name);
            defer self.allocator.free(cap_name);
            if (std.mem.eql(u8, cap_name, type_name)) return true;
        }
        return false;
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

    fn hasZeroDefaultBits(self: *StructGenerator, typ: schema.Type, default_value: schema.Value) bool {
        const bits = self.defaultBits(typ, default_value) orelse return false;
        return bits == 0;
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

    /// If the slot carries an explicit non-null pointer default, return the
    /// generated default-const name (caller owns it); otherwise return null.
    /// A null result means the field's default is the implicit empty value, so
    /// a null-pointer getter should fall back to an empty reader.
    fn pointerDefaultConstName(self: *StructGenerator, field: schema.Field, slot: schema.FieldSlot) !?[]const u8 {
        const default_value = slot.default_value orelse return null;
        if (self.defaultPointerBytes(default_value) == null) return null;
        return try self.defaultConstName(field.name);
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

    fn dataByteOffset(self: *StructGenerator, typ: schema.Type, offset: u32) error{InvalidFieldOffset}!u32 {
        _ = self;
        return switch (typ) {
            .bool => offset / 8,
            .int8, .uint8 => offset,
            .int16, .uint16, .@"enum" => std.math.mul(u32, offset, 2) catch return error.InvalidFieldOffset,
            .int32, .uint32, .float32 => std.math.mul(u32, offset, 4) catch return error.InvalidFieldOffset,
            .int64, .uint64, .float64 => std.math.mul(u32, offset, 8) catch return error.InvalidFieldOffset,
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

    fn writeNumericGetterWithoutDefault(self: *StructGenerator, typ: schema.Type, byte_offset: u32, indent: []const u8, writer: anytype) !void {
        const read_fn = self.readFnForType(typ);
        if (self.isUnsigned(typ)) {
            try writer.print("{s}return self._reader.{s}({});\n", .{ indent, read_fn, byte_offset });
        } else {
            try writer.print("{s}return @bitCast(self._reader.{s}({}));\n", .{ indent, read_fn, byte_offset });
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
        try writer.writeAll("            pub fn wrap(" ++ param_name ++ ": " ++ field_type ++ ") @This() {\n");
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
            const disc_byte_offset = try discriminantByteOffset(parent_struct_info.discriminant_offset);
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
        try writer.print("        pub fn init{s}(self: *{s}) !message.AnyPointerBuilder {{\n", .{ cap_name, self.builder_ref });
        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
        try writer.print("            return try self._builder.getAnyPointer({});\n", .{slot_offset});
        try writer.writeAll("        }\n\n");

        if (is_interface) {
            // clear method
            try writer.print("        pub fn clear{s}(self: *{s}) !void {{\n", .{ cap_name, self.builder_ref });
            try self.writeUnionDiscriminant(field, parent_struct_info, writer);
            try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
            try writer.writeAll("            try any.setNull();\n");
            try writer.writeAll("        }\n\n");
        } else {
            // setNull method
            try writer.print("        pub fn set{s}Null(self: *{s}) !void {{\n", .{ cap_name, self.builder_ref });
            try self.writeUnionDiscriminant(field, parent_struct_info, writer);
            try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
            try writer.writeAll("            try any.setNull();\n");
            try writer.writeAll("        }\n\n");

            // setText method
            try writer.print("        pub fn set{s}Text(self: *{s}, value: []const u8) !void {{\n", .{ cap_name, self.builder_ref });
            try self.writeUnionDiscriminant(field, parent_struct_info, writer);
            try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
            try writer.writeAll("            try any.setText(value);\n");
            try writer.writeAll("        }\n\n");

            // setData method
            try writer.print("        pub fn set{s}Data(self: *{s}, value: []const u8) !void {{\n", .{ cap_name, self.builder_ref });
            try self.writeUnionDiscriminant(field, parent_struct_info, writer);
            try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
            try writer.writeAll("            try any.setData(value);\n");
            try writer.writeAll("        }\n\n");
        }

        // setCapability method
        try writer.print("        pub fn set{s}Capability(self: *{s}, cap: message.Capability) !void {{\n", .{ cap_name, self.builder_ref });
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
        const iface_name = try self.interfaceTypeName(type_id) orelse return;
        defer self.allocator.free(iface_name);

        // setXxxServer: exports a server and writes the capability pointer
        try writer.print("        pub fn set{s}Server(self: *{s}, peer: *rpc.peer.Peer, server: *{s}.Server) !void {{\n", .{ cap_name, self.builder_ref, iface_name });
        try self.writeUnionDiscriminant(field, parent_struct_info, writer);
        try writer.print("            const cap_id = try {s}.exportServer(peer, server);\n", .{iface_name});
        try writer.print("            var any = try self._builder.getAnyPointer({});\n", .{slot_offset});
        try writer.writeAll("            try any.setCapability(.{ .id = cap_id });\n");
        try writer.writeAll("        }\n\n");

        // setXxxClient: writes an existing client's capability pointer
        try writer.print("        pub fn set{s}Client(self: *{s}, client: {s}.Client) !void {{\n", .{ cap_name, self.builder_ref, iface_name });
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
        indent: []const u8,
        writer: anytype,
    ) !void {
        const byte_offset = try self.dataByteOffset(slot.type, slot.offset);
        if (slot.default_value) |default_value| {
            if (self.hasZeroDefaultBits(slot.type, default_value)) {
                // Zero XOR default is a no-op; emit direct write.
            } else if (try self.defaultLiteral(slot.type, default_value)) |literal| {
                defer self.allocator.free(literal);
                try writer.print("{s}const stored = @as({s}, @bitCast(value)) ^ {s};\n", .{ indent, cast_width, literal });
                try writer.print("{s}self._builder.{s}({}, stored);\n", .{ indent, write_fn, byte_offset });
                return;
            }
        }
        try writer.print("{s}self._builder.{s}({}, @bitCast(value));\n", .{ indent, write_fn, byte_offset });
    }

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
                const enum_name = try self.enumTypeName(enum_info.type_id);
                defer if (enum_name) |name| self.allocator.free(name);
                if (enum_name) |name| {
                    try writer.print("                const raw = try self._builder.writeU16List({}, element_count);\n", .{slot_offset});
                    try writer.print("                return EnumListBuilder({s}){{ ._list = raw }};\n", .{name});
                } else {
                    try writer.print("                return try self._builder.writeU16List({}, element_count);\n", .{slot_offset});
                }
            },
            .@"struct" => |struct_info| {
                const struct_name = try self.structTypeName(struct_info.type_id);
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
};

// ---------------------------------------------------------------------------
// Inline unit tests for pure helper functions
// ---------------------------------------------------------------------------

test "StructGenerator.capitalizeFirst capitalizes first character" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    const r1 = try sg.capitalizeFirst("fooBar");
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("FooBar", r1);

    const r2 = try sg.capitalizeFirst("x");
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("X", r2);

    const r3 = try sg.capitalizeFirst("Already");
    defer alloc.free(r3);
    try std.testing.expectEqualStrings("Already", r3);
}

test "StructGenerator.capitalizeFirst handles empty string" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    const r = try sg.capitalizeFirst("");
    defer alloc.free(r);
    try std.testing.expectEqualStrings("", r);
}

test "StructGenerator.dataByteOffset computes correct offsets" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    // bool: offset / 8
    try std.testing.expectEqual(@as(u32, 1), try sg.dataByteOffset(.bool, 8));
    try std.testing.expectEqual(@as(u32, 0), try sg.dataByteOffset(.bool, 0));
    try std.testing.expectEqual(@as(u32, 0), try sg.dataByteOffset(.bool, 7));

    // u8/i8: offset * 1
    try std.testing.expectEqual(@as(u32, 3), try sg.dataByteOffset(.uint8, 3));
    try std.testing.expectEqual(@as(u32, 0), try sg.dataByteOffset(.int8, 0));

    // u16/i16/enum: offset * 2
    try std.testing.expectEqual(@as(u32, 4), try sg.dataByteOffset(.uint16, 2));
    try std.testing.expectEqual(@as(u32, 6), try sg.dataByteOffset(.int16, 3));

    // u32/i32/f32: offset * 4
    try std.testing.expectEqual(@as(u32, 8), try sg.dataByteOffset(.uint32, 2));
    try std.testing.expectEqual(@as(u32, 4), try sg.dataByteOffset(.float32, 1));

    // u64/i64/f64: offset * 8
    try std.testing.expectEqual(@as(u32, 8), try sg.dataByteOffset(.uint64, 1));
    try std.testing.expectEqual(@as(u32, 16), try sg.dataByteOffset(.float64, 2));
}

test "StructGenerator.discriminantByteOffset rejects overflow" {
    try std.testing.expectError(
        error.InvalidDiscriminantOffset,
        StructGenerator.discriminantByteOffset(std.math.maxInt(u32)),
    );
}

test "StructGenerator.dataByteOffset rejects overflow" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);
    try std.testing.expectError(error.InvalidFieldOffset, sg.dataByteOffset(.float64, std.math.maxInt(u32)));
}

test "StructGenerator.readFnForType maps types to reader methods" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    try std.testing.expectEqualStrings("readU8", sg.readFnForType(.int8));
    try std.testing.expectEqualStrings("readU8", sg.readFnForType(.uint8));
    try std.testing.expectEqualStrings("readU16", sg.readFnForType(.int16));
    try std.testing.expectEqualStrings("readU16", sg.readFnForType(.uint16));
    try std.testing.expectEqualStrings("readU32", sg.readFnForType(.int32));
    try std.testing.expectEqualStrings("readU32", sg.readFnForType(.uint32));
    try std.testing.expectEqualStrings("readU32", sg.readFnForType(.float32));
    try std.testing.expectEqualStrings("readU64", sg.readFnForType(.int64));
    try std.testing.expectEqualStrings("readU64", sg.readFnForType(.uint64));
    try std.testing.expectEqualStrings("readU64", sg.readFnForType(.float64));
}

test "StructGenerator.isUnsigned identifies unsigned integer types" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    try std.testing.expect(sg.isUnsigned(.uint8));
    try std.testing.expect(sg.isUnsigned(.uint16));
    try std.testing.expect(sg.isUnsigned(.uint32));
    try std.testing.expect(sg.isUnsigned(.uint64));

    try std.testing.expect(!sg.isUnsigned(.int8));
    try std.testing.expect(!sg.isUnsigned(.int16));
    try std.testing.expect(!sg.isUnsigned(.int32));
    try std.testing.expect(!sg.isUnsigned(.int64));
    try std.testing.expect(!sg.isUnsigned(.float32));
    try std.testing.expect(!sg.isUnsigned(.float64));
    try std.testing.expect(!sg.isUnsigned(.bool));
    try std.testing.expect(!sg.isUnsigned(.void));
}

test "StructGenerator.bitWidth returns correct widths" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    try std.testing.expectEqual(@as(?u8, 8), sg.bitWidth(.int8));
    try std.testing.expectEqual(@as(?u8, 8), sg.bitWidth(.uint8));
    try std.testing.expectEqual(@as(?u8, 16), sg.bitWidth(.int16));
    try std.testing.expectEqual(@as(?u8, 16), sg.bitWidth(.uint16));
    try std.testing.expectEqual(@as(?u8, 32), sg.bitWidth(.int32));
    try std.testing.expectEqual(@as(?u8, 32), sg.bitWidth(.uint32));
    try std.testing.expectEqual(@as(?u8, 32), sg.bitWidth(.float32));
    try std.testing.expectEqual(@as(?u8, 64), sg.bitWidth(.int64));
    try std.testing.expectEqual(@as(?u8, 64), sg.bitWidth(.uint64));
    try std.testing.expectEqual(@as(?u8, 64), sg.bitWidth(.float64));
    try std.testing.expectEqual(@as(?u8, null), sg.bitWidth(.void));
    try std.testing.expectEqual(@as(?u8, null), sg.bitWidth(.bool));
    try std.testing.expectEqual(@as(?u8, null), sg.bitWidth(.text));
}

test "StructGenerator.listReaderMethod maps types to list reader methods" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    try std.testing.expectEqualStrings("readVoidList", sg.listReaderMethod(.void));
    try std.testing.expectEqualStrings("readBoolList", sg.listReaderMethod(.bool));
    try std.testing.expectEqualStrings("readI8List", sg.listReaderMethod(.int8));
    try std.testing.expectEqualStrings("readU8List", sg.listReaderMethod(.uint8));
    try std.testing.expectEqualStrings("readI16List", sg.listReaderMethod(.int16));
    try std.testing.expectEqualStrings("readU16List", sg.listReaderMethod(.uint16));
    try std.testing.expectEqualStrings("readI32List", sg.listReaderMethod(.int32));
    try std.testing.expectEqualStrings("readU32List", sg.listReaderMethod(.uint32));
    try std.testing.expectEqualStrings("readF32List", sg.listReaderMethod(.float32));
    try std.testing.expectEqualStrings("readI64List", sg.listReaderMethod(.int64));
    try std.testing.expectEqualStrings("readU64List", sg.listReaderMethod(.uint64));
    try std.testing.expectEqualStrings("readF64List", sg.listReaderMethod(.float64));
    try std.testing.expectEqualStrings("readTextList", sg.listReaderMethod(.text));
    try std.testing.expectEqualStrings("readPointerList", sg.listReaderMethod(.data));
    try std.testing.expectEqualStrings("readPointerList", sg.listReaderMethod(.any_pointer));
}

test "StructGenerator.listElementSize returns correct Cap'n Proto element sizes" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    try std.testing.expectEqual(@as(u3, 0), try sg.listElementSize(.void));
    try std.testing.expectEqual(@as(u3, 1), try sg.listElementSize(.bool));
    try std.testing.expectEqual(@as(u3, 2), try sg.listElementSize(.int8));
    try std.testing.expectEqual(@as(u3, 2), try sg.listElementSize(.uint8));
    try std.testing.expectEqual(@as(u3, 3), try sg.listElementSize(.int16));
    try std.testing.expectEqual(@as(u3, 3), try sg.listElementSize(.uint16));
    try std.testing.expectEqual(@as(u3, 4), try sg.listElementSize(.int32));
    try std.testing.expectEqual(@as(u3, 4), try sg.listElementSize(.uint32));
    try std.testing.expectEqual(@as(u3, 4), try sg.listElementSize(.float32));
    try std.testing.expectEqual(@as(u3, 5), try sg.listElementSize(.int64));
    try std.testing.expectEqual(@as(u3, 5), try sg.listElementSize(.uint64));
    try std.testing.expectEqual(@as(u3, 5), try sg.listElementSize(.float64));
    try std.testing.expectEqual(@as(u3, 6), try sg.listElementSize(.text));
    try std.testing.expectEqual(@as(u3, 6), try sg.listElementSize(.data));
    try std.testing.expectEqual(@as(u3, 6), try sg.listElementSize(.any_pointer));
}

test "StructGenerator.defaultBool extracts boolean default" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    try std.testing.expectEqual(true, sg.defaultBool(schema.Value{ .bool = true }));
    try std.testing.expectEqual(false, sg.defaultBool(schema.Value{ .bool = false }));
    try std.testing.expectEqual(false, sg.defaultBool(schema.Value{ .void = {} }));
    try std.testing.expectEqual(false, sg.defaultBool(schema.Value{ .uint32 = 42 }));
}

test "StructGenerator.defaultText extracts text default" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    const text_val = schema.Value{ .text = "hello" };
    try std.testing.expectEqualStrings("hello", sg.defaultText(text_val).?);

    const non_text = schema.Value{ .uint32 = 42 };
    try std.testing.expect(sg.defaultText(non_text) == null);
}

test "StructGenerator.defaultData extracts data default" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    const data_val = schema.Value{ .data = &[_]u8{ 1, 2, 3 } };
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3 }, sg.defaultData(data_val).?);

    const non_data = schema.Value{ .void = {} };
    try std.testing.expect(sg.defaultData(non_data) == null);
}

test "StructGenerator.defaultBits extracts numeric default bits" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    // uint32 default
    try std.testing.expectEqual(@as(?u64, 42), sg.defaultBits(.uint32, schema.Value{ .uint32 = 42 }));

    // int8 default (-1 as u8 = 255)
    try std.testing.expectEqual(@as(?u64, 255), sg.defaultBits(.int8, schema.Value{ .int8 = -1 }));

    // Type mismatch returns null
    try std.testing.expectEqual(@as(?u64, null), sg.defaultBits(.uint32, schema.Value{ .uint16 = 5 }));

    // Non-numeric type returns null
    try std.testing.expectEqual(@as(?u64, null), sg.defaultBits(.void, schema.Value{ .void = {} }));
}

test "StructGenerator.defaultLiteral formats XOR default literal" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    // A u32 default of 42 should produce a literal
    const literal = try sg.defaultLiteral(.uint32, schema.Value{ .uint32 = 42 });
    if (literal) |lit| {
        defer alloc.free(lit);
        try std.testing.expectEqualStrings("@as(u32, 42)", lit);
    } else {
        return error.TestUnexpectedResult;
    }

    // Zero default should also produce a literal
    const zero_lit = try sg.defaultLiteral(.uint16, schema.Value{ .uint16 = 0 });
    if (zero_lit) |lit| {
        defer alloc.free(lit);
        try std.testing.expectEqualStrings("@as(u16, 0)", lit);
    } else {
        return error.TestUnexpectedResult;
    }

    // Mismatched type returns null
    const no_lit = try sg.defaultLiteral(.uint32, schema.Value{ .void = {} });
    try std.testing.expect(no_lit == null);
}

test "StructGenerator.readerTypeString maps primitive types" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    const cases = .{
        .{ .typ = schema.Type.void, .expected = "void" },
        .{ .typ = schema.Type.bool, .expected = "bool" },
        .{ .typ = schema.Type.int8, .expected = "i8" },
        .{ .typ = schema.Type.int16, .expected = "i16" },
        .{ .typ = schema.Type.int32, .expected = "i32" },
        .{ .typ = schema.Type.int64, .expected = "i64" },
        .{ .typ = schema.Type.uint8, .expected = "u8" },
        .{ .typ = schema.Type.uint16, .expected = "u16" },
        .{ .typ = schema.Type.uint32, .expected = "u32" },
        .{ .typ = schema.Type.uint64, .expected = "u64" },
        .{ .typ = schema.Type.float32, .expected = "f32" },
        .{ .typ = schema.Type.float64, .expected = "f64" },
        .{ .typ = schema.Type.text, .expected = "[]const u8" },
        .{ .typ = schema.Type.data, .expected = "[]const u8" },
        .{ .typ = schema.Type.any_pointer, .expected = "message.AnyPointerReader" },
        .{ .typ = @as(schema.Type, .{ .interface = .{ .type_id = 0 } }), .expected = "message.Capability" },
    };

    inline for (cases) |case| {
        const result = try sg.readerTypeString(case.typ);
        defer alloc.free(result);
        try std.testing.expectEqualStrings(case.expected, result);
    }
}

test "StructGenerator.writerTypeString maps primitive types" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    const cases = .{
        .{ .typ = schema.Type.void, .expected = "void" },
        .{ .typ = schema.Type.bool, .expected = "bool" },
        .{ .typ = schema.Type.int8, .expected = "i8" },
        .{ .typ = schema.Type.uint32, .expected = "u32" },
        .{ .typ = schema.Type.float64, .expected = "f64" },
        .{ .typ = schema.Type.text, .expected = "[]const u8" },
        .{ .typ = schema.Type.data, .expected = "[]const u8" },
    };

    inline for (cases) |case| {
        const result = try sg.writerTypeString(case.typ);
        defer alloc.free(result);
        try std.testing.expectEqualStrings(case.expected, result);
    }
}

test "StructGenerator.writeByteArrayLiteral formats bytes" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(alloc);
    const writer = ArrayListWriter{ .list = &buf, .allocator = alloc };

    try sg.writeByteArrayLiteral(writer, &[_]u8{ 0xDE, 0xAD });
    try std.testing.expectEqualStrings("&[_]u8{0xDE, 0xAD}", buf.items);
}

test "StructGenerator.getSimpleName extracts name from display_name" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    const node = schema.Node{
        .id = 1,
        .display_name = "file.capnp:Nested",
        .display_name_prefix_length = 11,
        .scope_id = 0,
        .kind = .file,
        .nested_nodes = &.{},
        .annotations = &.{},
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
    try std.testing.expectEqualStrings("Nested", sg.getSimpleName(&node));
}

test "StructGenerator.defaultPointerBytes extracts bytes from pointer defaults" {
    const alloc = std.testing.allocator;
    var sg = StructGenerator.init(alloc);

    const list_bytes = [_]u8{ 1, 2, 3 };
    const list_val = schema.Value{ .list = .{ .message_bytes = &list_bytes } };
    try std.testing.expectEqualSlices(u8, &list_bytes, sg.defaultPointerBytes(list_val).?);

    const struct_bytes = [_]u8{ 4, 5 };
    const struct_val = schema.Value{ .@"struct" = .{ .message_bytes = &struct_bytes } };
    try std.testing.expectEqualSlices(u8, &struct_bytes, sg.defaultPointerBytes(struct_val).?);

    const non_pointer = schema.Value{ .uint32 = 0 };
    try std.testing.expect(sg.defaultPointerBytes(non_pointer) == null);
}
