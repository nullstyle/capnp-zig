const std = @import("std");
const schema = @import("../serialization/schema.zig");
const StructGenerator = @import("struct_gen.zig").StructGenerator;
const types = @import("types.zig");
pub const TypeGenerator = types.TypeGenerator;

/// Minimal writer wrapping an unmanaged `ArrayList(u8)` with an explicit
/// allocator, providing `writeAll`, `print`, and `writeByte` methods
/// compatible with the duck-typed writer pattern used by the code generator.
pub const ArrayListWriter = struct {
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,

    pub fn writeAll(self: ArrayListWriter, bytes: []const u8) !void {
        try self.list.appendSlice(self.allocator, bytes);
    }

    pub fn print(self: ArrayListWriter, comptime fmt: []const u8, args: anytype) !void {
        try self.list.print(self.allocator, fmt, args);
    }

    pub fn writeByte(self: ArrayListWriter, byte: u8) !void {
        try self.list.append(self.allocator, byte);
    }

    pub fn writeByteNTimes(self: ArrayListWriter, byte: u8, n: usize) !void {
        try self.list.appendNTimes(self.allocator, byte, n);
    }
};

/// Code generation driver that turns a set of parsed Cap'n Proto schema nodes
/// into idiomatic Zig source code with Reader and Builder types for each struct.
pub const Generator = struct {
    pub const ApiProfile = StructGenerator.ApiProfile;

    allocator: std.mem.Allocator,
    nodes: []const schema.Node,
    node_map: std.AutoHashMap(schema.Id, usize),
    /// Set during generateFile to the current file's node ID.
    current_file_id: ?schema.Id = null,
    /// Maps imported file node IDs to their Zig module const names.
    import_modules: std.AutoHashMap(schema.Id, []const u8),
    /// Tracks imported files actually referenced by generated type resolution.
    used_import_file_ids: std.AutoHashMap(schema.Id, void),
    /// Emit codegen trace logs when true.
    verbose: bool = false,
    /// Emit CAPNP_SCHEMA_MANIFEST_JSON and capnpSchemaManifestJson() when true.
    emit_schema_manifest: bool = true,
    /// Controls generated Reader/Builder convenience API surface.
    api_profile: ApiProfile = .full,
    /// When enabled, reuse the first emitted struct declaration for later
    /// structs with identical generated bodies.
    shape_sharing: bool = false,
    shape_share_map: std.StringHashMap([]const u8),

    const GeneratedNameScope = struct {
        allocator: std.mem.Allocator,
        names: std.ArrayList([]const u8) = .empty,

        fn init(allocator: std.mem.Allocator) GeneratedNameScope {
            return .{ .allocator = allocator };
        }

        fn deinit(self: *GeneratedNameScope) void {
            for (self.names.items) |name| {
                self.allocator.free(name);
            }
            self.names.deinit(self.allocator);
        }

        fn addOwned(self: *GeneratedNameScope, name: []const u8) !void {
            errdefer self.allocator.free(name);
            for (self.names.items) |existing| {
                if (std.mem.eql(u8, existing, name)) return error.DuplicateGeneratedName;
            }
            try self.names.append(self.allocator, name);
        }

        fn addCopy(self: *GeneratedNameScope, name: []const u8) !void {
            const owned = try self.allocator.dupe(u8, name);
            try self.addOwned(owned);
        }

        fn addPrint(self: *GeneratedNameScope, comptime fmt: []const u8, args: anytype) !void {
            const name = try std.fmt.allocPrint(self.allocator, fmt, args);
            try self.addOwned(name);
        }
    };

    /// Build a generator from the full set of schema nodes, indexing them by ID.
    pub fn init(allocator: std.mem.Allocator, nodes: []const schema.Node) !Generator {
        var node_map = std.AutoHashMap(schema.Id, usize).init(allocator);
        errdefer node_map.deinit();

        for (nodes, 0..) |node, i| {
            try node_map.put(node.id, i);
        }

        return .{
            .allocator = allocator,
            .nodes = nodes,
            .node_map = node_map,
            .import_modules = std.AutoHashMap(schema.Id, []const u8).init(allocator),
            .used_import_file_ids = std.AutoHashMap(schema.Id, void).init(allocator),
            .shape_share_map = std.StringHashMap([]const u8).init(allocator),
        };
    }

    pub fn deinit(self: *Generator) void {
        self.clearShapeShareMap();
        self.shape_share_map.deinit();
        self.clearImportModules();
        self.import_modules.deinit();
        self.used_import_file_ids.deinit();
        self.node_map.deinit();
    }

    /// Enable/disable verbose codegen traces.
    pub fn setVerbose(self: *Generator, verbose: bool) void {
        self.verbose = verbose;
    }

    /// Enable/disable schema manifest emission in generated files.
    pub fn setEmitSchemaManifest(self: *Generator, emit: bool) void {
        self.emit_schema_manifest = emit;
    }

    /// Set generation profile for struct Reader/Builder convenience APIs.
    pub fn setApiProfile(self: *Generator, profile: ApiProfile) void {
        self.api_profile = profile;
    }

    /// Enable/disable shape-based sharing for identical struct declarations.
    pub fn setShapeSharing(self: *Generator, enabled: bool) void {
        self.shape_sharing = enabled;
    }

    fn clearImportModules(self: *Generator) void {
        var it = self.import_modules.valueIterator();
        while (it.next()) |v| self.allocator.free(v.*);
        self.import_modules.clearRetainingCapacity();
    }

    fn clearShapeShareMap(self: *Generator) void {
        var it = self.shape_share_map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.shape_share_map.clearRetainingCapacity();
    }

    fn verboseLog(self: *const Generator, comptime fmt: []const u8, args: anytype) void {
        if (!self.verbose) return;
        std.debug.print(fmt, args);
    }

    /// Get a node by its ID
    pub fn getNode(self: *const Generator, id: schema.Id) ?*const schema.Node {
        const index = self.node_map.get(id) orelse return null;
        return &self.nodes[index];
    }

    /// Generate Zig source code for a single requested `.capnp` file.
    ///
    /// Walks the file node's nested declarations, emitting struct/enum/const
    /// definitions. Returns an allocator-owned byte slice containing the
    /// generated `.zig` source.
    pub fn generateFile(self: *Generator, requested_file: schema.RequestedFile) ![]const u8 {
        try validateRelativeSchemaPath(requested_file.filename);

        self.verboseLog("capnpc-zig: generating file {s}\n", .{requested_file.filename});

        // Set current file context for cross-file type resolution.
        self.current_file_id = requested_file.id;
        defer self.current_file_id = null;
        self.clearImportModules();
        self.used_import_file_ids.clearRetainingCapacity();
        self.clearShapeShareMap();

        const file_node = self.getNode(requested_file.id) orelse return error.FileNodeNotFound;
        const needs_rpc = try self.fileNeedsRpc(file_node);
        try self.validateGeneratedNames(file_node, needs_rpc);

        // Register import module aliases up-front so cross-file type resolution
        // works while generating declarations.
        var import_aliases = std.StringHashMap(void).init(self.allocator);
        defer import_aliases.deinit();
        for (requested_file.imports) |imp| {
            const mod_name = try self.uniqueImportModuleName(imp.name, &import_aliases);
            errdefer self.allocator.free(mod_name);
            try self.import_modules.put(imp.id, mod_name);
        }

        // Generate declarations into a body buffer first, then emit only imports
        // that are actually referenced by generated declarations.
        var body = std.ArrayList(u8).empty;
        defer body.deinit(self.allocator);
        const body_writer = ArrayListWriter{ .list = &body, .allocator = self.allocator };

        if (self.emit_schema_manifest) {
            try self.writeSchemaManifest(requested_file, file_node, body_writer);
        }

        var generated = std.AutoHashMap(schema.Id, void).init(self.allocator);
        defer generated.deinit();

        // Generate code for all nested nodes (including nested definitions).
        for (file_node.nested_nodes) |nested| {
            try self.generateNodeRecursive(nested.id, &generated, &body);
        }

        var output = std.ArrayList(u8).empty;
        errdefer output.deinit(self.allocator);
        const writer = ArrayListWriter{ .list = &output, .allocator = self.allocator };

        // Write file header
        try writer.writeAll("// Generated by capnpc-zig\n");
        try writer.print("// Source: {f}\n\n", .{std.zig.fmtString(requested_file.filename)});
        try writer.writeAll("const std = @import(\"std\");\n");
        try writer.writeAll("const capnpc = @import(\"capnpc-zig\");\n");
        try writer.writeAll("const message = capnpc.message;\n");
        try writer.writeAll("const schema = capnpc.schema;\n");
        if (needs_rpc) {
            try writer.writeAll("const rpc = capnpc.rpc;\n");
        }

        // Emit only imports that are referenced by generated declarations.
        for (requested_file.imports) |imp| {
            if (!self.used_import_file_ids.contains(imp.id)) continue;
            const mod_name = self.import_modules.get(imp.id) orelse continue;
            const import_path = try self.importPathFromCapnpName(imp.name);
            defer self.allocator.free(import_path);
            try writer.print("const {s} = @import(\"{f}\");\n", .{ mod_name, std.zig.fmtString(import_path) });
        }
        try writer.writeByte('\n');

        try writer.writeAll(body.items);

        return output.toOwnedSlice(self.allocator);
    }

    const ManifestSerdeEntry = struct {
        id: u64,
        type_name: []const u8,
        to_json_export: []const u8,
        from_json_export: []const u8,
    };

    const ManifestSerdeJsonEntry = struct {
        id: u64,
        type_name: []const u8,
        to_json_export: []const u8,
        from_json_export: []const u8,
    };

    const SchemaManifestJson = struct {
        schema: []const u8,
        module: []const u8,
        serde: []const ManifestSerdeJsonEntry,
    };

    fn writeSchemaManifest(
        self: *Generator,
        requested_file: schema.RequestedFile,
        file_node: *const schema.Node,
        writer: anytype,
    ) !void {
        const module_name = try self.moduleNameFromFilename(requested_file.filename);
        defer self.allocator.free(module_name);

        var seen = std.AutoHashMap(schema.Id, void).init(self.allocator);
        defer seen.deinit();

        var entries = std.ArrayList(ManifestSerdeEntry).empty;
        defer {
            for (entries.items) |entry| {
                self.allocator.free(entry.type_name);
                self.allocator.free(entry.to_json_export);
                self.allocator.free(entry.from_json_export);
            }
            entries.deinit(self.allocator);
        }

        for (file_node.nested_nodes) |nested| {
            try self.collectManifestSerdeEntries(nested.id, module_name, &seen, &entries);
        }

        self.sortManifestSerdeEntries(entries.items);

        var json_entries = try self.allocator.alloc(ManifestSerdeJsonEntry, entries.items.len);
        defer self.allocator.free(json_entries);
        for (entries.items, 0..) |entry, i| {
            json_entries[i] = .{
                .id = entry.id,
                .type_name = entry.type_name,
                .to_json_export = entry.to_json_export,
                .from_json_export = entry.from_json_export,
            };
        }

        const manifest_json_bytes = try std.json.Stringify.valueAlloc(self.allocator, SchemaManifestJson{
            .schema = requested_file.filename,
            .module = module_name,
            .serde = json_entries,
        }, .{});
        defer self.allocator.free(manifest_json_bytes);

        try writer.print("pub const CAPNP_SCHEMA_MANIFEST_JSON: []const u8 = \"{f}\";\n", .{
            std.zig.fmtString(manifest_json_bytes),
        });
        try writer.writeAll("pub fn capnpSchemaManifestJson() []const u8 {\n");
        try writer.writeAll("    return CAPNP_SCHEMA_MANIFEST_JSON;\n");
        try writer.writeAll("}\n\n");
    }

    fn collectManifestSerdeEntries(
        self: *Generator,
        id: schema.Id,
        module_name: []const u8,
        seen: *std.AutoHashMap(schema.Id, void),
        entries: *std.ArrayList(ManifestSerdeEntry),
    ) !void {
        if (seen.contains(id)) return;
        const node = self.getNode(id) orelse return;
        try seen.put(id, {});

        if (node.kind == .@"struct") {
            const simple_name = self.getSimpleName(node);
            const type_name = try self.toZigIdentifier(simple_name);
            errdefer self.allocator.free(type_name);
            const type_export = try self.toSnakeCaseLower(simple_name);
            defer self.allocator.free(type_export);

            const to_json = try std.fmt.allocPrint(
                self.allocator,
                "capnp_{s}_{s}_to_json",
                .{ module_name, type_export },
            );
            errdefer self.allocator.free(to_json);
            const from_json = try std.fmt.allocPrint(
                self.allocator,
                "capnp_{s}_{s}_from_json",
                .{ module_name, type_export },
            );
            errdefer self.allocator.free(from_json);

            try entries.append(self.allocator, .{
                .id = node.id,
                .type_name = type_name,
                .to_json_export = to_json,
                .from_json_export = from_json,
            });
        }

        for (node.nested_nodes) |nested| {
            try self.collectManifestSerdeEntries(nested.id, module_name, seen, entries);
        }

        if (node.kind == .interface) {
            const iface = node.interface_node orelse return;
            for (iface.methods) |method| {
                try self.collectManifestSerdeEntries(method.param_struct_type, module_name, seen, entries);
                try self.collectManifestSerdeEntries(method.result_struct_type, module_name, seen, entries);
            }
            // Also include superclass method param/result types
            for (iface.superclasses) |parent_id| {
                const parent_node = self.getNode(parent_id) orelse continue;
                const parent_iface = parent_node.interface_node orelse continue;
                for (parent_iface.methods) |method| {
                    try self.collectManifestSerdeEntries(method.param_struct_type, module_name, seen, entries);
                    try self.collectManifestSerdeEntries(method.result_struct_type, module_name, seen, entries);
                }
            }
        }
    }

    fn sortManifestSerdeEntries(self: *Generator, entries: []ManifestSerdeEntry) void {
        _ = self;
        var i: usize = 1;
        while (i < entries.len) : (i += 1) {
            var j = i;
            while (j > 0 and manifestSerdeEntryLess(entries[j], entries[j - 1])) : (j -= 1) {
                std.mem.swap(ManifestSerdeEntry, &entries[j], &entries[j - 1]);
            }
        }
    }

    fn manifestSerdeEntryLess(a: ManifestSerdeEntry, b: ManifestSerdeEntry) bool {
        const type_order = std.mem.order(u8, a.type_name, b.type_name);
        if (type_order == .lt) return true;
        if (type_order == .gt) return false;
        return a.id < b.id;
    }

    /// Generate code for a single node
    fn generateNode(self: *Generator, node: *const schema.Node, output: *std.ArrayList(u8)) !void {
        self.verboseLog("capnpc-zig: generating node id=0x{x} kind={s}\n", .{ node.id, @tagName(node.kind) });
        const writer = ArrayListWriter{ .list = output, .allocator = self.allocator };

        switch (node.kind) {
            .@"struct" => {
                if (self.shape_sharing) {
                    try self.generateStructWithShapeSharing(node, writer);
                } else {
                    try self.generateStruct(node, writer);
                }
            },
            .@"enum" => try self.generateEnum(node, writer),
            .interface => try self.generateInterface(node, writer),
            .@"const" => try self.generateConst(node, writer),
            .file => {}, // File nodes are handled separately
            .annotation => try self.generateAnnotation(node, writer),
        }

        if (node.kind != .file) {
            try self.generateAnnotationUses(node, writer);
        }
    }

    // NOTE: Nested types with the same simple name in different parents will collide
    // at the file scope. Cap'n Proto's own compiler prevents this for well-formed schemas,
    // but hand-crafted binary schemas could trigger it.
    fn generateNodeRecursive(
        self: *Generator,
        id: schema.Id,
        generated: *std.AutoHashMap(schema.Id, void),
        output: *std.ArrayList(u8),
    ) !void {
        if (generated.contains(id)) return;
        const node = self.getNode(id) orelse return;
        try generated.put(id, {});

        // Mark group nodes as generated so they don't get generated as top-level types
        // (they are generated inline by their parent struct)
        if (node.kind == .@"struct") {
            if (node.struct_node) |struct_node| {
                for (struct_node.fields) |field| {
                    if (field.group) |group| {
                        try generated.put(group.type_id, {});
                    }
                }
            }
        }

        try self.generateNode(node, output);
        for (node.nested_nodes) |nested| {
            try self.generateNodeRecursive(nested.id, generated, output);
        }
        if (node.kind == .interface) {
            const iface = node.interface_node orelse return;
            for (iface.methods) |method| {
                try self.generateNodeRecursive(method.param_struct_type, generated, output);
                try self.generateNodeRecursive(method.result_struct_type, generated, output);
            }
            // Also ensure superclass method param/result types are generated
            for (iface.superclasses) |parent_id| {
                const parent_node = self.getNode(parent_id) orelse continue;
                const parent_iface = parent_node.interface_node orelse continue;
                for (parent_iface.methods) |method| {
                    try self.generateNodeRecursive(method.param_struct_type, generated, output);
                    try self.generateNodeRecursive(method.result_struct_type, generated, output);
                }
            }
        }
    }

    fn validateGeneratedNames(self: *Generator, file_node: *const schema.Node, needs_rpc: bool) !void {
        var generated = std.AutoHashMap(schema.Id, void).init(self.allocator);
        defer generated.deinit();

        var file_scope = GeneratedNameScope.init(self.allocator);
        defer file_scope.deinit();
        try file_scope.addCopy("std");
        try file_scope.addCopy("capnpc");
        try file_scope.addCopy("message");
        try file_scope.addCopy("schema");
        if (needs_rpc) try file_scope.addCopy("rpc");
        if (self.emit_schema_manifest) {
            try file_scope.addCopy("CAPNP_SCHEMA_MANIFEST_JSON");
            try file_scope.addCopy("capnpSchemaManifestJson");
        }

        for (file_node.nested_nodes) |nested| {
            try self.validateGeneratedNodeRecursive(nested.id, &generated, &file_scope);
        }
    }

    fn validateGeneratedNodeRecursive(
        self: *Generator,
        id: schema.Id,
        generated: *std.AutoHashMap(schema.Id, void),
        file_scope: *GeneratedNameScope,
    ) !void {
        if (generated.contains(id)) return;
        const node = self.getNode(id) orelse return;
        try generated.put(id, {});

        if (node.kind == .@"struct") {
            if (node.struct_node) |struct_node| {
                for (struct_node.fields) |field| {
                    if (field.group) |group| {
                        try generated.put(group.type_id, {});
                    }
                }
            }
        }

        try self.validateNodeGeneratedNames(node, file_scope);

        for (node.nested_nodes) |nested| {
            try self.validateGeneratedNodeRecursive(nested.id, generated, file_scope);
        }

        if (node.kind == .interface) {
            const iface = node.interface_node orelse return;
            for (iface.methods) |method| {
                try self.validateGeneratedNodeRecursive(method.param_struct_type, generated, file_scope);
                try self.validateGeneratedNodeRecursive(method.result_struct_type, generated, file_scope);
            }
            for (iface.superclasses) |parent_id| {
                const parent_node = self.getNode(parent_id) orelse continue;
                const parent_iface = parent_node.interface_node orelse continue;
                for (parent_iface.methods) |method| {
                    try self.validateGeneratedNodeRecursive(method.param_struct_type, generated, file_scope);
                    try self.validateGeneratedNodeRecursive(method.result_struct_type, generated, file_scope);
                }
            }
        }
    }

    fn validateNodeGeneratedNames(
        self: *Generator,
        node: *const schema.Node,
        file_scope: *GeneratedNameScope,
    ) !void {
        switch (node.kind) {
            .@"struct" => {
                const name = try self.allocTypeDeclName(node);
                try file_scope.addOwned(name);
                try self.validateStructGeneratedNames(node);
            },
            .@"enum" => {
                const name = try self.allocTypeDeclName(node);
                try file_scope.addOwned(name);
                try self.validateEnumGeneratedNames(node);
            },
            .interface => {
                const name = try self.allocTypeDeclName(node);
                try file_scope.addOwned(name);
                try self.validateInterfaceGeneratedNames(node);
            },
            .@"const", .annotation => {
                const name = try self.allocValueDeclName(node);
                try file_scope.addOwned(name);
            },
            .file => {},
        }
    }

    const StructListHelperUsage = struct {
        enum_list: bool = false,
        struct_list: bool = false,
        data_list: bool = false,
        capability_list: bool = false,
    };

    fn validateStructGeneratedNames(self: *Generator, node: *const schema.Node) !void {
        const struct_info = node.struct_node orelse return error.InvalidStructNode;

        var struct_scope = GeneratedNameScope.init(self.allocator);
        defer struct_scope.deinit();
        try struct_scope.addCopy("Reader");
        try struct_scope.addCopy("Builder");

        const usage = try self.collectStructListHelperUsage(struct_info.fields);
        if (usage.enum_list) {
            try struct_scope.addCopy("EnumListReader");
            try struct_scope.addCopy("EnumListBuilder");
        }
        if (usage.struct_list) {
            try struct_scope.addCopy("StructListReader");
            try struct_scope.addCopy("StructListBuilder");
        }
        if (usage.data_list) {
            try struct_scope.addCopy("DataListReader");
            try struct_scope.addCopy("DataListBuilder");
        }
        if (usage.capability_list) {
            try struct_scope.addCopy("CapabilityListReader");
            try struct_scope.addCopy("CapabilityListBuilder");
        }

        if (struct_info.discriminant_count > 0) {
            try struct_scope.addCopy("WhichTag");
        }

        for (struct_info.fields) |field| {
            const group = field.group orelse continue;
            const group_node = self.getNode(group.type_id) orelse continue;
            const group_name = try self.allocTypeDeclName(group_node);
            try struct_scope.addOwned(group_name);
            try self.validateStructGeneratedNames(group_node);
        }

        try self.validateStructReaderNames(struct_info);
        try self.validateStructBuilderNames(struct_info);
        if (struct_info.discriminant_count > 0) {
            try self.validateUnionTagNames(struct_info.fields);
        }
    }

    fn collectStructListHelperUsage(self: *Generator, fields: []const schema.Field) !StructListHelperUsage {
        var usage = StructListHelperUsage{};
        try self.collectStructListHelperUsageFromFields(fields, &usage);
        return usage;
    }

    fn collectStructListHelperUsageFromFields(
        self: *Generator,
        fields: []const schema.Field,
        usage: *StructListHelperUsage,
    ) !void {
        for (fields) |field| {
            if (field.group) |group| {
                const group_node = self.getNode(group.type_id) orelse continue;
                const group_struct_info = group_node.struct_node orelse continue;
                try self.collectStructListHelperUsageFromFields(group_struct_info.fields, usage);
            }

            const slot = field.slot orelse continue;
            if (slot.type != .list) continue;
            switch (slot.type.list.element_type.*) {
                .@"enum" => |enum_info| {
                    const enum_node = self.getNode(enum_info.type_id) orelse continue;
                    if (enum_node.kind == .@"enum") usage.enum_list = true;
                },
                .@"struct" => |struct_info| {
                    const struct_node = self.getNode(struct_info.type_id) orelse continue;
                    if (struct_node.kind == .@"struct") usage.struct_list = true;
                },
                .data => usage.data_list = true,
                .interface => usage.capability_list = true,
                else => {},
            }
        }
    }

    fn validateStructReaderNames(self: *Generator, struct_info: schema.StructNode) !void {
        var scope = GeneratedNameScope.init(self.allocator);
        defer scope.deinit();

        try scope.addCopy("_reader");
        try scope.addCopy("wrap");
        if (self.api_profile == .full) try scope.addCopy("init");
        if (struct_info.discriminant_count > 0) try scope.addCopy("which");

        for (struct_info.fields) |field| {
            if (field.group == null and field.slot == null) continue;

            const cap_name = try self.allocFieldCapName(field.name);
            defer self.allocator.free(cap_name);

            try scope.addPrint("get{s}", .{cap_name});

            if (field.slot) |slot| {
                if (self.defaultPointerBytes(slot.default_value)) |_| {
                    const default_name = try self.allocDefaultConstName(field.name);
                    defer self.allocator.free(default_name);
                    try scope.addPrint("{s}_bytes", .{default_name});
                    try scope.addPrint("{s}_segments", .{default_name});
                    try scope.addPrint("{s}_message", .{default_name});
                    try scope.addCopy(default_name);
                }

                if (slot.type == .interface) {
                    try scope.addPrint("resolve{s}", .{cap_name});
                } else if (slot.type == .list and slot.type.list.element_type.* == .interface) {
                    try scope.addPrint("resolve{s}", .{cap_name});
                }
            }
        }
    }

    fn validateStructBuilderNames(self: *Generator, struct_info: schema.StructNode) !void {
        var scope = GeneratedNameScope.init(self.allocator);
        defer scope.deinit();

        try scope.addCopy("_builder");
        try scope.addCopy("wrap");
        if (self.api_profile == .full) try scope.addCopy("init");

        for (struct_info.fields) |field| {
            const cap_name = try self.allocFieldCapName(field.name);
            defer self.allocator.free(cap_name);

            if (field.group != null) {
                if (field.discriminant_value != 0xFFFF and struct_info.discriminant_count > 0) {
                    try scope.addPrint("init{s}", .{cap_name});
                } else {
                    try scope.addPrint("get{s}", .{cap_name});
                }
                continue;
            }

            const slot = field.slot orelse continue;
            switch (slot.type) {
                .list, .@"struct" => try scope.addPrint("init{s}", .{cap_name}),
                .any_pointer => {
                    try scope.addPrint("init{s}", .{cap_name});
                    try scope.addPrint("set{s}Null", .{cap_name});
                    try scope.addPrint("set{s}Text", .{cap_name});
                    try scope.addPrint("set{s}Data", .{cap_name});
                    try scope.addPrint("set{s}Capability", .{cap_name});
                },
                .interface => {
                    try scope.addPrint("init{s}", .{cap_name});
                    try scope.addPrint("clear{s}", .{cap_name});
                    try scope.addPrint("set{s}Capability", .{cap_name});
                    try scope.addPrint("set{s}Server", .{cap_name});
                    try scope.addPrint("set{s}Client", .{cap_name});
                },
                else => try scope.addPrint("set{s}", .{cap_name}),
            }
        }
    }

    fn validateUnionTagNames(self: *Generator, fields: []const schema.Field) !void {
        var scope = GeneratedNameScope.init(self.allocator);
        defer scope.deinit();

        for (fields) |field| {
            if (field.discriminant_value == 0xFFFF) continue;
            const tag_name = try self.allocEscapedTypeIdentifier(field.name);
            try scope.addOwned(tag_name);
        }
    }

    fn validateEnumGeneratedNames(self: *Generator, node: *const schema.Node) !void {
        const enum_info = node.enum_node orelse return error.InvalidEnumNode;

        var scope = GeneratedNameScope.init(self.allocator);
        defer scope.deinit();

        for (enum_info.enumerants) |enumerant| {
            const name = try self.allocEscapedTypeIdentifier(enumerant.name);
            try scope.addOwned(name);
        }
    }

    fn validateInterfaceGeneratedNames(self: *Generator, node: *const schema.Node) !void {
        const interface_info = node.interface_node orelse return error.InvalidInterfaceNode;

        var interface_scope = GeneratedNameScope.init(self.allocator);
        defer interface_scope.deinit();

        try interface_scope.addCopy("interface_id");
        try interface_scope.addCopy("Method");
        try interface_scope.addCopy("Client");
        if (self.interfaceHasStreamingMethods(interface_info)) {
            try interface_scope.addCopy("StreamClient");
        }
        try interface_scope.addCopy("PipelinedClient");
        try interface_scope.addCopy("BootstrapResponse");
        try interface_scope.addCopy("BootstrapCallback");
        try interface_scope.addCopy("BootstrapContext");
        try interface_scope.addCopy("Server");
        try interface_scope.addCopy("VTable");
        try interface_scope.addCopy("bootstrapReturn");
        try interface_scope.addCopy("bootstrap");
        try interface_scope.addCopy("exportServer");
        try interface_scope.addCopy("setBootstrap");
        try interface_scope.addCopy("onCall");

        var method_enum_scope = GeneratedNameScope.init(self.allocator);
        defer method_enum_scope.deinit();

        var client_scope = GeneratedNameScope.init(self.allocator);
        defer client_scope.deinit();
        try client_scope.addCopy("peer");
        try client_scope.addCopy("cap_id");
        try client_scope.addCopy("init");
        try client_scope.addCopy("fromBootstrap");

        var pipelined_scope = GeneratedNameScope.init(self.allocator);
        defer pipelined_scope.deinit();
        try pipelined_scope.addCopy("peer");
        try pipelined_scope.addCopy("question_id");
        try pipelined_scope.addCopy("pointer_index");

        var stream_scope = GeneratedNameScope.init(self.allocator);
        defer stream_scope.deinit();
        if (self.interfaceHasStreamingMethods(interface_info)) {
            try stream_scope.addCopy("client");
            try stream_scope.addCopy("stream");
            try stream_scope.addCopy("init");
            try stream_scope.addCopy("waitStreaming");
        }

        var vtable_scope = GeneratedNameScope.init(self.allocator);
        defer vtable_scope.deinit();

        for (interface_info.methods) |method| {
            const method_name = try self.allocEscapedTypeIdentifier(method.name);
            try interface_scope.addOwned(method_name);

            const enum_name = try self.allocEscapedTypeIdentifier(method.name);
            try method_enum_scope.addOwned(enum_name);

            const call_name = try self.allocMethodCallName(method.name);
            defer self.allocator.free(call_name);
            try client_scope.addCopy(call_name);
            try pipelined_scope.addCopy(call_name);
            if (self.interfaceHasStreamingMethods(interface_info)) {
                try stream_scope.addCopy(call_name);
            }

            if (try self.methodHasInterfaceResultFields(method)) {
                const pipeline_name = try self.allocMethodPipelineName(method.name);
                try interface_scope.addOwned(pipeline_name);
                try self.validatePipelineGeneratedNames(method);
                try client_scope.addPrint("{s}Pipelined", .{call_name});
            }

            const field_name = try self.allocMethodVTableFieldName(method.name);
            defer self.allocator.free(field_name);
            try vtable_scope.addCopy(field_name);
            if (!method.isStreaming()) {
                try vtable_scope.addPrint("{s}_deferred", .{field_name});
            }
        }
    }

    fn validatePipelineGeneratedNames(self: *Generator, method: schema.Method) !void {
        const result_node = self.getNode(method.result_struct_type) orelse return;
        const result_struct = result_node.struct_node orelse return;

        var scope = GeneratedNameScope.init(self.allocator);
        defer scope.deinit();
        try scope.addCopy("peer");
        try scope.addCopy("question_id");

        for (result_struct.fields) |field| {
            const slot = field.slot orelse continue;
            if (slot.type != .interface) continue;
            const field_name = try types.identToZigTypeName(self.allocator, field.name);
            defer self.allocator.free(field_name);
            try scope.addPrint("get{s}", .{field_name});
        }
    }

    fn interfaceHasStreamingMethods(self: *Generator, interface_info: schema.InterfaceNode) bool {
        _ = self;
        for (interface_info.methods) |method| {
            if (method.isStreaming()) return true;
        }
        return false;
    }

    fn methodHasInterfaceResultFields(self: *Generator, method: schema.Method) !bool {
        const result_node = self.getNode(method.result_struct_type) orelse return false;
        const result_struct = result_node.struct_node orelse return false;
        for (result_struct.fields) |field| {
            const slot = field.slot orelse continue;
            if (slot.type == .interface) return true;
        }
        return false;
    }

    fn allocEscapedTypeIdentifier(self: *Generator, name: []const u8) ![]const u8 {
        const zig_name = try self.toZigIdentifier(name);
        defer self.allocator.free(zig_name);
        return types.escapeZigKeyword(self.allocator, zig_name);
    }

    fn allocFieldCapName(self: *Generator, name: []const u8) ![]const u8 {
        const zig_name = try types.identToZigValueName(self.allocator, name);
        defer self.allocator.free(zig_name);
        return self.capitalizeFirst(zig_name);
    }

    fn allocDefaultConstName(self: *Generator, field_name: []const u8) ![]const u8 {
        const zig_name = try types.identToZigValueName(self.allocator, field_name);
        defer self.allocator.free(zig_name);
        return std.fmt.allocPrint(self.allocator, "_default_{s}", .{zig_name});
    }

    fn allocMethodCallName(self: *Generator, method_name: []const u8) ![]const u8 {
        const zig_name = try self.toZigIdentifier(method_name);
        defer self.allocator.free(zig_name);
        return std.fmt.allocPrint(self.allocator, "call{s}", .{zig_name});
    }

    fn allocMethodPipelineName(self: *Generator, method_name: []const u8) ![]const u8 {
        const method_name_zig = try self.allocEscapedTypeIdentifier(method_name);
        defer self.allocator.free(method_name_zig);
        return std.fmt.allocPrint(self.allocator, "{s}Pipeline", .{method_name_zig});
    }

    fn allocMethodVTableFieldName(self: *Generator, method_name: []const u8) ![]const u8 {
        const zig_name = try self.toZigIdentifier(method_name);
        defer self.allocator.free(zig_name);
        const method_field = try self.lowerFirst(zig_name);
        defer self.allocator.free(method_field);
        return types.escapeZigKeyword(self.allocator, method_field);
    }

    fn defaultPointerBytes(self: *Generator, value: ?schema.Value) ?[]const u8 {
        _ = self;
        const v = value orelse return null;
        return switch (v) {
            .list => |info| info.message_bytes,
            .@"struct" => |info| info.message_bytes,
            .any_pointer => |info| info.message_bytes,
            else => null,
        };
    }

    fn fileNeedsRpc(self: *Generator, file_node: *const schema.Node) !bool {
        var visited = std.AutoHashMap(schema.Id, void).init(self.allocator);
        defer visited.deinit();

        for (file_node.nested_nodes) |nested| {
            if (try self.nodeNeedsRpcRecursive(nested.id, &visited)) return true;
        }
        return false;
    }

    fn nodeNeedsRpcRecursive(
        self: *Generator,
        id: schema.Id,
        visited: *std.AutoHashMap(schema.Id, void),
    ) !bool {
        if (visited.contains(id)) return false;
        try visited.put(id, {});

        const node = self.getNode(id) orelse return false;
        if (self.nodeNeedsRpc(node)) return true;

        for (node.nested_nodes) |nested| {
            if (try self.nodeNeedsRpcRecursive(nested.id, visited)) return true;
        }

        if (node.kind == .interface) {
            const iface = node.interface_node orelse return false;
            for (iface.methods) |method| {
                if (try self.nodeNeedsRpcRecursive(method.param_struct_type, visited)) return true;
                if (try self.nodeNeedsRpcRecursive(method.result_struct_type, visited)) return true;
            }
            for (iface.superclasses) |parent_id| {
                if (try self.nodeNeedsRpcRecursive(parent_id, visited)) return true;
            }
        }

        return false;
    }

    fn nodeNeedsRpc(self: *Generator, node: *const schema.Node) bool {
        return switch (node.kind) {
            .interface => true,
            .@"struct" => blk: {
                const struct_node = node.struct_node orelse break :blk false;
                for (struct_node.fields) |field| {
                    const slot = field.slot orelse continue;
                    if (self.typeNeedsRpc(slot.type)) break :blk true;
                }
                break :blk false;
            },
            else => false,
        };
    }

    fn typeNeedsRpc(self: *Generator, typ: schema.Type) bool {
        return switch (typ) {
            .interface => true,
            .list => |list_info| self.typeNeedsRpc(list_info.element_type.*),
            else => false,
        };
    }

    /// Generate a struct definition
    fn generateStruct(self: *Generator, node: *const schema.Node, writer: anytype) !void {
        var struct_gen = StructGenerator.initWithLookup(self.allocator, lookupNode, self);
        struct_gen.type_prefix_fn = lookupTypePrefix;
        struct_gen.setApiProfile(self.api_profile);
        try struct_gen.generate(node, writer);
    }

    /// Generate a struct definition with optional shape sharing.
    ///
    /// Reuses the first declaration for identical struct bodies by emitting a
    /// type alias for later occurrences.
    fn generateStructWithShapeSharing(self: *Generator, node: *const schema.Node, writer: anytype) !void {
        var struct_buf = std.ArrayList(u8).empty;
        defer struct_buf.deinit(self.allocator);

        {
            const struct_writer = ArrayListWriter{ .list = &struct_buf, .allocator = self.allocator };
            try self.generateStruct(node, struct_writer);
        }

        if (struct_buf.items.len == 0) return;
        const first_newline = std.mem.indexOfScalar(u8, struct_buf.items, '\n') orelse return;
        const shape_key_slice = struct_buf.items[first_newline + 1 ..];
        const decl_name = try self.allocTypeDeclName(node);
        defer self.allocator.free(decl_name);

        if (self.shape_share_map.get(shape_key_slice)) |canonical_name| {
            try writer.print("pub const {s} = {s};\n\n", .{ decl_name, canonical_name });
            return;
        }

        const owned_key = try self.allocator.dupe(u8, shape_key_slice);
        errdefer self.allocator.free(owned_key);
        const owned_decl_name = try self.allocator.dupe(u8, decl_name);
        errdefer self.allocator.free(owned_decl_name);

        try self.shape_share_map.put(owned_key, owned_decl_name);
        try writer.writeAll(struct_buf.items);
    }

    /// Generate an enum definition
    fn generateEnum(self: *Generator, node: *const schema.Node, writer: anytype) !void {
        const enum_info = node.enum_node orelse return error.InvalidEnumNode;
        const decl_name = try self.allocTypeDeclName(node);
        defer self.allocator.free(decl_name);

        try writer.print("pub const {s} = enum(u16) {{\n", .{decl_name});

        for (enum_info.enumerants) |enumerant| {
            const zig_name = try self.toZigIdentifier(enumerant.name);
            defer self.allocator.free(zig_name);
            const escaped_name = try types.escapeZigKeyword(self.allocator, zig_name);
            defer self.allocator.free(escaped_name);
            try writer.print("    {s} = {},\n", .{ escaped_name, enumerant.code_order });
        }

        try writer.writeAll("};\n\n");
    }

    /// Info about a single ancestor interface for code generation.
    const AncestorInfo = struct {
        interface_id: u64,
        name: []const u8,
        methods: []const schema.Method,
    };

    /// Walk superclasses recursively to collect all ancestor interfaces (not including self).
    /// Deduplicates by interface_id to handle diamond inheritance.
    fn collectAncestors(self: *Generator, node: *const schema.Node) ![]AncestorInfo {
        var result = std.ArrayList(AncestorInfo).empty;
        errdefer {
            for (result.items) |a| self.allocator.free(a.name);
            result.deinit(self.allocator);
        }
        var seen = std.AutoHashMap(u64, void).init(self.allocator);
        defer seen.deinit();
        // Exclude self
        try seen.put(node.id, {});
        try self.collectAncestorsRecursive(node, &result, &seen);
        return result.toOwnedSlice(self.allocator);
    }

    fn collectAncestorsRecursive(
        self: *Generator,
        node: *const schema.Node,
        result: *std.ArrayList(AncestorInfo),
        seen: *std.AutoHashMap(u64, void),
    ) !void {
        const iface = node.interface_node orelse return;
        for (iface.superclasses) |parent_id| {
            if (seen.contains(parent_id)) continue;
            try seen.put(parent_id, {});
            const parent_node = self.getNode(parent_id) orelse continue;
            const parent_iface = parent_node.interface_node orelse continue;
            // Recurse into grandparents first (depth-first)
            try self.collectAncestorsRecursive(parent_node, result, seen);
            const parent_name = try self.qualifiedTypeName(parent_id);
            errdefer self.allocator.free(parent_name);
            try result.append(self.allocator, .{
                .interface_id = parent_id,
                .name = parent_name,
                .methods = parent_iface.methods,
            });
        }
    }

    fn freeAncestors(self: *Generator, ancestors: []AncestorInfo) void {
        for (ancestors) |a| self.allocator.free(a.name);
        self.allocator.free(ancestors);
    }

    /// Check whether an interface (or any ancestor) has streaming methods.
    fn hasStreamingMethods(_: *Generator, node: *const schema.Node, ancestors: []const AncestorInfo) bool {
        const iface = node.interface_node orelse return false;
        for (iface.methods) |method| {
            if (method.isStreaming()) return true;
        }
        for (ancestors) |ancestor| {
            for (ancestor.methods) |method| {
                if (method.isStreaming()) return true;
            }
        }
        return false;
    }

    /// Generate an interface definition
    fn generateInterface(self: *Generator, node: *const schema.Node, writer: anytype) !void {
        const interface_info = node.interface_node orelse return error.InvalidInterfaceNode;
        const decl_name = try self.allocTypeDeclName(node);
        defer self.allocator.free(decl_name);

        const ancestors = try self.collectAncestors(node);
        defer self.freeAncestors(ancestors);
        const has_ancestors = ancestors.len > 0;

        try writer.print("pub const {s} = struct {{\n", .{decl_name});
        try writer.print("    pub const interface_id: u64 = 0x{x};\n", .{node.id});
        // Zero-method interfaces produce an empty enum; this is valid Zig but uninhabitable.
        try writer.writeAll("    pub const Method = enum(u16) {\n");
        for (interface_info.methods) |method| {
            const zig_name = try self.toZigIdentifier(method.name);
            defer self.allocator.free(zig_name);
            const escaped_name = try types.escapeZigKeyword(self.allocator, zig_name);
            defer self.allocator.free(escaped_name);
            try writer.print("        {s} = {},\n", .{ escaped_name, method.code_order });
        }
        try writer.writeAll("    };\n\n");

        for (interface_info.methods) |method| {
            try self.generateMethodStruct(method, writer);
        }

        // --- Client ---
        try writer.writeAll("    pub const Client = struct {\n");
        try writer.writeAll("        peer: *rpc.peer.Peer,\n");
        try writer.writeAll("        cap_id: u32,\n\n");
        try writer.writeAll("        pub fn init(peer: *rpc.peer.Peer, cap_id: u32) Client {\n");
        try writer.writeAll("            return .{ .peer = peer, .cap_id = cap_id };\n");
        try writer.writeAll("        }\n\n");

        // Own call methods
        for (interface_info.methods) |method| {
            try self.generateClientCallMethod(method, "interface_id", null, writer);
        }
        // Inherited call methods
        for (ancestors) |ancestor| {
            for (ancestor.methods) |method| {
                try self.generateClientCallMethod(method, null, ancestor.name, writer);
            }
        }

        // Generate callXxxPipelined methods for own methods with interface-typed results
        for (interface_info.methods) |method| {
            try self.generateClientPipelinedMethod(method, null, writer);
        }
        // Inherited pipelined call methods
        for (ancestors) |ancestor| {
            for (ancestor.methods) |method| {
                try self.generateClientPipelinedMethod(method, ancestor.name, writer);
            }
        }

        try writer.writeAll("        pub fn fromBootstrap(peer: *rpc.peer.Peer, user_ctx: *anyopaque, callback: BootstrapCallback) !u32 {\n");
        try writer.writeAll("            return bootstrap(peer, user_ctx, callback);\n");
        try writer.writeAll("        }\n\n");

        try writer.writeAll("    };\n\n");

        // --- StreamClient (only when interface or ancestors have streaming methods) ---
        if (self.hasStreamingMethods(node, ancestors)) {
            try self.generateStreamClient(node, interface_info, ancestors, writer);
        }

        // Generate Pipeline types for methods with interface-typed results (own methods)
        for (interface_info.methods) |method| {
            try self.generatePipelineType(method, null, writer);
        }
        // Inherited pipeline types
        for (ancestors) |ancestor| {
            for (ancestor.methods) |method| {
                try self.generatePipelineType(method, ancestor.name, writer);
            }
        }

        // --- PipelinedClient ---
        try writer.writeAll("    pub const PipelinedClient = struct {\n");
        try writer.writeAll("        peer: *rpc.peer.Peer,\n");
        try writer.writeAll("        question_id: u32,\n");
        try writer.writeAll("        pointer_index: u16,\n\n");

        // Own pipelined call methods
        for (interface_info.methods) |method| {
            try self.generatePipelinedClientCallMethod(method, "interface_id", null, writer);
        }
        // Inherited pipelined call methods
        for (ancestors) |ancestor| {
            for (ancestor.methods) |method| {
                try self.generatePipelinedClientCallMethod(method, null, ancestor.name, writer);
            }
        }

        try writer.writeAll("    };\n\n");

        // --- Bootstrap ---
        try writer.writeAll("    pub const BootstrapResponse = union(enum) {\n");
        try writer.writeAll("        client: Client,\n");
        try writer.writeAll("        exception: rpc.protocol.Exception,\n");
        try writer.writeAll("        canceled,\n");
        try writer.writeAll("        results_sent_elsewhere,\n");
        try writer.writeAll("        take_from_other_question: u32,\n");
        try writer.writeAll("        accept_from_third_party,\n");
        try writer.writeAll("    };\n");
        try writer.writeAll("    pub const BootstrapCallback = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, response: BootstrapResponse) anyerror!void;\n\n");

        try writer.writeAll("    const BootstrapContext = struct {\n");
        try writer.writeAll("        user_ctx: *anyopaque,\n");
        try writer.writeAll("        callback: BootstrapCallback,\n");
        try writer.writeAll("    };\n\n");

        try writer.writeAll("    fn bootstrapReturn(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, ret: rpc.protocol.Return, caps: *const rpc.cap_table.InboundCapTable) anyerror!void {\n");
        try writer.writeAll("        const ctx: *BootstrapContext = @ptrCast(@alignCast(ctx_ptr));\n");
        try writer.writeAll("        defer peer.allocator.destroy(ctx);\n");
        try writer.writeAll("        var response: BootstrapResponse = undefined;\n");
        try writer.writeAll("        switch (ret.tag) {\n");
        try writer.writeAll("            .results => {\n");
        try writer.writeAll("                const payload = ret.results orelse return error.MissingReturnPayload;\n");
        try writer.writeAll("                const cap = try payload.content.getCapability();\n");
        try writer.writeAll("                var mutable_caps = caps.*;\n");
        try writer.writeAll("                try mutable_caps.retainCapability(cap);\n");
        try writer.writeAll("                const resolved = try caps.resolveCapability(cap);\n");
        try writer.writeAll("                switch (resolved) {\n");
        try writer.writeAll("                    .imported => |imported| response = .{ .client = Client.init(peer, imported.id) },\n");
        try writer.writeAll("                    else => return error.UnexpectedBootstrapCapability,\n");
        try writer.writeAll("                }\n");
        try writer.writeAll("            },\n");
        try writer.writeAll("            .exception => {\n");
        try writer.writeAll("                const ex = ret.exception orelse return error.MissingException;\n");
        try writer.writeAll("                response = .{ .exception = ex };\n");
        try writer.writeAll("            },\n");
        try writer.writeAll("            .canceled => response = .canceled,\n");
        try writer.writeAll("            .resultsSentElsewhere => response = .results_sent_elsewhere,\n");
        try writer.writeAll("            .takeFromOtherQuestion => {\n");
        try writer.writeAll("                const qid = ret.take_from_other_question orelse return error.MissingQuestionId;\n");
        try writer.writeAll("                response = .{ .take_from_other_question = qid };\n");
        try writer.writeAll("            },\n");
        try writer.writeAll("            .awaitFromThirdParty => response = .accept_from_third_party,\n");
        try writer.writeAll("        }\n");
        try writer.writeAll("        try ctx.callback(ctx.user_ctx, peer, response);\n");
        try writer.writeAll("    }\n\n");

        try writer.writeAll("    pub fn bootstrap(peer: *rpc.peer.Peer, user_ctx: *anyopaque, callback: BootstrapCallback) !u32 {\n");
        try writer.writeAll("        const ctx = try peer.allocator.create(BootstrapContext);\n");
        try writer.writeAll("        ctx.* = .{ .user_ctx = user_ctx, .callback = callback };\n");
        try writer.writeAll("        return peer.sendBootstrap(ctx, bootstrapReturn);\n");
        try writer.writeAll("    }\n\n");

        // --- Server + VTable ---
        try writer.writeAll("    pub const Server = struct {\n");
        try writer.writeAll("        ctx: *anyopaque,\n");
        try writer.writeAll("        vtable: VTable,\n");
        try writer.writeAll("    };\n\n");

        try writer.writeAll("    pub const VTable = struct {\n");
        // Own method fields
        for (interface_info.methods) |method| {
            try self.generateVTableField(method, null, writer);
        }
        // Inherited method fields
        for (ancestors) |ancestor| {
            for (ancestor.methods) |method| {
                try self.generateVTableField(method, ancestor.name, writer);
            }
        }
        try writer.writeAll("    };\n\n");

        try writer.writeAll("    pub fn exportServer(peer: *rpc.peer.Peer, server: *Server) !u32 {\n");
        try writer.writeAll("        return peer.addExport(.{ .ctx = server, .on_call = onCall });\n");
        try writer.writeAll("    }\n\n");

        try writer.writeAll("    pub fn setBootstrap(peer: *rpc.peer.Peer, server: *Server) !u32 {\n");
        try writer.writeAll("        return peer.setBootstrap(.{ .ctx = server, .on_call = onCall });\n");
        try writer.writeAll("    }\n\n");

        // --- onCall dispatch ---
        try writer.writeAll("    fn onCall(ctx: *anyopaque, peer: *rpc.peer.Peer, call: rpc.protocol.Call, caps: *const rpc.cap_table.InboundCapTable) anyerror!void {\n");
        try writer.writeAll("        const server: *Server = @ptrCast(@alignCast(ctx));\n");

        var dispatch_method_count: usize = interface_info.methods.len;
        for (ancestors) |ancestor| {
            dispatch_method_count += ancestor.methods.len;
        }
        if (dispatch_method_count == 0) {
            try writer.writeAll("        _ = server;\n");
            try writer.writeAll("        _ = caps;\n");
        }

        if (has_ancestors) {
            // Dispatch by interface_id first, then method_id
            try writer.writeAll("        if (call.interface_id == interface_id) {\n");
            try writer.writeAll("            switch (call.method_id) {\n");
            for (interface_info.methods) |method| {
                const zig_name = try self.toZigIdentifier(method.name);
                defer self.allocator.free(zig_name);
                try writer.print("                {s}.ordinal => try {s}.handleCall(server, peer, call, caps),\n", .{ zig_name, zig_name });
            }
            try writer.writeAll("                else => try peer.sendReturnException(call.question_id, \"unknown method\"),\n");
            try writer.writeAll("            }\n");

            for (ancestors) |ancestor| {
                try writer.print("        }} else if (call.interface_id == {s}.interface_id) {{\n", .{ancestor.name});
                try writer.writeAll("            switch (call.method_id) {\n");
                for (ancestor.methods) |method| {
                    const zig_name = try self.toZigIdentifier(method.name);
                    defer self.allocator.free(zig_name);
                    const method_field = try self.lowerFirst(zig_name);
                    defer self.allocator.free(method_field);
                    const escaped_field = try types.escapeZigKeyword(self.allocator, method_field);
                    defer self.allocator.free(escaped_field);
                    const deferred_field = try std.fmt.allocPrint(self.allocator, "{s}_deferred", .{method_field});
                    defer self.allocator.free(deferred_field);
                    const escaped_deferred_field = try types.escapeZigKeyword(self.allocator, deferred_field);
                    defer self.allocator.free(escaped_deferred_field);
                    if (method.isStreaming()) {
                        try writer.print("                {s}.{s}.ordinal => try {s}.{s}.handleCallDirect(server.vtable.{s}, server.ctx, peer, call, caps),\n", .{
                            ancestor.name, zig_name, ancestor.name, zig_name, escaped_field,
                        });
                    } else {
                        try writer.print("                {s}.{s}.ordinal => try {s}.{s}.handleCallDirect(server.vtable.{s}, server.vtable.{s}, server.ctx, peer, call, caps),\n", .{
                            ancestor.name, zig_name, ancestor.name, zig_name, escaped_field, escaped_deferred_field,
                        });
                    }
                }
                try writer.writeAll("                else => try peer.sendReturnException(call.question_id, \"unknown method\"),\n");
                try writer.writeAll("            }\n");
            }
            try writer.writeAll("        } else {\n");
            try writer.writeAll("            try peer.sendReturnException(call.question_id, \"unknown interface\");\n");
            try writer.writeAll("        }\n");
        } else {
            // No ancestors — simple dispatch by method_id only (backward compatible)
            try writer.writeAll("        switch (call.method_id) {\n");
            for (interface_info.methods) |method| {
                const zig_name = try self.toZigIdentifier(method.name);
                defer self.allocator.free(zig_name);
                try writer.print("            {s}.ordinal => try {s}.handleCall(server, peer, call, caps),\n", .{ zig_name, zig_name });
            }
            try writer.writeAll("            else => try peer.sendReturnException(call.question_id, \"unknown method\"),\n");
            try writer.writeAll("        }\n");
        }

        try writer.writeAll("    }\n");

        try writer.writeAll("};\n\n");
    }

    /// Generate a single method struct inside an interface.
    fn generateMethodStruct(self: *Generator, method: schema.Method, writer: anytype) !void {
        const zig_name = try self.toZigIdentifier(method.name);
        defer self.allocator.free(zig_name);
        const escaped_zig_name = try types.escapeZigKeyword(self.allocator, zig_name);
        defer self.allocator.free(escaped_zig_name);
        const method_field = try self.lowerFirst(zig_name);
        defer self.allocator.free(method_field);
        const escaped_method_field = try types.escapeZigKeyword(self.allocator, method_field);
        defer self.allocator.free(escaped_method_field);
        const param_name = try self.resolveNodeName(method.param_struct_type);
        defer self.allocator.free(param_name);
        const result_name = try self.resolveNodeName(method.result_struct_type);
        defer self.allocator.free(result_name);

        const param_layout = self.structLayout(method.param_struct_type) orelse return error.InvalidStructNode;
        const result_layout = self.structLayout(method.result_struct_type) orelse return error.InvalidStructNode;
        const is_streaming = method.isStreaming();

        try writer.print("    pub const {s} = struct {{\n", .{escaped_zig_name});
        try writer.print("        pub const ordinal: u16 = {};\n", .{method.code_order});
        try writer.print("        pub const is_streaming: bool = {};\n", .{is_streaming});
        try writer.print("        pub const Params = {s};\n", .{param_name});
        try writer.print("        pub const Results = {s};\n", .{result_name});
        try writer.writeAll("        pub const BuildFn = *const fn (ctx: *anyopaque, params: *Params.Builder) anyerror!void;\n");

        if (is_streaming) {
            try writer.writeAll("        pub const StreamHandler = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, params: Params.Reader, caps: *const rpc.cap_table.InboundCapTable) anyerror!void;\n");
        } else {
            try writer.writeAll("        pub const Handler = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, params: Params.Reader, results: *Results.Builder, caps: *const rpc.cap_table.InboundCapTable) anyerror!void;\n");
            try writer.writeAll("        pub const DeferredHandler = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, params: Params.Reader, caps: *const rpc.cap_table.InboundCapTable, sender: ReturnSender) anyerror!void;\n");
        }

        try writer.writeAll("        pub const Response = union(enum) {\n");
        try writer.writeAll("            results: Results.Reader,\n");
        try writer.writeAll("            exception: rpc.protocol.Exception,\n");
        try writer.writeAll("            canceled,\n");
        try writer.writeAll("            results_sent_elsewhere,\n");
        try writer.writeAll("            take_from_other_question: u32,\n");
        try writer.writeAll("            accept_from_third_party,\n");
        try writer.writeAll("        };\n");
        try writer.writeAll("        pub const Callback = *const fn (ctx: *anyopaque, peer: *rpc.peer.Peer, response: Response, caps: *const rpc.cap_table.InboundCapTable) anyerror!void;\n\n");

        try writer.writeAll("        const CallContext = struct {\n");
        try writer.writeAll("            user_ctx: *anyopaque,\n");
        try writer.writeAll("            build: ?BuildFn,\n");
        try writer.writeAll("            callback: Callback,\n");
        try writer.writeAll("        };\n\n");

        if (!is_streaming) {
            try writer.writeAll("        const DirectReturnContext = struct {\n");
            try writer.writeAll("            handler: Handler,\n");
            try writer.writeAll("            ctx: *anyopaque,\n");
            try writer.writeAll("            peer: *rpc.peer.Peer,\n");
            try writer.writeAll("            params: Params.Reader,\n");
            try writer.writeAll("            caps: *const rpc.cap_table.InboundCapTable,\n");
            try writer.writeAll("        };\n\n");

            try writer.writeAll("        pub const ReturnSender = struct {\n");
            try writer.writeAll("            peer: *rpc.peer.Peer,\n");
            try writer.writeAll("            question_id: u32,\n\n");
            try writer.writeAll("            pub fn sendResults(self: ReturnSender, ctx: *anyopaque, build: *const fn (ctx: *anyopaque, ret: *rpc.protocol.ReturnBuilder) anyerror!void) !void {\n");
            try writer.writeAll("                try self.peer.sendReturnResults(self.question_id, ctx, build);\n");
            try writer.writeAll("            }\n\n");
            try writer.writeAll("            pub fn sendException(self: ReturnSender, reason: []const u8) !void {\n");
            try writer.writeAll("                try self.peer.sendReturnException(self.question_id, reason);\n");
            try writer.writeAll("            }\n");
            try writer.writeAll("        };\n\n");
        }

        try writer.writeAll("        fn callBuild(ctx_ptr: *anyopaque, call: *rpc.protocol.CallBuilder) anyerror!void {\n");
        try writer.writeAll("            const ctx: *CallContext = @ptrCast(@alignCast(ctx_ptr));\n");
        try writer.writeAll("            var payload = try call.payloadTyped();\n");
        try writer.writeAll("            var params_any = try payload.initContent();\n");
        try writer.print("            const params_builder = try params_any.initStruct({}, {});\n", .{
            param_layout.data_words,
            param_layout.pointer_words,
        });
        try writer.writeAll("            var params = Params.Builder.wrap(params_builder);\n");
        try writer.writeAll("            if (ctx.build) |build_fn| {\n");
        try writer.writeAll("                try build_fn(ctx.user_ctx, &params);\n");
        try writer.writeAll("            }\n");
        try writer.writeAll("            _ = try call.initCapTableTyped(0);\n");
        try writer.writeAll("        }\n\n");

        try writer.writeAll("        fn callReturn(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, ret: rpc.protocol.Return, caps: *const rpc.cap_table.InboundCapTable) anyerror!void {\n");
        try writer.writeAll("            const ctx: *CallContext = @ptrCast(@alignCast(ctx_ptr));\n");
        try writer.writeAll("            defer peer.allocator.destroy(ctx);\n");
        try writer.writeAll("            var response: Response = undefined;\n");
        try writer.writeAll("            switch (ret.tag) {\n");
        try writer.writeAll("                .results => {\n");
        try writer.writeAll("                    const payload = ret.results orelse return error.MissingReturnPayload;\n");
        try writer.writeAll("                    const struct_reader = try payload.content.getStruct();\n");
        try writer.writeAll("                    const results = Results.Reader.wrap(struct_reader);\n");
        try writer.writeAll("                    response = .{ .results = results };\n");
        try writer.writeAll("                },\n");
        try writer.writeAll("                .exception => {\n");
        try writer.writeAll("                    const ex = ret.exception orelse return error.MissingException;\n");
        try writer.writeAll("                    response = .{ .exception = ex };\n");
        try writer.writeAll("                },\n");
        try writer.writeAll("                .canceled => response = .canceled,\n");
        try writer.writeAll("                .resultsSentElsewhere => response = .results_sent_elsewhere,\n");
        try writer.writeAll("                .takeFromOtherQuestion => {\n");
        try writer.writeAll("                    const qid = ret.take_from_other_question orelse return error.MissingQuestionId;\n");
        try writer.writeAll("                    response = .{ .take_from_other_question = qid };\n");
        try writer.writeAll("                },\n");
        try writer.writeAll("                .awaitFromThirdParty => response = .accept_from_third_party,\n");
        try writer.writeAll("            }\n");
        try writer.writeAll("            try ctx.callback(ctx.user_ctx, peer, response, caps);\n");
        try writer.writeAll("        }\n\n");

        if (is_streaming) {
            // handleCallDirect: takes StreamHandler + ctx directly
            try writer.writeAll("        pub fn handleCallDirect(handler: StreamHandler, ctx: *anyopaque, peer: *rpc.peer.Peer, call: rpc.protocol.Call, caps: *const rpc.cap_table.InboundCapTable) anyerror!void {\n");
            try writer.writeAll("            const params_struct = try call.params.content.getStruct();\n");
            try writer.writeAll("            const params = Params.Reader.wrap(params_struct);\n");
            try writer.writeAll("            try handler(ctx, peer, params, caps);\n");
            try writer.writeAll("            try peer.sendReturnEmptyStruct(call.question_id);\n");
            try writer.writeAll("        }\n\n");

            // handleCall delegates to handleCallDirect
            try writer.writeAll("        fn handleCall(server: *Server, peer: *rpc.peer.Peer, call: rpc.protocol.Call, caps: *const rpc.cap_table.InboundCapTable) anyerror!void {\n");
            try writer.print("            try handleCallDirect(server.vtable.{s}, server.ctx, peer, call, caps);\n", .{escaped_method_field});
            try writer.writeAll("        }\n\n");

            // StreamCallContext + streamCallBuild + streamCallReturn for fire-and-forget streaming
            try writer.writeAll("        pub const StreamCallContext = struct {\n");
            try writer.writeAll("            stream: *rpc.stream_state.StreamState,\n");
            try writer.writeAll("            build_ctx: *anyopaque,\n");
            try writer.writeAll("            build: ?BuildFn,\n");
            try writer.writeAll("        };\n\n");

            try writer.writeAll("        fn streamCallBuild(ctx_ptr: *anyopaque, call: *rpc.protocol.CallBuilder) anyerror!void {\n");
            try writer.writeAll("            const ctx: *StreamCallContext = @ptrCast(@alignCast(ctx_ptr));\n");
            try writer.writeAll("            var payload = try call.payloadTyped();\n");
            try writer.writeAll("            var params_any = try payload.initContent();\n");
            try writer.print("            const params_builder = try params_any.initStruct({}, {});\n", .{
                param_layout.data_words,
                param_layout.pointer_words,
            });
            try writer.writeAll("            var params = Params.Builder.wrap(params_builder);\n");
            try writer.writeAll("            if (ctx.build) |build_fn| try build_fn(ctx.build_ctx, &params);\n");
            try writer.writeAll("            _ = try call.initCapTableTyped(0);\n");
            try writer.writeAll("        }\n\n");

            try writer.writeAll("        fn streamCallReturn(ctx_ptr: *anyopaque, peer: *rpc.peer.Peer, ret: rpc.protocol.Return, caps: *const rpc.cap_table.InboundCapTable) anyerror!void {\n");
            try writer.writeAll("            const ctx: *StreamCallContext = @ptrCast(@alignCast(ctx_ptr));\n");
            try writer.writeAll("            defer peer.allocator.destroy(ctx);\n");
            try writer.writeAll("            _ = caps;\n");
            try writer.writeAll("            ctx.stream.handleReturn(ret.tag == .exception);\n");
            try writer.writeAll("        }\n");
        } else {
            // handleCallDirect: takes Handler + ?DeferredHandler + ctx directly
            try writer.writeAll("        pub fn handleCallDirect(handler: Handler, deferred_handler: ?DeferredHandler, ctx: *anyopaque, peer: *rpc.peer.Peer, call: rpc.protocol.Call, caps: *const rpc.cap_table.InboundCapTable) anyerror!void {\n");
            try writer.writeAll("            const params_struct = try call.params.content.getStruct();\n");
            try writer.writeAll("            const params = Params.Reader.wrap(params_struct);\n");
            try writer.writeAll("            if (deferred_handler) |deferred_fn| {\n");
            try writer.writeAll("                const sender = ReturnSender{ .peer = peer, .question_id = call.question_id };\n");
            try writer.writeAll("                try deferred_fn(ctx, peer, params, caps, sender);\n");
            try writer.writeAll("            } else {\n");
            try writer.writeAll("                var dctx = DirectReturnContext{\n");
            try writer.writeAll("                    .handler = handler,\n");
            try writer.writeAll("                    .ctx = ctx,\n");
            try writer.writeAll("                    .peer = peer,\n");
            try writer.writeAll("                    .params = params,\n");
            try writer.writeAll("                    .caps = caps,\n");
            try writer.writeAll("                };\n");
            try writer.writeAll("                try peer.sendReturnResults(call.question_id, &dctx, buildReturnDirect);\n");
            try writer.writeAll("            }\n");
            try writer.writeAll("        }\n\n");

            // handleCall delegates to handleCallDirect
            try writer.writeAll("        fn handleCall(server: *Server, peer: *rpc.peer.Peer, call: rpc.protocol.Call, caps: *const rpc.cap_table.InboundCapTable) anyerror!void {\n");
            const deferred_field = try std.fmt.allocPrint(self.allocator, "{s}_deferred", .{method_field});
            defer self.allocator.free(deferred_field);
            const escaped_deferred_field = try types.escapeZigKeyword(self.allocator, deferred_field);
            defer self.allocator.free(escaped_deferred_field);
            try writer.print("            try handleCallDirect(server.vtable.{s}, server.vtable.{s}, server.ctx, peer, call, caps);\n", .{ escaped_method_field, escaped_deferred_field });
            try writer.writeAll("        }\n\n");

            try writer.writeAll("        fn buildReturnDirect(ctx_ptr: *anyopaque, ret: *rpc.protocol.ReturnBuilder) anyerror!void {\n");
            try writer.writeAll("            const dctx: *DirectReturnContext = @ptrCast(@alignCast(ctx_ptr));\n");
            try writer.writeAll("            var payload = try ret.payloadTyped();\n");
            try writer.writeAll("            var results_any = try payload.initContent();\n");
            try writer.print("            const results_builder = try results_any.initStruct({}, {});\n", .{
                result_layout.data_words,
                result_layout.pointer_words,
            });
            try writer.writeAll("            var results = Results.Builder.wrap(results_builder);\n");
            try writer.writeAll("            try dctx.handler(dctx.ctx, dctx.peer, dctx.params, &results, dctx.caps);\n");
            try writer.writeAll("            _ = try ret.initCapTableTyped(0);\n");
            try writer.writeAll("        }\n");
        }

        try writer.writeAll("    };\n\n");
    }

    /// Generate a VTable field for a method. If `ancestor_name` is set, uses the ancestor's types.
    fn generateVTableField(self: *Generator, method: schema.Method, ancestor_name: ?[]const u8, writer: anytype) !void {
        const zig_name = try self.toZigIdentifier(method.name);
        defer self.allocator.free(zig_name);
        const method_field = try self.lowerFirst(zig_name);
        defer self.allocator.free(method_field);
        const escaped_field = try types.escapeZigKeyword(self.allocator, method_field);
        defer self.allocator.free(escaped_field);
        const deferred_field = try std.fmt.allocPrint(self.allocator, "{s}_deferred", .{method_field});
        defer self.allocator.free(deferred_field);
        const escaped_deferred_field = try types.escapeZigKeyword(self.allocator, deferred_field);
        defer self.allocator.free(escaped_deferred_field);
        if (ancestor_name) |aname| {
            if (method.isStreaming()) {
                try writer.print("        {s}: {s}.{s}.StreamHandler,\n", .{ escaped_field, aname, zig_name });
            } else {
                try writer.print("        {s}: {s}.{s}.Handler,\n", .{ escaped_field, aname, zig_name });
                try writer.print("        {s}: ?{s}.{s}.DeferredHandler = null,\n", .{ escaped_deferred_field, aname, zig_name });
            }
        } else {
            if (method.isStreaming()) {
                try writer.print("        {s}: {s}.StreamHandler,\n", .{ escaped_field, zig_name });
            } else {
                try writer.print("        {s}: {s}.Handler,\n", .{ escaped_field, zig_name });
                try writer.print("        {s}: ?{s}.DeferredHandler = null,\n", .{ escaped_deferred_field, zig_name });
            }
        }
    }

    /// Resolved names for a method call, shared across Client, StreamClient, and PipelinedClient generation.
    const MethodCallParams = struct {
        zig_name: []const u8,
        call_name: []const u8,
        method_prefix: []const u8,
        dot: []const u8,
        iface_id: []const u8,
        iface_id_owned: bool,
    };

    fn resolveMethodCallParams(self: *Generator, method: schema.Method, interface_id_expr: ?[]const u8, ancestor_name: ?[]const u8) !MethodCallParams {
        const zig_name = try self.toZigIdentifier(method.name);
        errdefer self.allocator.free(zig_name);
        const call_name = try std.fmt.allocPrint(self.allocator, "call{s}", .{zig_name});
        errdefer self.allocator.free(call_name);

        const iface_id = if (interface_id_expr) |expr| expr else blk: {
            const temp = try std.fmt.allocPrint(self.allocator, "{s}.interface_id", .{ancestor_name.?});
            break :blk temp;
        };

        return .{
            .zig_name = zig_name,
            .call_name = call_name,
            .method_prefix = ancestor_name orelse "",
            .dot = if (ancestor_name != null) "." else "",
            .iface_id = iface_id,
            .iface_id_owned = interface_id_expr == null,
        };
    }

    fn freeMethodCallParams(self: *Generator, params: MethodCallParams) void {
        self.allocator.free(params.zig_name);
        self.allocator.free(params.call_name);
        if (params.iface_id_owned) self.allocator.free(params.iface_id);
    }

    /// Generate a Client call method. Uses `interface_id_expr` for own methods or ancestor_name for inherited.
    fn generateClientCallMethod(self: *Generator, method: schema.Method, interface_id_expr: ?[]const u8, ancestor_name: ?[]const u8, writer: anytype) !void {
        const p = try self.resolveMethodCallParams(method, interface_id_expr, ancestor_name);
        defer self.freeMethodCallParams(p);

        try writer.print("        pub fn {s}(self: *Client, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback) !u32 {{\n", .{
            p.call_name, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
        });
        try writer.print("            const ctx = try self.peer.allocator.create({s}{s}{s}.CallContext);\n", .{ p.method_prefix, p.dot, p.zig_name });
        try writer.writeAll("            ctx.* = .{ .user_ctx = user_ctx, .build = build, .callback = on_return };\n");
        try writer.print("            return self.peer.sendCall(self.cap_id, {s}, {s}{s}{s}.ordinal, ctx, {s}{s}{s}.callBuild, {s}{s}{s}.callReturn);\n", .{
            p.iface_id, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
        });
        try writer.writeAll("        }\n\n");
    }

    /// Generate a StreamClient type for an interface with streaming methods.
    fn generateStreamClient(
        self: *Generator,
        node: *const schema.Node,
        interface_info: schema.InterfaceNode,
        ancestors: []const AncestorInfo,
        writer: anytype,
    ) !void {
        _ = node;
        try writer.writeAll("    pub const StreamClient = struct {\n");
        try writer.writeAll("        client: Client,\n");
        try writer.writeAll("        stream: rpc.stream_state.StreamState = .{},\n\n");

        try writer.writeAll("        pub fn init(client: Client) StreamClient {\n");
        try writer.writeAll("            return .{ .client = client };\n");
        try writer.writeAll("        }\n\n");

        // Own methods
        for (interface_info.methods) |method| {
            try self.generateStreamClientCallMethod(method, "interface_id", null, writer);
        }
        // Inherited methods
        for (ancestors) |ancestor| {
            for (ancestor.methods) |method| {
                try self.generateStreamClientCallMethod(method, null, ancestor.name, writer);
            }
        }

        try writer.writeAll("        pub fn waitStreaming(self: *StreamClient, ctx: *anyopaque, callback: rpc.stream_state.StreamState.DrainCallback) void {\n");
        try writer.writeAll("            self.stream.waitStreaming(ctx, callback);\n");
        try writer.writeAll("        }\n");

        try writer.writeAll("    };\n\n");
    }

    /// Generate a single StreamClient call method. Streaming methods become
    /// fire-and-forget; non-streaming methods pass through to the inner Client.
    fn generateStreamClientCallMethod(
        self: *Generator,
        method: schema.Method,
        interface_id_expr: ?[]const u8,
        ancestor_name: ?[]const u8,
        writer: anytype,
    ) !void {
        const p = try self.resolveMethodCallParams(method, interface_id_expr, ancestor_name);
        defer self.freeMethodCallParams(p);

        if (method.isStreaming()) {
            // Fire-and-forget streaming call
            try writer.print("        pub fn {s}(self: *StreamClient, build_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn) !void {{\n", .{
                p.call_name, p.method_prefix, p.dot, p.zig_name,
            });
            try writer.writeAll("            if (self.stream.hasFailed()) return self.stream.stream_error.?;\n");
            try writer.writeAll("            try self.stream.noteCallSent();\n");
            try writer.print("            const ctx = self.client.peer.allocator.create({s}{s}{s}.StreamCallContext) catch |err| {{\n", .{ p.method_prefix, p.dot, p.zig_name });
            try writer.writeAll("                self.stream.in_flight -= 1;\n");
            try writer.writeAll("                return err;\n");
            try writer.writeAll("            };\n");
            try writer.writeAll("            ctx.* = .{ .stream = &self.stream, .build_ctx = build_ctx, .build = build };\n");
            try writer.print("            _ = self.client.peer.sendCall(self.client.cap_id, {s}, {s}{s}{s}.ordinal, ctx, {s}{s}{s}.streamCallBuild, {s}{s}{s}.streamCallReturn) catch |err| {{\n", .{
                p.iface_id, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
            });
            try writer.writeAll("                self.stream.in_flight -= 1;\n");
            try writer.writeAll("                self.client.peer.allocator.destroy(ctx);\n");
            try writer.writeAll("                return err;\n");
            try writer.writeAll("            };\n");
            try writer.writeAll("        }\n\n");
        } else {
            // Pass-through to inner Client
            try writer.print("        pub fn {s}(self: *StreamClient, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback) !u32 {{\n", .{
                p.call_name, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
            });
            try writer.print("            return self.client.{s}(user_ctx, build, on_return);\n", .{p.call_name});
            try writer.writeAll("        }\n\n");
        }
    }

    /// Generate a callXxxPipelined method on Client if the method has interface-typed results.
    fn generateClientPipelinedMethod(self: *Generator, method: schema.Method, ancestor_name: ?[]const u8, writer: anytype) !void {
        const iface_fields = try self.getInterfaceFields(method.result_struct_type);
        defer self.freeInterfaceFields(iface_fields);
        if (iface_fields.len == 0) return;

        const zig_name = try self.toZigIdentifier(method.name);
        defer self.allocator.free(zig_name);

        const method_prefix = ancestor_name orelse "";
        const dot = if (ancestor_name != null) "." else "";

        try writer.print("        pub fn call{s}Pipelined(self: *Client, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback) !{s}{s}{s}.Pipeline {{\n", .{
            zig_name, method_prefix, dot, zig_name, method_prefix, dot, zig_name, method_prefix, dot, zig_name,
        });
        try writer.print("            const qid = try self.call{s}(user_ctx, build, on_return);\n", .{zig_name});
        try writer.writeAll("            return .{ .peer = self.peer, .question_id = qid };\n");
        try writer.writeAll("        }\n\n");
    }

    /// Generate a Pipeline type for a method with interface-typed results.
    fn generatePipelineType(self: *Generator, method: schema.Method, ancestor_name: ?[]const u8, writer: anytype) !void {
        const iface_fields = try self.getInterfaceFields(method.result_struct_type);
        defer self.freeInterfaceFields(iface_fields);
        if (iface_fields.len == 0) return;

        const zig_name = try self.toZigIdentifier(method.name);
        defer self.allocator.free(zig_name);
        const escaped_zig_name = try types.escapeZigKeyword(self.allocator, zig_name);
        defer self.allocator.free(escaped_zig_name);

        // For inherited methods, the Pipeline type is defined on the parent interface,
        // so we don't re-generate it here. The client method references the parent's Pipeline type.
        if (ancestor_name != null) return;

        try writer.print("    pub const {s}Pipeline = struct {{\n", .{escaped_zig_name});
        try writer.writeAll("        peer: *rpc.peer.Peer,\n");
        try writer.writeAll("        question_id: u32,\n\n");

        for (iface_fields) |ifield| {
            try writer.print("        pub fn get{s}(self: @This()) {s}.PipelinedClient {{\n", .{ ifield.name, ifield.type_name });
            try writer.print("            return .{{ .peer = self.peer, .question_id = self.question_id, .pointer_index = {} }};\n", .{ifield.pointer_offset});
            try writer.writeAll("        }\n\n");
        }

        try writer.writeAll("    };\n\n");
    }

    /// Generate a PipelinedClient call method.
    fn generatePipelinedClientCallMethod(self: *Generator, method: schema.Method, interface_id_expr: ?[]const u8, ancestor_name: ?[]const u8, writer: anytype) !void {
        const p = try self.resolveMethodCallParams(method, interface_id_expr, ancestor_name);
        defer self.freeMethodCallParams(p);

        try writer.print("        pub fn {s}(self: *PipelinedClient, user_ctx: *anyopaque, build: ?{s}{s}{s}.BuildFn, on_return: {s}{s}{s}.Callback) !u32 {{\n", .{
            p.call_name, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
        });
        try writer.print("            const ctx = try self.peer.allocator.create({s}{s}{s}.CallContext);\n", .{ p.method_prefix, p.dot, p.zig_name });
        try writer.writeAll("            ctx.* = .{ .user_ctx = user_ctx, .build = build, .callback = on_return };\n");
        try writer.print("            return self.peer.sendCallPromisedWithOps(self.question_id, &[_]rpc.protocol.PromisedAnswerOp{{.{{ .tag = .getPointerField, .pointer_index = self.pointer_index }}}}, {s}, {s}{s}{s}.ordinal, ctx, {s}{s}{s}.callBuild, {s}{s}{s}.callReturn);\n", .{
            p.iface_id, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name, p.method_prefix, p.dot, p.zig_name,
        });
        try writer.writeAll("        }\n\n");
    }

    /// Generate a constant definition
    fn generateConst(self: *Generator, node: *const schema.Node, writer: anytype) !void {
        const const_info = node.const_node orelse return error.InvalidConstNode;
        const name = try self.allocValueDeclName(node);
        defer self.allocator.free(name);

        switch (const_info.value) {
            .text => |text| {
                try writer.print("pub const {s}: []const u8 = \"{f}\";\n\n", .{ name, std.zig.fmtString(text) });
            },
            .data => |data| {
                try writer.print("pub const {s}: []const u8 = ", .{name});
                try self.writeByteArrayLiteral(writer, data);
                try writer.writeAll(";\n\n");
            },
            .list, .@"struct", .any_pointer => {
                try self.generatePointerConst(name, const_info.type, const_info.value, writer);
            },
            else => {
                const type_name = try self.typeNameForConst(const_info.type);
                defer self.allocator.free(type_name);

                if (try self.constValueLiteral(const_info.type, const_info.value)) |literal| {
                    defer self.allocator.free(literal);
                    try writer.print("pub const {s}: {s} = {s};\n\n", .{ name, type_name, literal });
                } else {
                    return error.UnsupportedConstType;
                }
            },
        }
    }

    /// Generate an annotation definition
    fn generateAnnotation(self: *Generator, node: *const schema.Node, writer: anytype) !void {
        const annotation_info = node.annotation_node orelse return error.InvalidAnnotationNode;
        const name = try self.allocValueDeclName(node);
        defer self.allocator.free(name);

        const type_name = try self.typeNameForConst(annotation_info.type);
        defer self.allocator.free(type_name);

        try writer.print("pub const {s} = struct {{\n", .{name});
        try writer.print("    pub const Type = {s};\n", .{type_name});
        try writer.writeAll("    pub const targets = .{\n");
        try writer.print("        .file = {},\n", .{annotation_info.targets_file});
        try writer.print("        .@\"const\" = {},\n", .{annotation_info.targets_const});
        try writer.print("        .@\"enum\" = {},\n", .{annotation_info.targets_enum});
        try writer.print("        .enumerant = {},\n", .{annotation_info.targets_enumerant});
        try writer.print("        .@\"struct\" = {},\n", .{annotation_info.targets_struct});
        try writer.print("        .field = {},\n", .{annotation_info.targets_field});
        try writer.print("        .@\"union\" = {},\n", .{annotation_info.targets_union});
        try writer.print("        .group = {},\n", .{annotation_info.targets_group});
        try writer.print("        .interface = {},\n", .{annotation_info.targets_interface});
        try writer.print("        .method = {},\n", .{annotation_info.targets_method});
        try writer.print("        .param = {},\n", .{annotation_info.targets_param});
        try writer.print("        .annotation = {},\n", .{annotation_info.targets_annotation});
        try writer.writeAll("    };\n");
        try writer.writeAll("};\n\n");
    }

    /// Get simple name from display name
    fn getSimpleName(self: *Generator, node: *const schema.Node) []const u8 {
        _ = self;
        const prefix_len = node.display_name_prefix_length;
        if (prefix_len >= node.display_name.len) return node.display_name;
        return node.display_name[prefix_len..];
    }

    fn allocTypeDeclName(self: *Generator, node: *const schema.Node) ![]const u8 {
        return types.normalizeAndEscapeTypeIdentifier(self.allocator, self.getSimpleName(node));
    }

    fn allocValueDeclName(self: *Generator, node: *const schema.Node) ![]const u8 {
        return types.normalizeAndEscapeValueIdentifier(self.allocator, self.getSimpleName(node));
    }

    fn allocAnnotationUseBaseName(self: *Generator, node: *const schema.Node) ![]u8 {
        return switch (node.kind) {
            .@"const", .annotation => types.identToZigValueName(self.allocator, self.getSimpleName(node)),
            else => types.identToZigTypeName(self.allocator, self.getSimpleName(node)),
        };
    }

    fn resolveNodeName(self: *Generator, id: schema.Id) ![]const u8 {
        if (self.getNode(id)) |_| {
            return self.qualifiedTypeName(id);
        }
        return try self.allocator.dupe(u8, "void");
    }

    /// Describes an interface-typed pointer field in a struct.
    const InterfaceFieldInfo = struct {
        name: []const u8,
        type_name: []const u8,
        pointer_offset: u32,
    };

    /// Return the list of interface-typed pointer fields in the given struct node.
    /// Caller must free each entry's name and type_name, as well as the returned slice.
    fn getInterfaceFields(self: *Generator, struct_id: schema.Id) ![]InterfaceFieldInfo {
        const node = self.getNode(struct_id) orelse return try self.allocator.alloc(InterfaceFieldInfo, 0);
        const struct_info = node.struct_node orelse return try self.allocator.alloc(InterfaceFieldInfo, 0);

        var result = std.ArrayList(InterfaceFieldInfo).empty;
        errdefer {
            for (result.items) |item| {
                self.allocator.free(item.name);
                self.allocator.free(item.type_name);
            }
            result.deinit(self.allocator);
        }

        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (slot.type != .interface) continue;
            const iface_id = slot.type.interface.type_id;
            const iface_name = try self.qualifiedTypeName(iface_id);
            errdefer self.allocator.free(iface_name);
            const field_name = try types.identToZigTypeName(self.allocator, field.name);
            errdefer self.allocator.free(field_name);
            try result.append(self.allocator, .{
                .name = field_name,
                .type_name = iface_name,
                .pointer_offset = slot.offset,
            });
        }

        return result.toOwnedSlice(self.allocator);
    }

    fn freeInterfaceFields(self: *Generator, fields: []InterfaceFieldInfo) void {
        for (fields) |item| {
            self.allocator.free(item.name);
            self.allocator.free(item.type_name);
        }
        self.allocator.free(fields);
    }

    fn structLayout(self: *Generator, id: schema.Id) ?struct { data_words: u16, pointer_words: u16 } {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .@"struct") return null;
        const info = node.struct_node orelse return null;
        return .{ .data_words = info.data_word_count, .pointer_words = info.pointer_count };
    }

    fn lowerFirst(self: *Generator, name: []const u8) ![]const u8 {
        if (name.len == 0) return try self.allocator.dupe(u8, name);
        var result = try self.allocator.alloc(u8, name.len);
        result[0] = std.ascii.toLower(name[0]);
        @memcpy(result[1..], name[1..]);
        return result;
    }

    fn capitalizeFirst(self: *Generator, name: []const u8) ![]const u8 {
        if (name.len == 0) return try self.allocator.dupe(u8, name);
        var result = try self.allocator.alloc(u8, name.len);
        result[0] = std.ascii.toUpper(name[0]);
        @memcpy(result[1..], name[1..]);
        return result;
    }

    /// Convert Cap'n Proto identifier to Zig type name (PascalCase)
    fn toZigIdentifier(self: *Generator, name: []const u8) ![]const u8 {
        return types.identToZigTypeName(self.allocator, name);
    }

    fn moduleNameFromFilename(self: *Generator, filename: []const u8) ![]const u8 {
        const stem = std.fs.path.stem(filename);
        return self.toSnakeCaseLower(stem);
    }

    fn uniqueImportModuleName(
        self: *Generator,
        filename: []const u8,
        used_aliases: *std.StringHashMap(void),
    ) ![]const u8 {
        const base = try self.moduleNameFromFilename(filename);
        defer self.allocator.free(base);

        var suffix: usize = 0;
        while (true) : (suffix += 1) {
            const candidate_raw = if (suffix == 0)
                try self.allocator.dupe(u8, base)
            else
                try std.fmt.allocPrint(self.allocator, "{s}_{d}", .{ base, suffix + 1 });
            defer self.allocator.free(candidate_raw);

            const candidate = try types.escapeZigKeyword(self.allocator, candidate_raw);
            errdefer self.allocator.free(candidate);

            const gop = try used_aliases.getOrPut(candidate);
            if (!gop.found_existing) return candidate;

            self.allocator.free(candidate);
        }
    }

    /// Derive the .zig import path from a .capnp import name.
    /// E.g., "other.capnp" → "other.zig", "path/to/types.capnp" → "path/to/types.zig"
    fn importPathFromCapnpName(self: *Generator, capnp_name: []const u8) ![]const u8 {
        const normalized = if (std.mem.startsWith(u8, capnp_name, "/")) capnp_name[1..] else capnp_name;
        try validateRelativeSchemaPath(normalized);

        if (std.mem.endsWith(u8, normalized, ".capnp")) {
            const base = normalized[0 .. normalized.len - 6];
            return std.fmt.allocPrint(self.allocator, "{s}.zig", .{base});
        }
        return std.fmt.allocPrint(self.allocator, "{s}.zig", .{normalized});
    }

    fn validateRelativeSchemaPath(path: []const u8) !void {
        if (path.len == 0) return error.InvalidSchemaPath;
        if (std.mem.indexOfScalar(u8, path, '\\') != null) return error.InvalidSchemaPath;
        if (path[0] == '/') return error.InvalidSchemaPath;
        if (hasWindowsDriveRoot(path)) return error.InvalidSchemaPath;

        var component_start: usize = 0;
        for (path, 0..) |c, i| {
            if (c != '/') continue;
            try validateSchemaPathComponent(path[component_start..i]);
            component_start = i + 1;
        }
        try validateSchemaPathComponent(path[component_start..]);
    }

    fn hasWindowsDriveRoot(path: []const u8) bool {
        return path.len >= 3 and std.ascii.isAlphabetic(path[0]) and path[1] == ':' and path[2] == '/';
    }

    fn validateSchemaPathComponent(component: []const u8) !void {
        if (component.len == 0) return error.InvalidSchemaPath;
        if (std.mem.eql(u8, component, ".") or std.mem.eql(u8, component, "..")) return error.InvalidSchemaPath;
    }

    /// Walk the scope chain from a node to find its owning file node ID.
    fn findOwningFileId(self: *const Generator, id: schema.Id) ?schema.Id {
        var current_id = id;
        var depth: u32 = 0;
        while (depth < 64) : (depth += 1) {
            const node = self.getNode(current_id) orelse return null;
            if (node.kind == .file) return node.id;
            if (node.scope_id == current_id) return null; // self-referential, stop
            current_id = node.scope_id;
        }
        return null;
    }

    /// Return the import module name for a type if it belongs to a different file,
    /// or null if it belongs to the current file.
    fn typeModulePrefix(self: *Generator, type_id: schema.Id) !?[]const u8 {
        const current = self.current_file_id orelse return null;
        const owning_file = self.findOwningFileId(type_id) orelse return null;
        if (owning_file == current) return null;
        const prefix = self.import_modules.get(owning_file) orelse return null;
        try self.used_import_file_ids.put(owning_file, {});
        return prefix;
    }

    /// Resolve a type name, qualifying with module prefix if it's from another file.
    fn qualifiedTypeName(self: *Generator, id: schema.Id) ![]const u8 {
        const node = self.getNode(id) orelse return try self.allocator.dupe(u8, "void");
        const simple_name = self.getSimpleName(node);
        const zig_name = try types.normalizeAndEscapeTypeIdentifier(self.allocator, simple_name);

        if (try self.typeModulePrefix(id)) |prefix| {
            defer self.allocator.free(zig_name);
            return std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ prefix, zig_name });
        }
        return zig_name;
    }

    fn toSnakeCaseLower(self: *Generator, name: []const u8) ![]u8 {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);

        var prev_was_sep = false;
        for (name, 0..) |c, i| {
            if (!std.ascii.isAlphanumeric(c)) {
                if (out.items.len != 0 and !prev_was_sep) {
                    try out.append(self.allocator, '_');
                    prev_was_sep = true;
                }
                continue;
            }

            if (std.ascii.isUpper(c)) {
                if (i != 0 and out.items.len != 0 and !prev_was_sep) {
                    try out.append(self.allocator, '_');
                }
                try out.append(self.allocator, std.ascii.toLower(c));
                prev_was_sep = false;
                continue;
            }

            try out.append(self.allocator, c);
            prev_was_sep = false;
        }

        if (out.items.len == 0) {
            try out.append(self.allocator, 'x');
        }

        if (out.items[out.items.len - 1] == '_') {
            _ = out.pop();
        }

        return out.toOwnedSlice(self.allocator);
    }

    fn lookupNode(ctx: ?*anyopaque, id: schema.Id) ?*const schema.Node {
        const generator: *const Generator = @ptrCast(@alignCast(ctx.?));
        return generator.getNode(id);
    }

    fn lookupTypePrefix(ctx: ?*anyopaque, id: schema.Id) std.mem.Allocator.Error!?[]const u8 {
        const generator: *Generator = @ptrCast(@alignCast(ctx.?));
        return generator.typeModulePrefix(id);
    }

    fn typeNameForConst(self: *Generator, typ: schema.Type) ![]const u8 {
        if (types.primitiveTypeToZig(typ)) |prim| return try self.allocator.dupe(u8, prim);
        return switch (typ) {
            .list => |list_info| try self.listReaderTypeString(list_info.element_type.*),
            .@"enum" => |enum_info| blk: {
                if (self.getNode(enum_info.type_id)) |node| {
                    if (node.kind == .@"enum") {
                        break :blk try self.allocTypeDeclName(node);
                    }
                }
                break :blk try self.allocator.dupe(u8, "u16");
            },
            .@"struct" => |struct_info| blk: {
                if (try self.structTypeName(struct_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "{s}.Reader", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.StructReader");
            },
            .interface => try self.allocator.dupe(u8, "message.Capability"),
            .any_pointer => try self.allocator.dupe(u8, "message.AnyPointerReader"),
            else => unreachable,
        };
    }

    fn constValueLiteral(self: *Generator, typ: schema.Type, value: schema.Value) !?[]const u8 {
        return switch (typ) {
            .void => if (value == .void) try self.allocator.dupe(u8, "{}") else null,
            .bool => if (value == .bool)
                try self.allocator.dupe(u8, if (value.bool) "true" else "false")
            else
                null,
            .int8 => if (value == .int8) try std.fmt.allocPrint(self.allocator, "@as(i8, {d})", .{value.int8}) else null,
            .uint8 => if (value == .uint8) try std.fmt.allocPrint(self.allocator, "@as(u8, {d})", .{value.uint8}) else null,
            .int16 => if (value == .int16) try std.fmt.allocPrint(self.allocator, "@as(i16, {d})", .{value.int16}) else null,
            .uint16 => if (value == .uint16) try std.fmt.allocPrint(self.allocator, "@as(u16, {d})", .{value.uint16}) else null,
            .int32 => if (value == .int32) try std.fmt.allocPrint(self.allocator, "@as(i32, {d})", .{value.int32}) else null,
            .uint32 => if (value == .uint32) try std.fmt.allocPrint(self.allocator, "@as(u32, {d})", .{value.uint32}) else null,
            .int64 => if (value == .int64) try std.fmt.allocPrint(self.allocator, "@as(i64, {d})", .{value.int64}) else null,
            .uint64 => if (value == .uint64) try std.fmt.allocPrint(self.allocator, "@as(u64, {d})", .{value.uint64}) else null,
            .float32 => if (value == .float32) blk: {
                const bits: u32 = @bitCast(value.float32);
                break :blk try std.fmt.allocPrint(self.allocator, "@bitCast(@as(u32, {d}))", .{bits});
            } else null,
            .float64 => if (value == .float64) blk: {
                const bits: u64 = @bitCast(value.float64);
                break :blk try std.fmt.allocPrint(self.allocator, "@bitCast(@as(u64, {d}))", .{bits});
            } else null,
            .@"enum" => if (value == .@"enum") blk: {
                const enum_val = value.@"enum";
                break :blk try std.fmt.allocPrint(self.allocator, "@enumFromInt(@as(u16, {d}))", .{enum_val});
            } else null,
            else => null,
        };
    }

    fn generateAnnotationUses(self: *Generator, node: *const schema.Node, writer: anytype) !void {
        const name = try self.allocAnnotationUseBaseName(node);
        defer self.allocator.free(name);

        if (node.annotations.len > 0) {
            try writer.print("pub const {s}_annotations = ", .{name});
            try self.writeAnnotationList(writer, node.annotations);
            try writer.writeAll(";\n\n");
        }

        switch (node.kind) {
            .@"struct" => if (node.struct_node) |struct_node| {
                var any = false;
                for (struct_node.fields) |field| {
                    if (field.annotations.len > 0) {
                        any = true;
                        break;
                    }
                }
                if (any) {
                    try writer.print(
                        "pub const {s}_field_annotations = [_]struct {{ name: []const u8, annotations: []const schema.AnnotationUse }}{{\n",
                        .{name},
                    );
                    for (struct_node.fields) |field| {
                        if (field.annotations.len == 0) continue;
                        try writer.print("    .{{ .name = \"{f}\", .annotations = ", .{std.zig.fmtString(field.name)});
                        try self.writeAnnotationList(writer, field.annotations);
                        try writer.writeAll(" },\n");
                    }
                    try writer.writeAll("};\n\n");
                }
            },
            .@"enum" => if (node.enum_node) |enum_node| {
                var any = false;
                for (enum_node.enumerants) |enumerant| {
                    if (enumerant.annotations.len > 0) {
                        any = true;
                        break;
                    }
                }
                if (any) {
                    try writer.print(
                        "pub const {s}_enumerant_annotations = [_]struct {{ name: []const u8, annotations: []const schema.AnnotationUse }}{{\n",
                        .{name},
                    );
                    for (enum_node.enumerants) |enumerant| {
                        if (enumerant.annotations.len == 0) continue;
                        try writer.print("    .{{ .name = \"{f}\", .annotations = ", .{std.zig.fmtString(enumerant.name)});
                        try self.writeAnnotationList(writer, enumerant.annotations);
                        try writer.writeAll(" },\n");
                    }
                    try writer.writeAll("};\n\n");
                }
            },
            .interface => if (node.interface_node) |interface_node| {
                var any = false;
                for (interface_node.methods) |method| {
                    if (method.annotations.len > 0) {
                        any = true;
                        break;
                    }
                }
                if (any) {
                    try writer.print(
                        "pub const {s}_method_annotations = [_]struct {{ name: []const u8, annotations: []const schema.AnnotationUse }}{{\n",
                        .{name},
                    );
                    for (interface_node.methods) |method| {
                        if (method.annotations.len == 0) continue;
                        try writer.print("    .{{ .name = \"{f}\", .annotations = ", .{std.zig.fmtString(method.name)});
                        try self.writeAnnotationList(writer, method.annotations);
                        try writer.writeAll(" },\n");
                    }
                    try writer.writeAll("};\n\n");
                }
            },
            else => {},
        }
    }

    fn writeAnnotationList(self: *Generator, writer: anytype, annotations: []const schema.AnnotationUse) !void {
        try writer.writeAll("&[_]schema.AnnotationUse{");
        for (annotations) |annotation| {
            try writer.print(".{{ .id = 0x{X}, .value = ", .{annotation.id});
            try self.writeValueLiteral(writer, annotation.value);
            try writer.writeAll(" },");
        }
        try writer.writeAll("}");
    }

    fn writeValueLiteral(self: *Generator, writer: anytype, value: schema.Value) !void {
        switch (value) {
            .void => try writer.writeAll("schema.Value{ .void = {} }"),
            .bool => |v| try writer.print("schema.Value{{ .bool = {} }}", .{v}),
            .int8 => |v| try writer.print("schema.Value{{ .int8 = @as(i8, {d}) }}", .{v}),
            .int16 => |v| try writer.print("schema.Value{{ .int16 = @as(i16, {d}) }}", .{v}),
            .int32 => |v| try writer.print("schema.Value{{ .int32 = @as(i32, {d}) }}", .{v}),
            .int64 => |v| try writer.print("schema.Value{{ .int64 = @as(i64, {d}) }}", .{v}),
            .uint8 => |v| try writer.print("schema.Value{{ .uint8 = @as(u8, {d}) }}", .{v}),
            .uint16 => |v| try writer.print("schema.Value{{ .uint16 = @as(u16, {d}) }}", .{v}),
            .uint32 => |v| try writer.print("schema.Value{{ .uint32 = @as(u32, {d}) }}", .{v}),
            .uint64 => |v| try writer.print("schema.Value{{ .uint64 = @as(u64, {d}) }}", .{v}),
            .float32 => |v| blk: {
                const bits: u32 = @bitCast(v);
                break :blk try writer.print("schema.Value{{ .float32 = @bitCast(@as(u32, {d})) }}", .{bits});
            },
            .float64 => |v| blk: {
                const bits: u64 = @bitCast(v);
                break :blk try writer.print("schema.Value{{ .float64 = @bitCast(@as(u64, {d})) }}", .{bits});
            },
            .text => |text| try writer.print("schema.Value{{ .text = \"{f}\" }}", .{std.zig.fmtString(text)}),
            .data => |data| blk: {
                try writer.writeAll("schema.Value{ .data = ");
                try self.writeByteArrayLiteral(writer, data);
                break :blk try writer.writeAll(" }");
            },
            .list => |info| blk: {
                try writer.writeAll("schema.Value{ .list = .{ .message_bytes = ");
                try self.writeByteArrayLiteral(writer, info.message_bytes);
                break :blk try writer.writeAll(" } }");
            },
            .@"enum" => |v| try writer.print("schema.Value{{ .@\"enum\" = @as(u16, {d}) }}", .{v}),
            .@"struct" => |info| blk: {
                try writer.writeAll("schema.Value{ .@\"struct\" = .{ .message_bytes = ");
                try self.writeByteArrayLiteral(writer, info.message_bytes);
                break :blk try writer.writeAll(" } }");
            },
            .interface => try writer.writeAll("schema.Value{ .interface = {} }"),
            .any_pointer => |info| blk: {
                try writer.writeAll("schema.Value{ .any_pointer = .{ .message_bytes = ");
                try self.writeByteArrayLiteral(writer, info.message_bytes);
                break :blk try writer.writeAll(" } }");
            },
        }
    }

    fn generatePointerConst(self: *Generator, name: []const u8, typ: schema.Type, value: schema.Value, writer: anytype) !void {
        const bytes = switch (value) {
            .list => |info| info.message_bytes,
            .@"struct" => |info| info.message_bytes,
            .any_pointer => |info| info.message_bytes,
            else => return,
        };

        const return_type = try self.pointerConstReturnType(typ);
        defer self.allocator.free(return_type);

        try writer.print("pub const {s} = struct {{\n", .{name});
        try writer.writeAll("    const _bytes = ");
        try self.writeByteArrayInitializer(writer, bytes);
        try writer.writeAll(";\n");
        try writer.writeAll("    const _segments = [_][]const u8{ _bytes[0..] };\n");
        try writer.writeAll(
            "    const _message = message.Message{ .allocator = std.heap.page_allocator, .segments = _segments[0..], .backing_data = null, .segments_owned = false };\n\n",
        );
        try writer.print("    pub fn get() !{s} {{\n", .{return_type});
        switch (typ) {
            .list => |list_info| {
                const elem_type = list_info.element_type.*;
                try writer.writeAll("        const root = try _message.getRootAnyPointer();\n");
                if (elem_type == .@"struct") {
                    try writer.writeAll("        const list = try root.getInlineCompositeList();\n");
                    try writer.writeAll("        return message.StructListReader{\n");
                    try writer.writeAll("            .message = &_message,\n");
                    try writer.writeAll("            .segment_id = list.segment_id,\n");
                    try writer.writeAll("            .elements_offset = list.elements_offset,\n");
                    try writer.writeAll("            .element_count = list.element_count,\n");
                    try writer.writeAll("            .data_words = list.data_words,\n");
                    try writer.writeAll("            .pointer_words = list.pointer_words,\n");
                    try writer.writeAll("        };\n");
                } else {
                    const element_size = try self.listElementSize(elem_type);
                    try writer.writeAll("        const list = try root.getList();\n");
                    try writer.print("        if (list.element_size != {}) return error.InvalidPointer;\n", .{element_size});
                    try writer.print("        return {s}{{\n", .{return_type});
                    try writer.writeAll("            .message = &_message,\n");
                    try writer.writeAll("            .segment_id = list.segment_id,\n");
                    try writer.writeAll("            .elements_offset = list.content_offset,\n");
                    try writer.writeAll("            .element_count = list.element_count,\n");
                    try writer.writeAll("        };\n");
                }
            },
            .@"struct" => |struct_info| {
                if (try self.structTypeName(struct_info.type_id)) |struct_name| {
                    defer self.allocator.free(struct_name);
                    try writer.writeAll("        const value = try _message.getRootStruct();\n");
                    try writer.print("        return {s}.Reader{{ ._reader = value }};\n", .{struct_name});
                } else {
                    try writer.writeAll("        return try _message.getRootStruct();\n");
                }
            },
            .any_pointer => {
                try writer.writeAll("        return try _message.getRootAnyPointer();\n");
            },
            else => return error.InvalidPointerConstType,
        }
        try writer.writeAll("    }\n");
        try writer.writeAll("};\n\n");
    }

    fn pointerConstReturnType(self: *Generator, typ: schema.Type) ![]const u8 {
        return switch (typ) {
            .list => |list_info| try self.listReaderTypeString(list_info.element_type.*),
            .@"struct" => |struct_info| blk: {
                if (try self.structTypeName(struct_info.type_id)) |name| {
                    defer self.allocator.free(name);
                    break :blk try std.fmt.allocPrint(self.allocator, "{s}.Reader", .{name});
                }
                break :blk try self.allocator.dupe(u8, "message.StructReader");
            },
            .any_pointer => try self.allocator.dupe(u8, "message.AnyPointerReader"),
            else => return error.InvalidPointerConstType,
        };
    }

    fn listReaderTypeString(self: *Generator, elem_type: schema.Type) ![]const u8 {
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

    fn listElementSize(self: *Generator, elem_type: schema.Type) !u3 {
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

    fn structTypeName(self: *Generator, id: schema.Id) !?[]const u8 {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .@"struct") return null;
        return try self.allocTypeDeclName(node);
    }

    fn writeByteArray(self: *Generator, writer: anytype, prefix: []const u8, data: []const u8, suffix: []const u8) !void {
        _ = self;
        try writer.writeAll(prefix);
        for (data, 0..) |byte, i| {
            if (i != 0) try writer.writeAll(", ");
            try writer.print("0x{X:0>2}", .{byte});
        }
        try writer.writeAll(suffix);
    }

    fn writeByteArrayInitializer(self: *Generator, writer: anytype, data: []const u8) !void {
        return self.writeByteArray(writer, "[_]u8{", data, "}");
    }

    fn writeByteArrayLiteral(self: *Generator, writer: anytype, data: []const u8) !void {
        return self.writeByteArray(writer, "&[_]u8{", data, "}");
    }
};

// ---------------------------------------------------------------------------
// Inline unit tests for pure helper functions
// ---------------------------------------------------------------------------

fn testFileNode(id: schema.Id, filename: []const u8, nested_nodes: []schema.Node.NestedNode) schema.Node {
    return .{
        .id = id,
        .display_name = filename,
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested_nodes,
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
}

fn testStructNode(
    id: schema.Id,
    scope_id: schema.Id,
    name: []const u8,
    fields: []schema.Field,
    nested_nodes: []schema.Node.NestedNode,
) schema.Node {
    return .{
        .id = id,
        .display_name = name,
        .display_name_prefix_length = 0,
        .scope_id = scope_id,
        .nested_nodes = nested_nodes,
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 1,
            .pointer_count = 0,
            .preferred_list_encoding = .inline_composite,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
}

fn testUint32Field(name: []const u8, offset: u32) schema.Field {
    return .{
        .name = name,
        .code_order = @intCast(offset),
        .annotations = &[_]schema.AnnotationUse{},
        .discriminant_value = 0xFFFF,
        .slot = .{
            .offset = offset,
            .type = .uint32,
            .default_value = null,
        },
        .group = null,
    };
}

test "Generator.toSnakeCaseLower converts camelCase" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r1 = try gen.toSnakeCaseLower("myFieldName");
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("my_field_name", r1);
}

test "Generator.toSnakeCaseLower handles simple lowercase" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r = try gen.toSnakeCaseLower("simple");
    defer alloc.free(r);
    try std.testing.expectEqualStrings("simple", r);
}

test "Generator.toSnakeCaseLower converts PascalCase" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r = try gen.toSnakeCaseLower("PascalCase");
    defer alloc.free(r);
    try std.testing.expectEqualStrings("pascal_case", r);
}

test "Generator.toSnakeCaseLower strips non-alphanumeric separators" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r = try gen.toSnakeCaseLower("foo-bar.baz");
    defer alloc.free(r);
    try std.testing.expectEqualStrings("foo_bar_baz", r);
}

test "Generator.toSnakeCaseLower handles empty input" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r = try gen.toSnakeCaseLower("");
    defer alloc.free(r);
    try std.testing.expectEqualStrings("x", r);
}

test "Generator.toSnakeCaseLower trims trailing separator" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r = try gen.toSnakeCaseLower("foo-");
    defer alloc.free(r);
    try std.testing.expectEqualStrings("foo", r);
}

test "Generator.importPathFromCapnpName replaces .capnp with .zig" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r1 = try gen.importPathFromCapnpName("other.capnp");
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("other.zig", r1);

    const r2 = try gen.importPathFromCapnpName("path/to/types.capnp");
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("path/to/types.zig", r2);

    const r3 = try gen.importPathFromCapnpName("/capnp/stream.capnp");
    defer alloc.free(r3);
    try std.testing.expectEqualStrings("capnp/stream.zig", r3);
}

test "Generator.importPathFromCapnpName appends .zig for non-.capnp" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r = try gen.importPathFromCapnpName("something_else");
    defer alloc.free(r);
    try std.testing.expectEqualStrings("something_else.zig", r);
}

test "Generator.importPathFromCapnpName handles empty and minimal names" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    try std.testing.expectError(error.InvalidSchemaPath, gen.importPathFromCapnpName(""));
    try std.testing.expectError(error.InvalidSchemaPath, gen.importPathFromCapnpName("/"));

    const minimal = try gen.importPathFromCapnpName("x");
    defer alloc.free(minimal);
    try std.testing.expectEqualStrings("x.zig", minimal);
}

test "Generator.importPathFromCapnpName rejects unsafe import paths" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    try std.testing.expectError(error.InvalidSchemaPath, gen.importPathFromCapnpName("/"));
    try std.testing.expectError(error.InvalidSchemaPath, gen.importPathFromCapnpName("C:/tmp/schema.capnp"));
    try std.testing.expectError(error.InvalidSchemaPath, gen.importPathFromCapnpName("path//types.capnp"));
    try std.testing.expectError(error.InvalidSchemaPath, gen.importPathFromCapnpName("path/../types.capnp"));
    try std.testing.expectError(error.InvalidSchemaPath, gen.importPathFromCapnpName("path/./types.capnp"));
    try std.testing.expectError(error.InvalidSchemaPath, gen.importPathFromCapnpName("path\\types.capnp"));
}

test "Generator.lowerFirst lowercases first character" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r1 = try gen.lowerFirst("FooBar");
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("fooBar", r1);

    const r2 = try gen.lowerFirst("already");
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("already", r2);
}

test "Generator.lowerFirst handles empty string" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r = try gen.lowerFirst("");
    defer alloc.free(r);
    try std.testing.expectEqualStrings("", r);
}

test "Generator.lowerFirst handles single character" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r = try gen.lowerFirst("X");
    defer alloc.free(r);
    try std.testing.expectEqualStrings("x", r);
}

test "Generator.listElementSize returns correct Cap'n Proto element sizes" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    try std.testing.expectEqual(@as(u3, 0), try gen.listElementSize(.void));
    try std.testing.expectEqual(@as(u3, 1), try gen.listElementSize(.bool));
    try std.testing.expectEqual(@as(u3, 2), try gen.listElementSize(.int8));
    try std.testing.expectEqual(@as(u3, 2), try gen.listElementSize(.uint8));
    try std.testing.expectEqual(@as(u3, 3), try gen.listElementSize(.int16));
    try std.testing.expectEqual(@as(u3, 3), try gen.listElementSize(.uint16));
    try std.testing.expectEqual(@as(u3, 4), try gen.listElementSize(.int32));
    try std.testing.expectEqual(@as(u3, 4), try gen.listElementSize(.uint32));
    try std.testing.expectEqual(@as(u3, 4), try gen.listElementSize(.float32));
    try std.testing.expectEqual(@as(u3, 5), try gen.listElementSize(.int64));
    try std.testing.expectEqual(@as(u3, 5), try gen.listElementSize(.uint64));
    try std.testing.expectEqual(@as(u3, 5), try gen.listElementSize(.float64));
    try std.testing.expectEqual(@as(u3, 6), try gen.listElementSize(.text));
    try std.testing.expectEqual(@as(u3, 6), try gen.listElementSize(.data));
    try std.testing.expectEqual(@as(u3, 6), try gen.listElementSize(.any_pointer));
}

test "Generator.moduleNameFromFilename extracts snake_case module name" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const r1 = try gen.moduleNameFromFilename("MySchema.capnp");
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("my_schema", r1);

    const r2 = try gen.moduleNameFromFilename("path/to/types.capnp");
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("types", r2);

    const r3 = try gen.moduleNameFromFilename("simple.capnp");
    defer alloc.free(r3);
    try std.testing.expectEqualStrings("simple", r3);
}

test "Generator.uniqueImportModuleName escapes keywords and disambiguates collisions" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    var used = std.StringHashMap(void).init(alloc);
    defer used.deinit();

    const r1 = try gen.uniqueImportModuleName("a/foo.capnp", &used);
    defer alloc.free(r1);
    try std.testing.expectEqualStrings("foo", r1);

    const r2 = try gen.uniqueImportModuleName("b/foo.capnp", &used);
    defer alloc.free(r2);
    try std.testing.expectEqualStrings("foo_2", r2);

    const r3 = try gen.uniqueImportModuleName("error.capnp", &used);
    defer alloc.free(r3);
    try std.testing.expectEqualStrings("@\"error\"", r3);

    const r4 = try gen.uniqueImportModuleName("nested/error.capnp", &used);
    defer alloc.free(r4);
    try std.testing.expectEqualStrings("error_2", r4);
}

test "Generator.generateFile rejects duplicate file scope generated names" {
    const alloc = std.testing.allocator;

    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "foo", .id = 2 },
        .{ .name = "Foo", .id = 3 },
    };
    const root_file = testFileNode(1, "root.capnp", root_nested[0..]);
    const node_a = testStructNode(2, 1, "foo", &[_]schema.Field{}, &[_]schema.Node.NestedNode{});
    const node_b = testStructNode(3, 1, "Foo", &[_]schema.Field{}, &[_]schema.Node.NestedNode{});
    const nodes = [_]schema.Node{ root_file, node_a, node_b };

    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    try std.testing.expectError(error.DuplicateGeneratedName, gen.generateFile(requested));
}

test "Generator.generateFile rejects duplicate struct field generated names" {
    const alloc = std.testing.allocator;

    var fields = [_]schema.Field{
        testUint32Field("foo_bar", 0),
        testUint32Field("foo$bar", 1),
    };
    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "Root", .id = 2 },
    };
    const root_file = testFileNode(1, "root.capnp", root_nested[0..]);
    const root_struct = testStructNode(2, 1, "Root", fields[0..], &[_]schema.Node.NestedNode{});
    const nodes = [_]schema.Node{ root_file, root_struct };

    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    try std.testing.expectError(error.DuplicateGeneratedName, gen.generateFile(requested));
}

test "Generator.generateFile rejects duplicate enum enumerant generated names" {
    const alloc = std.testing.allocator;

    var enumerants = [_]schema.Enumerant{
        .{
            .name = "foo_bar",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
        },
        .{
            .name = "foo$bar",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
        },
    };
    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "Choice", .id = 2 },
    };
    const root_file = testFileNode(1, "root.capnp", root_nested[0..]);
    const enum_node = schema.Node{
        .id = 2,
        .display_name = "Choice",
        .display_name_prefix_length = 0,
        .scope_id = 1,
        .nested_nodes = &[_]schema.Node.NestedNode{},
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .@"enum",
        .struct_node = null,
        .enum_node = .{ .enumerants = enumerants[0..] },
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
    const nodes = [_]schema.Node{ root_file, enum_node };

    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    try std.testing.expectError(error.DuplicateGeneratedName, gen.generateFile(requested));
}

test "Generator.generateFile emits unique escaped import aliases" {
    const alloc = std.testing.allocator;

    var root_fields = [_]schema.Field{
        .{
            .name = "a",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 0,
                .type = .{ .@"struct" = .{ .type_id = 10 } },
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "b",
            .code_order = 1,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 1,
                .type = .{ .@"struct" = .{ .type_id = 20 } },
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "e",
            .code_order = 2,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0,
            .slot = .{
                .offset = 2,
                .type = .{ .@"struct" = .{ .type_id = 30 } },
                .default_value = null,
            },
            .group = null,
        },
    };

    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "Root", .id = 2 },
    };
    const root_file = schema.Node{
        .id = 1,
        .display_name = "root.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = root_nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const root_struct = schema.Node{
        .id = 2,
        .display_name = "Root",
        .display_name_prefix_length = 0,
        .scope_id = 1,
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
            .fields = &root_fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    var a_file_nested = [_]schema.Node.NestedNode{
        .{ .name = "AType", .id = 10 },
    };
    const a_file = schema.Node{
        .id = 100,
        .display_name = "a/foo.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = a_file_nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
    const a_type = schema.Node{
        .id = 10,
        .display_name = "AType",
        .display_name_prefix_length = 0,
        .scope_id = 100,
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

    var b_file_nested = [_]schema.Node.NestedNode{
        .{ .name = "BType", .id = 20 },
    };
    const b_file = schema.Node{
        .id = 200,
        .display_name = "b/foo.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = b_file_nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
    const b_type = schema.Node{
        .id = 20,
        .display_name = "BType",
        .display_name_prefix_length = 0,
        .scope_id = 200,
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

    var error_file_nested = [_]schema.Node.NestedNode{
        .{ .name = "ErrorType", .id = 30 },
    };
    const error_file = schema.Node{
        .id = 300,
        .display_name = "error.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = error_file_nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
    const error_type = schema.Node{
        .id = 30,
        .display_name = "ErrorType",
        .display_name_prefix_length = 0,
        .scope_id = 300,
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

    const nodes = [_]schema.Node{
        root_file,
        root_struct,
        a_file,
        a_type,
        b_file,
        b_type,
        error_file,
        error_type,
    };

    var imports = [_]schema.Import{
        .{ .id = 100, .name = "a/foo.capnp" },
        .{ .id = 200, .name = "b/foo.capnp" },
        .{ .id = 300, .name = "error.capnp" },
    };

    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = imports[0..],
    };

    const output = try gen.generateFile(requested);
    defer alloc.free(output);

    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "const foo = @import(\"a/foo.zig\");"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "const foo_2 = @import(\"b/foo.zig\");"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "const @\"error\" = @import(\"error.zig\");"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getA(self: Reader) !foo.AType.Reader"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getB(self: Reader) !foo_2.BType.Reader"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn getE(self: Reader) !@\"error\".ErrorType.Reader"));
}

test "Generator.generateFile can omit schema manifest emission" {
    const alloc = std.testing.allocator;

    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "Root", .id = 2 },
    };
    const root_file = schema.Node{
        .id = 1,
        .display_name = "root.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = root_nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const root_struct = schema.Node{
        .id = 2,
        .display_name = "Root",
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

    const nodes = [_]schema.Node{ root_file, root_struct };
    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();
    gen.setEmitSchemaManifest(false);

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested);
    defer alloc.free(output);

    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const Root = struct {"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, output, 1, "CAPNP_SCHEMA_MANIFEST_JSON"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, output, 1, "capnpSchemaManifestJson"));
}

test "Generator.generateFile compact api profile omits root init helpers" {
    const alloc = std.testing.allocator;

    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "Root", .id = 2 },
    };
    const root_file = schema.Node{
        .id = 1,
        .display_name = "root.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = root_nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const root_struct = schema.Node{
        .id = 2,
        .display_name = "Root",
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

    const nodes = [_]schema.Node{ root_file, root_struct };
    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();
    gen.setApiProfile(.compact);

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested);
    defer alloc.free(output);

    try std.testing.expect(!std.mem.containsAtLeast(u8, output, 1, "pub fn init(msg: *const message.Message) !Reader"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, output, 1, "pub fn init(msg: *message.MessageBuilder) !Builder"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn wrap(reader: message.StructReader) Reader"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn wrap(builder: message.StructBuilder) Builder"));
}

test "Generator.generateFile shape sharing aliases identical structs" {
    const alloc = std.testing.allocator;

    var fields_a = [_]schema.Field{
        .{
            .name = "value",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 0,
                .type = .uint32,
                .default_value = null,
            },
            .group = null,
        },
    };
    var fields_b = [_]schema.Field{
        .{
            .name = "value",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 0,
                .type = .uint32,
                .default_value = null,
            },
            .group = null,
        },
    };

    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "A", .id = 2 },
        .{ .name = "B", .id = 3 },
    };
    const root_file = schema.Node{
        .id = 1,
        .display_name = "root.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = root_nested[0..],
        .annotations = &[_]schema.AnnotationUse{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const node_a = schema.Node{
        .id = 2,
        .display_name = "A",
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
            .fields = fields_a[0..],
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const node_b = schema.Node{
        .id = 3,
        .display_name = "B",
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
            .fields = fields_b[0..],
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };

    const nodes = [_]schema.Node{ root_file, node_a, node_b };
    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();
    gen.setShapeSharing(true);

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested);
    defer alloc.free(output);

    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const A = struct {"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const B = A;"));
}

test "Generator.getSimpleName extracts name after prefix" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    const node1 = schema.Node{
        .id = 1,
        .display_name = "test.capnp:MyStruct",
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
    try std.testing.expectEqualStrings("MyStruct", gen.getSimpleName(&node1));

    const node2 = schema.Node{
        .id = 2,
        .display_name = "Standalone",
        .display_name_prefix_length = 0,
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
    try std.testing.expectEqualStrings("Standalone", gen.getSimpleName(&node2));

    // Prefix length exceeds display_name length -- should return entire display_name
    const node3 = schema.Node{
        .id = 3,
        .display_name = "short",
        .display_name_prefix_length = 100,
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
    try std.testing.expectEqualStrings("short", gen.getSimpleName(&node3));
}

test "Generator.getNode returns null for unknown ID" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    try std.testing.expect(gen.getNode(12345) == null);
}

test "Generator.getNode returns node by ID" {
    const alloc = std.testing.allocator;
    const nodes = [_]schema.Node{
        .{
            .id = 0xABCD,
            .display_name = "test",
            .display_name_prefix_length = 0,
            .scope_id = 0,
            .kind = .file,
            .nested_nodes = &.{},
            .annotations = &.{},
            .struct_node = null,
            .enum_node = null,
            .interface_node = null,
            .const_node = null,
            .annotation_node = null,
        },
    };
    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();

    const found = gen.getNode(0xABCD);
    try std.testing.expect(found != null);
    try std.testing.expectEqualStrings("test", found.?.display_name);
    try std.testing.expect(gen.getNode(0xDEAD) == null);
}

test "Generator.writeByteArrayLiteral formats bytes" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(alloc);
    const writer = ArrayListWriter{ .list = &buf, .allocator = alloc };

    try gen.writeByteArrayLiteral(writer, &[_]u8{ 0x00, 0xFF, 0x42 });
    try std.testing.expectEqualStrings("&[_]u8{0x00, 0xFF, 0x42}", buf.items);
}

test "Generator.writeByteArrayLiteral handles empty data" {
    const alloc = std.testing.allocator;
    var gen = Generator.init(alloc, &.{}) catch unreachable;
    defer gen.deinit();

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(alloc);
    const writer = ArrayListWriter{ .list = &buf, .allocator = alloc };

    try gen.writeByteArrayLiteral(writer, &[_]u8{});
    try std.testing.expectEqualStrings("&[_]u8{}", buf.items);
}
