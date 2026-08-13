const std = @import("std");
const schema = @import("../serialization/schema.zig");
const type_resolver = @import("../serialization/type_resolver.zig");
const brand_fidelity = @import("brand_fidelity.zig");
const StructGenerator = @import("struct_gen.zig").StructGenerator;
const interface_gen = @import("interface_gen.zig");
const types = @import("types.zig");
pub const TypeGenerator = types.TypeGenerator;

/// Minimal writer wrapping an unmanaged `ArrayList(u8)` with an explicit
/// allocator, providing `writeAll`, `print`, and `writeByte` methods
/// compatible with the duck-typed writer pattern used by the code generator.
pub const ArrayListWriter = struct {
    list: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    max_bytes: ?usize = null,

    fn reserve(self: ArrayListWriter, additional: usize) !void {
        const max_bytes = self.max_bytes orelse return;
        if (additional > max_bytes) return error.CodegenBudgetExceeded;
        if (self.list.items.len > max_bytes - additional) return error.CodegenBudgetExceeded;
    }

    pub fn writeAll(self: ArrayListWriter, bytes: []const u8) !void {
        try self.reserve(bytes.len);
        try self.list.appendSlice(self.allocator, bytes);
    }

    /// The error set is written out explicitly rather than inferred: `args`
    /// keeps this function generic, and a generic function's inferred error set
    /// cannot be resolved by the API-snapshot renderer, so an inferred `!void`
    /// here renders as an opaque marker and leaves the set unpinned. The set is
    /// the union of `reserve` (CodegenBudgetExceeded) and
    /// `std.ArrayList(u8).print` (OutOfMemory).
    pub fn print(self: ArrayListWriter, comptime fmt: []const u8, args: anytype) error{ CodegenBudgetExceeded, OutOfMemory }!void {
        try self.reserve(std.fmt.count(fmt, args));
        try self.list.print(self.allocator, fmt, args);
    }

    pub fn writeByte(self: ArrayListWriter, byte: u8) !void {
        try self.reserve(1);
        try self.list.append(self.allocator, byte);
    }

    pub fn writeByteNTimes(self: ArrayListWriter, byte: u8, n: usize) !void {
        try self.reserve(n);
        try self.list.appendNTimes(self.allocator, byte, n);
    }
};

/// Code generation driver that turns a set of parsed Cap'n Proto schema nodes
/// into idiomatic Zig source code with Reader and Builder types for each struct.
pub const Generator = struct {
    pub const ApiProfile = StructGenerator.ApiProfile;
    pub const CodegenBudget = struct {
        max_nodes: usize = 16 * 1024,
        max_imports: usize = 16 * 1024,
        max_fields: usize = 256 * 1024,
        max_name_bytes: usize = 8 * 1024 * 1024,
        max_default_bytes: usize = 16 * 1024 * 1024,
        max_manifest_bytes: usize = 8 * 1024 * 1024,
        max_output_bytes: usize = 32 * 1024 * 1024,
        max_brand_specializations: usize = 4096,
    };

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
    /// Resource limits for hostile or accidentally enormous schemas.
    codegen_budget: CodegenBudget = .{},
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

    /// Set resource limits enforced before and during code generation.
    pub fn setCodegenBudget(self: *Generator, budget: CodegenBudget) void {
        self.codegen_budget = budget;
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
        try self.validateCodegenBudget(requested_file);

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
        const body_writer = ArrayListWriter{
            .list = &body,
            .allocator = self.allocator,
            .max_bytes = self.codegen_budget.max_output_bytes,
        };

        if (self.emit_schema_manifest) {
            try self.writeSchemaManifest(requested_file, file_node, body_writer);
        }

        var generated = std.AutoHashMap(schema.Id, void).init(self.allocator);
        defer generated.deinit();

        // Generate code for all nested nodes (including nested definitions).
        for (file_node.nested_nodes) |nested| {
            try self.generateNodeRecursive(nested.id, &generated, body_writer, false);
        }

        var output = std.ArrayList(u8).empty;
        errdefer output.deinit(self.allocator);
        const writer = ArrayListWriter{
            .list = &output,
            .allocator = self.allocator,
            .max_bytes = self.codegen_budget.max_output_bytes,
        };

        // Write file header
        try writer.writeAll("// Generated by capnpc-zig\n");
        try writer.print("// Source: {f}\n\n", .{std.zig.fmtString(requested_file.filename)});
        try writer.writeAll("const std = @import(\"std\");\n");
        try writer.writeAll("const capnpc = @import(\"capnpc-zig\");\n");
        try writer.writeAll("const message = capnpc.message;\n");
        try writer.writeAll("const schema = capnpc.schema;\n");
        // Brand views contain field-named wrapper types. Anchor schema type
        // references at the generated file namespace so those wrappers cannot
        // shadow their target declarations. Omit the alias from files that do
        // not emit such views to avoid unrelated generated-artifact churn.
        if (std.mem.indexOf(u8, body.items, "_capnp_file.") != null) {
            try writer.writeAll("const _capnp_file = @This();\n");
        }
        if (needs_rpc) {
            try writer.writeAll("const rpc = capnpc.rpc;\n");
        }

        // Emit only imports that are referenced by generated declarations.
        // Sibling imports are pub so consumers can name cross-schema types the
        // generated API returns (e.g. a Client whose method results live in an
        // imported schema module).
        for (requested_file.imports) |imp| {
            if (!self.used_import_file_ids.contains(imp.id)) continue;
            const mod_name = self.import_modules.get(imp.id) orelse continue;
            const import_path = try self.importPathFromCapnpName(imp.name);
            defer self.allocator.free(import_path);
            try writer.print("pub const {s} = @import(\"{f}\");\n", .{ mod_name, std.zig.fmtString(import_path) });
        }
        try writer.writeByte('\n');

        try writer.writeAll(body.items);

        return output.toOwnedSlice(self.allocator);
    }

    fn validateCodegenBudget(self: *const Generator, requested_file: schema.RequestedFile) !void {
        if (self.nodes.len > self.codegen_budget.max_nodes) return error.CodegenBudgetExceeded;
        if (requested_file.imports.len > self.codegen_budget.max_imports) return error.CodegenBudgetExceeded;

        var field_count: usize = 0;
        var name_bytes: usize = 0;
        var default_bytes: usize = 0;
        var brand_specializations: usize = 0;

        try addBudgeted(&name_bytes, requested_file.filename.len, self.codegen_budget.max_name_bytes);
        for (requested_file.imports) |imp| {
            try addBudgeted(&name_bytes, imp.name.len, self.codegen_budget.max_name_bytes);
        }

        for (self.nodes) |node| {
            try addBudgeted(&name_bytes, node.display_name.len, self.codegen_budget.max_name_bytes);
            try addAnnotationBudget(node.annotations, &default_bytes, self.codegen_budget.max_default_bytes);

            for (node.nested_nodes) |nested| {
                try addBudgeted(&name_bytes, nested.name.len, self.codegen_budget.max_name_bytes);
            }

            switch (node.kind) {
                .file => {},
                .@"struct" => {
                    const struct_info = node.struct_node orelse continue;
                    try addBudgeted(&field_count, struct_info.fields.len, self.codegen_budget.max_fields);
                    for (struct_info.fields) |field| {
                        try addBudgeted(&name_bytes, field.name.len, self.codegen_budget.max_name_bytes);
                        try addAnnotationBudget(field.annotations, &default_bytes, self.codegen_budget.max_default_bytes);
                        if (field.slot) |slot| {
                            const specialization_count = try self.concreteBrandSpecializationCount(
                                &node,
                                slot,
                                self.codegen_budget.max_brand_specializations - brand_specializations,
                            );
                            if (specialization_count != 0) {
                                try addBudgeted(
                                    &brand_specializations,
                                    specialization_count,
                                    self.codegen_budget.max_brand_specializations,
                                );
                            }
                            if (slot.default_value) |value| {
                                try addValueBudget(value, &default_bytes, self.codegen_budget.max_default_bytes);
                            }
                        }
                    }
                },
                .@"enum" => {
                    const enum_info = node.enum_node orelse continue;
                    for (enum_info.enumerants) |enumerant| {
                        try addBudgeted(&name_bytes, enumerant.name.len, self.codegen_budget.max_name_bytes);
                        try addAnnotationBudget(enumerant.annotations, &default_bytes, self.codegen_budget.max_default_bytes);
                    }
                },
                .interface => {
                    const interface_info = node.interface_node orelse continue;
                    for (interface_info.methods) |method| {
                        try addBudgeted(&name_bytes, method.name.len, self.codegen_budget.max_name_bytes);
                        try addAnnotationBudget(method.annotations, &default_bytes, self.codegen_budget.max_default_bytes);
                    }
                },
                .@"const" => {
                    const const_info = node.const_node orelse continue;
                    try addValueBudget(const_info.value, &default_bytes, self.codegen_budget.max_default_bytes);
                },
                .annotation => {},
            }
        }
    }

    fn concreteBrandSpecializationCount(
        self: *const Generator,
        owner: *const schema.Node,
        slot: schema.FieldSlot,
        remaining: usize,
    ) !usize {
        const inspection = (try self.concreteBrandInspection(owner, slot, remaining)) orelse return 0;
        return inspection.specialization_count;
    }

    fn concreteBrandInspection(
        self: *const Generator,
        owner: *const schema.Node,
        slot: schema.FieldSlot,
        remaining: usize,
    ) !?brand_fidelity.Inspection {
        if (slot.type != .@"struct") return null;
        const brand = switch (slot.type_metadata) {
            .named => |value| value,
            else => return null,
        };
        const target = self.getNode(slot.type.@"struct".type_id) orelse return error.InvalidStructNode;
        const caller = type_resolver.Resolver.init(self.nodes, owner, .{}) catch return error.InvalidStructNode;
        const resolver = caller.enterNamed(target.id, brand, caller.contextDepth()) catch return error.InvalidStructNode;
        return brand_fidelity.inspectApplication(
            target,
            &resolver,
            generatorBrandLookup,
            @ptrCast(@constCast(self)),
            remaining,
        ) catch |err| switch (err) {
            error.CodegenBudgetExceeded => return error.CodegenBudgetExceeded,
            error.InvalidSchema => return error.InvalidStructNode,
        };
    }

    fn addBudgeted(total: *usize, amount: usize, limit: usize) !void {
        if (amount > limit) return error.CodegenBudgetExceeded;
        if (total.* > limit - amount) return error.CodegenBudgetExceeded;
        total.* += amount;
    }

    fn addAnnotationBudget(
        annotations: []const schema.AnnotationUse,
        default_bytes: *usize,
        limit: usize,
    ) !void {
        for (annotations) |annotation| {
            try addValueBudget(annotation.value, default_bytes, limit);
        }
    }

    fn addValueBudget(value: schema.Value, default_bytes: *usize, limit: usize) !void {
        switch (value) {
            .text => |text| try addBudgeted(default_bytes, text.len, limit),
            .data => |data| try addBudgeted(default_bytes, data.len, limit),
            .list, .@"struct", .any_pointer => |pointer| try addBudgeted(
                default_bytes,
                pointer.message_bytes.len,
                limit,
            ),
            else => {},
        }
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
            try self.collectManifestSerdeEntries(nested.id, module_name, &seen, &entries, null);
        }

        // Backstop: parent-qualification makes export symbols unique by
        // construction, but identifier normalization (snake-casing) can still
        // fold two distinct source names onto one C symbol. The second export
        // would silently shadow the first at link time — fail loudly instead.
        {
            var by_export = std.StringHashMap(schema.Id).init(self.allocator);
            defer by_export.deinit();
            for (entries.items) |entry| {
                const slot = try by_export.getOrPut(entry.to_json_export);
                if (slot.found_existing) {
                    const first = self.getNode(slot.value_ptr.*);
                    const second = self.getNode(entry.id);
                    std.log.warn(
                        "duplicate serde export symbol '{s}' for schema types '{s}' and '{s}'",
                        .{
                            entry.to_json_export,
                            if (first) |n| n.display_name else "<unknown>",
                            if (second) |n| n.display_name else "<unknown>",
                        },
                    );
                    return error.DuplicateSerdeExportSymbol;
                }
                slot.value_ptr.* = entry.id;
            }
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
        var manifest_bytes: usize = 0;
        try addBudgeted(&manifest_bytes, manifest_json_bytes.len, self.codegen_budget.max_manifest_bytes);

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
        parent_override: ?[]const u8,
    ) !void {
        if (seen.contains(id)) return;
        const node = self.getNode(id) orelse return;
        try seen.put(id, {});

        if (node.kind == .@"struct") {
            const simple_name = self.getSimpleName(node);
            // Manifest names mirror the emitted (scope-qualified) type path:
            // nested types are Parent.Child and their C ABI export symbols
            // are parent-qualified too, so two same-simple-name types under
            // different parents — legal in source — cannot collide on the
            // exported symbol. Auto-generated method param/result structs
            // (scope_id == 0) qualify under their emitting interface.
            const parent_path = blk: {
                if (node.scope_id == 0) {
                    if (parent_override) |ov| break :blk try self.allocator.dupe(u8, ov);
                    break :blk try self.allocator.dupe(u8, "");
                }
                break :blk try self.parentScopePath(node.id);
            };
            defer self.allocator.free(parent_path);

            const type_name = blk: {
                const simple_ident = try self.toZigIdentifier(simple_name);
                if (parent_path.len == 0) break :blk simple_ident;
                defer self.allocator.free(simple_ident);
                break :blk try std.mem.join(self.allocator, ".", &.{ parent_path, simple_ident });
            };
            errdefer self.allocator.free(type_name);

            const type_export = blk: {
                const simple_snake = try self.toSnakeCaseLower(simple_name);
                if (parent_path.len == 0) break :blk simple_snake;
                defer self.allocator.free(simple_snake);
                var parts = std.ArrayList([]const u8).empty;
                defer {
                    for (parts.items) |part| self.allocator.free(part);
                    parts.deinit(self.allocator);
                }
                var segs = std.mem.splitScalar(u8, parent_path, '.');
                while (segs.next()) |seg| {
                    try parts.append(self.allocator, try self.toSnakeCaseLower(seg));
                }
                try parts.append(self.allocator, try self.allocator.dupe(u8, simple_snake));
                break :blk try std.mem.join(self.allocator, "_", parts.items);
            };
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
            try self.collectManifestSerdeEntries(nested.id, module_name, seen, entries, null);
        }

        if (node.kind == .interface) {
            const iface = node.interface_node orelse return;
            const iface_name = try self.toZigIdentifier(self.getSimpleName(node));
            defer self.allocator.free(iface_name);
            for (iface.methods) |method| {
                try self.collectManifestSerdeEntries(method.param_struct_type, module_name, seen, entries, iface_name);
                try self.collectManifestSerdeEntries(method.result_struct_type, module_name, seen, entries, iface_name);
            }
            // Also include superclass method param/result types; those structs
            // are emitted under the superclass interface, so qualify there.
            for (iface.superclasses) |parent_id| {
                const parent_node = self.getNode(parent_id) orelse continue;
                const parent_iface = parent_node.interface_node orelse continue;
                const parent_name = try self.toZigIdentifier(self.getSimpleName(parent_node));
                defer self.allocator.free(parent_name);
                for (parent_iface.methods) |method| {
                    try self.collectManifestSerdeEntries(method.param_struct_type, module_name, seen, entries, parent_name);
                    try self.collectManifestSerdeEntries(method.result_struct_type, module_name, seen, entries, parent_name);
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

    /// Generate code for a single node. `children` is the pre-rendered text of
    /// the node's nested named types (null if none), spliced inside the body of
    /// container kinds (struct / interface); ignored by leaf kinds. `self_qualify`
    /// is set for a struct nested inside another struct (see StructGenerator).
    fn generateNode(self: *Generator, node: *const schema.Node, writer: anytype, children: ?[]const u8, self_qualify: bool) !void {
        self.verboseLog("capnpc-zig: generating node id=0x{x} kind={s}\n", .{ node.id, @tagName(node.kind) });

        switch (node.kind) {
            .@"struct" => {
                if (self.shape_sharing) {
                    try self.generateStructWithShapeSharing(node, writer, children, self_qualify);
                } else {
                    try self.generateStruct(node, writer, children, self_qualify);
                }
            },
            .@"enum" => try self.generateEnum(node, writer),
            .interface => try self.generateInterface(node, writer, children, self_qualify),
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
        writer: anytype,
        self_qualify: bool,
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

        // Render this node's nested children (nested named types, and for an
        // interface its OWN method param/result structs) into a canonical-indent
        // blob, which the container emitter splices inside the node's body. This
        // is what puts `Outer1.Inner` under `Outer1` instead of at file scope,
        // and `Svc1.DoItParams` / `Svc2.DoItParams` under their own interfaces.
        // Superclass method params belong to the superclass and are emitted when
        // it is generated — not re-emitted here.
        var child_blob = std.ArrayList(u8).empty;
        defer child_blob.deinit(self.allocator);
        {
            const child_writer = ArrayListWriter{
                .list = &child_blob,
                .allocator = self.allocator,
                .max_bytes = self.codegen_budget.max_output_bytes,
            };
            // Every named nested struct self-qualifies Reader/Builder/WhichTag:
            // a lexical ancestor may itself be nested and expose the same names,
            // even when the immediate parent is an interface. Nested interfaces
            // likewise qualify Client/Server/VTable and related declarations.
            for (node.nested_nodes) |nested| {
                const child = self.getNode(nested.id) orelse continue;
                const child_self_qualify = child.kind == .@"struct" or child.kind == .interface;
                try self.generateNodeRecursive(nested.id, generated, child_writer, child_self_qualify);
            }
            if (node.kind == .interface) {
                const iface = node.interface_node orelse return error.InvalidInterfaceNode;
                for (iface.methods) |method| {
                    if (self.isAutoGeneratedMethodStruct(method.param_struct_type))
                        try self.generateNodeRecursive(method.param_struct_type, generated, child_writer, self_qualify);
                    if (self.isAutoGeneratedMethodStruct(method.result_struct_type))
                        try self.generateNodeRecursive(method.result_struct_type, generated, child_writer, self_qualify);
                }
            }
        }

        const children: ?[]const u8 = if (child_blob.items.len > 0) child_blob.items else null;
        try self.generateNode(node, writer, children, self_qualify);
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
        try file_scope.addCopy("_capnp_file");
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
        scope: *GeneratedNameScope,
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

        // The node's own decl name competes only with its SIBLINGS (the passed
        // parent scope). Nested types now emit inside this node's body, so they
        // live in a fresh per-node scope — a nested `Inner` under `Outer1` no
        // longer collides with a nested `Inner` under `Outer2`.
        try self.validateNodeGeneratedNames(node, scope);

        var child_scope = GeneratedNameScope.init(self.allocator);
        defer child_scope.deinit();

        for (node.nested_nodes) |nested| {
            // Every nested named type — structs, enums, AND interfaces — now emits
            // inside this node's body, so it lives in this node's own scope. A
            // nested `Handle` under `Alpha` no longer competes with a nested
            // `Handle` under `Beta` at file scope.
            try self.validateGeneratedNodeRecursive(nested.id, generated, &child_scope);
        }

        if (node.kind == .interface) {
            const iface = node.interface_node orelse return;
            // Own method param/result structs nest inside THIS interface, so two
            // interfaces each with a same-named method (both `DoItParams`) land in
            // distinct interface scopes. Superclass params are owned and validated
            // by the superclass's own walk — do not re-validate them here.
            for (iface.methods) |method| {
                if (self.isAutoGeneratedMethodStruct(method.param_struct_type))
                    try self.validateGeneratedNodeRecursive(method.param_struct_type, generated, &child_scope);
                if (self.isAutoGeneratedMethodStruct(method.result_struct_type))
                    try self.validateGeneratedNodeRecursive(method.result_struct_type, generated, &child_scope);
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

        try self.validateStructReaderNames(node, struct_info);
        try self.validateStructBuilderNames(node, struct_info);
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

    fn validateStructReaderNames(self: *Generator, node: *const schema.Node, struct_info: schema.StructNode) !void {
        var scope = GeneratedNameScope.init(self.allocator);
        defer scope.deinit();

        try scope.addCopy("_reader");
        try scope.addCopy("wrap");
        if (self.api_profile == .full) try scope.addCopy("init");
        if (struct_info.discriminant_count > 0) {
            try scope.addCopy("which");
            try scope.addCopy("whichOrdinal");
        }
        if (structHasDirectEnumSlot(struct_info)) {
            try scope.addCopy("EnumOrdinals");
            try scope.addCopy("enumOrdinals");
        }
        if (structHasDirectNestedListSlot(struct_info)) {
            try scope.addCopy("NestedLists");
            try scope.addCopy("nestedLists");
        }
        if (try self.structHasDirectConcreteBrandSlot(node, struct_info)) {
            try scope.addCopy("Brands");
            try scope.addCopy("brands");
        }
        if (structHasDirectPointerKindSlot(struct_info)) {
            try scope.addCopy("PointerKinds");
            try scope.addCopy("pointerKinds");
        }

        for (struct_info.fields) |field| {
            if (field.group == null and field.slot == null) continue;

            const cap_name = try self.allocFieldCapName(field.name);
            defer self.allocator.free(cap_name);

            try scope.addPrint("get{s}", .{cap_name});

            if (field.slot) |slot| {
                if (isPointerSlotType(slot.type)) {
                    try scope.addPrint("has{s}", .{cap_name});
                }
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

    fn validateStructBuilderNames(self: *Generator, node: *const schema.Node, struct_info: schema.StructNode) !void {
        var scope = GeneratedNameScope.init(self.allocator);
        defer scope.deinit();

        try scope.addCopy("_builder");
        try scope.addCopy("wrap");
        if (self.api_profile == .full) try scope.addCopy("init");
        if (structHasDirectEnumSlot(struct_info)) {
            try scope.addCopy("EnumOrdinals");
            try scope.addCopy("enumOrdinals");
        }
        if (structHasDirectNestedListSlot(struct_info)) {
            try scope.addCopy("NestedLists");
            try scope.addCopy("nestedLists");
        }
        if (try self.structHasDirectConcreteBrandSlot(node, struct_info)) {
            try scope.addCopy("Brands");
            try scope.addCopy("brands");
        }
        if (structHasDirectPointerKindSlot(struct_info)) {
            try scope.addCopy("PointerKinds");
            try scope.addCopy("pointerKinds");
        }

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
            if (isPointerSlotType(slot.type)) {
                try scope.addPrint("has{s}", .{cap_name});
            }
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

        try self.validateNestedListBuilderViewNames(struct_info);
    }

    fn validateNestedListBuilderViewNames(self: *Generator, struct_info: schema.StructNode) !void {
        if (!structHasDirectNestedListSlot(struct_info)) return;

        var scope = GeneratedNameScope.init(self.allocator);
        defer scope.deinit();
        try scope.addCopy("_builder");

        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (slot.type != .list or slot.type.list.element_type.* != .list) continue;

            const cap_name = try self.allocFieldCapName(field.name);
            defer self.allocator.free(cap_name);
            try scope.addPrint("init{s}", .{cap_name});
            try scope.addPrint("init{s}InSegment", .{cap_name});
        }
    }

    fn structHasDirectEnumSlot(struct_info: schema.StructNode) bool {
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (slot.type == .@"enum") return true;
        }
        return false;
    }

    fn structHasDirectNestedListSlot(struct_info: schema.StructNode) bool {
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (slot.type != .list) continue;
            if (slot.type.list.element_type.* == .list) return true;
        }
        return false;
    }

    fn pointerKind(slot: schema.FieldSlot) ?schema.TypeMetadata.AnyPointer.Unconstrained {
        if (slot.type != .any_pointer) return null;
        return switch (slot.type_metadata) {
            .any_pointer => |metadata| switch (metadata) {
                .unconstrained => |kind| if (kind == .any_kind) null else kind,
                else => null,
            },
            else => null,
        };
    }

    fn structHasDirectPointerKindSlot(struct_info: schema.StructNode) bool {
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (pointerKind(slot) != null) return true;
        }
        return false;
    }

    fn generatorBrandLookup(context: ?*anyopaque, id: schema.Id) ?*const schema.Node {
        const self: *const Generator = @ptrCast(@alignCast(context orelse return null));
        return self.getNode(id);
    }

    /// Compatibility predicate retained for the generator's focused tests.
    /// Production eligibility and accounting both use `brand_fidelity`.
    fn isFullyConcreteBrand(
        self: *const Generator,
        target: *const schema.Node,
        target_info: schema.StructNode,
        brand: schema.Brand,
    ) bool {
        _ = target_info;
        const resolver = type_resolver.Resolver.init(self.nodes, target, brand) catch return false;
        return (brand_fidelity.inspectApplication(
            target,
            &resolver,
            generatorBrandLookup,
            @ptrCast(@constCast(self)),
            self.codegen_budget.max_brand_specializations,
        ) catch return false) != null;
    }

    fn structHasDirectConcreteBrandSlot(self: *Generator, node: *const schema.Node, struct_info: schema.StructNode) !bool {
        for (struct_info.fields) |field| {
            const slot = field.slot orelse continue;
            if (try self.concreteBrandInspection(node, slot, self.codegen_budget.max_brand_specializations) != null) return true;
        }
        return false;
    }

    fn isPointerSlotType(typ: schema.Type) bool {
        return switch (typ) {
            .text, .data, .list, .@"struct", .any_pointer, .interface => true,
            else => false,
        };
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

        // Inherited methods are emitted into the same Client / PipelinedClient /
        // StreamClient / VTable namespaces as own methods (see generateInterface),
        // so the validator must walk the same ancestor set to catch collisions
        // between an own method and an inherited one, or between two inherited
        // methods that happen to share a Zig name.
        const ancestors = try self.collectAncestors(node);
        defer self.freeAncestors(ancestors);
        const has_streaming = self.hasStreamingMethods(node, ancestors);

        var interface_scope = GeneratedNameScope.init(self.allocator);
        defer interface_scope.deinit();

        try interface_scope.addCopy("interface_id");
        try interface_scope.addCopy("Method");
        try interface_scope.addCopy("Client");
        if (has_streaming) {
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
        if (has_streaming) {
            try stream_scope.addCopy("client");
            try stream_scope.addCopy("stream");
            try stream_scope.addCopy("init");
            try stream_scope.addCopy("waitStreaming");
        }

        var vtable_scope = GeneratedNameScope.init(self.allocator);
        defer vtable_scope.deinit();

        const scopes = InterfaceMethodScopes{
            .interface_scope = &interface_scope,
            .method_enum_scope = &method_enum_scope,
            .client_scope = &client_scope,
            .pipelined_scope = &pipelined_scope,
            .stream_scope = &stream_scope,
            .vtable_scope = &vtable_scope,
            .has_streaming = has_streaming,
        };

        // Own methods: these additionally introduce the Method enum value, the
        // {Method} call-struct declaration, and (for interface-typed results) the
        // {Method}Pipeline type into the interface's own namespace.
        for (interface_info.methods) |method| {
            try self.registerInterfaceMethodNames(method, scopes, true);
        }
        // Inherited methods: only the Client/PipelinedClient/StreamClient/VTable
        // members are re-emitted; the call-struct and Pipeline type live on the
        // ancestor, so they are not re-registered in the interface namespace.
        for (ancestors) |ancestor| {
            for (ancestor.methods) |method| {
                try self.registerInterfaceMethodNames(method, scopes, false);
            }
        }
    }

    /// Bundle of the per-declaration name scopes an interface's methods populate,
    /// so inherited and own methods can share one registration routine.
    const InterfaceMethodScopes = struct {
        interface_scope: *GeneratedNameScope,
        method_enum_scope: *GeneratedNameScope,
        client_scope: *GeneratedNameScope,
        pipelined_scope: *GeneratedNameScope,
        stream_scope: *GeneratedNameScope,
        vtable_scope: *GeneratedNameScope,
        has_streaming: bool,
    };

    /// Register the generated names a single method contributes. `is_own`
    /// controls whether the method also claims names in the interface's own
    /// namespace (Method enum value, call-struct type, Pipeline type); inherited
    /// methods reuse the ancestor's declarations and only add call/vtable names.
    fn registerInterfaceMethodNames(
        self: *Generator,
        method: schema.Method,
        scopes: InterfaceMethodScopes,
        is_own: bool,
    ) !void {
        if (is_own) {
            const method_name = try self.allocEscapedTypeIdentifier(method.name);
            try scopes.interface_scope.addOwned(method_name);

            const enum_name = try self.allocEscapedTypeIdentifier(method.name);
            try scopes.method_enum_scope.addOwned(enum_name);
        }

        const call_name = try self.allocMethodCallName(method.name);
        defer self.allocator.free(call_name);
        try scopes.client_scope.addCopy(call_name);
        try scopes.client_scope.addPrint("{s}WithOptions", .{call_name});
        try scopes.pipelined_scope.addCopy(call_name);
        try scopes.pipelined_scope.addPrint("{s}WithOptions", .{call_name});
        if (scopes.has_streaming) {
            try scopes.stream_scope.addCopy(call_name);
        }

        if (try self.methodHasInterfaceResultFields(method)) {
            if (is_own) {
                const pipeline_name = try self.allocMethodPipelineName(method.name);
                try scopes.interface_scope.addOwned(pipeline_name);
                try self.validatePipelineGeneratedNames(method);
            }
            try scopes.client_scope.addPrint("{s}Pipelined", .{call_name});
            try scopes.client_scope.addPrint("{s}PipelinedWithOptions", .{call_name});
        }

        const field_name = try self.allocMethodVTableFieldName(method.name);
        defer self.allocator.free(field_name);
        try scopes.vtable_scope.addCopy(field_name);
        if (!method.isStreaming()) {
            try scopes.vtable_scope.addPrint("{s}_deferred", .{field_name});
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

    pub fn allocMethodPipelineName(self: *Generator, method_name: []const u8) ![]const u8 {
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

    /// Generate a struct definition. `children` is the pre-rendered text of the
    /// struct's nested named types (null if none), spliced inside the body.
    /// `self_qualify` is set when the struct is nested inside another struct.
    fn generateStruct(self: *Generator, node: *const schema.Node, writer: anytype, children: ?[]const u8, self_qualify: bool) !void {
        var struct_gen = StructGenerator.initWithLookup(self.allocator, lookupNode, self);
        struct_gen.type_prefix_fn = lookupTypePrefix;
        struct_gen.parent_path_fn = lookupParentPath;
        struct_gen.max_brand_specializations = self.codegen_budget.max_brand_specializations;
        struct_gen.setApiProfile(self.api_profile);
        try struct_gen.generate(node, writer, children, self_qualify);
    }

    /// Generate a struct definition with optional shape sharing.
    ///
    /// Reuses the first declaration for identical struct bodies by emitting a
    /// type alias for later occurrences.
    fn generateStructWithShapeSharing(self: *Generator, node: *const schema.Node, writer: anytype, children: ?[]const u8, self_qualify: bool) !void {
        var struct_buf = std.ArrayList(u8).empty;
        defer struct_buf.deinit(self.allocator);

        {
            const struct_writer = ArrayListWriter{
                .list = &struct_buf,
                .allocator = self.allocator,
                .max_bytes = self.codegen_budget.max_output_bytes,
            };
            try self.generateStruct(node, struct_writer, children, self_qualify);
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
        // Store the canonical struct's FULL path so a later alias to it resolves
        // even when the canonical struct is nested (`pub const X = Outer.Inner;`).
        const owned_decl_name = try self.qualifiedTypeName(node.id);
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

        // The enumerants list is ordered by ordinal (numeric value), not declaration
        // order (code_order). The wire value IS the list index, so use it directly.
        for (enum_info.enumerants, 0..) |enumerant, ordinal| {
            const zig_name = try self.toZigIdentifier(enumerant.name);
            defer self.allocator.free(zig_name);
            const escaped_name = try types.escapeZigKeyword(self.allocator, zig_name);
            defer self.allocator.free(escaped_name);
            try writer.print("    {s} = {},\n", .{ escaped_name, ordinal });
        }

        try writer.writeAll("};\n\n");
    }

    /// Info about a single ancestor interface for code generation.
    pub const AncestorInfo = struct {
        interface_id: u64,
        name: []const u8,
        methods: []const schema.Method,
    };

    /// Walk superclasses recursively to collect all ancestor interfaces (not including self).
    /// Deduplicates by interface_id to handle diamond inheritance.
    pub fn collectAncestors(self: *Generator, node: *const schema.Node) ![]AncestorInfo {
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

    pub fn freeAncestors(self: *Generator, ancestors: []AncestorInfo) void {
        for (ancestors) |a| self.allocator.free(a.name);
        self.allocator.free(ancestors);
    }

    /// Check whether an interface (or any ancestor) has streaming methods.
    pub fn hasStreamingMethods(_: *Generator, node: *const schema.Node, ancestors: []const AncestorInfo) bool {
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
    /// Reindent every non-empty line of `block` by `spaces` and write it. Used
    /// to splice a canonical-indent nested-type blob inside a parent's body.
    /// Mirrors struct_gen's private `writeReindented`.
    pub fn writeReindented(writer: anytype, block: []const u8, spaces: usize) !void {
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

    fn generateInterface(self: *Generator, node: *const schema.Node, writer: anytype, children: ?[]const u8, self_qualify: bool) !void {
        return interface_gen.Interface(Generator).generateInterface(self, node, writer, children, self_qualify);
    }

    fn generateMethodStruct(self: *Generator, iface_node: *const schema.Node, method: schema.Method, ordinal: usize, qual: []const u8, writer: anytype) !void {
        return interface_gen.Interface(Generator).generateMethodStruct(self, iface_node, method, ordinal, qual, writer);
    }

    fn generateVTableField(self: *Generator, method: schema.Method, ancestor_name: ?[]const u8, qual: []const u8, writer: anytype) !void {
        return interface_gen.Interface(Generator).generateVTableField(self, method, ancestor_name, qual, writer);
    }

    fn resolveMethodCallParams(self: *Generator, method: schema.Method, interface_id_expr: ?[]const u8, ancestor_name: ?[]const u8, qual: []const u8) !interface_gen.Interface(Generator).MethodCallParams {
        return interface_gen.Interface(Generator).resolveMethodCallParams(self, method, interface_id_expr, ancestor_name, qual);
    }

    fn freeMethodCallParams(self: *Generator, params: interface_gen.Interface(Generator).MethodCallParams) void {
        return interface_gen.Interface(Generator).freeMethodCallParams(self, params);
    }

    fn generateClientCallMethod(self: *Generator, method: schema.Method, interface_id_expr: ?[]const u8, ancestor_name: ?[]const u8, qual: []const u8, writer: anytype) !void {
        return interface_gen.Interface(Generator).generateClientCallMethod(self, method, interface_id_expr, ancestor_name, qual, writer);
    }

    fn generateStreamClient(
        self: *Generator,
        node: *const schema.Node,
        interface_info: schema.InterfaceNode,
        ancestors: []const AncestorInfo,
        qual: []const u8,
        writer: anytype,
    ) !void {
        return interface_gen.Interface(Generator).generateStreamClient(self, node, interface_info, ancestors, qual, writer);
    }

    fn generateStreamClientCallMethod(
        self: *Generator,
        method: schema.Method,
        interface_id_expr: ?[]const u8,
        ancestor_name: ?[]const u8,
        qual: []const u8,
        writer: anytype,
    ) !void {
        return interface_gen.Interface(Generator).generateStreamClientCallMethod(self, method, interface_id_expr, ancestor_name, qual, writer);
    }

    fn generateClientPipelinedMethod(self: *Generator, method: schema.Method, ancestor_name: ?[]const u8, qual: []const u8, writer: anytype) !void {
        return interface_gen.Interface(Generator).generateClientPipelinedMethod(self, method, ancestor_name, qual, writer);
    }

    fn generatePipelineType(self: *Generator, method: schema.Method, ancestor_name: ?[]const u8, writer: anytype) !void {
        return interface_gen.Interface(Generator).generatePipelineType(self, method, ancestor_name, writer);
    }

    fn generatePipelinedClientCallMethod(self: *Generator, method: schema.Method, interface_id_expr: ?[]const u8, ancestor_name: ?[]const u8, qual: []const u8, writer: anytype) !void {
        return interface_gen.Interface(Generator).generatePipelinedClientCallMethod(self, method, interface_id_expr, ancestor_name, qual, writer);
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

    pub fn allocTypeDeclName(self: *Generator, node: *const schema.Node) ![]const u8 {
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

    /// Resolve the Zig path of an interface method's param/result struct.
    ///
    /// These structs carry `scope_id == 0` (no walkable parent), so
    /// `qualifiedTypeName` cannot reach the interface; they are emitted nested
    /// inside the interface body, so their reference is the interface's own
    /// qualified path plus the member simple name — e.g. `Persistent.SaveParams`,
    /// or `Svc1.DoItParams` vs `Svc2.DoItParams` for same-named methods across
    /// interfaces. Caller owns the returned slice.
    pub fn resolveMethodStructName(self: *Generator, iface_node: *const schema.Node, member_id: schema.Id) ![]const u8 {
        const member = self.getNode(member_id) orelse return self.allocator.dupe(u8, "void");
        // A method whose params/results are an EXISTING named struct (e.g.
        // `foo @0 () -> StreamResult`) has scope_id != 0 and is emitted at its
        // own location — reference it normally. Only the auto-generated
        // `<method>$Params` / `$Results` structs (scope_id == 0) are emitted
        // nested inside the interface and take the interface-qualified path.
        if (member.scope_id != 0) return self.qualifiedTypeName(member_id);
        const iface_path = try self.qualifiedTypeName(iface_node.id);
        defer self.allocator.free(iface_path);
        const member_simple = self.getSimpleName(member);
        const member_zig = try types.normalizeAndEscapeTypeIdentifier(self.allocator, member_simple);
        defer self.allocator.free(member_zig);
        return std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ iface_path, member_zig });
    }

    /// True when a method's param/result struct is the auto-generated
    /// `<method>$Params`/`$Results` node (scope_id == 0), which is emitted nested
    /// inside the interface. Named param/result structs are emitted at their own
    /// scope and must not be re-emitted (or validated) under the interface.
    fn isAutoGeneratedMethodStruct(self: *Generator, member_id: schema.Id) bool {
        const member = self.getNode(member_id) orelse return false;
        return member.scope_id == 0;
    }

    /// Describes an interface-typed pointer field in a struct.
    const InterfaceFieldInfo = struct {
        name: []const u8,
        type_name: []const u8,
        pointer_offset: u32,
    };

    /// Return the list of interface-typed pointer fields in the given struct node.
    /// Caller must free each entry's name and type_name, as well as the returned slice.
    pub fn getInterfaceFields(self: *Generator, struct_id: schema.Id) ![]InterfaceFieldInfo {
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

    pub fn freeInterfaceFields(self: *Generator, fields: []InterfaceFieldInfo) void {
        for (fields) |item| {
            self.allocator.free(item.name);
            self.allocator.free(item.type_name);
        }
        self.allocator.free(fields);
    }

    pub fn structLayout(self: *Generator, id: schema.Id) ?struct { data_words: u16, pointer_words: u16 } {
        const node = self.getNode(id) orelse return null;
        if (node.kind != .@"struct") return null;
        const info = node.struct_node orelse return null;
        return .{ .data_words = info.data_word_count, .pointer_words = info.pointer_count };
    }

    pub fn lowerFirst(self: *Generator, name: []const u8) ![]const u8 {
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
    pub fn toZigIdentifier(self: *Generator, name: []const u8) ![]const u8 {
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

    /// The parent-scope path for a node as a Zig-normalized, dot-joined string
    /// from the file root down to (but not including) the node itself — e.g.
    /// `"Outer1"` for `Outer1.Inner`, `"Wrapper.Svc"` for a nested interface's
    /// child, and `""` for a file-scoped type. Walks `scope_id` up to the file
    /// node, mirroring `findOwningFileId`. Returns `""` for interface method
    /// param/result structs (their `scope_id` is 0, so they have no walkable
    /// parent) — those are qualified via `resolveMethodStructName` instead.
    /// Caller owns the returned slice.
    fn parentScopePath(self: *Generator, id: schema.Id) ![]const u8 {
        var segments = std.ArrayList([]const u8).empty;
        defer {
            for (segments.items) |seg| self.allocator.free(seg);
            segments.deinit(self.allocator);
        }

        const start = self.getNode(id) orelse return self.allocator.dupe(u8, "");
        // Nested interfaces are emitted INSIDE their parent's body (self-qualified,
        // like nested structs), so their scope path walks parents too. A file-scoped
        // interface's `scope_id` is the file node, so the walk yields "" — same as
        // any file-scoped type — keeping file-scope output byte-identical.
        var current_id = start.scope_id;
        var depth: u32 = 0;
        while (depth < 64) : (depth += 1) {
            const node = self.getNode(current_id) orelse break;
            if (node.kind == .file) break;
            const simple = self.getSimpleName(node);
            const zig_name = try types.normalizeAndEscapeTypeIdentifier(self.allocator, simple);
            try segments.append(self.allocator, zig_name);
            if (node.scope_id == current_id) break; // self-referential guard
            current_id = node.scope_id;
        }

        if (segments.items.len == 0) return self.allocator.dupe(u8, "");

        // `segments` is child-to-root; emit root-to-child joined with '.'.
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);
        var i: usize = segments.items.len;
        while (i > 0) {
            i -= 1;
            try out.appendSlice(self.allocator, segments.items[i]);
            if (i > 0) try out.append(self.allocator, '.');
        }
        return out.toOwnedSlice(self.allocator);
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

    /// Resolve a type name to its full Zig path: `[module.][Parent.….]Simple`.
    /// A same-file, file-scoped type yields just `Simple` (byte-identical to the
    /// pre-nesting behavior); a nested type yields the dotted scope path from the
    /// file root (`Outer1.Inner`), cross-file prefixed with the import module.
    fn qualifiedTypeName(self: *Generator, id: schema.Id) ![]const u8 {
        const node = self.getNode(id) orelse return try self.allocator.dupe(u8, "void");
        const simple_name = self.getSimpleName(node);
        const zig_name = try types.normalizeAndEscapeTypeIdentifier(self.allocator, simple_name);
        defer self.allocator.free(zig_name);

        const parent = try self.parentScopePath(id);
        defer self.allocator.free(parent);
        const module = try self.typeModulePrefix(id); // borrowed; do not free

        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);
        if (module) |m| try out.appendSlice(self.allocator, m);
        if (parent.len > 0) {
            if (out.items.len > 0) try out.append(self.allocator, '.');
            try out.appendSlice(self.allocator, parent);
        }
        if (out.items.len > 0) try out.append(self.allocator, '.');
        try out.appendSlice(self.allocator, zig_name);
        return out.toOwnedSlice(self.allocator);
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

    /// Adapter for struct_gen's `parent_path_fn`: returns the owned parent-scope
    /// path for a nested type, or null (never an empty string) for a file-scoped
    /// type, so the caller frees exactly when a path is present.
    fn lookupParentPath(ctx: ?*anyopaque, id: schema.Id) std.mem.Allocator.Error!?[]const u8 {
        const generator: *Generator = @ptrCast(@alignCast(ctx.?));
        const path = try generator.parentScopePath(id);
        if (path.len == 0) {
            generator.allocator.free(path);
            return null;
        }
        return path;
    }

    fn typeNameForConst(self: *Generator, typ: schema.Type) ![]const u8 {
        if (types.primitiveTypeToZig(typ)) |prim| return try self.allocator.dupe(u8, prim);
        return switch (typ) {
            .list => |list_info| try self.listReaderTypeString(list_info.element_type.*),
            .@"enum" => |enum_info| blk: {
                if (self.getNode(enum_info.type_id)) |node| {
                    if (node.kind == .@"enum") {
                        break :blk try self.qualifiedTypeName(enum_info.type_id);
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
            // A schema.Type comes off a CodeGeneratorRequest parsed from stdin.
            // `unreachable` here is a panic in Debug and undefined behaviour in
            // ReleaseFast, on input this process does not control; an error is
            // the honest response. `typeNameForConst` is private and appears in
            // neither API snapshot, so widening its inferred error set is free.
            // The sibling `types.zig:typeToZig` has the identical `else` arm but
            // IS frozen Stable as `error{OutOfMemory}![]const u8`, so converting
            // it has to wait for a snapshot ceremony.
            else => error.UnsupportedConstType,
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
                    if (elem_type == .void) {
                        try writer.writeAll("        return .{ .element_count = list.element_count };\n");
                    } else {
                        try writer.print("        return {s}{{\n", .{return_type});
                        try writer.writeAll("            .message = &_message,\n");
                        try writer.writeAll("            .segment_id = list.segment_id,\n");
                        try writer.writeAll("            .elements_offset = list.content_offset,\n");
                        try writer.writeAll("            .element_count = list.element_count,\n");
                        try writer.writeAll("        };\n");
                    }
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
        return try self.qualifiedTypeName(id);
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

fn testPointerKindField(name: []const u8, offset: u32) schema.Field {
    return .{
        .name = name,
        .code_order = @intCast(offset),
        .annotations = &.{},
        .discriminant_value = 0xFFFF,
        .slot = .{
            .offset = offset,
            .type = .any_pointer,
            .default_value = null,
            .type_metadata = .{ .any_pointer = .{ .unconstrained = .list } },
        },
        .group = null,
    };
}

fn testBrandParameterField(name: []const u8, offset: u32, scope_id: schema.Id, parameter_index: u16) schema.Field {
    return .{
        .name = name,
        .code_order = @intCast(offset),
        .annotations = &.{},
        .discriminant_value = 0xFFFF,
        .slot = .{
            .offset = offset,
            .type = .any_pointer,
            .default_value = null,
            .type_metadata = .{ .any_pointer = .{ .parameter = .{
                .scope_id = scope_id,
                .parameter_index = parameter_index,
            } } },
        },
        .group = null,
    };
}

test "typed brand eligibility requires exact bindings for every lexical generic scope" {
    var outer_fields = [_]schema.Field{};
    var outer = testStructNode(2, 1, "Outer", outer_fields[0..], &.{});
    var outer_parameters = [_]schema.Parameter{.{ .name = "T" }};
    outer.parameters = outer_parameters[0..];
    outer.is_generic = true;

    var inner_fields = [_]schema.Field{
        testBrandParameterField("outerValue", 0, 2, 0),
        testBrandParameterField("innerValue", 1, 3, 0),
    };
    var inner = testStructNode(3, 2, "Inner", inner_fields[0..], &.{});
    var inner_parameters = [_]schema.Parameter{.{ .name = "U" }};
    inner.parameters = inner_parameters[0..];
    inner.is_generic = true;

    var bad_index_fields = [_]schema.Field{
        testBrandParameterField("bad", 0, 4, 1),
    };
    var bad_index = testStructNode(4, 2, "BadIndex", bad_index_fields[0..], &.{});
    var bad_index_parameters = [_]schema.Parameter{.{ .name = "V" }};
    bad_index.parameters = bad_index_parameters[0..];
    bad_index.is_generic = true;

    const file = testFileNode(1, "root.capnp", &.{});
    const nodes = [_]schema.Node{ file, outer, inner, bad_index };
    var gen = try Generator.init(std.testing.allocator, &nodes);
    defer gen.deinit();

    var text_expression = schema.TypeExpression{ .type = .text };
    var data_expression = schema.TypeExpression{ .type = .data };
    var outer_bindings = [_]schema.Brand.Binding{.{ .type = &text_expression }};
    var inner_bindings = [_]schema.Brand.Binding{.{ .type = &data_expression }};
    var valid_scopes = [_]schema.Brand.Scope{
        .{ .scope_id = 3, .binding = .{ .bind = inner_bindings[0..] } },
        .{ .scope_id = 2, .binding = .{ .bind = outer_bindings[0..] } },
    };
    const inner_node = gen.getNode(3).?;
    try std.testing.expect(gen.isFullyConcreteBrand(inner_node, inner_node.struct_node.?, .{ .scopes = valid_scopes[0..] }));

    var missing_outer_scopes = [_]schema.Brand.Scope{
        .{ .scope_id = 3, .binding = .{ .bind = inner_bindings[0..] } },
    };
    try std.testing.expect(!gen.isFullyConcreteBrand(inner_node, inner_node.struct_node.?, .{ .scopes = missing_outer_scopes[0..] }));

    var extra_inner_bindings = [_]schema.Brand.Binding{
        .{ .type = &data_expression },
        .{ .type = &text_expression },
    };
    var wrong_arity_scopes = [_]schema.Brand.Scope{
        .{ .scope_id = 3, .binding = .{ .bind = extra_inner_bindings[0..] } },
        .{ .scope_id = 2, .binding = .{ .bind = outer_bindings[0..] } },
    };
    try std.testing.expect(!gen.isFullyConcreteBrand(inner_node, inner_node.struct_node.?, .{ .scopes = wrong_arity_scopes[0..] }));

    var bad_index_bindings = [_]schema.Brand.Binding{.{ .type = &data_expression }};
    var bad_index_scopes = [_]schema.Brand.Scope{
        .{ .scope_id = 4, .binding = .{ .bind = bad_index_bindings[0..] } },
        .{ .scope_id = 2, .binding = .{ .bind = outer_bindings[0..] } },
    };
    const bad_index_node = gen.getNode(4).?;
    try std.testing.expect(!gen.isFullyConcreteBrand(bad_index_node, bad_index_node.struct_node.?, .{ .scopes = bad_index_scopes[0..] }));
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

test "Generator rejects normalized collisions in PointerKinds sidecars" {
    const alloc = std.testing.allocator;

    var fields = [_]schema.Field{
        testPointerKindField("shape_name", 0),
        testPointerKindField("shape$name", 1),
    };
    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "Root", .id = 2 },
    };
    const root_file = testFileNode(1, "root.capnp", root_nested[0..]);
    const root_struct = testStructNode(2, 1, "Root", fields[0..], &.{});
    const nodes = [_]schema.Node{ root_file, root_struct };

    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();
    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &.{},
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

test "Generator.generateFile qualifies serde export symbols by parent scope" {
    const alloc = std.testing.allocator;

    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "Outer1", .id = 2 },
        .{ .name = "Outer2", .id = 4 },
    };
    var outer1_nested = [_]schema.Node.NestedNode{.{ .name = "Inner", .id = 3 }};
    var outer2_nested = [_]schema.Node.NestedNode{.{ .name = "Inner", .id = 5 }};
    const root_file = testFileNode(1, "root.capnp", root_nested[0..]);
    const outer1 = testStructNode(2, 1, "Outer1", &[_]schema.Field{}, outer1_nested[0..]);
    const inner1 = testStructNode(3, 2, "Inner", &[_]schema.Field{}, &[_]schema.Node.NestedNode{});
    const outer2 = testStructNode(4, 1, "Outer2", &[_]schema.Field{}, outer2_nested[0..]);
    const inner2 = testStructNode(5, 4, "Inner", &[_]schema.Field{}, &[_]schema.Node.NestedNode{});
    const nodes = [_]schema.Node{ root_file, outer1, inner1, outer2, inner2 };

    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    const output = try gen.generateFile(requested);
    defer alloc.free(output);

    // Same simple name under different parents is source-legal; the export
    // symbols must be parent-qualified and therefore distinct.
    try std.testing.expect(std.mem.indexOf(u8, output, "capnp_root_outer1_inner_to_json") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "capnp_root_outer2_inner_to_json") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Outer1.Inner") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Outer2.Inner") != null);
}

test "Generator.generateFile rejects colliding serde export symbols" {
    const alloc = std.testing.allocator;

    // Distinct emitted Zig paths (FooBar.Baz vs Foo.BarBaz) whose
    // snake-cased C export symbols fold onto the same string
    // (capnp_root_foo_bar_baz_to_json) — the backstop must fail loudly.
    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "FooBar", .id = 2 },
        .{ .name = "Foo", .id = 4 },
    };
    var foobar_nested = [_]schema.Node.NestedNode{.{ .name = "Baz", .id = 3 }};
    var foo_nested = [_]schema.Node.NestedNode{.{ .name = "BarBaz", .id = 5 }};
    const root_file = testFileNode(1, "root.capnp", root_nested[0..]);
    const foobar = testStructNode(2, 1, "FooBar", &[_]schema.Field{}, foobar_nested[0..]);
    const baz = testStructNode(3, 2, "Baz", &[_]schema.Field{}, &[_]schema.Node.NestedNode{});
    const foo = testStructNode(4, 1, "Foo", &[_]schema.Field{}, foo_nested[0..]);
    const barbaz = testStructNode(5, 4, "BarBaz", &[_]schema.Field{}, &[_]schema.Node.NestedNode{});
    const nodes = [_]schema.Node{ root_file, foobar, baz, foo, barbaz };

    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    try std.testing.expectError(error.DuplicateSerdeExportSymbol, gen.generateFile(requested));
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

    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const foo = @import(\"a/foo.zig\");"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const foo_2 = @import(\"b/foo.zig\");"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub const @\"error\" = @import(\"error.zig\");"));
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

    var root_fields = [_]schema.Field{
        .{
            .name = "state",
            .code_order = 0,
            .annotations = &[_]schema.AnnotationUse{},
            .discriminant_value = 0xFFFF,
            .slot = .{
                .offset = 0,
                .type = .{ .@"enum" = .{ .type_id = 999 } },
                .default_value = null,
            },
            .group = null,
        },
        .{
            .name = "label",
            .code_order = 1,
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
    const root_struct = schema.Node{
        .id = 2,
        .display_name = "Root",
        .display_name_prefix_length = 0,
        .scope_id = 1,
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
            .fields = &root_fields,
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
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 2, "pub const EnumOrdinals = struct"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 2, "pub fn enumOrdinals(self: @This()) EnumOrdinals"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn hasLabel(self: Reader) bool"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "pub fn hasLabel(self: Builder) bool"));
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

test "Generator.generateFile enforces output byte budget" {
    const alloc = std.testing.allocator;

    const root_file = testFileNode(1, "root.capnp", &[_]schema.Node.NestedNode{});
    const nodes = [_]schema.Node{root_file};
    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();
    gen.setEmitSchemaManifest(false);
    gen.setCodegenBudget(.{ .max_output_bytes = 32 });

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    try std.testing.expectError(error.CodegenBudgetExceeded, gen.generateFile(requested));
}

test "Generator.generateFile enforces field budget" {
    const alloc = std.testing.allocator;

    var fields = [_]schema.Field{
        testUint32Field("a", 0),
        testUint32Field("b", 1),
    };
    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "Root", .id = 2 },
    };
    const root_file = testFileNode(1, "root.capnp", root_nested[0..]);
    const root_struct = testStructNode(2, 1, "Root", fields[0..], &[_]schema.Node.NestedNode{});
    const nodes = [_]schema.Node{ root_file, root_struct };
    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();
    gen.setEmitSchemaManifest(false);
    gen.setCodegenBudget(.{ .max_fields = 1 });

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    try std.testing.expectError(error.CodegenBudgetExceeded, gen.generateFile(requested));
}

test "Generator.generateFile enforces default byte budget" {
    const alloc = std.testing.allocator;

    var fields = [_]schema.Field{.{
        .name = "payload",
        .code_order = 0,
        .annotations = &[_]schema.AnnotationUse{},
        .discriminant_value = 0xFFFF,
        .slot = .{
            .offset = 0,
            .type = .data,
            .default_value = .{ .data = "abcdef" },
        },
        .group = null,
    }};
    var root_nested = [_]schema.Node.NestedNode{
        .{ .name = "Root", .id = 2 },
    };
    const root_file = testFileNode(1, "root.capnp", root_nested[0..]);
    const root_struct = testStructNode(2, 1, "Root", fields[0..], &[_]schema.Node.NestedNode{});
    const nodes = [_]schema.Node{ root_file, root_struct };
    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();
    gen.setEmitSchemaManifest(false);
    gen.setCodegenBudget(.{ .max_default_bytes = 4 });

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    try std.testing.expectError(error.CodegenBudgetExceeded, gen.generateFile(requested));
}

test "Generator.generateFile enforces manifest byte budget" {
    const alloc = std.testing.allocator;

    const root_file = testFileNode(1, "root.capnp", &[_]schema.Node.NestedNode{});
    const nodes = [_]schema.Node{root_file};
    var gen = try Generator.init(alloc, &nodes);
    defer gen.deinit();
    gen.setCodegenBudget(.{ .max_manifest_bytes = 4 });

    const requested = schema.RequestedFile{
        .id = 1,
        .filename = "root.capnp",
        .imports = &[_]schema.Import{},
    };

    try std.testing.expectError(error.CodegenBudgetExceeded, gen.generateFile(requested));
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
