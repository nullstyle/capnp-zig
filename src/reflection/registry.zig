//! Owned schema graph with borrowed, brand-aware reflection views.
const std = @import("std");
const wire = @import("../serialization/message.zig");
const schema = @import("../serialization/schema.zig");
const request = @import("../serialization/request_reader.zig");
const resolution = @import("../serialization/type_resolver.zig");
const AllocationBudget = @import("../serialization/allocation_budget.zig");

/// A generated type's identity and its complete binary schema dependency bundle.
pub const SchemaRef = struct {
    id: schema.Id,
    encoded_request: []const u8,

    pub fn load(self: SchemaRef, allocator: std.mem.Allocator) !Registry {
        return self.loadWithOptions(allocator, .{});
    }

    pub fn loadWithOptions(self: SchemaRef, allocator: std.mem.Allocator, options: Registry.Options) !Registry {
        const registry = try Registry.initWithOptions(allocator, self.encoded_request, options);
        errdefer registry.deinit();
        _ = try registry.get(self.id);
        return registry;
    }

    pub fn resolve(self: SchemaRef, registry: Registry) !Schema {
        return registry.get(self.id);
    }
};

/// Owns both the original wire descriptors and their parsed graph. All views,
/// including readers of pointer defaults, remain valid until deinit(). Copies
/// of Registry are borrowed handles; call deinit exactly once. Mutation of the
/// lazy default cache requires external synchronization if sharing across threads.
pub const Registry = struct {
    state: *State,

    pub const Options = struct {
        /// Reject larger requests before allocating or copying their bytes.
        max_input_bytes: usize = 64 * 1024 * 1024,
        /// Total backing allocation, including registry state, arena overhead,
        /// parsed descriptors, and lazily cached pointer defaults.
        max_memory_bytes: usize = 128 * 1024 * 1024,
        max_nodes: usize = 65536,
        validation: wire.Message.ValidationOptions = .{},
    };

    const State = struct {
        owner: std.mem.Allocator,
        budget: AllocationBudget,
        options: Options,
        arena: std.heap.ArenaAllocator,
        bytes: []const u8,
        message: wire.Message,
        request: schema.CodeGeneratorRequest,
        by_id: std.AutoHashMap(schema.Id, usize),
        defaults: std.AutoHashMap(usize, *wire.Message),
    };

    pub fn init(allocator: std.mem.Allocator, encoded_request: []const u8) !Registry {
        return initWithOptions(allocator, encoded_request, .{});
    }

    pub fn initWithOptions(allocator: std.mem.Allocator, encoded_request: []const u8, options: Options) !Registry {
        if (encoded_request.len > options.max_input_bytes) return error.SchemaInputLimitExceeded;
        if (options.max_memory_bytes < @sizeOf(State)) return error.SchemaMemoryLimitExceeded;
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.owner = allocator;
        state.options = options;
        state.budget = .{ .parent = allocator, .limit = options.max_memory_bytes, .used = @sizeOf(State) };
        state.arena = std.heap.ArenaAllocator.init(state.budget.allocator());
        errdefer state.arena.deinit();
        return load(state, encoded_request) catch |err| {
            if (err == error.OutOfMemory and state.budget.denied) return error.SchemaMemoryLimitExceeded;
            return err;
        };
    }

    fn load(state: *State, encoded_request: []const u8) !Registry {
        const owned = state.arena.allocator();
        state.bytes = try owned.dupe(u8, encoded_request);
        state.message = try wire.Message.init(owned, state.bytes, state.options.validation);
        const root = try state.message.getRootStruct();
        const node_list = try root.readStructList(0);
        if (node_list.len() > state.options.max_nodes) return error.SchemaNodeLimitExceeded;
        state.request = try request.parseCodeGeneratorRequestMessage(owned, &state.message);
        state.by_id = std.AutoHashMap(schema.Id, usize).init(owned);
        state.defaults = std.AutoHashMap(usize, *wire.Message).init(owned);
        for (state.request.nodes, 0..) |node, index| {
            try validateLayout(node);
            const entry = try state.by_id.getOrPut(node.id);
            if (entry.found_existing) return error.DuplicateSchemaId;
            entry.value_ptr.* = index;
        }
        return .{ .state = state };
    }

    pub fn deinit(self: Registry) void {
        const owner = self.state.owner;
        self.state.arena.deinit();
        owner.destroy(self.state);
    }

    pub fn encodedRequest(self: Registry) []const u8 {
        return self.state.bytes;
    }

    pub fn nodes(self: Registry) []const schema.Node {
        return self.state.request.nodes;
    }

    pub fn get(self: Registry, id: schema.Id) !Schema {
        const index = self.state.by_id.get(id) orelse return error.SchemaNotFound;
        return .{ .registry = self, .node = &self.state.request.nodes[index] };
    }

    /// Returns a stable reader for a pointer default from this registry's parsed
    /// graph. Do not pass external Values. Materializing a mutable default must
    /// clone this reader into the destination message.
    pub fn defaultPointer(self: Registry, value: schema.Value) !wire.AnyPointerReader {
        return self.loadDefault(value) catch |err| {
            if (err == error.OutOfMemory and self.state.budget.denied) return error.SchemaMemoryLimitExceeded;
            return err;
        };
    }

    fn loadDefault(self: Registry, value: schema.Value) !wire.AnyPointerReader {
        const bytes = switch (value) {
            .list => |p| p.message_bytes,
            .@"struct" => |p| p.message_bytes,
            .any_pointer => |p| p.message_bytes,
            else => return error.TypeMismatch,
        };
        const key = @intFromPtr(bytes.ptr);
        if (self.state.defaults.get(key)) |msg| return msg.getRootAnyPointer();
        const allocator = self.state.arena.allocator();
        const msg = try allocator.create(wire.Message);
        msg.* = try wire.Message.initFlat(allocator, bytes, self.state.options.validation);
        try self.state.defaults.put(key, msg);
        return msg.getRootAnyPointer();
    }
};

// A valid wire message can still describe invalid offsets. Validate these
// before dynamic access so even wasm32 arithmetic stays bounded by a u16 word
// count. Missing external dependencies are resolved lazily by Registry.get().
fn validateLayout(node: schema.Node) !void {
    if (node.parameters.len > 65536) return error.InvalidSchema;
    if (node.struct_node) |info| {
        const data_bits = @as(u64, info.data_word_count) * 64;
        if (info.discriminant_count != 0 and
            (@as(u64, info.discriminant_offset) + 1) * 16 > data_bits)
            return error.InvalidSchema;
        for (info.fields) |field| {
            if (field.discriminant_value != 0xffff and field.discriminant_value >= info.discriminant_count)
                return error.InvalidSchema;
            if (field.slot) |slot| {
                const width: u64 = switch (slot.type) {
                    .void => 0,
                    .bool => 1,
                    .int8, .uint8 => 8,
                    .int16, .uint16, .@"enum" => 16,
                    .int32, .uint32, .float32 => 32,
                    .int64, .uint64, .float64 => 64,
                    else => {
                        if (slot.offset >= info.pointer_count) return error.InvalidSchema;
                        continue;
                    },
                };
                if ((@as(u64, slot.offset) + 1) * width > data_bits) return error.InvalidSchema;
            }
        }
    }
    if (node.enum_node) |info| {
        if (info.enumerants.len > 65536) return error.InvalidSchema;
    }
    if (node.interface_node) |info| {
        if (info.methods.len > 65536) return error.InvalidSchema;
    }
}

pub const Schema = struct {
    registry: Registry,
    node: *const schema.Node,

    pub fn id(self: Schema) schema.Id {
        return self.node.id;
    }

    pub fn proto(self: Schema) *const schema.Node {
        return self.node;
    }

    pub fn kind(self: Schema) schema.NodeKind {
        return self.node.kind;
    }

    pub fn displayName(self: Schema) []const u8 {
        return self.node.display_name;
    }

    /// The original schema::Node, including fields unknown to the parsed model.
    pub fn raw(self: Schema) !wire.StructReader {
        const root = try self.registry.state.message.getRootStruct();
        const list = try root.readStructList(0);
        const index = self.registry.state.by_id.get(self.id()) orelse return error.SchemaNotFound;
        return list.get(@intCast(index));
    }

    pub fn nested(self: Schema, name: []const u8) !Schema {
        for (self.node.nested_nodes) |child| {
            if (std.mem.eql(u8, child.name, name)) return self.registry.get(child.id);
        }
        return error.SchemaNotFound;
    }

    pub fn asStruct(self: Schema) !StructSchema {
        return self.asStructWithBrand(.{});
    }

    /// Explicit brands borrow their binding slices as well as the registry.
    /// Keep caller-owned bindings alive for the lifetime of the returned views.
    pub fn asStructWithBrand(self: Schema, brand: schema.Brand) !StructSchema {
        if (self.kind() != .@"struct") return error.TypeMismatch;
        return .{
            .schema = self,
            .resolver = try resolution.Resolver.init(self.registry.nodes(), self.node, brand),
        };
    }

    pub fn asEnum(self: Schema) !EnumSchema {
        if (self.kind() != .@"enum") return error.TypeMismatch;
        return .{ .schema = self };
    }

    pub fn asInterface(self: Schema) !InterfaceSchema {
        if (self.kind() != .interface) return error.TypeMismatch;
        return .{
            .schema = self,
            .resolver = try resolution.Resolver.init(self.registry.nodes(), self.node, .{}),
        };
    }
};

pub const StructSchema = struct {
    schema: Schema,
    resolver: resolution.Resolver,

    pub fn proto(self: StructSchema) *const schema.StructNode {
        return &self.schema.node.struct_node.?;
    }

    pub fn fields(self: StructSchema) []const schema.Field {
        return self.proto().fields;
    }

    pub fn field(self: StructSchema, name: []const u8) !Field {
        for (self.fields(), 0..) |entry, index| {
            if (std.mem.eql(u8, entry.name, name)) return .{ .parent = self, .index = index };
        }
        return error.FieldNotFound;
    }

    pub fn fieldByIndex(self: StructSchema, index: usize) !Field {
        if (index >= self.fields().len) return error.OutOfBounds;
        return .{ .parent = self, .index = index };
    }
};

pub const Field = struct {
    parent: StructSchema,
    index: usize,

    pub fn proto(self: Field) *const schema.Field {
        return &self.parent.proto().fields[self.index];
    }

    pub fn raw(self: Field) !wire.StructReader {
        return (try (try self.parent.schema.raw()).readStructList(3)).get(@intCast(self.index));
    }

    pub fn explicitOrdinal(self: Field) !?u16 {
        const reader = try self.raw();
        return switch (reader.readU16(10)) {
            0 => null,
            1 => reader.readU16(12),
            else => error.InvalidSchema,
        };
    }

    pub fn hadExplicitDefault(self: Field) !bool {
        if (self.proto().slot == null) return false;
        return (try self.raw()).readBool(16, 0);
    }

    pub fn @"type"(self: Field) !Type {
        const slot = self.proto().slot orelse return error.TypeMismatch;
        return .{
            .registry = self.parent.schema.registry,
            .resolver = self.parent.resolver,
            .cursor = self.parent.resolver.cursor(.{ .type = slot.type, .metadata = slot.type_metadata }),
        };
    }

    pub fn groupSchema(self: Field) !StructSchema {
        const group = self.proto().group orelse return error.TypeMismatch;
        const child = try self.parent.schema.registry.get(group.type_id);
        if (child.kind() != .@"struct" or !child.node.struct_node.?.is_group) return error.InvalidSchema;
        return .{ .schema = child, .resolver = self.parent.resolver };
    }
};

pub const EnumSchema = struct {
    schema: Schema,

    pub fn proto(self: EnumSchema) *const schema.EnumNode {
        return &self.schema.node.enum_node.?;
    }

    pub fn ordinal(self: EnumSchema, enumerant_name: []const u8) !u16 {
        for (self.proto().enumerants, 0..) |entry, index| {
            if (std.mem.eql(u8, entry.name, enumerant_name)) return @intCast(index);
        }
        return error.EnumerantNotFound;
    }

    /// Unknown ordinals are valid wire values and have no declared name.
    pub fn name(self: EnumSchema, value: u16) ?[]const u8 {
        if (value >= self.proto().enumerants.len) return null;
        return self.proto().enumerants[value].name;
    }
};

pub const Type = struct {
    registry: Registry,
    resolver: resolution.Resolver,
    cursor: resolution.Cursor,

    pub fn resolved(self: Type) !Type {
        const result = try self.resolver.resolve(self.cursor);
        return .{ .registry = self.registry, .resolver = self.resolver, .cursor = result.cursor };
    }

    pub fn proto(self: Type) !schema.Type {
        return (try self.resolved()).cursor.expression.type;
    }

    pub fn isUnbound(self: Type) !bool {
        return (try self.resolver.resolve(self.cursor)).unbound;
    }

    pub fn listElement(self: Type) !Type {
        const current = try self.resolved();
        if (current.cursor.expression.type != .list) return error.TypeMismatch;
        return .{
            .registry = self.registry,
            .resolver = self.resolver,
            .cursor = try resolution.Resolver.listElement(current.cursor),
        };
    }

    pub fn asStruct(self: Type) !StructSchema {
        const current = try self.resolved();
        const named = switch (current.cursor.expression.type) {
            .@"struct" => |value| value,
            else => return error.TypeMismatch,
        };
        const node = try self.registry.get(named.type_id);
        if (node.kind() != .@"struct") return error.InvalidSchema;
        return .{
            .schema = node,
            .resolver = try current.resolver.enterNamed(named.type_id, try resolution.Resolver.namedBrand(current.cursor.expression), current.cursor.context_depth),
        };
    }

    pub fn asEnum(self: Type) !EnumSchema {
        const value = try self.proto();
        if (value != .@"enum") return error.TypeMismatch;
        return (try self.registry.get(value.@"enum".type_id)).asEnum();
    }

    pub fn asInterface(self: Type) !InterfaceSchema {
        const current = try self.resolved();
        const named = switch (current.cursor.expression.type) {
            .interface => |value| value,
            else => return error.TypeMismatch,
        };
        const node = try self.registry.get(named.type_id);
        if (node.kind() != .interface) return error.InvalidSchema;
        return .{
            .schema = node,
            .resolver = try current.resolver.enterNamed(named.type_id, try resolution.Resolver.namedBrand(current.cursor.expression), current.cursor.context_depth),
        };
    }
};

pub const InterfaceSchema = struct {
    schema: Schema,
    resolver: resolution.Resolver,

    pub fn proto(self: InterfaceSchema) *const schema.InterfaceNode {
        return &self.schema.node.interface_node.?;
    }

    pub fn method(self: InterfaceSchema, name: []const u8) !Method {
        for (self.proto().methods, 0..) |entry, index| {
            if (std.mem.eql(u8, entry.name, name)) return .{ .parent = self, .index = index };
        }
        return error.MethodNotFound;
    }

    pub fn superclass(self: InterfaceSchema, index: usize) !InterfaceSchema {
        const node = self.proto();
        if (index >= node.superclasses.len) return error.OutOfBounds;
        const id = node.superclasses[index];
        const brand = if (node.superclass_brands.len == 0) schema.Brand{} else node.superclass_brands[index];
        const parent = try self.schema.registry.get(id);
        if (parent.kind() != .interface) return error.InvalidSchema;
        return .{
            .schema = parent,
            .resolver = try self.resolver.enterNamed(id, brand, self.resolver.contextDepth()),
        };
    }
};

pub const Method = struct {
    parent: InterfaceSchema,
    index: usize,

    pub fn proto(self: Method) *const schema.Method {
        return &self.parent.proto().methods[self.index];
    }

    pub fn params(self: Method) !StructSchema {
        return self.structSchema(self.proto().param_struct_type, self.proto().param_brand);
    }

    pub fn results(self: Method) !StructSchema {
        return self.structSchema(self.proto().result_struct_type, self.proto().result_brand);
    }

    fn structSchema(self: Method, id: schema.Id, brand: schema.Brand) !StructSchema {
        const node = try self.parent.schema.registry.get(id);
        if (node.kind() != .@"struct") return error.InvalidSchema;
        return .{
            .schema = node,
            .resolver = try self.parent.resolver.enterNamed(id, brand, self.parent.resolver.contextDepth()),
        };
    }
};
