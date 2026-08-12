//! Allocation-free resolution of Cap'n Proto brand/type metadata.
//!
//! The frozen `schema.Type` union deliberately erases generic applications.
//! This module combines it with the parallel `TypeMetadata` tree and a bounded
//! lexical environment.  It is internal so validation and code generation can
//! share one interpretation without growing the Stable schema representation.
const schema = @import("schema.zig");

pub const max_resolution_depth: usize = 64;
pub const ResolveError = error{InvalidSchema};

pub const Cursor = struct {
    expression: schema.TypeExpression,
    /// Number of active application frames visible to this expression.
    context_depth: u8,
};

pub const Resolution = struct {
    cursor: Cursor,
    /// True when a parameter is valid but has no concrete binding.  Consumers
    /// must retain the legacy erased AnyPointer behavior in this case.
    unbound: bool = false,
};

/// A bounded lexical environment. Frames are slices into the parsed request;
/// no allocation or ownership transfer occurs.
pub const Resolver = struct {
    nodes: []const schema.Node = &.{},
    lookup_context: ?*anyopaque = null,
    lookup_node: ?*const fn (?*anyopaque, schema.Id) ?*const schema.Node = null,
    frames: [max_resolution_depth]Frame = undefined,
    depth: u8 = 0,

    const Frame = struct {
        target_id: schema.Id,
        brand: schema.Brand,
    };

    const ParameterKey = struct {
        context_depth: u8,
        scope_id: schema.Id,
        parameter_index: u16,
    };

    pub fn init(
        nodes: []const schema.Node,
        root: *const schema.Node,
        root_brand: schema.Brand,
    ) !Resolver {
        var resolver = Resolver{ .nodes = nodes };
        try resolver.push(root.id, root_brand, 0);
        try resolver.validateBrandBindings(root_brand, 0, 0);
        return resolver;
    }

    pub fn initWithLookup(
        root: *const schema.Node,
        root_brand: schema.Brand,
        lookup_node: *const fn (?*anyopaque, schema.Id) ?*const schema.Node,
        lookup_context: ?*anyopaque,
    ) !Resolver {
        var resolver = Resolver{
            .lookup_context = lookup_context,
            .lookup_node = lookup_node,
        };
        try resolver.push(root.id, root_brand, 0);
        try resolver.validateBrandBindings(root_brand, 0, 0);
        return resolver;
    }

    /// Enter an application of a named type. `parent_context_depth` comes from
    /// the cursor containing the named expression, which matters when a bound
    /// type itself contains another branded type.
    pub fn enterNamed(
        self: Resolver,
        type_id: schema.Id,
        brand: schema.Brand,
        parent_context_depth: u8,
    ) !Resolver {
        var result = self;
        try result.push(type_id, brand, parent_context_depth);
        try result.validateBrandBindings(brand, parent_context_depth, 0);
        return result;
    }

    pub fn cursor(self: *const Resolver, expression: schema.TypeExpression) Cursor {
        return .{ .expression = expression, .context_depth = self.depth };
    }

    pub fn contextDepth(self: *const Resolver) u8 {
        return self.depth;
    }

    /// Resolve a parameter chain to its first concrete expression. A valid
    /// missing/unbound binding is reported rather than rejected so old erased
    /// accessors and old validation entry points remain compatible.
    pub fn resolve(self: *const Resolver, initial: Cursor) !Resolution {
        var current = initial;
        var seen: [max_resolution_depth]ParameterKey = undefined;
        var seen_count: usize = 0;

        while (true) {
            try validateMetadataShape(current.expression);
            if (current.expression.type != .any_pointer) return .{ .cursor = current };
            const any = switch (current.expression.metadata) {
                .none => return .{ .cursor = current, .unbound = true },
                .any_pointer => |value| value,
                else => return error.InvalidSchema,
            };
            const parameter = switch (any) {
                .parameter => |value| value,
                // Method-local implicit parameters have no schema-node lexical
                // scope in this API. They intentionally retain erased behavior.
                .implicit_method_parameter => return .{ .cursor = current, .unbound = true },
                .unconstrained => return .{ .cursor = current },
            };

            if (seen_count >= max_resolution_depth) return error.InvalidSchema;
            const key = ParameterKey{
                .context_depth = current.context_depth,
                .scope_id = parameter.scope_id,
                .parameter_index = parameter.parameter_index,
            };
            for (seen[0..seen_count]) |previous| {
                if (previous.context_depth == key.context_depth and
                    previous.scope_id == key.scope_id and
                    previous.parameter_index == key.parameter_index)
                {
                    return error.InvalidSchema;
                }
            }
            seen[seen_count] = key;
            seen_count += 1;

            const lookup = try self.lookupParameter(parameter, current.context_depth);
            switch (lookup) {
                .unbound => return .{ .cursor = current, .unbound = true },
                .bound => |bound| current = bound,
            }
        }
    }

    /// Validate the complete metadata/type-expression graph reachable from a
    /// cursor, including brand scopes, binding arity, parameter indexes and
    /// recursive list/binding depth.
    pub fn validateExpression(self: *const Resolver, cursor_value: Cursor) !void {
        try self.validateExpressionDepth(cursor_value, 0);
    }

    pub fn namedBrand(expression: schema.TypeExpression) !schema.Brand {
        return switch (expression.metadata) {
            .none => .{},
            .named => |brand| brand,
            else => error.InvalidSchema,
        };
    }

    pub fn listElement(cursor_value: Cursor) !Cursor {
        const element_type = switch (cursor_value.expression.type) {
            .list => |list| list.element_type.*,
            else => return error.InvalidSchema,
        };
        const element_metadata: schema.TypeMetadata = switch (cursor_value.expression.metadata) {
            .none => .none,
            .list => |metadata| metadata.*,
            else => return error.InvalidSchema,
        };
        return .{
            .expression = .{ .type = element_type, .metadata = element_metadata },
            .context_depth = cursor_value.context_depth,
        };
    }

    fn push(
        self: *Resolver,
        target_id: schema.Id,
        brand: schema.Brand,
        parent_context_depth: u8,
    ) !void {
        if (parent_context_depth > self.depth) return error.InvalidSchema;
        if (parent_context_depth >= max_resolution_depth) return error.InvalidSchema;
        const target = self.findNode(target_id) orelse return error.InvalidSchema;
        try self.validateBrand(target, brand);
        self.frames[parent_context_depth] = .{ .target_id = target_id, .brand = brand };
        self.depth = parent_context_depth + 1;
    }

    fn validateExpressionDepth(self: *const Resolver, initial: Cursor, depth: usize) ResolveError!void {
        if (depth >= max_resolution_depth) return error.InvalidSchema;
        try validateMetadataShape(initial.expression);
        const resolved = try self.resolve(initial);
        if (resolved.unbound) return;
        const cursor_value = resolved.cursor;

        switch (cursor_value.expression.type) {
            .list => {
                const element = try listElement(cursor_value);
                try self.validateExpressionDepth(element, depth + 1);
            },
            .@"enum" => |named| try self.validateNamedExpression(cursor_value, named.type_id, .@"enum", depth),
            .@"struct" => |named| try self.validateNamedExpression(cursor_value, named.type_id, .@"struct", depth),
            .interface => |named| try self.validateNamedExpression(cursor_value, named.type_id, .interface, depth),
            else => {},
        }
    }

    fn validateNamedExpression(
        self: *const Resolver,
        cursor_value: Cursor,
        type_id: schema.Id,
        expected_kind: schema.NodeKind,
        depth: usize,
    ) ResolveError!void {
        const target = self.findNode(type_id) orelse return error.InvalidSchema;
        if (target.kind != expected_kind) return error.InvalidSchema;
        const brand = try namedBrand(cursor_value.expression);
        try self.validateBrand(target, brand);
        try self.validateBrandBindings(brand, cursor_value.context_depth, depth + 1);
    }

    const ParameterLookup = union(enum) {
        unbound: void,
        bound: Cursor,
    };

    fn lookupParameter(
        self: *const Resolver,
        parameter: schema.TypeMetadata.AnyPointer.Parameter,
        initial_depth: u8,
    ) !ParameterLookup {
        const scope_node = self.findNode(parameter.scope_id) orelse return error.InvalidSchema;
        if (scope_node.parameters.len == 0 or parameter.parameter_index >= scope_node.parameters.len) {
            return error.InvalidSchema;
        }

        var context_depth = initial_depth;
        var hops: usize = 0;
        var saw_lexical_scope = false;
        while (context_depth > 0) {
            if (hops >= max_resolution_depth) return error.InvalidSchema;
            hops += 1;
            const frame_index = context_depth - 1;
            const frame = self.frames[frame_index];
            if (!try self.isLexicalScope(frame.target_id, parameter.scope_id)) {
                context_depth = frame_index;
                continue;
            }
            saw_lexical_scope = true;

            const scope = (try findBrandScope(frame.brand, parameter.scope_id)) orelse return .unbound;
            switch (scope.binding) {
                .inherit => context_depth = frame_index,
                .bind => |bindings| {
                    if (bindings.len != scope_node.parameters.len) return error.InvalidSchema;
                    return switch (bindings[parameter.parameter_index]) {
                        .unbound => .unbound,
                        .type => |expression| .{
                            .bound = .{
                                .expression = expression.*,
                                // A brand binding is written in its caller's lexical
                                // environment, not the newly-applied target's.
                                .context_depth = frame_index,
                            },
                        },
                    };
                },
            }
        }
        // Exhausting an inherited lexical frame is the normal erased-caller
        // case. A parameter whose declaring scope is unrelated to every
        // active frame is instead a malformed cross-scope reference.
        return if (saw_lexical_scope) .unbound else error.InvalidSchema;
    }

    fn validateBrand(
        self: *const Resolver,
        target: *const schema.Node,
        brand: schema.Brand,
    ) !void {
        if (brand.scopes.len > max_resolution_depth) return error.InvalidSchema;
        for (brand.scopes, 0..) |scope, i| {
            for (brand.scopes[0..i]) |previous| {
                if (previous.scope_id == scope.scope_id) return error.InvalidSchema;
            }
            if (!try self.isLexicalScope(target.id, scope.scope_id)) return error.InvalidSchema;
            const scope_node = self.findNode(scope.scope_id) orelse return error.InvalidSchema;
            if (scope_node.parameters.len == 0) return error.InvalidSchema;
            switch (scope.binding) {
                .inherit => {},
                .bind => |bindings| {
                    if (bindings.len != scope_node.parameters.len) return error.InvalidSchema;
                    for (bindings) |binding| switch (binding) {
                        .unbound => {},
                        .type => |expression| {
                            try validateMetadataShape(expression.*);
                            if (!isPointerCapable(expression.type)) return error.InvalidSchema;
                        },
                    };
                },
            }
        }
    }

    fn validateBrandBindings(
        self: *const Resolver,
        brand: schema.Brand,
        binding_context_depth: u8,
        depth: usize,
    ) !void {
        for (brand.scopes) |scope| switch (scope.binding) {
            .inherit => {},
            .bind => |bindings| for (bindings) |binding| switch (binding) {
                .unbound => {},
                .type => |expression| try self.validateExpressionDepth(.{
                    .expression = expression.*,
                    .context_depth = binding_context_depth,
                }, depth),
            },
        };
    }

    fn isLexicalScope(self: *const Resolver, target_id: schema.Id, scope_id: schema.Id) !bool {
        var cursor_id = target_id;
        var depth: usize = 0;
        while (cursor_id != 0) {
            if (depth >= max_resolution_depth) return error.InvalidSchema;
            depth += 1;
            const node = self.findNode(cursor_id) orelse return error.InvalidSchema;
            if (node.id == scope_id) return true;
            if (node.scope_id == node.id) return error.InvalidSchema;
            cursor_id = node.scope_id;
        }
        return false;
    }

    fn findNode(self: *const Resolver, id: schema.Id) ?*const schema.Node {
        if (self.lookup_node) |lookup| {
            if (lookup(self.lookup_context, id)) |node| return node;
        }
        for (self.nodes) |*node| if (node.id == id) return node;
        return null;
    }
};

fn findBrandScope(brand: schema.Brand, scope_id: schema.Id) !?schema.Brand.Scope {
    var found: ?schema.Brand.Scope = null;
    for (brand.scopes) |scope| {
        if (scope.scope_id != scope_id) continue;
        if (found != null) return error.InvalidSchema;
        found = scope;
    }
    return found;
}

fn validateMetadataShape(expression: schema.TypeExpression) !void {
    switch (expression.metadata) {
        .none => {},
        .list => if (expression.type != .list) return error.InvalidSchema,
        .named => if (expression.type != .@"enum" and
            expression.type != .@"struct" and
            expression.type != .interface) return error.InvalidSchema,
        .any_pointer => if (expression.type != .any_pointer) return error.InvalidSchema,
    }
}

fn isPointerCapable(typ: schema.Type) bool {
    return switch (typ) {
        .text, .data, .list, .@"struct", .interface, .any_pointer => true,
        else => false,
    };
}
