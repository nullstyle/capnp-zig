//! Experimental: resolve generic parameters and brands in a parsed schema.
//!
//! For code generators built on the Stable `request` and `schema` modules
//! that want to emit real generics instead of erasing them to AnyPointer.
//!
//! The frozen `schema.Type` union erases generic applications: a field of
//! type `T` reads as `any_pointer`, and one of type `Box(Text)` as plain
//! `Box`. The parallel `schema.TypeMetadata` tree keeps the parameter
//! references and the brand bindings. A `Context` reads both with the rules
//! that capnpc-zig's own validator and generator use, so a foreign generator
//! and capnpc-zig agree on what each expression means.
//!
//! This is a facade over the internal `type_resolver.zig`, which stays free
//! to change. Nothing here allocates. A `Context` holds a bounded copy of the
//! brand frames in force, at most `max_depth` of them.
const schema = @import("schema.zig");
const internal = @import("type_resolver.zig");

/// Every failure is a malformed schema graph: a brand that names a scope
/// outside the type's lexical scopes, a wrong binding count, a parameter
/// index out of range, a cycle, or nesting deeper than `max_depth`.
pub const Error = internal.ResolveError;

/// The deepest chain of nested brand applications a `Context` follows.
pub const max_depth: usize = internal.max_resolution_depth;

/// Finds a node by id, for a schema whose nodes are not in one slice.
pub const LookupFn = *const fn (context: ?*anyopaque, id: schema.Id) ?*const schema.Node;

/// A type expression resolved in a `Context`.
pub const Type = struct {
    /// The concrete expression. A bound parameter resolves to its binding.
    /// An unbound parameter stays the `any_pointer` expression that names it.
    expression: schema.TypeExpression,
    /// True when `expression` is a parameter with no binding where it is
    /// used. capnpc-zig erases such a field to AnyPointer.
    unbound: bool,
    /// The brand frames `expression` is read in. Pass this `Type` only to
    /// the `Context` that produced it.
    context_depth: u8,

    /// The generic parameter `expression` names, if it is one: its declaring
    /// node (`scope_id`) and its index in that node's `parameters`.
    pub fn parameter(self: Type) ?schema.TypeMetadata.AnyPointer.Parameter {
        return switch (self.expression.metadata) {
            .any_pointer => |any| switch (any) {
                .parameter => |value| value,
                else => null,
            },
            else => null,
        };
    }
};

/// The brand bindings in force inside one schema node.
pub const Context = struct {
    resolver: internal.Resolver,

    /// The context inside `node`, applied with `brand`. An empty brand
    /// (`.{}`) leaves `node`'s own parameters unbound, which is the view of
    /// a generic declaration itself. `nodes` must hold every node the
    /// resolution reaches, as `CodeGeneratorRequest.nodes` does.
    pub fn init(nodes: []const schema.Node, node: *const schema.Node, brand: schema.Brand) Error!Context {
        return .{ .resolver = try internal.Resolver.init(nodes, node, brand) };
    }

    /// `init` with a lookup function in place of a node slice.
    pub fn initWithLookup(
        node: *const schema.Node,
        brand: schema.Brand,
        lookup: LookupFn,
        lookup_context: ?*anyopaque,
    ) Error!Context {
        return .{ .resolver = try internal.Resolver.initWithLookup(node, brand, lookup, lookup_context) };
    }

    /// Resolve `expression` as written inside this context's node: a field
    /// slot's type, a method's parameter or result type, or a binding.
    pub fn resolve(self: *const Context, expression: schema.TypeExpression) Error!Type {
        return finish(try self.resolver.resolve(self.resolver.cursor(expression)));
    }

    /// The resolved element type of `list`, a resolved list type.
    pub fn listElement(self: *const Context, list: Type) Error!Type {
        const element = try internal.Resolver.listElement(cursorOf(list));
        return finish(try self.resolver.resolve(element));
    }

    /// The context inside the struct or interface that `named` applies,
    /// with its brand bound: inside `Box` with `T = Text` for `Box(Text)`.
    pub fn enter(self: *const Context, named: Type) Error!Context {
        const type_id = switch (named.expression.type) {
            .@"struct" => |value| value.type_id,
            .interface => |value| value.type_id,
            else => return error.InvalidSchema,
        };
        const brand = try internal.Resolver.namedBrand(named.expression);
        return .{ .resolver = try self.resolver.enterNamed(type_id, brand, named.context_depth) };
    }

    /// Check the whole graph reachable from `expression`: brand scopes,
    /// binding counts, parameter indexes and nesting depth.
    pub fn validate(self: *const Context, expression: schema.TypeExpression) Error!void {
        try self.resolver.validateExpression(self.resolver.cursor(expression));
    }

    fn finish(resolution: internal.Resolution) Type {
        return .{
            .expression = resolution.cursor.expression,
            .unbound = resolution.unbound,
            .context_depth = resolution.cursor.context_depth,
        };
    }

    fn cursorOf(value: Type) internal.Cursor {
        return .{ .expression = value.expression, .context_depth = value.context_depth };
    }
};
