//! Shared eligibility and accounting for executable generic brand views.
//!
//! Both the generator's preflight budget and `StructGenerator` call this
//! helper.  Keeping the traversal here prevents malformed, inherited, and
//! nested applications from being counted differently from the wrappers that
//! are emitted for them.
const std = @import("std");
const schema = @import("../serialization/schema.zig");
const type_resolver = @import("../serialization/type_resolver.zig");

pub const Error = error{ InvalidSchema, CodegenBudgetExceeded };
pub const LookupNode = *const fn (?*anyopaque, schema.Id) ?*const schema.Node;

pub const Inspection = struct {
    /// Number of application-specific wrappers rooted at this application.
    /// The root itself is one; nested generic struct fields add their own.
    specialization_count: usize,
};

/// Return `null` for a well-formed application that the typed sidecar cannot
/// represent. Malformed metadata is `InvalidSchema`.
pub fn inspectApplication(
    target: *const schema.Node,
    resolver: *const type_resolver.Resolver,
    lookup_node: LookupNode,
    lookup_context: ?*anyopaque,
    max_specializations: usize,
) Error!?Inspection {
    var specialization_count: usize = 0;
    if (!try inspectApplicationDepth(
        target,
        resolver,
        lookup_node,
        lookup_context,
        0,
        &specialization_count,
        max_specializations,
    )) return null;
    return .{ .specialization_count = specialization_count };
}

fn inspectApplicationDepth(
    target: *const schema.Node,
    resolver: *const type_resolver.Resolver,
    lookup_node: LookupNode,
    lookup_context: ?*anyopaque,
    depth: usize,
    specialization_count: *usize,
    max_specializations: usize,
) Error!bool {
    // Recursive generic schemas are legal, but an infinitely recursive typed
    // sidecar is not finite code. Treat the bounded application as unsupported
    // so generation retains the erased accessor instead of rejecting the
    // schema itself.
    if (depth >= type_resolver.max_resolution_depth) return false;
    if (target.kind != .@"struct") return error.InvalidSchema;
    const target_info = target.struct_node orelse return error.InvalidSchema;
    if (target_info.is_group or !target.is_generic) return false;

    if (!try applicationParametersSupported(
        target,
        resolver,
        lookup_node,
        lookup_context,
        depth,
    )) return false;

    if (specialization_count.* >= max_specializations) return error.CodegenBudgetExceeded;
    specialization_count.* += 1;

    // These are exactly the fields for which StructGenerator may recursively
    // emit a nested application wrapper: an erased parameter field or a named
    // generic struct field (including one whose brand inherits a caller scope).
    for (target_info.fields) |field| {
        if (field.group) |group| {
            const group_node = lookup_node(lookup_context, group.type_id) orelse return error.InvalidSchema;
            if (!try inspectApplicationFields(
                group_node,
                resolver,
                lookup_node,
                lookup_context,
                depth + 1,
                specialization_count,
                max_specializations,
            )) return false;
            continue;
        }
        const slot = field.slot orelse continue;
        const is_parameter = slot.type == .any_pointer and switch (slot.type_metadata) {
            .any_pointer => |metadata| metadata == .parameter,
            else => false,
        };
        const is_generic_named = if (slot.type == .@"struct") blk: {
            const named = lookup_node(lookup_context, slot.type.@"struct".type_id) orelse return error.InvalidSchema;
            break :blk named.kind == .@"struct" and named.is_generic;
        } else false;
        if (!is_parameter and !is_generic_named) continue;

        const initial = resolver.cursor(.{ .type = slot.type, .metadata = slot.type_metadata });
        if (!try inspectNestedApplication(
            resolver,
            initial,
            lookup_node,
            lookup_context,
            depth + 1,
            specialization_count,
            max_specializations,
        )) return false;
    }

    return true;
}

fn inspectApplicationFields(
    node: *const schema.Node,
    resolver: *const type_resolver.Resolver,
    lookup_node: LookupNode,
    lookup_context: ?*anyopaque,
    depth: usize,
    specialization_count: *usize,
    max_specializations: usize,
) Error!bool {
    if (depth >= type_resolver.max_resolution_depth) return false;
    if (node.kind != .@"struct") return error.InvalidSchema;
    const info = node.struct_node orelse return error.InvalidSchema;
    if (!info.is_group) return error.InvalidSchema;

    // The application-aware group forwarding wrapper is itself specialized
    // code and must be charged before descending into any branching children.
    if (specialization_count.* >= max_specializations) return error.CodegenBudgetExceeded;
    specialization_count.* += 1;

    for (info.fields) |field| {
        if (field.group) |group| {
            const child = lookup_node(lookup_context, group.type_id) orelse return error.InvalidSchema;
            if (!try inspectApplicationFields(
                child,
                resolver,
                lookup_node,
                lookup_context,
                depth + 1,
                specialization_count,
                max_specializations,
            )) return false;
            continue;
        }
        const slot = field.slot orelse continue;
        const is_parameter = slot.type == .any_pointer and switch (slot.type_metadata) {
            .any_pointer => |metadata| metadata == .parameter,
            else => false,
        };
        const is_generic_named = if (slot.type == .@"struct") blk: {
            const named = lookup_node(lookup_context, slot.type.@"struct".type_id) orelse return error.InvalidSchema;
            break :blk named.kind == .@"struct" and named.is_generic;
        } else false;
        if (!is_parameter and !is_generic_named) continue;
        const initial = resolver.cursor(.{ .type = slot.type, .metadata = slot.type_metadata });
        if (!try inspectNestedApplication(
            resolver,
            initial,
            lookup_node,
            lookup_context,
            depth + 1,
            specialization_count,
            max_specializations,
        )) return false;
    }
    return true;
}

fn applicationParametersSupported(
    target: *const schema.Node,
    resolver: *const type_resolver.Resolver,
    lookup_node: LookupNode,
    lookup_context: ?*anyopaque,
    depth: usize,
) Error!bool {
    if (depth >= type_resolver.max_resolution_depth) return false;

    var node: ?*const schema.Node = target;
    var lexical_depth: usize = 0;
    while (node) |scope| {
        if (lexical_depth >= type_resolver.max_resolution_depth) return error.InvalidSchema;
        lexical_depth += 1;
        for (scope.parameters, 0..) |_, parameter_index| {
            if (parameter_index > std.math.maxInt(u16)) return error.InvalidSchema;
            const parameter = resolver.cursor(.{
                .type = .any_pointer,
                .metadata = .{ .any_pointer = .{ .parameter = .{
                    .scope_id = scope.id,
                    .parameter_index = @intCast(parameter_index),
                } } },
            });
            if (!try supportsResolvedBinding(
                resolver,
                parameter,
                lookup_node,
                lookup_context,
                depth + 1,
                false,
            )) return false;
        }
        if (scope.scope_id == 0) break;
        node = lookup_node(lookup_context, scope.scope_id) orelse return error.InvalidSchema;
    }

    return true;
}

fn supportsResolvedBinding(
    resolver: *const type_resolver.Resolver,
    initial: type_resolver.Cursor,
    lookup_node: LookupNode,
    lookup_context: ?*anyopaque,
    depth: usize,
    list_element: bool,
) Error!bool {
    if (depth >= type_resolver.max_resolution_depth) return false;
    try resolver.validateExpression(initial);
    const resolution = try resolver.resolve(initial);
    if (resolution.unbound) return false;
    const cursor = resolution.cursor;
    return switch (cursor.expression.type) {
        .text, .data => true,
        .void, .bool, .int8, .uint8, .int16, .uint16, .int32, .uint32, .float32, .int64, .uint64, .float64, .@"enum" => list_element,
        .list => blk: {
            const element = try type_resolver.Resolver.listElement(cursor);
            break :blk try supportsResolvedBinding(
                resolver,
                element,
                lookup_node,
                lookup_context,
                depth + 1,
                true,
            );
        },
        .@"struct" => |named| blk: {
            const node = lookup_node(lookup_context, named.type_id) orelse return error.InvalidSchema;
            if (node.kind != .@"struct" or node.struct_node == null) return error.InvalidSchema;
            if (!node.is_generic) break :blk true;
            const brand = try type_resolver.Resolver.namedBrand(cursor.expression);
            const child = try resolver.enterNamed(named.type_id, brand, cursor.context_depth);
            break :blk try applicationParametersSupported(
                node,
                &child,
                lookup_node,
                lookup_context,
                depth + 1,
            );
        },
        .interface => |named| blk: {
            const node = lookup_node(lookup_context, named.type_id) orelse return error.InvalidSchema;
            break :blk node.kind == .interface;
        },
        .any_pointer => switch (cursor.expression.metadata) {
            .none => true,
            .any_pointer => |metadata| metadata == .unconstrained,
            else => false,
        },
    };
}

/// Count application-specific wrappers reachable through a resolved field
/// binding. Lists do not erase a finite branded struct terminal: each terminal
/// application receives a generated element adapter, recursively accounting
/// for applications nested inside that struct's own fields.
fn inspectNestedApplication(
    resolver: *const type_resolver.Resolver,
    initial: type_resolver.Cursor,
    lookup_node: LookupNode,
    lookup_context: ?*anyopaque,
    depth: usize,
    specialization_count: *usize,
    max_specializations: usize,
) Error!bool {
    if (depth >= type_resolver.max_resolution_depth) return false;
    try resolver.validateExpression(initial);
    const resolution = try resolver.resolve(initial);
    if (resolution.unbound) return true;
    const cursor = resolution.cursor;
    return switch (cursor.expression.type) {
        .list => inspectNestedApplication(
            resolver,
            try type_resolver.Resolver.listElement(cursor),
            lookup_node,
            lookup_context,
            depth + 1,
            specialization_count,
            max_specializations,
        ),
        .@"struct" => |named| blk: {
            const node = lookup_node(lookup_context, named.type_id) orelse return error.InvalidSchema;
            if (node.kind != .@"struct" or node.struct_node == null) return error.InvalidSchema;
            if (!node.is_generic) break :blk true;
            const brand = try type_resolver.Resolver.namedBrand(cursor.expression);
            const child = try resolver.enterNamed(named.type_id, brand, cursor.context_depth);
            break :blk try inspectApplicationDepth(
                node,
                &child,
                lookup_node,
                lookup_context,
                depth + 1,
                specialization_count,
                max_specializations,
            );
        },
        else => true,
    };
}

test "branching specialization inspection stops at the remaining budget" {
    const level_count = 25;
    const first_id: schema.Id = 0x1000;

    var parameters: [level_count][1]schema.Parameter = undefined;
    var fields: [level_count][2]schema.Field = undefined;
    var nodes: [level_count]schema.Node = undefined;
    var inherited_expressions: [level_count - 1]schema.TypeExpression = undefined;
    var inherited_bindings: [level_count - 1][1]schema.Brand.Binding = undefined;
    var inherited_scopes: [level_count - 1][1]schema.Brand.Scope = undefined;

    for (0..level_count) |level| {
        parameters[level][0] = .{ .name = "T" };
        if (level + 1 < level_count) {
            inherited_expressions[level] = .{
                .type = .any_pointer,
                .metadata = .{ .any_pointer = .{ .parameter = .{
                    .scope_id = first_id + level,
                    .parameter_index = 0,
                } } },
            };
            inherited_bindings[level][0] = .{ .type = &inherited_expressions[level] };
            inherited_scopes[level][0] = .{
                .scope_id = first_id + level + 1,
                .binding = .{ .bind = inherited_bindings[level][0..] },
            };
            for (0..2) |field_index| {
                fields[level][field_index] = .{
                    .name = if (field_index == 0) "left" else "right",
                    .code_order = @intCast(field_index),
                    .annotations = &.{},
                    .discriminant_value = 0xffff,
                    .slot = .{
                        .offset = @intCast(field_index),
                        .type = .{ .@"struct" = .{ .type_id = first_id + level + 1 } },
                        .default_value = null,
                        .type_metadata = .{ .named = .{ .scopes = inherited_scopes[level][0..] } },
                    },
                    .group = null,
                };
            }
        }

        nodes[level] = .{
            .id = first_id + level,
            .display_name = "Branch",
            .display_name_prefix_length = 0,
            .scope_id = 0,
            .nested_nodes = &.{},
            .annotations = &.{},
            .kind = .@"struct",
            .struct_node = .{
                .data_word_count = 0,
                .pointer_count = if (level + 1 < level_count) 2 else 0,
                .preferred_list_encoding = .pointer,
                .is_group = false,
                .discriminant_count = 0,
                .discriminant_offset = 0,
                .fields = if (level + 1 < level_count) fields[level][0..] else fields[level][0..0],
            },
            .enum_node = null,
            .interface_node = null,
            .const_node = null,
            .annotation_node = null,
            .parameters = parameters[level][0..],
            .is_generic = true,
        };
    }

    var text_expression = schema.TypeExpression{ .type = .text };
    var root_bindings = [_]schema.Brand.Binding{.{ .type = &text_expression }};
    var root_scopes = [_]schema.Brand.Scope{.{
        .scope_id = first_id,
        .binding = .{ .bind = root_bindings[0..] },
    }};

    const CountingLookup = struct {
        nodes: []const schema.Node,
        visits: usize = 0,

        fn find(context: ?*anyopaque, id: schema.Id) ?*const schema.Node {
            const self: *@This() = @ptrCast(@alignCast(context orelse return null));
            self.visits += 1;
            for (self.nodes) |*node| if (node.id == id) return node;
            return null;
        }
    };

    var lookup = CountingLookup{ .nodes = nodes[0..] };
    const resolver = try type_resolver.Resolver.initWithLookup(
        &nodes[0],
        .{ .scopes = root_scopes[0..] },
        CountingLookup.find,
        &lookup,
    );
    lookup.visits = 0;

    try std.testing.expectError(
        error.CodegenBudgetExceeded,
        inspectApplication(&nodes[0], &resolver, CountingLookup.find, &lookup, 8),
    );
    try std.testing.expect(lookup.visits > 0);
    // The unbounded binary tree has 2^25 - 1 application visits. This loose
    // ceiling deliberately permits validation lookups while proving the walk
    // stops near the remaining specialization budget.
    try std.testing.expect(lookup.visits < 1024);
}
