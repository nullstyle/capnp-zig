//! Concrete application identity shared by eligibility, budgets, and emission.
const std = @import("std");
const schema = @import("../serialization/schema.zig");
const resolution = @import("../serialization/type_resolver.zig");

pub const LookupNode = *const fn (?*anyopaque, schema.Id) ?*const schema.Node;
pub const Application = struct {
    target: *const schema.Node,
    resolver: resolution.Resolver,
};

/// Lists retain the lexical environment of their terminal named application.
pub fn fromSlot(owner: *const schema.Node, slot: schema.FieldSlot, lookup: LookupNode, context: ?*anyopaque) error{InvalidSchema}!?Application {
    if (slot.type != .@"struct" and slot.type != .list) return null;
    var cursor = resolution.Cursor{ .expression = .{ .type = slot.type, .metadata = slot.type_metadata }, .context_depth = 0 };
    var depth: usize = 0;
    while (cursor.expression.type == .list) {
        if (depth >= resolution.max_resolution_depth) return error.InvalidSchema;
        depth += 1;
        cursor = try resolution.Resolver.listElement(cursor);
    }
    // Unresolved ordinary names retain their established erased fallback.
    // Only an explicit generic application enters the brand resolver here.
    if (cursor.expression.type != .@"struct" or cursor.expression.metadata != .named) return null;
    const target = lookup(context, cursor.expression.type.@"struct".type_id) orelse return error.InvalidSchema;
    if (target.kind != .@"struct") return error.InvalidSchema;
    const info = target.struct_node orelse return error.InvalidSchema;
    if (!target.is_generic or info.is_group) return null;
    const caller = try resolution.Resolver.initWithLookup(owner, .{}, lookup, context);
    cursor.context_depth = caller.contextDepth();
    try caller.validateExpression(cursor);
    const brand = try resolution.Resolver.namedBrand(cursor.expression);
    return .{ .target = target, .resolver = try caller.enterNamed(target.id, brand, cursor.context_depth) };
}

pub const Ref = struct {
    target: *const schema.Node,
    resolver: *const resolution.Resolver,
};

/// Identity follows concrete bindings, including lexical parent parameters;
/// walking pointer fields here would recursively expand legal linked schemas.
pub fn same(a: Ref, b: Ref, lookup: LookupNode, context: ?*anyopaque) error{InvalidSchema}!bool {
    return sameDepth(a, b, lookup, context, 0);
}

fn sameDepth(a: Ref, b: Ref, lookup: LookupNode, context: ?*anyopaque, depth: usize) error{InvalidSchema}!bool {
    if (a.target.id != b.target.id) return false;
    if (depth >= resolution.max_resolution_depth) return error.InvalidSchema;
    var scope = a.target;
    var lexical_depth: usize = 0;
    while (true) {
        if (lexical_depth >= resolution.max_resolution_depth) return error.InvalidSchema;
        lexical_depth += 1;
        for (scope.parameters, 0..) |_, index| {
            if (index > std.math.maxInt(u16)) return error.InvalidSchema;
            const expression = schema.TypeExpression{
                .type = .any_pointer,
                .metadata = .{ .any_pointer = .{ .parameter = .{ .scope_id = scope.id, .parameter_index = @intCast(index) } } },
            };
            if (!try sameExpression(a.resolver, a.resolver.cursor(expression), b.resolver, b.resolver.cursor(expression), lookup, context, depth + 1)) return false;
        }
        if (scope.scope_id == 0) break;
        scope = lookup(context, scope.scope_id) orelse return error.InvalidSchema;
    }
    return true;
}

fn sameExpression(a: *const resolution.Resolver, ac: resolution.Cursor, b: *const resolution.Resolver, bc: resolution.Cursor, lookup: LookupNode, context: ?*anyopaque, depth: usize) error{InvalidSchema}!bool {
    if (depth >= resolution.max_resolution_depth) return error.InvalidSchema;
    const left = try a.resolve(ac);
    const right = try b.resolve(bc);
    if (left.unbound or right.unbound) {
        if (!left.unbound or !right.unbound) return false;
        const lm = left.cursor.expression.metadata;
        const rm = right.cursor.expression.metadata;
        if (lm != .any_pointer or rm != .any_pointer) return false;
        if (lm.any_pointer != .parameter or rm.any_pointer != .parameter) return false;
        return lm.any_pointer.parameter.scope_id == rm.any_pointer.parameter.scope_id and lm.any_pointer.parameter.parameter_index == rm.any_pointer.parameter.parameter_index;
    }
    const at = left.cursor.expression.type;
    const bt = right.cursor.expression.type;
    if (std.meta.activeTag(at) != std.meta.activeTag(bt)) return false;
    return switch (at) {
        .list => sameExpression(a, try resolution.Resolver.listElement(left.cursor), b, try resolution.Resolver.listElement(right.cursor), lookup, context, depth + 1),
        .@"enum" => |named| named.type_id == bt.@"enum".type_id,
        .@"struct", .interface => blk: {
            const aid = if (at == .@"struct") at.@"struct".type_id else at.interface.type_id;
            const bid = if (bt == .@"struct") bt.@"struct".type_id else bt.interface.type_id;
            if (aid != bid) break :blk false;
            const node = lookup(context, aid) orelse return error.InvalidSchema;
            const ar = try a.enterNamed(aid, try resolution.Resolver.namedBrand(left.cursor.expression), left.cursor.context_depth);
            const br = try b.enterNamed(bid, try resolution.Resolver.namedBrand(right.cursor.expression), right.cursor.context_depth);
            break :blk try sameDepth(.{ .target = node, .resolver = &ar }, .{ .target = node, .resolver = &br }, lookup, context, depth + 1);
        },
        .any_pointer => blk: {
            const am = left.cursor.expression.metadata;
            const bm = right.cursor.expression.metadata;
            if (am != .any_pointer or bm != .any_pointer) break :blk false;
            if (am.any_pointer != .unconstrained or bm.any_pointer != .unconstrained) break :blk false;
            break :blk am.any_pointer.unconstrained == bm.any_pointer.unconstrained;
        },
        else => true,
    };
}

/// Resolve every branded ancestor in the caller's environment. Equal diamonds
/// share one application. Conflicting applications remain distinct, matching
/// accepted reference schemas; callers must choose their intended ancestor.
pub fn interfaceAncestors(allocator: std.mem.Allocator, nodes: []const schema.Node, root: *const schema.Node, max_applications: usize) ![]Application {
    var result = std.ArrayList(Application).empty;
    errdefer result.deinit(allocator);
    const resolver = try resolution.Resolver.init(nodes, root, .{});
    try appendInterfaceAncestors(allocator, nodes, .{ .target = root, .resolver = resolver }, &result, max_applications, &.{});
    return result.toOwnedSlice(allocator);
}
fn nodeFromSlice(context: ?*anyopaque, id: schema.Id) ?*const schema.Node {
    const nodes: *[]const schema.Node = @ptrCast(@alignCast(context orelse return null));
    for (nodes.*) |*node| if (node.id == id) return node;
    return null;
}
fn appendInterfaceAncestors(allocator: std.mem.Allocator, nodes: []const schema.Node, current: Application, result: *std.ArrayList(Application), max_applications: usize, ancestors: []const schema.Id) (std.mem.Allocator.Error || error{ InvalidSchema, CodegenBudgetExceeded })!void {
    if (ancestors.len >= resolution.max_resolution_depth) return error.InvalidSchema;
    for (ancestors) |id| if (current.target.id == id) return error.InvalidSchema;
    var path: [resolution.max_resolution_depth]schema.Id = undefined;
    @memcpy(path[0..ancestors.len], ancestors);
    path[ancestors.len] = current.target.id;
    const iface = current.target.interface_node orelse return error.InvalidSchema;
    for (iface.superclasses, 0..) |id, index| {
        var node_slice = nodes;
        const target = nodeFromSlice(@ptrCast(&node_slice), id) orelse return error.InvalidSchema;
        const brand = if (index < iface.superclass_brands.len) iface.superclass_brands[index] else schema.Brand{};
        const application = Application{ .target = target, .resolver = try current.resolver.enterNamed(id, brand, current.resolver.contextDepth()) };
        var seen = false;
        for (result.items) |*previous| if (previous.target.id == id) {
            if (try same(.{ .target = previous.target, .resolver = &previous.resolver }, .{ .target = application.target, .resolver = &application.resolver }, nodeFromSlice, @ptrCast(&node_slice))) {
                seen = true;
                break;
            }
        };
        if (seen) continue;
        if (result.items.len >= max_applications) return error.CodegenBudgetExceeded;
        try result.append(allocator, application);
        try appendInterfaceAncestors(allocator, nodes, application, result, max_applications, path[0 .. ancestors.len + 1]);
    }
}
