//! Finds the declarations in generated Zig source that a function parameter,
//! a local constant or variable, or a capture shadows.
//!
//! Zig rejects a local that has the name of a declaration in any container
//! that encloses it ("function parameter shadows declaration of 'ctx'"). The
//! emitters name their locals plainly (`self`, `value`, `ctx`, `peer`, ...),
//! and a schema decides some declaration names: its constants and
//! annotations, and the aliases of the files it imports. So a schema with
//! `const ctx` beside an interface used to give a binding that did not
//! compile. The generator runs this check on each file it generates and
//! renames the schema declarations it reports (see
//! `Generator.renameShadowedDeclarations`).
//!
//! The check follows the compiler's rule (`detectLocalShadowing` in
//! std/zig/AstGen.zig): a container is the file or a `struct`, `enum`,
//! `union` or `opaque` body; its declarations are its `const`, `var` and `fn`
//! members; a binding is a parameter of a function that has a body, a local
//! `const` or `var`, or an `if`, `while`, `for`, `switch` or `catch`
//! capture. The parameters of a function type and block labels are not
//! checked by the compiler, so they are not bindings here.

const std = @import("std");
const Ast = std.zig.Ast;
const Allocator = std.mem.Allocator;

pub const Shadowed = struct {
    /// The path of the container that declares the shadowed name: the names
    /// of the `const X = struct {...}` members that lead to it from the file,
    /// joined with dots (`Outer.Inner`). Empty for the file itself. A
    /// container that is not the value of a member declaration (`return
    /// struct {...}`, or a local `const T = struct {...}`) contributes the
    /// segment `()`, which no declaration name can spell.
    container: []const u8,
    /// The declaration's name as the source spells it (`ctx`, `@"type"`).
    name: []const u8,
};

/// Every container declaration in `source` that a binding shadows, once per
/// (container, name), in source order of the first shadowing binding.
/// Source that does not parse yields an empty list. Allocates from `arena`.
pub fn findShadowedDeclarations(arena: Allocator, source: [:0]const u8) Allocator.Error![]const Shadowed {
    var tree = try Ast.parse(arena, source, .{});
    if (tree.errors.len != 0) return &.{};

    const containers = try collectContainers(arena, &tree);
    const bindings = try collectBindings(arena, &tree, containers.members);

    var found: std.ArrayList(Shadowed) = .empty;
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    // Walk the bindings in source order beside the containers sorted by
    // first token, keeping the stack of containers that enclose the binding.
    var stack: std.ArrayList(*const Container) = .empty;
    var next: usize = 0;
    for (bindings) |binding| {
        while (stack.items.len != 0 and stack.items[stack.items.len - 1].last < binding.token) _ = stack.pop();
        while (next < containers.list.len and containers.list[next].first <= binding.token) : (next += 1) {
            const container = &containers.list[next];
            while (stack.items.len != 0 and stack.items[stack.items.len - 1].last < container.first) _ = stack.pop();
            if (container.last >= binding.token) try stack.append(arena, container);
        }
        for (stack.items) |container| {
            const name = container.decls.get(binding.name) orelse continue;
            const key = try std.fmt.allocPrint(arena, "{s}\x00{s}", .{ container.path, name });
            const gop = try seen.getOrPut(arena, key);
            if (gop.found_existing) continue;
            try found.append(arena, .{ .container = container.path, .name = name });
        }
    }
    return found.items;
}

const Container = struct {
    first: Ast.TokenIndex,
    last: Ast.TokenIndex,
    path: []const u8,
    /// Declaration name (unquoted) -> the name as the source spells it.
    decls: std.StringHashMapUnmanaged([]const u8),
};

const Containers = struct {
    /// Sorted by first token; the file comes first.
    list: []Container,
    /// Every container member, so a `const` that is not one is a local.
    members: std.AutoHashMapUnmanaged(Ast.Node.Index, void),
};

const Binding = struct {
    token: Ast.TokenIndex,
    /// Unquoted name.
    name: []const u8,
};

fn collectContainers(arena: Allocator, tree: *const Ast) Allocator.Error!Containers {
    var members: std.AutoHashMapUnmanaged(Ast.Node.Index, void) = .empty;
    // A container that is the value of a member declaration takes the
    // declaration's name as its path segment.
    var names: std.AutoHashMapUnmanaged(Ast.Node.Index, []const u8) = .empty;
    var list: std.ArrayList(Container) = .empty;

    try list.append(arena, .{
        .first = 0,
        .last = @intCast(tree.tokens.len - 1),
        .path = "",
        .decls = try memberDecls(arena, tree, tree.rootDecls(), &members, &names),
    });
    // Node 0 is the file itself, which `fullContainerDecl` also accepts.
    var index: u32 = 1;
    while (index < tree.nodes.len) : (index += 1) {
        const node: Ast.Node.Index = @fromBackingInt(@intCast(index));
        var buffer: [2]Ast.Node.Index = undefined;
        const container = tree.fullContainerDecl(&buffer, node) orelse continue;
        try list.append(arena, .{
            .first = tree.firstToken(node),
            .last = tree.lastToken(node),
            .path = undefined,
            .decls = try memberDecls(arena, tree, container.ast.members, &members, &names),
        });
    }

    // A container can come before its parent in the node list, so name the
    // containers only once every member is seen. Then sort them and derive
    // each path from the innermost enclosing container.
    index = 1;
    var position: usize = 1;
    while (index < tree.nodes.len) : (index += 1) {
        const node: Ast.Node.Index = @fromBackingInt(@intCast(index));
        var buffer: [2]Ast.Node.Index = undefined;
        if (tree.fullContainerDecl(&buffer, node) == null) continue;
        list.items[position].path = names.get(node) orelse "()";
        position += 1;
    }
    std.mem.sort(Container, list.items[1..], {}, struct {
        fn lessThan(_: void, a: Container, b: Container) bool {
            return a.first < b.first;
        }
    }.lessThan);

    var stack: std.ArrayList(*const Container) = .empty;
    try stack.append(arena, &list.items[0]);
    for (list.items[1..]) |*container| {
        while (stack.items[stack.items.len - 1].last < container.first) _ = stack.pop();
        const parent = stack.items[stack.items.len - 1];
        container.path = if (parent.path.len == 0)
            container.path
        else
            try std.fmt.allocPrint(arena, "{s}.{s}", .{ parent.path, container.path });
        try stack.append(arena, container);
    }
    return .{ .list = list.items, .members = members };
}

fn memberDecls(
    arena: Allocator,
    tree: *const Ast,
    member_nodes: []const Ast.Node.Index,
    members: *std.AutoHashMapUnmanaged(Ast.Node.Index, void),
    names: *std.AutoHashMapUnmanaged(Ast.Node.Index, []const u8),
) Allocator.Error!std.StringHashMapUnmanaged([]const u8) {
    var decls: std.StringHashMapUnmanaged([]const u8) = .empty;
    for (member_nodes) |member| {
        try members.put(arena, member, {});
        const name_token = if (tree.fullVarDecl(member)) |var_decl| blk: {
            if (var_decl.ast.init_node.unwrap()) |init_node| {
                try names.put(arena, init_node, tree.tokenSlice(var_decl.ast.mut_token + 1));
            }
            break :blk var_decl.ast.mut_token + 1;
        } else switch (tree.nodeTag(member)) {
            .fn_decl, .fn_proto, .fn_proto_multi, .fn_proto_one, .fn_proto_simple => blk: {
                var buffer: [1]Ast.Node.Index = undefined;
                const proto_node = if (tree.nodeTag(member) == .fn_decl) tree.nodeData(member).node_and_node[0] else member;
                break :blk (tree.fullFnProto(&buffer, proto_node) orelse continue).name_token orelse continue;
            },
            else => continue,
        };
        const spelled = tree.tokenSlice(name_token);
        try decls.put(arena, try unquote(arena, spelled), spelled);
    }
    return decls;
}

fn collectBindings(
    arena: Allocator,
    tree: *const Ast,
    members: std.AutoHashMapUnmanaged(Ast.Node.Index, void),
) Allocator.Error![]const Binding {
    var bindings: std.ArrayList(Binding) = .empty;
    var index: u32 = 0;
    while (index < tree.nodes.len) : (index += 1) {
        const node: Ast.Node.Index = @fromBackingInt(@intCast(index));
        switch (tree.nodeTag(node)) {
            .fn_decl => {
                var buffer: [1]Ast.Node.Index = undefined;
                const proto = tree.fullFnProto(&buffer, tree.nodeData(node).node_and_node[0]).?;
                var params = proto.iterate(tree);
                while (params.next()) |param| {
                    const token = param.name_token orelse continue;
                    try addBinding(arena, tree, &bindings, token);
                }
            },
            .simple_var_decl, .local_var_decl, .aligned_var_decl, .global_var_decl => {
                if (members.contains(node)) continue;
                try addBinding(arena, tree, &bindings, tree.fullVarDecl(node).?.ast.mut_token + 1);
            },
            .if_simple, .@"if" => {
                const full = tree.fullIf(node).?;
                if (full.payload_token) |token| try addCaptureList(arena, tree, &bindings, token);
                if (full.error_token) |token| try addBinding(arena, tree, &bindings, token);
            },
            .while_simple, .while_cont, .@"while" => {
                const full = tree.fullWhile(node).?;
                if (full.payload_token) |token| try addCaptureList(arena, tree, &bindings, token);
                if (full.error_token) |token| try addBinding(arena, tree, &bindings, token);
            },
            .for_simple, .@"for" => try addCaptureList(arena, tree, &bindings, tree.fullFor(node).?.payload_token),
            .switch_case_one, .switch_case_inline_one, .switch_case, .switch_case_inline => {
                if (tree.fullSwitchCase(node).?.payload_token) |token| try addCaptureList(arena, tree, &bindings, token);
            },
            .@"catch" => {
                const keyword = tree.nodeMainToken(node);
                if (tree.tokenTag(keyword + 1) == .pipe) try addBinding(arena, tree, &bindings, keyword + 2);
            },
            else => {},
        }
    }
    std.mem.sort(Binding, bindings.items, {}, struct {
        fn lessThan(_: void, a: Binding, b: Binding) bool {
            return a.token < b.token;
        }
    }.lessThan);
    return bindings.items;
}

/// A capture list starts after its opening `|`: `x`, `*x`, `x, i` or
/// `*x, i`. Collect every name up to the closing `|`.
fn addCaptureList(arena: Allocator, tree: *const Ast, bindings: *std.ArrayList(Binding), first: Ast.TokenIndex) Allocator.Error!void {
    var token = first;
    while (tree.tokenTag(token) != .pipe) : (token += 1) {
        if (tree.tokenTag(token) == .identifier) try addBinding(arena, tree, bindings, token);
    }
}

fn addBinding(arena: Allocator, tree: *const Ast, bindings: *std.ArrayList(Binding), token: Ast.TokenIndex) Allocator.Error!void {
    const spelled = tree.tokenSlice(token);
    // `_` discards; it is never a binding.
    if (std.mem.eql(u8, spelled, "_")) return;
    try bindings.append(arena, .{ .token = token, .name = try unquote(arena, spelled) });
}

/// The name an identifier token denotes: `@"x y"` -> `x y`, `x` -> `x`.
fn unquote(arena: Allocator, spelled: []const u8) Allocator.Error![]const u8 {
    if (!std.mem.startsWith(u8, spelled, "@\"")) return spelled;
    return std.zig.string_literal.parseAlloc(arena, spelled[1..]) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        // An identifier that does not parse cannot match a declaration.
        else => spelled,
    };
}

fn expectShadowed(source: [:0]const u8, expected: []const Shadowed) !void {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const found = try findShadowedDeclarations(arena_state.allocator(), source);
    errdefer for (found) |got| std.debug.print("found {s}: {s}\n", .{ got.container, got.name });
    try std.testing.expectEqual(expected.len, found.len);
    for (expected, found) |want, got| {
        try std.testing.expectEqualStrings(want.container, got.container);
        try std.testing.expectEqualStrings(want.name, got.name);
    }
}

test "local shadowing: every binding kind the compiler checks" {
    try expectShadowed(
        \\const p = 1;
        \\const l = 2;
        \\const v = 3;
        \\const a = 4;
        \\const b = 5;
        \\const w = 6;
        \\const e = 7;
        \\const x = 8;
        \\const y = 9;
        \\const s = 10;
        \\const t = 11;
        \\const c = 12;
        \\fn f(p: u32, opt: ?u32, list: []const u32, u: union(enum) { one: u32 }, eu: error{Bad}!u32) void {
        \\    const l = p;
        \\    var v = l;
        \\    v += 1;
        \\    if (opt) |a| {
        \\        _ = a;
        \\    }
        \\    if (eu) |_| {} else |b| {
        \\        _ = b;
        \\    }
        \\    while (opt) |w| : (v += 1) {
        \\        _ = w;
        \\    }
        \\    while (eu) |_| {} else |e| {
        \\        _ = e;
        \\    }
        \\    for (list, 0..) |*x, y| {
        \\        _ = .{ x, y };
        \\    }
        \\    switch (u) {
        \\        .one => |s| _ = s,
        \\    }
        \\    switch (u) {
        \\        inline else => |*t, tag| _ = .{ t, tag },
        \\    }
        \\    _ = eu catch |c| c;
        \\}
    , &.{
        .{ .container = "", .name = "p" },
        .{ .container = "", .name = "l" },
        .{ .container = "", .name = "v" },
        .{ .container = "", .name = "a" },
        .{ .container = "", .name = "b" },
        .{ .container = "", .name = "w" },
        .{ .container = "", .name = "e" },
        .{ .container = "", .name = "x" },
        .{ .container = "", .name = "y" },
        .{ .container = "", .name = "s" },
        .{ .container = "", .name = "t" },
        .{ .container = "", .name = "c" },
    });
}

test "local shadowing: enclosing containers only, by path" {
    try expectShadowed(
        \\pub const Outer = struct {
        \\    pub const value: u32 = 1;
        \\    pub const Inner = struct {
        \\        pub const ctx: u32 = 2;
        \\        pub fn set(value: u32, ctx: u32) void {
        \\            _ = .{ value, ctx };
        \\        }
        \\    };
        \\    pub fn Apply(comptime T: type) type {
        \\        return struct {
        \\            pub const peer = T;
        \\            fn f(peer: u32) void {
        \\                _ = peer;
        \\            }
        \\        };
        \\    }
        \\};
        \\pub const Sibling = struct {
        \\    pub const ctx: u32 = 3;
        \\};
        \\pub const @"type" = struct {};
        \\fn g(@"type": u32) void {
        \\    _ = @"type";
        \\}
    , &.{
        .{ .container = "Outer", .name = "value" },
        .{ .container = "Outer.Inner", .name = "ctx" },
        .{ .container = "Outer.()", .name = "peer" },
        .{ .container = "", .name = "@\"type\"" },
    });
}

test "local shadowing: names the compiler does not check are not bindings" {
    try expectShadowed(
        \\const ctx = 1;
        \\const blk = 2;
        \\const field = 3;
        \\const Callback = *const fn (ctx: *anyopaque) void;
        \\const S = struct { field: u32 };
        \\fn f(@"settled flag": u32, _: u32) u32 {
        \\    const s: S = .{ .field = @"settled flag" };
        \\    return blk: {
        \\        break :blk s.field;
        \\    };
        \\}
    , &.{});
}
