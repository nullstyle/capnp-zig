//! The Experimental `type_resolver` facade, as a foreign code generator uses
//! it: on the compiler's own CodeGeneratorRequest, and on hand-built graphs
//! for lexical inheritance and malformed brands.
const std = @import("std");
const capnpc = @import("capnpc-zig");

const schema = capnpc.schema;
const type_resolver = capnpc.type_resolver;

/// `tests/test_schemas/generic_collections.capnp`, compiled:
///   struct Box(T) { value @0 :T; }
///   struct Root { boxes @0 :List(Box(Text)); }
const generic_collections_request = @embedFile("generic-collections-request");

fn findNode(nodes: []const schema.Node, suffix: []const u8) !*const schema.Node {
    for (nodes) |*node| {
        if (std.mem.endsWith(u8, node.display_name, suffix)) return node;
    }
    return error.NodeNotFound;
}

fn slotExpression(node: *const schema.Node, field_name: []const u8) !schema.TypeExpression {
    const struct_node = node.struct_node orelse return error.NotAStruct;
    for (struct_node.fields) |field| {
        if (!std.mem.eql(u8, field.name, field_name)) continue;
        const slot = field.slot orelse return error.NotASlot;
        return .{ .type = slot.type, .metadata = slot.type_metadata };
    }
    return error.FieldNotFound;
}

test "type_resolver binds a generic field through the compiler's brand metadata" {
    const allocator = std.testing.allocator;
    const request = try capnpc.request.parseCodeGeneratorRequest(allocator, generic_collections_request);
    defer capnpc.request.freeCodeGeneratorRequest(allocator, request);
    const nodes = request.nodes;
    const root = try findNode(nodes, ":Root");
    const box = try findNode(nodes, ":Box");

    // Erased view: `boxes` is a list of plain `Box`.
    const boxes_expression = try slotExpression(root, "boxes");
    try std.testing.expectEqual(box.id, boxes_expression.type.list.element_type.@"struct".type_id);

    // Resolved view: List(Box(Text)), so `Box.value` is Text here.
    const root_context = try type_resolver.Context.init(nodes, root, .{});
    try root_context.validate(boxes_expression);
    const boxes = try root_context.resolve(boxes_expression);
    try std.testing.expect(!boxes.unbound);
    const element = try root_context.listElement(boxes);
    try std.testing.expectEqual(box.id, element.expression.type.@"struct".type_id);
    const box_context = try root_context.enter(element);
    const value = try box_context.resolve(try slotExpression(box, "value"));
    try std.testing.expect(!value.unbound);
    try std.testing.expect(value.expression.type == .text);
    try std.testing.expectEqual(@as(?schema.TypeMetadata.AnyPointer.Parameter, null), value.parameter());

    // The declaration itself: `value` is Box's own parameter `T`, unbound.
    const declaration = try type_resolver.Context.init(nodes, box, .{});
    const generic_value = try declaration.resolve(try slotExpression(box, "value"));
    try std.testing.expect(generic_value.unbound);
    try std.testing.expect(generic_value.expression.type == .any_pointer);
    const parameter = generic_value.parameter() orelse return error.ExpectedParameter;
    try std.testing.expectEqual(box.id, parameter.scope_id);
    try std.testing.expectEqualStrings("T", box.parameters[parameter.parameter_index].name);

    // An unbound parameter names no type to enter.
    try std.testing.expectError(error.InvalidSchema, declaration.enter(generic_value));
}

fn structNode(
    id: schema.Id,
    scope_id: schema.Id,
    name: []const u8,
    fields: []schema.Field,
    parameters: []schema.Parameter,
) schema.Node {
    return .{
        .id = id,
        .display_name = name,
        .display_name_prefix_length = 0,
        .scope_id = scope_id,
        .nested_nodes = &.{},
        .annotations = &.{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 0,
            .pointer_count = 1,
            .preferred_list_encoding = .pointer,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = fields,
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
        .parameters = parameters,
        .is_generic = parameters.len != 0,
    };
}

fn pointerField(name: []const u8, typ: schema.Type, metadata: schema.TypeMetadata) schema.Field {
    return .{
        .name = name,
        .code_order = 0,
        .annotations = &.{},
        .discriminant_value = 0xffff,
        .slot = .{
            .offset = 0,
            .type = typ,
            .default_value = null,
            .type_metadata = metadata,
        },
        .group = null,
    };
}

test "type_resolver follows an inherited binding into a nested type" {
    // struct Outer(T) {
    //   inner @0 :Inner;
    //   items @1 :List(T);
    //   struct Inner { x @0 :T; }
    // }
    // Inside Outer(Text), `inner` names Inner with Outer's scope inherited,
    // so Inner.x is Text, and `items` is a List(Text).
    const outer_id: schema.Id = 10;
    const inner_id: schema.Id = 11;
    var inherit_scopes = [_]schema.Brand.Scope{.{ .scope_id = outer_id, .binding = .inherit }};
    var parameter_type: schema.Type = .any_pointer;
    var parameter_metadata: schema.TypeMetadata = .{ .any_pointer = .{ .parameter = .{
        .scope_id = outer_id,
        .parameter_index = 0,
    } } };
    var outer_fields = [_]schema.Field{
        pointerField(
            "inner",
            .{ .@"struct" = .{ .type_id = inner_id } },
            .{ .named = .{ .scopes = inherit_scopes[0..] } },
        ),
        pointerField("items", .{ .list = .{ .element_type = &parameter_type } }, .{ .list = &parameter_metadata }),
    };
    var inner_fields = [_]schema.Field{pointerField("x", .any_pointer, .{ .any_pointer = .{ .parameter = .{
        .scope_id = outer_id,
        .parameter_index = 0,
    } } })};
    var parameters = [_]schema.Parameter{.{ .name = "T" }};
    const nodes = [_]schema.Node{
        structNode(outer_id, 0, "Outer", outer_fields[0..], parameters[0..]),
        structNode(inner_id, outer_id, "Outer.Inner", inner_fields[0..], &.{}),
    };

    var text_expression = schema.TypeExpression{ .type = .text };
    var bindings = [_]schema.Brand.Binding{.{ .type = &text_expression }};
    var text_scopes = [_]schema.Brand.Scope{.{ .scope_id = outer_id, .binding = .{ .bind = bindings[0..] } }};
    const outer_text = try type_resolver.Context.init(nodes[0..], &nodes[0], .{ .scopes = text_scopes[0..] });

    const inner = try outer_text.resolve(.{ .type = outer_fields[0].slot.?.type, .metadata = outer_fields[0].slot.?.type_metadata });
    const inner_context = try outer_text.enter(inner);
    const x = try inner_context.resolve(.{ .type = inner_fields[0].slot.?.type, .metadata = inner_fields[0].slot.?.type_metadata });
    try std.testing.expect(!x.unbound);
    try std.testing.expect(x.expression.type == .text);

    const items = try outer_text.resolve(.{ .type = outer_fields[1].slot.?.type, .metadata = outer_fields[1].slot.?.type_metadata });
    const item = try outer_text.listElement(items);
    try std.testing.expect(!item.unbound);
    try std.testing.expect(item.expression.type == .text);

    // Without a binding for Outer, the same path leaves `x` unbound.
    const outer_erased = try type_resolver.Context.init(nodes[0..], &nodes[0], .{});
    const erased_inner = try outer_erased.enter(try outer_erased.resolve(.{
        .type = outer_fields[0].slot.?.type,
        .metadata = outer_fields[0].slot.?.type_metadata,
    }));
    const erased_x = try erased_inner.resolve(.{ .type = inner_fields[0].slot.?.type, .metadata = inner_fields[0].slot.?.type_metadata });
    try std.testing.expect(erased_x.unbound);
}

test "type_resolver rejects a brand outside the node's lexical scopes" {
    const box_id: schema.Id = 20;
    const root_id: schema.Id = 21;
    var box_fields = [_]schema.Field{pointerField("value", .any_pointer, .{ .any_pointer = .{ .parameter = .{
        .scope_id = box_id,
        .parameter_index = 0,
    } } })};
    var root_fields = [_]schema.Field{pointerField("text", .text, .none)};
    var parameters = [_]schema.Parameter{.{ .name = "T" }};
    const nodes = [_]schema.Node{
        structNode(box_id, 0, "Box", box_fields[0..], parameters[0..]),
        structNode(root_id, 0, "Root", root_fields[0..], &.{}),
    };

    var text_expression = schema.TypeExpression{ .type = .text };
    var bindings = [_]schema.Brand.Binding{.{ .type = &text_expression }};
    var box_scopes = [_]schema.Brand.Scope{.{ .scope_id = box_id, .binding = .{ .bind = bindings[0..] } }};
    // Root is not inside Box, so a brand for Box's scope cannot apply to it.
    try std.testing.expectError(
        error.InvalidSchema,
        type_resolver.Context.init(nodes[0..], &nodes[1], .{ .scopes = box_scopes[0..] }),
    );
    // Two bindings for Box's one parameter.
    var two_bindings = [_]schema.Brand.Binding{ .{ .type = &text_expression }, .{ .type = &text_expression } };
    var wrong_arity = [_]schema.Brand.Scope{.{ .scope_id = box_id, .binding = .{ .bind = two_bindings[0..] } }};
    try std.testing.expectError(
        error.InvalidSchema,
        type_resolver.Context.init(nodes[0..], &nodes[0], .{ .scopes = wrong_arity[0..] }),
    );
}
