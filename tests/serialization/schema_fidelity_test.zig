const std = @import("std");
const capnpc = @import("capnpc-zig");
const capnp_cli = @import("support/capnp_cli.zig");

const codegen = capnpc.codegen;
const message = capnpc.message;
const request_reader = capnpc.request;
const schema = capnpc.schema;
const schema_validation = capnpc.schema_validation;

fn structNode(
    id: schema.Id,
    scope_id: schema.Id,
    name: []const u8,
    fields: []schema.Field,
    parameters: []schema.Parameter,
    is_generic: bool,
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
        .is_generic = is_generic,
    };
}

fn pointerField(
    name: []const u8,
    typ: schema.Type,
    metadata: schema.TypeMetadata,
) schema.Field {
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

fn rootParameterField(scope_id: schema.Id, parameter_index: u16) schema.Field {
    return pointerField("value", .any_pointer, .{ .any_pointer = .{ .parameter = .{
        .scope_id = scope_id,
        .parameter_index = parameter_index,
    } } });
}

fn fileNode(id: schema.Id, nested_nodes: []schema.Node.NestedNode) schema.Node {
    return .{
        .id = id,
        .display_name = "fixture.capnp",
        .display_name_prefix_length = 0,
        .scope_id = 0,
        .nested_nodes = nested_nodes,
        .annotations = &.{},
        .kind = .file,
        .struct_node = null,
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
}

test "WithBrand validates concrete root bindings and canonicalizes identically" {
    const allocator = std.testing.allocator;
    var fields = [_]schema.Field{rootParameterField(1, 0)};
    var parameters = [_]schema.Parameter{.{ .name = "T" }};
    var nodes = [_]schema.Node{structNode(1, 0, "Box", fields[0..], parameters[0..], true)};

    var text_expression = schema.TypeExpression{ .type = .text };
    var bindings = [_]schema.Brand.Binding{.{ .type = &text_expression }};
    var scopes = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = bindings[0..] } }};
    const text_brand = schema.Brand{ .scopes = scopes[0..] };

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(0, 1);
    try root.writeText(0, "brand-aware");
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();
    try schema_validation.validateMessageWithBrand(&msg, nodes[0..], &nodes[0], text_brand, .{});

    const branded_flat = try schema_validation.canonicalizeMessageFlatWithBrand(
        allocator,
        &msg,
        nodes[0..],
        &nodes[0],
        text_brand,
        .{},
    );
    defer allocator.free(branded_flat);
    const erased_flat = try schema_validation.canonicalizeMessageFlat(
        allocator,
        &msg,
        nodes[0..],
        &nodes[0],
        .{},
    );
    defer allocator.free(erased_flat);
    try std.testing.expectEqualSlices(u8, erased_flat, branded_flat);

    const framed = try schema_validation.canonicalizeMessageWithBrand(
        allocator,
        &msg,
        nodes[0..],
        &nodes[0],
        text_brand,
        .{},
    );
    defer allocator.free(framed);
    var canonical = try message.Message.init(allocator, framed, .{});
    defer canonical.deinit();
    try schema_validation.validateMessageWithBrand(&canonical, nodes[0..], &nodes[0], text_brand, .{});
}

test "legacy validation keeps an unbound parameter erased while WithBrand enforces it" {
    const allocator = std.testing.allocator;
    var fields = [_]schema.Field{rootParameterField(1, 0)};
    var parameters = [_]schema.Parameter{.{ .name = "T" }};
    var nodes = [_]schema.Node{structNode(1, 0, "Box", fields[0..], parameters[0..], true)};

    var text_expression = schema.TypeExpression{ .type = .text };
    var bindings = [_]schema.Brand.Binding{.{ .type = &text_expression }};
    var scopes = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = bindings[0..] } }};
    const text_brand = schema.Brand{ .scopes = scopes[0..] };

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(0, 1);
    _ = try root.initStruct(0, 0, 0);
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();
    try schema_validation.validateMessage(&msg, nodes[0..], &nodes[0], .{});
    try std.testing.expectError(
        error.InvalidPointer,
        schema_validation.validateMessageWithBrand(&msg, nodes[0..], &nodes[0], text_brand, .{}),
    );
    try std.testing.expectError(
        error.InvalidPointer,
        schema_validation.canonicalizeMessageFlatWithBrand(
            allocator,
            &msg,
            nodes[0..],
            &nodes[0],
            text_brand,
            .{},
        ),
    );
}

test "legacy entry points honor concrete nested brand metadata" {
    const allocator = std.testing.allocator;
    var box_fields = [_]schema.Field{rootParameterField(1, 0)};
    var parameters = [_]schema.Parameter{.{ .name = "T" }};
    var text_expression = schema.TypeExpression{ .type = .text };
    var bindings = [_]schema.Brand.Binding{.{ .type = &text_expression }};
    var scopes = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = bindings[0..] } }};
    var root_fields = [_]schema.Field{pointerField(
        "box",
        .{ .@"struct" = .{ .type_id = 1 } },
        .{ .named = .{ .scopes = scopes[0..] } },
    )};
    var nodes = [_]schema.Node{
        structNode(1, 0, "Box", box_fields[0..], parameters[0..], true),
        structNode(2, 0, "Root", root_fields[0..], &.{}, false),
    };

    var wrong_builder = message.MessageBuilder.init(allocator);
    defer wrong_builder.deinit();
    var wrong_root = try wrong_builder.allocateStruct(0, 1);
    try wrong_root.writeText(0, "not-a-box");
    const wrong_bytes = try wrong_builder.toBytes();
    defer allocator.free(wrong_bytes);
    var wrong_msg = try message.Message.init(allocator, wrong_bytes, .{});
    defer wrong_msg.deinit();
    try std.testing.expectError(
        error.InvalidRootPointer,
        schema_validation.validateMessage(&wrong_msg, nodes[0..], &nodes[1], .{}),
    );

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(0, 1);
    var box = try root.initStruct(0, 0, 1);
    try box.writeText(0, "nested");
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();
    try schema_validation.validateMessage(&msg, nodes[0..], &nodes[1], .{});
    const canonical = try schema_validation.canonicalizeMessageFlat(
        allocator,
        &msg,
        nodes[0..],
        &nodes[1],
        .{},
    );
    defer allocator.free(canonical);
}

test "List(T) retains pointer-list layout when T resolves to a struct" {
    const allocator = std.testing.allocator;
    var parameter_type: schema.Type = .any_pointer;
    var parameter_metadata: schema.TypeMetadata = .{ .any_pointer = .{ .parameter = .{
        .scope_id = 1,
        .parameter_index = 0,
    } } };
    const list_type: schema.Type = .{ .list = .{ .element_type = &parameter_type } };
    const list_metadata: schema.TypeMetadata = .{ .list = &parameter_metadata };
    var bucket_fields = [_]schema.Field{pointerField("items", list_type, list_metadata)};
    var parameters = [_]schema.Parameter{.{ .name = "T" }};
    var child_fields = [_]schema.Field{pointerField("name", .text, .none)};
    var child_expression = schema.TypeExpression{
        .type = .{ .@"struct" = .{ .type_id = 2 } },
        .metadata = .{ .named = .{} },
    };
    var bindings = [_]schema.Brand.Binding{.{ .type = &child_expression }};
    var scopes = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = bindings[0..] } }};
    var root_fields = [_]schema.Field{pointerField(
        "bucket",
        .{ .@"struct" = .{ .type_id = 1 } },
        .{ .named = .{ .scopes = scopes[0..] } },
    )};
    var nodes = [_]schema.Node{
        structNode(1, 0, "Bucket", bucket_fields[0..], parameters[0..], true),
        structNode(2, 0, "Child", child_fields[0..], &.{}, false),
        structNode(3, 0, "Root", root_fields[0..], &.{}, false),
    };

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(0, 1);
    var bucket = try root.initStruct(0, 0, 1);
    const items_pointer = try bucket.getAnyPointer(0);
    const items = try items_pointer.initPointerList(1);
    var child = try items.initStruct(0, 0, 1);
    try child.writeText(0, "pointer-layout");
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();
    try schema_validation.validateMessage(&msg, nodes[0..], &nodes[2], .{});
    const canonical = try schema_validation.canonicalizeMessageFlat(
        allocator,
        &msg,
        nodes[0..],
        &nodes[2],
        .{},
    );
    defer allocator.free(canonical);

    var wrong_builder = message.MessageBuilder.init(allocator);
    defer wrong_builder.deinit();
    var wrong_root = try wrong_builder.allocateStruct(0, 1);
    var wrong_bucket = try wrong_root.initStruct(0, 0, 1);
    const wrong_pointer = try wrong_bucket.getAnyPointer(0);
    _ = try wrong_pointer.initStructList(1, 0, 1);
    const wrong_bytes = try wrong_builder.toBytes();
    defer allocator.free(wrong_bytes);
    var wrong_msg = try message.Message.init(allocator, wrong_bytes, .{});
    defer wrong_msg.deinit();
    try std.testing.expectError(
        error.InvalidListElementSize,
        schema_validation.validateMessage(&wrong_msg, nodes[0..], &nodes[2], .{}),
    );
}

test "canonicalization trims resolved struct elements in generic pointer lists" {
    const allocator = std.testing.allocator;
    var parameter_type: schema.Type = .any_pointer;
    var parameter_metadata: schema.TypeMetadata = .{ .any_pointer = .{ .parameter = .{
        .scope_id = 1,
        .parameter_index = 0,
    } } };
    const list_type: schema.Type = .{ .list = .{ .element_type = &parameter_type } };
    const list_metadata: schema.TypeMetadata = .{ .list = &parameter_metadata };
    var bucket_fields = [_]schema.Field{pointerField("items", list_type, list_metadata)};
    var parameters = [_]schema.Parameter{.{ .name = "T" }};
    var child_fields = [_]schema.Field{pointerField("trailing", .text, .none)};
    var child_expression = schema.TypeExpression{
        .type = .{ .@"struct" = .{ .type_id = 2 } },
        .metadata = .{ .named = .{} },
    };
    var bindings = [_]schema.Brand.Binding{.{ .type = &child_expression }};
    var scopes = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = bindings[0..] } }};
    var root_fields = [_]schema.Field{pointerField(
        "bucket",
        .{ .@"struct" = .{ .type_id = 1 } },
        .{ .named = .{ .scopes = scopes[0..] } },
    )};
    var nodes = [_]schema.Node{
        structNode(1, 0, "Bucket", bucket_fields[0..], parameters[0..], true),
        structNode(2, 0, "Child", child_fields[0..], &.{}, false),
        structNode(3, 0, "Root", root_fields[0..], &.{}, false),
    };

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(0, 1);
    var bucket = try root.initStruct(0, 0, 1);
    const items = try (try bucket.getAnyPointer(0)).initPointerList(1);
    // The source element is explicitly non-null and physically has the
    // schema's trailing pointer word, but its logical canonical size is 0/0.
    _ = try items.initStruct(0, 0, 1);
    const wire = try builder.toBytes();
    defer allocator.free(wire);
    var msg = try message.Message.init(allocator, wire, .{});
    defer msg.deinit();

    const flat = try schema_validation.canonicalizeMessageFlat(
        allocator,
        &msg,
        nodes[0..],
        &nodes[2],
        .{},
    );
    defer allocator.free(flat);
    const framed = try schema_validation.canonicalizeMessage(
        allocator,
        &msg,
        nodes[0..],
        &nodes[2],
        .{},
    );
    defer allocator.free(framed);
    var canonical = try message.Message.init(allocator, framed, .{});
    defer canonical.deinit();
    try std.testing.expectEqual(@as(usize, 1), canonical.segments.len);
    try std.testing.expectEqualSlices(u8, flat, canonical.segments[0]);

    const canonical_root = try canonical.getRootStruct();
    const canonical_bucket = try canonical_root.readStruct(0);
    const canonical_items = try canonical_bucket.readPointerList(0);
    const child = try canonical_items.getStruct(0);
    try std.testing.expectEqual(@as(u16, 0), child.data_size);
    try std.testing.expectEqual(@as(u16, 0), child.pointer_count);
}

test "malformed brand scopes arity indexes scalar bindings and recursive metadata fail closed" {
    const allocator = std.testing.allocator;
    var fields = [_]schema.Field{rootParameterField(1, 0)};
    var parameters = [_]schema.Parameter{.{ .name = "T" }};
    var nodes = [_]schema.Node{structNode(1, 0, "Box", fields[0..], parameters[0..], true)};

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    _ = try builder.allocateStruct(0, 1);
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    var text_expression = schema.TypeExpression{ .type = .text };
    var scalar_expression = schema.TypeExpression{ .type = .uint32 };
    var text_binding = [_]schema.Brand.Binding{.{ .type = &text_expression }};
    var scalar_binding = [_]schema.Brand.Binding{.{ .type = &scalar_expression }};
    var two_bindings = [_]schema.Brand.Binding{
        .{ .type = &text_expression },
        .{ .type = &text_expression },
    };
    var wrong_scope = [_]schema.Brand.Scope{.{ .scope_id = 99, .binding = .{ .bind = text_binding[0..] } }};
    var wrong_arity = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = two_bindings[0..] } }};
    var scalar_scope = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = scalar_binding[0..] } }};
    try std.testing.expectError(error.InvalidSchema, schema_validation.validateMessageWithBrand(
        &msg,
        nodes[0..],
        &nodes[0],
        .{ .scopes = wrong_scope[0..] },
        .{},
    ));
    try std.testing.expectError(error.InvalidSchema, schema_validation.validateMessageWithBrand(
        &msg,
        nodes[0..],
        &nodes[0],
        .{ .scopes = wrong_arity[0..] },
        .{},
    ));
    // Generic parameters occupy AnyPointer slots; binding one directly to a
    // scalar must not reinterpret the pointer offset as a data offset.
    try std.testing.expectError(error.InvalidSchema, schema_validation.validateMessageWithBrand(
        &msg,
        nodes[0..],
        &nodes[0],
        .{ .scopes = scalar_scope[0..] },
        .{},
    ));

    fields[0] = rootParameterField(1, 1);
    var valid_scope = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = text_binding[0..] } }};
    try std.testing.expectError(error.InvalidSchema, schema_validation.validateMessageWithBrand(
        &msg,
        nodes[0..],
        &nodes[0],
        .{ .scopes = valid_scope[0..] },
        .{},
    ));

    fields[0] = rootParameterField(1, 0);
    var recursive_type: schema.Type = undefined;
    recursive_type = .{ .list = .{ .element_type = &recursive_type } };
    var recursive_metadata: schema.TypeMetadata = undefined;
    recursive_metadata = .{ .list = &recursive_metadata };
    var recursive_expression = schema.TypeExpression{
        .type = recursive_type,
        .metadata = recursive_metadata,
    };
    var recursive_binding = [_]schema.Brand.Binding{.{ .type = &recursive_expression }};
    var recursive_scope = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = recursive_binding[0..] } }};
    try std.testing.expectError(error.InvalidSchema, schema_validation.validateMessageWithBrand(
        &msg,
        nodes[0..],
        &nodes[0],
        .{ .scopes = recursive_scope[0..] },
        .{},
    ));

    // Root brands are validated independently of whether a field happens to
    // reference the parameter. Otherwise an unused malformed graph could pass
    // validation and fail later when reused as a nested application.
    var unused_nodes = [_]schema.Node{structNode(2, 0, "Unused", &.{}, parameters[0..], true)};
    var unused_recursive_scope = [_]schema.Brand.Scope{.{
        .scope_id = 2,
        .binding = .{ .bind = recursive_binding[0..] },
    }};
    try std.testing.expectError(error.InvalidSchema, schema_validation.validateMessageWithBrand(
        &msg,
        unused_nodes[0..],
        &unused_nodes[0],
        .{ .scopes = unused_recursive_scope[0..] },
        .{},
    ));

    // A parameter reference is only meaningful in a lexical frame belonging
    // to its declaring node.  An unrelated generic node with a valid index
    // must not be treated as a merely-erased caller binding.
    var unrelated_parameters = [_]schema.Parameter{.{ .name = "U" }};
    var unrelated_nodes = [_]schema.Node{
        structNode(1, 0, "Box", fields[0..], parameters[0..], true),
        structNode(3, 0, "Unrelated", &.{}, unrelated_parameters[0..], true),
    };
    var unrelated_expression = schema.TypeExpression{
        .type = .any_pointer,
        .metadata = .{ .any_pointer = .{ .parameter = .{
            .scope_id = 3,
            .parameter_index = 0,
        } } },
    };
    var unrelated_binding = [_]schema.Brand.Binding{.{ .type = &unrelated_expression }};
    var unrelated_scope = [_]schema.Brand.Scope{.{
        .scope_id = 1,
        .binding = .{ .bind = unrelated_binding[0..] },
    }};
    try std.testing.expectError(error.InvalidSchema, schema_validation.validateMessageWithBrand(
        &msg,
        unrelated_nodes[0..],
        &unrelated_nodes[0],
        .{ .scopes = unrelated_scope[0..] },
        .{},
    ));
}

test "inactive union slots cannot hide malformed brand metadata" {
    const allocator = std.testing.allocator;
    var fields = [_]schema.Field{
        pointerField("active", .text, .none),
        pointerField("inactive", .text, .{ .named = .{} }),
    };
    fields[0].discriminant_value = 0;
    fields[1].discriminant_value = 1;
    var inactive_slot = fields[1].slot orelse return error.InvalidTestSchema;
    inactive_slot.offset = 1;
    fields[1].slot = inactive_slot;
    var nodes = [_]schema.Node{structNode(40, 0, "UnionRoot", fields[0..], &.{}, false)};
    var root_info = nodes[0].struct_node orelse return error.InvalidTestSchema;
    root_info.data_word_count = 1;
    root_info.pointer_count = 2;
    root_info.discriminant_count = 2;
    root_info.discriminant_offset = 0;
    nodes[0].struct_node = root_info;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(1, 2);
    root.writeUnionDiscriminant(0, 0);
    try root.writeText(0, "selected");
    const wire = try builder.toBytes();
    defer allocator.free(wire);
    var msg = try message.Message.init(allocator, wire, .{});
    defer msg.deinit();

    try std.testing.expectError(
        error.InvalidSchema,
        schema_validation.validateMessage(&msg, nodes[0..], &nodes[0], .{}),
    );
    try std.testing.expectError(
        error.InvalidSchema,
        schema_validation.canonicalizeMessageFlat(
            allocator,
            &msg,
            nodes[0..],
            &nodes[0],
            .{ .validate = false },
        ),
    );
    try std.testing.expectError(
        error.InvalidSchema,
        schema_validation.canonicalizeMessage(
            allocator,
            &msg,
            nodes[0..],
            &nodes[0],
            .{ .validate = false },
        ),
    );
}

test "inactive union groups cannot hide malformed child brand metadata" {
    const allocator = std.testing.allocator;
    var root_fields = [_]schema.Field{
        pointerField("active", .text, .none),
        .{
            .name = "inactiveGroup",
            .code_order = 1,
            .annotations = &.{},
            .discriminant_value = 1,
            .slot = null,
            .group = .{ .type_id = 41 },
        },
    };
    root_fields[0].discriminant_value = 0;
    var group_fields = [_]schema.Field{
        pointerField("malformed", .text, .{ .named = .{} }),
    };
    var nodes = [_]schema.Node{
        structNode(40, 0, "UnionRoot", root_fields[0..], &.{}, false),
        structNode(41, 40, "UnionRoot.inactiveGroup", group_fields[0..], &.{}, false),
    };
    var root_info = nodes[0].struct_node orelse return error.InvalidTestSchema;
    root_info.data_word_count = 1;
    root_info.pointer_count = 1;
    root_info.discriminant_count = 2;
    root_info.discriminant_offset = 0;
    nodes[0].struct_node = root_info;
    var group_info = nodes[1].struct_node orelse return error.InvalidTestSchema;
    group_info.is_group = true;
    nodes[1].struct_node = group_info;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(1, 1);
    root.writeUnionDiscriminant(0, 0);
    try root.writeText(0, "selected");
    const wire = try builder.toBytes();
    defer allocator.free(wire);
    var msg = try message.Message.init(allocator, wire, .{});
    defer msg.deinit();

    try std.testing.expectError(
        error.InvalidSchema,
        schema_validation.validateMessage(&msg, nodes[0..], &nodes[0], .{}),
    );
    try std.testing.expectError(
        error.InvalidSchema,
        schema_validation.canonicalizeMessageFlat(
            allocator,
            &msg,
            nodes[0..],
            &nodes[0],
            .{ .validate = false },
        ),
    );
    try std.testing.expectError(
        error.InvalidSchema,
        schema_validation.canonicalizeMessage(
            allocator,
            &msg,
            nodes[0..],
            &nodes[0],
            .{ .validate = false },
        ),
    );
}

test "inherited lexical bindings resolve in the caller context" {
    const allocator = std.testing.allocator;
    var outer_parameters = [_]schema.Parameter{.{ .name = "T" }};
    var inherit_scopes = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .inherit }};
    var outer_fields = [_]schema.Field{pointerField(
        "inner",
        .{ .@"struct" = .{ .type_id = 2 } },
        .{ .named = .{ .scopes = inherit_scopes[0..] } },
    )};
    var inner_fields = [_]schema.Field{rootParameterField(1, 0)};
    var nodes = [_]schema.Node{
        structNode(1, 0, "Outer", outer_fields[0..], outer_parameters[0..], true),
        structNode(2, 1, "Outer.Inner", inner_fields[0..], &.{}, true),
    };

    var text_expression = schema.TypeExpression{ .type = .text };
    var bindings = [_]schema.Brand.Binding{.{ .type = &text_expression }};
    var scopes = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = bindings[0..] } }};

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(0, 1);
    var inner = try root.initStruct(0, 0, 1);
    try inner.writeText(0, "inherited");
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    try schema_validation.validateMessageWithBrand(
        &msg,
        nodes[0..],
        &nodes[0],
        .{ .scopes = scopes[0..] },
        .{},
    );
}

test "codegen distinguishes lexical caller parameters from unrelated scopes" {
    const allocator = std.testing.allocator;
    var box_parameters = [_]schema.Parameter{.{ .name = "T" }};
    var caller_parameters = [_]schema.Parameter{.{ .name = "T" }};
    var unrelated_parameters = [_]schema.Parameter{.{ .name = "U" }};
    var box_fields = [_]schema.Field{rootParameterField(11, 0)};
    var caller_parameter_expression = schema.TypeExpression{
        .type = .any_pointer,
        .metadata = .{ .any_pointer = .{ .parameter = .{
            .scope_id = 12,
            .parameter_index = 0,
        } } },
    };
    var caller_bindings = [_]schema.Brand.Binding{.{ .type = &caller_parameter_expression }};
    var caller_scopes = [_]schema.Brand.Scope{.{
        .scope_id = 11,
        .binding = .{ .bind = caller_bindings[0..] },
    }};
    var caller_fields = [_]schema.Field{pointerField(
        "box",
        .{ .@"struct" = .{ .type_id = 11 } },
        .{ .named = .{ .scopes = caller_scopes[0..] } },
    )};
    var nested = [_]schema.Node.NestedNode{
        .{ .name = "Box", .id = 11 },
        .{ .name = "Caller", .id = 12 },
        .{ .name = "Unrelated", .id = 13 },
    };
    var nodes = [_]schema.Node{
        fileNode(10, nested[0..]),
        structNode(11, 10, "Box", box_fields[0..], box_parameters[0..], true),
        structNode(12, 10, "Caller", caller_fields[0..], caller_parameters[0..], true),
        structNode(13, 10, "Unrelated", &.{}, unrelated_parameters[0..], true),
    };
    const requested = schema.RequestedFile{ .id = 10, .filename = "fixture.capnp", .imports = &.{} };

    var valid_generator = try codegen.Generator.init(allocator, nodes[0..]);
    defer valid_generator.deinit();
    const valid = try valid_generator.generateFile(requested);
    defer allocator.free(valid);

    var unrelated_expression = schema.TypeExpression{
        .type = .any_pointer,
        .metadata = .{ .any_pointer = .{ .parameter = .{
            .scope_id = 13,
            .parameter_index = 0,
        } } },
    };
    caller_bindings[0] = .{ .type = &unrelated_expression };
    var invalid_generator = try codegen.Generator.init(allocator, nodes[0..]);
    defer invalid_generator.deinit();
    try std.testing.expectError(error.InvalidStructNode, invalid_generator.generateFile(requested));
}

test "constrained AnyPointer shapes reject non-null wrong kinds and accept null" {
    const allocator = std.testing.allocator;
    const cases = [_]schema.TypeMetadata.AnyPointer.Unconstrained{
        .@"struct",
        .list,
        .capability,
    };
    for (cases, 0..) |kind, index| {
        var fields = [_]schema.Field{pointerField(
            "value",
            .any_pointer,
            .{ .any_pointer = .{ .unconstrained = kind } },
        )};
        var nodes = [_]schema.Node{structNode(100 + index, 0, "Constrained", fields[0..], &.{}, false)};

        var null_builder = message.MessageBuilder.init(allocator);
        defer null_builder.deinit();
        _ = try null_builder.allocateStruct(0, 1);
        const null_bytes = try null_builder.toBytes();
        defer allocator.free(null_bytes);
        var null_msg = try message.Message.init(allocator, null_bytes, .{});
        defer null_msg.deinit();
        try schema_validation.validateMessage(&null_msg, nodes[0..], &nodes[0], .{});

        var wrong_builder = message.MessageBuilder.init(allocator);
        defer wrong_builder.deinit();
        var wrong_root = try wrong_builder.allocateStruct(0, 1);
        if (kind == .list) {
            _ = try wrong_root.initStruct(0, 0, 0);
        } else {
            try wrong_root.writeText(0, "wrong-kind");
        }
        const wrong_bytes = try wrong_builder.toBytes();
        defer allocator.free(wrong_bytes);
        var wrong_msg = try message.Message.init(allocator, wrong_bytes, .{});
        defer wrong_msg.deinit();
        if (schema_validation.validateMessage(&wrong_msg, nodes[0..], &nodes[0], .{})) |_| {
            return error.ExpectedWrongPointerKind;
        } else |_| {}
    }
}

fn canonicalizeBrandOom(
    allocator: std.mem.Allocator,
    msg: *const message.Message,
    nodes: []schema.Node,
    root: *const schema.Node,
    brand: schema.Brand,
) !void {
    const bytes = try schema_validation.canonicalizeMessageFlatWithBrand(
        allocator,
        msg,
        nodes,
        root,
        brand,
        .{},
    );
    allocator.free(bytes);
}

test "WithBrand canonicalization releases every partial allocation" {
    const allocator = std.testing.allocator;
    var fields = [_]schema.Field{rootParameterField(1, 0)};
    var parameters = [_]schema.Parameter{.{ .name = "T" }};
    var nodes = [_]schema.Node{structNode(1, 0, "Box", fields[0..], parameters[0..], true)};
    var text_expression = schema.TypeExpression{ .type = .text };
    var bindings = [_]schema.Brand.Binding{.{ .type = &text_expression }};
    var scopes = [_]schema.Brand.Scope{.{ .scope_id = 1, .binding = .{ .bind = bindings[0..] } }};

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(0, 1);
    try root.writeText(0, "oom");
    const wire = try builder.toBytes();
    defer allocator.free(wire);
    var msg = try message.Message.init(allocator, wire, .{});
    defer msg.deinit();

    try std.testing.checkAllAllocationFailures(
        allocator,
        canonicalizeBrandOom,
        .{ &msg, nodes[0..], &nodes[0], schema.Brand{ .scopes = scopes[0..] } },
    );
}

test "brand specialization budget defaults to 4096 and rejects composite fixture exhaustion" {
    const allocator = std.testing.allocator;
    try std.testing.expectEqual(@as(usize, 4096), (codegen.Generator.CodegenBudget{}).max_brand_specializations);
    const result = try capnp_cli.run(allocator, std.testing.io, &.{
        "compile",
        "-o-",
        "tests/test_schemas/brand_pointer_fidelity.capnp",
    }, .{});
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);
    try std.testing.expect(result.term == .exited and result.term.exited == 0);
    const request = try request_reader.parseCodeGeneratorRequest(allocator, result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    var generator = try codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();
    generator.setCodegenBudget(.{ .max_brand_specializations = 1 });
    try std.testing.expectError(
        error.CodegenBudgetExceeded,
        generator.generateFile(request.requested_files[0]),
    );
}

fn generateBrandSpecializationOom(
    allocator: std.mem.Allocator,
    request: schema.CodeGeneratorRequest,
) !void {
    var generator = try codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();
    const generated = try generator.generateFile(request.requested_files[0]);
    allocator.free(generated);
}

fn checkAllAllocationFailuresIsolated(
    comptime test_fn: anytype,
    extra_args: anytype,
) !void {
    const needed = blk: {
        var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
        defer std.debug.assert(debug_allocator.deinit() == .ok);
        var failing = std.testing.FailingAllocator.init(debug_allocator.allocator(), .{});
        try @call(.auto, test_fn, .{failing.allocator()} ++ extra_args);
        break :blk failing.alloc_index;
    };

    for (0..needed) |fail_index| {
        var debug_allocator: std.heap.DebugAllocator(.{}) = .init;
        defer std.debug.assert(debug_allocator.deinit() == .ok);
        var failing = std.testing.FailingAllocator.init(
            debug_allocator.allocator(),
            .{ .fail_index = fail_index },
        );
        if (@call(.auto, test_fn, .{failing.allocator()} ++ extra_args)) |_| {
            if (failing.has_induced_failure) return error.SwallowedOutOfMemoryError;
            return error.NondeterministicMemoryUsage;
        } else |err| switch (err) {
            error.OutOfMemory => {
                if (failing.allocated_bytes != failing.freed_bytes) return error.MemoryLeakDetected;
            },
            else => |unexpected| return unexpected,
        }
    }
}

test "brand specialization budget recursively counts generic struct list terminals" {
    const allocator = std.testing.allocator;
    const result = try capnp_cli.run(allocator, std.testing.io, &.{
        "compile",
        "-o-",
        "tests/test_schemas/brand_list_specialization.capnp",
    }, .{});
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);
    try std.testing.expect(result.term == .exited and result.term.exited == 0);
    const request = try request_reader.parseCodeGeneratorRequest(allocator, result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    var rejected = try codegen.Generator.init(allocator, request.nodes);
    defer rejected.deinit();
    rejected.setCodegenBudget(.{ .max_brand_specializations = 4 });
    try std.testing.expectError(
        error.CodegenBudgetExceeded,
        rejected.generateFile(request.requested_files[0]),
    );

    var accepted = try codegen.Generator.init(allocator, request.nodes);
    defer accepted.deinit();
    accepted.setCodegenBudget(.{ .max_brand_specializations = 5 });
    const generated = try accepted.generateFile(request.requested_files[0]);
    defer allocator.free(generated);
    try std.testing.expect(std.mem.indexOf(u8, generated, "pub const Nested = struct") != null);

    try checkAllAllocationFailuresIsolated(
        generateBrandSpecializationOom,
        .{request},
    );
}

fn writeFile(dir: std.Io.Dir, name: []const u8, bytes: []const u8) !void {
    const io = std.testing.io;
    var file = try dir.createFile(io, name, .{});
    defer file.close(io);
    try file.writeStreamingAll(io, bytes);
}

fn generatedFilename(allocator: std.mem.Allocator, schema_filename: []const u8) ![]u8 {
    const basename = std.fs.path.basename(schema_filename);
    if (!std.mem.endsWith(u8, basename, ".capnp")) return error.InvalidSchemaFilename;
    return std.fmt.allocPrint(allocator, "{s}.zig", .{basename[0 .. basename.len - ".capnp".len]});
}

fn compileUpstreamSchemaProfile(
    allocator: std.mem.Allocator,
    request: schema.CodeGeneratorRequest,
    profile: codegen.Generator.ApiProfile,
) !void {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var generator = try codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();
    generator.setApiProfile(profile);
    try tmp.dir.createDir(io, "capnp", .default_dir);
    for (request.requested_files) |file| {
        const generated = try generator.generateFile(file);
        defer allocator.free(generated);
        const filename = try generatedFilename(allocator, file.filename);
        defer allocator.free(filename);
        try writeFile(tmp.dir, filename, generated);
        const standard_filename = try std.fmt.allocPrint(allocator, "capnp/{s}", .{filename});
        defer allocator.free(standard_filename);
        try writeFile(tmp.dir, standard_filename, generated);
    }

    const harness =
        \\const std = @import("std");
        \\const upstream = @import("test.zig");
        \\
        \\fn refAllRecursive(comptime T: type, comptime depth: u8) void {
        \\    if (depth == 0) return;
        \\    inline for (comptime std.meta.declarations(T)) |decl_name| {
        \\        const field = @field(T, decl_name);
        \\        if (@TypeOf(field) == type) switch (@typeInfo(field)) {
        \\            .@"struct", .@"union", .@"enum", .@"opaque" => refAllRecursive(field, depth - 1),
        \\            else => {},
        \\        };
        \\        _ = &field;
        \\    }
        \\}
        \\
        \\test "all upstream capnp/test declarations analyze" {
        \\    refAllRecursive(upstream, 32);
        \\}
    ;
    try writeFile(tmp.dir, "harness.zig", harness);
    const harness_path = try tmp.dir.realPathFileAlloc(io, "harness.zig", allocator);
    defer allocator.free(harness_path);
    const lib_path = try std.Io.Dir.cwd().realPathFileAlloc(io, "src/lib.zig", allocator);
    defer allocator.free(lib_path);
    const root_arg = try std.fmt.allocPrint(allocator, "-Mroot={s}", .{harness_path});
    defer allocator.free(root_arg);
    const lib_arg = try std.fmt.allocPrint(allocator, "-Mcapnpc-zig={s}", .{lib_path});
    defer allocator.free(lib_arg);
    const result = std.process.run(allocator, io, .{ .argv = &.{
        "zig",
        "test",
        "--dep",
        "capnpc-zig",
        root_arg,
        "--dep",
        "capnpc-zig",
        lib_arg,
    } }) catch |err| switch (err) {
        error.FileNotFound => return error.ZigCompilerUnavailable,
        else => return err,
    };
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);
    if (!(result.term == .exited and result.term.exited == 0)) {
        std.debug.print("upstream {s} stdout:\n{s}\n", .{ @tagName(profile), result.stdout });
        std.debug.print("upstream {s} stderr:\n{s}\n", .{ @tagName(profile), result.stderr });
        return error.UpstreamGeneratedCompileFailed;
    }
}

test "vendored upstream capnp/test schema analyzes in full and compact profiles" {
    const allocator = std.testing.allocator;
    const root = "vendor/ext/capnproto/c++/src/capnp";
    const test_schema = root ++ "/test.capnp";
    const cxx_schema = root ++ "/c++.capnp";
    const stream_schema = root ++ "/stream.capnp";
    const result = try capnp_cli.run(allocator, std.testing.io, &.{
        "compile",
        "--no-standard-import",
        "-I" ++ root,
        "-o-",
        test_schema,
        cxx_schema,
        stream_schema,
    }, .{ .missing = .required });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);
    if (!(result.term == .exited and result.term.exited == 0)) {
        std.debug.print("capnp upstream compile stderr:\n{s}\n", .{result.stderr});
        return error.CapnpCompileFailed;
    }
    const request = try request_reader.parseCodeGeneratorRequest(allocator, result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);
    try std.testing.expectEqual(@as(usize, 3), request.requested_files.len);

    try compileUpstreamSchemaProfile(allocator, request, .full);
    try compileUpstreamSchemaProfile(allocator, request, .compact);
}
