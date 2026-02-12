const std = @import("std");
const capnpc = @import("capnpc-zig");
const request_reader = capnpc.request;

fn findNode(nodes: []const capnpc.schema.Node, id: capnpc.schema.Id) ?capnpc.schema.Node {
    for (nodes) |node| {
        if (node.id == id) return node;
    }
    return null;
}

fn containsName(items: []const capnpc.schema.Node.NestedNode, name: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item.name, name)) return true;
    }
    return false;
}

fn containsField(fields: []const capnpc.schema.Field, name: []const u8) bool {
    for (fields) |field| {
        if (std.mem.eql(u8, field.name, name)) return true;
    }
    return false;
}

fn containsEnumerant(enums: []const capnpc.schema.Enumerant, name: []const u8) bool {
    for (enums) |enumerant| {
        if (std.mem.eql(u8, enumerant.name, name)) return true;
    }
    return false;
}

fn findNestedNodeId(items: []const capnpc.schema.Node.NestedNode, name: []const u8) ?capnpc.schema.Id {
    for (items) |item| {
        if (std.mem.eql(u8, item.name, name)) return item.id;
    }
    return null;
}

fn fieldDiscriminant(fields: []const capnpc.schema.Field, name: []const u8) ?u16 {
    for (fields) |field| {
        if (std.mem.eql(u8, field.name, name)) return field.discriminant_value;
    }
    return null;
}

test "CodeGeneratorRequest parsing from capnp compile" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "capnp",
        "compile",
        "-o-",
        "tests/test_schemas/example.capnp",
    };

    const result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = argv,
        .max_output_bytes = 10 * 1024 * 1024,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try std.testing.expect(result.term == .Exited and result.term.Exited == 0);

    const request = try request_reader.parseCodeGeneratorRequest(allocator, result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    try std.testing.expect(request.requested_files.len >= 1);
    const file = request.requested_files[0];
    try std.testing.expect(std.mem.endsWith(u8, file.filename, "example.capnp"));

    const file_node = findNode(request.nodes, file.id) orelse return error.InvalidRequest;
    try std.testing.expect(file_node.kind == .file);

    try std.testing.expect(containsName(file_node.nested_nodes, "Person"));
    try std.testing.expect(containsName(file_node.nested_nodes, "Address"));
    try std.testing.expect(containsName(file_node.nested_nodes, "Color"));

    var person_id: ?capnpc.schema.Id = null;
    var address_id: ?capnpc.schema.Id = null;
    var color_id: ?capnpc.schema.Id = null;
    for (file_node.nested_nodes) |nested| {
        if (std.mem.eql(u8, nested.name, "Person")) person_id = nested.id;
        if (std.mem.eql(u8, nested.name, "Address")) address_id = nested.id;
        if (std.mem.eql(u8, nested.name, "Color")) color_id = nested.id;
    }

    const person_node = findNode(request.nodes, person_id orelse return error.InvalidRequest) orelse return error.InvalidRequest;
    try std.testing.expect(person_node.kind == .@"struct");
    const person_struct = person_node.struct_node orelse return error.InvalidRequest;
    try std.testing.expect(containsField(person_struct.fields, "name"));
    try std.testing.expect(containsField(person_struct.fields, "age"));
    try std.testing.expect(containsField(person_struct.fields, "email"));

    const address_node = findNode(request.nodes, address_id orelse return error.InvalidRequest) orelse return error.InvalidRequest;
    try std.testing.expect(address_node.kind == .@"struct");

    const color_node = findNode(request.nodes, color_id orelse return error.InvalidRequest) orelse return error.InvalidRequest;
    try std.testing.expect(color_node.kind == .@"enum");
    const color_enum = color_node.enum_node orelse return error.InvalidRequest;
    try std.testing.expect(containsEnumerant(color_enum.enumerants, "red"));
    try std.testing.expect(containsEnumerant(color_enum.enumerants, "green"));
    try std.testing.expect(containsEnumerant(color_enum.enumerants, "blue"));
}

test "CodeGeneratorRequest parsing preserves rpc union discriminants" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "capnp",
        "compile",
        "-Ivendor/ext/capnproto/c++/src",
        "--src-prefix=src/rpc",
        "-o-",
        "src/rpc/capnp/rpc.capnp",
    };

    const result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = argv,
        .max_output_bytes = 20 * 1024 * 1024,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try std.testing.expect(result.term == .Exited and result.term.Exited == 0);

    const request = try request_reader.parseCodeGeneratorRequest(allocator, result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    try std.testing.expect(request.requested_files.len >= 1);
    const file = request.requested_files[0];

    const file_node = findNode(request.nodes, file.id) orelse return error.InvalidRequest;
    const message_id = findNestedNodeId(file_node.nested_nodes, "Message") orelse return error.InvalidRequest;
    const return_id = findNestedNodeId(file_node.nested_nodes, "Return") orelse return error.InvalidRequest;
    const resolve_id = findNestedNodeId(file_node.nested_nodes, "Resolve") orelse return error.InvalidRequest;
    const message_target_id = findNestedNodeId(file_node.nested_nodes, "MessageTarget") orelse return error.InvalidRequest;
    const cap_descriptor_id = findNestedNodeId(file_node.nested_nodes, "CapDescriptor") orelse return error.InvalidRequest;
    const promised_answer_id = findNestedNodeId(file_node.nested_nodes, "PromisedAnswer") orelse return error.InvalidRequest;

    const message_node = findNode(request.nodes, message_id) orelse return error.InvalidRequest;
    const message_struct = message_node.struct_node orelse return error.InvalidRequest;
    try std.testing.expectEqual(@as(u16, 0), fieldDiscriminant(message_struct.fields, "unimplemented") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 1), fieldDiscriminant(message_struct.fields, "abort") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 2), fieldDiscriminant(message_struct.fields, "call") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 3), fieldDiscriminant(message_struct.fields, "return") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 14), fieldDiscriminant(message_struct.fields, "thirdPartyAnswer") orelse return error.InvalidRequest);

    const return_node = findNode(request.nodes, return_id) orelse return error.InvalidRequest;
    const return_struct = return_node.struct_node orelse return error.InvalidRequest;
    try std.testing.expectEqual(@as(u16, 0xffff), fieldDiscriminant(return_struct.fields, "answerId") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 0), fieldDiscriminant(return_struct.fields, "results") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 1), fieldDiscriminant(return_struct.fields, "exception") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 5), fieldDiscriminant(return_struct.fields, "awaitFromThirdParty") orelse return error.InvalidRequest);

    const resolve_node = findNode(request.nodes, resolve_id) orelse return error.InvalidRequest;
    const resolve_struct = resolve_node.struct_node orelse return error.InvalidRequest;
    try std.testing.expectEqual(@as(u16, 0xffff), fieldDiscriminant(resolve_struct.fields, "promiseId") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 0), fieldDiscriminant(resolve_struct.fields, "cap") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 1), fieldDiscriminant(resolve_struct.fields, "exception") orelse return error.InvalidRequest);

    const message_target_node = findNode(request.nodes, message_target_id) orelse return error.InvalidRequest;
    const message_target_struct = message_target_node.struct_node orelse return error.InvalidRequest;
    try std.testing.expectEqual(@as(u16, 0), fieldDiscriminant(message_target_struct.fields, "importedCap") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 1), fieldDiscriminant(message_target_struct.fields, "promisedAnswer") orelse return error.InvalidRequest);

    const cap_descriptor_node = findNode(request.nodes, cap_descriptor_id) orelse return error.InvalidRequest;
    const cap_descriptor_struct = cap_descriptor_node.struct_node orelse return error.InvalidRequest;
    try std.testing.expectEqual(@as(u16, 0), fieldDiscriminant(cap_descriptor_struct.fields, "none") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 1), fieldDiscriminant(cap_descriptor_struct.fields, "senderHosted") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 4), fieldDiscriminant(cap_descriptor_struct.fields, "receiverAnswer") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 5), fieldDiscriminant(cap_descriptor_struct.fields, "thirdPartyHosted") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 0xffff), fieldDiscriminant(cap_descriptor_struct.fields, "attachedFd") orelse return error.InvalidRequest);

    const promised_answer_node = findNode(request.nodes, promised_answer_id) orelse return error.InvalidRequest;
    const op_id = findNestedNodeId(promised_answer_node.nested_nodes, "Op") orelse return error.InvalidRequest;
    const op_node = findNode(request.nodes, op_id) orelse return error.InvalidRequest;
    const op_struct = op_node.struct_node orelse return error.InvalidRequest;
    try std.testing.expectEqual(@as(u16, 0), fieldDiscriminant(op_struct.fields, "noop") orelse return error.InvalidRequest);
    try std.testing.expectEqual(@as(u16, 1), fieldDiscriminant(op_struct.fields, "getPointerField") orelse return error.InvalidRequest);
}
