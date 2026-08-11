const std = @import("std");
const capnpc = @import("capnpc-zig");
const compare = @import("support/capnp_compare.zig");
const capnp_cli = @import("support/capnp_cli.zig");

const message = capnpc.message;
const schema = capnpc.schema;
const request_reader = capnpc.request;
const schema_validation = capnpc.schema_validation;

fn loadCodeGeneratorRequest(allocator: std.mem.Allocator) !schema.CodeGeneratorRequest {
    return loadCodeGeneratorRequestForSchema(allocator, "tests/capnp_testdata/test.capnp");
}

fn loadCodeGeneratorRequestForSchema(
    allocator: std.mem.Allocator,
    schema_path: []const u8,
) !schema.CodeGeneratorRequest {
    const argv = [_][]const u8{
        "compile",
        "--no-standard-import",
        "-Itests/capnp_testdata",
        "-o-",
        schema_path,
    };

    const result = try capnp_cli.run(allocator, std.testing.io, &argv, .{});
    defer allocator.free(result.stderr);

    switch (result.term) {
        .exited => |code| {
            if (code != 0) {
                std.debug.print("capnp compile failed: {s}\n", .{result.stderr});
                allocator.free(result.stdout);
                return error.CapnpCompileFailed;
            }
        },
        else => {
            std.debug.print("capnp compile failed: unexpected termination\n", .{});
            allocator.free(result.stdout);
            return error.CapnpCompileFailed;
        },
    }

    const request = try request_reader.parseCodeGeneratorRequest(allocator, result.stdout);
    allocator.free(result.stdout);
    return request;
}

test "schema-aware validation remains strict for unknown enum ordinals" {
    const allocator = std.testing.allocator;

    const request = try loadCodeGeneratorRequestForSchema(
        allocator,
        "tests/test_schemas/enum_evolution_v1.capnp",
    );
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    const root_node = compare.findStructBySuffix(request.nodes, "Evolution") orelse
        return error.InvalidSchema;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    // Status has schema default ordinal 1, so logical unknown ordinal 2 is
    // stored XOR 1. The generated raw ordinal view may forward it, but the
    // schema-aware validator intentionally remains exhaustive.
    var root = try builder.allocateStruct(1, 10);
    root.writeU16(0, 2 ^ 1);

    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    try std.testing.expectError(
        error.InvalidEnumValue,
        schema_validation.validateMessage(&msg, request.nodes, root_node, .{}),
    );
}

fn capnpConvertCanonical(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    return capnpConvert(allocator, input, "binary:canonical", "TestAllTypes");
}

fn capnpConvert(
    allocator: std.mem.Allocator,
    input: []const u8,
    conversion: []const u8,
    type_name: []const u8,
) ![]u8 {
    const io = std.testing.io;
    const argv = [_][]const u8{
        "convert",
        conversion,
        "--no-standard-import",
        "-Itests/capnp_testdata",
        "tests/capnp_testdata/test.capnp",
        type_name,
    };

    var child = try capnp_cli.spawn(allocator, io, &argv, .{
        .stdin = .pipe,
        .stdout = .pipe,
        .stderr = .pipe,
    });

    try child.stdin.?.writeStreamingAll(io, input);
    child.stdin.?.close(io);
    child.stdin = null;

    var stdout_buf: [4096]u8 = undefined;
    var stdout_rdr = child.stdout.?.reader(io, &stdout_buf);
    const stdout_bytes = try stdout_rdr.interface.allocRemaining(allocator, .unlimited);

    var stderr_buf: [4096]u8 = undefined;
    var stderr_rdr = child.stderr.?.reader(io, &stderr_buf);
    const stderr_bytes = try stderr_rdr.interface.allocRemaining(allocator, .unlimited);

    const term = try child.wait(io);
    switch (term) {
        .exited => |code| {
            if (code != 0) {
                std.debug.print("capnp convert failed: {s}\n", .{stderr_bytes});
                allocator.free(stdout_bytes);
                allocator.free(stderr_bytes);
                return error.CapnpConvertFailed;
            }
        },
        else => {
            std.debug.print("capnp convert failed: unexpected termination\n", .{});
            allocator.free(stdout_bytes);
            allocator.free(stderr_bytes);
            return error.CapnpConvertFailed;
        },
    }

    allocator.free(stderr_bytes);
    return stdout_bytes;
}

test "Schema validation and canonicalization (TestAllTypes)" {
    const allocator = std.testing.allocator;

    const request = try loadCodeGeneratorRequest(allocator);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    const root_node = compare.findStructBySuffix(request.nodes, "TestAllTypes") orelse return error.InvalidSchema;

    const bytes = try compare.readFileAlloc(allocator, "tests/capnp_testdata/testdata/binary");
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    try schema_validation.validateMessage(&msg, request.nodes, root_node, .{});

    const canonical_flat = try schema_validation.canonicalizeMessageFlat(allocator, &msg, request.nodes, root_node, .{});
    defer allocator.free(canonical_flat);

    const expected = try capnpConvertCanonical(allocator, bytes);
    defer allocator.free(expected);

    try std.testing.expectEqualSlices(u8, expected, canonical_flat);

    // Also exercise the segment-framed variant. It is part of the frozen Stable
    // surface but had NO caller anywhere in the tree, so its body was never
    // type-checked — and it did not in fact compile (it returned `[]const u8`
    // from a `![]u8` signature). Calling it here is what keeps it honest.
    const canonical_framed = try schema_validation.canonicalizeMessage(allocator, &msg, request.nodes, root_node, .{});
    defer allocator.free(canonical_framed);

    // The framed form carries the segment table; the flat form is the bare
    // single segment. The framed payload must contain the flat bytes.
    try std.testing.expect(canonical_framed.len >= canonical_flat.len);
    try std.testing.expect(std.mem.indexOf(u8, canonical_framed, canonical_flat) != null);
}

test "canonicalization keeps a pointer that equals its schema default, matching capnp" {
    const allocator = std.testing.allocator;

    const request = try loadCodeGeneratorRequest(allocator);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    const root_node = compare.findStructBySuffix(request.nodes, "TestDefaults") orelse return error.InvalidSchema;

    // Let the reference implementation build the input, so no layout is
    // hand-computed here: a TestDefaults whose textField is EXPLICITLY set to
    // "foo" -- which is that field's schema default.
    const input = try capnpConvert(allocator, "(textField = \"foo\")", "text:binary", "TestDefaults");
    defer allocator.free(input);

    var msg = try message.Message.init(allocator, input, .{});
    defer msg.deinit();

    // The reference canonicalizer is SCHEMA-FREE: it cannot know the written
    // text equals the field default, so its canonical form keeps the pointer.
    // Ours must agree BY DEFAULT, or canonicalize-and-compare against any
    // other implementation's output breaks exactly on default-valued fields.
    const expected = try capnpConvert(allocator, input, "binary:canonical", "TestDefaults");
    defer allocator.free(expected);

    const ours = try schema_validation.canonicalizeMessageFlat(allocator, &msg, request.nodes, root_node, .{});
    defer allocator.free(ours);

    try std.testing.expectEqualSlices(u8, expected, ours);

    // The schema-aware omission stays available as an explicit opt-in -- it is
    // a capnp-zig extension for schema-aware equality, not the spec's
    // canonical form. With it, the default-equal pointer is nulled and the
    // output must therefore be strictly smaller.
    const omitted = try schema_validation.canonicalizeMessageFlat(allocator, &msg, request.nodes, root_node, .{
        .omit_default_pointers = true,
    });
    defer allocator.free(omitted);
    try std.testing.expect(omitted.len < ours.len);
}
