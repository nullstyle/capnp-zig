const std = @import("std");
const capnpc = @import("capnpc-zig");
const compare = @import("support/capnp_compare.zig");

const message = capnpc.message;
const schema = capnpc.schema;
const request_reader = capnpc.request;
const schema_validation = capnpc.schema_validation;

const max_output = 32 * 1024 * 1024;

fn loadCodeGeneratorRequest(allocator: std.mem.Allocator) !schema.CodeGeneratorRequest {
    const argv = [_][]const u8{
        "capnp",
        "compile",
        "--no-standard-import",
        "-Itests/capnp_testdata",
        "-o-",
        "tests/capnp_testdata/test.capnp",
    };

    var child = std.process.Child.init(&argv, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    child.spawn() catch |err| {
        return switch (err) {
            error.FileNotFound => error.SkipZigTest,
            else => err,
        };
    };

    const stdout_bytes = try child.stdout.?.readToEndAlloc(allocator, max_output);
    const stderr_bytes = try child.stderr.?.readToEndAlloc(allocator, max_output);

    const term = try child.wait();
    switch (term) {
        .Exited => |code| {
            if (code != 0) {
                std.debug.print("capnp compile failed: {s}\n", .{stderr_bytes});
                allocator.free(stdout_bytes);
                allocator.free(stderr_bytes);
                return error.CapnpCompileFailed;
            }
        },
        else => {
            std.debug.print("capnp compile failed: unexpected termination\n", .{});
            allocator.free(stdout_bytes);
            allocator.free(stderr_bytes);
            return error.CapnpCompileFailed;
        },
    }

    allocator.free(stderr_bytes);

    const request = try request_reader.parseCodeGeneratorRequest(allocator, stdout_bytes);
    allocator.free(stdout_bytes);
    return request;
}

fn capnpConvertCanonical(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const argv = [_][]const u8{
        "capnp",
        "convert",
        "binary:canonical",
        "--no-standard-import",
        "-Itests/capnp_testdata",
        "tests/capnp_testdata/test.capnp",
        "TestAllTypes",
    };

    var child = std.process.Child.init(&argv, allocator);
    child.stdin_behavior = .Pipe;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    child.spawn() catch |err| {
        return switch (err) {
            error.FileNotFound => error.SkipZigTest,
            else => err,
        };
    };

    try child.stdin.?.writeAll(input);
    child.stdin.?.close();
    child.stdin = null;

    const stdout_bytes = try child.stdout.?.readToEndAlloc(allocator, max_output);
    const stderr_bytes = try child.stderr.?.readToEndAlloc(allocator, max_output);

    const term = try child.wait();
    switch (term) {
        .Exited => |code| {
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

    var msg = try message.Message.init(allocator, bytes);
    defer msg.deinit();

    try schema_validation.validateMessage(&msg, request.nodes, root_node, .{});

    const canonical_flat = try schema_validation.canonicalizeMessageFlat(allocator, &msg, request.nodes, root_node, .{});
    defer allocator.free(canonical_flat);

    const expected = try capnpConvertCanonical(allocator, bytes);
    defer allocator.free(expected);

    try std.testing.expectEqualSlices(u8, expected, canonical_flat);
}
