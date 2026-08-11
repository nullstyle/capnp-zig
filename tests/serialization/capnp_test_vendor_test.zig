const std = @import("std");
const capnpc = @import("capnpc-zig");
const compare = @import("support/capnp_compare.zig");
const capnp_cli = @import("support/capnp_cli.zig");

const message = capnpc.message;
const schema = capnpc.schema;
const request_reader = capnpc.request;
const json = std.json;

fn requirePath(path: []const u8) !void {
    std.Io.Dir.cwd().access(std.testing.io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
}

fn runCapnp(allocator: std.mem.Allocator, argv: []const []const u8) ![]u8 {
    const result = try capnp_cli.run(allocator, std.testing.io, argv, .{});
    defer allocator.free(result.stderr);

    switch (result.term) {
        .exited => |code| {
            if (code != 0) {
                std.debug.print("capnp command failed: {s}\n", .{result.stderr});
                allocator.free(result.stdout);
                return error.CapnpCommandFailed;
            }
        },
        else => {
            std.debug.print("capnp command failed: unexpected termination\n", .{});
            allocator.free(result.stdout);
            return error.CapnpCommandFailed;
        },
    }

    return result.stdout;
}

fn capnpEvalJsonStruct(allocator: std.mem.Allocator, name: []const u8) !json.Parsed(json.Value) {
    const argv = [_][]const u8{
        "eval",
        "--output=json",
        "--short",
        "vendor/ext/capnp_test/test.capnp",
        name,
    };
    const stdout_bytes = try runCapnp(allocator, &argv);
    defer allocator.free(stdout_bytes);
    return try json.parseFromSlice(json.Value, allocator, stdout_bytes, .{ .allocate = .alloc_always });
}

fn capnpEvalTextAsJson(allocator: std.mem.Allocator, name: []const u8) !json.Parsed(json.Value) {
    const argv = [_][]const u8{
        "eval",
        "--output=text",
        "--short",
        "vendor/ext/capnp_test/test.capnp",
        name,
    };
    const stdout_bytes = try runCapnp(allocator, &argv);
    defer allocator.free(stdout_bytes);
    return try json.parseFromSlice(json.Value, allocator, stdout_bytes, .{ .allocate = .alloc_always });
}

fn capnpEvalBinary(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const argv = [_][]const u8{
        "eval",
        "-obinary",
        "vendor/ext/capnp_test/test.capnp",
        name,
    };
    return try runCapnp(allocator, &argv);
}

fn loadCodeGeneratorRequest(allocator: std.mem.Allocator) !schema.CodeGeneratorRequest {
    const argv = [_][]const u8{
        "compile",
        "-Ivendor/ext/capnp_test",
        "-o-",
        "vendor/ext/capnp_test/test.capnp",
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

fn expectJsonArray(value: json.Value) ![]const json.Value {
    return switch (value) {
        .array => |arr| arr.items,
        else => error.InvalidJsonFixture,
    };
}

fn expectJsonString(value: json.Value) ![]const u8 {
    return switch (value) {
        .string => |s| s,
        else => error.InvalidJsonFixture,
    };
}

test "capnp_test vendor suite" {
    const allocator = std.testing.allocator;

    try requirePath("vendor/ext/capnp_test/test.capnp");

    const request = try loadCodeGeneratorRequest(allocator);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    var tests_json = try capnpEvalTextAsJson(allocator, "allTests");
    defer tests_json.deinit();

    const tests_array = try expectJsonArray(tests_json.value);

    for (tests_array) |test_value| {
        const test_name = try expectJsonString(test_value);

        const type_key = try std.fmt.allocPrint(allocator, "{s}Type", .{test_name});
        defer allocator.free(type_key);

        var type_json = try capnpEvalTextAsJson(allocator, type_key);
        defer type_json.deinit();

        const type_name = try expectJsonString(type_json.value);
        const suffix = try std.fmt.allocPrint(allocator, ":{s}", .{type_name});
        defer allocator.free(suffix);

        const struct_node = compare.findStructBySuffix(request.nodes, suffix) orelse return error.InvalidSchema;

        var expected_json = try capnpEvalJsonStruct(allocator, test_name);
        defer expected_json.deinit();

        const bytes = try capnpEvalBinary(allocator, test_name);
        defer allocator.free(bytes);

        var msg = try message.Message.init(allocator, bytes, .{});
        defer msg.deinit();

        const root = try msg.getRootStruct();
        const ctx = compare.Context{ .allocator = allocator, .nodes = request.nodes };
        try compare.compareStruct(&ctx, struct_node, root, expected_json.value);
    }
}
