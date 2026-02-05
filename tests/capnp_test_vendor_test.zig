const std = @import("std");
const capnpc = @import("capnpc-zig");
const compare = @import("support/capnp_compare.zig");

const message = capnpc.message;
const schema = capnpc.schema;
const request_reader = capnpc.request;
const json = std.json;

const max_capnp_output = 32 * 1024 * 1024;

fn runCapnp(allocator: std.mem.Allocator, argv: []const []const u8) ![]u8 {
    var child = std.process.Child.init(argv, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;

    child.spawn() catch |err| {
        return switch (err) {
            error.FileNotFound => error.SkipZigTest,
            else => err,
        };
    };

    const stdout_bytes = try child.stdout.?.readToEndAlloc(allocator, max_capnp_output);
    const stderr_bytes = try child.stderr.?.readToEndAlloc(allocator, max_capnp_output);

    const term = try child.wait();
    switch (term) {
        .Exited => |code| {
            if (code != 0) {
                std.debug.print("capnp command failed: {s}\n", .{stderr_bytes});
                allocator.free(stdout_bytes);
                allocator.free(stderr_bytes);
                return error.CapnpCommandFailed;
            }
        },
        else => {
            std.debug.print("capnp command failed: unexpected termination\n", .{});
            allocator.free(stdout_bytes);
            allocator.free(stderr_bytes);
            return error.CapnpCommandFailed;
        },
    }

    allocator.free(stderr_bytes);
    return stdout_bytes;
}

fn capnpEvalJsonStruct(allocator: std.mem.Allocator, name: []const u8) !json.Parsed(json.Value) {
    const argv = [_][]const u8{
        "capnp",
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
        "capnp",
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
        "capnp",
        "eval",
        "-obinary",
        "vendor/ext/capnp_test/test.capnp",
        name,
    };
    return try runCapnp(allocator, &argv);
}

fn loadCodeGeneratorRequest(allocator: std.mem.Allocator) !schema.CodeGeneratorRequest {
    const argv = [_][]const u8{
        "capnp",
        "compile",
        "-Ivendor/ext/capnp_test",
        "-o-",
        "vendor/ext/capnp_test/test.capnp",
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

    const stdout_bytes = try child.stdout.?.readToEndAlloc(allocator, max_capnp_output);
    const stderr_bytes = try child.stderr.?.readToEndAlloc(allocator, max_capnp_output);

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

        var msg = try message.Message.init(allocator, bytes);
        defer msg.deinit();

        const root = try msg.getRootStruct();
        const ctx = compare.Context{ .allocator = allocator, .nodes = request.nodes };
        try compare.compareStruct(&ctx, struct_node, root, expected_json.value);
    }
}
