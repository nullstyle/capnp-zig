const std = @import("std");
const capnpc = @import("capnpc-zig");
const compare = @import("support/capnp_compare.zig");

const message = capnpc.message;
const schema = capnpc.schema;
const request_reader = capnpc.request;
const schema_validation = capnpc.schema_validation;

fn loadCodeGeneratorRequest(allocator: std.mem.Allocator) !schema.CodeGeneratorRequest {
    const argv = [_][]const u8{
        "capnp",
        "compile",
        "--no-standard-import",
        "-Itests/capnp_testdata",
        "-o-",
        "tests/capnp_testdata/test.capnp",
    };

    const result = std.process.run(allocator, std.testing.io, .{
        .argv = &argv,
    }) catch |err| {
        return switch (err) {
            error.FileNotFound => error.SkipZigTest,
            else => err,
        };
    };
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

fn capnpConvertCanonical(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const io = std.testing.io;
    const argv = [_][]const u8{
        "capnp",
        "convert",
        "binary:canonical",
        "--no-standard-import",
        "-Itests/capnp_testdata",
        "tests/capnp_testdata/test.capnp",
        "TestAllTypes",
    };

    var child = std.process.spawn(io, .{
        .argv = &argv,
        .stdin = .pipe,
        .stdout = .pipe,
        .stderr = .pipe,
    }) catch |err| {
        return switch (err) {
            error.FileNotFound => error.SkipZigTest,
            else => err,
        };
    };

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

    var msg = try message.Message.init(allocator, bytes);
    defer msg.deinit();

    try schema_validation.validateMessage(&msg, request.nodes, root_node, .{});

    const canonical_flat = try schema_validation.canonicalizeMessageFlat(allocator, &msg, request.nodes, root_node, .{});
    defer allocator.free(canonical_flat);

    const expected = try capnpConvertCanonical(allocator, bytes);
    defer allocator.free(expected);

    try std.testing.expectEqualSlices(u8, expected, canonical_flat);
}
