const std = @import("std");
const capnpc = @import("capnpc-zig");
const compare = @import("support/capnp_compare.zig");
const capnp_cli = @import("support/capnp_cli.zig");

const message = capnpc.message;
const schema = capnpc.schema;
const request_reader = capnpc.request;

const Fixture = struct {
    path: []const u8,
    is_packed: bool,
};

fn loadCodeGeneratorRequest(allocator: std.mem.Allocator) !schema.CodeGeneratorRequest {
    const argv = [_][]const u8{
        "compile",
        "--no-standard-import",
        "-Itests/capnp_testdata",
        "-o-",
        "tests/capnp_testdata/test.capnp",
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

test "capnp testdata: TestAllTypes fixtures" {
    const allocator = std.testing.allocator;

    const request = try loadCodeGeneratorRequest(allocator);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    const root_node = compare.findStructBySuffix(request.nodes, "TestAllTypes") orelse return error.InvalidSchema;

    var parsed = try compare.loadJson(allocator, "tests/capnp_testdata/testdata/pretty.json");
    defer parsed.deinit();

    const ctx = compare.Context{ .allocator = allocator, .nodes = request.nodes };

    const fixtures = [_]Fixture{
        .{ .path = "tests/capnp_testdata/testdata/binary", .is_packed = false },
        .{ .path = "tests/capnp_testdata/testdata/segmented", .is_packed = false },
        .{ .path = "tests/capnp_testdata/testdata/packed", .is_packed = true },
        .{ .path = "tests/capnp_testdata/testdata/segmented-packed", .is_packed = true },
    };

    for (fixtures) |fixture| {
        const bytes = try compare.readFileAlloc(allocator, fixture.path);
        defer allocator.free(bytes);

        var msg = if (fixture.is_packed)
            try message.Message.initPacked(allocator, bytes, .{})
        else
            try message.Message.init(allocator, bytes, .{});
        defer msg.deinit();

        const root = try msg.getRootStruct();
        try compare.compareStruct(&ctx, root_node, root, parsed.value);
    }
}
