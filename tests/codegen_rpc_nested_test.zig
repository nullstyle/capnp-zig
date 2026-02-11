const std = @import("std");
const capnpc = @import("capnpc-zig");
const request_reader = capnpc.request;

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) == null) {
        return error.MissingExpectedOutput;
    }
}

test "Codegen emits nested param/result structs with sanitized names" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "capnp",
        "compile",
        "-o-",
        "tests/test_schemas/rpc_nested.capnp",
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

    var generator = try capnpc.codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();

    const output = try generator.generateFile(file);
    defer allocator.free(output);

    try expectContains(output, "pub const PingParams");
    try expectContains(output, "pub const PingResults");
    try expectContains(output, "pub const RpcNested = struct");
    try expectContains(output, "pub const Ping = struct");
    try expectContains(output, "const rpc = capnpc.rpc;");
    try std.testing.expect(std.mem.indexOf(u8, output, "$") == null);
}

test "Codegen emits nested interface definitions" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "capnp",
        "compile",
        "-o-",
        "tests/test_schemas/nested_interfaces.capnp",
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

    var generator = try capnpc.codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();

    const output = try generator.generateFile(file);
    defer allocator.free(output);

    try expectContains(output, "pub const Outer = struct");
    try expectContains(output, "pub const Inner = struct");
    try expectContains(output, "pub const GetInner = struct");
    try expectContains(output, "pub const Ping = struct");
    try expectContains(output, "pub const GetInnerResults");
    try expectContains(output, "pub const PingParams");
    try expectContains(output, "pub fn callGetInner");
    try expectContains(output, "pub fn callPing");

    // GAP-5: Typed capability resolution in readers
    try expectContains(output, "pub fn resolveInner");
    try expectContains(output, "Inner.Client");
    try expectContains(output, "var mutable_caps = caps.*;");
    try expectContains(output, "try mutable_caps.retainCapability(cap);");

    // Bootstrap callback should retain returned client capability so
    // callback cleanup does not immediately release it.
    try expectContains(output,
        \\const cap = try payload.content.getCapability();
        \\                var mutable_caps = caps.*;
        \\                try mutable_caps.retainCapability(cap);
        \\                const resolved = try caps.resolveCapability(cap);
    );

    // GAP-3: Typed capability parameter passing in builders
    try expectContains(output, "pub fn setInnerServer");
    try expectContains(output, "pub fn setInnerClient");

    // GAP-8: Deferred handler returns
    try expectContains(output, "pub const DeferredHandler");
    try expectContains(output, "pub const ReturnSender");
    try expectContains(output, "_deferred: ?");

    // GAP-1: Promise pipelining
    try expectContains(output, "pub const PipelinedClient");
    try expectContains(output, "sendCallPromisedWithOps");
    try expectContains(output, "GetInnerPipeline");
    try expectContains(output, "callGetInnerPipelined");
}
