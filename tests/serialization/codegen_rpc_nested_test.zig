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

    const result = std.process.run(allocator, std.testing.io, .{
        .argv = argv,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try std.testing.expect(result.term == .exited and result.term.exited == 0);

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

    const result = std.process.run(allocator, std.testing.io, .{
        .argv = argv,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try std.testing.expect(result.term == .exited and result.term.exited == 0);

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

// BUG 1: enum values and method ordinals must come from the element's ordinal
// (its @N, equal to its index in the ordinal-sorted schema list), not from
// declaration order (code_order). The schema declares enumerants and methods
// out of ordinal order so the two orderings diverge.
test "Codegen enum values and method ordinals use ordinal, not declaration order" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "capnp",
        "compile",
        "-o-",
        "tests/test_schemas/ordinal_ordering.capnp",
    };

    const result = std.process.run(allocator, std.testing.io, .{
        .argv = argv,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try std.testing.expect(result.term == .exited and result.term.exited == 0);

    const request = try request_reader.parseCodeGeneratorRequest(allocator, result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    try std.testing.expect(request.requested_files.len >= 1);
    const file = request.requested_files[0];

    var generator = try capnpc.codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();

    const output = try generator.generateFile(file);
    defer allocator.free(output);

    // Enum values must match @N ordinals (red@0, green@1, blue@2), NOT the
    // declaration order (which would give green=0, red=1).
    try expectContains(output, "Red = 0,");
    try expectContains(output, "Green = 1,");
    try expectContains(output, "Blue = 2,");
    // The buggy code_order output would have emitted these:
    try std.testing.expect(std.mem.indexOf(u8, output, "Green = 0,") == null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Red = 1,") == null);

    // Method enum values must match @N ordinals (alpha@0, beta@1, gamma@2).
    try expectContains(output, "Alpha = 0,");
    try expectContains(output, "Beta = 1,");
    try expectContains(output, "Gamma = 2,");
    try std.testing.expect(std.mem.indexOf(u8, output, "Beta = 0,") == null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Alpha = 1,") == null);

    // Per-method `ordinal` constants must match @N ordinals too. The methods
    // list is ordinal-sorted, so Alpha (ordinal 0) is generated first.
    const alpha_idx = std.mem.indexOf(u8, output, "pub const Alpha = struct").?;
    const beta_idx = std.mem.indexOf(u8, output, "pub const Beta = struct").?;
    const gamma_idx = std.mem.indexOf(u8, output, "pub const Gamma = struct").?;
    try std.testing.expect(alpha_idx < beta_idx);
    try std.testing.expect(beta_idx < gamma_idx);

    // The ordinal constants appear in the same order: 0, then 1, then 2.
    const ord0 = std.mem.indexOf(u8, output, "pub const ordinal: u16 = 0;").?;
    const ord1 = std.mem.indexOf(u8, output, "pub const ordinal: u16 = 1;").?;
    const ord2 = std.mem.indexOf(u8, output, "pub const ordinal: u16 = 2;").?;
    try std.testing.expect(ord0 < ord1);
    try std.testing.expect(ord1 < ord2);
    // The ordinal-0 constant belongs to the Alpha struct (declared @0).
    try std.testing.expect(alpha_idx < ord0);
    try std.testing.expect(ord0 < beta_idx);
}
