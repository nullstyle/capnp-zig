const std = @import("std");
const capnpc = @import("capnpc-zig");
const request_reader = capnpc.request;
const capnp_cli = @import("support/capnp_cli.zig");

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) == null) {
        return error.MissingExpectedOutput;
    }
}

test "Codegen emits nested param/result structs with sanitized names" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "compile",
        "-o-",
        "tests/test_schemas/rpc_nested.capnp",
    };

    const result = try capnp_cli.run(allocator, std.testing.io, argv, .{});
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
        "compile",
        "-o-",
        "tests/test_schemas/nested_interfaces.capnp",
    };

    const result = try capnp_cli.run(allocator, std.testing.io, argv, .{});
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
    try expectContains(output, "pub fn callGetInnerWithOptions");
    try expectContains(output, "pub fn callPing");
    try expectContains(output, "sendCallGeneratedWithOptions");

    // Parent-qualified nested interface: `Inner` is now emitted INSIDE `Outer`'s
    // body (self-qualified), not flat at file scope. So `pub const Inner = struct`
    // appears AFTER `pub const Outer = struct` opens and BEFORE it closes, and
    // Inner's own decls self-qualify as `Inner.X` so they aren't shadowed by
    // Outer's same-named Client/Server/VTable/PipelinedClient/BootstrapContext.
    {
        const outer_idx = std.mem.indexOf(u8, output, "pub const Outer = struct").?;
        const inner_idx = std.mem.indexOf(u8, output, "pub const Inner = struct").?;
        // Outer's closing `};` is at column 0 (file scope). Find the first
        // file-scope `};\n` after Outer opens — Inner must come before it.
        const outer_close = std.mem.indexOfPos(u8, output, outer_idx, "\n};\n").?;
        try std.testing.expect(outer_idx < inner_idx);
        try std.testing.expect(inner_idx < outer_close);
    }
    // Inner self-qualifies its own decls (would be `Client`/`Server`/… bare under
    // the old flat emission, which then collided with Outer's).
    try expectContains(output, "pub fn init(peer: *rpc.peer.Peer, cap_id: u32) Inner.Client {");
    try expectContains(output, "vtable: Inner.VTable,");
    try expectContains(output, "const server: *Inner.Server = @ptrCast(@alignCast(ctx));");
    try expectContains(output, "response: Inner.BootstrapResponse");

    // GAP-5: Typed capability resolution in readers. Outer's resolveInner now
    // reaches the nested interface by its full parent-qualified path.
    try expectContains(output, "pub fn resolveInner");
    try expectContains(output, "Outer.Inner.Client");
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

    // GAP-3: Typed capability parameter passing in builders. The nested-interface
    // param types are parent-qualified.
    try expectContains(output, "pub fn setInnerServer(self: *Builder, peer: *rpc.peer.Peer, server: *Outer.Inner.Server) !void {");
    try expectContains(output, "pub fn setInnerClient(self: *Builder, client: Outer.Inner.Client) !void {");

    // GAP-8: Deferred handler returns
    try expectContains(output, "pub const DeferredHandler");
    try expectContains(output, "pub const ReturnSender");
    try expectContains(output, "_deferred: ?");

    // GAP-1: Promise pipelining
    try expectContains(output, "pub const PipelinedClient");
    try expectContains(output, "sendCallPromisedWithOps");
    try expectContains(output, "sendCallPromisedWithOpsGeneratedWithOptions");
    try expectContains(output, "GetInnerPipeline");
    try expectContains(output, "callGetInnerPipelined");
    try expectContains(output, "callGetInnerPipelinedWithOptions");
}

// BUG 1: enum values and method ordinals must come from the element's ordinal
// (its @N, equal to its index in the ordinal-sorted schema list), not from
// declaration order (code_order). The schema declares enumerants and methods
// out of ordinal order so the two orderings diverge.
test "Codegen enum values and method ordinals use ordinal, not declaration order" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "compile",
        "-o-",
        "tests/test_schemas/ordinal_ordering.capnp",
    };

    const result = try capnp_cli.run(allocator, std.testing.io, argv, .{});
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

// BUG 2: an interface may inherit a method whose generated Zig name collides
// with one of its own (or another ancestor's) methods. The generator emits
// callXxx/VTable members for own AND inherited methods into the same Client /
// PipelinedClient / VTable namespaces, so a name clash would previously emit
// duplicate declarations = uncompilable Zig. The name validator must instead
// reject the collision with a clean error.DuplicateGeneratedName before any
// code is emitted.
test "Codegen rejects inherited method-name collisions with a clean error" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "compile",
        "-o-",
        "tests/test_schemas/inherited_method_collision.capnp",
    };

    const result = try capnp_cli.run(allocator, std.testing.io, argv, .{});
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    try std.testing.expect(result.term == .exited and result.term.exited == 0);

    const request = try request_reader.parseCodeGeneratorRequest(allocator, result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    try std.testing.expect(request.requested_files.len >= 1);
    const file = request.requested_files[0];

    var generator = try capnpc.codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();

    // Derived.ping collides with the inherited Base.ping -> clean plugin error,
    // not uncompilable duplicate-declaration output.
    try std.testing.expectError(error.DuplicateGeneratedName, generator.generateFile(file));
}
