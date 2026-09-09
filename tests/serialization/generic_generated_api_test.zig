const std = @import("std");
const capnpc = @import("capnpc-zig");
const capnp_cli = @import("support/capnp_cli.zig");

fn writeFile(dir: std.Io.Dir, name: []const u8, bytes: []const u8) !void {
    var file = try dir.createFile(std.testing.io, name, .{});
    defer file.close(std.testing.io);
    try file.writeStreamingAll(std.testing.io, bytes);
}

fn run(schema_path: []const u8, harness: []const u8, profile: capnpc.codegen.Generator.ApiProfile) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const compiled = try capnp_cli.run(allocator, io, &.{ "compile", "-o-", schema_path }, .{});
    defer allocator.free(compiled.stdout);
    defer allocator.free(compiled.stderr);
    if (compiled.term != .exited or compiled.term.exited != 0) {
        std.debug.print("schema compiler: {s}\n", .{compiled.stderr});
        return error.SchemaCompileFailed;
    }
    const request = try capnpc.request.parseCodeGeneratorRequest(allocator, compiled.stdout);
    defer capnpc.request.freeCodeGeneratorRequest(allocator, request);
    var generator = try capnpc.codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();
    generator.setApiProfile(profile);
    generator.setCodegenBudget(.{ .max_brand_specializations = 2 });
    const generated = try generator.generateFile(request.requested_files[0]);
    defer allocator.free(generated);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeFile(tmp.dir, "generated.zig", generated);
    try writeFile(tmp.dir, "harness.zig", harness);
    const harness_path = try tmp.dir.realPathFileAlloc(io, "harness.zig", allocator);
    defer allocator.free(harness_path);
    const library_path = try std.Io.Dir.cwd().realPathFileAlloc(io, "src/lib.zig", allocator);
    defer allocator.free(library_path);
    const root_arg = try std.fmt.allocPrint(allocator, "-Mroot={s}", .{harness_path});
    defer allocator.free(root_arg);
    const library_arg = try std.fmt.allocPrint(allocator, "-Mcapnpc-zig={s}", .{library_path});
    defer allocator.free(library_arg);
    const result = try std.process.run(allocator, io, .{
        .argv = &.{ "zig", "test", "--dep", "capnpc-zig", root_arg, "--dep", "capnpc-zig", library_arg },
    });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);
    if (result.term != .exited or result.term.exited != 0) {
        std.debug.print("generated API: {s}\n{s}\n", .{ result.stdout, result.stderr });
        return error.GeneratedApiFailed;
    }
}

test "generated lists retain concrete generic element reader and builder types" {
    inline for (.{ capnpc.codegen.Generator.ApiProfile.full, .compact }) |profile| {
        try run("tests/test_schemas/generic_collections.capnp", @embedFile("support/generic_collections_consumer.zig"), profile);
    }
}

test "generated recursive applications retain typed readers and builders" {
    inline for (.{ capnpc.codegen.Generator.ApiProfile.full, .compact }) |profile| {
        try run("tests/test_schemas/generic_recursive.capnp", @embedFile("support/generic_recursive_consumer.zig"), profile);
    }
}

test "typed generic list reopening preserves unknown data while growing element pointers" {
    try run("tests/test_schemas/generic_collections.capnp", @embedFile("support/generic_evolution_consumer.zig"), .full);
}

test "recursive applications distinguish alternating concrete bindings" {
    inline for (.{ capnpc.codegen.Generator.ApiProfile.full, .compact }) |profile| {
        try run("tests/test_schemas/generic_alternating.capnp", @embedFile("support/generic_alternating_consumer.zig"), profile);
    }
}

test "generic list views retain pointer defaults and union guards" {
    inline for (.{ capnpc.codegen.Generator.ApiProfile.full, .compact }) |profile| {
        try run("tests/test_schemas/generic_list_defaults.capnp", @embedFile("support/generic_list_defaults_consumer.zig"), profile);
    }
}
