const std = @import("std");
const capnpc = @import("capnpc-zig");
const request_reader = capnpc.request;
const capnp_cli = @import("support/capnp_cli.zig");
fn writeFile(dir: std.Io.Dir, name: []const u8, data: []const u8) !void {
    var file = try dir.createFile(std.testing.io, name, .{});
    defer file.close(std.testing.io);
    try file.writeStreamingAll(std.testing.io, data);
}
fn runGeneratedHarness(
    allocator: std.mem.Allocator,
    schema_path: []const u8,
    harness_source: []const u8,
) !void {
    return runGeneratedHarnessProfile(allocator, schema_path, harness_source, .full);
}

fn runGeneratedHarnessProfile(
    allocator: std.mem.Allocator,
    schema_path: []const u8,
    harness_source: []const u8,
    profile: capnpc.codegen.Generator.ApiProfile,
) !void {
    return runGeneratedHarnessFiles(allocator, &.{schema_path}, harness_source, profile, false);
}

fn runGeneratedHarnessFiles(allocator: std.mem.Allocator, schema_paths: []const []const u8, harness_source: []const u8, profile: capnpc.codegen.Generator.ApiProfile, packaged_includes: bool) !void {
    const io = std.testing.io;

    var capnp_argv = std.ArrayList([]const u8).empty;
    defer capnp_argv.deinit(allocator);
    try capnp_argv.appendSlice(allocator, &.{ "compile", "-o-", "--src-prefix=tests/test_schemas" });
    try capnp_argv.appendSlice(allocator, schema_paths);
    const capnp_result = if (packaged_includes) blk: {
        try capnp_argv.insert(allocator, 0, "capnp");
        try capnp_argv.appendSlice(allocator, &.{ "--no-standard-import", "-Isrc/rpc" });
        break :blk try std.process.run(allocator, io, .{ .argv = capnp_argv.items });
    } else try capnp_cli.run(allocator, io, capnp_argv.items, .{});
    defer allocator.free(capnp_result.stdout);
    defer allocator.free(capnp_result.stderr);
    try std.testing.expect(capnp_result.term == .exited and capnp_result.term.exited == 0);

    const request = try request_reader.parseCodeGeneratorRequest(allocator, capnp_result.stdout);
    defer request_reader.freeCodeGeneratorRequest(allocator, request);
    if (request.requested_files.len == 0) return error.InvalidCodeGeneratorRequest;

    var generator = try capnpc.codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();
    generator.setApiProfile(profile);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for (request.requested_files, 0..) |file, index| {
        const output = try generator.generateFile(file);
        defer allocator.free(output);
        const filename = if (index == 0) try allocator.dupe(u8, "generated.zig") else try std.fmt.allocPrint(allocator, "{s}.zig", .{file.filename[0 .. file.filename.len - ".capnp".len]});
        defer allocator.free(filename);
        try writeFile(tmp.dir, filename, output);
    }
    try writeFile(tmp.dir, "harness.zig", harness_source);

    const harness_path = try tmp.dir.realPathFileAlloc(io, "harness.zig", allocator);
    defer allocator.free(harness_path);

    const lib_path = try std.Io.Dir.cwd().realPathFileAlloc(io, "src/lib.zig", allocator);
    defer allocator.free(lib_path);

    const lib_arg = try std.fmt.allocPrint(allocator, "-Mcapnpc-zig={s}", .{lib_path});
    defer allocator.free(lib_arg);
    const root_arg = try std.fmt.allocPrint(allocator, "-Mroot={s}", .{harness_path});
    defer allocator.free(root_arg);

    var zig_argv = std.ArrayList([]const u8).empty;
    defer zig_argv.deinit(allocator);
    try zig_argv.append(allocator, "zig");
    try zig_argv.append(allocator, "test");
    // root module depends on capnpc-zig
    try zig_argv.append(allocator, "--dep");
    try zig_argv.append(allocator, "capnpc-zig");
    try zig_argv.append(allocator, root_arg);
    // capnpc-zig depends on itself: generated interface code imports the RPC
    // runtime via `@import("capnpc-zig")`, which the library re-exports through
    // its own module name. Without this self-dependency, interface schemas fail
    // to compile with "no module named 'capnpc-zig'".
    try zig_argv.append(allocator, "--dep");
    try zig_argv.append(allocator, "capnpc-zig");
    try zig_argv.append(allocator, lib_arg);

    const zig_result = std.process.run(allocator, io, .{
        .argv = zig_argv.items,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.ZigCompilerUnavailable,
        else => return err,
    };
    defer allocator.free(zig_result.stdout);
    defer allocator.free(zig_result.stderr);

    if (!(zig_result.term == .exited and zig_result.term.exited == 0)) {
        std.debug.print("zig test stdout:\n{s}\n", .{zig_result.stdout});
        std.debug.print("zig test stderr:\n{s}\n", .{zig_result.stderr});
        return error.GeneratedRuntimeCompileFailed;
    }
}

test "generated nested pipeline paths preserve pointer transforms and recursive traversal" {
    try runGeneratedHarness(std.testing.allocator, "tests/test_schemas/rpc_pipeline_paths.capnp", @embedFile("support/rpc_pipeline_consumer.zig"));
}

test "generated inherited collisions preserve separate client and server methods" {
    try runGeneratedHarnessFiles(std.testing.allocator, &.{ "tests/test_schemas/rpc_inherited_paths.capnp", "tests/test_schemas/rpc_inherited_external.capnp" }, @embedFile("support/rpc_inherited_consumer.zig"), .full, false);
}

test "packaged streaming schemas compile and run without generated standard imports" {
    try runGeneratedHarnessFiles(std.testing.allocator, &.{"tests/test_schemas/streaming.capnp"}, @embedFile("support/rpc_stream_consumer.zig"), .full, true);
}
