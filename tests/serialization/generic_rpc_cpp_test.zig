const std = @import("std");
const builtin = @import("builtin");
const capnp = @import("capnpc-zig");
const cli = @import("support/capnp_cli.zig");

fn run(argv: []const []const u8) ![]u8 {
    const result = try std.process.run(std.testing.allocator, std.testing.io, .{ .argv = argv });
    defer std.testing.allocator.free(result.stderr);
    if (result.term != .exited or result.term.exited != 0) {
        defer std.testing.allocator.free(result.stdout);
        std.debug.print("command {s}:\n{s}\n{s}\n", .{ argv[0], result.stdout, result.stderr });
        return error.GenericInteropCommandFailed;
    }
    return result.stdout;
}
fn writeFile(dir: std.Io.Dir, path: []const u8, bytes: []const u8) !void {
    var file = try dir.createFile(std.testing.io, path, .{});
    defer file.close(std.testing.io);
    try file.writeStreamingAll(std.testing.io, bytes);
}
fn runProfile(profile: capnp.codegen.Generator.ApiProfile) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const compiled = try cli.run(allocator, io, &.{ "compile", "-o-", "--src-prefix=tests/test_schemas", "tests/test_schemas/generic_rpc.capnp", "tests/test_schemas/generic_rpc_external.capnp" }, .{ .missing = .required });
    defer allocator.free(compiled.stdout);
    defer allocator.free(compiled.stderr);
    try std.testing.expect(compiled.term == .exited and compiled.term.exited == 0);
    const request = try capnp.request.parseCodeGeneratorRequest(allocator, compiled.stdout);
    defer capnp.request.freeCodeGeneratorRequest(allocator, request);
    var generator = try capnp.codegen.Generator.init(allocator, request.nodes);
    defer generator.deinit();
    generator.setApiProfile(profile);
    const generated = try generator.generateFile(request.requested_files[0]);
    defer allocator.free(generated);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeFile(tmp.dir, "generated.zig", generated);
    const imported = try generator.generateFile(request.requested_files[1]);
    defer allocator.free(imported);
    try writeFile(tmp.dir, "generic_rpc_external.zig", imported);
    try writeFile(tmp.dir, "endpoint.zig", @embedFile("support/rpc_generic_endpoint.zig"));
    try writeFile(tmp.dir, "driver.cpp", @embedFile("support/rpc_generic_cpp.cpp"));
    const directory = try tmp.dir.realPathFileAlloc(io, ".", allocator);
    defer allocator.free(directory);
    const cpp_output = try std.fmt.allocPrint(allocator, "c++:{s}", .{directory});
    defer allocator.free(cpp_output);
    const cpp_source = try std.fs.path.join(allocator, &.{ directory, "driver.cpp" });
    defer allocator.free(cpp_source);
    const cpp_schema = try std.fs.path.join(allocator, &.{ directory, "generic_rpc.capnp.c++" });
    defer allocator.free(cpp_schema);
    const cpp_import = try std.fs.path.join(allocator, &.{ directory, "generic_rpc_external.capnp.c++" });
    defer allocator.free(cpp_import);
    const driver = try std.fs.path.join(allocator, &.{ directory, "driver" });
    defer allocator.free(driver);
    const endpoint = try std.fs.path.join(allocator, &.{ directory, "endpoint" });
    defer allocator.free(endpoint);
    const endpoint_source = try std.fs.path.join(allocator, &.{ directory, "endpoint.zig" });
    defer allocator.free(endpoint_source);
    const lib = try std.Io.Dir.cwd().realPathFileAlloc(io, "src/lib.zig", allocator);
    defer allocator.free(lib);
    const root_arg = try std.fmt.allocPrint(allocator, "-Mroot={s}", .{endpoint_source});
    defer allocator.free(root_arg);
    const lib_arg = try std.fmt.allocPrint(allocator, "-Mcapnpc-zig={s}", .{lib});
    defer allocator.free(lib_arg);
    const emit_arg = try std.fmt.allocPrint(allocator, "-femit-bin={s}", .{endpoint});
    defer allocator.free(emit_arg);
    allocator.free(try run(&.{ "zig", "build-exe", "-lc", "-ODebug", "--dep", "capnpc-zig", root_arg, "--dep", "capnpc-zig", lib_arg, emit_arg }));
    allocator.free(try run(&.{ "capnp", "compile", "-o", cpp_output, "--src-prefix=tests/test_schemas", "tests/test_schemas/generic_rpc.capnp", "tests/test_schemas/generic_rpc_external.capnp" }));
    const includes = try run(&.{ "pkg-config", "--variable=includedir", "capnp" });
    defer allocator.free(includes);
    const libraries = try run(&.{ "pkg-config", "--variable=libdir", "capnp" });
    defer allocator.free(libraries);
    allocator.free(try run(&.{ "c++", "-std=c++20", "-I", std.mem.trim(u8, includes, " \r\n"), cpp_source, cpp_schema, cpp_import, "-L", std.mem.trim(u8, libraries, " \r\n"), "-lcapnp-rpc", "-lcapnp", "-lkj-async", "-lkj", "-pthread", "-o", driver }));
    const output = try run(&.{ driver, endpoint });
    defer allocator.free(output);
    try std.testing.expect(std.mem.indexOf(u8, output, "C++ <-> Zig generic interfaces") != null);
}
test "C++ and generated Zig invoke concrete interface inheritance and method bindings" {
    if (builtin.os.tag != .linux and builtin.os.tag != .macos) return error.SkipZigTest;
    try runProfile(.full);
    try runProfile(.compact);
}
