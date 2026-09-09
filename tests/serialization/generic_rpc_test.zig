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
    const compiled = try capnp_cli.run(allocator, io, &.{ "compile", "-o-", "--src-prefix=tests/test_schemas", schema_path, "tests/test_schemas/generic_rpc_external.capnp" }, .{});
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
    const generated = try generator.generateFile(request.requested_files[0]);
    defer allocator.free(generated);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeFile(tmp.dir, "generated.zig", generated);
    for (request.requested_files[1..]) |file| {
        const source = try generator.generateFile(file);
        defer allocator.free(source);
        const filename = try std.fmt.allocPrint(allocator, "{s}.zig", .{file.filename[0 .. file.filename.len - ".capnp".len]});
        defer allocator.free(filename);
        try writeFile(tmp.dir, filename, source);
    }
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
    try writeFile(tmp.dir, "bad-binding.zig", "const g = @import(\"generated.zig\");\ntest { _ = g.Service.Apply(.{ .T = u32 }).Echo; }\n");
    const negative_path = try tmp.dir.realPathFileAlloc(io, "bad-binding.zig", allocator);
    defer allocator.free(negative_path);
    const negative_arg = try std.fmt.allocPrint(allocator, "-Mroot={s}", .{negative_path});
    defer allocator.free(negative_arg);
    const negative = try std.process.run(allocator, io, .{ .argv = &.{ "zig", "test", "--dep", "capnpc-zig", negative_arg, "--dep", "capnpc-zig", library_arg } });
    defer allocator.free(negative.stdout);
    defer allocator.free(negative.stderr);
    try std.testing.expect(negative.term == .exited and negative.term.exited != 0);
    try std.testing.expect(std.mem.indexOf(u8, negative.stderr, "Apply binding must be a Cap'n Proto pointer codec") != null);
    const wasm_path = try tmp.dir.realPathFileAlloc(io, ".", allocator);
    defer allocator.free(wasm_path);
    const wasm_file = try std.fmt.allocPrint(allocator, "{s}/consumer.wasm", .{wasm_path});
    defer allocator.free(wasm_file);
    const emit = try std.fmt.allocPrint(allocator, "-femit-bin={s}", .{wasm_file});
    defer allocator.free(emit);
    const wasm = try std.process.run(allocator, io, .{ .argv = &.{ "zig", "test", "-target", "wasm32-wasi", "--test-no-exec", "--dep", "capnpc-zig", root_arg, "--dep", "capnpc-zig", library_arg, emit } });
    defer allocator.free(wasm.stdout);
    defer allocator.free(wasm.stderr);
    if (wasm.term != .exited or wasm.term.exited != 0) {
        std.debug.print("WASI compile: {s}\n", .{wasm.stderr});
        return error.WasiCompileFailed;
    }
    const execution = try std.process.run(allocator, io, .{ .argv = &.{ "wasmtime", "run", wasm_file } });
    defer allocator.free(execution.stdout);
    defer allocator.free(execution.stderr);
    if (execution.term != .exited or execution.term.exited != 0) {
        std.debug.print("WASI execution: {s}\n", .{execution.stderr});
        return error.WasiExecutionFailed;
    }
}

test "concrete generic RPC clients and adapters run in full and compact profiles" {
    inline for (.{ capnpc.codegen.Generator.ApiProfile.full, .compact }) |profile| {
        try run("tests/test_schemas/generic_rpc.capnp", @embedFile("support/generic_rpc_consumer.zig"), profile);
    }
}

test "generated application namespaces reject schema names that collide with their helpers" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    for ([_][]const u8{
        "@0xf1c2d3e4f5a68799; interface Example(T) { struct Apply {} }",
        "@0xf1c2d3e4f5a68799; struct Example(T) { struct Apply {} }",
        "@0xf1c2d3e4f5a68799; interface Example { raw @0 () -> (); }",
        "@0xf1c2d3e4f5a68799; interface Example { const dispatchCall :Void = void; }",
        "@0xf1c2d3e4f5a68799; interface Example(T) { echo @0 (value :T) -> (value :T); echoPipelined @1 () -> (); }",
    }) |source| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try writeFile(tmp.dir, "reserved.capnp", source);
        const path = try tmp.dir.realPathFileAlloc(io, "reserved.capnp", allocator);
        defer allocator.free(path);
        const compiled = try capnp_cli.run(allocator, io, &.{ "compile", "-o-", path }, .{});
        defer allocator.free(compiled.stdout);
        defer allocator.free(compiled.stderr);
        if (compiled.term != .exited or compiled.term.exited != 0) {
            std.debug.print("reference rejected collision fixture: {s}\n", .{compiled.stderr});
            return error.SchemaCompileFailed;
        }
        const request = try capnpc.request.parseCodeGeneratorRequest(allocator, compiled.stdout);
        defer capnpc.request.freeCodeGeneratorRequest(allocator, request);
        var generator = try capnpc.codegen.Generator.init(allocator, request.nodes);
        defer generator.deinit();
        if (generator.generateFile(request.requested_files[0])) |generated| {
            allocator.free(generated);
            return error.ExpectedDuplicateGeneratedName;
        } else |err| try std.testing.expectEqual(error.DuplicateGeneratedName, err);
    }
}
