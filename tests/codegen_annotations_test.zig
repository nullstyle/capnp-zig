const std = @import("std");
const capnpc = @import("capnpc-zig");
const request_reader = capnpc.request;

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) == null) {
        return error.MissingExpectedOutput;
    }
}

fn expectNotContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) != null) {
        return error.UnexpectedOutput;
    }
}

fn writeFile(dir: std.fs.Dir, name: []const u8, data: []const u8) !void {
    var file = try dir.createFile(name, .{});
    defer file.close();
    try file.writeAll(data);
}

fn expectGeneratedOutputParsesAsZig(allocator: std.mem.Allocator, output: []const u8) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeFile(tmp.dir, "generated.zig", output);
    const generated_path = try tmp.dir.realpathAlloc(allocator, "generated.zig");
    defer allocator.free(generated_path);

    const zig_result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = &[_][]const u8{
            "zig",
            "fmt",
            generated_path,
        },
        .max_output_bytes = 10 * 1024 * 1024,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(zig_result.stdout);
    defer allocator.free(zig_result.stderr);

    if (!(zig_result.term == .Exited and zig_result.term.Exited == 0)) {
        std.debug.print("zig fmt stdout:\n{s}\n", .{zig_result.stdout});
        std.debug.print("zig fmt stderr:\n{s}\n", .{zig_result.stderr});
        return error.GeneratedOutputFailedZigParse;
    }
}

test "Codegen annotation uses" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "capnp",
        "compile",
        "-o-",
        "tests/test_schemas/annotations.capnp",
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

    try expectContains(output, "Person_annotations");
    try expectContains(output, "Person_field_annotations");
    try expectContains(output, "Color_enumerant_annotations");
    try expectContains(output, "Service_method_annotations");
    try expectContains(output, "PingParams_field_annotations");
    try expectContains(output, "fromBootstrap");
    try expectContains(output, ".@\"const\" = false");
    try expectContains(output, ".@\"enum\" = true");
    try expectContains(output, ".@\"struct\" = true");
    try expectContains(output, ".@\"union\" = false");
    try expectContains(output, ".text = \"type\"");
    try expectContains(output, ".text = \"id\"");
    try expectContains(output, ".text = \"arg\"");
    try expectContains(output, ".bool = true");
    try expectNotContains(output, "$");

    try expectGeneratedOutputParsesAsZig(allocator, output);
}
