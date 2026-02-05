const std = @import("std");
const capnpc = @import("capnpc-zig");
const request_reader = capnpc.request;

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) == null) {
        return error.MissingExpectedOutput;
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
    try expectContains(output, ".text = \"type\"");
    try expectContains(output, ".text = \"id\"");
    try expectContains(output, ".bool = true");
}
