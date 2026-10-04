const std = @import("std");
const capnpc = @import("capnpc-zig");
const request_reader = capnpc.request;
const capnp_cli = @import("support/capnp_cli.zig");
const zig_fmt = @import("support/zig_fmt.zig");

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

test "Codegen annotation uses" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "compile",
        "-o-",
        "tests/test_schemas/annotations.capnp",
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

    try zig_fmt.expectFmtClean(allocator, file.filename, output);
}
