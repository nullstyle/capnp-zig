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

test "Codegen defaults and constants" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "capnp",
        "compile",
        "-o-",
        "tests/test_schemas/defaults.capnp",
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

    try expectContains(output, "pub const magicNumber");
    try expectContains(output, "987");
    try expectContains(output, "pub const magicData");
    try expectContains(output, "0x0A");
    try expectContains(output, "pub const magicList = struct");
    try expectContains(output, "pub const magicInner = struct");
    try expectContains(output, "isPointerNull(2)");
    try expectContains(output, "\"widget\"");
    try expectContains(output, "_default_nums_bytes");
    try expectContains(output, "_default_inner_bytes");
    try expectContains(output, "segments_owned = false");
    try expectContains(output, "value != true");
    try expectContains(output, "const stored = @as(u32, @bitCast(value)) ^ @as(u32, 123);");
    try expectContains(output, "const stored = raw ^ @as(u16, 1);");
    try expectNotContains(output, "const rpc = capnpc.rpc;");
}
