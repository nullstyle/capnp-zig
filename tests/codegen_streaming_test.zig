const std = @import("std");
const capnpc = @import("capnpc-zig");
const request_reader = capnpc.request;

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) == null) {
        std.debug.print("=== MISSING ===\n{s}\n=== IN OUTPUT ===\n{s}\n=== END ===\n", .{ needle, haystack });
        return error.MissingExpectedOutput;
    }
}

fn expectNotContains(haystack: []const u8, needle: []const u8) !void {
    if (std.mem.indexOf(u8, haystack, needle) != null) {
        std.debug.print("=== UNEXPECTED ===\n{s}\n=== IN OUTPUT ===\n{s}\n=== END ===\n", .{ needle, haystack });
        return error.UnexpectedOutput;
    }
}

test "Codegen emits streaming method types and handlers" {
    const allocator = std.testing.allocator;

    const argv = &[_][]const u8{
        "capnp",
        "compile",
        "-o-",
        "tests/test_schemas/streaming.capnp",
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

    // Streaming methods should have StreamHandler instead of Handler
    try expectContains(output, "pub const DoStreamI = struct");
    try expectContains(output, "pub const DoStreamJ = struct");
    try expectContains(output, "pub const FinishStream = struct");

    // Streaming methods get is_streaming = true
    try expectContains(output, "pub const is_streaming: bool = true");

    // Non-streaming method gets is_streaming = false
    try expectContains(output, "pub const is_streaming: bool = false");

    // Streaming methods use StreamHandler (no results builder in signature)
    try expectContains(output, "pub const StreamHandler = ");

    // Streaming methods should NOT have Handler or DeferredHandler
    // (check that DoStreamI doesn't have a Handler type)
    // We can check that the streaming method struct has StreamHandler but not Handler
    const doStreamI_start = std.mem.indexOf(u8, output, "pub const DoStreamI = struct") orelse return error.MissingExpectedOutput;
    const doStreamI_end = std.mem.indexOf(u8, output[doStreamI_start..], "pub const DoStreamJ = struct") orelse return error.MissingExpectedOutput;
    const doStreamI_section = output[doStreamI_start .. doStreamI_start + doStreamI_end];

    // StreamHandler present in streaming method
    try expectContains(doStreamI_section, "StreamHandler");
    // No ReturnSender/ReturnContext/DeferredHandler/buildReturn for streaming methods
    try expectNotContains(doStreamI_section, "pub const DeferredHandler");
    try expectNotContains(doStreamI_section, "ReturnSender");
    try expectNotContains(doStreamI_section, "ReturnContext");
    try expectNotContains(doStreamI_section, "buildReturn");

    // sendReturnEmptyStruct for streaming auto-ack
    try expectContains(doStreamI_section, "sendReturnEmptyStruct");

    // Non-streaming method (FinishStream) should have regular Handler and DeferredHandler
    const finishStream_start = std.mem.indexOf(u8, output, "pub const FinishStream = struct") orelse return error.MissingExpectedOutput;
    const finishStream_end_offset = std.mem.indexOf(u8, output[finishStream_start..], "pub const Client = struct") orelse return error.MissingExpectedOutput;
    const finishStream_section = output[finishStream_start .. finishStream_start + finishStream_end_offset];

    try expectContains(finishStream_section, "pub const Handler = ");
    try expectContains(finishStream_section, "pub const DeferredHandler = ");
    try expectContains(finishStream_section, "ReturnSender");
    try expectNotContains(finishStream_section, "StreamHandler");

    // VTable should use StreamHandler for streaming methods
    try expectContains(output, "doStreamI: DoStreamI.StreamHandler");
    try expectContains(output, "doStreamJ: DoStreamJ.StreamHandler");
    // VTable should use Handler for non-streaming methods
    try expectContains(output, "finishStream: FinishStream.Handler");

    // Streaming methods in VTable should NOT have _deferred
    // Find the VTable section
    const vtable_start = std.mem.indexOf(u8, output, "pub const VTable = struct") orelse return error.MissingExpectedOutput;
    const vtable_end = std.mem.indexOf(u8, output[vtable_start..], "};") orelse return error.MissingExpectedOutput;
    const vtable_section = output[vtable_start .. vtable_start + vtable_end];

    try expectNotContains(vtable_section, "doStreamI_deferred");
    try expectNotContains(vtable_section, "doStreamJ_deferred");
    try expectContains(vtable_section, "finishStream_deferred");

    // Interface should still have Client with callXxx methods
    try expectContains(output, "pub const TestStreaming = struct");
    try expectContains(output, "pub fn callDoStreamI");
    try expectContains(output, "pub fn callDoStreamJ");
    try expectContains(output, "pub fn callFinishStream");
}
