const std = @import("std");
const Generator = @import("capnpc-zig/generator.zig").Generator;
const request_reader = @import("request_reader.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Read CodeGeneratorRequest from stdin
    const stdin = std.fs.File.stdin();
    const stdout = std.fs.File.stdout();
    const stderr = std.fs.File.stderr();

    // For now, we'll implement a simple version that just reads and processes
    // In a full implementation, we would parse the Cap'n Proto message

    // Read all data from stdin
    const max_size = 10 * 1024 * 1024; // 10 MB max
    const input_data = stdin.readToEndAlloc(allocator, max_size) catch |err| {
        const msg = std.fmt.allocPrint(allocator, "Error reading stdin: {}\n", .{err}) catch return err;
        defer allocator.free(msg);
        stderr.writeAll(msg) catch {};
        return err;
    };
    defer allocator.free(input_data);

    // Parse the message
    const request = request_reader.parseCodeGeneratorRequest(allocator, input_data) catch |err| {
        const msg1 = std.fmt.allocPrint(allocator, "Error parsing CodeGeneratorRequest: {}\n", .{err}) catch return err;
        defer allocator.free(msg1);
        stderr.writeAll(msg1) catch {};
        return err;
    };
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    // Initialize generator
    var generator = try Generator.init(allocator, request.nodes);
    defer generator.deinit();

    // Generate code for each requested file
    for (request.requested_files) |requested_file| {
        const output_code = try generator.generateFile(requested_file);
        defer allocator.free(output_code);

        // Determine output filename
        const output_filename = try getOutputFilename(allocator, requested_file.filename);
        defer allocator.free(output_filename);

        // Write to file
        const file = try std.fs.cwd().createFile(output_filename, .{});
        defer file.close();

        try file.writeAll(output_code);

        const msg = std.fmt.allocPrint(allocator, "Generated: {s}\n", .{output_filename}) catch continue;
        defer allocator.free(msg);
        stderr.writeAll(msg) catch {};
    }

    stdout.writeAll("Code generation complete.\n") catch {};
}

// Parsing and freeing are handled by request_reader.zig.

/// Get output filename from input filename
fn getOutputFilename(allocator: std.mem.Allocator, input_filename: []const u8) ![]const u8 {
    // Replace .capnp extension with .zig
    if (std.mem.endsWith(u8, input_filename, ".capnp")) {
        const base = input_filename[0 .. input_filename.len - 6];
        return std.fmt.allocPrint(allocator, "{s}.zig", .{base});
    }

    return std.fmt.allocPrint(allocator, "{s}.zig", .{input_filename});
}

test "main tests" {
    @import("std").testing.refAllDecls(@This());
}

test "getOutputFilename" {
    const allocator = std.testing.allocator;

    const result1 = try getOutputFilename(allocator, "test.capnp");
    defer allocator.free(result1);
    try std.testing.expectEqualStrings("test.zig", result1);

    const result2 = try getOutputFilename(allocator, "schema/example.capnp");
    defer allocator.free(result2);
    try std.testing.expectEqualStrings("schema/example.zig", result2);
}
