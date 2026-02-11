const std = @import("std");
const Generator = @import("capnpc-zig/generator.zig").Generator;
const request_reader = @import("serialization/request_reader.zig");

const RunOptions = struct {
    verbose: bool = false,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);
    const options = parseRunOptions(argv);

    // Read CodeGeneratorRequest from stdin
    const stdin = std.fs.File.stdin();
    const stderr = std.fs.File.stderr();

    // For now, we'll implement a simple version that just reads and processes
    // In a full implementation, we would parse the Cap'n Proto message

    // Read all data from stdin
    const max_size = 10 * 1024 * 1024; // 10 MB max
    const input_data = stdin.readToEndAlloc(allocator, max_size) catch |err| {
        logStderr(stderr, "Error reading stdin: {}\n", .{err});
        return err;
    };
    defer allocator.free(input_data);

    // Parse the message
    const request = request_reader.parseCodeGeneratorRequest(allocator, input_data) catch |err| {
        logStderr(stderr, "Error parsing CodeGeneratorRequest: {}\n", .{err});
        return err;
    };
    defer request_reader.freeCodeGeneratorRequest(allocator, request);

    // Initialize generator
    var generator = try Generator.init(allocator, request.nodes);
    defer generator.deinit();
    generator.setVerbose(options.verbose);

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

        if (options.verbose) {
            logStderr(stderr, "Generated: {s}\n", .{output_filename});
        }
    }
    if (options.verbose) {
        logStderr(stderr, "Code generation complete.\n", .{});
    }
}

/// Best-effort diagnostic output to stderr using a stack buffer.
fn logStderr(stderr: std.fs.File, comptime fmt: []const u8, args: anytype) void {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    std.fmt.format(fbs.writer(), fmt, args) catch {};
    stderr.writeAll(fbs.getWritten()) catch {};
}

fn parseRunOptions(argv: anytype) RunOptions {
    var options = RunOptions{};
    if (argv.len <= 1) return options;

    for (argv[1..]) |arg| {
        const arg_slice: []const u8 = arg;
        if (isVerboseOption(arg_slice)) {
            options.verbose = true;
            continue;
        }
        var tokens = std.mem.tokenizeAny(u8, arg_slice, ",");
        while (tokens.next()) |token| {
            if (isVerboseOption(token)) {
                options.verbose = true;
                break;
            }
        }
    }
    return options;
}

fn isVerboseOption(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "--verbose") or
        std.mem.eql(u8, arg, "-v") or
        std.mem.eql(u8, arg, "verbose");
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

test "parseRunOptions defaults to quiet" {
    const argv = [_][]const u8{"capnpc-zig"};
    const options = parseRunOptions(argv[0..]);
    try std.testing.expect(!options.verbose);
}

test "parseRunOptions enables verbose for --verbose" {
    const argv = [_][]const u8{ "capnpc-zig", "--verbose" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expect(options.verbose);
}

test "parseRunOptions enables verbose for capnp style token" {
    const argv = [_][]const u8{ "capnpc-zig", "out,verbose,foo" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expect(options.verbose);
}
