const std = @import("std");
const Generator = @import("capnpc-zig/generator.zig").Generator;
const request_reader = @import("serialization/request_reader.zig");

const RunOptions = struct {
    verbose: bool = false,
    emit_schema_manifest: bool = true,
    api_profile: Generator.ApiProfile = .full,
    shape_sharing: bool = false,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);
    var options = parseRunOptions(argv);
    try applyEnvRunOptions(allocator, &options);

    // Read CodeGeneratorRequest from stdin
    const stdin = std.fs.File.stdin();
    const stderr = std.fs.File.stderr();

    // For now, we'll implement a simple version that just reads and processes
    // In a full implementation, we would parse the Cap'n Proto message

    // Read the full CodeGeneratorRequest from stdin. Keep this effectively
    // unbounded and let allocator/OOM behavior enforce practical limits.
    const max_size = std.math.maxInt(usize);
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
    generator.setEmitSchemaManifest(options.emit_schema_manifest);
    generator.setApiProfile(options.api_profile);
    generator.setShapeSharing(options.shape_sharing);

    // Generate code for each requested file
    for (request.requested_files) |requested_file| {
        const output_code = try generator.generateFile(requested_file);
        defer allocator.free(output_code);

        // Determine output filename
        const output_filename = try getOutputFilename(allocator, requested_file.filename);
        defer allocator.free(output_filename);

        // Write to file (creating parent directories for nested schema paths)
        const file = try createOutputFileInDir(std.fs.cwd(), output_filename);
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
        applyOptionToken(arg_slice, &options);
        var tokens = std.mem.tokenizeAny(u8, arg_slice, ",");
        while (tokens.next()) |token| applyOptionToken(token, &options);
    }
    return options;
}

fn applyEnvRunOptions(allocator: std.mem.Allocator, options: *RunOptions) !void {
    if (try getEnvBoolOption(allocator, "CAPNPC_ZIG_SCHEMA_MANIFEST")) |emit_manifest| {
        options.emit_schema_manifest = emit_manifest;
    }
    if (try getEnvBoolOption(allocator, "CAPNPC_ZIG_NO_MANIFEST")) |no_manifest| {
        if (no_manifest) options.emit_schema_manifest = false;
    }

    if (try getEnvStringOption(allocator, "CAPNPC_ZIG_API_PROFILE")) |profile_value| {
        defer allocator.free(profile_value);
        if (parseApiProfileToken(profile_value)) |profile| {
            options.api_profile = profile;
        }
    }
    if (try getEnvBoolOption(allocator, "CAPNPC_ZIG_COMPACT_API")) |compact_api| {
        options.api_profile = if (compact_api) .compact else .full;
    }
    if (try getEnvBoolOption(allocator, "CAPNPC_ZIG_SHAPE_SHARING")) |shape_sharing| {
        options.shape_sharing = shape_sharing;
    }
}

fn getEnvBoolOption(allocator: std.mem.Allocator, name: []const u8) !?bool {
    const value = try getEnvStringOption(allocator, name) orelse return null;
    defer allocator.free(value);
    return parseBoolToken(value);
}

fn getEnvStringOption(allocator: std.mem.Allocator, name: []const u8) !?[]u8 {
    return std.process.getEnvVarOwned(allocator, name) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => return err,
    };
}

fn parseBoolToken(value: []const u8) ?bool {
    if (value.len == 0) return null;
    if (std.ascii.eqlIgnoreCase(value, "1") or
        std.ascii.eqlIgnoreCase(value, "true") or
        std.ascii.eqlIgnoreCase(value, "on") or
        std.ascii.eqlIgnoreCase(value, "yes"))
    {
        return true;
    }
    if (std.ascii.eqlIgnoreCase(value, "0") or
        std.ascii.eqlIgnoreCase(value, "false") or
        std.ascii.eqlIgnoreCase(value, "off") or
        std.ascii.eqlIgnoreCase(value, "no"))
    {
        return false;
    }
    return null;
}

fn applyOptionToken(token: []const u8, options: *RunOptions) void {
    if (isVerboseOption(token)) {
        options.verbose = true;
    }

    if (parseApiProfileToken(token)) |profile| {
        options.api_profile = profile;
    }
    if (parseShapeSharingToken(token)) |enabled| {
        options.shape_sharing = enabled;
    }

    if (isNoManifestOption(token)) {
        options.emit_schema_manifest = false;
    } else if (isManifestOption(token)) {
        options.emit_schema_manifest = true;
    }
}

fn isVerboseOption(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "--verbose") or
        std.mem.eql(u8, arg, "-v") or
        std.mem.eql(u8, arg, "verbose");
}

fn isNoManifestOption(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "--no-manifest") or
        std.mem.eql(u8, arg, "no-manifest") or
        std.mem.eql(u8, arg, "no_manifest") or
        std.mem.eql(u8, arg, "manifest=0") or
        std.mem.eql(u8, arg, "manifest=false") or
        std.mem.eql(u8, arg, "manifest=off");
}

fn isManifestOption(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "--manifest") or
        std.mem.eql(u8, arg, "manifest") or
        std.mem.eql(u8, arg, "manifest=1") or
        std.mem.eql(u8, arg, "manifest=true") or
        std.mem.eql(u8, arg, "manifest=on");
}

fn parseApiProfileToken(token: []const u8) ?Generator.ApiProfile {
    if (std.ascii.eqlIgnoreCase(token, "compact") or
        std.ascii.eqlIgnoreCase(token, "--api-profile=compact") or
        std.ascii.eqlIgnoreCase(token, "api=compact") or
        std.ascii.eqlIgnoreCase(token, "api_profile=compact") or
        std.ascii.eqlIgnoreCase(token, "profile=compact") or
        std.ascii.eqlIgnoreCase(token, "compact-api") or
        std.ascii.eqlIgnoreCase(token, "compact_api"))
    {
        return .compact;
    }
    if (std.ascii.eqlIgnoreCase(token, "full") or
        std.ascii.eqlIgnoreCase(token, "--api-profile=full") or
        std.ascii.eqlIgnoreCase(token, "api=full") or
        std.ascii.eqlIgnoreCase(token, "api_profile=full") or
        std.ascii.eqlIgnoreCase(token, "profile=full") or
        std.ascii.eqlIgnoreCase(token, "full-api") or
        std.ascii.eqlIgnoreCase(token, "full_api"))
    {
        return .full;
    }
    return null;
}

fn parseShapeSharingToken(token: []const u8) ?bool {
    if (std.ascii.eqlIgnoreCase(token, "shape-sharing") or
        std.ascii.eqlIgnoreCase(token, "shape_sharing") or
        std.ascii.eqlIgnoreCase(token, "share-shapes") or
        std.ascii.eqlIgnoreCase(token, "share_shapes") or
        std.ascii.eqlIgnoreCase(token, "shape=shared") or
        std.ascii.eqlIgnoreCase(token, "shape-sharing=on") or
        std.ascii.eqlIgnoreCase(token, "shape-sharing=true") or
        std.ascii.eqlIgnoreCase(token, "--shape-sharing"))
    {
        return true;
    }
    if (std.ascii.eqlIgnoreCase(token, "shape=inline") or
        std.ascii.eqlIgnoreCase(token, "shape-sharing=off") or
        std.ascii.eqlIgnoreCase(token, "shape-sharing=false") or
        std.ascii.eqlIgnoreCase(token, "--no-shape-sharing"))
    {
        return false;
    }
    return null;
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

fn createOutputFileInDir(dir: std.fs.Dir, output_filename: []const u8) !std.fs.File {
    if (std.fs.path.dirname(output_filename)) |parent_dir| {
        if (parent_dir.len != 0 and !std.mem.eql(u8, parent_dir, ".")) {
            try dir.makePath(parent_dir);
        }
    }
    return dir.createFile(output_filename, .{});
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
    try std.testing.expect(options.emit_schema_manifest);
    try std.testing.expectEqual(Generator.ApiProfile.full, options.api_profile);
    try std.testing.expect(!options.shape_sharing);
}

test "parseRunOptions enables verbose for --verbose" {
    const argv = [_][]const u8{ "capnpc-zig", "--verbose" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expect(options.verbose);
    try std.testing.expect(options.emit_schema_manifest);
    try std.testing.expectEqual(Generator.ApiProfile.full, options.api_profile);
    try std.testing.expect(!options.shape_sharing);
}

test "parseRunOptions enables verbose for capnp style token" {
    const argv = [_][]const u8{ "capnpc-zig", "out,verbose,foo" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expect(options.verbose);
    try std.testing.expect(options.emit_schema_manifest);
    try std.testing.expectEqual(Generator.ApiProfile.full, options.api_profile);
    try std.testing.expect(!options.shape_sharing);
}

test "parseRunOptions disables schema manifest via direct flag" {
    const argv = [_][]const u8{ "capnpc-zig", "--no-manifest" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expect(!options.emit_schema_manifest);
    try std.testing.expectEqual(Generator.ApiProfile.full, options.api_profile);
    try std.testing.expect(!options.shape_sharing);
}

test "parseRunOptions disables schema manifest via capnp style token" {
    const argv = [_][]const u8{ "capnpc-zig", "out,no_manifest,foo" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expect(!options.emit_schema_manifest);
    try std.testing.expectEqual(Generator.ApiProfile.full, options.api_profile);
    try std.testing.expect(!options.shape_sharing);
}

test "parseRunOptions allows explicit manifest re-enable" {
    const argv = [_][]const u8{ "capnpc-zig", "out,no-manifest,manifest=on" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expect(options.emit_schema_manifest);
    try std.testing.expectEqual(Generator.ApiProfile.full, options.api_profile);
    try std.testing.expect(!options.shape_sharing);
}

test "parseRunOptions enables compact api profile" {
    const argv = [_][]const u8{ "capnpc-zig", "out,compact-api,foo" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expectEqual(Generator.ApiProfile.compact, options.api_profile);
    try std.testing.expect(!options.shape_sharing);
}

test "parseRunOptions allows explicit full api profile" {
    const argv = [_][]const u8{ "capnpc-zig", "out,compact-api,profile=full" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expectEqual(Generator.ApiProfile.full, options.api_profile);
    try std.testing.expect(!options.shape_sharing);
}

test "parseRunOptions enables shape sharing" {
    const argv = [_][]const u8{ "capnpc-zig", "out,shape-sharing,foo" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expect(options.shape_sharing);
}

test "parseRunOptions disables shape sharing explicitly" {
    const argv = [_][]const u8{ "capnpc-zig", "out,shape-sharing,--no-shape-sharing" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expect(!options.shape_sharing);
}

test "parseBoolToken accepts common true values" {
    try std.testing.expectEqual(@as(?bool, true), parseBoolToken("1"));
    try std.testing.expectEqual(@as(?bool, true), parseBoolToken("true"));
    try std.testing.expectEqual(@as(?bool, true), parseBoolToken("ON"));
    try std.testing.expectEqual(@as(?bool, true), parseBoolToken("yes"));
}

test "parseBoolToken accepts common false values" {
    try std.testing.expectEqual(@as(?bool, false), parseBoolToken("0"));
    try std.testing.expectEqual(@as(?bool, false), parseBoolToken("false"));
    try std.testing.expectEqual(@as(?bool, false), parseBoolToken("Off"));
    try std.testing.expectEqual(@as(?bool, false), parseBoolToken("NO"));
}

test "parseBoolToken rejects unknown values" {
    try std.testing.expectEqual(@as(?bool, null), parseBoolToken(""));
    try std.testing.expectEqual(@as(?bool, null), parseBoolToken("maybe"));
}

test "parseApiProfileToken parses supported values" {
    try std.testing.expectEqual(@as(?Generator.ApiProfile, .compact), parseApiProfileToken("compact"));
    try std.testing.expectEqual(@as(?Generator.ApiProfile, .compact), parseApiProfileToken("compact-api"));
    try std.testing.expectEqual(@as(?Generator.ApiProfile, .compact), parseApiProfileToken("API=COMPACT"));
    try std.testing.expectEqual(@as(?Generator.ApiProfile, .full), parseApiProfileToken("full"));
    try std.testing.expectEqual(@as(?Generator.ApiProfile, .full), parseApiProfileToken("profile=full"));
    try std.testing.expectEqual(@as(?Generator.ApiProfile, .full), parseApiProfileToken("--api-profile=full"));
    try std.testing.expectEqual(@as(?Generator.ApiProfile, null), parseApiProfileToken("profile=other"));
}

test "parseShapeSharingToken parses supported values" {
    try std.testing.expectEqual(@as(?bool, true), parseShapeSharingToken("shape-sharing"));
    try std.testing.expectEqual(@as(?bool, true), parseShapeSharingToken("SHARE_SHAPES"));
    try std.testing.expectEqual(@as(?bool, true), parseShapeSharingToken("shape=shared"));
    try std.testing.expectEqual(@as(?bool, false), parseShapeSharingToken("--no-shape-sharing"));
    try std.testing.expectEqual(@as(?bool, false), parseShapeSharingToken("shape=inline"));
    try std.testing.expectEqual(@as(?bool, null), parseShapeSharingToken("shape=unknown"));
}

test "createOutputFileInDir creates parent directories for nested output paths" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try createOutputFileInDir(tmp.dir, "capnp/persistent.zig");
    defer file.close();
    try file.writeAll("// generated\n");

    var reopened = try tmp.dir.openFile("capnp/persistent.zig", .{});
    defer reopened.close();
}
