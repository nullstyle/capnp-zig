const std = @import("std");
const builtin = @import("builtin");
const Generator = @import("capnpc-zig/generator.zig").Generator;
const request_reader = @import("serialization/request_reader.zig");

const max_code_generator_request_bytes: usize = 64 * 1024 * 1024;

const RunOptions = struct {
    verbose: bool = false,
    emit_schema_manifest: bool = true,
    api_profile: Generator.ApiProfile = .full,
    shape_sharing: bool = false,
    codegen_budget: Generator.CodegenBudget = .{},
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var options = try parseRunOptionsFromArgs(allocator, init.minimal.args);
    applyEnvRunOptions(init.environ_map, &options);

    // Read CodeGeneratorRequest from stdin
    const stdin = std.Io.File.stdin();
    var read_buf: [65536]u8 = undefined;
    var reader = stdin.reader(io, &read_buf);
    reader.mode = .streaming;
    const input_data = readCodeGeneratorRequestInput(allocator, &reader.interface) catch |err| {
        logStderr("Error reading stdin: {}\n", .{err});
        return err;
    };
    defer allocator.free(input_data);

    // Parse the message
    const request = request_reader.parseCodeGeneratorRequest(allocator, input_data) catch |err| {
        logStderr("Error parsing CodeGeneratorRequest: {}\n", .{err});
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
    generator.setCodegenBudget(options.codegen_budget);

    // Generate code for each requested file
    for (request.requested_files) |requested_file| {
        const output_code = try generator.generateFile(requested_file);
        defer allocator.free(output_code);

        // Determine output filename
        const output_filename = try getOutputFilename(allocator, requested_file.filename);
        defer allocator.free(output_filename);

        // Write to file (creating parent directories for nested schema paths)
        const file = try createOutputFileInDir(std.Io.Dir.cwd(), io, output_filename);
        defer file.close(io);

        try file.writeStreamingAll(io, output_code);

        if (options.verbose) {
            logStderr("Generated: {s}\n", .{output_filename});
        }
    }
    if (options.verbose) {
        logStderr("Code generation complete.\n", .{});
    }
}

/// Best-effort diagnostic output to stderr using a stack buffer.
fn logStderr(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

fn parseRunOptionsFromArgs(allocator: std.mem.Allocator, args: std.process.Args) !RunOptions {
    var options = RunOptions{};
    // initAllocator is the cross-platform form; plain init is a compile
    // error on Windows, where plugin CLI options used to be silently
    // ignored. All options parse to bools/enums, so nothing outlives
    // the iterator.
    var iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer iter.deinit();
    _ = iter.skip(); // skip program name
    while (iter.next()) |arg| {
        applyOptionToken(arg, &options);
        var tokens = std.mem.tokenizeAny(u8, arg, ",");
        while (tokens.next()) |token| applyOptionToken(token, &options);
    }
    return options;
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

fn applyEnvRunOptions(environ_map: *const std.process.Environ.Map, options: *RunOptions) void {
    if (getEnvBoolOption(environ_map, "CAPNPC_ZIG_SCHEMA_MANIFEST")) |emit_manifest| {
        options.emit_schema_manifest = emit_manifest;
    }
    if (getEnvBoolOption(environ_map, "CAPNPC_ZIG_NO_MANIFEST")) |no_manifest| {
        if (no_manifest) options.emit_schema_manifest = false;
    }

    if (environ_map.get("CAPNPC_ZIG_API_PROFILE")) |profile_value| {
        if (parseApiProfileToken(profile_value)) |profile| {
            options.api_profile = profile;
        }
    }
    if (getEnvBoolOption(environ_map, "CAPNPC_ZIG_COMPACT_API")) |compact_api| {
        options.api_profile = if (compact_api) .compact else .full;
    }
    if (getEnvBoolOption(environ_map, "CAPNPC_ZIG_SHAPE_SHARING")) |shape_sharing| {
        options.shape_sharing = shape_sharing;
    }
    applyEnvBudgetOptions(environ_map, &options.codegen_budget);
}

fn getEnvBoolOption(environ_map: *const std.process.Environ.Map, name: []const u8) ?bool {
    const value = environ_map.get(name) orelse return null;
    return parseBoolToken(value);
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
    applyBudgetOptionToken(token, &options.codegen_budget);
}

fn applyEnvBudgetOptions(environ_map: *const std.process.Environ.Map, budget: *Generator.CodegenBudget) void {
    if (getEnvUsizeOption(environ_map, "CAPNPC_ZIG_MAX_CODEGEN_NODES")) |value| budget.max_nodes = value;
    if (getEnvUsizeOption(environ_map, "CAPNPC_ZIG_MAX_CODEGEN_IMPORTS")) |value| budget.max_imports = value;
    if (getEnvUsizeOption(environ_map, "CAPNPC_ZIG_MAX_CODEGEN_FIELDS")) |value| budget.max_fields = value;
    if (getEnvUsizeOption(environ_map, "CAPNPC_ZIG_MAX_CODEGEN_NAME_BYTES")) |value| budget.max_name_bytes = value;
    if (getEnvUsizeOption(environ_map, "CAPNPC_ZIG_MAX_CODEGEN_DEFAULT_BYTES")) |value| budget.max_default_bytes = value;
    if (getEnvUsizeOption(environ_map, "CAPNPC_ZIG_MAX_SCHEMA_MANIFEST_BYTES")) |value| budget.max_manifest_bytes = value;
    if (getEnvUsizeOption(environ_map, "CAPNPC_ZIG_MAX_CODEGEN_OUTPUT_BYTES")) |value| budget.max_output_bytes = value;
    if (getEnvUsizeOption(environ_map, "CAPNPC_ZIG_MAX_CODEGEN_BRAND_SPECIALIZATIONS")) |value| budget.max_brand_specializations = value;
}

fn applyBudgetOptionToken(token: []const u8, budget: *Generator.CodegenBudget) void {
    if (parseUsizeAssignment(token, "max-codegen-nodes=")) |value| budget.max_nodes = value;
    if (parseUsizeAssignment(token, "max-codegen-imports=")) |value| budget.max_imports = value;
    if (parseUsizeAssignment(token, "max-codegen-fields=")) |value| budget.max_fields = value;
    if (parseUsizeAssignment(token, "max-codegen-name-bytes=")) |value| budget.max_name_bytes = value;
    if (parseUsizeAssignment(token, "max-codegen-default-bytes=")) |value| budget.max_default_bytes = value;
    if (parseUsizeAssignment(token, "max-schema-manifest-bytes=")) |value| budget.max_manifest_bytes = value;
    if (parseUsizeAssignment(token, "max-codegen-output-bytes=")) |value| budget.max_output_bytes = value;
    if (parseUsizeAssignment(token, "max-codegen-brand-specializations=")) |value| budget.max_brand_specializations = value;
    if (parseUsizeAssignment(token, "max-output-bytes=")) |value| budget.max_output_bytes = value;
}

fn getEnvUsizeOption(environ_map: *const std.process.Environ.Map, name: []const u8) ?usize {
    const value = environ_map.get(name) orelse return null;
    return parseUsizeToken(value);
}

fn parseUsizeAssignment(token: []const u8, prefix: []const u8) ?usize {
    if (!std.mem.startsWith(u8, token, prefix)) return null;
    return parseUsizeToken(token[prefix.len..]);
}

fn parseUsizeToken(value: []const u8) ?usize {
    if (value.len == 0) return null;
    return std.fmt.parseUnsigned(usize, value, 10) catch null;
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

fn readCodeGeneratorRequestInput(allocator: std.mem.Allocator, reader: anytype) ![]u8 {
    return readCodeGeneratorRequestInputWithLimit(allocator, reader, max_code_generator_request_bytes);
}

fn readCodeGeneratorRequestInputWithLimit(allocator: std.mem.Allocator, reader: anytype, limit: usize) ![]u8 {
    return reader.allocRemaining(allocator, .limited(limit));
}

fn validateRelativeSchemaPath(path: []const u8) !void {
    if (path.len == 0) return error.InvalidSchemaPath;
    if (std.mem.indexOfScalar(u8, path, '\\') != null) return error.InvalidSchemaPath;
    if (path[0] == '/') return error.InvalidSchemaPath;
    if (hasWindowsDriveRoot(path)) return error.InvalidSchemaPath;

    var component_start: usize = 0;
    for (path, 0..) |c, i| {
        if (c != '/') continue;
        try validateSchemaPathComponent(path[component_start..i]);
        component_start = i + 1;
    }
    try validateSchemaPathComponent(path[component_start..]);
}

fn hasWindowsDriveRoot(path: []const u8) bool {
    return path.len >= 3 and std.ascii.isAlphabetic(path[0]) and path[1] == ':' and path[2] == '/';
}

fn validateSchemaPathComponent(component: []const u8) !void {
    if (component.len == 0) return error.InvalidSchemaPath;
    if (std.mem.eql(u8, component, ".") or std.mem.eql(u8, component, "..")) return error.InvalidSchemaPath;
}

/// Get output filename from input filename
fn getOutputFilename(allocator: std.mem.Allocator, input_filename: []const u8) ![]const u8 {
    try validateRelativeSchemaPath(input_filename);

    // Replace .capnp extension with .zig
    if (std.mem.endsWith(u8, input_filename, ".capnp")) {
        const base = input_filename[0 .. input_filename.len - 6];
        return std.fmt.allocPrint(allocator, "{s}.zig", .{base});
    }

    return std.fmt.allocPrint(allocator, "{s}.zig", .{input_filename});
}

const OutputParentDir = struct {
    dir: std.Io.Dir,
    owns_dir: bool,

    fn deinit(self: OutputParentDir, io: std.Io) void {
        if (self.owns_dir) self.dir.close(io);
    }
};

fn createOutputFileInDir(dir: std.Io.Dir, io: std.Io, output_filename: []const u8) !std.Io.File {
    try validateRelativeSchemaPath(output_filename);

    const parent = try createOutputParentDirNoFollow(dir, io, std.fs.path.dirname(output_filename));
    defer parent.deinit(io);

    const basename = std.fs.path.basename(output_filename);
    try rejectSymlinkComponent(parent.dir, io, basename);
    return parent.dir.createFile(io, basename, .{ .resolve_beneath = true });
}

fn createOutputParentDirNoFollow(
    dir: std.Io.Dir,
    io: std.Io,
    parent_path: ?[]const u8,
) !OutputParentDir {
    const path = parent_path orelse return .{ .dir = dir, .owns_dir = false };
    if (path.len == 0 or std.mem.eql(u8, path, ".")) return .{ .dir = dir, .owns_dir = false };

    var current = dir;
    var owns_current = false;
    errdefer if (owns_current) current.close(io);

    var components = std.mem.splitScalar(u8, path, '/');
    while (components.next()) |component| {
        current.createDir(io, component, .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => |e| return e,
        };
        try rejectSymlinkComponent(current, io, component);

        const next = current.openDir(io, component, .{
            .access_sub_paths = true,
            .follow_symlinks = false,
        }) catch |err| switch (err) {
            error.NotDir => return error.NotDir,
            else => |e| return e,
        };

        if (owns_current) current.close(io);
        current = next;
        owns_current = true;
    }

    return .{ .dir = current, .owns_dir = owns_current };
}

fn rejectSymlinkComponent(dir: std.Io.Dir, io: std.Io, component: []const u8) !void {
    var target_buf: [std.Io.Dir.max_path_bytes]u8 = undefined;
    _ = dir.readLink(io, component, &target_buf) catch |err| switch (err) {
        error.NotLink, error.FileNotFound => return,
        else => |e| return e,
    };
    return error.OutputPathSymlink;
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

test "getOutputFilename rejects unsafe output paths" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(error.InvalidSchemaPath, getOutputFilename(allocator, ""));
    try std.testing.expectError(error.InvalidSchemaPath, getOutputFilename(allocator, "/tmp/schema.capnp"));
    try std.testing.expectError(error.InvalidSchemaPath, getOutputFilename(allocator, "C:/tmp/schema.capnp"));
    try std.testing.expectError(error.InvalidSchemaPath, getOutputFilename(allocator, "schema//example.capnp"));
    try std.testing.expectError(error.InvalidSchemaPath, getOutputFilename(allocator, "schema/../example.capnp"));
    try std.testing.expectError(error.InvalidSchemaPath, getOutputFilename(allocator, "schema/./example.capnp"));
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

test "parseRunOptions applies codegen budget tokens" {
    const argv = [_][]const u8{ "capnpc-zig", "out,max-codegen-output-bytes=4096,max-codegen-fields=12,max-codegen-brand-specializations=27" };
    const options = parseRunOptions(argv[0..]);
    try std.testing.expectEqual(@as(usize, 4096), options.codegen_budget.max_output_bytes);
    try std.testing.expectEqual(@as(usize, 12), options.codegen_budget.max_fields);
    try std.testing.expectEqual(@as(usize, 27), options.codegen_budget.max_brand_specializations);
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
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try createOutputFileInDir(tmp.dir, io, "capnp/persistent.zig");
    defer file.close(io);
    try file.writeStreamingAll(io, "// generated\n");

    var reopened = try tmp.dir.openFile(io, "capnp/persistent.zig", .{});
    defer reopened.close(io);
}

test "createOutputFileInDir rejects traversal output paths" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expectError(error.InvalidSchemaPath, createOutputFileInDir(tmp.dir, io, "../escape.zig"));
}

test "createOutputFileInDir rejects symlink parent components" {
    if (builtin.target.os.tag == .windows) return error.SkipZigTest;

    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDir(io, "target", .default_dir);
    tmp.dir.symLink(io, "target", "capnp", .{ .is_directory = true }) catch |err| switch (err) {
        error.AccessDenied, error.PermissionDenied => return error.SkipZigTest,
        else => |e| return e,
    };

    try std.testing.expectError(error.OutputPathSymlink, createOutputFileInDir(tmp.dir, io, "capnp/generated.zig"));
}

test "createOutputFileInDir rejects symlink final output" {
    if (builtin.target.os.tag == .windows) return error.SkipZigTest;

    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDir(io, "capnp", .default_dir);
    tmp.dir.symLink(io, "../escape.zig", "capnp/generated.zig", .{}) catch |err| switch (err) {
        error.AccessDenied, error.PermissionDenied => return error.SkipZigTest,
        else => |e| return e,
    };

    try std.testing.expectError(error.OutputPathSymlink, createOutputFileInDir(tmp.dir, io, "capnp/generated.zig"));
}

test "readCodeGeneratorRequestInput enforces size limit" {
    var reader: std.Io.Reader = .fixed("abcd");
    try std.testing.expectError(
        error.StreamTooLong,
        readCodeGeneratorRequestInputWithLimit(std.testing.allocator, &reader, 3),
    );
}
