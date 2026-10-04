const std = @import("std");
const builtin = @import("builtin");
const Generator = @import("capnpc-zig/generator.zig").Generator;
const request_reader = @import("serialization/request_reader.zig");

const max_code_generator_request_bytes: usize = 64 * 1024 * 1024;

/// Writes generated files under a directory instead of the current one. This
/// lets a consumer's `build.zig` run the pinned plugin as a cached step whose
/// output is a LazyPath:
/// `run.addPrefixedOutputDirectoryArg("--output-dir=", "capnp-gen")`.
/// `capnp compile -o<plugin>:<dir>` passes no arguments and changes into
/// `<dir>` itself, so that contract is unchanged.
///
/// With this flag the plugin also ignores the `CAPNPC_ZIG_*` environment
/// options (see `applyEnvironment`).
const output_dir_option = "--output-dir=";

const RunOptions = struct {
    verbose: bool = false,
    emit_schema_manifest: bool = true,
    emit_reflection: bool = true,
    api_profile: Generator.ApiProfile = .full,
    shape_sharing: bool = false,
    codegen_budget: Generator.CodegenBudget = .{},
    /// Root for generated files; null means the current directory.
    output_dir: ?[]const u8 = null,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var options = try parseRunOptionsFromArgs(init.arena.allocator(), init.minimal.args);
    if (applyEnvironment(init.environ_map, &options)) |ignored| {
        logStderr(
            "capnpc-zig: ignoring {s}: environment options do not apply with --output-dir=; pass the option as an argument\n",
            .{ignored},
        );
    }

    // Open (creating if needed) the output root before reading stdin, so a
    // bad `--output-dir=` fails before any work is done.
    var owned_output_root: ?std.Io.Dir = null;
    defer if (owned_output_root) |dir| dir.close(io);
    if (options.output_dir) |path| {
        owned_output_root = openOutputRoot(std.Io.Dir.cwd(), io, path) catch |err| {
            logStderr("Error opening output directory '{s}': {}\n", .{ path, err });
            return err;
        };
    }
    const output_root = owned_output_root orelse std.Io.Dir.cwd();

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
    if (options.emit_reflection) try generator.setSchemaRequest(input_data);
    generator.setEmitReflection(options.emit_reflection);
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
        const file = try createOutputFileInDir(output_root, io, output_filename);
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

/// `allocator` must outlive the returned options: `output_dir` is copied into
/// it, because the argument iterator's storage is freed on return.
fn parseRunOptionsFromArgs(allocator: std.mem.Allocator, args: std.process.Args) !RunOptions {
    var options = RunOptions{};
    // initAllocator is the cross-platform form; plain init is a compile
    // error on Windows, where plugin CLI options used to be silently
    // ignored.
    var iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer iter.deinit();
    _ = iter.skip(); // skip program name
    while (iter.next()) |arg| {
        applyArgument(arg, &options);
        if (std.mem.startsWith(u8, arg, output_dir_option)) {
            options.output_dir = try allocator.dupe(u8, arg[output_dir_option.len..]);
        }
    }
    return options;
}

fn parseRunOptions(argv: anytype) RunOptions {
    var options = RunOptions{};
    if (argv.len <= 1) return options;

    for (argv[1..]) |arg| applyArgument(arg, &options);
    return options;
}

fn applyArgument(arg: []const u8, options: *RunOptions) void {
    if (std.mem.startsWith(u8, arg, output_dir_option)) {
        // A path may contain commas; never split it into option tokens.
        options.output_dir = arg[output_dir_option.len..];
        return;
    }
    applyOptionToken(arg, options);
    var tokens = std.mem.tokenizeAny(u8, arg, ",");
    while (tokens.next()) |token| applyOptionToken(token, options);
}

/// Open the `--output-dir=` root, creating it and its parents if missing.
/// The path is relative to `base`, or absolute.
fn openOutputRoot(base: std.Io.Dir, io: std.Io, path: []const u8) !std.Io.Dir {
    if (path.len == 0) return error.InvalidOutputDir;
    return base.createDirPathOpen(io, path, .{});
}

/// Every environment variable `applyEnvRunOptions` reads.
const env_option_names = [_][]const u8{
    "CAPNPC_ZIG_SCHEMA_MANIFEST",
    "CAPNPC_ZIG_NO_MANIFEST",
    "CAPNPC_ZIG_API_PROFILE",
    "CAPNPC_ZIG_COMPACT_API",
    "CAPNPC_ZIG_SHAPE_SHARING",
    "CAPNPC_ZIG_MAX_CODEGEN_NODES",
    "CAPNPC_ZIG_MAX_CODEGEN_IMPORTS",
    "CAPNPC_ZIG_MAX_CODEGEN_FIELDS",
    "CAPNPC_ZIG_MAX_CODEGEN_NAME_BYTES",
    "CAPNPC_ZIG_MAX_CODEGEN_DEFAULT_BYTES",
    "CAPNPC_ZIG_MAX_SCHEMA_MANIFEST_BYTES",
    "CAPNPC_ZIG_MAX_CODEGEN_OUTPUT_BYTES",
    "CAPNPC_ZIG_MAX_CODEGEN_BRAND_SPECIALIZATIONS",
};

/// Applies the `CAPNPC_ZIG_*` environment options, except in build-step mode
/// (`--output-dir=`), where it applies none of them and returns the name of
/// one that is set, for a diagnostic.
///
/// A Zig build Run step inherits the shell's environment, but Zig hashes
/// only the arguments, stdin and declared inputs into the step's cache key,
/// not the inherited environment. An exported `CAPNPC_ZIG_SHAPE_SHARING=1`
/// would change the generated code but not the key: a cached build would
/// serve whichever output it made first, and another machine would generate
/// different code from the same build.zig. Arguments are hashed, and every
/// option has an argument form, so arguments are the only way to set one
/// from a build step. `capnp compile -ozig:<dir>` passes no arguments, so the
/// environment is still how that path takes options.
fn applyEnvironment(environ_map: *const std.process.Environ.Map, options: *RunOptions) ?[]const u8 {
    if (options.output_dir == null) {
        applyEnvRunOptions(environ_map, options);
        return null;
    }
    for (env_option_names) |name| {
        if (environ_map.get(name) != null) return name;
    }
    return null;
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
    if (std.mem.eql(u8, token, "--no-reflection") or std.mem.eql(u8, token, "no-reflection")) {
        options.emit_reflection = false;
    } else if (std.mem.eql(u8, token, "--reflection") or std.mem.eql(u8, token, "reflection")) {
        options.emit_reflection = true;
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

test "reflection metadata encoding" {
    _ = @import("capnpc-zig/reflection_metadata.zig");
    _ = @import("capnpc-zig/generator.zig");
}

test "parseRunOptions controls reflection independently of the JSON manifest" {
    const defaults = parseRunOptions(@as([]const []const u8, &.{"capnpc-zig"}));
    try std.testing.expect(defaults.emit_reflection);
    const no_manifest = parseRunOptions(@as([]const []const u8, &.{ "capnpc-zig", "--no-manifest" }));
    try std.testing.expect(no_manifest.emit_reflection);
    try std.testing.expect(!no_manifest.emit_schema_manifest);
    const no_reflection = parseRunOptions(@as([]const []const u8, &.{ "capnpc-zig", "--no-reflection" }));
    try std.testing.expect(!no_reflection.emit_reflection);
    try std.testing.expect(no_reflection.emit_schema_manifest);
    const reenabled = parseRunOptions(@as([]const []const u8, &.{ "capnpc-zig", "no-reflection,reflection" }));
    try std.testing.expect(reenabled.emit_reflection);
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

test "parseRunOptions writes to the current directory unless --output-dir= is given" {
    const defaults = parseRunOptions(@as([]const []const u8, &.{"capnpc-zig"}));
    try std.testing.expectEqual(@as(?[]const u8, null), defaults.output_dir);

    const options = parseRunOptions(@as([]const []const u8, &.{ "capnpc-zig", "--output-dir=zig-cache/o/abc/capnp-gen" }));
    try std.testing.expectEqualStrings("zig-cache/o/abc/capnp-gen", options.output_dir.?);
}

test "parseRunOptions never splits an --output-dir= path into option tokens" {
    // A directory may contain commas. Tokenizing it would turn a path segment
    // into an option (here `verbose` and `no-reflection`).
    const options = parseRunOptions(@as([]const []const u8, &.{ "capnpc-zig", "--output-dir=out,verbose,no-reflection" }));
    try std.testing.expectEqualStrings("out,verbose,no-reflection", options.output_dir.?);
    try std.testing.expect(!options.verbose);
    try std.testing.expect(options.emit_reflection);
}

/// An environment that sets every option away from its default.
fn nonDefaultOptionEnvironment(allocator: std.mem.Allocator) !std.process.Environ.Map {
    var env: std.process.Environ.Map = .init(allocator);
    errdefer env.deinit();
    try env.put("CAPNPC_ZIG_NO_MANIFEST", "1");
    try env.put("CAPNPC_ZIG_API_PROFILE", "compact");
    try env.put("CAPNPC_ZIG_SHAPE_SHARING", "1");
    try env.put("CAPNPC_ZIG_MAX_CODEGEN_FIELDS", "3");
    try env.put("CAPNPC_ZIG_MAX_CODEGEN_BRAND_SPECIALIZATIONS", "5");
    return env;
}

test "applyEnvironment applies CAPNPC_ZIG_* options without --output-dir=" {
    var env = try nonDefaultOptionEnvironment(std.testing.allocator);
    defer env.deinit();

    // `capnp compile -ozig:<dir>` passes no arguments: the environment is how
    // that path takes options.
    var options = parseRunOptions(@as([]const []const u8, &.{"capnpc-zig"}));
    try std.testing.expectEqual(@as(?[]const u8, null), applyEnvironment(&env, &options));
    try std.testing.expect(!options.emit_schema_manifest);
    try std.testing.expectEqual(Generator.ApiProfile.compact, options.api_profile);
    try std.testing.expect(options.shape_sharing);
    try std.testing.expectEqual(@as(usize, 3), options.codegen_budget.max_fields);
    try std.testing.expectEqual(@as(usize, 5), options.codegen_budget.max_brand_specializations);
}

test "applyEnvironment ignores CAPNPC_ZIG_* options in --output-dir= build-step mode" {
    var env = try nonDefaultOptionEnvironment(std.testing.allocator);
    defer env.deinit();

    // A build Run step inherits this environment but does not hash it into
    // its cache key, so honoring it would let a shell variable change cached
    // generated code. The output must depend on the arguments alone.
    var options = parseRunOptions(@as([]const []const u8, &.{ "capnpc-zig", "--output-dir=capnp-gen" }));
    const ignored = applyEnvironment(&env, &options) orelse return error.TestExpectedIgnoredOption;
    try std.testing.expect(std.mem.startsWith(u8, ignored, "CAPNPC_ZIG_"));
    const defaults = parseRunOptions(@as([]const []const u8, &.{ "capnpc-zig", "--output-dir=capnp-gen" }));
    try std.testing.expectEqual(defaults.emit_schema_manifest, options.emit_schema_manifest);
    try std.testing.expectEqual(defaults.api_profile, options.api_profile);
    try std.testing.expectEqual(defaults.shape_sharing, options.shape_sharing);
    try std.testing.expectEqual(defaults.codegen_budget, options.codegen_budget);

    // Arguments still set options in that mode.
    var from_args = parseRunOptions(@as([]const []const u8, &.{ "capnpc-zig", "--output-dir=capnp-gen", "--no-manifest", "max-codegen-fields=3" }));
    _ = applyEnvironment(&env, &from_args);
    try std.testing.expect(!from_args.emit_schema_manifest);
    try std.testing.expectEqual(@as(usize, 3), from_args.codegen_budget.max_fields);

    // Nothing to report when no option variable is set.
    var empty: std.process.Environ.Map = .init(std.testing.allocator);
    defer empty.deinit();
    try empty.put("CAPNPC_ZIG_UPDATE_GOLDENS", "1");
    var quiet = parseRunOptions(@as([]const []const u8, &.{ "capnpc-zig", "--output-dir=capnp-gen" }));
    try std.testing.expectEqual(@as(?[]const u8, null), applyEnvironment(&empty, &quiet));
}

test "env_option_names lists only variables applyEnvRunOptions reads" {
    // Each listed name, set alone, must move some option off its default;
    // a stale entry would make the build-step diagnostic name a variable
    // that never did anything.
    const defaults = parseRunOptions(@as([]const []const u8, &.{"capnpc-zig"}));
    for (env_option_names) |name| {
        var env: std.process.Environ.Map = .init(std.testing.allocator);
        defer env.deinit();
        const value = if (std.mem.eql(u8, name, "CAPNPC_ZIG_API_PROFILE"))
            "compact"
        else if (std.mem.eql(u8, name, "CAPNPC_ZIG_SCHEMA_MANIFEST"))
            "0"
        else
            "1";
        try env.put(name, value);
        var options = defaults;
        applyEnvRunOptions(&env, &options);
        try std.testing.expect(!std.meta.eql(defaults, options));
    }
}

test "openOutputRoot creates the directory and rejects an empty path" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try std.testing.expectError(error.InvalidOutputDir, openOutputRoot(tmp.dir, io, ""));

    var root = try openOutputRoot(tmp.dir, io, "build/capnp-gen");
    defer root.close(io);
    var file = try createOutputFileInDir(root, io, "schema/addressbook.zig");
    defer file.close(io);
    try file.writeStreamingAll(io, "// generated\n");

    var reopened = try tmp.dir.openFile(io, "build/capnp-gen/schema/addressbook.zig", .{});
    defer reopened.close(io);
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
