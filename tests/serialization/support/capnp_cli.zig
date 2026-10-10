const std = @import("std");

/// Tests use the same vendored standard schemas on every host.
pub const standard_include_arg = "-Ivendor/ext/capnproto/c++/src";
const driver_prefix = [_][]const u8{
    "deno", "run", "--allow-all", "--no-config", "tools/capnp_tool.ts", "compiler", "--",
};
const driver_script_index = 4;

pub const StandardIncludes = enum {
    vendored,
    /// Pass only the caller's include flags, including --no-standard-import
    /// when the test must exclude the compiler package's standard schemas.
    explicit,
};

pub const MissingPolicy = enum {
    /// Preserve the repository's developer-friendly behavior when the optional
    /// reference compiler is not installed locally.
    skip,
    /// Turn a missing compiler into a hard failure for CI and release gates.
    required,
};

pub const Options = struct {
    missing: MissingPolicy = .skip,
    cwd: std.process.Child.Cwd = .inherit,
    standard_includes: StandardIncludes = .vendored,
};

pub const SpawnOptions = struct {
    missing: MissingPolicy = .skip,
    cwd: std.process.Child.Cwd = .inherit,
    standard_includes: StandardIncludes = .vendored,
    stdin: std.process.SpawnOptions.StdIo = .inherit,
    stdout: std.process.SpawnOptions.StdIo = .inherit,
    stderr: std.process.SpawnOptions.StdIo = .inherit,
};

pub const Command = struct {
    allocator: std.mem.Allocator,
    argv: [][]const u8,
    driver_path: ?[]u8 = null,
    include_arg: ?[]u8 = null,

    fn resolveRepositoryPaths(prepared: *Command, io: std.Io) !void {
        const root = try std.process.currentPathAlloc(io, prepared.allocator);
        defer prepared.allocator.free(root);
        prepared.driver_path = try std.fs.path.join(prepared.allocator, &.{ root, "tools", "capnp_tool.ts" });
        prepared.argv[driver_script_index] = prepared.driver_path.?;
        for (prepared.argv) |*arg| {
            if (!std.mem.eql(u8, arg.*, standard_include_arg)) continue;
            const path = try std.fs.path.join(prepared.allocator, &.{ root, "vendor", "ext", "capnproto", "c++", "src" });
            defer prepared.allocator.free(path);
            prepared.include_arg = try std.fmt.allocPrint(prepared.allocator, "-I{s}", .{path});
            arg.* = prepared.include_arg.?;
            break;
        }
    }

    pub fn deinit(prepared: *Command) void {
        if (prepared.driver_path) |path| prepared.allocator.free(path);
        if (prepared.include_arg) |arg| prepared.allocator.free(arg);
        prepared.allocator.free(prepared.argv);
        prepared.* = undefined;
    }
};

fn hasStandardInclude(args: []const []const u8) bool {
    for (args) |arg| {
        if (std.mem.eql(u8, arg, standard_include_arg)) return true;
    }
    return false;
}

/// Prepare the pinned WASM driver's argv from a Cap'n Proto subcommand.
///
/// The include flag is inserted immediately after the subcommand, which is the
/// position accepted consistently by `compile`, `convert`, and `eval`.
pub fn command(allocator: std.mem.Allocator, args: []const []const u8, includes: StandardIncludes) !Command {
    if (args.len == 0) return error.MissingCapnpSubcommand;

    const add_include = includes == .vendored and !hasStandardInclude(args);
    const argv = try allocator.alloc([]const u8, args.len + driver_prefix.len + @intFromBool(add_include));
    @memcpy(argv[0..driver_prefix.len], &driver_prefix);
    argv[driver_prefix.len] = args[0];

    var out_index: usize = driver_prefix.len + 1;
    if (add_include) {
        argv[out_index] = standard_include_arg;
        out_index += 1;
    }
    @memcpy(argv[out_index..], args[1..]);

    return .{ .allocator = allocator, .argv = argv };
}

fn missingError(policy: MissingPolicy, ci: bool) anyerror {
    return switch (if (ci) MissingPolicy.required else policy) {
        .skip => error.SkipZigTest,
        .required => error.CapnpCompilerUnavailable,
    };
}

fn missingLauncherError(allocator: std.mem.Allocator, policy: MissingPolicy) anyerror {
    var environ = std.process.Environ.createMap(std.testing.environ, allocator) catch |err| return err;
    defer environ.deinit();
    const ci = environ.get("CI") orelse "";
    return missingError(policy, ci.len != 0 and !std.ascii.eqlIgnoreCase(ci, "false") and !std.mem.eql(u8, ci, "0"));
}

fn validateCwd(io: std.Io, cwd: std.process.Child.Cwd) !void {
    // A missing requested cwd is a configuration error, not a missing tool.
    if (cwd == .path) {
        var dir = try std.Io.Dir.cwd().openDir(io, cwd.path, .{});
        dir.close(io);
    }
}

pub fn run(
    allocator: std.mem.Allocator,
    io: std.Io,
    args: []const []const u8,
    options: Options,
) !std.process.RunResult {
    try validateCwd(io, options.cwd);
    var prepared = try command(allocator, args, options.standard_includes);
    defer prepared.deinit();
    try prepared.resolveRepositoryPaths(io);

    return std.process.run(allocator, io, .{
        .argv = prepared.argv,
        .cwd = options.cwd,
    }) catch |err| switch (err) {
        error.FileNotFound => return missingLauncherError(allocator, options.missing),
        else => return err,
    };
}

pub fn spawn(
    allocator: std.mem.Allocator,
    io: std.Io,
    args: []const []const u8,
    options: SpawnOptions,
) !std.process.Child {
    try validateCwd(io, options.cwd);
    var prepared = try command(allocator, args, options.standard_includes);
    defer prepared.deinit();
    try prepared.resolveRepositoryPaths(io);

    return std.process.spawn(io, .{
        .argv = prepared.argv,
        .cwd = options.cwd,
        .stdin = options.stdin,
        .stdout = options.stdout,
        .stderr = options.stderr,
    }) catch |err| switch (err) {
        error.FileNotFound => return missingLauncherError(allocator, options.missing),
        else => return err,
    };
}

test "command injects the vendored standard include exactly once" {
    var prepared = try command(std.testing.allocator, &.{ "compile", "-o-", "example.capnp" }, .vendored);
    defer prepared.deinit();

    try std.testing.expectEqualSlices([]const u8, &driver_prefix, prepared.argv[0..driver_prefix.len]);
    try std.testing.expectEqualSlices([]const u8, &.{
        "compile",
        standard_include_arg,
        "-o-",
        "example.capnp",
    }, prepared.argv[driver_prefix.len..]);

    var already_present = try command(std.testing.allocator, &.{
        "compile",
        standard_include_arg,
        "-o-",
        "example.capnp",
    }, .vendored);
    defer already_present.deinit();
    try std.testing.expectEqual(driver_prefix.len + 4, already_present.argv.len);
}

test "explicit include policy preserves a packaged-schema isolation command" {
    const args = [_][]const u8{ "compile", "-o-", "--no-standard-import", "-Isrc/rpc", "streaming.capnp" };
    var prepared = try command(std.testing.allocator, &args, .explicit);
    defer prepared.deinit();
    try std.testing.expectEqualSlices([]const u8, &args, prepared.argv[driver_prefix.len..]);
}

test "missing optional launcher skips only outside CI" {
    try std.testing.expectEqual(error.SkipZigTest, missingError(.skip, false));
    try std.testing.expectEqual(error.CapnpCompilerUnavailable, missingError(.skip, true));
    try std.testing.expectEqual(error.CapnpCompilerUnavailable, missingError(.required, false));
}

test "missing working directory fails instead of skipping an optional compiler" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", std.testing.allocator);
    defer std.testing.allocator.free(root);
    const missing = try std.fs.path.join(std.testing.allocator, &.{ root, "missing" });
    defer std.testing.allocator.free(missing);
    try std.testing.expectError(error.FileNotFound, run(std.testing.allocator, std.testing.io, &.{"--version"}, .{
        .cwd = .{ .path = missing },
    }));
}
