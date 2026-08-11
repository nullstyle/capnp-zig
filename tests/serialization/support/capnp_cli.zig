const std = @import("std");

/// The upstream Windows tool archive contains `capnp.exe`, but not the
/// standard schema tree used by imports such as `/capnp/stream.capnp`.
/// Tests always add the vendored upstream tree explicitly so the exact same
/// command line works on Linux, macOS, and Windows.
pub const standard_include_arg = "-Ivendor/ext/capnproto/c++/src";

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
};

pub const SpawnOptions = struct {
    missing: MissingPolicy = .skip,
    cwd: std.process.Child.Cwd = .inherit,
    stdin: std.process.SpawnOptions.StdIo = .inherit,
    stdout: std.process.SpawnOptions.StdIo = .inherit,
    stderr: std.process.SpawnOptions.StdIo = .inherit,
};

pub const Command = struct {
    allocator: std.mem.Allocator,
    argv: []const []const u8,

    pub fn deinit(prepared: *Command) void {
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

/// Prepare a `capnp` argv from arguments beginning with the subcommand.
///
/// The include flag is inserted immediately after the subcommand, which is the
/// position accepted consistently by `compile`, `convert`, and `eval`.
pub fn command(allocator: std.mem.Allocator, args: []const []const u8) !Command {
    if (args.len == 0) return error.MissingCapnpSubcommand;

    const add_include = !hasStandardInclude(args);
    const argv = try allocator.alloc([]const u8, args.len + 1 + @intFromBool(add_include));
    argv[0] = "capnp";
    argv[1] = args[0];

    var out_index: usize = 2;
    if (add_include) {
        argv[out_index] = standard_include_arg;
        out_index += 1;
    }
    @memcpy(argv[out_index..], args[1..]);

    return .{ .allocator = allocator, .argv = argv };
}

fn missingError(policy: MissingPolicy) anyerror {
    return switch (policy) {
        .skip => error.SkipZigTest,
        .required => error.CapnpCompilerUnavailable,
    };
}

pub fn run(
    allocator: std.mem.Allocator,
    io: std.Io,
    args: []const []const u8,
    options: Options,
) !std.process.RunResult {
    var prepared = try command(allocator, args);
    defer prepared.deinit();

    return std.process.run(allocator, io, .{
        .argv = prepared.argv,
        .cwd = options.cwd,
    }) catch |err| switch (err) {
        error.FileNotFound => return missingError(options.missing),
        else => return err,
    };
}

pub fn spawn(
    allocator: std.mem.Allocator,
    io: std.Io,
    args: []const []const u8,
    options: SpawnOptions,
) !std.process.Child {
    var prepared = try command(allocator, args);
    defer prepared.deinit();

    return std.process.spawn(io, .{
        .argv = prepared.argv,
        .cwd = options.cwd,
        .stdin = options.stdin,
        .stdout = options.stdout,
        .stderr = options.stderr,
    }) catch |err| switch (err) {
        error.FileNotFound => return missingError(options.missing),
        else => return err,
    };
}

test "command injects the vendored standard include exactly once" {
    var prepared = try command(std.testing.allocator, &.{ "compile", "-o-", "example.capnp" });
    defer prepared.deinit();

    try std.testing.expectEqualSlices([]const u8, &.{
        "capnp",
        "compile",
        standard_include_arg,
        "-o-",
        "example.capnp",
    }, prepared.argv);

    var already_present = try command(std.testing.allocator, &.{
        "compile",
        standard_include_arg,
        "-o-",
        "example.capnp",
    });
    defer already_present.deinit();
    try std.testing.expectEqual(@as(usize, 5), already_present.argv.len);
}
