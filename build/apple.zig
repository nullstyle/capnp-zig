//! The embedder static-library gates: `check-ios` and
//! `check-fd-passing-off-symbols`, and the host test of their shared root,
//! `tests/apple/apple_check_root.zig`.
//!
//! Each library is `capnpc-zig-core` plus that root, built with its own
//! `capnp_build_options` (a fixed `-Dfd-passing`), so the gates do not
//! depend on the options of the build that runs them. A static library never
//! links, so `check-ios` needs no Apple SDK and runs on any host.
//!
//! Self-contained on purpose: one call from build_impl.zig registers it.

const std = @import("std");
const helpers = @import("helpers.zig");

/// One library `check-ios` builds.
const Point = struct {
    query: std.Target.Query,
    /// The `-Dfd-passing` value the library is built with.
    fd_passing: bool,
};

/// The iOS device and both simulators keep the option on, so the target
/// gate alone must compile fd passing out there. The macOS point turns it
/// off, so the root's comptime check proves the option reaches the gate on
/// any host (`check-fd-passing-off-symbols` needs a macOS host).
const points = [_]Point{
    .{ .query = .{ .cpu_arch = .aarch64, .os_tag = .ios }, .fd_passing = true },
    .{ .query = .{ .cpu_arch = .aarch64, .os_tag = .ios, .abi = .simulator }, .fd_passing = true },
    .{ .query = .{ .cpu_arch = .x86_64, .os_tag = .ios, .abi = .simulator }, .fd_passing = true },
    .{ .query = .{ .cpu_arch = .aarch64, .os_tag = .macos }, .fd_passing = false },
};

const modes = [_]std.builtin.OptimizeMode{ .Debug, .ReleaseSafe };

/// What `zig build check-fd-passing-off-symbols` forbids in a macOS static
/// library of the core built with `-Dfd-passing=false`: the futex imports
/// and the thread and rlimit calls of the fd closer and the fd budget
/// (capnp-swift's symbol gate fails on `___ulock_*` and `_pthread_create`),
/// and any fd closer function at all. (A Debug build still emits the
/// constant `fd_closer.supported` as data; only functions count.)
const forbidden_rules = [_][]const u8{
    "--forbid-undefined-prefix",    "___ulock_",
    "--forbid-undefined",           "_pthread_create",
    "--forbid-undefined",           "_getrlimit",
    "--forbid-function-containing", "fd_closer",
};

pub const Steps = struct {
    /// Runs the root's tests on the requested target, with the build's own
    /// `-Dfd-passing`, and the `nm` reader's own tests. `test` depends on
    /// both.
    host_tests: [2]*std.Build.Step,
};

pub fn register(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    capnp_build_options_module: *std.Build.Module,
) Steps {
    const check_ios_step = b.step(
        "check-ios",
        "Compile capnpc-zig-core as static libraries for iOS, the iOS simulators, and macOS with fd passing off (Debug and ReleaseSafe; no SDK, nothing links or runs)",
    );
    for (points) |point| {
        const point_target = b.resolveTargetQuery(point.query);
        for (modes) |mode| {
            const lib = checkLibrary(b, point_target, mode, optionsModule(b, point.fd_passing));
            // Emit the archive: code generation can fail where analysis
            // passes.
            _ = lib.getEmittedBin();
            check_ios_step.dependOn(&lib.step);
        }
    }

    registerSymbolCheck(b);

    const root_test = b.addTest(.{
        .name = "apple-check-root-test",
        .root_module = rootModule(b, target, optimize, capnp_build_options_module),
    });
    helpers.registered_test_compile_steps.append(b.allocator, &root_test.step) catch @panic("OOM");
    const tool_test = b.addTest(.{
        .name = "archive-symbols-test",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/archive_symbols.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    helpers.registered_test_compile_steps.append(b.allocator, &tool_test.step) catch @panic("OOM");
    return .{ .host_tests = .{ &b.addRunArtifact(root_test).step, &b.addRunArtifact(tool_test).step } };
}

/// `check-fd-passing-off-symbols`: a static library of the core for the
/// macOS host with `-Dfd-passing=false`, read with `nm`. ReleaseSafe and
/// Debug both. Elsewhere the step prints why it skipped and succeeds.
fn registerSymbolCheck(b: *std.Build) void {
    const step = b.step(
        "check-fd-passing-off-symbols",
        "Fail if a macOS static library of capnpc-zig-core built with -Dfd-passing=false imports ___ulock_*, _pthread_create or _getrlimit, or defines an fd closer function (macOS host only; skips elsewhere)",
    );
    const host = b.graph.host;
    const checker = b.addExecutable(.{
        .name = "archive-symbols",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/archive_symbols.zig"),
            .target = host,
            .optimize = .Debug,
        }),
    });
    if (host.result.os.tag != .macos) {
        const skip = b.addRunArtifact(checker);
        skip.addArgs(&.{ "--skip", "check-fd-passing-off-symbols needs a macOS host (it reads a Mach-O archive with nm); `zig build check-ios` still compiles the macOS library with fd passing off" });
        step.dependOn(&skip.step);
        return;
    }
    const options = optionsModule(b, false);
    for (modes) |mode| {
        const lib = checkLibrary(b, host, mode, options);
        const nm = b.addSystemCommand(&.{"nm"});
        nm.addFileArg(lib.getEmittedBin());
        const listing = nm.captureStdOut(.{});
        const check = b.addRunArtifact(checker);
        check.addArg("--nm");
        check.addFileArg(listing);
        check.addArgs(&forbidden_rules);
        // The handoff's ReleaseSafe library lists 11 imports and hundreds
        // of definitions; an empty listing must never pass.
        check.addArgs(&.{ "--min-symbols", "100" });
        step.dependOn(&check.step);
    }
}

/// A `capnp_build_options` module with a fixed `fd_passing`.
fn optionsModule(b: *std.Build, fd_passing: bool) *std.Build.Module {
    const options = b.addOptions();
    options.addOption(bool, "fd_passing", fd_passing);
    return options.createModule();
}

/// The check root over a `capnpc-zig-core` built for `target`.
fn rootModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    build_options: *std.Build.Module,
) *std.Build.Module {
    const core = b.createModule(.{
        .root_source_file = b.path("src/lib_core.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnp_build_options", .module = build_options }},
    });
    core.addImport("capnpc-zig", core);
    return b.createModule(.{
        .root_source_file = b.path("tests/apple/apple_check_root.zig"),
        .target = target,
        .optimize = optimize,
        // The root's exported functions allocate with `std.heap.c_allocator`,
        // as an embedder's do.
        .link_libc = true,
        .imports = &.{
            .{ .name = "capnpc-zig-core", .module = core },
            .{ .name = "capnp_build_options", .module = build_options },
        },
    });
}

fn checkLibrary(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    build_options: *std.Build.Module,
) *std.Build.Step.Compile {
    return b.addLibrary(.{
        .name = "capnp-embedder-check",
        .linkage = .static,
        .root_module = rootModule(b, target, optimize, build_options),
    });
}
