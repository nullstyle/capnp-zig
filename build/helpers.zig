//! Build-graph helpers shared by the build clusters (B-series decomposition).
//!
//! These construct test compile/run steps and the optional quic-zig import.
//! `registered_test_compile_steps` is the cross-target compile inventory:
//! every add*Test family appends to it so `check-test-compile` can gate test
//! compilation for targets with no runner. Keep new test-step helpers
//! appending here — a family that skips it makes that gate silently green.

const std = @import("std");

/// Compile steps for every test registered through `addLibTest`, collected
/// so `check-test-compile` can gate test compilation for cross targets
/// (e.g. `zig build check-test-compile -Dtarget=x86_64-windows` on a POSIX
/// host catches Windows test rot without a Windows runner).
pub var registered_test_compile_steps: std.ArrayList(*std.Build.Step) = .empty;

/// Create a test step that imports capnpc-zig and return its run step.
pub fn addLibTest(
    b: *std.Build,
    path: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    lib_module: *std.Build.Module,
) *std.Build.Step {
    const t = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(path),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &t.step) catch @panic("OOM");
    return &b.addRunArtifact(t).step;
}

pub fn addPersistenceLibTest(
    b: *std.Build,
    path: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    lib_module: *std.Build.Module,
) *std.Build.Step {
    const harness_module = b.createModule(.{
        .root_source_file = b.path("tests/rpc/support/persistence_harness.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnpc-zig", .module = lib_module },
        },
    });
    const t = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(path),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "persistence-test-harness", .module = harness_module },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &t.step) catch @panic("OOM");
    return &b.addRunArtifact(t).step;
}

/// Create a QUIC-only test step that imports capnpc-zig and quic_zig.
pub fn addQuicLibTest(
    b: *std.Build,
    path: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    lib_module: *std.Build.Module,
    quic_zig_module: *std.Build.Module,
) *std.Build.Step {
    const t = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(path),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "quic_zig", .module = quic_zig_module },
            },
        }),
    });
    // QUIC tests used to be the only add*Test family omitted from the
    // cross-target compile inventory. That made
    // `-Dquic=true check-test-compile -Dtarget=x86_64-windows` look green
    // without compiling any of the QUIC test sources. Keep the optional
    // dependency boundary, but register every test once it is enabled.
    registered_test_compile_steps.append(b.allocator, &t.step) catch @panic("OOM");
    return &b.addRunArtifact(t).step;
}

pub fn addMainTest(
    b: *std.Build,
    path: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step.Run {
    const t = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(path),
            .target = target,
            .optimize = optimize,
        }),
    });
    return b.addRunArtifact(t);
}

pub fn addQuicImport(module: *std.Build.Module, quic_zig_module: ?*std.Build.Module) void {
    if (quic_zig_module) |m| module.addImport("quic_zig", m);
}
