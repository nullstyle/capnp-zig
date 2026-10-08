//! The native shim's gates (`src/native/`, capnp-swift handoff H7):
//!
//!   test-native-abi   the C ABI through `src/native/include/capnp_core.h`
//!                     (`src/native/abi_test.zig`, translate-c), linked
//!                     against a host static library whose root references
//!                     `native.abi` (`tests/native/abi_lib_root.zig`). The
//!                     test never imports abi.zig, so a prototype, layout or
//!                     linkage mistake fails here. Plus
//!                     `tests/native/abi_version_test.zig` (the root's
//!                     version string). `test` runs both.
//!   fuzz-native-abi   random operation sequences over the C ABI
//!                     (`tests/native/fuzz_abi.zig`; pass
//!                     `-- --seconds N [--seed S]`); exit 1 on a violation.
//!                     A seeded 3-second run joins `test-fuzz-smoke`.
//!
//! The shim's own tests (conn_test.zig and the tests in abi.zig, including
//! the header-drift gate) run in the `capnpc-zig-core` test root
//! (src/lib_core.zig), which needs the header imports `addHeaderImports`
//! adds.
//!
//! Nothing here links libc: these binaries compile for every CI cross
//! target (`check-test-compile`), where Zig cannot always provide one.
//!
//! Self-contained on purpose: build_impl.zig calls `addHeaderImports` once
//! and `register` once.

const std = @import("std");
const helpers = @import("helpers.zig");

const header_path = "src/native/include/capnp_core.h";

/// The two imports abi.zig's tests read: `capnp_core_h` (the header through
/// translate-c, for the declared shapes and constants) and
/// `capnp_core_h_text` (its raw text, for the header -> export direction).
pub fn addHeaderImports(
    b: *std.Build,
    module: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    module.addImport("capnp_core_h", headerModule(b, target, optimize));
    module.addAnonymousImport("capnp_core_h_text", .{ .root_source_file = b.path(header_path) });
}

fn headerModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Module {
    const header_c = b.addTranslateC(.{
        .root_source_file = b.path(header_path),
        .target = headerTarget(b, target),
        .optimize = optimize,
    });
    // A module of the real target, whatever target translated the header.
    return b.createModule(.{
        .root_source_file = header_c.getOutput(),
        .target = target,
        .optimize = optimize,
    });
}

/// The target translate-c reads the header for. The header includes
/// <stddef.h> and <stdint.h>, and Zig ships no C headers for some CI cross
/// targets (powerpc64-linux-gnu: clang then reads the host's SDK and
/// fails). A Linux target without a libc of its own reads musl's headers
/// instead: the same data model, so the same declarations.
fn headerTarget(b: *std.Build, target: std.Build.ResolvedTarget) std.Build.ResolvedTarget {
    if (target.result.os.tag != .linux or std.zig.target.canBuildLibC(&target.result)) return target;
    var query = target.query;
    query.abi = .musl;
    return b.resolveTargetQuery(query);
}

pub const Steps = struct {
    /// `test-native-abi`; `test` depends on it.
    test_abi: *std.Build.Step,
    /// The seeded short fuzz run; `test-fuzz-smoke` depends on it.
    fuzz_smoke: *std.Build.Step,
};

pub fn register(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    core_module: *std.Build.Module,
) Steps {
    // ---- test-native-abi ------------------------------------------------
    const host_lib = b.addLibrary(.{
        .name = "capnp_core",
        .linkage = .static,
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/native/abi_lib_root.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "capnpc-zig-core", .module = core_module }},
        }),
    });
    // The test binary brings its own compiler-rt; two copies would collide.
    host_lib.bundle_compiler_rt = false;
    host_lib.bundle_ubsan_rt = false;

    const abi_test_module = b.createModule(.{
        .root_source_file = b.path("src/native/abi_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnpc-zig", .module = core_module },
            .{ .name = "capnp_core_h", .module = headerModule(b, target, optimize) },
        },
    });
    abi_test_module.linkLibrary(host_lib);
    const abi_tests = b.addTest(.{ .name = "native-abi-test", .root_module = abi_test_module });
    helpers.registered_test_compile_steps.append(b.allocator, &abi_tests.step) catch @panic("OOM");
    const test_abi_step = b.step("test-native-abi", "Run the native C ABI tests (capnp_core.h against a host static library)");
    test_abi_step.dependOn(&b.addRunArtifact(abi_tests).step);

    // The root's version string through the header. A separate binary so
    // abi_test.zig stays reusable against an embedder's own library root.
    const version_test_module = b.createModule(.{
        .root_source_file = b.path("tests/native/abi_version_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnp_core_h", .module = headerModule(b, target, optimize) },
        },
    });
    version_test_module.linkLibrary(host_lib);
    const version_tests = b.addTest(.{ .name = "native-abi-version-test", .root_module = version_test_module });
    helpers.registered_test_compile_steps.append(b.allocator, &version_tests.step) catch @panic("OOM");
    test_abi_step.dependOn(&b.addRunArtifact(version_tests).step);

    // ---- fuzz-native-abi ------------------------------------------------
    const fuzz_module = b.createModule(.{
        .root_source_file = b.path("tests/native/fuzz_abi.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = core_module }},
    });
    // The seeds are the framing fixtures capnp-swift copied from this
    // repository (tests/fixtures/framing/README.md).
    fuzz_module.addAnonymousImport("fuzz_seeds_json", .{ .root_source_file = b.path("tests/fixtures/framing/framing_fixtures.json") });
    const fuzz_exe = b.addExecutable(.{ .name = "fuzz-native-abi", .root_module = fuzz_module });
    const fuzz_step = b.step("fuzz-native-abi", "Fuzz the native C ABI (pass -- --seconds N [--seed S]); exit 1 on a violation");
    // Also installed (zig-out/bin/fuzz-native-abi) so a long run can use a
    // binary that later builds do not touch.
    fuzz_step.dependOn(&b.addInstallArtifact(fuzz_exe, .{}).step);
    const fuzz_run = b.addRunArtifact(fuzz_exe);
    fuzz_run.addPassthruArgs();
    fuzz_step.dependOn(&fuzz_run.step);

    // A short seeded run for the gates: no install step (the `test-compile`
    // warmup walks this graph and accepts only compile and run steps). The
    // smoke runs it, so it is test code: cross targets compile it too.
    helpers.registered_test_compile_steps.append(b.allocator, &fuzz_exe.step) catch @panic("OOM");
    const fuzz_smoke = b.addRunArtifact(fuzz_exe);
    fuzz_smoke.addArgs(&.{ "--seconds", "3", "--seed", "1" });

    return .{ .test_abi = test_abi_step, .fuzz_smoke = &fuzz_smoke.step };
}
