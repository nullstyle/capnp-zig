const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // The runtime. Generated code imports it as "capnpc-zig".
    const capnpc_dep = b.dependency("capnpc_zig", .{
        .target = target,
        .optimize = optimize,
    });
    const capnpc_core = capnpc_dep.module("capnpc-zig-core");

    // The plugin, built from the same pinned package as the runtime, never a
    // PATH binary. It targets the build host so generation also works when
    // you cross-compile; its output does not depend on its optimize mode.
    const capnpc_host = b.dependency("capnpc_zig", .{
        .target = b.graph.host,
        .optimize = .ReleaseSafe,
    });
    const codegen = b.addRunArtifact(capnpc_host.artifact("capnpc-zig"));
    codegen.setStdIn(.{ .lazy_path = b.path("schema/addressbook.request.bin") });
    const gen_dir = codegen.addPrefixedOutputDirectoryArg("--output-dir=", "capnp-gen");

    const addressbook = b.createModule(.{
        .root_source_file = gen_dir.path(b, "addressbook.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = capnpc_core }},
    });

    const exe = b.addExecutable(.{
        .name = "app",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = capnpc_core },
                .{ .name = "addressbook", .module = addressbook },
            },
        }),
    });
    b.installArtifact(exe);

    // Only if you also commit the generated file. `zig build gen` refreshes
    // the checked-in copy from the pinned plugin, and `zig build gen-check`
    // fails, printing the diff, when that copy is stale.
    const gen = b.addUpdateSourceFiles();
    gen.addCopyFileToSource(gen_dir.path(b, "addressbook.zig"), "src/gen/addressbook.zig");
    b.step("gen", "Regenerate src/gen with the pinned capnpc-zig").dependOn(&gen.step);

    // No pager, and no external diff or textconv driver from your git config,
    // so only the file contents decide the result.
    const gen_check = b.addSystemCommand(&.{ "git", "--no-pager", "diff", "--no-index", "--no-ext-diff", "--no-textconv", "--exit-code", "--" });
    gen_check.addFileArg(b.path("src/gen/addressbook.zig"));
    gen_check.addFileArg(gen_dir.path(b, "addressbook.zig"));
    b.step("gen-check", "Fail if src/gen differs from the pinned capnpc-zig").dependOn(&gen_check.step);
}
