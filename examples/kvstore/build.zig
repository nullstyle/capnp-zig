const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const capnpc_dep = b.dependency("capnpc_zig", .{
        .target = target,
        .optimize = optimize,
    });

    const lib_module = capnpc_dep.module("capnpc-zig");

    // Server. The storage backend (store.zig) is pure Zig and imported
    // directly by server.zig, so there is no external dependency to wire up.
    const server = b.addExecutable(.{
        .name = "kvstore-server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });
    b.installArtifact(server);

    const run_server = b.addRunArtifact(server);
    run_server.addPassthruArgs();
    const server_step = b.step("server", "Run the KVStore server");
    server_step.dependOn(&run_server.step);

    const stressor = b.addExecutable(.{
        .name = "kvstore-stressor",
        .root_module = b.createModule(.{
            .root_source_file = b.path("stressor.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });
    b.installArtifact(stressor);

    const run_stressor = b.addRunArtifact(stressor);
    run_stressor.addPassthruArgs();
    const stressor_step = b.step("stressor", "Run the KVStore stressor");
    stressor_step.dependOn(&run_stressor.step);

    // Compile + run tests for the server and its store backend.
    const server_tests = b.addTest(.{
        .name = "kvstore-server-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });
    const run_server_tests = b.addRunArtifact(server_tests);

    const store_tests = b.addTest(.{
        .name = "kvstore-store-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("store.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_store_tests = b.addRunArtifact(store_tests);

    const test_step = b.step("test", "Run KVStore example tests");
    test_step.dependOn(&run_server_tests.step);
    test_step.dependOn(&run_store_tests.step);
}
