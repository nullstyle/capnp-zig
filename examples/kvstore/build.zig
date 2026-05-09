const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const capnpc_dep = b.dependency("capnpc_zig", .{
        .target = target,
        .optimize = optimize,
    });
    const rocksdb_dep = b.dependency("rocksdb", .{
        .target = target,
        .optimize = optimize,
    });

    const lib_module = capnpc_dep.module("capnpc-zig");
    const rocksdb_module = rocksdb_dep.module("bindings");
    const rocksdb_c_module = rocksdb_dep.module("rocksdb");

    // Server
    const server = b.addExecutable(.{
        .name = "kvstore-server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "rocksdb-zig", .module = rocksdb_module },
                .{ .name = "rocksdb", .module = rocksdb_c_module },
            },
        }),
    });
    b.installArtifact(server);

    const run_server = b.addRunArtifact(server);
    if (b.args) |args| {
        run_server.addArgs(args);
    }
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
    if (b.args) |args| {
        run_stressor.addArgs(args);
    }
    const stressor_step = b.step("stressor", "Run the KVStore stressor");
    stressor_step.dependOn(&run_stressor.step);

    // Compile smoke tests for the server module.
    const server_tests = b.addTest(.{
        .name = "kvstore-server-tests",
        .root_module = b.createModule(.{
            .root_source_file = b.path("server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "rocksdb-zig", .module = rocksdb_module },
                .{ .name = "rocksdb", .module = rocksdb_c_module },
            },
        }),
    });
    const run_server_tests = b.addRunArtifact(server_tests);

    const test_step = b.step("test", "Run KVStore example compile tests");
    test_step.dependOn(&run_server_tests.step);
}
