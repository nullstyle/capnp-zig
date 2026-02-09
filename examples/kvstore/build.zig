const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const capnpc_dep = b.dependency("capnpc_zig", .{
        .target = target,
        .optimize = optimize,
    });

    const lib_module = capnpc_dep.module("capnpc-zig");

    // Server
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
    if (b.args) |args| {
        run_server.addArgs(args);
    }
    const server_step = b.step("server", "Run the KVStore server");
    server_step.dependOn(&run_server.step);

    // Client
    const client = b.addExecutable(.{
        .name = "kvstore-client",
        .root_module = b.createModule(.{
            .root_source_file = b.path("client.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });
    b.installArtifact(client);

    const run_client = b.addRunArtifact(client);
    if (b.args) |args| {
        run_client.addArgs(args);
    }
    const client_step = b.step("client", "Run the KVStore client");
    client_step.dependOn(&run_client.step);
}
