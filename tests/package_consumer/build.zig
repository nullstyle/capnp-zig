const std = @import("std");

const Profile = enum {
    default,
    core,
    quic,
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const profile_name = b.option(
        []const u8,
        "consumer-profile",
        "Package root to consume: default|core|quic",
    ) orelse "default";
    const profile = std.meta.stringToEnum(Profile, profile_name) orelse
        @panic("invalid -Dconsumer-profile; expected default|core|quic");

    const dependency = b.dependency("capnpc_zig", .{
        .target = target,
        .optimize = optimize,
        .quic = profile == .quic,
    });
    const module_name = if (profile == .core) "capnpc-zig-core" else "capnpc-zig";
    const root_source = switch (profile) {
        .default => "src/default.zig",
        .core => "src/core.zig",
        .quic => "src/quic.zig",
    };

    const consumer = b.addExecutable(.{
        .name = switch (profile) {
            .default => "consumer-default",
            .core => "consumer-core",
            .quic => "consumer-quic",
        },
        .root_module = b.createModule(.{
            .root_source_file = b.path(root_source),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "capnpc-zig", .module = dependency.module(module_name) }},
        }),
    });
    b.installArtifact(consumer);

    // A library-only consumer would not notice that the compiler plugin was
    // accidentally omitted from `.paths`. Build and install the dependency's
    // real executable artifact in the default profile.
    if (profile == .default) {
        b.installArtifact(dependency.artifact("capnpc-zig"));
    }
}
