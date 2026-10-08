const std = @import("std");

const Profile = enum {
    default,
    core,
    quic,
    native,
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const profile_name = b.option(
        []const u8,
        "consumer-profile",
        "Package root to consume: default|core|quic|native",
    ) orelse "default";
    const profile = std.meta.stringToEnum(Profile, profile_name) orelse
        @panic("invalid -Dconsumer-profile; expected default|core|quic|native");
    if (profile == .native) return buildNative(b, target, optimize);

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
        .native => unreachable, // buildNative
    };

    const consumer = b.addExecutable(.{
        .name = switch (profile) {
            .default => "consumer-default",
            .core => "consumer-core",
            .quic => "consumer-quic",
            .native => unreachable, // buildNative
        },
        .root_module = b.createModule(.{
            .root_source_file = b.path(root_source),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = dependency.module(module_name) },
                // capnp-zig's own options module (`-Dfd-passing`), the one
                // copy a consumer may import next to it (common.zig).
                .{ .name = "capnp_build_options", .module = dependency.module("capnp_build_options") },
            },
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

/// The native C ABI consumer (docs/native-abi.md): a static library that
/// ships capnp-zig's `native` C ABI, as capnp-swift's XCFramework slices do,
/// and a C program that includes the packaged header and links it.
fn buildNative(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    // The host owns every socket: build capnp-zig without fd passing, the fd
    // closer threads and the AF_UNIX transport.
    const capnp = b.dependency("capnpc_zig", .{
        .target = target,
        .optimize = optimize,
        .@"fd-passing" = false,
    });
    // The library root references `native.abi`, so the `capnp_*` symbols
    // land in the archive.
    const core = b.addLibrary(.{
        .name = "capnp_core",
        .linkage = .static,
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/native_lib.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .imports = &.{
                .{ .name = "capnpc-zig-core", .module = capnp.module("capnpc-zig-core") },
            },
        }),
    });
    // The program that links the archive brings its own compiler-rt.
    core.bundle_compiler_rt = false;
    // The header ships in the package, next to the module.
    core.installHeader(capnp.path("src/native/include/capnp_core.h"), "capnp_core.h");
    b.installArtifact(core);

    const host = b.addExecutable(.{
        .name = "consumer-native",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    host.root_module.addCSourceFile(.{ .file = b.path("src/native_host.c") });
    host.root_module.linkLibrary(core);
    b.installArtifact(host);
}
