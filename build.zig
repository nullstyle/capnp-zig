const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create the library module
    const lib_module = b.addModule("capnpc-zig", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Main executable
    const exe = b.addExecutable(.{
        .name = "capnpc-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    b.installArtifact(exe);

    // Run command
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the plugin");
    run_step.dependOn(&run_cmd.step);

    // Benchmarks
    const ping_pong_bench = b.addExecutable(.{
        .name = "bench-ping-pong",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/ping_pong.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    b.installArtifact(ping_pong_bench);

    const run_ping_pong = b.addRunArtifact(ping_pong_bench);
    if (b.args) |args| {
        run_ping_pong.addArgs(args);
    }

    const bench_ping_pong_step = b.step("bench-ping-pong", "Run ping-pong benchmark");
    bench_ping_pong_step.dependOn(&run_ping_pong.step);

    // Unit tests for main
    const main_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_main_tests = b.addRunArtifact(main_tests);

    // Message tests
    const message_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/message_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_message_tests = b.addRunArtifact(message_tests);

    // Code generation tests
    const codegen_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/codegen_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_codegen_tests = b.addRunArtifact(codegen_tests);

    const codegen_defaults_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/codegen_defaults_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_codegen_defaults_tests = b.addRunArtifact(codegen_defaults_tests);

    const codegen_annotations_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/codegen_annotations_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_codegen_annotations_tests = b.addRunArtifact(codegen_annotations_tests);

    // Integration tests
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/integration_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_integration_tests = b.addRunArtifact(integration_tests);

    // Interop tests
    const interop_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/interop_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_interop_tests = b.addRunArtifact(interop_tests);

    const interop_roundtrip_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/interop_roundtrip_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_interop_roundtrip_tests = b.addRunArtifact(interop_roundtrip_tests);

    // Real-world Person tests
    const real_world_person_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/real_world_person_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_real_world_person_tests = b.addRunArtifact(real_world_person_tests);

    // Real-world AddressBook tests
    const real_world_addressbook_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/real_world_addressbook_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_real_world_addressbook_tests = b.addRunArtifact(real_world_addressbook_tests);

    // Union tests
    const union_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/union_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_union_tests = b.addRunArtifact(union_tests);

    // Cap'n Proto official testdata fixtures
    const capnp_testdata_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/capnp_testdata_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_capnp_testdata_tests = b.addRunArtifact(capnp_testdata_tests);

    // Test step runs all tests
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_main_tests.step);
    test_step.dependOn(&run_message_tests.step);
    test_step.dependOn(&run_codegen_tests.step);
    test_step.dependOn(&run_codegen_defaults_tests.step);
    test_step.dependOn(&run_codegen_annotations_tests.step);
    test_step.dependOn(&run_integration_tests.step);
    test_step.dependOn(&run_interop_tests.step);
    test_step.dependOn(&run_interop_roundtrip_tests.step);
    test_step.dependOn(&run_real_world_person_tests.step);
    test_step.dependOn(&run_real_world_addressbook_tests.step);
    test_step.dependOn(&run_union_tests.step);
    test_step.dependOn(&run_capnp_testdata_tests.step);

    // Individual test steps
    const test_message_step = b.step("test-message", "Run message serialization tests");
    test_message_step.dependOn(&run_message_tests.step);

    const test_codegen_step = b.step("test-codegen", "Run code generation tests");
    test_codegen_step.dependOn(&run_codegen_tests.step);
    test_codegen_step.dependOn(&run_codegen_defaults_tests.step);
    test_codegen_step.dependOn(&run_codegen_annotations_tests.step);

    const test_integration_step = b.step("test-integration", "Run integration tests");
    test_integration_step.dependOn(&run_integration_tests.step);

    const test_interop_step = b.step("test-interop", "Run interop tests");
    test_interop_step.dependOn(&run_interop_tests.step);

    const test_real_world_step = b.step("test-real-world", "Run real-world schema tests");
    test_real_world_step.dependOn(&run_real_world_person_tests.step);
    test_real_world_step.dependOn(&run_real_world_addressbook_tests.step);

    const test_union_step = b.step("test-union", "Run union tests");
    test_union_step.dependOn(&run_union_tests.step);

    const test_capnp_testdata_step = b.step("test-capnp-testdata", "Run Cap'n Proto official testdata fixtures");
    test_capnp_testdata_step.dependOn(&run_capnp_testdata_tests.step);

    // Check step (compile without linking)
    const check = b.addExecutable(.{
        .name = "capnpc-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const check_step = b.step("check", "Check for compilation errors");
    check_step.dependOn(&check.step);
}
