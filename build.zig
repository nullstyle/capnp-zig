const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const xev_module = b.addModule("xev", .{
        .root_source_file = b.path("vendor/ext/libxev/src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Create the library module
    const lib_module = b.addModule("capnpc-zig", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "xev", .module = xev_module },
        },
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

    const pack_unpack_bench = b.addExecutable(.{
        .name = "bench-pack-unpack",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/packed_unpacked.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    b.installArtifact(pack_unpack_bench);

    const run_pack = b.addRunArtifact(pack_unpack_bench);
    run_pack.addArgs(&.{ "--mode", "pack" });
    if (b.args) |args| {
        run_pack.addArgs(args);
    }

    const run_unpack = b.addRunArtifact(pack_unpack_bench);
    run_unpack.addArgs(&.{ "--mode", "unpack" });
    if (b.args) |args| {
        run_unpack.addArgs(args);
    }

    const bench_pack_step = b.step("bench-packed", "Run packed (packing) benchmark");
    bench_pack_step.dependOn(&run_pack.step);

    const bench_unpack_step = b.step("bench-unpacked", "Run unpacked (unpacking) benchmark");
    bench_unpack_step.dependOn(&run_unpack.step);

    const bench_check = b.addExecutable(.{
        .name = "bench-check",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/bench_check.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    b.installArtifact(bench_check);

    const run_bench_check = b.addRunArtifact(bench_check);
    run_bench_check.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_bench_check.addArgs(args);
    }

    const bench_check_step = b.step("bench-check", "Run benchmark regression checks");
    bench_check_step.dependOn(&run_bench_check.step);

    // RPC ping-pong example
    const rpc_pingpong_example = b.addExecutable(.{
        .name = "example-rpc-pingpong",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/rpc_pingpong.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "xev", .module = xev_module },
            },
        }),
    });

    const run_rpc_pingpong = b.addRunArtifact(rpc_pingpong_example);
    if (b.args) |args| {
        run_rpc_pingpong.addArgs(args);
    }

    const example_rpc_step = b.step("example-rpc", "Run RPC ping-pong example");
    example_rpc_step.dependOn(&run_rpc_pingpong.step);

    // Zig e2e RPC hooks
    const e2e_zig_client = b.addExecutable(.{
        .name = "e2e-zig-client",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/e2e/zig/main_client.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "xev", .module = xev_module },
            },
        }),
    });

    const run_e2e_zig_client = b.addRunArtifact(e2e_zig_client);
    if (b.args) |args| {
        run_e2e_zig_client.addArgs(args);
    }

    const e2e_zig_client_step = b.step("e2e-zig-client", "Run Zig RPC e2e client hook");
    e2e_zig_client_step.dependOn(&run_e2e_zig_client.step);

    const e2e_zig_server = b.addExecutable(.{
        .name = "e2e-zig-server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/e2e/zig/main_server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "xev", .module = xev_module },
            },
        }),
    });

    const run_e2e_zig_server = b.addRunArtifact(e2e_zig_server);
    if (b.args) |args| {
        run_e2e_zig_server.addArgs(args);
    }

    const e2e_zig_server_step = b.step("e2e-zig-server", "Run Zig RPC e2e server hook");
    e2e_zig_server_step.dependOn(&run_e2e_zig_server.step);

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

    const codegen_rpc_nested_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/codegen_rpc_nested_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_codegen_rpc_nested_tests = b.addRunArtifact(codegen_rpc_nested_tests);

    const codegen_generated_runtime_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/codegen_generated_runtime_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_codegen_generated_runtime_tests = b.addRunArtifact(codegen_generated_runtime_tests);

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

    // capnp_test vendor fixtures
    const capnp_test_vendor_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/capnp_test_vendor_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_capnp_test_vendor_tests = b.addRunArtifact(capnp_test_vendor_tests);

    // Schema validation + canonicalization tests
    const schema_validation_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/schema_validation_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_schema_validation_tests = b.addRunArtifact(schema_validation_tests);

    // RPC framing tests
    const rpc_framing_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/rpc_framing_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_rpc_framing_tests = b.addRunArtifact(rpc_framing_tests);

    // RPC cap table encoding tests
    const rpc_cap_table_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/rpc_cap_table_encode_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_rpc_cap_table_tests = b.addRunArtifact(rpc_cap_table_tests);

    // RPC promised answer transform tests
    const rpc_promised_answer_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/rpc_promised_answer_transform_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_rpc_promised_answer_tests = b.addRunArtifact(rpc_promised_answer_tests);

    // RPC protocol/cap table tests
    const rpc_protocol_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/rpc_protocol_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_rpc_protocol_tests = b.addRunArtifact(rpc_protocol_tests);

    // RPC peer behavior tests
    const rpc_peer_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/rpc_peer_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_rpc_peer_tests = b.addRunArtifact(rpc_peer_tests);

    // Test step runs all tests
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_main_tests.step);
    test_step.dependOn(&run_message_tests.step);
    test_step.dependOn(&run_codegen_tests.step);
    test_step.dependOn(&run_codegen_defaults_tests.step);
    test_step.dependOn(&run_codegen_annotations_tests.step);
    test_step.dependOn(&run_codegen_rpc_nested_tests.step);
    test_step.dependOn(&run_codegen_generated_runtime_tests.step);
    test_step.dependOn(&run_integration_tests.step);
    test_step.dependOn(&run_interop_tests.step);
    test_step.dependOn(&run_interop_roundtrip_tests.step);
    test_step.dependOn(&run_real_world_person_tests.step);
    test_step.dependOn(&run_real_world_addressbook_tests.step);
    test_step.dependOn(&run_union_tests.step);
    test_step.dependOn(&run_capnp_testdata_tests.step);
    test_step.dependOn(&run_capnp_test_vendor_tests.step);
    test_step.dependOn(&run_schema_validation_tests.step);
    test_step.dependOn(&run_rpc_framing_tests.step);
    test_step.dependOn(&run_rpc_cap_table_tests.step);
    test_step.dependOn(&run_rpc_promised_answer_tests.step);
    test_step.dependOn(&run_rpc_protocol_tests.step);
    test_step.dependOn(&run_rpc_peer_tests.step);

    // Individual test steps
    const test_message_step = b.step("test-message", "Run message serialization tests");
    test_message_step.dependOn(&run_message_tests.step);

    const test_codegen_step = b.step("test-codegen", "Run code generation tests");
    test_codegen_step.dependOn(&run_codegen_tests.step);
    test_codegen_step.dependOn(&run_codegen_defaults_tests.step);
    test_codegen_step.dependOn(&run_codegen_annotations_tests.step);
    test_codegen_step.dependOn(&run_codegen_rpc_nested_tests.step);
    test_codegen_step.dependOn(&run_codegen_generated_runtime_tests.step);

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

    const test_capnp_test_vendor_step = b.step("test-capnp-test-vendor", "Run capnp_test vendor fixtures");
    test_capnp_test_vendor_step.dependOn(&run_capnp_test_vendor_tests.step);

    const test_schema_validation_step = b.step("test-schema-validation", "Run schema validation + canonicalization tests");
    test_schema_validation_step.dependOn(&run_schema_validation_tests.step);

    const test_rpc_step = b.step("test-rpc", "Run RPC framing tests");
    test_rpc_step.dependOn(&run_rpc_framing_tests.step);
    test_rpc_step.dependOn(&run_rpc_cap_table_tests.step);
    test_rpc_step.dependOn(&run_rpc_promised_answer_tests.step);
    test_rpc_step.dependOn(&run_rpc_protocol_tests.step);
    test_rpc_step.dependOn(&run_rpc_peer_tests.step);

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
