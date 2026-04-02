const std = @import("std");

/// Create a test step that imports capnpc-zig and return its run step.
fn addLibTest(
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
    return &b.addRunArtifact(t).step;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create the library module
    const lib_module = b.addModule("capnpc-zig", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{},
    });
    lib_module.addImport("capnpc-zig", lib_module);

    const core_module = b.addModule("capnpc-zig-core", .{
        .root_source_file = b.path("src/lib_core.zig"),
        .target = target,
        .optimize = optimize,
    });
    core_module.addImport("capnpc-zig", core_module);

    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
    });

    const wasm_example_schema_module = b.addModule("capnp-wasm-example-schema", .{
        .root_source_file = b.path("src/wasm/generated/example.zig"),
        .target = wasm_target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnpc-zig", .module = core_module },
        },
    });

    const wasm_host_module = b.addExecutable(.{
        .name = "capnp_wasm_host",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/wasm/capnp_host_abi.zig"),
            .target = wasm_target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig-core", .module = core_module },
                .{ .name = "capnpc-zig", .module = core_module },
                .{ .name = "capnp-wasm-example-schema", .module = wasm_example_schema_module },
            },
        }),
    });
    wasm_host_module.entry = .disabled;
    wasm_host_module.rdynamic = true;
    wasm_host_module.export_memory = true;
    wasm_host_module.initial_memory = 4 * 1024 * 1024;
    wasm_host_module.max_memory = 64 * 1024 * 1024;
    const install_wasm_host = b.addInstallArtifact(wasm_host_module, .{});

    const wasm_host_step = b.step("wasm-host", "Build host-neutral WebAssembly ABI module");
    wasm_host_step.dependOn(&install_wasm_host.step);

    const wasm_deno_step = b.step("wasm-deno", "Compatibility alias for wasm-host");
    wasm_deno_step.dependOn(&install_wasm_host.step);

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

    const docs_obj = b.addObject(.{
        .name = "capnpc-zig-docs",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lib.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{},
        }),
    });
    const install_docs = b.addInstallDirectory(.{
        .source_dir = docs_obj.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });
    const docs_step = b.step("docs", "Generate API documentation");
    docs_step.dependOn(&install_docs.step);

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
            },
        }),
    });

    const run_e2e_zig_server = b.addRunArtifact(e2e_zig_server);
    if (b.args) |args| {
        run_e2e_zig_server.addArgs(args);
    }

    const e2e_zig_server_step = b.step("e2e-zig-server", "Run Zig RPC e2e server hook");
    e2e_zig_server_step.dependOn(&run_e2e_zig_server.step);

    const install_e2e_zig_server_step = b.step("e2e-zig-server-install", "Build Zig RPC e2e server (install only)");
    install_e2e_zig_server_step.dependOn(&b.addInstallArtifact(e2e_zig_server, .{}).step);

    // Unit tests for main
    const main_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_main_tests = b.addRunArtifact(main_tests);

    const lib_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/lib.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{},
        }),
    });

    const run_lib_tests = b.addRunArtifact(lib_tests);

    // Serialization tests
    const run_message_tests = addLibTest(b, "tests/serialization/message_test.zig", target, optimize, lib_module);
    const run_serialization_fuzz_tests = addLibTest(b, "tests/serialization/serialization_fuzz_test.zig", target, optimize, lib_module);
    const run_codegen_tests = addLibTest(b, "tests/serialization/codegen_test.zig", target, optimize, lib_module);
    const run_codegen_defaults_tests = addLibTest(b, "tests/serialization/codegen_defaults_test.zig", target, optimize, lib_module);
    const run_codegen_annotations_tests = addLibTest(b, "tests/serialization/codegen_annotations_test.zig", target, optimize, lib_module);
    const run_codegen_rpc_nested_tests = addLibTest(b, "tests/serialization/codegen_rpc_nested_test.zig", target, optimize, lib_module);
    const run_codegen_streaming_tests = addLibTest(b, "tests/serialization/codegen_streaming_test.zig", target, optimize, lib_module);
    const run_codegen_generated_runtime_tests = addLibTest(b, "tests/serialization/codegen_generated_runtime_test.zig", target, optimize, lib_module);
    const run_integration_tests = addLibTest(b, "tests/serialization/integration_test.zig", target, optimize, lib_module);
    const run_interop_tests = addLibTest(b, "tests/serialization/interop_test.zig", target, optimize, lib_module);
    const run_interop_roundtrip_tests = addLibTest(b, "tests/serialization/interop_roundtrip_test.zig", target, optimize, lib_module);
    const run_real_world_person_tests = addLibTest(b, "tests/serialization/real_world_person_test.zig", target, optimize, lib_module);
    const run_real_world_addressbook_tests = addLibTest(b, "tests/serialization/real_world_addressbook_test.zig", target, optimize, lib_module);
    const run_union_tests = addLibTest(b, "tests/serialization/union_test.zig", target, optimize, lib_module);
    const run_union_runtime_tests = addLibTest(b, "tests/serialization/union_runtime_test.zig", target, optimize, lib_module);
    const run_codegen_union_group_tests = addLibTest(b, "tests/serialization/codegen_union_group_test.zig", target, optimize, lib_module);
    const run_codegen_golden_tests = addLibTest(b, "tests/serialization/codegen_golden_test.zig", target, optimize, lib_module);
    const run_capnp_testdata_tests = addLibTest(b, "tests/serialization/capnp_testdata_test.zig", target, optimize, lib_module);
    const run_capnp_test_vendor_tests = addLibTest(b, "tests/serialization/capnp_test_vendor_test.zig", target, optimize, lib_module);
    const run_schema_validation_tests = addLibTest(b, "tests/serialization/schema_validation_test.zig", target, optimize, lib_module);

    // RPC tests (level 0–3)
    const run_rpc_framing_tests = addLibTest(b, "tests/rpc/level0/rpc_framing_test.zig", target, optimize, lib_module);
    const run_rpc_cap_table_tests = addLibTest(b, "tests/rpc/level0/rpc_cap_table_encode_test.zig", target, optimize, lib_module);
    const run_rpc_release_and_failure_level0_tests = addLibTest(b, "tests/rpc/level0/rpc_release_and_failure_test.zig", target, optimize, lib_module);
    const run_rpc_protocol_tests = addLibTest(b, "tests/rpc/level0/rpc_protocol_test.zig", target, optimize, lib_module);
    const run_rpc_promised_answer_tests = addLibTest(b, "tests/rpc/level1/rpc_promised_answer_transform_test.zig", target, optimize, lib_module);
    const run_rpc_peer_return_send_helpers_tests = addLibTest(b, "tests/rpc/level1/rpc_peer_return_send_helpers_test.zig", target, optimize, lib_module);
    const run_rpc_host_peer_tests = addLibTest(b, "tests/rpc/level2/rpc_host_peer_test.zig", target, optimize, lib_module);
    const run_rpc_peer_transport_callbacks_tests = addLibTest(b, "tests/rpc/level2/rpc_peer_transport_callbacks_test.zig", target, optimize, lib_module);
    const run_rpc_peer_transport_state_tests = addLibTest(b, "tests/rpc/level2/rpc_peer_transport_state_test.zig", target, optimize, lib_module);
    const run_rpc_peer_cleanup_tests = addLibTest(b, "tests/rpc/level2/rpc_peer_cleanup_test.zig", target, optimize, lib_module);
    const run_rpc_connection_failure_tests = addLibTest(b, "tests/rpc/level2/rpc_connection_failure_test.zig", target, optimize, lib_module);
    const run_rpc_worker_pool_tests = addLibTest(b, "tests/rpc/level2/rpc_worker_pool_test.zig", target, optimize, lib_module);
    const run_rpc_peer_tests = addLibTest(b, "tests/rpc/level3/rpc_peer_test.zig", target, optimize, lib_module);
    const run_rpc_peer_from_peer_zig_tests = addLibTest(b, "tests/rpc/level3/rpc_peer_from_peer_zig_test.zig", target, optimize, lib_module);
    const run_rpc_peer_control_from_peer_control_zig_tests = addLibTest(b, "tests/rpc/level3/rpc_peer_control_from_peer_control_zig_test.zig", target, optimize, lib_module);
    const run_rpc_release_and_failure_level3_tests = addLibTest(b, "tests/rpc/level3/rpc_release_and_failure_test.zig", target, optimize, lib_module);
    const run_rpc_concurrent_calls_tests = addLibTest(b, "tests/rpc/level3/rpc_concurrent_calls_test.zig", target, optimize, lib_module);

    const wasm_host_abi_test_module = b.createModule(.{
        .root_source_file = b.path("src/wasm/capnp_host_abi.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnpc-zig-core", .module = core_module },
            .{ .name = "capnpc-zig", .module = core_module },
        },
    });

    const rpc_fixture_tool_module = b.createModule(.{
        .root_source_file = b.path("tests/rpc/support/rpc_fixture_tool.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnpc-zig-core", .module = core_module },
        },
    });

    const wasm_host_abi_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/wasm_host_abi_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig-core", .module = core_module },
                .{ .name = "capnpc-zig", .module = core_module },
                .{ .name = "capnp-wasm-host-abi", .module = wasm_host_abi_test_module },
                .{ .name = "rpc-fixture-tool", .module = rpc_fixture_tool_module },
            },
        }),
    });

    const run_wasm_host_abi_tests = b.addRunArtifact(wasm_host_abi_tests);

    // Individual test steps
    const test_message_step = b.step("test-message", "Run message serialization tests");
    test_message_step.dependOn(run_message_tests);
    test_message_step.dependOn(run_serialization_fuzz_tests);

    const test_codegen_step = b.step("test-codegen", "Run code generation tests");
    test_codegen_step.dependOn(run_codegen_tests);
    test_codegen_step.dependOn(run_codegen_defaults_tests);
    test_codegen_step.dependOn(run_codegen_annotations_tests);
    test_codegen_step.dependOn(run_codegen_rpc_nested_tests);
    test_codegen_step.dependOn(run_codegen_streaming_tests);
    test_codegen_step.dependOn(run_codegen_generated_runtime_tests);
    test_codegen_step.dependOn(run_codegen_union_group_tests);
    test_codegen_step.dependOn(run_codegen_golden_tests);

    const test_integration_step = b.step("test-integration", "Run integration tests");
    test_integration_step.dependOn(run_integration_tests);

    const test_interop_step = b.step("test-interop", "Run interop tests");
    test_interop_step.dependOn(run_interop_tests);
    test_interop_step.dependOn(run_interop_roundtrip_tests);

    const test_real_world_step = b.step("test-real-world", "Run real-world schema tests");
    test_real_world_step.dependOn(run_real_world_person_tests);
    test_real_world_step.dependOn(run_real_world_addressbook_tests);

    const test_union_step = b.step("test-union", "Run union tests");
    test_union_step.dependOn(run_union_tests);
    test_union_step.dependOn(run_union_runtime_tests);

    const test_capnp_testdata_step = b.step("test-capnp-testdata", "Run Cap'n Proto official testdata fixtures");
    test_capnp_testdata_step.dependOn(run_capnp_testdata_tests);

    const test_capnp_test_vendor_step = b.step("test-capnp-test-vendor", "Run capnp_test vendor fixtures");
    test_capnp_test_vendor_step.dependOn(run_capnp_test_vendor_tests);

    const test_schema_validation_step = b.step("test-schema-validation", "Run schema validation + canonicalization tests");
    test_schema_validation_step.dependOn(run_schema_validation_tests);

    const test_serialization_step = b.step("test-serialization", "Run serialization-oriented tests");
    test_serialization_step.dependOn(&run_main_tests.step);
    test_serialization_step.dependOn(&run_lib_tests.step);
    test_serialization_step.dependOn(run_message_tests);
    test_serialization_step.dependOn(run_serialization_fuzz_tests);
    test_serialization_step.dependOn(run_codegen_tests);
    test_serialization_step.dependOn(run_codegen_defaults_tests);
    test_serialization_step.dependOn(run_codegen_annotations_tests);
    test_serialization_step.dependOn(run_codegen_rpc_nested_tests);
    test_serialization_step.dependOn(run_codegen_streaming_tests);
    test_serialization_step.dependOn(run_codegen_generated_runtime_tests);
    test_serialization_step.dependOn(run_integration_tests);
    test_serialization_step.dependOn(run_interop_tests);
    test_serialization_step.dependOn(run_interop_roundtrip_tests);
    test_serialization_step.dependOn(run_real_world_person_tests);
    test_serialization_step.dependOn(run_real_world_addressbook_tests);
    test_serialization_step.dependOn(run_union_tests);
    test_serialization_step.dependOn(run_union_runtime_tests);
    test_serialization_step.dependOn(run_codegen_union_group_tests);
    test_serialization_step.dependOn(run_codegen_golden_tests);
    test_serialization_step.dependOn(run_capnp_testdata_tests);
    test_serialization_step.dependOn(run_capnp_test_vendor_tests);
    test_serialization_step.dependOn(run_schema_validation_tests);

    // Cumulative RPC levels:
    // - level0: framing/protocol/cap-table encoding
    // - level1: promise/pipelining primitives
    // - level2: runtime plumbing and transport integration
    // - level3: advanced peer semantics (provide/accept/join/third-party/disembargo)
    const test_rpc_level0_step = b.step("test-rpc-level0", "Run RPC level 0 tests (framing/protocol/cap-table)");
    test_rpc_level0_step.dependOn(run_rpc_framing_tests);
    test_rpc_level0_step.dependOn(run_rpc_protocol_tests);
    test_rpc_level0_step.dependOn(run_rpc_cap_table_tests);
    test_rpc_level0_step.dependOn(run_rpc_release_and_failure_level0_tests);

    const test_rpc_level1_step = b.step("test-rpc-level1", "Run RPC level 1 tests (promises/pipelining)");
    test_rpc_level1_step.dependOn(test_rpc_level0_step);
    test_rpc_level1_step.dependOn(run_rpc_promised_answer_tests);
    test_rpc_level1_step.dependOn(run_rpc_peer_return_send_helpers_tests);

    const test_rpc_level2_step = b.step("test-rpc-level2", "Run RPC level 2 tests (runtime plumbing)");
    test_rpc_level2_step.dependOn(test_rpc_level1_step);
    test_rpc_level2_step.dependOn(run_rpc_host_peer_tests);
    test_rpc_level2_step.dependOn(run_rpc_peer_transport_callbacks_tests);
    test_rpc_level2_step.dependOn(run_rpc_peer_transport_state_tests);
    test_rpc_level2_step.dependOn(run_rpc_peer_cleanup_tests);
    test_rpc_level2_step.dependOn(run_rpc_connection_failure_tests);
    test_rpc_level2_step.dependOn(run_rpc_worker_pool_tests);

    const test_rpc_level3_step = b.step("test-rpc-level3", "Run RPC level 3+ tests (advanced peer semantics)");
    test_rpc_level3_step.dependOn(test_rpc_level2_step);
    test_rpc_level3_step.dependOn(run_rpc_peer_tests);
    test_rpc_level3_step.dependOn(run_rpc_peer_from_peer_zig_tests);
    test_rpc_level3_step.dependOn(run_rpc_peer_control_from_peer_control_zig_tests);
    test_rpc_level3_step.dependOn(run_rpc_release_and_failure_level3_tests);
    test_rpc_level3_step.dependOn(run_rpc_concurrent_calls_tests);

    const test_rpc_step = b.step("test-rpc", "Run all RPC tests");
    test_rpc_step.dependOn(test_rpc_level3_step);

    const test_wasm_host_step = b.step("test-wasm-host", "Run wasm host ABI tests");
    test_wasm_host_step.dependOn(&run_wasm_host_abi_tests.step);

    const test_lib_step = b.step("test-lib", "Run source module tests from src/lib.zig");
    test_lib_step.dependOn(&run_lib_tests.step);

    // Test step runs all tests
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(test_serialization_step);
    test_step.dependOn(test_rpc_step);
    test_step.dependOn(test_wasm_host_step);

    // Check step (compile without linking) — reuse the main exe
    const check_step = b.step("check", "Check for compilation errors");
    check_step.dependOn(&exe.step);
}
