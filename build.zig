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

/// Create a QUIC-only test step that imports capnpc-zig and quic_zig.
fn addQuicLibTest(
    b: *std.Build,
    path: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    lib_module: *std.Build.Module,
    quic_zig_module: *std.Build.Module,
) *std.Build.Step {
    const t = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(path),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "quic_zig", .module = quic_zig_module },
            },
        }),
    });
    return &b.addRunArtifact(t).step;
}

fn addMainTest(
    b: *std.Build,
    path: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step.Run {
    const t = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(path),
            .target = target,
            .optimize = optimize,
        }),
    });
    return b.addRunArtifact(t);
}

fn addQuicImport(module: *std.Build.Module, quic_zig_module: ?*std.Build.Module) void {
    if (quic_zig_module) |m| module.addImport("quic_zig", m);
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const enable_quic = b.option(
        bool,
        "quic",
        "Enable quic-zig-backed QUIC RPC transport (default: false)",
    ) orelse false;
    const lib_root = if (enable_quic) "src/lib_quic.zig" else "src/lib.zig";

    // Keep the normal module graph free of quic-zig/BoringSSL. The dependency
    // is declared in build.zig.zon for opt-in builds, but only resolved when
    // callers explicitly request the QUIC transport surface.
    const quic_zig_module: ?*std.Build.Module = if (enable_quic) blk: {
        const quic_zig_dep = b.dependency("quic_zig", .{
            .target = target,
            .optimize = optimize,
        });
        break :blk quic_zig_dep.module("quic_zig");
    } else null;

    // Selects which std.Io backend RPC entry points should construct. See
    // src/io_backend.zig for the full list of accepted spellings; the
    // default `process_init` reuses the std.Io that std.process.Init
    // already provides. The `evented` selector constructs std.Io.Evented on
    // targets where Zig exposes it, and fails clearly on unsupported targets.
    const io_backend_kind = b.option(
        []const u8,
        "io-backend",
        "Io backend used by RPC entry points: process_init|threaded|evented (default: process_init)",
    ) orelse "process_init";

    const io_backend_options = b.addOptions();
    io_backend_options.addOption([]const u8, "kind", io_backend_kind);
    const io_backend_options_module = io_backend_options.createModule();

    // Create the library module
    const lib_module = b.addModule("capnpc-zig", .{
        .root_source_file = b.path(lib_root),
        .target = target,
        .optimize = optimize,
        .imports = &.{},
    });
    lib_module.addImport("capnpc-zig", lib_module);
    addQuicImport(lib_module, quic_zig_module);

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

    const docs_module = b.createModule(.{
        .root_source_file = b.path(lib_root),
        .target = target,
        .optimize = optimize,
        .imports = &.{},
    });
    docs_module.addImport("capnpc-zig", docs_module);
    addQuicImport(docs_module, quic_zig_module);
    const docs_obj = b.addObject(.{
        .name = "capnpc-zig-docs",
        .root_module = docs_module,
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

    const run_bench_check = b.addRunArtifact(bench_check);
    if (b.args) |args| {
        run_bench_check.addArgs(args);
    }

    const bench_check_step = b.step("bench-check", "Run benchmark regression checks");
    bench_check_step.dependOn(&run_bench_check.step);

    const hardening_gate = b.addExecutable(.{
        .name = "hardening-gate",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/hardening_gate.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_hardening_gate = b.addRunArtifact(hardening_gate);
    const hardening_step = b.step("hardening", "Run static hardening gates");
    hardening_step.dependOn(&run_hardening_gate.step);

    const docs_examples_smoke = b.addExecutable(.{
        .name = "docs-examples-smoke",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/docs_examples_smoke.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_docs_examples_smoke = b.addRunArtifact(docs_examples_smoke);
    const docs_smoke_step = b.step("docs-smoke", "Run documentation and examples smoke checks");
    docs_smoke_step.dependOn(&run_docs_examples_smoke.step);
    const run_rpc_getting_started_snippet_tests = addLibTest(b, "tests/docs/rpc_getting_started_snippets_test.zig", target, optimize, lib_module);
    const run_serialization_getting_started_snippet_tests = addLibTest(b, "tests/docs/serialization_getting_started_snippets_test.zig", target, optimize, lib_module);
    const run_rpc_events_snippet_tests = addLibTest(b, "tests/docs/rpc_events_snippets_test.zig", target, optimize, lib_module);
    const run_quic_transport_disabled_snippet_tests: ?*std.Build.Step = if (!enable_quic)
        addLibTest(b, "tests/docs/quic_transport_disabled_snippets_test.zig", target, optimize, lib_module)
    else
        null;
    const run_quic_transport_snippet_tests: ?*std.Build.Step = if (enable_quic)
        addQuicLibTest(b, "tests/docs/quic_transport_snippets_test.zig", target, optimize, lib_module, quic_zig_module.?)
    else
        null;
    const test_docs_snippets_step = b.step("test-docs-snippets", "Compile documentation snippet fixtures");
    test_docs_snippets_step.dependOn(run_rpc_getting_started_snippet_tests);
    test_docs_snippets_step.dependOn(run_serialization_getting_started_snippet_tests);
    test_docs_snippets_step.dependOn(run_rpc_events_snippet_tests);
    if (run_quic_transport_disabled_snippet_tests) |step| test_docs_snippets_step.dependOn(step);
    const test_docs_snippets_quic_step = b.step("test-docs-snippets-quic", "Compile QUIC documentation snippet fixtures (requires -Dquic=true)");
    if (run_quic_transport_snippet_tests) |step| test_docs_snippets_quic_step.dependOn(step);
    docs_smoke_step.dependOn(test_docs_snippets_step);
    if (run_quic_transport_snippet_tests != null) docs_smoke_step.dependOn(test_docs_snippets_quic_step);

    // RPC ping-pong example
    const rpc_pingpong_example = b.addExecutable(.{
        .name = "example-rpc-pingpong",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/rpc_pingpong.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "io_backend_options", .module = io_backend_options_module },
            },
        }),
    });

    const run_rpc_pingpong = b.addRunArtifact(rpc_pingpong_example);
    if (b.args) |args| {
        run_rpc_pingpong.addArgs(args);
    }

    const example_rpc_step = b.step("example-rpc", "Run RPC ping-pong example");
    example_rpc_step.dependOn(&run_rpc_pingpong.step);

    const install_rpc_pingpong_step = b.step("example-rpc-install", "Build RPC ping-pong example (install only)");
    install_rpc_pingpong_step.dependOn(&b.addInstallArtifact(rpc_pingpong_example, .{}).step);

    // Zig e2e RPC hooks
    const e2e_zig_client = b.addExecutable(.{
        .name = "e2e-zig-client",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/e2e/zig/main_client.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "io_backend_options", .module = io_backend_options_module },
            },
        }),
    });

    const run_e2e_zig_client = b.addRunArtifact(e2e_zig_client);
    if (b.args) |args| {
        run_e2e_zig_client.addArgs(args);
    }

    const e2e_zig_client_step = b.step("e2e-zig-client", "Run Zig RPC e2e client hook");
    e2e_zig_client_step.dependOn(&run_e2e_zig_client.step);

    const install_e2e_zig_client_step = b.step("e2e-zig-client-install", "Build Zig RPC e2e client (install only)");
    install_e2e_zig_client_step.dependOn(&b.addInstallArtifact(e2e_zig_client, .{}).step);

    const e2e_zig_server = b.addExecutable(.{
        .name = "e2e-zig-server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/e2e/zig/main_server.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "io_backend_options", .module = io_backend_options_module },
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

    const lib_tests_module = b.createModule(.{
        .root_source_file = b.path(lib_root),
        .target = target,
        .optimize = optimize,
        .imports = &.{},
    });
    addQuicImport(lib_tests_module, quic_zig_module);
    const lib_tests = b.addTest(.{
        .root_module = lib_tests_module,
    });

    const run_lib_tests = b.addRunArtifact(lib_tests);

    // Serialization tests
    const run_message_tests = addLibTest(b, "tests/serialization/message_test.zig", target, optimize, lib_module);
    const run_serialization_fuzz_tests = addLibTest(b, "tests/serialization/serialization_fuzz_test.zig", target, optimize, lib_module);
    const run_fuzz_smoke_tests = addLibTest(b, "tests/hardening/fuzz_smoke_test.zig", target, optimize, lib_module);
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

    // RPC tests (domain-organized)
    const run_rpc_framing_tests = addLibTest(b, "tests/rpc/wire/rpc_framing_test.zig", target, optimize, lib_module);
    const run_rpc_cap_table_tests = addLibTest(b, "tests/rpc/caps/rpc_cap_table_encode_test.zig", target, optimize, lib_module);
    const run_rpc_caps_release_and_failure_tests = addLibTest(b, "tests/rpc/caps/rpc_release_and_failure_test.zig", target, optimize, lib_module);
    const run_rpc_protocol_tests = addLibTest(b, "tests/rpc/wire/rpc_protocol_test.zig", target, optimize, lib_module);
    const run_rpc_promised_answer_tests = addLibTest(b, "tests/rpc/promises/rpc_promised_answer_transform_test.zig", target, optimize, lib_module);
    const run_rpc_peer_return_send_helpers_tests = addLibTest(b, "tests/rpc/promises/rpc_peer_return_send_helpers_test.zig", target, optimize, lib_module);
    const run_rpc_host_peer_tests = addLibTest(b, "tests/rpc/integration/rpc_host_peer_test.zig", target, optimize, lib_module);
    const run_rpc_peer_transport_callbacks_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_transport_callbacks_test.zig", target, optimize, lib_module);
    const run_rpc_peer_transport_state_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_transport_state_test.zig", target, optimize, lib_module);
    const run_rpc_peer_cleanup_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_cleanup_test.zig", target, optimize, lib_module);
    const run_rpc_connection_failure_tests = addLibTest(b, "tests/rpc/transport/tcp/rpc_connection_failure_test.zig", target, optimize, lib_module);
    const run_rpc_worker_pool_tests = addLibTest(b, "tests/rpc/integration/rpc_worker_pool_test.zig", target, optimize, lib_module);
    const run_rpc_events_tests = addLibTest(b, "tests/rpc/transport/rpc_events_test.zig", target, optimize, lib_module);
    const run_rpc_quic_transport_tests: ?*std.Build.Step = if (enable_quic)
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_transport_test.zig", target, optimize, lib_module, quic_zig_module.?)
    else
        null;
    const run_rpc_quic_public_api_tests: ?*std.Build.Step = if (enable_quic)
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_public_api_test.zig", target, optimize, lib_module, quic_zig_module.?)
    else
        null;
    const run_rpc_quic_connection_internal_tests: ?*std.Build.Step = if (enable_quic)
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_connection_internal_test.zig", target, optimize, lib_module, quic_zig_module.?)
    else
        null;
    const run_rpc_raw_frame_security_tests = addLibTest(b, "tests/rpc/transport/rpc_raw_frame_security_test.zig", target, optimize, lib_module);
    const run_rpc_peer_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_test.zig", target, optimize, lib_module);
    const run_rpc_peer_from_peer_zig_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_from_peer_zig_test.zig", target, optimize, lib_module);
    const run_rpc_peer_semantic_helpers_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_semantic_helpers_test.zig", target, optimize, lib_module);
    const run_rpc_peer_release_and_failure_tests = addLibTest(b, "tests/rpc/peer/rpc_release_and_failure_test.zig", target, optimize, lib_module);
    const run_rpc_concurrent_calls_tests = addLibTest(b, "tests/rpc/peer/rpc_concurrent_calls_test.zig", target, optimize, lib_module);

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

    const test_fuzz_smoke_step = b.step("test-fuzz-smoke", "Run deterministic hardening fuzz/smoke coverage");
    test_fuzz_smoke_step.dependOn(run_fuzz_smoke_tests);

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

    const test_rpc_wire_step = b.step("test-rpc-wire", "Run RPC wire framing/protocol tests");
    test_rpc_wire_step.dependOn(run_rpc_framing_tests);
    test_rpc_wire_step.dependOn(run_rpc_protocol_tests);

    const test_rpc_caps_step = b.step("test-rpc-caps", "Run RPC capability table tests");
    test_rpc_caps_step.dependOn(run_rpc_cap_table_tests);
    test_rpc_caps_step.dependOn(run_rpc_caps_release_and_failure_tests);

    const test_rpc_promises_step = b.step("test-rpc-promises", "Run RPC promise/pipelining tests");
    test_rpc_promises_step.dependOn(run_rpc_promised_answer_tests);
    test_rpc_promises_step.dependOn(run_rpc_peer_return_send_helpers_tests);

    const test_rpc_transport_step = b.step("test-rpc-transport", "Run RPC TCP/raw-frame transport tests");
    test_rpc_transport_step.dependOn(run_rpc_connection_failure_tests);
    test_rpc_transport_step.dependOn(run_rpc_events_tests);
    test_rpc_transport_step.dependOn(run_rpc_raw_frame_security_tests);

    const test_rpc_quic_step = b.step("test-rpc-quic", "Run quic-zig-backed QUIC RPC transport tests (requires -Dquic=true)");
    if (run_rpc_quic_transport_tests) |step| test_rpc_quic_step.dependOn(step);
    if (run_rpc_quic_public_api_tests) |step| test_rpc_quic_step.dependOn(step);
    if (run_rpc_quic_connection_internal_tests) |step| test_rpc_quic_step.dependOn(step);

    const test_rpc_peer_step = b.step("test-rpc-peer", "Run RPC peer semantics tests");
    test_rpc_peer_step.dependOn(run_rpc_peer_transport_callbacks_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_transport_state_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_cleanup_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_from_peer_zig_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_semantic_helpers_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_release_and_failure_tests);
    test_rpc_peer_step.dependOn(run_rpc_concurrent_calls_tests);

    const test_rpc_integration_step = b.step("test-rpc-integration", "Run RPC integration tests");
    test_rpc_integration_step.dependOn(run_rpc_host_peer_tests);
    test_rpc_integration_step.dependOn(run_rpc_worker_pool_tests);

    const test_rpc_step = b.step("test-rpc", "Run all RPC tests");
    test_rpc_step.dependOn(test_rpc_wire_step);
    test_rpc_step.dependOn(test_rpc_caps_step);
    test_rpc_step.dependOn(test_rpc_promises_step);
    test_rpc_step.dependOn(test_rpc_transport_step);
    test_rpc_step.dependOn(test_rpc_quic_step);
    test_rpc_step.dependOn(test_rpc_peer_step);
    test_rpc_step.dependOn(test_rpc_integration_step);

    const test_e2e_security_step = b.step("test-e2e-security", "Run raw-frame RPC security e2e tests");
    test_e2e_security_step.dependOn(run_rpc_raw_frame_security_tests);

    const test_wasm_host_step = b.step("test-wasm-host", "Run wasm host ABI tests");
    test_wasm_host_step.dependOn(&run_wasm_host_abi_tests.step);

    const test_resource_budgets_step = b.step("test-resource-budgets", "Run resource budget regression tests");
    test_resource_budgets_step.dependOn(&run_main_tests.step);
    test_resource_budgets_step.dependOn(run_message_tests);
    test_resource_budgets_step.dependOn(run_serialization_fuzz_tests);
    test_resource_budgets_step.dependOn(run_fuzz_smoke_tests);
    test_resource_budgets_step.dependOn(run_codegen_tests);
    test_resource_budgets_step.dependOn(run_schema_validation_tests);
    test_resource_budgets_step.dependOn(run_rpc_framing_tests);
    test_resource_budgets_step.dependOn(run_rpc_connection_failure_tests);
    if (run_rpc_quic_transport_tests) |step| test_resource_budgets_step.dependOn(step);
    if (run_rpc_quic_connection_internal_tests) |step| test_resource_budgets_step.dependOn(step);
    test_resource_budgets_step.dependOn(run_rpc_raw_frame_security_tests);
    test_resource_budgets_step.dependOn(&run_wasm_host_abi_tests.step);

    const test_oom_step = b.step("test-oom", "Run OOM and failing allocator regression tests");
    test_oom_step.dependOn(&run_main_tests.step);
    test_oom_step.dependOn(run_message_tests);
    test_oom_step.dependOn(run_fuzz_smoke_tests);
    test_oom_step.dependOn(run_codegen_tests);
    test_oom_step.dependOn(run_codegen_defaults_tests);
    test_oom_step.dependOn(run_rpc_framing_tests);
    test_oom_step.dependOn(run_rpc_connection_failure_tests);
    test_oom_step.dependOn(run_rpc_raw_frame_security_tests);
    test_oom_step.dependOn(&run_wasm_host_abi_tests.step);

    const test_lib_step = b.step("test-lib", "Run source module tests from src/lib.zig");
    test_lib_step.dependOn(&run_lib_tests.step);

    const release_safe_optimize: std.builtin.OptimizeMode = .ReleaseSafe;
    const release_safe_quic_zig_module: ?*std.Build.Module = if (enable_quic) blk: {
        const release_safe_quic_zig_dep = b.dependency("quic_zig", .{
            .target = target,
            .optimize = release_safe_optimize,
        });
        break :blk release_safe_quic_zig_dep.module("quic_zig");
    } else null;
    const release_safe_lib_module = b.addModule("capnpc-zig-release-safe", .{
        .root_source_file = b.path(lib_root),
        .target = target,
        .optimize = release_safe_optimize,
        .imports = &.{},
    });
    release_safe_lib_module.addImport("capnpc-zig", release_safe_lib_module);
    addQuicImport(release_safe_lib_module, release_safe_quic_zig_module);

    const run_release_safe_main_tests = addMainTest(b, "src/main.zig", target, release_safe_optimize);
    const run_release_safe_message_tests = addLibTest(b, "tests/serialization/message_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_serialization_fuzz_tests = addLibTest(b, "tests/serialization/serialization_fuzz_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_fuzz_smoke_tests = addLibTest(b, "tests/hardening/fuzz_smoke_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_codegen_tests = addLibTest(b, "tests/serialization/codegen_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_codegen_defaults_tests = addLibTest(b, "tests/serialization/codegen_defaults_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_schema_validation_tests = addLibTest(b, "tests/serialization/schema_validation_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_rpc_framing_tests = addLibTest(b, "tests/rpc/wire/rpc_framing_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_rpc_connection_failure_tests = addLibTest(b, "tests/rpc/transport/tcp/rpc_connection_failure_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_rpc_quic_transport_tests: ?*std.Build.Step = if (enable_quic)
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_transport_test.zig", target, release_safe_optimize, release_safe_lib_module, release_safe_quic_zig_module.?)
    else
        null;
    const run_release_safe_rpc_quic_connection_internal_tests: ?*std.Build.Step = if (enable_quic)
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_connection_internal_test.zig", target, release_safe_optimize, release_safe_lib_module, release_safe_quic_zig_module.?)
    else
        null;
    const run_release_safe_rpc_raw_frame_security_tests = addLibTest(b, "tests/rpc/transport/rpc_raw_frame_security_test.zig", target, release_safe_optimize, release_safe_lib_module);

    const test_release_safe_step = b.step("test-release-safe", "Run key hardening gates under ReleaseSafe");
    test_release_safe_step.dependOn(&run_release_safe_main_tests.step);
    test_release_safe_step.dependOn(run_release_safe_message_tests);
    test_release_safe_step.dependOn(run_release_safe_serialization_fuzz_tests);
    test_release_safe_step.dependOn(run_release_safe_fuzz_smoke_tests);
    test_release_safe_step.dependOn(run_release_safe_codegen_tests);
    test_release_safe_step.dependOn(run_release_safe_codegen_defaults_tests);
    test_release_safe_step.dependOn(run_release_safe_schema_validation_tests);
    test_release_safe_step.dependOn(run_release_safe_rpc_framing_tests);
    test_release_safe_step.dependOn(run_release_safe_rpc_connection_failure_tests);
    if (run_release_safe_rpc_quic_transport_tests) |step| test_release_safe_step.dependOn(step);
    if (run_release_safe_rpc_quic_connection_internal_tests) |step| test_release_safe_step.dependOn(step);
    test_release_safe_step.dependOn(run_release_safe_rpc_raw_frame_security_tests);

    // Test step runs all tests
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(test_serialization_step);
    test_step.dependOn(test_rpc_step);
    test_step.dependOn(test_wasm_host_step);
    test_step.dependOn(test_fuzz_smoke_step);
    test_step.dependOn(test_resource_budgets_step);
    test_step.dependOn(test_oom_step);

    // Check step (compile visible user-facing targets without running them).
    const check_step = b.step("check", "Check for compilation errors");
    check_step.dependOn(&exe.step);
    check_step.dependOn(&lib_tests.step);
    check_step.dependOn(&main_tests.step);
    check_step.dependOn(&rpc_pingpong_example.step);
    check_step.dependOn(&e2e_zig_client.step);
    check_step.dependOn(&e2e_zig_server.step);
    check_step.dependOn(&wasm_host_module.step);
    check_step.dependOn(docs_smoke_step);
}
