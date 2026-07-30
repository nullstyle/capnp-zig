const std = @import("std");

/// Compile steps for every test registered through `addLibTest`, collected
/// so `check-test-compile` can gate test compilation for cross targets
/// (e.g. `zig build check-test-compile -Dtarget=x86_64-windows` on a POSIX
/// host catches Windows test rot without a Windows runner).
var registered_test_compile_steps: std.ArrayList(*std.Build.Step) = .empty;

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
    registered_test_compile_steps.append(b.allocator, &t.step) catch @panic("OOM");
    return &b.addRunArtifact(t).step;
}

fn addPersistenceLibTest(
    b: *std.Build,
    path: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    lib_module: *std.Build.Module,
) *std.Build.Step {
    const harness_module = b.createModule(.{
        .root_source_file = b.path("tests/rpc/support/persistence_harness.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnpc-zig", .module = lib_module },
        },
    });
    const t = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path(path),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "persistence-test-harness", .module = harness_module },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &t.step) catch @panic("OOM");
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
    // is declared lazy in build.zig.zon so non-QUIC builds neither fetch it
    // nor compile its build.zig; it is only resolved when callers explicitly
    // request the QUIC transport surface.
    const quic_zig_module: ?*std.Build.Module = if (enable_quic) blk: {
        const quic_zig_dep = b.lazyDependency("quic_zig", .{
            .target = target,
            .optimize = optimize,
        }) orelse break :blk null;
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

    // The wasm host needs a wasm-targeted core module: mixing the host
    // `target` into the wasm exe's module graph breaks cross builds
    // (`zig build check-compile -Dtarget=...`).
    const core_module_wasm = b.createModule(.{
        .root_source_file = b.path("src/lib_core.zig"),
        .target = wasm_target,
        .optimize = optimize,
    });
    core_module_wasm.addImport("capnpc-zig", core_module_wasm);

    const wasm_example_schema_module = b.addModule("capnp-wasm-example-schema", .{
        .root_source_file = b.path("src/wasm/generated/example.zig"),
        .target = wasm_target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnpc-zig", .module = core_module_wasm },
        },
    });

    const wasm_host_module = b.addExecutable(.{
        .name = "capnp_wasm_host",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/wasm/capnp_host_abi.zig"),
            .target = wasm_target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig-core", .module = core_module_wasm },
                .{ .name = "capnpc-zig", .module = core_module_wasm },
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

    run_cmd.addPassthruArgs();

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
    run_ping_pong.addPassthruArgs();

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
    run_pack.addPassthruArgs();

    const run_unpack = b.addRunArtifact(pack_unpack_bench);
    run_unpack.addArgs(&.{ "--mode", "unpack" });
    run_unpack.addPassthruArgs();

    const bench_pack_step = b.step("bench-packed", "Run packed (packing) benchmark");
    bench_pack_step.dependOn(&run_pack.step);

    const bench_unpack_step = b.step("bench-unpacked", "Run unpacked (unpacking) benchmark");
    bench_unpack_step.dependOn(&run_unpack.step);

    // RPC round-trip benchmark (loopback TCP, warmed steady-state client)
    const rpc_round_trip_bench = b.addExecutable(.{
        .name = "bench-rpc",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/rpc_round_trip.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_rpc_round_trip = b.addRunArtifact(rpc_round_trip_bench);
    run_rpc_round_trip.addPassthruArgs();

    const bench_rpc_step = b.step("bench-rpc", "Run RPC round-trip benchmark (use -- --mode pipelined --inflight K)");
    bench_rpc_step.dependOn(&run_rpc_round_trip.step);

    // RPC soak harness (loopback TCP, chaos + deadline sessions)
    const soak_rpc = b.addExecutable(.{
        .name = "soak-rpc",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/soak_rpc.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_soak_rpc = b.addRunArtifact(soak_rpc);
    run_soak_rpc.addPassthruArgs();

    const soak_step = b.step("soak", "Run RPC soak harness (use -- --seconds N --workers N)");
    soak_step.dependOn(&run_soak_rpc.step);

    const bench_check = b.addExecutable(.{
        .name = "bench-check",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/bench_check.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_bench_check = b.addRunArtifact(bench_check);
    run_bench_check.addPassthruArgs();
    // bench-check spawns the benchmark binaries via the paths recorded in
    // bench/baselines.json (./zig-out/bin/...), so the step must install
    // them first — a fresh checkout (CI) has no zig-out.
    run_bench_check.step.dependOn(&b.addInstallArtifact(ping_pong_bench, .{}).step);
    run_bench_check.step.dependOn(&b.addInstallArtifact(pack_unpack_bench, .{}).step);
    run_bench_check.step.dependOn(&b.addInstallArtifact(rpc_round_trip_bench, .{}).step);

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
    // The RPC getting-started snippets compile against the REAL generated
    // modules (not hand-written mirrors), so codegen-surface drift breaks
    // the doc gate too.
    const docs_pingpong_module = b.createModule(.{
        .root_source_file = b.path("examples/pingpong.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = lib_module }},
    });
    const docs_matchmaking_module = b.createModule(.{
        .root_source_file = b.path("tests/e2e/zig/generated/matchmaking.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = lib_module }},
    });
    const rpc_getting_started_snippet_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/docs/rpc_getting_started_snippets_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "pingpong", .module = docs_pingpong_module },
                .{ .name = "matchmaking", .module = docs_matchmaking_module },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &rpc_getting_started_snippet_tests.step) catch @panic("OOM");
    const run_rpc_getting_started_snippet_tests = &b.addRunArtifact(rpc_getting_started_snippet_tests).step;
    const run_serialization_getting_started_snippet_tests = addLibTest(b, "tests/docs/serialization_getting_started_snippets_test.zig", target, optimize, lib_module);
    const run_rpc_events_snippet_tests = addLibTest(b, "tests/docs/rpc_events_snippets_test.zig", target, optimize, lib_module);
    const run_build_integration_snippet_tests = addLibTest(b, "tests/docs/build_integration_snippets_test.zig", target, optimize, lib_module);
    const run_troubleshooting_contracts_snippet_tests = addLibTest(b, "tests/docs/troubleshooting_contracts_snippets_test.zig", target, optimize, lib_module);
    const run_quic_transport_disabled_snippet_tests: ?*std.Build.Step = if (!enable_quic)
        addLibTest(b, "tests/docs/quic_transport_disabled_snippets_test.zig", target, optimize, lib_module)
    else
        null;
    const run_quic_transport_snippet_tests: ?*std.Build.Step = if (quic_zig_module) |qm|
        addQuicLibTest(b, "tests/docs/quic_transport_snippets_test.zig", target, optimize, lib_module, qm)
    else
        null;
    const test_docs_snippets_step = b.step("test-docs-snippets", "Compile documentation snippet fixtures");
    test_docs_snippets_step.dependOn(run_rpc_getting_started_snippet_tests);
    test_docs_snippets_step.dependOn(run_serialization_getting_started_snippet_tests);
    test_docs_snippets_step.dependOn(run_rpc_events_snippet_tests);
    test_docs_snippets_step.dependOn(run_build_integration_snippet_tests);
    test_docs_snippets_step.dependOn(run_troubleshooting_contracts_snippet_tests);
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
    run_rpc_pingpong.addPassthruArgs();

    const example_rpc_step = b.step("example-rpc", "Run RPC ping-pong example");
    example_rpc_step.dependOn(&run_rpc_pingpong.step);

    const install_rpc_pingpong_step = b.step("example-rpc-install", "Build RPC ping-pong example (install only)");
    install_rpc_pingpong_step.dependOn(&b.addInstallArtifact(rpc_pingpong_example, .{}).step);

    // Standalone serialization example (no RPC). Both the generated schema
    // code and the runtime are wired through capnpc-zig-core — the
    // serialization-only module, with no TCP/QUIC transport in the graph — to
    // demonstrate the RPC-free dependency path an adopter would copy.
    const serialization_demo_schema_module = b.createModule(.{
        .root_source_file = b.path("examples/addressbook.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = core_module }},
    });
    const serialization_demo_example = b.addExecutable(.{
        .name = "example-serialization-demo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/serialization_demo.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = core_module },
                .{ .name = "addressbook", .module = serialization_demo_schema_module },
            },
        }),
    });

    const run_serialization_demo = b.addRunArtifact(serialization_demo_example);
    run_serialization_demo.addPassthruArgs();

    const example_serialization_step = b.step("example-serialization", "Run standalone serialization example (no RPC)");
    example_serialization_step.dependOn(&run_serialization_demo.step);

    const install_serialization_demo_step = b.step("example-serialization-install", "Build standalone serialization example (install only)");
    install_serialization_demo_step.dependOn(&b.addInstallArtifact(serialization_demo_example, .{}).step);

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
    run_e2e_zig_client.addPassthruArgs();

    const e2e_zig_client_step = b.step("e2e-zig-client", "Run Zig RPC e2e client hook");
    e2e_zig_client_step.dependOn(&run_e2e_zig_client.step);

    const install_e2e_zig_client_step = b.step("e2e-zig-client-install", "Build Zig RPC e2e client (install only)");
    install_e2e_zig_client_step.dependOn(&b.addInstallArtifact(e2e_zig_client, .{}).step);

    const e2e_l3_l4_module = b.createModule(.{
        .root_source_file = b.path("tests/e2e/zig/generated/l3_l4_interop.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnpc-zig", .module = lib_module },
        },
    });

    const e2e_l3_cpp = b.addExecutable(.{
        .name = "e2e-l3-cpp",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/e2e_l3_cpp.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "io_backend_options", .module = io_backend_options_module },
                .{ .name = "l3_l4_interop", .module = e2e_l3_l4_module },
            },
        }),
    });

    const run_e2e_l3_cpp = b.addRunArtifact(e2e_l3_cpp);
    run_e2e_l3_cpp.addPassthruArgs();

    const e2e_l3_cpp_step = b.step("e2e-l3-cpp", "Run Zig/C++ L3 handoff e2e client hook");
    e2e_l3_cpp_step.dependOn(&run_e2e_l3_cpp.step);

    const install_e2e_l3_cpp_step = b.step("e2e-l3-cpp-install", "Build Zig/C++ L3 handoff e2e client (install only)");
    install_e2e_l3_cpp_step.dependOn(&b.addInstallArtifact(e2e_l3_cpp, .{}).step);

    const e2e_l4_zig = b.addExecutable(.{
        .name = "e2e-l4-zig",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/e2e_l4_zig.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "io_backend_options", .module = io_backend_options_module },
            },
        }),
    });

    const run_e2e_l4_zig = b.addRunArtifact(e2e_l4_zig);
    run_e2e_l4_zig.addPassthruArgs();

    const e2e_l4_zig_step = b.step("e2e-l4-zig", "Run Zig/Zig Experimental L4 Join e2e over loopback TCP");
    e2e_l4_zig_step.dependOn(&run_e2e_l4_zig.step);

    const install_e2e_l4_zig_step = b.step("e2e-l4-zig-install", "Build Zig/Zig Experimental L4 Join e2e (install only)");
    install_e2e_l4_zig_step.dependOn(&b.addInstallArtifact(e2e_l4_zig, .{}).step);

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
    run_e2e_zig_server.addPassthruArgs();

    const e2e_zig_server_step = b.step("e2e-zig-server", "Run Zig RPC e2e server hook");
    e2e_zig_server_step.dependOn(&run_e2e_zig_server.step);

    const install_e2e_zig_server_step = b.step("e2e-zig-server-install", "Build Zig RPC e2e server (install only)");
    install_e2e_zig_server_step.dependOn(&b.addInstallArtifact(e2e_zig_server, .{}).step);

    // Self-interop e2e: zig client vs zig server over loopback, no docker
    // and no reference toolchains — the cross-platform end-to-end gate
    // (Windows CI runners cannot run the Linux reference containers).
    const e2e_self = b.addExecutable(.{
        .name = "e2e-self",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/e2e_self.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_e2e_self = b.addRunArtifact(e2e_self);
    run_e2e_self.addArtifactArg(e2e_zig_server);
    run_e2e_self.addArtifactArg(e2e_zig_client);
    const e2e_self_step = b.step("e2e-self", "Run self-interop e2e (zig client vs zig server over loopback)");
    e2e_self_step.dependOn(&run_e2e_self.step);

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
    const run_toolchain_gate_tests = addLibTest(b, "tests/hardening/toolchain_gate_test.zig", target, optimize, lib_module);
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
    const run_rpc_tick_idle_tests = addLibTest(b, "tests/rpc/transport/tcp/rpc_tick_idle_test.zig", target, optimize, lib_module);
    const run_rpc_connection_teardown_tests = addLibTest(b, "tests/rpc/transport/tcp/rpc_connection_teardown_test.zig", target, optimize, lib_module);
    const run_rpc_client_session_tests = addLibTest(b, "tests/rpc/transport/tcp/rpc_client_session_test.zig", target, optimize, lib_module);
    const run_rpc_server_session_tests = addLibTest(b, "tests/rpc/transport/tcp/rpc_server_session_test.zig", target, optimize, lib_module);
    const run_rpc_quic_transport_tests: ?*std.Build.Step = if (quic_zig_module) |qm|
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_transport_test.zig", target, optimize, lib_module, qm)
    else
        null;
    const run_rpc_quic_public_api_tests: ?*std.Build.Step = if (quic_zig_module) |qm|
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_public_api_test.zig", target, optimize, lib_module, qm)
    else
        null;
    const run_rpc_quic_connection_internal_tests: ?*std.Build.Step = if (quic_zig_module) |qm|
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_connection_internal_test.zig", target, optimize, lib_module, qm)
    else
        null;
    const run_rpc_raw_frame_security_tests = addLibTest(b, "tests/rpc/transport/rpc_raw_frame_security_test.zig", target, optimize, lib_module);
    const run_rpc_peer_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_test.zig", target, optimize, lib_module);
    const run_rpc_peer_from_peer_zig_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_from_peer_zig_test.zig", target, optimize, lib_module);
    const run_rpc_reflected_resolve_disembargo_tests = addLibTest(b, "tests/rpc/peer/rpc_reflected_resolve_disembargo_test.zig", target, optimize, lib_module);
    const run_rpc_three_party_handoff_origination_tests = addLibTest(b, "tests/rpc/peer/rpc_three_party_handoff_origination_test.zig", target, optimize, lib_module);
    const run_rpc_three_party_handoff_pickup_tests = addLibTest(b, "tests/rpc/peer/rpc_three_party_handoff_pickup_test.zig", target, optimize, lib_module);
    const run_rpc_three_party_handoff_embargo_tests = addLibTest(b, "tests/rpc/peer/rpc_three_party_handoff_embargo_test.zig", target, optimize, lib_module);
    const run_rpc_handoff_export_pin_tests = addLibTest(b, "tests/rpc/peer/rpc_handoff_export_pin_test.zig", target, optimize, lib_module);
    const run_rpc_three_party_handoff_vatc_tests = addLibTest(b, "tests/rpc/peer/rpc_three_party_handoff_vatc_test.zig", target, optimize, lib_module);
    const run_rpc_three_party_handoff_redirected_return_tests = addLibTest(b, "tests/rpc/peer/rpc_three_party_handoff_redirected_return_test.zig", target, optimize, lib_module);
    const run_rpc_reflected_direct_handler_tests = addLibTest(b, "tests/rpc/peer/rpc_reflected_direct_handler_test.zig", target, optimize, lib_module);
    const run_rpc_answer_lifecycle_tests = addLibTest(b, "tests/rpc/peer/rpc_answer_lifecycle_test.zig", target, optimize, lib_module);
    const run_rpc_join_readiness_tests = addLibTest(b, "tests/rpc/peer/rpc_join_readiness_test.zig", target, optimize, lib_module);
    const run_rpc_peer_alloc_failure_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_alloc_failure_test.zig", target, optimize, lib_module);
    const run_rpc_peer_semantic_helpers_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_semantic_helpers_test.zig", target, optimize, lib_module);
    const run_rpc_peer_release_and_failure_tests = addLibTest(b, "tests/rpc/peer/rpc_release_and_failure_test.zig", target, optimize, lib_module);
    const run_rpc_return_release_param_caps_tests = addLibTest(b, "tests/rpc/peer/rpc_return_release_param_caps_test.zig", target, optimize, lib_module);
    const run_rpc_concurrent_calls_tests = addLibTest(b, "tests/rpc/peer/rpc_concurrent_calls_test.zig", target, optimize, lib_module);
    const run_rpc_deadline_tests = addLibTest(b, "tests/rpc/peer/rpc_deadline_test.zig", target, optimize, lib_module);
    const run_rpc_persistence_tests = addPersistenceLibTest(b, "tests/rpc/peer/rpc_persistence_test.zig", target, optimize, lib_module);
    const run_rpc_persistence_reconnect_tests = addPersistenceLibTest(b, "tests/rpc/integration/rpc_persistence_reconnect_test.zig", target, optimize, lib_module);

    // Runtime probe for the generated typed-pipelining stubs: drives the
    // checked-in e2e generated modules (tests/e2e/zig/generated) end-to-end
    // over two in-process peers. The generated files import each other by
    // relative path plus "capnpc-zig", so they form one module rooted at
    // bootstrap.zig.
    const e2e_generated_module = b.createModule(.{
        .root_source_file = b.path("tests/e2e/zig/generated/bootstrap.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "capnpc-zig", .module = lib_module },
        },
    });
    const rpc_typed_pipelining_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/rpc/integration/rpc_typed_pipelining_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "e2e_generated", .module = e2e_generated_module },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &rpc_typed_pipelining_tests.step) catch @panic("OOM");
    const run_rpc_typed_pipelining_tests = &b.addRunArtifact(rpc_typed_pipelining_tests).step;

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

    const test_toolchain_gate_step = b.step("test-toolchain-gate", "Assert required tools/fixtures are present (no silent skips)");
    test_toolchain_gate_step.dependOn(run_toolchain_gate_tests);

    const run_fuzz_target_tests = addLibTest(b, "tests/fuzz/fuzz_targets.zig", target, optimize, lib_module);
    const test_fuzz_step = b.step("test-fuzz", "Run coverage-guided fuzz targets (add --fuzz to actually fuzz)");
    test_fuzz_step.dependOn(run_fuzz_target_tests);

    // Public API snapshot gate. `check-api` diffs the live pub-decl surface
    // against docs/api-snapshot.txt; `api-snapshot` regenerates the file.
    const api_snapshot_tool = b.addExecutable(.{
        .name = "api-snapshot",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/api_snapshot.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });

    const run_api_snapshot_write = b.addRunArtifact(api_snapshot_tool);
    run_api_snapshot_write.addArg("--write");
    run_api_snapshot_write.setCwd(b.path("."));
    const api_snapshot_step = b.step("api-snapshot", "Regenerate docs/api-snapshot.txt from the live public API");
    api_snapshot_step.dependOn(&run_api_snapshot_write.step);

    const run_api_snapshot_check = b.addRunArtifact(api_snapshot_tool);
    run_api_snapshot_check.addArg("--check");
    run_api_snapshot_check.setCwd(b.path("."));
    // A gate. It began as a diagnostic (14 violations on the first run, each a
    // genuine API decision); with those resolved, gating it forces the next such
    // decision to surface at review time rather than accumulate silently.
    const run_api_closure = b.addRunArtifact(api_snapshot_tool);
    run_api_closure.addArg("--closure");
    run_api_closure.setCwd(b.path("."));
    const api_closure_step = b.step("api-closure", "Report Stable declarations whose signatures mention Experimental types");
    api_closure_step.dependOn(&run_api_closure.step);

    const check_api_step = b.step("check-api", "Fail when the public API drifts from docs/api-snapshot.txt");
    check_api_step.dependOn(&run_api_snapshot_check.step);

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
    test_rpc_transport_step.dependOn(run_rpc_tick_idle_tests);
    test_rpc_transport_step.dependOn(run_rpc_connection_teardown_tests);
    test_rpc_transport_step.dependOn(run_rpc_client_session_tests);
    test_rpc_transport_step.dependOn(run_rpc_server_session_tests);
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
    test_rpc_peer_step.dependOn(run_rpc_reflected_resolve_disembargo_tests);
    test_rpc_peer_step.dependOn(run_rpc_three_party_handoff_origination_tests);
    test_rpc_peer_step.dependOn(run_rpc_three_party_handoff_pickup_tests);
    test_rpc_peer_step.dependOn(run_rpc_three_party_handoff_embargo_tests);
    test_rpc_peer_step.dependOn(run_rpc_handoff_export_pin_tests);
    test_rpc_peer_step.dependOn(run_rpc_three_party_handoff_vatc_tests);
    test_rpc_peer_step.dependOn(run_rpc_three_party_handoff_redirected_return_tests);
    test_rpc_peer_step.dependOn(run_rpc_reflected_direct_handler_tests);
    test_rpc_peer_step.dependOn(run_rpc_answer_lifecycle_tests);
    test_rpc_peer_step.dependOn(run_rpc_join_readiness_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_alloc_failure_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_semantic_helpers_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_release_and_failure_tests);
    test_rpc_peer_step.dependOn(run_rpc_return_release_param_caps_tests);
    test_rpc_peer_step.dependOn(run_rpc_concurrent_calls_tests);
    test_rpc_peer_step.dependOn(run_rpc_deadline_tests);
    test_rpc_peer_step.dependOn(run_rpc_persistence_tests);

    const test_rpc_integration_step = b.step("test-rpc-integration", "Run RPC integration tests");
    test_rpc_integration_step.dependOn(run_rpc_host_peer_tests);
    test_rpc_integration_step.dependOn(run_rpc_worker_pool_tests);
    test_rpc_integration_step.dependOn(run_rpc_persistence_reconnect_tests);
    test_rpc_integration_step.dependOn(run_rpc_typed_pipelining_tests);

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
    test_resource_budgets_step.dependOn(run_rpc_join_readiness_tests);
    test_resource_budgets_step.dependOn(run_rpc_persistence_tests);
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
    test_oom_step.dependOn(run_rpc_join_readiness_tests);
    test_oom_step.dependOn(run_rpc_persistence_tests);
    test_oom_step.dependOn(run_rpc_raw_frame_security_tests);
    test_oom_step.dependOn(&run_wasm_host_abi_tests.step);

    const test_lib_step = b.step("test-lib", "Run source module tests from src/lib.zig");
    test_lib_step.dependOn(&run_lib_tests.step);

    const release_safe_optimize: std.builtin.OptimizeMode = .ReleaseSafe;
    const release_safe_quic_zig_module: ?*std.Build.Module = if (enable_quic) blk: {
        const release_safe_quic_zig_dep = b.lazyDependency("quic_zig", .{
            .target = target,
            .optimize = release_safe_optimize,
        }) orelse break :blk null;
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
    const run_release_safe_rpc_quic_transport_tests: ?*std.Build.Step = if (release_safe_quic_zig_module) |qm|
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_transport_test.zig", target, release_safe_optimize, release_safe_lib_module, qm)
    else
        null;
    const run_release_safe_rpc_quic_connection_internal_tests: ?*std.Build.Step = if (release_safe_quic_zig_module) |qm|
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_connection_internal_test.zig", target, release_safe_optimize, release_safe_lib_module, qm)
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
    test_step.dependOn(test_toolchain_gate_step);
    test_step.dependOn(test_resource_budgets_step);
    test_step.dependOn(test_oom_step);

    // Compile-only check: no run steps, so it works for cross targets
    // (`zig build check-compile -Dtarget=powerpc64-linux-gnu`).
    const check_compile_step = b.step("check-compile", "Compile user-facing targets without running anything (cross-target safe)");
    check_compile_step.dependOn(&exe.step);
    check_compile_step.dependOn(&lib_tests.step);
    check_compile_step.dependOn(&main_tests.step);
    check_compile_step.dependOn(&rpc_pingpong_example.step);
    check_compile_step.dependOn(&serialization_demo_example.step);
    check_compile_step.dependOn(&e2e_zig_client.step);
    check_compile_step.dependOn(&e2e_zig_server.step);
    // The Experimental L4 e2e driver otherwise only compiles inside its own
    // run-only step, which no per-push CI job invokes. It is safe in every
    // config this gate runs in — native, `-Dquic=true`, and cross-target
    // (verified on x86_64-windows). The L3/C++ driver is deliberately NOT here:
    // its TCP rendezvous uses posix `poll`, which std does not wire for Windows
    // on the current toolchain, and this gate runs natively on Windows in CI.
    check_compile_step.dependOn(&e2e_l4_zig.step);
    check_compile_step.dependOn(&wasm_host_module.step);

    // Compile every registered test binary without running it. CI pairs
    // this with -Dtarget=x86_64-windows on a Linux runner so Windows test
    // compile rot is caught on every push, cheaply.
    const check_test_compile_step = b.step("check-test-compile", "Compile all registered test binaries without running them (cross-target safe)");
    for (registered_test_compile_steps.items) |test_compile| {
        check_test_compile_step.dependOn(test_compile);
    }

    // Check step (compile visible user-facing targets without running them,
    // plus the docs/examples smoke gate which does execute on the host).
    const check_step = b.step("check", "Check for compilation errors");
    check_step.dependOn(check_compile_step);
    check_step.dependOn(docs_smoke_step);
}
