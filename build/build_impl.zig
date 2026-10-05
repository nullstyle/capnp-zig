const std = @import("std");
const helpers = @import("./helpers.zig");
const modules = @import("./modules.zig");

const registered_test_compile_steps = &helpers.registered_test_compile_steps;
const addLibTest = helpers.addLibTest;
const addLibTestWithFile = helpers.addLibTestWithFile;
const addPersistenceLibTest = helpers.addPersistenceLibTest;
const addQuicLibTest = helpers.addQuicLibTest;
const addMainTest = helpers.addMainTest;
const addQuicImport = helpers.addQuicImport;
const addQuicLibImports = helpers.addQuicLibImports;

/// Returns `!void` so `error.LazyDependencyNeeded` can propagate.
///
/// This is load-bearing, not style. `std.Build.runPackageScript` only fetches
/// unresolved lazy dependencies and re-runs the configure phase when `build`
/// returns an ERROR; a `build` that returns normally goes straight to the make
/// phase with whatever graph it managed to construct. This file previously
/// swallowed the unresolved case (`b.lazyDependency(...) orelse break :blk
/// null`) and returned void, so `-Dquic=true` silently produced a graph with
/// NO quic steps in it: `check`, `test-rpc-quic` and `test` all exited 0 while
/// compiling zero QUIC code, and `just ci-quic`, the CI QUIC job and
/// `release-preflight` were no-ops together. Measured, before the fix:
/// `-Dquic=true test-rpc-quic --summary all` reported `1/1 steps succeeded`
/// against a healthy `13/13`.
pub fn buildImpl(b: *std.Build) !void {
    const graph = try modules.setup(b);
    const target = graph.target;
    const optimize = graph.optimize;
    const enable_quic = graph.enable_quic;
    const lib_root = graph.lib_root;
    const io_backend_options_module = graph.io_backend_options_module;
    const lib_module = graph.lib_module;
    const core_module = graph.core_module;
    const quic_zig_module = graph.quic_zig_module;
    const quic_boringssl_module = graph.quic_boringssl_module;
    const wasm_host_module = graph.wasm_host_module;

    // Expected-fail canary for std.Io.Evented; self-contained in its own file.
    @import("./evented_canary.zig").register(b, target, optimize);

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
    addQuicLibImports(docs_module, quic_zig_module, quic_boringssl_module);
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

    // QUIC RPC benchmark. Gated on -Dquic=true because the transport is an
    // opt-in lazy dependency; without it there is nothing to measure. The TLS
    // fixtures are the loopback cert/key the QUIC tests already use, imported
    // rather than copied so the two cannot drift.
    const quic_round_trip_bench: ?*std.Build.Step.Compile = if (quic_zig_module) |qm| quic_bench: {
        const bench_exe = b.addExecutable(.{
            .name = "bench-quic",
            .root_module = b.createModule(.{
                .root_source_file = b.path("bench/quic_round_trip.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "capnpc-zig", .module = lib_module },
                },
            }),
        });
        addQuicImport(bench_exe.root_module, qm);
        bench_exe.root_module.addAnonymousImport("quic_bench_cert", .{
            .root_source_file = b.path("tests/rpc/transport/quic/loopback_cert.pem"),
        });
        bench_exe.root_module.addAnonymousImport("quic_bench_key", .{
            .root_source_file = b.path("tests/rpc/transport/quic/loopback_key.pem"),
        });
        const run = b.addRunArtifact(bench_exe);
        run.addPassthruArgs();
        const step = b.step("bench-quic", "Run the QUIC RPC benchmark (requires -Dquic=true; -- --mode bulk for throughput)");
        step.dependOn(&run.step);
        break :quic_bench bench_exe;
    } else null;

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

    // The harness's own verdict logic (memory trend, transport-error bound,
    // setup-failure classifier, latency histogram, RSS reader) is unit
    // tested. Before this target existed its `test` blocks never compiled:
    // nothing used tools/soak_rpc.zig as a test root.
    const soak_rpc_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/soak_rpc.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &soak_rpc_tests.step) catch @panic("OOM");
    const test_soak_harness_step = b.step("test-soak-harness", "Run the soak harness's own unit tests (gate verdicts, classifiers, latency histogram, RSS reader)");
    test_soak_harness_step.dependOn(&b.addRunArtifact(soak_rpc_tests).step);

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

    // QUIC benchmark gate. Separate from `bench-check` and separately
    // baselined, because the QUIC binary only exists under -Dquic=true:
    // folding these cases into bench/baselines.json would make the ordinary
    // gate fail on a missing binary in every non-QUIC build.
    if (quic_round_trip_bench) |bench_exe| {
        const run_quic_check = b.addRunArtifact(bench_check);
        run_quic_check.addArgs(&.{ "--baseline", "bench/baselines-quic.json" });
        run_quic_check.addPassthruArgs();
        run_quic_check.step.dependOn(&b.addInstallArtifact(bench_exe, .{}).step);
        const step = b.step("bench-check-quic", "Run QUIC benchmark regression checks (requires -Dquic=true)");
        step.dependOn(&run_quic_check.step);
    }

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

    const quic_test_evidence = b.addExecutable(.{
        .name = "quic-test-evidence",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/quic_test_evidence.zig"),
            // The scanner validates source inventory while the four test
            // roots keep their requested target. It must remain runnable
            // during cross-compilation rather than inheriting that target.
            .target = b.graph.host,
            .optimize = optimize,
        }),
    });
    const run_quic_test_evidence = b.addRunArtifact(quic_test_evidence);

    const package_preflight = b.addExecutable(.{
        .name = "package-preflight",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/package_preflight.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_package_preflight = b.addRunArtifact(package_preflight);
    run_package_preflight.setCwd(b.path("."));
    run_package_preflight.addPassthruArgs();
    const package_preflight_step = b.step("package-preflight", "Validate the filtered package with clean-room consumers");
    package_preflight_step.dependOn(&run_package_preflight.step);
    // The preflight's own verdict logic (does a failed gen-check show the
    // injected drift?). `test` runs it too, since package-preflight is slow.
    const package_preflight_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/package_preflight.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    registered_test_compile_steps.append(b.allocator, &package_preflight_tests.step) catch @panic("OOM");
    const run_package_preflight_tests = &b.addRunArtifact(package_preflight_tests).step;
    package_preflight_step.dependOn(run_package_preflight_tests);

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
    // The smoke tool's own matcher tests (the verbatim doc-snippet check).
    const docs_examples_smoke_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/docs_examples_smoke.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    registered_test_compile_steps.append(b.allocator, &docs_examples_smoke_tests.step) catch @panic("OOM");
    docs_smoke_step.dependOn(&b.addRunArtifact(docs_examples_smoke_tests).step);
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
    // docs/rpc-unix-sockets.md: its snippets run over a real socket file
    // (Linux, macOS; elsewhere the unsupported stubs are checked), and a
    // test reads the doc to require each one word for word.
    const rpc_unix_snippet_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/docs/rpc_unix_snippets_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "pingpong", .module = docs_pingpong_module },
            },
        }),
    });
    rpc_unix_snippet_tests.root_module.addAnonymousImport("rpc-unix-sockets-doc", .{ .root_source_file = b.path("docs/rpc-unix-sockets.md") });
    registered_test_compile_steps.append(b.allocator, &rpc_unix_snippet_tests.step) catch @panic("OOM");
    const run_rpc_unix_snippet_tests = &b.addRunArtifact(rpc_unix_snippet_tests).step;
    // The documented pinned-plugin recipe (docs/build-integration.md), run
    // against this checkout: the plugin, built for the host so cross-target
    // compile checks can still run it, reads the codegen consumer's checked-in
    // request on stdin and writes into a cached output directory; the
    // consumer's `exercise` then runs as a test against that output.
    // package-preflight runs the same consumer from the filtered archive.
    const docs_codegen_plugin = if (target.query.isNative()) exe else b.addExecutable(.{
        .name = "capnpc-zig-host",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = b.graph.host,
            .optimize = optimize,
        }),
    });
    const docs_codegen = b.addRunArtifact(docs_codegen_plugin);
    docs_codegen.setStdIn(.{ .lazy_path = b.path("tests/package_consumer/codegen/schema/addressbook.request.bin") });
    const docs_codegen_dir = docs_codegen.addPrefixedOutputDirectoryArg("--output-dir=", "capnp-gen");
    const docs_codegen_addressbook = b.createModule(.{
        .root_source_file = docs_codegen_dir.path(b, "addressbook.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = lib_module }},
    });
    const build_integration_snippet_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/docs/build_integration_snippets_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "codegen_consumer", .module = b.createModule(.{
                    .root_source_file = b.path("tests/package_consumer/codegen/src/main.zig"),
                    .target = target,
                    .optimize = optimize,
                    .imports = &.{
                        .{ .name = "capnpc-zig", .module = lib_module },
                        .{ .name = "addressbook", .module = docs_codegen_addressbook },
                    },
                }) },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &build_integration_snippet_tests.step) catch @panic("OOM");
    const run_build_integration_snippet_tests = &b.addRunArtifact(build_integration_snippet_tests).step;
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
    test_docs_snippets_step.dependOn(run_rpc_unix_snippet_tests);
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

    // The same ping-pong over a Unix-domain socket, through `unix.listen` +
    // `unix.connect` (Experimental, Linux and macOS). It compiles for every
    // target (check-compile below); elsewhere it prints that and exits.
    const rpc_pingpong_unix_example = b.addExecutable(.{
        .name = "example-rpc-pingpong-unix",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/rpc_pingpong_unix.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });
    const run_rpc_pingpong_unix = b.addRunArtifact(rpc_pingpong_unix_example);
    run_rpc_pingpong_unix.addPassthruArgs();
    const example_rpc_unix_step = b.step("example-rpc-unix", "Run the RPC ping-pong example over a Unix-domain socket (Linux, macOS)");
    example_rpc_unix_step.dependOn(&run_rpc_pingpong_unix.step);

    // Fd passing over a Unix-domain socket (Experimental, Linux and macOS):
    // the server attaches a pipe's write end to its bootstrap capability,
    // the client writes through `Peer.importFd`, and the run fails unless
    // every copy of the fd is closed at the end. It compiles for every
    // target (check-compile below); elsewhere it prints that and exits.
    const rpc_fd_passing_example = b.addExecutable(.{
        .name = "example-rpc-fd-passing",
        .root_module = b.createModule(.{
            .root_source_file = b.path("examples/rpc_fd_passing.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
            },
        }),
    });
    const run_rpc_fd_passing = b.addRunArtifact(rpc_fd_passing_example);
    run_rpc_fd_passing.addPassthruArgs();
    const example_rpc_fd_step = b.step("example-rpc-fd", "Run the fd-passing example over a Unix-domain socket (Linux, macOS)");
    example_rpc_fd_step.dependOn(&run_rpc_fd_passing.step);

    // The same ping-pong over QUIC, through `quic.serve` + `quic.connect`.
    // Gated on -Dquic=true like bench-quic; without it the step fails with
    // the flag to pass instead of silently doing nothing. The TLS pair is the
    // loopback fixture the QUIC tests use, imported (not copied) so the two
    // cannot drift.
    const example_rpc_quic_step = b.step("example-rpc-quic", "Run the QUIC RPC ping-pong example (requires -Dquic=true)");
    const rpc_pingpong_quic_example: ?*std.Build.Step.Compile = if (quic_zig_module) |qm| quic_example: {
        const example_exe = b.addExecutable(.{
            .name = "example-rpc-pingpong-quic",
            .root_module = b.createModule(.{
                .root_source_file = b.path("examples/rpc_pingpong_quic.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "capnpc-zig", .module = lib_module },
                },
            }),
        });
        addQuicImport(example_exe.root_module, qm);
        example_exe.root_module.addAnonymousImport("quic_example_cert", .{
            .root_source_file = b.path("tests/rpc/transport/quic/loopback_cert.pem"),
        });
        example_exe.root_module.addAnonymousImport("quic_example_key", .{
            .root_source_file = b.path("tests/rpc/transport/quic/loopback_key.pem"),
        });
        const run = b.addRunArtifact(example_exe);
        run.addPassthruArgs();
        example_rpc_quic_step.dependOn(&run.step);
        break :quic_example example_exe;
    } else quic_example: {
        example_rpc_quic_step.dependOn(&b.addFail("example-rpc-quic requires -Dquic=true").step);
        break :quic_example null;
    };

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

    // The L3 drivers write raw frames to their sockets. They share the test
    // suites' socket-write shim rather than each carrying a private copy of
    // the std vtable-vs-Operation selection (see build/helpers.zig).
    const e2e_io_write_compat_module = helpers.ioWriteCompatModule(b, target, optimize, lib_module);

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
                .{ .name = "io-write-compat", .module = e2e_io_write_compat_module },
            },
        }),
    });

    const run_e2e_l3_cpp = b.addRunArtifact(e2e_l3_cpp);
    run_e2e_l3_cpp.addPassthruArgs();

    const e2e_l3_cpp_step = b.step("e2e-l3-cpp", "Run Zig/C++ L3 handoff e2e client hook");
    e2e_l3_cpp_step.dependOn(&run_e2e_l3_cpp.step);

    const install_e2e_l3_cpp_step = b.step("e2e-l3-cpp-install", "Build Zig/C++ L3 handoff e2e client (install only)");
    install_e2e_l3_cpp_step.dependOn(&b.addInstallArtifact(e2e_l3_cpp, .{}).step);

    // Orchestrator for the cross-impl HOSTING lane (C++ drives A+B against the
    // Zig VatC host binary below). Mirrors the e2e-l3-cpp lane, inverted.
    const e2e_l3_vatc = b.addExecutable(.{
        .name = "e2e-l3-vatc",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/e2e_l3_vatc.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    const run_e2e_l3_vatc = b.addRunArtifact(e2e_l3_vatc);
    run_e2e_l3_vatc.addPassthruArgs();

    const e2e_l3_vatc_step = b.step("e2e-l3-vatc", "Run the C++ A+B -> Zig VatC hosting interop lane");
    e2e_l3_vatc_step.dependOn(&run_e2e_l3_vatc.step);

    const install_e2e_l3_vatc_step = b.step("e2e-l3-vatc-install", "Build the C++ -> Zig VatC hosting lane orchestrator (install only)");
    install_e2e_l3_vatc_step.dependOn(&b.addInstallArtifact(e2e_l3_vatc, .{}).step);

    const e2e_l3_vatc_host = b.addExecutable(.{
        .name = "e2e-l3-vatc-host",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/e2e/zig/l3_vatc_host.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "io_backend_options", .module = io_backend_options_module },
                .{ .name = "l3_l4_interop", .module = e2e_l3_l4_module },
                .{ .name = "io-write-compat", .module = e2e_io_write_compat_module },
            },
        }),
    });

    const run_e2e_l3_vatc_host = b.addRunArtifact(e2e_l3_vatc_host);
    run_e2e_l3_vatc_host.addPassthruArgs();

    const e2e_l3_vatc_host_step = b.step("e2e-l3-vatc-host", "Run Zig two-peer VatC host for the cross-impl L3 harness");
    e2e_l3_vatc_host_step.dependOn(&run_e2e_l3_vatc_host.step);

    const install_e2e_l3_vatc_host_step = b.step("e2e-l3-vatc-host-install", "Build Zig two-peer VatC host (install only)");
    install_e2e_l3_vatc_host_step.dependOn(&b.addInstallArtifact(e2e_l3_vatc_host, .{}).step);

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
    addQuicLibImports(lib_tests_module, quic_zig_module, quic_boringssl_module);
    // The checked-in generated code under src/rpc/gen/ imports the library by
    // its MODULE name (`@import("capnpc-zig")`), the way a consumer would. The
    // self-import makes that resolve when the library is its own test root --
    // `lib_module` already does this, and without it here the generated files
    // cannot be analysed, which silently excludes their tests.
    lib_tests_module.addImport("capnpc-zig", lib_tests_module);
    const lib_tests = b.addTest(.{
        .root_module = lib_tests_module,
    });

    const run_lib_tests = b.addRunArtifact(lib_tests);

    // The CORE library root gets its own test target. `lib_tests` above roots
    // at src/lib.zig, so nothing in src/lib_core.zig was ever compiled as a
    // test -- including the parity guard that asserts core exports every
    // serialization module the full library does. A deliberately failing probe
    // in lib_core.zig produced no output before this target existed.
    const core_tests_module = b.createModule(.{
        .root_source_file = b.path("src/lib_core.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{},
    });
    core_tests_module.addImport("capnpc-zig", core_tests_module);
    const core_tests = b.addTest(.{
        .root_module = core_tests_module,
    });
    registered_test_compile_steps.append(b.allocator, &core_tests.step) catch @panic("OOM");
    const run_core_tests = b.addRunArtifact(core_tests);

    // Serialization tests
    const reflection = @import("reflection.zig").add(b, target, optimize, core_module);
    const test_reflection_step = reflection.test_step;
    const run_message_tests = addLibTest(b, "tests/serialization/message_test.zig", target, optimize, lib_module);
    const run_serialization_fuzz_tests = addLibTest(b, "tests/serialization/serialization_fuzz_test.zig", target, optimize, lib_module);
    const run_fuzz_smoke_tests = addLibTest(b, "tests/hardening/fuzz_smoke_test.zig", target, optimize, lib_module);
    const run_toolchain_gate_tests = addLibTest(b, "tests/hardening/toolchain_gate_test.zig", target, optimize, lib_module);
    const run_codegen_tests = addLibTest(b, "tests/serialization/codegen_test.zig", target, optimize, lib_module);
    const run_codegen_defaults_tests = addLibTest(b, "tests/serialization/codegen_defaults_test.zig", target, optimize, lib_module);
    const run_codegen_annotations_tests = addLibTest(b, "tests/serialization/codegen_annotations_test.zig", target, optimize, lib_module);
    const run_codegen_rpc_nested_tests = addLibTest(b, "tests/serialization/codegen_rpc_nested_test.zig", target, optimize, lib_module);
    const run_codegen_rpc_paths_tests = addLibTest(b, "tests/serialization/codegen_rpc_paths_test.zig", target, optimize, lib_module);
    const run_codegen_generic_rpc = addLibTest(b, "tests/serialization/generic_rpc_test.zig", target, optimize, lib_module);
    const run_generic_generated_api_tests = addLibTest(b, "tests/serialization/generic_generated_api_test.zig", target, optimize, lib_module);
    const run_codegen_streaming_tests = addLibTest(b, "tests/serialization/codegen_streaming_test.zig", target, optimize, lib_module);
    const run_codegen_generated_runtime_tests = addLibTest(b, "tests/serialization/codegen_generated_runtime_test.zig", target, optimize, lib_module);
    const run_nested_lists_runtime_tests = addLibTest(b, "tests/serialization/nested_lists_runtime_test.zig", target, optimize, lib_module);
    // These bindings are checked in so schema-evolution behavior runs as an
    // ordinary test on every platform, including Windows workers without the
    // `capnp` executable. The V1 and V2 modules use identical schema/type IDs
    // and intentionally describe different revisions of the same protocol.
    const schema_evolution_v1_module = b.createModule(.{
        .root_source_file = b.path("tests/serialization/generated/schema_evolution_v1.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = lib_module }},
    });
    const schema_evolution_v2_module = b.createModule(.{
        .root_source_file = b.path("tests/serialization/generated/schema_evolution_v2.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = lib_module }},
    });
    const schema_evolution_api_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/serialization/schema_evolution_api_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "schema-evolution-v1", .module = schema_evolution_v1_module },
                .{ .name = "schema-evolution-v2", .module = schema_evolution_v2_module },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &schema_evolution_api_tests.step) catch @panic("OOM");
    const run_schema_evolution_api_tests = &b.addRunArtifact(schema_evolution_api_tests).step;
    // The committed kvstore bindings are the consumer-shaped fixture for the
    // generated error-set contract (slcp 07 F6): an RPC schema with struct
    // lists, copy setters and capability fields, regenerated by `just gen`.
    const kvstore_generated_module = b.createModule(.{
        .root_source_file = b.path("examples/kvstore/gen/kvstore.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = lib_module }},
    });
    const codegen_error_sets_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/serialization/codegen_error_sets_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "kvstore", .module = kvstore_generated_module },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &codegen_error_sets_tests.step) catch @panic("OOM");
    const run_codegen_error_sets_tests = &b.addRunArtifact(codegen_error_sets_tests).step;
    const codegen_skew_step = addCodegenSkewChecks(b, target, optimize);
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
    const run_schema_fidelity_tests = addLibTest(b, "tests/serialization/schema_fidelity_test.zig", target, optimize, lib_module);
    const run_brand_fidelity_internal_tests = addLibTest(b, "src/brand_fidelity_test.zig", target, optimize, lib_module);
    const run_canonical_tests = addLibTest(b, "tests/serialization/canonical_test.zig", target, optimize, lib_module);

    // RPC tests (domain-organized)
    const run_rpc_framing_tests = addLibTest(b, "tests/rpc/wire/rpc_framing_test.zig", target, optimize, lib_module);
    const run_rpc_cap_table_tests = addLibTest(b, "tests/rpc/caps/rpc_cap_table_encode_test.zig", target, optimize, lib_module);
    const run_rpc_caps_release_and_failure_tests = addLibTest(b, "tests/rpc/caps/rpc_release_and_failure_test.zig", target, optimize, lib_module);
    const run_rpc_copy_tests = addLibTest(b, "tests/rpc/caps/rpc_copy_test.zig", target, optimize, lib_module);
    b.step("test-rpc-copy", "Run capability-aware copy rollback and remapping regressions").dependOn(run_rpc_copy_tests);
    const run_rpc_protocol_tests = addLibTest(b, "tests/rpc/wire/rpc_protocol_test.zig", target, optimize, lib_module);
    // The published framing conformance fixtures are meant to be vendored
    // by downstreams; running them here keeps the published bytes and the
    // implementation from drifting apart.
    const run_rpc_framing_fixture_tests = addLibTestWithFile(
        b,
        "tests/rpc/wire/framing_fixtures_test.zig",
        target,
        optimize,
        lib_module,
        "framing-fixtures",
        "tests/fixtures/framing/framing_fixtures.json",
    );
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
    const run_rpc_cross_thread_stress_tests = addLibTest(b, "tests/rpc/transport/rpc_cross_thread_stress_test.zig", target, optimize, lib_module);
    const run_rpc_client_session_tests = addLibTest(b, "tests/rpc/transport/tcp/rpc_client_session_test.zig", target, optimize, lib_module);
    const run_rpc_server_session_tests = addLibTest(b, "tests/rpc/transport/tcp/rpc_server_session_test.zig", target, optimize, lib_module);
    // AF_UNIX regressions, plus the IP side of the non-IP TCP_NODELAY skip
    // (every TCP path must still set it). POSIX-only: the suite compiles
    // everywhere and skips on Windows.
    const run_rpc_unix_regression_tests = addLibTest(b, "tests/rpc/transport/unix/rpc_unix_regression_test.zig", target, optimize, lib_module);
    // Drain mode (sprint item 6): every fd a peer attaches on an AF_UNIX
    // connection is closed, off the reader and teardown threads. Linux and
    // macOS run them; other targets compile them and skip.
    const run_rpc_unix_fd_drain_tests = addLibTest(b, "tests/rpc/transport/unix/rpc_unix_fd_drain_test.zig", target, optimize, lib_module);
    const run_rpc_unix_linger_tests = addLibTest(b, "tests/rpc/transport/unix/rpc_unix_linger_test.zig", target, optimize, lib_module);
    // FD passing, send side (sprint item 10): `fd_io.sendWithFds` and fds in
    // the write queue; every dup the queue makes is closed exactly once.
    // Linux and macOS run it; other targets compile it and run the stubs.
    const run_rpc_unix_fd_send_tests = addLibTest(b, "tests/rpc/transport/unix/rpc_unix_fd_send_test.zig", target, optimize, lib_module);
    // FD passing, receive side (sprint item 11): exact-boundary reads put
    // every fd in the frame whose bytes carried it; the per-message cap, two
    // batches in one frame, CTRUNC/EMFILE, hostile headers, and a fuzz.
    // Linux and macOS run it; other targets compile it and run the TCP test.
    const run_rpc_unix_fd_boundary_tests = addLibTest(b, "tests/rpc/transport/unix/rpc_unix_fd_boundary_test.zig", target, optimize, lib_module);
    // FD passing through the Peer (sprint item 12): `setExportFd`/`importFd`,
    // the side tables outside the frozen cap table, which descriptor keeps
    // which fd, every close hook, TCP (0xff), and the wake fds never sent.
    // Linux and macOS run it; other targets compile it and run the stub test.
    const run_rpc_unix_fd_peer_tests = addLibTest(b, "tests/rpc/transport/unix/rpc_unix_fd_peer_test.zig", target, optimize, lib_module);
    // FD passing limits and fault injection (sprint item 13): the process fd
    // budget, N connections at their per-connection cap with accept still
    // working, OOM at every allocation of the Peer's and the transport's fd
    // paths (and of the closer's queues), EMFILE on a sent fd's dup, and
    // Linux ETOOMANYREFS. Part of the Hardening gate (`test-oom`,
    // `test-resource-budgets` and its own step). Linux and macOS run it;
    // other targets compile it and skip.
    const run_rpc_unix_fd_limits_tests = addLibTest(b, "tests/rpc/transport/unix/rpc_unix_fd_limits_test.zig", target, optimize, lib_module);
    b.step("test-rpc-unix-fd-limits", "Run the fd-passing limit and fault-injection suite (process fd budget, OOM sweeps, EMFILE, ETOOMANYREFS; Linux and macOS)").dependOn(run_rpc_unix_fd_limits_tests);
    // `rpc.transport.unix.listen`/`connect` (sprint item 7): sessions over a
    // socket file, the path guards, the lock, stale files, permissions and
    // close. Linux and macOS run it; other targets compile it and run only
    // the unsupported-target stub test.
    const run_rpc_unix_session_tests = addLibTest(b, "tests/rpc/transport/unix/rpc_unix_session_test.zig", target, optimize, lib_module);
    const test_rpc_unix_step = b.step("test-rpc-unix", "Run the AF_UNIX transport suites (regressions, fd drain, lingering close, fd send, fd boundary reads, fd limits, listen/connect)");
    test_rpc_unix_step.dependOn(run_rpc_unix_regression_tests);
    test_rpc_unix_step.dependOn(run_rpc_unix_fd_drain_tests);
    test_rpc_unix_step.dependOn(run_rpc_unix_linger_tests);
    test_rpc_unix_step.dependOn(run_rpc_unix_fd_send_tests);
    test_rpc_unix_step.dependOn(run_rpc_unix_fd_boundary_tests);
    test_rpc_unix_step.dependOn(run_rpc_unix_fd_peer_tests);
    test_rpc_unix_step.dependOn(run_rpc_unix_fd_limits_tests);
    test_rpc_unix_step.dependOn(run_rpc_unix_session_tests);
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
    const run_rpc_quic_peer_tests: ?*std.Build.Step = if (quic_zig_module) |qm|
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_peer_test.zig", target, optimize, lib_module, qm)
    else
        null;
    const run_rpc_raw_frame_security_tests = addLibTest(b, "tests/rpc/transport/rpc_raw_frame_security_test.zig", target, optimize, lib_module);
    // FD-0: per-OS kernel semantics of SCM_RIGHTS that the Unix transport
    // and fd passing depend on. Raw syscalls only; skips off Linux/macOS.
    const run_rpc_unix_kernel_semantics_tests = addLibTest(b, "tests/rpc/transport/unix/unix_kernel_semantics_test.zig", target, optimize, lib_module);
    b.step("test-rpc-unix-kernel", "Run the FD-0 kernel-semantics suite (SCM_RIGHTS over AF_UNIX, Linux and macOS)").dependOn(run_rpc_unix_kernel_semantics_tests);
    test_rpc_unix_step.dependOn(run_rpc_unix_kernel_semantics_tests);
    const run_rpc_peer_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_test.zig", target, optimize, lib_module);
    const run_rpc_peer_from_peer_zig_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_from_peer_zig_test.zig", target, optimize, lib_module);
    const run_rpc_quic_vat_network_tests = addLibTest(b, "tests/rpc/peer/rpc_quic_vat_network_test.zig", target, optimize, lib_module);
    const run_rpc_reflected_resolve_disembargo_tests = addLibTest(b, "tests/rpc/peer/rpc_reflected_resolve_disembargo_test.zig", target, optimize, lib_module);
    const run_rpc_three_party_handoff_origination_tests = addLibTest(b, "tests/rpc/peer/rpc_three_party_handoff_origination_test.zig", target, optimize, lib_module);
    const run_rpc_three_party_handoff_pickup_tests = addLibTest(b, "tests/rpc/peer/rpc_three_party_handoff_pickup_test.zig", target, optimize, lib_module);
    const run_rpc_three_party_handoff_embargo_tests = addLibTest(b, "tests/rpc/peer/rpc_three_party_handoff_embargo_test.zig", target, optimize, lib_module);
    const run_rpc_handoff_export_pin_tests = addLibTest(b, "tests/rpc/peer/rpc_handoff_export_pin_test.zig", target, optimize, lib_module);
    const run_rpc_handoff_import_pin_tests = addLibTest(b, "tests/rpc/peer/rpc_handoff_import_pin_test.zig", target, optimize, lib_module);
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
    const run_codegen_streaming_cpp = addLibTest(b, "tests/serialization/codegen_streaming_cpp_test.zig", target, optimize, lib_module);
    b.step("test-codegen-streaming-cpp", "Run C++ and Zig deferred streaming interoperability").dependOn(run_codegen_streaming_cpp);
    b.step("test-codegen-generic-rpc-cpp", "Run C++ and Zig typed generic RPC interoperability").dependOn(addLibTest(b, "tests/serialization/generic_rpc_cpp_test.zig", target, optimize, lib_module));
    // Fd passing against the C++ reference (sprint item 15): the reference's
    // fd tests ported to a C++ <-> Zig connection over a socketpair, both
    // directions, through the real Connection and Peer. Linux only (other
    // targets compile it and skip; on macOS kj itself can drop the fds); it
    // needs the reference found by `pkg-config capnp`, so CI runs it in the
    // reflection-conformance job, not in `test`.
    const rpc_fd_cpp_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/rpc/transport/unix/rpc_unix_fd_cpp_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "capnpc-zig", .module = lib_module },
                .{ .name = "capnp-cli", .module = b.createModule(.{
                    .root_source_file = b.path("tests/serialization/support/capnp_cli.zig"),
                    .target = target,
                    .optimize = optimize,
                }) },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &rpc_fd_cpp_tests.step) catch @panic("OOM");
    b.step("test-rpc-fd-cpp", "Run fd passing between the C++ reference and capnp-zig over AF_UNIX (Linux; needs pkg-config capnp)").dependOn(&b.addRunArtifact(rpc_fd_cpp_tests).step);
    const fuzz_filter = b.option([]const u8, "fuzz-filter", "Select one wire/RPC fuzz target");
    const selected_fuzz = b.addTest(.{
        .root_module = b.createModule(.{ .root_source_file = b.path("tests/fuzz/fuzz_targets.zig"), .target = target, .optimize = optimize, .imports = &.{.{ .name = "capnpc-zig", .module = lib_module }} }),
        .filters = if (fuzz_filter) |filter| &.{filter} else &.{},
    });
    helpers.registered_test_compile_steps.append(b.allocator, &selected_fuzz.step) catch @panic("OOM");
    b.step("test-fuzz-target", "Run a selected fuzz target for evidence collection").dependOn(&b.addRunArtifact(selected_fuzz).step);
    const stream_fixture_host_core = b.createModule(.{ .root_source_file = b.path("src/lib_core.zig"), .target = b.graph.host, .optimize = optimize });
    stream_fixture_host_core.addImport("capnpc-zig", stream_fixture_host_core);
    const stream_fixture_generator = b.addExecutable(.{ .name = "fuzz-streaming-generate", .root_module = b.createModule(.{
        .root_source_file = b.path("tests/serialization/generate_streaming.zig"),
        .target = b.graph.host,
        .optimize = optimize,
        .imports = &.{.{ .name = "capnpc-zig", .module = stream_fixture_host_core }},
    }) });
    const generate_stream_fixture = b.addRunArtifact(stream_fixture_generator);
    const generated_stream_dir = generate_stream_fixture.addOutputDirectoryArg("generated");
    const generated_stream_module = b.createModule(.{ .root_source_file = generated_stream_dir.path(b, "generated.zig"), .target = target, .optimize = optimize, .imports = &.{.{ .name = "capnpc-zig", .module = lib_module }} });
    const generated_rpc_fuzz_module = b.createModule(.{ .root_source_file = b.path("tests/fuzz/generated_rpc_test.zig"), .target = target, .optimize = optimize, .imports = &.{ .{ .name = "capnpc-zig", .module = lib_module }, .{ .name = "generated", .module = generated_stream_module } } });
    const generated_rpc_fuzz = b.addTest(.{ .root_module = generated_rpc_fuzz_module });
    registered_test_compile_steps.append(b.allocator, &generated_rpc_fuzz.step) catch @panic("OOM");
    test_fuzz_smoke_step.dependOn(&b.addRunArtifact(generated_rpc_fuzz).step);
    const generated_rpc_fuzz_filter = b.option([]const u8, "generated-rpc-fuzz-filter", "Select one generated RPC lifecycle fuzz target");
    const selected_generated_rpc_fuzz = b.addTest(.{ .root_module = generated_rpc_fuzz_module, .filters = if (generated_rpc_fuzz_filter) |filter| &.{filter} else &.{} });
    registered_test_compile_steps.append(b.allocator, &selected_generated_rpc_fuzz.step) catch @panic("OOM");
    b.step("test-fuzz-generated-rpc", "Run generated RPC lifecycle fuzz targets").dependOn(&b.addRunArtifact(selected_generated_rpc_fuzz).step);
    const wire_fuzz = b.addTest(.{
        .root_module = b.createModule(.{ .root_source_file = b.path("tests/fuzz/fuzz_targets.zig"), .target = target, .optimize = optimize, .imports = &.{.{ .name = "capnpc-zig", .module = lib_module }} }),
        .filters = &.{"equivalent far list encodings"},
    });
    helpers.registered_test_compile_steps.append(b.allocator, &wire_fuzz.step) catch @panic("OOM");
    b.step("test-fuzz-wire-evolution", "Fuzz equivalent near/far encodings and mutation (add --fuzz=10K)").dependOn(&b.addRunArtifact(wire_fuzz).step);

    // The comptime walker, line renderers, tier matcher and closure check that
    // the snapshot gates share. It gets NO imports on purpose: it must stay
    // independent of capnpc-zig so another package can point it at its own
    // surface, and an `@import("capnpc-zig")` in it fails to compile here.
    const snapshot_render_module = b.createModule(.{
        .root_source_file = b.path("tools/snapshot_render.zig"),
        .target = target,
        .optimize = optimize,
    });
    // Its own tests run against fixture namespaces, with no capnpc-zig
    // import either, so they also prove the module stands alone.
    const snapshot_render_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/tools/snapshot_render_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "snapshot-render", .module = snapshot_render_module },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &snapshot_render_tests.step) catch @panic("OOM");
    const test_snapshot_render_step = b.step("test-snapshot-render", "Run the snapshot walker/renderer's own tests (tools/snapshot_render.zig)");
    test_snapshot_render_step.dependOn(&b.addRunArtifact(snapshot_render_tests).step);

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
                .{ .name = "snapshot-render", .module = snapshot_render_module },
            },
        }),
    });

    // Every api-snapshot run is marked `has_side_effects`, so it executes on
    // every invocation and is never answered from the build cache. The tool
    // reads and writes docs/api-snapshot*.txt, none of which the build graph
    // knows about: a cached result would be keyed on the tool binary alone,
    // and a hand-edited or stale snapshot would pass the freeze gate.
    //
    // Today Zig 0.17.0 already re-runs these (a Run step with no output
    // arguments and no captured stdio is inferred side-effecting), but that
    // is an inference this file does not control: the first
    // `expectExitCode(0)` or `captureStdOut()` added to one of these steps
    // makes it cacheable. Measured with exactly that change: a one-line edit
    // to docs/api-snapshot.txt left `check-api` GREEN ("run exe api-snapshot
    // cached") until this flag was set, and RED with it. Cost: none on 0.17.0,
    // since the steps already re-ran (~0.4 s each).
    const run_api_snapshot_write = b.addRunArtifact(api_snapshot_tool);
    run_api_snapshot_write.addArg("--write");
    run_api_snapshot_write.setCwd(b.path("."));
    run_api_snapshot_write.has_side_effects = true;
    const api_snapshot_step = b.step("api-snapshot", "Regenerate docs/api-snapshot.txt from the live public API");
    api_snapshot_step.dependOn(&run_api_snapshot_write.step);

    const run_api_snapshot_check = b.addRunArtifact(api_snapshot_tool);
    run_api_snapshot_check.addArg("--check");
    run_api_snapshot_check.setCwd(b.path("."));
    run_api_snapshot_check.has_side_effects = true;
    // A gate. It began as a diagnostic (14 violations on the first run, each a
    // genuine API decision); with those resolved, gating it forces the next such
    // decision to surface at review time rather than accumulate silently.
    const run_api_closure = b.addRunArtifact(api_snapshot_tool);
    run_api_closure.addArg("--closure");
    run_api_closure.setCwd(b.path("."));
    run_api_closure.has_side_effects = true;
    const api_closure_step = b.step("api-closure", "Report Stable declarations whose signatures mention Experimental types");
    api_closure_step.dependOn(&run_api_closure.step);

    const check_api_step = b.step("check-api", "Fail when the public API drifts from docs/api-snapshot.txt");
    check_api_step.dependOn(&run_api_snapshot_check.step);

    // Strict CI twin of `check-api`: also fails when the COMMITTED
    // experimental snapshot is stale, instead of silently refreshing it.
    // Locally `check-api` keeps the refresh-in-place behavior; in CI the
    // rendered surface must match the committed file byte-for-byte, which is
    // what makes platform-dependent renderings (the u32/u64 `std.Thread.Id`
    // shape) and forgotten-refresh commits visible. Requires the stored
    // surface to render platform-stably — thread ids are widened to u64 for
    // exactly this gate.
    const run_api_snapshot_check_strict = b.addRunArtifact(api_snapshot_tool);
    run_api_snapshot_check_strict.addArgs(&.{ "--check", "--strict-experimental" });
    run_api_snapshot_check_strict.setCwd(b.path("."));
    run_api_snapshot_check_strict.has_side_effects = true;
    const check_api_experimental_step = b.step(
        "check-api-experimental",
        "Fail when the committed experimental snapshot is stale (strict CI mode)",
    );
    check_api_experimental_step.dependOn(&run_api_snapshot_check_strict.step);

    // The QUIC-enabled surface, snapshotted separately.
    //
    // `check-api` runs WITHOUT `-Dquic=true`, so the snapshot it maintains sees
    // `rpc.transport.quic` as the disabled stub — the real `ServerOptions`
    // fields are absent from it entirely. quic-zig v0.10.0's breaking
    // `Server.Config` rename therefore produced ZERO snapshot movement, which
    // is exactly the drift a snapshot exists to catch.
    //
    // Two assertions, and the first is the valuable one:
    //   * the STABLE file is checked with its DEFAULT path, so enabling QUIC
    //     must leave the frozen contract byte-identical. An Experimental
    //     transport that alters the frozen surface is a bug by definition.
    //   * the experimental surface goes to its own file, since it legitimately
    //     differs between the two roots.
    //
    // Registered only under `-Dquic=true`: without the dependency the tool
    // would render the stub surface and fight the non-QUIC snapshot.
    if (enable_quic) {
        const run_api_snapshot_write_quic = b.addRunArtifact(api_snapshot_tool);
        run_api_snapshot_write_quic.addArgs(&.{
            "--write",
            "--experimental-path",
            "docs/api-snapshot-experimental-quic.txt",
        });
        run_api_snapshot_write_quic.setCwd(b.path("."));
        // Never cached; see `run_api_snapshot_write`.
        run_api_snapshot_write_quic.has_side_effects = true;
        const api_snapshot_quic_step = b.step(
            "api-snapshot-quic",
            "Regenerate the QUIC-enabled experimental snapshot (requires -Dquic=true)",
        );
        api_snapshot_quic_step.dependOn(&run_api_snapshot_write_quic.step);

        const run_api_snapshot_check_quic = b.addRunArtifact(api_snapshot_tool);
        run_api_snapshot_check_quic.addArgs(&.{
            "--check",
            "--experimental-path",
            "docs/api-snapshot-experimental-quic.txt",
        });
        run_api_snapshot_check_quic.setCwd(b.path("."));
        run_api_snapshot_check_quic.has_side_effects = true;
        const check_api_quic_step = b.step(
            "check-api-quic",
            "Fail when the QUIC-enabled public API drifts (requires -Dquic=true)",
        );
        check_api_quic_step.dependOn(&run_api_snapshot_check_quic.step);

        // Strict CI twin for the QUIC-enabled experimental snapshot; see
        // `check-api-experimental`.
        const run_api_snapshot_check_quic_strict = b.addRunArtifact(api_snapshot_tool);
        run_api_snapshot_check_quic_strict.addArgs(&.{
            "--check",
            "--strict-experimental",
            "--experimental-path",
            "docs/api-snapshot-experimental-quic.txt",
        });
        run_api_snapshot_check_quic_strict.setCwd(b.path("."));
        run_api_snapshot_check_quic_strict.has_side_effects = true;
        const check_api_experimental_quic_step = b.step(
            "check-api-experimental-quic",
            "Fail when the committed QUIC experimental snapshot is stale (strict CI mode, requires -Dquic=true)",
        );
        check_api_experimental_quic_step.dependOn(&run_api_snapshot_check_quic_strict.step);
    }

    // Release drift hook (`just check-release-drift <prev-tag> [version]`,
    // called by release-preflight and release-tag). It diffs the five
    // surface snapshots (the three docs/api-snapshot*.txt files and the two
    // docs/generated-shape*.txt files) against a previous release and fails
    // a release whose bump or CHANGELOG under-declares the drift. It runs
    // `git show`, so it executes from the repository root, and it reads
    // files the build graph does not know about, so it is never cached
    // (see `run_api_snapshot_write`). Args pass through:
    // `zig build release-drift -- --prev v0.19.1 [--version X.Y.Z] [--head <ref>]`.
    const release_drift_module = b.createModule(.{
        .root_source_file = b.path("tools/release_drift.zig"),
        .target = target,
        .optimize = optimize,
    });
    const release_drift_tool = b.addExecutable(.{
        .name = "release-drift",
        .root_module = release_drift_module,
    });
    const run_release_drift = b.addRunArtifact(release_drift_tool);
    run_release_drift.addPassthruArgs();
    run_release_drift.setCwd(b.path("."));
    run_release_drift.has_side_effects = true;
    b.step("release-drift", "Classify snapshot drift since a release tag (pass `-- --prev <tag>`)").dependOn(&run_release_drift.step);

    const release_drift_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/tools/release_drift_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "release-drift", .module = release_drift_module },
            },
        }),
    });
    registered_test_compile_steps.append(b.allocator, &release_drift_tests.step) catch @panic("OOM");
    const test_release_drift_step = b.step("test-release-drift", "Run the release drift hook's own tests (tools/release_drift.zig)");
    test_release_drift_step.dependOn(&b.addRunArtifact(release_drift_tests).step);

    // Generated-shape gate: the shape of the code the plugin generates for a
    // fixed corpus of committed requests (docs/generated-shape*.txt). The
    // plugin runs on the host, like the build-integration snippet above.
    @import("./generated_shape.zig").add(b, .{
        .target = target,
        .optimize = optimize,
        .lib_module = lib_module,
        .snapshot_render_module = snapshot_render_module,
        .plugin = docs_codegen_plugin,
    });

    const test_codegen_step = b.step("test-codegen", "Run code generation tests");
    test_codegen_step.dependOn(run_codegen_tests);
    test_codegen_step.dependOn(run_codegen_defaults_tests);
    test_codegen_step.dependOn(run_codegen_annotations_tests);
    test_codegen_step.dependOn(run_codegen_rpc_nested_tests);
    test_codegen_step.dependOn(run_codegen_rpc_paths_tests);
    test_codegen_step.dependOn(run_codegen_generic_rpc);
    b.step("test-codegen-generic-rpc", "Run typed generic RPC client/server and pipeline regressions").dependOn(run_codegen_generic_rpc);
    test_codegen_step.dependOn(run_generic_generated_api_tests);
    b.step("test-codegen-rpc-paths", "Run nested pipelines and inherited method generation regressions").dependOn(run_codegen_rpc_paths_tests);
    b.step("test-codegen-generics", "Run concrete generic list and recursive generated views").dependOn(run_generic_generated_api_tests);
    test_codegen_step.dependOn(run_codegen_streaming_tests);
    test_codegen_step.dependOn(run_codegen_generated_runtime_tests);
    test_codegen_step.dependOn(run_nested_lists_runtime_tests);
    test_codegen_step.dependOn(run_schema_evolution_api_tests);
    test_codegen_step.dependOn(run_codegen_error_sets_tests);
    b.step("test-codegen-error-sets", "Pin the named error sets on generated Builder mutators").dependOn(run_codegen_error_sets_tests);
    test_codegen_step.dependOn(codegen_skew_step);
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
    test_schema_validation_step.dependOn(run_canonical_tests);

    const test_schema_fidelity_step = b.step("test-schema-fidelity", "Run executable brand fidelity and upstream schema closure tests");
    test_schema_fidelity_step.dependOn(run_schema_fidelity_tests);
    test_schema_fidelity_step.dependOn(run_brand_fidelity_internal_tests);

    const test_schema_evolution_step = b.step("test-schema-evolution", "Run checked-in V1/V2 schema-evolution API tests");
    test_schema_evolution_step.dependOn(run_schema_evolution_api_tests);

    const test_serialization_step = b.step("test-serialization", "Run serialization-oriented tests");
    test_serialization_step.dependOn(test_reflection_step);
    test_serialization_step.dependOn(&run_main_tests.step);
    test_serialization_step.dependOn(&run_lib_tests.step);
    test_serialization_step.dependOn(&run_core_tests.step);
    test_serialization_step.dependOn(run_message_tests);
    test_serialization_step.dependOn(run_serialization_fuzz_tests);
    test_serialization_step.dependOn(run_codegen_tests);
    test_serialization_step.dependOn(run_codegen_defaults_tests);
    test_serialization_step.dependOn(run_codegen_annotations_tests);
    test_serialization_step.dependOn(run_codegen_rpc_nested_tests);
    test_serialization_step.dependOn(run_codegen_rpc_paths_tests);
    test_serialization_step.dependOn(run_codegen_generic_rpc);
    test_serialization_step.dependOn(run_generic_generated_api_tests);
    test_serialization_step.dependOn(run_codegen_streaming_tests);
    test_serialization_step.dependOn(run_codegen_generated_runtime_tests);
    test_serialization_step.dependOn(run_nested_lists_runtime_tests);
    test_serialization_step.dependOn(run_schema_evolution_api_tests);
    test_serialization_step.dependOn(run_codegen_error_sets_tests);
    test_serialization_step.dependOn(codegen_skew_step);
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
    test_serialization_step.dependOn(run_schema_fidelity_tests);
    test_serialization_step.dependOn(run_brand_fidelity_internal_tests);
    test_serialization_step.dependOn(run_canonical_tests);

    const test_rpc_wire_step = b.step("test-rpc-wire", "Run RPC wire framing/protocol tests");
    test_rpc_wire_step.dependOn(run_rpc_framing_tests);
    test_rpc_wire_step.dependOn(run_rpc_protocol_tests);
    test_rpc_wire_step.dependOn(run_rpc_framing_fixture_tests);

    const test_rpc_caps_step = b.step("test-rpc-caps", "Run RPC capability table tests");
    test_rpc_caps_step.dependOn(run_rpc_cap_table_tests);
    test_rpc_caps_step.dependOn(run_rpc_caps_release_and_failure_tests);
    test_rpc_caps_step.dependOn(run_rpc_copy_tests);

    const test_rpc_promises_step = b.step("test-rpc-promises", "Run RPC promise/pipelining tests");
    test_rpc_promises_step.dependOn(run_rpc_promised_answer_tests);
    test_rpc_promises_step.dependOn(run_rpc_peer_return_send_helpers_tests);

    const test_rpc_transport_step = b.step("test-rpc-transport", "Run RPC TCP/Unix/raw-frame transport tests");
    test_rpc_transport_step.dependOn(run_rpc_connection_failure_tests);
    test_rpc_transport_step.dependOn(run_rpc_events_tests);
    test_rpc_transport_step.dependOn(run_rpc_tick_idle_tests);
    test_rpc_transport_step.dependOn(run_rpc_connection_teardown_tests);
    test_rpc_transport_step.dependOn(run_rpc_cross_thread_stress_tests);
    test_rpc_transport_step.dependOn(run_rpc_client_session_tests);
    test_rpc_transport_step.dependOn(run_rpc_server_session_tests);
    test_rpc_transport_step.dependOn(run_rpc_unix_regression_tests);
    test_rpc_transport_step.dependOn(run_rpc_unix_fd_drain_tests);
    test_rpc_transport_step.dependOn(run_rpc_unix_linger_tests);
    test_rpc_transport_step.dependOn(run_rpc_unix_fd_send_tests);
    test_rpc_transport_step.dependOn(run_rpc_unix_fd_boundary_tests);
    test_rpc_transport_step.dependOn(run_rpc_unix_fd_peer_tests);
    test_rpc_transport_step.dependOn(run_rpc_unix_fd_limits_tests);
    test_rpc_transport_step.dependOn(run_rpc_unix_session_tests);
    test_rpc_transport_step.dependOn(run_rpc_raw_frame_security_tests);
    test_rpc_transport_step.dependOn(run_rpc_unix_kernel_semantics_tests);

    const test_rpc_quic_step = b.step("test-rpc-quic", "Run quic-zig-backed QUIC RPC transport tests (requires -Dquic=true)");
    if (run_rpc_quic_transport_tests) |step| test_rpc_quic_step.dependOn(step);
    if (run_rpc_quic_public_api_tests) |step| test_rpc_quic_step.dependOn(step);
    if (run_rpc_quic_connection_internal_tests) |step| test_rpc_quic_step.dependOn(step);
    if (run_rpc_quic_peer_tests) |step| test_rpc_quic_step.dependOn(step);

    // Executable proof that the optional QUIC lane is both enabled and
    // non-vacuous. The four run-artifact dependencies are stronger than
    // parsing a target-dependent Build Summary, while the host scanner keeps
    // the source inventory and no-skip contract explicit.
    const test_rpc_quic_evidence_step = b.step("test-rpc-quic-evidence", "Run all QUIC evidence roots and enforce their inventory");
    if (run_rpc_quic_transport_tests) |transport_step| {
        test_rpc_quic_evidence_step.dependOn(transport_step);
        test_rpc_quic_evidence_step.dependOn(run_rpc_quic_public_api_tests.?);
        test_rpc_quic_evidence_step.dependOn(run_rpc_quic_connection_internal_tests.?);
        test_rpc_quic_evidence_step.dependOn(run_rpc_quic_peer_tests.?);
        test_rpc_quic_evidence_step.dependOn(&run_quic_test_evidence.step);
    } else {
        test_rpc_quic_evidence_step.dependOn(&b.addFail("test-rpc-quic-evidence requires -Dquic=true").step);
    }

    const test_rpc_peer_step = b.step("test-rpc-peer", "Run RPC peer semantics tests");
    test_rpc_peer_step.dependOn(run_rpc_peer_transport_callbacks_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_transport_state_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_cleanup_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_tests);
    test_rpc_peer_step.dependOn(run_rpc_peer_from_peer_zig_tests);
    test_rpc_peer_step.dependOn(run_rpc_quic_vat_network_tests);
    test_rpc_peer_step.dependOn(run_rpc_reflected_resolve_disembargo_tests);
    test_rpc_peer_step.dependOn(run_rpc_three_party_handoff_origination_tests);
    test_rpc_peer_step.dependOn(run_rpc_three_party_handoff_pickup_tests);
    test_rpc_peer_step.dependOn(run_rpc_three_party_handoff_embargo_tests);
    test_rpc_peer_step.dependOn(run_rpc_handoff_export_pin_tests);
    test_rpc_peer_step.dependOn(run_rpc_handoff_import_pin_tests);
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

    const test_rpc_l4_step = b.step("test-rpc-l4", "Run Experimental L4 Join lease and lifecycle tests");
    test_rpc_l4_step.dependOn(run_rpc_join_readiness_tests);

    // Focused Level-3 handoff gate. These are the same run nodes already
    // included by test-rpc-peer, so selecting both steps never duplicates a
    // compile in one build graph.
    const test_rpc_l3_step = b.step("test-rpc-l3", "Run the seven RPC Level-3 handoff suites");
    test_rpc_l3_step.dependOn(run_rpc_three_party_handoff_origination_tests);
    test_rpc_l3_step.dependOn(run_rpc_three_party_handoff_pickup_tests);
    test_rpc_l3_step.dependOn(run_rpc_three_party_handoff_embargo_tests);
    test_rpc_l3_step.dependOn(run_rpc_handoff_export_pin_tests);
    test_rpc_l3_step.dependOn(run_rpc_handoff_import_pin_tests);
    test_rpc_l3_step.dependOn(run_rpc_three_party_handoff_vatc_tests);
    test_rpc_l3_step.dependOn(run_rpc_three_party_handoff_redirected_return_tests);

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
    test_resource_budgets_step.dependOn(run_schema_fidelity_tests);
    test_resource_budgets_step.dependOn(run_brand_fidelity_internal_tests);
    test_resource_budgets_step.dependOn(run_canonical_tests);
    test_resource_budgets_step.dependOn(run_rpc_framing_tests);
    test_resource_budgets_step.dependOn(run_rpc_connection_failure_tests);
    test_resource_budgets_step.dependOn(run_rpc_join_readiness_tests);
    test_resource_budgets_step.dependOn(run_rpc_persistence_tests);
    test_resource_budgets_step.dependOn(run_rpc_three_party_handoff_vatc_tests);
    if (run_rpc_quic_transport_tests) |step| test_resource_budgets_step.dependOn(step);
    if (run_rpc_quic_connection_internal_tests) |step| test_resource_budgets_step.dependOn(step);
    if (run_rpc_quic_peer_tests) |step| test_resource_budgets_step.dependOn(step);
    test_resource_budgets_step.dependOn(run_rpc_raw_frame_security_tests);
    test_resource_budgets_step.dependOn(run_rpc_unix_fd_limits_tests);
    test_resource_budgets_step.dependOn(&run_wasm_host_abi_tests.step);

    const test_oom_step = b.step("test-oom", "Run OOM and failing allocator regression tests");
    test_oom_step.dependOn(&run_main_tests.step);
    test_oom_step.dependOn(run_message_tests);
    test_oom_step.dependOn(run_fuzz_smoke_tests);
    test_oom_step.dependOn(run_codegen_tests);
    test_oom_step.dependOn(run_codegen_defaults_tests);
    test_oom_step.dependOn(run_schema_fidelity_tests);
    test_oom_step.dependOn(run_brand_fidelity_internal_tests);
    test_oom_step.dependOn(run_rpc_framing_tests);
    test_oom_step.dependOn(run_rpc_connection_failure_tests);
    test_oom_step.dependOn(run_rpc_join_readiness_tests);
    test_oom_step.dependOn(run_rpc_persistence_tests);
    test_oom_step.dependOn(run_rpc_three_party_handoff_vatc_tests);
    test_oom_step.dependOn(run_rpc_raw_frame_security_tests);
    test_oom_step.dependOn(run_rpc_unix_fd_limits_tests);
    test_oom_step.dependOn(&run_wasm_host_abi_tests.step);

    const test_lib_step = b.step("test-lib", "Run source module tests from src/lib.zig");
    test_lib_step.dependOn(&run_lib_tests.step);

    const release_safe_optimize: std.builtin.OptimizeMode = .ReleaseSafe;
    // Same contract as the debug-mode resolution above: propagate, never swallow.
    const release_safe_quic_dep: ?*std.Build.Dependency = if (enable_quic)
        try b.dependencyLazy("quic", .{
            .target = target,
            // See build/modules.zig: we pass quic-zig the boolean
            // `release` (never `optimize`, which it ignored through
            // v0.24.0), the same option map every parent uses.
            .release = true,
            // See build/modules.zig: BoringSSL archives must not
            // reference the UBSan runtime; `trap` keeps the checks
            // without the link dependency.
            .@"sanitize-c" = @as([]const u8, "trap"),
        })
    else
        null;
    const release_safe_quic_zig_module: ?*std.Build.Module = if (release_safe_quic_dep) |dep| dep.module("quic") else null;
    // See build/modules.zig: the library root also imports the boringssl
    // module instance this quic dependency exports.
    const release_safe_quic_boringssl_module: ?*std.Build.Module = if (release_safe_quic_dep) |dep| dep.module("boringssl") else null;
    const release_safe_lib_module = b.addModule("capnpc-zig-release-safe", .{
        .root_source_file = b.path(lib_root),
        .target = target,
        .optimize = release_safe_optimize,
        .imports = &.{},
    });
    release_safe_lib_module.addImport("capnpc-zig", release_safe_lib_module);
    addQuicLibImports(release_safe_lib_module, release_safe_quic_zig_module, release_safe_quic_boringssl_module);

    const run_release_safe_main_tests = addMainTest(b, "src/main.zig", target, release_safe_optimize);
    const run_release_safe_message_tests = addLibTest(b, "tests/serialization/message_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_serialization_fuzz_tests = addLibTest(b, "tests/serialization/serialization_fuzz_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_fuzz_smoke_tests = addLibTest(b, "tests/hardening/fuzz_smoke_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_codegen_tests = addLibTest(b, "tests/serialization/codegen_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_codegen_defaults_tests = addLibTest(b, "tests/serialization/codegen_defaults_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_nested_lists_runtime_tests = addLibTest(b, "tests/serialization/nested_lists_runtime_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_schema_validation_tests = addLibTest(b, "tests/serialization/schema_validation_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_schema_fidelity_tests = addLibTest(b, "tests/serialization/schema_fidelity_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_brand_fidelity_internal_tests = addLibTest(b, "src/brand_fidelity_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_canonical_tests = addLibTest(b, "tests/serialization/canonical_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_rpc_framing_tests = addLibTest(b, "tests/rpc/wire/rpc_framing_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_rpc_connection_failure_tests = addLibTest(b, "tests/rpc/transport/tcp/rpc_connection_failure_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_rpc_vatc_tests = addLibTest(b, "tests/rpc/peer/rpc_three_party_handoff_vatc_test.zig", target, release_safe_optimize, release_safe_lib_module);
    const run_release_safe_rpc_quic_transport_tests: ?*std.Build.Step = if (release_safe_quic_zig_module) |qm|
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_transport_test.zig", target, release_safe_optimize, release_safe_lib_module, qm)
    else
        null;
    const run_release_safe_rpc_quic_public_api_tests: ?*std.Build.Step = if (release_safe_quic_zig_module) |qm|
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_public_api_test.zig", target, release_safe_optimize, release_safe_lib_module, qm)
    else
        null;
    const run_release_safe_rpc_quic_connection_internal_tests: ?*std.Build.Step = if (release_safe_quic_zig_module) |qm|
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_connection_internal_test.zig", target, release_safe_optimize, release_safe_lib_module, qm)
    else
        null;
    const run_release_safe_rpc_quic_peer_tests: ?*std.Build.Step = if (release_safe_quic_zig_module) |qm|
        addQuicLibTest(b, "tests/rpc/transport/quic/rpc_quic_peer_test.zig", target, release_safe_optimize, release_safe_lib_module, qm)
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
    test_release_safe_step.dependOn(run_release_safe_nested_lists_runtime_tests);
    test_release_safe_step.dependOn(run_release_safe_schema_validation_tests);
    test_release_safe_step.dependOn(run_release_safe_schema_fidelity_tests);
    test_release_safe_step.dependOn(run_release_safe_brand_fidelity_internal_tests);
    test_release_safe_step.dependOn(run_release_safe_canonical_tests);
    test_release_safe_step.dependOn(run_release_safe_rpc_framing_tests);
    test_release_safe_step.dependOn(run_release_safe_rpc_connection_failure_tests);
    test_release_safe_step.dependOn(run_release_safe_rpc_vatc_tests);
    if (run_release_safe_rpc_quic_transport_tests) |step| test_release_safe_step.dependOn(step);
    if (run_release_safe_rpc_quic_public_api_tests) |step| test_release_safe_step.dependOn(step);
    if (run_release_safe_rpc_quic_connection_internal_tests) |step| test_release_safe_step.dependOn(step);
    if (run_release_safe_rpc_quic_peer_tests) |step| test_release_safe_step.dependOn(step);
    test_release_safe_step.dependOn(run_release_safe_rpc_raw_frame_security_tests);

    // ReleaseFast lane. This exists because of a specific class the other lanes
    // structurally cannot see: a use-after-free reached from a DESTRUCTOR.
    //
    // `HostPeer.deinit` freed the queue that `Peer.deinit`'s own Finish/Release
    // sends then appended to. Debug and ReleaseSafe both poison the freed
    // ArrayList handle, and these suites happened never to trip a safety check
    // on the poisoned value, so all 981 tests passed. ReleaseFast leaves the
    // freed pointer intact, the append lands in freed memory, and
    // `SafeAllocator`'s free-fill check catches it. No lane ran ReleaseFast, so
    // the bug was invisible for as long as it existed.
    //
    // Scoped deliberately to the teardown-heavy RPC suites plus the bounded
    // schema-fidelity ownership/brand resolver gate rather than the whole tree:
    // those are where destructors and recursive ownership do real work, and
    // running everything here would buy little for the wall-clock. Note this
    // lane is about MEMORY SAFETY, not assertions
    // -- `unreachable` is UB rather than a panic here, so a failure in this lane
    // deserves reading before it is "fixed".
    const release_fast_optimize: std.builtin.OptimizeMode = .ReleaseFast;
    const release_fast_lib_module = b.addModule("capnpc-zig-release-fast", .{
        .root_source_file = b.path(lib_root),
        .target = target,
        .optimize = release_fast_optimize,
        .imports = &.{},
    });
    release_fast_lib_module.addImport("capnpc-zig", release_fast_lib_module);
    addQuicImport(release_fast_lib_module, null);

    const run_release_fast_rpc_host_peer_tests = addLibTest(b, "tests/rpc/integration/rpc_host_peer_test.zig", target, release_fast_optimize, release_fast_lib_module);
    const run_release_fast_rpc_persistence_reconnect_tests = addPersistenceLibTest(b, "tests/rpc/integration/rpc_persistence_reconnect_test.zig", target, release_fast_optimize, release_fast_lib_module);
    const run_release_fast_rpc_peer_tests = addLibTest(b, "tests/rpc/peer/rpc_peer_test.zig", target, release_fast_optimize, release_fast_lib_module);
    const run_release_fast_rpc_vatc_tests = addLibTest(b, "tests/rpc/peer/rpc_three_party_handoff_vatc_test.zig", target, release_fast_optimize, release_fast_lib_module);
    const run_release_fast_schema_fidelity_tests = addLibTest(b, "tests/serialization/schema_fidelity_test.zig", target, release_fast_optimize, release_fast_lib_module);
    const run_release_fast_brand_fidelity_internal_tests = addLibTest(b, "src/brand_fidelity_test.zig", target, release_fast_optimize, release_fast_lib_module);

    const test_release_fast_step = b.step("test-release-fast", "Run teardown-heavy RPC and schema-fidelity suites under ReleaseFast, where safety poisoning cannot mask ownership defects");
    test_release_fast_step.dependOn(run_release_fast_rpc_host_peer_tests);
    test_release_fast_step.dependOn(run_release_fast_rpc_persistence_reconnect_tests);
    test_release_fast_step.dependOn(run_release_fast_rpc_peer_tests);
    test_release_fast_step.dependOn(run_release_fast_rpc_vatc_tests);
    test_release_fast_step.dependOn(run_release_fast_schema_fidelity_tests);
    test_release_fast_step.dependOn(run_release_fast_brand_fidelity_internal_tests);

    // ThreadSanitizer lane. Linux-only, because Linux is where it is
    // validated. When it was written, libtsan segfaulted at startup on
    // darwin (an instrumented binary died with SIGSEGV before any output).
    // Re-probed 2026-10-03 on Darwin 27: a `zig test -fsanitize-thread -lc`
    // probe now runs clean and reports a seeded race at both dev.1683 and
    // 0.17.0, but the full lane has never run there, so the gate stays.
    // Races are optimize-mode-independent, so the lane runs
    // Debug for the best stacks. The instrumented modules link libc: without
    // it std.Thread uses raw clone() on Linux, which TSan cannot intercept,
    // leaving every spawned thread invisible to the runtime. On unsupported
    // hosts the steps hard-fail rather than pass vacuously.
    const tsan_target_ok = target.result.os.tag == .linux;
    const tsan_host_ok = tsan_target_ok and b.graph.host.result.os.tag == .linux;
    const test_tsan_step = b.step("test-tsan", "Run threaded transport suites under ThreadSanitizer (Linux host+target only at this Zig pin)");
    const soak_tsan_step = b.step("soak-tsan", "Run the RPC soak harness under ThreadSanitizer (Linux host+target only at this Zig pin; use -- --seconds N)");
    const check_tsan_step = b.step("check-tsan", "Compile the ThreadSanitizer suites for a Linux target without running them (usable from any host via -Dtarget=aarch64-linux-gnu or x86_64-linux-gnu)");
    if (tsan_target_ok) {
        const tsan_lib_module = b.addModule("capnpc-zig-tsan", .{
            .root_source_file = b.path(lib_root),
            .target = target,
            .optimize = .Debug,
            .sanitize_thread = true,
            .link_libc = true,
            .imports = &.{},
        });
        tsan_lib_module.addImport("capnpc-zig", tsan_lib_module);
        addQuicLibImports(tsan_lib_module, quic_zig_module, quic_boringssl_module);

        const tsan_suites = [_][]const u8{
            "tests/rpc/transport/rpc_cross_thread_stress_test.zig",
            "tests/rpc/transport/tcp/rpc_connection_teardown_test.zig",
            "tests/rpc/transport/tcp/rpc_client_session_test.zig",
            "tests/rpc/transport/tcp/rpc_server_session_test.zig",
            "tests/rpc/integration/rpc_worker_pool_test.zig",
            // Single-threaded, but its deadline sweep runs on transport tick
            // threads in production; keep the deadline-cancel failure
            // routing (a `.cancel_failure` observer event, never on_error)
            // TSan-built.
            "tests/rpc/peer/rpc_deadline_test.zig",
            // Single-threaded apart from one writer thread, but it is the
            // only lane that runs FD-0 against glibc's `cmsghdr` layout.
            "tests/rpc/transport/unix/unix_kernel_semantics_test.zig",
            // Drain mode hands received fds from the reader thread to the
            // process-wide closer thread; TSan also runs it against glibc's
            // cmsghdr layout.
            "tests/rpc/transport/unix/rpc_unix_fd_drain_test.zig",
            "tests/rpc/transport/unix/rpc_unix_linger_test.zig",
            // Fd passing, send side: the owner thread queues dups, the writer
            // and the owner hand them to the closer's `.sent` lane.
            "tests/rpc/transport/unix/rpc_unix_fd_send_test.zig",
            // Fd passing, receive side: the fuzz's sender thread against the
            // exact-boundary reader, and fds handed to the closer.
            "tests/rpc/transport/unix/rpc_unix_fd_boundary_test.zig",
            // Fd passing through the Peer: session threads, the closer
            // taking imported fds, and the wake socketpair, against glibc.
            "tests/rpc/transport/unix/rpc_unix_fd_peer_test.zig",
            // Fd passing limits: the process fd budget's atomic count, shared
            // by readers, writers, session threads and the closer.
            "tests/rpc/transport/unix/rpc_unix_fd_limits_test.zig",
            // unix.listen/connect: the session threads, racing listeners and
            // the accept wake-up, against glibc.
            "tests/rpc/transport/unix/rpc_unix_session_test.zig",
        };
        for (tsan_suites) |suite_path| {
            const t = b.addTest(.{
                .root_module = b.createModule(.{
                    .root_source_file = b.path(suite_path),
                    .target = target,
                    .optimize = .Debug,
                    .sanitize_thread = true,
                    .link_libc = true,
                    .imports = &.{
                        .{ .name = "capnpc-zig", .module = tsan_lib_module },
                    },
                }),
            });
            // Deliberately NOT registered into check-test-compile. TSan
            // instrumentation is only available on a subset of architectures,
            // so including these here made `check-test-compile` fail for any
            // other target with "unable to build TSAN library:
            // TSANUnsupportedCPUArchitecture" -- which is why that CI step had
            // been pinned to x86_64-windows alone, and why a 32-bit test break
            // went unnoticed from 2026-07-29. `check-tsan` below is the step
            // that owns TSan compile coverage.
            check_tsan_step.dependOn(&t.step);
            if (tsan_host_ok) test_tsan_step.dependOn(&b.addRunArtifact(t).step);
        }

        const soak_tsan = b.addExecutable(.{
            .name = "soak-rpc-tsan",
            .root_module = b.createModule(.{
                .root_source_file = b.path("tools/soak_rpc.zig"),
                .target = target,
                .optimize = .Debug,
                .sanitize_thread = true,
                .link_libc = true,
                .imports = &.{
                    .{ .name = "capnpc-zig", .module = tsan_lib_module },
                },
            }),
        });
        check_tsan_step.dependOn(&soak_tsan.step);
        if (tsan_host_ok) {
            const run_soak_tsan = b.addRunArtifact(soak_tsan);
            run_soak_tsan.addPassthruArgs();
            soak_tsan_step.dependOn(&run_soak_tsan.step);
        } else {
            const tsan_run_fail = b.addFail("test-tsan/soak-tsan run only on a Linux host, where the lane is validated (libtsan used to segfault at startup on darwin). Use check-tsan locally (compile-only) and run the lane in CI or a Linux container.");
            test_tsan_step.dependOn(&tsan_run_fail.step);
            soak_tsan_step.dependOn(&tsan_run_fail.step);
        }
    } else {
        const tsan_fail = b.addFail("ThreadSanitizer steps need a Linux target, where the lane is validated (libtsan used to segfault on darwin): pass -Dtarget=aarch64-linux-gnu or x86_64-linux-gnu for check-tsan, or run test-tsan/soak-tsan on a Linux host.");
        test_tsan_step.dependOn(&tsan_fail.step);
        soak_tsan_step.dependOn(&tsan_fail.step);
        check_tsan_step.dependOn(&tsan_fail.step);
    }

    // Test step runs all tests
    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(test_serialization_step);
    test_step.dependOn(test_rpc_step);
    // `test-lib` existed but NOTHING depended on it -- not `test`, not any
    // domain step, and it appears in no CI job or Justfile recipe. So the
    // source-module tests it runs never ran anywhere: 322 of them by default,
    // 325 with -Dquic=true. Tests in src/ are only ever collected when
    // src/lib.zig is the ROOT module, which is exactly what this step does;
    // the tests/ roots import capnpc-zig as a separate module, and Zig does
    // not collect tests from non-root modules.
    //
    // This does NOT reach everything under src/. Ablation: inverting an
    // assertion in src/rpc/transport/quic/{close,datagram_io}.zig still leaves
    // `test`, `test-lib` and `test-rpc-quic` exiting 0, because those files sit
    // past the refAllRecursive depth that forces their analysis. That gap is
    // tracked separately; this step closes the part that is closeable here.
    test_step.dependOn(test_lib_step);
    test_step.dependOn(test_wasm_host_step);
    test_step.dependOn(test_fuzz_smoke_step);
    test_step.dependOn(test_toolchain_gate_step);
    test_step.dependOn(test_resource_budgets_step);
    test_step.dependOn(test_oom_step);
    test_step.dependOn(test_soak_harness_step);
    test_step.dependOn(run_package_preflight_tests);
    test_step.dependOn(test_snapshot_render_step);
    test_step.dependOn(test_release_drift_step);

    // Configure these after the suites are complete. Windows can warm their
    // exact compile prerequisites in parallel, then run the unchanged suites
    // in a separate `-j1` invocation to avoid concurrent child pipe inheritance.
    _ = helpers.addSuiteCompileStep(b, "test-compile", "Compile prerequisites for test without running its test binaries", test_step);
    _ = helpers.addSuiteCompileStep(b, "test-release-fast-compile", "Compile prerequisites for test-release-fast without running its test binaries", test_release_fast_step);
    _ = helpers.addSuiteCompileStep(b, "test-rpc-quic-evidence-compile", "Compile prerequisites for test-rpc-quic-evidence without running its test binaries", test_rpc_quic_evidence_step);

    // Compile-only check: no run steps, so it works for cross targets
    // (`zig build check-compile -Dtarget=powerpc64-linux-gnu`).
    const check_compile_step = b.step("check-compile", "Compile user-facing targets without running anything (cross-target safe)");
    check_compile_step.dependOn(&exe.step);
    check_compile_step.dependOn(&lib_tests.step);
    check_compile_step.dependOn(&main_tests.step);
    check_compile_step.dependOn(&rpc_pingpong_example.step);
    check_compile_step.dependOn(&rpc_pingpong_unix_example.step);
    check_compile_step.dependOn(&rpc_fd_passing_example.step);
    if (rpc_pingpong_quic_example) |example_exe| check_compile_step.dependOn(&example_exe.step);
    check_compile_step.dependOn(&serialization_demo_example.step);
    // The soak is a real cross-platform executable; compile it for cross
    // targets so a Windows-only no-op or POSIX API cannot regress silently.
    check_compile_step.dependOn(&soak_rpc.step);
    check_compile_step.dependOn(&e2e_zig_client.step);
    check_compile_step.dependOn(&e2e_zig_server.step);
    // The Experimental L4 e2e driver otherwise only compiles inside its own
    // run-only step, which no per-push CI job invokes. It is safe in every
    // config this gate runs in — native, `-Dquic=true`, and cross-target
    // (verified on x86_64-windows). The L3/C++ driver is deliberately NOT here:
    // its TCP rendezvous uses posix `poll`, which std does not wire for Windows
    // on the current toolchain, and this gate runs natively on Windows in CI.
    // It is compiled by `check-tools` below, which `check` pulls in on every
    // non-Windows host and target.
    check_compile_step.dependOn(&e2e_l4_zig.step);
    check_compile_step.dependOn(&wasm_host_module.step);

    // Compile every registered test binary without running it. CI pairs
    // this with -Dtarget=x86_64-windows on a Linux runner so Windows test
    // compile rot is caught on every push, cheaply.
    const check_test_compile_step = b.step("check-test-compile", "Compile all registered test binaries without running them (cross-target safe)");
    for (registered_test_compile_steps.items) |test_compile| {
        check_test_compile_step.dependOn(test_compile);
    }

    // Tool, bench and e2e executables that `check-compile` leaves out.
    //
    // Before this step each of them compiled only inside its own run or
    // install step, and three never entered the build graph at all: they run
    // as `zig run` from tests/e2e/Justfile or the Nightly workflow. 428197a
    // moved to tagged Zig 0.17.0, which deleted `Io.VTable.netWrite`;
    // tools/e2e_l3_cpp.zig still called it, `zig build check` stayed green,
    // and only the Docker e2e lane in CI went red. Ablation: making the
    // vtable arm of that tool's socket write unconditional leaves the old
    // `check` green and turns this step (and so `check`) red. That write now
    // lives in the shared io-write-compat shim; forcing its vtable arm turns
    // `check` red through e2e-l3-cpp and e2e-l3-vatc-host the same way.
    //
    // `check` depends on this only when neither the host nor the target is
    // Windows. The L3/C++ driver and the VatC host wait on their TCP sockets
    // with posix `poll`, which std does not wire for Windows on the current
    // toolchain (`ws2_32` has no `pollfd`), and `check` runs natively on
    // Windows in CI; tools/e2e_runner.zig also opens raw posix sockets with
    // `SOCK.CLOEXEC`. The host matters too: quic-test-evidence and
    // reflection-cpp-build always build for it. Asked for directly on
    // Windows, the step fails with that reason rather than passing having
    // compiled nothing.
    const check_tools_step = b.step("check-tools", "Compile the tool, bench and e2e executables that check-compile leaves out (non-Windows host and target)");
    const check_tools_supported = target.result.os.tag != .windows and b.graph.host.result.os.tag != .windows;
    if (check_tools_supported) {
        // Already in the graph behind run or install steps; depending on the
        // same compile nodes shares their cache with `*-install`.
        const graph_tools = [_]*std.Build.Step.Compile{
            e2e_l3_cpp,
            e2e_l3_vatc,
            e2e_l3_vatc_host,
            e2e_self,
            bench_check,
            hardening_gate,
            package_preflight,
            quic_test_evidence,
            ping_pong_bench,
            pack_unpack_bench,
            rpc_round_trip_bench,
        } ++ reflection.tools;
        for (graph_tools) |tool| check_tools_step.dependOn(&tool.step);
        if (quic_round_trip_bench) |bench_exe| check_tools_step.dependOn(&bench_exe.step);

        // api-snapshot is the one expensive member. After a src/ edit its
        // full build took 25s on an M-series Mac against 11s for analysis
        // alone, and the full build roughly doubled `check` (15s -> 30s).
        // So this is a separate analysis-only node (-fno-emit-bin) on the
        // same root module, which still fails on a std break; `check-api`
        // builds and runs the real binary on every push.
        const api_snapshot_analysis = b.addExecutable(.{
            .name = "api-snapshot",
            .root_module = api_snapshot_tool.root_module,
        });
        check_tools_step.dependOn(&api_snapshot_analysis.step);

        // `zig run` tools with no other build node. Each binary is emitted
        // (not -fno-emit-bin) so link-time breaks surface here too. They
        // import only std, so this costs little.
        const zig_run_tools = [_]struct { name: []const u8, path: []const u8 }{
            .{ .name = "e2e-l3-go-probe", .path = "tools/e2e_l3_go_probe.zig" },
            .{ .name = "e2e-runner", .path = "tools/e2e_runner.zig" },
            .{ .name = "fuzz-evidence", .path = "tools/fuzz_evidence.zig" },
        };
        for (zig_run_tools) |tool| {
            const tool_module = b.createModule(.{
                .root_source_file = b.path(tool.path),
                .target = target,
                .optimize = optimize,
            });
            const tool_exe = b.addExecutable(.{ .name = tool.name, .root_module = tool_module });
            _ = tool_exe.getEmittedBin();
            check_tools_step.dependOn(&tool_exe.step);
        }
        // The Nightly also runs `zig test tools/fuzz_evidence.zig`: its tests
        // prove the evidence gate rejects missing activity, so compile them.
        const fuzz_evidence_tests = b.addTest(.{
            .name = "fuzz-evidence-test",
            .root_module = b.createModule(.{
                .root_source_file = b.path("tools/fuzz_evidence.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        _ = fuzz_evidence_tests.getEmittedBin();
        check_tools_step.dependOn(&fuzz_evidence_tests.step);
    } else {
        check_tools_step.dependOn(&b.addFail("check-tools needs a non-Windows host and target: the L3/C++ e2e driver and the VatC host use posix poll, which std does not wire for Windows on the current toolchain, and the e2e runner uses raw posix sockets. Run it on Linux or macOS without -Dtarget=*-windows.").step);
    }

    // Check step (compile visible user-facing targets without running them,
    // plus the docs/examples smoke gate which does execute on the host).
    const check_step = b.step("check", "Check for compilation errors");
    check_step.dependOn(check_compile_step);
    check_step.dependOn(docs_smoke_step);
    if (check_tools_supported) check_step.dependOn(check_tools_step);
}

/// Plugin/runtime skew guard (`test-codegen-skew`).
///
/// Every generated file resolves `capnpc` through a comptime check of the
/// runtime's `codegen_abi` (see src/codegen_abi.zig). This step generates a
/// binding with the generator from this tree, then compiles it against stub
/// runtimes that report other ABIs, and requires EXACTLY one compile error:
/// the guard's message naming the release to move to. Any other error, or a
/// second one, means a consumer with a mismatched plugin would again see
/// failures deep inside generated code.
///
/// The binding is generated at build time rather than taken from a committed
/// file, so the step follows the emitter: dropping the guard from
/// `Generator.generateFile` turns it red with no regeneration needed.
fn addCodegenSkewChecks(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step {
    const codegen_abi = @import("../src/codegen_abi.zig");
    const step = b.step("test-codegen-skew", "Compile generated code against older/newer stub runtimes and expect the one skew error");

    // The generator runs at build time, so it is built for the host.
    const host_core = b.createModule(.{
        .root_source_file = b.path("src/lib_core.zig"),
        .target = b.graph.host,
        .optimize = .Debug,
    });
    host_core.addImport("capnpc-zig", host_core);
    const generate_fixture = b.addExecutable(.{
        .name = "codegen-skew-fixture",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/codegen_skew/generate_fixture.zig"),
            .target = b.graph.host,
            .optimize = .Debug,
            .imports = &.{.{ .name = "capnpc-zig", .module = host_core }},
        }),
    });
    const run_generate = b.addRunArtifact(generate_fixture);
    const fixture = run_generate.addOutputFileArg("skew.zig");

    const too_old = std.fmt.comptimePrint(
        ":?:?: error: capnpc-zig version skew: this file was generated for codegen ABI {d}, which needs the capnpc-zig {s} runtime or newer, but the imported runtime provides ABI 0. Upgrade the capnpc-zig dependency, or regenerate the file with the plugin that matches it.",
        .{ codegen_abi.version, codegen_abi.release },
    );
    const too_new = std.fmt.comptimePrint(
        ":?:?: error: capnpc-zig version skew: this file was generated for codegen ABI {d}, but the imported capnpc-zig runtime (ABI 1000) only supports ABI 999 and newer. Regenerate the file with the capnpc-zig 99.0.0 plugin or newer.",
        .{codegen_abi.version},
    );
    const cases = [_]struct { name: []const u8, stub: []const u8, expected: []const u8 }{
        // A runtime that declares an older ABI than the plugin emits.
        .{ .name = "codegen-skew-older-abi", .stub = "tests/codegen_skew/stub_runtime_older_abi.zig", .expected = too_old },
        // A runtime from before the guard existed (no `codegen_abi` at all).
        .{ .name = "codegen-skew-pre-guard", .stub = "tests/codegen_skew/stub_runtime_pre_guard.zig", .expected = too_old },
        // A runtime that no longer supports the ABI the plugin emits.
        .{ .name = "codegen-skew-newer-abi", .stub = "tests/codegen_skew/stub_runtime_newer_abi.zig", .expected = too_new },
    };
    inline for (cases) |case| {
        const stub = b.createModule(.{
            .root_source_file = b.path(case.stub),
            .target = target,
            .optimize = optimize,
        });
        const fixture_module = b.createModule(.{
            .root_source_file = fixture,
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "capnpc-zig", .module = stub }},
        });
        const check = b.addObject(.{
            .name = case.name,
            .root_module = b.createModule(.{
                .root_source_file = b.path("tests/codegen_skew/use_fixture.zig"),
                .target = target,
                .optimize = optimize,
                .imports = &.{.{ .name = "fixture", .module = fixture_module }},
            }),
        });
        check.expect_errors = .{ .exact = &.{case.expected} };
        step.dependOn(&check.step);
    }
    return step;
}
