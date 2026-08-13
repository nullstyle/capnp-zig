//! Module graph + wasm host setup — the first build cluster (B-series
//! decomposition).
//!
//! This runs FIRST and stays one contiguous slice for two reasons:
//!
//!  1. Registration order is the build's public ABI. `wasm-host`/`wasm-deno`
//!     are the first two steps registered, and they are registered here.
//!  2. The `try` on the lazy quic-zig dependency is load-bearing: it must
//!     propagate `error.LazyDependencyNeeded` out of `build` so Zig fetches
//!     and re-runs configure. See the note on `buildImpl` in build_impl.zig.
//!
//! Everything after this cluster is densely coupled (a mid-file cut costs
//! 68-95 threaded locals, measured), so the decomposition stops at this
//! boundary: exactly ten values cross it, and they are the `Graph` below.

const std = @import("std");
const helpers = @import("./helpers.zig");

/// The values the rest of the build graph consumes from this cluster.
pub const Graph = struct {
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    enable_quic: bool,
    lib_root: []const u8,
    io_backend_options_module: *std.Build.Module,
    lib_module: *std.Build.Module,
    core_module: *std.Build.Module,
    quic_zig_module: ?*std.Build.Module,
    wasm_host_module: *std.Build.Step.Compile,
};

/// Returns `!Graph` so `error.LazyDependencyNeeded` propagates (see above).
pub fn setup(b: *std.Build) !Graph {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const enable_quic = b.option(
        bool,
        "quic",
        "Enable quic-zig-backed QUIC RPC transport (default: false)",
    ) orelse false;
    const lib_root = if (enable_quic) "src/lib_quic.zig" else "src/lib.zig";

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

    const core_module = b.addModule("capnpc-zig-core", .{
        .root_source_file = b.path("src/lib_core.zig"),
        .target = target,
        .optimize = optimize,
    });
    core_module.addImport("capnpc-zig", core_module);

    // Register the package's public modules before resolving the optional lazy
    // dependency. When this project is itself a child dependency, Zig catches
    // `LazyDependencyNeeded` and exposes the partial child builder to the
    // consumer during its fetch/reconfigure pass. If dependency resolution
    // happens first, that partial builder contains no `capnpc-zig` module and a
    // clean opt-in QUIC consumer panics before Zig can fetch and retry.
    //
    // Keep the normal module graph free of quic-zig/BoringSSL. The dependency
    // is declared lazy in build.zig.zon so non-QUIC builds neither fetch it nor
    // compile its build.zig; it is resolved only for `.quic = true` consumers.
    const quic_zig_module: ?*std.Build.Module = if (enable_quic)
        (try b.dependencyLazy("quic", .{
            .target = target,
            .optimize = optimize,
        })).module("quic")
    else
        null;
    helpers.addQuicImport(lib_module, quic_zig_module);

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
    return .{
        .target = target,
        .optimize = optimize,
        .enable_quic = enable_quic,
        .lib_root = lib_root,
        .io_backend_options_module = io_backend_options_module,
        .lib_module = lib_module,
        .core_module = core_module,
        .quic_zig_module = quic_zig_module,
        .wasm_host_module = wasm_host_module,
    };
}
