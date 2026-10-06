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
//! boundary: only the fields of the `Graph` below cross it.

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
    /// What a QUIC test root imports from the same quic-zig dependency
    /// (`helpers.addQuicLibTest`): only `quic`, as library roots do. Null
    /// without `-Dquic=true`.
    quic_test_imports: ?helpers.QuicTestImports,
    wasm_host_module: *std.Build.Step.Compile,
    /// The `capnp_build_options` module (`-Dfd-passing`). Every module
    /// rooted at a library root (`src/lib.zig`, `src/lib_quic.zig`,
    /// `src/lib_core.zig`) imports it: `src/rpc/transport/fd_passing.zig`
    /// reads it, and a compile that reaches that file without it fails with
    /// `no module named 'capnp_build_options'`.
    capnp_build_options_module: *std.Build.Module,
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
    // already provides. The `evented` selector fails with
    // error.EventedBackendUnsupported on every target at Zig 0.17.0, because
    // no std.Io.Evented compiles there (evented_available = false).
    const io_backend_kind = b.option(
        []const u8,
        "io-backend",
        "Io backend used by RPC entry points: process_init|threaded|evented (default: process_init)",
    ) orelse "process_init";

    const io_backend_options = b.addOptions();
    io_backend_options.addOption([]const u8, "kind", io_backend_kind);
    const io_backend_options_module = io_backend_options.createModule();

    // Fd passing, the fd closer threads, the process fd budget and the
    // AF_UNIX transport (src/rpc/transport/fd_passing.zig `supported`).
    // They exist only on Linux and macOS; `false` compiles them out there
    // too, for an embedder that owns its sockets and wants no closer thread.
    // A consumer passes it as `.@"fd-passing" = false` in `b.dependency`.
    // Like quic's, the option map is part of the module's identity: every
    // package that depends on capnp-zig in one build must pass the same map,
    // or the build gets two capnp-zig module sets.
    const fd_passing = b.option(
        bool,
        "fd-passing",
        "Compile in fd passing, the fd closer threads and the AF_UNIX transport on Linux and macOS (default: true)",
    ) orelse true;
    const capnp_build_options = b.addOptions();
    capnp_build_options.addOption(bool, "fd_passing", fd_passing);
    const capnp_build_options_module = capnp_build_options.createModule();

    // Create the library module
    const lib_module = b.addModule("capnpc-zig", .{
        .root_source_file = b.path(lib_root),
        .target = target,
        .optimize = optimize,
        .imports = &.{},
    });
    lib_module.addImport("capnpc-zig", lib_module);
    lib_module.addImport("capnp_build_options", capnp_build_options_module);

    const core_module = b.addModule("capnpc-zig-core", .{
        .root_source_file = b.path("src/lib_core.zig"),
        .target = target,
        .optimize = optimize,
    });
    core_module.addImport("capnpc-zig", core_module);
    core_module.addImport("capnp_build_options", capnp_build_options_module);

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
    const quic_dep: ?*std.Build.Dependency = if (enable_quic)
        try b.dependencyLazy("quic", .{
            .target = target,
            // quic-zig builds Debug or ReleaseSafe only, selected by the
            // boolean `release`. Through v0.24.0 it had no `optimize`
            // option at all: an `.optimize` here was reported as `invalid
            // option` and ignored, which silently built quic and BoringSSL
            // in Debug inside every ReleaseSafe build (`--verbose` showed
            // `-Odebug -Mquic=`). v0.24.1 also accepts `optimize`, but we
            // keep passing `release`: every parent of quic in one build
            // must pass the same option map (nest/qmsg/qmesh use this one),
            // or the build makes two quic modules.
            .release = optimize != .debug,
            // BoringSSL's C/C++ objects must not reference the UBSan
            // runtime: they are linked as static archives into test
            // binaries whose root module is Zig code, so nothing pulls
            // __ubsan_handle_* in and ReleaseSafe links fail on
            // Linux/lld (found by the v0.12->v0.14 bump, whose repinned
            // boringssl instruments C under safe modes by default).
            // `trap` keeps the UB checks and needs no runtime.
            .@"sanitize-c" = @as([]const u8, "trap"),
        })
    else
        null;
    const quic_zig_module: ?*std.Build.Module = if (quic_dep) |dep| dep.module("quic") else null;
    helpers.addQuicImport(lib_module, quic_zig_module);

    const wasm_target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
    });
    // Keep the distributable wasm small by default without changing native
    // debug builds. An explicit wasm mode also allows Debug for host debugging.
    const wasm_optimize = b.option(
        std.builtin.OptimizeMode,
        "wasm-optimize",
        "WebAssembly optimization mode (default: ReleaseSmall for Debug builds, otherwise optimize)",
    ) orelse if (optimize == std.builtin.OptimizeMode.Debug) std.builtin.OptimizeMode.ReleaseSmall else optimize;

    // The wasm host needs a wasm-targeted core module: mixing the host
    // `target` into the wasm exe's module graph breaks cross builds
    // (`zig build check-compile -Dtarget=...`).
    const core_module_wasm = b.createModule(.{
        .root_source_file = b.path("src/lib_core.zig"),
        .target = wasm_target,
        .optimize = wasm_optimize,
    });
    core_module_wasm.addImport("capnpc-zig", core_module_wasm);
    core_module_wasm.addImport("capnp_build_options", capnp_build_options_module);

    const wasm_example_schema_module = b.addModule("capnp-wasm-example-schema", .{
        .root_source_file = b.path("src/wasm/generated/example.zig"),
        .target = wasm_target,
        .optimize = wasm_optimize,
        .imports = &.{
            .{ .name = "capnpc-zig", .module = core_module_wasm },
        },
    });

    const wasm_host_module = b.addExecutable(.{
        .name = "capnp_wasm_host",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/wasm/capnp_host_abi.zig"),
            .target = wasm_target,
            .optimize = wasm_optimize,
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
        .quic_test_imports = helpers.QuicTestImports.of(quic_dep),
        .wasm_host_module = wasm_host_module,
        .capnp_build_options_module = capnp_build_options_module,
    };
}
