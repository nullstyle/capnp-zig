const std = @import("std");
const builtin = @import("builtin");

const max_process_output = 16 * 1024 * 1024;
const max_fixture_file = 32 * 1024 * 1024;

const Context = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    base_env: *const std.process.Environ.Map,
    repo_abs: []const u8,
    work_rel: []const u8,
    work_abs: []const u8,
    skip_quic: bool,
    keep_temp: bool,
};

const BuildCase = struct {
    profile: []const u8,
    optimize: []const u8,
};

const Args = struct {
    skip_quic: bool = false,
    keep_temp: bool = false,
};

const build_cases = [_]BuildCase{
    .{ .profile = "default", .optimize = "Debug" },
    .{ .profile = "default", .optimize = "ReleaseSafe" },
    .{ .profile = "core", .optimize = "Debug" },
    .{ .profile = "core", .optimize = "ReleaseSafe" },
    .{ .profile = "quic", .optimize = "Debug" },
    .{ .profile = "quic", .optimize = "ReleaseSafe" },
};

const allowed_package_roots = [_][]const u8{
    "build.zig",
    "build.zig.zon",
    // The B-series build decomposition: build.zig is a thin driver over
    // build/*.zig, so the package must ship the directory or a consumer's
    // `zig build` cannot even parse the graph. Required, like src/.
    "build",
    "src",
    "README.md",
    "LICENSE",
};

fn printUsage() void {
    std.debug.print(
        \\Usage: zig run tools/package_preflight.zig -- [--skip-quic] [--keep-temp]
        \\
        \\Builds a clean-room consumer from the `.paths`-filtered package:
        \\  - snapshots tracked + untracked source files outside the worktree
        \\  - lets `zig fetch` apply build.zig.zon's package filter
        \\  - archives and re-fetches that filtered result (never a path dependency)
        \\  - builds and runs default, core, and QUIC consumers in Debug/ReleaseSafe
        \\  - proves normal/core builds do not fetch the lazy QUIC dependency
        \\  - runs the packaged compiler plugin and compares checked-in output
        \\
    , .{});
}

fn parseArgs(init: std.process.Init) !Args {
    var iterator = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer iterator.deinit();
    _ = iterator.skip();

    var result: Args = .{};
    while (iterator.next()) |arg| {
        if (std.mem.eql(u8, arg, "--skip-quic")) {
            result.skip_quic = true;
        } else if (std.mem.eql(u8, arg, "--keep-temp")) {
            result.keep_temp = true;
        } else if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            printUsage();
            std.process.exit(0);
        } else {
            std.debug.print("unknown argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    return result;
}

fn run(
    ctx: *const Context,
    argv: []const []const u8,
    cwd: std.process.Child.Cwd,
    environ_map: ?*const std.process.Environ.Map,
) !std.process.RunResult {
    const result = try std.process.run(ctx.allocator, ctx.io, .{
        .argv = argv,
        .cwd = cwd,
        .environ_map = environ_map,
        .stdout_limit = .limited(max_process_output),
        .stderr_limit = .limited(max_process_output),
    });
    if (!result.term.success()) {
        std.debug.print(
            "command failed: {s}\nterm: {f}\nstdout:\n{s}\nstderr:\n{s}\n",
            .{ argv[0], result.term, result.stdout, result.stderr },
        );
        ctx.allocator.free(result.stdout);
        ctx.allocator.free(result.stderr);
        return error.PackagePreflightCommandFailed;
    }
    return result;
}

fn runDiscard(
    ctx: *const Context,
    argv: []const []const u8,
    cwd: std.process.Child.Cwd,
    environ_map: ?*const std.process.Environ.Map,
) !void {
    const result = try run(ctx, argv, cwd, environ_map);
    ctx.allocator.free(result.stdout);
    ctx.allocator.free(result.stderr);
}

fn join(allocator: std.mem.Allocator, parts: []const []const u8) ![]u8 {
    return std.fs.path.join(allocator, parts);
}

fn snapshotCheckout(ctx: *const Context, snapshot_abs: []const u8) !void {
    try std.Io.Dir.cwd().createDirPath(ctx.io, snapshot_abs);
    const files = try run(
        ctx,
        &.{ "git", "ls-files", "--cached", "--others", "--exclude-standard", "-z" },
        .inherit,
        null,
    );
    defer ctx.allocator.free(files.stdout);
    defer ctx.allocator.free(files.stderr);

    var paths = std.mem.splitScalar(u8, files.stdout, 0);
    var copied: usize = 0;
    while (paths.next()) |path| {
        if (path.len == 0) continue;
        const source_stat = try std.Io.Dir.cwd().statFile(ctx.io, path, .{});
        // Git records submodules as entries, but their checked-out paths are
        // directories. Skip them before creating destination parents; copying
        // first would leave empty development directories in the snapshot.
        if (source_stat.kind == .directory) continue;
        const destination = try join(ctx.allocator, &.{ snapshot_abs, path });
        defer ctx.allocator.free(destination);
        std.Io.Dir.cwd().copyFile(path, .cwd(), destination, ctx.io, .{
            .make_path = true,
        }) catch |err| switch (err) {
            // Git submodule entries name directories. They are development-only
            // and intentionally outside `.paths`, so no copy is needed.
            error.IsDir => continue,
            else => return err,
        };
        copied += 1;
    }
    if (copied == 0) return error.EmptyCheckoutSnapshot;
}

fn makeZigEnvironment(
    ctx: *const Context,
    global_cache: []const u8,
    local_cache: []const u8,
) !std.process.Environ.Map {
    var environment = try ctx.base_env.clone(ctx.allocator);
    errdefer environment.deinit();
    try environment.put("ZIG_GLOBAL_CACHE_DIR", global_cache);
    try environment.put("ZIG_LOCAL_CACHE_DIR", local_cache);
    return environment;
}

fn findFilteredStage(ctx: *const Context, global_cache: []const u8) ![]u8 {
    const temp_root = try join(ctx.allocator, &.{ global_cache, "tmp" });
    defer ctx.allocator.free(temp_root);
    var dir = try std.Io.Dir.cwd().openDir(ctx.io, temp_root, .{ .iterate = true });
    defer dir.close(ctx.io);

    var iterator = dir.iterate();
    while (try iterator.next(ctx.io)) |entry| {
        if (entry.kind != .directory or !std.mem.startsWith(u8, entry.name, ".tmp-")) continue;
        const candidate = try join(ctx.allocator, &.{ temp_root, entry.name });
        const manifest = try join(ctx.allocator, &.{ candidate, "build.zig.zon" });
        defer ctx.allocator.free(manifest);
        std.Io.Dir.cwd().access(ctx.io, manifest, .{}) catch {
            ctx.allocator.free(candidate);
            continue;
        };
        return candidate;
    }
    return error.FilteredPackageNotFound;
}

fn rootAllowed(name: []const u8) bool {
    for (allowed_package_roots) |allowed| {
        if (std.mem.eql(u8, name, allowed)) return true;
    }
    return false;
}

fn directoryContainsMaterialEntry(ctx: *const Context, path: []const u8) !bool {
    var dir = try std.Io.Dir.cwd().openDir(ctx.io, path, .{ .iterate = true });
    defer dir.close(ctx.io);
    var walker = try dir.walk(ctx.allocator);
    defer walker.deinit();
    while (try walker.next(ctx.io)) |entry| {
        if (entry.kind != .directory) return true;
    }
    return false;
}

fn assertFilteredSurface(ctx: *const Context, stage_abs: []const u8) !void {
    var dir = try std.Io.Dir.cwd().openDir(ctx.io, stage_abs, .{ .iterate = true });
    defer dir.close(ctx.io);

    var seen: [allowed_package_roots.len]bool = @splat(false);
    var empty_development_roots: std.ArrayList([]u8) = .empty;
    defer {
        for (empty_development_roots.items) |name| ctx.allocator.free(name);
        empty_development_roots.deinit(ctx.allocator);
    }
    var iterator = dir.iterate();
    while (try iterator.next(ctx.io)) |entry| {
        if (!rootAllowed(entry.name)) {
            if (entry.kind != .directory) {
                std.debug.print("filtered package leaked development path: {s}\n", .{entry.name});
                return error.PackagePathLeak;
            }
            const unexpected_path = try join(ctx.allocator, &.{ stage_abs, entry.name });
            defer ctx.allocator.free(unexpected_path);
            if (try directoryContainsMaterialEntry(ctx, unexpected_path)) {
                std.debug.print("filtered package leaked development path: {s}\n", .{entry.name});
                return error.PackagePathLeak;
            }
            // `zig fetch <local-directory>` currently leaves parent directory
            // entries after `.paths` removes all of their files. They carry no
            // package content; normalize them away before creating the archive
            // so the consumer sees the same five-root surface we assert.
            try empty_development_roots.append(ctx.allocator, try ctx.allocator.dupe(u8, entry.name));
            continue;
        }
        for (allowed_package_roots, 0..) |allowed, index| {
            if (std.mem.eql(u8, entry.name, allowed)) seen[index] = true;
        }
    }
    for (allowed_package_roots, seen) |required, present| {
        if (!present) {
            std.debug.print("filtered package omitted required path: {s}\n", .{required});
            return error.RequiredPackagePathMissing;
        }
    }
    for (empty_development_roots.items) |name| try dir.deleteTree(ctx.io, name);
}

fn copyTree(ctx: *const Context, source_path: []const u8, destination_path: []const u8) !void {
    var source = try std.Io.Dir.cwd().openDir(ctx.io, source_path, .{ .iterate = true });
    defer source.close(ctx.io);
    try std.Io.Dir.cwd().createDirPath(ctx.io, destination_path);
    var destination = try std.Io.Dir.cwd().openDir(ctx.io, destination_path, .{});
    defer destination.close(ctx.io);

    var walker = try source.walk(ctx.allocator);
    defer walker.deinit();
    while (try walker.next(ctx.io)) |entry| {
        switch (entry.kind) {
            .directory => try destination.createDirPath(ctx.io, entry.path),
            .file => try source.copyFile(entry.path, destination, entry.path, ctx.io, .{ .make_path = true }),
            else => return error.UnsupportedFixtureEntry,
        }
    }
}

fn cacheContainsQuicPackage(ctx: *const Context, cache_path: []const u8) !bool {
    var cache = std.Io.Dir.cwd().openDir(ctx.io, cache_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer cache.close(ctx.io);

    var walker = try cache.walk(ctx.allocator);
    defer walker.deinit();
    while (try walker.next(ctx.io)) |entry| {
        // Zig may keep a fetched package as a content-addressed archive in the
        // global `p/` directory rather than extracting its manifest there.
        if (std.mem.startsWith(u8, std.fs.path.basename(entry.path), "quic_zig-")) return true;
        if (entry.kind != .file or !std.mem.eql(u8, std.fs.path.basename(entry.path), "build.zig.zon")) continue;
        const bytes = try cache.readFileAlloc(ctx.io, entry.path, ctx.allocator, .limited(max_fixture_file));
        defer ctx.allocator.free(bytes);
        if (std.mem.indexOf(u8, bytes, ".name = .quic_zig") != null) return true;
    }
    return false;
}

fn executableName(allocator: std.mem.Allocator, stem: []const u8) ![]u8 {
    return if (builtin.os.tag == .windows)
        std.fmt.allocPrint(allocator, "{s}.exe", .{stem})
    else
        allocator.dupe(u8, stem);
}

fn runConsumerBuilds(
    ctx: *const Context,
    consumer_abs: []const u8,
    consumer_env: *const std.process.Environ.Map,
    consumer_global_cache: []const u8,
) ![]u8 {
    var plugin_path: ?[]u8 = null;
    errdefer if (plugin_path) |path| ctx.allocator.free(path);

    for (build_cases) |case| {
        if (ctx.skip_quic and std.mem.eql(u8, case.profile, "quic")) continue;
        const install_dir_name = try std.fmt.allocPrint(
            ctx.allocator,
            "install-{s}-{s}",
            .{ case.profile, case.optimize },
        );
        defer ctx.allocator.free(install_dir_name);
        const install_abs = try join(ctx.allocator, &.{ ctx.work_abs, install_dir_name });
        defer ctx.allocator.free(install_abs);
        const profile_arg = try std.fmt.allocPrint(ctx.allocator, "-Dconsumer-profile={s}", .{case.profile});
        defer ctx.allocator.free(profile_arg);
        const optimize_arg = try std.fmt.allocPrint(ctx.allocator, "-Doptimize={s}", .{case.optimize});
        defer ctx.allocator.free(optimize_arg);

        try runDiscard(ctx, &.{
            "zig",
            "build",
            profile_arg,
            optimize_arg,
            "--prefix",
            install_abs,
            "--summary",
            "all",
        }, .{ .path = consumer_abs }, consumer_env);

        const consumer_stem = try std.fmt.allocPrint(ctx.allocator, "consumer-{s}", .{case.profile});
        defer ctx.allocator.free(consumer_stem);
        const consumer_exe = try executableName(ctx.allocator, consumer_stem);
        defer ctx.allocator.free(consumer_exe);
        const consumer_path = try join(ctx.allocator, &.{ install_abs, "bin", consumer_exe });
        defer ctx.allocator.free(consumer_path);
        try runDiscard(ctx, &.{consumer_path}, .inherit, null);

        if (std.mem.eql(u8, case.profile, "default") and
            std.mem.eql(u8, case.optimize, "Debug"))
        {
            const plugin_exe = try executableName(ctx.allocator, "capnpc-zig");
            defer ctx.allocator.free(plugin_exe);
            plugin_path = try join(ctx.allocator, &.{ install_abs, "bin", plugin_exe });
        }

        if (std.mem.eql(u8, case.profile, "core") and
            std.mem.eql(u8, case.optimize, "ReleaseSafe"))
        {
            if (try cacheContainsQuicPackage(ctx, consumer_global_cache)) {
                return error.LazyQuicDependencyFetchedByNormalConsumer;
            }
        }
    }

    if (!ctx.skip_quic and !try cacheContainsQuicPackage(ctx, consumer_global_cache)) {
        return error.QuicConsumerDidNotFetchQuicPackage;
    }
    return plugin_path orelse error.PackagedPluginMissing;
}

fn verifyPackagedPlugin(ctx: *const Context, plugin_abs: []const u8) !void {
    const plugin_rel = try std.fs.path.relative(
        ctx.allocator,
        ctx.repo_abs,
        ctx.base_env,
        ctx.repo_abs,
        plugin_abs,
    );
    defer ctx.allocator.free(plugin_rel);
    const output_rel = try join(ctx.allocator, &.{ ctx.work_rel, "plugin-output" });
    defer ctx.allocator.free(output_rel);
    try std.Io.Dir.cwd().createDirPath(ctx.io, output_rel);
    const output_option = try std.fmt.allocPrint(ctx.allocator, "-o{s}:{s}", .{ plugin_rel, output_rel });
    defer ctx.allocator.free(output_option);

    try runDiscard(ctx, &.{
        "capnp",
        "compile",
        "-Ivendor/ext/capnproto/c++/src",
        output_option,
        "tests/test_schemas/enum_evolution_v1.capnp",
    }, .inherit, null);

    const actual_path = try join(ctx.allocator, &.{
        output_rel,
        "tests",
        "test_schemas",
        "enum_evolution_v1.zig",
    });
    defer ctx.allocator.free(actual_path);
    // Checked-in generator artifacts are normalized by `just fmt` after the
    // plugin runs. Apply that same repository contract before byte comparison;
    // raw generator output intentionally is not the golden byte surface.
    try runDiscard(ctx, &.{ "zig", "fmt", actual_path }, .inherit, null);
    const actual = try std.Io.Dir.cwd().readFileAlloc(ctx.io, actual_path, ctx.allocator, .limited(max_fixture_file));
    defer ctx.allocator.free(actual);
    const expected = try std.Io.Dir.cwd().readFileAlloc(
        ctx.io,
        "tests/serialization/generated/schema_evolution_v1.zig",
        ctx.allocator,
        .limited(max_fixture_file),
    );
    defer ctx.allocator.free(expected);
    if (!std.mem.eql(u8, actual, expected)) return error.PackagedPluginGoldenMismatch;
}

fn status(ctx: *const Context) ![]u8 {
    const result = try run(
        ctx,
        &.{ "git", "status", "--porcelain=v1", "--untracked-files=all" },
        .inherit,
        null,
    );
    ctx.allocator.free(result.stderr);
    return result.stdout;
}

pub fn main(init: std.process.Init) !void {
    const args = try parseArgs(init);
    const cwd_abs = try std.process.currentPathAlloc(init.io, init.gpa);
    defer init.gpa.free(cwd_abs);
    std.Io.Dir.cwd().access(init.io, "build.zig.zon", .{}) catch {
        std.debug.print("package preflight must run from the capnp-zig repository root\n", .{});
        return error.NotRepositoryRoot;
    };

    const nonce = std.Io.Timestamp.now(init.io, .awake).toNanoseconds();
    const work_rel = try std.fmt.allocPrint(init.gpa, ".zig-cache/package-preflight-{d}", .{nonce});
    defer init.gpa.free(work_rel);
    const work_abs = try join(init.gpa, &.{ cwd_abs, work_rel });
    defer init.gpa.free(work_abs);
    try std.Io.Dir.cwd().createDirPath(init.io, work_rel);

    const ctx = Context{
        .allocator = init.gpa,
        .io = init.io,
        .base_env = init.environ_map,
        .repo_abs = cwd_abs,
        .work_rel = work_rel,
        .work_abs = work_abs,
        .skip_quic = args.skip_quic,
        .keep_temp = args.keep_temp,
    };
    defer if (!ctx.keep_temp) std.Io.Dir.cwd().deleteTree(init.io, work_rel) catch {};

    const before = try status(&ctx);
    defer init.gpa.free(before);

    const snapshot_abs = try join(init.gpa, &.{ work_abs, "checkout" });
    defer init.gpa.free(snapshot_abs);
    try snapshotCheckout(&ctx, snapshot_abs);

    const stage_global = try join(init.gpa, &.{ work_abs, "stage-global-cache" });
    defer init.gpa.free(stage_global);
    const stage_local = try join(init.gpa, &.{ work_abs, "stage-local-cache" });
    defer init.gpa.free(stage_local);
    var stage_env = try makeZigEnvironment(&ctx, stage_global, stage_local);
    defer stage_env.deinit();
    const fetch_result = try run(&ctx, &.{ "zig", "fetch", snapshot_abs }, .inherit, &stage_env);
    defer init.gpa.free(fetch_result.stdout);
    defer init.gpa.free(fetch_result.stderr);
    if (!std.mem.startsWith(u8, std.mem.trim(u8, fetch_result.stdout, " \r\n\t"), "capnpc_zig-")) {
        return error.UnexpectedPackageHash;
    }

    const filtered_stage = try findFilteredStage(&ctx, stage_global);
    defer init.gpa.free(filtered_stage);
    try assertFilteredSurface(&ctx, filtered_stage);

    const archive_abs = try join(init.gpa, &.{ work_abs, "capnpc-zig-filtered.tar.gz" });
    defer init.gpa.free(archive_abs);
    try runDiscard(&ctx, &.{ "tar", "-czf", archive_abs, "." }, .{ .path = filtered_stage }, null);

    const consumer_abs = try join(init.gpa, &.{ work_abs, "consumer" });
    defer init.gpa.free(consumer_abs);
    try copyTree(&ctx, "tests/package_consumer", consumer_abs);

    const consumer_global = try join(init.gpa, &.{ work_abs, "consumer-global-cache" });
    defer init.gpa.free(consumer_global);
    const consumer_local = try join(init.gpa, &.{ work_abs, "consumer-local-cache" });
    defer init.gpa.free(consumer_local);
    var consumer_env = try makeZigEnvironment(&ctx, consumer_global, consumer_local);
    defer consumer_env.deinit();
    try runDiscard(
        &ctx,
        &.{ "zig", "fetch", "--save=capnpc_zig", archive_abs },
        .{ .path = consumer_abs },
        &consumer_env,
    );

    const plugin_path = try runConsumerBuilds(&ctx, consumer_abs, &consumer_env, consumer_global);
    defer init.gpa.free(plugin_path);
    try verifyPackagedPlugin(&ctx, plugin_path);

    const after = try status(&ctx);
    defer init.gpa.free(after);
    if (!std.mem.eql(u8, before, after)) {
        std.debug.print("package preflight changed the worktree:\n{s}\n", .{after});
        return error.PackagePreflightChangedWorktree;
    }

    std.debug.print(
        "package preflight passed: filtered archive, default/core{s}, packaged plugin, clean worktree\n",
        .{if (ctx.skip_quic) " (QUIC skipped)" else "/QUIC"},
    );
    if (ctx.keep_temp) std.debug.print("kept preflight workspace at {s}\n", .{ctx.work_abs});
}
