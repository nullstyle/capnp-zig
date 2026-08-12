const std = @import("std");

const max_file_bytes = 8 * 1024 * 1024;

const Needle = struct {
    needle: []const u8,
    reason: []const u8,
};

const RequiredNeedle = struct {
    path: []const u8,
    needle: []const u8,
    reason: []const u8,
};

const active_docs = [_][]const u8{
    "README.md",
    "CLAUDE.md",
    "CHANGELOG.md",
    "llms.txt",
    "src/rpc/llms.txt",
    "tests/llms.txt",
    "docs/architecture.md",
    "docs/api_contracts.md",
    "docs/build-integration.md",
    "docs/getting-started-rpc.md",
    "docs/getting-started-serialization.md",
    "docs/quic-transport.md",
    "docs/rpc_runtime_design.md",
    "docs/security-regression-matrix.md",
    "docs/stability.md",
    "docs/troubleshooting.md",
};

const required_paths = [_][]const u8{
    "README.md",
    "CHANGELOG.md",
    "CLAUDE.md",
    "RELEASING.md",
    "docs/architecture.md",
    "docs/getting-started-rpc.md",
    "docs/getting-started-serialization.md",
    "docs/quic-transport.md",
    "docs/rpc-migration-guide.md",
    "examples/rpc_pingpong.zig",
    "examples/pingpong.zig",
    "examples/pingpong.capnp",
};

const doc_forbidden = [_]Needle{
    .{ .needle = "capnpc.rpc.quic", .reason = "QUIC public API lives under capnpc.rpc.transport.quic" },
    // The error is spelled `EventedBackendUnsupported`. Note this forbids a
    // wrong ERROR NAME, not a claim that the backend works: at dev.1683 the
    // evented selector compiles but has no socket vtable upstream, which
    // docs/stability.md and README.md now say explicitly.
    .{ .needle = "EventedBackendNotImplemented", .reason = "the selector's error is spelled EventedBackendUnsupported" },
    .{ .needle = "placeholder for `std.Io.Evented`", .reason = "evented docs must describe the real selector, not a placeholder" },
    .{ .needle = "organized following the Cap'n Proto RPC specification levels", .reason = "RPC layout is domain-shaped now" },
    .{ .needle = "runtime.loop", .reason = "TCP examples should use std.Io and Connection.run()" },
    .{ .needle = "event loop thread", .reason = "TCP examples should refer to the connection owner thread" },
    .{ .needle = "connect callback", .reason = "TCP examples should not mention removed callback-loop APIs" },
    .{ .needle = "@import(\"xev\")", .reason = "active docs/examples should not require xev" },
    .{ .needle = "no RPC/xev dependency", .reason = "core-module docs should use current transport wording" },
};

const source_forbidden = [_]Needle{
    .{ .needle = "capnpc.rpc.quic", .reason = "use capnpc.rpc.transport.quic" },
    .{ .needle = "capnpc.rpc.protocol", .reason = "use capnpc.rpc.wire.protocol" },
    .{ .needle = "capnpc.rpc.framing", .reason = "use capnpc.rpc.wire.framing" },
    .{ .needle = "capnpc.rpc.cap_table", .reason = "use capnpc.rpc.caps.table" },
    .{ .needle = "capnpc.rpc.promise_pipeline", .reason = "use capnpc.rpc.promises.pipeline" },
    .{ .needle = "capnpc.rpc.connection", .reason = "use capnpc.rpc.transport.tcp.Connection" },
    .{ .needle = "capnpc.rpc.runtime", .reason = "use capnpc.rpc.transport.tcp.Runtime" },
    .{ .needle = "capnpc.rpc.transport_binding", .reason = "use capnpc.rpc.transport.binding" },
    .{ .needle = "capnpc.rpc.host_peer", .reason = "use capnpc.rpc.integration.host_peer" },
    .{ .needle = "capnpc.rpc._internal", .reason = "use capnpc.rpc.testing in tests only" },
    .{ .needle = "rpc.protocol", .reason = "use rpc.wire.protocol" },
    .{ .needle = "rpc.framing", .reason = "use rpc.wire.framing" },
    .{ .needle = "rpc.cap_table", .reason = "use rpc.caps.table" },
    .{ .needle = "rpc.promise_pipeline", .reason = "use rpc.promises.pipeline" },
    .{ .needle = "rpc.connection", .reason = "use rpc.transport.tcp.Connection" },
    .{ .needle = "rpc.runtime", .reason = "use rpc.transport.tcp.Runtime" },
    .{ .needle = "rpc.transport_binding", .reason = "use rpc.transport.binding" },
    .{ .needle = "rpc.host_peer", .reason = "use rpc.integration.host_peer" },
    .{ .needle = "rpc._internal", .reason = "use rpc.testing in tests only" },
};

const source_dirs = [_][]const u8{
    "src",
    "tests",
    "examples",
};

const required_build_steps = [_][]const u8{
    "check",
    "docs",
    "docs-smoke",
    "example-rpc",
    "example-rpc-install",
    "test-docs-snippets",
    "test-rpc-wire",
    "test-rpc-caps",
    "test-rpc-promises",
    "test-rpc-transport",
    "test-rpc-peer",
    "test-rpc-integration",
    "test-rpc-quic",
};

const required_just_recipes = [_][]const u8{
    "check",
    "docs",
    "docs-smoke",
    "release-tag",
    "example",
    "test-docs-snippets",
    "test-rpc-wire",
    "test-rpc-caps",
    "test-rpc-promises",
    "test-rpc-transport",
    "test-rpc-peer",
    "test-rpc-integration",
    "test-rpc-quic",
};

const required_doc_needles = [_]RequiredNeedle{
    .{ .path = "README.md", .needle = "zig build docs-smoke", .reason = "README should advertise the docs/examples smoke gate" },
    .{ .path = "README.md", .needle = "rpc.transport.quic", .reason = "README should use the current QUIC public API path" },
    .{ .path = "CHANGELOG.md", .needle = "RPC public exports", .reason = "changelog should preserve the public-breaking RPC migration note" },
    .{ .path = "docs/architecture.md", .needle = "rpc.events", .reason = "architecture doc should list the event observer surface" },
    .{ .path = "docs/getting-started-rpc.md", .needle = "ServerSession", .reason = "RPC guide should describe the current one-call server driver" },
    .{ .path = "docs/getting-started-serialization.md", .needle = "capnpc-zig-core", .reason = "serialization guide should document the core module import" },
    .{ .path = "docs/quic-transport.md", .needle = "rpc.transport.quic.Server", .reason = "QUIC guide should document multi-session fanout" },
    .{ .path = "docs/rpc-migration-guide.md", .needle = "rpc.protocol", .reason = "migration guide should preserve old-name mapping coverage" },
    .{ .path = "examples/rpc_pingpong.zig", .needle = "rpc.transport.tcp.ServerSession", .reason = "RPC example should use the current one-call session transport path" },
};

/// A doc string that must carry the version declared in `build.zig.zon`.
/// `{v}` expands to the bare version (`0.5.0`), so `v{v}` renders `v0.5.0`.
///
/// This is the mechanical half of the release ceremony in RELEASING.md: the
/// v0.4.0 cut bumped the manifest and left every consumer-facing doc pinned to
/// v0.3.0, which nothing caught. These make that failure a red build.
const VersionNeedle = struct {
    path: []const u8,
    template: []const u8,
    reason: []const u8,
};

const version_needles = [_]VersionNeedle{
    .{ .path = "README.md", .template = "**Status (v{v}):**", .reason = "README status banner must name the released version" },
    .{ .path = "README.md", .template = "capnp-zig.git#v{v}", .reason = "README install snippet must pin the released version" },
    .{ .path = "docs/build-integration.md", .template = "capnp-zig.git#v{v}", .reason = "build-integration install snippet must pin the released version" },
    .{ .path = "docs/build-integration.md", .template = "capnpc_zig-{v}-", .reason = "build-integration hash example must match the released version" },
    .{ .path = "docs/supported-surface.md", .template = "# Supported Surface (v{v})", .reason = "the authoritative consumer contract must be titled for the released version" },
    .{ .path = "docs/supported-surface.md", .template = "#v{v}", .reason = "supported-surface pinning advice must name the released version" },
    .{ .path = "docs/supported-surface.md", .template = "## Known limitations (v{v})", .reason = "known-limitations heading must name the released version" },
    .{ .path = "docs/stability.md", .template = "The current version is **{v}**", .reason = "stability semver guidance must name the released version" },
    .{ .path = "CHANGELOG.md", .template = "## [{v}] - ", .reason = "CHANGELOG must carry a dated section for the released version" },
    .{ .path = "CHANGELOG.md", .template = "[{v}]: https://github.com/nullstyle/capnp-zig/compare/", .reason = "CHANGELOG link footer must define the released version" },
};

/// Markers whose immediately-following text is a version. Any occurrence in
/// `version_pinned_docs` that does not continue with the manifest version is a
/// stale pin — the failure mode that shipped consumers at v0.3.0 for two
/// releases.
const version_pin_markers = [_][]const u8{
    "capnp-zig.git#v",
    "capnpc_zig-",
};

const version_pinned_docs = [_][]const u8{
    "README.md",
    "docs/build-integration.md",
};

const Context = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    failures: usize = 0,
    checked_files: usize = 0,
    checks: usize = 0,

    fn fail(self: *Context, comptime fmt: []const u8, args: anytype) void {
        self.failures += 1;
        std.debug.print("[FAIL] " ++ fmt ++ "\n", args);
    }
};

fn printUsage() void {
    std.debug.print(
        \\Usage: zig build docs-smoke
        \\
        \\Static documentation/example smoke gate:
        \\  - verifies release-facing docs and examples exist
        \\  - checks documented build and Justfile recipes still exist
        \\  - rejects stale RPC public-surface names in source/examples/tests
        \\  - rejects stale event-loop/xev wording in active docs
        \\  - requires consumer-facing docs to carry the build.zig.zon version
        \\
    , .{});
}

fn parseArgs(allocator: std.mem.Allocator, args: std.process.Args) !bool {
    // initAllocator is the cross-platform form; plain init is a
    // compile error on Windows, where `zig build check` also runs in CI.
    var iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer iter.deinit();
    _ = iter.skip();
    while (iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            printUsage();
            return false;
        }
        return error.InvalidArgument;
    }
    return true;
}

fn readFile(ctx: *Context, path: []const u8) ![]u8 {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(ctx.io, path, ctx.allocator, .limited(max_file_bytes));
    ctx.checked_files += 1;
    return bytes;
}

fn ensureRequiredPaths(ctx: *Context) void {
    for (required_paths) |path| {
        ctx.checks += 1;
        const bytes = readFile(ctx, path) catch |err| {
            ctx.fail("missing required path {s}: {s}", .{ path, @errorName(err) });
            continue;
        };
        ctx.allocator.free(bytes);
    }
}

fn scanForbiddenInFile(ctx: *Context, path: []const u8, needles: []const Needle) !void {
    const bytes = try readFile(ctx, path);
    defer ctx.allocator.free(bytes);
    ctx.checks += needles.len;

    var line_no: usize = 1;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw_line_with_cr| : (line_no += 1) {
        const line = std.mem.trimEnd(u8, raw_line_with_cr, "\r");
        for (needles) |needle| {
            if (std.mem.indexOf(u8, line, needle.needle) != null) {
                const trimmed = std.mem.trim(u8, line, " \t\r");
                ctx.fail("{s}:{d}: stale text `{s}` ({s}): {s}", .{
                    path,
                    line_no,
                    needle.needle,
                    needle.reason,
                    trimmed,
                });
            }
        }
    }
}

fn scanActiveDocs(ctx: *Context) !void {
    for (active_docs) |path| {
        scanForbiddenInFile(ctx, path, &doc_forbidden) catch |err| {
            ctx.fail("could not scan active doc {s}: {s}", .{ path, @errorName(err) });
        };
    }
}

fn scanSourceFile(ctx: *Context, path: []const u8) !void {
    try scanForbiddenInFile(ctx, path, &source_forbidden);
}

fn normalizePathInPlace(path: []u8) void {
    for (path) |*c| {
        if (c.* == std.fs.path.sep) c.* = '/';
    }
}

fn shouldSkipSourcePath(path: []const u8) bool {
    return std.mem.startsWith(u8, path, "src/rpc/gen/") or
        std.mem.startsWith(u8, path, "src/wasm/generated/") or
        std.mem.startsWith(u8, path, "tests/e2e/zig/generated/") or
        std.mem.startsWith(u8, path, "tests/golden/") or
        std.mem.startsWith(u8, path, "examples/kvstore/gen/") or
        std.mem.startsWith(u8, path, "examples/kvstore/vendor/") or
        std.mem.startsWith(u8, path, "examples/kvstore/zig-pkg/");
}

fn scanSourceDir(ctx: *Context, dir_path: []const u8) !void {
    var dir = try std.Io.Dir.cwd().openDir(ctx.io, dir_path, .{ .iterate = true });
    defer dir.close(ctx.io);

    var walker = try dir.walk(ctx.allocator);
    defer walker.deinit();

    while (try walker.next(ctx.io)) |entry| {
        const path = try std.fmt.allocPrint(ctx.allocator, "{s}/{s}", .{ dir_path, entry.path });
        defer ctx.allocator.free(path);
        normalizePathInPlace(path);

        if (entry.kind == .directory) {
            if (shouldSkipSourcePath(path)) walker.leave(ctx.io);
            continue;
        }
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, path, ".zig")) continue;
        if (shouldSkipSourcePath(path)) continue;
        try scanSourceFile(ctx, path);
    }
}

fn scanSourcePublicNames(ctx: *Context) !void {
    for (source_dirs) |dir_path| try scanSourceDir(ctx, dir_path);
}

fn requireNeedle(ctx: *Context, path: []const u8, needle: []const u8, reason: []const u8) void {
    ctx.checks += 1;
    const bytes = readFile(ctx, path) catch |err| {
        ctx.fail("could not read {s} while checking `{s}`: {s}", .{ path, needle, @errorName(err) });
        return;
    };
    defer ctx.allocator.free(bytes);

    if (std.mem.indexOf(u8, bytes, needle) == null) {
        ctx.fail("{s}: missing `{s}` ({s})", .{ path, needle, reason });
    }
}

fn hasBuildStep(build_zig: []const u8, allocator: std.mem.Allocator, step: []const u8) !bool {
    const needle = try std.fmt.allocPrint(allocator, "b.step(\"{s}\"", .{step});
    defer allocator.free(needle);
    return std.mem.indexOf(u8, build_zig, needle) != null;
}

fn hasJustRecipe(justfile: []const u8, recipe: []const u8) bool {
    var lines = std.mem.splitScalar(u8, justfile, '\n');
    while (lines.next()) |raw_line_with_cr| {
        const line = std.mem.trimEnd(u8, raw_line_with_cr, "\r");
        if (line.len == 0 or std.ascii.isWhitespace(line[0]) or line[0] == '#') continue;
        if (!std.mem.startsWith(u8, line, recipe)) continue;
        if (line.len == recipe.len) continue;
        const next = line[recipe.len];
        if (next == ':') return true;
        if (next == ' ' and std.mem.indexOfScalar(u8, line[recipe.len..], ':') != null) return true;
    }
    return false;
}

fn verifyBuildAndJustfile(ctx: *Context) !void {
    // The B-series build decomposition made build.zig a thin driver; the step
    // registrations live in build/build_impl.zig. Scan both so the documented
    // step check keeps its teeth against the real registration site.
    const build_zig_driver = try readFile(ctx, "build.zig");
    defer ctx.allocator.free(build_zig_driver);
    const build_zig_impl = try readFile(ctx, "build/build_impl.zig");
    defer ctx.allocator.free(build_zig_impl);
    const build_zig = try std.mem.concat(ctx.allocator, u8, &.{ build_zig_driver, build_zig_impl });
    defer ctx.allocator.free(build_zig);
    const justfile = try readFile(ctx, "Justfile");
    defer ctx.allocator.free(justfile);

    for (required_build_steps) |step| {
        ctx.checks += 1;
        if (!(try hasBuildStep(build_zig, ctx.allocator, step))) {
            ctx.fail("build.zig: missing build step `{s}`", .{step});
        }
    }

    for (required_just_recipes) |recipe| {
        ctx.checks += 1;
        if (!hasJustRecipe(justfile, recipe)) {
            ctx.fail("Justfile: missing recipe `{s}`", .{recipe});
        }
    }
}

fn verifyRequiredDocNeedles(ctx: *Context) void {
    for (required_doc_needles) |item| {
        requireNeedle(ctx, item.path, item.needle, item.reason);
    }
}

fn parseManifestVersion(manifest: []const u8) ?[]const u8 {
    const key = ".version = \"";
    const start = std.mem.indexOf(u8, manifest, key) orelse return null;
    const rest = manifest[start + key.len ..];
    const end = std.mem.indexOfScalar(u8, rest, '"') orelse return null;
    return rest[0..end];
}

fn renderVersionTemplate(allocator: std.mem.Allocator, template: []const u8, version: []const u8) ![]u8 {
    const size = std.mem.replacementSize(u8, template, "{v}", version);
    const out = try allocator.alloc(u8, size);
    _ = std.mem.replace(u8, template, "{v}", version, out);
    return out;
}

fn scanStalePins(ctx: *Context, path: []const u8, version: []const u8) !void {
    const bytes = try readFile(ctx, path);
    defer ctx.allocator.free(bytes);
    ctx.checks += version_pin_markers.len;

    var line_no: usize = 1;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw_line_with_cr| : (line_no += 1) {
        const line = std.mem.trimEnd(u8, raw_line_with_cr, "\r");
        for (version_pin_markers) |marker| {
            var search: usize = 0;
            while (std.mem.indexOfPos(u8, line, search, marker)) |idx| {
                const after = line[idx + marker.len ..];
                if (!std.mem.startsWith(u8, after, version)) {
                    ctx.fail(
                        "{s}:{d}: `{s}` pin does not match build.zig.zon version {s}",
                        .{ path, line_no, marker, version },
                    );
                }
                search = idx + marker.len;
            }
        }
    }
}

fn verifyVersionStamps(ctx: *Context) !void {
    const manifest = try readFile(ctx, "build.zig.zon");
    defer ctx.allocator.free(manifest);

    ctx.checks += 1;
    const version = parseManifestVersion(manifest) orelse {
        ctx.fail("build.zig.zon: could not parse `.version = \"...\"`", .{});
        return;
    };

    for (version_needles) |item| {
        const needle = try renderVersionTemplate(ctx.allocator, item.template, version);
        defer ctx.allocator.free(needle);
        requireNeedle(ctx, item.path, needle, item.reason);
    }

    for (version_pinned_docs) |path| {
        try scanStalePins(ctx, path, version);
    }
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    const io = init.io;

    const should_run = parseArgs(allocator, init.minimal.args) catch |err| {
        std.debug.print("Argument error: {s}\n", .{@errorName(err)});
        printUsage();
        return error.InvalidArgument;
    };
    if (!should_run) return;

    var ctx = Context{
        .allocator = allocator,
        .io = io,
    };

    ensureRequiredPaths(&ctx);
    try scanActiveDocs(&ctx);
    try scanSourcePublicNames(&ctx);
    try verifyBuildAndJustfile(&ctx);
    verifyRequiredDocNeedles(&ctx);
    try verifyVersionStamps(&ctx);

    if (ctx.failures != 0) {
        std.debug.print(
            "Docs/examples smoke failed: {d} failure(s), {d} checks, {d} file reads\n",
            .{ ctx.failures, ctx.checks, ctx.checked_files },
        );
        return error.DocsExamplesSmokeFailed;
    }

    std.debug.print(
        "Docs/examples smoke passed: {d} checks, {d} file reads\n",
        .{ ctx.checks, ctx.checked_files },
    );
}
