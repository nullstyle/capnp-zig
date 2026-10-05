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
    "docs/reflection.md",
    "docs/generated-api.md",
    "docs/rpc_runtime_design.md",
    "docs/rpc-unix-sockets.md",
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
    "docs/reflection.md",
    "docs/generated-api.md",
    "docs/rpc-migration-guide.md",
    "docs/rpc-unix-sockets.md",
    "examples/rpc_pingpong.zig",
    "examples/rpc_pingpong_unix.zig",
    "examples/rpc_fd_passing.zig",
    "examples/pingpong.zig",
    "examples/pingpong.capnp",
};

const doc_forbidden = [_]Needle{
    .{ .needle = "capnpc.rpc.quic", .reason = "QUIC public API lives under capnpc.rpc.transport.quic" },
    // The error is spelled `EventedBackendUnsupported`. Note this forbids a
    // wrong ERROR NAME, not a claim that the backend works: at 0.17.0 no std
    // evented backend compiles, so the selector reports unsupported, which
    // docs/stability.md and README.md say explicitly.
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
    "example-rpc-unix",
    "example-rpc-fd",
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
    "example-unix",
    "example-fd",
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
    .{ .path = "docs/rpc-unix-sockets.md", .needle = "## Threat table", .reason = "the Unix-socket guide must keep its threat table (the fd-passing security review)" },
    .{ .path = "docs/rpc-unix-sockets.md", .needle = "<!-- verbatim: tests/docs/rpc_unix_snippets_test.zig -->", .reason = "the Unix-socket guide's code must come from the snippet test that runs it" },
    // The pinned-plugin recipe. A plugin found on PATH can come from any
    // revision, and with reflection on by default a mismatch is a compile
    // break, so both consumer entry points say so and show executed code.
    .{ .path = "docs/build-integration.md", .needle = "never a PATH binary", .reason = "build guide must steer codegen to the pinned dep.artifact plugin" },
    .{ .path = "docs/build-integration.md", .needle = verbatim_file_marker ++ codegen_consumer_build ++ verbatim_marker_end, .reason = "build guide's canonical build.zig must be the whole package-preflight codegen consumer build.zig" },
    .{ .path = "docs/getting-started-serialization.md", .needle = "never a PATH binary", .reason = "serialization guide must steer codegen to the pinned dep.artifact plugin" },
    .{ .path = "docs/getting-started-serialization.md", .needle = verbatim_marker ++ codegen_consumer_build ++ verbatim_marker_end, .reason = "serialization guide's codegen snippet must come from the package-preflight codegen consumer" },
};

/// A fenced code block in an active doc that must appear verbatim in a
/// checked-in file some gate executes. The marker is an HTML comment on the
/// line before the opening fence:
///
///     <!-- verbatim: tests/package_consumer/codegen/build.zig -->
///
/// The block may be an excerpt and may be dedented: it must match a run of
/// consecutive lines of the file, every line carrying the same extra leading
/// spaces there. A doc snippet tied this way cannot drift from code that runs.
const verbatim_marker = "<!-- verbatim: ";
const verbatim_marker_end = " -->";
/// Like `verbatim_marker`, but the block must be the WHOLE file: every line,
/// in order, nothing dedented, nothing left out. Use it where the doc says
/// the block is the file, as build-integration.md does for the canonical
/// `build.zig`; an excerpt would let the file grow steps the doc never shows.
/// Line endings are compared after CRLF normalization, so a Windows checkout
/// with `core.autocrlf` still matches.
const verbatim_file_marker = "<!-- verbatim-file: ";
/// package-preflight builds and runs this consumer from the filtered archive.
const codegen_consumer_build = "tests/package_consumer/codegen/build.zig";

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
    "capnp-zig/archive/refs/tags/v",
    "capnpc_zig-",
};

const version_pinned_docs = [_][]const u8{
    "README.md",
    "docs/build-integration.md",
    "docs/getting-started-serialization.md",
};

/// Marks a doc caveat about a feature that is on `main` but in no release yet:
///
///     ... a release after v0.18.0. <!-- unreleased-after: v0.18.0 -->
///
/// The marker names the latest release, the one that lacks the feature, and
/// must equal the `build.zig.zon` version. The release ceremony bumps the
/// manifest first, so at the cut every such caveat fails here and is rewritten
/// or removed instead of telling readers a shipped feature is unreleased.
const unreleased_marker = "<!-- unreleased-after: v";
const unreleased_marker_end = " -->";

const UnreleasedMarkerProblem = enum { malformed, stale };

/// Checks every unreleased-after marker on `line` against `version`.
fn unreleasedMarkerProblem(line: []const u8, version: []const u8) ?UnreleasedMarkerProblem {
    var search: usize = 0;
    while (std.mem.indexOfPos(u8, line, search, unreleased_marker)) |idx| {
        const after = line[idx + unreleased_marker.len ..];
        const end = std.mem.indexOf(u8, after, unreleased_marker_end) orelse return .malformed;
        const named = after[0..end];
        if (named.len == 0 or std.mem.indexOfAny(u8, named, " \t<>") != null) return .malformed;
        if (!std.mem.eql(u8, named, version)) return .stale;
        search = idx + unreleased_marker.len + end + unreleased_marker_end.len;
    }
    return null;
}

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
        \\  - fails unreleased-feature caveats once build.zig.zon moves past them
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

fn splitLines(allocator: std.mem.Allocator, bytes: []const u8) ![][]const u8 {
    var lines: std.ArrayList([]const u8) = .empty;
    errdefer lines.deinit(allocator);
    var it = std.mem.splitScalar(u8, bytes, '\n');
    while (it.next()) |line| try lines.append(allocator, std.mem.trimEnd(u8, line, "\r"));
    // A trailing newline is a line terminator, not an empty last line.
    if (lines.items.len > 0 and lines.items[lines.items.len - 1].len == 0) _ = lines.pop();
    return lines.toOwnedSlice(allocator);
}

fn isBlank(line: []const u8) bool {
    return std.mem.trim(u8, line, " \t").len == 0;
}

/// True when `block` matches consecutive lines of `file`, every non-blank
/// line carrying the same number of extra leading spaces in `file`.
fn blockAppearsIn(block: []const []const u8, file: []const []const u8) bool {
    const anchor = for (block, 0..) |line, i| {
        if (!isBlank(line)) break i;
    } else return false;
    if (block.len > file.len) return false;

    var start: usize = 0;
    while (start + block.len <= file.len) : (start += 1) {
        const file_anchor = file[start + anchor];
        if (!std.mem.endsWith(u8, file_anchor, block[anchor])) continue;
        const indent = file_anchor.len - block[anchor].len;
        const matches = for (block, file[start..][0..block.len]) |want, have| {
            if (!lineMatches(want, have, indent)) break false;
        } else true;
        if (matches) return true;
    }
    return false;
}

fn lineMatches(want: []const u8, have: []const u8, indent: usize) bool {
    if (isBlank(want)) return isBlank(have);
    if (have.len != indent + want.len) return false;
    for (have[0..indent]) |c| {
        if (c != ' ') return false;
    }
    return std.mem.eql(u8, have[indent..], want);
}

fn verifyVerbatimBlocksIn(ctx: *Context, doc_path: []const u8) !void {
    const doc = try readFile(ctx, doc_path);
    defer ctx.allocator.free(doc);
    const lines = try splitLines(ctx.allocator, doc);
    defer ctx.allocator.free(lines);

    var i: usize = 0;
    while (i < lines.len) : (i += 1) {
        const marker_line = std.mem.trim(u8, lines[i], " \t");
        const whole_file = std.mem.startsWith(u8, marker_line, verbatim_file_marker);
        if (!whole_file and !std.mem.startsWith(u8, marker_line, verbatim_marker)) continue;
        const marker = if (whole_file) verbatim_file_marker else verbatim_marker;
        ctx.checks += 1;
        const marker_no = i + 1;
        if (!std.mem.endsWith(u8, marker_line, verbatim_marker_end) or
            marker_line.len < marker.len + verbatim_marker_end.len)
        {
            ctx.fail("{s}:{d}: unterminated verbatim marker", .{ doc_path, marker_no });
            continue;
        }
        const source_path = marker_line[marker.len .. marker_line.len - verbatim_marker_end.len];
        if (i + 1 >= lines.len or !std.mem.startsWith(u8, lines[i + 1], "```")) {
            ctx.fail("{s}:{d}: verbatim marker must sit directly above a fenced block", .{ doc_path, marker_no });
            continue;
        }
        const block_start = i + 2;
        var block_end = block_start;
        while (block_end < lines.len and !std.mem.startsWith(u8, lines[block_end], "```")) block_end += 1;
        if (block_end == lines.len) {
            ctx.fail("{s}:{d}: verbatim block is never closed", .{ doc_path, marker_no });
            return;
        }
        i = block_end;

        const source = readFile(ctx, source_path) catch |err| {
            ctx.fail("{s}:{d}: cannot read verbatim source {s}: {s}", .{ doc_path, marker_no, source_path, @errorName(err) });
            continue;
        };
        defer ctx.allocator.free(source);
        const source_lines = try splitLines(ctx.allocator, source);
        defer ctx.allocator.free(source_lines);
        if (whole_file) {
            if (!blockEqualsFile(lines[block_start..block_end], source_lines)) {
                ctx.fail("{s}:{d}: fenced block is not the whole of {s}; copy the entire file", .{ doc_path, marker_no, source_path });
            }
        } else if (!blockAppearsIn(lines[block_start..block_end], source_lines)) {
            ctx.fail("{s}:{d}: fenced block is not a verbatim excerpt of {s}; copy it from that file", .{ doc_path, marker_no, source_path });
        }
    }
}

/// True when `block` is every line of `file`, in order and unindented. Both
/// come from `splitLines`, so CR before LF is already gone on both sides.
fn blockEqualsFile(block: []const []const u8, file: []const []const u8) bool {
    if (block.len != file.len) return false;
    for (block, file) |want, have| {
        if (!std.mem.eql(u8, want, have)) return false;
    }
    return true;
}

fn verifyVerbatimBlocks(ctx: *Context) !void {
    for (active_docs) |path| try verifyVerbatimBlocksIn(ctx, path);
}

test "blockAppearsIn matches whole files and dedented excerpts only" {
    const file = [_][]const u8{
        "pub fn build(b: *std.Build) void {",
        "    const run = b.addRunArtifact(plugin);",
        "",
        "    run.setStdIn(.{ .lazy_path = request });",
        "}",
    };
    try std.testing.expect(blockAppearsIn(&file, &file));
    try std.testing.expect(blockAppearsIn(&.{
        "const run = b.addRunArtifact(plugin);",
        "",
        "run.setStdIn(.{ .lazy_path = request });",
    }, &file));
    // An edited line, a skipped line, or inconsistent indentation all fail.
    try std.testing.expect(!blockAppearsIn(&.{"const run = b.addSystemCommand(plugin);"}, &file));
    try std.testing.expect(!blockAppearsIn(&.{
        "const run = b.addRunArtifact(plugin);",
        "run.setStdIn(.{ .lazy_path = request });",
    }, &file));
    try std.testing.expect(!blockAppearsIn(&.{
        "    const run = b.addRunArtifact(plugin);",
        "",
        "run.setStdIn(.{ .lazy_path = request });",
    }, &file));
    try std.testing.expect(!blockAppearsIn(&.{ "", "" }, &file));
}

test "blockEqualsFile accepts only the whole file, CRLF or LF" {
    const allocator = std.testing.allocator;
    const lf = "pub fn build(b: *std.Build) void {\n    _ = b;\n\n}\n";
    const crlf = "pub fn build(b: *std.Build) void {\r\n    _ = b;\r\n\r\n}\r\n";
    const file = try splitLines(allocator, lf);
    defer allocator.free(file);
    const file_crlf = try splitLines(allocator, crlf);
    defer allocator.free(file_crlf);

    const block = [_][]const u8{ "pub fn build(b: *std.Build) void {", "    _ = b;", "", "}" };
    try std.testing.expect(blockEqualsFile(&block, file));
    try std.testing.expect(blockEqualsFile(&block, file_crlf));
    // An excerpt passes the excerpt check but not this one.
    try std.testing.expect(blockAppearsIn(block[0..2], file));
    try std.testing.expect(!blockEqualsFile(block[0..2], file));
    // An edited line, a dedented file, or an extra trailing line all fail.
    try std.testing.expect(!blockEqualsFile(&.{ "pub fn build(b: *std.Build) void {", "    _ = a;", "", "}" }, file));
    try std.testing.expect(!blockEqualsFile(&.{ "pub fn build(b: *std.Build) void {", "_ = b;", "", "}" }, file));
    try std.testing.expect(!blockEqualsFile(&.{ "pub fn build(b: *std.Build) void {", "    _ = b;", "", "}", "" }, file));
}

test "unreleasedMarkerProblem flags caveats the manifest has moved past" {
    // The current release: the caveat still describes an unreleased feature.
    try std.testing.expectEqual(@as(?UnreleasedMarkerProblem, null), unreleasedMarkerProblem(
        "- **`--flag`** is new. <!-- unreleased-after: v0.18.0 -->",
        "0.18.0",
    ));
    try std.testing.expectEqual(@as(?UnreleasedMarkerProblem, null), unreleasedMarkerProblem("No marker here.", "0.18.0"));
    // The cut bumped build.zig.zon: the feature has shipped.
    try std.testing.expectEqual(@as(?UnreleasedMarkerProblem, .stale), unreleasedMarkerProblem(
        "> Needs a release after v0.18.0. <!-- unreleased-after: v0.18.0 -->",
        "0.19.0",
    ));
    // A prefix of the manifest version is not a match.
    try std.testing.expectEqual(@as(?UnreleasedMarkerProblem, .stale), unreleasedMarkerProblem(
        "<!-- unreleased-after: v0.1 -->",
        "0.18.0",
    ));
    // The second marker on a line is checked too.
    try std.testing.expectEqual(@as(?UnreleasedMarkerProblem, .stale), unreleasedMarkerProblem(
        "<!-- unreleased-after: v0.18.0 --> and <!-- unreleased-after: v0.17.0 -->",
        "0.18.0",
    ));
    try std.testing.expectEqual(@as(?UnreleasedMarkerProblem, .malformed), unreleasedMarkerProblem(
        "<!-- unreleased-after: v0.18.0",
        "0.18.0",
    ));
    try std.testing.expectEqual(@as(?UnreleasedMarkerProblem, .malformed), unreleasedMarkerProblem(
        "<!-- unreleased-after: v -->",
        "0.18.0",
    ));
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

fn scanUnreleasedMarkers(ctx: *Context, path: []const u8, version: []const u8) !void {
    const bytes = try readFile(ctx, path);
    defer ctx.allocator.free(bytes);
    ctx.checks += 1;

    var line_no: usize = 1;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw_line_with_cr| : (line_no += 1) {
        const line = std.mem.trimEnd(u8, raw_line_with_cr, "\r");
        const problem = unreleasedMarkerProblem(line, version) orelse continue;
        switch (problem) {
            .malformed => ctx.fail(
                "{s}:{d}: malformed marker; write `{s}X.Y.Z{s}`",
                .{ path, line_no, unreleased_marker, unreleased_marker_end },
            ),
            .stale => ctx.fail(
                "{s}:{d}: unreleased-after marker does not name build.zig.zon version {s}; " ++
                    "if the feature has shipped, rewrite or remove the caveat and its marker",
                .{ path, line_no, version },
            ),
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

    for (active_docs) |path| {
        try scanUnreleasedMarkers(ctx, path, version);
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
    try verifyVerbatimBlocks(&ctx);
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
