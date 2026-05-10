const std = @import("std");

const max_file_bytes = 8 * 1024 * 1024;

const PatternKind = enum {
    catch_unreachable,
    panic_call,
    optional_unwrap,
    unchecked_unreachable,
    runtime_safety_disabled,
    unsafe_optimize,
    disclosure_source_path,
    disclosure_stack_trace,
    disclosure_build_identity,
    disclosure_banner,
    disclosure_verbose_close_reason,
    disclosure_debug_details,
};

const Allow = struct {
    path: []const u8,
    kind: PatternKind,
    needle: []const u8,
    reason: []const u8,
};

/// Existing reviewed exceptions. Matching is one-for-one: if the same risky
/// line is copied elsewhere, the copy fails until it gets its own review note.
const allowlist = [_]Allow{
    .{ .path = "src/serialization/message/struct_builder.zig", .kind = .optional_unwrap, .needle = "const landing_pos = landing_pad_pos.?;", .reason = "guarded by earlier landing-pad presence check" },
    .{ .path = "src/serialization/message/struct_builder.zig", .kind = .optional_unwrap, .needle = "const landing_pos = landing_pad_pos.?;", .reason = "guarded by earlier landing-pad presence check" },
    .{ .path = "src/serialization/message.zig", .kind = .catch_unreachable, .needle = "framedHeaderBytes(segment_count_limit) catch unreachable", .reason = "compile-time default limit arithmetic is bounded by construction" },
    .{ .path = "src/serialization/message.zig", .kind = .catch_unreachable, .needle = "std.math.mul(usize, total_words_limit, 8) catch unreachable", .reason = "compile-time default limit arithmetic is bounded by construction" },
    .{ .path = "src/serialization/message.zig", .kind = .catch_unreachable, .needle = "std.math.add(usize, header_bytes, payload_bytes) catch unreachable", .reason = "compile-time default limit arithmetic is bounded by construction" },
    .{ .path = "src/serialization/message.zig", .kind = .optional_unwrap, .needle = "const tag_pos = content_override.?;", .reason = "guarded by content override presence check" },
    .{ .path = "src/serialization/message.zig", .kind = .optional_unwrap, .needle = "const landing_pos = landing_pad_pos.?;", .reason = "guarded by earlier landing-pad presence check" },
    .{ .path = "src/serialization/message.zig", .kind = .optional_unwrap, .needle = "const landing_pos = landing_pad_pos.?;", .reason = "guarded by earlier landing-pad presence check" },
    .{ .path = "src/serialization/schema_validation.zig", .kind = .optional_unwrap, .needle = "field.discriminant_value != discriminant_value.?", .reason = "preceded by discriminant_value != null guard" },
    .{ .path = "src/serialization/schema_validation.zig", .kind = .optional_unwrap, .needle = "field.discriminant_value != discriminant_value.?", .reason = "preceded by discriminant_value != null guard" },
    .{ .path = "src/serialization/schema_validation.zig", .kind = .optional_unwrap, .needle = "field.discriminant_value != discriminant_value.?", .reason = "preceded by discriminant_value != null guard" },

    .{ .path = "src/rpc/peer/state.zig", .kind = .panic_call, .needle = "Peer method called from wrong thread", .reason = "debug misuse guard, not input-driven protocol handling" },
    .{ .path = "src/rpc/peer/mod.zig", .kind = .panic_call, .needle = "attachConnection called while a transport is already attached", .reason = "programmer misuse guard" },
    .{ .path = "src/rpc/peer/mod.zig", .kind = .panic_call, .needle = "attachTransportBinding called while a transport is already attached", .reason = "programmer misuse guard" },
    .{ .path = "src/rpc/peer/mod.zig", .kind = .optional_unwrap, .needle = "promise_entry.value_ptr.resolved.?", .reason = "guarded by resolved export state check" },
    .{ .path = "src/rpc/peer/mod.zig", .kind = .optional_unwrap, .needle = "entry.cap.?", .reason = "resolved import cap is checked before dispatch" },
    .{ .path = "src/rpc/peer/mod.zig", .kind = .optional_unwrap, .needle = "ret.results.?", .reason = "guarded by Return tag/results presence check" },
    .{ .path = "src/rpc/peer/peer_cap_lifecycle.zig", .kind = .optional_unwrap, .needle = "ret.results.?.cap_table", .reason = "guarded by Return results tag path" },
    .{ .path = "src/rpc/peer/peer_transport_callbacks.zig", .kind = .optional_unwrap, .needle = "conn.context().?", .reason = "transport callback only installed with context" },
    .{ .path = "src/rpc/peer/return/peer_return_orchestration.zig", .kind = .optional_unwrap, .needle = "ret.results.?.cap_table", .reason = "guarded by Return results tag/path check" },
    .{ .path = "src/rpc/peer/call/peer_call_orchestration.zig", .kind = .unchecked_unreachable, .needle = ".queue_promise_export => unreachable", .reason = "filtered by handleCallImportedTargetForPeer before dispatch" },

    .{ .path = "src/rpc/transport/tcp/connection.zig", .kind = .panic_call, .needle = "Connection method called from wrong thread", .reason = "debug misuse guard, not input-driven protocol handling" },
    .{ .path = "src/rpc/transport/tcp/connection.zig", .kind = .optional_unwrap, .needle = "const bytes = frame.?;", .reason = "guarded by preceding null frame branch" },
    .{ .path = "src/rpc/transport/tcp/connection.zig", .kind = .optional_unwrap, .needle = "self.on_message.?", .reason = "checked before callback invocation" },
    .{ .path = "src/rpc/transport/stream_state.zig", .kind = .optional_unwrap, .needle = "cb(ctx.?, self.stream_error)", .reason = "callback context is paired with callback registration" },
    .{ .path = "src/rpc/transport/quic/connection.zig", .kind = .panic_call, .needle = "QUIC Connection method called from wrong thread", .reason = "debug misuse guard, not input-driven protocol handling" },

    .{ .path = "src/rpc/wire/protocol.zig", .kind = .catch_unreachable, .needle = "generated.setSenderHosted(id) catch unreachable", .reason = "generated builder setter is infallible for scalar capability descriptor" },
    .{ .path = "src/rpc/wire/protocol.zig", .kind = .catch_unreachable, .needle = "generated.setSenderPromise(id) catch unreachable", .reason = "generated builder setter is infallible for scalar capability descriptor" },
    .{ .path = "src/rpc/wire/protocol.zig", .kind = .catch_unreachable, .needle = "generated.setReceiverHosted(id) catch unreachable", .reason = "generated builder setter is infallible for scalar capability descriptor" },
    .{ .path = "src/rpc/wire/protocol.zig", .kind = .optional_unwrap, .needle = "message_tag.?", .reason = "validated tag presence before Unimplemented construction" },
    .{ .path = "src/rpc/wire/protocol.zig", .kind = .catch_unreachable, .needle = "send_results_to.setCaller({}) catch unreachable", .reason = "generated union setter is infallible for empty variant" },
    .{ .path = "src/rpc/wire/protocol.zig", .kind = .catch_unreachable, .needle = "send_results_to.setYourself({}) catch unreachable", .reason = "generated union setter is infallible for empty variant" },
    .{ .path = "src/rpc/wire/protocol.zig", .kind = .catch_unreachable, .needle = "ret_builder.setReleaseParamCaps(release_param_caps) catch unreachable", .reason = "generated scalar setter is infallible" },
    .{ .path = "src/rpc/wire/protocol.zig", .kind = .catch_unreachable, .needle = "ret_builder.setNoFinishNeeded(no_finish_needed) catch unreachable", .reason = "generated scalar setter is infallible" },
    .{ .path = "src/rpc/wire/protocol.zig", .kind = .catch_unreachable, .needle = "ret_builder.setCanceled({}) catch unreachable", .reason = "generated union setter is infallible for empty variant" },

    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .optional_unwrap, .needle = "state.bootstrap_stub_export_id.?", .reason = "bootstrap export id is initialized before write" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_source_path, .needle = "\"/Users/\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_source_path, .needle = "\"/home/\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_source_path, .needle = "\"/private/\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_source_path, .needle = "\"/tmp/\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_source_path, .needle = "\"\\\\Users\\\\\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_source_path, .needle = "\"src/\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_source_path, .needle = "\"tests/\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_source_path, .needle = "\".zig:\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_stack_trace, .needle = "\"stack trace\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_stack_trace, .needle = "\"Stack trace\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/wasm/capnp_host_abi.zig", .kind = .disclosure_stack_trace, .needle = "\"panicked at\"", .reason = "defensive disclosure-marker filter data" },
    .{ .path = "src/io_backend.zig", .kind = .unchecked_unreachable, .needle = "if (comptime std.Io.Evented == void) unreachable;", .reason = "Backend.init rejects unsupported Evented targets; this is reachable only through manual union construction" },
    .{ .path = ".github/workflows/ci.yml", .kind = .unsafe_optimize, .needle = "zig build -Doptimize=ReleaseFast bench-check", .reason = "benchmark-only job intentionally runs optimized code for stable timing" },
    .{ .path = "Justfile", .kind = .unsafe_optimize, .needle = "zig build -Doptimize=ReleaseFast bench-check", .reason = "benchmark-only local recipe intentionally runs optimized code for stable timing" },
};

const unsafe_dirs = [_][]const u8{
    "src/serialization",
    "src/rpc",
    "src/wasm",
};

const unsafe_files = [_][]const u8{
    "src/io_backend.zig",
};

const disclosure_extra_dirs = [_][]const u8{
    "src/capnpc-zig",
};

const disclosure_extra_files = [_][]const u8{
    "src/main.zig",
    "src/lib.zig",
    "src/lib_core.zig",
    "examples/rpc_pingpong.zig",
    "examples/kvstore/server.zig",
    "examples/kvstore/stressor.zig",
};

const build_policy_files = [_][]const u8{
    "build.zig",
    "Justfile",
    ".github/workflows/ci.yml",
};

const Context = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    allow_used: []bool,
    failures: usize = 0,
    checked_files: usize = 0,
    findings: usize = 0,

    fn record(self: *Context, path: []const u8, line_no: usize, kind: PatternKind, line: []const u8) void {
        self.findings += 1;
        if (self.consumeAllow(path, kind, line)) return;

        self.failures += 1;
        const trimmed = std.mem.trim(u8, line, " \t\r");
        std.debug.print(
            "[FAIL] {s}:{d}: {s}: {s}\n",
            .{ path, line_no, kindName(kind), trimmed },
        );
    }

    fn consumeAllow(self: *Context, path: []const u8, kind: PatternKind, line: []const u8) bool {
        for (allowlist, 0..) |allow, idx| {
            if (self.allow_used[idx]) continue;
            if (allow.kind != kind) continue;
            if (!std.mem.eql(u8, allow.path, path)) continue;
            if (std.mem.indexOf(u8, line, allow.needle) == null) continue;
            self.allow_used[idx] = true;
            return true;
        }
        return false;
    }
};

fn printUsage() void {
    std.debug.print(
        \\Usage: zig build hardening
        \\
        \\Static hardening gate for production source paths:
        \\  - bans unreviewed catch unreachable, @panic, .? unwraps,
        \\    unchecked unreachable, and @setRuntimeSafety(false)
        \\  - bans unreviewed ReleaseFast/ReleaseSmall build-policy drift
        \\  - bans public disclosure drift for source paths, stack traces,
        \\    build identity strings, banners, and verbose close reasons
        \\  - skips generated code and Zig test blocks to reduce test-only noise
        \\  - keeps reviewed exceptions in tools/hardening_gate.zig
        \\
    , .{});
}

fn parseArgs(args: std.process.Args) !bool {
    var iter = std.process.Args.Iterator.init(args);
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

fn kindName(kind: PatternKind) []const u8 {
    return switch (kind) {
        .catch_unreachable => "catch unreachable",
        .panic_call => "@panic",
        .optional_unwrap => "optional unwrap",
        .unchecked_unreachable => "unchecked unreachable",
        .runtime_safety_disabled => "@setRuntimeSafety(false)",
        .unsafe_optimize => "unsafe optimize policy",
        .disclosure_source_path => "disclosure source path",
        .disclosure_stack_trace => "disclosure stack trace",
        .disclosure_build_identity => "disclosure build identity",
        .disclosure_banner => "disclosure banner",
        .disclosure_verbose_close_reason => "disclosure verbose close reason",
        .disclosure_debug_details => "disclosure debug details",
    };
}

fn isIdentChar(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

fn hasWord(haystack: []const u8, word: []const u8) bool {
    var index: usize = 0;
    while (std.mem.indexOfPos(u8, haystack, index, word)) |pos| {
        const before_ok = pos == 0 or !isIdentChar(haystack[pos - 1]);
        const after_pos = pos + word.len;
        const after_ok = after_pos >= haystack.len or !isIdentChar(haystack[after_pos]);
        if (before_ok and after_ok) return true;
        index = after_pos;
    }
    return false;
}

fn sanitizeZigLine(allocator: std.mem.Allocator, line: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, line.len);
    @memset(out, ' ');

    const trimmed_left = std.mem.trimStart(u8, line, " \t");
    if (std.mem.startsWith(u8, trimmed_left, "\\\\")) return out;

    var in_string = false;
    var escaped = false;
    var index: usize = 0;
    while (index < line.len) : (index += 1) {
        const c = line[index];
        if (!in_string and index + 1 < line.len and c == '/' and line[index + 1] == '/') {
            break;
        }

        if (in_string) {
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                in_string = false;
            }
            continue;
        }

        if (c == '"') {
            in_string = true;
            continue;
        }

        out[index] = c;
    }

    return out;
}

fn sanitizePolicyLine(allocator: std.mem.Allocator, path: []const u8, line: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, path, ".zig")) return sanitizeZigLine(allocator, line);

    const out = try allocator.alloc(u8, line.len);
    @memset(out, ' ');
    var index: usize = 0;
    while (index < line.len) : (index += 1) {
        if (line[index] == '#') break;
        out[index] = line[index];
    }
    return out;
}

fn braceDelta(code: []const u8) isize {
    var delta: isize = 0;
    for (code) |c| {
        if (c == '{') delta += 1;
        if (c == '}') delta -= 1;
    }
    return delta;
}

fn adjustDepth(depth: usize, delta: isize) usize {
    if (delta >= 0) return depth + @as(usize, @intCast(delta));
    const amount: usize = @intCast(-delta);
    if (amount >= depth) return 0;
    return depth - amount;
}

fn isZigTestStart(code: []const u8) bool {
    const trimmed = std.mem.trimStart(u8, code, " \t");
    return std.mem.startsWith(u8, trimmed, "test ") or std.mem.startsWith(u8, trimmed, "test{");
}

fn scanUnsafeCode(ctx: *Context, path: []const u8, line_no: usize, raw_line: []const u8, code: []const u8) void {
    const catch_unreachable = std.mem.indexOf(u8, code, "catch unreachable") != null;
    if (catch_unreachable) ctx.record(path, line_no, .catch_unreachable, raw_line);
    if (std.mem.indexOf(u8, code, "@panic(") != null) ctx.record(path, line_no, .panic_call, raw_line);
    if (std.mem.indexOf(u8, code, ".?") != null) ctx.record(path, line_no, .optional_unwrap, raw_line);
    if (std.mem.indexOf(u8, code, "@setRuntimeSafety(false)") != null) ctx.record(path, line_no, .runtime_safety_disabled, raw_line);
    if (!catch_unreachable and hasWord(code, "unreachable")) ctx.record(path, line_no, .unchecked_unreachable, raw_line);
}

const DisclosureMarker = struct {
    kind: PatternKind,
    needle: []const u8,
};

const disclosure_string_markers = [_]DisclosureMarker{
    .{ .kind = .disclosure_source_path, .needle = "/Users/" },
    .{ .kind = .disclosure_source_path, .needle = "/home/" },
    .{ .kind = .disclosure_source_path, .needle = "/private/" },
    .{ .kind = .disclosure_source_path, .needle = "/var/folders/" },
    .{ .kind = .disclosure_source_path, .needle = "/tmp/" },
    .{ .kind = .disclosure_source_path, .needle = "/workspace/" },
    .{ .kind = .disclosure_source_path, .needle = "github/workspace" },
    .{ .kind = .disclosure_source_path, .needle = "\\\\Users\\\\" },
    .{ .kind = .disclosure_source_path, .needle = "C:\\\\" },
    .{ .kind = .disclosure_source_path, .needle = "C:/" },
    .{ .kind = .disclosure_source_path, .needle = ".zig:" },
    .{ .kind = .disclosure_source_path, .needle = "src/" },
    .{ .kind = .disclosure_source_path, .needle = "tests/" },
    .{ .kind = .disclosure_source_path, .needle = "capnp-zig/src/" },
    .{ .kind = .disclosure_stack_trace, .needle = "stack trace" },
    .{ .kind = .disclosure_stack_trace, .needle = "Stack trace" },
    .{ .kind = .disclosure_stack_trace, .needle = "error return trace" },
    .{ .kind = .disclosure_stack_trace, .needle = "panicked at" },
    .{ .kind = .disclosure_stack_trace, .needle = "panic:" },
    .{ .kind = .disclosure_build_identity, .needle = "build id" },
    .{ .kind = .disclosure_build_identity, .needle = "build-id" },
    .{ .kind = .disclosure_build_identity, .needle = "build_id" },
    .{ .kind = .disclosure_build_identity, .needle = "git sha" },
    .{ .kind = .disclosure_build_identity, .needle = "git_sha" },
    .{ .kind = .disclosure_build_identity, .needle = "gitSha" },
    .{ .kind = .disclosure_build_identity, .needle = "commit sha" },
    .{ .kind = .disclosure_build_identity, .needle = "commit_sha" },
    .{ .kind = .disclosure_build_identity, .needle = "commitHash" },
    .{ .kind = .disclosure_build_identity, .needle = "built at" },
    .{ .kind = .disclosure_build_identity, .needle = "build date" },
    .{ .kind = .disclosure_build_identity, .needle = "build time" },
    .{ .kind = .disclosure_build_identity, .needle = "dirty build" },
    .{ .kind = .disclosure_banner, .needle = "X-Powered-By" },
    .{ .kind = .disclosure_banner, .needle = "Server:" },
};

fn scanDisclosureLiteral(ctx: *Context, path: []const u8, line_no: usize, raw_line: []const u8, literal: []const u8) void {
    for (disclosure_string_markers) |marker| {
        if (std.mem.indexOf(u8, literal, marker.needle) != null) {
            ctx.record(path, line_no, marker.kind, raw_line);
        }
    }
}

fn scanZigStringLiterals(ctx: *Context, path: []const u8, line_no: usize, raw_line: []const u8) void {
    const trimmed_left = std.mem.trimStart(u8, raw_line, " \t");
    if (std.mem.startsWith(u8, trimmed_left, "\\\\")) {
        scanDisclosureLiteral(ctx, path, line_no, raw_line, trimmed_left[2..]);
        return;
    }

    var in_string = false;
    var escaped = false;
    var start: usize = 0;
    var index: usize = 0;
    while (index < raw_line.len) : (index += 1) {
        const c = raw_line[index];
        if (!in_string and index + 1 < raw_line.len and c == '/' and raw_line[index + 1] == '/') {
            break;
        }

        if (!in_string) {
            if (c == '"') {
                in_string = true;
                escaped = false;
                start = index + 1;
            }
            continue;
        }

        if (escaped) {
            escaped = false;
            continue;
        }
        if (c == '\\') {
            escaped = true;
            continue;
        }
        if (c == '"') {
            scanDisclosureLiteral(ctx, path, line_no, raw_line, raw_line[start..index]);
            in_string = false;
        }
    }
}

fn hasTrueAssignment(code: []const u8, name: []const u8) bool {
    var index: usize = 0;
    while (std.mem.indexOfPos(u8, code, index, name)) |pos| {
        var cursor = pos + name.len;
        if (cursor < code.len and isIdentChar(code[cursor])) {
            index = cursor;
            continue;
        }
        while (cursor < code.len and (code[cursor] == ' ' or code[cursor] == '\t')) : (cursor += 1) {}
        if (cursor >= code.len or code[cursor] != '=') {
            index = cursor;
            continue;
        }
        cursor += 1;
        while (cursor < code.len and (code[cursor] == ' ' or code[cursor] == '\t')) : (cursor += 1) {}
        if (std.mem.startsWith(u8, code[cursor..], "true")) {
            const end = cursor + "true".len;
            if (end >= code.len or !isIdentChar(code[end])) return true;
        }
        index = cursor;
    }
    return false;
}

fn scanDisclosureCode(ctx: *Context, path: []const u8, line_no: usize, raw_line: []const u8, code: []const u8) void {
    scanZigStringLiterals(ctx, path, line_no, raw_line);

    if (std.mem.indexOf(u8, code, "@src(") != null) ctx.record(path, line_no, .disclosure_source_path, raw_line);
    if (std.mem.indexOf(u8, code, "@errorReturnTrace") != null or
        std.mem.indexOf(u8, code, "std.debug.dumpStackTrace") != null or
        std.mem.indexOf(u8, code, "std.debug.dumpCurrentStackTrace") != null or
        std.mem.indexOf(u8, code, "std.debug.captureStackTrace") != null or
        std.mem.indexOf(u8, code, "std.builtin.StackTrace") != null)
    {
        ctx.record(path, line_no, .disclosure_stack_trace, raw_line);
    }
    if (hasTrueAssignment(code, "reveal_close_reason_on_wire")) {
        ctx.record(path, line_no, .disclosure_verbose_close_reason, raw_line);
    }
    if (hasTrueAssignment(code, "reveal_details")) {
        ctx.record(path, line_no, .disclosure_debug_details, raw_line);
    }
}

fn scanBuildPolicy(ctx: *Context, path: []const u8, line_no: usize, raw_line: []const u8, code: []const u8) void {
    if (std.mem.indexOf(u8, code, "ReleaseFast") != null or
        std.mem.indexOf(u8, code, "ReleaseSmall") != null)
    {
        ctx.record(path, line_no, .unsafe_optimize, raw_line);
    }
}

fn scanDisclosurePolicy(ctx: *Context, path: []const u8, line_no: usize, raw_line: []const u8, code: []const u8) void {
    if (std.mem.indexOf(u8, code, "--verbose-close-reason") != null or
        std.mem.indexOf(u8, code, "--reveal-close-reason") != null or
        std.mem.indexOf(u8, code, "-Ddebug-disclosure") != null)
    {
        ctx.record(path, line_no, .disclosure_verbose_close_reason, raw_line);
    }
    if (std.mem.indexOf(u8, code, "reveal_close_reason_on_wire=true") != null or
        std.mem.indexOf(u8, code, "reveal_close_reason_on_wire = true") != null)
    {
        ctx.record(path, line_no, .disclosure_verbose_close_reason, raw_line);
    }
    if (std.mem.indexOf(u8, code, "X-Powered-By") != null or
        std.mem.indexOf(u8, code, "Server:") != null)
    {
        ctx.record(path, line_no, .disclosure_banner, raw_line);
    }
}

const ZigScanMode = struct {
    unsafe: bool = false,
    disclosure: bool = false,
};

fn scanZigFile(ctx: *Context, path: []const u8, mode: ZigScanMode) !void {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(ctx.io, path, ctx.allocator, .limited(max_file_bytes));
    defer ctx.allocator.free(bytes);

    ctx.checked_files += 1;
    var test_depth: usize = 0;
    var line_no: usize = 1;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw_line_with_cr| : (line_no += 1) {
        const raw_line = std.mem.trimEnd(u8, raw_line_with_cr, "\r");
        const code = try sanitizeZigLine(ctx.allocator, raw_line);
        defer ctx.allocator.free(code);

        if (test_depth != 0) {
            test_depth = adjustDepth(test_depth, braceDelta(code));
            continue;
        }

        if (isZigTestStart(code)) {
            test_depth = adjustDepth(0, braceDelta(code));
            continue;
        }

        if (mode.unsafe) scanUnsafeCode(ctx, path, line_no, raw_line, code);
        if (mode.disclosure) scanDisclosureCode(ctx, path, line_no, raw_line, code);
    }
}

fn scanPolicyFile(ctx: *Context, path: []const u8) !void {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(ctx.io, path, ctx.allocator, .limited(max_file_bytes));
    defer ctx.allocator.free(bytes);

    ctx.checked_files += 1;
    var line_no: usize = 1;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw_line_with_cr| : (line_no += 1) {
        const raw_line = std.mem.trimEnd(u8, raw_line_with_cr, "\r");
        const code = try sanitizePolicyLine(ctx.allocator, path, raw_line);
        defer ctx.allocator.free(code);
        scanBuildPolicy(ctx, path, line_no, raw_line, code);
        scanDisclosurePolicy(ctx, path, line_no, raw_line, code);
    }
}

fn normalizePathInPlace(path: []u8) void {
    for (path) |*c| {
        if (c.* == std.fs.path.sep) c.* = '/';
    }
}

fn shouldSkipPath(path: []const u8) bool {
    return std.mem.startsWith(u8, path, "src/rpc/gen/") or
        std.mem.startsWith(u8, path, "src/wasm/generated/") or
        std.mem.startsWith(u8, path, "examples/kvstore/gen/") or
        std.mem.startsWith(u8, path, "examples/kvstore/vendor/") or
        std.mem.endsWith(u8, path, "_test.zig");
}

fn scanZigDir(ctx: *Context, dir_path: []const u8, mode: ZigScanMode) !void {
    var dir = try std.Io.Dir.cwd().openDir(ctx.io, dir_path, .{ .iterate = true });
    defer dir.close(ctx.io);

    var walker = try dir.walk(ctx.allocator);
    defer walker.deinit();

    while (try walker.next(ctx.io)) |entry| {
        const path = try std.fmt.allocPrint(ctx.allocator, "{s}/{s}", .{ dir_path, entry.path });
        defer ctx.allocator.free(path);
        normalizePathInPlace(path);

        if (entry.kind == .directory) {
            if (shouldSkipPath(path)) walker.leave(ctx.io);
            continue;
        }

        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, path, ".zig")) continue;
        if (shouldSkipPath(path)) continue;
        try scanZigFile(ctx, path, mode);
    }
}

fn verifyAllowlist(ctx: *Context) void {
    for (allowlist, 0..) |allow, idx| {
        if (ctx.allow_used[idx]) continue;
        ctx.failures += 1;
        std.debug.print(
            "[FAIL] stale hardening allowlist entry: {s}: {s}: {s} ({s})\n",
            .{ allow.path, kindName(allow.kind), allow.needle, allow.reason },
        );
    }
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    const io = init.io;

    const should_run = parseArgs(init.minimal.args) catch |err| {
        std.debug.print("Argument error: {s}\n", .{@errorName(err)});
        printUsage();
        return error.InvalidArgument;
    };
    if (!should_run) return;

    const allow_used = try allocator.alloc(bool, allowlist.len);
    defer allocator.free(allow_used);
    @memset(allow_used, false);

    var ctx = Context{
        .allocator = allocator,
        .io = io,
        .allow_used = allow_used,
    };

    for (unsafe_dirs) |dir_path| try scanZigDir(&ctx, dir_path, .{ .unsafe = true, .disclosure = true });
    for (unsafe_files) |path| {
        if (!shouldSkipPath(path)) try scanZigFile(&ctx, path, .{ .unsafe = true, .disclosure = true });
    }
    for (disclosure_extra_dirs) |dir_path| try scanZigDir(&ctx, dir_path, .{ .disclosure = true });
    for (disclosure_extra_files) |path| {
        if (!shouldSkipPath(path)) try scanZigFile(&ctx, path, .{ .disclosure = true });
    }
    for (build_policy_files) |path| try scanPolicyFile(&ctx, path);

    verifyAllowlist(&ctx);

    if (ctx.failures != 0) {
        std.debug.print(
            "Hardening gate failed: {d} unreviewed/stale item(s), {d} finding(s), {d} file(s) checked\n",
            .{ ctx.failures, ctx.findings, ctx.checked_files },
        );
        return error.HardeningGateFailed;
    }

    std.debug.print(
        "Hardening gate passed: {d} reviewed finding(s), {d} file(s) checked\n",
        .{ ctx.findings, ctx.checked_files },
    );
}
