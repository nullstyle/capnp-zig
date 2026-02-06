const std = @import("std");
const builtin = @import("builtin");

const Allocator = std.mem.Allocator;

const Backend = enum {
    cpp,
    go,
    python,
    rust,
};

const Schema = enum {
    game_world,
    chat,
    inventory,
    matchmaking,
};

const Direction = enum {
    both,
    zig_client,
    zig_server,
};

const CaseResult = struct {
    key: []u8,
    status: []u8,
};

const RunResult = struct {
    exit_code: i32,
    stdout: []u8,
    stderr: []u8,
};

const TapEval = struct {
    pass_count: usize,
    fail_count: usize,
    bailed: bool,
};

const Config = struct {
    build_only: bool = false,
    skip_build: bool = false,
    allow_missing_hooks: bool = false,
    verbose: bool = false,
    direction: Direction = .both,
    backend_selected: [4]bool = .{ false, false, false, false },
    schema_selected: [4]bool = .{ false, false, false, false },

    fn isBackendSelected(self: Config, b: Backend) bool {
        return self.backend_selected[@intFromEnum(b)];
    }

    fn isSchemaSelected(self: Config, s: Schema) bool {
        return self.schema_selected[@intFromEnum(s)];
    }
};

const all_backends = [_]Backend{ .cpp, .go, .python, .rust };
const all_schemas = [_]Schema{ .game_world, .chat, .inventory, .matchmaking };

const Paths = struct {
    repo_root: []const u8,
    compose_file: []const u8,
    results_dir: []const u8,
    zig_justfile: []const u8,
    go_justfile: []const u8,
    cpp_justfile: []const u8,
    python_justfile: []const u8,
    rust_justfile: []const u8,

    fn backendJustfile(self: Paths, backend: Backend) []const u8 {
        return switch (backend) {
            .cpp => self.cpp_justfile,
            .go => self.go_justfile,
            .python => self.python_justfile,
            .rust => self.rust_justfile,
        };
    }
};

const server_timeout_ms: i64 = 60 * 1000;
const case_timeout_ms: i64 = 20 * 1000;

fn usage() void {
    std.debug.print(
        \\Usage: zig run tools/e2e_runner.zig -- [OPTIONS]
        \\
        \\Options:
        \\  --build-only
        \\  --skip-build
        \\  --direction=both|zig-client|zig-server
        \\  --backend=cpp|go|python|rust (repeatable)
        \\  --schema=game_world|chat|inventory|matchmaking (repeatable)
        \\  --allow-missing-hooks
        \\  --verbose
        \\  --help
        \\
        \\Optional hook env vars (legacy override):
        \\  E2E_ZIG_CLIENT_CMD
        \\  E2E_ZIG_SERVER_CMD
        \\By default the Zig client hook runs via:
        \\  just --justfile tests/e2e/zig/Justfile client-hook ...
        \\By default the Zig server runs via:
        \\  zig build --global-cache-dir ./.zig-global-cache e2e-zig-server -- ...
        \\
    , .{});
}

fn backendName(b: Backend) []const u8 {
    return switch (b) {
        .cpp => "cpp",
        .go => "go",
        .python => "python",
        .rust => "rust",
    };
}

fn backendPort(b: Backend) u16 {
    return switch (b) {
        .cpp => 4000,
        .go => 4001,
        .python => 4002,
        .rust => 4003,
    };
}

fn backendDefaultEnabled(b: Backend) bool {
    _ = b;
    return true;
}

fn schemaName(s: Schema) []const u8 {
    return switch (s) {
        .game_world => "game_world",
        .chat => "chat",
        .inventory => "inventory",
        .matchmaking => "matchmaking",
    };
}

fn schemaNameForBackend(b: Backend, s: Schema) []const u8 {
    if (b == .go and s == .game_world) return "gameworld";
    return schemaName(s);
}

fn zigSchemaPort(s: Schema) u16 {
    return switch (s) {
        .game_world => 4700,
        .chat => 4701,
        .inventory => 4702,
        .matchmaking => 4703,
    };
}

fn reserveLocalPort() !u16 {
    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();
    return server.listen_address.getPort();
}

fn fileExists(path: []const u8) bool {
    std.fs.cwd().access(path, .{}) catch return false;
    return true;
}

fn detectPaths() !Paths {
    if (fileExists("tests/e2e/docker-compose.yml")) {
        return .{
            .repo_root = ".",
            .compose_file = "tests/e2e/docker-compose.yml",
            .results_dir = "tests/e2e/.results",
            .zig_justfile = "tests/e2e/zig/Justfile",
            .go_justfile = "tests/e2e/go/Justfile",
            .cpp_justfile = "tests/e2e/cpp/Justfile",
            .python_justfile = "tests/e2e/python/Justfile",
            .rust_justfile = "tests/e2e/rust/Justfile",
        };
    }

    if (fileExists("docker-compose.yml") and fileExists("schemas/game_world.capnp")) {
        return .{
            .repo_root = "../..",
            .compose_file = "docker-compose.yml",
            .results_dir = ".results",
            .zig_justfile = "zig/Justfile",
            .go_justfile = "go/Justfile",
            .cpp_justfile = "cpp/Justfile",
            .python_justfile = "python/Justfile",
            .rust_justfile = "rust/Justfile",
        };
    }

    return error.E2EPathsNotFound;
}

fn parseBackend(text: []const u8) !Backend {
    if (std.mem.eql(u8, text, "cpp")) return .cpp;
    if (std.mem.eql(u8, text, "go")) return .go;
    if (std.mem.eql(u8, text, "python")) return .python;
    if (std.mem.eql(u8, text, "rust")) return .rust;
    return error.InvalidBackend;
}

fn parseSchema(text: []const u8) !Schema {
    if (std.mem.eql(u8, text, "game_world")) return .game_world;
    if (std.mem.eql(u8, text, "chat")) return .chat;
    if (std.mem.eql(u8, text, "inventory")) return .inventory;
    if (std.mem.eql(u8, text, "matchmaking")) return .matchmaking;
    return error.InvalidSchema;
}

fn parseDirection(text: []const u8) !Direction {
    if (std.mem.eql(u8, text, "both")) return .both;
    if (std.mem.eql(u8, text, "zig-client")) return .zig_client;
    if (std.mem.eql(u8, text, "zig-server")) return .zig_server;
    return error.InvalidDirection;
}

fn parseArgs(allocator: Allocator) !Config {
    var cfg = Config{};
    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    var has_backend = false;
    var has_schema = false;

    for (argv[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            usage();
            return error.HelpRequested;
        }
        if (std.mem.eql(u8, arg, "--build-only")) {
            cfg.build_only = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--skip-build")) {
            cfg.skip_build = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--allow-missing-hooks")) {
            cfg.allow_missing_hooks = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--verbose")) {
            cfg.verbose = true;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--direction=")) {
            cfg.direction = try parseDirection(arg["--direction=".len..]);
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--backend=")) {
            const b = try parseBackend(arg["--backend=".len..]);
            cfg.backend_selected[@intFromEnum(b)] = true;
            has_backend = true;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--schema=")) {
            const s = try parseSchema(arg["--schema=".len..]);
            cfg.schema_selected[@intFromEnum(s)] = true;
            has_schema = true;
            continue;
        }

        std.debug.print("Unknown option: {s}\n", .{arg});
        return error.InvalidOption;
    }

    if (!has_backend) {
        for (all_backends) |backend| {
            cfg.backend_selected[@intFromEnum(backend)] = backendDefaultEnabled(backend);
        }
    }
    if (!has_schema) {
        for (&cfg.schema_selected) |*slot| slot.* = true;
    }

    return cfg;
}

fn getOptionalEnv(allocator: Allocator, key: []const u8) !?[]u8 {
    return std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => err,
    };
}

fn termExitCode(term: std.process.Child.Term) i32 {
    return switch (term) {
        .Exited => |code| @as(i32, code),
        .Signal => |sig| @as(i32, @intCast(sig)) + 128,
        else => 1,
    };
}

fn runCapture(
    allocator: Allocator,
    argv: []const []const u8,
    cwd: ?[]const u8,
    env_map: ?*const std.process.EnvMap,
) !RunResult {
    const result = try std.process.Child.run(.{
        .allocator = allocator,
        .argv = argv,
        .cwd = cwd,
        .env_map = env_map,
        .max_output_bytes = 64 * 1024 * 1024,
    });

    return .{
        .exit_code = termExitCode(result.term),
        .stdout = result.stdout,
        .stderr = result.stderr,
    };
}

const CollectThreadCtx = struct {
    child: std.process.Child,
    allocator: Allocator,
    max_output_bytes: usize,
    stdout: std.ArrayList(u8) = .{},
    stderr: std.ArrayList(u8) = .{},
    collect_err: ?anyerror = null,
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

fn collectOutputThread(ctx: *CollectThreadCtx) void {
    ctx.child.collectOutput(ctx.allocator, &ctx.stdout, &ctx.stderr, ctx.max_output_bytes) catch |err| {
        ctx.collect_err = err;
    };
    ctx.done.store(true, .release);
}

fn closeChildPipes(child: *std.process.Child) void {
    if (child.stdin) |*stdin_file| {
        stdin_file.close();
        child.stdin = null;
    }
    if (child.stdout) |*stdout_file| {
        stdout_file.close();
        child.stdout = null;
    }
    if (child.stderr) |*stderr_file| {
        stderr_file.close();
        child.stderr = null;
    }
}

fn termFromWaitStatus(status: u32) std.process.Child.Term {
    return if (std.posix.W.IFEXITED(status))
        .{ .Exited = std.posix.W.EXITSTATUS(status) }
    else if (std.posix.W.IFSIGNALED(status))
        .{ .Signal = std.posix.W.TERMSIG(status) }
    else if (std.posix.W.IFSTOPPED(status))
        .{ .Stopped = std.posix.W.STOPSIG(status) }
    else
        .{ .Unknown = status };
}

fn runCaptureWithTimeout(
    allocator: Allocator,
    argv: []const []const u8,
    cwd: ?[]const u8,
    env_map: ?*const std.process.EnvMap,
    timeout_ms: i64,
) !RunResult {
    if (timeout_ms <= 0 or builtin.os.tag == .windows) {
        return runCapture(allocator, argv, cwd, env_map);
    }

    var child = std.process.Child.init(argv, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Pipe;
    child.cwd = cwd;
    child.env_map = env_map;

    try child.spawn();
    errdefer {
        _ = child.kill() catch {};
    }

    var collect_ctx = CollectThreadCtx{
        .child = child,
        .allocator = allocator,
        .max_output_bytes = 64 * 1024 * 1024,
    };
    errdefer collect_ctx.stdout.deinit(allocator);
    errdefer collect_ctx.stderr.deinit(allocator);

    const collector = try std.Thread.spawn(.{}, collectOutputThread, .{&collect_ctx});
    defer collector.join();

    const deadline = std.time.milliTimestamp() + timeout_ms;
    var timed_out = false;
    var term: std.process.Child.Term = .{ .Unknown = 0 };

    wait_loop: while (true) {
        const wait_result = std.posix.waitpid(child.id, std.c.W.NOHANG);
        if (wait_result.pid == child.id) {
            term = termFromWaitStatus(wait_result.status);
            break :wait_loop;
        }

        if (std.time.milliTimestamp() >= deadline) {
            timed_out = true;
            std.posix.kill(child.id, std.posix.SIG.TERM) catch |err| switch (err) {
                error.ProcessNotFound => {},
                else => {},
            };

            const kill_deadline = std.time.milliTimestamp() + 5 * 1000;
            while (std.time.milliTimestamp() < kill_deadline) {
                const term_wait = std.posix.waitpid(child.id, std.c.W.NOHANG);
                if (term_wait.pid == child.id) {
                    term = termFromWaitStatus(term_wait.status);
                    break :wait_loop;
                }
                std.Thread.sleep(20 * std.time.ns_per_ms);
            }

            std.posix.kill(child.id, std.posix.SIG.KILL) catch |err| switch (err) {
                error.ProcessNotFound => {},
                else => {},
            };

            const kill_wait = std.posix.waitpid(child.id, 0);
            term = termFromWaitStatus(kill_wait.status);
            break :wait_loop;
        }

        std.Thread.sleep(20 * std.time.ns_per_ms);
    }

    while (!collect_ctx.done.load(.acquire)) {
        std.Thread.sleep(5 * std.time.ns_per_ms);
    }

    if (collect_ctx.collect_err) |err| {
        if (!timed_out) return err;
    }

    closeChildPipes(&child);
    child.term = term;
    child.id = undefined;

    return .{
        .exit_code = if (timed_out) 124 else termExitCode(term),
        .stdout = try collect_ctx.stdout.toOwnedSlice(allocator),
        .stderr = try collect_ctx.stderr.toOwnedSlice(allocator),
    };
}

fn runShellCapture(
    allocator: Allocator,
    command: []const u8,
    env_map: ?*const std.process.EnvMap,
) !RunResult {
    return runCapture(allocator, &.{ "sh", "-lc", command }, null, env_map);
}

fn runShellCaptureWithTimeout(
    allocator: Allocator,
    command: []const u8,
    env_map: ?*const std.process.EnvMap,
    timeout_ms: i64,
) !RunResult {
    return runCaptureWithTimeout(allocator, &.{ "sh", "-lc", command }, null, env_map, timeout_ms);
}

fn writeCombinedOutput(path: []const u8, stdout_bytes: []const u8, stderr_bytes: []const u8) !void {
    var file = try std.fs.cwd().createFile(path, .{});
    defer file.close();

    if (stdout_bytes.len > 0) try file.writeAll(stdout_bytes);
    if (stderr_bytes.len > 0) {
        if (stdout_bytes.len > 0 and stdout_bytes[stdout_bytes.len - 1] != '\n') {
            try file.writeAll("\n");
        }
        try file.writeAll(stderr_bytes);
    }
}

fn printCommandFailure(label: []const u8, res: RunResult) void {
    std.debug.print("{s} failed (exit={d})\n", .{ label, res.exit_code });
    if (res.stdout.len > 0) std.debug.print("stdout:\n{s}\n", .{res.stdout});
    if (res.stderr.len > 0) std.debug.print("stderr:\n{s}\n", .{res.stderr});
}

fn waitForPort(port: u16, timeout_ms: i64) !bool {
    const addr = try std.net.Address.parseIp4("127.0.0.1", port);
    const start = std.time.milliTimestamp();

    while (true) {
        const stream = std.net.tcpConnectToAddress(addr) catch {
            if (std.time.milliTimestamp() - start > timeout_ms) return false;
            std.Thread.sleep(100 * std.time.ns_per_ms);
            continue;
        };
        stream.close();
        return true;
    }
}

fn composeBaseArgs(allocator: Allocator, paths: Paths) !std.ArrayList([]const u8) {
    var args = std.ArrayList([]const u8){};
    try args.appendSlice(allocator, &.{
        "docker",
        "compose",
        "-f",
        paths.compose_file,
    });
    return args;
}

fn dockerRmForce(allocator: Allocator, container: []const u8) !void {
    const res = try runCapture(allocator, &.{ "docker", "rm", "-f", container }, null, null);
    defer allocator.free(res.stdout);
    defer allocator.free(res.stderr);
    // Ignore failures; container may not exist.
}

fn stopRefServers(allocator: Allocator) !void {
    for (all_backends) |backend| {
        for (all_schemas) |schema| {
            const name = try std.fmt.allocPrint(allocator, "e2e-ref-server-{s}-{s}", .{ backendName(backend), schemaName(schema) });
            defer allocator.free(name);
            try dockerRmForce(allocator, name);
        }
    }
}

fn composeDown(allocator: Allocator, paths: Paths) !void {
    var args = try composeBaseArgs(allocator, paths);
    defer args.deinit(allocator);
    try args.appendSlice(allocator, &.{ "down", "--remove-orphans", "--timeout", "5" });

    const res = try runCapture(allocator, args.items, null, null);
    defer allocator.free(res.stdout);
    defer allocator.free(res.stderr);
    // Ignore non-zero, used for cleanup only.
}

fn buildImages(allocator: Allocator, paths: Paths, cfg: Config) !void {
    for (all_backends) |backend| {
        if (!cfg.isBackendSelected(backend)) continue;

        const justfile = paths.backendJustfile(backend);
        const res = try runCapture(allocator, &.{ "just", "--justfile", justfile, "docker-build" }, null, null);
        defer allocator.free(res.stdout);
        defer allocator.free(res.stderr);

        if (res.exit_code != 0) {
            const label = try std.fmt.allocPrint(allocator, "just docker-build ({s})", .{backendName(backend)});
            defer allocator.free(label);
            printCommandFailure(label, res);
            return error.CommandFailed;
        }
    }
}

fn startRefServer(allocator: Allocator, paths: Paths, backend: Backend, schema: Schema) !void {
    const container_name = try std.fmt.allocPrint(allocator, "e2e-ref-server-{s}-{s}", .{ backendName(backend), schemaName(schema) });
    defer allocator.free(container_name);

    try dockerRmForce(allocator, container_name);

    var port_buf: [16]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{backendPort(backend)});

    const justfile = paths.backendJustfile(backend);
    const res = try runCapture(allocator, &.{
        "just",
        "--justfile",
        justfile,
        "docker-server",
        container_name,
        "0.0.0.0",
        port_text,
        schemaNameForBackend(backend, schema),
    }, null, null);
    defer allocator.free(res.stdout);
    defer allocator.free(res.stderr);

    if (res.exit_code != 0) {
        const label = try std.fmt.allocPrint(allocator, "just docker-server ({s})", .{backendName(backend)});
        defer allocator.free(label);
        printCommandFailure(label, res);
        return error.ServerStartFailed;
    }

    const ready = try waitForPort(backendPort(backend), server_timeout_ms);
    if (!ready) {
        try dockerRmForce(allocator, container_name);
        return error.ServerStartTimeout;
    }
}

fn stopRefServer(allocator: Allocator, backend: Backend, schema: Schema) !void {
    const container_name = try std.fmt.allocPrint(allocator, "e2e-ref-server-{s}-{s}", .{ backendName(backend), schemaName(schema) });
    defer allocator.free(container_name);
    try dockerRmForce(allocator, container_name);
}

fn evalTap(output: []const u8) TapEval {
    var pass_count: usize = 0;
    var fail_count: usize = 0;
    var bailed = false;

    var it = std.mem.splitScalar(u8, output, '\n');
    while (it.next()) |line| {
        if (std.mem.startsWith(u8, line, "Bail out!")) {
            bailed = true;
        } else if (std.mem.startsWith(u8, line, "ok ")) {
            pass_count += 1;
        } else if (std.mem.startsWith(u8, line, "not ok ")) {
            fail_count += 1;
        }
    }

    return .{ .pass_count = pass_count, .fail_count = fail_count, .bailed = bailed };
}

fn statusFromRun(allocator: Allocator, exit_code: i32, tap: TapEval) ![]u8 {
    if (exit_code == 0 and tap.fail_count == 0 and tap.pass_count > 0 and !tap.bailed) {
        return std.fmt.allocPrint(allocator, "PASS({d})", .{tap.pass_count});
    }

    if (tap.bailed) {
        if (exit_code == 0) return allocator.dupe(u8, "FAIL(BAIL)");
        return std.fmt.allocPrint(allocator, "FAIL(exit={d},BAIL)", .{exit_code});
    }

    if (tap.pass_count + tap.fail_count == 0) {
        if (exit_code == 0) return allocator.dupe(u8, "FAIL(NO_TESTS)");
        return std.fmt.allocPrint(allocator, "FAIL(exit={d},NO_TESTS)", .{exit_code});
    }

    if (exit_code == 0) {
        return std.fmt.allocPrint(allocator, "FAIL({d}/{d})", .{ tap.fail_count, tap.pass_count + tap.fail_count });
    }

    return std.fmt.allocPrint(allocator, "FAIL(exit={d},{d}/{d})", .{ exit_code, tap.fail_count, tap.pass_count + tap.fail_count });
}

fn appendResult(allocator: Allocator, list: *std.ArrayList(CaseResult), key: []const u8, status: []const u8) !void {
    try list.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .status = try allocator.dupe(u8, status),
    });
}

fn runZigClientCase(
    allocator: Allocator,
    paths: Paths,
    zig_client_cmd_override: ?[]const u8,
    backend: Backend,
    schema: Schema,
    output_path: []const u8,
) !RunResult {
    var port_buf: [16]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{backendPort(backend)});
    const res = if (zig_client_cmd_override) |cmd| blk: {
        var env = try std.process.getEnvMap(allocator);
        defer env.deinit();
        try env.put("E2E_TARGET_HOST", "127.0.0.1");
        try env.put("E2E_TARGET_PORT", port_text);
        try env.put("E2E_SCHEMA", schemaName(schema));
        try env.put("E2E_BACKEND", backendName(backend));
        break :blk try runShellCaptureWithTimeout(allocator, cmd, &env, case_timeout_ms);
    } else try runCaptureWithTimeout(allocator, &.{
        "just",
        "--justfile",
        paths.zig_justfile,
        "client-hook",
        "127.0.0.1",
        port_text,
        schemaName(schema),
        backendName(backend),
    }, null, null, case_timeout_ms);

    try writeCombinedOutput(output_path, res.stdout, res.stderr);
    return res;
}

fn runRefClientCase(
    allocator: Allocator,
    paths: Paths,
    backend: Backend,
    schema: Schema,
    target_port: u16,
    output_path: []const u8,
) !RunResult {
    var port_buf: [16]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{target_port});
    const justfile = paths.backendJustfile(backend);
    const res = try runCaptureWithTimeout(allocator, &.{
        "just",
        "--justfile",
        justfile,
        "docker-client",
        "host.docker.internal",
        port_text,
        schemaNameForBackend(backend, schema),
    }, null, null, case_timeout_ms);
    try writeCombinedOutput(output_path, res.stdout, res.stderr);
    return res;
}

fn startZigServer(
    allocator: Allocator,
    paths: Paths,
    zig_server_cmd_override: ?[]const u8,
    schema: Schema,
    port: u16,
    inherit_server_logs: bool,
) !std.process.Child {
    var port_buf: [16]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{port});
    var env_storage: ?std.process.EnvMap = null;
    var env_ptr: ?*std.process.EnvMap = null;
    if (zig_server_cmd_override != null) {
        env_storage = try std.process.getEnvMap(allocator);
        env_ptr = &env_storage.?;
    }
    defer if (env_storage) |*map| map.deinit();

    var child = if (zig_server_cmd_override) |cmd| blk: {
        try env_ptr.?.put("E2E_BIND_HOST", "0.0.0.0");
        try env_ptr.?.put("E2E_BIND_PORT", port_text);
        try env_ptr.?.put("E2E_SCHEMA", schemaName(schema));
        var c = std.process.Child.init(&.{ "sh", "-lc", cmd }, allocator);
        c.env_map = env_ptr.?;
        break :blk c;
    } else blk: {
        var c = std.process.Child.init(&.{
            "zig",
            "build",
            "--global-cache-dir",
            "./.zig-global-cache",
            "e2e-zig-server",
            "--",
            "--host",
            "0.0.0.0",
            "--port",
            port_text,
            "--schema",
            schemaName(schema),
        }, allocator);
        c.cwd = paths.repo_root;
        break :blk c;
    };

    child.stdin_behavior = .Ignore;
    child.stdout_behavior = if (inherit_server_logs) .Inherit else .Ignore;
    child.stderr_behavior = if (inherit_server_logs) .Inherit else .Ignore;

    try child.spawn();

    const ready = try waitForPort(port, server_timeout_ms);
    if (!ready) {
        _ = child.kill() catch {};
        _ = child.wait() catch {};
        return error.ServerStartTimeout;
    }

    return child;
}

fn runZigClientPhase(
    allocator: Allocator,
    cfg: Config,
    paths: Paths,
    zig_client_cmd_override: ?[]const u8,
    results: *std.ArrayList(CaseResult),
) !void {
    std.debug.print("==> Phase: Zig client -> reference server\n", .{});

    for (all_schemas) |schema| {
        if (!cfg.isSchemaSelected(schema)) continue;

        for (all_backends) |backend| {
            if (!cfg.isBackendSelected(backend)) continue;

            const key = try std.fmt.allocPrint(allocator, "zig-client:{s}:{s}", .{ schemaName(schema), backendName(backend) });
            defer allocator.free(key);

            std.debug.print("    case {s}\n", .{key});

            startRefServer(allocator, paths, backend, schema) catch |err| {
                const status = try std.fmt.allocPrint(allocator, "FAIL(server-start:{s})", .{@errorName(err)});
                defer allocator.free(status);
                try appendResult(allocator, results, key, status);
                continue;
            };

            defer stopRefServer(allocator, backend, schema) catch {};

            const output_path = try std.fmt.allocPrint(allocator, "{s}/zig_client_{s}_{s}.tap", .{ paths.results_dir, schemaName(schema), backendName(backend) });
            defer allocator.free(output_path);

            const run = try runZigClientCase(allocator, paths, zig_client_cmd_override, backend, schema, output_path);
            defer allocator.free(run.stdout);
            defer allocator.free(run.stderr);

            const combined = try std.mem.concat(allocator, u8, &.{ run.stdout, "\n", run.stderr });
            defer allocator.free(combined);

            const tap = evalTap(combined);
            const status = try statusFromRun(allocator, run.exit_code, tap);
            defer allocator.free(status);
            try appendResult(allocator, results, key, status);

            if (cfg.verbose and !std.mem.startsWith(u8, status, "PASS")) {
                std.debug.print("      output:\n{s}\n", .{combined});
            }
        }
    }
}

fn runZigServerPhase(
    allocator: Allocator,
    cfg: Config,
    paths: Paths,
    zig_server_cmd_override: ?[]const u8,
    results: *std.ArrayList(CaseResult),
) !void {
    std.debug.print("==> Phase: reference client -> Zig server\n", .{});

    for (all_schemas) |schema| {
        if (!cfg.isSchemaSelected(schema)) continue;
        for (all_backends) |backend| {
            if (!cfg.isBackendSelected(backend)) continue;

            const key = try std.fmt.allocPrint(allocator, "zig-server:{s}:{s}", .{ schemaName(schema), backendName(backend) });
            defer allocator.free(key);

            const zig_server_port = reserveLocalPort() catch |err| {
                const status = try std.fmt.allocPrint(allocator, "FAIL(port-reserve:{s})", .{@errorName(err)});
                defer allocator.free(status);
                try appendResult(allocator, results, key, status);
                continue;
            };

            std.debug.print("    starting Zig server for schema={s} on port={d}\n", .{ schemaName(schema), zig_server_port });
            var child = startZigServer(allocator, paths, zig_server_cmd_override, schema, zig_server_port, cfg.verbose) catch |err| {
                const status = try std.fmt.allocPrint(allocator, "FAIL(server-start:{s})", .{@errorName(err)});
                defer allocator.free(status);
                try appendResult(allocator, results, key, status);
                continue;
            };
            defer {
                _ = child.kill() catch {};
                _ = child.wait() catch {};
            }

            std.debug.print("    case {s}\n", .{key});

            const output_path = try std.fmt.allocPrint(allocator, "{s}/zig_server_{s}_{s}.tap", .{ paths.results_dir, schemaName(schema), backendName(backend) });
            defer allocator.free(output_path);

            const run = try runRefClientCase(allocator, paths, backend, schema, zig_server_port, output_path);
            defer allocator.free(run.stdout);
            defer allocator.free(run.stderr);

            const combined = try std.mem.concat(allocator, u8, &.{ run.stdout, "\n", run.stderr });
            defer allocator.free(combined);

            const tap = evalTap(combined);
            const status = try statusFromRun(allocator, run.exit_code, tap);
            defer allocator.free(status);
            try appendResult(allocator, results, key, status);

            if (cfg.verbose and !std.mem.startsWith(u8, status, "PASS")) {
                std.debug.print("      output:\n{s}\n", .{combined});
            }
        }
    }
}

fn writeFmt(allocator: Allocator, file: std.fs.File, comptime fmt: []const u8, args: anytype) !void {
    const text = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(text);
    try file.writeAll(text);
}

fn writeSummary(allocator: Allocator, paths: Paths, results: []const CaseResult) !void {
    var passed: usize = 0;
    var failed: usize = 0;
    var skipped: usize = 0;

    for (results) |item| {
        if (std.mem.startsWith(u8, item.status, "PASS")) {
            passed += 1;
        } else if (std.mem.startsWith(u8, item.status, "SKIP")) {
            skipped += 1;
        } else {
            failed += 1;
        }
    }

    std.debug.print("\n======================================================================\n", .{});
    std.debug.print("  Zig Interop E2E Summary\n", .{});
    std.debug.print("======================================================================\n\n", .{});
    std.debug.print("  Total cases: {d}\n", .{results.len});
    std.debug.print("  Passed:      {d}\n", .{passed});
    std.debug.print("  Failed:      {d}\n", .{failed});
    std.debug.print("  Skipped:     {d}\n\n", .{skipped});

    std.debug.print("{s: <36} {s}\n", .{ "case", "status" });
    std.debug.print("{s: <36} {s}\n", .{ "----", "------" });
    for (results) |item| {
        std.debug.print("{s: <36} {s}\n", .{ item.key, item.status });
    }

    const summary_path = try std.fmt.allocPrint(allocator, "{s}/summary.json", .{paths.results_dir});
    defer allocator.free(summary_path);

    var summary_file = try std.fs.cwd().createFile(summary_path, .{});
    defer summary_file.close();

    try writeFmt(allocator, summary_file, "{{\n", .{});
    try writeFmt(allocator, summary_file, "  \"total\": {d},\n", .{results.len});
    try writeFmt(allocator, summary_file, "  \"passed\": {d},\n", .{passed});
    try writeFmt(allocator, summary_file, "  \"failed\": {d},\n", .{failed});
    try writeFmt(allocator, summary_file, "  \"skipped\": {d},\n", .{skipped});
    try writeFmt(allocator, summary_file, "  \"results\": {{\n", .{});

    for (results, 0..) |item, idx| {
        const comma = if (idx + 1 == results.len) "" else ",";
        try writeFmt(allocator, summary_file, "    \"{s}\": \"{s}\"{s}\n", .{ item.key, item.status, comma });
    }

    try writeFmt(allocator, summary_file, "  }}\n", .{});
    try writeFmt(allocator, summary_file, "}}\n", .{});
    if (failed > 0) return error.E2EFailed;
}

fn runScaffoldWithMissingHooks(allocator: Allocator, cfg: Config, results: *std.ArrayList(CaseResult), have_client: bool, have_server: bool) !void {
    if ((cfg.direction == .both or cfg.direction == .zig_client) and !have_client) {
        for (all_schemas) |schema| {
            if (!cfg.isSchemaSelected(schema)) continue;
            for (all_backends) |backend| {
                if (!cfg.isBackendSelected(backend)) continue;
                const key = try std.fmt.allocPrint(allocator, "zig-client:{s}:{s}", .{ schemaName(schema), backendName(backend) });
                defer allocator.free(key);
                try appendResult(allocator, results, key, "SKIP(missing-zig-client-hook)");
            }
        }
    }

    if ((cfg.direction == .both or cfg.direction == .zig_server) and !have_server) {
        for (all_schemas) |schema| {
            if (!cfg.isSchemaSelected(schema)) continue;
            for (all_backends) |backend| {
                if (!cfg.isBackendSelected(backend)) continue;
                const key = try std.fmt.allocPrint(allocator, "zig-server:{s}:{s}", .{ schemaName(schema), backendName(backend) });
                defer allocator.free(key);
                try appendResult(allocator, results, key, "SKIP(missing-zig-server-hook)");
            }
        }
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const cfg = parseArgs(allocator) catch |err| switch (err) {
        error.HelpRequested => return,
        else => return err,
    };

    const paths = try detectPaths();
    try std.fs.cwd().makePath(paths.results_dir);

    // Best-effort cleanup of old servers from previous runs.
    try stopRefServers(allocator);
    defer {
        stopRefServers(allocator) catch {};
        composeDown(allocator, paths) catch {};
    }

    if (!cfg.skip_build) {
        try buildImages(allocator, paths, cfg);
    }

    if (cfg.build_only) return;

    const zig_client_cmd = try getOptionalEnv(allocator, "E2E_ZIG_CLIENT_CMD");
    defer if (zig_client_cmd) |cmd| allocator.free(cmd);

    const zig_server_cmd = try getOptionalEnv(allocator, "E2E_ZIG_SERVER_CMD");
    defer if (zig_server_cmd) |cmd| allocator.free(cmd);

    const have_default_hooks = fileExists(paths.zig_justfile);
    const have_client_hook = zig_client_cmd != null or have_default_hooks;
    const have_server_hook = zig_server_cmd != null or have_default_hooks;

    if (!cfg.allow_missing_hooks) {
        if ((cfg.direction == .both or cfg.direction == .zig_client) and !have_client_hook) {
            std.debug.print("Missing Zig client hook ({s}) and no E2E_ZIG_CLIENT_CMD override set\n", .{paths.zig_justfile});
            return error.MissingHook;
        }
        if ((cfg.direction == .both or cfg.direction == .zig_server) and !have_server_hook) {
            std.debug.print("Missing Zig server hook ({s}) and no E2E_ZIG_SERVER_CMD override set\n", .{paths.zig_justfile});
            return error.MissingHook;
        }
    }

    var results = std.ArrayList(CaseResult){};
    defer {
        for (results.items) |item| {
            allocator.free(item.key);
            allocator.free(item.status);
        }
        results.deinit(allocator);
    }

    if (cfg.allow_missing_hooks) {
        try runScaffoldWithMissingHooks(allocator, cfg, &results, have_client_hook, have_server_hook);
    }

    if ((cfg.direction == .both or cfg.direction == .zig_client) and have_client_hook) {
        try runZigClientPhase(allocator, cfg, paths, zig_client_cmd, &results);
    }

    if ((cfg.direction == .both or cfg.direction == .zig_server) and have_server_hook) {
        try runZigServerPhase(allocator, cfg, paths, zig_server_cmd, &results);
    }

    try writeSummary(allocator, paths, results.items);
}
