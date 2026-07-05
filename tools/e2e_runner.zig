const std = @import("std");
const builtin = @import("builtin");

const Allocator = std.mem.Allocator;

fn milliTimestamp(io: std.Io) i64 {
    return std.Io.Timestamp.now(io, .awake).toMilliseconds();
}

fn sleepNs(io: std.Io, ns: u64) void {
    std.Io.sleep(io, std.Io.Duration.fromNanoseconds(@intCast(ns)), .awake) catch {};
}

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
    resolve_disembargo,
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
    // Sized for every Schema enumerant (indexed by @intFromEnum). Only the
    // backend-backed schemas in `all_schemas` are ever exercised by the docker
    // matrix; resolve_disembargo has no reference backends yet, so it occupies
    // a slot here (to keep @intFromEnum indexing in bounds) but is not iterated.
    schema_selected: [5]bool = .{ false, false, false, false, false },

    fn isBackendSelected(self: Config, b: Backend) bool {
        return self.backend_selected[@intFromEnum(b)];
    }

    fn isSchemaSelected(self: Config, s: Schema) bool {
        return self.schema_selected[@intFromEnum(s)];
    }
};

const all_backends = [_]Backend{ .cpp, .go, .python, .rust };
// The docker interop matrix covers every (schema x backend x direction) case
// EXCEPT the per-direction reference-impl gaps in resolve_disembargo (see
// resolveDisembargoSkip below), which are recorded as SKIP rather than run.
const all_schemas = [_]Schema{ .game_world, .chat, .inventory, .matchmaking, .resolve_disembargo };

/// resolve_disembargo is the only scenario with per-direction reference-impl
/// gaps — all in the reference libraries, not capnp-zig. Returns a SKIP status
/// (recorded green, not run) when the given backend cannot play the required
/// role, else null. `zig_is_client` selects the direction: when Zig is the
/// client the reference plays the reflecting SERVER; when Zig is the server the
/// reference plays the embargo-driving CLIENT.
///   - Reflecting SERVER: must export an unresolved promise capability and
///     resolve it later. pycapnp has no such API, so python is client-only here.
///   - Embargo CLIENT: relays a reflected-loopback pipelined call back to a
///     caller-hosted cap. As of W1 Zig forwards that relayed call with
///     `sendResultsTo=caller` and translates the real results back as a plain
///     `.results` Return (see forwardModeForResolved / `.translate_to_caller`),
///     rather than the spec-canonical `yourself` + `takeFromOtherQuestion`
///     tail-call. Every reference client — cpp, python, go, and rust — consumes
///     that plain shape, so no client direction is skipped anymore. (The prior
///     skips existed because go-capnp rejects an inbound `sendResultsTo != caller`
///     call as Unimplemented and cannot parse a `takeFromOtherQuestion` return,
///     and capnp-rpc only completed the take-from path under narrow conditions.)
fn resolveDisembargoSkip(schema: Schema, backend: Backend, zig_is_client: bool) ?[]const u8 {
    if (schema != .resolve_disembargo) return null;
    if (zig_is_client) {
        // Reference is the reflecting server.
        return switch (backend) {
            .python => "SKIP(python-cannot-reflect-as-server)",
            else => null,
        };
    }
    // Reference is the embargo-driving client; W1 made the relayed return
    // universally consumable, so all reference clients run.
    return null;
}

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
/// How long a freshly accepted connection must stay open before the port
/// counts as ready (see `probeConnectionAlive`).
const ready_probe_window_ms: i32 = 250;
const default_e2e_zig_global_cache_dir = "./.zig-global-cache";

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
        \\Optional hook env vars:
        \\  E2E_ZIG_GLOBAL_CACHE_DIR
        \\  (if set, used to seed ZIG_GLOBAL_CACHE_DIR for zig build/run)
        \\
        \\Legacy override hook env vars:
        \\  E2E_ZIG_CLIENT_CMD
        \\  E2E_ZIG_SERVER_CMD
        \\By default the Zig client hook runs via:
        \\  just --justfile tests/e2e/zig/Justfile client-hook ...
        \\By default the Zig server runs via:
        \\  zig build e2e-zig-server -- ... (inherits ZIG_GLOBAL_CACHE_DIR)
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
        .resolve_disembargo => "resolve_disembargo",
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
        .resolve_disembargo => 4705,
    };
}

// ---------------------------------------------------------------------------
// Low-level socket helpers (matching src/rpc/transport/tcp/runtime.zig patterns)
// ---------------------------------------------------------------------------

const SockAddrStorage = extern union {
    any: std.posix.sockaddr,
    in: std.posix.sockaddr.in,
    in6: std.posix.sockaddr.in6,
};

fn ipAddressToSockaddr(addr: std.Io.net.IpAddress) struct { addr: SockAddrStorage, len: std.posix.socklen_t } {
    switch (addr) {
        .ip4 => |ip4| {
            return .{
                .addr = .{ .in = .{
                    .port = std.mem.nativeToBig(u16, ip4.port),
                    .addr = @bitCast(ip4.bytes),
                } },
                .len = @sizeOf(std.posix.sockaddr.in),
            };
        },
        .ip6 => |ip6| {
            return .{
                .addr = .{ .in6 = .{
                    .port = std.mem.nativeToBig(u16, ip6.port),
                    .flowinfo = ip6.flow,
                    .addr = ip6.bytes,
                    .scope_id = if (ip6.interface.isNone()) 0 else ip6.interface.index,
                } },
                .len = @sizeOf(std.posix.sockaddr.in6),
            };
        },
    }
}

fn closeFd(fd: std.posix.fd_t) void {
    switch (std.posix.errno(std.posix.system.close(fd))) {
        .SUCCESS, .INTR, .BADF => {},
        else => {},
    }
}

const socket_cloexec_unsupported = builtin.target.os.tag.isDarwin() or builtin.target.os.tag == .haiku;

fn rawTcpConnect(addr: std.Io.net.IpAddress) !std.posix.fd_t {
    const family: c_uint = switch (addr) {
        .ip4 => std.posix.AF.INET,
        .ip6 => std.posix.AF.INET6,
    };
    const flags: c_uint = std.posix.SOCK.STREAM | if (socket_cloexec_unsupported) @as(c_uint, 0) else std.posix.SOCK.CLOEXEC;
    const fd_rc = std.posix.system.socket(family, flags, 0);
    if (std.posix.errno(fd_rc) != .SUCCESS) return error.SocketCreateFailed;
    const fd: std.posix.fd_t = @intCast(fd_rc);
    errdefer closeFd(fd);

    if (socket_cloexec_unsupported) {
        _ = std.posix.system.fcntl(fd, std.posix.F.SETFD, @as(usize, std.posix.FD_CLOEXEC));
    }

    const storage = ipAddressToSockaddr(addr);
    while (true) {
        switch (std.posix.errno(std.posix.system.connect(fd, &storage.addr.any, storage.len))) {
            .SUCCESS => return fd,
            .INTR => continue,
            .CONNREFUSED => return error.ConnectionRefused,
            .CONNRESET => return error.ConnectionResetByPeer,
            .NETUNREACH => return error.NetworkUnreachable,
            .HOSTUNREACH => return error.HostUnreachable,
            .TIMEDOUT => return error.Timeout,
            else => return error.ConnectFailed,
        }
    }
}

// ---------------------------------------------------------------------------
// ListenSocket
// ---------------------------------------------------------------------------

const ListenSocket = struct {
    fd: std.posix.fd_t,
    port: u16,

    fn close(self: *ListenSocket) void {
        closeFd(self.fd);
        self.fd = -1;
    }
};

/// Create a listening socket on an OS-assigned port and return the fd + port.
/// The fd is kept open (with CLOEXEC cleared) so it can be inherited by a
/// child process, eliminating the TOCTOU race in reserve-then-rebind.
fn createListenSocket() !ListenSocket {
    const flags: c_uint = std.posix.SOCK.STREAM | if (socket_cloexec_unsupported) @as(c_uint, 0) else std.posix.SOCK.CLOEXEC;
    const fd_rc = std.posix.system.socket(std.posix.AF.INET, flags, 0);
    if (std.posix.errno(fd_rc) != .SUCCESS) return error.SocketCreateFailed;
    const fd: std.posix.fd_t = @intCast(fd_rc);
    errdefer closeFd(fd);

    if (socket_cloexec_unsupported) {
        _ = std.posix.system.fcntl(fd, std.posix.F.SETFD, @as(usize, std.posix.FD_CLOEXEC));
    }

    // Allow immediate rebind (defense in depth; primary fix is fd passing).
    try std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

    // Bind to 127.0.0.1:0 → OS assigns an ephemeral port.
    const addr: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    const storage = ipAddressToSockaddr(addr);
    if (std.posix.errno(std.posix.system.bind(fd, &storage.addr.any, storage.len)) != .SUCCESS) {
        return error.BindFailed;
    }

    if (std.posix.errno(std.posix.system.listen(fd, 128)) != .SUCCESS) {
        return error.ListenFailed;
    }

    // Read back the assigned port.
    var sa_storage: std.posix.sockaddr.storage = undefined;
    var sa_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.storage);
    if (std.posix.errno(std.posix.system.getsockname(fd, @ptrCast(&sa_storage), &sa_len)) != .SUCCESS) {
        return error.GetSockNameFailed;
    }
    const sa_in: *const std.posix.sockaddr.in = @ptrCast(@alignCast(&sa_storage));
    const port = std.mem.bigToNative(u16, sa_in.port);

    // Clear CLOEXEC so the fd survives fork/exec into the child process.
    if (comptime @hasDecl(std.posix.F, "SETFD")) {
        _ = std.posix.system.fcntl(fd, std.posix.F.SETFD, @as(usize, 0));
    }

    return .{ .fd = fd, .port = port };
}

fn fileExists(io: std.Io, path: []const u8) bool {
    const file = std.Io.Dir.cwd().openFile(io, path, .{}) catch return false;
    file.close(io);
    return true;
}

fn detectPaths(io: std.Io) !Paths {
    if (fileExists(io, "tests/e2e/docker-compose.yml")) {
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

    if (fileExists(io, "docker-compose.yml") and fileExists(io, "schemas/game_world.capnp")) {
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
    if (std.mem.eql(u8, text, "resolve_disembargo")) return .resolve_disembargo;
    return error.InvalidSchema;
}

fn parseDirection(text: []const u8) !Direction {
    if (std.mem.eql(u8, text, "both")) return .both;
    if (std.mem.eql(u8, text, "zig-client")) return .zig_client;
    if (std.mem.eql(u8, text, "zig-server")) return .zig_server;
    return error.InvalidDirection;
}

fn parseArgs(allocator: Allocator, args: std.process.Args) !Config {
    var cfg = Config{};

    var has_backend = false;
    var has_schema = false;

    // initAllocator is the cross-platform form; plain init is a compile
    // error on Windows. Every flag parses to a bool/enum, so nothing
    // outlives the iterator.
    var args_iter = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer args_iter.deinit();
    _ = args_iter.skip(); // skip program name
    while (args_iter.next()) |arg| {
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

fn termExitCode(term: std.process.Child.Term) i32 {
    return switch (term) {
        .exited => |code| @as(i32, code),
        .signal => |sig| @as(i32, @intCast(@intFromEnum(sig))) + 128,
        else => 1,
    };
}

fn runCapture(
    allocator: Allocator,
    io: std.Io,
    argv: []const []const u8,
    cwd: ?[]const u8,
    environ_map: ?*const std.process.Environ.Map,
) !RunResult {
    const result = try std.process.run(allocator, io, .{
        .argv = argv,
        .cwd = if (cwd) |c| .{ .path = c } else .inherit,
        .environ_map = environ_map,
    });

    return .{
        .exit_code = termExitCode(result.term),
        .stdout = result.stdout,
        .stderr = result.stderr,
    };
}

const PipeReaderCtx = struct {
    fd: std.posix.fd_t,
    allocator: Allocator,
    buf: std.ArrayList(u8) = .empty,
};

fn pipeReaderThread(ctx: *PipeReaderCtx) void {
    var tmp: [4096]u8 = undefined;
    while (true) {
        const n = std.posix.read(ctx.fd, &tmp) catch break;
        if (n == 0) break;
        ctx.buf.appendSlice(ctx.allocator, tmp[0..@intCast(n)]) catch break;
    }
}

const WaitThreadCtx = struct {
    child: *std.process.Child,
    io: std.Io,
    term: std.process.Child.Term = .{ .unknown = 0 },
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

fn waitThread(ctx: *WaitThreadCtx) void {
    ctx.term = ctx.child.wait(ctx.io) catch .{ .unknown = 0 };
    ctx.done.store(true, .release);
}

fn runCaptureWithTimeout(
    allocator: Allocator,
    io: std.Io,
    argv: []const []const u8,
    cwd: ?[]const u8,
    environ_map: ?*const std.process.Environ.Map,
    timeout_ms: i64,
) !RunResult {
    if (timeout_ms <= 0 or builtin.os.tag == .windows) {
        return runCapture(allocator, io, argv, cwd, environ_map);
    }

    // Spawn child with piped stdout/stderr
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .cwd = if (cwd) |c| .{ .path = c } else .inherit,
        .environ_map = environ_map,
        .stdin = .ignore,
        .stdout = .pipe,
        .stderr = .pipe,
    });

    const pid = child.id orelse return error.NoChildPid;

    // Spawn threads to read stdout/stderr (prevents pipe buffer deadlock)
    var stdout_ctx = PipeReaderCtx{ .fd = if (child.stdout) |f| f.handle else -1, .allocator = allocator };
    errdefer stdout_ctx.buf.deinit(allocator);
    var stderr_ctx = PipeReaderCtx{ .fd = if (child.stderr) |f| f.handle else -1, .allocator = allocator };
    errdefer stderr_ctx.buf.deinit(allocator);

    const stdout_reader = if (stdout_ctx.fd >= 0)
        try std.Thread.spawn(.{}, pipeReaderThread, .{&stdout_ctx})
    else
        null;
    const stderr_reader = if (stderr_ctx.fd >= 0)
        try std.Thread.spawn(.{}, pipeReaderThread, .{&stderr_ctx})
    else
        null;

    // Start a wait thread so the main thread can enforce a timeout.
    var wait_ctx = WaitThreadCtx{ .child = &child, .io = io };
    const waiter = try std.Thread.spawn(.{}, waitThread, .{&wait_ctx});

    // Wait for child with timeout
    const deadline = milliTimestamp(io) + timeout_ms;
    var timed_out = false;

    while (!wait_ctx.done.load(.acquire)) {
        if (milliTimestamp(io) >= deadline) {
            timed_out = true;
            // Send SIGTERM first
            std.posix.kill(pid, std.posix.SIG.TERM) catch {};

            // Wait up to 5s for graceful exit
            const kill_deadline = milliTimestamp(io) + 5 * 1000;
            while (!wait_ctx.done.load(.acquire) and milliTimestamp(io) < kill_deadline) {
                sleepNs(io, 20 * std.time.ns_per_ms);
            }

            // Force kill if still running
            if (!wait_ctx.done.load(.acquire)) {
                std.posix.kill(pid, std.posix.SIG.KILL) catch {};
            }
            break;
        }
        sleepNs(io, 20 * std.time.ns_per_ms);
    }

    // Join all threads
    if (stdout_reader) |t| t.join();
    if (stderr_reader) |t| t.join();
    waiter.join();

    return .{
        .exit_code = if (timed_out) 124 else termExitCode(wait_ctx.term),
        .stdout = try stdout_ctx.buf.toOwnedSlice(allocator),
        .stderr = try stderr_ctx.buf.toOwnedSlice(allocator),
    };
}

fn runShellCapture(
    allocator: Allocator,
    io: std.Io,
    command: []const u8,
    environ_map: ?*const std.process.Environ.Map,
) !RunResult {
    return runCapture(allocator, io, &.{ "sh", "-lc", command }, null, environ_map);
}

fn runShellCaptureWithTimeout(
    allocator: Allocator,
    io: std.Io,
    command: []const u8,
    environ_map: ?*const std.process.Environ.Map,
    timeout_ms: i64,
) !RunResult {
    return runCaptureWithTimeout(allocator, io, &.{ "sh", "-lc", command }, null, environ_map, timeout_ms);
}

fn writeCombinedOutput(io: std.Io, path: []const u8, stdout_bytes: []const u8, stderr_bytes: []const u8) !void {
    const file = try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);

    if (stdout_bytes.len > 0) try file.writeStreamingAll(io, stdout_bytes);
    if (stderr_bytes.len > 0) {
        if (stdout_bytes.len > 0 and stdout_bytes[stdout_bytes.len - 1] != '\n') {
            try file.writeStreamingAll(io, "\n");
        }
        try file.writeStreamingAll(io, stderr_bytes);
    }
}

fn printCommandFailure(label: []const u8, res: RunResult) void {
    std.debug.print("{s} failed (exit={d})\n", .{ label, res.exit_code });
    if (res.stdout.len > 0) std.debug.print("stdout:\n{s}\n", .{res.stdout});
    if (res.stderr.len > 0) std.debug.print("stderr:\n{s}\n", .{res.stderr});
}

/// Wait until `port` accepts a connection that survives a short probe
/// window. A bare connect is not enough for docker published ports: the
/// userland proxy accepts on the host port as soon as the container starts
/// and closes immediately when the in-container server has not bound yet,
/// which reads as a false "ready" and races slow-booting backends.
fn waitForPort(io: std.Io, port: u16, timeout_ms: i64) !bool {
    const addr: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    const start = milliTimestamp(io);

    while (true) {
        if (rawTcpConnect(addr)) |fd| {
            defer closeFd(fd);
            if (probeConnectionAlive(fd)) return true;
        } else |_| {}
        if (milliTimestamp(io) - start > timeout_ms) return false;
        sleepNs(io, 100 * std.time.ns_per_ms);
    }
}

/// True when a freshly connected socket stays open (quiet or readable)
/// through the probe window. Cap'n Proto servers wait for the client's
/// first frame, so silence means a live listener; an instant EOF or reset
/// means a proxy whose backend is not up yet.
fn probeConnectionAlive(fd: std.posix.fd_t) bool {
    var fds = [_]std.posix.pollfd{.{ .fd = fd, .events = std.posix.POLL.IN, .revents = 0 }};
    while (true) {
        const poll_rc = std.posix.system.poll(&fds, 1, ready_probe_window_ms);
        switch (std.posix.errno(poll_rc)) {
            .SUCCESS => {},
            .INTR => continue,
            else => return false,
        }
        if (poll_rc == 0) return true; // Quiet and still open.
        var buf: [1]u8 = undefined;
        const read_rc = std.posix.system.read(fd, &buf, 1);
        if (std.posix.errno(read_rc) != .SUCCESS) return false;
        return read_rc != 0; // Bytes mean a live server; 0 is EOF.
    }
}

fn composeBaseArgs(allocator: Allocator, paths: Paths) !std.ArrayList([]const u8) {
    var args = std.ArrayList([]const u8).empty;
    try args.appendSlice(allocator, &.{
        "docker",
        "compose",
        "-f",
        paths.compose_file,
    });
    return args;
}

fn dockerRmForce(allocator: Allocator, io: std.Io, container: []const u8) !void {
    const res = try runCapture(allocator, io, &.{ "docker", "rm", "-f", container }, null, null);
    defer allocator.free(res.stdout);
    defer allocator.free(res.stderr);
    // Ignore failures; container may not exist.
}

fn stopRefServers(allocator: Allocator, io: std.Io) !void {
    for (all_backends) |backend| {
        for (all_schemas) |schema| {
            const name = try std.fmt.allocPrint(allocator, "e2e-ref-server-{s}-{s}", .{ backendName(backend), schemaName(schema) });
            defer allocator.free(name);
            try dockerRmForce(allocator, io, name);
        }
    }
}

fn composeDown(allocator: Allocator, io: std.Io, paths: Paths) !void {
    var args = try composeBaseArgs(allocator, paths);
    defer args.deinit(allocator);
    try args.appendSlice(allocator, &.{ "down", "--remove-orphans", "--timeout", "5" });

    const res = try runCapture(allocator, io, args.items, null, null);
    defer allocator.free(res.stdout);
    defer allocator.free(res.stderr);
    // Ignore non-zero, used for cleanup only.
}

fn buildImages(allocator: Allocator, io: std.Io, paths: Paths, cfg: Config) !void {
    for (all_backends) |backend| {
        if (!cfg.isBackendSelected(backend)) continue;

        const justfile = paths.backendJustfile(backend);
        const res = try runCapture(allocator, io, &.{ "just", "--justfile", justfile, "docker-build" }, null, null);
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

fn startRefServer(allocator: Allocator, io: std.Io, paths: Paths, backend: Backend, schema: Schema) !void {
    const container_name = try std.fmt.allocPrint(allocator, "e2e-ref-server-{s}-{s}", .{ backendName(backend), schemaName(schema) });
    defer allocator.free(container_name);

    try dockerRmForce(allocator, io, container_name);

    var port_buf: [16]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{backendPort(backend)});

    const justfile = paths.backendJustfile(backend);
    const res = try runCapture(allocator, io, &.{
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

    const ready = try waitForPort(io, backendPort(backend), server_timeout_ms);
    if (!ready) {
        try dockerRmForce(allocator, io, container_name);
        return error.ServerStartTimeout;
    }
}

fn stopRefServer(allocator: Allocator, io: std.Io, backend: Backend, schema: Schema) !void {
    const container_name = try std.fmt.allocPrint(allocator, "e2e-ref-server-{s}-{s}", .{ backendName(backend), schemaName(schema) });
    defer allocator.free(container_name);
    try dockerRmForce(allocator, io, container_name);
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
    io: std.Io,
    environ: std.process.Environ,
    paths: Paths,
    zig_client_cmd_override: ?[]const u8,
    backend: Backend,
    schema: Schema,
    output_path: []const u8,
) !RunResult {
    var port_buf: [16]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{backendPort(backend)});
    const res = if (zig_client_cmd_override) |cmd| blk: {
        var env = try environ.createMap(allocator);
        defer env.deinit();
        try env.put("E2E_TARGET_HOST", "127.0.0.1");
        try env.put("E2E_TARGET_PORT", port_text);
        try env.put("E2E_SCHEMA", schemaName(schema));
        try env.put("E2E_BACKEND", backendName(backend));
        break :blk try runShellCaptureWithTimeout(allocator, io, cmd, &env, case_timeout_ms);
    } else try runCaptureWithTimeout(allocator, io, &.{
        "just",
        "--justfile",
        paths.zig_justfile,
        "client-hook",
        "127.0.0.1",
        port_text,
        schemaName(schema),
        backendName(backend),
    }, null, null, case_timeout_ms);

    try writeCombinedOutput(io, output_path, res.stdout, res.stderr);
    return res;
}

fn runRefClientCase(
    allocator: Allocator,
    io: std.Io,
    paths: Paths,
    backend: Backend,
    schema: Schema,
    target_port: u16,
    output_path: []const u8,
) !RunResult {
    var port_buf: [16]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{target_port});
    const justfile = paths.backendJustfile(backend);
    const res = try runCaptureWithTimeout(allocator, io, &.{
        "just",
        "--justfile",
        justfile,
        "docker-client",
        "host.docker.internal",
        port_text,
        schemaNameForBackend(backend, schema),
    }, null, null, case_timeout_ms);
    try writeCombinedOutput(io, output_path, res.stdout, res.stderr);
    return res;
}

/// Find a free ephemeral port by binding to port 0 and immediately closing.
fn findFreePort() !u16 {
    var listen = try createListenSocket();
    const port = listen.port;
    listen.close();
    return port;
}

/// Pre-build the e2e-zig-server binary so we can run it directly.
fn prebuildZigServer(allocator: Allocator, io: std.Io, environ: std.process.Environ, paths: Paths) ![]u8 {
    var env = try environ.createMap(allocator);
    defer env.deinit();

    const cache_dir = if (env.get("E2E_ZIG_GLOBAL_CACHE_DIR")) |dir|
        dir
    else if (env.get("ZIG_GLOBAL_CACHE_DIR")) |dir|
        dir
    else
        default_e2e_zig_global_cache_dir;
    try env.put("ZIG_GLOBAL_CACHE_DIR", cache_dir);

    std.debug.print("    pre-building e2e-zig-server...\n", .{});
    const res = try runCapture(allocator, io, &.{ "zig", "build", "e2e-zig-server-install" }, paths.repo_root, &env);
    defer allocator.free(res.stdout);
    defer allocator.free(res.stderr);

    if (res.exit_code != 0) {
        printCommandFailure("zig build e2e-zig-server-install", res);
        return error.ServerBuildFailed;
    }

    return std.fmt.allocPrint(allocator, "{s}/zig-out/bin/e2e-zig-server", .{paths.repo_root});
}

fn startZigServer(
    allocator: Allocator,
    io: std.Io,
    environ: std.process.Environ,
    zig_server_cmd_override: ?[]const u8,
    server_bin_path: ?[]const u8,
    schema: Schema,
    port: u16,
    inherit_server_logs: bool,
) !std.process.Child {
    var port_buf: [16]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var env = try environ.createMap(allocator);
    defer env.deinit();

    const cache_dir = if (env.get("E2E_ZIG_GLOBAL_CACHE_DIR")) |dir|
        dir
    else if (env.get("ZIG_GLOBAL_CACHE_DIR")) |dir|
        dir
    else
        default_e2e_zig_global_cache_dir;
    try env.put("ZIG_GLOBAL_CACHE_DIR", cache_dir);

    const stdout_io: std.process.SpawnOptions.StdIo = if (inherit_server_logs) .inherit else .ignore;
    const stderr_io: std.process.SpawnOptions.StdIo = if (inherit_server_logs) .inherit else .ignore;

    var child = if (zig_server_cmd_override) |cmd| blk: {
        try env.put("E2E_BIND_HOST", "0.0.0.0");
        try env.put("E2E_BIND_PORT", port_text);
        try env.put("E2E_SCHEMA", schemaName(schema));
        break :blk try std.process.spawn(io, .{
            .argv = &.{ "sh", "-lc", cmd },
            .environ_map = &env,
            .stdin = .ignore,
            .stdout = stdout_io,
            .stderr = stderr_io,
        });
    } else if (server_bin_path) |bin| blk: {
        break :blk try std.process.spawn(io, .{
            .argv = &.{ bin, "--host", "0.0.0.0", "--port", port_text, "--schema", schemaName(schema) },
            .environ_map = &env,
            .stdin = .ignore,
            .stdout = stdout_io,
            .stderr = stderr_io,
        });
    } else unreachable;

    const ready = try waitForPort(io, port, server_timeout_ms);
    if (!ready) {
        child.kill(io);
        return error.ServerStartTimeout;
    }

    return child;
}

fn runZigClientPhase(
    allocator: Allocator,
    io: std.Io,
    environ: std.process.Environ,
    cfg: Config,
    paths: Paths,
    zig_client_cmd_override: ?[]const u8,
    results: *std.ArrayList(CaseResult),
) !void {
    std.debug.print("==> Phase: Zig client -> reference server\n", .{});

    // Pre-build the client once so the first case does not spend its
    // case timeout on a cold `zig build` (the per-case hook then hits a
    // warm cache). Mirrors the zig-server phase pre-build.
    if (zig_client_cmd_override == null) {
        std.debug.print("    pre-building e2e-zig-client...\n", .{});
        const res = try runCapture(allocator, io, &.{
            "just",
            "--justfile",
            paths.zig_justfile,
            "build-client",
        }, null, null);
        defer allocator.free(res.stdout);
        defer allocator.free(res.stderr);
        if (res.exit_code != 0) {
            printCommandFailure("just build-client", res);
            std.debug.print("    WARNING: pre-build failed, client tests will fail\n", .{});
        }
    }

    for (all_schemas) |schema| {
        if (!cfg.isSchemaSelected(schema)) continue;

        for (all_backends) |backend| {
            if (!cfg.isBackendSelected(backend)) continue;

            const key = try std.fmt.allocPrint(allocator, "zig-client:{s}:{s}", .{ schemaName(schema), backendName(backend) });
            defer allocator.free(key);

            std.debug.print("    case {s}\n", .{key});

            if (resolveDisembargoSkip(schema, backend, true)) |reason| {
                std.debug.print("      {s}\n", .{reason});
                try appendResult(allocator, results, key, reason);
                continue;
            }

            startRefServer(allocator, io, paths, backend, schema) catch |err| {
                const status = try std.fmt.allocPrint(allocator, "FAIL(server-start:{s})", .{@errorName(err)});
                defer allocator.free(status);
                try appendResult(allocator, results, key, status);
                continue;
            };

            defer stopRefServer(allocator, io, backend, schema) catch {};

            const output_path = try std.fmt.allocPrint(allocator, "{s}/zig_client_{s}_{s}.tap", .{ paths.results_dir, schemaName(schema), backendName(backend) });
            defer allocator.free(output_path);

            const run = try runZigClientCase(allocator, io, environ, paths, zig_client_cmd_override, backend, schema, output_path);
            defer allocator.free(run.stdout);
            defer allocator.free(run.stderr);

            const combined = try std.mem.concat(allocator, u8, &.{ run.stdout, "\n", run.stderr });
            defer allocator.free(combined);

            const tap = evalTap(combined);
            const status = try statusFromRun(allocator, run.exit_code, tap);
            defer allocator.free(status);
            try appendResult(allocator, results, key, status);

            // Always surface failing-case output; CI logs are otherwise
            // blind (the per-case .tap files are not uploaded anywhere).
            if (!std.mem.startsWith(u8, status, "PASS")) {
                std.debug.print("      output:\n{s}\n", .{combined});
            }
        }
    }
}

fn runZigServerPhase(
    allocator: Allocator,
    io: std.Io,
    environ: std.process.Environ,
    cfg: Config,
    paths: Paths,
    zig_server_cmd_override: ?[]const u8,
    results: *std.ArrayList(CaseResult),
) !void {
    std.debug.print("==> Phase: reference client -> Zig server\n", .{});

    // Pre-build the server binary once (skip if using override command).
    var server_bin_path: ?[]u8 = null;
    defer if (server_bin_path) |p| allocator.free(p);
    if (zig_server_cmd_override == null) {
        server_bin_path = prebuildZigServer(allocator, io, environ, paths) catch null;
        if (server_bin_path == null) {
            std.debug.print("    WARNING: pre-build failed, server tests will fail\n", .{});
        }
    }

    for (all_schemas) |schema| {
        if (!cfg.isSchemaSelected(schema)) continue;
        for (all_backends) |backend| {
            if (!cfg.isBackendSelected(backend)) continue;

            const key = try std.fmt.allocPrint(allocator, "zig-server:{s}:{s}", .{ schemaName(schema), backendName(backend) });
            defer allocator.free(key);

            if (resolveDisembargoSkip(schema, backend, false)) |reason| {
                std.debug.print("    case {s} — {s}\n", .{ key, reason });
                try appendResult(allocator, results, key, reason);
                continue;
            }

            const port = findFreePort() catch |err| {
                const status = try std.fmt.allocPrint(allocator, "FAIL(port-reserve:{s})", .{@errorName(err)});
                defer allocator.free(status);
                try appendResult(allocator, results, key, status);
                continue;
            };

            std.debug.print("    starting Zig server for schema={s} on port={d}\n", .{ schemaName(schema), port });
            var child = startZigServer(allocator, io, environ, zig_server_cmd_override, server_bin_path, schema, port, cfg.verbose) catch |err| {
                const status = try std.fmt.allocPrint(allocator, "FAIL(server-start:{s})", .{@errorName(err)});
                defer allocator.free(status);
                try appendResult(allocator, results, key, status);
                continue;
            };
            defer {
                child.kill(io);
            }

            std.debug.print("    case {s}\n", .{key});

            const output_path = try std.fmt.allocPrint(allocator, "{s}/zig_server_{s}_{s}.tap", .{ paths.results_dir, schemaName(schema), backendName(backend) });
            defer allocator.free(output_path);

            const run = try runRefClientCase(allocator, io, paths, backend, schema, port, output_path);
            defer allocator.free(run.stdout);
            defer allocator.free(run.stderr);

            const combined = try std.mem.concat(allocator, u8, &.{ run.stdout, "\n", run.stderr });
            defer allocator.free(combined);

            const tap = evalTap(combined);
            const status = try statusFromRun(allocator, run.exit_code, tap);
            defer allocator.free(status);
            try appendResult(allocator, results, key, status);

            // Always surface failing-case output; CI logs are otherwise
            // blind (the per-case .tap files are not uploaded anywhere).
            if (!std.mem.startsWith(u8, status, "PASS")) {
                std.debug.print("      output:\n{s}\n", .{combined});
            }
        }
    }
}

fn writeSummary(allocator: Allocator, io: std.Io, paths: Paths, results: []const CaseResult) !void {
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

    // Build summary JSON in memory
    var content = std.ArrayList(u8).empty;
    defer content.deinit(allocator);

    try appendFmt(allocator, &content, "{{\n", .{});
    try appendFmt(allocator, &content, "  \"total\": {d},\n", .{results.len});
    try appendFmt(allocator, &content, "  \"passed\": {d},\n", .{passed});
    try appendFmt(allocator, &content, "  \"failed\": {d},\n", .{failed});
    try appendFmt(allocator, &content, "  \"skipped\": {d},\n", .{skipped});
    try appendFmt(allocator, &content, "  \"results\": {{\n", .{});

    for (results, 0..) |item, idx| {
        const comma = if (idx + 1 == results.len) "" else ",";
        try appendFmt(allocator, &content, "    \"{s}\": \"{s}\"{s}\n", .{ item.key, item.status, comma });
    }

    try appendFmt(allocator, &content, "  }}\n", .{});
    try appendFmt(allocator, &content, "}}\n", .{});

    // Write to file
    const summary_path = try std.fmt.allocPrint(allocator, "{s}/summary.json", .{paths.results_dir});
    defer allocator.free(summary_path);

    const file = try std.Io.Dir.cwd().createFile(io, summary_path, .{});
    defer file.close(io);
    try file.writeStreamingAll(io, content.items);

    if (failed > 0) return error.E2EFailed;
}

fn appendFmt(allocator: Allocator, list: *std.ArrayList(u8), comptime fmt: []const u8, args: anytype) !void {
    const text = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(text);
    try list.appendSlice(allocator, text);
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

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const cfg = parseArgs(allocator, init.minimal.args) catch |err| switch (err) {
        error.HelpRequested => return,
        else => return err,
    };

    const paths = try detectPaths(io);
    try std.Io.Dir.cwd().createDirPath(io, paths.results_dir);

    // Best-effort cleanup of old servers from previous runs.
    try stopRefServers(allocator, io);
    defer {
        stopRefServers(allocator, io) catch {};
        composeDown(allocator, io, paths) catch {};
    }

    if (!cfg.skip_build) {
        try buildImages(allocator, io, paths, cfg);
    }

    if (cfg.build_only) return;

    const zig_client_cmd = init.environ_map.get("E2E_ZIG_CLIENT_CMD");
    const zig_server_cmd = init.environ_map.get("E2E_ZIG_SERVER_CMD");

    const have_default_hooks = fileExists(io, paths.zig_justfile);
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

    var results = std.ArrayList(CaseResult).empty;
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
        try runZigClientPhase(allocator, io, init.minimal.environ, cfg, paths, zig_client_cmd, &results);
    }

    if ((cfg.direction == .both or cfg.direction == .zig_server) and have_server_hook) {
        try runZigServerPhase(allocator, io, init.minimal.environ, cfg, paths, zig_server_cmd, &results);
    }

    try writeSummary(allocator, io, paths, results.items);
}
