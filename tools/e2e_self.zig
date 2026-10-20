//! Self-interop e2e: drive the Zig e2e client against the Zig e2e server
//! for each schema in `schemas` and `quic_schemas` below, with TAP
//! accounting. It leaves out pipelined_params_deferred, whose checks fail
//! by design.
//!
//! This is the no-docker complement to tools/e2e_runner.zig: it needs no
//! reference-implementation containers, builds on `std.Io` only, and runs
//! on every platform — including Windows CI runners, which cannot run the
//! Linux reference containers. It is the end-to-end exercise of the
//! platform's real socket stack (listener, accept, framing, writer
//! thread, peer dispatch).
//!
//! Three transports:
//! - TCP over loopback (the default; `zig build e2e-self`, every OS).
//! - `--transport=unix`: an AF_UNIX socket file in a private (0700)
//!   directory under /tmp, through `--host unix:/path` on both binaries
//!   (`rpc.transport.unix.listen` and `.connect`; `zig build e2e-self-unix`).
//!   Linux and macOS only, like `rpc.transport.unix`. On any other target
//!   it fails with `error.UnixSocketsUnsupported` rather than passing
//!   having run nothing.
//! - `--transport=quic`: the QUIC baseline wire, through `--transport quic`
//!   on both binaries (`zig build -Dquic=true e2e-quic`). It runs all seven
//!   schemas, `resolve_disembargo` too. The server listens on UDP port 0
//!   and prints the port it got; this tool reads that port from the
//!   server's stderr. The server takes `--cert-pem` and `--key-pem`. The
//!   client verifies the server against `--ca-pem`; it never runs with
//!   `--insecure`. CI runs this lane on Linux only (job `e2e-quic`).
//!
//! Usage: e2e-self <server-binary> <client-binary> [--transport=tcp|unix|quic]
//!        [--cert-pem=F --key-pem=F --ca-pem=F]  (QUIC only; all three)
//! (The build system passes both artifact paths and, for QUIC, the
//! committed loopback test certificate; see the `e2e-self` and `e2e-quic`
//! steps in build/build_impl.zig.)

const std = @import("std");
const builtin = @import("builtin");

/// The TCP and Unix lanes. They do not run `resolve_disembargo` yet.
const schemas = [_][]const u8{ "game_world", "chat", "inventory", "matchmaking", "pass_back", "pipelined_params" };
/// The QUIC lane: every schema the e2e server serves (its `Schema` enum),
/// except pipelined_params_deferred, whose checks fail by design (a known
/// limit of tools/e2e_runner.zig, listed in tools/e2e_known_limits.zig).
const quic_schemas = [_][]const u8{ "game_world", "chat", "inventory", "matchmaking", "resolve_disembargo", "pass_back", "pipelined_params" };
const server_ready_timeout_ms: i64 = 30_000;
const client_timeout_ms: i64 = 60_000;

const Transport = enum { tcp, unix, quic };

/// The PEM files of the QUIC lane (paths).
const QuicTls = struct {
    cert_pem: []const u8,
    key_pem: []const u8,
    ca_pem: []const u8,
};

/// Where `rpc.transport.unix` works (`unix.supported`): this tool imports
/// only std, so it restates that predicate.
const unix_supported = builtin.os.tag == .linux or builtin.os.tag.isDarwin();

fn nowMs(io: std.Io) i64 {
    return @intCast(@divTrunc(std.Io.Clock.awake.now(io).nanoseconds, std.time.ns_per_ms));
}

fn sleepMs(io: std.Io, ms: u64) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}

/// Reserve an ephemeral loopback port by binding port 0 and closing.
/// Subject to the usual reuse race, which is acceptable on loopback in a
/// harness that retries nothing else on the port.
fn findFreePort(io: std.Io) !u16 {
    const addr: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var server = try addr.listen(io, .{ .kernel_backlog = 1, .reuse_address = true });
    const port = server.socket.address.ip4.port;
    server.socket.close(io);
    return port;
}

/// Wait until the server accepts a loopback connection. A direct child
/// listener (no proxy in between) cannot produce false readiness, so a
/// plain connect probe is sufficient.
fn waitForServer(io: std.Io, port: u16) bool {
    const deadline = nowMs(io) + server_ready_timeout_ms;
    var addr: std.Io.net.IpAddress = .{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = port } };
    while (nowMs(io) < deadline) {
        if (std.Io.net.IpAddress.connect(&addr, io, .{ .mode = .stream, .protocol = .tcp })) |stream| {
            var socket = stream.socket;
            socket.close(io);
            return true;
        } else |_| {
            sleepMs(io, 100);
        }
    }
    return false;
}

/// Wait until the server accepts on its socket file. The file appears at
/// `bind`, before the server listens, and a connect in between is refused,
/// so a connect probe (not the file's existence) proves readiness.
fn waitForUnixServer(io: std.Io, path: []const u8) bool {
    const deadline = nowMs(io) + server_ready_timeout_ms;
    const addr = std.Io.net.UnixAddress.init(path) catch return false;
    while (nowMs(io) < deadline) {
        if (addr.connect(io)) |stream| {
            var socket = stream.socket;
            socket.close(io);
            return true;
        } else |_| {
            sleepMs(io, 100);
        }
    }
    return false;
}

const TapCount = struct {
    pass: usize = 0,
    fail: usize = 0,
};

fn countTap(output: []const u8) TapCount {
    var counts = TapCount{};
    var it = std.mem.splitScalar(u8, output, '\n');
    while (it.next()) |line| {
        if (std.mem.startsWith(u8, line, "ok ")) counts.pass += 1;
        if (std.mem.startsWith(u8, line, "not ok ")) counts.fail += 1;
    }
    return counts;
}

/// The `--host` (and, for TCP, `--port`) arguments both binaries get.
const Endpoint = struct {
    /// `127.0.0.1`, or `unix:<path>`.
    host: []const u8,
    /// TCP only: the loopback port, also passed as `--port`.
    port: ?u16,
    port_text: []const u8,

    fn argv(self: *const Endpoint, buf: *[7][]const u8, bin: []const u8, schema: []const u8) []const []const u8 {
        buf[0] = bin;
        buf[1] = "--host";
        buf[2] = self.host;
        var n: usize = 3;
        if (self.port != null) {
            buf[n] = "--port";
            buf[n + 1] = self.port_text;
            n += 2;
        }
        buf[n] = "--schema";
        buf[n + 1] = schema;
        return buf[0 .. n + 2];
    }

    fn waitReady(self: *const Endpoint, io: std.Io) bool {
        if (self.port) |port| return waitForServer(io, port);
        return waitForUnixServer(io, self.host[unix_host_prefix.len..]);
    }
};

const unix_host_prefix = "unix:";

/// What every schema run needs: the two binaries and the transport.
const Lane = struct {
    server_bin: []const u8,
    client_bin: []const u8,
    transport: union(Transport) {
        tcp,
        /// The private directory that holds the socket files.
        unix: []const u8,
        quic: QuicTls,
    },
};

fn runSchema(allocator: std.mem.Allocator, io: std.Io, lane: *const Lane, schema: []const u8) !TapCount {
    var port_buf: [8]u8 = undefined;
    var host_buf: [128]u8 = undefined;
    const endpoint: Endpoint = switch (lane.transport) {
        .tcp => blk: {
            const port = try findFreePort(io);
            break :blk .{
                .host = "127.0.0.1",
                .port = port,
                .port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{port}),
            };
        },
        // One socket file per schema in the private directory.
        .unix => |dir| .{
            .host = try std.fmt.bufPrint(&host_buf, unix_host_prefix ++ "{s}/{s}.sock", .{ dir, schema }),
            .port = null,
            .port_text = "",
        },
        .quic => |tls| return runQuicSchema(allocator, io, lane, tls, schema),
    };

    var server_argv_buf: [7][]const u8 = undefined;
    var server = try std.process.spawn(io, .{
        .argv = endpoint.argv(&server_argv_buf, lane.server_bin, schema),
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
    defer server.kill(io);

    if (!endpoint.waitReady(io)) {
        std.debug.print("not ok - {s}: server did not become ready on {s}\n", .{ schema, endpoint.host });
        return .{ .fail = 1 };
    }

    var client_argv_buf: [7][]const u8 = undefined;
    return runClient(allocator, io, endpoint.argv(&client_argv_buf, lane.client_bin, schema), schema);
}

/// The QUIC server's stderr. A concurrent task drains the pipe for the
/// server's whole life, so the server never blocks on a full pipe. The
/// harness reads the bound port from it, and prints it when a schema fails.
const ServerLog = struct {
    allocator: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    bytes: std.ArrayList(u8) = .empty,
    /// The pipe reached end of stream: the server exited.
    ended: bool = false,

    /// Past this many bytes the drain keeps reading but drops the rest.
    const max_bytes = 1024 * 1024;
    /// The server's line after it binds: "... over quic on HOST:PORT".
    const port_marker = " over quic on ";

    fn deinit(self: *ServerLog) void {
        self.bytes.deinit(self.allocator);
    }

    fn drain(self: *ServerLog, io: std.Io, pipe: std.Io.File) void {
        var buf: [4096]u8 = undefined;
        var vecs = [_][]u8{&buf};
        while (true) {
            const n = pipe.readStreaming(io, &vecs) catch break;
            if (n == 0) break;
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            const keep = @min(n, max_bytes -| self.bytes.items.len);
            self.bytes.appendSlice(self.allocator, buf[0..keep]) catch {};
        }
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.ended = true;
    }

    /// `.none`: no port will come (the server exited, or its port line
    /// does not parse).
    const PortState = union(enum) { pending, none, port: u16 };

    /// The bound port, once the server's whole port line is in.
    fn port(self: *ServerLog, io: std.Io) PortState {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const text = self.bytes.items;
        if (std.mem.indexOf(u8, text, port_marker)) |at| {
            const rest = text[at + port_marker.len ..];
            if (std.mem.indexOfScalar(u8, rest, '\n')) |eol| {
                const address = std.mem.trimEnd(u8, rest[0..eol], "\r");
                const colon = std.mem.lastIndexOfScalar(u8, address, ':') orelse return .none;
                const parsed = std.fmt.parseInt(u16, address[colon + 1 ..], 10) catch return .none;
                if (parsed == 0) return .none;
                return .{ .port = parsed };
            }
        }
        return if (self.ended) .none else .pending;
    }

    fn print(self: *ServerLog, io: std.Io, schema: []const u8) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        std.debug.print("# {s}: server stderr:\n{s}\n", .{ schema, self.bytes.items });
    }
};

/// Wait for the QUIC server to print its bound port. A server that exits
/// first, or prints no port within the deadline, gives null.
fn waitForQuicPort(io: std.Io, log: *ServerLog) ?u16 {
    const deadline = nowMs(io) + server_ready_timeout_ms;
    while (nowMs(io) < deadline) {
        switch (log.port(io)) {
            .port => |p| return p,
            .none => return null,
            .pending => sleepMs(io, 50),
        }
    }
    return null;
}

fn runQuicSchema(allocator: std.mem.Allocator, io: std.Io, lane: *const Lane, tls: QuicTls, schema: []const u8) !TapCount {
    // Port 0: the server binds a free UDP port and prints it, so there is no
    // window in which another process can take a port chosen here.
    const server_argv = [_][]const u8{
        lane.server_bin, "--host",    "127.0.0.1",   "--port", "0",
        "--schema",      schema,      "--transport", "quic",   "--cert-pem",
        tls.cert_pem,    "--key-pem", tls.key_pem,
    };
    var server = try std.process.spawn(io, .{
        .argv = &server_argv,
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .pipe,
    });
    // The drain owns the read end of the pipe: `kill` must not close it
    // under a read in progress.
    const pipe = server.stderr orelse unreachable; // .stderr = .pipe
    server.stderr = null;

    var log: ServerLog = .{ .allocator = allocator };
    defer log.deinit();
    var drain = io.concurrent(ServerLog.drain, .{ &log, io, pipe }) catch |err| {
        server.kill(io);
        pipe.close(io);
        return err;
    };
    // Every return path stops the server (SIGTERM, which it does not
    // handle, then a reap; `TerminateProcess` on Windows). The server's
    // end of the pipe closes with it, so the drain then reads end of
    // stream and returns.
    defer {
        server.kill(io);
        drain.await(io);
        pipe.close(io);
    }

    const port = waitForQuicPort(io, &log) orelse {
        std.debug.print("not ok - {s}: the QUIC server reported no bound port (it exited, or printed none within {d}ms)\n", .{ schema, server_ready_timeout_ms });
        log.print(io, schema);
        return .{ .fail = 1 };
    };
    var port_buf: [8]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    const client_argv = [_][]const u8{
        lane.client_bin, "--host", "127.0.0.1",   "--port", port_text,
        "--schema",      schema,   "--transport", "quic",   "--ca-pem",
        tls.ca_pem,
    };
    const counts = try runClient(allocator, io, &client_argv, schema);
    if (counts.fail != 0) log.print(io, schema);
    return counts;
}

/// Run the client once against a ready server and count its TAP lines. A
/// client that fails, times out or reports no passing test counts as one
/// more failure, and its output is printed.
fn runClient(allocator: std.mem.Allocator, io: std.Io, argv: []const []const u8, schema: []const u8) !TapCount {
    // Bound the client run with an absolute deadline so a hung client fails
    // this one schema fast instead of burning the entire CI job timeout. A
    // `.deadline` (rather than `.duration`) timeout is a single wall-clock
    // bound across every internal read; a `.duration` would be a per-read
    // idle timeout that a slow trickle of output could reset forever.
    // std.process.run kills the child via its own `defer child.kill` when the
    // deadline trips, surfacing the wait as error.Timeout.
    const client_deadline: std.Io.Timeout = .{ .deadline = std.Io.Clock.Timestamp.fromNow(io, .{
        .raw = std.Io.Duration.fromMilliseconds(client_timeout_ms),
        .clock = .awake,
    }) };
    const result = std.process.run(allocator, io, .{
        .argv = argv,
        .stdout_limit = .limited(1024 * 1024),
        .stderr_limit = .limited(1024 * 1024),
        .timeout = client_deadline,
    }) catch |err| switch (err) {
        error.Timeout => {
            std.debug.print("not ok - {s}: client timed out after {d}ms\n", .{ schema, client_timeout_ms });
            return .{ .fail = 1 };
        },
        else => return err,
    };
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);

    // TAP lines are emitted via std.debug.print (stderr); count both
    // streams like tools/e2e_runner.zig does.
    var counts = countTap(result.stdout);
    const stderr_counts = countTap(result.stderr);
    counts.pass += stderr_counts.pass;
    counts.fail += stderr_counts.fail;
    const exited_zero = result.term == .exited and result.term.exited == 0;
    if (!exited_zero or counts.pass == 0) {
        std.debug.print(
            "not ok - {s}: client failed (term={any})\nstdout:\n{s}\nstderr:\n{s}\n",
            .{ schema, result.term, result.stdout, result.stderr },
        );
        counts.fail += 1;
    }
    return counts;
}

fn runAll(allocator: std.mem.Allocator, io: std.Io, lane: *const Lane) !void {
    const transport: Transport = lane.transport;
    const lane_schemas: []const []const u8 = if (transport == .quic) &quic_schemas else &schemas;
    var total = TapCount{};
    for (lane_schemas) |schema| {
        const counts = try runSchema(allocator, io, lane, schema);
        total.pass += counts.pass;
        total.fail += counts.fail;
        std.debug.print("self-interop ({t}) {s}: {d} pass, {d} fail\n", .{ transport, schema, counts.pass, counts.fail });
    }

    std.debug.print("self-interop ({t}) total: {d} pass, {d} fail across {d} schemas\n", .{ transport, total.pass, total.fail, lane_schemas.len });
    if (total.fail != 0 or total.pass == 0) return error.SelfInteropFailed;
}

/// Run every schema over Unix sockets in a fresh private directory, removed
/// afterwards (the killed servers leave their socket and lock files).
fn runAllUnix(allocator: std.mem.Allocator, io: std.Io, server_bin: []const u8, client_bin: []const u8) !void {
    if (comptime !unix_supported) {
        std.debug.print("e2e-self --transport=unix: Unix-domain sockets are not compiled in: they need Linux or macOS, built with -Dfd-passing=true (the default)\n", .{});
        return error.UnixSocketsUnsupported;
    }
    // Short and private: `sun_path` is 104 bytes on macOS, and the 0700
    // directory is the layout `unix.listen` documents.
    var dir_buf: [64]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "/tmp/capnp-e2e-self-{d}", .{std.posix.system.getpid()});
    const cwd = std.Io.Dir.cwd();
    cwd.deleteTree(io, dir) catch {};
    try cwd.createDir(io, dir, .fromMode(0o700));
    defer cwd.deleteTree(io, dir) catch {};
    const lane: Lane = .{ .server_bin = server_bin, .client_bin = client_bin, .transport = .{ .unix = dir } };
    return runAll(allocator, io, &lane);
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = init.io;

    var iter = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer iter.deinit();
    _ = iter.skip();
    const server_bin = iter.next() orelse return error.MissingServerBinaryArg;
    const client_bin = iter.next() orelse return error.MissingClientBinaryArg;
    var transport: Transport = .tcp;
    var cert_pem: ?[]const u8 = null;
    var key_pem: ?[]const u8 = null;
    var ca_pem: ?[]const u8 = null;
    while (iter.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--transport=")) {
            transport = std.meta.stringToEnum(Transport, arg["--transport=".len..]) orelse return error.InvalidTransport;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--cert-pem=")) {
            cert_pem = arg["--cert-pem=".len..];
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--key-pem=")) {
            key_pem = arg["--key-pem=".len..];
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--ca-pem=")) {
            ca_pem = arg["--ca-pem=".len..];
            continue;
        }
        std.debug.print("unknown option: {s}\n", .{arg});
        return error.InvalidOption;
    }

    const any_pem = cert_pem != null or key_pem != null or ca_pem != null;
    if (transport != .quic and any_pem) {
        std.debug.print("e2e-self: --cert-pem, --key-pem and --ca-pem need --transport=quic\n", .{});
        return error.InvalidOption;
    }

    switch (transport) {
        .tcp => {
            const lane: Lane = .{ .server_bin = server_bin, .client_bin = client_bin, .transport = .tcp };
            try runAll(allocator, io, &lane);
        },
        .unix => try runAllUnix(allocator, io, server_bin, client_bin),
        .quic => {
            const lane: Lane = .{ .server_bin = server_bin, .client_bin = client_bin, .transport = .{ .quic = .{
                .cert_pem = cert_pem orelse return missingPem("--cert-pem"),
                .key_pem = key_pem orelse return missingPem("--key-pem"),
                .ca_pem = ca_pem orelse return missingPem("--ca-pem"),
            } } };
            try runAll(allocator, io, &lane);
        },
    }
}

fn missingPem(flag: []const u8) error{MissingPemArg} {
    std.debug.print("e2e-self --transport=quic needs {s}=F (and --cert-pem, --key-pem, --ca-pem together)\n", .{flag});
    return error.MissingPemArg;
}
