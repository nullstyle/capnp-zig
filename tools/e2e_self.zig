//! Self-interop e2e: drive the Zig e2e client against the Zig e2e server
//! for every schema, with TAP accounting.
//!
//! This is the no-docker complement to tools/e2e_runner.zig: it needs no
//! reference-implementation containers, builds on `std.Io` only, and runs
//! on every platform — including Windows CI runners, which cannot run the
//! Linux reference containers. It is the end-to-end exercise of the
//! platform's real socket stack (listener, accept, framing, writer
//! thread, peer dispatch).
//!
//! Two transports:
//! - TCP over loopback (the default; `zig build e2e-self`, every OS).
//! - `--transport=unix`: an AF_UNIX socket file in a private (0700)
//!   directory under /tmp, through `--host unix:/path` on both binaries
//!   (`rpc.transport.unix.listen` and `.connect`; `zig build e2e-self-unix`).
//!   Linux and macOS only, like `rpc.transport.unix`. On any other target
//!   it fails with `error.UnixSocketsUnsupported` rather than passing
//!   having run nothing.
//!
//! Usage: e2e-self <server-binary> <client-binary> [--transport=tcp|unix]
//! (The build system passes both artifact paths; see `zig build e2e-self`.)

const std = @import("std");
const builtin = @import("builtin");

const schemas = [_][]const u8{ "game_world", "chat", "inventory", "matchmaking" };
const server_ready_timeout_ms: i64 = 30_000;
const client_timeout_ms: i64 = 60_000;

const Transport = enum { tcp, unix };

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

fn runSchema(
    allocator: std.mem.Allocator,
    io: std.Io,
    server_bin: []const u8,
    client_bin: []const u8,
    schema: []const u8,
    transport: Transport,
    unix_dir: []const u8,
) !TapCount {
    var port_buf: [8]u8 = undefined;
    var host_buf: [128]u8 = undefined;
    const endpoint: Endpoint = switch (transport) {
        .tcp => blk: {
            const port = try findFreePort(io);
            break :blk .{
                .host = "127.0.0.1",
                .port = port,
                .port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{port}),
            };
        },
        // One socket file per schema in the private directory.
        .unix => .{
            .host = try std.fmt.bufPrint(&host_buf, unix_host_prefix ++ "{s}/{s}.sock", .{ unix_dir, schema }),
            .port = null,
            .port_text = "",
        },
    };

    var server_argv_buf: [7][]const u8 = undefined;
    var server = try std.process.spawn(io, .{
        .argv = endpoint.argv(&server_argv_buf, server_bin, schema),
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
    defer server.kill(io);

    if (!endpoint.waitReady(io)) {
        std.debug.print("not ok - {s}: server did not become ready on {s}\n", .{ schema, endpoint.host });
        return .{ .fail = 1 };
    }

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
    var client_argv_buf: [7][]const u8 = undefined;
    const result = std.process.run(allocator, io, .{
        .argv = endpoint.argv(&client_argv_buf, client_bin, schema),
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

fn runAll(
    allocator: std.mem.Allocator,
    io: std.Io,
    server_bin: []const u8,
    client_bin: []const u8,
    transport: Transport,
    unix_dir: []const u8,
) !void {
    var total = TapCount{};
    for (schemas) |schema| {
        const counts = try runSchema(allocator, io, server_bin, client_bin, schema, transport, unix_dir);
        total.pass += counts.pass;
        total.fail += counts.fail;
        std.debug.print("self-interop ({t}) {s}: {d} pass, {d} fail\n", .{ transport, schema, counts.pass, counts.fail });
    }

    std.debug.print("self-interop ({t}) total: {d} pass, {d} fail across {d} schemas\n", .{ transport, total.pass, total.fail, schemas.len });
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
    return runAll(allocator, io, server_bin, client_bin, .unix, dir);
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
    while (iter.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--transport=")) {
            transport = std.meta.stringToEnum(Transport, arg["--transport=".len..]) orelse return error.InvalidTransport;
            continue;
        }
        std.debug.print("unknown option: {s}\n", .{arg});
        return error.InvalidOption;
    }

    switch (transport) {
        .tcp => try runAll(allocator, io, server_bin, client_bin, .tcp, ""),
        .unix => try runAllUnix(allocator, io, server_bin, client_bin),
    }
}
