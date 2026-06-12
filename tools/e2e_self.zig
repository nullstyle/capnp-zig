//! Self-interop e2e: drive the Zig e2e client against the Zig e2e server
//! over real loopback TCP for every schema, with TAP accounting.
//!
//! This is the no-docker complement to tools/e2e_runner.zig: it needs no
//! reference-implementation containers, builds on `std.Io` only, and runs
//! on every platform — including Windows CI runners, which cannot run the
//! Linux reference containers. It is the end-to-end exercise of the
//! platform's real socket stack (listener, accept, framing, writer
//! thread, peer dispatch).
//!
//! Usage: e2e-self <server-binary> <client-binary>
//! (The build system passes both artifact paths; see `zig build e2e-self`.)

const std = @import("std");

const schemas = [_][]const u8{ "game_world", "chat", "inventory", "matchmaking" };
const server_ready_timeout_ms: i64 = 30_000;
const client_timeout_ms: i64 = 60_000;

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

fn runSchema(
    allocator: std.mem.Allocator,
    io: std.Io,
    server_bin: []const u8,
    client_bin: []const u8,
    schema: []const u8,
) !TapCount {
    const port = try findFreePort(io);
    var port_buf: [8]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var server = try std.process.spawn(io, .{
        .argv = &.{ server_bin, "--host", "127.0.0.1", "--port", port_text, "--schema", schema },
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
    defer server.kill(io);

    if (!waitForServer(io, port)) {
        std.debug.print("not ok - {s}: server did not become ready\n", .{schema});
        return .{ .fail = 1 };
    }

    const result = try std.process.run(allocator, io, .{
        .argv = &.{ client_bin, "--host", "127.0.0.1", "--port", port_text, "--schema", schema },
        .stdout_limit = .limited(1024 * 1024),
        .stderr_limit = .limited(1024 * 1024),
    });
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

    var total = TapCount{};
    for (schemas) |schema| {
        const counts = try runSchema(allocator, io, server_bin, client_bin, schema);
        total.pass += counts.pass;
        total.fail += counts.fail;
        std.debug.print("self-interop {s}: {d} pass, {d} fail\n", .{ schema, counts.pass, counts.fail });
    }

    std.debug.print("self-interop total: {d} pass, {d} fail across {d} schemas\n", .{ total.pass, total.fail, schemas.len });
    if (total.fail != 0 or total.pass == 0) return error.SelfInteropFailed;
}
