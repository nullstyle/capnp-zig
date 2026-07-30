//! Cross-implementation Level-3 HOSTING lane: the vendored C++ reference plays
//! vats A (recipient) and B (introducer) over real TCP against a capnp-zig
//! two-peer VatC host.
//!
//! This is the INVERSE of `tools/e2e_l3_cpp.zig`, which puts capnp-zig in the
//! A and B roles against a C++ host. Until this lane existed, no reference
//! implementation had ever driven the client roles against a capnp-zig host,
//! so hosting conformance was argued from the vendored `rpc.c++` source rather
//! than observed — `tools/e2e_runner.zig` recorded that gap as a hard
//! `SKIP(l3-l4-zig-client-only)`.
//!
//! Per scenario this orchestrator:
//!   1. spawns `e2e-l3-vatc-host --port 0 --scenario <S>` and reads its
//!      `READY <port>` line (the host binds an ephemeral port and prints the
//!      resolved one, so parallel/CI runs never collide),
//!   2. runs the C++ driver container against `host.docker.internal:<port>`,
//!   3. requires BOTH sides to exit 0 with a clean TAP stream (the host
//!      asserts vat-internal state — provision registered, Accept served
//!      cross-peer, embargo queued/released, residue shape, leak-freedom —
//!      while the driver asserts the wire-visible behavior).
//!
//! Aggregate TAP goes to stdout: one `ok`/`not ok` per scenario per side.

const std = @import("std");

fn milliTimestamp(io: std.Io) i64 {
    return std.Io.Timestamp.now(io, .awake).toMilliseconds();
}

fn sleepMs(io: std.Io, ms: u64) void {
    std.Io.sleep(io, std.Io.Duration.fromNanoseconds(@intCast(ms * std.time.ns_per_ms)), .awake) catch {};
}

const scenarios = [_][]const u8{ "happy", "embargo", "unknown-token", "disconnect" };

/// Generous enough for a cold `docker compose run` (image already built);
/// the host binary has its own internal deadline well below this.
const case_timeout_ms: u64 = 90_000;
const ready_timeout_ms: u64 = 15_000;

const Tap = struct {
    io: std.Io,
    next: usize = 1,
    failures: usize = 0,

    fn ok(self: *Tap, comptime fmt: []const u8, args: anytype) void {
        self.line(true, fmt, args);
    }

    fn notOk(self: *Tap, comptime fmt: []const u8, args: anytype) void {
        self.failures += 1;
        self.line(false, fmt, args);
    }

    fn expect(self: *Tap, cond: bool, comptime fmt: []const u8, args: anytype) void {
        self.line(cond, fmt, args);
        if (!cond) self.failures += 1;
    }

    fn line(self: *Tap, good: bool, comptime fmt: []const u8, args: anytype) void {
        var buf: [512]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, fmt, args) catch "<message too long>";
        self.print("{s} {d} - {s}\n", .{ if (good) "ok" else "not ok", self.next, body });
        self.next += 1;
    }

    fn diag(self: *Tap, comptime fmt: []const u8, args: anytype) void {
        var buf: [512]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, fmt, args) catch "<message too long>";
        self.print("# {s}\n", .{body});
    }

    fn plan(self: *Tap) void {
        self.print("1..{d}\n", .{self.next - 1});
    }

    fn print(self: *Tap, comptime fmt: []const u8, args: anytype) void {
        var buf: [1024]u8 = undefined;
        const text = std.fmt.bufPrint(&buf, fmt, args) catch return;
        var stdout = std.Io.File.stdout().writer(self.io, &.{});
        stdout.interface.writeAll(text) catch {};
    }
};

/// Spawn the host with its stdout pointed at a per-scenario file, then poll
/// that file for the `READY <port>` handshake. The host keeps running (it
/// exits on its own once both driver connections close); the same file holds
/// its TAP afterwards.
const StartedHost = struct {
    child: std.process.Child,
    port: u16,
    tap_path: []u8,
};

fn startHost(
    allocator: std.mem.Allocator,
    io: std.Io,
    host_bin: []const u8,
    scenario: []const u8,
) !StartedHost {
    std.Io.Dir.cwd().createDirPath(io, ".results") catch {};
    const tap_path = try std.fmt.allocPrint(allocator, ".results/l3_vatc_host_{s}.tap", .{scenario});
    errdefer allocator.free(tap_path);

    const tap_file = try std.Io.Dir.cwd().createFile(io, tap_path, .{});
    defer tap_file.close(io);

    var child = try std.process.spawn(io, .{
        .argv = &.{ host_bin, "--port", "0", "--scenario", scenario },
        .stdin = .ignore,
        .stdout = .{ .file = tap_file },
        .stderr = .ignore,
    });
    errdefer child.kill(io);

    const deadline = milliTimestamp(io) + @as(i64, @intCast(ready_timeout_ms));
    while (milliTimestamp(io) < deadline) {
        if (readFile(allocator, io, tap_path)) |text| {
            defer allocator.free(text);
            if (std.mem.indexOf(u8, text, "READY ")) |idx| {
                const rest = text[idx + 6 ..];
                if (std.mem.indexOfAny(u8, rest, "\r\n")) |end_idx| {
                    if (std.fmt.parseInt(u16, rest[0..end_idx], 10)) |port| {
                        return .{ .child = child, .port = port, .tap_path = tap_path };
                    } else |_| {}
                }
            }
        } else |_| {}
        sleepMs(io, 20);
    }
    return error.HostNeverReady;
}

fn readFile(allocator: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
    const file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader = file.reader(io, &.{});
    return reader.interface.allocRemaining(allocator, .limited(1 << 20));
}

const RunResult = struct {
    exit_code: i32,
    stdout: []u8,
    stderr: []u8,
};

fn termExitCode(term: std.process.Child.Term) i32 {
    return switch (term) {
        .exited => |code| @intCast(code),
        else => -1,
    };
}

/// Run the C++ driver container against the host's ephemeral port. The docker
/// recipe (tests/e2e/cpp/Justfile docker-l3-vatc-client) reaches the host via
/// host.docker.internal, matching how the runner's reference clients dial a
/// Zig server.
fn runDriver(
    allocator: std.mem.Allocator,
    io: std.Io,
    port: u16,
    scenario: []const u8,
) !RunResult {
    var port_buf: [16]u8 = undefined;
    const port_text = try std.fmt.bufPrint(&port_buf, "{d}", .{port});
    const result = try std.process.run(allocator, io, .{
        .argv = &.{
            "just",
            "--justfile",
            "tests/e2e/cpp/Justfile",
            "docker-l3-vatc-client",
            "host.docker.internal",
            port_text,
            scenario,
            "60",
        },
    });
    return .{
        .exit_code = termExitCode(result.term),
        .stdout = result.stdout,
        .stderr = result.stderr,
    };
}

fn tapIsClean(text: []const u8) bool {
    var it = std.mem.splitScalar(u8, text, '\n');
    var saw_ok = false;
    while (it.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \r\t");
        if (std.mem.startsWith(u8, line, "not ok")) return false;
        if (std.mem.startsWith(u8, line, "Bail out!")) return false;
        if (std.mem.startsWith(u8, line, "ok ")) saw_ok = true;
    }
    return saw_ok;
}

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const allocator = gpa.allocator();
    const io = init.io;
    var tap = Tap{ .io = io };

    const host_bin = "./zig-out/bin/e2e-l3-vatc-host";
    if (!fileExists(io, host_bin)) {
        tap.notOk("host binary present at {s} (run: zig build e2e-l3-vatc-host-install)", .{host_bin});
        tap.plan();
        return error.TestFailed;
    }

    for (scenarios) |scenario| {
        tap.diag("scenario {s}", .{scenario});

        var started = startHost(allocator, io, host_bin, scenario) catch |err| {
            tap.notOk("[{s}] zig VatC host became ready", .{scenario});
            tap.diag("host start failed: {t}", .{err});
            continue;
        };
        defer allocator.free(started.tap_path);
        tap.ok("[{s}] zig VatC host became ready on port {d}", .{ scenario, started.port });

        const driver = runDriver(allocator, io, started.port, scenario) catch |err| {
            tap.notOk("[{s}] C++ A+B driver ran", .{scenario});
            tap.diag("driver spawn failed: {t}", .{err});
            started.child.kill(io);
            continue;
        };
        defer allocator.free(driver.stdout);
        defer allocator.free(driver.stderr);

        const driver_clean = driver.exit_code == 0 and tapIsClean(driver.stdout);
        tap.expect(driver_clean, "[{s}] C++ recipient+introducer driver passed", .{scenario});
        if (!driver_clean) {
            tap.diag("driver stdout:\n{s}", .{driver.stdout});
            tap.diag("driver stderr:\n{s}", .{driver.stderr});
        }

        // The host exits on its own once both driver connections close.
        const term = started.child.wait(io) catch std.process.Child.Term{ .unknown = 0 };
        const host_tap = readFile(allocator, io, started.tap_path) catch try allocator.dupe(u8, "");
        defer allocator.free(host_tap);
        const host_clean = termExitCode(term) == 0 and tapIsClean(host_tap);
        tap.expect(host_clean, "[{s}] zig VatC host assertions passed", .{scenario});
        if (!host_clean) tap.diag("host tap:\n{s}", .{host_tap});
    }

    tap.plan();
    if (tap.failures > 0) return error.TestFailed;
}

fn fileExists(io: std.Io, path: []const u8) bool {
    const file = std.Io.Dir.cwd().openFile(io, path, .{}) catch return false;
    file.close(io);
    return true;
}
