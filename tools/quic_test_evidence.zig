const std = @import("std");

const max_source_bytes = 8 * 1024 * 1024;

const Root = struct {
    path: []const u8,
    minimum_tests: usize,
};

const roots = [_]Root{
    .{ .path = "tests/rpc/transport/quic/rpc_quic_transport_test.zig", .minimum_tests = 26 },
    .{ .path = "tests/rpc/transport/quic/rpc_quic_public_api_test.zig", .minimum_tests = 1 },
    .{ .path = "tests/rpc/transport/quic/rpc_quic_connection_internal_test.zig", .minimum_tests = 17 },
    .{ .path = "tests/rpc/transport/quic/rpc_quic_peer_test.zig", .minimum_tests = 8 },
};

fn countTests(bytes: []const u8) usize {
    var count: usize = 0;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trimStart(u8, line, " \t");
        if (std.mem.startsWith(u8, trimmed, "test \"")) count += 1;
    }
    return count;
}

fn readSource(init: std.process.Init, path: []const u8) ![]u8 {
    return std.Io.Dir.cwd().readFileAlloc(
        init.io,
        path,
        init.gpa,
        .limited(max_source_bytes),
    );
}

fn rejectSkipTokens(init: std.process.Init) !void {
    const directory = "tests/rpc/transport/quic";
    var dir = try std.Io.Dir.cwd().openDir(init.io, directory, .{ .iterate = true });
    defer dir.close(init.io);

    var walker = try dir.walk(init.gpa);
    defer walker.deinit();
    while (try walker.next(init.io)) |entry| {
        if (entry.kind != .file or !std.mem.endsWith(u8, entry.path, ".zig")) continue;
        const path = try std.fs.path.join(init.gpa, &.{ directory, entry.path });
        defer init.gpa.free(path);
        const bytes = try readSource(init, path);
        defer init.gpa.free(bytes);
        if (std.mem.indexOf(u8, bytes, "SkipZigTest") != null) {
            std.debug.print("QUIC evidence forbids SkipZigTest: {s}\n", .{path});
            return error.QuicTestSkipPresent;
        }
    }
}

fn enforceWindowsReceiveBackend(init: std.process.Init) !void {
    const directories = [_][]const u8{
        "src/rpc/transport/quic",
        "tests/rpc/transport/quic",
    };
    const approved_directory = "src/rpc/transport/quic";
    const approved_file = "non_windows_receive.zig";
    for (directories) |directory| {
        var dir = try std.Io.Dir.cwd().openDir(init.io, directory, .{ .iterate = true });
        defer dir.close(init.io);
        var walker = try dir.walk(init.gpa);
        defer walker.deinit();
        while (try walker.next(init.io)) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.path, ".zig")) continue;
            const path = try std.fs.path.join(init.gpa, &.{ directory, entry.path });
            defer init.gpa.free(path);
            const bytes = try readSource(init, path);
            defer init.gpa.free(bytes);
            const approved = std.mem.eql(u8, directory, approved_directory) and
                std.mem.eql(u8, entry.path, approved_file);
            var timed_socket_calls: usize = 0;
            var search_offset: usize = 0;
            while (std.mem.indexOfPos(u8, bytes, search_offset, ".receiveTimeout")) |call_start| {
                var cursor = call_start + ".receiveTimeout".len;
                search_offset = cursor;
                while (cursor < bytes.len and std.ascii.isWhitespace(bytes[cursor])) cursor += 1;
                if (cursor == bytes.len or bytes[cursor] != '(') continue;
                cursor += 1;
                while (cursor < bytes.len and std.ascii.isWhitespace(bytes[cursor])) cursor += 1;
                // QUIC engine deadline accessors take no arguments. A socket
                // timed receive necessarily has arguments and must live only
                // in the compile-time-guarded helper, regardless of the
                // receiver variable's spelling (`socket`, `self.socket`, ...).
                if (cursor < bytes.len and bytes[cursor] == ')') continue;
                timed_socket_calls += 1;
                if (!approved) {
                    std.debug.print("QUIC socket receiveTimeout must stay behind the non-Windows compile guard: {s}\n", .{path});
                    return error.WindowsQuicTimedReceiveReachable;
                }
            }
            if (!approved or timed_socket_calls == 0) continue;
            if (timed_socket_calls != 1 or
                std.mem.indexOf(u8, bytes, "@compileError(\"Windows QUIC UDP receives must use udp_receive_bridge.Bridge\")") == null)
            {
                return error.WindowsQuicTimedReceiveGuardMissing;
            }
        }
    }
}

pub fn main(init: std.process.Init) !void {
    comptime {
        if (roots.len != 4) @compileError("QUIC evidence must register exactly four test roots");
    }

    var total: usize = 0;
    for (roots) |root| {
        const bytes = try readSource(init, root.path);
        defer init.gpa.free(bytes);
        const count = countTests(bytes);
        if (count < root.minimum_tests) {
            std.debug.print(
                "QUIC evidence inventory fell for {s}: found {d}, require at least {d}\n",
                .{ root.path, count, root.minimum_tests },
            );
            return error.QuicTestInventoryTooSmall;
        }
        total += count;
    }
    if (total < 52) return error.QuicTestInventoryTooSmall;
    try rejectSkipTokens(init);
    try enforceWindowsReceiveBackend(init);
    std.debug.print("QUIC evidence inventory: {d} tests across {d} runnable roots, zero skips\n", .{ total, roots.len });
}
