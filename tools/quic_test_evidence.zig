const std = @import("std");

const max_source_bytes = 8 * 1024 * 1024;

const Root = struct {
    path: []const u8,
    minimum_tests: usize,
};

// Each floor is the count of `test "` lines in its root when the floor was
// last raised. When you add tests to a root, raise its floor, then put the
// sentence that this tool prints on a mismatch in `floors_doc`.
const roots = [_]Root{
    .{ .path = "tests/rpc/transport/quic/rpc_quic_transport_test.zig", .minimum_tests = 116 },
    .{ .path = "tests/rpc/transport/quic/rpc_quic_public_api_test.zig", .minimum_tests = 1 },
    .{ .path = "tests/rpc/transport/quic/rpc_quic_connection_internal_test.zig", .minimum_tests = 45 },
    .{ .path = "tests/rpc/transport/quic/rpc_quic_peer_test.zig", .minimum_tests = 50 },
};

/// The doc that states the floors. Every statement of them in it must equal
/// `floors_sentence`, so the doc cannot drift from `roots`.
const floors_doc = "docs/stability.md";
const floors_prefix = "per-root floors ";

/// "per-root floors A + B + C + D = SUM" for `entries`.
fn renderFloorsSentence(comptime entries: []const Root) []const u8 {
    comptime {
        var text: []const u8 = floors_prefix;
        var sum: usize = 0;
        for (entries, 0..) |root, index| {
            if (index != 0) text = text ++ " + ";
            text = text ++ std.fmt.comptimePrint("{d}", .{root.minimum_tests});
            sum += root.minimum_tests;
        }
        return text ++ std.fmt.comptimePrint(" = {d}", .{sum});
    }
}

const floors_sentence = renderFloorsSentence(&roots);

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

/// Copies `bytes` with each run of whitespace (line breaks included) made
/// one space, so a sentence that Markdown wraps still matches.
fn collapseWhitespace(gpa: std.mem.Allocator, bytes: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    try out.ensureTotalCapacity(gpa, bytes.len);
    var in_space = false;
    for (bytes) |byte| {
        if (std.ascii.isWhitespace(byte)) {
            if (!in_space) out.appendAssumeCapacity(' ');
            in_space = true;
        } else {
            out.appendAssumeCapacity(byte);
            in_space = false;
        }
    }
    return out.toOwnedSlice(gpa);
}

const FloorsStatement = enum { matches, missing, stale };

/// `matches` when `text` states the floors at least once and every
/// statement equals `sentence`. Letter case is ignored, so a statement that
/// starts a sentence ("Per-root floors ...") is checked too.
fn checkFloorsStatement(text: []const u8, sentence: []const u8) FloorsStatement {
    var found = false;
    var offset: usize = 0;
    while (std.ascii.findIgnoreCasePos(text, offset, floors_prefix)) |start| {
        if (!std.ascii.startsWithIgnoreCase(text[start..], sentence)) return .stale;
        // "= 212" must not be the head of "= 2120".
        const end = start + sentence.len;
        if (end < text.len and std.ascii.isDigit(text[end])) return .stale;
        found = true;
        offset = end;
    }
    return if (found) .matches else .missing;
}

fn enforceFloorsDoc(init: std.process.Init) !void {
    const bytes = try readSource(init, floors_doc);
    defer init.gpa.free(bytes);
    const text = try collapseWhitespace(init.gpa, bytes);
    defer init.gpa.free(text);
    switch (checkFloorsStatement(text, floors_sentence)) {
        .matches => {},
        .missing, .stale => |verdict| {
            std.debug.print(
                "{s} must state the QUIC floors as \"{s}\" ({s})\n",
                .{ floors_doc, floors_sentence, @tagName(verdict) },
            );
            return error.QuicFloorsDocDrift;
        },
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
        std.debug.print("QUIC evidence root {s}: {d} tests (floor {d})\n", .{ root.path, count, root.minimum_tests });
        if (count < root.minimum_tests) {
            std.debug.print(
                "QUIC evidence inventory fell for {s}: found {d}, require at least {d}\n",
                .{ root.path, count, root.minimum_tests },
            );
            return error.QuicTestInventoryTooSmall;
        }
        total += count;
    }
    try enforceFloorsDoc(init);
    try rejectSkipTokens(init);
    try enforceWindowsReceiveBackend(init);
    std.debug.print("QUIC evidence inventory: {d} tests across {d} runnable roots, zero skips\n", .{ total, roots.len });
}

test "renderFloorsSentence names every floor and their sum" {
    const entries = [_]Root{
        .{ .path = "a", .minimum_tests = 116 },
        .{ .path = "b", .minimum_tests = 1 },
        .{ .path = "c", .minimum_tests = 45 },
    };
    try std.testing.expectEqualStrings("per-root floors 116 + 1 + 45 = 162", comptime renderFloorsSentence(&entries));
}

test "checkFloorsStatement accepts only statements equal to the sentence" {
    const sentence = "per-root floors 1 + 2 = 3";
    try std.testing.expectEqual(FloorsStatement.matches, checkFloorsStatement("enforces per-root floors 1 + 2 = 3, and", sentence));
    try std.testing.expectEqual(FloorsStatement.matches, checkFloorsStatement("a per-root floors 1 + 2 = 3. b per-root floors 1 + 2 = 3", sentence));
    try std.testing.expectEqual(FloorsStatement.missing, checkFloorsStatement("no statement here", sentence));
    try std.testing.expectEqual(FloorsStatement.stale, checkFloorsStatement("per-root floors 1 + 2 = 4", sentence));
    try std.testing.expectEqual(FloorsStatement.stale, checkFloorsStatement("per-root floors 1 + 2 = 30", sentence));
    try std.testing.expectEqual(FloorsStatement.stale, checkFloorsStatement("per-root floors 1 + 2 = 3 and per-root floors 0 + 2 = 2", sentence));
}

test "checkFloorsStatement finds a statement in any letter case" {
    const sentence = "per-root floors 1 + 2 = 3";
    try std.testing.expectEqual(FloorsStatement.matches, checkFloorsStatement("Per-root floors 1 + 2 = 3.", sentence));
    try std.testing.expectEqual(FloorsStatement.stale, checkFloorsStatement("per-root floors 1 + 2 = 3. Per-root floors 0 + 2 = 2.", sentence));
    try std.testing.expectEqual(FloorsStatement.stale, checkFloorsStatement("PER-ROOT FLOORS 1 + 2 = 4", sentence));
}

test "collapseWhitespace joins a sentence that Markdown wraps" {
    const gpa = std.testing.allocator;
    const text = try collapseWhitespace(gpa, "per-root\r\n  floors 1 +\n2 = 3");
    defer gpa.free(text);
    try std.testing.expectEqualStrings("per-root floors 1 + 2 = 3", text);
}
