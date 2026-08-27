//! Runs the PUBLISHED framing conformance fixtures
//! (tests/fixtures/framing/framing_fixtures.json).
//!
//! The fixtures exist to be vendored: a downstream that reuses
//! `rpc.wire.framing.Framer` — or reimplements the length-prefixed frame
//! shape — can execute the same byte streams against its own code and get
//! the same verdicts. Running them here is what keeps them honest: a
//! behavior change breaks this suite before it can silently invalidate a
//! consumer's vendored copy.
//!
//! Format and semantics are documented inside the JSON itself.

const std = @import("std");
const capnpc = @import("capnpc-zig");

const Framer = capnpc.rpc.wire.framing.Framer;

const fixtures_json = @embedFile("framing-fixtures");

fn hexAlloc(allocator: std.mem.Allocator, hex: []const u8) ![]u8 {
    if (hex.len % 2 != 0) return error.InvalidHex;
    const out = try allocator.alloc(u8, hex.len / 2);
    errdefer allocator.free(out);
    _ = try std.fmt.hexToBytes(out, hex);
    return out;
}

test "published framing fixtures match the Framer's behavior" {
    const allocator = std.testing.allocator;

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, fixtures_json, .{});
    defer parsed.deinit();

    const root = parsed.value.object;

    // The fixtures publish the limits they were generated against; if the
    // implementation's constants move, every consumer's vendored copy is
    // stale and must be regenerated.
    const constants = root.get("constants").?.object;
    try std.testing.expectEqual(
        @as(i64, @intCast(Framer.max_frame_words)),
        constants.get("max_frame_words").?.integer,
    );
    try std.testing.expectEqual(
        @as(i64, @intCast(Framer.max_segment_count)),
        constants.get("max_segment_count").?.integer,
    );
    try std.testing.expectEqual(
        @as(i64, @intCast(Framer.max_header_bytes)),
        constants.get("max_header_bytes").?.integer,
    );
    try std.testing.expectEqual(
        @as(i64, @intCast(Framer.default_max_buffered_bytes)),
        constants.get("default_max_buffered_bytes").?.integer,
    );

    const cases = root.get("cases").?.array;
    try std.testing.expect(cases.items.len > 0);

    for (cases.items) |case_value| {
        const case = case_value.object;
        const name = case.get("name").?.string;
        errdefer std.debug.print("framing fixture failed: {s}\n", .{name});

        var framer = if (case.get("options")) |opts|
            Framer.initWithOptions(allocator, .{
                .max_buffered_bytes = @intCast(opts.object.get("max_buffered_bytes").?.integer),
            })
        else
            Framer.init(allocator);
        defer framer.deinit();

        const expect = case.get("expect").?.object;
        const want_error: ?[]const u8 = switch (expect.get("error").?) {
            .string => |s| s,
            else => null,
        };
        const error_on: ?[]const u8 = switch (expect.get("error_on").?) {
            .string => |s| s,
            else => null,
        };
        const want_frames = expect.get("frames").?.array;

        var popped: std.ArrayList([]u8) = .empty;
        defer {
            for (popped.items) |f| allocator.free(f);
            popped.deinit(allocator);
        }

        var saw_error: ?anyerror = null;
        var saw_error_on: []const u8 = "";

        for (case.get("chunks").?.array.items) |chunk_value| {
            const chunk = try hexAlloc(allocator, chunk_value.string);
            defer allocator.free(chunk);

            framer.push(chunk) catch |err| {
                saw_error = err;
                saw_error_on = "push";
                break;
            };

            while (true) {
                const maybe_frame = framer.popFrame() catch |err| {
                    saw_error = err;
                    saw_error_on = "pop";
                    break;
                };
                const frame = maybe_frame orelse break;
                try popped.append(allocator, frame);
            }
            if (saw_error != null) break;
        }

        if (want_error) |want| {
            const got = saw_error orelse {
                std.debug.print("fixture {s}: expected error {s}, got none\n", .{ name, want });
                return error.TestExpectedError;
            };
            try std.testing.expectEqualStrings(want, @errorName(got));
            if (error_on) |where| try std.testing.expectEqualStrings(where, saw_error_on);
        } else {
            if (saw_error) |got| {
                std.debug.print("fixture {s}: unexpected error {s}\n", .{ name, @errorName(got) });
                return error.TestUnexpectedResult;
            }
        }

        try std.testing.expectEqual(want_frames.items.len, popped.items.len);
        for (want_frames.items, popped.items) |want_value, got| {
            const want_bytes = try hexAlloc(allocator, want_value.string);
            defer allocator.free(want_bytes);
            try std.testing.expectEqualSlices(u8, want_bytes, got);
        }
    }
}
