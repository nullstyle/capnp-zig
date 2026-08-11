//! Tests for `canonical` — the spec-faithful, schema-FREE canonicalizer.
//!
//! Two families:
//!
//! 1. Differential tests against the reference implementation's CLI
//!    (`capnp convert binary:canonical`). These skip with `error.SkipZigTest`
//!    when `capnp` is not installed, matching CI's per-OS install policy.
//!    Per vendor/ext/capnproto/c++/src/capnp/compiler/capnp.c++:147 the
//!    `canonical` format is "canonicalized binary single segment, no segment
//!    table", so the CLI bytes are compared against our framed output with
//!    the 8-byte single-segment table prepended.
//!
//! 2. Ports of the reference acceptance suite
//!    (vendor/ext/capnproto/c++/src/capnp/canonicalize-test.c++), which need
//!    no CLI: the fixture bytes and expected bytes come straight from the
//!    C++ test file, cited per case.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const compare = @import("support/capnp_compare.zig");
const capnp_cli = @import("support/capnp_cli.zig");

const message = capnpc.message;
const canonical = capnpc.canonical;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Run `capnp convert <conversion>` for a type in tests/capnp_testdata/test.capnp,
/// feeding `input` on stdin. Skips the test when the CLI is absent. Mirrors the
/// harness in tests/serialization/schema_validation_test.zig.
fn capnpConvert(
    allocator: std.mem.Allocator,
    input: []const u8,
    conversion: []const u8,
    type_name: []const u8,
    extra_args: []const []const u8,
) ![]u8 {
    const io = std.testing.io;
    var argv_buf: [16][]const u8 = undefined;
    const base = [_][]const u8{
        "convert",
        conversion,
        "--no-standard-import",
        "-Itests/capnp_testdata",
    };
    var argc: usize = 0;
    for (base) |a| {
        argv_buf[argc] = a;
        argc += 1;
    }
    for (extra_args) |a| {
        argv_buf[argc] = a;
        argc += 1;
    }
    argv_buf[argc] = "tests/capnp_testdata/test.capnp";
    argc += 1;
    argv_buf[argc] = type_name;
    argc += 1;

    var child = try capnp_cli.spawn(allocator, io, argv_buf[0..argc], .{
        .stdin = .pipe,
        .stdout = .pipe,
        .stderr = .pipe,
    });

    try child.stdin.?.writeStreamingAll(io, input);
    child.stdin.?.close(io);
    child.stdin = null;

    var stdout_buf: [4096]u8 = undefined;
    var stdout_rdr = child.stdout.?.reader(io, &stdout_buf);
    const stdout_bytes = try stdout_rdr.interface.allocRemaining(allocator, .unlimited);

    var stderr_buf: [4096]u8 = undefined;
    var stderr_rdr = child.stderr.?.reader(io, &stderr_buf);
    const stderr_bytes = try stderr_rdr.interface.allocRemaining(allocator, .unlimited);

    const term = try child.wait(io);
    switch (term) {
        .exited => |code| {
            if (code != 0) {
                std.debug.print("capnp convert failed: {s}\n", .{stderr_bytes});
                allocator.free(stdout_bytes);
                allocator.free(stderr_bytes);
                return error.CapnpConvertFailed;
            }
        },
        else => {
            std.debug.print("capnp convert failed: unexpected termination\n", .{});
            allocator.free(stdout_bytes);
            allocator.free(stderr_bytes);
            return error.CapnpConvertFailed;
        },
    }

    allocator.free(stderr_bytes);
    return stdout_bytes;
}

/// Prepend the framed segment table for a SINGLE segment: u32 segment count
/// minus one (0), u32 segment size in words. One segment means an odd header
/// word count, so no padding word (matches Message.initUnvalidated).
fn frameSingleSegment(allocator: std.mem.Allocator, segment: []const u8) ![]u8 {
    std.debug.assert(segment.len % 8 == 0);
    const out = try allocator.alloc(u8, 8 + segment.len);
    std.mem.writeInt(u32, out[0..4], 0, .little);
    std.mem.writeInt(u32, out[4..8], @intCast(segment.len / 8), .little);
    @memcpy(out[8..], segment);
    return out;
}

/// Frame two raw segments (count-1 = 1, two sizes, plus the padding word an
/// even segment count requires).
fn frameTwoSegments(allocator: std.mem.Allocator, seg0: []const u8, seg1: []const u8) ![]u8 {
    std.debug.assert(seg0.len % 8 == 0 and seg1.len % 8 == 0);
    const out = try allocator.alloc(u8, 16 + seg0.len + seg1.len);
    std.mem.writeInt(u32, out[0..4], 1, .little);
    std.mem.writeInt(u32, out[4..8], @intCast(seg0.len / 8), .little);
    std.mem.writeInt(u32, out[8..12], @intCast(seg1.len / 8), .little);
    std.mem.writeInt(u32, out[12..16], 0, .little); // padding word
    @memcpy(out[16..][0..seg0.len], seg0);
    @memcpy(out[16 + seg0.len ..], seg1);
    return out;
}

/// Differential core: take framed binary input, canonicalize it with our
/// implementation, and compare byte-for-byte against the reference CLI.
fn expectMatchesCli(allocator: std.mem.Allocator, framed_input: []const u8, type_name: []const u8) !void {
    const cli_canonical = try capnpConvert(allocator, framed_input, "binary:canonical", type_name, &.{});
    defer allocator.free(cli_canonical);

    const expected = try frameSingleSegment(allocator, cli_canonical);
    defer allocator.free(expected);

    var msg = try message.Message.init(allocator, framed_input, .{});
    defer msg.deinit();

    const ours = try canonical.canonicalize(allocator, &msg);
    defer allocator.free(ours);

    try std.testing.expectEqualSlices(u8, expected, ours);

    // The canonical form must satisfy our own reader-side predicate too.
    var out_msg = try message.Message.init(allocator, ours, .{});
    defer out_msg.deinit();
    try std.testing.expect(canonical.isCanonical(&out_msg));
}

/// Differential from capnp text format: text -> framed binary via the CLI
/// (optionally forcing small fixed-size segments), then `expectMatchesCli`.
fn expectTextCaseMatchesCli(
    allocator: std.mem.Allocator,
    text: []const u8,
    type_name: []const u8,
    binary_extra_args: []const []const u8,
) !void {
    const framed = try capnpConvert(allocator, text, "text:binary", type_name, binary_extra_args);
    defer allocator.free(framed);
    try expectMatchesCli(allocator, framed, type_name);
}

// ---------------------------------------------------------------------------
// Differential tests vs the reference CLI
// ---------------------------------------------------------------------------

test "differential: standard TestAllTypes fixture matches capnp binary:canonical" {
    const allocator = std.testing.allocator;
    const bytes = try compare.readFileAlloc(allocator, "tests/capnp_testdata/testdata/binary");
    defer allocator.free(bytes);
    try expectMatchesCli(allocator, bytes, "TestAllTypes");
}

test "differential: multi-segment message with far pointers collapses to one segment" {
    const allocator = std.testing.allocator;

    // --segment-size forces FIXED_SIZE allocation, so the builder spills into
    // additional segments joined by far pointers.
    const framed = try capnpConvert(
        allocator,
        "(textField = \"the quick brown fox jumps over the lazy dog\", int32Field = 77, structField = (uInt8Field = 9), boolList = [true, false, true])",
        "text:binary",
        "TestAllTypes",
        &.{ "--segment-size", "4" },
    );
    defer allocator.free(framed);

    // The fixture must actually be multi-segment or it proves nothing.
    var in_msg = try message.Message.init(allocator, framed, .{});
    defer in_msg.deinit();
    try std.testing.expect(in_msg.segments.len > 1);
    try std.testing.expect(!canonical.isCanonical(&in_msg));

    try expectMatchesCli(allocator, framed, "TestAllTypes");
}

test "differential: struct with trailing zero data words is truncated" {
    const allocator = std.testing.allocator;
    // Only int32Field set: the builder emits the full declared data section
    // (6 words), everything after word 0 zero, so canonicalization must drop
    // the trailing zero words AND the all-null pointer section.
    try expectTextCaseMatchesCli(allocator, "(int32Field = 1078)", "TestAllTypes", &.{});
}

test "differential: heterogeneous struct list gets one uniform truncated element size" {
    const allocator = std.testing.allocator;
    // Three TestAllTypes elements needing different amounts of their data and
    // pointer sections; the canonical inline-composite tag must use the MAX
    // truncated size across elements, uniformly.
    try expectTextCaseMatchesCli(
        allocator,
        "(structList = [(uInt8Field = 1), (int64Field = 99), (textField = \"deep\")])",
        "TestAllTypes",
        &.{},
    );
}

test "differential: text and data lists" {
    const allocator = std.testing.allocator;
    try expectTextCaseMatchesCli(
        allocator,
        "(textField = \"hi\", dataField = \"rawbytes\", textList = [\"plugh\", \"xyzzy\", \"thud\"], dataList = [\"oops\", \"exhausted\"])",
        "TestAllTypes",
        &.{},
    );
}

test "differential: bit list" {
    const allocator = std.testing.allocator;
    try expectTextCaseMatchesCli(
        allocator,
        "(boolList = [true, false, false, true, true, false, true, true, true, false, false])",
        "TestAllTypes",
        &.{},
    );
}

test "differential: nested List(List(Int32))" {
    const allocator = std.testing.allocator;
    try expectTextCaseMatchesCli(
        allocator,
        "(int32ListList = [[1, 2, 3], [4], [], [2147483647, -2147483648]])",
        "TestLists",
        &.{},
    );
}

test "differential: interior null pointer stays null, empty struct child kept non-null" {
    const allocator = std.testing.allocator;
    // textField (pointer 0) stays null while dataField (pointer 1) and
    // structField (pointer 2, an all-default = zero-sized struct) are set.
    try expectTextCaseMatchesCli(
        allocator,
        "(dataField = \"x\", structField = ())",
        "TestAllTypes",
        &.{},
    );
}

test "differential: all-default root canonicalizes to a lone empty-struct pointer" {
    const allocator = std.testing.allocator;
    const framed = try capnpConvert(allocator, "()", "text:binary", "TestAllTypes", &.{});
    defer allocator.free(framed);

    try expectMatchesCli(allocator, framed, "TestAllTypes");

    // And the canonical form of "nothing" is pinned: one word, an offset -1
    // zero-sized struct pointer (verified against the CLI above; asserted
    // here so the shape is documented in-tree).
    var msg = try message.Message.init(allocator, framed, .{});
    defer msg.deinit();
    const ours = try canonical.canonicalize(allocator, &msg);
    defer allocator.free(ours);
    const expected_word = [8]u8{ 0xfc, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00 };
    try std.testing.expectEqual(@as(usize, 16), ours.len);
    try std.testing.expectEqualSlices(u8, &expected_word, ours[8..]);
}

// ---------------------------------------------------------------------------
// Ports of canonicalize-test.c++ (no CLI needed)
// ---------------------------------------------------------------------------

// canonicalize-test.c++:57 "data word with only its most significant bit set
// does not get truncated": already canonical; canonicalize must reproduce it
// byte-for-byte (the historic off-by-one truncated the second data word).
test "reference port: data word with only its MSB set is not truncated" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
    };
    const framed = try frameSingleSegment(allocator, &segment);
    defer allocator.free(framed);

    var msg = try message.Message.init(allocator, framed, .{});
    defer msg.deinit();
    try std.testing.expect(canonical.isCanonical(&msg));

    const ours = try canonical.canonicalize(allocator, &msg);
    defer allocator.free(ours);
    try std.testing.expectEqualSlices(u8, framed, ours);
}

// canonicalize-test.c++:79: same regression inside an INLINE_COMPOSITE list.
test "reference port: INLINE_COMPOSITE data word with only its MSB set is not truncated" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80,
    };
    const framed = try frameSingleSegment(allocator, &segment);
    defer allocator.free(framed);

    var msg = try message.Message.init(allocator, framed, .{});
    defer msg.deinit();
    try std.testing.expect(canonical.isCanonical(&msg));

    const ours = try canonical.canonicalize(allocator, &msg);
    defer allocator.free(ours);
    try std.testing.expectEqualSlices(u8, framed, ours);
}

// canonicalize-test.c++:336 "primitive list with nonzero padding": the byte
// past the last element is nonzero; canonicalization must zero it (input and
// expected output bytes are the C++ test's own).
test "reference port: primitive list with nonzero padding is masked" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00,
        0x01, 0x02, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00,
    };
    const expected_segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00,
        0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    const framed = try frameSingleSegment(allocator, &segment);
    defer allocator.free(framed);
    const expected = try frameSingleSegment(allocator, &expected_segment);
    defer allocator.free(expected);

    var msg = try message.Message.init(allocator, framed, .{});
    defer msg.deinit();
    try std.testing.expect(!canonical.isCanonical(&msg));

    const ours = try canonical.canonicalize(allocator, &msg);
    defer allocator.free(ours);
    try std.testing.expectEqualSlices(u8, expected, ours);
}

// canonicalize-test.c++:363 "bit list with nonzero padding": the twelfth bit
// of an eleven-bit list is set; canonicalization must mask the partial byte.
test "reference port: bit list with nonzero padding is masked" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x59, 0x00, 0x00, 0x00,
        0xee, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    const expected_segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x59, 0x00, 0x00, 0x00,
        0xee, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    const framed = try frameSingleSegment(allocator, &segment);
    defer allocator.free(framed);
    const expected = try frameSingleSegment(allocator, &expected_segment);
    defer allocator.free(expected);

    var msg = try message.Message.init(allocator, framed, .{});
    defer msg.deinit();
    try std.testing.expect(!canonical.isCanonical(&msg));

    const ours = try canonical.canonicalize(allocator, &msg);
    defer allocator.free(ours);
    try std.testing.expectEqualSlices(u8, expected, ours);
}

// canonicalize-test.c++:289 "upgraded lists can be canonicalized": a list of
// int16 read (in C++) through a struct-list schema. Schema-free, this message
// is already canonical, so the output must be byte-identical — an upgraded
// list is NOT re-encoded (the schema-driven canonicalizer would re-encode it;
// that divergence is the reason this module exists).
test "reference port: upgraded list is preserved byte-for-byte" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x04, 0x05, 0x06,
        0x07, 0x08, 0x09, 0x10, 0x00, 0x00, 0x00, 0x00,
    };
    const framed = try frameSingleSegment(allocator, &segment);
    defer allocator.free(framed);

    var msg = try message.Message.init(allocator, framed, .{});
    defer msg.deinit();

    const ours = try canonical.canonicalize(allocator, &msg);
    defer allocator.free(ours);
    try std.testing.expectEqualSlices(u8, framed, ours);
}

// ---------------------------------------------------------------------------
// isCanonical acceptance rules, ported case by case
// ---------------------------------------------------------------------------

fn isCanonicalOfSegment(allocator: std.mem.Allocator, segment: []const u8) !bool {
    const framed = try frameSingleSegment(allocator, segment);
    defer allocator.free(framed);
    var msg = try message.Message.init(allocator, framed, .{});
    defer msg.deinit();
    return canonical.isCanonical(&msg);
}

// canonicalize-test.c++:107 "canonical non-null empty struct field": the
// zero-sized struct pointer must target its own location (offset -1).
test "isCanonical accepts an empty-struct pointer with offset -1" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0xfc, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00,
        0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee,
    };
    try std.testing.expect(try isCanonicalOfSegment(allocator, &segment));
}

// canonicalize-test.c++:127 "for pointers to empty structs, preorder is not
// canonical": same message but the empty-struct pointer has offset +1.
test "isCanonical rejects an empty-struct pointer in preorder position" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee,
    };
    try std.testing.expect(!try isCanonicalOfSegment(allocator, &segment));
}

// canonicalize-test.c++:148 "isCanonical requires pointer preorder".
test "isCanonical rejects out-of-order pointer targets" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00,
    };
    try std.testing.expect(!try isCanonicalOfSegment(allocator, &segment));
}

// canonicalize-test.c++:168 "isCanonical requires dense packing".
test "isCanonical rejects a gap before the struct body" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    };
    try std.testing.expect(!try isCanonicalOfSegment(allocator, &segment));
}

// canonicalize-test.c++:184 "isCanonical rejects multi-segment messages".
test "isCanonical rejects multi-segment messages" {
    const allocator = std.testing.allocator;
    const seg0 = [_]u8{
        0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    };
    const seg1 = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    };
    const framed = try frameTwoSegments(allocator, &seg0, &seg1);
    defer allocator.free(framed);
    var msg = try message.Message.init(allocator, framed, .{});
    defer msg.deinit();
    try std.testing.expect(!canonical.isCanonical(&msg));
}

// canonicalize-test.c++:212 "isCanonical requires truncation of 0-valued
// struct fields".
test "isCanonical rejects a trailing zero data word" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    try std.testing.expect(!try isCanonicalOfSegment(allocator, &segment));
}

// canonicalize-test.c++:227 "isCanonical rejects unused trailing words".
test "isCanonical rejects unused trailing words" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    try std.testing.expect(!try isCanonicalOfSegment(allocator, &segment));
}

// canonicalize-test.c++:244 "isCanonical accepts empty inline composite list
// of zero-sized structs".
test "isCanonical accepts an empty inline-composite list of zero-sized structs" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    try std.testing.expect(try isCanonicalOfSegment(allocator, &segment));
}

// canonicalize-test.c++:261 "isCanonical rejects inline composite list with
// inaccurate word-length" (list claims two words but needs only one).
test "isCanonical rejects an inline-composite list with an inflated word count" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
        0x05, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00,
    };
    try std.testing.expect(!try isCanonicalOfSegment(allocator, &segment));
}

// canonicalize-test.c++:313 "isCanonical requires truncation of 0-valued
// struct fields in all list members": every element has a trailing zero data
// word, so the uniform element size is not the max needed.
test "isCanonical rejects a struct list whose members all have truncatable words" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        0x01, 0x00, 0x00, 0x00, 0x27, 0x00, 0x00, 0x00,
        0x08, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    try std.testing.expect(!try isCanonicalOfSegment(allocator, &segment));
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

// layout.c++:2101-2104: copyPointer with canonical=true fails with "Cannot
// create a canonical message with a capability"; layout.c++:2822-2826: a
// capability ("OTHER") pointer is not positional, so isCanonical is false.
test "capability pointers cannot be canonicalized and are never canonical" {
    const allocator = std.testing.allocator;
    const segment = [_]u8{
        // Root struct pointer: no data, one pointer field.
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        // Capability pointer, index 0.
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    const framed = try frameSingleSegment(allocator, &segment);
    defer allocator.free(framed);

    var msg = try message.Message.init(allocator, framed, .{});
    defer msg.deinit();

    try std.testing.expect(!canonical.isCanonical(&msg));
    try std.testing.expectError(
        error.CannotCanonicalizeCapability,
        canonical.canonicalize(allocator, &msg),
    );
}

// ---------------------------------------------------------------------------
// Invariants (no CLI needed)
// ---------------------------------------------------------------------------

test "canonicalize is deterministic, idempotent, and yields a valid canonical message" {
    const allocator = std.testing.allocator;
    const bytes = try compare.readFileAlloc(allocator, "tests/capnp_testdata/testdata/binary");
    defer allocator.free(bytes);

    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const first = try canonical.canonicalize(allocator, &msg);
    defer allocator.free(first);

    // Determinism: a second run over the same message is byte-identical.
    const second = try canonical.canonicalize(allocator, &msg);
    defer allocator.free(second);
    try std.testing.expectEqualSlices(u8, first, second);

    // The output passes full validation and our isCanonical predicate.
    var out_msg = try message.Message.init(allocator, first, .{});
    defer out_msg.deinit();
    try std.testing.expect(canonical.isCanonical(&out_msg));

    // Idempotence: canonicalize(canonicalize(x)) == canonicalize(x).
    const again = try canonical.canonicalize(allocator, &out_msg);
    defer allocator.free(again);
    try std.testing.expectEqualSlices(u8, first, again);
}

/// `std.testing.checkAllAllocationFailures`, but with a pristine backing
/// allocator per iteration. The std version shares one backing allocator
/// across fail-index iterations, which this repo has measured reporting
/// `NondeterministicMemoryUsage` for deterministic code (in-place resize on a
/// warm heap swallows an alloc). Pattern copied from
/// tests/rpc/peer/three_party_handoff_harness.zig `checkAllAllocationFailuresIsolated`.
fn checkAllAllocationFailuresIsolated(
    comptime test_fn: anytype,
    extra_args: anytype,
) !void {
    const needed = blk: {
        var dbg: std.heap.DebugAllocator(.{}) = .init;
        defer std.debug.assert(dbg.deinit() == .ok);
        var fa = std.testing.FailingAllocator.init(dbg.allocator(), .{});
        try @call(.auto, test_fn, .{fa.allocator()} ++ extra_args);
        break :blk fa.alloc_index;
    };

    for (0..needed) |fail_index| {
        var dbg: std.heap.DebugAllocator(.{}) = .init;
        defer std.debug.assert(dbg.deinit() == .ok);
        var fa = std.testing.FailingAllocator.init(dbg.allocator(), .{ .fail_index = fail_index });
        if (@call(.auto, test_fn, .{fa.allocator()} ++ extra_args)) |_| {
            if (fa.has_induced_failure) return error.SwallowedOutOfMemoryError;
            return error.NondeterministicMemoryUsage;
        } else |err| switch (err) {
            error.OutOfMemory => {
                if (fa.allocated_bytes != fa.freed_bytes) return error.MemoryLeakDetected;
            },
            else => |e| return e,
        }
    }
}

fn canonicalizeOomProbe(allocator: std.mem.Allocator, framed: []const u8) !void {
    var msg = try message.Message.init(allocator, framed, .{});
    defer msg.deinit();
    const out = try canonical.canonicalize(allocator, &msg);
    allocator.free(out);
}

test "canonicalize survives allocation failure at every alloc point" {
    const allocator = std.testing.allocator;
    // Small but structurally rich fixture: struct -> 4 pointers -> data list.
    const segment = [_]u8{
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x04, 0x05, 0x06,
        0x07, 0x08, 0x09, 0x10, 0x00, 0x00, 0x00, 0x00,
    };
    const framed = try frameSingleSegment(allocator, &segment);
    defer allocator.free(framed);
    try checkAllAllocationFailuresIsolated(canonicalizeOomProbe, .{@as([]const u8, framed)});
}
