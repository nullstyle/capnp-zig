const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.protocol;
const cap_table = capnpc.rpc.cap_table;
const message = capnpc.message;

// ---------------------------------------------------------------------------
// Release semantics tests for CapTable import ref-counting
// ---------------------------------------------------------------------------

test "CapTable noteImport increments ref count on repeated calls" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    try caps.noteImport(10);
    try caps.noteImport(10);
    try caps.noteImport(10);

    try std.testing.expectEqual(@as(usize, 1), caps.imports.count());
    const entry = caps.imports.get(10) orelse return error.MissingImport;
    try std.testing.expectEqual(@as(u32, 3), entry.ref_count);
}

test "CapTable releaseImport decrements ref count without removing" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    try caps.noteImport(20);
    try caps.noteImport(20);

    const fully_released = caps.releaseImport(20);
    try std.testing.expect(!fully_released);
    try std.testing.expectEqual(@as(usize, 1), caps.imports.count());
}

test "CapTable releaseImport removes entry when ref count reaches zero" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    try caps.noteImport(30);
    try caps.noteImport(30);

    _ = caps.releaseImport(30);
    const fully_released = caps.releaseImport(30);

    try std.testing.expect(fully_released);
    try std.testing.expectEqual(@as(usize, 0), caps.imports.count());
}

test "CapTable releaseImport on unknown id returns false gracefully" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    const result = caps.releaseImport(999);
    try std.testing.expect(!result);
    try std.testing.expectEqual(@as(usize, 0), caps.imports.count());
}

test "CapTable releaseImport on already-released id returns false" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    try caps.noteImport(40);
    const first = caps.releaseImport(40);
    try std.testing.expect(first);

    // Releasing again after full removal should return false.
    const second = caps.releaseImport(40);
    try std.testing.expect(!second);
}

test "CapTable ref-counting: multiple imports not freed until all refs released" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    // Import the same capability 5 times.
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        try caps.noteImport(50);
    }
    try std.testing.expectEqual(@as(usize, 1), caps.imports.count());

    // Release 4 times -- should not be fully released yet.
    i = 0;
    while (i < 4) : (i += 1) {
        const released = caps.releaseImport(50);
        try std.testing.expect(!released);
    }
    try std.testing.expectEqual(@as(usize, 1), caps.imports.count());

    // The 5th release should remove it.
    const released = caps.releaseImport(50);
    try std.testing.expect(released);
    try std.testing.expectEqual(@as(usize, 0), caps.imports.count());
}

test "CapTable multiple independent imports have separate ref counts" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    try caps.noteImport(60);
    try caps.noteImport(60);
    try caps.noteImport(61);

    try std.testing.expectEqual(@as(usize, 2), caps.imports.count());

    // Release one ref of 60 -- still present.
    _ = caps.releaseImport(60);
    try std.testing.expectEqual(@as(usize, 2), caps.imports.count());

    // Fully release 61.
    const released_61 = caps.releaseImport(61);
    try std.testing.expect(released_61);
    try std.testing.expectEqual(@as(usize, 1), caps.imports.count());

    // Fully release 60.
    const released_60 = caps.releaseImport(60);
    try std.testing.expect(released_60);
    try std.testing.expectEqual(@as(usize, 0), caps.imports.count());
}

// ---------------------------------------------------------------------------
// CapTable capacity / totalEntries tests
// ---------------------------------------------------------------------------

test "CapTable totalEntries counts imports correctly" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    try std.testing.expectEqual(@as(u32, 0), caps.totalEntries());

    try caps.noteImport(100);
    try caps.noteImport(101);
    try caps.noteImport(102);

    // Three distinct import IDs should each count as one entry.
    try std.testing.expectEqual(@as(u32, 3), caps.totalEntries());

    // Noting the same ID again should NOT increase the total (it bumps ref count).
    try caps.noteImport(100);
    try std.testing.expectEqual(@as(u32, 3), caps.totalEntries());
}

test "CapTable totalEntries decreases after full release" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    try caps.noteImport(200);
    try caps.noteImport(201);
    try std.testing.expectEqual(@as(u32, 2), caps.totalEntries());

    const released = caps.releaseImport(200);
    try std.testing.expect(released);
    try std.testing.expectEqual(@as(u32, 1), caps.totalEntries());
}

// ---------------------------------------------------------------------------
// InboundCapTable boundary tests
// ---------------------------------------------------------------------------

test "InboundCapTable get out of bounds returns error" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &caps);
    defer inbound.deinit();

    try std.testing.expectEqual(@as(u32, 0), inbound.len());
    try std.testing.expectError(error.CapabilityIndexOutOfBounds, inbound.get(0));
    try std.testing.expectError(error.CapabilityIndexOutOfBounds, inbound.get(999));
}

test "InboundCapTable retainIndex out of bounds returns error" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &caps);
    defer inbound.deinit();

    try std.testing.expectError(error.CapabilityIndexOutOfBounds, inbound.retainIndex(0));
}

test "InboundCapTable isRetained out of bounds returns false" {
    const allocator = std.testing.allocator;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &caps);
    defer inbound.deinit();

    try std.testing.expect(!inbound.isRetained(0));
    try std.testing.expect(!inbound.isRetained(999));
}

// ---------------------------------------------------------------------------
// InboundCapTable.clone() tests
// ---------------------------------------------------------------------------

/// Helper: create an InboundCapTable by manually allocating its entries/retained
/// slices with the given resolved caps and retained flags.
fn makeInboundCapTable(
    allocator: std.mem.Allocator,
    caps: []const cap_table.ResolvedCap,
    retained_flags: []const bool,
) !cap_table.InboundCapTable {
    std.debug.assert(caps.len == retained_flags.len);
    const entries = try allocator.dupe(cap_table.ResolvedCap, caps);
    errdefer allocator.free(entries);
    const retained = try allocator.dupe(bool, retained_flags);
    return .{
        .allocator = allocator,
        .entries = entries,
        .retained = retained,
    };
}

test "InboundCapTable clone produces independent copy with identical contents" {
    const allocator = std.testing.allocator;

    const caps = [_]cap_table.ResolvedCap{
        .{ .exported = .{ .id = 1 } },
        .{ .imported = .{ .id = 2 } },
        .none,
    };
    const retained_flags = [_]bool{ true, false, true };

    var original = try makeInboundCapTable(allocator, &caps, &retained_flags);
    defer original.deinit();

    var cloned = try original.clone();
    defer cloned.deinit();

    // Both should have the same length.
    try std.testing.expectEqual(original.len(), cloned.len());

    // Contents should match entry by entry.
    var i: u32 = 0;
    while (i < original.len()) : (i += 1) {
        const orig_entry = try original.get(i);
        const clone_entry = try cloned.get(i);
        try std.testing.expectEqual(orig_entry, clone_entry);
        try std.testing.expectEqual(original.isRetained(i), cloned.isRetained(i));
    }

    // The underlying slices must be different pointers (deep copy).
    try std.testing.expect(original.entries.ptr != cloned.entries.ptr);
    try std.testing.expect(original.retained.ptr != cloned.retained.ptr);
}

test "InboundCapTable clone is independent: modifying original does not affect clone" {
    const allocator = std.testing.allocator;

    const caps = [_]cap_table.ResolvedCap{
        .{ .exported = .{ .id = 10 } },
        .{ .imported = .{ .id = 20 } },
    };
    const retained_flags = [_]bool{ false, false };

    var original = try makeInboundCapTable(allocator, &caps, &retained_flags);
    defer original.deinit();

    var cloned = try original.clone();
    defer cloned.deinit();

    // Mutate the original entries and retained flags.
    original.entries[0] = .none;
    original.entries[1] = .{ .exported = .{ .id = 999 } };
    original.retained[0] = true;
    original.retained[1] = true;

    // The clone should still have the original values.
    try std.testing.expectEqual(cap_table.ResolvedCap{ .exported = .{ .id = 10 } }, try cloned.get(0));
    try std.testing.expectEqual(cap_table.ResolvedCap{ .imported = .{ .id = 20 } }, try cloned.get(1));
    try std.testing.expect(!cloned.isRetained(0));
    try std.testing.expect(!cloned.isRetained(1));
}

test "InboundCapTable clone survives deinit of original (no use-after-free)" {
    const allocator = std.testing.allocator;

    const caps = [_]cap_table.ResolvedCap{
        .{ .imported = .{ .id = 5 } },
        .{ .exported = .{ .id = 7 } },
        .{ .imported = .{ .id = 9 } },
    };
    const retained_flags = [_]bool{ true, false, true };

    var original = try makeInboundCapTable(allocator, &caps, &retained_flags);

    var cloned = try original.clone();
    defer cloned.deinit();

    // Deinit the original before accessing the clone.
    original.deinit();

    // The clone must still be fully usable.
    try std.testing.expectEqual(@as(u32, 3), cloned.len());
    try std.testing.expectEqual(cap_table.ResolvedCap{ .imported = .{ .id = 5 } }, try cloned.get(0));
    try std.testing.expectEqual(cap_table.ResolvedCap{ .exported = .{ .id = 7 } }, try cloned.get(1));
    try std.testing.expectEqual(cap_table.ResolvedCap{ .imported = .{ .id = 9 } }, try cloned.get(2));
    try std.testing.expect(cloned.isRetained(0));
    try std.testing.expect(!cloned.isRetained(1));
    try std.testing.expect(cloned.isRetained(2));

    // Retained state should be mutable on the clone independently.
    try cloned.retainIndex(1);
    try std.testing.expect(cloned.isRetained(1));
}

test "InboundCapTable clone of empty table produces empty clone" {
    const allocator = std.testing.allocator;

    var caps_table = cap_table.CapTable.init(allocator);
    defer caps_table.deinit();

    var empty = try cap_table.InboundCapTable.init(allocator, null, &caps_table);
    defer empty.deinit();

    var cloned = try empty.clone();
    defer cloned.deinit();

    try std.testing.expectEqual(@as(u32, 0), cloned.len());
    try std.testing.expectError(error.CapabilityIndexOutOfBounds, cloned.get(0));
}

fn inboundCapTableCloneOomImpl(allocator: std.mem.Allocator) !void {
    const caps = [_]cap_table.ResolvedCap{
        .{ .exported = .{ .id = 1 } },
        .{ .imported = .{ .id = 2 } },
    };
    const retained_flags = [_]bool{ true, false };

    var original = try makeInboundCapTable(allocator, &caps, &retained_flags);
    defer original.deinit();

    var cloned = try original.clone();
    cloned.deinit();
}

test "InboundCapTable clone propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, inboundCapTableCloneOomImpl, .{});
}

// ---------------------------------------------------------------------------
// Protocol-level failure injection tests
// ---------------------------------------------------------------------------

test "protocol DecodedMessage rejects empty input" {
    const allocator = std.testing.allocator;

    const empty: []const u8 = &.{};
    try std.testing.expectError(
        error.EndOfStream,
        protocol.DecodedMessage.init(allocator, empty),
    );
}

test "protocol DecodedMessage rejects truncated segment header" {
    const allocator = std.testing.allocator;

    // A valid Cap'n Proto frame needs at least a segment count word (4 bytes).
    const truncated = [_]u8{ 0x00, 0x00, 0x00 };
    try std.testing.expectError(
        error.EndOfStream,
        protocol.DecodedMessage.init(allocator, &truncated),
    );
}

test "protocol release message encodes and decodes correctly" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    try builder.buildRelease(42, 7);
    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(protocol.MessageTag.release, decoded.tag);
    const release = try decoded.asRelease();
    try std.testing.expectEqual(@as(u32, 42), release.id);
    try std.testing.expectEqual(@as(u32, 7), release.reference_count);
}

test "protocol release with zero reference count encodes correctly" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    try builder.buildRelease(99, 0);
    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(protocol.MessageTag.release, decoded.tag);
    const release = try decoded.asRelease();
    try std.testing.expectEqual(@as(u32, 99), release.id);
    try std.testing.expectEqual(@as(u32, 0), release.reference_count);
}

test "protocol return with exception round-trips reason string" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(55, .exception);
    try ret.setException("test failure reason");

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(protocol.MessageTag.@"return", decoded.tag);
    const parsed = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 55), parsed.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, parsed.tag);
    const ex = parsed.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("test failure reason", ex.reason);
}

test "protocol DecodedMessage rejects frame with invalid segment count" {
    const allocator = std.testing.allocator;

    // Build a frame whose first 4 bytes claim a very large segment count.
    // The decoder should reject it because there are not enough bytes for all
    // segment-size words that would follow.
    var bad_frame: [8]u8 = .{ 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    // segment_count word = 0x000000FF = 255 segments.
    // After the segment count, we would need 256 * 4 bytes for segment sizes,
    // but the frame only has 4 more bytes, so decoding must fail.
    const result = protocol.DecodedMessage.init(allocator, &bad_frame);
    try std.testing.expectError(error.TruncatedMessage, result);
}
