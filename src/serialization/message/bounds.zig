const std = @import("std");

/// Shared bounds-checking helpers for Cap'n Proto message serialization.
///
/// These inline functions deduplicate the offset+size bounds checks that are
/// repeated across readers, builders, and validation code. Each returns
/// `error.OutOfBounds` when the access would exceed the buffer.
/// Check that `data[offset..offset+size]` is in bounds.
/// Returns `error.OutOfBounds` if it is not.
pub inline fn checkBounds(data: []const u8, offset: usize, size: usize) error{OutOfBounds}!void {
    const end = std.math.add(usize, offset, size) catch return error.OutOfBounds;
    if (end > data.len) return error.OutOfBounds;
}

/// Check that `data[offset..offset+size]` is in bounds (mutable variant).
/// Returns `error.OutOfBounds` if it is not.
pub inline fn checkBoundsMut(data: []u8, offset: usize, size: usize) error{OutOfBounds}!void {
    const end = std.math.add(usize, offset, size) catch return error.OutOfBounds;
    if (end > data.len) return error.OutOfBounds;
}

/// Check that `data[offset]` is in bounds (single-byte access).
/// Returns `error.OutOfBounds` if it is not.
pub inline fn checkOffset(data: []const u8, offset: usize) error{OutOfBounds}!void {
    if (offset >= data.len) return error.OutOfBounds;
}

/// Check that `data[offset]` is in bounds (single-byte access, mutable variant).
/// Returns `error.OutOfBounds` if it is not.
pub inline fn checkOffsetMut(data: []u8, offset: usize) error{OutOfBounds}!void {
    if (offset >= data.len) return error.OutOfBounds;
}

/// Check that `segment[content_offset..content_offset + total_bytes]` is in bounds.
/// This is the common list-content bounds-check pattern used after resolving a list
/// pointer and computing the total content size.
pub inline fn checkListContentBounds(
    segments: anytype,
    segment_id: u32,
    content_offset: usize,
    total_bytes: usize,
) error{OutOfBounds}!void {
    const segment = segments[segment_id];
    const end = std.math.add(usize, content_offset, total_bytes) catch return error.OutOfBounds;
    if (end > segment.len) return error.OutOfBounds;
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

const testing = std.testing;

// -- checkBounds tests --

test "checkBounds: valid range at start of slice" {
    const data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try checkBounds(&data, 0, 4);
}

test "checkBounds: valid range spanning entire slice" {
    const data = [_]u8{ 1, 2, 3, 4 };
    try checkBounds(&data, 0, 4);
}

test "checkBounds: valid range at end of slice" {
    const data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try checkBounds(&data, 6, 2);
}

test "checkBounds: zero-size access at any valid position" {
    const data = [_]u8{ 1, 2, 3 };
    try checkBounds(&data, 0, 0);
    try checkBounds(&data, 1, 0);
    try checkBounds(&data, 3, 0); // end position, zero size is OK
}

test "checkBounds: zero-size access on empty slice" {
    const data: []const u8 = &.{};
    try checkBounds(data, 0, 0);
}

test "checkBounds: any access on empty slice fails" {
    const data: []const u8 = &.{};
    try testing.expectError(error.OutOfBounds, checkBounds(data, 0, 1));
}

test "checkBounds: offset past end" {
    const data = [_]u8{ 1, 2, 3, 4 };
    try testing.expectError(error.OutOfBounds, checkBounds(&data, 5, 1));
}

test "checkBounds: size exceeds remaining" {
    const data = [_]u8{ 1, 2, 3, 4 };
    try testing.expectError(error.OutOfBounds, checkBounds(&data, 2, 4));
}

test "checkBounds: offset + size overflow usize" {
    const data = [_]u8{ 1, 2, 3, 4 };
    try testing.expectError(error.OutOfBounds, checkBounds(&data, std.math.maxInt(usize), 1));
    try testing.expectError(error.OutOfBounds, checkBounds(&data, 1, std.math.maxInt(usize)));
    try testing.expectError(error.OutOfBounds, checkBounds(&data, std.math.maxInt(usize), std.math.maxInt(usize)));
}

test "checkBounds: maxInt(u32) offset on small slice" {
    const data = [_]u8{0} ** 4;
    try testing.expectError(error.OutOfBounds, checkBounds(&data, std.math.maxInt(u32), 1));
}

test "checkBounds: exactly one past end fails" {
    const data = [_]u8{ 1, 2, 3, 4 };
    try testing.expectError(error.OutOfBounds, checkBounds(&data, 0, 5));
    try testing.expectError(error.OutOfBounds, checkBounds(&data, 4, 1));
}

// -- checkBoundsMut tests --

test "checkBoundsMut: valid range at start of slice" {
    var data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try checkBoundsMut(&data, 0, 4);
}

test "checkBoundsMut: valid range spanning entire slice" {
    var data = [_]u8{ 1, 2, 3, 4 };
    try checkBoundsMut(&data, 0, 4);
}

test "checkBoundsMut: zero-size access on empty slice" {
    const data: []u8 = &.{};
    try checkBoundsMut(data, 0, 0);
}

test "checkBoundsMut: any access on empty slice fails" {
    const data: []u8 = &.{};
    try testing.expectError(error.OutOfBounds, checkBoundsMut(data, 0, 1));
}

test "checkBoundsMut: offset past end" {
    var data = [_]u8{ 1, 2, 3, 4 };
    try testing.expectError(error.OutOfBounds, checkBoundsMut(&data, 5, 1));
}

test "checkBoundsMut: size exceeds remaining" {
    var data = [_]u8{ 1, 2, 3, 4 };
    try testing.expectError(error.OutOfBounds, checkBoundsMut(&data, 2, 4));
}

test "checkBoundsMut: offset + size overflow usize" {
    var data = [_]u8{ 1, 2, 3, 4 };
    try testing.expectError(error.OutOfBounds, checkBoundsMut(&data, std.math.maxInt(usize), 1));
    try testing.expectError(error.OutOfBounds, checkBoundsMut(&data, 1, std.math.maxInt(usize)));
}

// -- checkOffset tests --

test "checkOffset: valid offset at start" {
    const data = [_]u8{ 1, 2, 3 };
    try checkOffset(&data, 0);
}

test "checkOffset: valid offset in middle" {
    const data = [_]u8{ 1, 2, 3 };
    try checkOffset(&data, 1);
}

test "checkOffset: valid offset at last element" {
    const data = [_]u8{ 1, 2, 3 };
    try checkOffset(&data, 2);
}

test "checkOffset: offset at length (one past end) fails" {
    const data = [_]u8{ 1, 2, 3 };
    try testing.expectError(error.OutOfBounds, checkOffset(&data, 3));
}

test "checkOffset: offset well past end fails" {
    const data = [_]u8{ 1, 2, 3 };
    try testing.expectError(error.OutOfBounds, checkOffset(&data, 100));
}

test "checkOffset: empty slice always fails" {
    const data: []const u8 = &.{};
    try testing.expectError(error.OutOfBounds, checkOffset(data, 0));
}

test "checkOffset: maxInt(usize) offset fails" {
    const data = [_]u8{ 1, 2, 3 };
    try testing.expectError(error.OutOfBounds, checkOffset(&data, std.math.maxInt(usize)));
}

test "checkOffset: maxInt(u32) offset on small slice fails" {
    const data = [_]u8{0} ** 4;
    try testing.expectError(error.OutOfBounds, checkOffset(&data, std.math.maxInt(u32)));
}

// -- checkOffsetMut tests --

test "checkOffsetMut: valid offset at start" {
    var data = [_]u8{ 1, 2, 3 };
    try checkOffsetMut(&data, 0);
}

test "checkOffsetMut: valid offset at last element" {
    var data = [_]u8{ 1, 2, 3 };
    try checkOffsetMut(&data, 2);
}

test "checkOffsetMut: offset at length fails" {
    var data = [_]u8{ 1, 2, 3 };
    try testing.expectError(error.OutOfBounds, checkOffsetMut(&data, 3));
}

test "checkOffsetMut: empty slice always fails" {
    const data: []u8 = &.{};
    try testing.expectError(error.OutOfBounds, checkOffsetMut(data, 0));
}

test "checkOffsetMut: maxInt(usize) offset fails" {
    var data = [_]u8{ 1, 2, 3 };
    try testing.expectError(error.OutOfBounds, checkOffsetMut(&data, std.math.maxInt(usize)));
}

// -- checkListContentBounds tests --

test "checkListContentBounds: valid content within segment" {
    const seg0 = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7 };
    const segments = [_][]const u8{&seg0};
    try checkListContentBounds(segments, 0, 0, 8);
    try checkListContentBounds(segments, 0, 4, 4);
    try checkListContentBounds(segments, 0, 0, 0);
}

test "checkListContentBounds: zero-length content at end of segment" {
    const seg0 = [_]u8{ 0, 1, 2, 3 };
    const segments = [_][]const u8{&seg0};
    try checkListContentBounds(segments, 0, 4, 0); // at end, zero bytes
}

test "checkListContentBounds: empty segment with zero content" {
    const seg0: []const u8 = &.{};
    const segments = [_][]const u8{seg0};
    try checkListContentBounds(segments, 0, 0, 0);
}

test "checkListContentBounds: empty segment with any content fails" {
    const seg0: []const u8 = &.{};
    const segments = [_][]const u8{seg0};
    try testing.expectError(error.OutOfBounds, checkListContentBounds(segments, 0, 0, 1));
}

test "checkListContentBounds: content exceeds segment" {
    const seg0 = [_]u8{ 0, 1, 2, 3 };
    const segments = [_][]const u8{&seg0};
    try testing.expectError(error.OutOfBounds, checkListContentBounds(segments, 0, 2, 4));
}

test "checkListContentBounds: offset past segment end" {
    const seg0 = [_]u8{ 0, 1, 2, 3 };
    const segments = [_][]const u8{&seg0};
    try testing.expectError(error.OutOfBounds, checkListContentBounds(segments, 0, 10, 1));
}

test "checkListContentBounds: offset + total_bytes overflow usize" {
    const seg0 = [_]u8{ 0, 1, 2, 3 };
    const segments = [_][]const u8{&seg0};
    try testing.expectError(error.OutOfBounds, checkListContentBounds(segments, 0, std.math.maxInt(usize), 1));
    try testing.expectError(error.OutOfBounds, checkListContentBounds(segments, 0, 1, std.math.maxInt(usize)));
}

test "checkListContentBounds: multiple segments selects correct one" {
    const seg0 = [_]u8{ 0, 1 };
    const seg1 = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7 };
    const segments = [_][]const u8{ &seg0, &seg1 };
    // Should fail on seg0 (too small) but succeed on seg1
    try testing.expectError(error.OutOfBounds, checkListContentBounds(segments, 0, 0, 4));
    try checkListContentBounds(segments, 1, 0, 4);
}

test "checkListContentBounds: content at exact segment boundary" {
    const seg0 = [_]u8{0} ** 16;
    const segments = [_][]const u8{&seg0};
    try checkListContentBounds(segments, 0, 8, 8); // exactly fills remaining
    try testing.expectError(error.OutOfBounds, checkListContentBounds(segments, 0, 8, 9)); // one byte over
}

test "checkListContentBounds: large element_count * element_size overflow simulation" {
    // Simulate what happens when a list with large element_count and element_size
    // computes total_bytes that would overflow. The caller typically does this
    // multiplication before calling us, but if total_bytes itself is huge or
    // offset + total_bytes overflows, we must catch it.
    const seg0 = [_]u8{0} ** 64;
    const segments = [_][]const u8{&seg0};
    // maxInt(usize) / 2 + maxInt(usize) / 2 + 2 would overflow
    const half = std.math.maxInt(usize) / 2;
    try testing.expectError(error.OutOfBounds, checkListContentBounds(segments, 0, half + 1, half + 1));
}
