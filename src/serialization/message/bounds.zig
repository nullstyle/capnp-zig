/// Shared bounds-checking helpers for Cap'n Proto message serialization.
///
/// These inline functions deduplicate the offset+size bounds checks that are
/// repeated across readers, builders, and validation code. Each returns
/// `error.OutOfBounds` when the access would exceed the buffer.
/// Check that `data[offset..offset+size]` is in bounds.
/// Returns `error.OutOfBounds` if it is not.
pub inline fn checkBounds(data: []const u8, offset: usize, size: usize) error{OutOfBounds}!void {
    if (offset + size > data.len) return error.OutOfBounds;
}

/// Check that `data[offset..offset+size]` is in bounds (mutable variant).
/// Returns `error.OutOfBounds` if it is not.
pub inline fn checkBoundsMut(data: []u8, offset: usize, size: usize) error{OutOfBounds}!void {
    if (offset + size > data.len) return error.OutOfBounds;
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
    if (content_offset + total_bytes > segment.len) return error.OutOfBounds;
}
