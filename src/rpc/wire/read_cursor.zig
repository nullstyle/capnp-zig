//! Read-cursor bookkeeping shared by the stream framers: `wire/framing.zig`
//! (TCP) and the QUIC `length_framer.zig` / `native_framer.zig`.
//!
//! A framer appends inbound bytes to an `ArrayList(u8)` and hands whole
//! frames out from the front. The framers used to shift the unread tail down
//! after every frame, which costs O(tail) per frame: one read carrying k
//! small frames paid O(k * read size) in copies. Instead each framer keeps a
//! cursor, `consumed`, with `buffer.items[consumed..]` the unread bytes, and
//! reclaims the consumed prefix only when one of these holds:
//!
//!   * everything has been read: the buffer resets to empty, with no copy;
//!   * the cursor passes half of the buffered bytes: the unread tail is then
//!     no longer than the prefix it replaces, so a byte is moved at most once
//!     per prefix it outlives (amortized O(1) per byte, not O(tail) per frame);
//!   * an append would otherwise grow the allocation: compacting first keeps
//!     the buffer from holding more than the unread bytes plus the new data,
//!     the same memory bound the framers had before the cursor existed.
//!
//! Nothing outside the framers should read `buffer.items` directly; the
//! unread bytes are `unread(...)`.

const std = @import("std");

/// The bytes not yet consumed.
pub fn unread(buffer: *const std.ArrayList(u8), consumed: usize) []u8 {
    return buffer.items[consumed..];
}

/// Append `data`, compacting the consumed prefix first when that avoids a
/// reallocation. On error.OutOfMemory the unread bytes are unchanged (they
/// may have been compacted to the front, which no caller can observe).
pub fn append(
    buffer: *std.ArrayList(u8),
    consumed: *usize,
    allocator: std.mem.Allocator,
    data: []const u8,
) error{OutOfMemory}!void {
    if (consumed.* != 0 and buffer.capacity - buffer.items.len < data.len) compact(buffer, consumed);
    try buffer.appendSlice(allocator, data);
}

/// Mark the next `n` unread bytes consumed.
pub fn advance(buffer: *std.ArrayList(u8), consumed: *usize, n: usize) void {
    std.debug.assert(n <= buffer.items.len - consumed.*);
    consumed.* += n;
    const live = buffer.items.len - consumed.*;
    if (live == 0) {
        clear(buffer, consumed);
    } else if (consumed.* >= live) {
        compact(buffer, consumed);
    }
}

/// Drop every buffered byte, consumed or not. Keeps the allocation.
pub fn clear(buffer: *std.ArrayList(u8), consumed: *usize) void {
    buffer.items.len = 0;
    consumed.* = 0;
}

fn compact(buffer: *std.ArrayList(u8), consumed: *usize) void {
    const live = buffer.items.len - consumed.*;
    // Overlap is possible: `advance` compacts only once consumed >= live
    // (disjoint ranges), but `append` compacts at any cursor, where the
    // unread tail can overlap its own destination.
    @memmove(buffer.items[0..live], buffer.items[consumed.*..]);
    buffer.items.len = live;
    consumed.* = 0;
}
