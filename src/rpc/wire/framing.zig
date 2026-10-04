const std = @import("std");
const read_cursor = @import("read_cursor.zig");
const log = std.log.scoped(.rpc_framing);

pub const Framer = struct {
    /// Default matches Cap'n Proto reference traversalLimitInWords. Production deployments
    /// may want to lower this for untrusted peers.
    pub const max_frame_words: usize = 8 * 1024 * 1024;
    pub const max_segment_count: u32 = 512;
    pub const max_header_bytes: usize = (1 + @as(usize, max_segment_count) + 1) * 4;
    pub const default_max_buffered_bytes: usize = max_header_bytes + max_frame_words * 8;

    pub const Options = struct {
        max_buffered_bytes: usize = default_max_buffered_bytes,
    };

    allocator: std.mem.Allocator,
    /// Inbound bytes. This is framer state, not consumer API: the supported
    /// contract is `push` / `popFrame` / `bufferedBytes` / `reset`.
    ///
    /// `buffer.items` is NOT just the unread bytes (it was in 0.18.0).
    /// It starts with a prefix that `popFrame` already returned, which is
    /// reclaimed lazily (see `rpc/wire/read_cursor.zig`); the unread bytes
    /// are `buffer.items[consumed..]`. Count them with `bufferedBytes` and
    /// discard them with `reset`. Code that changes `buffer` directly must
    /// also set `consumed` so that `buffer.items[consumed..]` is exactly the
    /// unread bytes (0 after emptying or replacing the list). A cursor past
    /// the end makes `push`, `popFrame` and `bufferedBytes` panic in safe
    /// builds and is undefined behavior in ReleaseFast.
    buffer: std.ArrayList(u8),
    expected_total: ?usize = null,
    max_buffered_bytes: usize = default_max_buffered_bytes,
    /// Read cursor into `buffer`: `buffer.items[0..consumed]` was already
    /// returned by `popFrame`. Implementation state, held out of the frozen
    /// API (an Experimental override in tools/api_snapshot.zig).
    consumed: usize = 0,

    pub fn init(allocator: std.mem.Allocator) Framer {
        return initWithOptions(allocator, .{});
    }

    pub fn initWithOptions(allocator: std.mem.Allocator, options: Options) Framer {
        return .{
            .allocator = allocator,
            .buffer = std.ArrayList(u8).empty,
            .expected_total = null,
            .max_buffered_bytes = options.max_buffered_bytes,
        };
    }

    pub fn deinit(self: *Framer) void {
        self.buffer.deinit(self.allocator);
        self.consumed = 0;
        self.expected_total = null;
    }

    pub fn push(self: *Framer, data: []const u8) !void {
        if (data.len == 0) return;
        try self.ensureAppendBudget(data.len);
        try read_cursor.append(&self.buffer, &self.consumed, self.allocator, data);
    }

    /// Bytes pushed but not yet returned by `popFrame`.
    pub fn bufferedBytes(self: *const Framer) usize {
        return self.buffer.items.len - self.consumed;
    }

    /// Discard all buffered data and reset framing state.
    /// Called after an unrecoverable framing error to prevent the framer
    /// from repeatedly failing on the same corrupt bytes.
    pub fn reset(self: *Framer) void {
        read_cursor.clear(&self.buffer, &self.consumed);
        self.expected_total = null;
    }

    /// Attempt to extract a complete frame from the buffer.
    /// On framing error, caller must call `reset()` to discard corrupt data before retrying.
    pub fn popFrame(self: *Framer) !?[]u8 {
        try self.updateExpected();
        const total = self.expected_total orelse return null;
        const pending = read_cursor.unread(&self.buffer, self.consumed);
        if (pending.len < total) return null;

        const frame = try self.allocator.alloc(u8, total);
        @memcpy(frame, pending[0..total]);
        read_cursor.advance(&self.buffer, &self.consumed, total);
        self.expected_total = null;
        return frame;
    }

    fn updateExpected(self: *Framer) !void {
        if (self.expected_total != null) return;
        const pending = read_cursor.unread(&self.buffer, self.consumed);
        if (pending.len < 4) return;

        const segment_count_minus_one = std.mem.readInt(u32, pending[0..4], .little);
        const segment_count = std.math.add(u32, segment_count_minus_one, 1) catch {
            log.debug("InvalidFrame: segment_count overflow (raw={})", .{segment_count_minus_one});
            return error.InvalidFrame;
        };
        if (segment_count > max_segment_count) {
            log.debug("InvalidFrame: segment_count {} exceeds limit {}", .{ segment_count, max_segment_count });
            return error.InvalidFrame;
        }
        const segment_count_usize = std.math.cast(usize, segment_count) orelse return error.InvalidFrame;
        const padding_words: usize = if (segment_count_usize % 2 == 0) 1 else 0;
        const header_words_no_padding = std.math.add(usize, 1, segment_count_usize) catch return error.InvalidFrame;
        const header_words = std.math.add(usize, header_words_no_padding, padding_words) catch return error.InvalidFrame;
        const header_bytes = std.math.mul(usize, header_words, 4) catch return error.InvalidFrame;

        if (pending.len < header_bytes) return;

        var total_words: usize = 0;
        var offset: usize = 4;
        var idx: u32 = 0;
        while (idx < segment_count) : (idx += 1) {
            const size_words = std.mem.readInt(u32, pending[offset..][0..4], .little);
            total_words = std.math.add(usize, total_words, @as(usize, size_words)) catch return error.InvalidFrame;
            offset += 4;
        }
        if (total_words > max_frame_words) {
            log.debug("frame too large: {} words exceeds limit of {}", .{ total_words, max_frame_words });
            return error.FrameTooLarge;
        }

        const body_bytes = std.math.mul(usize, total_words, 8) catch return error.InvalidFrame;
        const total_bytes = std.math.add(usize, header_bytes, body_bytes) catch return error.InvalidFrame;
        self.expected_total = total_bytes;
    }

    fn ensureAppendBudget(self: *const Framer, data_len: usize) !void {
        const next = std.math.add(usize, self.bufferedBytes(), data_len) catch return error.FrameTooLarge;
        if (next > self.max_buffered_bytes) return error.FrameTooLarge;
    }
};
