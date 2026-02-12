const std = @import("std");
const log = std.log.scoped(.rpc_framing);

pub const Framer = struct {
    pub const max_frame_words: usize = 8 * 1024 * 1024;
    pub const max_segment_count: u32 = 512;

    allocator: std.mem.Allocator,
    buffer: std.ArrayList(u8),
    expected_total: ?usize = null,

    pub fn init(allocator: std.mem.Allocator) Framer {
        return .{
            .allocator = allocator,
            .buffer = std.ArrayList(u8){},
            .expected_total = null,
        };
    }

    pub fn deinit(self: *Framer) void {
        self.buffer.deinit(self.allocator);
        self.expected_total = null;
    }

    pub fn push(self: *Framer, data: []const u8) !void {
        if (data.len == 0) return;
        try self.buffer.appendSlice(self.allocator, data);
    }

    pub fn bufferedBytes(self: *const Framer) usize {
        return self.buffer.items.len;
    }

    /// Discard all buffered data and reset framing state.
    /// Called after an unrecoverable framing error to prevent the framer
    /// from repeatedly failing on the same corrupt bytes.
    pub fn reset(self: *Framer) void {
        self.buffer.items.len = 0;
        self.expected_total = null;
    }

    pub fn popFrame(self: *Framer) !?[]u8 {
        try self.updateExpected();
        const total = self.expected_total orelse return null;
        if (self.buffer.items.len < total) return null;

        const frame = try self.allocator.alloc(u8, total);
        std.mem.copyForwards(u8, frame, self.buffer.items[0..total]);

        const remaining = self.buffer.items.len - total;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.buffer.items[0..remaining], self.buffer.items[total..]);
        }
        self.buffer.items.len = remaining;
        self.expected_total = null;
        return frame;
    }

    fn updateExpected(self: *Framer) !void {
        if (self.expected_total != null) return;
        if (self.buffer.items.len < 4) return;

        const segment_count_minus_one = std.mem.readInt(u32, self.buffer.items[0..4], .little);
        const segment_count = std.math.add(u32, segment_count_minus_one, 1) catch return error.InvalidFrame;
        if (segment_count > max_segment_count) return error.InvalidFrame;
        const segment_count_usize = std.math.cast(usize, segment_count) orelse return error.InvalidFrame;
        const padding_words: usize = if (segment_count_usize % 2 == 0) 1 else 0;
        const header_words_no_padding = std.math.add(usize, 1, segment_count_usize) catch return error.InvalidFrame;
        const header_words = std.math.add(usize, header_words_no_padding, padding_words) catch return error.InvalidFrame;
        const header_bytes = std.math.mul(usize, header_words, 4) catch return error.InvalidFrame;

        if (self.buffer.items.len < header_bytes) return;

        var total_words: usize = 0;
        var offset: usize = 4;
        var idx: u32 = 0;
        while (idx < segment_count) : (idx += 1) {
            const size_words = std.mem.readInt(u32, self.buffer.items[offset..][0..4], .little);
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
};
