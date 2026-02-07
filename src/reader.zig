const std = @import("std");
const message = @import("message.zig");

/// Cap'n Proto message reader (segment-aware)
pub const Reader = struct {
    pub const max_total_words: usize = 8 * 1024 * 1024;

    msg: message.Message,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, data: []const u8) !Reader {
        return .{
            .msg = try message.Message.init(allocator, data),
            .allocator = allocator,
        };
    }

    pub fn initPacked(allocator: std.mem.Allocator, data: []const u8) !Reader {
        return .{
            .msg = try message.Message.initPacked(allocator, data),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Reader) void {
        self.msg.deinit();
    }

    /// Read a segment-framed message from a reader and return the framed bytes.
    pub fn readMessage(allocator: std.mem.Allocator, reader: anytype) ![]const u8 {
        const segment_count_minus_one = try reader.readInt(u32, .little);
        const segment_count = std.math.add(u32, segment_count_minus_one, 1) catch return error.InvalidSegmentCount;
        const segment_count_usize = std.math.cast(usize, segment_count) orelse return error.InvalidSegmentCount;
        if (segment_count_usize > message.Message.max_segment_count) return error.SegmentCountLimitExceeded;

        const segment_sizes = try allocator.alloc(u32, segment_count_usize);
        defer allocator.free(segment_sizes);

        var total_words: usize = 0;
        for (segment_sizes) |*size| {
            size.* = try reader.readInt(u32, .little);
            const size_words = std.math.cast(usize, size.*) orelse return error.InvalidMessageSize;
            total_words = std.math.add(usize, total_words, size_words) catch return error.InvalidMessageSize;
        }
        if (total_words > max_total_words) {
            return error.MessageTooLarge;
        }

        const padding_words: usize = if (segment_count_usize % 2 == 0) 1 else 0;
        if (padding_words == 1) {
            _ = try reader.readInt(u32, .little);
        }

        const header_words_no_padding = std.math.add(usize, 1, segment_count_usize) catch return error.InvalidMessageSize;
        const header_words = std.math.add(usize, header_words_no_padding, padding_words) catch return error.InvalidMessageSize;
        const header_bytes = std.math.mul(usize, header_words, 4) catch return error.InvalidMessageSize;
        const total_bytes = std.math.mul(usize, total_words, 8) catch return error.InvalidMessageSize;
        const framed_len = std.math.add(usize, header_bytes, total_bytes) catch return error.InvalidMessageSize;

        const framed = try allocator.alloc(u8, framed_len);
        errdefer allocator.free(framed);

        std.mem.writeInt(u32, framed[0..4], segment_count_minus_one, .little);
        var header_offset: usize = 4;
        for (segment_sizes) |size| {
            std.mem.writeInt(u32, framed[header_offset..][0..4], size, .little);
            header_offset += 4;
        }
        if (padding_words == 1) {
            std.mem.writeInt(u32, framed[header_offset..][0..4], 0, .little);
            header_offset += 4;
        }
        std.debug.assert(header_offset == header_bytes);

        const bytes_read = try reader.readAll(framed[header_bytes..]);
        if (bytes_read != total_bytes) {
            return error.UnexpectedEof;
        }

        return framed;
    }

    /// Read a packed message from a reader and return the unpacked framed bytes.
    pub fn readPackedMessage(allocator: std.mem.Allocator, reader: anytype) ![]const u8 {
        var out = std.ArrayList(u8){};
        errdefer out.deinit(allocator);

        var total_needed: ?usize = null;

        while (true) {
            if (total_needed) |needed| {
                if (out.items.len >= needed) break;
            }

            const tag = try reader.readByte();
            if (tag == 0x00) {
                const count = try reader.readByte();
                const words = @as(usize, count) + 1;
                try out.appendNTimes(allocator, 0, words * 8);
            } else if (tag == 0xFF) {
                var word: [8]u8 = undefined;
                try reader.readNoEof(&word);
                try out.appendSlice(allocator, &word);
                const count = try reader.readByte();
                if (count > 0) {
                    const byte_count = @as(usize, count) * 8;
                    const start = out.items.len;
                    try out.appendNTimes(allocator, 0, byte_count);
                    try reader.readNoEof(out.items[start .. start + byte_count]);
                }
            } else {
                var word = std.mem.zeroes([8]u8);
                for (0..8) |i| {
                    if ((tag & (@as(u8, 1) << @as(u3, @intCast(i)))) != 0) {
                        word[i] = try reader.readByte();
                    }
                }
                try out.appendSlice(allocator, &word);
            }

            if (total_needed == null and out.items.len >= 4) {
                const segment_count_minus_one = std.mem.readInt(u32, out.items[0..4], .little);
                const segment_count = std.math.add(u32, segment_count_minus_one, 1) catch return error.InvalidSegmentCount;
                const segment_count_usize = std.math.cast(usize, segment_count) orelse return error.InvalidSegmentCount;
                if (segment_count_usize > message.Message.max_segment_count) return error.SegmentCountLimitExceeded;
                const padding_words: usize = if (segment_count_usize % 2 == 0) 1 else 0;
                const header_words_no_padding = std.math.add(usize, 1, segment_count_usize) catch return error.InvalidPackedMessage;
                const header_words = std.math.add(usize, header_words_no_padding, padding_words) catch return error.InvalidPackedMessage;
                const header_bytes = std.math.mul(usize, header_words, 4) catch return error.InvalidPackedMessage;

                if (out.items.len >= header_bytes) {
                    var total_words: usize = 0;
                    var offset: usize = 4;
                    var idx: u32 = 0;
                    while (idx < segment_count) : (idx += 1) {
                        const size_words = std.mem.readInt(u32, out.items[offset..][0..4], .little);
                        total_words = std.math.add(usize, total_words, @as(usize, size_words)) catch return error.InvalidPackedMessage;
                        offset += 4;
                    }
                    if (total_words > max_total_words) return error.MessageTooLarge;
                    const total_bytes = std.math.mul(usize, total_words, 8) catch return error.InvalidPackedMessage;
                    total_needed = std.math.add(usize, header_bytes, total_bytes) catch return error.InvalidPackedMessage;
                }
            }

            if (total_needed) |needed| {
                if (out.items.len >= needed) break;
            }
        }

        if (total_needed) |needed| {
            if (out.items.len != needed) return error.InvalidPackedMessage;
        }

        return out.toOwnedSlice(allocator);
    }

    fn segment0(self: Reader) []const u8 {
        if (self.msg.segments.len == 0) return &[_]u8{};
        return self.msg.segments[0];
    }

    fn readPointer(self: Reader, offset: usize) u64 {
        const segment = self.segment0();
        if (offset + 8 > segment.len) return 0;
        return std.mem.readInt(u64, segment[offset..][0..8], .little);
    }

    /// Read text from a pointer located in segment 0 at the given offset.
    pub fn readText(self: Reader, offset: usize) ?[]const u8 {
        const pointer_word = self.readPointer(offset);
        if (pointer_word == 0) return null;

        const list = self.msg.resolveListPointer(0, offset, pointer_word) catch return null;
        if (list.element_size != 2) return null;

        const segment = self.msg.segments[list.segment_id];
        if (list.content_offset + list.element_count > segment.len) return null;
        if (list.element_count == 0) return "";

        const data = segment[list.content_offset .. list.content_offset + list.element_count];
        if (data.len > 0 and data[data.len - 1] == 0) {
            return data[0 .. data.len - 1];
        }
        return data;
    }

    /// Read a primitive value at the given offset in segment 0.
    pub fn readPrimitive(self: Reader, comptime T: type, offset: usize) T {
        const segment = self.segment0();
        if (offset + @sizeOf(T) > segment.len) return 0;
        return std.mem.readInt(T, segment[offset..][0..@sizeOf(T)], .little);
    }

    /// Read a boolean at the given bit offset in segment 0.
    pub fn readBool(self: Reader, byte_offset: usize, bit_offset: u3) bool {
        const segment = self.segment0();
        if (byte_offset >= segment.len) return false;
        const byte = segment[byte_offset];
        return (byte & (@as(u8, 1) << bit_offset)) != 0;
    }
};

test "Reader basic functionality" {
    const allocator = std.testing.allocator;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(1, 0);
    struct_builder.writeU32(0, 42);

    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    var reader = try Reader.init(allocator, bytes);
    defer reader.deinit();

    const value = reader.readPrimitive(u32, 8);
    try std.testing.expectEqual(@as(u32, 42), value);
}

test "Reader.readText handles byte list with low tag bits" {
    const allocator = std.testing.allocator;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    try struct_builder.writeText(0, "@");

    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    var reader = try Reader.init(allocator, bytes);
    defer reader.deinit();

    const maybe_text = reader.readText(8);
    try std.testing.expect(maybe_text != null);
    try std.testing.expectEqualStrings("@", maybe_text.?);
}

test "Reader.readText follows far pointers" {
    const allocator = std.testing.allocator;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    const segment_id = try builder.createSegment();
    try struct_builder.writeTextInSegment(0, "far", segment_id);

    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    var reader = try Reader.init(allocator, bytes);
    defer reader.deinit();

    const maybe_text = reader.readText(8);
    try std.testing.expect(maybe_text != null);
    try std.testing.expectEqualStrings("far", maybe_text.?);
}

test "Reader.readPackedMessage unpacks a packed stream" {
    const allocator = std.testing.allocator;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var struct_builder = try builder.allocateStruct(0, 1);
    try struct_builder.writeText(0, "packed-stream");

    const packed_bytes = try builder.toPackedBytes();
    defer allocator.free(packed_bytes);

    var stream = std.io.fixedBufferStream(packed_bytes);
    const framed = try Reader.readPackedMessage(allocator, stream.reader());
    defer allocator.free(framed);

    var reader = try Reader.init(allocator, framed);
    defer reader.deinit();

    const maybe_text = reader.readText(8);
    try std.testing.expect(maybe_text != null);
    try std.testing.expectEqualStrings("packed-stream", maybe_text.?);
}

test "Reader.readMessage rejects overflowing segment count" {
    const bytes = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    var stream = std.io.fixedBufferStream(&bytes);
    try std.testing.expectError(error.InvalidSegmentCount, Reader.readMessage(std.testing.allocator, stream.reader()));
}

test "Reader.readMessage rejects oversized payload claims" {
    const oversized_words: u32 = @as(u32, @intCast(Reader.max_total_words + 1));
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u32, bytes[0..4], 0, .little);
    std.mem.writeInt(u32, bytes[4..8], oversized_words, .little);

    var stream = std.io.fixedBufferStream(&bytes);
    try std.testing.expectError(error.MessageTooLarge, Reader.readMessage(std.testing.allocator, stream.reader()));
}

test "Reader.readPackedMessage rejects overflowing segment count" {
    var packed_bytes: [10]u8 = [_]u8{0} ** 10;
    packed_bytes[0] = 0xff;
    std.mem.writeInt(u64, packed_bytes[1..9], 0x00000000ffffffff, .little);
    packed_bytes[9] = 0;

    var stream = std.io.fixedBufferStream(&packed_bytes);
    try std.testing.expectError(error.InvalidSegmentCount, Reader.readPackedMessage(std.testing.allocator, stream.reader()));
}

test "Reader.readPackedMessage rejects oversized payload claims" {
    const oversized_words: u32 = @as(u32, @intCast(Reader.max_total_words + 1));
    var packed_bytes: [10]u8 = [_]u8{0} ** 10;
    packed_bytes[0] = 0xff;
    std.mem.writeInt(u64, packed_bytes[1..9], @as(u64, oversized_words) << 32, .little);
    packed_bytes[9] = 0;

    var stream = std.io.fixedBufferStream(&packed_bytes);
    try std.testing.expectError(error.MessageTooLarge, Reader.readPackedMessage(std.testing.allocator, stream.reader()));
}

fn readMessageOomImpl(allocator: std.mem.Allocator, framed: []const u8) !void {
    var stream = std.io.fixedBufferStream(framed);
    const out = try Reader.readMessage(allocator, stream.reader());
    defer allocator.free(out);
}

fn readPackedMessageOomImpl(allocator: std.mem.Allocator, packed_bytes: []const u8) !void {
    var stream = std.io.fixedBufferStream(packed_bytes);
    const out = try Reader.readPackedMessage(allocator, stream.reader());
    defer allocator.free(out);
}

test "Reader.readMessage propagates OOM without leaks" {
    const framed = [_]u8{
        0x00, 0x00, 0x00, 0x00, // segment_count_minus_one = 0 (1 segment)
        0x01, 0x00, 0x00, 0x00, // segment 0 size = 1 word
        0x00, 0x00, 0x00, 0x00, // segment payload (8 bytes total)
        0x00, 0x00, 0x00, 0x00,
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, readMessageOomImpl, .{&framed});
}

test "Reader.readPackedMessage propagates OOM without leaks" {
    const packed_bytes = [_]u8{
        0x10, 0x01, // first word: one non-zero byte at index 4
        0x00, 0x00, // second word: one zero word run
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, readPackedMessageOomImpl, .{&packed_bytes});
}

test "Reader.readPackedMessage rejects decoded header/body length mismatch" {
    // Decodes to 16 zero bytes, but the embedded frame header says only 8 bytes are needed.
    const packed_bytes = [_]u8{ 0x00, 0x01 };
    var stream = std.io.fixedBufferStream(&packed_bytes);
    try std.testing.expectError(error.InvalidPackedMessage, Reader.readPackedMessage(std.testing.allocator, stream.reader()));
}

fn expectPackedTruncationError(packed_bytes: []const u8) !void {
    var stream = std.io.fixedBufferStream(packed_bytes);
    const framed = Reader.readPackedMessage(std.testing.allocator, stream.reader()) catch |err| {
        switch (err) {
            error.EndOfStream => return,
            else => return err,
        }
    };
    defer std.testing.allocator.free(framed);
    return error.ExpectedPackedDecodeFailure;
}

test "Reader.readPackedMessage reports truncation for zero-run tag" {
    const packed_bytes = [_]u8{0x00}; // missing run-length byte
    try expectPackedTruncationError(&packed_bytes);
}

test "Reader.readPackedMessage reports truncation for full-run tag" {
    const packed_bytes = [_]u8{0xFF}; // missing literal word and run-length byte
    try expectPackedTruncationError(&packed_bytes);
}

test "Reader.readPackedMessage reports truncation for literal tag payload" {
    const packed_bytes = [_]u8{0x01}; // tag expects one payload byte, but none present
    try expectPackedTruncationError(&packed_bytes);
}
