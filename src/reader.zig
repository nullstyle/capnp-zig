const std = @import("std");
const message = @import("message.zig");

/// Cap'n Proto message reader (segment-aware)
pub const Reader = struct {
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
        const segment_count = segment_count_minus_one + 1;

        const segment_sizes = try allocator.alloc(u32, segment_count);
        defer allocator.free(segment_sizes);

        var total_words: usize = 0;
        for (segment_sizes) |*size| {
            size.* = try reader.readInt(u32, .little);
            total_words += size.*;
        }

        const padding_words: usize = if (segment_count % 2 == 0) 1 else 0;
        if (padding_words == 1) {
            _ = try reader.readInt(u32, .little);
        }

        const header_words = 1 + segment_count + padding_words;
        const header_bytes = header_words * 4;
        const total_bytes = total_words * 8;

        const framed = try allocator.alloc(u8, header_bytes + total_bytes);
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
                const segment_count = segment_count_minus_one + 1;
                const padding_words: usize = if (segment_count % 2 == 0) 1 else 0;
                const header_words = 1 + segment_count + padding_words;
                const header_bytes = header_words * 4;

                if (out.items.len >= header_bytes) {
                    var total_words: usize = 0;
                    var offset: usize = 4;
                    var idx: u32 = 0;
                    while (idx < segment_count) : (idx += 1) {
                        const size_words = std.mem.readInt(u32, out.items[offset..][0..4], .little);
                        total_words += size_words;
                        offset += 4;
                    }

                    total_needed = header_bytes + total_words * 8;
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
