const std = @import("std");
const list_reader_module = @import("message/list_readers.zig");
const list_builder_module = @import("message/list_builders.zig");
const any_pointer_reader_module = @import("message/any_pointer_reader.zig");
const any_pointer_builder_module = @import("message/any_pointer_builder.zig");
const struct_builder_module = @import("message/struct_builder.zig");
const clone_any_pointer_module = @import("message/clone_any_pointer.zig");

fn decodeOffsetWords(pointer_word: u64) i32 {
    // Cap'n Proto stores a 30-bit signed offset in words (two's complement).
    const raw: u32 = @truncate((pointer_word >> 2) & 0x3FFFFFFF);
    if ((raw & 0x20000000) != 0) {
        return @as(i32, @intCast(raw)) - (@as(i32, 1) << 30);
    }
    return @as(i32, @intCast(raw));
}

fn encodeOffsetWords(offset_words: i32) u32 {
    if (offset_words < 0) {
        const base: i64 = 1 << 30;
        return @as(u32, @intCast(base + offset_words));
    }
    return @as(u32, @intCast(offset_words));
}

fn makeStructPointer(offset_words: i32, data_words: u16, pointer_words: u16) u64 {
    var pointer: u64 = 0;
    pointer |= @as(u64, encodeOffsetWords(offset_words)) << 2;
    pointer |= @as(u64, data_words) << 32;
    pointer |= @as(u64, pointer_words) << 48;
    return pointer;
}

fn makeListPointer(offset_words: i32, element_size: u3, element_count: u32) u64 {
    var pointer: u64 = 1; // list pointer
    pointer |= @as(u64, encodeOffsetWords(offset_words)) << 2;
    pointer |= @as(u64, element_size) << 32;
    pointer |= @as(u64, element_count) << 35;
    return pointer;
}

fn makeFarPointer(landing_pad_is_double: bool, landing_pad_offset_words: u32, segment_id: u32) u64 {
    var pointer: u64 = 2; // far pointer
    if (landing_pad_is_double) {
        pointer |= @as(u64, 1) << 2;
    }
    pointer |= @as(u64, landing_pad_offset_words) << 3;
    pointer |= @as(u64, segment_id) << 32;
    return pointer;
}

fn makeCapabilityPointer(cap_id: u32) !u64 {
    if (cap_id >= (@as(u32, 1) << 30)) return error.CapabilityIdTooLarge;
    return 3 | (@as(u64, cap_id) << 2);
}

fn decodeCapabilityPointer(pointer_word: u64) !u32 {
    if ((pointer_word & 0x3) != 3) return error.InvalidPointer;
    if ((pointer_word >> 32) != 0) return error.InvalidPointer;
    return @as(u32, @intCast((pointer_word >> 2) & 0x3FFFFFFF));
}

fn listContentBytes(element_size: u3, element_count: u32) !usize {
    const count = @as(u64, element_count);
    const total: u64 = switch (element_size) {
        0 => 0,
        1 => (count + 7) / 8,
        2 => count,
        3 => count * 2,
        4 => count * 4,
        5 => count * 8,
        6 => count * 8,
        else => return error.InvalidPointer,
    };
    if (total > std.math.maxInt(usize)) return error.ListTooLarge;
    return @as(usize, @intCast(total));
}

fn listContentWords(element_size: u3, element_count: u32) !usize {
    const bytes = try listContentBytes(element_size, element_count);
    if (bytes == 0) return 0;
    if (bytes > std.math.maxInt(usize) - 7) return error.ListTooLarge;
    return (bytes + 7) / 8;
}

fn unpackPacked(allocator: std.mem.Allocator, packed_bytes: []const u8) ![]u8 {
    // Size-estimation pass: scan packed bytes to calculate exact output size.
    const total_size = try estimateUnpackedSize(packed_bytes);

    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);

    try out.ensureTotalCapacity(allocator, total_size);

    var index: usize = 0;
    while (index < packed_bytes.len) {
        const tag = packed_bytes[index];
        index += 1;

        if (tag == 0x00) {
            // Zero tag: emit current all-zero word plus run-length encoded extra zero words.
            if (index >= packed_bytes.len) return error.UnexpectedEof;
            const count = packed_bytes[index];
            index += 1;
            const zero_bytes = (1 + @as(usize, count)) * 8;
            const dest = try out.addManyAsSlice(allocator, zero_bytes);
            @memset(dest, 0);
            continue;
        }

        if (tag == 0xFF) {
            // 0xFF tag: current word is literal and may be followed by literal run words.
            if (index + 8 > packed_bytes.len) return error.UnexpectedEof;
            const dest = try out.addManyAsSlice(allocator, 8);
            @memcpy(dest, packed_bytes[index .. index + 8]);
            index += 8;
            if (index >= packed_bytes.len) return error.UnexpectedEof;
            const count = packed_bytes[index];
            index += 1;
            if (count > 0) {
                const byte_count = @as(usize, count) * 8;
                if (index + byte_count > packed_bytes.len) return error.UnexpectedEof;
                const run_dest = try out.addManyAsSlice(allocator, byte_count);
                @memcpy(run_dest, packed_bytes[index .. index + byte_count]);
                index += byte_count;
            }
            continue;
        }

        const dest = try out.addManyAsSlice(allocator, 8);
        @memset(dest, 0);
        var bit_index: u8 = 0;
        while (bit_index < 8) : (bit_index += 1) {
            if ((tag & (@as(u8, 1) << @intCast(bit_index))) != 0) {
                if (index >= packed_bytes.len) return error.UnexpectedEof;
                dest[@intCast(bit_index)] = packed_bytes[index];
                index += 1;
            }
        }
    }

    return out.toOwnedSlice(allocator);
}

/// Scans packed bytes to calculate the exact unpacked output size without
/// performing any allocation. This mirrors the structure of the packing format:
///   - Tag 0x00: 1 zero word + N extra zero words (N from count byte)
///   - Tag 0xFF: 1 literal word + N literal words (N from count byte)
///   - Other tags: 1 word with @popCount(tag) non-zero bytes
fn estimateUnpackedSize(packed_bytes: []const u8) !usize {
    var total: usize = 0;
    var index: usize = 0;

    while (index < packed_bytes.len) {
        const tag = packed_bytes[index];
        index += 1;

        if (tag == 0x00) {
            total = std.math.add(usize, total, 8) catch return error.Overflow;
            if (index >= packed_bytes.len) return error.UnexpectedEof;
            const count = packed_bytes[index];
            index += 1;
            total = std.math.add(usize, total, @as(usize, count) * 8) catch return error.Overflow;
            continue;
        }

        if (tag == 0xFF) {
            if (index + 8 > packed_bytes.len) return error.UnexpectedEof;
            total = std.math.add(usize, total, 8) catch return error.Overflow;
            index += 8;
            if (index >= packed_bytes.len) return error.UnexpectedEof;
            const count = packed_bytes[index];
            index += 1;
            const byte_count = @as(usize, count) * 8;
            if (index + byte_count > packed_bytes.len) return error.UnexpectedEof;
            total = std.math.add(usize, total, byte_count) catch return error.Overflow;
            index += byte_count;
            continue;
        }

        // Regular tag: each set bit means one non-zero byte follows.
        total = std.math.add(usize, total, 8) catch return error.Overflow;
        index += @popCount(tag);
    }

    return total;
}

fn packPacked(allocator: std.mem.Allocator, bytes: []const u8) ![]u8 {
    if (bytes.len % 8 != 0) return error.InvalidMessageSize;

    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);

    var index: usize = 0;
    while (index < bytes.len) {
        const word = bytes[index .. index + 8];
        const word_val = std.mem.readInt(u64, word[0..8], .little);

        if (word_val == 0) {
            // Collapse consecutive zero words into a single run record.
            var run: usize = 1;
            var scan = index + 8;
            while (run < 256 and scan + 8 <= bytes.len) : (scan += 8) {
                const next_val = std.mem.readInt(u64, bytes[scan..][0..8], .little);
                if (next_val != 0) break;
                run += 1;
            }

            try out.append(allocator, 0x00);
            try out.append(allocator, @as(u8, @intCast(run - 1)));
            index += run * 8;
            continue;
        }

        // Build tag byte and collect nonzero bytes.
        var tag: u8 = 0;
        var nonzero: [8]u8 = undefined;
        var nonzero_len: usize = 0;
        for (word, 0..) |byte, i| {
            if (byte != 0) {
                tag |= @as(u8, 1) << @as(u3, @intCast(i));
                nonzero[nonzero_len] = byte;
                nonzero_len += 1;
            }
        }

        if (tag == 0xFF) {
            // Collapse consecutive all-nonzero words into a literal run record.
            var run: usize = 1;
            var scan = index + 8;
            while (run < 256 and scan + 8 <= bytes.len) : (scan += 8) {
                const next_word = bytes[scan .. scan + 8];
                var has_zero = false;
                for (next_word) |b| {
                    if (b == 0) {
                        has_zero = true;
                        break;
                    }
                }
                if (has_zero) break;
                run += 1;
            }

            try out.append(allocator, 0xFF);
            try out.appendSlice(allocator, word);
            try out.append(allocator, @as(u8, @intCast(run - 1)));
            if (run > 1) {
                try out.appendSlice(allocator, bytes[index + 8 .. index + run * 8]);
            }
            index += run * 8;
            continue;
        }

        try out.append(allocator, tag);
        try out.appendSlice(allocator, nonzero[0..nonzero_len]);
        index += 8;
    }

    return out.toOwnedSlice(allocator);
}

const FarPointer = struct {
    landing_pad_is_double: bool,
    landing_pad_offset_words: u32,
    segment_id: u32,
};

fn decodeFarPointer(pointer_word: u64) FarPointer {
    return .{
        .landing_pad_is_double = ((pointer_word >> 2) & 0x1) != 0,
        .landing_pad_offset_words = @as(u32, @truncate((pointer_word >> 3) & 0x1FFFFFFF)),
        .segment_id = @as(u32, @truncate(pointer_word >> 32)),
    };
}

/// Decoded metadata for a Cap'n Proto inline-composite (struct) list.
pub const InlineCompositeList = struct {
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,
    data_words: u16,
    pointer_words: u16,
};

/// A capability pointer index, used in the RPC layer to reference entries
/// in the message's capability table.
pub const Capability = struct {
    id: u32,
};

/// A deserialized Cap'n Proto message providing zero-copy access to its contents.
///
/// The message is split into one or more segments. Reading is zero-copy:
/// `StructReader` and list readers reference the original byte slices directly.
/// Callers must keep the source data (or `backing_data`) alive for the lifetime
/// of any readers obtained from this message. Call `deinit` to free the
/// segment index (and backing data, if owned).
pub const Message = struct {
    pub const max_segment_count: usize = 512;

    allocator: std.mem.Allocator,
    segments: []const []const u8,
    segments_owned: bool = true,
    backing_data: ?[]u8,

    const ResolvedPointer = struct {
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        content_override: ?usize,
    };

    pub const ResolvedListPointer = struct {
        segment_id: u32,
        content_offset: usize,
        element_size: u3,
        element_count: u32,
    };

    pub const ValidationOptions = struct {
        segment_count_limit: usize = max_segment_count,
        traversal_limit_words: usize = 8 * 1024 * 1024,
        nesting_limit: usize = 64,
    };

    /// Deserialize a Cap'n Proto message from its framed wire representation.
    ///
    /// Parses the segment table header and slices `data` into per-segment views.
    /// The caller retains ownership of `data`; this message borrows into it.
    pub fn init(allocator: std.mem.Allocator, data: []const u8) !Message {
        var stream = std.io.fixedBufferStream(data);
        const reader = stream.reader();

        // Read segment count
        const segment_count_minus_one = try reader.readInt(u32, .little);
        const segment_count = std.math.add(u32, segment_count_minus_one, 1) catch return error.InvalidSegmentCount;
        const segment_count_usize = std.math.cast(usize, segment_count) orelse return error.InvalidSegmentCount;
        if (segment_count_usize > max_segment_count) return error.SegmentCountLimitExceeded;
        const padding_words: usize = if (segment_count_usize % 2 == 0) 1 else 0;
        const header_words_no_padding = std.math.add(usize, 1, segment_count_usize) catch return error.InvalidMessageSize;
        const header_words = std.math.add(usize, header_words_no_padding, padding_words) catch return error.InvalidMessageSize;
        const header_bytes = std.math.mul(usize, header_words, 4) catch return error.InvalidMessageSize;
        if (header_bytes > data.len) return error.TruncatedMessage;

        // Allocate segment array
        const segments = try allocator.alloc([]const u8, segment_count_usize);
        errdefer allocator.free(segments);

        // Read segment sizes (in words)
        const segment_sizes = try allocator.alloc(u32, segment_count_usize);
        defer allocator.free(segment_sizes);

        // First segment size is in the next word
        segment_sizes[0] = try reader.readInt(u32, .little);
        for (segment_sizes[1..]) |*size| {
            size.* = try reader.readInt(u32, .little);
        }

        // Padding to 8-byte boundary
        if (segment_count % 2 == 0) {
            _ = try reader.readInt(u32, .little);
        }

        // Read segment data
        var offset: usize = stream.pos;
        for (segment_sizes, 0..) |size_words, i| {
            const size_words_usize = std.math.cast(usize, size_words) orelse return error.InvalidMessageSize;
            const size_bytes = std.math.mul(usize, size_words_usize, 8) catch return error.InvalidMessageSize;
            const end = std.math.add(usize, offset, size_bytes) catch return error.TruncatedMessage;
            if (end > data.len) {
                return error.TruncatedMessage;
            }
            segments[i] = data[offset..end];
            offset = end;
        }

        return .{
            .allocator = allocator,
            .segments = segments,
            .segments_owned = true,
            .backing_data = null,
        };
    }

    /// Deserialize a Cap'n Proto message from packed encoding.
    ///
    /// Unpacks `packed_bytes` into standard framed format, then parses segments.
    /// The unpacked buffer is owned by this message and freed on `deinit`.
    pub fn initPacked(allocator: std.mem.Allocator, packed_bytes: []const u8) !Message {
        const unpacked = try unpackPacked(allocator, packed_bytes);
        errdefer allocator.free(unpacked);

        var msg = try Message.init(allocator, unpacked);
        msg.backing_data = unpacked;
        msg.segments_owned = true;
        return msg;
    }

    /// Free the segment index and any owned backing data.
    pub fn deinit(self: *Message) void {
        if (self.segments_owned) {
            self.allocator.free(self.segments);
        }
        if (self.backing_data) |data| {
            self.allocator.free(data);
        }
    }

    fn readWord(self: *const Message, segment_id: u32, byte_offset: usize) !u64 {
        if (segment_id >= self.segments.len) return error.InvalidSegmentId;
        const segment = self.segments[segment_id];
        if (byte_offset + 8 > segment.len) return error.OutOfBounds;
        return std.mem.readInt(u64, segment[byte_offset..][0..8], .little);
    }

    pub fn resolvePointer(self: *const Message, segment_id: u32, pointer_pos: usize, pointer_word: u64, depth: u8) !ResolvedPointer {
        if (depth == 0) return error.PointerDepthLimit;

        const pointer_type = @as(u2, @truncate(pointer_word & 0x3));
        if (pointer_type != 2) {
            return .{
                .segment_id = segment_id,
                .pointer_pos = pointer_pos,
                .pointer_word = pointer_word,
                .content_override = null,
            };
        }

        const far = decodeFarPointer(pointer_word);
        if (far.segment_id >= self.segments.len) return error.InvalidSegmentId;
        const landing_pos = @as(usize, far.landing_pad_offset_words) * 8;
        const landing_segment = self.segments[far.segment_id];
        if (landing_pos + 8 > landing_segment.len) return error.OutOfBounds;

        if (!far.landing_pad_is_double) {
            // Single-far: landing pad directly stores the pointed-to pointer word.
            const landing_word = try self.readWord(far.segment_id, landing_pos);
            return try self.resolvePointer(far.segment_id, landing_pos, landing_word, depth - 1);
        }

        // Double-far: landing pad has [far-to-content, tag-word].
        if (landing_pos + 16 > landing_segment.len) return error.OutOfBounds;

        const landing_word = try self.readWord(far.segment_id, landing_pos);
        const tag_word = try self.readWord(far.segment_id, landing_pos + 8);

        const landing_type = @as(u2, @truncate(landing_word & 0x3));
        if (landing_type != 2) return error.InvalidFarPointer;
        const landing_far = decodeFarPointer(landing_word);
        if (landing_far.landing_pad_is_double) return error.InvalidFarPointer;
        if (landing_far.segment_id >= self.segments.len) return error.InvalidSegmentId;

        const content_offset = @as(usize, landing_far.landing_pad_offset_words) * 8;
        return .{
            .segment_id = landing_far.segment_id,
            .pointer_pos = 0,
            .pointer_word = tag_word,
            .content_override = content_offset,
        };
    }

    pub fn resolveStructPointer(self: *const Message, segment_id: u32, pointer_pos: usize, pointer_word: u64) !StructReader {
        const resolved = try self.resolvePointer(segment_id, pointer_pos, pointer_word, 8);
        if (resolved.pointer_word == 0) return error.InvalidRootPointer;

        const pointer_type = @as(u2, @truncate(resolved.pointer_word & 0x3));
        if (pointer_type != 0) return error.InvalidRootPointer;

        const offset = decodeOffsetWords(resolved.pointer_word);
        const data_size = @as(u16, @truncate((resolved.pointer_word >> 32) & 0xFFFF));
        const pointer_count = @as(u16, @truncate((resolved.pointer_word >> 48) & 0xFFFF));

        var struct_offset: usize = undefined;
        if (resolved.content_override) |override| {
            struct_offset = override;
        } else {
            const struct_offset_signed = @as(isize, @intCast(resolved.pointer_pos)) + 8 + @as(isize, offset) * 8;
            if (struct_offset_signed < 0) return error.InvalidRootPointer;
            struct_offset = @as(usize, @intCast(struct_offset_signed));
        }

        const segment = self.segments[resolved.segment_id];
        const total_bytes = (@as(usize, data_size) + @as(usize, pointer_count)) * 8;
        if (struct_offset + total_bytes > segment.len) return error.TruncatedMessage;

        return StructReader{
            .message = self,
            .segment_id = resolved.segment_id,
            .offset = struct_offset,
            .data_size = data_size,
            .pointer_count = pointer_count,
        };
    }

    pub fn resolveListPointer(self: *const Message, segment_id: u32, pointer_pos: usize, pointer_word: u64) !ResolvedListPointer {
        const resolved = try self.resolvePointer(segment_id, pointer_pos, pointer_word, 8);
        if (resolved.pointer_word == 0) return error.InvalidPointer;

        const pointer_type = @as(u2, @truncate(resolved.pointer_word & 0x3));
        if (pointer_type != 1) return error.InvalidPointer;

        const offset = decodeOffsetWords(resolved.pointer_word);
        const element_size = @as(u3, @truncate((resolved.pointer_word >> 32) & 0x7));
        const element_count = @as(u32, @truncate((resolved.pointer_word >> 35)));

        var content_offset: usize = undefined;
        if (resolved.content_override) |override| {
            content_offset = override;
        } else {
            const content_offset_signed = @as(isize, @intCast(resolved.pointer_pos)) + 8 + @as(isize, offset) * 8;
            if (content_offset_signed < 0) return error.OutOfBounds;
            content_offset = @as(usize, @intCast(content_offset_signed));
        }

        return .{
            .segment_id = resolved.segment_id,
            .content_offset = content_offset,
            .element_size = element_size,
            .element_count = element_count,
        };
    }

    pub fn resolveInlineCompositeList(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
    ) !InlineCompositeList {
        if (pointer_word == 0) return error.InvalidPointer;

        const pointer_type = @as(u2, @truncate(pointer_word & 0x3));
        if (pointer_type == 1) {
            const element_size = @as(u3, @truncate((pointer_word >> 32) & 0x7));
            if (element_size != 7) return error.InvalidInlineCompositePointer;

            const offset = decodeOffsetWords(pointer_word);
            const word_count = @as(u32, @truncate(pointer_word >> 35));

            const tag_pos_signed = @as(isize, @intCast(pointer_pos)) + 8 + @as(isize, offset) * 8;
            if (tag_pos_signed < 0) return error.OutOfBounds;
            const tag_pos = @as(usize, @intCast(tag_pos_signed));

            const tag_word = try self.readWord(segment_id, tag_pos);
            const tag_type = @as(u2, @truncate(tag_word & 0x3));
            if (tag_type != 0) return error.InvalidInlineCompositePointer;

            const element_count_signed = decodeOffsetWords(tag_word);
            if (element_count_signed < 0) return error.InvalidInlineCompositePointer;
            const element_count = @as(u32, @intCast(element_count_signed));

            const data_words = @as(u16, @truncate((tag_word >> 32) & 0xFFFF));
            const pointer_words = @as(u16, @truncate((tag_word >> 48) & 0xFFFF));

            const words_per_element = @as(u32, data_words) + @as(u32, pointer_words);
            const expected_words_u64 = @as(u64, element_count) * @as(u64, words_per_element);
            if (expected_words_u64 > @as(u64, word_count)) return error.InvalidInlineCompositePointer;

            const elements_offset = tag_pos + 8;
            const segment = self.segments[segment_id];
            const total_bytes = @as(usize, word_count) * 8;
            if (elements_offset + total_bytes > segment.len) return error.OutOfBounds;

            return .{
                .segment_id = segment_id,
                .elements_offset = elements_offset,
                .element_count = element_count,
                .data_words = data_words,
                .pointer_words = pointer_words,
            };
        }

        if (pointer_type != 2) return error.InvalidPointer;

        const far = decodeFarPointer(pointer_word);
        if (far.segment_id >= self.segments.len) return error.InvalidSegmentId;
        const landing_pos = @as(usize, far.landing_pad_offset_words) * 8;
        const landing_segment = self.segments[far.segment_id];
        if (landing_pos + 8 > landing_segment.len) return error.OutOfBounds;

        if (!far.landing_pad_is_double) {
            const landing_word = try self.readWord(far.segment_id, landing_pos);
            return try self.resolveInlineCompositeList(far.segment_id, landing_pos, landing_word);
        }

        if (landing_pos + 16 > landing_segment.len) return error.OutOfBounds;

        const landing_word = try self.readWord(far.segment_id, landing_pos);
        const second_word = try self.readWord(far.segment_id, landing_pos + 8);

        const landing_type = @as(u2, @truncate(landing_word & 0x3));
        if (landing_type != 2) return error.InvalidFarPointer;
        const landing_far = decodeFarPointer(landing_word);
        if (landing_far.landing_pad_is_double) return error.InvalidFarPointer;
        if (landing_far.segment_id >= self.segments.len) return error.InvalidSegmentId;

        const second_type = @as(u2, @truncate(second_word & 0x3));

        if (second_type == 0) {
            // Layout A (used by our builder): landing pad stores tag word directly.
            const tag_word = second_word;
            const element_count_signed = decodeOffsetWords(tag_word);
            if (element_count_signed < 0) return error.InvalidInlineCompositePointer;
            const element_count = @as(u32, @intCast(element_count_signed));
            const data_words = @as(u16, @truncate((tag_word >> 32) & 0xFFFF));
            const pointer_words = @as(u16, @truncate((tag_word >> 48) & 0xFFFF));

            const words_per_element = @as(u32, data_words) + @as(u32, pointer_words);
            const total_words_u64 = @as(u64, element_count) * @as(u64, words_per_element);
            if (total_words_u64 > std.math.maxInt(usize) / 8) return error.OutOfBounds;
            const total_words = @as(usize, @intCast(total_words_u64));
            const elements_offset = @as(usize, landing_far.landing_pad_offset_words) * 8;
            const content_segment = self.segments[landing_far.segment_id];
            const total_bytes = total_words * 8;
            if (elements_offset + total_bytes > content_segment.len) return error.OutOfBounds;

            return .{
                .segment_id = landing_far.segment_id,
                .elements_offset = elements_offset,
                .element_count = element_count,
                .data_words = data_words,
                .pointer_words = pointer_words,
            };
        }

        if (second_type == 1) {
            // Layout B (used by the reference implementation): landing pad stores list pointer; tag is at target.
            const list_pointer_word = second_word;
            const element_size = @as(u3, @truncate((list_pointer_word >> 32) & 0x7));
            if (element_size != 7) return error.InvalidInlineCompositePointer;
            const word_count = @as(u32, @truncate(list_pointer_word >> 35));

            const tag_pos = @as(usize, landing_far.landing_pad_offset_words) * 8;
            const tag_word = try self.readWord(landing_far.segment_id, tag_pos);
            const tag_type = @as(u2, @truncate(tag_word & 0x3));
            if (tag_type != 0) return error.InvalidInlineCompositePointer;

            const element_count_signed = decodeOffsetWords(tag_word);
            if (element_count_signed < 0) return error.InvalidInlineCompositePointer;
            const element_count = @as(u32, @intCast(element_count_signed));
            const data_words = @as(u16, @truncate((tag_word >> 32) & 0xFFFF));
            const pointer_words = @as(u16, @truncate((tag_word >> 48) & 0xFFFF));

            const words_per_element = @as(u32, data_words) + @as(u32, pointer_words);
            const expected_words_u64 = @as(u64, element_count) * @as(u64, words_per_element);
            if (expected_words_u64 > @as(u64, word_count)) return error.InvalidInlineCompositePointer;

            const elements_offset = tag_pos + 8;
            const content_segment = self.segments[landing_far.segment_id];
            const total_bytes = @as(usize, word_count) * 8;
            if (elements_offset + total_bytes > content_segment.len) return error.OutOfBounds;

            return .{
                .segment_id = landing_far.segment_id,
                .elements_offset = elements_offset,
                .element_count = element_count,
                .data_words = data_words,
                .pointer_words = pointer_words,
            };
        }

        return error.InvalidInlineCompositePointer;
    }

    /// Validate the message structure against configurable traversal and nesting limits.
    pub fn validate(self: *const Message, options: ValidationOptions) anyerror!void {
        if (self.segments.len == 0) return error.EmptyMessage;
        if (self.segments.len > options.segment_count_limit) return error.SegmentCountLimitExceeded;
        const segment = self.segments[0];
        if (segment.len < 8) return error.TruncatedMessage;

        const root_pointer = std.mem.readInt(u64, segment[0..8], .little);
        var remaining = options.traversal_limit_words;
        try self.validatePointer(0, 0, root_pointer, &remaining, options.nesting_limit);
    }

    fn consumeWords(remaining: *usize, words: usize) !void {
        if (words > remaining.*) return error.TraversalLimitExceeded;
        remaining.* -= words;
    }

    fn validatePointer(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        remaining: *usize,
        nesting: usize,
    ) anyerror!void {
        if (pointer_word == 0) return;
        if (nesting == 0) return error.NestingLimitExceeded;
        if (segment_id >= self.segments.len) return error.InvalidSegmentId;

        const pointer_type = @as(u2, @truncate(pointer_word & 0x3));
        switch (pointer_type) {
            0 => try self.validateStructPointer(segment_id, pointer_pos, pointer_word, null, remaining, nesting - 1),
            1 => try self.validateListPointer(segment_id, pointer_pos, pointer_word, null, remaining, nesting - 1),
            2 => try self.validateFarPointer(segment_id, pointer_pos, pointer_word, remaining, nesting - 1),
            else => return error.InvalidPointer,
        }
    }

    fn validateFarPointer(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        remaining: *usize,
        nesting: usize,
    ) anyerror!void {
        _ = segment_id;
        _ = pointer_pos;
        const far = decodeFarPointer(pointer_word);
        if (far.segment_id >= self.segments.len) return error.InvalidSegmentId;
        const landing_pos = @as(usize, far.landing_pad_offset_words) * 8;
        const landing_segment = self.segments[far.segment_id];
        if (landing_pos + 8 > landing_segment.len) return error.OutOfBounds;

        if (!far.landing_pad_is_double) {
            const landing_word = try self.readWord(far.segment_id, landing_pos);
            return self.validatePointer(far.segment_id, landing_pos, landing_word, remaining, nesting);
        }

        if (landing_pos + 16 > landing_segment.len) return error.OutOfBounds;

        const landing_word = try self.readWord(far.segment_id, landing_pos);
        const tag_word = try self.readWord(far.segment_id, landing_pos + 8);

        const landing_type = @as(u2, @truncate(landing_word & 0x3));
        if (landing_type != 2) return error.InvalidFarPointer;
        const landing_far = decodeFarPointer(landing_word);
        if (landing_far.landing_pad_is_double) return error.InvalidFarPointer;
        if (landing_far.segment_id >= self.segments.len) return error.InvalidSegmentId;

        const elements_offset = @as(usize, landing_far.landing_pad_offset_words) * 8;
        const tag_type = @as(u2, @truncate(tag_word & 0x3));
        if (tag_type == 0) {
            return self.validateInlineCompositeTag(landing_far.segment_id, elements_offset, tag_word, remaining, nesting);
        }
        if (tag_type == 1) {
            return self.validateListPointer(landing_far.segment_id, 0, tag_word, elements_offset, remaining, nesting);
        }
        return error.InvalidFarPointer;
    }

    fn validateStructPointer(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        content_override: ?usize,
        remaining: *usize,
        nesting: usize,
    ) anyerror!void {
        const data_size = @as(u16, @truncate((pointer_word >> 32) & 0xFFFF));
        const pointer_count = @as(u16, @truncate((pointer_word >> 48) & 0xFFFF));

        var struct_offset: usize = undefined;
        if (content_override) |override| {
            struct_offset = override;
        } else {
            const offset = decodeOffsetWords(pointer_word);
            const struct_offset_signed = @as(isize, @intCast(pointer_pos)) + 8 + @as(isize, offset) * 8;
            if (struct_offset_signed < 0) return error.OutOfBounds;
            struct_offset = @as(usize, @intCast(struct_offset_signed));
        }

        const segment = self.segments[segment_id];
        const total_words = @as(usize, data_size) + @as(usize, pointer_count);
        const total_bytes = total_words * 8;
        if (struct_offset > segment.len) return error.OutOfBounds;
        if (total_bytes > segment.len - struct_offset) return error.OutOfBounds;

        try consumeWords(remaining, total_words);

        if (pointer_count == 0) return;
        const pointer_section_offset = struct_offset + @as(usize, data_size) * 8;
        var idx: usize = 0;
        while (idx < pointer_count) : (idx += 1) {
            const ptr_pos = pointer_section_offset + idx * 8;
            const word = std.mem.readInt(u64, segment[ptr_pos..][0..8], .little);
            try self.validatePointer(segment_id, ptr_pos, word, remaining, nesting);
        }
    }

    fn validateListPointer(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        content_override: ?usize,
        remaining: *usize,
        nesting: usize,
    ) anyerror!void {
        const element_size = @as(u3, @truncate((pointer_word >> 32) & 0x7));
        if (element_size == 7 and content_override == null) {
            return self.validateInlineCompositeList(segment_id, pointer_pos, pointer_word, remaining, nesting);
        }
        if (element_size == 7 and content_override != null) {
            return error.InvalidInlineCompositePointer;
        }

        const element_count = @as(u32, @truncate((pointer_word >> 35)));
        var content_offset: usize = undefined;
        if (content_override) |override| {
            content_offset = override;
        } else {
            const offset = decodeOffsetWords(pointer_word);
            const content_offset_signed = @as(isize, @intCast(pointer_pos)) + 8 + @as(isize, offset) * 8;
            if (content_offset_signed < 0) return error.OutOfBounds;
            content_offset = @as(usize, @intCast(content_offset_signed));
        }

        const segment = self.segments[segment_id];
        const total_bytes = try listContentBytes(element_size, element_count);
        if (content_offset > segment.len) return error.OutOfBounds;
        if (total_bytes > segment.len - content_offset) return error.OutOfBounds;

        const total_words = try listContentWords(element_size, element_count);
        try consumeWords(remaining, total_words);

        if (element_size != 6 or element_count == 0) return;
        var index: u32 = 0;
        while (index < element_count) : (index += 1) {
            const ptr_pos = content_offset + @as(usize, index) * 8;
            const word = std.mem.readInt(u64, segment[ptr_pos..][0..8], .little);
            try self.validatePointer(segment_id, ptr_pos, word, remaining, nesting);
        }
    }

    fn validateInlineCompositeList(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        remaining: *usize,
        nesting: usize,
    ) anyerror!void {
        const list = try self.resolveInlineCompositeList(segment_id, pointer_pos, pointer_word);
        const word_count = @as(u32, @truncate(pointer_word >> 35));
        try consumeWords(remaining, @as(usize, word_count));

        if (list.pointer_words == 0 or list.element_count == 0) return;

        const segment = self.segments[list.segment_id];
        const words_per_element = @as(usize, list.data_words) + @as(usize, list.pointer_words);
        const element_stride = words_per_element * 8;
        var element_index: u32 = 0;
        while (element_index < list.element_count) : (element_index += 1) {
            const element_base = list.elements_offset + @as(usize, element_index) * element_stride;
            const pointer_section = element_base + @as(usize, list.data_words) * 8;
            var ptr_index: u16 = 0;
            while (ptr_index < list.pointer_words) : (ptr_index += 1) {
                const ptr_pos = pointer_section + @as(usize, ptr_index) * 8;
                const word = std.mem.readInt(u64, segment[ptr_pos..][0..8], .little);
                try self.validatePointer(list.segment_id, ptr_pos, word, remaining, nesting);
            }
        }
    }

    fn validateInlineCompositeTag(
        self: *const Message,
        segment_id: u32,
        elements_offset: usize,
        tag_word: u64,
        remaining: *usize,
        nesting: usize,
    ) anyerror!void {
        const element_count_signed = decodeOffsetWords(tag_word);
        if (element_count_signed < 0) return error.InvalidInlineCompositePointer;
        const element_count = @as(u32, @intCast(element_count_signed));
        const data_words = @as(u16, @truncate((tag_word >> 32) & 0xFFFF));
        const pointer_words = @as(u16, @truncate((tag_word >> 48) & 0xFFFF));

        const words_per_element = @as(u32, data_words) + @as(u32, pointer_words);
        const total_words_u64 = @as(u64, element_count) * @as(u64, words_per_element);
        if (total_words_u64 > std.math.maxInt(usize) / 8) return error.ListTooLarge;
        const total_words = @as(usize, @intCast(total_words_u64));

        const segment = self.segments[segment_id];
        const total_bytes = total_words * 8;
        if (elements_offset > segment.len) return error.OutOfBounds;
        if (total_bytes > segment.len - elements_offset) return error.OutOfBounds;

        try consumeWords(remaining, total_words);

        if (pointer_words == 0 or element_count == 0) return;
        const element_stride = @as(usize, words_per_element) * 8;
        var element_index: u32 = 0;
        while (element_index < element_count) : (element_index += 1) {
            const element_base = elements_offset + @as(usize, element_index) * element_stride;
            const pointer_section = element_base + @as(usize, data_words) * 8;
            var ptr_index: u16 = 0;
            while (ptr_index < pointer_words) : (ptr_index += 1) {
                const ptr_pos = pointer_section + @as(usize, ptr_index) * 8;
                const word = std.mem.readInt(u64, segment[ptr_pos..][0..8], .little);
                try self.validatePointer(segment_id, ptr_pos, word, remaining, nesting);
            }
        }
    }

    /// Return a reader for the root struct of this message.
    ///
    /// The root pointer is always at offset 0 in segment 0.
    pub fn getRootStruct(self: *const Message) !StructReader {
        if (self.segments.len == 0) return error.EmptyMessage;

        // The root is always at offset 0 in segment 0
        // For our simple case, we'll treat the first segment as the struct data directly
        const segment = self.segments[0];
        if (segment.len < 8) return error.TruncatedMessage;

        // Read the root pointer
        const root_pointer = std.mem.readInt(u64, segment[0..8], .little);

        return try self.resolveStructPointer(0, 0, root_pointer);
    }

    /// Return a type-erased pointer reader for the root of this message.
    pub fn getRootAnyPointer(self: *const Message) !AnyPointerReader {
        if (self.segments.len == 0) return error.EmptyMessage;
        const segment = self.segments[0];
        if (segment.len < 8) return error.TruncatedMessage;
        const root_pointer = std.mem.readInt(u64, segment[0..8], .little);
        return .{
            .message = self,
            .segment_id = 0,
            .pointer_pos = 0,
            .pointer_word = root_pointer,
        };
    }
};

/// Zero-copy reader for a Cap'n Proto struct within a `Message`.
///
/// All read methods that access the data section return the type's default
/// value (zero/false/"") when the requested offset falls outside the struct's
/// data section. This is by design per the Cap'n Proto spec: it enables
/// schema evolution so that fields added in newer schemas read as their
/// default from messages built with older schemas. Use the `*Strict` variants
/// when an out-of-bounds access should be treated as an error.
pub const StructReader = struct {
    message: *const Message,
    segment_id: u32,
    offset: usize,
    data_size: u16,
    pointer_count: u16,

    pub fn getDataSection(self: StructReader) []const u8 {
        const segment = self.message.segments[self.segment_id];
        const start = self.offset;
        const end = start + @as(usize, self.data_size) * 8;
        if (end > segment.len) return &[_]u8{};
        return segment[start..end];
    }

    pub fn getPointerSection(self: StructReader) []const u8 {
        const segment = self.message.segments[self.segment_id];
        const start = self.offset + @as(usize, self.data_size) * 8;
        const end = start + @as(usize, self.pointer_count) * 8;
        if (end > segment.len) return &[_]u8{};
        return segment[start..end];
    }

    pub fn isPointerNull(self: StructReader, pointer_index: usize) bool {
        const pointers = self.getPointerSection();
        const pointer_offset = pointer_index * 8;
        if (pointer_offset + 8 > pointers.len) return true;
        const pointer_word = std.mem.readInt(u64, pointers[pointer_offset..][0..8], .little);
        return pointer_word == 0;
    }

    /// Read a u64 from the struct's data section at the given byte offset.
    ///
    /// Returns 0 if `byte_offset` falls outside the data section. This is
    /// intentional per the Cap'n Proto spec: reading past the end of a struct's
    /// data section yields the default value (zero), which enables schema
    /// evolution — a field added in a newer schema reads as zero from messages
    /// built with an older schema that had a smaller data section.
    ///
    /// Use `readU64Strict` when an out-of-bounds access should be treated as
    /// an error (e.g. protocol-internal parsing where the field must exist).
    pub fn readU64(self: StructReader, byte_offset: usize) u64 {
        const data = self.getDataSection();
        if (byte_offset + 8 > data.len) return 0;
        return std.mem.readInt(u64, data[byte_offset..][0..8], .little);
    }

    /// Strict variant of `readU64` that returns `error.OutOfBounds` instead of
    /// the default value when the byte offset falls outside the data section.
    /// Intended for protocol-internal parsing where the field is required.
    pub fn readU64Strict(self: StructReader, byte_offset: usize) error{OutOfBounds}!u64 {
        const data = self.getDataSection();
        if (byte_offset + 8 > data.len) return error.OutOfBounds;
        return std.mem.readInt(u64, data[byte_offset..][0..8], .little);
    }

    /// Read a u32 from the struct's data section at the given byte offset.
    ///
    /// Returns 0 if `byte_offset` falls outside the data section. This is
    /// intentional per the Cap'n Proto spec for schema evolution compatibility.
    /// See `readU64` for details.
    ///
    /// Use `readU32Strict` when an out-of-bounds access should be treated as
    /// an error.
    pub fn readU32(self: StructReader, byte_offset: usize) u32 {
        const data = self.getDataSection();
        if (byte_offset + 4 > data.len) return 0;
        return std.mem.readInt(u32, data[byte_offset..][0..4], .little);
    }

    /// Strict variant of `readU32` that returns `error.OutOfBounds` instead of
    /// the default value when the byte offset falls outside the data section.
    /// Intended for protocol-internal parsing where the field is required.
    pub fn readU32Strict(self: StructReader, byte_offset: usize) error{OutOfBounds}!u32 {
        const data = self.getDataSection();
        if (byte_offset + 4 > data.len) return error.OutOfBounds;
        return std.mem.readInt(u32, data[byte_offset..][0..4], .little);
    }

    /// Read a u16 from the struct's data section at the given byte offset.
    ///
    /// Returns 0 if `byte_offset` falls outside the data section. This is
    /// intentional per the Cap'n Proto spec for schema evolution compatibility.
    /// See `readU64` for details.
    ///
    /// Use `readU16Strict` when an out-of-bounds access should be treated as
    /// an error.
    pub fn readU16(self: StructReader, byte_offset: usize) u16 {
        const data = self.getDataSection();
        if (byte_offset + 2 > data.len) return 0;
        return std.mem.readInt(u16, data[byte_offset..][0..2], .little);
    }

    /// Strict variant of `readU16` that returns `error.OutOfBounds` instead of
    /// the default value when the byte offset falls outside the data section.
    /// Intended for protocol-internal parsing where the field is required.
    pub fn readU16Strict(self: StructReader, byte_offset: usize) error{OutOfBounds}!u16 {
        const data = self.getDataSection();
        if (byte_offset + 2 > data.len) return error.OutOfBounds;
        return std.mem.readInt(u16, data[byte_offset..][0..2], .little);
    }

    /// Read a u8 from the struct's data section at the given byte offset.
    ///
    /// Returns 0 if `byte_offset` falls outside the data section. This is
    /// intentional per the Cap'n Proto spec for schema evolution compatibility.
    /// See `readU64` for details.
    ///
    /// Use `readU8Strict` when an out-of-bounds access should be treated as
    /// an error.
    pub fn readU8(self: StructReader, byte_offset: usize) u8 {
        const data = self.getDataSection();
        if (byte_offset >= data.len) return 0;
        return data[byte_offset];
    }

    /// Strict variant of `readU8` that returns `error.OutOfBounds` instead of
    /// the default value when the byte offset falls outside the data section.
    /// Intended for protocol-internal parsing where the field is required.
    pub fn readU8Strict(self: StructReader, byte_offset: usize) error{OutOfBounds}!u8 {
        const data = self.getDataSection();
        if (byte_offset >= data.len) return error.OutOfBounds;
        return data[byte_offset];
    }

    /// Read a boolean from the struct's data section at the given byte and bit offset.
    ///
    /// Returns false if `byte_offset` falls outside the data section. This is
    /// intentional per the Cap'n Proto spec for schema evolution compatibility.
    /// See `readU64` for details.
    ///
    /// Use `readBoolStrict` when an out-of-bounds access should be treated as
    /// an error.
    pub fn readBool(self: StructReader, byte_offset: usize, bit_offset: u3) bool {
        const data = self.getDataSection();
        if (byte_offset >= data.len) return false;
        const byte = data[byte_offset];
        return (byte & (@as(u8, 1) << bit_offset)) != 0;
    }

    /// Strict variant of `readBool` that returns `error.OutOfBounds` instead of
    /// the default value when the byte offset falls outside the data section.
    /// Intended for protocol-internal parsing where the field is required.
    pub fn readBoolStrict(self: StructReader, byte_offset: usize, bit_offset: u3) error{OutOfBounds}!bool {
        const data = self.getDataSection();
        if (byte_offset >= data.len) return error.OutOfBounds;
        const byte = data[byte_offset];
        return (byte & (@as(u8, 1) << bit_offset)) != 0;
    }

    /// Read a union discriminant (which field is set)
    pub fn readUnionDiscriminant(self: StructReader, byte_offset: usize) u16 {
        return self.readU16(byte_offset);
    }

    pub fn readStructList(self: StructReader, pointer_index: usize) !StructListReader {
        const pointers = self.getPointerSection();
        const pointer_offset = pointer_index * 8;
        if (pointer_offset + 8 > pointers.len) return error.OutOfBounds;

        const pointer_data = pointers[pointer_offset..][0..8];
        const pointer_word = std.mem.readInt(u64, pointer_data, .little);
        if (pointer_word == 0) return error.InvalidPointer;

        const absolute_pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_offset;
        const list = try self.message.resolveInlineCompositeList(self.segment_id, absolute_pointer_pos, pointer_word);

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
            .data_words = list.data_words,
            .pointer_words = list.pointer_words,
        };
    }

    fn resolveListPointerAt(self: StructReader, pointer_index: usize) !Message.ResolvedListPointer {
        const pointers = self.getPointerSection();
        const pointer_offset = pointer_index * 8;
        if (pointer_offset + 8 > pointers.len) return error.OutOfBounds;

        const pointer_data = pointers[pointer_offset..][0..8];
        const pointer_word = std.mem.readInt(u64, pointer_data, .little);
        if (pointer_word == 0) return error.InvalidPointer;

        const absolute_pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_offset;
        return self.message.resolveListPointer(self.segment_id, absolute_pointer_pos, pointer_word);
    }

    pub fn readTextList(self: StructReader, pointer_index: usize) !TextListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 6) return error.InvalidPointer;

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readPointerList(self: StructReader, pointer_index: usize) !PointerListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 6) return error.InvalidPointer;

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readStruct(self: StructReader, pointer_index: usize) !StructReader {
        const pointers = self.getPointerSection();
        const pointer_offset = pointer_index * 8;
        if (pointer_offset + 8 > pointers.len) return error.OutOfBounds;

        const pointer_data = pointers[pointer_offset..][0..8];
        const pointer_word = std.mem.readInt(u64, pointer_data, .little);
        if (pointer_word == 0) return error.InvalidPointer;

        const absolute_pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_offset;
        return self.message.resolveStructPointer(self.segment_id, absolute_pointer_pos, pointer_word);
    }

    pub fn readAnyPointer(self: StructReader, pointer_index: usize) !AnyPointerReader {
        const pointers = self.getPointerSection();
        const pointer_offset = pointer_index * 8;
        if (pointer_offset + 8 > pointers.len) return error.OutOfBounds;

        const pointer_data = pointers[pointer_offset..][0..8];
        const pointer_word = std.mem.readInt(u64, pointer_data, .little);
        const absolute_pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_offset;

        return .{
            .message = self.message,
            .segment_id = self.segment_id,
            .pointer_pos = absolute_pointer_pos,
            .pointer_word = pointer_word,
        };
    }

    pub fn readCapability(self: StructReader, pointer_index: usize) !Capability {
        const any = try self.readAnyPointer(pointer_index);
        return any.getCapability();
    }

    pub fn readData(self: StructReader, pointer_index: usize) ![]const u8 {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 2) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;

        return segment[list.content_offset .. list.content_offset + total_bytes];
    }

    pub fn readU8List(self: StructReader, pointer_index: usize) !U8ListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 2) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readI8List(self: StructReader, pointer_index: usize) !I8ListReader {
        const list = try self.readU8List(pointer_index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readU16List(self: StructReader, pointer_index: usize) !U16ListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 3) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readI16List(self: StructReader, pointer_index: usize) !I16ListReader {
        const list = try self.readU16List(pointer_index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readU32List(self: StructReader, pointer_index: usize) !U32ListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 4) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readI32List(self: StructReader, pointer_index: usize) !I32ListReader {
        const list = try self.readU32List(pointer_index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readF32List(self: StructReader, pointer_index: usize) !F32ListReader {
        const list = try self.readU32List(pointer_index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readU64List(self: StructReader, pointer_index: usize) !U64ListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 5) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readI64List(self: StructReader, pointer_index: usize) !I64ListReader {
        const list = try self.readU64List(pointer_index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readF64List(self: StructReader, pointer_index: usize) !F64ListReader {
        const list = try self.readU64List(pointer_index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readBoolList(self: StructReader, pointer_index: usize) !BoolListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 1) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    pub fn readVoidList(self: StructReader, pointer_index: usize) !VoidListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 0) return error.InvalidPointer;
        return .{ .element_count = list.element_count };
    }

    /// Read a text (string) field from the struct's pointer section.
    ///
    /// Returns an empty string `""` when the pointer index is out of bounds or
    /// when the pointer is null. This follows the Cap'n Proto convention where
    /// absent/null text fields default to the empty string for schema evolution
    /// compatibility.
    pub fn readText(self: StructReader, pointer_index: usize) ![]const u8 {
        const pointers = self.getPointerSection();
        const pointer_offset = pointer_index * 8;
        if (pointer_offset + 8 > pointers.len) return "";

        const pointer_data = pointers[pointer_offset..][0..8];
        const pointer_word = std.mem.readInt(u64, pointer_data, .little);

        // Check if null pointer
        if (pointer_word == 0) return "";

        const absolute_pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_offset;
        const list = try self.message.resolveListPointer(self.segment_id, absolute_pointer_pos, pointer_word);

        // Text should be byte-sized elements
        if (list.element_size != 2) return error.InvalidTextPointer;

        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + list.element_count > segment.len) return error.OutOfBounds;

        // Text includes null terminator, so return without it
        const text_data = segment[list.content_offset .. list.content_offset + list.element_count];
        if (text_data.len > 0 and text_data[text_data.len - 1] == 0) {
            return text_data[0 .. text_data.len - 1];
        }
        return text_data;
    }

    /// Read a text field with strict UTF-8 validation.
    ///
    /// Like `readText`, but returns `error.InvalidUtf8` when the text
    /// contains ill-formed UTF-8 byte sequences.
    pub fn readTextStrict(self: StructReader, pointer_index: usize) ![]const u8 {
        const text = try self.readText(pointer_index);
        if (text.len > 0 and !std.unicode.utf8ValidateSlice(text)) {
            return error.InvalidUtf8;
        }
        return text;
    }
};

const list_reader_defs = list_reader_module.define(
    Message,
    StructReader,
    Capability,
    InlineCompositeList,
    listContentBytes,
    listContentWords,
    decodeCapabilityPointer,
);

/// Zero-copy reader for a list of structs (inline-composite encoding).
pub const StructListReader = list_reader_defs.StructListReader;
/// Zero-copy reader for a list of text (pointer) elements.
pub const TextListReader = list_reader_defs.TextListReader;
pub const U8ListReader = list_reader_defs.U8ListReader;
pub const I8ListReader = list_reader_defs.I8ListReader;
pub const U16ListReader = list_reader_defs.U16ListReader;
pub const I16ListReader = list_reader_defs.I16ListReader;
pub const U32ListReader = list_reader_defs.U32ListReader;
pub const I32ListReader = list_reader_defs.I32ListReader;
pub const F32ListReader = list_reader_defs.F32ListReader;
pub const U64ListReader = list_reader_defs.U64ListReader;
pub const I64ListReader = list_reader_defs.I64ListReader;
pub const F64ListReader = list_reader_defs.F64ListReader;
pub const BoolListReader = list_reader_defs.BoolListReader;
pub const VoidListReader = list_reader_defs.VoidListReader;
pub const PointerListReader = list_reader_defs.PointerListReader;
const any_pointer_reader_defs = any_pointer_reader_module.define(
    Message,
    StructReader,
    PointerListReader,
    InlineCompositeList,
    Capability,
    listContentBytes,
    decodeCapabilityPointer,
);

/// Zero-copy reader for a type-erased pointer within a `Message`.
///
/// Can be inspected or cast to struct, list, text, data, or capability readers.
pub const AnyPointerReader = any_pointer_reader_defs.AnyPointerReader;

/// Builder for a type-erased pointer slot within a `MessageBuilder`.
///
/// Can be used to write a struct, list, text, data, or capability into
/// any pointer position regardless of the expected schema type.
pub const AnyPointerBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    pointer_pos: usize,

    pub fn setNull(self: AnyPointerBuilder) !void {
        return any_pointer_builder_module.setNull(self.builder, self.segment_id, self.pointer_pos);
    }

    pub fn setText(self: AnyPointerBuilder, text: []const u8) !void {
        return any_pointer_builder_module.setText(self.builder, self.segment_id, self.pointer_pos, text);
    }

    pub fn setData(self: AnyPointerBuilder, data: []const u8) !void {
        return any_pointer_builder_module.setData(self.builder, self.segment_id, self.pointer_pos, data);
    }

    pub fn setCapability(self: AnyPointerBuilder, cap: Capability) !void {
        return any_pointer_builder_module.setCapability(
            self.builder,
            self.segment_id,
            self.pointer_pos,
            cap,
            makeCapabilityPointer,
        );
    }

    pub fn initStruct(self: AnyPointerBuilder, data_words: u16, pointer_words: u16) !StructBuilder {
        return self.builder.writeStructPointer(self.segment_id, self.pointer_pos, data_words, pointer_words, self.segment_id);
    }

    pub fn initStructList(self: AnyPointerBuilder, element_count: u32, data_words: u16, pointer_words: u16) !StructListBuilder {
        return self.builder.writeStructListPointer(
            self.segment_id,
            self.pointer_pos,
            element_count,
            data_words,
            pointer_words,
            self.segment_id,
            self.segment_id,
        );
    }

    pub fn initPointerList(self: AnyPointerBuilder, element_count: u32) !PointerListBuilder {
        const offset = try any_pointer_builder_module.initList(self.builder, self.segment_id, self.pointer_pos, 6, element_count);
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = offset,
            .element_count = element_count,
        };
    }

    fn initList(self: AnyPointerBuilder, element_size: u3, element_count: u32) !struct { offset: usize } {
        const offset = try any_pointer_builder_module.initList(self.builder, self.segment_id, self.pointer_pos, element_size, element_count);
        return .{ .offset = offset };
    }

    pub fn initVoidList(self: AnyPointerBuilder, element_count: u32) !VoidListBuilder {
        _ = try self.initList(0, element_count);
        return .{ .element_count = element_count };
    }

    pub fn initU8List(self: AnyPointerBuilder, element_count: u32) !U8ListBuilder {
        const info = try self.initList(2, element_count);
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initU16List(self: AnyPointerBuilder, element_count: u32) !U16ListBuilder {
        const info = try self.initList(3, element_count);
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initU32List(self: AnyPointerBuilder, element_count: u32) !U32ListBuilder {
        const info = try self.initList(4, element_count);
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initU64List(self: AnyPointerBuilder, element_count: u32) !U64ListBuilder {
        const info = try self.initList(5, element_count);
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initBoolList(self: AnyPointerBuilder, element_count: u32) !BoolListBuilder {
        const info = try self.initList(1, element_count);
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initF32List(self: AnyPointerBuilder, element_count: u32) !F32ListBuilder {
        const info = try self.initList(4, element_count);
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initF64List(self: AnyPointerBuilder, element_count: u32) !F64ListBuilder {
        const info = try self.initList(5, element_count);
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }
};

/// Builder for constructing Cap'n Proto messages in memory.
///
/// Typical lifecycle: `init` -> `allocateStruct` (root) -> write fields via
/// `StructBuilder` -> `toBytes` to serialize the framed wire format ->
/// `deinit` to release all segment memory. The builder owns all allocated
/// segment storage.
pub const MessageBuilder = struct {
    allocator: std.mem.Allocator,
    segments: std.ArrayList(std.ArrayList(u8)),
    const initial_segment_capacity_bytes: usize = 1024;

    /// Create a new, empty message builder.
    pub fn init(allocator: std.mem.Allocator) MessageBuilder {
        return .{
            .allocator = allocator,
            .segments = std.ArrayList(std.ArrayList(u8)){},
        };
    }

    /// Free all segment storage owned by this builder.
    pub fn deinit(self: *MessageBuilder) void {
        for (self.segments.items) |*segment| {
            segment.deinit(self.allocator);
        }
        self.segments.deinit(self.allocator);
    }

    fn createSegmentWithCapacity(self: *MessageBuilder, min_capacity: usize) !u32 {
        const id: u32 = @intCast(self.segments.items.len);
        try self.segments.append(self.allocator, std.ArrayList(u8){});
        if (min_capacity > 0) {
            try self.segments.items[id].ensureTotalCapacity(self.allocator, min_capacity);
        }
        return id;
    }

    pub fn createSegment(self: *MessageBuilder) !u32 {
        return self.createSegmentWithCapacity(0);
    }

    fn getSegment(self: *MessageBuilder, segment_id: u32) !*std.ArrayList(u8) {
        if (segment_id >= self.segments.items.len) return error.InvalidSegmentId;
        return &self.segments.items[segment_id];
    }

    /// Initialize the root pointer slot, returning a type-erased builder for it.
    pub fn initRootAnyPointer(self: *MessageBuilder) !AnyPointerBuilder {
        if (self.segments.items.len == 0) {
            _ = try self.createSegmentWithCapacity(initial_segment_capacity_bytes);
        }
        const segment = &self.segments.items[0];
        if (segment.items.len < 8) {
            const missing = 8 - segment.items.len;
            const bytes = try segment.addManyAsSlice(self.allocator, missing);
            @memset(bytes, 0);
        }
        return .{
            .builder = self,
            .segment_id = 0,
            .pointer_pos = 0,
        };
    }

    fn allocateBytes(self: *MessageBuilder, segment_id: u32, byte_count: usize) !usize {
        const segment = try self.getSegment(segment_id);
        const offset = segment.items.len;
        const bytes = try segment.addManyAsSlice(self.allocator, byte_count);
        @memset(bytes, 0);
        return offset;
    }

    pub fn allocateStructInSegment(self: *MessageBuilder, segment_id: u32, data_words: u16, pointer_words: u16) !StructBuilder {
        if (self.segments.items.len == 0) {
            _ = try self.createSegmentWithCapacity(initial_segment_capacity_bytes);
        }
        while (self.segments.items.len <= segment_id) {
            _ = try self.createSegment();
        }

        const total_words = @as(usize, data_words) + @as(usize, pointer_words);
        const total_bytes = total_words * 8;
        const offset = try self.allocateBytes(segment_id, total_bytes);

        return StructBuilder{
            .builder = self,
            .segment_id = segment_id,
            .offset = offset,
            .data_size = data_words,
            .pointer_count = pointer_words,
        };
    }

    pub fn allocateRootStructInSegment(self: *MessageBuilder, segment_id: u32, data_words: u16, pointer_words: u16) !StructBuilder {
        if (segment_id == 0) return self.allocateStruct(data_words, pointer_words);

        if (self.segments.items.len == 0) {
            _ = try self.createSegmentWithCapacity(initial_segment_capacity_bytes);
        }

        const root_segment = &self.segments.items[0];
        if (root_segment.items.len != 0) return error.RootAlreadyAllocated;

        while (self.segments.items.len <= segment_id) {
            _ = try self.createSegment();
        }

        const target_segment = &self.segments.items[segment_id];
        const landing_pad_pos = target_segment.items.len;
        {
            const landing_pad = try target_segment.addManyAsSlice(self.allocator, 8);
            @memset(landing_pad, 0);
        }

        const total_words = @as(usize, data_words) + @as(usize, pointer_words);
        const total_bytes = total_words * 8;
        const struct_offset = target_segment.items.len;
        {
            const struct_storage = try target_segment.addManyAsSlice(self.allocator, total_bytes);
            @memset(struct_storage, 0);
        }

        const struct_ptr = makeStructPointer(0, data_words, pointer_words);
        std.mem.writeInt(u64, target_segment.items[landing_pad_pos..][0..8], struct_ptr, .little);

        {
            const root_pointer = try root_segment.addManyAsSlice(self.allocator, 8);
            @memset(root_pointer, 0);
        }
        const far_ptr = makeFarPointer(false, @as(u32, @intCast(landing_pad_pos / 8)), segment_id);
        std.mem.writeInt(u64, root_segment.items[0..8], far_ptr, .little);

        return StructBuilder{
            .builder = self,
            .segment_id = segment_id,
            .offset = struct_offset,
            .data_size = data_words,
            .pointer_count = pointer_words,
        };
    }

    pub fn writeTextPointer(
        self: *MessageBuilder,
        pointer_segment_id: u32,
        pointer_pos: usize,
        text: []const u8,
        target_segment_id: u32,
    ) !void {
        if (pointer_segment_id >= self.segments.items.len) return error.InvalidSegmentId;
        if (target_segment_id >= self.segments.items.len) return error.InvalidSegmentId;

        const pointer_segment = &self.segments.items[pointer_segment_id];
        if (pointer_pos + 8 > pointer_segment.items.len) return error.OutOfBounds;

        const target_segment = &self.segments.items[target_segment_id];
        const padding = (8 - ((text.len + 1) % 8)) % 8;
        const text_offset = target_segment.items.len;
        {
            const storage = try target_segment.addManyAsSlice(self.allocator, text.len + 1 + padding);
            @memcpy(storage[0..text.len], text);
            storage[text.len] = 0;
            if (padding != 0) {
                @memset(storage[text.len + 1 ..], 0);
            }
        }

        if (pointer_segment_id == target_segment_id) {
            const relative_offset = @as(i32, @intCast(@divTrunc(
                @as(isize, @intCast(text_offset)) - @as(isize, @intCast(pointer_pos)) - 8,
                8,
            )));
            const pointer = makeListPointer(relative_offset, 2, @as(u32, @intCast(text.len + 1)));
            std.mem.writeInt(u64, pointer_segment.items[pointer_pos..][0..8], pointer, .little);
            return;
        }

        const landing_pad_pos = target_segment.items.len;
        {
            const landing_pad = try target_segment.addManyAsSlice(self.allocator, 8);
            @memset(landing_pad, 0);
        }

        const list_offset = @as(i32, @intCast(@divTrunc(
            @as(isize, @intCast(text_offset)) - @as(isize, @intCast(landing_pad_pos)) - 8,
            8,
        )));
        const list_ptr = makeListPointer(list_offset, 2, @as(u32, @intCast(text.len + 1)));
        std.mem.writeInt(u64, target_segment.items[landing_pad_pos..][0..8], list_ptr, .little);

        const far_ptr = makeFarPointer(false, @as(u32, @intCast(landing_pad_pos / 8)), target_segment_id);
        std.mem.writeInt(u64, pointer_segment.items[pointer_pos..][0..8], far_ptr, .little);
    }

    pub fn writeStructPointer(
        self: *MessageBuilder,
        pointer_segment_id: u32,
        pointer_pos: usize,
        data_words: u16,
        pointer_words: u16,
        target_segment_id: u32,
    ) !StructBuilder {
        if (pointer_segment_id >= self.segments.items.len) return error.InvalidSegmentId;
        if (target_segment_id >= self.segments.items.len) return error.InvalidSegmentId;

        const pointer_segment = &self.segments.items[pointer_segment_id];
        if (pointer_pos + 8 > pointer_segment.items.len) return error.OutOfBounds;

        const target_segment = &self.segments.items[target_segment_id];
        const total_words = @as(usize, data_words) + @as(usize, pointer_words);
        const total_bytes = total_words * 8;

        if (pointer_segment_id == target_segment_id) {
            const struct_offset = target_segment.items.len;
            try target_segment.appendNTimes(self.allocator, 0, total_bytes);

            const relative_offset = @as(i32, @intCast(@divTrunc(
                @as(isize, @intCast(struct_offset)) - @as(isize, @intCast(pointer_pos)) - 8,
                8,
            )));
            const pointer = makeStructPointer(relative_offset, data_words, pointer_words);
            std.mem.writeInt(u64, pointer_segment.items[pointer_pos..][0..8], pointer, .little);

            return StructBuilder{
                .builder = self,
                .segment_id = target_segment_id,
                .offset = struct_offset,
                .data_size = data_words,
                .pointer_count = pointer_words,
            };
        }

        const landing_pad_pos = target_segment.items.len;
        try target_segment.appendNTimes(self.allocator, 0, 8);

        const struct_offset = target_segment.items.len;
        try target_segment.appendNTimes(self.allocator, 0, total_bytes);

        const struct_ptr = makeStructPointer(0, data_words, pointer_words);
        std.mem.writeInt(u64, target_segment.items[landing_pad_pos..][0..8], struct_ptr, .little);

        const far_ptr = makeFarPointer(false, @as(u32, @intCast(landing_pad_pos / 8)), target_segment_id);
        std.mem.writeInt(u64, pointer_segment.items[pointer_pos..][0..8], far_ptr, .little);

        return StructBuilder{
            .builder = self,
            .segment_id = target_segment_id,
            .offset = struct_offset,
            .data_size = data_words,
            .pointer_count = pointer_words,
        };
    }

    pub fn writeListPointer(
        self: *MessageBuilder,
        pointer_segment_id: u32,
        pointer_pos: usize,
        element_size: u3,
        element_count: u32,
        target_segment_id: u32,
    ) !usize {
        if (pointer_segment_id >= self.segments.items.len) return error.InvalidSegmentId;
        if (target_segment_id >= self.segments.items.len) return error.InvalidSegmentId;

        const pointer_segment = &self.segments.items[pointer_segment_id];
        if (pointer_pos + 8 > pointer_segment.items.len) return error.OutOfBounds;

        const target_segment = &self.segments.items[target_segment_id];
        const landing_pad_pos = if (pointer_segment_id == target_segment_id) null else target_segment.items.len;
        if (landing_pad_pos) |_| {
            try target_segment.appendNTimes(self.allocator, 0, 8);
        }

        const alignment = target_segment.items.len % 8;
        if (alignment != 0) {
            try target_segment.appendNTimes(self.allocator, 0, 8 - alignment);
        }

        const total_bytes = try listContentBytes(element_size, element_count);
        const list_offset = target_segment.items.len;
        try target_segment.appendNTimes(self.allocator, 0, total_bytes);
        const padding = (8 - (total_bytes % 8)) % 8;
        if (padding != 0) {
            try target_segment.appendNTimes(self.allocator, 0, padding);
        }

        if (pointer_segment_id == target_segment_id) {
            const rel_offset = @as(i32, @intCast(@divTrunc(
                @as(isize, @intCast(list_offset)) - @as(isize, @intCast(pointer_pos)) - 8,
                8,
            )));
            const list_ptr = makeListPointer(rel_offset, element_size, element_count);
            std.mem.writeInt(u64, pointer_segment.items[pointer_pos..][0..8], list_ptr, .little);
        } else {
            const landing_pos = landing_pad_pos.?;
            const rel_offset = @as(i32, @intCast(@divTrunc(
                @as(isize, @intCast(list_offset)) - @as(isize, @intCast(landing_pos)) - 8,
                8,
            )));
            const list_ptr = makeListPointer(rel_offset, element_size, element_count);
            std.mem.writeInt(u64, target_segment.items[landing_pos..][0..8], list_ptr, .little);

            const far_ptr = makeFarPointer(false, @as(u32, @intCast(landing_pos / 8)), target_segment_id);
            std.mem.writeInt(u64, pointer_segment.items[pointer_pos..][0..8], far_ptr, .little);
        }

        return list_offset;
    }

    pub fn writeStructListPointer(
        self: *MessageBuilder,
        pointer_segment_id: u32,
        pointer_pos: usize,
        element_count: u32,
        data_words: u16,
        pointer_words: u16,
        landing_segment_id: u32,
        content_segment_id: u32,
    ) !StructListBuilder {
        if (element_count > @as(u32, @intCast(std.math.maxInt(i32)))) return error.ElementCountTooLarge;

        const words_per_element = @as(u32, data_words) + @as(u32, pointer_words);
        const total_words_u64 = @as(u64, element_count) * @as(u64, words_per_element);
        if (total_words_u64 > std.math.maxInt(u32)) return error.ListTooLarge;
        const total_words = @as(u32, @intCast(total_words_u64));
        const total_bytes = @as(usize, total_words) * 8;

        while (self.segments.items.len <= landing_segment_id or self.segments.items.len <= content_segment_id) {
            _ = try self.createSegment();
        }

        if (pointer_segment_id >= self.segments.items.len) return error.InvalidSegmentId;
        const source_segment = &self.segments.items[pointer_segment_id];
        if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

        if (landing_segment_id == content_segment_id) {
            const target_segment = &self.segments.items[landing_segment_id];
            const landing_pad_pos = if (pointer_segment_id == landing_segment_id) null else target_segment.items.len;
            if (landing_pad_pos) |_| {
                try target_segment.appendNTimes(self.allocator, 0, 8);
            }

            const tag_offset = target_segment.items.len;
            try target_segment.appendNTimes(self.allocator, 0, 8);

            const elements_offset = target_segment.items.len;
            try target_segment.appendNTimes(self.allocator, 0, total_bytes);

            const tag_word = makeStructPointer(@as(i32, @intCast(element_count)), data_words, pointer_words);
            std.mem.writeInt(u64, target_segment.items[tag_offset..][0..8], tag_word, .little);

            if (pointer_segment_id == landing_segment_id) {
                const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(tag_offset)) - @as(isize, @intCast(pointer_pos)) - 8, 8)));
                const list_ptr = makeListPointer(rel_offset, 7, total_words);
                std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], list_ptr, .little);
            } else {
                const landing_pos = landing_pad_pos.?;
                const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(tag_offset)) - @as(isize, @intCast(landing_pos)) - 8, 8)));
                const list_ptr = makeListPointer(rel_offset, 7, total_words);
                std.mem.writeInt(u64, target_segment.items[landing_pos..][0..8], list_ptr, .little);

                const far_ptr = makeFarPointer(false, @as(u32, @intCast(landing_pos / 8)), landing_segment_id);
                std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], far_ptr, .little);
            }

            return StructListBuilder{
                .builder = self,
                .segment_id = landing_segment_id,
                .elements_offset = elements_offset,
                .element_count = element_count,
                .data_words = data_words,
                .pointer_words = pointer_words,
            };
        }

        const landing_segment = &self.segments.items[landing_segment_id];
        const landing_pad_pos = landing_segment.items.len;
        try landing_segment.appendNTimes(self.allocator, 0, 16);

        const content_segment = &self.segments.items[content_segment_id];
        const elements_offset = content_segment.items.len;
        try content_segment.appendNTimes(self.allocator, 0, total_bytes);

        const landing_far = makeFarPointer(false, @as(u32, @intCast(elements_offset / 8)), content_segment_id);
        std.mem.writeInt(u64, landing_segment.items[landing_pad_pos..][0..8], landing_far, .little);

        const tag_word = makeStructPointer(@as(i32, @intCast(element_count)), data_words, pointer_words);
        std.mem.writeInt(u64, landing_segment.items[landing_pad_pos + 8 ..][0..8], tag_word, .little);

        const far_ptr = makeFarPointer(true, @as(u32, @intCast(landing_pad_pos / 8)), landing_segment_id);
        std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], far_ptr, .little);

        return StructListBuilder{
            .builder = self,
            .segment_id = content_segment_id,
            .elements_offset = elements_offset,
            .element_count = element_count,
            .data_words = data_words,
            .pointer_words = pointer_words,
        };
    }

    /// Allocate the root struct in segment 0 (on first call) or append a new
    /// struct to segment 0 (on subsequent calls). Returns a `StructBuilder`
    /// positioned at the allocated region.
    pub fn allocateStruct(self: *MessageBuilder, data_words: u16, pointer_words: u16) !StructBuilder {
        // Ensure we have at least one segment
        if (self.segments.items.len == 0) {
            _ = try self.createSegmentWithCapacity(initial_segment_capacity_bytes);
        }

        const segment = &self.segments.items[0];

        // Reserve space for root pointer if this is the first allocation
        if (segment.items.len == 0) {
            // Write root pointer
            const total_words = @as(usize, data_words) + @as(usize, pointer_words);
            const total_bytes = total_words * 8;

            // Root pointer points to offset 0 (next word after the pointer)
            var root_pointer: u64 = 0; // Struct pointer tag
            root_pointer |= @as(u64, 0) << 2; // Offset = 0
            root_pointer |= @as(u64, data_words) << 32;
            root_pointer |= @as(u64, pointer_words) << 48;

            // Reserve the whole root allocation in one growth step.
            try segment.ensureUnusedCapacity(self.allocator, 8 + total_bytes);

            // Write root pointer
            const root_slot = try segment.addManyAsSlice(self.allocator, 8);
            @memset(root_slot, 0);
            std.mem.writeInt(u64, segment.items[0..8], root_pointer, .little);

            // Allocate struct data
            const struct_offset = segment.items.len;
            const struct_storage = try segment.addManyAsSlice(self.allocator, total_bytes);
            @memset(struct_storage, 0);

            return StructBuilder{
                .builder = self,
                .segment_id = 0,
                .offset = struct_offset,
                .data_size = data_words,
                .pointer_count = pointer_words,
            };
        }

        // For subsequent allocations, just append
        const offset = segment.items.len;
        const total_words = @as(usize, data_words) + @as(usize, pointer_words);
        const total_bytes = total_words * 8;
        try segment.appendNTimes(self.allocator, 0, total_bytes);

        return StructBuilder{
            .builder = self,
            .segment_id = 0,
            .offset = offset,
            .data_size = data_words,
            .pointer_count = pointer_words,
        };
    }

    /// Serialize the message into the Cap'n Proto framed wire format.
    ///
    /// Returns an allocator-owned byte slice containing the segment table
    /// header followed by all segment data. The caller must free the returned
    /// slice.
    pub fn toBytes(self: *MessageBuilder) ![]const u8 {
        var result = std.ArrayList(u8){};
        errdefer result.deinit(self.allocator);

        // Ensure we have at least one segment
        if (self.segments.items.len == 0) {
            _ = try self.createSegmentWithCapacity(initial_segment_capacity_bytes);
        }

        const segment_count_usize = self.segments.items.len;
        const segment_count = @as(u32, @intCast(segment_count_usize));
        const padding_words: usize = if (segment_count_usize % 2 == 0) 1 else 0;
        const header_words = 1 + segment_count_usize + padding_words;
        const header_bytes = header_words * 4;

        var payload_bytes: usize = 0;
        for (self.segments.items) |segment| {
            payload_bytes = std.math.add(usize, payload_bytes, segment.items.len) catch return error.InvalidMessageSize;
        }

        const total_bytes = std.math.add(usize, header_bytes, payload_bytes) catch return error.InvalidMessageSize;
        try result.ensureTotalCapacity(self.allocator, total_bytes);

        // Write segment count.
        var word_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, word_buf[0..4], segment_count - 1, .little);
        result.appendSliceAssumeCapacity(&word_buf);

        // Write segment sizes
        for (self.segments.items) |segment| {
            const size_words = @as(u32, @intCast(segment.items.len / 8));
            std.mem.writeInt(u32, word_buf[0..4], size_words, .little);
            result.appendSliceAssumeCapacity(&word_buf);
        }

        // Padding to 8-byte boundary
        if (padding_words == 1) {
            result.appendSliceAssumeCapacity(&[_]u8{ 0, 0, 0, 0 });
        }

        // Write segment data
        for (self.segments.items) |segment| {
            result.appendSliceAssumeCapacity(segment.items);
        }

        return result.toOwnedSlice(self.allocator);
    }

    /// Serialize the message using Cap'n Proto packed encoding.
    ///
    /// Returns an allocator-owned byte slice. The caller must free it.
    pub fn toPackedBytes(self: *MessageBuilder) ![]const u8 {
        const bytes = try self.toBytes();
        defer self.allocator.free(bytes);
        return packPacked(self.allocator, bytes);
    }

    /// Stream the framed wire format directly to `writer` without intermediate allocation.
    pub fn writeTo(self: *MessageBuilder, writer: anytype) !void {
        if (self.segments.items.len == 0) {
            try self.segments.append(self.allocator, std.ArrayList(u8){});
        }

        const segment_count = @as(u32, @intCast(self.segments.items.len));
        var word_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, word_buf[0..4], segment_count - 1, .little);
        try writer.writeAll(&word_buf);

        for (self.segments.items) |segment| {
            const size_words = @as(u32, @intCast(segment.items.len / 8));
            std.mem.writeInt(u32, word_buf[0..4], size_words, .little);
            try writer.writeAll(&word_buf);
        }

        if (segment_count % 2 == 0) {
            try writer.writeAll(&[_]u8{ 0, 0, 0, 0 });
        }

        for (self.segments.items) |segment| {
            try writer.writeAll(segment.items);
        }
    }

    pub fn writePackedTo(self: *MessageBuilder, writer: anytype) !void {
        const packed_bytes = try self.toPackedBytes();
        defer self.allocator.free(packed_bytes);
        try writer.writeAll(packed_bytes);
    }
};

pub const StructListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,
    data_words: u16,
    pointer_words: u16,

    pub fn len(self: StructListBuilder) u32 {
        return self.element_count;
    }

    pub fn get(self: StructListBuilder, index: u32) !StructBuilder {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const stride = (@as(usize, self.data_words) + @as(usize, self.pointer_words)) * 8;
        const offset = self.elements_offset + @as(usize, index) * stride;
        return StructBuilder{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .offset = offset,
            .data_size = self.data_words,
            .pointer_count = self.pointer_words,
        };
    }
};

const list_builder_defs = list_builder_module.define(
    MessageBuilder,
);

pub const TextListBuilder = list_builder_defs.TextListBuilder;
pub const VoidListBuilder = list_builder_defs.VoidListBuilder;
pub const U8ListBuilder = list_builder_defs.U8ListBuilder;
pub const I8ListBuilder = list_builder_defs.I8ListBuilder;
pub const U16ListBuilder = list_builder_defs.U16ListBuilder;
pub const I16ListBuilder = list_builder_defs.I16ListBuilder;
pub const U32ListBuilder = list_builder_defs.U32ListBuilder;
pub const I32ListBuilder = list_builder_defs.I32ListBuilder;
pub const F32ListBuilder = list_builder_defs.F32ListBuilder;
pub const U64ListBuilder = list_builder_defs.U64ListBuilder;
pub const I64ListBuilder = list_builder_defs.I64ListBuilder;
pub const F64ListBuilder = list_builder_defs.F64ListBuilder;
pub const BoolListBuilder = list_builder_defs.BoolListBuilder;

const struct_builder_defs = struct_builder_module.define(
    MessageBuilder,
    AnyPointerBuilder,
    StructListBuilder,
    TextListBuilder,
    VoidListBuilder,
    U8ListBuilder,
    I8ListBuilder,
    U16ListBuilder,
    I16ListBuilder,
    U32ListBuilder,
    I32ListBuilder,
    F32ListBuilder,
    U64ListBuilder,
    I64ListBuilder,
    F64ListBuilder,
    BoolListBuilder,
    Capability,
    makeCapabilityPointer,
    makeListPointer,
    makeFarPointer,
);

pub const PointerListBuilder = struct_builder_defs.PointerListBuilder;

/// Builder for writing fields into a Cap'n Proto struct within a `MessageBuilder`.
///
/// Write methods that target out-of-bounds offsets silently do nothing,
/// mirroring the reader's zero-default behavior and allowing forward-compatible
/// writes when the struct was allocated with a smaller schema.
pub const StructBuilder = struct_builder_defs.StructBuilder;

const clone_defs = clone_any_pointer_module.define(
    MessageBuilder,
    AnyPointerReader,
    AnyPointerBuilder,
    StructReader,
    StructBuilder,
    StructListReader,
    listContentBytes,
    decodeCapabilityPointer,
);

/// Serialize a type-erased pointer into a standalone framed message byte slice.
pub const cloneAnyPointerToBytes = clone_defs.cloneAnyPointerToBytes;

/// Deep-copy a type-erased pointer from a reader into a builder position.
pub const cloneAnyPointer = clone_defs.cloneAnyPointer;
