const std = @import("std");

fn decodeOffsetWords(pointer_word: u64) i32 {
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
    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);

    var index: usize = 0;
    while (index < packed_bytes.len) {
        const tag = packed_bytes[index];
        index += 1;

        if (tag == 0x00) {
            try out.appendNTimes(allocator, 0, 8);
            if (index >= packed_bytes.len) return error.UnexpectedEof;
            const count = packed_bytes[index];
            index += 1;
            if (count > 0) {
                try out.appendNTimes(allocator, 0, @as(usize, count) * 8);
            }
            continue;
        }

        if (tag == 0xFF) {
            if (index + 8 > packed_bytes.len) return error.UnexpectedEof;
            try out.appendSlice(allocator, packed_bytes[index .. index + 8]);
            index += 8;
            if (index >= packed_bytes.len) return error.UnexpectedEof;
            const count = packed_bytes[index];
            index += 1;
            if (count > 0) {
                const byte_count = @as(usize, count) * 8;
                if (index + byte_count > packed_bytes.len) return error.UnexpectedEof;
                try out.appendSlice(allocator, packed_bytes[index .. index + byte_count]);
                index += byte_count;
            }
            continue;
        }

        var word: [8]u8 = .{0} ** 8;
        var bit_index: u8 = 0;
        while (bit_index < 8) : (bit_index += 1) {
            if ((tag & (@as(u8, 1) << @intCast(bit_index))) != 0) {
                if (index >= packed_bytes.len) return error.UnexpectedEof;
                word[@intCast(bit_index)] = packed_bytes[index];
                index += 1;
            }
        }
        try out.appendSlice(allocator, &word);
    }

    return out.toOwnedSlice(allocator);
}

fn packPacked(allocator: std.mem.Allocator, bytes: []const u8) ![]u8 {
    if (bytes.len % 8 != 0) return error.InvalidMessageSize;

    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);

    var index: usize = 0;
    while (index < bytes.len) {
        const word = bytes[index .. index + 8];

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

        if (tag == 0x00) {
            var run: usize = 1;
            var scan = index + 8;
            while (run < 256 and scan + 8 <= bytes.len) : (scan += 8) {
                const next_word = bytes[scan .. scan + 8];
                var all_zero = true;
                for (next_word) |b| {
                    if (b != 0) {
                        all_zero = false;
                        break;
                    }
                }
                if (!all_zero) break;
                run += 1;
            }

            try out.append(allocator, 0x00);
            try out.append(allocator, @as(u8, @intCast(run - 1)));
            index += run * 8;
            continue;
        }

        if (tag == 0xFF) {
            var run: usize = 1;
            var scan = index + 8;
            while (run < 256 and scan + 8 <= bytes.len) : (scan += 8) {
                const next_word = bytes[scan .. scan + 8];
                var all_nonzero = true;
                for (next_word) |b| {
                    if (b == 0) {
                        all_nonzero = false;
                        break;
                    }
                }
                if (!all_nonzero) break;
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

pub const InlineCompositeList = struct {
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,
    data_words: u16,
    pointer_words: u16,
};

pub const Capability = struct {
    id: u32,
};

/// Cap'n Proto message reader with full segment support
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

    pub fn initPacked(allocator: std.mem.Allocator, packed_bytes: []const u8) !Message {
        const unpacked = try unpackPacked(allocator, packed_bytes);
        errdefer allocator.free(unpacked);

        var msg = try Message.init(allocator, unpacked);
        msg.backing_data = unpacked;
        msg.segments_owned = true;
        return msg;
    }

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
            const landing_word = try self.readWord(far.segment_id, landing_pos);
            return try self.resolvePointer(far.segment_id, landing_pos, landing_word, depth - 1);
        }

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
        if (total_words_u64 > std.math.maxInt(usize)) return error.ListTooLarge;
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

/// Struct reader for Cap'n Proto structs
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

    pub fn readU64(self: StructReader, byte_offset: usize) u64 {
        const data = self.getDataSection();
        if (byte_offset + 8 > data.len) return 0;
        return std.mem.readInt(u64, data[byte_offset..][0..8], .little);
    }

    pub fn readU32(self: StructReader, byte_offset: usize) u32 {
        const data = self.getDataSection();
        if (byte_offset + 4 > data.len) return 0;
        return std.mem.readInt(u32, data[byte_offset..][0..4], .little);
    }

    pub fn readU16(self: StructReader, byte_offset: usize) u16 {
        const data = self.getDataSection();
        if (byte_offset + 2 > data.len) return 0;
        return std.mem.readInt(u16, data[byte_offset..][0..2], .little);
    }

    pub fn readU8(self: StructReader, byte_offset: usize) u8 {
        const data = self.getDataSection();
        if (byte_offset >= data.len) return 0;
        return data[byte_offset];
    }

    pub fn readBool(self: StructReader, byte_offset: usize, bit_offset: u3) bool {
        const data = self.getDataSection();
        if (byte_offset >= data.len) return false;
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
};

pub const StructListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,
    data_words: u16,
    pointer_words: u16,

    pub fn len(self: StructListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: StructListReader, index: u32) !StructReader {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const stride = (@as(usize, self.data_words) + @as(usize, self.pointer_words)) * 8;
        const offset = self.elements_offset + @as(usize, index) * stride;
        return StructReader{
            .message = self.message,
            .segment_id = self.segment_id,
            .offset = offset,
            .data_size = self.data_words,
            .pointer_count = self.pointer_words,
        };
    }
};

pub const TextListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: TextListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: TextListReader, index: u32) ![]const u8 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const pointer_pos = self.elements_offset + @as(usize, index) * 8;
        const segment = self.message.segments[self.segment_id];
        if (pointer_pos + 8 > segment.len) return error.OutOfBounds;

        const pointer_word = std.mem.readInt(u64, segment[pointer_pos..][0..8], .little);
        if (pointer_word == 0) return "";

        const list = try self.message.resolveListPointer(self.segment_id, pointer_pos, pointer_word);
        if (list.element_size != 2) return error.InvalidTextPointer;

        const list_segment = self.message.segments[list.segment_id];
        if (list.content_offset + list.element_count > list_segment.len) return error.OutOfBounds;

        const text_data = list_segment[list.content_offset .. list.content_offset + list.element_count];
        if (text_data.len > 0 and text_data[text_data.len - 1] == 0) {
            return text_data[0 .. text_data.len - 1];
        }
        return text_data;
    }
};

pub const U8ListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: U8ListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: U8ListReader, index: u32) !u8 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index);
        const segment = self.message.segments[self.segment_id];
        if (offset >= segment.len) return error.OutOfBounds;
        return segment[offset];
    }

    pub fn slice(self: U8ListReader) []const u8 {
        const segment = self.message.segments[self.segment_id];
        return segment[self.elements_offset .. self.elements_offset + @as(usize, self.element_count)];
    }
};

pub const I8ListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: I8ListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: I8ListReader, index: u32) !i8 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index);
        const segment = self.message.segments[self.segment_id];
        if (offset >= segment.len) return error.OutOfBounds;
        return @bitCast(segment[offset]);
    }
};

pub const U16ListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: U16ListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: U16ListReader, index: u32) !u16 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 2;
        const segment = self.message.segments[self.segment_id];
        if (offset + 2 > segment.len) return error.OutOfBounds;
        return std.mem.readInt(u16, segment[offset..][0..2], .little);
    }
};

pub const I16ListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: I16ListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: I16ListReader, index: u32) !i16 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 2;
        const segment = self.message.segments[self.segment_id];
        if (offset + 2 > segment.len) return error.OutOfBounds;
        return std.mem.readInt(i16, segment[offset..][0..2], .little);
    }
};

pub const U32ListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: U32ListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: U32ListReader, index: u32) !u32 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 4;
        const segment = self.message.segments[self.segment_id];
        if (offset + 4 > segment.len) return error.OutOfBounds;
        return std.mem.readInt(u32, segment[offset..][0..4], .little);
    }
};

pub const I32ListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: I32ListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: I32ListReader, index: u32) !i32 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 4;
        const segment = self.message.segments[self.segment_id];
        if (offset + 4 > segment.len) return error.OutOfBounds;
        return std.mem.readInt(i32, segment[offset..][0..4], .little);
    }
};

pub const F32ListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: F32ListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: F32ListReader, index: u32) !f32 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 4;
        const segment = self.message.segments[self.segment_id];
        if (offset + 4 > segment.len) return error.OutOfBounds;
        const raw = std.mem.readInt(u32, segment[offset..][0..4], .little);
        return @bitCast(raw);
    }
};

pub const U64ListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: U64ListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: U64ListReader, index: u32) !u64 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 8;
        const segment = self.message.segments[self.segment_id];
        if (offset + 8 > segment.len) return error.OutOfBounds;
        return std.mem.readInt(u64, segment[offset..][0..8], .little);
    }
};

pub const I64ListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: I64ListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: I64ListReader, index: u32) !i64 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 8;
        const segment = self.message.segments[self.segment_id];
        if (offset + 8 > segment.len) return error.OutOfBounds;
        return std.mem.readInt(i64, segment[offset..][0..8], .little);
    }
};

pub const F64ListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: F64ListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: F64ListReader, index: u32) !f64 {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 8;
        const segment = self.message.segments[self.segment_id];
        if (offset + 8 > segment.len) return error.OutOfBounds;
        const raw = std.mem.readInt(u64, segment[offset..][0..8], .little);
        return @bitCast(raw);
    }
};

pub const BoolListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: BoolListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: BoolListReader, index: u32) !bool {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const byte_index = @as(usize, index / 8);
        const bit_index: u3 = @intCast(index % 8);
        const offset = self.elements_offset + byte_index;
        const segment = self.message.segments[self.segment_id];
        if (offset >= segment.len) return error.OutOfBounds;
        return (segment[offset] & (@as(u8, 1) << bit_index)) != 0;
    }
};

pub const VoidListReader = struct {
    element_count: u32,

    pub fn len(self: VoidListReader) u32 {
        return self.element_count;
    }

    pub fn get(self: VoidListReader, index: u32) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
    }
};

pub const PointerListReader = struct {
    message: *const Message,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: PointerListReader) u32 {
        return self.element_count;
    }

    fn readPointer(self: PointerListReader, index: u32) !struct { pos: usize, word: u64 } {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const pointer_pos = self.elements_offset + @as(usize, index) * 8;
        const segment = self.message.segments[self.segment_id];
        if (pointer_pos + 8 > segment.len) return error.OutOfBounds;
        const pointer_word = std.mem.readInt(u64, segment[pointer_pos..][0..8], .little);
        return .{ .pos = pointer_pos, .word = pointer_word };
    }

    fn readList(self: PointerListReader, index: u32) !Message.ResolvedListPointer {
        const ptr = try self.readPointer(index);
        if (ptr.word == 0) return error.InvalidPointer;
        return self.message.resolveListPointer(self.segment_id, ptr.pos, ptr.word);
    }

    pub fn getText(self: PointerListReader, index: u32) ![]const u8 {
        const ptr = try self.readPointer(index);
        if (ptr.word == 0) return "";

        const list = try self.message.resolveListPointer(self.segment_id, ptr.pos, ptr.word);
        if (list.element_size != 2) return error.InvalidTextPointer;

        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + list.element_count > segment.len) return error.OutOfBounds;

        const text_data = segment[list.content_offset .. list.content_offset + list.element_count];
        if (text_data.len > 0 and text_data[text_data.len - 1] == 0) {
            return text_data[0 .. text_data.len - 1];
        }
        return text_data;
    }

    pub fn getStruct(self: PointerListReader, index: u32) !StructReader {
        const ptr = try self.readPointer(index);
        if (ptr.word == 0) return error.InvalidPointer;
        return self.message.resolveStructPointer(self.segment_id, ptr.pos, ptr.word);
    }

    pub fn getCapability(self: PointerListReader, index: u32) !Capability {
        const ptr = try self.readPointer(index);
        if (ptr.word == 0) return error.InvalidPointer;
        const any = AnyPointerReader{
            .message = self.message,
            .segment_id = self.segment_id,
            .pointer_pos = ptr.pos,
            .pointer_word = ptr.word,
        };
        return any.getCapability();
    }

    pub fn getData(self: PointerListReader, index: u32) ![]const u8 {
        const list = try self.readList(index);
        if (list.element_size != 2) return error.InvalidPointer;
        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;
        return segment[list.content_offset .. list.content_offset + total_bytes];
    }

    pub fn getU8List(self: PointerListReader, index: u32) !U8ListReader {
        const list = try self.readList(index);
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

    pub fn getI8List(self: PointerListReader, index: u32) !I8ListReader {
        const list = try self.getU8List(index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn getU16List(self: PointerListReader, index: u32) !U16ListReader {
        const list = try self.readList(index);
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

    pub fn getI16List(self: PointerListReader, index: u32) !I16ListReader {
        const list = try self.getU16List(index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn getU32List(self: PointerListReader, index: u32) !U32ListReader {
        const list = try self.readList(index);
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

    pub fn getI32List(self: PointerListReader, index: u32) !I32ListReader {
        const list = try self.getU32List(index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn getF32List(self: PointerListReader, index: u32) !F32ListReader {
        const list = try self.getU32List(index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn getU64List(self: PointerListReader, index: u32) !U64ListReader {
        const list = try self.readList(index);
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

    pub fn getI64List(self: PointerListReader, index: u32) !I64ListReader {
        const list = try self.getU64List(index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn getF64List(self: PointerListReader, index: u32) !F64ListReader {
        const list = try self.getU64List(index);
        return .{
            .message = list.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
        };
    }

    pub fn getBoolList(self: PointerListReader, index: u32) !BoolListReader {
        const list = try self.readList(index);
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
};

pub const AnyPointerReader = struct {
    message: *const Message,
    segment_id: u32,
    pointer_pos: usize,
    pointer_word: u64,

    pub fn isNull(self: AnyPointerReader) bool {
        return self.pointer_word == 0;
    }

    pub fn getStruct(self: AnyPointerReader) !StructReader {
        if (self.pointer_word == 0) return error.InvalidPointer;
        return self.message.resolveStructPointer(self.segment_id, self.pointer_pos, self.pointer_word);
    }

    pub fn getList(self: AnyPointerReader) !Message.ResolvedListPointer {
        if (self.pointer_word == 0) return error.InvalidPointer;
        return self.message.resolveListPointer(self.segment_id, self.pointer_pos, self.pointer_word);
    }

    pub fn getInlineCompositeList(self: AnyPointerReader) !InlineCompositeList {
        if (self.pointer_word == 0) return error.InvalidPointer;
        return self.message.resolveInlineCompositeList(self.segment_id, self.pointer_pos, self.pointer_word);
    }

    pub fn getText(self: AnyPointerReader) ![]const u8 {
        if (self.pointer_word == 0) return "";
        const list = try self.message.resolveListPointer(self.segment_id, self.pointer_pos, self.pointer_word);
        if (list.element_size != 2) return error.InvalidTextPointer;

        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + list.element_count > segment.len) return error.OutOfBounds;

        const text_data = segment[list.content_offset .. list.content_offset + list.element_count];
        if (text_data.len > 0 and text_data[text_data.len - 1] == 0) {
            return text_data[0 .. text_data.len - 1];
        }
        return text_data;
    }

    pub fn getData(self: AnyPointerReader) ![]const u8 {
        if (self.pointer_word == 0) return "";
        const list = try self.message.resolveListPointer(self.segment_id, self.pointer_pos, self.pointer_word);
        if (list.element_size != 2) return error.InvalidPointer;
        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        const segment = self.message.segments[list.segment_id];
        if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;
        return segment[list.content_offset .. list.content_offset + total_bytes];
    }

    pub fn getPointerList(self: AnyPointerReader) !PointerListReader {
        const list = try self.getList();
        if (list.element_size != 6) return error.InvalidPointer;
        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    pub fn getCapability(self: AnyPointerReader) !Capability {
        if (self.pointer_word == 0) return error.InvalidPointer;
        const resolved = try self.message.resolvePointer(self.segment_id, self.pointer_pos, self.pointer_word, 8);
        if (resolved.pointer_word == 0) return error.InvalidPointer;
        return .{ .id = try decodeCapabilityPointer(resolved.pointer_word) };
    }
};

pub const AnyPointerBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    pointer_pos: usize,

    pub fn setNull(self: AnyPointerBuilder) !void {
        if (self.segment_id >= self.builder.segments.items.len) return error.InvalidSegmentId;
        const segment = &self.builder.segments.items[self.segment_id];
        if (self.pointer_pos + 8 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(u64, segment.items[self.pointer_pos..][0..8], 0, .little);
    }

    pub fn setText(self: AnyPointerBuilder, text: []const u8) !void {
        try self.builder.writeTextPointer(self.segment_id, self.pointer_pos, text, self.segment_id);
    }

    pub fn setData(self: AnyPointerBuilder, data: []const u8) !void {
        if (data.len > std.math.maxInt(u32)) return error.ElementCountTooLarge;
        const offset = try self.builder.writeListPointer(
            self.segment_id,
            self.pointer_pos,
            2,
            @as(u32, @intCast(data.len)),
            self.segment_id,
        );
        const segment = &self.builder.segments.items[self.segment_id];
        const slice = segment.items[offset .. offset + data.len];
        std.mem.copyForwards(u8, slice, data);
    }

    pub fn setCapability(self: AnyPointerBuilder, cap: Capability) !void {
        if (self.segment_id >= self.builder.segments.items.len) return error.InvalidSegmentId;
        const segment = &self.builder.segments.items[self.segment_id];
        if (self.pointer_pos + 8 > segment.items.len) return error.OutOfBounds;
        const pointer_word = try makeCapabilityPointer(cap.id);
        std.mem.writeInt(u64, segment.items[self.pointer_pos..][0..8], pointer_word, .little);
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
        const offset = try self.builder.writeListPointer(self.segment_id, self.pointer_pos, 6, element_count, self.segment_id);
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = offset,
            .element_count = element_count,
        };
    }

    fn initList(self: AnyPointerBuilder, element_size: u3, element_count: u32) !struct { offset: usize } {
        const offset = try self.builder.writeListPointer(self.segment_id, self.pointer_pos, element_size, element_count, self.segment_id);
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

/// Message builder for creating Cap'n Proto messages
pub const MessageBuilder = struct {
    allocator: std.mem.Allocator,
    segments: std.ArrayList(std.ArrayList(u8)),

    pub fn init(allocator: std.mem.Allocator) MessageBuilder {
        return .{
            .allocator = allocator,
            .segments = std.ArrayList(std.ArrayList(u8)){},
        };
    }

    pub fn deinit(self: *MessageBuilder) void {
        for (self.segments.items) |*segment| {
            segment.deinit(self.allocator);
        }
        self.segments.deinit(self.allocator);
    }

    pub fn createSegment(self: *MessageBuilder) !u32 {
        const id: u32 = @intCast(self.segments.items.len);
        try self.segments.append(self.allocator, std.ArrayList(u8){});
        return id;
    }

    fn getSegment(self: *MessageBuilder, segment_id: u32) !*std.ArrayList(u8) {
        if (segment_id >= self.segments.items.len) return error.InvalidSegmentId;
        return &self.segments.items[segment_id];
    }

    pub fn initRootAnyPointer(self: *MessageBuilder) !AnyPointerBuilder {
        if (self.segments.items.len == 0) {
            try self.segments.append(self.allocator, std.ArrayList(u8){});
        }
        const segment = &self.segments.items[0];
        if (segment.items.len < 8) {
            try segment.appendNTimes(self.allocator, 0, 8 - segment.items.len);
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
        try segment.appendNTimes(self.allocator, 0, byte_count);
        return offset;
    }

    pub fn allocateStructInSegment(self: *MessageBuilder, segment_id: u32, data_words: u16, pointer_words: u16) !StructBuilder {
        if (self.segments.items.len == 0) {
            try self.segments.append(self.allocator, std.ArrayList(u8){});
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
            try self.segments.append(self.allocator, std.ArrayList(u8){});
        }

        const root_segment = &self.segments.items[0];
        if (root_segment.items.len != 0) return error.RootAlreadyAllocated;

        while (self.segments.items.len <= segment_id) {
            _ = try self.createSegment();
        }

        const target_segment = &self.segments.items[segment_id];
        const landing_pad_pos = target_segment.items.len;
        try target_segment.appendNTimes(self.allocator, 0, 8);

        const total_words = @as(usize, data_words) + @as(usize, pointer_words);
        const total_bytes = total_words * 8;
        const struct_offset = target_segment.items.len;
        try target_segment.appendNTimes(self.allocator, 0, total_bytes);

        const struct_ptr = makeStructPointer(0, data_words, pointer_words);
        std.mem.writeInt(u64, target_segment.items[landing_pad_pos..][0..8], struct_ptr, .little);

        try root_segment.appendNTimes(self.allocator, 0, 8);
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

    fn writeTextPointer(
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
        const text_offset = target_segment.items.len;
        try target_segment.appendSlice(self.allocator, text);
        try target_segment.append(self.allocator, 0);

        const padding = (8 - ((text.len + 1) % 8)) % 8;
        try target_segment.appendNTimes(self.allocator, 0, padding);

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
        try target_segment.appendNTimes(self.allocator, 0, 8);

        const list_offset = @as(i32, @intCast(@divTrunc(
            @as(isize, @intCast(text_offset)) - @as(isize, @intCast(landing_pad_pos)) - 8,
            8,
        )));
        const list_ptr = makeListPointer(list_offset, 2, @as(u32, @intCast(text.len + 1)));
        std.mem.writeInt(u64, target_segment.items[landing_pad_pos..][0..8], list_ptr, .little);

        const far_ptr = makeFarPointer(false, @as(u32, @intCast(landing_pad_pos / 8)), target_segment_id);
        std.mem.writeInt(u64, pointer_segment.items[pointer_pos..][0..8], far_ptr, .little);
    }

    fn writeStructPointer(
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

    fn writeListPointer(
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

    fn writeStructListPointer(
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

    pub fn allocateStruct(self: *MessageBuilder, data_words: u16, pointer_words: u16) !StructBuilder {
        // Ensure we have at least one segment
        if (self.segments.items.len == 0) {
            try self.segments.append(self.allocator, std.ArrayList(u8){});
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

            // Write root pointer
            try segment.appendNTimes(self.allocator, 0, 8);
            std.mem.writeInt(u64, segment.items[0..8], root_pointer, .little);

            // Allocate struct data
            const struct_offset = segment.items.len;
            try segment.appendNTimes(self.allocator, 0, total_bytes);

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

    pub fn toBytes(self: *MessageBuilder) ![]const u8 {
        var result = std.ArrayList(u8){};
        errdefer result.deinit(self.allocator);

        // Ensure we have at least one segment
        if (self.segments.items.len == 0) {
            try self.segments.append(self.allocator, std.ArrayList(u8){});
        }

        // Write segment count
        const segment_count = @as(u32, @intCast(self.segments.items.len));
        try result.append(self.allocator, @as(u8, @truncate(segment_count - 1)));
        try result.append(self.allocator, @as(u8, @truncate((segment_count - 1) >> 8)));
        try result.append(self.allocator, @as(u8, @truncate((segment_count - 1) >> 16)));
        try result.append(self.allocator, @as(u8, @truncate((segment_count - 1) >> 24)));

        // Write segment sizes
        for (self.segments.items) |segment| {
            const size_words = @as(u32, @intCast(segment.items.len / 8));
            try result.append(self.allocator, @as(u8, @truncate(size_words)));
            try result.append(self.allocator, @as(u8, @truncate(size_words >> 8)));
            try result.append(self.allocator, @as(u8, @truncate(size_words >> 16)));
            try result.append(self.allocator, @as(u8, @truncate(size_words >> 24)));
        }

        // Padding to 8-byte boundary
        if (segment_count % 2 == 0) {
            try result.appendNTimes(self.allocator, 0, 4);
        }

        // Write segment data
        for (self.segments.items) |segment| {
            try result.appendSlice(self.allocator, segment.items);
        }

        return result.toOwnedSlice(self.allocator);
    }

    pub fn toPackedBytes(self: *MessageBuilder) ![]const u8 {
        const bytes = try self.toBytes();
        defer self.allocator.free(bytes);
        return packPacked(self.allocator, bytes);
    }

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

pub const TextListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: TextListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: TextListBuilder, index: u32, value: []const u8) !void {
        return self.setInSegment(index, value, self.segment_id);
    }

    pub fn setInSegment(self: TextListBuilder, index: u32, value: []const u8, target_segment_id: u32) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const pointer_pos = self.elements_offset + @as(usize, index) * 8;
        try self.builder.writeTextPointer(self.segment_id, pointer_pos, value, target_segment_id);
    }
};

pub const VoidListBuilder = struct {
    element_count: u32,

    pub fn len(self: VoidListBuilder) u32 {
        return self.element_count;
    }
};

pub const U8ListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: U8ListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: U8ListBuilder, index: u32, value: u8) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index);
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset >= segment.items.len) return error.OutOfBounds;
        segment.items[offset] = value;
    }

    pub fn setAll(self: U8ListBuilder, data: []const u8) !void {
        if (data.len != self.element_count) return error.InvalidLength;
        const segment = &self.builder.segments.items[self.segment_id];
        const slice = segment.items[self.elements_offset .. self.elements_offset + data.len];
        std.mem.copyForwards(u8, slice, data);
    }
};

pub const I8ListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: I8ListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: I8ListBuilder, index: u32, value: i8) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index);
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset >= segment.items.len) return error.OutOfBounds;
        segment.items[offset] = @bitCast(value);
    }
};

pub const U16ListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: U16ListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: U16ListBuilder, index: u32, value: u16) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 2;
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset + 2 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(u16, segment.items[offset..][0..2], value, .little);
    }
};

pub const I16ListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: I16ListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: I16ListBuilder, index: u32, value: i16) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 2;
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset + 2 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(i16, segment.items[offset..][0..2], value, .little);
    }
};

pub const U32ListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: U32ListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: U32ListBuilder, index: u32, value: u32) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 4;
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset + 4 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(u32, segment.items[offset..][0..4], value, .little);
    }
};

pub const I32ListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: I32ListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: I32ListBuilder, index: u32, value: i32) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 4;
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset + 4 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(i32, segment.items[offset..][0..4], value, .little);
    }
};

pub const F32ListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: F32ListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: F32ListBuilder, index: u32, value: f32) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 4;
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset + 4 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(u32, segment.items[offset..][0..4], @bitCast(value), .little);
    }
};

pub const U64ListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: U64ListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: U64ListBuilder, index: u32, value: u64) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 8;
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset + 8 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(u64, segment.items[offset..][0..8], value, .little);
    }
};

pub const I64ListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: I64ListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: I64ListBuilder, index: u32, value: i64) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 8;
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset + 8 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(i64, segment.items[offset..][0..8], value, .little);
    }
};

pub const F64ListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: F64ListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: F64ListBuilder, index: u32, value: f64) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const offset = self.elements_offset + @as(usize, index) * 8;
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset + 8 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(u64, segment.items[offset..][0..8], @bitCast(value), .little);
    }
};

pub const BoolListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: BoolListBuilder) u32 {
        return self.element_count;
    }

    pub fn set(self: BoolListBuilder, index: u32, value: bool) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const byte_index = @as(usize, index / 8);
        const bit_index: u3 = @intCast(index % 8);
        const offset = self.elements_offset + byte_index;
        const segment = &self.builder.segments.items[self.segment_id];
        if (offset >= segment.items.len) return error.OutOfBounds;
        const mask = @as(u8, 1) << bit_index;
        if (value) {
            segment.items[offset] |= mask;
        } else {
            segment.items[offset] &= ~mask;
        }
    }
};

pub const PointerListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,

    pub fn len(self: PointerListBuilder) u32 {
        return self.element_count;
    }

    pub fn setNull(self: PointerListBuilder, index: u32) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const pointer_pos = self.elements_offset + @as(usize, index) * 8;
        const segment = &self.builder.segments.items[self.segment_id];
        if (pointer_pos + 8 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(u64, segment.items[pointer_pos..][0..8], 0, .little);
    }

    pub fn setCapability(self: PointerListBuilder, index: u32, cap: Capability) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const pointer_pos = self.elements_offset + @as(usize, index) * 8;
        const segment = &self.builder.segments.items[self.segment_id];
        if (pointer_pos + 8 > segment.items.len) return error.OutOfBounds;
        const pointer_word = try makeCapabilityPointer(cap.id);
        std.mem.writeInt(u64, segment.items[pointer_pos..][0..8], pointer_word, .little);
    }

    pub fn setText(self: PointerListBuilder, index: u32, value: []const u8) !void {
        return self.setTextInSegment(index, value, self.segment_id);
    }

    pub fn setTextInSegment(self: PointerListBuilder, index: u32, value: []const u8, target_segment_id: u32) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const pointer_pos = self.elements_offset + @as(usize, index) * 8;
        try self.builder.writeTextPointer(self.segment_id, pointer_pos, value, target_segment_id);
    }

    pub fn setData(self: PointerListBuilder, index: u32, value: []const u8) !void {
        return self.setDataInSegment(index, value, self.segment_id);
    }

    pub fn setDataInSegment(self: PointerListBuilder, index: u32, value: []const u8, target_segment_id: u32) !void {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        if (value.len > std.math.maxInt(u32)) return error.ElementCountTooLarge;

        while (self.builder.segments.items.len <= target_segment_id) {
            _ = try self.builder.createSegment();
        }

        const pointer_pos = self.elements_offset + @as(usize, index) * 8;
        const offset = try self.builder.writeListPointer(
            self.segment_id,
            pointer_pos,
            2,
            @as(u32, @intCast(value.len)),
            target_segment_id,
        );

        const segment = &self.builder.segments.items[target_segment_id];
        const slice = segment.items[offset .. offset + value.len];
        std.mem.copyForwards(u8, slice, value);
    }

    pub fn initStruct(self: PointerListBuilder, index: u32, data_words: u16, pointer_words: u16) !StructBuilder {
        return self.initStructInSegment(index, data_words, pointer_words, self.segment_id);
    }

    pub fn initStructInSegment(
        self: PointerListBuilder,
        index: u32,
        data_words: u16,
        pointer_words: u16,
        target_segment_id: u32,
    ) !StructBuilder {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        const pointer_pos = self.elements_offset + @as(usize, index) * 8;
        return self.builder.writeStructPointer(self.segment_id, pointer_pos, data_words, pointer_words, target_segment_id);
    }

    fn initListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_size: u3,
        element_count: u32,
        target_segment_id: u32,
    ) !struct { segment_id: u32, offset: usize } {
        if (index >= self.element_count) return error.IndexOutOfBounds;
        while (self.builder.segments.items.len <= target_segment_id) {
            _ = try self.builder.createSegment();
        }

        const pointer_pos = self.elements_offset + @as(usize, index) * 8;
        const offset = try self.builder.writeListPointer(self.segment_id, pointer_pos, element_size, element_count, target_segment_id);
        return .{ .segment_id = target_segment_id, .offset = offset };
    }

    pub fn initU8List(self: PointerListBuilder, index: u32, element_count: u32) !U8ListBuilder {
        return self.initU8ListInSegment(index, element_count, self.segment_id);
    }

    pub fn initU8ListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !U8ListBuilder {
        const info = try self.initListInSegment(index, 2, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initI8List(self: PointerListBuilder, index: u32, element_count: u32) !I8ListBuilder {
        return self.initI8ListInSegment(index, element_count, self.segment_id);
    }

    pub fn initI8ListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !I8ListBuilder {
        const info = try self.initListInSegment(index, 2, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initU16List(self: PointerListBuilder, index: u32, element_count: u32) !U16ListBuilder {
        return self.initU16ListInSegment(index, element_count, self.segment_id);
    }

    pub fn initU16ListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !U16ListBuilder {
        const info = try self.initListInSegment(index, 3, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initI16List(self: PointerListBuilder, index: u32, element_count: u32) !I16ListBuilder {
        return self.initI16ListInSegment(index, element_count, self.segment_id);
    }

    pub fn initI16ListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !I16ListBuilder {
        const info = try self.initListInSegment(index, 3, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initU32List(self: PointerListBuilder, index: u32, element_count: u32) !U32ListBuilder {
        return self.initU32ListInSegment(index, element_count, self.segment_id);
    }

    pub fn initU32ListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !U32ListBuilder {
        const info = try self.initListInSegment(index, 4, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initI32List(self: PointerListBuilder, index: u32, element_count: u32) !I32ListBuilder {
        return self.initI32ListInSegment(index, element_count, self.segment_id);
    }

    pub fn initI32ListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !I32ListBuilder {
        const info = try self.initListInSegment(index, 4, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initF32List(self: PointerListBuilder, index: u32, element_count: u32) !F32ListBuilder {
        return self.initF32ListInSegment(index, element_count, self.segment_id);
    }

    pub fn initF32ListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !F32ListBuilder {
        const info = try self.initListInSegment(index, 4, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initU64List(self: PointerListBuilder, index: u32, element_count: u32) !U64ListBuilder {
        return self.initU64ListInSegment(index, element_count, self.segment_id);
    }

    pub fn initU64ListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !U64ListBuilder {
        const info = try self.initListInSegment(index, 5, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initI64List(self: PointerListBuilder, index: u32, element_count: u32) !I64ListBuilder {
        return self.initI64ListInSegment(index, element_count, self.segment_id);
    }

    pub fn initI64ListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !I64ListBuilder {
        const info = try self.initListInSegment(index, 5, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initF64List(self: PointerListBuilder, index: u32, element_count: u32) !F64ListBuilder {
        return self.initF64ListInSegment(index, element_count, self.segment_id);
    }

    pub fn initF64ListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !F64ListBuilder {
        const info = try self.initListInSegment(index, 5, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn initBoolList(self: PointerListBuilder, index: u32, element_count: u32) !BoolListBuilder {
        return self.initBoolListInSegment(index, element_count, self.segment_id);
    }

    pub fn initBoolListInSegment(
        self: PointerListBuilder,
        index: u32,
        element_count: u32,
        target_segment_id: u32,
    ) !BoolListBuilder {
        const info = try self.initListInSegment(index, 1, element_count, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }
};

/// Struct builder for creating Cap'n Proto structs
pub const StructBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    offset: usize,
    data_size: u16,
    pointer_count: u16,

    fn getDataSection(self: StructBuilder) []u8 {
        const segment = &self.builder.segments.items[self.segment_id];
        const start = self.offset;
        const end = start + @as(usize, self.data_size) * 8;
        return segment.items[start..end];
    }

    pub fn writeU64(self: StructBuilder, byte_offset: usize, value: u64) void {
        const data = self.getDataSection();
        if (byte_offset + 8 > data.len) return;
        std.mem.writeInt(u64, data[byte_offset..][0..8], value, .little);
    }

    pub fn writeU32(self: StructBuilder, byte_offset: usize, value: u32) void {
        const data = self.getDataSection();
        if (byte_offset + 4 > data.len) return;
        std.mem.writeInt(u32, data[byte_offset..][0..4], value, .little);
    }

    pub fn writeU16(self: StructBuilder, byte_offset: usize, value: u16) void {
        const data = self.getDataSection();
        if (byte_offset + 2 > data.len) return;
        std.mem.writeInt(u16, data[byte_offset..][0..2], value, .little);
    }

    pub fn writeU8(self: StructBuilder, byte_offset: usize, value: u8) void {
        const data = self.getDataSection();
        if (byte_offset >= data.len) return;
        data[byte_offset] = value;
    }

    pub fn writeBool(self: StructBuilder, byte_offset: usize, bit_offset: u3, value: bool) void {
        const data = self.getDataSection();
        if (byte_offset >= data.len) return;
        const mask = @as(u8, 1) << bit_offset;
        if (value) {
            data[byte_offset] |= mask;
        } else {
            data[byte_offset] &= ~mask;
        }
    }

    /// Write a union discriminant (which field is set)
    pub fn writeUnionDiscriminant(self: StructBuilder, byte_offset: usize, value: u16) void {
        self.writeU16(byte_offset, value);
    }

    pub fn writeText(self: StructBuilder, pointer_index: usize, text: []const u8) !void {
        return self.writeTextInSegment(pointer_index, text, self.segment_id);
    }

    pub fn writeTextInSegment(
        self: StructBuilder,
        pointer_index: usize,
        text: []const u8,
        target_segment_id: u32,
    ) !void {
        if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;
        if (self.segment_id >= self.builder.segments.items.len) return error.InvalidSegmentId;
        if (target_segment_id >= self.builder.segments.items.len) return error.InvalidSegmentId;

        const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
        try self.builder.writeTextPointer(self.segment_id, pointer_pos, text, target_segment_id);
    }

    pub fn initStruct(self: StructBuilder, pointer_index: usize, data_words: u16, pointer_words: u16) !StructBuilder {
        return self.initStructInSegment(pointer_index, data_words, pointer_words, self.segment_id);
    }

    pub fn initStructInSegment(
        self: StructBuilder,
        pointer_index: usize,
        data_words: u16,
        pointer_words: u16,
        target_segment_id: u32,
    ) !StructBuilder {
        if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;
        while (self.builder.segments.items.len <= target_segment_id) {
            _ = try self.builder.createSegment();
        }

        const source_segment = &self.builder.segments.items[self.segment_id];
        const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
        if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

        return self.builder.writeStructPointer(self.segment_id, pointer_pos, data_words, pointer_words, target_segment_id);
    }

    pub fn getAnyPointer(self: StructBuilder, pointer_index: usize) !AnyPointerBuilder {
        if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;

        const segment = &self.builder.segments.items[self.segment_id];
        const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
        if (pointer_pos + 8 > segment.items.len) return error.OutOfBounds;

        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .pointer_pos = pointer_pos,
        };
    }

    fn writePrimitiveListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        element_size: u3,
        target_segment_id: u32,
    ) !struct { segment_id: u32, offset: usize } {
        if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;

        while (self.builder.segments.items.len <= target_segment_id) {
            _ = try self.builder.createSegment();
        }

        const source_segment = &self.builder.segments.items[self.segment_id];
        const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
        if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

        const offset = try self.builder.writeListPointer(self.segment_id, pointer_pos, element_size, element_count, target_segment_id);
        return .{ .segment_id = target_segment_id, .offset = offset };
    }

    pub fn writeData(self: StructBuilder, pointer_index: usize, data: []const u8) !void {
        return self.writeDataInSegment(pointer_index, data, self.segment_id);
    }

    pub fn writeDataInSegment(
        self: StructBuilder,
        pointer_index: usize,
        data: []const u8,
        target_segment_id: u32,
    ) !void {
        if (data.len > std.math.maxInt(u32)) return error.ElementCountTooLarge;
        const info = try self.writePrimitiveListInSegment(pointer_index, @as(u32, @intCast(data.len)), 2, target_segment_id);
        const segment = &self.builder.segments.items[info.segment_id];
        const slice = segment.items[info.offset .. info.offset + data.len];
        std.mem.copyForwards(u8, slice, data);
    }

    pub fn writeVoidList(self: StructBuilder, pointer_index: usize, element_count: u32) !VoidListBuilder {
        return self.writeVoidListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeVoidListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !VoidListBuilder {
        if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;

        while (self.builder.segments.items.len <= target_segment_id) {
            _ = try self.builder.createSegment();
        }

        const source_segment = &self.builder.segments.items[self.segment_id];
        const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
        if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

        _ = try self.builder.writeListPointer(self.segment_id, pointer_pos, 0, element_count, target_segment_id);
        return .{ .element_count = element_count };
    }

    pub fn writeU8List(self: StructBuilder, pointer_index: usize, element_count: u32) !U8ListBuilder {
        return self.writeU8ListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeU8ListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !U8ListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 2, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeI8List(self: StructBuilder, pointer_index: usize, element_count: u32) !I8ListBuilder {
        return self.writeI8ListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeI8ListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !I8ListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 2, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeU16List(self: StructBuilder, pointer_index: usize, element_count: u32) !U16ListBuilder {
        return self.writeU16ListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeU16ListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !U16ListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 3, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeI16List(self: StructBuilder, pointer_index: usize, element_count: u32) !I16ListBuilder {
        return self.writeI16ListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeI16ListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !I16ListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 3, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeU32List(self: StructBuilder, pointer_index: usize, element_count: u32) !U32ListBuilder {
        return self.writeU32ListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeU32ListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !U32ListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 4, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeI32List(self: StructBuilder, pointer_index: usize, element_count: u32) !I32ListBuilder {
        return self.writeI32ListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeI32ListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !I32ListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 4, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeF32List(self: StructBuilder, pointer_index: usize, element_count: u32) !F32ListBuilder {
        return self.writeF32ListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeF32ListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !F32ListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 4, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeU64List(self: StructBuilder, pointer_index: usize, element_count: u32) !U64ListBuilder {
        return self.writeU64ListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeU64ListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !U64ListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 5, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeI64List(self: StructBuilder, pointer_index: usize, element_count: u32) !I64ListBuilder {
        return self.writeI64ListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeI64ListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !I64ListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 5, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeF64List(self: StructBuilder, pointer_index: usize, element_count: u32) !F64ListBuilder {
        return self.writeF64ListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeF64ListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !F64ListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 5, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeBoolList(self: StructBuilder, pointer_index: usize, element_count: u32) !BoolListBuilder {
        return self.writeBoolListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writeBoolListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !BoolListBuilder {
        const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 1, target_segment_id);
        return .{
            .builder = self.builder,
            .segment_id = info.segment_id,
            .elements_offset = info.offset,
            .element_count = element_count,
        };
    }

    pub fn writeStructList(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        data_words: u16,
        pointer_words: u16,
    ) !StructListBuilder {
        return self.writeStructListInSegments(pointer_index, element_count, data_words, pointer_words, self.segment_id, self.segment_id);
    }

    pub fn writeStructListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        data_words: u16,
        pointer_words: u16,
        target_segment_id: u32,
    ) !StructListBuilder {
        return self.writeStructListInSegments(pointer_index, element_count, data_words, pointer_words, target_segment_id, target_segment_id);
    }

    pub fn writeStructListInSegments(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        data_words: u16,
        pointer_words: u16,
        landing_segment_id: u32,
        content_segment_id: u32,
    ) !StructListBuilder {
        if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;
        const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
        return self.builder.writeStructListPointer(
            self.segment_id,
            pointer_pos,
            element_count,
            data_words,
            pointer_words,
            landing_segment_id,
            content_segment_id,
        );
    }

    pub fn writeTextList(self: StructBuilder, pointer_index: usize, element_count: u32) !TextListBuilder {
        return self.writeTextListInSegments(pointer_index, element_count, self.segment_id, self.segment_id);
    }

    pub fn writeTextListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !TextListBuilder {
        return self.writeTextListInSegments(pointer_index, element_count, target_segment_id, target_segment_id);
    }

    pub fn writeTextListInSegments(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        landing_segment_id: u32,
        content_segment_id: u32,
    ) !TextListBuilder {
        if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;

        const total_bytes = @as(usize, element_count) * 8;

        while (self.builder.segments.items.len <= landing_segment_id or self.builder.segments.items.len <= content_segment_id) {
            _ = try self.builder.createSegment();
        }

        const source_segment = &self.builder.segments.items[self.segment_id];
        const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
        if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

        if (landing_segment_id == content_segment_id) {
            const target_segment = &self.builder.segments.items[landing_segment_id];
            const landing_pad_pos = if (self.segment_id == landing_segment_id) null else target_segment.items.len;
            if (landing_pad_pos) |_| {
                try target_segment.appendNTimes(self.builder.allocator, 0, 8);
            }

            const elements_offset = target_segment.items.len;
            try target_segment.appendNTimes(self.builder.allocator, 0, total_bytes);

            if (self.segment_id == landing_segment_id) {
                const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(elements_offset)) - @as(isize, @intCast(pointer_pos)) - 8, 8)));
                const list_ptr = makeListPointer(rel_offset, 6, element_count);
                std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], list_ptr, .little);
            } else {
                const landing_pos = landing_pad_pos.?;
                const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(elements_offset)) - @as(isize, @intCast(landing_pos)) - 8, 8)));
                const list_ptr = makeListPointer(rel_offset, 6, element_count);
                std.mem.writeInt(u64, target_segment.items[landing_pos..][0..8], list_ptr, .little);

                const far_ptr = makeFarPointer(false, @as(u32, @intCast(landing_pos / 8)), landing_segment_id);
                std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], far_ptr, .little);
            }

            return TextListBuilder{
                .builder = self.builder,
                .segment_id = landing_segment_id,
                .elements_offset = elements_offset,
                .element_count = element_count,
            };
        }

        const landing_segment = &self.builder.segments.items[landing_segment_id];
        const landing_pad_pos = landing_segment.items.len;
        try landing_segment.appendNTimes(self.builder.allocator, 0, 16);

        const content_segment = &self.builder.segments.items[content_segment_id];
        const elements_offset = content_segment.items.len;
        try content_segment.appendNTimes(self.builder.allocator, 0, total_bytes);

        const landing_far = makeFarPointer(false, @as(u32, @intCast(elements_offset / 8)), content_segment_id);
        std.mem.writeInt(u64, landing_segment.items[landing_pad_pos..][0..8], landing_far, .little);

        const tag_word = makeListPointer(0, 6, element_count);
        std.mem.writeInt(u64, landing_segment.items[landing_pad_pos + 8 ..][0..8], tag_word, .little);

        const far_ptr = makeFarPointer(true, @as(u32, @intCast(landing_pad_pos / 8)), landing_segment_id);
        std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], far_ptr, .little);

        return TextListBuilder{
            .builder = self.builder,
            .segment_id = content_segment_id,
            .elements_offset = elements_offset,
            .element_count = element_count,
        };
    }

    pub fn writePointerList(self: StructBuilder, pointer_index: usize, element_count: u32) !PointerListBuilder {
        return self.writePointerListInSegment(pointer_index, element_count, self.segment_id);
    }

    pub fn writePointerListInSegment(
        self: StructBuilder,
        pointer_index: usize,
        element_count: u32,
        target_segment_id: u32,
    ) !PointerListBuilder {
        if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;

        const total_bytes = @as(usize, element_count) * 8;

        while (self.builder.segments.items.len <= target_segment_id) {
            _ = try self.builder.createSegment();
        }

        const source_segment = &self.builder.segments.items[self.segment_id];
        const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
        if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

        const target_segment = &self.builder.segments.items[target_segment_id];
        const landing_pad_pos = if (self.segment_id == target_segment_id) null else target_segment.items.len;
        if (landing_pad_pos) |_| {
            try target_segment.appendNTimes(self.builder.allocator, 0, 8);
        }

        const elements_offset = target_segment.items.len;
        try target_segment.appendNTimes(self.builder.allocator, 0, total_bytes);

        if (self.segment_id == target_segment_id) {
            const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(elements_offset)) - @as(isize, @intCast(pointer_pos)) - 8, 8)));
            const list_ptr = makeListPointer(rel_offset, 6, element_count);
            std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], list_ptr, .little);
        } else {
            const landing_pos = landing_pad_pos.?;
            const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(elements_offset)) - @as(isize, @intCast(landing_pos)) - 8, 8)));
            const list_ptr = makeListPointer(rel_offset, 6, element_count);
            std.mem.writeInt(u64, target_segment.items[landing_pos..][0..8], list_ptr, .little);

            const far_ptr = makeFarPointer(false, @as(u32, @intCast(landing_pos / 8)), target_segment_id);
            std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], far_ptr, .little);
        }

        return PointerListBuilder{
            .builder = self.builder,
            .segment_id = target_segment_id,
            .elements_offset = elements_offset,
            .element_count = element_count,
        };
    }
};

pub fn cloneAnyPointerToBytes(allocator: std.mem.Allocator, src: AnyPointerReader) ![]const u8 {
    var builder = MessageBuilder.init(allocator);
    defer builder.deinit();

    const root = try builder.initRootAnyPointer();
    try cloneAnyPointer(src, root);

    if (builder.segments.items.len == 0) {
        return allocator.alloc(u8, 0);
    }
    const segment = builder.segments.items[0].items;
    const out = try allocator.alloc(u8, segment.len);
    std.mem.copyForwards(u8, out, segment);
    return out;
}

pub fn cloneAnyPointer(src: AnyPointerReader, dest: AnyPointerBuilder) anyerror!void {
    const resolved = try src.message.resolvePointer(src.segment_id, src.pointer_pos, src.pointer_word, 8);
    if (resolved.pointer_word == 0) {
        try dest.setNull();
        return;
    }

    const pointer_type = @as(u2, @truncate(resolved.pointer_word & 0x3));
    switch (pointer_type) {
        0 => {
            const src_struct = try src.message.resolveStructPointer(resolved.segment_id, resolved.pointer_pos, resolved.pointer_word);
            const dest_struct = try dest.initStruct(src_struct.data_size, src_struct.pointer_count);
            try cloneStruct(src_struct, dest_struct);
        },
        1 => try cloneList(src, dest, resolved),
        3 => {
            const cap_id = try decodeCapabilityPointer(resolved.pointer_word);
            try dest.setCapability(.{ .id = cap_id });
        },
        else => return error.InvalidPointer,
    }
}

fn cloneStruct(src: StructReader, dest: StructBuilder) anyerror!void {
    const src_data = src.getDataSection();
    const dest_segment = &dest.builder.segments.items[dest.segment_id];
    const dest_start = dest.offset;
    const dest_end = dest_start + src_data.len;
    if (dest_end > dest_segment.items.len) return error.OutOfBounds;
    std.mem.copyForwards(u8, dest_segment.items[dest_start..dest_end], src_data);

    const pointer_section = src.getPointerSection();
    var ptr_index: u16 = 0;
    while (ptr_index < src.pointer_count) : (ptr_index += 1) {
        const pointer_offset = @as(usize, ptr_index) * 8;
        if (pointer_offset + 8 > pointer_section.len) return error.OutOfBounds;
        const pointer_word = std.mem.readInt(u64, pointer_section[pointer_offset..][0..8], .little);
        const pointer_pos = src.offset + @as(usize, src.data_size) * 8 + pointer_offset;
        var dest_ptr = try dest.getAnyPointer(ptr_index);

        if (pointer_word == 0) {
            try dest_ptr.setNull();
        } else {
            const src_ptr = AnyPointerReader{
                .message = src.message,
                .segment_id = src.segment_id,
                .pointer_pos = pointer_pos,
                .pointer_word = pointer_word,
            };
            try cloneAnyPointer(src_ptr, dest_ptr);
        }
    }
}

fn cloneList(src: AnyPointerReader, dest: AnyPointerBuilder, resolved: Message.ResolvedPointer) anyerror!void {
    const element_size = @as(u3, @truncate((resolved.pointer_word >> 32) & 0x7));

    if (element_size == 7) {
        const list = try src.message.resolveInlineCompositeList(src.segment_id, src.pointer_pos, src.pointer_word);
        var fake = StructBuilder{
            .builder = dest.builder,
            .segment_id = dest.segment_id,
            .offset = dest.pointer_pos,
            .data_size = 0,
            .pointer_count = 1,
        };
        var dest_list = try fake.writeStructListInSegments(0, list.element_count, list.data_words, list.pointer_words, dest.segment_id, dest.segment_id);

        const src_list = StructListReader{
            .message = src.message,
            .segment_id = list.segment_id,
            .elements_offset = list.elements_offset,
            .element_count = list.element_count,
            .data_words = list.data_words,
            .pointer_words = list.pointer_words,
        };

        var idx: u32 = 0;
        while (idx < list.element_count) : (idx += 1) {
            const src_elem = try src_list.get(idx);
            const dest_elem = try dest_list.get(idx);
            try cloneStruct(src_elem, dest_elem);
        }
        return;
    }

    const list = try src.message.resolveListPointer(src.segment_id, src.pointer_pos, src.pointer_word);
    const element_count = list.element_count;

    if (element_size == 6) {
        const dest_offset = try dest.builder.writeListPointer(dest.segment_id, dest.pointer_pos, 6, element_count, dest.segment_id);
        const src_segment = src.message.segments[list.segment_id];
        var idx: u32 = 0;
        while (idx < element_count) : (idx += 1) {
            const src_ptr_pos = list.content_offset + @as(usize, idx) * 8;
            if (src_ptr_pos + 8 > src_segment.len) return error.OutOfBounds;
            const pointer_word = std.mem.readInt(u64, src_segment[src_ptr_pos..][0..8], .little);
            const src_ptr = AnyPointerReader{
                .message = src.message,
                .segment_id = list.segment_id,
                .pointer_pos = src_ptr_pos,
                .pointer_word = pointer_word,
            };

            const dest_ptr_pos = dest_offset + @as(usize, idx) * 8;
            const dest_ptr = AnyPointerBuilder{
                .builder = dest.builder,
                .segment_id = dest.segment_id,
                .pointer_pos = dest_ptr_pos,
            };

            if (pointer_word == 0) {
                try dest_ptr.setNull();
            } else {
                try cloneAnyPointer(src_ptr, dest_ptr);
            }
        }
        return;
    }

    const total_bytes = try listContentBytes(element_size, element_count);
    const dest_offset = try dest.builder.writeListPointer(dest.segment_id, dest.pointer_pos, element_size, element_count, dest.segment_id);
    if (total_bytes == 0) return;
    const src_segment = src.message.segments[list.segment_id];
    if (list.content_offset + total_bytes > src_segment.len) return error.OutOfBounds;

    const dest_segment = &dest.builder.segments.items[dest.segment_id];
    if (dest_offset + total_bytes > dest_segment.items.len) return error.OutOfBounds;
    std.mem.copyForwards(
        u8,
        dest_segment.items[dest_offset .. dest_offset + total_bytes],
        src_segment[list.content_offset .. list.content_offset + total_bytes],
    );
}
