const std = @import("std");
const log = std.log.scoped(.capnp_message);
const bounds = @import("message/bounds.zig");
const list_reader_module = @import("message/list_readers.zig");
const list_builder_module = @import("message/list_builders.zig");
const any_pointer_reader_module = @import("message/any_pointer_reader.zig");
const any_pointer_builder_module = @import("message/any_pointer_builder.zig");
const struct_builder_module = @import("message/struct_builder.zig");
const clone_any_pointer_module = @import("message/clone_any_pointer.zig");
const typed_list_helpers_module = @import("message/typed_list_helpers.zig");

const parse_diagnostics_enabled = if (@hasField(std.Options, "capnp_message_parse_diagnostics"))
    std.options.capnp_message_parse_diagnostics
else
    false;

inline fn parseDiagnostic(comptime fmt: []const u8, args: anytype) void {
    if (parse_diagnostics_enabled) log.warn(fmt, args);
}

fn decodeOffsetWords(pointer_word: u64) i32 {
    // Cap'n Proto stores a 30-bit signed offset in words (two's complement).
    const raw: u32 = @truncate((pointer_word >> 2) & 0x3FFFFFFF);
    if ((raw & 0x20000000) != 0) {
        return @as(i32, @intCast(raw)) - (@as(i32, 1) << 30);
    }
    return @as(i32, @intCast(raw));
}

fn encodeOffsetWords(offset_words: i32) !u32 {
    if (offset_words < -(1 << 29) or offset_words >= (1 << 29)) return error.OffsetOutOfRange;
    if (offset_words < 0) {
        const base: i64 = 1 << 30;
        return @as(u32, @intCast(base + offset_words));
    }
    return @as(u32, @intCast(offset_words));
}

fn makeStructPointer(offset_words: i32, data_words: u16, pointer_words: u16) !u64 {
    var pointer: u64 = 0;
    pointer |= @as(u64, try encodeOffsetWords(offset_words)) << 2;
    pointer |= @as(u64, data_words) << 32;
    pointer |= @as(u64, pointer_words) << 48;
    return pointer;
}

/// Maximum value representable in a Cap'n Proto list-pointer count field.
///
/// The count occupies bits 35..64 of a list pointer (29 bits). For inline
/// composite lists (element size 7) the same field holds the total word count
/// of the list body. Any count `>= (1 << 29)` cannot be encoded and would be
/// silently truncated by the `<< 35` shift, producing a valid-looking pointer
/// whose count is `count mod 2^29` (silent data corruption). Every list-pointer
/// write path funnels through `makeListPointer`, so guarding here rejects any
/// oversized count with `error.ListTooLarge`.
pub const MAX_LIST_ELEMENT_COUNT: u32 = (1 << 29) - 1;

fn makeListPointer(offset_words: i32, element_size: u3, element_count: u32) !u64 {
    if (element_count > MAX_LIST_ELEMENT_COUNT) return error.ListTooLarge;
    var pointer: u64 = 1; // list pointer
    pointer |= @as(u64, try encodeOffsetWords(offset_words)) << 2;
    pointer |= @as(u64, element_size) << 32;
    pointer |= @as(u64, element_count) << 35;
    return pointer;
}

fn makeFarPointer(landing_pad_is_double: bool, landing_pad_offset_words: u32, segment_id: u32) !u64 {
    // The landing-pad offset occupies bits 3..32 (29 bits). An offset >= 2^29
    // would overflow the `<< 3` shift into the segment-id field (bits 32..64),
    // silently corrupting the segment id. Reject it instead. (Same 29-bit wire
    // limit as list counts; reuse the constant.)
    if (landing_pad_offset_words > MAX_LIST_ELEMENT_COUNT) return error.FarPointerOffsetTooLarge;
    var pointer: u64 = 2; // far pointer
    if (landing_pad_is_double) {
        pointer |= @as(u64, 1) << 2;
    }
    pointer |= @as(u64, landing_pad_offset_words) << 3;
    pointer |= @as(u64, segment_id) << 32;
    return pointer;
}

fn makeCapabilityPointer(cap_id: u32) !u64 {
    return 3 | (@as(u64, cap_id) << 32);
}

fn decodeCapabilityPointer(pointer_word: u64) !u32 {
    if ((pointer_word & 0x3) != 3) return error.InvalidPointer;
    if (((pointer_word >> 2) & 0x3FFFFFFF) != 0) return error.InvalidPointer;
    return @as(u32, @truncate(pointer_word >> 32));
}

inline fn checkedAddUsize(a: usize, b: usize) error{OutOfBounds}!usize {
    return std.math.add(usize, a, b) catch return error.OutOfBounds;
}

inline fn checkedMulUsize(a: usize, b: usize) error{OutOfBounds}!usize {
    return std.math.mul(usize, a, b) catch return error.OutOfBounds;
}

inline fn hasByteRange(data: []const u8, offset: usize, size: usize) bool {
    const end = std.math.add(usize, offset, size) catch return false;
    return end <= data.len;
}

inline fn checkedSlice(data: []const u8, offset: usize, size: usize) error{OutOfBounds}![]const u8 {
    const end = try checkedAddUsize(offset, size);
    if (end > data.len) return error.OutOfBounds;
    return data[offset..end];
}

inline fn pointerIndexByteOffset(pointer_index: usize) error{OutOfBounds}!usize {
    return checkedMulUsize(pointer_index, 8);
}

inline fn wordsToBytes(words: usize) error{OutOfBounds}!usize {
    return checkedMulUsize(words, 8);
}

fn framedHeaderBytes(segment_count: usize) error{InvalidMessageSize}!usize {
    const padding_words: usize = if (segment_count % 2 == 0) 1 else 0;
    const header_words_no_padding = std.math.add(usize, 1, segment_count) catch return error.InvalidMessageSize;
    const header_words = std.math.add(usize, header_words_no_padding, padding_words) catch return error.InvalidMessageSize;
    return std.math.mul(usize, header_words, 4) catch return error.InvalidMessageSize;
}

fn maxFramedBytesForLimits(segment_count_limit: usize, total_words_limit: usize) usize {
    const header_bytes = framedHeaderBytes(segment_count_limit) catch unreachable;
    const payload_bytes = std.math.mul(usize, total_words_limit, 8) catch unreachable;
    return std.math.add(usize, header_bytes, payload_bytes) catch unreachable;
}

fn requireTextTerminator(text_data: []const u8) ![]const u8 {
    if (text_data.len == 0 or text_data[text_data.len - 1] != 0) {
        return error.InvalidTextPointer;
    }
    return text_data[0 .. text_data.len - 1];
}

fn requireStrictText(text_data: []const u8) ![]const u8 {
    const text = try requireTextTerminator(text_data);
    if (!std.unicode.utf8ValidateSlice(text)) {
        return error.InvalidUtf8;
    }
    return text;
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

fn addEstimatedUnpackedBytes(total: *usize, amount: usize, max_unpacked_bytes: usize) !void {
    total.* = std.math.add(usize, total.*, amount) catch return error.MessageTooLarge;
    if (total.* > max_unpacked_bytes) return error.MessageTooLarge;
}

fn unpackPackedLimited(allocator: std.mem.Allocator, packed_bytes: []const u8, max_unpacked_bytes: usize) ![]u8 {
    // Size-estimation pass: scan packed bytes to calculate exact output size.
    const total_size = try estimateUnpackedSizeLimited(packed_bytes, max_unpacked_bytes);

    var out = std.ArrayList(u8).empty;
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

fn unpackPacked(allocator: std.mem.Allocator, packed_bytes: []const u8) ![]u8 {
    return unpackPackedLimited(allocator, packed_bytes, std.math.maxInt(usize));
}

/// Scans packed bytes to calculate the exact unpacked output size without
/// performing any allocation. This mirrors the structure of the packing format:
///   - Tag 0x00: 1 zero word + N extra zero words (N from count byte)
///   - Tag 0xFF: 1 literal word + N literal words (N from count byte)
///   - Other tags: 1 word with @popCount(tag) non-zero bytes
fn estimateUnpackedSizeLimited(packed_bytes: []const u8, max_unpacked_bytes: usize) !usize {
    var total: usize = 0;
    var index: usize = 0;

    while (index < packed_bytes.len) {
        const tag = packed_bytes[index];
        index += 1;

        if (tag == 0x00) {
            try addEstimatedUnpackedBytes(&total, 8, max_unpacked_bytes);
            if (index >= packed_bytes.len) return error.UnexpectedEof;
            const count = packed_bytes[index];
            index += 1;
            try addEstimatedUnpackedBytes(&total, @as(usize, count) * 8, max_unpacked_bytes);
            continue;
        }

        if (tag == 0xFF) {
            if (index + 8 > packed_bytes.len) return error.UnexpectedEof;
            try addEstimatedUnpackedBytes(&total, 8, max_unpacked_bytes);
            index += 8;
            if (index >= packed_bytes.len) return error.UnexpectedEof;
            const count = packed_bytes[index];
            index += 1;
            const byte_count = @as(usize, count) * 8;
            if (index + byte_count > packed_bytes.len) return error.UnexpectedEof;
            try addEstimatedUnpackedBytes(&total, byte_count, max_unpacked_bytes);
            index += byte_count;
            continue;
        }

        // Regular tag: each set bit means one non-zero byte follows.
        try addEstimatedUnpackedBytes(&total, 8, max_unpacked_bytes);
        const nonzero_bytes = @popCount(tag);
        if (index + nonzero_bytes > packed_bytes.len) return error.UnexpectedEof;
        index += nonzero_bytes;
    }

    return total;
}

fn estimateUnpackedSize(packed_bytes: []const u8) !usize {
    return estimateUnpackedSizeLimited(packed_bytes, std.math.maxInt(usize));
}

/// Returns true if any byte in the u64 value is zero.
/// Uses the SWAR (SIMD Within A Register) bit trick to test all 8 bytes
/// in parallel with a constant number of operations, regardless of endianness.
inline fn wordHasZeroByte(v: u64) bool {
    return (v -% @as(u64, 0x0101010101010101)) & ~v & @as(u64, 0x8080808080808080) != 0;
}

fn packPacked(allocator: std.mem.Allocator, bytes: []const u8) ![]u8 {
    if (bytes.len % 8 != 0) return error.InvalidMessageSize;

    var out = std.ArrayList(u8).empty;
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

        // Fast path: if no byte in the word is zero, all 8 bytes are nonzero
        // so the tag must be 0xFF. Read the raw u64 value (endianness does not
        // matter — wordHasZeroByte operates on individual byte lanes) and use
        // the SWAR zero-byte test to skip per-byte iteration.
        const native_val: u64 = @bitCast(word[0..8].*);
        if (!wordHasZeroByte(native_val)) {
            // All bytes nonzero — collapse consecutive all-nonzero words
            // into a literal run record.
            var run: usize = 1;
            var scan = index + 8;
            while (run < 256 and scan + 8 <= bytes.len) : (scan += 8) {
                const next_native: u64 = @bitCast(bytes[scan..][0..8].*);
                if (wordHasZeroByte(next_native)) break;
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

        // Build tag byte and collect nonzero bytes for mixed words.
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
///
/// Use `init` or `initPacked` to deserialize and validate in one step (recommended
/// for untrusted input). Use `initUnvalidated` or `initPackedUnvalidated` to skip
/// validation when reading trusted data or when custom validation is needed.
///
/// Callers must keep the source data (or `backing_data`) alive for the lifetime
/// of any readers obtained from this message. Call `deinit` to free the
/// segment index (and backing data, if owned).
pub const Message = struct {
    pub const max_segment_count: usize = 512;
    pub const max_total_words: usize = 8 * 1024 * 1024;
    pub const max_packed_unpacked_bytes: usize = maxFramedBytesForLimits(max_segment_count, max_total_words);

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

    /// Configurable limits for message validation.
    ///
    /// - `segment_count_limit`: maximum number of segments allowed in the message.
    /// - `total_segment_words_limit`: maximum total words across all segments, including unreferenced segments.
    /// - `traversal_limit_words`: total words the validator may visit (prevents amplification attacks).
    /// - `inline_composite_element_limit`: total inline-composite struct-list elements the validator may visit.
    /// - `nesting_limit`: maximum pointer-following depth (prevents stack overflow from deep nesting).
    pub const ValidationOptions = struct {
        segment_count_limit: usize = max_segment_count,
        total_segment_words_limit: usize = max_total_words,
        traversal_limit_words: usize = 8 * 1024 * 1024,
        inline_composite_element_limit: usize = max_total_words,
        nesting_limit: usize = 64,
    };

    fn checkFramedWordLimit(data: []const u8, total_words_limit: usize) !void {
        if (data.len < 4) return;

        const segment_count_minus_one = std.mem.readInt(u32, data[0..4], .little);
        const segment_count = std.math.add(u32, segment_count_minus_one, 1) catch return error.InvalidSegmentCount;
        const segment_count_usize = std.math.cast(usize, segment_count) orelse return error.InvalidSegmentCount;
        if (segment_count_usize > max_segment_count) return error.SegmentCountLimitExceeded;

        const header_bytes = framedHeaderBytes(segment_count_usize) catch return error.InvalidMessageSize;
        if (header_bytes > data.len) return;

        var total_words: usize = 0;
        var cursor: usize = 4;
        for (0..segment_count_usize) |_| {
            const size_words = std.mem.readInt(u32, data[cursor..][0..4], .little);
            total_words = std.math.add(usize, total_words, @as(usize, size_words)) catch return error.InvalidMessageSize;
            if (total_words > total_words_limit) return error.MessageTooLarge;
            cursor += 4;
        }
    }

    /// Deserialize and validate a Cap'n Proto message from its framed wire representation.
    ///
    /// Parses the segment table header, slices `data` into per-segment views, and
    /// performs a full pointer-graph validation walk to enforce traversal and nesting
    /// limits. This is the recommended entry point for reading untrusted input.
    ///
    /// The caller retains ownership of `data`; this message borrows into it.
    pub fn init(allocator: std.mem.Allocator, data: []const u8, options: ValidationOptions) !Message {
        var msg = try initUnvalidated(allocator, data);
        errdefer msg.deinit();
        try msg.validateTotalSegmentWords(options.total_segment_words_limit);
        try msg.validate(options);
        return msg;
    }

    /// Deserialize a Cap'n Proto message from its framed wire representation
    /// without performing validation.
    ///
    /// Parses the segment table header and slices `data` into per-segment views.
    /// The caller retains ownership of `data`; this message borrows into it.
    pub fn initUnvalidated(allocator: std.mem.Allocator, data: []const u8) !Message {
        // Read segment count
        if (data.len < 4) {
            parseDiagnostic("TruncatedMessage: data too short for header ({} bytes)", .{data.len});
            return error.TruncatedMessage;
        }
        const segment_count_minus_one = std.mem.readInt(u32, data[0..4], .little);
        const segment_count = std.math.add(u32, segment_count_minus_one, 1) catch {
            parseDiagnostic("InvalidSegmentCount: raw segment_count_minus_one={}", .{segment_count_minus_one});
            return error.InvalidSegmentCount;
        };
        const segment_count_usize = std.math.cast(usize, segment_count) orelse return error.InvalidSegmentCount;
        if (segment_count_usize > max_segment_count) return error.SegmentCountLimitExceeded;
        const padding_words: usize = if (segment_count_usize % 2 == 0) 1 else 0;
        const header_words_no_padding = std.math.add(usize, 1, segment_count_usize) catch return error.InvalidMessageSize;
        const header_words = std.math.add(usize, header_words_no_padding, padding_words) catch return error.InvalidMessageSize;
        const header_bytes = std.math.mul(usize, header_words, 4) catch return error.InvalidMessageSize;
        if (header_bytes > data.len) {
            parseDiagnostic("TruncatedMessage: header requires {} bytes but data has {} bytes (segments={})", .{ header_bytes, data.len, segment_count });
            return error.TruncatedMessage;
        }

        // Allocate segment array
        const segments = try allocator.alloc([]const u8, segment_count_usize);
        errdefer allocator.free(segments);

        // Read segment sizes (in words)
        const segment_sizes = try allocator.alloc(u32, segment_count_usize);
        defer allocator.free(segment_sizes);

        // First segment size is in the next word
        var cursor: usize = 4;
        for (segment_sizes) |*size| {
            size.* = std.mem.readInt(u32, data[cursor..][0..4], .little);
            cursor += 4;
        }

        // Padding to 8-byte boundary
        if (segment_count % 2 == 0) {
            cursor += 4;
        }

        // Read segment data
        var offset: usize = cursor;
        for (segment_sizes, 0..) |size_words, i| {
            const size_words_usize = std.math.cast(usize, size_words) orelse return error.InvalidMessageSize;
            const size_bytes = std.math.mul(usize, size_words_usize, 8) catch return error.InvalidMessageSize;
            const end = std.math.add(usize, offset, size_bytes) catch return error.TruncatedMessage;
            if (end > data.len) {
                parseDiagnostic("TruncatedMessage: segment {} needs {} bytes at offset {} but data has {} bytes", .{ i, size_bytes, offset, data.len });
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

    /// Deserialize and validate a Cap'n Proto message from packed encoding.
    ///
    /// Unpacks `packed_bytes` into standard framed format, parses segments, and
    /// performs a full pointer-graph validation walk. The unpacked buffer is owned
    /// by this message and freed on `deinit`.
    pub fn initPacked(allocator: std.mem.Allocator, packed_bytes: []const u8, options: ValidationOptions) !Message {
        var msg = try initPackedUnvalidated(allocator, packed_bytes);
        errdefer msg.deinit();
        try msg.validateTotalSegmentWords(options.total_segment_words_limit);
        try msg.validate(options);
        return msg;
    }

    /// Deserialize a Cap'n Proto message from packed encoding without
    /// performing validation.
    ///
    /// Unpacks `packed_bytes` into standard framed format, then parses segments.
    /// The unpacked buffer is owned by this message and freed on `deinit`.
    pub fn initPackedUnvalidated(allocator: std.mem.Allocator, packed_bytes: []const u8) !Message {
        const unpacked = try unpackPackedLimited(allocator, packed_bytes, max_packed_unpacked_bytes);
        errdefer allocator.free(unpacked);
        try checkFramedWordLimit(unpacked, max_total_words);

        var msg = try Message.initUnvalidated(allocator, unpacked);
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
        try bounds.checkBounds(segment, byte_offset, 8);
        return std.mem.readInt(u64, segment[byte_offset..][0..8], .little);
    }

    fn validateTotalSegmentWords(self: *const Message, total_words_limit: usize) !void {
        var total_words: usize = 0;
        for (self.segments) |segment| {
            const words = segment.len / 8;
            total_words = std.math.add(usize, total_words, words) catch return error.InvalidMessageSize;
            if (total_words > total_words_limit) return error.MessageTooLarge;
        }
    }

    /// Resolve a far pointer's landing pad: validate the segment ID, compute
    /// the landing position, and bounds-check the landing pad (single or double).
    /// Returns the landing segment data and landing byte offset.
    fn resolveFarLandingPad(self: *const Message, far: FarPointer) !struct { segment: []const u8, landing_pos: usize } {
        if (far.segment_id >= self.segments.len) return error.InvalidSegmentId;
        const landing_pos = try wordsToBytes(@as(usize, far.landing_pad_offset_words));
        const landing_segment = self.segments[far.segment_id];
        const pad_size: usize = if (far.landing_pad_is_double) 16 else 8;
        try bounds.checkBounds(landing_segment, landing_pos, pad_size);
        return .{ .segment = landing_segment, .landing_pos = landing_pos };
    }

    /// Compute the content byte offset from a resolved pointer, using either
    /// the content_override (for double-far pointers) or the standard offset
    /// calculation from the pointer position.
    fn computeContentOffset(pointer_pos: usize, offset_words: i32, content_override: ?usize) !usize {
        if (content_override) |override| {
            return override;
        }
        const base = try checkedAddUsize(pointer_pos, 8);
        if (offset_words >= 0) {
            const delta = try wordsToBytes(@as(usize, @intCast(offset_words)));
            return checkedAddUsize(base, delta);
        }

        const delta_words = @as(usize, @intCast(-@as(i64, offset_words)));
        const delta = try wordsToBytes(delta_words);
        if (delta > base) return error.OutOfBounds;
        return base - delta;
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
        const pad = try self.resolveFarLandingPad(far);

        if (!far.landing_pad_is_double) {
            // Single-far: landing pad directly stores the pointed-to pointer word.
            const landing_word = try self.readWord(far.segment_id, pad.landing_pos);
            return try self.resolvePointer(far.segment_id, pad.landing_pos, landing_word, depth - 1);
        }

        // Double-far: landing pad has [far-to-content, tag-word].
        const landing_word = try self.readWord(far.segment_id, pad.landing_pos);
        const tag_word = try self.readWord(far.segment_id, pad.landing_pos + 8);

        const landing_type = @as(u2, @truncate(landing_word & 0x3));
        if (landing_type != 2) {
            parseDiagnostic("InvalidFarPointer: resolve double-far landing type {} (expected 2) at segment={} offset={}", .{ landing_type, far.segment_id, pad.landing_pos });
            return error.InvalidFarPointer;
        }
        const landing_far = decodeFarPointer(landing_word);
        if (landing_far.landing_pad_is_double) {
            parseDiagnostic("InvalidFarPointer: resolve nested double-far at segment={} offset={}", .{ far.segment_id, pad.landing_pos });
            return error.InvalidFarPointer;
        }
        if (landing_far.segment_id >= self.segments.len) return error.InvalidSegmentId;

        const content_offset = try wordsToBytes(@as(usize, landing_far.landing_pad_offset_words));
        return .{
            .segment_id = landing_far.segment_id,
            .pointer_pos = 0,
            .pointer_word = tag_word,
            .content_override = content_offset,
        };
    }

    pub fn resolveStructPointer(self: *const Message, segment_id: u32, pointer_pos: usize, pointer_word: u64) !StructReader {
        const resolved = try self.resolvePointer(segment_id, pointer_pos, pointer_word, 3);
        if (resolved.segment_id >= self.segments.len) return error.InvalidSegmentId;
        if (resolved.pointer_word == 0) {
            parseDiagnostic("InvalidRootPointer: null pointer at segment={} pos={}", .{ segment_id, pointer_pos });
            return error.InvalidRootPointer;
        }

        const pointer_type = @as(u2, @truncate(resolved.pointer_word & 0x3));
        if (pointer_type != 0) {
            parseDiagnostic("InvalidRootPointer: expected struct pointer (type 0) but got type {} at segment={} pos={}", .{ pointer_type, resolved.segment_id, resolved.pointer_pos });
            return error.InvalidRootPointer;
        }

        const offset = decodeOffsetWords(resolved.pointer_word);
        const data_size = @as(u16, @truncate((resolved.pointer_word >> 32) & 0xFFFF));
        const pointer_count = @as(u16, @truncate((resolved.pointer_word >> 48) & 0xFFFF));

        var struct_offset: usize = undefined;
        if (resolved.content_override) |override| {
            struct_offset = override;
        } else {
            struct_offset = computeContentOffset(resolved.pointer_pos, offset, null) catch return error.InvalidRootPointer;
        }

        const segment = self.segments[resolved.segment_id];
        const total_bytes = (@as(usize, data_size) + @as(usize, pointer_count)) * 8;
        bounds.checkBounds(segment, struct_offset, total_bytes) catch {
            parseDiagnostic("TruncatedMessage: struct needs {} bytes at offset {} but segment {} has {} bytes (data_words={} pointer_words={})", .{ total_bytes, struct_offset, resolved.segment_id, segment.len, data_size, pointer_count });
            return error.TruncatedMessage;
        };

        return StructReader{
            .message = self,
            .segment_id = resolved.segment_id,
            .offset = struct_offset,
            .data_size = data_size,
            .pointer_count = pointer_count,
        };
    }

    pub fn resolveListPointer(self: *const Message, segment_id: u32, pointer_pos: usize, pointer_word: u64) !ResolvedListPointer {
        const resolved = try self.resolvePointer(segment_id, pointer_pos, pointer_word, 3);
        if (resolved.segment_id >= self.segments.len) return error.InvalidSegmentId;
        if (resolved.pointer_word == 0) return error.InvalidPointer;

        const pointer_type = @as(u2, @truncate(resolved.pointer_word & 0x3));
        if (pointer_type != 1) {
            parseDiagnostic("InvalidPointer: expected list pointer (type 1) but got type {} at segment={} pos={}", .{ pointer_type, resolved.segment_id, resolved.pointer_pos });
            return error.InvalidPointer;
        }

        const offset = decodeOffsetWords(resolved.pointer_word);
        const element_size = @as(u3, @truncate((resolved.pointer_word >> 32) & 0x7));
        const element_count = @as(u32, @truncate((resolved.pointer_word >> 35)));

        const content_offset = try computeContentOffset(resolved.pointer_pos, offset, resolved.content_override);
        const content_bytes = switch (element_size) {
            0 => @as(usize, 0),
            1 => blk: {
                const padded = std.math.add(usize, @as(usize, element_count), 7) catch return error.OutOfBounds;
                break :blk padded / 8;
            },
            2 => @as(usize, element_count),
            3 => std.math.mul(usize, @as(usize, element_count), 2) catch return error.OutOfBounds,
            4 => std.math.mul(usize, @as(usize, element_count), 4) catch return error.OutOfBounds,
            5, 6 => std.math.mul(usize, @as(usize, element_count), 8) catch return error.OutOfBounds,
            7 => blk: {
                const words_with_tag = std.math.add(u64, @as(u64, element_count), 1) catch return error.OutOfBounds;
                if (words_with_tag > std.math.maxInt(usize) / 8) return error.OutOfBounds;
                break :blk @as(usize, @intCast(words_with_tag)) * 8;
            },
        };
        try bounds.checkBounds(self.segments[resolved.segment_id], content_offset, content_bytes);

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

            const tag_pos = try computeContentOffset(pointer_pos, offset, null);

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

            const elements_offset = try checkedAddUsize(tag_pos, 8);
            const total_bytes = @as(usize, word_count) * 8;
            try bounds.checkBounds(self.segments[segment_id], elements_offset, total_bytes);

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
        const pad = try self.resolveFarLandingPad(far);

        if (!far.landing_pad_is_double) {
            const landing_word = try self.readWord(far.segment_id, pad.landing_pos);
            const landing_type = @as(u2, @truncate(landing_word & 0x3));
            if (landing_type != 1) return error.InvalidPointer;
            return try self.resolveInlineCompositeList(far.segment_id, pad.landing_pos, landing_word);
        }

        const landing_word = try self.readWord(far.segment_id, pad.landing_pos);
        const second_word = try self.readWord(far.segment_id, pad.landing_pos + 8);

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
            const elements_offset = try wordsToBytes(@as(usize, landing_far.landing_pad_offset_words));
            const total_bytes = total_words * 8;
            try bounds.checkBounds(self.segments[landing_far.segment_id], elements_offset, total_bytes);

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

            const tag_pos = try wordsToBytes(@as(usize, landing_far.landing_pad_offset_words));
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

            const elements_offset = try checkedAddUsize(tag_pos, 8);
            const total_bytes = @as(usize, word_count) * 8;
            try bounds.checkBounds(self.segments[landing_far.segment_id], elements_offset, total_bytes);

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

    /// Walk the entire pointer graph starting from the root, enforcing traversal
    /// and nesting limits from `options`. Returns an error if the message exceeds
    /// the configured segment count, word traversal budget, or nesting depth.
    pub fn validate(self: *const Message, options: ValidationOptions) anyerror!void {
        if (self.segments.len == 0) return error.EmptyMessage;
        if (self.segments.len > options.segment_count_limit) return error.SegmentCountLimitExceeded;
        const segment = self.segments[0];
        if (segment.len < 8) return error.TruncatedMessage;

        const root_pointer = std.mem.readInt(u64, segment[0..8], .little);
        var remaining_words = options.traversal_limit_words;
        var remaining_inline_composite_elements = options.inline_composite_element_limit;
        try self.validatePointer(0, 0, root_pointer, &remaining_words, &remaining_inline_composite_elements, options.nesting_limit);
    }

    fn consumeWords(remaining: *usize, words: usize) !void {
        if (words > remaining.*) return error.TraversalLimitExceeded;
        remaining.* -= words;
    }

    fn consumeInlineCompositeElements(remaining: *usize, elements: usize) !void {
        if (elements > remaining.*) return error.TraversalLimitExceeded;
        remaining.* -= elements;
    }

    fn validatePointer(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        remaining_words: *usize,
        remaining_inline_composite_elements: *usize,
        nesting: usize,
    ) anyerror!void {
        if (pointer_word == 0) return;
        if (nesting == 0) return error.NestingLimitExceeded;
        if (segment_id >= self.segments.len) return error.InvalidSegmentId;

        const pointer_type = @as(u2, @truncate(pointer_word & 0x3));
        switch (pointer_type) {
            0 => try self.validateStructPointer(segment_id, pointer_pos, pointer_word, null, remaining_words, remaining_inline_composite_elements, nesting - 1),
            1 => try self.validateListPointer(segment_id, pointer_pos, pointer_word, null, remaining_words, remaining_inline_composite_elements, nesting - 1),
            2 => try self.validateFarPointer(segment_id, pointer_pos, pointer_word, remaining_words, remaining_inline_composite_elements, nesting - 1),
            3 => {
                _ = try decodeCapabilityPointer(pointer_word);
            },
        }
    }

    fn validateFarPointer(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        remaining_words: *usize,
        remaining_inline_composite_elements: *usize,
        nesting: usize,
    ) anyerror!void {
        _ = segment_id;
        _ = pointer_pos;
        const far = decodeFarPointer(pointer_word);
        const pad = try self.resolveFarLandingPad(far);

        // Charge the landing-pad words (1 for a single far, 2 for a double far)
        // against the traversal budget so far-pointer hops are accounted for
        // rather than free — otherwise a message can traverse extra words per
        // hop without touching the amplification cap.
        try consumeWords(remaining_words, if (far.landing_pad_is_double) 2 else 1);

        if (!far.landing_pad_is_double) {
            const landing_word = try self.readWord(far.segment_id, pad.landing_pos);
            return self.validatePointer(far.segment_id, pad.landing_pos, landing_word, remaining_words, remaining_inline_composite_elements, nesting);
        }

        const landing_word = try self.readWord(far.segment_id, pad.landing_pos);
        const tag_word = try self.readWord(far.segment_id, pad.landing_pos + 8);

        const landing_type = @as(u2, @truncate(landing_word & 0x3));
        if (landing_type != 2) {
            parseDiagnostic("InvalidFarPointer: double-far landing pad type {} (expected 2) at segment={} offset={}", .{ landing_type, far.segment_id, pad.landing_pos });
            return error.InvalidFarPointer;
        }
        const landing_far = decodeFarPointer(landing_word);
        if (landing_far.landing_pad_is_double) {
            parseDiagnostic("InvalidFarPointer: nested double-far at segment={} offset={}", .{ far.segment_id, pad.landing_pos });
            return error.InvalidFarPointer;
        }
        if (landing_far.segment_id >= self.segments.len) return error.InvalidSegmentId;

        const elements_offset = try wordsToBytes(@as(usize, landing_far.landing_pad_offset_words));
        const tag_type = @as(u2, @truncate(tag_word & 0x3));
        if (tag_type == 0) {
            return self.validateInlineCompositeTag(landing_far.segment_id, elements_offset, tag_word, remaining_words, remaining_inline_composite_elements, nesting);
        }
        if (tag_type == 1) {
            return self.validateListPointer(landing_far.segment_id, 0, tag_word, elements_offset, remaining_words, remaining_inline_composite_elements, nesting);
        }
        parseDiagnostic("InvalidFarPointer: unexpected tag type {} in double-far at target segment={} offset={}", .{ tag_type, landing_far.segment_id, elements_offset });
        return error.InvalidFarPointer;
    }

    fn validateStructPointer(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        content_override: ?usize,
        remaining_words: *usize,
        remaining_inline_composite_elements: *usize,
        nesting: usize,
    ) anyerror!void {
        const data_size = @as(u16, @truncate((pointer_word >> 32) & 0xFFFF));
        const pointer_count = @as(u16, @truncate((pointer_word >> 48) & 0xFFFF));

        var struct_offset: usize = undefined;
        if (content_override) |override| {
            struct_offset = override;
        } else {
            const offset = decodeOffsetWords(pointer_word);
            struct_offset = try computeContentOffset(pointer_pos, offset, null);
        }

        const segment = self.segments[segment_id];
        const total_words = @as(usize, data_size) + @as(usize, pointer_count);
        const total_bytes = try wordsToBytes(total_words);
        if (struct_offset > segment.len) {
            parseDiagnostic("OutOfBounds: struct offset {} exceeds segment {} length {} (data_words={} pointer_words={})", .{ struct_offset, segment_id, segment.len, data_size, pointer_count });
            return error.OutOfBounds;
        }
        if (total_bytes > segment.len - struct_offset) {
            parseDiagnostic("OutOfBounds: struct needs {} bytes at offset {} but segment {} has {} bytes remaining (data_words={} pointer_words={})", .{ total_bytes, struct_offset, segment_id, segment.len - struct_offset, data_size, pointer_count });
            return error.OutOfBounds;
        }

        try consumeWords(remaining_words, total_words);

        if (pointer_count == 0) return;
        const pointer_section_offset = try checkedAddUsize(struct_offset, try wordsToBytes(@as(usize, data_size)));
        var idx: usize = 0;
        while (idx < pointer_count) : (idx += 1) {
            const ptr_pos = try checkedAddUsize(pointer_section_offset, try pointerIndexByteOffset(idx));
            const word = std.mem.readInt(u64, segment[ptr_pos..][0..8], .little);
            try self.validatePointer(segment_id, ptr_pos, word, remaining_words, remaining_inline_composite_elements, nesting);
        }
    }

    fn validateListPointer(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        content_override: ?usize,
        remaining_words: *usize,
        remaining_inline_composite_elements: *usize,
        nesting: usize,
    ) anyerror!void {
        const element_size = @as(u3, @truncate((pointer_word >> 32) & 0x7));
        if (element_size == 7 and content_override == null) {
            return self.validateInlineCompositeList(segment_id, pointer_pos, pointer_word, remaining_words, remaining_inline_composite_elements, nesting);
        }
        if (element_size == 7 and content_override != null) {
            // Layout B double-far inline-composite list: the list pointer (pointer_word)
            // is in the landing pad with element_size 7 and word_count; the tag word is
            // at content_override in the target segment.
            const word_count = @as(u32, @truncate(pointer_word >> 35));
            const tag_pos = content_override.?;
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

            const elements_offset = try checkedAddUsize(tag_pos, 8);
            const total_bytes = try wordsToBytes(@as(usize, word_count));
            const segment = self.segments[segment_id];
            if (elements_offset > segment.len) return error.OutOfBounds;
            if (total_bytes > segment.len - elements_offset) return error.OutOfBounds;

            try consumeInlineCompositeElements(remaining_inline_composite_elements, @as(usize, element_count));
            try consumeWords(remaining_words, @as(usize, word_count));

            if (pointer_words == 0 or element_count == 0) return;
            const element_stride = try wordsToBytes(@as(usize, data_words) + @as(usize, pointer_words));
            var element_index: u32 = 0;
            while (element_index < element_count) : (element_index += 1) {
                const element_base = try checkedAddUsize(elements_offset, try checkedMulUsize(@as(usize, element_index), element_stride));
                const pointer_section = try checkedAddUsize(element_base, try wordsToBytes(@as(usize, data_words)));
                var ptr_index: u16 = 0;
                while (ptr_index < pointer_words) : (ptr_index += 1) {
                    const ptr_pos = try checkedAddUsize(pointer_section, try pointerIndexByteOffset(@as(usize, ptr_index)));
                    const word = std.mem.readInt(u64, segment[ptr_pos..][0..8], .little);
                    try self.validatePointer(segment_id, ptr_pos, word, remaining_words, remaining_inline_composite_elements, nesting);
                }
            }
            return;
        }

        const element_count = @as(u32, @truncate((pointer_word >> 35)));
        var content_offset: usize = undefined;
        if (content_override) |override| {
            content_offset = override;
        } else {
            const offset = decodeOffsetWords(pointer_word);
            content_offset = try computeContentOffset(pointer_pos, offset, null);
        }

        const segment = self.segments[segment_id];
        const total_bytes = try listContentBytes(element_size, element_count);
        if (content_offset > segment.len) {
            parseDiagnostic("OutOfBounds: list offset {} exceeds segment {} length {} (element_size={} count={})", .{ content_offset, segment_id, segment.len, element_size, element_count });
            return error.OutOfBounds;
        }
        if (total_bytes > segment.len - content_offset) {
            parseDiagnostic("OutOfBounds: list needs {} bytes at offset {} but segment {} has {} bytes remaining (element_size={} count={})", .{ total_bytes, content_offset, segment_id, segment.len - content_offset, element_size, element_count });
            return error.OutOfBounds;
        }

        const total_words = try listContentWords(element_size, element_count);
        try consumeWords(remaining_words, total_words);

        if (element_size != 6 or element_count == 0) return;
        var index: u32 = 0;
        while (index < element_count) : (index += 1) {
            const ptr_pos = try checkedAddUsize(content_offset, try pointerIndexByteOffset(@as(usize, index)));
            const word = std.mem.readInt(u64, segment[ptr_pos..][0..8], .little);
            try self.validatePointer(segment_id, ptr_pos, word, remaining_words, remaining_inline_composite_elements, nesting);
        }
    }

    fn validateInlineCompositeList(
        self: *const Message,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        remaining_words: *usize,
        remaining_inline_composite_elements: *usize,
        nesting: usize,
    ) anyerror!void {
        const list = try self.resolveInlineCompositeList(segment_id, pointer_pos, pointer_word);
        const word_count = @as(u32, @truncate(pointer_word >> 35));
        try consumeInlineCompositeElements(remaining_inline_composite_elements, @as(usize, list.element_count));
        try consumeWords(remaining_words, @as(usize, word_count));

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
                try self.validatePointer(list.segment_id, ptr_pos, word, remaining_words, remaining_inline_composite_elements, nesting);
            }
        }
    }

    fn validateInlineCompositeTag(
        self: *const Message,
        segment_id: u32,
        elements_offset: usize,
        tag_word: u64,
        remaining_words: *usize,
        remaining_inline_composite_elements: *usize,
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

        try consumeInlineCompositeElements(remaining_inline_composite_elements, @as(usize, element_count));
        try consumeWords(remaining_words, total_words);

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
                try self.validatePointer(segment_id, ptr_pos, word, remaining_words, remaining_inline_composite_elements, nesting);
            }
        }
    }

    /// Return a reader for the root struct of this message.
    ///
    /// The root pointer is always at offset 0 in segment 0.
    ///
    /// **Warning**: This method does not enforce a traversal limit.  For
    /// untrusted input, call `validate()` first to guard against
    /// amplification attacks from cyclic or deeply nested pointers.
    pub fn getRootStruct(self: *const Message) !StructReader {
        if (self.segments.len == 0) return error.EmptyMessage;

        // The root is always at offset 0 in segment 0
        // For our simple case, we'll treat the first segment as the struct data directly
        const segment = self.segments[0];
        if (segment.len < 8) return error.TruncatedMessage;

        // Read the root pointer
        const root_pointer = std.mem.readInt(u64, segment[0..8], .little);

        // A null root pointer denotes an absent root: per the Cap'n Proto spec
        // (and consistent with getRootAnyPointer and reference implementations)
        // it reads as an empty default struct rather than error.InvalidPointer,
        // which is what schema-evolution reads of an added struct field rely on.
        if (root_pointer == 0) {
            return .{ .message = self, .segment_id = 0, .offset = 0, .data_size = 0, .pointer_count = 0 };
        }

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
        const data_bytes = wordsToBytes(@as(usize, self.data_size)) catch return &[_]u8{};
        const end = checkedAddUsize(start, data_bytes) catch return &[_]u8{};
        if (end > segment.len) return &[_]u8{};
        return segment[start..end];
    }

    pub fn getPointerSection(self: StructReader) []const u8 {
        const segment = self.message.segments[self.segment_id];
        const data_bytes = wordsToBytes(@as(usize, self.data_size)) catch return &[_]u8{};
        const pointer_bytes = wordsToBytes(@as(usize, self.pointer_count)) catch return &[_]u8{};
        const start = checkedAddUsize(self.offset, data_bytes) catch return &[_]u8{};
        const end = checkedAddUsize(start, pointer_bytes) catch return &[_]u8{};
        if (end > segment.len) return &[_]u8{};
        return segment[start..end];
    }

    /// Check whether the pointer at `pointer_index` is null.
    // pointer_index is always a small codegen constant; overflow not possible in practice
    pub fn isPointerNull(self: StructReader, pointer_index: usize) bool {
        const pointers = self.getPointerSection();
        const pointer_offset = pointerIndexByteOffset(pointer_index) catch return true;
        if (!hasByteRange(pointers, pointer_offset, 8)) return true;
        const pointer_word = std.mem.readInt(u64, pointers[pointer_offset..][0..8], .little);
        return pointer_word == 0;
    }

    /// An empty (zero-size) struct reader anchored to this reader's message.
    ///
    /// Used by generated getters as the default for an unset (null-pointer)
    /// struct field: per the Cap'n Proto spec a null struct pointer reads back
    /// as a struct with all fields at their defaults, which a zero data/pointer
    /// StructReader provides. This mirrors how `readText` returns "" for null.
    pub fn emptyStruct(self: StructReader) StructReader {
        return .{
            .message = self.message,
            .segment_id = self.segment_id,
            .offset = self.offset,
            .data_size = 0,
            .pointer_count = 0,
        };
    }

    /// An empty struct-list reader anchored to this reader's message. Used by
    /// generated getters as the default for an unset (null-pointer) List(struct)
    /// field, mirroring the null-pointer-means-default semantics.
    pub fn emptyStructList(self: StructReader) StructListReader {
        return .{
            .message = self.message,
            .segment_id = self.segment_id,
            .elements_offset = 0,
            .element_count = 0,
            .data_words = 0,
            .pointer_words = 0,
        };
    }

    /// An empty list reader of type `ListReader` anchored to this reader's
    /// message. Works for every primitive/text/pointer list reader (which share
    /// the `{ message, segment_id, elements_offset, element_count }` layout) as
    /// well as `VoidListReader` (which carries only `element_count`). Used by
    /// generated getters as the default for an unset (null-pointer) list field.
    pub fn emptyList(self: StructReader, comptime ListReader: type) ListReader {
        var reader: ListReader = undefined;
        reader.element_count = 0;
        if (@hasField(ListReader, "message")) reader.message = self.message;
        if (@hasField(ListReader, "segment_id")) reader.segment_id = self.segment_id;
        if (@hasField(ListReader, "elements_offset")) reader.elements_offset = 0;
        return reader;
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
        if (!hasByteRange(data, byte_offset, 8)) return 0;
        return std.mem.readInt(u64, data[byte_offset..][0..8], .little);
    }

    /// Strict variant of `readU64` that returns `error.OutOfBounds` instead of
    /// the default value when the byte offset falls outside the data section.
    /// Intended for protocol-internal parsing where the field is required.
    pub fn readU64Strict(self: StructReader, byte_offset: usize) error{OutOfBounds}!u64 {
        const data = self.getDataSection();
        try bounds.checkBounds(data, byte_offset, 8);
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
        if (!hasByteRange(data, byte_offset, 4)) return 0;
        return std.mem.readInt(u32, data[byte_offset..][0..4], .little);
    }

    /// Strict variant of `readU32` that returns `error.OutOfBounds` instead of
    /// the default value when the byte offset falls outside the data section.
    /// Intended for protocol-internal parsing where the field is required.
    pub fn readU32Strict(self: StructReader, byte_offset: usize) error{OutOfBounds}!u32 {
        const data = self.getDataSection();
        try bounds.checkBounds(data, byte_offset, 4);
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
        if (!hasByteRange(data, byte_offset, 2)) return 0;
        return std.mem.readInt(u16, data[byte_offset..][0..2], .little);
    }

    /// Strict variant of `readU16` that returns `error.OutOfBounds` instead of
    /// the default value when the byte offset falls outside the data section.
    /// Intended for protocol-internal parsing where the field is required.
    pub fn readU16Strict(self: StructReader, byte_offset: usize) error{OutOfBounds}!u16 {
        const data = self.getDataSection();
        try bounds.checkBounds(data, byte_offset, 2);
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
        try bounds.checkOffset(data, byte_offset);
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
        try bounds.checkOffset(data, byte_offset);
        const byte = data[byte_offset];
        return (byte & (@as(u8, 1) << bit_offset)) != 0;
    }

    /// Read a union discriminant (which field is set)
    pub fn readUnionDiscriminant(self: StructReader, byte_offset: usize) u16 {
        return self.readU16(byte_offset);
    }

    fn absolutePointerPos(self: StructReader, pointer_offset: usize) !usize {
        const data_bytes = try wordsToBytes(@as(usize, self.data_size));
        const pointer_section_start = try checkedAddUsize(self.offset, data_bytes);
        return checkedAddUsize(pointer_section_start, pointer_offset);
    }

    /// Read a struct list (inline-composite encoding) from the given pointer index.
    pub fn readStructList(self: StructReader, pointer_index: usize) !StructListReader {
        const pointers = self.getPointerSection();
        const pointer_offset = try pointerIndexByteOffset(pointer_index);
        try bounds.checkBounds(pointers, pointer_offset, 8);

        const pointer_data = pointers[pointer_offset..][0..8];
        const pointer_word = std.mem.readInt(u64, pointer_data, .little);
        if (pointer_word == 0) return error.InvalidPointer;

        const absolute_pointer_pos = try self.absolutePointerPos(pointer_offset);
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
        const pointer_offset = try pointerIndexByteOffset(pointer_index);
        try bounds.checkBounds(pointers, pointer_offset, 8);

        const pointer_data = pointers[pointer_offset..][0..8];
        const pointer_word = std.mem.readInt(u64, pointer_data, .little);
        if (pointer_word == 0) return error.InvalidPointer;

        const absolute_pointer_pos = try self.absolutePointerPos(pointer_offset);
        return self.message.resolveListPointer(self.segment_id, absolute_pointer_pos, pointer_word);
    }

    /// Read a list of text (string) pointers from the given pointer index.
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

    /// Read a list of pointers (type-erased) from the given pointer index.
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

    /// Read a nested struct from the given pointer index.
    pub fn readStruct(self: StructReader, pointer_index: usize) !StructReader {
        const pointers = self.getPointerSection();
        const pointer_offset = try pointerIndexByteOffset(pointer_index);
        try bounds.checkBounds(pointers, pointer_offset, 8);

        const pointer_data = pointers[pointer_offset..][0..8];
        const pointer_word = std.mem.readInt(u64, pointer_data, .little);
        if (pointer_word == 0) return error.InvalidPointer;

        const absolute_pointer_pos = try self.absolutePointerPos(pointer_offset);
        return self.message.resolveStructPointer(self.segment_id, absolute_pointer_pos, pointer_word);
    }

    /// Read a type-erased pointer from the given pointer index.
    pub fn readAnyPointer(self: StructReader, pointer_index: usize) !AnyPointerReader {
        const pointers = self.getPointerSection();
        const pointer_offset = try pointerIndexByteOffset(pointer_index);
        try bounds.checkBounds(pointers, pointer_offset, 8);

        const pointer_data = pointers[pointer_offset..][0..8];
        const pointer_word = std.mem.readInt(u64, pointer_data, .little);
        const absolute_pointer_pos = try self.absolutePointerPos(pointer_offset);

        return .{
            .message = self.message,
            .segment_id = self.segment_id,
            .pointer_pos = absolute_pointer_pos,
            .pointer_word = pointer_word,
        };
    }

    /// Read a capability index from the given pointer index.
    pub fn readCapability(self: StructReader, pointer_index: usize) !Capability {
        const any = try self.readAnyPointer(pointer_index);
        return any.getCapability();
    }

    /// Read raw bytes from a data pointer at the given pointer index.
    pub fn readData(self: StructReader, pointer_index: usize) ![]const u8 {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 2) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, total_bytes);

        const segment = self.message.segments[list.segment_id];
        return checkedSlice(segment, list.content_offset, total_bytes);
    }

    /// Read a `List(UInt8)` from the given pointer index.
    pub fn readU8List(self: StructReader, pointer_index: usize) !U8ListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 2) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, total_bytes);

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    /// Cast a list reader to a different element type with the same layout.
    /// Used to reinterpret unsigned list readers as signed/float variants.
    fn castListReader(comptime Target: type, source: anytype) Target {
        return .{
            .message = source.message,
            .segment_id = source.segment_id,
            .elements_offset = source.elements_offset,
            .element_count = source.element_count,
        };
    }

    /// Read a `List(Int8)` from the given pointer index.
    pub fn readI8List(self: StructReader, pointer_index: usize) !I8ListReader {
        return castListReader(I8ListReader, try self.readU8List(pointer_index));
    }

    /// Read a `List(UInt16)` from the given pointer index.
    pub fn readU16List(self: StructReader, pointer_index: usize) !U16ListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 3) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, total_bytes);

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    /// Read a `List(Int16)` from the given pointer index.
    pub fn readI16List(self: StructReader, pointer_index: usize) !I16ListReader {
        return castListReader(I16ListReader, try self.readU16List(pointer_index));
    }

    /// Read a `List(UInt32)` from the given pointer index.
    pub fn readU32List(self: StructReader, pointer_index: usize) !U32ListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 4) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, total_bytes);

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    /// Read a `List(Int32)` from the given pointer index.
    pub fn readI32List(self: StructReader, pointer_index: usize) !I32ListReader {
        return castListReader(I32ListReader, try self.readU32List(pointer_index));
    }

    /// Read a `List(Float32)` from the given pointer index.
    pub fn readF32List(self: StructReader, pointer_index: usize) !F32ListReader {
        return castListReader(F32ListReader, try self.readU32List(pointer_index));
    }

    /// Read a `List(UInt64)` from the given pointer index.
    pub fn readU64List(self: StructReader, pointer_index: usize) !U64ListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 5) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, total_bytes);

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    /// Read a `List(Int64)` from the given pointer index.
    pub fn readI64List(self: StructReader, pointer_index: usize) !I64ListReader {
        return castListReader(I64ListReader, try self.readU64List(pointer_index));
    }

    /// Read a `List(Float64)` from the given pointer index.
    pub fn readF64List(self: StructReader, pointer_index: usize) !F64ListReader {
        return castListReader(F64ListReader, try self.readU64List(pointer_index));
    }

    /// Read a `List(Bool)` from the given pointer index.
    pub fn readBoolList(self: StructReader, pointer_index: usize) !BoolListReader {
        const list = try self.resolveListPointerAt(pointer_index);
        if (list.element_size != 1) return error.InvalidPointer;

        const total_bytes = try listContentBytes(list.element_size, list.element_count);
        try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, total_bytes);

        return .{
            .message = self.message,
            .segment_id = list.segment_id,
            .elements_offset = list.content_offset,
            .element_count = list.element_count,
        };
    }

    /// Read a `List(Void)` from the given pointer index.
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
    fn readTextData(self: StructReader, pointer_index: usize) !?[]const u8 {
        const pointers = self.getPointerSection();
        const pointer_offset = pointerIndexByteOffset(pointer_index) catch return null;
        if (!hasByteRange(pointers, pointer_offset, 8)) return null;

        const pointer_data = pointers[pointer_offset..][0..8];
        const pointer_word = std.mem.readInt(u64, pointer_data, .little);

        // Check if null pointer
        if (pointer_word == 0) return null;

        const absolute_pointer_pos = try self.absolutePointerPos(pointer_offset);
        const list = try self.message.resolveListPointer(self.segment_id, absolute_pointer_pos, pointer_word);

        // Text should be byte-sized elements
        if (list.element_size != 2) return error.InvalidTextPointer;

        try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, list.element_count);

        const segment = self.message.segments[list.segment_id];
        return try checkedSlice(segment, list.content_offset, list.element_count);
    }

    pub fn readText(self: StructReader, pointer_index: usize) ![]const u8 {
        const text_data = (try self.readTextData(pointer_index)) orelse return "";
        return bounds.stripNullTerminator(text_data);
    }

    /// Read a text field with strict UTF-8 validation.
    ///
    /// Like `readText`, but non-null text must include the wire-format trailing
    /// NUL byte and contain well-formed UTF-8 before that terminator.
    pub fn readTextStrict(self: StructReader, pointer_index: usize) ![]const u8 {
        const text_data = (try self.readTextData(pointer_index)) orelse return "";
        return requireStrictText(text_data);
    }
};

const list_reader_defs = list_reader_module.define(
    Message,
    StructReader,
    Capability,
    InlineCompositeList,
    listContentBytes,
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

    /// Set this pointer to null (zero).
    pub fn setNull(self: AnyPointerBuilder) !void {
        return any_pointer_builder_module.setNull(self.builder, self.segment_id, self.pointer_pos);
    }

    /// Write a text (string) value at this pointer position.
    pub fn setText(self: AnyPointerBuilder, text: []const u8) !void {
        return any_pointer_builder_module.setText(self.builder, self.segment_id, self.pointer_pos, text);
    }

    /// Write a data (byte slice) value at this pointer position.
    pub fn setData(self: AnyPointerBuilder, data: []const u8) !void {
        return any_pointer_builder_module.setData(self.builder, self.segment_id, self.pointer_pos, data);
    }

    /// Write a capability pointer referencing the given capability table entry.
    pub fn setCapability(self: AnyPointerBuilder, cap: Capability) !void {
        return any_pointer_builder_module.setCapability(
            self.builder,
            self.segment_id,
            self.pointer_pos,
            cap,
            makeCapabilityPointer,
        );
    }

    /// Allocate and initialize a struct at this pointer position.
    pub fn initStruct(self: AnyPointerBuilder, data_words: u16, pointer_words: u16) !StructBuilder {
        return self.builder.writeStructPointer(self.segment_id, self.pointer_pos, data_words, pointer_words, self.segment_id);
    }

    /// Allocate and initialize a struct list (inline-composite) at this pointer position.
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

    /// Allocate and initialize a list of pointers at this pointer position.
    pub fn initPointerList(self: AnyPointerBuilder, element_count: u32) !PointerListBuilder {
        const offset = try any_pointer_builder_module.initList(self.builder, self.segment_id, self.pointer_pos, 6, element_count);
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = offset,
            .element_count = element_count,
        };
    }

    fn initTypedList(self: AnyPointerBuilder, comptime T: type, element_size: u3, element_count: u32) !T {
        const offset = try any_pointer_builder_module.initList(self.builder, self.segment_id, self.pointer_pos, element_size, element_count);
        if (T == VoidListBuilder) {
            return .{ .element_count = element_count };
        }
        return .{
            .builder = self.builder,
            .segment_id = self.segment_id,
            .elements_offset = offset,
            .element_count = element_count,
        };
    }

    /// Initialize a `List(Void)` at this pointer position.
    pub fn initVoidList(self: AnyPointerBuilder, element_count: u32) !VoidListBuilder {
        return self.initTypedList(VoidListBuilder, 0, element_count);
    }

    /// Initialize a `List(UInt8)` at this pointer position.
    pub fn initU8List(self: AnyPointerBuilder, element_count: u32) !U8ListBuilder {
        return self.initTypedList(U8ListBuilder, 2, element_count);
    }

    /// Initialize a `List(UInt16)` at this pointer position.
    pub fn initU16List(self: AnyPointerBuilder, element_count: u32) !U16ListBuilder {
        return self.initTypedList(U16ListBuilder, 3, element_count);
    }

    /// Initialize a `List(UInt32)` at this pointer position.
    pub fn initU32List(self: AnyPointerBuilder, element_count: u32) !U32ListBuilder {
        return self.initTypedList(U32ListBuilder, 4, element_count);
    }

    /// Initialize a `List(UInt64)` at this pointer position.
    pub fn initU64List(self: AnyPointerBuilder, element_count: u32) !U64ListBuilder {
        return self.initTypedList(U64ListBuilder, 5, element_count);
    }

    /// Initialize a `List(Bool)` at this pointer position.
    pub fn initBoolList(self: AnyPointerBuilder, element_count: u32) !BoolListBuilder {
        return self.initTypedList(BoolListBuilder, 1, element_count);
    }

    /// Initialize a `List(Float32)` at this pointer position.
    pub fn initF32List(self: AnyPointerBuilder, element_count: u32) !F32ListBuilder {
        return self.initTypedList(F32ListBuilder, 4, element_count);
    }

    /// Initialize a `List(Float64)` at this pointer position.
    pub fn initF64List(self: AnyPointerBuilder, element_count: u32) !F64ListBuilder {
        return self.initTypedList(F64ListBuilder, 5, element_count);
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
            .segments = std.ArrayList(std.ArrayList(u8)).empty,
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
        if (self.segments.items.len > std.math.maxInt(u32)) return error.TooManySegments;
        const id: u32 = @intCast(self.segments.items.len);
        try self.segments.append(self.allocator, std.ArrayList(u8).empty);
        errdefer _ = self.segments.pop();
        if (min_capacity > 0) {
            try self.segments.items[id].ensureTotalCapacity(self.allocator, min_capacity);
        }
        return id;
    }

    /// Allocate a new empty segment and return its ID.
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

    /// Allocate a struct in a specific segment, creating it if necessary.
    pub fn allocateStructInSegment(self: *MessageBuilder, segment_id: u32, data_words: u16, pointer_words: u16) !StructBuilder {
        if (self.segments.items.len == 0) {
            _ = try self.createSegmentWithCapacity(initial_segment_capacity_bytes);
        }
        try self.segments.ensureTotalCapacity(self.allocator, segment_id + 1);
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

        try self.segments.ensureTotalCapacity(self.allocator, segment_id + 1);
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

        const struct_ptr = try makeStructPointer(0, data_words, pointer_words);
        std.mem.writeInt(u64, target_segment.items[landing_pad_pos..][0..8], struct_ptr, .little);

        {
            const root_pointer = try root_segment.addManyAsSlice(self.allocator, 8);
            @memset(root_pointer, 0);
        }
        const far_ptr = try makeFarPointer(false, @as(u32, @intCast(landing_pad_pos / 8)), segment_id);
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
        try bounds.checkBoundsMut(pointer_segment.items, pointer_pos, 8);

        // Text is a List(UInt8) with a NUL terminator, so the on-wire element
        // count is `text.len + 1`. The 29-bit list-pointer count field caps
        // that at MAX_LIST_ELEMENT_COUNT; reject longer text before allocating.
        if (text.len >= MAX_LIST_ELEMENT_COUNT) return error.TextTooLong;

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
            // Safe: segment sizes bounded well below i32 range by allocation limits
            const relative_offset = @as(i32, @intCast(@divTrunc(
                @as(isize, @intCast(text_offset)) - @as(isize, @intCast(pointer_pos)) - 8,
                8,
            )));
            const pointer = try makeListPointer(relative_offset, 2, @as(u32, @intCast(text.len + 1)));
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
        const list_ptr = try makeListPointer(list_offset, 2, @as(u32, @intCast(text.len + 1)));
        std.mem.writeInt(u64, target_segment.items[landing_pad_pos..][0..8], list_ptr, .little);

        const far_ptr = try makeFarPointer(false, @as(u32, @intCast(landing_pad_pos / 8)), target_segment_id);
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

        const pointer_segment = &self.segments.items[pointer_segment_id];
        try bounds.checkBoundsMut(pointer_segment.items, pointer_pos, 8);

        // Empty structs (0 data words, 0 pointers) are canonically represented
        // as NULL pointers per the Cap'n Proto spec. Writing a non-zero struct
        // pointer with an offset causes strict validators (e.g. capnp-rust) to
        // reject the message.
        if (data_words == 0 and pointer_words == 0) {
            std.mem.writeInt(u64, pointer_segment.items[pointer_pos..][0..8], 0, .little);
            return StructBuilder{
                .builder = self,
                .segment_id = pointer_segment_id,
                .offset = pointer_pos,
                .data_size = 0,
                .pointer_count = 0,
            };
        }

        if (target_segment_id >= self.segments.items.len) return error.InvalidSegmentId;

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
            const pointer = try makeStructPointer(relative_offset, data_words, pointer_words);
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

        const struct_ptr = try makeStructPointer(0, data_words, pointer_words);
        std.mem.writeInt(u64, target_segment.items[landing_pad_pos..][0..8], struct_ptr, .little);

        const far_ptr = try makeFarPointer(false, @as(u32, @intCast(landing_pad_pos / 8)), target_segment_id);
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

        // Reject counts the 29-bit list-pointer field cannot hold before
        // allocating content (a hostile count would otherwise allocate up to
        // gigabytes and then silently truncate to `count mod 2^29`).
        if (element_count > MAX_LIST_ELEMENT_COUNT) return error.ListTooLarge;

        const pointer_segment = &self.segments.items[pointer_segment_id];
        try bounds.checkBoundsMut(pointer_segment.items, pointer_pos, 8);

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
            const list_ptr = try makeListPointer(rel_offset, element_size, element_count);
            std.mem.writeInt(u64, pointer_segment.items[pointer_pos..][0..8], list_ptr, .little);
        } else {
            const landing_pos = landing_pad_pos.?;
            const rel_offset = @as(i32, @intCast(@divTrunc(
                @as(isize, @intCast(list_offset)) - @as(isize, @intCast(landing_pos)) - 8,
                8,
            )));
            const list_ptr = try makeListPointer(rel_offset, element_size, element_count);
            std.mem.writeInt(u64, target_segment.items[landing_pos..][0..8], list_ptr, .little);

            const far_ptr = try makeFarPointer(false, @as(u32, @intCast(landing_pos / 8)), target_segment_id);
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
        // The inline-composite list pointer stores `total_words` in the 29-bit
        // count field; reject anything the field cannot hold before allocating.
        if (total_words_u64 > MAX_LIST_ELEMENT_COUNT) return error.ListTooLarge;
        const total_words = @as(u32, @intCast(total_words_u64));
        const total_bytes = @as(usize, total_words) * 8;

        while (self.segments.items.len <= landing_segment_id or self.segments.items.len <= content_segment_id) {
            _ = try self.createSegment();
        }

        if (pointer_segment_id >= self.segments.items.len) return error.InvalidSegmentId;
        const source_segment = &self.segments.items[pointer_segment_id];
        try bounds.checkBoundsMut(source_segment.items, pointer_pos, 8);

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

            const tag_word = try makeStructPointer(@as(i32, @intCast(element_count)), data_words, pointer_words);
            std.mem.writeInt(u64, target_segment.items[tag_offset..][0..8], tag_word, .little);

            if (pointer_segment_id == landing_segment_id) {
                const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(tag_offset)) - @as(isize, @intCast(pointer_pos)) - 8, 8)));
                const list_ptr = try makeListPointer(rel_offset, 7, total_words);
                std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], list_ptr, .little);
            } else {
                const landing_pos = landing_pad_pos.?;
                const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(tag_offset)) - @as(isize, @intCast(landing_pos)) - 8, 8)));
                const list_ptr = try makeListPointer(rel_offset, 7, total_words);
                std.mem.writeInt(u64, target_segment.items[landing_pos..][0..8], list_ptr, .little);

                const far_ptr = try makeFarPointer(false, @as(u32, @intCast(landing_pos / 8)), landing_segment_id);
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

        const landing_far = try makeFarPointer(false, @as(u32, @intCast(elements_offset / 8)), content_segment_id);
        std.mem.writeInt(u64, landing_segment.items[landing_pad_pos..][0..8], landing_far, .little);

        const tag_word = try makeStructPointer(@as(i32, @intCast(element_count)), data_words, pointer_words);
        std.mem.writeInt(u64, landing_segment.items[landing_pad_pos + 8 ..][0..8], tag_word, .little);

        const far_ptr = try makeFarPointer(true, @as(u32, @intCast(landing_pad_pos / 8)), landing_segment_id);
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
        var result = std.ArrayList(u8).empty;
        errdefer result.deinit(self.allocator);

        // Ensure we have at least one segment
        if (self.segments.items.len == 0) {
            _ = try self.createSegmentWithCapacity(initial_segment_capacity_bytes);
        }

        const segment_count_usize = self.segments.items.len;
        if (segment_count_usize > std.math.maxInt(u32)) return error.InvalidMessageSize;
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
            const size_words = std.math.cast(u32, segment.items.len / 8) orelse return error.InvalidMessageSize;
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
            try self.segments.append(self.allocator, std.ArrayList(u8).empty);
        }

        const segment_count_usize = self.segments.items.len;
        if (segment_count_usize > std.math.maxInt(u32)) return error.InvalidMessageSize;
        const segment_count = @as(u32, @intCast(segment_count_usize));
        var word_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, word_buf[0..4], segment_count - 1, .little);
        try writer.writeAll(&word_buf);

        for (self.segments.items) |segment| {
            const size_words = std.math.cast(u32, segment.items.len / 8) orelse return error.InvalidMessageSize;
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

    /// Stream the packed wire format directly to `writer`.
    pub fn writePackedTo(self: *MessageBuilder, writer: anytype) !void {
        const packed_bytes = try self.toPackedBytes();
        defer self.allocator.free(packed_bytes);
        try writer.writeAll(packed_bytes);
    }
};

/// Builder for writing elements of a struct list (inline-composite encoding).
pub const StructListBuilder = struct {
    builder: *MessageBuilder,
    segment_id: u32,
    elements_offset: usize,
    element_count: u32,
    data_words: u16,
    pointer_words: u16,

    /// Return the number of elements in this struct list.
    pub fn len(self: StructListBuilder) u32 {
        return self.element_count;
    }

    /// Return a `StructBuilder` for the element at the given index.
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

/// Typed list helpers for generated code: EnumListReader/Builder,
/// StructListReader/Builder (typed wrappers), DataListReader/Builder,
/// CapabilityListReader/Builder.
pub const typed_list_helpers = typed_list_helpers_module.define(@This());

/// Helpers for viewing and rewriting capability pointer words in-place.
pub const capability_remap = @import("message/capability_remap.zig").define(@This());

test "capability remap pointer word roundtrip" {
    const cap_id: u32 = 12345;
    const word = capability_remap.makeCapabilityPointer(cap_id);
    try std.testing.expectEqual(@as(u64, 3 | (@as(u64, cap_id) << 32)), word);
    try std.testing.expectEqual(cap_id, try capability_remap.decodeCapabilityPointer(word));
}

test "capability remap rejects invalid pointer words" {
    try std.testing.expectError(error.InvalidPointer, capability_remap.decodeCapabilityPointer(0));
    try std.testing.expectError(error.InvalidPointer, capability_remap.decodeCapabilityPointer(1));
    try std.testing.expectError(error.InvalidPointer, capability_remap.decodeCapabilityPointer(2));
    try std.testing.expectError(error.InvalidPointer, capability_remap.decodeCapabilityPointer((@as(u64, 1) << 2) | 3));
}

test {
    _ = bounds;
}

test "packed decoder handles zero and literal runs" {
    const packed_bytes = [_]u8{
        0x00, 0x01, // two zero words
        0xFF, // literal word
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        0x00, // no extra literal words
    };

    try std.testing.expectEqual(@as(usize, 24), try estimateUnpackedSize(&packed_bytes));
    const unpacked = try unpackPacked(std.testing.allocator, &packed_bytes);
    defer std.testing.allocator.free(unpacked);

    try std.testing.expectEqual(@as(usize, 24), unpacked.len);
    try std.testing.expect(std.mem.allEqual(u8, unpacked[0..16], 0));
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 }, unpacked[16..24]);
}

test "packed decoder rejects truncated regular tag payload" {
    const packed_bytes = [_]u8{
        0x03, // expects two literal bytes
        0xAA, // only one provided
    };
    try std.testing.expectError(error.UnexpectedEof, estimateUnpackedSize(&packed_bytes));
    try std.testing.expectError(error.UnexpectedEof, unpackPacked(std.testing.allocator, &packed_bytes));
}

test "cloneAnyPointer handles null pointers" {
    var src_builder = MessageBuilder.init(std.testing.allocator);
    defer src_builder.deinit();
    _ = try src_builder.initRootAnyPointer();

    const src_bytes = try src_builder.toBytes();
    defer std.testing.allocator.free(src_bytes);

    var src_msg = try Message.init(std.testing.allocator, src_bytes, .{});
    defer src_msg.deinit();
    const src_any = try src_msg.getRootAnyPointer();

    var dest_builder = MessageBuilder.init(std.testing.allocator);
    defer dest_builder.deinit();
    const dest_root = try dest_builder.initRootAnyPointer();
    try cloneAnyPointer(src_any, dest_root);

    const dest_bytes = try dest_builder.toBytes();
    defer std.testing.allocator.free(dest_bytes);

    var dest_msg = try Message.init(std.testing.allocator, dest_bytes, .{});
    defer dest_msg.deinit();
    const dest_any = try dest_msg.getRootAnyPointer();
    try std.testing.expect(dest_any.isNull());
}

test "cloneAnyPointer clones capability pointers" {
    var src_builder = MessageBuilder.init(std.testing.allocator);
    defer src_builder.deinit();
    const src_root = try src_builder.initRootAnyPointer();
    try src_root.setCapability(.{ .id = 123 });

    const src_bytes = try src_builder.toBytes();
    defer std.testing.allocator.free(src_bytes);

    var src_msg = try Message.initUnvalidated(std.testing.allocator, src_bytes);
    defer src_msg.deinit();
    const src_any = try src_msg.getRootAnyPointer();

    var dest_builder = MessageBuilder.init(std.testing.allocator);
    defer dest_builder.deinit();
    const dest_root = try dest_builder.initRootAnyPointer();
    try cloneAnyPointer(src_any, dest_root);

    const dest_bytes = try dest_builder.toBytes();
    defer std.testing.allocator.free(dest_bytes);

    var dest_msg = try Message.initUnvalidated(std.testing.allocator, dest_bytes);
    defer dest_msg.deinit();
    const dest_any = try dest_msg.getRootAnyPointer();
    const cap = try dest_any.getCapability();
    try std.testing.expectEqual(@as(u32, 123), cap.id);
}

test "cloneAnyPointer clones inline-composite struct lists" {
    var src_builder = MessageBuilder.init(std.testing.allocator);
    defer src_builder.deinit();
    var root = try src_builder.allocateStruct(0, 1);
    var list = try root.writeStructList(0, 2, 1, 0);
    (try list.get(0)).writeU32(0, 11);
    (try list.get(1)).writeU32(0, 22);

    const src_bytes = try src_builder.toBytes();
    defer std.testing.allocator.free(src_bytes);

    var src_msg = try Message.init(std.testing.allocator, src_bytes, .{});
    defer src_msg.deinit();
    const src_any = try src_msg.getRootAnyPointer();

    var dest_builder = MessageBuilder.init(std.testing.allocator);
    defer dest_builder.deinit();
    const dest_root = try dest_builder.initRootAnyPointer();
    try cloneAnyPointer(src_any, dest_root);

    const dest_bytes = try dest_builder.toBytes();
    defer std.testing.allocator.free(dest_bytes);

    var dest_msg = try Message.init(std.testing.allocator, dest_bytes, .{});
    defer dest_msg.deinit();
    const dest_root_struct = try dest_msg.getRootStruct();
    const dest_list = try dest_root_struct.readStructList(0);
    try std.testing.expectEqual(@as(u32, 2), dest_list.len());
    try std.testing.expectEqual(@as(u32, 11), (try dest_list.get(0)).readU32(0));
    try std.testing.expectEqual(@as(u32, 22), (try dest_list.get(1)).readU32(0));
}

test "cloneAnyPointer clones pointer lists" {
    var src_builder = MessageBuilder.init(std.testing.allocator);
    defer src_builder.deinit();
    const src_root = try src_builder.initRootAnyPointer();
    var src_list = try src_root.initPointerList(2);
    try src_list.setText(0, "north");
    try src_list.setNull(1);

    const src_bytes = try src_builder.toBytes();
    defer std.testing.allocator.free(src_bytes);

    var src_msg = try Message.init(std.testing.allocator, src_bytes, .{});
    defer src_msg.deinit();
    const src_any = try src_msg.getRootAnyPointer();

    var dest_builder = MessageBuilder.init(std.testing.allocator);
    defer dest_builder.deinit();
    const dest_root = try dest_builder.initRootAnyPointer();
    try cloneAnyPointer(src_any, dest_root);

    const dest_bytes = try dest_builder.toBytes();
    defer std.testing.allocator.free(dest_bytes);

    var dest_msg = try Message.init(std.testing.allocator, dest_bytes, .{});
    defer dest_msg.deinit();
    const dest_any = try dest_msg.getRootAnyPointer();
    const dest_list = try dest_any.getPointerList();
    try std.testing.expectEqual(@as(u32, 2), dest_list.len());
    try std.testing.expectEqualStrings("north", try dest_list.getText(0));
    try std.testing.expectEqualStrings("", try dest_list.getText(1));
    try std.testing.expectError(error.InvalidPointer, dest_list.getData(1));
}

test "cloneAnyPointer enforces recursion limit" {
    var src_builder = MessageBuilder.init(std.testing.allocator);
    defer src_builder.deinit();

    var current = try src_builder.initRootAnyPointer();
    var depth: usize = 0;
    while (depth < 70) : (depth += 1) {
        var node = try current.initStruct(0, 1);
        current = try node.getAnyPointer(0);
    }
    try current.setNull();

    const src_bytes = try src_builder.toBytes();
    defer std.testing.allocator.free(src_bytes);

    var src_msg = try Message.initUnvalidated(std.testing.allocator, src_bytes);
    defer src_msg.deinit();
    const src_any = try src_msg.getRootAnyPointer();

    var dest_builder = MessageBuilder.init(std.testing.allocator);
    defer dest_builder.deinit();
    const dest_root = try dest_builder.initRootAnyPointer();
    try std.testing.expectError(error.RecursionLimitExceeded, cloneAnyPointer(src_any, dest_root));
}

test "cloneAnyPointer rejects invalid far-pointer cycles" {
    const frame = [_]u8{
        0x00, 0x00, 0x00, 0x00, // one segment
        0x01, 0x00, 0x00, 0x00, // one word
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // far pointer to itself
    };

    var src_msg = try Message.initUnvalidated(std.testing.allocator, &frame);
    defer src_msg.deinit();
    const src_any = try src_msg.getRootAnyPointer();

    var dest_builder = MessageBuilder.init(std.testing.allocator);
    defer dest_builder.deinit();
    const dest_root = try dest_builder.initRootAnyPointer();
    try std.testing.expectError(error.PointerDepthLimit, cloneAnyPointer(src_any, dest_root));
}

test "StructReader.readBoolStrict enforces bounds" {
    var builder = MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(1, 0);
    root.writeBool(0, 0, true);

    const bytes = try builder.toBytes();
    defer std.testing.allocator.free(bytes);

    var msg = try Message.init(std.testing.allocator, bytes, .{});
    defer msg.deinit();
    const reader = try msg.getRootStruct();
    try std.testing.expect(try reader.readBoolStrict(0, 0));
    try std.testing.expectError(error.OutOfBounds, reader.readBoolStrict(8, 0));
}
