//! Valid wire frames with canonical double-far struct landing pads. Keep this
//! independent of MessageBuilder so copy tests do not reuse its encoding path.
const std = @import("std");

pub const Options = struct {
    prefix_words: u32 = 0,
    values: [3]u64 = .{ 77, 88, 99 },
    empty: bool = false,
};

pub const Frame = struct { bytes: [128]u8, len: usize };

pub fn make(options: Options) Frame {
    std.debug.assert(options.prefix_words <= 3);
    const prefix = options.prefix_words;
    var result: Frame = .{ .bytes = @splat(0), .len = 56 + (@as(usize, prefix) + 6) * 8 };
    const bytes = &result.bytes;
    std.mem.writeInt(u32, bytes[0..4], 2, .little);
    std.mem.writeInt(u32, bytes[4..8], 1, .little);
    std.mem.writeInt(u32, bytes[8..12], 4, .little);
    std.mem.writeInt(u32, bytes[12..16], prefix + 6, .little);
    word(bytes, 16, 0x0000000100000006); // Root -> double-far pad in segment 1.
    word(bytes, 24, (@as(u64, 2) << 32) | (@as(u64, prefix) << 3) | 2);
    word(bytes, 32, if (options.empty) 0 else 0x0002000200000000);
    word(bytes, 40, (@as(u64, 2) << 32) | (@as(u64, prefix + 4) << 3) | 2);
    word(bytes, 48, 0x0000000100000000); // Nested double-far struct: one data word.
    for (0..prefix) |index| word(bytes, 56 + index * 8, 0xfeedface);
    const content = 56 + @as(usize, prefix) * 8;
    word(bytes, content, options.values[0]);
    word(bytes, content + 8, options.values[1]);
    word(bytes, content + 16, 0x0000003200000009); // Six-byte Text two words ahead.
    word(bytes, content + 24, 0x0000000100000016); // Nested struct via pad at word 2.
    word(bytes, content + 32, options.values[2]);
    @memcpy(bytes[content + 40 ..][0..6], "hello\x00");
    return result;
}

fn word(bytes: []u8, offset: usize, value: u64) void {
    std.mem.writeInt(u64, bytes[offset..][0..8], value, .little);
}
