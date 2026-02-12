const std = @import("std");
const bounds = @import("bounds.zig");

pub fn setNull(builder: anytype, segment_id: u32, pointer_pos: usize) !void {
    if (segment_id >= builder.segments.items.len) return error.InvalidSegmentId;
    const segment = &builder.segments.items[segment_id];
    try bounds.checkBoundsMut(segment.items, pointer_pos, 8);
    std.mem.writeInt(u64, segment.items[pointer_pos..][0..8], 0, .little);
}

pub fn setText(builder: anytype, segment_id: u32, pointer_pos: usize, text: []const u8) !void {
    try builder.writeTextPointer(segment_id, pointer_pos, text, segment_id);
}

pub fn setData(builder: anytype, segment_id: u32, pointer_pos: usize, data: []const u8) !void {
    if (data.len > std.math.maxInt(u32)) return error.ElementCountTooLarge;
    const offset = try builder.writeListPointer(
        segment_id,
        pointer_pos,
        2,
        @as(u32, @intCast(data.len)),
        segment_id,
    );
    const segment = &builder.segments.items[segment_id];
    const slice = segment.items[offset .. offset + data.len];
    std.mem.copyForwards(u8, slice, data);
}

pub fn setCapability(
    builder: anytype,
    segment_id: u32,
    pointer_pos: usize,
    cap: anytype,
    make_capability_pointer: *const fn (u32) anyerror!u64,
) !void {
    if (segment_id >= builder.segments.items.len) return error.InvalidSegmentId;
    const segment = &builder.segments.items[segment_id];
    try bounds.checkBoundsMut(segment.items, pointer_pos, 8);
    const pointer_word = try make_capability_pointer(cap.id);
    std.mem.writeInt(u64, segment.items[pointer_pos..][0..8], pointer_word, .little);
}

pub fn initList(builder: anytype, segment_id: u32, pointer_pos: usize, element_size: u3, element_count: u32) !usize {
    return builder.writeListPointer(segment_id, pointer_pos, element_size, element_count, segment_id);
}
