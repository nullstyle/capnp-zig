const std = @import("std");
const cap_table = @import("../level0/cap_table.zig");
const message = @import("../../serialization/message.zig");
const protocol = @import("../level0/protocol.zig");

pub fn clonePayloadWithRemappedCaps(
    comptime PeerType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    builder: *message.MessageBuilder,
    payload_builder: message.StructBuilder,
    source: protocol.Payload,
    inbound_caps: *const cap_table.InboundCapTable,
    map_inbound_cap: *const fn (*PeerType, *const cap_table.InboundCapTable, u32) anyerror!?u32,
) !void {
    const any_builder = try payload_builder.getAnyPointer(protocol.PAYLOAD_CONTENT_PTR);
    try message.cloneAnyPointer(source.content, any_builder);
    try remapPayloadCapabilities(
        PeerType,
        allocator,
        peer,
        builder,
        any_builder,
        inbound_caps,
        map_inbound_cap,
    );
}

fn remapPayloadCapabilities(
    comptime PeerType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    builder: *message.MessageBuilder,
    root: message.AnyPointerBuilder,
    inbound_caps: *const cap_table.InboundCapTable,
    map_inbound_cap: *const fn (*PeerType, *const cap_table.InboundCapTable, u32) anyerror!?u32,
) !void {
    const view = try buildMessageView(allocator, builder);
    defer allocator.free(view.segments);

    if (root.segment_id >= view.msg.segments.len) return error.InvalidSegmentId;
    const segment = view.msg.segments[root.segment_id];
    if (root.pointer_pos + 8 > segment.len) return error.OutOfBounds;
    const root_word = std.mem.readInt(u64, segment[root.pointer_pos..][0..8], .little);
    try remapPayloadCapabilityPointer(
        PeerType,
        peer,
        &view.msg,
        builder,
        inbound_caps,
        root.segment_id,
        root.pointer_pos,
        root_word,
        map_inbound_cap,
        max_traversal_depth,
    );
}

const max_traversal_depth: u32 = 64;

fn remapPayloadCapabilityPointer(
    comptime PeerType: type,
    peer: *PeerType,
    msg: *const message.Message,
    builder: *message.MessageBuilder,
    inbound_caps: *const cap_table.InboundCapTable,
    segment_id: u32,
    pointer_pos: usize,
    pointer_word: u64,
    map_inbound_cap: *const fn (*PeerType, *const cap_table.InboundCapTable, u32) anyerror!?u32,
    depth: u32,
) !void {
    if (depth == 0) return error.RecursionLimitExceeded;
    if (pointer_word == 0) return;
    const resolved = try msg.resolvePointer(segment_id, pointer_pos, pointer_word, 8);
    if (resolved.pointer_word == 0) return;

    const pointer_type: u2 = @truncate(resolved.pointer_word & 0x3);
    switch (pointer_type) {
        0 => {
            const struct_reader = try msg.resolveStructPointer(
                resolved.segment_id,
                resolved.pointer_pos,
                resolved.pointer_word,
            );
            const pointer_base = struct_reader.offset + @as(usize, struct_reader.data_size) * 8;
            var idx: usize = 0;
            while (idx < struct_reader.pointer_count) : (idx += 1) {
                const child_pos = pointer_base + idx * 8;
                const child_word = std.mem.readInt(
                    u64,
                    msg.segments[struct_reader.segment_id][child_pos..][0..8],
                    .little,
                );
                try remapPayloadCapabilityPointer(
                    PeerType,
                    peer,
                    msg,
                    builder,
                    inbound_caps,
                    struct_reader.segment_id,
                    child_pos,
                    child_word,
                    map_inbound_cap,
                    depth - 1,
                );
            }
        },
        1 => {
            const list = try msg.resolveListPointer(
                resolved.segment_id,
                resolved.pointer_pos,
                resolved.pointer_word,
            );
            if (list.element_size == 6) {
                var idx: u32 = 0;
                while (idx < list.element_count) : (idx += 1) {
                    const child_pos = list.content_offset + @as(usize, idx) * 8;
                    const child_word = std.mem.readInt(
                        u64,
                        msg.segments[list.segment_id][child_pos..][0..8],
                        .little,
                    );
                    try remapPayloadCapabilityPointer(
                        PeerType,
                        peer,
                        msg,
                        builder,
                        inbound_caps,
                        list.segment_id,
                        child_pos,
                        child_word,
                        map_inbound_cap,
                        depth - 1,
                    );
                }
            } else if (list.element_size == 7) {
                const inline_list = try msg.resolveInlineCompositeList(
                    resolved.segment_id,
                    resolved.pointer_pos,
                    resolved.pointer_word,
                );
                const stride = (@as(usize, inline_list.data_words) + @as(usize, inline_list.pointer_words)) * 8;
                var elem_idx: u32 = 0;
                while (elem_idx < inline_list.element_count) : (elem_idx += 1) {
                    const element_offset = inline_list.elements_offset + @as(usize, elem_idx) * stride;
                    const pointer_base = element_offset + @as(usize, inline_list.data_words) * 8;
                    var pointer_idx: usize = 0;
                    while (pointer_idx < inline_list.pointer_words) : (pointer_idx += 1) {
                        const child_pos = pointer_base + pointer_idx * 8;
                        const child_word = std.mem.readInt(
                            u64,
                            msg.segments[inline_list.segment_id][child_pos..][0..8],
                            .little,
                        );
                        try remapPayloadCapabilityPointer(
                            PeerType,
                            peer,
                            msg,
                            builder,
                            inbound_caps,
                            inline_list.segment_id,
                            child_pos,
                            child_word,
                            map_inbound_cap,
                            depth - 1,
                        );
                    }
                }
            }
        },
        3 => {
            const cap_index = try decodeCapabilityPointerWord(resolved.pointer_word);
            if (try map_inbound_cap(peer, inbound_caps, cap_index)) |cap_id| {
                const cap_word = try capabilityPointerWord(cap_id);
                try writePointerWord(builder, resolved.segment_id, resolved.pointer_pos, cap_word);
            } else {
                try writePointerWord(builder, resolved.segment_id, resolved.pointer_pos, 0);
            }
        },
        else => return error.InvalidPointer,
    }
}

fn capabilityPointerWord(cap_id: u32) !u64 {
    return 3 | (@as(u64, cap_id) << 32);
}

fn decodeCapabilityPointerWord(pointer_word: u64) !u32 {
    if ((pointer_word & 0x3) != 3) return error.InvalidPointer;
    if (((pointer_word >> 2) & 0x3FFFFFFF) != 0) return error.InvalidPointer;
    return @as(u32, @truncate(pointer_word >> 32));
}

fn writePointerWord(builder: *message.MessageBuilder, segment_id: u32, pointer_pos: usize, word: u64) !void {
    if (segment_id >= builder.segments.items.len) return error.InvalidSegmentId;
    var segment = &builder.segments.items[segment_id];
    if (pointer_pos + 8 > segment.items.len) return error.OutOfBounds;
    std.mem.writeInt(u64, segment.items[pointer_pos..][0..8], word, .little);
}

fn buildMessageView(
    allocator: std.mem.Allocator,
    builder: *message.MessageBuilder,
) !struct { msg: message.Message, segments: []const []const u8 } {
    const segment_count = builder.segments.items.len;
    const segments = try allocator.alloc([]const u8, segment_count);
    errdefer allocator.free(segments);

    for (builder.segments.items, 0..) |segment, idx| {
        segments[idx] = segment.items;
    }

    const msg = message.Message{
        .allocator = allocator,
        .segments = segments,
        .segments_owned = false,
        .backing_data = null,
    };
    return .{ .msg = msg, .segments = segments };
}

test "payload_remap capability pointer roundtrip" {
    const cap_id: u32 = 12345;
    const word = try capabilityPointerWord(cap_id);
    try std.testing.expectEqual(@as(u64, 3 | (@as(u64, cap_id) << 32)), word);
    try std.testing.expectEqual(cap_id, try decodeCapabilityPointerWord(word));
}

test "payload_remap decode capability pointer rejects invalid tags" {
    try std.testing.expectError(error.InvalidPointer, decodeCapabilityPointerWord(0));
    try std.testing.expectError(error.InvalidPointer, decodeCapabilityPointerWord(1));
    try std.testing.expectError(error.InvalidPointer, decodeCapabilityPointerWord(2));
}

test "payload_remap decode capability pointer rejects high bits" {
    const invalid_word = (@as(u64, 1) << 2) | 3;
    try std.testing.expectError(error.InvalidPointer, decodeCapabilityPointerWord(invalid_word));
}

test "payload_remap capability pointer supports full u32 range" {
    const word = try capabilityPointerWord(std.math.maxInt(u32));
    try std.testing.expectEqual(std.math.maxInt(u32), try decodeCapabilityPointerWord(word));
}
