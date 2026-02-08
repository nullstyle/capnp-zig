const std = @import("std");

pub fn define(
    comptime MessageBuilderType: type,
    comptime AnyPointerReaderType: type,
    comptime AnyPointerBuilderType: type,
    comptime StructReaderType: type,
    comptime StructBuilderType: type,
    comptime StructListReaderType: type,
    comptime list_content_bytes: *const fn (u3, u32) anyerror!usize,
    comptime decode_capability_pointer: *const fn (u64) anyerror!u32,
) type {
    return struct {
        const max_depth: u32 = 64;

        pub fn cloneAnyPointerToBytes(allocator: std.mem.Allocator, src: AnyPointerReaderType) ![]const u8 {
            var builder = MessageBuilderType.init(allocator);
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

        pub fn cloneAnyPointer(src: AnyPointerReaderType, dest: AnyPointerBuilderType) anyerror!void {
            return cloneAnyPointerDepth(src, dest, max_depth);
        }

        fn cloneAnyPointerDepth(src: AnyPointerReaderType, dest: AnyPointerBuilderType, depth: u32) anyerror!void {
            if (depth == 0) return error.RecursionLimitExceeded;
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
                    try cloneStructDepth(src_struct, dest_struct, depth - 1);
                },
                1 => try cloneListDepth(src, dest, resolved.pointer_word, depth - 1),
                3 => {
                    const cap_id = try decode_capability_pointer(resolved.pointer_word);
                    try dest.setCapability(.{ .id = cap_id });
                },
                else => return error.InvalidPointer,
            }
        }

        fn cloneStructDepth(src: StructReaderType, dest: StructBuilderType, depth: u32) anyerror!void {
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
                    const src_ptr = AnyPointerReaderType{
                        .message = src.message,
                        .segment_id = src.segment_id,
                        .pointer_pos = pointer_pos,
                        .pointer_word = pointer_word,
                    };
                    try cloneAnyPointerDepth(src_ptr, dest_ptr, depth);
                }
            }
        }

        fn cloneListDepth(src: AnyPointerReaderType, dest: AnyPointerBuilderType, resolved_pointer_word: u64, depth: u32) anyerror!void {
            const element_size = @as(u3, @truncate((resolved_pointer_word >> 32) & 0x7));

            if (element_size == 7) {
                const list = try src.message.resolveInlineCompositeList(src.segment_id, src.pointer_pos, src.pointer_word);
                var fake = StructBuilderType{
                    .builder = dest.builder,
                    .segment_id = dest.segment_id,
                    .offset = dest.pointer_pos,
                    .data_size = 0,
                    .pointer_count = 1,
                };
                var dest_list = try fake.writeStructListInSegments(0, list.element_count, list.data_words, list.pointer_words, dest.segment_id, dest.segment_id);

                const src_list = StructListReaderType{
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
                    try cloneStructDepth(src_elem, dest_elem, depth);
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
                    const src_ptr = AnyPointerReaderType{
                        .message = src.message,
                        .segment_id = list.segment_id,
                        .pointer_pos = src_ptr_pos,
                        .pointer_word = pointer_word,
                    };

                    const dest_ptr_pos = dest_offset + @as(usize, idx) * 8;
                    const dest_ptr = AnyPointerBuilderType{
                        .builder = dest.builder,
                        .segment_id = dest.segment_id,
                        .pointer_pos = dest_ptr_pos,
                    };

                    if (pointer_word == 0) {
                        try dest_ptr.setNull();
                    } else {
                        try cloneAnyPointerDepth(src_ptr, dest_ptr, depth);
                    }
                }
                return;
            }

            const total_bytes = try list_content_bytes(element_size, element_count);
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
    };
}
