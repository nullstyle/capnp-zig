const std = @import("std");
const bounds = @import("bounds.zig");
const list_readers = @import("list_readers.zig");

// Re-use the shared comptime generic from list_readers.
const PrimitiveListBuilder = list_readers.PrimitiveListBuilder;

pub fn define(
    comptime MessageBuilderType: type,
) type {
    return struct {
        /// Builder for a list of text (string) values within a Cap'n Proto message.
        pub const TextListBuilder = struct {
            builder: *MessageBuilderType,
            segment_id: u32,
            elements_offset: usize,
            element_count: u32,

            /// Return the number of elements in this text list.
            pub fn len(self: TextListBuilder) u32 {
                return self.element_count;
            }

            /// Set the text value at the given index.
            pub fn set(self: TextListBuilder, index: u32, value: []const u8) !void {
                return self.setInSegment(index, value, self.segment_id);
            }

            /// Set the text value at the given index, allocating in the target segment.
            pub fn setInSegment(self: TextListBuilder, index: u32, value: []const u8, target_segment_id: u32) !void {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const pointer_pos = self.elements_offset + @as(usize, index) * 8;
                try self.builder.writeTextPointer(self.segment_id, pointer_pos, value, target_segment_id);
            }
        };

        /// Builder for a list of void values. Carries only a count; no data is stored.
        pub const VoidListBuilder = struct {
            element_count: u32,

            /// Return the number of elements in this void list.
            pub fn len(self: VoidListBuilder) u32 {
                return self.element_count;
            }
        };

        /// Builder for a list of `u8` values within a Cap'n Proto message.
        pub const U8ListBuilder = struct {
            builder: *MessageBuilderType,
            segment_id: u32,
            elements_offset: usize,
            element_count: u32,

            /// Return the number of elements in this list.
            pub fn len(self: U8ListBuilder) u32 {
                return self.element_count;
            }

            /// Set the value at the given index.
            pub fn set(self: U8ListBuilder, index: u32, value: u8) !void {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const offset = self.elements_offset + @as(usize, index);
                const segment = &self.builder.segments.items[self.segment_id];
                try bounds.checkOffsetMut(segment.items, offset);
                segment.items[offset] = value;
            }

            /// Copy all values from `data` into the list. The slice length must
            /// match the element count exactly.
            pub fn setAll(self: U8ListBuilder, data: []const u8) !void {
                if (data.len != self.element_count) return error.InvalidLength;
                const segment = &self.builder.segments.items[self.segment_id];
                try bounds.checkBoundsMut(segment.items, self.elements_offset, data.len);
                const slice = segment.items[self.elements_offset .. self.elements_offset + data.len];
                std.mem.copyForwards(u8, slice, data);
            }
        };

        /// Builder for a list of `i8` values within a Cap'n Proto message.
        pub const I8ListBuilder = struct {
            builder: *MessageBuilderType,
            segment_id: u32,
            elements_offset: usize,
            element_count: u32,

            /// Return the number of elements in this list.
            pub fn len(self: I8ListBuilder) u32 {
                return self.element_count;
            }

            /// Set the value at the given index.
            pub fn set(self: I8ListBuilder, index: u32, value: i8) !void {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const offset = self.elements_offset + @as(usize, index);
                const segment = &self.builder.segments.items[self.segment_id];
                try bounds.checkOffsetMut(segment.items, offset);
                segment.items[offset] = @bitCast(value);
            }
        };

        // Multi-byte primitive list builders — generated via comptime generic.
        pub const U16ListBuilder = PrimitiveListBuilder(u16, MessageBuilderType);
        pub const I16ListBuilder = PrimitiveListBuilder(i16, MessageBuilderType);
        pub const U32ListBuilder = PrimitiveListBuilder(u32, MessageBuilderType);
        pub const I32ListBuilder = PrimitiveListBuilder(i32, MessageBuilderType);
        pub const F32ListBuilder = PrimitiveListBuilder(f32, MessageBuilderType);
        pub const U64ListBuilder = PrimitiveListBuilder(u64, MessageBuilderType);
        pub const I64ListBuilder = PrimitiveListBuilder(i64, MessageBuilderType);
        pub const F64ListBuilder = PrimitiveListBuilder(f64, MessageBuilderType);

        /// Builder for a list of boolean values within a Cap'n Proto message.
        /// Each element is stored as a single bit.
        pub const BoolListBuilder = struct {
            builder: *MessageBuilderType,
            segment_id: u32,
            elements_offset: usize,
            element_count: u32,

            /// Return the number of elements in this boolean list.
            pub fn len(self: BoolListBuilder) u32 {
                return self.element_count;
            }

            /// Set the boolean value at the given index.
            pub fn set(self: BoolListBuilder, index: u32, value: bool) !void {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const byte_index = @as(usize, index / 8);
                const bit_index: u3 = @intCast(index % 8);
                const offset = self.elements_offset + byte_index;
                const segment = &self.builder.segments.items[self.segment_id];
                try bounds.checkOffsetMut(segment.items, offset);
                const mask = @as(u8, 1) << bit_index;
                if (value) {
                    segment.items[offset] |= mask;
                } else {
                    segment.items[offset] &= ~mask;
                }
            }
        };
    };
}
