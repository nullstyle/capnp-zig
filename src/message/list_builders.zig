const std = @import("std");

pub fn define(
    comptime MessageBuilderType: type,
) type {
    return struct {
        pub const TextListBuilder = struct {
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
            builder: *MessageBuilderType,
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
    };
}
