const std = @import("std");
const bounds = @import("bounds.zig");

/// Returns the unsigned integer type used for reading/writing a given type
/// on the wire. For float types, this is the same-sized unsigned integer;
/// for all other types, it is the type itself.
fn WireType(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .float => std.meta.Int(.unsigned, @bitSizeOf(T)),
        else => T,
    };
}

/// Generic list reader for primitive types >= 2 bytes (u16..u64, i16..i64, f32, f64).
/// The struct layout (message, segment_id, elements_offset, element_count) is
/// identical for every instantiation, so callers can coerce between readers of
/// the same width via simple struct-literal copies.
pub fn PrimitiveListReader(comptime T: type, comptime MessageType: type) type {
    const byte_size = @sizeOf(T);

    return struct {
        message: *const MessageType,
        segment_id: u32,
        elements_offset: usize,
        element_count: u32,

        pub fn len(self: @This()) u32 {
            return self.element_count;
        }

        pub fn get(self: @This(), index: u32) !T {
            if (index >= self.element_count) return error.IndexOutOfBounds;
            const offset = self.elements_offset + @as(usize, index) * byte_size;
            const segment = self.message.segments[self.segment_id];
            try bounds.checkBounds(segment, offset, byte_size);
            const raw = std.mem.readInt(WireType(T), segment[offset..][0..byte_size], .little);
            return @bitCast(raw);
        }
    };
}

/// Generic list builder for primitive types >= 2 bytes (u16..u64, i16..i64, f32, f64).
pub fn PrimitiveListBuilder(comptime T: type, comptime MessageBuilderType: type) type {
    const byte_size = @sizeOf(T);

    return struct {
        builder: *MessageBuilderType,
        segment_id: u32,
        elements_offset: usize,
        element_count: u32,

        pub fn len(self: @This()) u32 {
            return self.element_count;
        }

        pub fn set(self: @This(), index: u32, value: T) !void {
            if (index >= self.element_count) return error.IndexOutOfBounds;
            const offset = self.elements_offset + @as(usize, index) * byte_size;
            const segment = &self.builder.segments.items[self.segment_id];
            try bounds.checkBoundsMut(segment.items, offset, byte_size);
            std.mem.writeInt(WireType(T), segment.items[offset..][0..byte_size], @bitCast(value), .little);
        }
    };
}

pub fn define(
    comptime MessageType: type,
    comptime StructReaderType: type,
    comptime CapabilityType: type,
    comptime InlineCompositeListType: type,
    comptime list_content_bytes: *const fn (u3, u32) anyerror!usize,
    comptime list_content_words: *const fn (u3, u32) anyerror!usize,
    comptime decode_capability_pointer: *const fn (u64) anyerror!u32,
) type {
    return struct {
        pub const StructListReader = struct {
            message: *const MessageType,
            segment_id: u32,
            elements_offset: usize,
            element_count: u32,
            data_words: u16,
            pointer_words: u16,

            pub fn len(self: StructListReader) u32 {
                return self.element_count;
            }

            pub fn get(self: StructListReader, index: u32) !StructReaderType {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const stride = (@as(usize, self.data_words) + @as(usize, self.pointer_words)) * 8;
                const offset = self.elements_offset + @as(usize, index) * stride;
                const segment = self.message.segments[self.segment_id];
                try bounds.checkBounds(segment, offset, stride);
                return StructReaderType{
                    .message = self.message,
                    .segment_id = self.segment_id,
                    .offset = offset,
                    .data_size = self.data_words,
                    .pointer_count = self.pointer_words,
                };
            }
        };

        pub const TextListReader = struct {
            message: *const MessageType,
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
                try bounds.checkBounds(segment, pointer_pos, 8);

                const pointer_word = std.mem.readInt(u64, segment[pointer_pos..][0..8], .little);
                if (pointer_word == 0) return "";

                const list = try self.message.resolveListPointer(self.segment_id, pointer_pos, pointer_word);
                if (list.element_size != 2) return error.InvalidTextPointer;

                try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, list.element_count);

                const list_segment = self.message.segments[list.segment_id];
                const text_data = list_segment[list.content_offset .. list.content_offset + list.element_count];
                if (text_data.len > 0 and text_data[text_data.len - 1] == 0) {
                    return text_data[0 .. text_data.len - 1];
                }
                return text_data;
            }

            /// Like `get`, but returns `error.InvalidUtf8` when the text
            /// contains ill-formed UTF-8 byte sequences.
            pub fn getStrict(self: TextListReader, index: u32) ![]const u8 {
                const text = try self.get(index);
                if (text.len > 0 and !std.unicode.utf8ValidateSlice(text)) {
                    return error.InvalidUtf8;
                }
                return text;
            }
        };

        pub const U8ListReader = struct {
            message: *const MessageType,
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
                try bounds.checkOffset(segment, offset);
                return segment[offset];
            }

            pub fn slice(self: U8ListReader) ![]const u8 {
                const segment = self.message.segments[self.segment_id];
                try bounds.checkBounds(segment, self.elements_offset, @as(usize, self.element_count));
                return segment[self.elements_offset .. self.elements_offset + @as(usize, self.element_count)];
            }
        };

        pub const I8ListReader = struct {
            message: *const MessageType,
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
                try bounds.checkOffset(segment, offset);
                return @bitCast(segment[offset]);
            }
        };

        // Multi-byte primitive list readers — generated via comptime generic.
        pub const U16ListReader = PrimitiveListReader(u16, MessageType);
        pub const I16ListReader = PrimitiveListReader(i16, MessageType);
        pub const U32ListReader = PrimitiveListReader(u32, MessageType);
        pub const I32ListReader = PrimitiveListReader(i32, MessageType);
        pub const F32ListReader = PrimitiveListReader(f32, MessageType);
        pub const U64ListReader = PrimitiveListReader(u64, MessageType);
        pub const I64ListReader = PrimitiveListReader(i64, MessageType);
        pub const F64ListReader = PrimitiveListReader(f64, MessageType);

        pub const BoolListReader = struct {
            message: *const MessageType,
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
                try bounds.checkOffset(segment, offset);
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
            message: *const MessageType,
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
                try bounds.checkBounds(segment, pointer_pos, 8);
                const pointer_word = std.mem.readInt(u64, segment[pointer_pos..][0..8], .little);
                return .{ .pos = pointer_pos, .word = pointer_word };
            }

            fn readList(self: PointerListReader, index: u32) !MessageType.ResolvedListPointer {
                const ptr = try self.readPointer(index);
                if (ptr.word == 0) return error.InvalidPointer;
                return self.message.resolveListPointer(self.segment_id, ptr.pos, ptr.word);
            }

            pub fn getText(self: PointerListReader, index: u32) ![]const u8 {
                const ptr = try self.readPointer(index);
                if (ptr.word == 0) return "";

                const list = try self.message.resolveListPointer(self.segment_id, ptr.pos, ptr.word);
                if (list.element_size != 2) return error.InvalidTextPointer;

                try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, list.element_count);

                const text_data = self.message.segments[list.segment_id][list.content_offset .. list.content_offset + list.element_count];
                if (text_data.len > 0 and text_data[text_data.len - 1] == 0) {
                    return text_data[0 .. text_data.len - 1];
                }
                return text_data;
            }

            /// Like `getText`, but returns `error.InvalidUtf8` when the text
            /// contains ill-formed UTF-8 byte sequences.
            pub fn getTextStrict(self: PointerListReader, index: u32) ![]const u8 {
                const text = try self.getText(index);
                if (text.len > 0 and !std.unicode.utf8ValidateSlice(text)) {
                    return error.InvalidUtf8;
                }
                return text;
            }

            pub fn getStruct(self: PointerListReader, index: u32) !StructReaderType {
                const ptr = try self.readPointer(index);
                if (ptr.word == 0) return error.InvalidPointer;
                return self.message.resolveStructPointer(self.segment_id, ptr.pos, ptr.word);
            }

            pub fn getCapability(self: PointerListReader, index: u32) !CapabilityType {
                const ptr = try self.readPointer(index);
                if (ptr.word == 0) return error.InvalidPointer;
                const resolved = try self.message.resolvePointer(self.segment_id, ptr.pos, ptr.word, 8);
                if (resolved.pointer_word == 0) return error.InvalidPointer;
                return .{ .id = try decode_capability_pointer(resolved.pointer_word) };
            }

            pub fn getData(self: PointerListReader, index: u32) ![]const u8 {
                const list = try self.readList(index);
                if (list.element_size != 2) return error.InvalidPointer;
                const total_bytes = try list_content_bytes(list.element_size, list.element_count);
                try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, total_bytes);
                return self.message.segments[list.segment_id][list.content_offset .. list.content_offset + total_bytes];
            }

            /// Resolve a list pointer at `index`, validate its element size,
            /// bounds-check the content, and return a 4-field reader struct.
            fn readPrimitiveList(
                self: PointerListReader,
                comptime ReaderType: type,
                index: u32,
                expected_element_size: u3,
            ) !ReaderType {
                const list = try self.readList(index);
                if (list.element_size != expected_element_size) return error.InvalidPointer;
                const total_bytes = try list_content_bytes(list.element_size, list.element_count);
                try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, total_bytes);
                return .{
                    .message = self.message,
                    .segment_id = list.segment_id,
                    .elements_offset = list.content_offset,
                    .element_count = list.element_count,
                };
            }

            pub fn getU8List(self: PointerListReader, index: u32) !U8ListReader {
                return self.readPrimitiveList(U8ListReader, index, 2);
            }

            pub fn getI8List(self: PointerListReader, index: u32) !I8ListReader {
                return self.readPrimitiveList(I8ListReader, index, 2);
            }

            pub fn getU16List(self: PointerListReader, index: u32) !U16ListReader {
                return self.readPrimitiveList(U16ListReader, index, 3);
            }

            pub fn getI16List(self: PointerListReader, index: u32) !I16ListReader {
                return self.readPrimitiveList(I16ListReader, index, 3);
            }

            pub fn getU32List(self: PointerListReader, index: u32) !U32ListReader {
                return self.readPrimitiveList(U32ListReader, index, 4);
            }

            pub fn getI32List(self: PointerListReader, index: u32) !I32ListReader {
                return self.readPrimitiveList(I32ListReader, index, 4);
            }

            pub fn getF32List(self: PointerListReader, index: u32) !F32ListReader {
                return self.readPrimitiveList(F32ListReader, index, 4);
            }

            pub fn getU64List(self: PointerListReader, index: u32) !U64ListReader {
                return self.readPrimitiveList(U64ListReader, index, 5);
            }

            pub fn getI64List(self: PointerListReader, index: u32) !I64ListReader {
                return self.readPrimitiveList(I64ListReader, index, 5);
            }

            pub fn getF64List(self: PointerListReader, index: u32) !F64ListReader {
                return self.readPrimitiveList(F64ListReader, index, 5);
            }

            pub fn getBoolList(self: PointerListReader, index: u32) !BoolListReader {
                return self.readPrimitiveList(BoolListReader, index, 1);
            }

            pub fn getStructList(self: PointerListReader, index: u32) !StructListReader {
                const list = try self.readList(index);
                if (list.element_size != 7) return error.InvalidPointer;
                const total_words = try list_content_words(list.element_size, list.element_count);
                try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, total_words * 8);
                return .{
                    .message = self.message,
                    .segment_id = list.segment_id,
                    .elements_offset = list.content_offset,
                    .element_count = list.element_count,
                    .data_words = @intCast(list.inline_data_words),
                    .pointer_words = @intCast(list.inline_pointer_words),
                };
            }

            pub fn getPointerList(self: PointerListReader, index: u32) !PointerListReader {
                return self.readPrimitiveList(PointerListReader, index, 6);
            }

            pub fn getInlineCompositeList(self: PointerListReader, index: u32) !InlineCompositeListType {
                const ptr = try self.readPointer(index);
                if (ptr.word == 0) return error.InvalidPointer;
                return self.message.resolveInlineCompositeList(self.segment_id, ptr.pos, ptr.word);
            }
        };
    };
}
