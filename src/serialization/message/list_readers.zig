const std = @import("std");

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
                if (offset + stride > segment.len) return error.OutOfBounds;
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
                if (offset >= segment.len) return error.OutOfBounds;
                return segment[offset];
            }

            pub fn slice(self: U8ListReader) ![]const u8 {
                const segment = self.message.segments[self.segment_id];
                const end = self.elements_offset + @as(usize, self.element_count);
                if (end > segment.len) return error.OutOfBounds;
                return segment[self.elements_offset..end];
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
                if (offset >= segment.len) return error.OutOfBounds;
                return @bitCast(segment[offset]);
            }
        };

        pub const U16ListReader = struct {
            message: *const MessageType,
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
            message: *const MessageType,
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
            message: *const MessageType,
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
            message: *const MessageType,
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
            message: *const MessageType,
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
            message: *const MessageType,
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
            message: *const MessageType,
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
            message: *const MessageType,
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
                if (pointer_pos + 8 > segment.len) return error.OutOfBounds;
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

                const segment = self.message.segments[list.segment_id];
                if (list.content_offset + list.element_count > segment.len) return error.OutOfBounds;

                const text_data = segment[list.content_offset .. list.content_offset + list.element_count];
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
                const segment = self.message.segments[list.segment_id];
                if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;
                return segment[list.content_offset .. list.content_offset + total_bytes];
            }

            pub fn getU8List(self: PointerListReader, index: u32) !U8ListReader {
                const list = try self.readList(index);
                if (list.element_size != 2) return error.InvalidPointer;
                const total_bytes = try list_content_bytes(list.element_size, list.element_count);
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
                const total_bytes = try list_content_bytes(list.element_size, list.element_count);
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
                const total_bytes = try list_content_bytes(list.element_size, list.element_count);
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
                const total_bytes = try list_content_bytes(list.element_size, list.element_count);
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
                const total_bytes = try list_content_bytes(list.element_size, list.element_count);
                const segment = self.message.segments[list.segment_id];
                if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;
                return .{
                    .message = self.message,
                    .segment_id = list.segment_id,
                    .elements_offset = list.content_offset,
                    .element_count = list.element_count,
                };
            }

            pub fn getStructList(self: PointerListReader, index: u32) !StructListReader {
                const list = try self.readList(index);
                if (list.element_size != 7) return error.InvalidPointer;
                const total_words = try list_content_words(list.element_size, list.element_count);
                const segment = self.message.segments[list.segment_id];
                if (list.content_offset + total_words * 8 > segment.len) return error.OutOfBounds;
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
                const list = try self.readList(index);
                if (list.element_size != 6) return error.InvalidPointer;
                const total_bytes = try list_content_bytes(list.element_size, list.element_count);
                const segment = self.message.segments[list.segment_id];
                if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;
                return .{
                    .message = self.message,
                    .segment_id = list.segment_id,
                    .elements_offset = list.content_offset,
                    .element_count = list.element_count,
                };
            }

            pub fn getInlineCompositeList(self: PointerListReader, index: u32) !InlineCompositeListType {
                const ptr = try self.readPointer(index);
                if (ptr.word == 0) return error.InvalidPointer;
                return self.message.resolveInlineCompositeList(self.segment_id, ptr.pos, ptr.word);
            }
        };
    };
}
