const std = @import("std");
const bounds = @import("bounds.zig");
const element_list = @import("element_list.zig");

/// Returns the unsigned integer type used for reading/writing a given type
/// on the wire. For float types, this is the same-sized unsigned integer;
/// for all other types, it is the type itself.
fn WireType(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .float => @Int(.unsigned, @bitSizeOf(T)),
        else => T,
    };
}

/// Distance between consecutive elements of a list reader.
///
/// `stride_bytes` is 0 for a list stored in its own encoding, where elements are
/// packed at their natural width. It is non-zero only for a struct list being
/// read back as the primitive or pointer list an older schema declares (the
/// inverse of the Cap'n Proto list-upgrade rule), where consecutive elements are
/// separated by the whole struct — data section plus pointer section — and not
/// by the width actually being read.
fn elementStride(stride_bytes: u32, natural_bytes: usize) usize {
    if (stride_bytes != 0) return @as(usize, stride_bytes);
    return natural_bytes;
}

/// Generic list reader for primitive types >= 2 bytes (u16..u64, i16..i64, f32, f64).
/// The struct layout (message, segment_id, elements_offset, element_count,
/// stride_bytes) is identical for every instantiation, so callers can coerce
/// between readers of the same width via simple struct-literal copies.
pub fn PrimitiveListReader(comptime T: type, comptime MessageType: type) type {
    const byte_size = @sizeOf(T);

    return struct {
        message: *const MessageType,
        segment_id: u32,
        elements_offset: usize,
        element_count: u32,
        /// See `elementStride`. Defaulted so existing four-field literals —
        /// including the ones in checked-in generated code — keep compiling
        /// and keep meaning "natural stride".
        stride_bytes: u32 = 0,

        pub fn len(self: @This()) u32 {
            return self.element_count;
        }

        pub fn get(self: @This(), index: u32) !T {
            if (index >= self.element_count) return error.IndexOutOfBounds;
            const offset = self.elements_offset + @as(usize, index) * elementStride(self.stride_bytes, byte_size);
            const segment = self.message.segments[self.segment_id];
            // Checks `byte_size`, not the stride: for a downgraded struct list
            // the bytes past the element's data section belong to that
            // element's pointers, and are never read here.
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

        /// Return the number of elements in this list.
        pub fn len(self: @This()) u32 {
            return self.element_count;
        }

        /// Set the value at the given index.
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
            /// Element size in BYTES for a struct list upgraded from element
            /// size C = 2/3/4, where it is both the stride and the data-section
            /// length. Zero means "derive from data_words/pointer_words".
            ///
            /// Invariant: non-zero implies `data_words == 0`,
            /// `pointer_words == 0` and a value below 8. Only C = 2/3/4 can
            /// produce it; C = 0/5/6 map exactly onto (0,0)/(1,0)/(0,1) words.
            ///
            /// Defaulted so the six-field literals emitted by codegen keep
            /// compiling and the checked-in generated files need no
            /// regeneration.
            sub_word_data_bytes: u8 = 0,

            pub fn len(self: StructListReader) u32 {
                return self.element_count;
            }

            pub fn get(self: StructListReader, index: u32) !StructReaderType {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                if (self.sub_word_data_bytes != 0) {
                    std.debug.assert(self.data_words == 0);
                    std.debug.assert(self.pointer_words == 0);
                    std.debug.assert(self.sub_word_data_bytes < 8);
                }
                const stride: usize = if (self.sub_word_data_bytes != 0)
                    @as(usize, self.sub_word_data_bytes)
                else
                    (@as(usize, self.data_words) + @as(usize, self.pointer_words)) * 8;
                const offset = self.elements_offset + @as(usize, index) * stride;
                const segment = self.message.segments[self.segment_id];
                // For an upgraded list the stride *is* the exact element
                // footprint, so this checks the final element tightly and no
                // read ever reaches into a neighbour.
                try bounds.checkBounds(segment, offset, stride);
                return StructReaderType{
                    .message = self.message,
                    .segment_id = self.segment_id,
                    .offset = offset,
                    .data_size = self.data_words,
                    .pointer_count = self.pointer_words,
                    .sub_word_data_bytes = self.sub_word_data_bytes,
                };
            }
        };

        pub const TextListReader = struct {
            message: *const MessageType,
            segment_id: u32,
            /// Byte offset of the first text pointer. For a downgraded struct
            /// list that is element 0's *pointer section*, not its start.
            elements_offset: usize,
            element_count: u32,
            /// See `elementStride`.
            stride_bytes: u32 = 0,

            pub fn len(self: TextListReader) u32 {
                return self.element_count;
            }

            pub fn get(self: TextListReader, index: u32) ![]const u8 {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const pointer_pos = self.elements_offset + @as(usize, index) * elementStride(self.stride_bytes, 8);
                const segment = self.message.segments[self.segment_id];
                try bounds.checkBounds(segment, pointer_pos, 8);

                const pointer_word = std.mem.readInt(u64, segment[pointer_pos..][0..8], .little);
                if (pointer_word == 0) return "";

                const list = try self.message.resolveListPointer(self.segment_id, pointer_pos, pointer_word);
                if (list.element_size != 2) return error.InvalidTextPointer;

                try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, list.element_count);

                const list_segment = self.message.segments[list.segment_id];
                const text_data = list_segment[list.content_offset .. list.content_offset + list.element_count];
                return bounds.stripNullTerminator(text_data);
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
            /// See `elementStride`.
            stride_bytes: u32 = 0,

            pub fn len(self: U8ListReader) u32 {
                return self.element_count;
            }

            pub fn get(self: U8ListReader, index: u32) !u8 {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const offset = self.elements_offset + @as(usize, index) * elementStride(self.stride_bytes, 1);
                const segment = self.message.segments[self.segment_id];
                try bounds.checkOffset(segment, offset);
                return segment[offset];
            }

            /// The list's bytes as one contiguous slice.
            ///
            /// Fails with `error.InvalidPointer` for a downgraded struct list:
            /// there its elements are separated by the rest of each struct, so
            /// no contiguous slice of the segment holds them and only them.
            /// `get` still works element by element.
            pub fn slice(self: U8ListReader) ![]const u8 {
                if (self.stride_bytes != 0) return error.InvalidPointer;
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
            /// See `elementStride`.
            stride_bytes: u32 = 0,

            pub fn len(self: I8ListReader) u32 {
                return self.element_count;
            }

            pub fn get(self: I8ListReader, index: u32) !i8 {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const offset = self.elements_offset + @as(usize, index) * elementStride(self.stride_bytes, 1);
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
            /// Byte offset of the first pointer. For a downgraded struct list
            /// that is element 0's *pointer section*, not its start.
            elements_offset: usize,
            element_count: u32,
            /// See `elementStride`.
            stride_bytes: u32 = 0,

            pub fn len(self: PointerListReader) u32 {
                return self.element_count;
            }

            /// Return whether the pointer at `index` is null without resolving
            /// its target. A malformed non-null pointer therefore remains
            /// distinguishable from an absent nested list; resolving it through
            /// a typed getter still reports the underlying pointer error.
            pub fn isNull(self: PointerListReader, index: u32) !bool {
                return (try self.readPointer(index)).word == 0;
            }

            fn readPointer(self: PointerListReader, index: u32) !struct { pos: usize, word: u64 } {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const pointer_pos = self.elements_offset + @as(usize, index) * elementStride(self.stride_bytes, 8);
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
                return bounds.stripNullTerminator(text_data);
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

            /// Resolve the list pointer at `index` for reading as a list of
            /// `expected_element_size` elements, accepting both a list pointer
            /// that already carries that element size and — per the inverse of
            /// the list-upgrade rule — a struct list (C = 7) whose elements are
            /// wide enough to satisfy the request. The nested half of what
            /// `StructReader.resolveElementListAt` does for a struct's own
            /// fields, sharing the very same resolver.
            ///
            /// The five-field literal is deliberate: every reader built here
            /// carries a stride, and `stride_bytes` defaults to 0 ("natural
            /// width"), so a four-field literal would compile and read a
            /// downgraded struct list at the wrong stride from element 1 on.
            fn readElementList(
                self: PointerListReader,
                comptime ReaderType: type,
                index: u32,
                expected_element_size: u3,
            ) !ReaderType {
                const ptr = try self.readPointer(index);
                const view = try element_list.resolve(
                    self.message,
                    self.segment_id,
                    ptr.pos,
                    ptr.word,
                    expected_element_size,
                    list_content_bytes,
                );
                return .{
                    .message = self.message,
                    .segment_id = view.segment_id,
                    .elements_offset = view.elements_offset,
                    .element_count = view.element_count,
                    .stride_bytes = view.stride_bytes,
                };
            }

            pub fn getU8List(self: PointerListReader, index: u32) !U8ListReader {
                return self.readElementList(U8ListReader, index, 2);
            }

            pub fn getI8List(self: PointerListReader, index: u32) !I8ListReader {
                return self.readElementList(I8ListReader, index, 2);
            }

            pub fn getU16List(self: PointerListReader, index: u32) !U16ListReader {
                return self.readElementList(U16ListReader, index, 3);
            }

            pub fn getI16List(self: PointerListReader, index: u32) !I16ListReader {
                return self.readElementList(I16ListReader, index, 3);
            }

            pub fn getU32List(self: PointerListReader, index: u32) !U32ListReader {
                return self.readElementList(U32ListReader, index, 4);
            }

            pub fn getI32List(self: PointerListReader, index: u32) !I32ListReader {
                return self.readElementList(I32ListReader, index, 4);
            }

            pub fn getF32List(self: PointerListReader, index: u32) !F32ListReader {
                return self.readElementList(F32ListReader, index, 4);
            }

            pub fn getU64List(self: PointerListReader, index: u32) !U64ListReader {
                return self.readElementList(U64ListReader, index, 5);
            }

            pub fn getI64List(self: PointerListReader, index: u32) !I64ListReader {
                return self.readElementList(I64ListReader, index, 5);
            }

            pub fn getF64List(self: PointerListReader, index: u32) !F64ListReader {
                return self.readElementList(F64ListReader, index, 5);
            }

            /// The one primitive width that stays strict, and the one reader
            /// with no `stride_bytes` field to carry: C++ hard-fails reading a
            /// struct list as a bit list ("upgrading boolean lists to structs is
            /// no longer supported"), so relaxing this would diverge from the
            /// reference rather than converge with it.
            pub fn getBoolList(self: PointerListReader, index: u32) !BoolListReader {
                const list = try self.readList(index);
                if (list.element_size != 1) return error.InvalidPointer;
                const total_bytes = try list_content_bytes(list.element_size, list.element_count);
                try bounds.checkListContentBounds(self.message.segments, list.segment_id, list.content_offset, total_bytes);
                return .{
                    .message = self.message,
                    .segment_id = list.segment_id,
                    .elements_offset = list.content_offset,
                    .element_count = list.element_count,
                };
            }

            /// Read a nested `List(Void)`. Inline-composite lists are accepted
            /// for the inverse list-upgrade rule, matching
            /// `StructReader.readVoidList`.
            pub fn getVoidList(self: PointerListReader, index: u32) !VoidListReader {
                const ptr = try self.readPointer(index);
                return .{ .element_count = try element_list.resolveVoidElementCount(
                    self.message,
                    self.segment_id,
                    ptr.pos,
                    ptr.word,
                ) };
            }

            /// Read a nested `List(Text)`.
            pub fn getTextList(self: PointerListReader, index: u32) !TextListReader {
                return self.readElementList(TextListReader, index, 6);
            }

            pub fn getStructList(self: PointerListReader, index: u32) !StructListReader {
                const ptr = try self.readPointer(index);
                if (ptr.word == 0) return error.InvalidPointer;
                const layout = try self.message.resolveStructListPointer(self.segment_id, ptr.pos, ptr.word);
                return .{
                    .message = self.message,
                    .segment_id = layout.segment_id,
                    .elements_offset = layout.elements_offset,
                    .element_count = layout.element_count,
                    .data_words = @intCast(layout.data_bytes / 8),
                    .pointer_words = layout.pointer_count,
                    .sub_word_data_bytes = if (layout.data_bytes % 8 == 0)
                        0
                    else
                        @intCast(layout.data_bytes),
                };
            }

            pub fn getPointerList(self: PointerListReader, index: u32) !PointerListReader {
                return self.readElementList(PointerListReader, index, 6);
            }

            pub fn getInlineCompositeList(self: PointerListReader, index: u32) !InlineCompositeListType {
                const ptr = try self.readPointer(index);
                if (ptr.word == 0) return error.InvalidPointer;
                return self.message.resolveInlineCompositeList(self.segment_id, ptr.pos, ptr.word);
            }
        };
    };
}
