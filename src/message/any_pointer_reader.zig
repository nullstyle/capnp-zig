const std = @import("std");

pub fn define(
    comptime MessageType: type,
    comptime StructReaderType: type,
    comptime PointerListReaderType: type,
    comptime InlineCompositeListType: type,
    comptime CapabilityType: type,
    comptime list_content_bytes: *const fn (u3, u32) anyerror!usize,
    comptime decode_capability_pointer: *const fn (u64) anyerror!u32,
) type {
    return struct {
        pub const AnyPointerReader = struct {
            message: *const MessageType,
            segment_id: u32,
            pointer_pos: usize,
            pointer_word: u64,

            pub fn isNull(self: AnyPointerReader) bool {
                return self.pointer_word == 0;
            }

            pub fn getStruct(self: AnyPointerReader) !StructReaderType {
                if (self.pointer_word == 0) return error.InvalidPointer;
                return self.message.resolveStructPointer(self.segment_id, self.pointer_pos, self.pointer_word);
            }

            pub fn getList(self: AnyPointerReader) !MessageType.ResolvedListPointer {
                if (self.pointer_word == 0) return error.InvalidPointer;
                return self.message.resolveListPointer(self.segment_id, self.pointer_pos, self.pointer_word);
            }

            pub fn getInlineCompositeList(self: AnyPointerReader) !InlineCompositeListType {
                if (self.pointer_word == 0) return error.InvalidPointer;
                return self.message.resolveInlineCompositeList(self.segment_id, self.pointer_pos, self.pointer_word);
            }

            pub fn getText(self: AnyPointerReader) ![]const u8 {
                if (self.pointer_word == 0) return "";
                const list = try self.message.resolveListPointer(self.segment_id, self.pointer_pos, self.pointer_word);
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
            pub fn getTextStrict(self: AnyPointerReader) ![]const u8 {
                const text = try self.getText();
                if (text.len > 0 and !std.unicode.utf8ValidateSlice(text)) {
                    return error.InvalidUtf8;
                }
                return text;
            }

            pub fn getData(self: AnyPointerReader) ![]const u8 {
                if (self.pointer_word == 0) return "";
                const list = try self.message.resolveListPointer(self.segment_id, self.pointer_pos, self.pointer_word);
                if (list.element_size != 2) return error.InvalidPointer;
                const total_bytes = try list_content_bytes(list.element_size, list.element_count);
                const segment = self.message.segments[list.segment_id];
                if (list.content_offset + total_bytes > segment.len) return error.OutOfBounds;
                return segment[list.content_offset .. list.content_offset + total_bytes];
            }

            pub fn getPointerList(self: AnyPointerReader) !PointerListReaderType {
                const list = try self.getList();
                if (list.element_size != 6) return error.InvalidPointer;
                return .{
                    .message = self.message,
                    .segment_id = list.segment_id,
                    .elements_offset = list.content_offset,
                    .element_count = list.element_count,
                };
            }

            pub fn getCapability(self: AnyPointerReader) !CapabilityType {
                if (self.pointer_word == 0) return error.InvalidPointer;
                const resolved = try self.message.resolvePointer(self.segment_id, self.pointer_pos, self.pointer_word, 8);
                if (resolved.pointer_word == 0) return error.InvalidPointer;
                return .{ .id = try decode_capability_pointer(resolved.pointer_word) };
            }
        };
    };
}
