const std = @import("std");

pub fn define(
    comptime MessageBuilderType: type,
    comptime AnyPointerBuilderType: type,
    comptime StructListBuilderType: type,
    comptime TextListBuilderType: type,
    comptime VoidListBuilderType: type,
    comptime U8ListBuilderType: type,
    comptime I8ListBuilderType: type,
    comptime U16ListBuilderType: type,
    comptime I16ListBuilderType: type,
    comptime U32ListBuilderType: type,
    comptime I32ListBuilderType: type,
    comptime F32ListBuilderType: type,
    comptime U64ListBuilderType: type,
    comptime I64ListBuilderType: type,
    comptime F64ListBuilderType: type,
    comptime BoolListBuilderType: type,
    comptime CapabilityType: type,
    comptime make_capability_pointer: *const fn (u32) anyerror!u64,
    comptime make_list_pointer: *const fn (i32, u3, u32) u64,
    comptime make_far_pointer: *const fn (bool, u32, u32) u64,
) type {
    return struct {
        pub const PointerListBuilder = struct {
            builder: *MessageBuilderType,
            segment_id: u32,
            elements_offset: usize,
            element_count: u32,

            pub fn len(self: PointerListBuilder) u32 {
                return self.element_count;
            }

            pub fn setNull(self: PointerListBuilder, index: u32) !void {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const pointer_pos = self.elements_offset + @as(usize, index) * 8;
                const segment = &self.builder.segments.items[self.segment_id];
                if (pointer_pos + 8 > segment.items.len) return error.OutOfBounds;
                std.mem.writeInt(u64, segment.items[pointer_pos..][0..8], 0, .little);
            }

            pub fn setCapability(self: PointerListBuilder, index: u32, cap: CapabilityType) !void {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const pointer_pos = self.elements_offset + @as(usize, index) * 8;
                const segment = &self.builder.segments.items[self.segment_id];
                if (pointer_pos + 8 > segment.items.len) return error.OutOfBounds;
                const pointer_word = try make_capability_pointer(cap.id);
                std.mem.writeInt(u64, segment.items[pointer_pos..][0..8], pointer_word, .little);
            }

            pub fn setText(self: PointerListBuilder, index: u32, value: []const u8) !void {
                return self.setTextInSegment(index, value, self.segment_id);
            }

            pub fn setTextInSegment(self: PointerListBuilder, index: u32, value: []const u8, target_segment_id: u32) !void {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const pointer_pos = self.elements_offset + @as(usize, index) * 8;
                try self.builder.writeTextPointer(self.segment_id, pointer_pos, value, target_segment_id);
            }

            pub fn setData(self: PointerListBuilder, index: u32, value: []const u8) !void {
                return self.setDataInSegment(index, value, self.segment_id);
            }

            pub fn setDataInSegment(self: PointerListBuilder, index: u32, value: []const u8, target_segment_id: u32) !void {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                if (value.len > std.math.maxInt(u32)) return error.ElementCountTooLarge;

                while (self.builder.segments.items.len <= target_segment_id) {
                    _ = try self.builder.createSegment();
                }

                const pointer_pos = self.elements_offset + @as(usize, index) * 8;
                const offset = try self.builder.writeListPointer(
                    self.segment_id,
                    pointer_pos,
                    2,
                    @as(u32, @intCast(value.len)),
                    target_segment_id,
                );

                const segment = &self.builder.segments.items[target_segment_id];
                const slice = segment.items[offset .. offset + value.len];
                std.mem.copyForwards(u8, slice, value);
            }

            pub fn initStruct(self: PointerListBuilder, index: u32, data_words: u16, pointer_words: u16) !StructBuilder {
                return self.initStructInSegment(index, data_words, pointer_words, self.segment_id);
            }

            pub fn initStructInSegment(
                self: PointerListBuilder,
                index: u32,
                data_words: u16,
                pointer_words: u16,
                target_segment_id: u32,
            ) !StructBuilder {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                const pointer_pos = self.elements_offset + @as(usize, index) * 8;
                return self.builder.writeStructPointer(self.segment_id, pointer_pos, data_words, pointer_words, target_segment_id);
            }

            fn initListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_size: u3,
                element_count: u32,
                target_segment_id: u32,
            ) !struct { segment_id: u32, offset: usize } {
                if (index >= self.element_count) return error.IndexOutOfBounds;
                while (self.builder.segments.items.len <= target_segment_id) {
                    _ = try self.builder.createSegment();
                }

                const pointer_pos = self.elements_offset + @as(usize, index) * 8;
                const offset = try self.builder.writeListPointer(self.segment_id, pointer_pos, element_size, element_count, target_segment_id);
                return .{ .segment_id = target_segment_id, .offset = offset };
            }

            pub fn initU8List(self: PointerListBuilder, index: u32, element_count: u32) !U8ListBuilderType {
                return self.initU8ListInSegment(index, element_count, self.segment_id);
            }

            pub fn initU8ListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !U8ListBuilderType {
                const info = try self.initListInSegment(index, 2, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn initI8List(self: PointerListBuilder, index: u32, element_count: u32) !I8ListBuilderType {
                return self.initI8ListInSegment(index, element_count, self.segment_id);
            }

            pub fn initI8ListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !I8ListBuilderType {
                const info = try self.initListInSegment(index, 2, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn initU16List(self: PointerListBuilder, index: u32, element_count: u32) !U16ListBuilderType {
                return self.initU16ListInSegment(index, element_count, self.segment_id);
            }

            pub fn initU16ListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !U16ListBuilderType {
                const info = try self.initListInSegment(index, 3, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn initI16List(self: PointerListBuilder, index: u32, element_count: u32) !I16ListBuilderType {
                return self.initI16ListInSegment(index, element_count, self.segment_id);
            }

            pub fn initI16ListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !I16ListBuilderType {
                const info = try self.initListInSegment(index, 3, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn initU32List(self: PointerListBuilder, index: u32, element_count: u32) !U32ListBuilderType {
                return self.initU32ListInSegment(index, element_count, self.segment_id);
            }

            pub fn initU32ListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !U32ListBuilderType {
                const info = try self.initListInSegment(index, 4, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn initI32List(self: PointerListBuilder, index: u32, element_count: u32) !I32ListBuilderType {
                return self.initI32ListInSegment(index, element_count, self.segment_id);
            }

            pub fn initI32ListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !I32ListBuilderType {
                const info = try self.initListInSegment(index, 4, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn initF32List(self: PointerListBuilder, index: u32, element_count: u32) !F32ListBuilderType {
                return self.initF32ListInSegment(index, element_count, self.segment_id);
            }

            pub fn initF32ListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !F32ListBuilderType {
                const info = try self.initListInSegment(index, 4, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn initU64List(self: PointerListBuilder, index: u32, element_count: u32) !U64ListBuilderType {
                return self.initU64ListInSegment(index, element_count, self.segment_id);
            }

            pub fn initU64ListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !U64ListBuilderType {
                const info = try self.initListInSegment(index, 5, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn initI64List(self: PointerListBuilder, index: u32, element_count: u32) !I64ListBuilderType {
                return self.initI64ListInSegment(index, element_count, self.segment_id);
            }

            pub fn initI64ListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !I64ListBuilderType {
                const info = try self.initListInSegment(index, 5, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn initF64List(self: PointerListBuilder, index: u32, element_count: u32) !F64ListBuilderType {
                return self.initF64ListInSegment(index, element_count, self.segment_id);
            }

            pub fn initF64ListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !F64ListBuilderType {
                const info = try self.initListInSegment(index, 5, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn initBoolList(self: PointerListBuilder, index: u32, element_count: u32) !BoolListBuilderType {
                return self.initBoolListInSegment(index, element_count, self.segment_id);
            }

            pub fn initBoolListInSegment(
                self: PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !BoolListBuilderType {
                const info = try self.initListInSegment(index, 1, element_count, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }
        };
        pub const StructBuilder = struct {
            builder: *MessageBuilderType,
            segment_id: u32,
            offset: usize,
            data_size: u16,
            pointer_count: u16,

            fn getDataSection(self: @This()) []u8 {
                const segment = &self.builder.segments.items[self.segment_id];
                const start = self.offset;
                const end = start + @as(usize, self.data_size) * 8;
                return segment.items[start..end];
            }

            /// Write a u64 into the struct's data section at the given byte offset.
            ///
            /// Silently drops the write if `byte_offset` falls outside the data
            /// section. This is intentional per the Cap'n Proto spec: writing past
            /// the end of a struct's data section is a no-op, which enables schema
            /// evolution — a builder allocated with an older schema (smaller data
            /// section) gracefully ignores fields that do not fit.
            ///
            /// Use `writeU64Strict` when a failed write should be treated as an
            /// error (e.g. protocol-internal construction where the field must exist).
            pub fn writeU64(self: @This(), byte_offset: usize, value: u64) void {
                const data = self.getDataSection();
                if (byte_offset + 8 > data.len) return;
                std.mem.writeInt(u64, data[byte_offset..][0..8], value, .little);
            }

            /// Strict variant of `writeU64` that returns `error.OutOfBounds`
            /// instead of silently dropping the write when the byte offset falls
            /// outside the data section.
            pub fn writeU64Strict(self: @This(), byte_offset: usize, value: u64) error{OutOfBounds}!void {
                const data = self.getDataSection();
                if (byte_offset + 8 > data.len) return error.OutOfBounds;
                std.mem.writeInt(u64, data[byte_offset..][0..8], value, .little);
            }

            /// Write a u32 into the struct's data section at the given byte offset.
            ///
            /// Silently drops the write if `byte_offset` falls outside the data
            /// section. This is intentional per the Cap'n Proto spec for schema
            /// evolution compatibility. See `writeU64` for details.
            ///
            /// Use `writeU32Strict` when a failed write should be treated as an
            /// error.
            pub fn writeU32(self: @This(), byte_offset: usize, value: u32) void {
                const data = self.getDataSection();
                if (byte_offset + 4 > data.len) return;
                std.mem.writeInt(u32, data[byte_offset..][0..4], value, .little);
            }

            /// Strict variant of `writeU32` that returns `error.OutOfBounds`
            /// instead of silently dropping the write when the byte offset falls
            /// outside the data section.
            pub fn writeU32Strict(self: @This(), byte_offset: usize, value: u32) error{OutOfBounds}!void {
                const data = self.getDataSection();
                if (byte_offset + 4 > data.len) return error.OutOfBounds;
                std.mem.writeInt(u32, data[byte_offset..][0..4], value, .little);
            }

            /// Write a u16 into the struct's data section at the given byte offset.
            ///
            /// Silently drops the write if `byte_offset` falls outside the data
            /// section. This is intentional per the Cap'n Proto spec for schema
            /// evolution compatibility. See `writeU64` for details.
            ///
            /// Use `writeU16Strict` when a failed write should be treated as an
            /// error.
            pub fn writeU16(self: @This(), byte_offset: usize, value: u16) void {
                const data = self.getDataSection();
                if (byte_offset + 2 > data.len) return;
                std.mem.writeInt(u16, data[byte_offset..][0..2], value, .little);
            }

            /// Strict variant of `writeU16` that returns `error.OutOfBounds`
            /// instead of silently dropping the write when the byte offset falls
            /// outside the data section.
            pub fn writeU16Strict(self: @This(), byte_offset: usize, value: u16) error{OutOfBounds}!void {
                const data = self.getDataSection();
                if (byte_offset + 2 > data.len) return error.OutOfBounds;
                std.mem.writeInt(u16, data[byte_offset..][0..2], value, .little);
            }

            /// Write a u8 into the struct's data section at the given byte offset.
            ///
            /// Silently drops the write if `byte_offset` falls outside the data
            /// section. This is intentional per the Cap'n Proto spec for schema
            /// evolution compatibility. See `writeU64` for details.
            ///
            /// Use `writeU8Strict` when a failed write should be treated as an
            /// error.
            pub fn writeU8(self: @This(), byte_offset: usize, value: u8) void {
                const data = self.getDataSection();
                if (byte_offset >= data.len) return;
                data[byte_offset] = value;
            }

            /// Strict variant of `writeU8` that returns `error.OutOfBounds`
            /// instead of silently dropping the write when the byte offset falls
            /// outside the data section.
            pub fn writeU8Strict(self: @This(), byte_offset: usize, value: u8) error{OutOfBounds}!void {
                const data = self.getDataSection();
                if (byte_offset >= data.len) return error.OutOfBounds;
                data[byte_offset] = value;
            }

            /// Write a boolean into the struct's data section at the given byte
            /// and bit offset.
            ///
            /// Silently drops the write if `byte_offset` falls outside the data
            /// section. This is intentional per the Cap'n Proto spec for schema
            /// evolution compatibility. See `writeU64` for details.
            ///
            /// Use `writeBoolStrict` when a failed write should be treated as an
            /// error.
            pub fn writeBool(self: @This(), byte_offset: usize, bit_offset: u3, value: bool) void {
                const data = self.getDataSection();
                if (byte_offset >= data.len) return;
                const mask = @as(u8, 1) << bit_offset;
                if (value) {
                    data[byte_offset] |= mask;
                } else {
                    data[byte_offset] &= ~mask;
                }
            }

            /// Strict variant of `writeBool` that returns `error.OutOfBounds`
            /// instead of silently dropping the write when the byte offset falls
            /// outside the data section.
            pub fn writeBoolStrict(self: @This(), byte_offset: usize, bit_offset: u3, value: bool) error{OutOfBounds}!void {
                const data = self.getDataSection();
                if (byte_offset >= data.len) return error.OutOfBounds;
                const mask = @as(u8, 1) << bit_offset;
                if (value) {
                    data[byte_offset] |= mask;
                } else {
                    data[byte_offset] &= ~mask;
                }
            }

            /// Write a union discriminant (which field is set)
            pub fn writeUnionDiscriminant(self: @This(), byte_offset: usize, value: u16) void {
                self.writeU16(byte_offset, value);
            }

            pub fn writeText(self: @This(), pointer_index: usize, text: []const u8) !void {
                return self.writeTextInSegment(pointer_index, text, self.segment_id);
            }

            pub fn writeTextInSegment(
                self: @This(),
                pointer_index: usize,
                text: []const u8,
                target_segment_id: u32,
            ) !void {
                if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;
                if (self.segment_id >= self.builder.segments.items.len) return error.InvalidSegmentId;
                if (target_segment_id >= self.builder.segments.items.len) return error.InvalidSegmentId;

                const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
                try self.builder.writeTextPointer(self.segment_id, pointer_pos, text, target_segment_id);
            }

            pub fn initStruct(self: @This(), pointer_index: usize, data_words: u16, pointer_words: u16) !@This() {
                return self.initStructInSegment(pointer_index, data_words, pointer_words, self.segment_id);
            }

            pub fn initStructInSegment(
                self: @This(),
                pointer_index: usize,
                data_words: u16,
                pointer_words: u16,
                target_segment_id: u32,
            ) !@This() {
                if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;
                while (self.builder.segments.items.len <= target_segment_id) {
                    _ = try self.builder.createSegment();
                }

                const source_segment = &self.builder.segments.items[self.segment_id];
                const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
                if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

                return self.builder.writeStructPointer(self.segment_id, pointer_pos, data_words, pointer_words, target_segment_id);
            }

            pub fn getAnyPointer(self: @This(), pointer_index: usize) !AnyPointerBuilderType {
                if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;

                const segment = &self.builder.segments.items[self.segment_id];
                const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
                if (pointer_pos + 8 > segment.items.len) return error.OutOfBounds;

                return .{
                    .builder = self.builder,
                    .segment_id = self.segment_id,
                    .pointer_pos = pointer_pos,
                };
            }

            fn writePrimitiveListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                element_size: u3,
                target_segment_id: u32,
            ) !struct { segment_id: u32, offset: usize } {
                if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;

                while (self.builder.segments.items.len <= target_segment_id) {
                    _ = try self.builder.createSegment();
                }

                const source_segment = &self.builder.segments.items[self.segment_id];
                const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
                if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

                const offset = try self.builder.writeListPointer(self.segment_id, pointer_pos, element_size, element_count, target_segment_id);
                return .{ .segment_id = target_segment_id, .offset = offset };
            }

            pub fn writeData(self: @This(), pointer_index: usize, data: []const u8) !void {
                return self.writeDataInSegment(pointer_index, data, self.segment_id);
            }

            pub fn writeDataInSegment(
                self: @This(),
                pointer_index: usize,
                data: []const u8,
                target_segment_id: u32,
            ) !void {
                if (data.len > std.math.maxInt(u32)) return error.ElementCountTooLarge;
                const info = try self.writePrimitiveListInSegment(pointer_index, @as(u32, @intCast(data.len)), 2, target_segment_id);
                const segment = &self.builder.segments.items[info.segment_id];
                const slice = segment.items[info.offset .. info.offset + data.len];
                std.mem.copyForwards(u8, slice, data);
            }

            pub fn writeVoidList(self: @This(), pointer_index: usize, element_count: u32) !VoidListBuilderType {
                return self.writeVoidListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeVoidListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !VoidListBuilderType {
                if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;

                while (self.builder.segments.items.len <= target_segment_id) {
                    _ = try self.builder.createSegment();
                }

                const source_segment = &self.builder.segments.items[self.segment_id];
                const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
                if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

                _ = try self.builder.writeListPointer(self.segment_id, pointer_pos, 0, element_count, target_segment_id);
                return .{ .element_count = element_count };
            }

            pub fn writeU8List(self: @This(), pointer_index: usize, element_count: u32) !U8ListBuilderType {
                return self.writeU8ListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeU8ListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !U8ListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 2, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeI8List(self: @This(), pointer_index: usize, element_count: u32) !I8ListBuilderType {
                return self.writeI8ListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeI8ListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !I8ListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 2, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeU16List(self: @This(), pointer_index: usize, element_count: u32) !U16ListBuilderType {
                return self.writeU16ListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeU16ListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !U16ListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 3, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeI16List(self: @This(), pointer_index: usize, element_count: u32) !I16ListBuilderType {
                return self.writeI16ListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeI16ListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !I16ListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 3, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeU32List(self: @This(), pointer_index: usize, element_count: u32) !U32ListBuilderType {
                return self.writeU32ListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeU32ListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !U32ListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 4, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeI32List(self: @This(), pointer_index: usize, element_count: u32) !I32ListBuilderType {
                return self.writeI32ListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeI32ListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !I32ListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 4, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeF32List(self: @This(), pointer_index: usize, element_count: u32) !F32ListBuilderType {
                return self.writeF32ListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeF32ListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !F32ListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 4, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeU64List(self: @This(), pointer_index: usize, element_count: u32) !U64ListBuilderType {
                return self.writeU64ListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeU64ListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !U64ListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 5, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeI64List(self: @This(), pointer_index: usize, element_count: u32) !I64ListBuilderType {
                return self.writeI64ListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeI64ListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !I64ListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 5, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeF64List(self: @This(), pointer_index: usize, element_count: u32) !F64ListBuilderType {
                return self.writeF64ListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeF64ListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !F64ListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 5, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeBoolList(self: @This(), pointer_index: usize, element_count: u32) !BoolListBuilderType {
                return self.writeBoolListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writeBoolListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !BoolListBuilderType {
                const info = try self.writePrimitiveListInSegment(pointer_index, element_count, 1, target_segment_id);
                return .{
                    .builder = self.builder,
                    .segment_id = info.segment_id,
                    .elements_offset = info.offset,
                    .element_count = element_count,
                };
            }

            pub fn writeStructList(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                data_words: u16,
                pointer_words: u16,
            ) !StructListBuilderType {
                return self.writeStructListInSegments(pointer_index, element_count, data_words, pointer_words, self.segment_id, self.segment_id);
            }

            pub fn writeStructListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                data_words: u16,
                pointer_words: u16,
                target_segment_id: u32,
            ) !StructListBuilderType {
                return self.writeStructListInSegments(pointer_index, element_count, data_words, pointer_words, target_segment_id, target_segment_id);
            }

            pub fn writeStructListInSegments(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                data_words: u16,
                pointer_words: u16,
                landing_segment_id: u32,
                content_segment_id: u32,
            ) !StructListBuilderType {
                if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;
                const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
                return self.builder.writeStructListPointer(
                    self.segment_id,
                    pointer_pos,
                    element_count,
                    data_words,
                    pointer_words,
                    landing_segment_id,
                    content_segment_id,
                );
            }

            pub fn writeTextList(self: @This(), pointer_index: usize, element_count: u32) !TextListBuilderType {
                return self.writeTextListInSegments(pointer_index, element_count, self.segment_id, self.segment_id);
            }

            pub fn writeTextListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !TextListBuilderType {
                return self.writeTextListInSegments(pointer_index, element_count, target_segment_id, target_segment_id);
            }

            pub fn writeTextListInSegments(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                landing_segment_id: u32,
                content_segment_id: u32,
            ) !TextListBuilderType {
                if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;

                const total_bytes = @as(usize, element_count) * 8;

                while (self.builder.segments.items.len <= landing_segment_id or self.builder.segments.items.len <= content_segment_id) {
                    _ = try self.builder.createSegment();
                }

                const source_segment = &self.builder.segments.items[self.segment_id];
                const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
                if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

                if (landing_segment_id == content_segment_id) {
                    const target_segment = &self.builder.segments.items[landing_segment_id];
                    const landing_pad_pos = if (self.segment_id == landing_segment_id) null else target_segment.items.len;
                    if (landing_pad_pos) |_| {
                        try target_segment.appendNTimes(self.builder.allocator, 0, 8);
                    }

                    const elements_offset = target_segment.items.len;
                    try target_segment.appendNTimes(self.builder.allocator, 0, total_bytes);

                    if (self.segment_id == landing_segment_id) {
                        const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(elements_offset)) - @as(isize, @intCast(pointer_pos)) - 8, 8)));
                        const list_ptr = make_list_pointer(rel_offset, 6, element_count);
                        std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], list_ptr, .little);
                    } else {
                        const landing_pos = landing_pad_pos.?;
                        const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(elements_offset)) - @as(isize, @intCast(landing_pos)) - 8, 8)));
                        const list_ptr = make_list_pointer(rel_offset, 6, element_count);
                        std.mem.writeInt(u64, target_segment.items[landing_pos..][0..8], list_ptr, .little);

                        const far_ptr = make_far_pointer(false, @as(u32, @intCast(landing_pos / 8)), landing_segment_id);
                        std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], far_ptr, .little);
                    }

                    return TextListBuilderType{
                        .builder = self.builder,
                        .segment_id = landing_segment_id,
                        .elements_offset = elements_offset,
                        .element_count = element_count,
                    };
                }

                const landing_segment = &self.builder.segments.items[landing_segment_id];
                const landing_pad_pos = landing_segment.items.len;
                try landing_segment.appendNTimes(self.builder.allocator, 0, 16);

                const content_segment = &self.builder.segments.items[content_segment_id];
                const elements_offset = content_segment.items.len;
                try content_segment.appendNTimes(self.builder.allocator, 0, total_bytes);

                const landing_far = make_far_pointer(false, @as(u32, @intCast(elements_offset / 8)), content_segment_id);
                std.mem.writeInt(u64, landing_segment.items[landing_pad_pos..][0..8], landing_far, .little);

                const tag_word = make_list_pointer(0, 6, element_count);
                std.mem.writeInt(u64, landing_segment.items[landing_pad_pos + 8 ..][0..8], tag_word, .little);

                const far_ptr = make_far_pointer(true, @as(u32, @intCast(landing_pad_pos / 8)), landing_segment_id);
                std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], far_ptr, .little);

                return TextListBuilderType{
                    .builder = self.builder,
                    .segment_id = content_segment_id,
                    .elements_offset = elements_offset,
                    .element_count = element_count,
                };
            }

            pub fn writePointerList(self: @This(), pointer_index: usize, element_count: u32) !PointerListBuilder {
                return self.writePointerListInSegment(pointer_index, element_count, self.segment_id);
            }

            pub fn writePointerListInSegment(
                self: @This(),
                pointer_index: usize,
                element_count: u32,
                target_segment_id: u32,
            ) !PointerListBuilder {
                if (pointer_index >= self.pointer_count) return error.PointerIndexOutOfBounds;

                const total_bytes = @as(usize, element_count) * 8;

                while (self.builder.segments.items.len <= target_segment_id) {
                    _ = try self.builder.createSegment();
                }

                const source_segment = &self.builder.segments.items[self.segment_id];
                const pointer_pos = self.offset + @as(usize, self.data_size) * 8 + pointer_index * 8;
                if (pointer_pos + 8 > source_segment.items.len) return error.OutOfBounds;

                const target_segment = &self.builder.segments.items[target_segment_id];
                const landing_pad_pos = if (self.segment_id == target_segment_id) null else target_segment.items.len;
                if (landing_pad_pos) |_| {
                    try target_segment.appendNTimes(self.builder.allocator, 0, 8);
                }

                const elements_offset = target_segment.items.len;
                try target_segment.appendNTimes(self.builder.allocator, 0, total_bytes);

                if (self.segment_id == target_segment_id) {
                    const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(elements_offset)) - @as(isize, @intCast(pointer_pos)) - 8, 8)));
                    const list_ptr = make_list_pointer(rel_offset, 6, element_count);
                    std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], list_ptr, .little);
                } else {
                    const landing_pos = landing_pad_pos.?;
                    const rel_offset = @as(i32, @intCast(@divTrunc(@as(isize, @intCast(elements_offset)) - @as(isize, @intCast(landing_pos)) - 8, 8)));
                    const list_ptr = make_list_pointer(rel_offset, 6, element_count);
                    std.mem.writeInt(u64, target_segment.items[landing_pos..][0..8], list_ptr, .little);

                    const far_ptr = make_far_pointer(false, @as(u32, @intCast(landing_pos / 8)), target_segment_id);
                    std.mem.writeInt(u64, source_segment.items[pointer_pos..][0..8], far_ptr, .little);
                }

                return PointerListBuilder{
                    .builder = self.builder,
                    .segment_id = target_segment_id,
                    .elements_offset = elements_offset,
                    .element_count = element_count,
                };
            }
        };
    };
}
