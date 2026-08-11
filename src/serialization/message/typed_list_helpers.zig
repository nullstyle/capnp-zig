const std = @import("std");

/// Generic typed list helpers used by generated Cap'n Proto code.
///
/// These provide thin wrappers around the low-level list reader/builder types,
/// adding type-safe enum conversions and struct wrapping. Generated code
/// references these instead of emitting identical helper implementations
/// in every struct.
pub fn define(
    comptime MessageModule: type,
) type {
    return struct {
        pub fn EnumListReader(comptime EnumType: type) type {
            return struct {
                _list: MessageModule.U16ListReader,

                pub fn len(self: @This()) u32 {
                    return self._list.len();
                }

                pub fn get(self: @This(), index: u32) !EnumType {
                    return std.enums.fromInt(EnumType, try self.getOrdinal(index)) orelse return error.InvalidEnumValue;
                }

                /// Return the logical wire ordinal without requiring it to be a
                /// currently-known member of `EnumType`.
                pub fn getOrdinal(self: @This(), index: u32) !u16 {
                    return try self._list.get(index);
                }

                pub fn raw(self: @This()) MessageModule.U16ListReader {
                    return self._list;
                }
            };
        }

        pub fn EnumListBuilder(comptime EnumType: type) type {
            return struct {
                _list: MessageModule.U16ListBuilder,

                pub fn len(self: @This()) u32 {
                    return self._list.len();
                }

                pub fn set(self: @This(), index: u32, value: EnumType) !void {
                    try self.setOrdinal(index, @backingInt(value));
                }

                /// Store a logical wire ordinal, including values added by a
                /// newer version of the schema.
                pub fn setOrdinal(self: @This(), index: u32, value: u16) !void {
                    try self._list.set(index, value);
                }

                pub fn raw(self: @This()) MessageModule.U16ListBuilder {
                    return self._list;
                }
            };
        }

        pub fn StructListReader(comptime StructType: type) type {
            return struct {
                _list: MessageModule.StructListReader,

                pub fn len(self: @This()) u32 {
                    return self._list.len();
                }

                pub fn get(self: @This(), index: u32) !StructType.Reader {
                    const item = try self._list.get(index);
                    return StructType.Reader.wrap(item);
                }

                pub fn raw(self: @This()) MessageModule.StructListReader {
                    return self._list;
                }
            };
        }

        pub fn StructListBuilder(comptime StructType: type) type {
            return struct {
                _list: MessageModule.StructListBuilder,

                pub fn len(self: @This()) u32 {
                    return self._list.len();
                }

                pub fn get(self: @This(), index: u32) !StructType.Builder {
                    const item = try self._list.get(index);
                    return StructType.Builder.wrap(item);
                }

                pub fn raw(self: @This()) MessageModule.StructListBuilder {
                    return self._list;
                }
            };
        }

        pub const DataListReader = struct {
            _list: MessageModule.PointerListReader,

            pub fn len(self: @This()) u32 {
                return self._list.len();
            }

            pub fn get(self: @This(), index: u32) ![]const u8 {
                return try self._list.getData(index);
            }

            pub fn raw(self: @This()) MessageModule.PointerListReader {
                return self._list;
            }
        };

        pub const DataListBuilder = struct {
            _list: MessageModule.PointerListBuilder,

            pub fn len(self: @This()) u32 {
                return self._list.len();
            }

            pub fn set(self: @This(), index: u32, value: []const u8) !void {
                try self._list.setData(index, value);
            }

            pub fn setNull(self: @This(), index: u32) !void {
                try self._list.setNull(index);
            }

            pub fn raw(self: @This()) MessageModule.PointerListBuilder {
                return self._list;
            }
        };

        pub const CapabilityListReader = struct {
            _list: MessageModule.PointerListReader,

            pub fn len(self: @This()) u32 {
                return self._list.len();
            }

            pub fn get(self: @This(), index: u32) !MessageModule.Capability {
                return try self._list.getCapability(index);
            }

            pub fn raw(self: @This()) MessageModule.PointerListReader {
                return self._list;
            }
        };

        pub const CapabilityListBuilder = struct {
            _list: MessageModule.PointerListBuilder,

            pub fn len(self: @This()) u32 {
                return self._list.len();
            }

            pub fn set(self: @This(), index: u32, cap: MessageModule.Capability) !void {
                try self._list.setCapability(index, cap);
            }

            pub fn setNull(self: @This(), index: u32) !void {
                try self._list.setNull(index);
            }

            pub fn raw(self: @This()) MessageModule.PointerListBuilder {
                return self._list;
            }
        };

        /// Wire element kinds for the scalar/text list codecs used by recursive
        /// generated list views.
        pub const ScalarListKind = enum {
            void,
            bool,
            int8,
            uint8,
            int16,
            uint16,
            int32,
            uint32,
            float32,
            int64,
            uint64,
            float64,
            text,
        };

        fn emptyListReader(comptime ReaderType: type, anchor: MessageModule.PointerListReader) ReaderType {
            var reader: ReaderType = undefined;
            reader.element_count = 0;
            if (@hasField(ReaderType, "message")) reader.message = anchor.message;
            if (@hasField(ReaderType, "segment_id")) reader.segment_id = anchor.segment_id;
            if (@hasField(ReaderType, "elements_offset")) reader.elements_offset = 0;
            if (@hasField(ReaderType, "stride_bytes")) reader.stride_bytes = 0;
            return reader;
        }

        /// Codec for the terminal `List(T)` of a recursively nested list where
        /// T is scalar, Void, or Text.
        pub fn ScalarListCodec(comptime kind: ScalarListKind) type {
            const ReaderType = switch (kind) {
                .void => MessageModule.VoidListReader,
                .bool => MessageModule.BoolListReader,
                .int8 => MessageModule.I8ListReader,
                .uint8 => MessageModule.U8ListReader,
                .int16 => MessageModule.I16ListReader,
                .uint16 => MessageModule.U16ListReader,
                .int32 => MessageModule.I32ListReader,
                .uint32 => MessageModule.U32ListReader,
                .float32 => MessageModule.F32ListReader,
                .int64 => MessageModule.I64ListReader,
                .uint64 => MessageModule.U64ListReader,
                .float64 => MessageModule.F64ListReader,
                .text => MessageModule.TextListReader,
            };
            const BuilderType = switch (kind) {
                .void => MessageModule.VoidListBuilder,
                .bool => MessageModule.BoolListBuilder,
                .int8 => MessageModule.I8ListBuilder,
                .uint8 => MessageModule.U8ListBuilder,
                .int16 => MessageModule.I16ListBuilder,
                .uint16 => MessageModule.U16ListBuilder,
                .int32 => MessageModule.I32ListBuilder,
                .uint32 => MessageModule.U32ListBuilder,
                .float32 => MessageModule.F32ListBuilder,
                .int64 => MessageModule.I64ListBuilder,
                .uint64 => MessageModule.U64ListBuilder,
                .float64 => MessageModule.F64ListBuilder,
                .text => MessageModule.TextListBuilder,
            };

            return struct {
                pub const Reader = ReaderType;
                pub const Builder = BuilderType;

                pub fn read(list: MessageModule.PointerListReader, index: u32) !Reader {
                    return switch (kind) {
                        .void => list.getVoidList(index),
                        .bool => list.getBoolList(index),
                        .int8 => list.getI8List(index),
                        .uint8 => list.getU8List(index),
                        .int16 => list.getI16List(index),
                        .uint16 => list.getU16List(index),
                        .int32 => list.getI32List(index),
                        .uint32 => list.getU32List(index),
                        .float32 => list.getF32List(index),
                        .int64 => list.getI64List(index),
                        .uint64 => list.getU64List(index),
                        .float64 => list.getF64List(index),
                        .text => list.getTextList(index),
                    };
                }

                pub fn empty(anchor: MessageModule.PointerListReader) Reader {
                    return emptyListReader(Reader, anchor);
                }

                pub fn init(list: MessageModule.PointerListBuilder, index: u32, element_count: u32) !Builder {
                    return switch (kind) {
                        .void => list.initVoidList(index, element_count),
                        .bool => list.initBoolList(index, element_count),
                        .int8 => list.initI8List(index, element_count),
                        .uint8 => list.initU8List(index, element_count),
                        .int16 => list.initI16List(index, element_count),
                        .uint16 => list.initU16List(index, element_count),
                        .int32 => list.initI32List(index, element_count),
                        .uint32 => list.initU32List(index, element_count),
                        .float32 => list.initF32List(index, element_count),
                        .int64 => list.initI64List(index, element_count),
                        .uint64 => list.initU64List(index, element_count),
                        .float64 => list.initF64List(index, element_count),
                        .text => list.initTextList(index, element_count),
                    };
                }

                pub fn initInSegment(
                    list: MessageModule.PointerListBuilder,
                    index: u32,
                    element_count: u32,
                    target_segment_id: u32,
                ) !Builder {
                    return switch (kind) {
                        .void => list.initVoidListInSegment(index, element_count, target_segment_id),
                        .bool => list.initBoolListInSegment(index, element_count, target_segment_id),
                        .int8 => list.initI8ListInSegment(index, element_count, target_segment_id),
                        .uint8 => list.initU8ListInSegment(index, element_count, target_segment_id),
                        .int16 => list.initI16ListInSegment(index, element_count, target_segment_id),
                        .uint16 => list.initU16ListInSegment(index, element_count, target_segment_id),
                        .int32 => list.initI32ListInSegment(index, element_count, target_segment_id),
                        .uint32 => list.initU32ListInSegment(index, element_count, target_segment_id),
                        .float32 => list.initF32ListInSegment(index, element_count, target_segment_id),
                        .int64 => list.initI64ListInSegment(index, element_count, target_segment_id),
                        .uint64 => list.initU64ListInSegment(index, element_count, target_segment_id),
                        .float64 => list.initF64ListInSegment(index, element_count, target_segment_id),
                        .text => list.initTextListInSegment(index, element_count, target_segment_id),
                    };
                }
            };
        }

        pub fn EnumListCodec(comptime EnumType: type) type {
            return struct {
                pub const Reader = EnumListReader(EnumType);
                pub const Builder = EnumListBuilder(EnumType);

                pub fn read(list: MessageModule.PointerListReader, index: u32) !Reader {
                    return .{ ._list = try list.getU16List(index) };
                }

                pub fn empty(anchor: MessageModule.PointerListReader) Reader {
                    return .{ ._list = emptyListReader(MessageModule.U16ListReader, anchor) };
                }

                pub fn init(list: MessageModule.PointerListBuilder, index: u32, element_count: u32) !Builder {
                    return .{ ._list = try list.initU16List(index, element_count) };
                }

                pub fn initInSegment(
                    list: MessageModule.PointerListBuilder,
                    index: u32,
                    element_count: u32,
                    target_segment_id: u32,
                ) !Builder {
                    return .{ ._list = try list.initU16ListInSegment(index, element_count, target_segment_id) };
                }
            };
        }

        pub fn StructListCodec(comptime StructType: type, comptime data_words: u16, comptime pointer_words: u16) type {
            return struct {
                pub const Reader = StructListReader(StructType);
                pub const Builder = StructListBuilder(StructType);

                pub fn read(list: MessageModule.PointerListReader, index: u32) !Reader {
                    return .{ ._list = try list.getStructList(index) };
                }

                pub fn empty(anchor: MessageModule.PointerListReader) Reader {
                    return .{ ._list = .{
                        .message = anchor.message,
                        .segment_id = anchor.segment_id,
                        .elements_offset = 0,
                        .element_count = 0,
                        .data_words = 0,
                        .pointer_words = 0,
                        .sub_word_data_bytes = 0,
                    } };
                }

                pub fn init(list: MessageModule.PointerListBuilder, index: u32, element_count: u32) !Builder {
                    return .{ ._list = try list.initStructList(index, element_count, data_words, pointer_words) };
                }

                pub fn initInSegment(
                    list: MessageModule.PointerListBuilder,
                    index: u32,
                    element_count: u32,
                    target_segment_id: u32,
                ) !Builder {
                    return .{ ._list = try list.initStructListInSegment(
                        index,
                        element_count,
                        data_words,
                        pointer_words,
                        target_segment_id,
                    ) };
                }
            };
        }

        /// Reader-only terminal codec for an unresolved struct id. Generated
        /// code intentionally exposes the raw struct-list reader in this case.
        pub const RawStructListReaderCodec = struct {
            pub const Reader = MessageModule.StructListReader;

            pub fn read(list: MessageModule.PointerListReader, index: u32) !Reader {
                return list.getStructList(index);
            }

            pub fn empty(anchor: MessageModule.PointerListReader) Reader {
                return .{
                    .message = anchor.message,
                    .segment_id = anchor.segment_id,
                    .elements_offset = 0,
                    .element_count = 0,
                    .data_words = 0,
                    .pointer_words = 0,
                    .sub_word_data_bytes = 0,
                };
            }
        };

        pub const DataListCodec = struct {
            pub const Reader = DataListReader;
            pub const Builder = DataListBuilder;

            pub fn read(list: MessageModule.PointerListReader, index: u32) !Reader {
                return .{ ._list = try list.getPointerList(index) };
            }

            pub fn empty(anchor: MessageModule.PointerListReader) Reader {
                return .{ ._list = emptyListReader(MessageModule.PointerListReader, anchor) };
            }

            pub fn init(list: MessageModule.PointerListBuilder, index: u32, element_count: u32) !Builder {
                return .{ ._list = try list.initPointerList(index, element_count) };
            }

            pub fn initInSegment(
                list: MessageModule.PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !Builder {
                return .{ ._list = try list.initPointerListInSegment(index, element_count, target_segment_id) };
            }
        };

        pub const CapabilityListCodec = struct {
            pub const Reader = CapabilityListReader;
            pub const Builder = CapabilityListBuilder;

            pub fn read(list: MessageModule.PointerListReader, index: u32) !Reader {
                return .{ ._list = try list.getPointerList(index) };
            }

            pub fn empty(anchor: MessageModule.PointerListReader) Reader {
                return .{ ._list = emptyListReader(MessageModule.PointerListReader, anchor) };
            }

            pub fn init(list: MessageModule.PointerListBuilder, index: u32, element_count: u32) !Builder {
                return .{ ._list = try list.initPointerList(index, element_count) };
            }

            pub fn initInSegment(
                list: MessageModule.PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !Builder {
                return .{ ._list = try list.initPointerListInSegment(index, element_count, target_segment_id) };
            }
        };

        pub const RawPointerListCodec = struct {
            pub const Reader = MessageModule.PointerListReader;
            pub const Builder = MessageModule.PointerListBuilder;

            pub fn read(list: MessageModule.PointerListReader, index: u32) !Reader {
                return list.getPointerList(index);
            }

            pub fn empty(anchor: MessageModule.PointerListReader) Reader {
                return emptyListReader(MessageModule.PointerListReader, anchor);
            }

            pub fn init(list: MessageModule.PointerListBuilder, index: u32, element_count: u32) !Builder {
                return list.initPointerList(index, element_count);
            }

            pub fn initInSegment(
                list: MessageModule.PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !Builder {
                return list.initPointerListInSegment(index, element_count, target_segment_id);
            }
        };

        /// A recursive reader view over a pointer list whose elements are lists
        /// described by `ElementCodec`.
        pub fn NestedListReader(comptime ElementCodec: type) type {
            return struct {
                _list: MessageModule.PointerListReader,

                pub fn len(self: @This()) u32 {
                    return self._list.len();
                }

                pub fn get(self: @This(), index: u32) !ElementCodec.Reader {
                    if (try self._list.isNull(index)) return ElementCodec.empty(self._list);
                    return ElementCodec.read(self._list, index);
                }

                pub fn isNull(self: @This(), index: u32) !bool {
                    return self._list.isNull(index);
                }

                pub fn raw(self: @This()) MessageModule.PointerListReader {
                    return self._list;
                }
            };
        }

        /// A recursive builder view over a pointer list whose elements are
        /// lists described by `ElementCodec`.
        pub fn NestedListBuilder(comptime ElementCodec: type) type {
            return struct {
                _list: MessageModule.PointerListBuilder,

                pub fn len(self: @This()) u32 {
                    return self._list.len();
                }

                pub fn init(self: @This(), index: u32, element_count: u32) !ElementCodec.Builder {
                    return ElementCodec.init(self._list, index, element_count);
                }

                pub fn initInSegment(
                    self: @This(),
                    index: u32,
                    element_count: u32,
                    target_segment_id: u32,
                ) !ElementCodec.Builder {
                    return ElementCodec.initInSegment(self._list, index, element_count, target_segment_id);
                }

                pub fn setNull(self: @This(), index: u32) !void {
                    return self._list.setNull(index);
                }

                pub fn raw(self: @This()) MessageModule.PointerListBuilder {
                    return self._list;
                }
            };
        }

        pub fn NestedReaderCodec(comptime InnerCodec: type) type {
            return struct {
                pub const Reader = NestedListReader(InnerCodec);

                pub fn read(list: MessageModule.PointerListReader, index: u32) !Reader {
                    return .{ ._list = try list.getPointerList(index) };
                }

                pub fn empty(anchor: MessageModule.PointerListReader) Reader {
                    return .{ ._list = emptyListReader(MessageModule.PointerListReader, anchor) };
                }
            };
        }

        pub fn NestedBuilderCodec(comptime InnerCodec: type) type {
            return struct {
                pub const Builder = NestedListBuilder(InnerCodec);

                pub fn init(list: MessageModule.PointerListBuilder, index: u32, element_count: u32) !Builder {
                    return .{ ._list = try list.initPointerList(index, element_count) };
                }

                pub fn initInSegment(
                    list: MessageModule.PointerListBuilder,
                    index: u32,
                    element_count: u32,
                    target_segment_id: u32,
                ) !Builder {
                    return .{ ._list = try list.initPointerListInSegment(index, element_count, target_segment_id) };
                }
            };
        }

        /// Builder used for `List(List(UnknownStruct))`. The struct layout is
        /// supplied at the point where the innermost list is initialized, just
        /// like the existing raw direct-list API.
        pub const RawStructNestedListBuilder = struct {
            _list: MessageModule.PointerListBuilder,

            pub fn len(self: @This()) u32 {
                return self._list.len();
            }

            pub fn init(
                self: @This(),
                index: u32,
                element_count: u32,
                data_words: u16,
                pointer_words: u16,
            ) !MessageModule.StructListBuilder {
                return self._list.initStructList(index, element_count, data_words, pointer_words);
            }

            pub fn initInSegment(
                self: @This(),
                index: u32,
                element_count: u32,
                data_words: u16,
                pointer_words: u16,
                target_segment_id: u32,
            ) !MessageModule.StructListBuilder {
                return self._list.initStructListInSegment(
                    index,
                    element_count,
                    data_words,
                    pointer_words,
                    target_segment_id,
                );
            }

            pub fn setNull(self: @This(), index: u32) !void {
                return self._list.setNull(index);
            }

            pub fn raw(self: @This()) MessageModule.PointerListBuilder {
                return self._list;
            }
        };

        pub const RawStructNestedBuilderCodec = struct {
            pub const Builder = RawStructNestedListBuilder;

            pub fn init(list: MessageModule.PointerListBuilder, index: u32, element_count: u32) !Builder {
                return .{ ._list = try list.initPointerList(index, element_count) };
            }

            pub fn initInSegment(
                list: MessageModule.PointerListBuilder,
                index: u32,
                element_count: u32,
                target_segment_id: u32,
            ) !Builder {
                return .{ ._list = try list.initPointerListInSegment(index, element_count, target_segment_id) };
            }
        };
    };
}

test "enum list ordinal access preserves unknown values" {
    const FakeMessage = struct {
        pub const U16ListReader = struct {
            values: []const u16,

            pub fn len(self: @This()) u32 {
                return @intCast(self.values.len);
            }

            pub fn get(self: @This(), index: u32) !u16 {
                if (index >= self.values.len) return error.IndexOutOfBounds;
                return self.values[index];
            }
        };

        pub const U16ListBuilder = struct {
            values: []u16,

            pub fn len(self: @This()) u32 {
                return @intCast(self.values.len);
            }

            pub fn set(self: @This(), index: u32, value: u16) !void {
                if (index >= self.values.len) return error.IndexOutOfBounds;
                self.values[index] = value;
            }
        };

        // The remaining types are referenced only by lazy declarations in the
        // generic helper namespace and are not instantiated by this test.
        pub const StructListReader = struct {};
        pub const StructListBuilder = struct {};
        pub const PointerListReader = struct {};
        pub const PointerListBuilder = struct {};
        pub const Capability = struct { id: u32 };
    };
    const State = enum(u16) { ready = 1 };
    const Helpers = define(FakeMessage);

    const source = [_]u16{ 1, 77 };
    const reader = Helpers.EnumListReader(State){ ._list = .{ .values = &source } };
    try std.testing.expectEqual(State.ready, try reader.get(0));
    try std.testing.expectEqual(@as(u16, 77), try reader.getOrdinal(1));
    try std.testing.expectError(error.InvalidEnumValue, reader.get(1));

    var destination = [_]u16{ 0, 0 };
    const builder = Helpers.EnumListBuilder(State){ ._list = .{ .values = &destination } };
    try builder.set(0, .ready);
    try builder.setOrdinal(1, 77);
    try std.testing.expectEqualSlices(u16, &source, &destination);
}
