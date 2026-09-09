//! Experimental binding codecs for generated Apply namespaces. A codec describes
//! a Cap'n Proto pointer type, not a Zig in-memory representation.
const std = @import("std");
const message = @import("message.zig");
const helpers = @import("generated_helpers.zig");

pub const Text = Bytes(true);
pub const Data = Bytes(false);
fn Bytes(comptime text: bool) type {
    return struct {
        pub const Reader = []const u8;
        pub const Builder = []const u8;
        pub fn read(pointer: message.AnyPointerReader) !Reader {
            return if (text) pointer.getTextStrict() else pointer.getData();
        }
        pub fn set(pointer: message.AnyPointerBuilder, value: Reader) !void {
            if (text) {
                if (!std.unicode.utf8ValidateSlice(value)) return error.InvalidUtf8;
                try pointer.setText(value);
            } else try pointer.setData(value);
        }
        pub fn get(pointer: message.AnyPointerBuilder) !Builder {
            var storage = helpers.ReaderStorage.init(pointer.builder.allocator);
            defer storage.deinit();
            try storage.bind(pointer.builder);
            return read(try pointerReader(pointer, &storage));
        }
    };
}

fn pointerReader(pointer: message.AnyPointerBuilder, storage: *const helpers.ReaderStorage) !message.AnyPointerReader {
    const data = storage.message_view.segments[pointer.segment_id];
    if (pointer.pointer_pos > data.len or data.len - pointer.pointer_pos < 8) return error.OutOfBounds;
    return .{ .message = &storage.message_view, .segment_id = pointer.segment_id, .pointer_pos = pointer.pointer_pos, .pointer_word = std.mem.readInt(u64, data[pointer.pointer_pos..][0..8], .little) };
}

pub fn Struct(comptime Type: type, comptime data_words: u16, comptime pointer_words: u16) type {
    return struct {
        pub const StructType = Type;
        pub const data_size = data_words;
        pub const pointer_size = pointer_words;
        pub const Reader = Type.Reader;
        pub const Builder = Type.Builder;
        pub fn wrapRawReader(value: anytype) !Reader {
            return Reader.wrap(value._reader);
        }
        pub fn wrapRawBuilder(value: anytype) !Builder {
            return Builder.wrap(value._builder);
        }
        pub const Pipeline = Type.Pipeline;
        pub fn pipeline(path: anytype) Pipeline {
            return .{ .peer = path.peer, .question_id = path.question_id, .pointer_indexes = path.pointer_indexes, .pointer_count = path.pointer_count };
        }
        pub fn read(pointer: message.AnyPointerReader) !Reader {
            return Reader.wrap(try pointer.getStruct());
        }
        pub fn get(pointer: message.AnyPointerBuilder) !Builder {
            return Builder.wrap(try helpers.getStruct(pointer, data_words, pointer_words));
        }
        pub fn init(pointer: message.AnyPointerBuilder) !Builder {
            return Builder.wrap(try pointer.initStruct(data_words, pointer_words));
        }
        pub fn set(pointer: message.AnyPointerBuilder, value: Reader) !void {
            try helpers.setStruct(pointer, if (@hasField(Reader, "inner")) value.inner else value._reader);
        }
    };
}

pub fn Capability(comptime Interface: type) type {
    return struct {
        pub const Reader = message.Capability;
        pub const Builder = message.Capability;
        pub const Pipeline = Interface.PipelinedClient;
        pub fn pipeline(path: anytype) Pipeline {
            const RawPipeline = if (@hasDecl(Interface, "Raw")) Interface.Raw.PipelinedClient else Pipeline;
            var raw: RawPipeline = .{ .peer = path.peer, .question_id = path.question_id, .pointer_index = path.pointer_indexes[path.pointer_count - 1] };
            raw.pointer_count = path.pointer_count - 1;
            @memcpy(raw.pointer_indexes[0..raw.pointer_count], path.pointer_indexes[0..raw.pointer_count]);
            return if (@hasDecl(Interface, "Raw")) .{ .raw = raw } else raw;
        }
        pub fn read(pointer: message.AnyPointerReader) !Reader {
            return pointer.getCapability();
        }
        pub fn get(pointer: message.AnyPointerBuilder) !Builder {
            var storage = helpers.ReaderStorage.init(pointer.builder.allocator);
            defer storage.deinit();
            try storage.bind(pointer.builder);
            return read(try pointerReader(pointer, &storage));
        }
        pub fn set(pointer: message.AnyPointerBuilder, value: Reader) !void {
            try pointer.setCapability(value);
        }
    };
}

/// Fail at the binding site, before a malformed binding reaches a field access.
pub fn requirePointer(comptime Codec: type) void {
    if (@typeInfo(Codec) != .@"struct") @compileError("Apply binding must be a Cap'n Proto pointer codec");
    if (!@hasDecl(Codec, "Reader") or !@hasDecl(Codec, "Builder") or !@hasDecl(Codec, "read") or !@hasDecl(Codec, "get") or !@hasDecl(Codec, "set")) @compileError("Apply binding must be a Cap'n Proto pointer codec (Text, Data, Struct, Capability, or List)");
}

pub const AnyPointer = struct {
    pub const Reader = message.AnyPointerReader;
    pub const Builder = message.AnyPointerBuilder;
    pub fn read(pointer: Reader) !Reader {
        return pointer;
    }
    pub fn get(pointer: Builder) !Builder {
        return pointer;
    }
    pub fn set(pointer: Builder, value: Reader) !void {
        return helpers.setPointer(pointer, value);
    }
};

/// Typed callbacks are compile-time adapters; the ordinary generated call owns
/// the sole context and controls all Return/disconnect/cancellation cleanup.
pub fn Method(comptime Raw: type, comptime ParamType: type, comptime ResultType: type) type {
    const callback_info = @typeInfo(@typeInfo(Raw.Callback).pointer.child).@"fn";
    const PeerPtr = callback_info.param_types[1] orelse @compileError("RPC callback peer parameter must have a concrete type");
    const CapsPtr = callback_info.param_types[3] orelse @compileError("RPC callback capability parameter must have a concrete type");
    return struct {
        pub const Params = ParamType;
        pub const Results = ResultType;
        pub const ordinal = Raw.ordinal;
        pub const is_streaming = Raw.is_streaming;
        pub const BuildFn = *const fn (*anyopaque, *Params.Builder) anyerror!void;
        pub const Response = struct {
            raw: Raw.Response,
            pub fn unwrap(self: @This()) !Results.Reader {
                return Results.Reader.wrap((try self.raw.unwrap())._reader);
            }
        };
        pub const Callback = *const fn (*anyopaque, PeerPtr, Response, CapsPtr) anyerror!void;
        pub const Handler = *const fn (*anyopaque, PeerPtr, Params.Reader, *Results.Builder, CapsPtr) anyerror!void;
        pub fn ClientAdapter(comptime build_fn: ?BuildFn, comptime callback_fn: Callback) type {
            return struct {
                pub fn build(ctx: *anyopaque, raw: *Raw.Params.Builder) anyerror!void {
                    var params = Params.Builder.wrap(raw._builder);
                    if (build_fn) |function| try function(ctx, &params);
                }
                pub fn callback(ctx: *anyopaque, peer: PeerPtr, response: Raw.Response, caps: CapsPtr) anyerror!void {
                    try callback_fn(ctx, peer, .{ .raw = response }, caps);
                }
            };
        }
        pub fn ServerAdapter(comptime handler: Handler) type {
            return struct {
                pub fn handle(ctx: *anyopaque, peer: PeerPtr, params: Raw.Params.Reader, results: *Raw.Results.Builder, caps: CapsPtr) anyerror!void {
                    var typed_results = Results.Builder.wrap(results._builder);
                    try handler(ctx, peer, Params.Reader.wrap(params._reader), &typed_results, caps);
                }
            };
        }
    };
}

pub fn Scalar(comptime kind: message.typed_list_helpers.ScalarListKind) type {
    return struct {
        pub const scalar_kind = kind;
    };
}
pub fn Enum(comptime Type: type) type {
    return struct {
        pub const enum_type = Type;
    };
}
pub fn List(comptime Element: type) type {
    if (@hasDecl(Element, "StructType")) return StructList(Element);
    if (@hasDecl(Element, "enum_type")) return EnumList(Element.enum_type);
    if (Element == Text) return ScalarList(.text);
    if (@hasDecl(Element, "scalar_kind")) return ScalarList(Element.scalar_kind);
    return PointerList(Element);
}
fn ScalarList(comptime kind: message.typed_list_helpers.ScalarListKind) type {
    const Codec = message.typed_list_helpers.ScalarListCodec(kind);
    const suffix = switch (kind) {
        .void => "Void",
        .bool => "Bool",
        .int8 => "I8",
        .uint8 => "U8",
        .int16 => "I16",
        .uint16 => "U16",
        .int32 => "I32",
        .uint32 => "U32",
        .float32 => "F32",
        .int64 => "I64",
        .uint64 => "U64",
        .float64 => "F64",
        .text => "Text",
    };
    return struct {
        pub const is_list = true;
        pub const Reader = Codec.Reader;
        pub const Builder = Codec.Builder;
        pub fn wrapRawReader(value: anytype) !Reader {
            return value;
        }
        pub fn wrapRawBuilder(value: anytype) !Builder {
            return value;
        }
        pub fn read(pointer: message.AnyPointerReader) !Reader {
            const anchor = message.PointerListReader{ .message = pointer.message, .segment_id = pointer.segment_id, .elements_offset = pointer.pointer_pos, .element_count = 1 };
            return if (pointer.pointer_word == 0) Codec.empty(anchor) else Codec.read(anchor, 0);
        }
        pub fn get(pointer: message.AnyPointerBuilder) !Builder {
            return @field(message.AnyPointerBuilder, "get" ++ suffix ++ "List")(pointer);
        }
        pub fn init(pointer: message.AnyPointerBuilder, count: u32) !Builder {
            const anchor = message.PointerListBuilder{ .builder = pointer.builder, .segment_id = pointer.segment_id, .elements_offset = pointer.pointer_pos, .element_count = 1 };
            return Codec.init(anchor, 0, count);
        }
        pub fn set(pointer: message.AnyPointerBuilder, value: Reader) !void {
            try helpers.setList(pointer, value);
        }
    };
}
fn StructList(comptime Element: type) type {
    const Type = Element.StructType;
    return struct {
        pub const is_list = true;
        pub const Reader = message.typed_list_helpers.StructListReader(Type);
        pub const Builder = message.typed_list_helpers.StructListBuilder(Type);
        pub fn wrapRawReader(value: anytype) !Reader {
            return .{ ._list = if (@hasDecl(@TypeOf(value), "raw")) value.raw() else value };
        }
        pub fn wrapRawBuilder(value: anytype) !Builder {
            return .{ ._list = if (@hasDecl(@TypeOf(value), "raw")) value.raw() else value };
        }
        pub fn read(pointer: message.AnyPointerReader) !Reader {
            return .{ ._list = try (try pointer.getList()).getStructList() };
        }
        pub fn get(pointer: message.AnyPointerBuilder) !Builder {
            return .{ ._list = try helpers.getStructList(pointer, Element.data_size, Element.pointer_size) };
        }
        pub fn init(pointer: message.AnyPointerBuilder, count: u32) !Builder {
            return .{ ._list = try pointer.initStructList(count, Element.data_size, Element.pointer_size) };
        }
        pub fn set(pointer: message.AnyPointerBuilder, value: Reader) !void {
            try helpers.setList(pointer, value.raw());
        }
    };
}
fn EnumList(comptime Type: type) type {
    return struct {
        pub const is_list = true;
        pub const Reader = message.typed_list_helpers.EnumListReader(Type);
        pub const Builder = message.typed_list_helpers.EnumListBuilder(Type);
        pub fn wrapRawReader(value: anytype) !Reader {
            return .{ ._list = if (@hasDecl(@TypeOf(value), "raw")) value.raw() else value };
        }
        pub fn wrapRawBuilder(value: anytype) !Builder {
            return .{ ._list = if (@hasDecl(@TypeOf(value), "raw")) value.raw() else value };
        }
        pub fn read(pointer: message.AnyPointerReader) !Reader {
            return .{ ._list = try (try pointer.getList()).getU16List() };
        }
        pub fn get(pointer: message.AnyPointerBuilder) !Builder {
            return .{ ._list = try pointer.getU16List() };
        }
        pub fn init(pointer: message.AnyPointerBuilder, count: u32) !Builder {
            return .{ ._list = try pointer.initU16List(count) };
        }
        pub fn set(pointer: message.AnyPointerBuilder, value: Reader) !void {
            try helpers.setList(pointer, value.raw());
        }
    };
}
fn PointerList(comptime Element: type) type {
    return struct {
        pub const is_list = true;
        pub const Reader = struct {
            _list: message.PointerListReader,
            pub fn len(self: @This()) u32 {
                return self._list.len();
            }
            pub fn get(self: @This(), index: u32) !Element.Reader {
                if (index >= self.len()) return error.IndexOutOfBounds;
                const offset = self._list.elements_offset + @as(usize, index) * (if (self._list.stride_bytes == 0) @as(usize, 8) else self._list.stride_bytes);
                const holder = message.StructReader{ .message = self._list.message, .segment_id = self._list.segment_id, .offset = offset, .data_size = 0, .pointer_count = 1 };
                return Element.read(try holder.readAnyPointer(0));
            }
            pub fn raw(self: @This()) message.PointerListReader {
                return self._list;
            }
        };
        pub const Builder = struct {
            _list: message.PointerListBuilder,
            pub fn len(self: @This()) u32 {
                return self._list.len();
            }
            fn element(self: @This(), index: u32) !message.AnyPointerBuilder {
                if (index >= self.len()) return error.IndexOutOfBounds;
                const offset = self._list.elements_offset + @as(usize, index) * (if (self._list.stride_bytes == 0) @as(usize, 8) else self._list.stride_bytes);
                const holder = message.StructBuilder{ .builder = self._list.builder, .segment_id = self._list.segment_id, .offset = offset, .data_size = 0, .pointer_count = 1 };
                return holder.getAnyPointer(0);
            }
            pub fn get(self: @This(), index: u32) !Element.Builder {
                return Element.get(try self.element(index));
            }
            pub fn set(self: @This(), index: u32, value: Element.Reader) !void {
                return Element.set(try self.element(index), value);
            }
            pub fn init(self: @This(), index: u32, count: u32) !Element.Builder {
                return Element.init(try self.element(index), count);
            }
            pub fn raw(self: @This()) message.PointerListBuilder {
                return self._list;
            }
        };
        pub fn wrapRawReader(value: anytype) !Reader {
            return .{ ._list = if (@hasDecl(@TypeOf(value), "raw")) value.raw() else value };
        }
        pub fn wrapRawBuilder(value: anytype) !Builder {
            return .{ ._list = if (@hasDecl(@TypeOf(value), "raw")) value.raw() else value };
        }
        pub fn read(pointer: message.AnyPointerReader) !Reader {
            return .{ ._list = try (try pointer.getList()).getPointerList() };
        }
        pub fn get(pointer: message.AnyPointerBuilder) !Builder {
            return .{ ._list = try pointer.getPointerList() };
        }
        pub fn init(pointer: message.AnyPointerBuilder, count: u32) !Builder {
            return .{ ._list = try pointer.initPointerList(count) };
        }
        pub fn set(pointer: message.AnyPointerBuilder, value: Reader) !void {
            try helpers.setList(pointer, value.raw());
        }
    };
}

/// A generated field initializer has the arity of its resolved pointer kind.
pub fn Initializer(comptime Codec: type, comptime Owner: type, comptime slot: u32, comptime tag_offset: u32, comptime tag: u16) type {
    return struct {
        pub const call = if (@hasDecl(Codec, "is_list")) list else single;
        fn single(self: Owner) !Codec.Builder {
            const result = try Codec.init(try self.inner.getAnyPointer(slot));
            if (tag != 0xffff) try self.inner.writeU16Strict(tag_offset, tag);
            return result;
        }
        fn list(self: Owner, count: u32) !Codec.Builder {
            const result = try Codec.init(try self.inner.getAnyPointer(slot), count);
            if (tag != 0xffff) try self.inner.writeU16Strict(tag_offset, tag);
            return result;
        }
    };
}
