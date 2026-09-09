//! Runtime support for generated mutable views. Returned byte slices borrow
//! the MessageBuilder and are invalidated by subsequent builder mutation.
const std = @import("std");
const message = @import("message.zig");

/// Owns only a segment index, borrowing the MessageBuilder's actual storage.
/// Keep this value at a stable address while any readers are live. Rebinding,
/// deinitialization, or ANY mutation of the builder invalidates those readers.
pub const ReaderStorage = struct {
    allocator: std.mem.Allocator,
    segments: std.ArrayList([]const u8) = .empty,
    message_view: message.Message = undefined,

    pub fn init(allocator: std.mem.Allocator) ReaderStorage {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ReaderStorage) void {
        self.segments.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn bind(self: *ReaderStorage, builder: *message.MessageBuilder) !void {
        try self.segments.resize(self.allocator, builder.segments.items.len);
        for (builder.segments.items, self.segments.items) |source, *destination| destination.* = source.items;
        self.message_view = .{ .allocator = self.allocator, .segments = self.segments.items, .segments_owned = false, .backing_data = null };
    }

    pub fn reader(self: *const ReaderStorage, builder: message.StructBuilder) !message.StructReader {
        if (builder.segment_id >= self.message_view.segments.len) return error.InvalidSegmentId;
        const data = self.message_view.segments[builder.segment_id];
        const width = (@as(usize, builder.data_size) + builder.pointer_count) * 8;
        if (builder.offset > data.len or width > data.len - builder.offset) return error.OutOfBounds;
        return .{ .message = &self.message_view, .segment_id = builder.segment_id, .offset = builder.offset, .data_size = builder.data_size, .pointer_count = builder.pointer_count };
    }
};

/// Scalar reads need no segment index allocation and cannot follow pointers.
pub fn scalarReader(builder: message.StructBuilder) ScalarReader {
    return .{ .builder = builder };
}

pub const ScalarReader = struct {
    builder: message.StructBuilder,
    fn read(self: ScalarReader, comptime T: type, offset: usize) T {
        if (self.builder.segment_id >= self.builder.builder.segments.items.len) return 0;
        const data = self.builder.builder.segments.items[self.builder.segment_id].items;
        const width = @as(usize, self.builder.data_size) * 8;
        if (offset > width or @sizeOf(T) > width - offset) return 0;
        if (self.builder.offset > data.len or offset > data.len - self.builder.offset) return 0;
        const start = self.builder.offset + offset;
        if (@sizeOf(T) > data.len - start) return 0;
        return std.mem.readInt(T, data[start..][0..@sizeOf(T)], .little);
    }
    pub fn readU8(self: ScalarReader, offset: usize) u8 {
        return self.read(u8, offset);
    }
    pub fn readU16(self: ScalarReader, offset: usize) u16 {
        return self.read(u16, offset);
    }
    pub fn readU32(self: ScalarReader, offset: usize) u32 {
        return self.read(u32, offset);
    }
    pub fn readU64(self: ScalarReader, offset: usize) u64 {
        return self.read(u64, offset);
    }
    pub fn readBool(self: ScalarReader, offset: usize, bit: u3) bool {
        return (self.readU8(offset) & (@as(u8, 1) << bit)) != 0;
    }
};

fn pointerWord(pointer: message.AnyPointerBuilder) !u64 {
    if (pointer.segment_id >= pointer.builder.segments.items.len) return error.InvalidSegmentId;
    const data = pointer.builder.segments.items[pointer.segment_id].items;
    if (pointer.pointer_pos > data.len or data.len - pointer.pointer_pos < 8) return error.OutOfBounds;
    return std.mem.readInt(u64, data[pointer.pointer_pos..][0..8], .little);
}

fn restorePointer(pointer: message.AnyPointerBuilder, word: u64) void {
    std.mem.writeInt(u64, pointer.builder.segments.items[pointer.segment_id].items[pointer.pointer_pos..][0..8], word, .little);
}

fn snapshotPointer(pointer: message.AnyPointerBuilder, snapshot: *const message.Message) !message.AnyPointerReader {
    if (pointer.segment_id >= snapshot.segments.len) return error.InvalidSegmentId;
    const data = snapshot.segments[pointer.segment_id];
    if (pointer.pointer_pos > data.len or data.len - pointer.pointer_pos < 8) return error.OutOfBounds;
    return .{ .message = snapshot, .segment_id = pointer.segment_id, .pointer_pos = pointer.pointer_pos, .pointer_word = std.mem.readInt(u64, data[pointer.pointer_pos..][0..8], .little) };
}

fn copyStruct(source: message.StructReader, dest: message.StructBuilder) !void {
    const data = source.getDataSection();
    if (data.len > @as(usize, dest.data_size) * 8 or source.pointer_count > dest.pointer_count) return error.StructSizeMismatch;
    for (data, 0..) |byte, index| try dest.writeU8Strict(index, byte);
    for (0..source.pointer_count) |index| try message.cloneAnyPointer(try source.readAnyPointer(index), try dest.getAnyPointer(index));
}

pub fn materializeDefault(pointer: message.AnyPointerBuilder, source: message.AnyPointerReader) !void {
    if (try pointerWord(pointer) != 0) return;
    errdefer restorePointer(pointer, 0);
    try message.cloneAnyPointer(source, pointer);
}

/// Reopens the declared struct layout, retaining unknown sections on growth.
pub fn getStruct(pointer: message.AnyPointerBuilder, data_words: u16, pointer_words: u16) !message.StructBuilder {
    const old = try pointer.getStruct();
    if (old.data_size >= data_words and old.pointer_count >= pointer_words) return old;
    const allocator = pointer.builder.allocator;
    const bytes = try pointer.builder.toBytes();
    defer allocator.free(bytes);
    var snapshot = try message.Message.initUnvalidated(allocator, bytes);
    defer snapshot.deinit();
    const original = try snapshotPointer(pointer, &snapshot);
    const source = try original.getStruct();
    errdefer restorePointer(pointer, original.pointer_word);
    const result = try pointer.initStruct(@max(data_words, source.data_size), @max(pointer_words, source.pointer_count));
    try copyStruct(source, result);
    return result;
}

/// Promotes older primitive/composite list encodings to the declared struct
/// layout. A larger unknown section is retained, including pointer targets.
/// Acquired element builders expire if a later operation replaces this list.
pub fn getStructList(pointer: message.AnyPointerBuilder, data_words: u16, pointer_words: u16) !message.StructListBuilder {
    if (pointer.getStructList()) |existing| {
        if (existing.data_words >= data_words and existing.pointer_words >= pointer_words) return existing;
    } else |_| {}
    const allocator = pointer.builder.allocator;
    const bytes = try pointer.builder.toBytes();
    defer allocator.free(bytes);
    var snapshot = try message.Message.initUnvalidated(allocator, bytes);
    defer snapshot.deinit();
    const original = try snapshotPointer(pointer, &snapshot);
    const any_list = try message.AnyListReader.wrap(original);
    if (!original.isNull() and try any_list.elementSize() == 1) return error.TypeMismatch;
    const source = try any_list.getStructList();
    const source_words = @max(source.data_words, @as(u16, if (source.sub_word_data_bytes != 0) 1 else 0));
    errdefer restorePointer(pointer, original.pointer_word);
    const result = try pointer.initStructList(source.len(), @max(data_words, source_words), @max(pointer_words, source.pointer_words));
    if (source_words != 0 or source.pointer_words != 0) {
        for (0..source.len()) |index| try copyStruct(try source.get(@intCast(index)), try result.get(@intCast(index)));
    }
    return result;
}

/// Copy setters first create independent input storage. This permits copying
/// from a borrowed Reader of the same builder without use-after-reallocation.
pub fn setStruct(pointer: message.AnyPointerBuilder, source: message.StructReader) !void {
    var scratch = message.MessageBuilder.init(pointer.builder.allocator);
    defer scratch.deinit();
    const temporary = try scratch.allocateStruct(@max(source.data_size, @as(u16, if (source.sub_word_data_bytes != 0) 1 else 0)), source.pointer_count);
    try copyStruct(source, temporary);
    var storage = ReaderStorage.init(pointer.builder.allocator);
    defer storage.deinit();
    try storage.bind(&scratch);
    try replacePointer(pointer, try storage.message_view.getRootAnyPointer());
}

fn replacePointer(pointer: message.AnyPointerBuilder, source: message.AnyPointerReader) !void {
    const old_word = try pointerWord(pointer);
    errdefer restorePointer(pointer, old_word);
    try message.cloneAnyPointer(source, pointer);
}

pub fn setPointer(pointer: message.AnyPointerBuilder, source: message.AnyPointerReader) !void {
    const bytes = try message.cloneAnyPointerToBytes(pointer.builder.allocator, source);
    defer pointer.builder.allocator.free(bytes);
    var snapshot = try message.Message.initFlatUnvalidated(pointer.builder.allocator, bytes);
    defer snapshot.deinit();
    try replacePointer(pointer, try snapshot.getRootAnyPointer());
}

pub fn setList(pointer: message.AnyPointerBuilder, source: anytype) !void {
    var scratch = message.MessageBuilder.init(pointer.builder.allocator);
    defer scratch.deinit();
    const temporary = try scratch.initRootAnyPointer();
    const raw = if (@hasField(@TypeOf(source), "_list")) source._list else source;
    if (raw.source_list) |source_list| {
        try message.cloneAnyPointer(.{ .message = source_list.message, .segment_id = source_list.pointer.segment_id, .pointer_pos = source_list.pointer.pointer_pos, .pointer_word = source_list.pointer.pointer_word }, temporary);
    } else try copyList(temporary, raw);
    var storage = ReaderStorage.init(pointer.builder.allocator);
    defer storage.deinit();
    try storage.bind(&scratch);
    try replacePointer(pointer, try storage.message_view.getRootAnyPointer());
}

fn copyList(pointer: message.AnyPointerBuilder, source: anytype) !void {
    const T = @TypeOf(source);
    if (T == message.StructListReader) {
        const data_words = @max(source.data_words, @as(u16, if (source.sub_word_data_bytes != 0) 1 else 0));
        const result = try pointer.initStructList(source.len(), data_words, source.pointer_words);
        if (data_words != 0 or source.pointer_words != 0) for (0..source.len()) |i| try copyStruct(try source.get(@intCast(i)), try result.get(@intCast(i)));
    } else if (T == message.VoidListReader) {
        _ = try pointer.initVoidList(source.len());
    } else if (T == message.PointerListReader or T == message.TextListReader or T == message.StrictTextListReader) {
        const result = try pointer.initPointerList(source.len());
        for (0..source.len()) |i| {
            const pos = source.elements_offset + i * @as(usize, if (source.stride_bytes != 0) source.stride_bytes else 8);
            if (source.segment_id >= source.message.segments.len) return error.InvalidSegmentId;
            const segment = source.message.segments[source.segment_id];
            if (pos > segment.len or segment.len - pos < 8) return error.OutOfBounds;
            const from = message.AnyPointerReader{ .message = source.message, .segment_id = source.segment_id, .pointer_pos = pos, .pointer_word = std.mem.readInt(u64, segment[pos..][0..8], .little) };
            const to = message.AnyPointerBuilder{ .builder = result.builder, .segment_id = result.segment_id, .pointer_pos = result.elements_offset + i * 8 };
            try message.cloneAnyPointer(from, to);
        }
    } else {
        const method = comptime if (T == message.BoolListReader) "initBoolList" else if (T == message.U8ListReader) "initU8List" else if (T == message.I8ListReader) "initI8List" else if (T == message.U16ListReader) "initU16List" else if (T == message.I16ListReader) "initI16List" else if (T == message.U32ListReader) "initU32List" else if (T == message.I32ListReader) "initI32List" else if (T == message.U64ListReader) "initU64List" else if (T == message.I64ListReader) "initI64List" else if (T == message.F32ListReader) "initF32List" else if (T == message.F64ListReader) "initF64List" else @compileError("unsupported generated list reader");
        const result = try @field(message.AnyPointerBuilder, method)(pointer, source.len());
        for (0..source.len()) |i| try result.set(@intCast(i), try source.get(@intCast(i)));
    }
}

pub fn requirePointerKind(source: message.AnyPointerReader, expected: u2) !void {
    if (source.isNull()) return;
    const resolved = try source.message.resolvePointer(source.segment_id, source.pointer_pos, source.pointer_word, 8);
    if (@as(u2, @truncate(resolved.pointer_word)) != expected) return error.InvalidPointer;
}
