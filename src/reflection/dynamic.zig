//! Schema-driven access over the ordinary Cap'n Proto message runtime.
//!
//! Schema views borrow their Registry; readers also borrow their Message.
//! Builders borrow their MessageBuilder. A builder mutation can invalidate
//! readers into that builder's storage, so obtain readers only from serialized
//! messages. Pointer defaults belong to the Registry and are copied on mutation.
//! Widening a struct list invalidates its previously acquired element and nested
//! builders. Reacquire those views through the list, whose own handle remains valid.
const std = @import("std");
const message = @import("../serialization/message.zig");
const schema = @import("../serialization/schema.zig");
const meta = @import("registry.zig");

const no_discriminant = 0xffff;

pub const Enum = struct {
    schema: meta.EnumSchema,
    ordinal: u16,

    pub fn name(self: Enum) ?[]const u8 {
        return self.schema.name(self.ordinal);
    }
};

pub const Value = union(enum) {
    void: void,
    bool: bool,
    int8: i8,
    int16: i16,
    int32: i32,
    int64: i64,
    uint8: u8,
    uint16: u16,
    uint32: u32,
    uint64: u64,
    float32: f32,
    float64: f64,
    text: []const u8,
    data: []const u8,
    @"enum": Enum,
    @"struct": DynamicStruct.Reader,
    list: DynamicList.Reader,
    any_pointer: message.AnyPointerReader,
    /// Null is distinct from the valid first capability-table entry (ID zero).
    capability: ?message.Capability,
};

pub const DynamicStruct = struct {
    pub const Reader = struct {
        schema: meta.StructSchema,
        reader: message.StructReader,

        pub fn init(struct_schema: meta.StructSchema, msg: *const message.Message) !Reader {
            if (struct_schema.proto().is_group) return error.TypeMismatch;
            return .{ .schema = struct_schema, .reader = try msg.getRootStruct() };
        }

        /// Unknown future union discriminants return null. The raw value is
        /// available through whichDiscriminant(), including unknown ordinals.
        pub fn which(self: Reader) !?meta.Field {
            const ordinal = self.whichDiscriminant() orelse return null;
            for (self.schema.proto().fields, 0..) |field, index| {
                if (field.discriminant_value == ordinal) return try self.schema.fieldByIndex(index);
            }
            return null;
        }

        pub fn whichDiscriminant(self: Reader) ?u16 {
            const info = self.schema.proto();
            if (info.discriminant_count == 0) return null;
            return self.reader.readU16(@as(usize, info.discriminant_offset) * 2);
        }

        pub fn get(self: Reader, name: []const u8) anyerror!Value {
            return self.getField(try self.schema.field(name));
        }

        pub fn getField(self: Reader, field: meta.Field) anyerror!Value {
            try checkFieldOwner(self.schema, field);
            try self.requireActive(field);
            const proto = field.proto();
            if (proto.group != null) return .{ .@"struct" = .{
                .schema = try field.groupSchema(),
                .reader = self.reader,
            } };
            const slot = proto.slot orelse return error.InvalidSchema;
            const typ = try (try field.type()).resolved();
            const kind = try typ.proto();
            const offset: usize = slot.offset;
            return switch (kind) {
                .void => .void,
                .bool => .{ .bool = self.reader.readBool(offset / 8, @intCast(offset % 8)) != defaultBool(slot) },
                .int8 => .{ .int8 = @bitCast(self.reader.readU8(offset) ^ defaultBits(u8, slot)) },
                .int16 => .{ .int16 = @bitCast(self.reader.readU16(offset * 2) ^ defaultBits(u16, slot)) },
                .int32 => .{ .int32 = @bitCast(self.reader.readU32(offset * 4) ^ defaultBits(u32, slot)) },
                .int64 => .{ .int64 = @bitCast(self.reader.readU64(offset * 8) ^ defaultBits(u64, slot)) },
                .uint8 => .{ .uint8 = self.reader.readU8(offset) ^ defaultBits(u8, slot) },
                .uint16 => .{ .uint16 = self.reader.readU16(offset * 2) ^ defaultBits(u16, slot) },
                .uint32 => .{ .uint32 = self.reader.readU32(offset * 4) ^ defaultBits(u32, slot) },
                .uint64 => .{ .uint64 = self.reader.readU64(offset * 8) ^ defaultBits(u64, slot) },
                .float32 => .{ .float32 = @bitCast(self.reader.readU32(offset * 4) ^ defaultBits(u32, slot)) },
                .float64 => .{ .float64 = @bitCast(self.reader.readU64(offset * 8) ^ defaultBits(u64, slot)) },
                .@"enum" => .{ .@"enum" = .{
                    .schema = try typ.asEnum(),
                    .ordinal = self.reader.readU16(offset * 2) ^ defaultBits(u16, slot),
                } },
                .text => blk: {
                    if (self.reader.isPointerNull(offset)) if (slot.default_value) |value| {
                        if (value == .text) break :blk .{ .text = value.text };
                    };
                    break :blk .{ .text = try self.reader.readTextStrict(offset) };
                },
                .data => blk: {
                    if (self.reader.isPointerNull(offset)) if (slot.default_value) |value| {
                        if (value == .data) break :blk .{ .data = value.data };
                    };
                    const source = try self.pointer(slot);
                    break :blk .{ .data = try source.getData() };
                },
                else => try readPointer(typ, try self.pointer(slot)),
            };
        }

        /// Reference NON_NULL semantics: active scalars and groups are present;
        /// pointers are present only when their physical pointer is non-null.
        /// A schema pointer default does not make an absent field present.
        pub fn has(self: Reader, name: []const u8) anyerror!bool {
            return self.hasField(try self.schema.field(name), false);
        }

        /// Reference NON_DEFAULT semantics: additionally checks scalar wire
        /// bits against zero. Active groups are present regardless of contents.
        pub fn hasNonDefault(self: Reader, name: []const u8) anyerror!bool {
            return self.hasField(try self.schema.field(name), true);
        }

        fn hasField(self: Reader, field: meta.Field, non_default: bool) anyerror!bool {
            if (!self.isActive(field)) return false;
            if (field.proto().group != null) return true;
            const slot = field.proto().slot orelse return error.InvalidSchema;
            const offset: usize = slot.offset;
            return switch (slot.type) {
                .void => !non_default,
                .bool => !non_default or self.reader.readBool(offset / 8, @intCast(offset % 8)),
                .int8, .uint8 => !non_default or self.reader.readU8(offset) != 0,
                .int16, .uint16, .@"enum" => !non_default or self.reader.readU16(offset * 2) != 0,
                .int32, .uint32, .float32 => !non_default or self.reader.readU32(offset * 4) != 0,
                .int64, .uint64, .float64 => !non_default or self.reader.readU64(offset * 8) != 0,
                else => !self.reader.isPointerNull(offset),
            };
        }

        fn isActive(self: Reader, field: meta.Field) bool {
            const discriminant = field.proto().discriminant_value;
            return discriminant == no_discriminant or self.whichDiscriminant() == discriminant;
        }

        fn requireActive(self: Reader, field: meta.Field) !void {
            if (!self.isActive(field)) return error.InactiveUnionField;
        }

        fn pointer(self: Reader, slot: schema.FieldSlot) !message.AnyPointerReader {
            if (self.reader.isPointerNull(slot.offset)) {
                if (slot.default_value) |value| {
                    switch (value) {
                        .list, .@"struct", .any_pointer => return self.schema.schema.registry.defaultPointer(value),
                        else => {},
                    }
                }
                return .{ .message = self.reader.message, .segment_id = self.reader.segment_id, .pointer_pos = 0, .pointer_word = 0 };
            }
            return self.reader.readAnyPointer(slot.offset);
        }
    };

    pub const Builder = struct {
        schema: meta.StructSchema,
        builder: message.StructBuilder,

        pub fn init(struct_schema: meta.StructSchema, msg: *message.MessageBuilder) !Builder {
            const info = struct_schema.proto();
            if (info.is_group) return error.TypeMismatch;
            return .{ .schema = struct_schema, .builder = try msg.allocateStruct(info.data_word_count, info.pointer_count) };
        }

        pub fn set(self: Builder, name: []const u8, value: Value) anyerror!void {
            return self.setField(try self.schema.field(name), value);
        }

        pub fn setField(self: Builder, field: meta.Field, value: Value) anyerror!void {
            try checkFieldOwner(self.schema, field);
            if (field.proto().group != null) {
                if (value != .@"struct") return error.TypeMismatch;
                const group_schema = try field.groupSchema();
                if (!try sameStructSchema(group_schema, value.@"struct".schema, 0)) return error.TypeMismatch;
                const group = try self.initGroup(field.proto().name);
                return copyGroup(value.@"struct", group, 0);
            }
            const slot = field.proto().slot orelse return error.InvalidSchema;
            const typ = try (try field.type()).resolved();
            try requireValue(typ, value);
            const offset: usize = slot.offset;
            switch (try typ.proto()) {
                .void => {},
                .bool => try self.builder.writeBoolStrict(offset / 8, @intCast(offset % 8), value.bool != defaultBool(slot)),
                .int8 => try self.builder.writeU8Strict(offset, @as(u8, @bitCast(value.int8)) ^ defaultBits(u8, slot)),
                .int16 => try self.builder.writeU16Strict(offset * 2, @as(u16, @bitCast(value.int16)) ^ defaultBits(u16, slot)),
                .int32 => try self.builder.writeU32Strict(offset * 4, @as(u32, @bitCast(value.int32)) ^ defaultBits(u32, slot)),
                .int64 => try self.builder.writeU64Strict(offset * 8, @as(u64, @bitCast(value.int64)) ^ defaultBits(u64, slot)),
                .uint8 => try self.builder.writeU8Strict(offset, value.uint8 ^ defaultBits(u8, slot)),
                .uint16 => try self.builder.writeU16Strict(offset * 2, value.uint16 ^ defaultBits(u16, slot)),
                .uint32 => try self.builder.writeU32Strict(offset * 4, value.uint32 ^ defaultBits(u32, slot)),
                .uint64 => try self.builder.writeU64Strict(offset * 8, value.uint64 ^ defaultBits(u64, slot)),
                .float32 => try self.builder.writeU32Strict(offset * 4, @as(u32, @bitCast(value.float32)) ^ defaultBits(u32, slot)),
                .float64 => try self.builder.writeU64Strict(offset * 8, @as(u64, @bitCast(value.float64)) ^ defaultBits(u64, slot)),
                .@"enum" => try self.builder.writeU16Strict(offset * 2, value.@"enum".ordinal ^ defaultBits(u16, slot)),
                else => try writePointer(typ, try self.builder.getAnyPointer(offset), value),
            }
            try self.select(field);
        }

        pub fn initStruct(self: Builder, name: []const u8) !Builder {
            const field = try self.schema.field(name);
            if (field.proto().group != null) return self.initGroup(name);
            const struct_schema = try (try field.type()).asStruct();
            const info = struct_schema.proto();
            const slot = field.proto().slot orelse return error.TypeMismatch;
            const result = try self.builder.initStruct(slot.offset, info.data_word_count, info.pointer_count);
            try self.select(field);
            return .{ .schema = struct_schema, .builder = result };
        }

        /// Reopen a struct field. A schema default is copied before it becomes
        /// writable; Registry-owned defaults are never modified.
        pub fn getStruct(self: Builder, name: []const u8) !Builder {
            const field = try self.schema.field(name);
            try self.requireActive(field);
            if (field.proto().group != null) return .{ .schema = try field.groupSchema(), .builder = self.builder };
            const struct_schema = try (try field.type()).asStruct();
            const pointer = try self.mutablePointer(field);
            const info = struct_schema.proto();
            const result = try writableStruct(pointer, info.data_word_count, info.pointer_count);
            return .{ .schema = struct_schema, .builder = result };
        }

        pub fn initList(self: Builder, name: []const u8, count: u32) !DynamicList.Builder {
            const field = try self.schema.field(name);
            const typ = try field.type();
            const slot = field.proto().slot orelse return error.TypeMismatch;
            const result = try DynamicList.Builder.init(typ, try self.builder.getAnyPointer(slot.offset), count);
            try self.select(field);
            return result;
        }

        pub fn getList(self: Builder, name: []const u8) !DynamicList.Builder {
            const field = try self.schema.field(name);
            try self.requireActive(field);
            const typ = try field.type();
            _ = try typ.listElement();
            const pointer = try self.mutablePointer(field);
            if (self.builder.isPointerNull(field.proto().slot.?.offset)) return DynamicList.Builder.init(typ, pointer, 0);
            return .{ .type = typ, .pointer = pointer };
        }

        pub fn initGroup(self: Builder, name: []const u8) anyerror!Builder {
            const field = try self.schema.field(name);
            const group = Builder{ .schema = try field.groupSchema(), .builder = self.builder };
            try clearGroup(group, 0);
            try self.select(field);
            return group;
        }

        /// Reset a field to its schema default, selecting it if it is a union
        /// arm. Pointer defaults are represented by a null pointer on the wire.
        pub fn clear(self: Builder, name: []const u8) anyerror!void {
            const field = try self.schema.field(name);
            try self.clearStorage(field, 0);
            try self.select(field);
        }

        fn clearStorage(self: Builder, field: meta.Field, depth: usize) anyerror!void {
            if (depth >= 64) return error.SchemaRecursionLimitExceeded;
            if (field.proto().group != null) return clearGroup(.{ .schema = try field.groupSchema(), .builder = self.builder }, depth + 1);
            const slot = field.proto().slot orelse return error.InvalidSchema;
            const offset: usize = slot.offset;
            switch (slot.type) {
                .void => {},
                .bool => try self.builder.writeBoolStrict(offset / 8, @intCast(offset % 8), false),
                .int8, .uint8 => try self.builder.writeU8Strict(offset, 0),
                .int16, .uint16, .@"enum" => try self.builder.writeU16Strict(offset * 2, 0),
                .int32, .uint32, .float32 => try self.builder.writeU32Strict(offset * 4, 0),
                .int64, .uint64, .float64 => try self.builder.writeU64Strict(offset * 8, 0),
                else => try (try self.builder.getAnyPointer(offset)).setNull(),
            }
        }

        fn select(self: Builder, field: meta.Field) !void {
            const ordinal = field.proto().discriminant_value;
            if (ordinal != no_discriminant) try self.builder.writeU16Strict(@as(usize, self.schema.proto().discriminant_offset) * 2, ordinal);
        }

        fn requireActive(self: Builder, field: meta.Field) !void {
            const ordinal = field.proto().discriminant_value;
            if (ordinal != no_discriminant and self.builder.readUnionDiscriminant(@as(usize, self.schema.proto().discriminant_offset) * 2) != ordinal)
                return error.InactiveUnionField;
        }

        fn mutablePointer(self: Builder, field: meta.Field) !message.AnyPointerBuilder {
            const slot = field.proto().slot orelse return error.TypeMismatch;
            const pointer = try self.builder.getAnyPointer(slot.offset);
            if (self.builder.isPointerNull(slot.offset)) if (slot.default_value) |value| {
                switch (value) {
                    .list, .@"struct", .any_pointer => try message.cloneAnyPointer(try self.schema.schema.registry.defaultPointer(value), pointer),
                    .text => |text| try pointer.setText(text),
                    .data => |data| try pointer.setData(data),
                    else => {},
                }
            };
            return pointer;
        }
    };
};

pub const DynamicList = struct {
    pub const Reader = struct {
        /// The complete List(T) type, including its brand environment.
        type: meta.Type,
        reader: message.AnyListReader,

        pub fn len(self: Reader) !u32 {
            return self.reader.len();
        }

        pub fn get(self: Reader, index: u32) anyerror!Value {
            if (index >= try self.len()) return error.IndexOutOfBounds;
            const element = try (try self.type.listElement()).resolved();
            return switch (try element.proto()) {
                .void => .void,
                .bool => .{ .bool = try (try self.reader.getBoolList()).get(index) },
                .int8 => .{ .int8 = try (try self.reader.getI8List()).get(index) },
                .int16 => .{ .int16 = try (try self.reader.getI16List()).get(index) },
                .int32 => .{ .int32 = try (try self.reader.getI32List()).get(index) },
                .int64 => .{ .int64 = try (try self.reader.getI64List()).get(index) },
                .uint8 => .{ .uint8 = try (try self.reader.getU8List()).get(index) },
                .uint16 => .{ .uint16 = try (try self.reader.getU16List()).get(index) },
                .uint32 => .{ .uint32 = try (try self.reader.getU32List()).get(index) },
                .uint64 => .{ .uint64 = try (try self.reader.getU64List()).get(index) },
                .float32 => .{ .float32 = try (try self.reader.getF32List()).get(index) },
                .float64 => .{ .float64 = try (try self.reader.getF64List()).get(index) },
                .@"enum" => .{ .@"enum" = .{ .schema = try element.asEnum(), .ordinal = try (try self.reader.getU16List()).get(index) } },
                .@"struct" => if (try usesInlineStructElements(self.type))
                    .{ .@"struct" = .{ .schema = try element.asStruct(), .reader = try (try self.reader.getStructList()).get(index) } }
                else
                    try readPointer(element, try listPointer(self.reader, index)),
                else => try readPointer(element, try listPointer(self.reader, index)),
            };
        }
    };

    pub const Builder = struct {
        type: meta.Type,
        pointer: message.AnyPointerBuilder,

        pub fn init(list_type: meta.Type, pointer: message.AnyPointerBuilder, count: u32) !Builder {
            const element = try (try list_type.listElement()).resolved();
            switch (try element.proto()) {
                .void => _ = try pointer.initVoidList(count),
                .bool => _ = try pointer.initBoolList(count),
                .int8 => _ = try pointer.initI8List(count),
                .int16 => _ = try pointer.initI16List(count),
                .int32 => _ = try pointer.initI32List(count),
                .int64 => _ = try pointer.initI64List(count),
                .uint8 => _ = try pointer.initU8List(count),
                .uint16, .@"enum" => _ = try pointer.initU16List(count),
                .uint32 => _ = try pointer.initU32List(count),
                .uint64 => _ = try pointer.initU64List(count),
                .float32 => _ = try pointer.initF32List(count),
                .float64 => _ = try pointer.initF64List(count),
                .@"struct" => {
                    if (try usesInlineStructElements(list_type)) {
                        const info = (try element.asStruct()).proto();
                        _ = try pointer.initStructList(count, info.data_word_count, info.pointer_count);
                    } else _ = try pointer.initPointerList(count);
                },
                else => _ = try pointer.initPointerList(count),
            }
            return .{ .type = list_type, .pointer = pointer };
        }

        pub fn len(self: Builder) !u32 {
            const element = try (try self.type.listElement()).resolved();
            return switch (try element.proto()) {
                .void => (try self.pointer.getVoidList()).len(),
                .bool => (try self.pointer.getBoolList()).len(),
                .int8 => (try self.pointer.getI8List()).len(),
                .int16 => (try self.pointer.getI16List()).len(),
                .int32 => (try self.pointer.getI32List()).len(),
                .int64 => (try self.pointer.getI64List()).len(),
                .uint8 => (try self.pointer.getU8List()).len(),
                .uint16, .@"enum" => (try self.pointer.getU16List()).len(),
                .uint32 => (try self.pointer.getU32List()).len(),
                .uint64 => (try self.pointer.getU64List()).len(),
                .float32 => (try self.pointer.getF32List()).len(),
                .float64 => (try self.pointer.getF64List()).len(),
                .@"struct" => if (try usesInlineStructElements(self.type))
                    try structListLength(self.pointer)
                else
                    (try self.pointer.getPointerList()).len(),
                else => (try self.pointer.getPointerList()).len(),
            };
        }

        pub fn set(self: Builder, index: u32, value: Value) anyerror!void {
            if (index >= try self.len()) return error.IndexOutOfBounds;
            const element = try (try self.type.listElement()).resolved();
            try requireValue(element, value);
            switch (try element.proto()) {
                .void => {},
                .bool => try (try self.pointer.getBoolList()).set(index, value.bool),
                .int8 => try (try self.pointer.getI8List()).set(index, value.int8),
                .int16 => try (try self.pointer.getI16List()).set(index, value.int16),
                .int32 => try (try self.pointer.getI32List()).set(index, value.int32),
                .int64 => try (try self.pointer.getI64List()).set(index, value.int64),
                .uint8 => try (try self.pointer.getU8List()).set(index, value.uint8),
                .uint16 => try (try self.pointer.getU16List()).set(index, value.uint16),
                .uint32 => try (try self.pointer.getU32List()).set(index, value.uint32),
                .uint64 => try (try self.pointer.getU64List()).set(index, value.uint64),
                .float32 => try (try self.pointer.getF32List()).set(index, value.float32),
                .float64 => try (try self.pointer.getF64List()).set(index, value.float64),
                .@"enum" => try (try self.pointer.getU16List()).set(index, value.@"enum".ordinal),
                .@"struct" => {
                    if (!try usesInlineStructElements(self.type))
                        return writePointer(element, try self.elementPointer(index), value);
                    const info = (try element.asStruct()).proto();
                    const source = value.@"struct".reader;
                    _ = try ensureStructList(
                        self.pointer,
                        @max(info.data_word_count, structDataWords(source)),
                        @max(info.pointer_count, source.pointer_count),
                        .{ .index = index, .reader = source },
                    );
                },
                else => try writePointer(element, try self.elementPointer(index), value),
            }
        }

        pub fn getStruct(self: Builder, index: u32) !DynamicStruct.Builder {
            const element = try (try self.type.listElement()).resolved();
            const struct_schema = try element.asStruct();
            if (index >= try self.len()) return error.IndexOutOfBounds;
            const info = struct_schema.proto();
            if (!try usesInlineStructElements(self.type)) return .{
                .schema = struct_schema,
                .builder = try writableStruct(try self.elementPointer(index), info.data_word_count, info.pointer_count),
            };
            const list = try ensureStructList(self.pointer, info.data_word_count, info.pointer_count, null);
            return .{ .schema = struct_schema, .builder = try list.get(index) };
        }

        pub fn initStruct(self: Builder, index: u32) anyerror!DynamicStruct.Builder {
            if (!try usesInlineStructElements(self.type)) {
                const struct_schema = try (try self.type.listElement()).asStruct();
                const info = struct_schema.proto();
                return .{
                    .schema = struct_schema,
                    .builder = try (try self.elementPointer(index)).initStruct(info.data_word_count, info.pointer_count),
                };
            }
            const result = try self.getStruct(index);
            // List elements are already allocated; initialize their complete
            // physical storage so previous unknown fields are cleared as well.
            try zeroStruct(result.builder);
            return result;
        }

        pub fn initList(self: Builder, index: u32, count: u32) !Builder {
            const element = try (try self.type.listElement()).resolved();
            _ = try element.listElement();
            return Builder.init(element, try self.elementPointer(index), count);
        }

        pub fn getList(self: Builder, index: u32) !Builder {
            const element = try (try self.type.listElement()).resolved();
            _ = try element.listElement();
            return .{ .type = element, .pointer = try self.elementPointer(index) };
        }

        fn elementPointer(self: Builder, index: u32) !message.AnyPointerBuilder {
            const list = try self.pointer.getPointerList();
            if (index >= list.len()) return error.IndexOutOfBounds;
            return .{
                .builder = list.builder,
                .segment_id = list.segment_id,
                .pointer_pos = list.elements_offset + @as(usize, index) * 8,
            };
        }
    };
};

// List(T) is physically a pointer list even when T resolves to a struct.
// Only a struct named directly in the list's type expression uses composite
// elements. Retain this distinction through nested lists and inherited brands.
fn usesInlineStructElements(list_type: meta.Type) !bool {
    return (try list_type.listElement()).cursor.expression.type == .@"struct";
}

fn writableStruct(pointer: message.AnyPointerBuilder, data_words: u16, pointer_words: u16) !message.StructBuilder {
    if (pointer.segment_id >= pointer.builder.segments.items.len) return error.InvalidSegmentId;
    const segment = pointer.builder.segments.items[pointer.segment_id].items;
    if (pointer.pointer_pos > segment.len or segment.len - pointer.pointer_pos < 8) return error.OutOfBounds;
    if (std.mem.readInt(u64, segment[pointer.pointer_pos..][0..8], .little) == 0)
        return pointer.initStruct(data_words, pointer_words);
    const existing = try pointer.getStruct();
    if (existing.data_size < data_words or existing.pointer_count < pointer_words)
        return growStruct(pointer, data_words, pointer_words);
    return existing;
}

fn checkFieldOwner(owner: meta.StructSchema, field: meta.Field) !void {
    if (owner.schema.node != field.parent.schema.node) return error.TypeMismatch;
    if (!try sameStructSchema(owner, field.parent, 0)) return error.TypeMismatch;
}

fn defaultBool(slot: schema.FieldSlot) bool {
    const value = slot.default_value orelse return false;
    return if (value == .bool) value.bool else false;
}

fn defaultBits(comptime T: type, slot: schema.FieldSlot) T {
    const value = slot.default_value orelse return 0;
    return switch (value) {
        .int8 => |v| @truncate(@as(u64, @as(u8, @bitCast(v)))),
        .int16 => |v| @truncate(@as(u64, @as(u16, @bitCast(v)))),
        .int32 => |v| @truncate(@as(u64, @as(u32, @bitCast(v)))),
        .int64 => |v| @truncate(@as(u64, @bitCast(v))),
        .uint8 => |v| @truncate(@as(u64, v)),
        .uint16 => |v| @truncate(@as(u64, v)),
        .uint32 => |v| @truncate(@as(u64, v)),
        .uint64 => |v| @truncate(v),
        .float32 => |v| @truncate(@as(u64, @as(u32, @bitCast(v)))),
        .float64 => |v| @truncate(@as(u64, @bitCast(v))),
        .@"enum" => |v| @truncate(@as(u64, v)),
        else => 0,
    };
}

fn readPointer(typ: meta.Type, pointer: message.AnyPointerReader) !Value {
    return switch (try typ.proto()) {
        .text => .{ .text = try pointer.getTextStrict() },
        .data => .{ .data = try pointer.getData() },
        .@"struct" => .{ .@"struct" = .{ .schema = try typ.asStruct(), .reader = try pointer.getStruct() } },
        .list => .{ .list = .{ .type = typ, .reader = try message.AnyListReader.wrap(pointer) } },
        .any_pointer => blk: {
            try requirePointerConstraint(typ, pointer);
            break :blk .{ .any_pointer = pointer };
        },
        .interface => .{ .capability = if (pointer.isNull()) null else try pointer.getCapability() },
        else => error.TypeMismatch,
    };
}

fn requireValue(typ: meta.Type, value: Value) !void {
    const expected = try typ.proto();
    const valid = switch (expected) {
        .void => value == .void,
        .bool => value == .bool,
        .int8 => value == .int8,
        .int16 => value == .int16,
        .int32 => value == .int32,
        .int64 => value == .int64,
        .uint8 => value == .uint8,
        .uint16 => value == .uint16,
        .uint32 => value == .uint32,
        .uint64 => value == .uint64,
        .float32 => value == .float32,
        .float64 => value == .float64,
        .text => value == .text,
        .data => value == .data,
        .@"enum" => |named| value == .@"enum" and value.@"enum".schema.schema.id() == named.type_id,
        .@"struct" => value == .@"struct" and try sameStructSchema(try typ.asStruct(), value.@"struct".schema, 0),
        .list => value == .list and try sameType(typ, value.list.type, 0),
        .any_pointer => value == .any_pointer,
        .interface => value == .capability,
    };
    if (!valid) return error.TypeMismatch;
    if (value == .text and !std.unicode.utf8ValidateSlice(value.text)) return error.InvalidUtf8;
    if (value == .any_pointer) try requirePointerConstraint(typ, value.any_pointer);
}

fn sameType(a: meta.Type, b: meta.Type, depth: usize) anyerror!bool {
    if (depth >= 64) return error.InvalidSchema;
    const left = try (try a.resolved()).proto();
    const right = try (try b.resolved()).proto();
    if (std.meta.activeTag(left) != std.meta.activeTag(right)) return false;
    return switch (left) {
        .list => (try usesInlineStructElements(a)) == (try usesInlineStructElements(b)) and
            try sameType(try a.listElement(), try b.listElement(), depth + 1),
        .@"struct" => try sameStructSchema(try a.asStruct(), try b.asStruct(), depth + 1),
        .@"enum" => |named| named.type_id == right.@"enum".type_id,
        .interface => |named| named.type_id == right.interface.type_id,
        .any_pointer => blk: {
            const left_metadata = (try a.resolved()).cursor.expression.metadata;
            const right_metadata = (try b.resolved()).cursor.expression.metadata;
            const left_constraint = pointerConstraint(left_metadata);
            const right_constraint = pointerConstraint(right_metadata);
            break :blk left_constraint == right_constraint;
        },
        else => true,
    };
}

/// Compare concrete generic bindings rather than merely the declaration ID.
/// The type parameters of lexical parents also affect nested declarations.
/// Comparing parameters avoids walking recursive pointer fields such as a
/// linked-list node's next pointer.
fn sameStructSchema(a: meta.StructSchema, b: meta.StructSchema, depth: usize) anyerror!bool {
    if (depth >= 64) return error.InvalidSchema;
    if (a.schema.id() != b.schema.id()) return false;
    var node = a.schema.node;
    var scope_depth: usize = 0;
    while (true) {
        if (scope_depth >= 64) return error.InvalidSchema;
        scope_depth += 1;
        for (node.parameters, 0..) |_, index| {
            const expression = schema.TypeExpression{
                .type = .any_pointer,
                .metadata = .{ .any_pointer = .{ .parameter = .{ .scope_id = node.id, .parameter_index = @intCast(index) } } },
            };
            const left = meta.Type{ .registry = a.schema.registry, .resolver = a.resolver, .cursor = a.resolver.cursor(expression) };
            const right = meta.Type{ .registry = b.schema.registry, .resolver = b.resolver, .cursor = b.resolver.cursor(expression) };
            if (!try sameType(left, right, depth + 1)) return false;
        }
        if (node.scope_id == 0) break;
        node = (try a.schema.registry.get(node.scope_id)).node;
    }
    return true;
}

fn pointerConstraint(metadata: schema.TypeMetadata) schema.TypeMetadata.AnyPointer.Unconstrained {
    if (metadata == .any_pointer and metadata.any_pointer == .unconstrained) return metadata.any_pointer.unconstrained;
    return .any_kind;
}

fn requirePointerConstraint(typ: meta.Type, pointer: message.AnyPointerReader) !void {
    if (pointer.isNull()) return;
    switch (pointerConstraint((try typ.resolved()).cursor.expression.metadata)) {
        .any_kind => {},
        .@"struct" => _ = pointer.getStruct() catch return error.TypeMismatch,
        .list => _ = message.AnyListReader.wrap(pointer) catch return error.TypeMismatch,
        .capability => _ = pointer.getCapability() catch return error.TypeMismatch,
    }
}

fn writePointer(typ: meta.Type, pointer: message.AnyPointerBuilder, value: Value) anyerror!void {
    try requireValue(typ, value);
    switch (value) {
        .text => |text| try pointer.setText(text),
        .data => |data| try pointer.setData(data),
        .@"struct" => |reader| {
            const info = (try typ.asStruct()).proto();
            const dest = try pointer.initStruct(@max(info.data_word_count, structDataWords(reader.reader)), @max(info.pointer_count, reader.reader.pointer_count));
            try copyStruct(reader.reader, dest);
        },
        .list => |list| try message.cloneAnyPointer(list.reader.raw(), pointer),
        .any_pointer => |source| try message.cloneAnyPointer(source, pointer),
        .capability => |capability| if (capability) |cap| try pointer.setCapability(cap) else try pointer.setNull(),
        else => return error.TypeMismatch,
    }
}

fn listPointer(list: message.AnyListReader, index: u32) !message.AnyPointerReader {
    const pointers = try list.getPointerList();
    if (index >= pointers.len()) return error.IndexOutOfBounds;
    const stride: usize = if (pointers.stride_bytes == 0) 8 else pointers.stride_bytes;
    const position = pointers.elements_offset + @as(usize, index) * stride;
    const bytes = pointers.message.segments[pointers.segment_id];
    if (position > bytes.len or bytes.len - position < 8) return error.OutOfBounds;
    return .{
        .message = pointers.message,
        .segment_id = pointers.segment_id,
        .pointer_pos = position,
        .pointer_word = std.mem.readInt(u64, bytes[position..][0..8], .little),
    };
}

fn copyStruct(source: message.StructReader, dest: message.StructBuilder) !void {
    // Copies the physical layout, preserving fields unknown to the schema.
    // Callers widen containers first. This guard also protects fixed-size
    // internal callers from truncation. Pointer targets are deep-copied.
    const data = source.getDataSection();
    if (data.len > @as(usize, dest.data_size) * 8 or source.pointer_count > dest.pointer_count) return error.StructSizeMismatch;
    try zeroStruct(dest);
    for (data, 0..) |byte, index| try dest.writeU8Strict(index, byte);
    for (0..source.pointer_count) |index| try message.cloneAnyPointer(try source.readAnyPointer(index), try dest.getAnyPointer(index));
}

fn structDataWords(source: message.StructReader) u16 {
    return @max(source.data_size, @as(u16, if (source.sub_word_data_bytes != 0) 1 else 0));
}

fn snapshotPointer(pointer: message.AnyPointerBuilder, snapshot: *const message.Message) !message.AnyPointerReader {
    if (pointer.segment_id >= snapshot.segments.len) return error.OutOfBounds;
    const segment = snapshot.segments[pointer.segment_id];
    if (pointer.pointer_pos > segment.len or segment.len - pointer.pointer_pos < 8) return error.OutOfBounds;
    return .{
        .message = snapshot,
        .segment_id = pointer.segment_id,
        .pointer_pos = pointer.pointer_pos,
        .pointer_word = std.mem.readInt(u64, segment[pointer.pointer_pos..][0..8], .little),
    };
}

fn readStructList(pointer: message.AnyPointerReader) !message.StructListReader {
    const list = try message.AnyListReader.wrap(pointer);
    // Packed bits cannot be upgraded into struct elements in the reference
    // format. Other primitive encodings expose their first data/pointer field.
    if (!pointer.isNull() and try list.elementSize() == 1) return error.TypeMismatch;
    return list.getStructList();
}

fn structListLength(pointer: message.AnyPointerBuilder) !u32 {
    // Ordinary composite lists and null pointers need no allocation. Reading
    // the length of an older primitive list must not replace its storage.
    if (pointer.getStructList()) |list| return list.len() else |_| {}
    const allocator = pointer.builder.allocator;
    const bytes = try pointer.builder.toBytes();
    defer allocator.free(bytes);
    var snapshot = try message.Message.initUnvalidated(allocator, bytes);
    defer snapshot.deinit();
    return (try readStructList(try snapshotPointer(pointer, &snapshot))).len();
}

const StructElementReplacement = struct { index: u32, reader: message.StructReader };

fn ensureStructList(pointer: message.AnyPointerBuilder, data_words: u16, pointer_words: u16, replacement: ?StructElementReplacement) !message.StructListBuilder {
    if (pointer.getStructList()) |list| {
        if (list.data_words >= data_words and list.pointer_words >= pointer_words) {
            if (replacement) |value| try copyStruct(value.reader, try list.get(value.index));
            return list;
        }
    } else |_| {}

    const allocator = pointer.builder.allocator;
    const bytes = try pointer.builder.toBytes();
    defer allocator.free(bytes);
    var snapshot = try message.Message.initUnvalidated(allocator, bytes);
    defer snapshot.deinit();
    const original = try snapshotPointer(pointer, &snapshot);
    const source = try readStructList(original);
    const old_data_words = @max(source.data_words, @as(u16, if (source.sub_word_data_bytes != 0) 1 else 0));

    // initStructList publishes the replacement pointer before copying targets.
    // Restore it on failure; the old list is untouched. Use a fresh segment
    // lookup because allocations can relocate both segment storage and indexes.
    errdefer std.mem.writeInt(
        u64,
        pointer.builder.segments.items[pointer.segment_id].items[pointer.pointer_pos..][0..8],
        original.pointer_word,
        .little,
    );
    const result = try pointer.initStructList(source.len(), @max(data_words, old_data_words), @max(pointer_words, source.pointer_words));
    if (old_data_words == 0 and source.pointer_words == 0) {
        // Void and zero-width structs have no payload to move. In particular,
        // their count can be large despite occupying only a pointer/tag word.
        if (replacement) |value| try copyStruct(value.reader, try result.get(value.index));
    } else {
        for (0..source.len()) |index| {
            const element = if (replacement != null and replacement.?.index == index)
                replacement.?.reader
            else
                try source.get(@intCast(index));
            try copyStruct(element, try result.get(@intCast(index)));
        }
    }
    return result;
}

fn growStruct(pointer: message.AnyPointerBuilder, data_words: u16, pointer_words: u16) !message.StructBuilder {
    // A snapshot keeps every source segment stable while destination allocation
    // may relocate the builder's segment buffers. This slow path runs only when
    // reopening an older, smaller physical struct with a newer schema.
    const allocator = pointer.builder.allocator;
    const bytes = try pointer.builder.toBytes();
    defer allocator.free(bytes);
    var snapshot = try message.Message.initUnvalidated(allocator, bytes);
    defer snapshot.deinit();
    const segment = snapshot.segments[pointer.segment_id];
    const source_pointer = message.AnyPointerReader{
        .message = &snapshot,
        .segment_id = pointer.segment_id,
        .pointer_pos = pointer.pointer_pos,
        .pointer_word = std.mem.readInt(u64, segment[pointer.pointer_pos..][0..8], .little),
    };
    const source = try source_pointer.getStruct();
    const result = try pointer.initStruct(@max(data_words, source.data_size), @max(pointer_words, source.pointer_count));
    try copyStruct(source, result);
    return result;
}

fn zeroStruct(dest: message.StructBuilder) !void {
    for (0..@as(usize, dest.data_size) * 8) |index| try dest.writeU8Strict(index, 0);
    for (0..dest.pointer_count) |index| try (try dest.getAnyPointer(index)).setNull();
}

fn clearGroup(dest: DynamicStruct.Builder, depth: usize) anyerror!void {
    if (depth >= 64) return error.SchemaRecursionLimitExceeded;
    for (dest.schema.proto().fields, 0..) |_, index| try dest.clearStorage(try dest.schema.fieldByIndex(index), depth + 1);
    const info = dest.schema.proto();
    if (info.discriminant_count != 0) try dest.builder.writeU16Strict(@as(usize, info.discriminant_offset) * 2, 0);
}

fn copyGroup(source: DynamicStruct.Reader, dest: DynamicStruct.Builder, depth: usize) anyerror!void {
    if (depth >= 64) return error.SchemaRecursionLimitExceeded;
    for (source.schema.proto().fields, 0..) |_, index| {
        const field = try source.schema.fieldByIndex(index);
        if (!source.isActive(field)) continue;
        if (field.proto().group != null) {
            const group_source = (try source.getField(field)).@"struct";
            const group_dest = try dest.initGroup(field.proto().name);
            try copyGroup(group_source, group_dest, depth + 1);
        } else try dest.set(field.proto().name, try source.getField(field));
    }
    if (source.whichDiscriminant()) |ordinal|
        try dest.builder.writeU16Strict(@as(usize, dest.schema.proto().discriminant_offset) * 2, ordinal);
}
