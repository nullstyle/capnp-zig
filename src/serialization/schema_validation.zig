const std = @import("std");
const message = @import("message.zig");
const schema = @import("schema.zig");
const type_resolver = @import("type_resolver.zig");

const no_discriminant: u16 = 0xffff;

/// Maximum schema recursion depth to prevent stack exhaustion from deeply nested schemas.
const max_schema_depth: usize = 128;

pub const ValidationOptions = struct {
    traversal_limit_words: usize = 8 * 1024 * 1024,
    nesting_limit: usize = 64,
    strict_text_termination: bool = false,
    require_struct_size: bool = false,
};

pub const CanonicalizeOptions = struct {
    traversal_limit_words: usize = 8 * 1024 * 1024,
    nesting_limit: usize = 64,
    strict_text_termination: bool = false,
    /// Null out a pointer field whose content equals its SCHEMA default before
    /// canonicalizing. This is a capnp-zig extension for schema-aware equality,
    /// NOT part of the spec's canonical form: the reference canonicalizer is
    /// schema-free and cannot know a written value equals the field default, so
    /// it keeps the pointer. Defaults to off so that our output is byte-exact
    /// with `capnp convert binary:canonical` — with it on, canonicalize-and-
    /// compare against any other implementation's output diverges precisely on
    /// default-valued fields (proven by differential test against capnp).
    omit_default_pointers: bool = false,
    validate: bool = true,
};

const Context = struct {
    allocator: std.mem.Allocator,
    nodes: []schema.Node,
};

/// Tracks recursion depth and visited node IDs to detect cycles and prevent stack exhaustion.
///
/// Cycle detection tracks which schema nodes have been visited along the current *group* path
/// (groups share the same physical struct data). When crossing a pointer boundary (struct
/// pointers, list elements), the visited set is reset because the message's pointer graph is
/// already bounded by the wire-level traversal limit -- revisiting the same *schema type* via
/// different *data instances* is legitimate (e.g. a linked list node pointing to another node
/// of the same type).
const RecursionGuard = struct {
    depth: usize = 0,
    /// Indices of visited nodes (index into the nodes slice), used for cycle detection
    /// within a single struct's group hierarchy.
    visited_count: usize = 0,
    visited: [max_visited]usize = undefined,

    const max_visited: usize = 256;

    fn nodeIndex(nodes: []schema.Node, node: *const schema.Node) !usize {
        const node_addr = @intFromPtr(node);
        const base_addr = @intFromPtr(nodes.ptr);
        const node_size = @sizeOf(schema.Node);
        if (node_addr < base_addr) return error.InvalidSchema;
        const byte_offset = node_addr - base_addr;
        if (byte_offset % node_size != 0) return error.InvalidSchema;
        const idx = byte_offset / node_size;
        if (idx >= nodes.len) return error.InvalidSchema;
        return idx;
    }

    /// Enter a node via group recursion (same physical struct data). Tracks the node for
    /// cycle detection and increments depth.
    fn enterGroup(self: *const RecursionGuard, nodes: []schema.Node, node: *const schema.Node) !RecursionGuard {
        if (self.depth >= max_schema_depth) return error.SchemaRecursionLimitExceeded;

        const node_index = try nodeIndex(nodes, node);

        // Check for cycle: have we already visited this node in the current group chain?
        for (self.visited[0..self.visited_count]) |idx| {
            if (idx == node_index) return error.SchemaCycleDetected;
        }

        var result = self.*;
        result.depth = self.depth + 1;
        if (result.visited_count < max_visited) {
            result.visited[result.visited_count] = node_index;
            result.visited_count += 1;
        }
        return result;
    }

    /// Enter a node via a pointer boundary (struct pointer, list element). Resets the
    /// visited set since we are now traversing a new data instance; the same schema type
    /// appearing again is not a cycle.
    fn enterViaPointer(self: *const RecursionGuard, nodes: []schema.Node, node: *const schema.Node) !RecursionGuard {
        if (self.depth >= max_schema_depth) return error.SchemaRecursionLimitExceeded;

        const node_index = try nodeIndex(nodes, node);

        var result: RecursionGuard = .{
            .depth = self.depth + 1,
            .visited_count = 1,
            .visited = undefined,
        };
        result.visited[0] = node_index;
        return result;
    }

    /// Enter a new recursion level without cycle detection (for non-node recursion like list nesting).
    fn descend(self: *const RecursionGuard) !RecursionGuard {
        if (self.depth >= max_schema_depth) return error.SchemaRecursionLimitExceeded;
        var result = self.*;
        result.depth = self.depth + 1;
        return result;
    }
};

const StructSize = struct {
    data_words: u16,
    pointer_words: u16,
};

/// Validate schema-owned type metadata without consulting message contents.
///
/// In particular, every field of every group is checked even when the group is
/// an inactive union arm. Groups share their parent's lexical brand context,
/// so this traversal deliberately reuses one resolver while bounding and
/// cycle-checking the schema-only group graph.
fn preflightStructMetadata(
    nodes: []schema.Node,
    node: *const schema.Node,
    resolver: *const type_resolver.Resolver,
    guard: RecursionGuard,
) !void {
    const inner_guard = try guard.enterGroup(nodes, node);
    if (node.kind != .@"struct") return error.InvalidSchema;
    const struct_info = node.struct_node orelse return error.InvalidSchema;

    for (struct_info.fields) |field| {
        if (field.slot) |slot| {
            try resolver.validateExpression(resolver.cursor(.{
                .type = slot.type,
                .metadata = slot.type_metadata,
            }));
        } else if (field.group) |group| {
            const group_node = findNodeById(nodes, group.type_id) orelse return error.InvalidSchema;
            try preflightStructMetadata(nodes, group_node, resolver, inner_guard);
        }
    }
}

pub fn validateMessage(
    msg: *const message.Message,
    nodes: []schema.Node,
    root: *const schema.Node,
    options: ValidationOptions,
) !void {
    return validateMessageWithBrand(msg, nodes, root, .{}, options);
}

/// Validate a message while applying a concrete brand to its root schema node.
/// The parsed schema graph remains borrowed and no resolver allocation occurs.
pub fn validateMessageWithBrand(
    msg: *const message.Message,
    nodes: []schema.Node,
    root: *const schema.Node,
    root_brand: schema.Brand,
    options: ValidationOptions,
) !void {
    try msg.validate(.{ .traversal_limit_words = options.traversal_limit_words, .nesting_limit = options.nesting_limit });
    const root_reader = try msg.getRootStruct();
    const guard = RecursionGuard{};
    const resolver = try type_resolver.Resolver.init(nodes, root, root_brand);
    try preflightStructMetadata(nodes, root, &resolver, .{});
    try validateStruct(nodes, root, &resolver, root_reader, options, guard, true);
}

pub fn canonicalizeMessage(
    allocator: std.mem.Allocator,
    msg: *const message.Message,
    nodes: []schema.Node,
    root: *const schema.Node,
    options: CanonicalizeOptions,
) ![]u8 {
    return canonicalizeMessageWithBrand(allocator, msg, nodes, root, .{}, options);
}

/// Schema-driven canonicalization with an explicit root brand.
pub fn canonicalizeMessageWithBrand(
    allocator: std.mem.Allocator,
    msg: *const message.Message,
    nodes: []schema.Node,
    root: *const schema.Node,
    root_brand: schema.Brand,
    options: CanonicalizeOptions,
) ![]u8 {
    var builder = try canonicalizeToBuilder(allocator, msg, nodes, root, root_brand, options);
    defer builder.deinit();
    const bytes = try builder.toBytes();
    // `toBytes` hands back freshly allocated, caller-owned memory but declares
    // it `[]const u8`, while this function's signature promises `[]u8` (matching
    // `canonicalizeMessageFlat`). The buffer is genuinely mutable and genuinely
    // ours, so the const is an artifact of the callee's declaration, not of the
    // memory — re-typing it is sound and avoids copying the whole message.
    //
    // This mismatch made the function uncompilable, and nothing called it, so it
    // was never type-checked despite being part of the frozen Stable surface.
    // The regression test in tests/serialization/schema_validation_test.zig now
    // calls it so that cannot recur.
    return @constCast(bytes);
}

pub fn canonicalizeMessageFlat(
    allocator: std.mem.Allocator,
    msg: *const message.Message,
    nodes: []schema.Node,
    root: *const schema.Node,
    options: CanonicalizeOptions,
) ![]u8 {
    return canonicalizeMessageFlatWithBrand(allocator, msg, nodes, root, .{}, options);
}

/// Flat single-segment schema-driven canonicalization with an explicit root
/// brand. The returned bytes are caller-owned.
pub fn canonicalizeMessageFlatWithBrand(
    allocator: std.mem.Allocator,
    msg: *const message.Message,
    nodes: []schema.Node,
    root: *const schema.Node,
    root_brand: schema.Brand,
    options: CanonicalizeOptions,
) ![]u8 {
    var builder = try canonicalizeToBuilder(allocator, msg, nodes, root, root_brand, options);
    defer builder.deinit();
    if (builder.segments.items.len == 0) return allocator.alloc(u8, 0);
    if (builder.segments.items.len != 1) return error.NonCanonicalSegments;
    const segment = builder.segments.items[0].items;
    const out = try allocator.alloc(u8, segment.len);
    std.mem.copyForwards(u8, out, segment);
    return out;
}

fn canonicalizeToBuilder(
    allocator: std.mem.Allocator,
    msg: *const message.Message,
    nodes: []schema.Node,
    root: *const schema.Node,
    root_brand: schema.Brand,
    options: CanonicalizeOptions,
) !message.MessageBuilder {
    const resolver = try type_resolver.Resolver.init(nodes, root, root_brand);
    try preflightStructMetadata(nodes, root, &resolver, .{});

    if (options.validate) {
        try validateMessageWithBrand(msg, nodes, root, root_brand, .{
            .traversal_limit_words = options.traversal_limit_words,
            .nesting_limit = options.nesting_limit,
            .strict_text_termination = options.strict_text_termination,
        });
    }

    var builder = message.MessageBuilder.init(allocator);
    errdefer builder.deinit();

    if (root.kind != .@"struct") return error.InvalidSchema;

    const root_reader = try msg.getRootStruct();
    const ctx = Context{ .allocator = allocator, .nodes = nodes };
    const guard = RecursionGuard{};
    const root_size = try canonicalStructSize(&ctx, root, &resolver, root_reader, options, guard, true);
    const root_builder = try builder.allocateStruct(root_size.data_words, root_size.pointer_words);
    try canonicalizeStructInto(&ctx, root, &resolver, root_reader, root_builder, options, guard, true);

    return builder;
}

fn canonicalStructSize(
    ctx: *const Context,
    node: *const schema.Node,
    resolver: *const type_resolver.Resolver,
    reader: message.StructReader,
    options: CanonicalizeOptions,
    guard: RecursionGuard,
    via_pointer: bool,
) anyerror!StructSize {
    const inner_guard = if (via_pointer)
        try guard.enterViaPointer(ctx.nodes, node)
    else
        try guard.enterGroup(ctx.nodes, node);
    if (node.kind != .@"struct") return error.InvalidSchema;
    const struct_info = node.struct_node orelse return error.InvalidSchema;

    var max_data_word: isize = -1;
    var max_pointer_index: isize = -1;

    var discriminant_value: ?u16 = null;
    if (struct_info.discriminant_count > 0) {
        const offset_bytes = try discriminantByteOffset(struct_info.discriminant_offset);
        const value = reader.readUnionDiscriminant(offset_bytes);
        discriminant_value = value;
        if (value != 0) {
            const word_index = @as(isize, @intCast(offset_bytes / 8));
            if (word_index > max_data_word) max_data_word = word_index;
        }
    }

    for (struct_info.fields) |field| {
        if (field.slot == null and field.group == null) continue;

        const in_union = field.discriminant_value != no_discriminant;
        if (in_union and discriminant_value != null and field.discriminant_value != discriminant_value.?) {
            continue;
        }

        if (field.slot) |slot| {
            const cursor = resolver.cursor(.{ .type = slot.type, .metadata = slot.type_metadata });
            try resolver.validateExpression(cursor);
            const resolved = try resolver.resolve(cursor);
            const effective_type = if (resolved.unbound) slot.type else resolved.cursor.expression.type;
            const byte_offset = try dataByteOffset(effective_type, slot.offset);
            switch (effective_type) {
                .void => {},
                .bool => {
                    const bit_offset: u3 = @intCast(slot.offset % 8);
                    if (reader.readBool(byte_offset, bit_offset)) {
                        updateMaxDataWord(&max_data_word, byte_offset, 1);
                    }
                },
                .int8, .uint8 => {
                    if (reader.readU8(byte_offset) != 0) updateMaxDataWord(&max_data_word, byte_offset, 1);
                },
                .int16, .uint16, .@"enum" => {
                    if (reader.readU16(byte_offset) != 0) updateMaxDataWord(&max_data_word, byte_offset, 2);
                },
                .int32, .uint32, .float32 => {
                    if (reader.readU32(byte_offset) != 0) updateMaxDataWord(&max_data_word, byte_offset, 4);
                },
                .int64, .uint64, .float64 => {
                    if (reader.readU64(byte_offset) != 0) updateMaxDataWord(&max_data_word, byte_offset, 8);
                },
                .text, .data, .list, .@"struct", .any_pointer, .interface => {
                    if (try pointerShouldEmit(ctx, reader, slot, options)) {
                        const index = @as(isize, @intCast(slot.offset));
                        if (index > max_pointer_index) max_pointer_index = index;
                    }
                },
            }
        } else if (field.group) |group| {
            const group_node = findNodeById(ctx.nodes, group.type_id) orelse return error.InvalidSchema;
            const group_size = try canonicalStructSize(ctx, group_node, resolver, reader, options, inner_guard, false);
            if (group_size.data_words > 0) {
                const group_max = @as(isize, @intCast(group_size.data_words - 1));
                if (group_max > max_data_word) max_data_word = group_max;
            }
            if (group_size.pointer_words > 0) {
                const group_max = @as(isize, @intCast(group_size.pointer_words - 1));
                if (group_max > max_pointer_index) max_pointer_index = group_max;
            }
        }
    }

    const data_words: u16 = if (max_data_word < 0) 0 else blk: {
        const word_count = max_data_word + 1;
        if (word_count > std.math.maxInt(u16)) return error.InvalidSchema;
        break :blk @intCast(word_count);
    };
    const pointer_words: u16 = if (max_pointer_index < 0) 0 else blk: {
        const word_count = max_pointer_index + 1;
        if (word_count > std.math.maxInt(u16)) return error.InvalidSchema;
        break :blk @intCast(word_count);
    };
    return .{ .data_words = data_words, .pointer_words = pointer_words };
}

fn updateMaxDataWord(max_data_word: *isize, byte_offset: u32, byte_len: usize) void {
    if (byte_len == 0) return;
    const end_byte = @as(usize, byte_offset) + byte_len - 1;
    const word_index = @as(isize, @intCast(end_byte / 8));
    if (word_index > max_data_word.*) max_data_word.* = word_index;
}

fn pointerShouldEmit(
    ctx: *const Context,
    reader: message.StructReader,
    slot: schema.FieldSlot,
    options: CanonicalizeOptions,
) anyerror!bool {
    const ptr = getPointer(reader, slot.offset) orelse return false;
    if (ptr.word == 0) return false;

    if (options.omit_default_pointers) {
        const any = message.AnyPointerReader{
            .message = reader.message,
            .segment_id = reader.segment_id,
            .pointer_pos = ptr.pos,
            .pointer_word = ptr.word,
        };
        if (try pointerEqualsDefault(ctx, slot, any)) return false;
    }

    return true;
}

fn validateStruct(
    nodes: []schema.Node,
    node: *const schema.Node,
    resolver: *const type_resolver.Resolver,
    reader: message.StructReader,
    options: ValidationOptions,
    guard: RecursionGuard,
    via_pointer: bool,
) anyerror!void {
    const inner_guard = if (via_pointer)
        try guard.enterViaPointer(nodes, node)
    else
        try guard.enterGroup(nodes, node);
    if (node.kind != .@"struct") return error.InvalidSchema;
    const struct_info = node.struct_node orelse return error.InvalidSchema;

    if (options.require_struct_size) {
        if (reader.data_size < struct_info.data_word_count or reader.pointer_count < struct_info.pointer_count) {
            return error.StructSizeTooSmall;
        }
    }

    var discriminant_value: ?u16 = null;
    if (struct_info.discriminant_count > 0) {
        const offset_bytes = try discriminantByteOffset(struct_info.discriminant_offset);
        discriminant_value = reader.readUnionDiscriminant(offset_bytes);
    }

    for (struct_info.fields) |field| {
        if (field.slot == null and field.group == null) continue;

        const in_union = field.discriminant_value != no_discriminant;
        if (in_union and discriminant_value != null and field.discriminant_value != discriminant_value.?) {
            continue;
        }

        if (field.slot) |slot| {
            try validateSlot(nodes, resolver, reader, slot, options, inner_guard);
        } else if (field.group) |group| {
            const group_node = findNodeById(nodes, group.type_id) orelse return error.InvalidSchema;
            try validateStruct(nodes, group_node, resolver, reader, options, inner_guard, false);
        }
    }
}

fn validateSlot(
    nodes: []schema.Node,
    resolver: *const type_resolver.Resolver,
    reader: message.StructReader,
    slot: schema.FieldSlot,
    options: ValidationOptions,
    guard: RecursionGuard,
) anyerror!void {
    const initial = resolver.cursor(.{ .type = slot.type, .metadata = slot.type_metadata });
    try resolver.validateExpression(initial);
    const resolution = try resolver.resolve(initial);
    if (resolution.unbound) return;
    const cursor = resolution.cursor;
    const byte_offset = try dataByteOffset(cursor.expression.type, slot.offset);
    switch (cursor.expression.type) {
        .void, .bool, .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64, .float32, .float64 => {},
        .@"enum" => |enum_info| {
            const raw = reader.readU16(byte_offset);
            const default_bits: u16 = if (slot.default_value) |dv| switch (dv) {
                .@"enum" => dv.@"enum",
                else => 0,
            } else 0;
            const actual = raw ^ default_bits;
            const enum_node = findNodeById(nodes, enum_info.type_id) orelse return error.InvalidSchema;
            const enum_def = enum_node.enum_node orelse return error.InvalidSchema;
            if (actual >= enum_def.enumerants.len) return error.InvalidEnumValue;
        },
        .text => try validateTextPointer(reader, slot.offset, options),
        .data => try validateDataPointer(reader, slot.offset),
        .list => try validateListPointer(nodes, resolver, reader, slot.offset, cursor, options, guard),
        .@"struct" => |struct_info| try validateStructPointer(nodes, resolver, reader, slot.offset, struct_info.type_id, cursor, options, guard),
        .interface => {
            const ptr = getPointer(reader, slot.offset) orelse return;
            if (ptr.word == 0) return;
            const any = message.AnyPointerReader{
                .message = reader.message,
                .segment_id = reader.segment_id,
                .pointer_pos = ptr.pos,
                .pointer_word = ptr.word,
            };
            _ = try any.getCapability();
        },
        .any_pointer => try validateConstrainedAnyPointer(reader, slot.offset, cursor),
    }
}

fn validateConstrainedAnyPointer(
    reader: message.StructReader,
    pointer_index: u32,
    cursor: type_resolver.Cursor,
) anyerror!void {
    const metadata = switch (cursor.expression.metadata) {
        .none => return,
        .any_pointer => |value| value,
        else => return error.InvalidSchema,
    };
    const kind = switch (metadata) {
        .unconstrained => |value| value,
        // Valid unresolved parameters retain erased compatibility.
        .parameter, .implicit_method_parameter => return,
    };
    if (kind == .any_kind) return;
    const ptr = getPointer(reader, pointer_index) orelse return;
    if (ptr.word == 0) return;
    const any = message.AnyPointerReader{
        .message = reader.message,
        .segment_id = reader.segment_id,
        .pointer_pos = ptr.pos,
        .pointer_word = ptr.word,
    };
    try validateConstrainedAnyPointerValue(any, cursor);
}

fn validateConstrainedAnyPointerValue(
    any: message.AnyPointerReader,
    cursor: type_resolver.Cursor,
) anyerror!void {
    const metadata = switch (cursor.expression.metadata) {
        .none => return,
        .any_pointer => |value| value,
        else => return error.InvalidSchema,
    };
    const kind = switch (metadata) {
        .unconstrained => |value| value,
        .parameter, .implicit_method_parameter => return,
    };
    switch (kind) {
        .any_kind => {},
        .@"struct" => _ = try any.getStruct(),
        .list => _ = try message.AnyListReader.wrap(any),
        .capability => _ = try any.getCapability(),
    }
}

fn validateTextPointer(reader: message.StructReader, pointer_index: u32, options: ValidationOptions) anyerror!void {
    const ptr = getPointer(reader, pointer_index) orelse return;
    if (ptr.word == 0) return;
    const list = try reader.message.resolveListPointer(reader.segment_id, ptr.pos, ptr.word);
    if (list.element_size != 2) return error.InvalidListElementSize;

    if (!options.strict_text_termination) return;

    if (list.element_count == 0) return error.InvalidTextPointer;
    const segment = reader.message.segments[list.segment_id];
    if (list.content_offset + list.element_count > segment.len) return error.OutOfBounds;
    const last = segment[list.content_offset + list.element_count - 1];
    if (last != 0) return error.InvalidTextPointer;
}

fn validateDataPointer(reader: message.StructReader, pointer_index: u32) anyerror!void {
    const ptr = getPointer(reader, pointer_index) orelse return;
    if (ptr.word == 0) return;
    const list = try reader.message.resolveListPointer(reader.segment_id, ptr.pos, ptr.word);
    if (list.element_size != 2) return error.InvalidListElementSize;
}

fn validateStructPointer(
    nodes: []schema.Node,
    resolver: *const type_resolver.Resolver,
    reader: message.StructReader,
    pointer_index: u32,
    type_id: schema.Id,
    cursor: type_resolver.Cursor,
    options: ValidationOptions,
    guard: RecursionGuard,
) anyerror!void {
    const ptr = getPointer(reader, pointer_index) orelse return;
    if (ptr.word == 0) return;
    const struct_node = findNodeById(nodes, type_id) orelse return error.InvalidSchema;
    const brand = try type_resolver.Resolver.namedBrand(cursor.expression);
    const child_resolver = try resolver.enterNamed(type_id, brand, cursor.context_depth);
    const struct_reader = try reader.message.resolveStructPointer(reader.segment_id, ptr.pos, ptr.word);
    try validateStruct(nodes, struct_node, &child_resolver, struct_reader, options, guard, true);
}

fn validateListPointer(
    nodes: []schema.Node,
    resolver: *const type_resolver.Resolver,
    reader: message.StructReader,
    pointer_index: u32,
    list_cursor: type_resolver.Cursor,
    options: ValidationOptions,
    guard: RecursionGuard,
) anyerror!void {
    const ptr = getPointer(reader, pointer_index) orelse return;
    if (ptr.word == 0) return;

    const element_initial = try type_resolver.Resolver.listElement(list_cursor);
    try resolver.validateExpression(element_initial);
    const element_resolution = try resolver.resolve(element_initial);
    if (element_resolution.unbound) {
        // An unbound list element is still physically a pointer list.
        const list = try reader.message.resolveListPointer(reader.segment_id, ptr.pos, ptr.word);
        if (list.element_size != 6) return error.InvalidListElementSize;
        return;
    }
    const element_cursor = element_resolution.cursor;
    const element_type = element_cursor.expression.type;

    // A concrete struct written directly in the schema uses inline-composite
    // layout. A generic `List(T)` is physically a pointer list even when T's
    // brand resolves to a struct; validate each erased pointer as that struct.
    if (element_type == .@"struct" and element_initial.expression.type == .@"struct") {
        const struct_info = element_type.@"struct";
        const struct_node = findNodeById(nodes, struct_info.type_id) orelse return error.InvalidSchema;
        const brand = try type_resolver.Resolver.namedBrand(element_cursor.expression);
        const child_resolver = try resolver.enterNamed(struct_info.type_id, brand, element_cursor.context_depth);
        const layout = try reader.message.resolveStructListPointer(reader.segment_id, ptr.pos, ptr.word);
        // The early-out keys on the stride, not on whole words: an upgraded
        // sub-word list has zero data/pointer *words* yet real elements, and
        // testing words here would skip validating every one of them.
        if (layout.element_count == 0 or layout.stride_bytes == 0) return;
        var idx: u32 = 0;
        while (idx < layout.element_count) : (idx += 1) {
            const element_reader = structListElementReader(reader.message, layout, idx);
            try validateStruct(nodes, struct_node, &child_resolver, element_reader, options, guard, true);
        }
        return;
    }

    const list = try reader.message.resolveListPointer(reader.segment_id, ptr.pos, ptr.word);
    const expected = elementSizeForType(element_type) orelse return error.InvalidSchema;
    if (list.element_size != expected) return error.InvalidListElementSize;

    switch (element_type) {
        .@"enum" => |enum_info| {
            const enum_node = findNodeById(nodes, enum_info.type_id) orelse return error.InvalidSchema;
            const enum_def = enum_node.enum_node orelse return error.InvalidSchema;
            const list_reader = message.U16ListReader{
                .message = reader.message,
                .segment_id = list.segment_id,
                .elements_offset = list.content_offset,
                .element_count = list.element_count,
            };
            var idx: u32 = 0;
            while (idx < list_reader.len()) : (idx += 1) {
                const value = try list_reader.get(idx);
                if (value >= enum_def.enumerants.len) return error.InvalidEnumValue;
            }
        },
        .text, .data, .list, .@"struct", .any_pointer, .interface => {
            if (list.element_size != 6) return error.InvalidListElementSize;
            try validatePointerList(nodes, resolver, reader.message, list, element_cursor, options, guard);
        },
        else => {},
    }
}

fn validatePointerList(
    nodes: []schema.Node,
    resolver: *const type_resolver.Resolver,
    msg: *const message.Message,
    list: message.Message.ResolvedListPointer,
    element_cursor: type_resolver.Cursor,
    options: ValidationOptions,
    guard: RecursionGuard,
) anyerror!void {
    if (list.element_count == 0) return;
    const inner_guard = try guard.descend();
    const segment = msg.segments[list.segment_id];
    var idx: u32 = 0;
    while (idx < list.element_count) : (idx += 1) {
        const pos = list.content_offset + @as(usize, idx) * 8;
        if (pos + 8 > segment.len) return error.OutOfBounds;
        const word = std.mem.readInt(u64, segment[pos..][0..8], .little);
        if (word == 0) continue;
        const any = message.AnyPointerReader{
            .message = msg,
            .segment_id = list.segment_id,
            .pointer_pos = pos,
            .pointer_word = word,
        };
        switch (element_cursor.expression.type) {
            .text => {
                _ = try any.getText();
                if (options.strict_text_termination) {
                    const list_info = try any.getList();
                    if (list_info.element_count == 0) return error.InvalidTextPointer;
                    const seg = msg.segments[list_info.segment_id];
                    if (list_info.content_offset + list_info.element_count > seg.len) return error.OutOfBounds;
                    const last = seg[list_info.content_offset + list_info.element_count - 1];
                    if (last != 0) return error.InvalidTextPointer;
                }
            },
            .data => {
                _ = try any.getData();
            },
            .@"struct" => |struct_info| {
                const struct_node = findNodeById(nodes, struct_info.type_id) orelse return error.InvalidSchema;
                const brand = try type_resolver.Resolver.namedBrand(element_cursor.expression);
                const child_resolver = try resolver.enterNamed(struct_info.type_id, brand, element_cursor.context_depth);
                const struct_reader = try any.getStruct();
                try validateStruct(nodes, struct_node, &child_resolver, struct_reader, options, inner_guard, true);
            },
            .list => {
                try validateListFromAnyPointer(nodes, resolver, any, element_cursor, options, inner_guard);
            },
            .any_pointer => try validateConstrainedAnyPointerValue(any, element_cursor),
            .interface => {
                _ = try any.getCapability();
            },
            else => {},
        }
    }
}

fn validateListFromAnyPointer(
    nodes: []schema.Node,
    resolver: *const type_resolver.Resolver,
    any: message.AnyPointerReader,
    list_cursor: type_resolver.Cursor,
    options: ValidationOptions,
    guard: RecursionGuard,
) anyerror!void {
    if (any.isNull()) return;
    const inner_guard = try guard.descend();
    const element_initial = try type_resolver.Resolver.listElement(list_cursor);
    try resolver.validateExpression(element_initial);
    const element_resolution = try resolver.resolve(element_initial);
    if (element_resolution.unbound) {
        const erased = try any.getList();
        if (erased.element_size != 6) return error.InvalidListElementSize;
        return;
    }
    const element_cursor = element_resolution.cursor;
    const element_type = element_cursor.expression.type;
    if (element_type == .@"struct" and element_initial.expression.type == .@"struct") {
        const struct_info = element_type.@"struct";
        const struct_node = findNodeById(nodes, struct_info.type_id) orelse return error.InvalidSchema;
        const brand = try type_resolver.Resolver.namedBrand(element_cursor.expression);
        const child_resolver = try resolver.enterNamed(struct_info.type_id, brand, element_cursor.context_depth);
        // Callers reach here only past an `isNull()` guard, so resolving the
        // pointer directly (rather than through `getInlineCompositeList`, whose
        // null-is-empty-list behavior does not exist here) cannot turn a null
        // pointer into an error.
        const layout = try any.message.resolveStructListPointer(any.segment_id, any.pointer_pos, any.pointer_word);
        if (layout.element_count == 0 or layout.stride_bytes == 0) return;
        var idx: u32 = 0;
        while (idx < layout.element_count) : (idx += 1) {
            const element_reader = structListElementReader(any.message, layout, idx);
            try validateStruct(nodes, struct_node, &child_resolver, element_reader, options, inner_guard, true);
        }
        return;
    }

    const list = try any.getList();
    const expected = elementSizeForType(element_type) orelse return error.InvalidSchema;
    if (list.element_size != expected) return error.InvalidListElementSize;

    switch (element_type) {
        .@"enum" => |enum_info| {
            const enum_node = findNodeById(nodes, enum_info.type_id) orelse return error.InvalidSchema;
            const enum_def = enum_node.enum_node orelse return error.InvalidSchema;
            const list_reader = message.U16ListReader{
                .message = any.message,
                .segment_id = list.segment_id,
                .elements_offset = list.content_offset,
                .element_count = list.element_count,
            };
            var idx: u32 = 0;
            while (idx < list_reader.len()) : (idx += 1) {
                const value = try list_reader.get(idx);
                if (value >= enum_def.enumerants.len) return error.InvalidEnumValue;
            }
        },
        .text, .data, .list, .@"struct", .any_pointer, .interface => {
            if (list.element_size != 6) return error.InvalidListElementSize;
            try validatePointerList(nodes, resolver, any.message, list, element_cursor, options, inner_guard);
        },
        else => {},
    }
}

fn canonicalizeStructInto(
    ctx: *const Context,
    node: *const schema.Node,
    resolver: *const type_resolver.Resolver,
    reader: message.StructReader,
    dest: message.StructBuilder,
    options: CanonicalizeOptions,
    guard: RecursionGuard,
    via_pointer: bool,
) anyerror!void {
    const inner_guard = if (via_pointer)
        try guard.enterViaPointer(ctx.nodes, node)
    else
        try guard.enterGroup(ctx.nodes, node);
    if (node.kind != .@"struct") return error.InvalidSchema;
    const struct_info = node.struct_node orelse return error.InvalidSchema;

    var discriminant_value: ?u16 = null;
    if (struct_info.discriminant_count > 0) {
        const offset_bytes = try discriminantByteOffset(struct_info.discriminant_offset);
        const value = reader.readUnionDiscriminant(offset_bytes);
        dest.writeUnionDiscriminant(offset_bytes, value);
        discriminant_value = value;
    }

    for (struct_info.fields) |field| {
        if (field.slot == null and field.group == null) continue;

        const in_union = field.discriminant_value != no_discriminant;
        if (in_union and discriminant_value != null and field.discriminant_value != discriminant_value.?) {
            continue;
        }

        if (field.slot) |slot| {
            try canonicalizeSlot(ctx, resolver, reader, dest, slot, options, inner_guard);
        } else if (field.group) |group| {
            const group_node = findNodeById(ctx.nodes, group.type_id) orelse return error.InvalidSchema;
            try canonicalizeStructInto(ctx, group_node, resolver, reader, dest, options, inner_guard, false);
        }
    }
}

fn canonicalizeSlot(
    ctx: *const Context,
    resolver: *const type_resolver.Resolver,
    reader: message.StructReader,
    dest: message.StructBuilder,
    slot: schema.FieldSlot,
    options: CanonicalizeOptions,
    guard: RecursionGuard,
) anyerror!void {
    const initial = resolver.cursor(.{ .type = slot.type, .metadata = slot.type_metadata });
    try resolver.validateExpression(initial);
    const resolution = try resolver.resolve(initial);
    const cursor = if (resolution.unbound) initial else resolution.cursor;
    const byte_offset = try dataByteOffset(cursor.expression.type, slot.offset);
    switch (cursor.expression.type) {
        .void => {},
        .bool => {
            const bit_offset: u3 = @intCast(slot.offset % 8);
            dest.writeBool(byte_offset, bit_offset, reader.readBool(byte_offset, bit_offset));
        },
        .int8, .uint8 => dest.writeU8(byte_offset, reader.readU8(byte_offset)),
        .int16, .uint16, .@"enum" => dest.writeU16(byte_offset, reader.readU16(byte_offset)),
        .int32, .uint32, .float32 => dest.writeU32(byte_offset, reader.readU32(byte_offset)),
        .int64, .uint64, .float64 => dest.writeU64(byte_offset, reader.readU64(byte_offset)),
        .text, .data, .list, .@"struct", .any_pointer, .interface => {
            try canonicalizePointerField(ctx, resolver, reader, dest, slot, cursor, options, guard);
        },
    }
}

fn canonicalizePointerField(
    ctx: *const Context,
    resolver: *const type_resolver.Resolver,
    reader: message.StructReader,
    dest: message.StructBuilder,
    slot: schema.FieldSlot,
    cursor: type_resolver.Cursor,
    options: CanonicalizeOptions,
    guard: RecursionGuard,
) anyerror!void {
    const pointer_index: u32 = slot.offset;
    if (pointer_index >= @as(u32, dest.pointer_count)) return;
    const ptr = getPointer(reader, pointer_index) orelse return;
    if (ptr.word == 0) return;

    const src_any = message.AnyPointerReader{
        .message = reader.message,
        .segment_id = reader.segment_id,
        .pointer_pos = ptr.pos,
        .pointer_word = ptr.word,
    };

    if (options.omit_default_pointers and try pointerEqualsDefault(ctx, slot, src_any)) {
        return;
    }

    const dest_any = try dest.getAnyPointer(pointer_index);
    try canonicalizePointerValue(ctx, resolver, cursor, src_any, dest_any, options, guard);
}

fn canonicalizePointerValue(
    ctx: *const Context,
    resolver: *const type_resolver.Resolver,
    cursor: type_resolver.Cursor,
    src_any: message.AnyPointerReader,
    dest_any: message.AnyPointerBuilder,
    options: CanonicalizeOptions,
    guard: RecursionGuard,
) anyerror!void {
    switch (cursor.expression.type) {
        .text => {
            const text = try src_any.getText();
            try dest_any.setText(text);
        },
        .data => {
            const data = try src_any.getData();
            try dest_any.setData(data);
        },
        .@"struct" => |struct_info| {
            const struct_node = findNodeById(ctx.nodes, struct_info.type_id) orelse return error.InvalidSchema;
            const brand = try type_resolver.Resolver.namedBrand(cursor.expression);
            const child_resolver = try resolver.enterNamed(struct_info.type_id, brand, cursor.context_depth);
            const src_struct = try src_any.getStruct();
            const size = try canonicalStructSize(ctx, struct_node, &child_resolver, src_struct, options, guard, true);
            const dest_struct = try dest_any.initStruct(size.data_words, size.pointer_words);
            try canonicalizeStructInto(ctx, struct_node, &child_resolver, src_struct, dest_struct, options, guard, true);
        },
        .list => {
            try canonicalizeListFromAnyPointer(ctx, resolver, cursor, src_any, dest_any, options, guard);
        },
        .any_pointer => {
            try validateConstrainedAnyPointerValue(src_any, cursor);
            try message.cloneAnyPointer(src_any, dest_any);
        },
        .interface => {
            const cap = try src_any.getCapability();
            try dest_any.setCapability(cap);
        },
        else => return error.InvalidPointer,
    }
}

fn canonicalizeListFromAnyPointer(
    ctx: *const Context,
    resolver: *const type_resolver.Resolver,
    list_cursor: type_resolver.Cursor,
    src_any: message.AnyPointerReader,
    dest_any: message.AnyPointerBuilder,
    options: CanonicalizeOptions,
    guard: RecursionGuard,
) anyerror!void {
    if (src_any.isNull()) return;
    const inner_guard = try guard.descend();

    const element_initial = try type_resolver.Resolver.listElement(list_cursor);
    try resolver.validateExpression(element_initial);
    const element_resolution = try resolver.resolve(element_initial);
    if (element_resolution.unbound) {
        try message.cloneAnyPointer(src_any, dest_any);
        return;
    }
    const element_cursor = element_resolution.cursor;
    const element_type = element_cursor.expression.type;

    if (element_type == .@"struct" and element_initial.expression.type == .@"struct") {
        const struct_info = element_type.@"struct";
        const struct_node = findNodeById(ctx.nodes, struct_info.type_id) orelse return error.InvalidSchema;
        const brand = try type_resolver.Resolver.namedBrand(element_cursor.expression);
        const child_resolver = try resolver.enterNamed(struct_info.type_id, brand, element_cursor.context_depth);
        // Reached only past an `isNull()` guard (see the validator above).
        // Canonicalizing an upgraded list re-emits it in the inline-composite
        // encoding, which is the only legal encoding for a struct list — so a
        // message carrying the old primitive-list form and one carrying the
        // evolved struct-list form canonicalize to identical bytes.
        const layout = try src_any.message.resolveStructListPointer(src_any.segment_id, src_any.pointer_pos, src_any.pointer_word);

        var max_data_words: u16 = 0;
        var max_pointer_words: u16 = 0;

        var idx: u32 = 0;
        while (idx < layout.element_count) : (idx += 1) {
            const src_struct = structListElementReader(src_any.message, layout, idx);
            const size = try canonicalStructSize(ctx, struct_node, &child_resolver, src_struct, options, inner_guard, true);
            if (size.data_words > max_data_words) max_data_words = size.data_words;
            if (size.pointer_words > max_pointer_words) max_pointer_words = size.pointer_words;
        }

        var out_list = try dest_any.initStructList(layout.element_count, max_data_words, max_pointer_words);
        if (layout.element_count == 0 or layout.stride_bytes == 0) return;

        idx = 0;
        while (idx < layout.element_count) : (idx += 1) {
            const src_struct = structListElementReader(src_any.message, layout, idx);
            const dest_struct = try out_list.get(idx);
            try canonicalizeStructInto(ctx, struct_node, &child_resolver, src_struct, dest_struct, options, inner_guard, true);
        }
        return;
    }

    const list = try src_any.getList();
    const expected = elementSizeForType(element_type) orelse return error.InvalidSchema;
    if (list.element_size != expected) return error.InvalidListElementSize;

    switch (element_type) {
        .void => {
            _ = try dest_any.initVoidList(list.element_count);
        },
        .bool => {
            var out = try dest_any.initBoolList(list.element_count);
            const src_list = message.BoolListReader{
                .message = src_any.message,
                .segment_id = list.segment_id,
                .elements_offset = list.content_offset,
                .element_count = list.element_count,
            };
            var idx: u32 = 0;
            while (idx < src_list.len()) : (idx += 1) {
                const value = try src_list.get(idx);
                try out.set(idx, value);
            }
        },
        .int8, .uint8 => {
            var out = try dest_any.initU8List(list.element_count);
            const src_list = message.U8ListReader{
                .message = src_any.message,
                .segment_id = list.segment_id,
                .elements_offset = list.content_offset,
                .element_count = list.element_count,
            };
            var idx: u32 = 0;
            while (idx < src_list.len()) : (idx += 1) {
                const value = try src_list.get(idx);
                try out.set(idx, value);
            }
        },
        .int16, .uint16, .@"enum" => {
            var out = try dest_any.initU16List(list.element_count);
            const src_list = message.U16ListReader{
                .message = src_any.message,
                .segment_id = list.segment_id,
                .elements_offset = list.content_offset,
                .element_count = list.element_count,
            };
            var idx: u32 = 0;
            while (idx < src_list.len()) : (idx += 1) {
                const value = try src_list.get(idx);
                try out.set(idx, value);
            }
        },
        .int32, .uint32 => {
            var out = try dest_any.initU32List(list.element_count);
            const src_list = message.U32ListReader{
                .message = src_any.message,
                .segment_id = list.segment_id,
                .elements_offset = list.content_offset,
                .element_count = list.element_count,
            };
            var idx: u32 = 0;
            while (idx < src_list.len()) : (idx += 1) {
                const value = try src_list.get(idx);
                try out.set(idx, value);
            }
        },
        .int64, .uint64 => {
            var out = try dest_any.initU64List(list.element_count);
            const src_list = message.U64ListReader{
                .message = src_any.message,
                .segment_id = list.segment_id,
                .elements_offset = list.content_offset,
                .element_count = list.element_count,
            };
            var idx: u32 = 0;
            while (idx < src_list.len()) : (idx += 1) {
                const value = try src_list.get(idx);
                try out.set(idx, value);
            }
        },
        .float32 => {
            var out = try dest_any.initF32List(list.element_count);
            const src_list = message.U32ListReader{
                .message = src_any.message,
                .segment_id = list.segment_id,
                .elements_offset = list.content_offset,
                .element_count = list.element_count,
            };
            var idx: u32 = 0;
            while (idx < src_list.len()) : (idx += 1) {
                const value = try src_list.get(idx);
                try out.set(idx, @bitCast(value));
            }
        },
        .float64 => {
            var out = try dest_any.initF64List(list.element_count);
            const src_list = message.U64ListReader{
                .message = src_any.message,
                .segment_id = list.segment_id,
                .elements_offset = list.content_offset,
                .element_count = list.element_count,
            };
            var idx: u32 = 0;
            while (idx < src_list.len()) : (idx += 1) {
                const value = try src_list.get(idx);
                try out.set(idx, @bitCast(value));
            }
        },
        .text, .data, .list, .@"struct", .any_pointer, .interface => {
            if (list.element_size != 6) return error.InvalidListElementSize;
            var out = try dest_any.initPointerList(list.element_count);
            var idx: u32 = 0;
            while (idx < list.element_count) : (idx += 1) {
                const ptr = try getListPointer(src_any.message, list, idx);
                if (ptr.word == 0) {
                    try out.setNull(idx);
                    continue;
                }
                const src_elem = message.AnyPointerReader{
                    .message = src_any.message,
                    .segment_id = list.segment_id,
                    .pointer_pos = ptr.pos,
                    .pointer_word = ptr.word,
                };

                const dest_elem = message.AnyPointerBuilder{
                    .builder = dest_any.builder,
                    .segment_id = out.segment_id,
                    .pointer_pos = out.elements_offset + @as(usize, idx) * 8,
                };

                switch (element_type) {
                    .text => {
                        const text = try src_elem.getText();
                        try dest_elem.setText(text);
                    },
                    .data => {
                        const data = try src_elem.getData();
                        try dest_elem.setData(data);
                    },
                    .@"struct" => |struct_info| {
                        const struct_node = findNodeById(ctx.nodes, struct_info.type_id) orelse return error.InvalidSchema;
                        const brand = try type_resolver.Resolver.namedBrand(element_cursor.expression);
                        const child_resolver = try resolver.enterNamed(struct_info.type_id, brand, element_cursor.context_depth);
                        const src_struct = try src_elem.getStruct();
                        const size = try canonicalStructSize(
                            ctx,
                            struct_node,
                            &child_resolver,
                            src_struct,
                            options,
                            inner_guard,
                            true,
                        );
                        const dest_struct = try dest_elem.initStruct(size.data_words, size.pointer_words);
                        try canonicalizeStructInto(ctx, struct_node, &child_resolver, src_struct, dest_struct, options, inner_guard, true);
                    },
                    .list => {
                        try canonicalizeListFromAnyPointer(ctx, resolver, element_cursor, src_elem, dest_elem, options, inner_guard);
                    },
                    .any_pointer => {
                        try validateConstrainedAnyPointerValue(src_elem, element_cursor);
                        try message.cloneAnyPointer(src_elem, dest_elem);
                    },
                    .interface => {
                        const cap = try src_elem.getCapability();
                        try dest_elem.setCapability(cap);
                    },
                    else => {},
                }
            }
        },
    }
}

fn pointerEqualsDefault(ctx: *const Context, slot: schema.FieldSlot, src_any: message.AnyPointerReader) anyerror!bool {
    const default_value = slot.default_value orelse return false;
    return switch (default_value) {
        .text => |value| blk: {
            const actual = try src_any.getText();
            break :blk std.mem.eql(u8, actual, value);
        },
        .data => |value| blk: {
            const actual = try src_any.getData();
            break :blk std.mem.eql(u8, actual, value);
        },
        .list => |value| blk: {
            const actual_bytes = try message.cloneAnyPointerToBytes(ctx.allocator, src_any);
            defer ctx.allocator.free(actual_bytes);
            break :blk std.mem.eql(u8, actual_bytes, value.message_bytes);
        },
        .@"struct" => |value| blk: {
            const actual_bytes = try message.cloneAnyPointerToBytes(ctx.allocator, src_any);
            defer ctx.allocator.free(actual_bytes);
            break :blk std.mem.eql(u8, actual_bytes, value.message_bytes);
        },
        .any_pointer => |value| blk: {
            const actual_bytes = try message.cloneAnyPointerToBytes(ctx.allocator, src_any);
            defer ctx.allocator.free(actual_bytes);
            break :blk std.mem.eql(u8, actual_bytes, value.message_bytes);
        },
        else => false,
    };
}

fn discriminantByteOffset(offset_words: u32) !usize {
    const bytes_u64 = @as(u64, offset_words) * 2;
    if (bytes_u64 > std.math.maxInt(usize)) return error.OffsetOverflow;
    return @as(usize, @intCast(bytes_u64));
}

fn dataByteOffset(typ: schema.Type, offset: u32) !u32 {
    return switch (typ) {
        .bool => offset / 8,
        .int8, .uint8 => offset,
        .int16, .uint16, .@"enum" => std.math.mul(u32, offset, 2) catch return error.OffsetOverflow,
        .int32, .uint32, .float32 => std.math.mul(u32, offset, 4) catch return error.OffsetOverflow,
        .int64, .uint64, .float64 => std.math.mul(u32, offset, 8) catch return error.OffsetOverflow,
        else => offset,
    };
}

fn elementSizeForType(typ: schema.Type) ?u3 {
    return switch (typ) {
        .void => 0,
        .bool => 1,
        .int8, .uint8 => 2,
        .int16, .uint16, .@"enum" => 3,
        .int32, .uint32, .float32 => 4,
        .int64, .uint64, .float64 => 5,
        .text, .data, .list, .@"struct", .any_pointer, .interface => 6,
    };
}

const PointerInfo = struct {
    pos: usize,
    word: u64,
};

fn getPointer(reader: message.StructReader, pointer_index: u32) ?PointerInfo {
    if (pointer_index >= reader.pointer_count) return null;
    const pointers = reader.getPointerSection();
    const pointer_offset = @as(usize, pointer_index) * 8;
    if (pointer_offset + 8 > pointers.len) return null;
    const word = std.mem.readInt(u64, pointers[pointer_offset..][0..8], .little);
    const pos = reader.offset + @as(usize, reader.data_size) * 8 + pointer_offset;
    return .{ .pos = pos, .word = word };
}

fn getListPointer(msg: *const message.Message, list: message.Message.ResolvedListPointer, index: u32) !PointerInfo {
    if (index >= list.element_count) return error.IndexOutOfBounds;
    const pos = list.content_offset + @as(usize, index) * 8;
    const segment = msg.segments[list.segment_id];
    if (pos + 8 > segment.len) return error.OutOfBounds;
    const word = std.mem.readInt(u64, segment[pos..][0..8], .little);
    return .{ .pos = pos, .word = word };
}

/// Build the element reader for index `idx` of a struct list being validated or
/// canonicalized, honouring a sub-word data section.
///
/// A list upgraded from element size C = 2/3/4 reports `data_bytes` of 1/2/4
/// with zero pointers, and its elements must keep that exact width — rounding
/// up to a word would read the following element's bytes.
fn structListElementReader(
    msg: anytype,
    layout: message.StructListLayout,
    idx: u32,
) message.StructReader {
    return .{
        .message = msg,
        .segment_id = layout.segment_id,
        .offset = layout.elements_offset + @as(usize, idx) * @as(usize, layout.stride_bytes),
        .data_size = @intCast(layout.data_bytes / 8),
        .pointer_count = layout.pointer_count,
        .sub_word_data_bytes = if (layout.data_bytes % 8 == 0) 0 else @intCast(layout.data_bytes),
    };
}

fn findNodeById(nodes: []schema.Node, id: schema.Id) ?*const schema.Node {
    for (nodes) |*node| {
        if (node.id == id) return node;
    }
    return null;
}

fn testNode(id: schema.Id) schema.Node {
    return .{
        .id = id,
        .display_name = "TestNode",
        .display_name_prefix_length = 0,
        .scope_id = id,
        .nested_nodes = &.{},
        .annotations = &.{},
        .kind = .@"struct",
        .struct_node = .{
            .data_word_count = 0,
            .pointer_count = 0,
            .preferred_list_encoding = .empty,
            .is_group = false,
            .discriminant_count = 0,
            .discriminant_offset = 0,
            .fields = &.{},
        },
        .enum_node = null,
        .interface_node = null,
        .const_node = null,
        .annotation_node = null,
    };
}

test "RecursionGuard detects group cycles and resets on pointer traversal" {
    var nodes = [_]schema.Node{
        testNode(1),
        testNode(2),
    };

    const base = RecursionGuard{};
    const first = try base.enterGroup(&nodes, &nodes[0]);
    const second = try first.enterGroup(&nodes, &nodes[1]);
    try std.testing.expectError(error.SchemaCycleDetected, second.enterGroup(&nodes, &nodes[0]));

    const via_pointer = try second.enterViaPointer(&nodes, &nodes[0]);
    try std.testing.expectEqual(@as(usize, second.depth + 1), via_pointer.depth);
    try std.testing.expectEqual(@as(usize, 1), via_pointer.visited_count);
    _ = try via_pointer.enterGroup(&nodes, &nodes[1]);
}

test "RecursionGuard enforces depth limit" {
    var nodes = [_]schema.Node{testNode(1)};
    const guard = RecursionGuard{
        .depth = max_schema_depth,
        .visited_count = 0,
        .visited = undefined,
    };
    try std.testing.expectError(error.SchemaRecursionLimitExceeded, guard.descend());
    try std.testing.expectError(error.SchemaRecursionLimitExceeded, guard.enterGroup(&nodes, &nodes[0]));
    try std.testing.expectError(error.SchemaRecursionLimitExceeded, guard.enterViaPointer(&nodes, &nodes[0]));
}
