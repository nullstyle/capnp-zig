const std = @import("std");
const log = std.log.scoped(.rpc_cap_table);
const message = @import("../../serialization/message.zig");
const protocol = @import("protocol.zig");
const promise_pipeline = @import("../common/promise_pipeline.zig");

/// An exported (local) capability referenced by ID.
pub const ExportCap = struct {
    id: u32,
};

/// An imported (remote) capability referenced by ID.
pub const ImportCap = struct {
    id: u32,
};

/// A capability reference resolved from a cap table entry to its
/// logical location: local export, remote import, promise pipeline, or absent.
pub const ResolvedCap = union(enum) {
    none,
    exported: ExportCap,
    imported: ImportCap,
    promised: protocol.PromisedAnswer,
};

/// A heap-owned copy of a `PromisedAnswer` (question ID + transform ops).
pub const OwnedPromisedAnswer = promise_pipeline.OwnedPromisedAnswer;

/// Tracks capability import/export state for an RPC connection.
///
/// Manages import reference counts, export ID allocation, promise-export
/// markers, and receiver-answer entries used for promise pipelining. Each
/// `Peer` owns one `CapTable`.
pub const max_table_size: u32 = 10_000;

pub const CapTable = struct {
    allocator: std.mem.Allocator,
    imports: std.AutoHashMap(u32, ImportEntry),
    promised_exports: std.AutoHashMap(u32, void),
    receiver_answers: std.AutoHashMap(u32, OwnedPromisedAnswer),
    next_export_id: u32 = 0,

    pub fn init(allocator: std.mem.Allocator) CapTable {
        return .{
            .allocator = allocator,
            .imports = std.AutoHashMap(u32, ImportEntry).init(allocator),
            .promised_exports = std.AutoHashMap(u32, void).init(allocator),
            .receiver_answers = std.AutoHashMap(u32, OwnedPromisedAnswer).init(allocator),
        };
    }

    pub fn deinit(self: *CapTable) void {
        self.imports.deinit();
        self.promised_exports.deinit();
        var answer_it = self.receiver_answers.valueIterator();
        while (answer_it.next()) |answer| {
            answer.deinit(self.allocator);
        }
        self.receiver_answers.deinit();
    }

    pub fn totalEntries(self: *const CapTable) u32 {
        return @as(u32, @intCast(self.imports.count())) +
            @as(u32, @intCast(self.promised_exports.count())) +
            @as(u32, @intCast(self.receiver_answers.count()));
    }

    /// Allocate a unique export ID that does not collide with any existing
    /// import, promise-export, or receiver-answer entry.
    pub fn allocExportId(self: *CapTable) error{CapTableFull}!u32 {
        return self.allocLocalCapId();
    }

    pub fn markExportPromise(self: *CapTable, export_id: u32) !void {
        try self.promised_exports.put(export_id, {});
    }

    pub fn clearExportPromise(self: *CapTable, export_id: u32) void {
        _ = self.promised_exports.remove(export_id);
    }

    pub fn isExportPromise(self: *const CapTable, export_id: u32) bool {
        return self.promised_exports.contains(export_id);
    }

    pub fn noteReceiverAnswer(self: *CapTable, promised: protocol.PromisedAnswer) !u32 {
        const id = try self.allocLocalCapId();
        var owned = try OwnedPromisedAnswer.fromPromised(self.allocator, promised);
        errdefer owned.deinit(self.allocator);
        try self.receiver_answers.put(id, owned);
        return id;
    }

    pub fn noteReceiverAnswerOps(
        self: *CapTable,
        question_id: u32,
        ops: []const protocol.PromisedAnswerOp,
    ) !u32 {
        const id = try self.allocLocalCapId();
        var owned = try OwnedPromisedAnswer.fromQuestionAndOps(self.allocator, question_id, ops);
        errdefer owned.deinit(self.allocator);
        try self.receiver_answers.put(id, owned);
        return id;
    }

    pub fn getReceiverAnswer(self: *const CapTable, cap_id: u32) ?OwnedPromisedAnswer {
        return self.receiver_answers.get(cap_id);
    }

    /// Record that a capability with `remote_id` was received from the remote
    /// peer. Increments the reference count if already known.
    pub fn noteImport(self: *CapTable, remote_id: u32) !void {
        var entry = try self.imports.getOrPut(remote_id);
        if (!entry.found_existing) {
            entry.value_ptr.* = .{ .ref_count = 1 };
        } else {
            entry.value_ptr.ref_count = std.math.add(u32, entry.value_ptr.ref_count, 1) catch return error.RefCountOverflow;
        }
    }

    /// Decrement the reference count for an imported capability.
    /// Returns true if the import was fully released (count reached zero).
    pub fn releaseImport(self: *CapTable, remote_id: u32) bool {
        var entry = self.imports.getEntry(remote_id) orelse return false;
        if (entry.value_ptr.ref_count > 1) {
            entry.value_ptr.ref_count -= 1;
            return false;
        }
        _ = self.imports.remove(remote_id);
        return true;
    }

    fn allocLocalCapId(self: *CapTable) error{CapTableFull}!u32 {
        const total = self.totalEntries();
        if (total >= max_table_size) {
            log.err("cap table full ({} entries)", .{total});
            return error.CapTableFull;
        }
        if (total >= max_table_size * 9 / 10) {
            log.warn("cap table near full: {}/{} entries", .{ total, max_table_size });
        }
        var iterations: u32 = 0;
        while (iterations < max_table_size + 1) : (iterations += 1) {
            const id = self.next_export_id;
            self.next_export_id +%= 1;
            if (self.imports.contains(id)) continue;
            if (self.promised_exports.contains(id)) continue;
            if (self.receiver_answers.contains(id)) continue;
            return id;
        }
        log.err("cap table full after exhaustive ID search", .{});
        return error.CapTableFull;
    }
};

const ImportEntry = struct {
    ref_count: u32,
};

/// Resolved capability table for an inbound Call or Return payload.
///
/// Created by decoding the cap descriptors from the wire message and
/// resolving each against the connection's `CapTable`. Tracks which entries
/// have been retained (referenced) so unused imports can be released.
pub const InboundCapTable = struct {
    allocator: std.mem.Allocator,
    entries: []ResolvedCap,
    retained: []bool,

    pub fn init(
        allocator: std.mem.Allocator,
        list_opt: ?message.StructListReader,
        table: *CapTable,
    ) !InboundCapTable {
        if (list_opt == null) {
            const entries = try allocator.alloc(ResolvedCap, 0);
            errdefer allocator.free(entries);
            const retained = try allocator.alloc(bool, 0);
            return .{
                .allocator = allocator,
                .entries = entries,
                .retained = retained,
            };
        }

        const list = list_opt.?;
        const count = list.len();
        var entries = try allocator.alloc(ResolvedCap, count);
        errdefer allocator.free(entries);
        const retained = try allocator.alloc(bool, count);
        errdefer allocator.free(retained);
        @memset(retained, false);
        var processed: u32 = 0;
        errdefer {
            // Roll back noteImport calls for already-processed entries
            for (entries[0..processed]) |entry| {
                if (entry == .imported) {
                    _ = table.releaseImport(entry.imported.id);
                }
            }
        }
        while (processed < count) : (processed += 1) {
            const reader = try list.get(processed);
            const descriptor = try protocol.CapDescriptor.fromReader(reader);
            entries[processed] = try resolveDescriptor(table, descriptor);
        }

        return .{
            .allocator = allocator,
            .entries = entries,
            .retained = retained,
        };
    }

    pub fn deinit(self: *InboundCapTable) void {
        self.allocator.free(self.entries);
        self.allocator.free(self.retained);
    }

    pub fn len(self: *const InboundCapTable) u32 {
        return @as(u32, @intCast(self.entries.len));
    }

    pub fn get(self: *const InboundCapTable, index: u32) !ResolvedCap {
        if (index >= self.entries.len) return error.CapabilityIndexOutOfBounds;
        return self.entries[index];
    }

    pub fn retainIndex(self: *InboundCapTable, index: u32) !void {
        if (index >= self.entries.len) return error.CapabilityIndexOutOfBounds;
        self.retained[index] = true;
    }

    pub fn retainCapability(self: *InboundCapTable, cap: message.Capability) !void {
        try self.retainIndex(cap.id);
    }

    pub fn isRetained(self: *const InboundCapTable, index: u32) bool {
        if (index >= self.retained.len) return false;
        return self.retained[index];
    }

    pub fn resolveCapability(self: *const InboundCapTable, cap: message.Capability) !ResolvedCap {
        return self.get(cap.id);
    }

    /// Create an independent deep copy that owns its own slices.
    pub fn clone(self: *const InboundCapTable) !InboundCapTable {
        const entries = try self.allocator.dupe(ResolvedCap, self.entries);
        errdefer self.allocator.free(entries);
        const retained = try self.allocator.dupe(bool, self.retained);
        return .{
            .allocator = self.allocator,
            .entries = entries,
            .retained = retained,
        };
    }
};

fn resolveDescriptor(table: *CapTable, descriptor: protocol.CapDescriptor) !ResolvedCap {
    return switch (descriptor.tag) {
        .none => .none,
        .senderHosted, .senderPromise => {
            const id = descriptor.id orelse return error.MissingCapDescriptorId;
            try table.noteImport(id);
            return .{ .imported = .{ .id = id } };
        },
        .receiverHosted => {
            const id = descriptor.id orelse return error.MissingCapDescriptorId;
            return .{ .exported = .{ .id = id } };
        },
        .receiverAnswer => {
            const promised = descriptor.promised_answer orelse return error.MissingPromisedAnswer;
            return .{ .promised = promised };
        },
        .thirdPartyHosted => {
            const third = descriptor.third_party orelse return error.MissingThirdPartyCapDescriptor;
            try table.noteImport(third.vine_id);
            return .{ .imported = .{ .id = third.vine_id } };
        },
    };
}

/// Resolve a cap descriptor from the wire format into a `ResolvedCap`,
/// noting any new imports in the cap table.
pub fn resolveCapDescriptor(table: *CapTable, descriptor: protocol.CapDescriptor) !ResolvedCap {
    return resolveDescriptor(table, descriptor);
}

const OutboundEntry = struct {
    tag: protocol.CapDescriptorTag,
    id: u32,
};

pub const CapEntryCallback = *const fn (ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) anyerror!void;

const OutboundCapTable = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(OutboundEntry),
    index_map: std.AutoHashMap(u64, u32),

    fn init(allocator: std.mem.Allocator) OutboundCapTable {
        return .{
            .allocator = allocator,
            .entries = std.ArrayList(OutboundEntry){},
            .index_map = std.AutoHashMap(u64, u32).init(allocator),
        };
    }

    fn deinit(self: *OutboundCapTable) void {
        self.entries.deinit(self.allocator);
        self.index_map.deinit();
    }

    fn key(tag: protocol.CapDescriptorTag, id: u32) u64 {
        return (@as(u64, @intFromEnum(tag)) << 32) | id;
    }

    fn indexFor(self: *OutboundCapTable, tag: protocol.CapDescriptorTag, id: u32) !u32 {
        const map_key = key(tag, id);
        if (self.index_map.get(map_key)) |existing| return existing;
        const index: u32 = @intCast(self.entries.items.len);
        try self.entries.append(self.allocator, .{ .tag = tag, .id = id });
        errdefer {
            _ = self.entries.pop();
        }
        try self.index_map.put(map_key, index);
        return index;
    }
};

fn makeCapabilityPointer(cap_id: u32) !u64 {
    return 3 | (@as(u64, cap_id) << 32);
}

fn decodeCapabilityPointer(pointer_word: u64) !u32 {
    if ((pointer_word & 0x3) != 3) return error.InvalidPointer;
    if (((pointer_word >> 2) & 0x3FFFFFFF) != 0) return error.InvalidPointer;
    return @as(u32, @truncate(pointer_word >> 32));
}

fn buildMessageView(
    allocator: std.mem.Allocator,
    builder: *message.MessageBuilder,
) !struct { msg: message.Message, segments: []const []const u8 } {
    const segment_count = builder.segments.items.len;
    const segs = try allocator.alloc([]const u8, segment_count);
    errdefer allocator.free(segs);
    for (builder.segments.items, 0..) |segment, i| {
        segs[i] = segment.items;
    }
    const msg = message.Message{
        .allocator = allocator,
        .segments = segs,
        .segments_owned = false,
        .backing_data = null,
    };
    return .{ .msg = msg, .segments = segs };
}

fn writePointerWord(builder: *message.MessageBuilder, segment_id: u32, pointer_pos: usize, word: u64) !void {
    if (segment_id >= builder.segments.items.len) return error.InvalidSegmentId;
    var segment = &builder.segments.items[segment_id];
    if (pointer_pos + 8 > segment.items.len) return error.OutOfBounds;
    std.mem.writeInt(u64, segment.items[pointer_pos..][0..8], word, .little);
}

fn classifyCap(table: *CapTable, cap_id: u32) protocol.CapDescriptorTag {
    if (table.receiver_answers.contains(cap_id)) return .receiverAnswer;
    if (table.imports.contains(cap_id)) return .receiverHosted;
    if (table.promised_exports.contains(cap_id)) return .senderPromise;
    return .senderHosted;
}

fn anyPointerReaderFromBuilder(
    msg: *const message.Message,
    builder: message.AnyPointerBuilder,
) message.AnyPointerReader {
    const segment = msg.segments[builder.segment_id];
    const pointer_word = std.mem.readInt(u64, segment[builder.pointer_pos..][0..8], .little);
    return .{
        .message = msg,
        .segment_id = builder.segment_id,
        .pointer_pos = builder.pointer_pos,
        .pointer_word = pointer_word,
    };
}

const max_traversal_depth: u32 = 64;

fn collectCapsFromPointer(
    outbound: *OutboundCapTable,
    table: *CapTable,
    msg: *const message.Message,
    builder: *message.MessageBuilder,
    segment_id: u32,
    pointer_pos: usize,
    pointer_word: u64,
    depth: u32,
) !void {
    if (depth == 0) return error.RecursionLimitExceeded;
    if (pointer_word == 0) return;
    const resolved = try msg.resolvePointer(segment_id, pointer_pos, pointer_word, 8);
    if (resolved.pointer_word == 0) return;
    const pointer_type = @as(u2, @truncate(resolved.pointer_word & 0x3));
    switch (pointer_type) {
        0 => {
            const struct_reader = try msg.resolveStructPointer(resolved.segment_id, resolved.pointer_pos, resolved.pointer_word);
            const pointer_base = struct_reader.offset + @as(usize, struct_reader.data_size) * 8;
            var idx: usize = 0;
            while (idx < struct_reader.pointer_count) : (idx += 1) {
                const pos = pointer_base + idx * 8;
                const word = std.mem.readInt(u64, msg.segments[struct_reader.segment_id][pos..][0..8], .little);
                try collectCapsFromPointer(outbound, table, msg, builder, struct_reader.segment_id, pos, word, depth - 1);
            }
        },
        1 => {
            const list = try msg.resolveListPointer(resolved.segment_id, resolved.pointer_pos, resolved.pointer_word);
            if (list.element_size == 6) {
                const segment = msg.segments[list.segment_id];
                var idx: u32 = 0;
                while (idx < list.element_count) : (idx += 1) {
                    const pos = list.content_offset + @as(usize, idx) * 8;
                    if (pos + 8 > segment.len) return error.OutOfBounds;
                    const word = std.mem.readInt(u64, segment[pos..][0..8], .little);
                    try collectCapsFromPointer(outbound, table, msg, builder, list.segment_id, pos, word, depth - 1);
                }
            } else if (list.element_size == 7) {
                const inline_list = try msg.resolveInlineCompositeList(resolved.segment_id, resolved.pointer_pos, resolved.pointer_word);
                const stride = (@as(usize, inline_list.data_words) + @as(usize, inline_list.pointer_words)) * 8;
                var idx: u32 = 0;
                while (idx < inline_list.element_count) : (idx += 1) {
                    const element_offset = inline_list.elements_offset + @as(usize, idx) * stride;
                    const pointer_base = element_offset + @as(usize, inline_list.data_words) * 8;
                    var pidx: usize = 0;
                    while (pidx < inline_list.pointer_words) : (pidx += 1) {
                        const pos = pointer_base + pidx * 8;
                        const word = std.mem.readInt(u64, msg.segments[inline_list.segment_id][pos..][0..8], .little);
                        try collectCapsFromPointer(outbound, table, msg, builder, inline_list.segment_id, pos, word, depth - 1);
                    }
                }
            }
        },
        3 => {
            const cap_id = try decodeCapabilityPointer(resolved.pointer_word);
            const tag = classifyCap(table, cap_id);
            const index = try outbound.indexFor(tag, cap_id);
            const new_word = try makeCapabilityPointer(index);
            try writePointerWord(builder, resolved.segment_id, resolved.pointer_pos, new_word);
        },
        else => return error.InvalidPointer,
    }
}

/// Encode capability descriptors into the outbound payload's cap table.
///
/// **Known limitation:** Receiver-answer entries referenced by the payload
/// are permanently removed from the `CapTable` after encoding. If the
/// caller encounters an error after this function returns but before the
/// encoded message is sent on the wire, those receiver-answer entries will
/// be lost and cannot be recovered. Callers must ensure the encoded message
/// is sent or accept that the receiver-answer state may be irrecoverable.
fn encodePayloadCaps(
    table: *CapTable,
    payload: protocol.PayloadBuilder,
    ctx: ?*anyopaque,
    on_entry: ?CapEntryCallback,
) !?ResolvedCap {
    var payload_builder = payload;
    var outbound = OutboundCapTable.init(table.allocator);
    defer outbound.deinit();

    const builder = payload_builder._builder.builder;
    const view = try buildMessageView(table.allocator, builder);
    defer table.allocator.free(view.segments);

    const any_builder = try payload_builder.initContent();
    const any_reader = anyPointerReaderFromBuilder(&view.msg, any_builder);

    var root_cap: ?ResolvedCap = null;
    if (!any_reader.isNull()) {
        const resolved = try view.msg.resolvePointer(any_reader.segment_id, any_reader.pointer_pos, any_reader.pointer_word, 8);
        if (resolved.pointer_word != 0 and (@as(u2, @truncate(resolved.pointer_word & 0x3)) == 3)) {
            const cap_id = try decodeCapabilityPointer(resolved.pointer_word);
            const tag = classifyCap(table, cap_id);
            root_cap = switch (tag) {
                .receiverHosted => .{ .imported = .{ .id = cap_id } },
                .senderHosted => .{ .exported = .{ .id = cap_id } },
                else => null,
            };
        }
    }

    try collectCapsFromPointer(&outbound, table, &view.msg, builder, any_reader.segment_id, any_reader.pointer_pos, any_reader.pointer_word, max_traversal_depth);

    var cap_list = try payload_builder.initCapTable(@intCast(outbound.entries.items.len));

    for (outbound.entries.items, 0..) |entry, idx| {
        var elem = try cap_list.get(@intCast(idx));
        switch (entry.tag) {
            .senderHosted => try elem.setSenderHosted(entry.id),
            .senderPromise => try elem.setSenderPromise(entry.id),
            .receiverHosted => try elem.setReceiverHosted(entry.id),
            .receiverAnswer => {
                const promised = table.getReceiverAnswer(entry.id) orelse return error.UnknownReceiverAnswerCap;
                try protocol.CapDescriptor.writeReceiverAnswer(elem._builder, promised.question_id, promised.ops);
            },
            else => {},
        }
        if (on_entry) |cb| {
            const context = ctx orelse return error.MissingCallbackContext;
            try cb(context, entry.tag, entry.id);
        }
    }

    for (outbound.entries.items) |entry| {
        if (entry.tag == .receiverAnswer) {
            if (table.receiver_answers.fetchRemove(entry.id)) |removed| {
                removed.value.deinit(table.allocator);
            }
        }
    }

    return root_cap;
}

pub fn encodeCallPayloadCaps(
    table: *CapTable,
    call: *protocol.CallBuilder,
    ctx: ?*anyopaque,
    on_entry: ?CapEntryCallback,
) !void {
    const payload = try call.payloadTyped();
    _ = try encodePayloadCaps(table, payload, ctx, on_entry);
}

pub fn encodeReturnPayloadCaps(
    table: *CapTable,
    ret: *protocol.ReturnBuilder,
    ctx: ?*anyopaque,
    on_entry: ?CapEntryCallback,
) !?ResolvedCap {
    const payload = try ret.payloadTyped();
    return encodePayloadCaps(table, payload, ctx, on_entry);
}

/// Walk a promised-answer transform path through a results payload to find
/// the referenced capability.
pub fn resolvePromisedAnswer(
    payload: protocol.Payload,
    transform: protocol.PromisedAnswerTransform,
) !ResolvedCap {
    const resolved = try promise_pipeline.resolvePromisedAnswer(payload, transform);
    return switch (resolved) {
        .none => .none,
        .exported_id => |id| .{ .exported = .{ .id = id } },
        .imported_id => |id| .{ .imported = .{ .id = id } },
        .promised => |promised| .{ .promised = promised },
    };
}
