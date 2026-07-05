const std = @import("std");
const message = @import("../../serialization/message.zig");
const bounds = @import("../../serialization/message/bounds.zig");
const protocol = @import("../wire/protocol.zig");
const descriptors = @import("descriptors.zig");
const lifecycle = @import("lifecycle.zig");

const capability_remap = message.capability_remap;
const makeCapabilityPointer = capability_remap.makeCapabilityPointer;
const decodeCapabilityWithOrigin = capability_remap.decodeCapabilityWithOrigin;
const buildMessageView = capability_remap.buildMessageView;
const writePointerWord = capability_remap.writePointerWord;
const max_traversal_depth = capability_remap.max_traversal_depth;

pub const CapTable = lifecycle.CapTable;
pub const ResolvedCap = descriptors.ResolvedCap;

pub const OutboundEntry = struct {
    tag: protocol.CapDescriptorTag,
    id: u32,
};

pub const CapEntryCallback = *const fn (ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) anyerror!void;
pub const CapEntryRollbackCallback = *const fn (ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) void;

/// Staged side effects produced while encoding outbound payload cap tables.
///
/// Effects are intentionally separated from encode-time structure rewriting so
/// callers can commit only after the encoded frame is successfully sent.
pub const OutboundCapEffects = struct {
    allocator: std.mem.Allocator,
    ctx: ?*anyopaque,
    on_entry_rollback: ?CapEntryRollbackCallback,
    callback_applied: std.ArrayList(OutboundEntry),
    consumed_receiver_answer_ids: std.ArrayList(u32),

    pub fn init(
        allocator: std.mem.Allocator,
        ctx: ?*anyopaque,
        on_entry_rollback: ?CapEntryRollbackCallback,
    ) OutboundCapEffects {
        return .{
            .allocator = allocator,
            .ctx = ctx,
            .on_entry_rollback = on_entry_rollback,
            .callback_applied = std.ArrayList(OutboundEntry).empty,
            .consumed_receiver_answer_ids = std.ArrayList(u32).empty,
        };
    }

    pub fn deinit(self: *OutboundCapEffects) void {
        self.callback_applied.deinit(self.allocator);
        self.consumed_receiver_answer_ids.deinit(self.allocator);
    }

    pub fn rollback(self: *const OutboundCapEffects) void {
        const rollback_cb = self.on_entry_rollback orelse return;
        const context = self.ctx orelse return;
        var idx = self.callback_applied.items.len;
        while (idx > 0) {
            idx -= 1;
            const entry = self.callback_applied.items[idx];
            rollback_cb(context, entry.tag, entry.id);
        }
    }

    fn noteCallbackApplied(self: *OutboundCapEffects, entry: OutboundEntry) !void {
        try self.callback_applied.append(self.allocator, entry);
    }

    fn popCallbackApplied(self: *OutboundCapEffects) void {
        _ = self.callback_applied.pop();
    }

    fn noteConsumedReceiverAnswer(self: *OutboundCapEffects, id: u32) !void {
        try self.consumed_receiver_answer_ids.append(self.allocator, id);
    }
};

pub const OutboundCapTable = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(OutboundEntry),
    index_map: std.AutoHashMap(u64, u32),

    fn init(allocator: std.mem.Allocator) OutboundCapTable {
        return .{
            .allocator = allocator,
            .entries = std.ArrayList(OutboundEntry).empty,
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

fn classifyCap(table: *CapTable, cap_id: u32) error{UnknownCapabilityId}!protocol.CapDescriptorTag {
    if (table.hasReceiverAnswer(cap_id)) return .receiverAnswer;
    if (table.isExportPromise(cap_id)) return .senderPromise;
    if (table.hasExport(cap_id)) return .senderHosted;
    if (table.hasImport(cap_id)) return .receiverHosted;
    return error.UnknownCapabilityId;
}

/// Resolve a payload capability pointer to its outbound descriptor variant.
///
/// If the pointer carries an origin tag (forwarded or provided caps, whose true
/// id-space is known at the boundary) that tag is authoritative — we NEVER
/// re-derive the space from the bare id, which is ambiguous when the local
/// export and remote import id spaces collide (a remote can force the collision;
/// see `lifecycle.noteImport`). Only genuinely local, app-authored pointers
/// (no origin tag) fall back to `classifyCap`, where the id is unambiguously one
/// of our own tables by construction.
fn resolveCapEntry(table: *CapTable, pointer_word: u64) !OutboundEntry {
    const info = try decodeCapabilityWithOrigin(pointer_word);
    const tag = if (info.origin_code) |code|
        try descriptors.tagForOriginCode(code)
    else
        try classifyCap(table, info.cap_id);
    return .{ .tag = tag, .id = info.cap_id };
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
                try bounds.checkBounds(msg.segments[struct_reader.segment_id], pos, 8);
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
                        try bounds.checkBounds(msg.segments[inline_list.segment_id], pos, 8);
                        const word = std.mem.readInt(u64, msg.segments[inline_list.segment_id][pos..][0..8], .little);
                        try collectCapsFromPointer(outbound, table, msg, builder, inline_list.segment_id, pos, word, depth - 1);
                    }
                }
            }
        },
        3 => {
            const entry = try resolveCapEntry(table, resolved.pointer_word);
            const index = try outbound.indexFor(entry.tag, entry.id);
            const new_word = makeCapabilityPointer(index);
            try writePointerWord(builder, resolved.segment_id, resolved.pointer_pos, new_word);
        },
        else => return error.InvalidPointer,
    }
}

/// Encode capability descriptors into the outbound payload's cap table.
///
// SAFETY: writePointerWord only mutates capability pointers after they have been fully
// resolved by collectCapsFromPointer, which processes each pointer exactly once.
// The read view will never re-read a mutated pointer.
fn encodePayloadCaps(
    table: *CapTable,
    payload: protocol.PayloadBuilder,
    on_entry: ?CapEntryCallback,
    effects: *OutboundCapEffects,
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
            const entry = try resolveCapEntry(table, resolved.pointer_word);
            root_cap = switch (entry.tag) {
                .receiverHosted => .{ .imported = .{ .id = entry.id } },
                .senderHosted => .{ .exported = .{ .id = entry.id } },
                else => null,
            };
        }
    }

    try collectCapsFromPointer(&outbound, table, &view.msg, builder, any_reader.segment_id, any_reader.pointer_pos, any_reader.pointer_word, max_traversal_depth);

    var cap_list = try payload_builder.initCapTable(@intCast(outbound.entries.items.len));

    for (outbound.entries.items, 0..) |entry, idx| {
        var elem = try cap_list.get(@intCast(idx));

        // ORIGINATION override: a cap marked for three-party handoff is emitted
        // as `thirdPartyHosted{ id = ThirdPartyToContact, vineId }` instead of
        // its two-party variant. The wire reference the recipient takes lands on
        // the VINE export (which anchors the handoff), not on the provided cap,
        // so the callback entry is rewritten to `(senderHosted, vine_id)`.
        if (table.getThirdPartyHosted(entry.id)) |record| {
            var contact_msg = try message.Message.initUnvalidated(table.allocator, record.contact_payload);
            defer contact_msg.deinit();
            const contact = try contact_msg.getRootAnyPointer();
            try protocol.CapDescriptor.writeThirdPartyHosted(elem._builder, contact, record.vine_id);

            if (on_entry) |cb| {
                const context = effects.ctx orelse return error.MissingCallbackContext;
                const vine_entry = OutboundEntry{ .tag = .senderHosted, .id = record.vine_id };
                try effects.noteCallbackApplied(vine_entry);
                cb(context, vine_entry.tag, vine_entry.id) catch |err| {
                    effects.popCallbackApplied();
                    return err;
                };
            }
            continue;
        }

        switch (entry.tag) {
            .senderHosted => try elem.setSenderHosted(entry.id),
            .senderPromise => try elem.setSenderPromise(entry.id),
            .receiverHosted => try elem.setReceiverHosted(entry.id),
            .receiverAnswer => {
                const promised = table.getReceiverAnswer(entry.id) orelse return error.UnknownReceiverAnswerCap;
                try protocol.CapDescriptor.writeReceiverAnswer(elem._builder, promised.question_id, promised.ops);
                try effects.noteConsumedReceiverAnswer(entry.id);
            },
            else => {},
        }
        if (on_entry) |cb| {
            const context = effects.ctx orelse return error.MissingCallbackContext;
            try effects.noteCallbackApplied(entry);
            cb(context, entry.tag, entry.id) catch |err| {
                effects.popCallbackApplied();
                return err;
            };
        }
    }

    return root_cap;
}

pub fn commitOutboundCapEffects(table: *CapTable, effects: *const OutboundCapEffects) void {
    for (effects.consumed_receiver_answer_ids.items) |id| {
        if (table.takeReceiverAnswer(id)) |answer| {
            answer.deinit(table.allocator);
        }
    }
}

pub fn encodeCallPayloadCapsWithEffects(
    table: *CapTable,
    call: *protocol.CallBuilder,
    on_entry: ?CapEntryCallback,
    effects: *OutboundCapEffects,
) !void {
    const payload = try call.payloadTyped();
    _ = try encodePayloadCaps(table, payload, on_entry, effects);
}

pub fn encodeCallPayloadCaps(
    table: *CapTable,
    call: *protocol.CallBuilder,
    ctx: ?*anyopaque,
    on_entry: ?CapEntryCallback,
    on_entry_rollback: ?CapEntryRollbackCallback,
) !void {
    var effects = OutboundCapEffects.init(table.allocator, ctx, on_entry_rollback);
    defer effects.deinit();
    errdefer effects.rollback();
    try encodeCallPayloadCapsWithEffects(table, call, on_entry, &effects);
    commitOutboundCapEffects(table, &effects);
}

pub fn encodeReturnPayloadCapsWithEffects(
    table: *CapTable,
    ret: *protocol.ReturnBuilder,
    on_entry: ?CapEntryCallback,
    effects: *OutboundCapEffects,
) !?ResolvedCap {
    const payload = try ret.payloadTyped();
    return encodePayloadCaps(table, payload, on_entry, effects);
}

pub fn encodeReturnPayloadCaps(
    table: *CapTable,
    ret: *protocol.ReturnBuilder,
    ctx: ?*anyopaque,
    on_entry: ?CapEntryCallback,
    on_entry_rollback: ?CapEntryRollbackCallback,
) !?ResolvedCap {
    var effects = OutboundCapEffects.init(table.allocator, ctx, on_entry_rollback);
    defer effects.deinit();
    errdefer effects.rollback();
    const root_cap = try encodeReturnPayloadCapsWithEffects(table, ret, on_entry, &effects);
    commitOutboundCapEffects(table, &effects);
    return root_cap;
}
