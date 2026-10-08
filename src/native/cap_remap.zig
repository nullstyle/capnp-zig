//! Capability remapping between host payloads and RPC payloads (plan §4,
//! D5; the M0 "cap-remap clone" spike, which decides handoff H6).
//!
//! The host (Swift) builds params/results as a STANDALONE message: its root
//! is the params/results struct, and every capability pointer in it holds an
//! index into a host `caps[]` array of `{kind, id}` (D5).
//!
//! Outbound (`writeHostContent`): validate the host message, clone its root
//! into the Call/Return `payload.content`, then walk the CLONED content and
//! rewrite every capability pointer from "host index i" to an origin-tagged
//! pointer carrying `caps[i]`'s real id space and id
//! (`AnyPointerBuilder.setCapabilityOriginTagged`, Stable). The Peer's own
//! encoder then builds the cap table from those pointers when it sends the
//! frame (`sendCall` runs `encodeCallPayloadCapsWithEffects` after the build
//! callback, `sendReturnResults` runs `encodeReturnPayloadCapsWithEffects`).
//! We never call `initCapTableTyped` or the encoders ourselves: the encoder
//! re-inits the cap table from the cap pointers it finds, and a plain
//! (untagged) pointer would be classified by bare id, which is ambiguous when
//! the export and import id spaces collide (`caps/outbound.zig` resolveCapEntry).
//!
//! A PROMISED host cap (a question of ours that has not returned, plus a
//! pipeline path) becomes a `receiverAnswer` descriptor: the pair is noted in
//! the Peer's cap table (`noteReceiverAnswerOps`) and the pointer tagged with
//! that entry; the Peer's encoder writes the descriptor and retires the entry.
//!
//! Inbound (`copyInbound`): clone the inbound payload content into a new
//! standalone message (its cap pointers keep their inbound cap-table indices)
//! and translate the inbound cap table into `caps[]`, retaining every import
//! so the host owns one wire reference per IMPORT entry until it releases it.
//! The remote controls that content, and Cap'n Proto pointers may alias: a
//! small frame can point at one blob thousands of times, and a clone pays for
//! every alias. So the copy is bounded by the message the content came from
//! (a tree without aliasing never clones larger than its message); a payload
//! that would grow past it is refused with `error.PayloadCopyExceedsFrame`.

const std = @import("std");
const capnp = @import("capnpc-zig");
const effects = @import("effects.zig");

const message = capnp.message;
const remap = message.capability_remap;
const protocol = capnp.rpc.wire.protocol;
const cap_table = capnp.rpc.caps.table;
const descriptors = capnp.rpc.caps.descriptors;

pub const Cap = effects.Cap;

pub const RemapError = error{
    /// A capability pointer's index is outside `caps[]`.
    CapIndexOutOfRange,
    /// An IMPORT id with no live wire reference, or an EXPORT id that is not
    /// exported (stale or forged host handle).
    BadCapId,
    /// A cap kind this side cannot encode (reserved values).
    UnsupportedCapKind,
};

/// Clone the host message `host_msg` (validated with default limits) into
/// `payload.content` and remap its capability pointers through `caps`.
/// `table` is the sending Peer's cap table (`peer.caps`); it is only written
/// to note PROMISED caps as receiver answers (the encoder retires them).
pub fn writeHostContent(
    allocator: std.mem.Allocator,
    table: *cap_table.CapTable,
    payload: *protocol.PayloadBuilder,
    host_msg: []const u8,
    caps: []const Cap,
) !void {
    var src = try message.Message.init(allocator, host_msg, .{});
    defer src.deinit();
    const root = try src.getRootAnyPointer();
    const content = try payload.initContent();
    // cloneAnyPointer copies capability pointers as plain indices (it rejects
    // origin-tagged ones, so a host cannot smuggle a pre-tagged pointer).
    try message.cloneAnyPointer(root, content);
    try remapContentCaps(allocator, table, content, caps);
}

/// Rewrite every capability pointer reachable from `content` (in place, in
/// `content.builder`) from a host index into an origin-tagged pointer.
pub fn remapContentCaps(
    allocator: std.mem.Allocator,
    table: *cap_table.CapTable,
    content: message.AnyPointerBuilder,
    caps: []const Cap,
) !void {
    const builder = content.builder;
    // A read view over the builder's own segments: walking it with the
    // reader's resolve functions follows far pointers and list encodings
    // exactly as the Peer's encoder will. Writes go to the same bytes; each
    // capability pointer is read once, before it is rewritten.
    const view = try remap.buildMessageView(allocator, builder);
    defer allocator.free(view.segments);
    if (content.segment_id >= view.msg.segments.len) return error.InvalidSegmentId;
    const seg = view.msg.segments[content.segment_id];
    if (content.pointer_pos + 8 > seg.len) return error.OutOfBounds;
    const word = std.mem.readInt(u64, seg[content.pointer_pos..][0..8], .little);
    try walk(&view.msg, builder, table, caps, content.segment_id, content.pointer_pos, word, remap.max_traversal_depth);
}

fn walk(
    msg: *const message.Message,
    builder: *message.MessageBuilder,
    table: *cap_table.CapTable,
    caps: []const Cap,
    segment_id: u32,
    pointer_pos: usize,
    pointer_word: u64,
    depth: u32,
) !void {
    if (depth == 0) return error.RecursionLimitExceeded;
    if (pointer_word == 0) return;
    const resolved = try msg.resolvePointer(segment_id, pointer_pos, pointer_word, 8);
    if (resolved.pointer_word == 0) return;
    switch (@as(u2, @truncate(resolved.pointer_word & 0x3))) {
        // Struct: visit its pointer section.
        0 => {
            const s = try msg.resolveStructPointer(resolved.segment_id, resolved.pointer_pos, resolved.pointer_word);
            const base = s.offset + @as(usize, s.data_size) * 8;
            var i: usize = 0;
            while (i < s.pointer_count) : (i += 1) {
                const pos = base + i * 8;
                try walk(msg, builder, table, caps, s.segment_id, pos, try slotWord(msg, s.segment_id, pos), depth - 1);
            }
        },
        // List: only pointer lists and struct lists can hold capabilities.
        1 => {
            const list = try msg.resolveListPointer(resolved.segment_id, resolved.pointer_pos, resolved.pointer_word);
            if (list.element_size == 6) {
                var i: u32 = 0;
                while (i < list.element_count) : (i += 1) {
                    const pos = list.content_offset + @as(usize, i) * 8;
                    try walk(msg, builder, table, caps, list.segment_id, pos, try slotWord(msg, list.segment_id, pos), depth - 1);
                }
            } else if (list.element_size == 7) {
                const ic = try msg.resolveInlineCompositeList(resolved.segment_id, resolved.pointer_pos, resolved.pointer_word);
                const stride = (@as(usize, ic.data_words) + @as(usize, ic.pointer_words)) * 8;
                var e: u32 = 0;
                while (e < ic.element_count) : (e += 1) {
                    const base = ic.elements_offset + @as(usize, e) * stride + @as(usize, ic.data_words) * 8;
                    var p: usize = 0;
                    while (p < ic.pointer_words) : (p += 1) {
                        const pos = base + p * 8;
                        try walk(msg, builder, table, caps, ic.segment_id, pos, try slotWord(msg, ic.segment_id, pos), depth - 1);
                    }
                }
            }
        },
        // Capability: host index -> origin-tagged real id.
        3 => try rewriteCap(builder, table, caps, resolved.segment_id, resolved.pointer_pos, resolved.pointer_word),
        else => return error.InvalidPointer,
    }
}

/// The pointer word stored at `pos` (bounds-checked).
fn slotWord(msg: *const message.Message, segment_id: u32, pos: usize) error{ InvalidSegmentId, OutOfBounds }!u64 {
    if (segment_id >= msg.segments.len) return error.InvalidSegmentId;
    const seg = msg.segments[segment_id];
    if (pos + 8 > seg.len) return error.OutOfBounds;
    return std.mem.readInt(u64, seg[pos..][0..8], .little);
}

fn rewriteCap(
    builder: *message.MessageBuilder,
    table: *cap_table.CapTable,
    caps: []const Cap,
    segment_id: u32,
    pointer_pos: usize,
    pointer_word: u64,
) !void {
    const index = try remap.decodeCapabilityPointer(pointer_word);
    if (index >= caps.len) return error.CapIndexOutOfRange;
    const dest = message.AnyPointerBuilder{
        .builder = builder,
        .segment_id = segment_id,
        .pointer_pos = pointer_pos,
    };
    const cap = caps[index];
    switch (cap.kind) {
        .none => try dest.setNull(),
        .import => {
            if (importRefCount(table, cap.id) == 0) return error.BadCapId;
            try dest.setCapabilityOriginTagged(descriptors.originCodeForTag(.receiverHosted), cap.id);
        },
        .@"export" => {
            if (!table.hasExport(cap.id)) return error.BadCapId;
            const tag: protocol.CapDescriptorTag = if (table.isExportPromise(cap.id)) .senderPromise else .senderHosted;
            try dest.setCapabilityOriginTagged(descriptors.originCodeForTag(tag), cap.id);
        },
        .promised => {
            // The question must be live; conn.zig checks that before the
            // send (the table cannot). The entry is retired by the encoder.
            const ops = try promisedOps(table.allocator, cap);
            defer table.allocator.free(ops);
            const entry = try table.noteReceiverAnswerOps(cap.id, ops);
            try dest.setCapabilityOriginTagged(descriptors.originCodeForTag(.receiverAnswer), entry);
        },
    }
}

/// The pipeline path of a PROMISED cap as the Peer's op structs.
pub fn promisedOps(allocator: std.mem.Allocator, cap: Cap) ![]protocol.PromisedAnswerOp {
    const indices = cap.opsSlice();
    const ops = try allocator.alloc(protocol.PromisedAnswerOp, indices.len);
    for (indices, ops) |index, *op| op.* = .{ .tag = .getPointerField, .pointer_index = index };
    return ops;
}

/// Wire references the peer holds on `import_id` (0 when unknown).
pub fn importRefCount(table: *const cap_table.CapTable, import_id: u32) u32 {
    const entry = table.imports.get(import_id) orelse return 0;
    return entry.ref_count;
}

pub const Inbound = struct {
    /// Standalone message (segment table + segments) whose root is a clone
    /// of the payload content. Owned; free with the same allocator.
    msg: []const u8,
    /// One entry per inbound cap-table entry. Owned.
    caps: []Cap,

    pub fn deinit(self: Inbound, allocator: std.mem.Allocator) void {
        allocator.free(self.msg);
        allocator.free(self.caps);
    }
};

/// Copy an inbound payload out as a standalone message plus `caps[]`.
///
/// The copy is bounded by `content`'s own message (the frame it arrived in):
/// its segments may hold no more bytes than that message's segments, and the
/// clone's working memory stays under `cloneMemoryLimit` of it. A payload
/// whose pointers alias (legal on the wire, and charged by the Peer's
/// traversal limit only as reads) would exceed that; it is refused with
/// `error.PayloadCopyExceedsFrame` before the copy is materialized.
///
/// On success every `.imported` entry of `inbound` is marked retained, so the
/// Peer's post-dispatch release pass leaves those wire references to the
/// host. Retention is the last step and cannot fail, so on error the host
/// owns nothing and the Peer releases the imports as usual.
///
/// `inbound` is `*const` because that is how the Peer hands it to question
/// callbacks and call handlers; its `retained` flags are written through a
/// by-value copy whose slice aliases the Peer's storage (the pattern capnp-zig's
/// own generated code and HostPeer use).
pub fn copyInbound(
    allocator: std.mem.Allocator,
    content: message.AnyPointerReader,
    inbound: *const cap_table.InboundCapTable,
) !Inbound {
    const source_bytes = messageBytes(content.message);
    var capped: CappedAllocator = .{ .parent = allocator, .limit = cloneMemoryLimit(source_bytes) };
    var mb = message.MessageBuilder.init(capped.allocator());
    defer mb.deinit();
    const root = mb.initRootAnyPointer() catch |err| return capped.explain(err);
    message.cloneAnyPointer(content, root) catch |err| return capped.explain(err);
    if (builderBytes(&mb) > source_bytes) return error.PayloadCopyExceedsFrame;
    // The clone is bounded now, and so is its serialization (segment table
    // plus the same bytes). Every allocation still goes to `allocator`.
    capped.limit = std.math.maxInt(usize);
    const bytes = try mb.toBytes();
    errdefer allocator.free(bytes);

    const n = inbound.len();
    const caps = try allocator.alloc(Cap, n);
    errdefer allocator.free(caps);
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        caps[i] = switch (try inbound.get(i)) {
            .none => .{ .kind = .none },
            .imported => |imp| .{ .kind = .import, .id = imp.id },
            .exported => |exp| .{ .kind = .@"export", .id = exp.id },
            // A remote reference to one of OUR answers (receiverAnswer) that
            // the Peer did not resolve. Since capnp-zig 0.23.0 (handoff H9)
            // it resolves only an answer that returned one of our exports
            // (or null). The entry stays unresolved while the answer is
            // pending, when it returned a capability the caller hosts (an
            // import here) or a promise, and for a call parked until after
            // the caller finished that answer. capnp-zig delivers such calls
            // at once and offers no local promise client for the entry (its
            // generated `resolveX` fails on it too), so the host could only
            // get a null capability. Refuse instead: the caller gets an
            // exception named after this error (a call), or RETURN{EXCEPTION}
            // (results).
            .promised => return error.PromisedCapUnsupported,
        };
    }

    var mutable = inbound.*;
    i = 0;
    while (i < n) : (i += 1) {
        if (caps[i].kind == .import) mutable.retainIndex(i) catch unreachable; // i < len
    }
    return .{ .msg = bytes, .caps = caps };
}

/// Bytes in `msg`'s segments (the segment table is not counted).
fn messageBytes(msg: *const message.Message) usize {
    var n: usize = 0;
    for (msg.segments) |segment| n +|= segment.len;
    return n;
}

/// Bytes in `mb`'s segments (what `toBytes` writes after the segment table).
fn builderBytes(mb: *const message.MessageBuilder) usize {
    var n: usize = 0;
    for (mb.segments.items) |segment| n +|= segment.items.len;
    return n;
}

/// Working memory a clone of a message of `source_bytes` may use. A clone
/// without aliasing needs at most `source_bytes`; a builder segment grows by
/// 1.5x and copies when it cannot grow in place, so it can briefly hold about
/// 2.5x what it needs, after a 1 KiB first segment. 4x plus 4 KiB covers that
/// for every non-aliased tree, and stops an aliased one early.
pub fn cloneMemoryLimit(source_bytes: usize) usize {
    return (source_bytes *| 4) +| 4096;
}

/// Forwards to `parent`, but refuses any allocation that would take the
/// bytes live through it past `limit`, and remembers that it did.
const CappedAllocator = struct {
    parent: std.mem.Allocator,
    limit: usize,
    live: usize = 0,
    refused: bool = false,

    fn allocator(self: *CappedAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = allocFn,
            .resize = resizeFn,
            .remap = remapFn,
            .free = freeFn,
        } };
    }

    /// The error a failed clone step reports: an allocation this allocator
    /// refused means the payload is too large for its frame, not that the
    /// process is out of memory.
    fn explain(self: *const CappedAllocator, err: anyerror) anyerror {
        if (err == error.OutOfMemory and self.refused) return error.PayloadCopyExceedsFrame;
        return err;
    }

    /// May an allocation grow from `old_len` to `new_len` bytes?
    fn admit(self: *CappedAllocator, old_len: usize, new_len: usize) bool {
        if (new_len <= old_len) return true;
        if (new_len - old_len <= self.limit -| self.live) return true;
        self.refused = true;
        return false;
    }

    fn allocFn(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CappedAllocator = @ptrCast(@alignCast(ctx));
        if (!self.admit(0, len)) return null;
        const p = self.parent.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.live += len;
        return p;
    }

    fn resizeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CappedAllocator = @ptrCast(@alignCast(ctx));
        if (!self.admit(memory.len, new_len)) return false;
        if (!self.parent.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.live = self.live - memory.len + new_len;
        return true;
    }

    fn remapFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CappedAllocator = @ptrCast(@alignCast(ctx));
        if (!self.admit(memory.len, new_len)) return null;
        const p = self.parent.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.live = self.live - memory.len + new_len;
        return p;
    }

    fn freeFn(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CappedAllocator = @ptrCast(@alignCast(ctx));
        self.parent.rawFree(memory, alignment, ret_addr);
        self.live -|= memory.len;
    }
};
