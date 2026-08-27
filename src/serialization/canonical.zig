//! Spec-faithful, schema-FREE canonicalization of Cap'n Proto messages.
//!
//! `schema_validation.canonicalizeMessage`/`Flat` are schema-DRIVEN: they
//! re-encode through builders using a loaded schema, so data written by a
//! newer peer for fields the schema lacks is silently dropped and upgraded
//! lists are re-encoded. This module instead implements the spec's actual
//! canonical form — a walk of the raw pointer graph that never consults a
//! schema — making the output byte-comparable with the reference
//! implementation (`capnp convert binary:canonical`) and appropriate as a
//! signing input.
//!
//! Semantic authority is the vendored C++ reference implementation. The rules,
//! each cited at its enforcement site below
//! (paths relative to vendor/ext/capnproto/c++/src/capnp/):
//!
//! - The canonical form is a SINGLE segment whose first word is the root
//!   pointer (layout.c++:3008-3020 `StructReader::canonicalize` builds into a
//!   flat single-segment arena rooted at word 0; message.c++:55-83
//!   `MessageReader::isCanonical` rejects zero or multiple segments).
//! - Objects are laid out in preorder: each object's body is allocated
//!   immediately, then its pointer fields' targets are copied depth-first in
//!   field order (layout.c++:1832-1836/2623-2628 — `setStructPointer` copies
//!   the body then recurses per pointer through the bump allocator, so
//!   children land directly after their parent; verified by
//!   layout.c++:3035-3038, which requires each object AT the read head).
//! - Struct data sections are truncated of trailing zero BYTES, rounded back
//!   up to words; pointer sections are truncated of trailing null pointers
//!   (layout.c++:1789-1811).
//! - A struct that truncates to zero size is encoded as a non-null struct
//!   pointer with offset -1 and zero sizes (layout.c++:503-508 `allocate`
//!   special-cases zero-size structs via `setKindAndTargetForEmptyStruct`;
//!   accepted by isCanonical only in that exact shape, layout.c++:2831-2837).
//! - Inline-composite (struct) lists use ONE uniform element size: the MAX of
//!   the per-element truncated data/pointer sizes (layout.c++:1901-1925), and
//!   element pointer targets are emitted after the whole list body
//!   (layout.c++:1935-1948; mirrored by the `pointerHead` walk in
//!   layout.c++:3287-3303).
//! - Primitive lists keep their element size — an "upgraded" list is NOT
//!   re-encoded (layout.c++:1858-1895 copies the source `elementSize`
//!   verbatim; canonicalize-test.c++:289). Bytes past the last element up to
//!   the word boundary are zero, including masking a trailing partial byte of
//!   a bit list (layout.c++:1884-1894; canonicalize-test.c++:336,363).
//! - Far pointers never appear in the output (single segment); far pointers
//!   in the INPUT are followed (layout.c++:1990-1996 `copyPointer` resolves
//!   via `followFars`).
//! - Capabilities cannot be canonicalized: layout.c++:2101-2104 "Cannot
//!   create a canonical message with a capability".
//! - Null pointers stay null (layout.c++:1979-1986).
//!
//! Deliberately self-contained: a signing primitive should own its decoder,
//! so this module walks `Message.segments` with its own pointer decoding and
//! leans on `message.Message`'s public helpers only where they exactly match
//! the reference semantics (`resolvePointer` = `followFars`,
//! `resolveListPointer`/`resolveInlineCompositeList` = the bounds-checked
//! list-header reads of layout.c++:2029-2101).

const std = @import("std");
const message = @import("message.zig");

/// Mirror of the C++ default `ReaderOptions::nestingLimit` (message.h:75),
/// which `copyPointer` decrements per level (layout.c++:1997-2000).
const default_nesting_limit: u32 = 64;

/// Belt-and-braces output cap. The input contract (a `Message` from
/// `Message.init`) already bounds traversal via the validator's budget; this
/// cap only turns a hypothetical amplification into an error instead of an
/// unbounded allocation.
const max_output_words: usize = message.Message.max_total_words;

/// Errors specific to canonicalization, beyond what the underlying
/// `message.Message` resolvers can already return.
pub const CanonicalError = error{
    /// layout.c++:2101-2104: "Cannot create a canonical message with a
    /// capability".
    CannotCanonicalizeCapability,
    /// layout.c++:1997-2000: "Message is too deeply-nested or contains
    /// cycles" (nesting limit mirrors ReaderOptions, message.h:75).
    NestingLimitExceeded,
    /// The canonical output would exceed `max_output_words`.
    MessageTooLarge,
};

/// Full error set of `canonicalize`/`canonicalizeFlat`: allocation failure,
/// the canonical-specific errors above, and the wire-decoding errors the
/// `message.Message` resolvers report for malformed pointers. Spelled out
/// (rather than inferred) because the writer is mutually recursive.
pub const CanonicalizeError = std.mem.Allocator.Error || CanonicalError || error{
    OutOfBounds,
    InvalidPointer,
    InvalidFarPointer,
    InvalidSegmentId,
    InvalidInlineCompositePointer,
    PointerDepthLimit,
    InvalidRootPointer,
};

// ---------------------------------------------------------------------------
// Wire-level decoding helpers (local on purpose; see module doc)
// ---------------------------------------------------------------------------

fn decodeOffsetWords(pointer_word: u64) i32 {
    // 30-bit signed word offset in bits 2..32 (two's complement).
    const raw: u32 = @truncate((pointer_word >> 2) & 0x3FFF_FFFF);
    if ((raw & 0x2000_0000) != 0) {
        return @as(i32, @intCast(raw)) - (@as(i32, 1) << 30);
    }
    return @as(i32, @intCast(raw));
}

fn readWordAt(segment: []const u8, byte_offset: usize) error{OutOfBounds}!u64 {
    const end = std.math.add(usize, byte_offset, 8) catch return error.OutOfBounds;
    if (end > segment.len) return error.OutOfBounds;
    return std.mem.readInt(u64, segment[byte_offset..][0..8], .little);
}

/// Content byte offset for a resolved positional pointer: either the
/// double-far content override, or pointer position + 8 + offset words.
fn contentOffset(pointer_pos: usize, offset_words: i32, content_override: ?usize) error{OutOfBounds}!usize {
    if (content_override) |override| return override;
    const base = std.math.add(usize, pointer_pos, 8) catch return error.OutOfBounds;
    if (offset_words >= 0) {
        const delta = std.math.mul(usize, @as(usize, @intCast(offset_words)), 8) catch return error.OutOfBounds;
        return std.math.add(usize, base, delta) catch return error.OutOfBounds;
    }
    const delta = std.math.mul(usize, @as(usize, @intCast(-@as(i64, offset_words))), 8) catch return error.OutOfBounds;
    if (delta > base) return error.OutOfBounds;
    return base - delta;
}

/// Number of data BITS per element for list element sizes 0..5
/// (layout.c++ `dataBitsPerElement`).
fn dataBitsPerElement(element_size: u3) u64 {
    return switch (element_size) {
        0 => 0, // void
        1 => 1, // bit
        2 => 8,
        3 => 16,
        4 => 32,
        5 => 64,
        // Pointer (6) and inline-composite (7) lists are dispatched to their
        // own paths before this is consulted; answered defensively (0 data
        // bits is what the C++ table returns for POINTER too) rather than
        // with `unreachable` so a future caller cannot turn a reachable path
        // into undefined behavior.
        6, 7 => 0,
    };
}

// ---------------------------------------------------------------------------
// Canonical writer
// ---------------------------------------------------------------------------

const Canonicalizer = struct {
    allocator: std.mem.Allocator,
    msg: *const message.Message,
    /// The canonical segment being built. Word 0 is the root pointer; all
    /// later objects are appended by the bump allocation in `allocWords`,
    /// which is what produces the preorder layout (layout.c++:480-540
    /// `allocate` appends sequentially; `setStructPointer` recurses per
    /// pointer field after placing the body).
    out: std.ArrayList(u8),

    fn allocWords(self: *Canonicalizer, words: usize) !usize {
        const pos = self.out.items.len;
        const bytes = std.math.mul(usize, words, 8) catch return error.MessageTooLarge;
        if ((pos + bytes) / 8 > max_output_words) return error.MessageTooLarge;
        try self.out.appendNTimes(self.allocator, 0, bytes);
        return pos;
    }

    fn writeWordAt(self: *Canonicalizer, byte_pos: usize, word: u64) void {
        std.mem.writeInt(u64, self.out.items[byte_pos..][0..8], word, .little);
    }

    fn segmentSlice(self: *const Canonicalizer, segment_id: u32) []const u8 {
        // resolvePointer/resolveListPointer already validated the id.
        return self.msg.segments[segment_id];
    }

    /// Port of layout.c++ `copyPointer` (1962-2112) with `canonical = true`.
    /// `dst_pos` is the byte offset of the (already zeroed) destination
    /// pointer word inside `out`.
    fn copyPointer(
        self: *Canonicalizer,
        dst_pos: usize,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
        depth: u32,
    ) CanonicalizeError!void {
        // layout.c++:1979-1986: a null source pointer stays null.
        if (pointer_word == 0) return;

        // layout.c++:1990-1996: follow far pointers to the object's actual
        // pointer word. `resolvePointer` is the exact analogue of
        // `followFars` (including double-far content overrides).
        const resolved = try self.msg.resolvePointer(segment_id, pointer_pos, pointer_word, 3);

        switch (@as(u2, @truncate(resolved.pointer_word & 0x3))) {
            0 => {
                // Struct (note: a resolved all-zero landing word decodes as a
                // zero-sized struct here, exactly as the C++ kind() switch
                // does after followFars).
                if (depth == 0) return error.NestingLimitExceeded;
                const data_words = @as(u16, @truncate((resolved.pointer_word >> 32) & 0xFFFF));
                const pointer_count = @as(u16, @truncate((resolved.pointer_word >> 48) & 0xFFFF));
                const offset = decodeOffsetWords(resolved.pointer_word);
                const content = try contentOffset(resolved.pointer_pos, offset, resolved.content_override);
                const seg = self.segmentSlice(resolved.segment_id);
                const total_bytes = (@as(usize, data_words) + @as(usize, pointer_count)) * 8;
                if (std.math.add(usize, content, total_bytes) catch null) |end| {
                    if (end > seg.len) return error.OutOfBounds;
                } else return error.OutOfBounds;
                try self.writeStruct(dst_pos, resolved.segment_id, content, data_words, pointer_count, depth);
            },
            1 => {
                if (depth == 0) return error.NestingLimitExceeded;
                const element_size = @as(u3, @truncate((resolved.pointer_word >> 32) & 0x7));
                if (element_size == 7) {
                    // Inline composite: `resolveInlineCompositeList` performs
                    // the same tag decoding and bounds/overrun checks as
                    // layout.c++:2029-2060 (including elements-overrun-word-
                    // count). Feed it the ORIGINAL pointer; it follows fars
                    // itself.
                    const list = try self.msg.resolveInlineCompositeList(segment_id, pointer_pos, pointer_word);
                    try self.writeCompositeList(dst_pos, list, depth);
                } else {
                    const list = try self.msg.resolveListPointer(segment_id, pointer_pos, pointer_word);
                    try self.writeDataOrPointerList(dst_pos, list, depth);
                }
            },
            2 => {
                // resolvePointer never yields a far pointer; a malformed
                // chain surfaces as its own error. Defensive mirror of
                // layout.c++:2095-2098 "Unexpected FAR pointer".
                return error.InvalidFarPointer;
            },
            3 => {
                // layout.c++:2101-2104: canonical messages cannot carry
                // capabilities.
                return error.CannotCanonicalizeCapability;
            },
        }
    }

    /// Port of layout.c++ `setStructPointer` (1783-1840) with
    /// `canonical = true`.
    ///
    /// The C++ code also special-cases 1-bit "structs" (layout.c++:1790-1798);
    /// those only arise from schema-typed readers upgraded out of bit lists,
    /// never from decoding raw wire pointers, so a schema-free walker cannot
    /// encounter them.
    fn writeStruct(
        self: *Canonicalizer,
        dst_pos: usize,
        segment_id: u32,
        content: usize,
        data_words: u16,
        pointer_count: u16,
        depth: u32,
    ) CanonicalizeError!void {
        const seg = self.segmentSlice(segment_id);

        // layout.c++:1799-1805: truncate trailing zero bytes of the data
        // section, then round back up to words.
        const data = seg[content .. content + @as(usize, data_words) * 8];
        var data_bytes: usize = data.len;
        while (data_bytes > 0 and data[data_bytes - 1] == 0) data_bytes -= 1;
        const out_data_words: usize = (data_bytes + 7) / 8;

        // layout.c++:1807-1810: truncate trailing null pointers.
        const ptr_section = content + @as(usize, data_words) * 8;
        var out_pointer_count: usize = pointer_count;
        while (out_pointer_count > 0) {
            const w = try readWordAt(seg, ptr_section + (out_pointer_count - 1) * 8);
            if (w != 0) break;
            out_pointer_count -= 1;
        }

        const total_words = out_data_words + out_pointer_count;
        if (total_words == 0) {
            // layout.c++:503-508: zero-sized structs are encoded as a struct
            // pointer with offset -1 (target == the pointer's own location)
            // and zero sizes.
            self.writeWordAt(dst_pos, 0x0000_0000_FFFF_FFFC);
            return;
        }

        const target = try self.allocWords(total_words);
        self.writeWordAt(dst_pos, structPointerWord(dst_pos, target, @intCast(out_data_words), @intCast(out_pointer_count)));

        // Copy the surviving data bytes; the freshly zeroed tail of the last
        // word plays the role of layout.c++:1822-1829's zero-filled arena.
        @memcpy(self.out.items[target..][0..data_bytes], data[0..data_bytes]);

        // layout.c++:1832-1836: recurse per pointer field, in order.
        for (0..out_pointer_count) |i| {
            const src_ptr_pos = ptr_section + i * 8;
            const w = try readWordAt(seg, src_ptr_pos);
            try self.copyPointer(target + out_data_words * 8 + i * 8, segment_id, src_ptr_pos, w, depth - 1);
        }
    }

    /// Port of the non-INLINE_COMPOSITE arm of layout.c++ `setListPointer`
    /// (1856-1895): pointer lists recurse per element; data lists are copied
    /// with everything past `elementCount * step` bits zeroed, including the
    /// partial-byte mask for bit lists.
    fn writeDataOrPointerList(
        self: *Canonicalizer,
        dst_pos: usize,
        list: message.Message.ResolvedListPointer,
        depth: u32,
    ) CanonicalizeError!void {
        const seg = self.segmentSlice(list.segment_id);
        const count: u64 = list.element_count;

        if (list.element_size == 6) {
            // List of pointers (layout.c++:1867-1878).
            const target = try self.allocWords(@intCast(count));
            self.writeWordAt(dst_pos, listPointerWord(dst_pos, target, 6, list.element_count));
            for (0..@as(usize, @intCast(count))) |i| {
                const src_ptr_pos = list.content_offset + i * 8;
                const w = try readWordAt(seg, src_ptr_pos);
                try self.copyPointer(target + i * 8, list.segment_id, src_ptr_pos, w, depth - 1);
            }
            return;
        }

        // List of data (layout.c++:1879-1895). The source element size is
        // preserved verbatim — upgraded lists are not re-encoded.
        const bits = count * dataBitsPerElement(list.element_size);
        const total_words: usize = @intCast((bits + 63) / 64);
        const whole_bytes: usize = @intCast(bits / 8);
        const target = try self.allocWords(total_words);
        self.writeWordAt(dst_pos, listPointerWord(dst_pos, target, list.element_size, list.element_count));
        @memcpy(self.out.items[target..][0..whole_bytes], seg[list.content_offset..][0..whole_bytes]);
        const leftover_bits: u6 = @intCast(bits % 8);
        if (leftover_bits > 0) {
            // layout.c++:1888-1894: mask the final partial byte.
            const mask: u8 = (@as(u8, 1) << @intCast(leftover_bits)) - 1;
            self.out.items[target + whole_bytes] = seg[list.content_offset + whole_bytes] & mask;
        }
    }

    /// Port of the INLINE_COMPOSITE arm of layout.c++ `setListPointer`
    /// (1896-1955) with `canonical = true`: scan every element for its
    /// truncated data/pointer sizes, take the MAX of each as the ONE uniform
    /// element size, then copy elements at the new stride, recursing into
    /// pointers (whose targets land after the whole list body).
    fn writeCompositeList(
        self: *Canonicalizer,
        dst_pos: usize,
        list: message.InlineCompositeList,
        depth: u32,
    ) CanonicalizeError!void {
        const seg = self.segmentSlice(list.segment_id);
        const decl_data_words: usize = list.data_words;
        const decl_pointer_count: usize = list.pointer_words;
        const decl_stride = decl_data_words + decl_pointer_count;
        const count: usize = list.element_count;

        var out_data_words: usize = 0;
        var out_pointer_count: usize = 0;
        if (decl_stride > 0) {
            // layout.c++:1901-1920. (Zero-sized elements skip the scan: with
            // nothing per element the truncated sizes are trivially zero, and
            // C++ only guards that case against amplification.)
            for (0..count) |e| {
                const base = list.elements_offset + e * decl_stride * 8;

                const data = seg[base .. base + decl_data_words * 8];
                var data_bytes: usize = data.len;
                while (data_bytes > 0 and data[data_bytes - 1] == 0) data_bytes -= 1;
                out_data_words = @max(out_data_words, (data_bytes + 7) / 8);

                var pc: usize = decl_pointer_count;
                while (pc > 0) {
                    const w = try readWordAt(seg, base + decl_data_words * 8 + (pc - 1) * 8);
                    if (w != 0) break;
                    pc -= 1;
                }
                out_pointer_count = @max(out_pointer_count, pc);
            }
        }

        const out_stride = out_data_words + out_pointer_count;
        const total_words = out_stride * count;
        // layout.c++:1922-1925: we only ever removed words.
        std.debug.assert(total_words <= decl_stride * count);

        const target = try self.allocWords(1 + total_words);
        // Tag word (layout.c++:1930-1932): kind STRUCT, element count in the
        // offset field, uniform truncated sizes.
        self.writeWordAt(target, (@as(u64, @intCast(count)) << 2) |
            (@as(u64, @intCast(out_data_words)) << 32) |
            (@as(u64, @intCast(out_pointer_count)) << 48));
        // List pointer: word count EXCLUDES the tag (layout.c++:1928
        // `setInlineComposite(totalSize)`).
        self.writeWordAt(dst_pos, listPointerWord(dst_pos, target, 7, @intCast(total_words)));

        // layout.c++:1935-1948: copy each element's surviving data words and
        // recurse into its surviving pointers. Child objects append after the
        // whole pre-allocated list block.
        for (0..count) |e| {
            const src_base = list.elements_offset + e * decl_stride * 8;
            const dst_base = target + 8 + e * out_stride * 8;
            @memcpy(
                self.out.items[dst_base..][0 .. out_data_words * 8],
                seg[src_base..][0 .. out_data_words * 8],
            );
            for (0..out_pointer_count) |j| {
                const src_ptr_pos = src_base + decl_data_words * 8 + j * 8;
                const w = try readWordAt(seg, src_ptr_pos);
                try self.copyPointer(dst_base + out_data_words * 8 + j * 8, list.segment_id, src_ptr_pos, w, depth - 1);
            }
        }
    }

    fn structPointerWord(dst_pos: usize, target: usize, data_words: u16, pointer_count: u16) u64 {
        const offset_words: i64 = @divExact(@as(i64, @intCast(target)) - @as(i64, @intCast(dst_pos)) - 8, 8);
        const raw_offset: u32 = @as(u32, @bitCast(@as(i32, @intCast(offset_words)))) & 0x3FFF_FFFF;
        return (@as(u64, raw_offset) << 2) |
            (@as(u64, data_words) << 32) |
            (@as(u64, pointer_count) << 48);
    }

    fn listPointerWord(dst_pos: usize, target: usize, element_size: u3, count_or_words: u32) u64 {
        const offset_words: i64 = @divExact(@as(i64, @intCast(target)) - @as(i64, @intCast(dst_pos)) - 8, 8);
        const raw_offset: u32 = @as(u32, @bitCast(@as(i32, @intCast(offset_words)))) & 0x3FFF_FFFF;
        return 1 |
            (@as(u64, raw_offset) << 2) |
            (@as(u64, element_size) << 32) |
            (@as(u64, count_or_words) << 35);
    }
};

/// Canonicalize `msg` into FRAMED bytes: the 8-byte segment table of a
/// single-segment message followed by the canonical segment.
///
/// The framing is this library's choice of transport-ready shape (the result
/// round-trips through `Message.init`). The reference form itself has NO
/// segment table — `capnp convert binary:canonical` writes the bare segment
/// (compiler/capnp.c++:147,1134-1137), i.e. exactly `result[8..]`. Use
/// `canonicalizeFlat` for that byte-comparable form.
///
/// The root is canonicalized AS A STRUCT, mirroring the only canonicalize
/// entry point the reference exposes (`StructReader::canonicalize`,
/// layout.c++:3008-3020, reached via message.h:556-560): a null root pointer
/// reads as an empty struct and canonicalizes to the offset -1 empty-struct
/// pointer, and a non-struct root is an error.
///
/// The caller owns the returned buffer. `msg` must come from a validating
/// `Message.init`; the walk nonetheless bounds-checks every access and
/// returns errors (never traps) on anything malformed.
pub fn canonicalize(allocator: std.mem.Allocator, msg: *const message.Message) CanonicalizeError![]u8 {
    const flat = try canonicalizeFlat(allocator, msg);
    defer allocator.free(flat);

    const out = try allocator.alloc(u8, 8 + flat.len);
    std.mem.writeInt(u32, out[0..4], 0, .little); // segment count - 1
    std.mem.writeInt(u32, out[4..8], @intCast(flat.len / 8), .little);
    @memcpy(out[8..], flat);
    return out;
}

/// How many builder segments the FromBuilder entry points can view without
/// touching the allocator. Beyond this, one transient index allocation is
/// made and freed before returning.
const builder_view_stack_segments = 16;

/// The FromBuilder entry points promise byte-parity with the
/// serialize -> `Message.init` -> canonicalize round trip, so their error
/// surface is the composed one: `TruncatedMessage` is the round trip's
/// verdict on a builder whose root segment holds no root word.
pub const CanonicalizeFromBuilderError = CanonicalizeError || error{TruncatedMessage};

/// Canonicalize the builder's current contents into the bare canonical
/// segment — byte-identical to
/// `canonicalizeFlat(&Message.init(gpa, try b.toBytes(), .{}))`, without the
/// framed copy, the re-parse, or the redundant validation walk of bytes the
/// local builder itself just wrote. The canonicalization walk still
/// bounds-checks every access and returns errors (never traps) on anything
/// malformed. The caller owns the returned buffer.
pub fn canonicalizeFlatFromBuilder(allocator: std.mem.Allocator, builder: *const message.MessageBuilder) CanonicalizeFromBuilderError![]u8 {
    return canonicalizeBuilderInner(allocator, builder, canonicalizeFlat);
}

/// Framed variant of `canonicalizeFlatFromBuilder` (8-byte single-segment
/// table + canonical segment), byte-identical to `canonicalize` over the
/// builder's serialized round trip. The caller owns the returned buffer.
pub fn canonicalizeFromBuilder(allocator: std.mem.Allocator, builder: *const message.MessageBuilder) CanonicalizeFromBuilderError![]u8 {
    return canonicalizeBuilderInner(allocator, builder, canonicalize);
}

fn canonicalizeBuilderInner(
    allocator: std.mem.Allocator,
    builder: *const message.MessageBuilder,
    comptime canonicalizeFn: fn (std.mem.Allocator, *const message.Message) CanonicalizeError![]u8,
) CanonicalizeFromBuilderError![]u8 {
    // Round-trip parity for the degenerate shapes: a segmentless builder
    // serializes as one zero-word segment, and `Message.init` rejects any
    // root segment without a root word as TruncatedMessage before the
    // canonicalizer would run.
    const n = builder.segments.items.len;
    if (n == 0 or builder.segments.items[0].items.len < 8) return error.TruncatedMessage;

    // A borrowed, never-deinit'd view of the builder's live segments: the
    // canonicalizer only reads `segments` through the resolver helpers, so
    // no parse, copy, or ownership transfer is needed.
    var stack_views: [builder_view_stack_segments][]const u8 = undefined;
    const views: [][]const u8 = if (n <= builder_view_stack_segments)
        stack_views[0..n]
    else
        try allocator.alloc([]const u8, n);
    defer if (n > builder_view_stack_segments) allocator.free(views);
    for (builder.segments.items, 0..) |segment, i| views[i] = segment.items;

    const view: message.Message = .{
        .allocator = allocator,
        .segments = views,
        .segments_owned = false,
        .backing_data = null,
    };
    return canonicalizeFn(allocator, &view);
}

/// Canonicalize `msg` into the bare canonical segment, byte-identical to the
/// reference `canonicalize()` word array (layout.c++:3008-3020) and to
/// `capnp convert binary:canonical` output (compiler/capnp.c++:1134-1137).
pub fn canonicalizeFlat(allocator: std.mem.Allocator, msg: *const message.Message) CanonicalizeError![]u8 {
    var c = Canonicalizer{
        .allocator = allocator,
        .msg = msg,
        .out = std.ArrayList(u8).empty,
    };
    defer c.out.deinit(allocator);

    // Word 0: the root pointer slot.
    _ = try c.allocWords(1);

    if (msg.segments.len == 0) return error.OutOfBounds;
    const seg0 = msg.segments[0];
    const root_word = if (seg0.len >= 8) std.mem.readInt(u64, seg0[0..8], .little) else 0;

    if (root_word == 0) {
        // Null root == empty struct (message.c++ getRootInternal yields the
        // default reader; setStruct then emits the offset -1 form).
        c.writeWordAt(0, 0x0000_0000_FFFF_FFFC);
        return c.out.toOwnedSlice(allocator);
    }

    const resolved = try msg.resolvePointer(0, 0, root_word, 3);
    switch (@as(u2, @truncate(resolved.pointer_word & 0x3))) {
        0 => {
            const data_words = @as(u16, @truncate((resolved.pointer_word >> 32) & 0xFFFF));
            const pointer_count = @as(u16, @truncate((resolved.pointer_word >> 48) & 0xFFFF));
            const offset = decodeOffsetWords(resolved.pointer_word);
            const content = try contentOffset(resolved.pointer_pos, offset, resolved.content_override);
            const seg = msg.segments[resolved.segment_id];
            const total_bytes = (@as(usize, data_words) + @as(usize, pointer_count)) * 8;
            const end = std.math.add(usize, content, total_bytes) catch return error.OutOfBounds;
            if (end > seg.len) return error.OutOfBounds;
            try c.writeStruct(0, resolved.segment_id, content, data_words, pointer_count, default_nesting_limit);
        },
        else => return error.InvalidRootPointer,
    }

    return c.out.toOwnedSlice(allocator);
}

// ---------------------------------------------------------------------------
// isCanonical
// ---------------------------------------------------------------------------

/// Port of `MessageReader::isCanonical` (message.c++:55-83): exactly one
/// segment, the root pointer in its first word, every object exactly at the
/// advancing read head (preorder + dense packing), all truncation rules
/// satisfied, and every word of the segment consumed.
///
/// Structurally malformed input (out-of-bounds pointers etc.) yields `false`,
/// matching the C++ recoverable-REQUIRE behavior of the read path.
pub fn isCanonical(msg: *const message.Message) bool {
    // message.c++:60-74: no segment or more than one segment -> false.
    if (msg.segments.len != 1) return false;
    const seg = msg.segments[0];
    if (seg.len < 8) return false;

    // message.c++:76-82: read head starts after the root pointer word; the
    // root must be canonical and all words consumed.
    var read_head: usize = 1;
    if (!pointerIsCanonical(seg, 0, &read_head)) return false;
    return read_head * 8 == seg.len;
}

/// Port of `PointerReader::isCanonical` (layout.c++:2816-2859).
/// `ptr_pos_words` is the pointer's own location; `read_head` is in words.
fn pointerIsCanonical(seg: []const u8, ptr_pos_words: usize, read_head: *usize) bool {
    const word = readWordAt(seg, ptr_pos_words * 8) catch return false;
    // layout.c++:2817-2820: null pointer is canonical, no read.
    if (word == 0) return true;
    switch (@as(u2, @truncate(word & 0x3))) {
        0 => {
            const data_words = @as(usize, @as(u16, @truncate((word >> 32) & 0xFFFF)));
            const pointer_count = @as(usize, @as(u16, @truncate((word >> 48) & 0xFFFF)));
            const offset = decodeOffsetWords(word);
            const target_signed = @as(i64, @intCast(ptr_pos_words)) + 1 + offset;
            if (target_signed < 0) return false;
            const target: usize = @intCast(target_signed);

            if (data_words == 0 and pointer_count == 0) {
                // layout.c++:2831-2837: a zero-sized struct is canonical iff
                // its target is the pointer's own location (offset -1).
                return target == ptr_pos_words;
            }
            if (target + data_words + pointer_count > seg.len / 8) return false;

            var data_trunc = false;
            var ptr_trunc = false;
            // layout.c++:2849: the top-level struct call aliases readHead as
            // both the object head and the pointer head.
            return structIsCanonical(seg, target, data_words, pointer_count, read_head, read_head, &data_trunc, &ptr_trunc) and
                data_trunc and ptr_trunc;
        },
        1 => return listIsCanonical(seg, ptr_pos_words, word, read_head),
        // layout.c++:2822-2826: far and "other" (capability) pointers are not
        // positional, hence never canonical.
        2, 3 => return false,
    }
}

/// Port of `StructReader::isCanonical` (layout.c++:3031-3071).
fn structIsCanonical(
    seg: []const u8,
    loc_words: usize,
    data_words: usize,
    pointer_count: usize,
    read_head: *usize,
    ptr_head: *usize,
    data_trunc: *bool,
    ptr_trunc: *bool,
) bool {
    // layout.c++:3035-3038: the object must sit exactly at the read head
    // (this is both the preorder and the dense-packing rule).
    if (loc_words != read_head.*) return false;
    if (loc_words + data_words + pointer_count > seg.len / 8) return false;

    // layout.c++:3046-3052: the LAST data word must be non-zero (or there is
    // no data section).
    if (data_words > 0) {
        const last = readWordAt(seg, (loc_words + data_words - 1) * 8) catch return false;
        data_trunc.* = last != 0;
    } else {
        data_trunc.* = true;
    }

    // layout.c++:3054-3058: the LAST pointer must be non-null (or none).
    if (pointer_count > 0) {
        const last = readWordAt(seg, (loc_words + data_words + pointer_count - 1) * 8) catch return false;
        ptr_trunc.* = last != 0;
    } else {
        ptr_trunc.* = true;
    }

    // layout.c++:3061.
    read_head.* += data_words + pointer_count;

    // layout.c++:3064-3068.
    for (0..pointer_count) |i| {
        if (!pointerIsCanonical(seg, loc_words + data_words + i, ptr_head)) return false;
    }
    return true;
}

/// Port of `ListReader::isCanonical` (layout.c++:3266-3351).
fn listIsCanonical(seg: []const u8, ptr_pos_words: usize, word: u64, read_head: *usize) bool {
    const element_size = @as(u3, @truncate((word >> 32) & 0x7));
    const count_or_words = @as(u32, @truncate(word >> 35));
    const offset = decodeOffsetWords(word);
    const target_signed = @as(i64, @intCast(ptr_pos_words)) + 1 + offset;
    if (target_signed < 0) return false;
    const target: usize = @intCast(target_signed);
    const seg_words = seg.len / 8;

    switch (element_size) {
        7 => {
            // layout.c++:3268-3303. `target` is the TAG word; the C++ list
            // reader's `ptr` is target+1, checked against the head AFTER the
            // tag increment.
            if (target != read_head.*) return false;
            read_head.* += 1;
            const tag = readWordAt(seg, target * 8) catch return false;
            if (@as(u2, @truncate(tag & 0x3)) != 0) return false;
            const element_count_signed = decodeOffsetWords(tag);
            if (element_count_signed < 0) return false;
            const element_count: usize = @intCast(element_count_signed);
            const tag_data_words = @as(usize, @as(u16, @truncate((tag >> 32) & 0xFFFF)));
            const tag_pointer_count = @as(usize, @as(u16, @truncate((tag >> 48) & 0xFFFF)));
            const element_words = tag_data_words + tag_pointer_count;

            // layout.c++:3277-3283: the list pointer's word count must be
            // EXACTLY elementCount * elementSize.
            const total_words_u64 = @as(u64, element_count) * @as(u64, element_words);
            if (total_words_u64 != count_or_words) return false;
            const total_words: usize = @intCast(total_words_u64);
            if (read_head.* + total_words > seg_words) return false;

            // layout.c++:3284-3286: zero-sized elements are fine as-is.
            if (element_words == 0) return true;

            const list_end = read_head.* + total_words;
            var ptr_head = list_end;
            var list_data_trunc = false;
            var list_ptr_trunc = false;
            for (0..element_count) |_| {
                var data_trunc = false;
                var ptr_trunc = false;
                if (!structIsCanonical(seg, read_head.*, tag_data_words, tag_pointer_count, read_head, &ptr_head, &data_trunc, &ptr_trunc)) {
                    return false;
                }
                list_data_trunc = list_data_trunc or data_trunc;
                list_ptr_trunc = list_ptr_trunc or ptr_trunc;
            }
            // layout.c++:3299-3302: at least one element must NEED the full
            // uniform data size and one the full pointer count.
            read_head.* = ptr_head;
            return list_data_trunc and list_ptr_trunc;
        },
        6 => {
            // layout.c++:3305-3315.
            if (target != read_head.*) return false;
            const element_count: usize = count_or_words;
            if (read_head.* + element_count > seg_words) return false;
            read_head.* += element_count;
            for (0..element_count) |i| {
                if (!pointerIsCanonical(seg, target + i, read_head)) return false;
            }
            return true;
        },
        else => {
            // layout.c++:3317-3348: data lists. Everything between the last
            // element and the word boundary must be zero, including the high
            // bits of a trailing partial byte.
            if (target != read_head.*) return false;
            const bit_size = @as(u64, count_or_words) * dataBitsPerElement(element_size);
            const truncated_bytes: usize = @intCast(bit_size / 8);
            const total_words: usize = @intCast((bit_size + 63) / 64);
            if (read_head.* + total_words > seg_words) return false;

            var byte_pos = target * 8 + truncated_bytes;
            const end_byte = (target + total_words) * 8;

            const leftover_bits: u6 = @intCast(bit_size % 8);
            if (leftover_bits > 0) {
                const mask: u8 = ~((@as(u8, 1) << @intCast(leftover_bits)) - 1);
                if (byte_pos >= seg.len) return false;
                if (seg[byte_pos] & mask != 0) return false;
                byte_pos += 1;
            }
            while (byte_pos < end_byte) : (byte_pos += 1) {
                if (seg[byte_pos] != 0) return false;
            }

            read_head.* = target + total_words;
            return true;
        },
    }
}
