//! One resolver for "read this list pointer as a list of N-byte elements".
//!
//! Every reader that can be handed a list pointer shares it: a struct's own
//! fields (`StructReader.read*List`), a nested element one level down
//! (`PointerListReader.getU32List` and friends), and the type-erased
//! `AnyPointerReader.getPointerList`. The alternative — each of the three
//! deciding for itself — is exactly how a downgrade ends up implemented on one
//! surface and silently wrong on another.
//!
//! Deliberately an internal module, imported directly by `message.zig`,
//! `list_readers.zig` and `any_pointer_reader.zig` rather than threaded through
//! their `define()` helpers. `define()`'s argument list renders inside every
//! generated reader type name, so a new parameter there would rewrite ~80 lines
//! of the frozen `docs/api-snapshot.txt` for an internal refactor. Nothing here
//! is reachable from a `pub` signature, so the snapshot does not move.

const std = @import("std");
const bounds = @import("bounds.zig");

inline fn checkedAddUsize(a: usize, b: usize) error{OutOfBounds}!usize {
    return std.math.add(usize, a, b) catch return error.OutOfBounds;
}

/// Bytes of segment content occupied by a list of `element_count` elements of
/// element size `element_size`.
///
/// Lives here, not in `message.zig`, so this module can bounds-check without
/// importing back. `message.listContentBytes` delegates to it and keeps its own
/// name (that name is rendered into the frozen API snapshot by way of the
/// `define()` calls, so it must not become an alias).
///
/// Inline-composite (C = 7) is rejected rather than sized: its D field is a
/// word count, so it is not the same kind of quantity at all.
pub fn listContentBytes(element_size: u3, element_count: u32) !usize {
    const count = @as(u64, element_count);
    const total: u64 = switch (element_size) {
        0 => 0,
        1 => (count + 7) / 8,
        2 => count,
        3 => count * 2,
        4 => count * 4,
        5 => count * 8,
        6 => count * 8,
        else => return error.InvalidPointer,
    };
    if (total > std.math.maxInt(usize)) return error.ListTooLarge;
    return @as(usize, @intCast(total));
}

/// Per-element view of a list being read as a list of primitives or pointers.
///
/// Deliberately carries no defaults: every construction site is an exact
/// four-field literal, so adding a field here is a compile error at each of
/// them rather than a silently dropped value.
pub const ElementListView = struct {
    segment_id: u32,
    /// Byte offset of element 0's *first read unit* — the element itself for a
    /// primitive list, the element's first pointer word for a pointer list.
    elements_offset: usize,
    element_count: u32,
    /// Distance between consecutive elements in BYTES, or 0 for the natural,
    /// tightly packed stride of the element type.
    stride_bytes: u32,
};

/// The inverse of the list-upgrade rule: decode a correctly encoded struct list
/// (element size C = 7) as the primitive or pointer list an older schema
/// declares for the same field.
///
/// The forward rule (`message.StructListLayout.fromUpgradedList`) lets a peer
/// that evolved `List(UInt32)` into `List(SomeStruct)` read old data. This is
/// the direction the *old* binary needs: it must read the struct list the
/// evolved peer now writes. Both reference implementations accept it, so
/// rejecting it rejects messages every other implementation reads.
///
/// The preconditions are the amplification mitigation, not politeness, and are
/// taken from the C++ reference (`layout.c++`, `readListPointer`, the
/// `checkElementSize` switch over `expectedElementSize`): a primitive list needs
/// a non-empty data section, a pointer list needs at least one pointer, and a
/// bit list is never satisfiable from a struct list. Without them a struct list
/// carrying no data at all would synthesize readable elements for free.
///
/// `data_words >= 1` also means every element carries at least eight bytes of
/// data, so no primitive read — eight bytes wide at most — can reach past an
/// element's data section into its pointers. That is why C++ checks whole words
/// here rather than comparing against the requested element width, and why this
/// does the same.
pub fn downgradeInlineCompositeList(
    list: anytype,
    expected_element_size: u3,
) !ElementListView {
    const data_bytes = @as(u32, list.data_words) * 8;
    const elements_offset = switch (expected_element_size) {
        // C++ hard-fails this one ("upgrading boolean lists to structs is no
        // longer supported"); matching it is convergence, not politeness.
        1 => return error.InvalidPointer,
        2, 3, 4, 5 => blk: {
            if (list.data_words == 0) return error.InvalidPointer;
            break :blk list.elements_offset;
        },
        // An element's pointer section starts after its data section. This
        // `+ data_bytes` is exactly what go-capnp's `TextList.At` omits (while
        // its own `PointerList.At` applies it), so go reads the wrong word
        // here; the C++ reference is the only cross-check for this arm.
        6 => blk: {
            if (list.pointer_words == 0) return error.InvalidPointer;
            break :blk try checkedAddUsize(list.elements_offset, @as(usize, data_bytes));
        },
        // Void and struct lists never route through here: `readVoidList` and
        // `readStructList` have their own paths. Answered defensively rather
        // than with `unreachable` so a future caller cannot turn a reachable
        // path into undefined behavior.
        0, 7 => return error.InvalidPointer,
    };
    return .{
        .segment_id = list.segment_id,
        .elements_offset = elements_offset,
        .element_count = list.element_count,
        // The struct's total size, which is what separates consecutive
        // elements. Non-zero: both surviving arms require a non-empty section.
        .stride_bytes = data_bytes + @as(u32, list.pointer_words) * 8,
    };
}

/// Resolve `pointer_word` (stored at `pointer_pos` in `segment_id`) for reading
/// as a list of `expected_element_size` elements, accepting both encodings a
/// conformant peer may have used: a list pointer that already carries that
/// element size, and — per the inverse of the list-upgrade rule — a struct list
/// (C = 7) whose elements are wide enough to satisfy the request.
///
/// Text, Data and `List(Bool)` deliberately do not come through here: the C++
/// reference requires `ElementSize::BYTE` for the blobs and hard-fails
/// composite-as-bit, so relaxing those would diverge from the reference rather
/// than converge with it.
///
/// `content_bytes` is `listContentBytes` — passed in rather than called
/// directly so each caller keeps the *exact* error set it had before this
/// resolver existed. `PointerListReader`'s eleven frozen `get*List` signatures
/// render `anyerror!` because they route through `define()`'s
/// `*const fn (u3, u32) anyerror!usize` argument; calling the concrete function
/// here instead would tighten all eleven to a named set, which is a change to a
/// frozen contract and belongs in its own reviewed commit, not as a side effect
/// of sharing a resolver. `StructReader` and `AnyPointerReader` pass concrete
/// functions and keep their concrete sets.
pub fn resolve(
    message: anytype,
    segment_id: u32,
    pointer_pos: usize,
    pointer_word: u64,
    expected_element_size: u3,
    comptime content_bytes: anytype,
) !ElementListView {
    if (pointer_word == 0) return error.InvalidPointer;

    // `resolveInlineCompositeList` is the *classifier* here, not merely the
    // decoder. It succeeds on exactly the pointers that really are C = 7 —
    // including the double-far shapes where `resolveListPointer` would report
    // "not a list", because for a double-far in the layout this builder writes
    // the landing pad's second word is the struct *tag*, which has pointer type
    // 0.
    //
    // And it is the only correct way in: for C = 7 `resolveListPointer` reports
    // the pointer's D field (a WORD count, not an element count) and a content
    // offset addressing the TAG word rather than the elements. A reader built
    // from that has the wrong length AND a base one word early — it compiles,
    // runs, and returns garbage with no error.
    //
    // A *failure* here is deliberately swallowed rather than propagated: the
    // plain path below then produces exactly the error it produced before this
    // rule existed, so no malformed pointer changes diagnosis. (It also keeps
    // `error.InvalidInlineCompositePointer` out of these readers' error sets,
    // where it would widen a frozen public contract.) Note this only swallows
    // a failure to *classify*: once the pointer is known to be C = 7, a
    // precondition rejection from `downgradeInlineCompositeList` is final and
    // never falls back.
    if (message.resolveInlineCompositeList(segment_id, pointer_pos, pointer_word)) |list| {
        return downgradeInlineCompositeList(list, expected_element_size);
    } else |_| {}

    const list = try message.resolveListPointer(segment_id, pointer_pos, pointer_word);
    if (list.element_size != expected_element_size) return error.InvalidPointer;

    const total_bytes = try content_bytes(list.element_size, list.element_count);
    try bounds.checkListContentBounds(message.segments, list.segment_id, list.content_offset, total_bytes);

    return .{
        .segment_id = list.segment_id,
        .elements_offset = list.content_offset,
        .element_count = list.element_count,
        .stride_bytes = 0,
    };
}

/// Resolve any well-formed list pointer as `List(Void)`, returning its logical
/// element count. For C = 7 the pointer's D field is a word count and the
/// element count lives in the struct tag, so a malformed inline-composite tag
/// must be terminal rather than falling back to the ordinary-list view.
///
/// Resolve the ordinary shape first because it also classifies direct,
/// single-far, and layout-B double-far pointers. Layout-A double-far struct
/// lists resolve to a tag word (pointer type 0) through that path, so only that
/// shape needs the failure path into the inline-composite decoder.
pub fn resolveVoidElementCount(
    message: anytype,
    segment_id: u32,
    pointer_pos: usize,
    pointer_word: u64,
) !u32 {
    if (pointer_word == 0) return error.InvalidPointer;

    const list = message.resolveListPointer(segment_id, pointer_pos, pointer_word) catch {
        return (try message.resolveInlineCompositeList(segment_id, pointer_pos, pointer_word)).element_count;
    };
    if (list.element_size == 7) {
        return (try message.resolveInlineCompositeList(segment_id, pointer_pos, pointer_word)).element_count;
    }
    return list.element_count;
}
