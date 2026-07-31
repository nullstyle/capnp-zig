const std = @import("std");
const log = std.log.scoped(.rpc_cap_table);
const protocol = @import("../wire/protocol.zig");
const descriptors = @import("descriptors.zig");

const OwnedPromisedAnswer = descriptors.OwnedPromisedAnswer;

/// Tracks capability import/export state for an RPC connection.
///
/// Manages import reference counts, export ID allocation, promise-export
/// markers, and receiver-answer entries used for promise pipelining. Each
/// `Peer` owns one `CapTable`.
pub const max_table_size: u32 = 10_000;

/// Out-of-band handoff record for a capability being ORIGINATED to a third
/// party. A `thirdPartyHosted` descriptor needs both a `ThirdPartyToContact`
/// (an AnyPointer, which cannot be squeezed into the bare cap id the outbound
/// classifier works with) and the vine export id. `sendProvide` records one of
/// these keyed by the provided cap id; the outbound emit loop consults the map
/// and, when a cap is marked, writes `thirdPartyHosted{ id, vineId }` instead of
/// a two-party descriptor.
pub const ThirdPartyHostedRecord = struct {
    /// Serialized `ThirdPartyToContact` AnyPointer message (owned by the table).
    contact_payload: []u8,
    /// Vine export id anchoring the handoff's refcount/liveness.
    vine_id: u32,
};

pub const CapTable = struct {
    allocator: std.mem.Allocator,
    exports: std.AutoHashMap(u32, void),
    imports: std.AutoHashMap(u32, ImportEntry),
    promised_exports: std.AutoHashMap(u32, void),
    receiver_answers: std.AutoHashMap(u32, OwnedPromisedAnswer),
    /// Caps being handed off to a third party (cap id -> handoff record). Owns
    /// the contact payload bytes.
    third_party_hosted: std.AutoHashMap(u32, ThirdPartyHostedRecord),
    next_export_id: u32 = 0,

    pub fn init(allocator: std.mem.Allocator) CapTable {
        return .{
            .allocator = allocator,
            .exports = std.AutoHashMap(u32, void).init(allocator),
            .imports = std.AutoHashMap(u32, ImportEntry).init(allocator),
            .promised_exports = std.AutoHashMap(u32, void).init(allocator),
            .receiver_answers = std.AutoHashMap(u32, OwnedPromisedAnswer).init(allocator),
            .third_party_hosted = std.AutoHashMap(u32, ThirdPartyHostedRecord).init(allocator),
        };
    }

    pub fn deinit(self: *CapTable) void {
        self.exports.deinit();
        self.imports.deinit();
        self.promised_exports.deinit();
        var answer_it = self.receiver_answers.valueIterator();
        while (answer_it.next()) |answer| {
            answer.deinit(self.allocator);
        }
        self.receiver_answers.deinit();
        var tph_it = self.third_party_hosted.valueIterator();
        while (tph_it.next()) |record| self.allocator.free(record.contact_payload);
        self.third_party_hosted.deinit();
    }

    /// Mark `cap_id` as being handed off to a third party. `contact_payload` is
    /// the serialized `ThirdPartyToContact` AnyPointer; the table copies it. The
    /// next outbound descriptor emitted for `cap_id` resolves to
    /// `thirdPartyHosted{ id = contact, vineId }`. Replaces any prior mark.
    pub fn markThirdPartyHosted(self: *CapTable, cap_id: u32, contact_payload: []const u8, vine_id: u32) !void {
        const owned = try self.allocator.dupe(u8, contact_payload);
        errdefer self.allocator.free(owned);
        const gop = try self.third_party_hosted.getOrPut(cap_id);
        if (gop.found_existing) self.allocator.free(gop.value_ptr.contact_payload);
        gop.value_ptr.* = .{ .contact_payload = owned, .vine_id = vine_id };
    }

    /// Look up the handoff record for `cap_id`, or null if it is not being
    /// handed off. The returned payload aliases table-owned memory.
    pub fn getThirdPartyHosted(self: *const CapTable, cap_id: u32) ?ThirdPartyHostedRecord {
        return self.third_party_hosted.get(cap_id);
    }

    /// Clear the handoff mark on `cap_id`, freeing its contact payload.
    pub fn clearThirdPartyHosted(self: *CapTable, cap_id: u32) void {
        if (self.third_party_hosted.fetchRemove(cap_id)) |removed| {
            self.allocator.free(removed.value.contact_payload);
        }
    }

    pub fn totalEntries(self: *const CapTable) u32 {
        return @as(u32, @intCast(self.exports.count())) +
            @as(u32, @intCast(self.imports.count())) +
            @as(u32, @intCast(self.receiver_answers.count()));
    }

    /// Allocate a unique export ID that does not collide with any existing
    /// import, export, or receiver-answer entry.
    pub fn allocExportId(self: *CapTable) error{CapTableFull}!u32 {
        return self.allocLocalCapId();
    }

    pub fn noteExport(self: *CapTable, export_id: u32) !void {
        try self.exports.put(export_id, {});
    }

    /// Register a caller-chosen export id in the export identity set,
    /// enforcing the same table-size bound as `allocExportId`. Re-adding an
    /// id that is already present is a no-op. `allocExportId` skips ids
    /// registered here, so explicit ids and locally allocated ids never
    /// collide.
    pub fn noteExportAt(self: *CapTable, export_id: u32) !void {
        if (self.exports.contains(export_id)) return;
        try self.ensureCanAddEntry();
        try self.exports.put(export_id, {});
    }

    pub fn clearExport(self: *CapTable, export_id: u32) void {
        _ = self.exports.remove(export_id);
        _ = self.promised_exports.remove(export_id);
    }

    pub fn hasExport(self: *const CapTable, export_id: u32) bool {
        return self.exports.contains(export_id);
    }

    pub fn markExportPromise(self: *CapTable, export_id: u32) !void {
        // Keep promise IDs in the same local export identity set used for
        // outbound descriptor classification.
        try self.exports.put(export_id, {});
        errdefer _ = self.exports.remove(export_id);
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

    pub fn hasReceiverAnswer(self: *const CapTable, cap_id: u32) bool {
        return self.receiver_answers.contains(cap_id);
    }

    pub fn getReceiverAnswer(self: *const CapTable, cap_id: u32) ?OwnedPromisedAnswer {
        return self.receiver_answers.get(cap_id);
    }

    pub fn takeReceiverAnswer(self: *CapTable, cap_id: u32) ?OwnedPromisedAnswer {
        if (self.receiver_answers.fetchRemove(cap_id)) |removed| {
            return removed.value;
        }
        return null;
    }

    /// Record that a capability with `remote_id` was received from the remote
    /// peer. Increments the reference count if already known.
    pub fn noteImport(self: *CapTable, remote_id: u32) !void {
        if (self.imports.getEntry(remote_id)) |entry| {
            entry.value_ptr.ref_count = std.math.add(u32, entry.value_ptr.ref_count, 1) catch return error.RefCountOverflow;
            return;
        }

        try self.ensureCanAddEntry();
        try self.imports.put(remote_id, .{ .ref_count = 1 });
    }

    pub fn hasImport(self: *const CapTable, remote_id: u32) bool {
        return self.imports.contains(remote_id);
    }

    /// Decrement the wire reference count for an imported capability.
    ///
    /// Returns true if the import's wire reference count reached zero AND the
    /// caller should treat the import as fully released for wire purposes (send
    /// its Release, clear resolved-import state). The import table entry itself
    /// is retained while a promise-held pin (`promise_ref_count`) survives, so a
    /// promise export that resolved to this import keeps a live entry to route
    /// through even after the remote's own wire references are gone. The pin is
    /// a purely local lifetime lease — it never corresponds to an outbound wire
    /// reference, so it does not affect the return value here.
    pub fn releaseImport(self: *CapTable, remote_id: u32) bool {
        var entry = self.imports.getEntry(remote_id) orelse return false;
        if (entry.value_ptr.ref_count > 1) {
            entry.value_ptr.ref_count -= 1;
            return false;
        }
        if (entry.value_ptr.ref_count == 0) return false;
        entry.value_ptr.ref_count = 0;
        // RETENTION under a handoff pin: wire references are exhausted, but a
        // live handoff pin keeps the entry AND returns false — for cleanup
        // purposes the import is NOT released. Firing the resolved-import
        // cleanup eagerly here would recurse through releaseResolvedCap into
        // another import's refs mid-handoff; the unpin
        // (Peer.releaseHandoffImportPin) performs the deferred removal +
        // cleanup instead.
        if (entry.value_ptr.handoff_pin_count > 0) return false;
        // Wire references are exhausted. Keep the entry alive if a promise-held
        // pin still leases it; otherwise remove it. Either way the wire side is
        // fully released (return true) so the Release frame is still sent once.
        if (entry.value_ptr.promise_ref_count == 0) {
            _ = self.imports.remove(remote_id);
        }
        return true;
    }

    /// Take a promise-held pin on an import: a promise export that resolved to
    /// this import (`Peer.resolvePromiseExportToImport`) leases the import for
    /// its own lifetime so an inbound Release that zeroes the wire `ref_count`
    /// cannot drop the entry while the promise still routes calls to it. The pin
    /// is local — it corresponds to NO wire reference, so it must never trigger
    /// an outbound Release. The import must already exist.
    pub fn notePromiseImportRef(self: *CapTable, remote_id: u32) !void {
        var entry = self.imports.getEntry(remote_id) orelse return error.UnknownImport;
        entry.value_ptr.promise_ref_count = std.math.add(u32, entry.value_ptr.promise_ref_count, 1) catch return error.RefCountOverflow;
    }

    /// Take a handoff pin on an import: a live provision whose receiverHosted
    /// target this import is (the Provide-time pin), or a cross-peer proxy
    /// forwarding to it (the serve-time pin), leases the import so the
    /// introducer draining its own wire refs between Provide and Accept cannot
    /// destroy the capability the provision still has to serve. Unlike the
    /// promise pin, a handoff pin ALSO withholds outbound Release frames while
    /// it lives (`deferReleaseWhilePinned`): a receiverHosted descriptor grants
    /// no transferable wire reference, so the withheld count is the only thing
    /// keeping the far-side export alive across the handoff window. The import
    /// must already exist.
    pub fn noteHandoffImportPin(self: *CapTable, remote_id: u32) error{ RefCountOverflow, UnknownImport }!void {
        var entry = self.imports.getEntry(remote_id) orelse return error.UnknownImport;
        entry.value_ptr.handoff_pin_count = std.math.add(u32, entry.value_ptr.handoff_pin_count, 1) catch return error.RefCountOverflow;
    }

    /// Roll back a handoff pin taken by an OOM ladder that has NOT yet
    /// transferred ownership: a plain decrement — no deferred emission, no
    /// removal check. Safe exactly because the ladder between the note and
    /// this rollback performs no import releases, so nothing can have
    /// accumulated into `deferred_release` under this pin. Never used after a
    /// transfer: the transferee unpins via `releaseHandoffImportPin`.
    pub fn rollbackHandoffImportPin(self: *CapTable, remote_id: u32) void {
        var entry = self.imports.getEntry(remote_id) orelse return;
        if (entry.value_ptr.handoff_pin_count == 0) return;
        entry.value_ptr.handoff_pin_count -= 1;
    }

    /// WITHHOLD seam helper for the peer's `send_release` callback: when
    /// `remote_id` is pinned by a live handoff (`handoff_pin_count > 0`), fold
    /// `count` into the entry's deferred wire-release tally and return true —
    /// the caller must NOT emit a Release frame now. Otherwise return false
    /// and leave the entry untouched (an import alive on `promise_ref_count`
    /// alone still releases eagerly). The deferred tally is emitted — and
    /// zeroed — by the peer at the last unpin (`releaseHandoffImportPin`).
    pub fn deferReleaseWhilePinned(self: *CapTable, remote_id: u32, count: u32) error{RefCountOverflow}!bool {
        var entry = self.imports.getEntry(remote_id) orelse return false;
        if (entry.value_ptr.handoff_pin_count == 0) return false;
        entry.value_ptr.deferred_release = std.math.add(u32, entry.value_ptr.deferred_release, count) catch return error.RefCountOverflow;
        return true;
    }

    pub const HandoffImportUnpin = struct {
        /// This call dropped the LAST handoff pin; `deferred_release` below was
        /// taken (and zeroed on the entry) and must be emitted by the caller.
        last_pin_released: bool,
        /// Withheld wire-release count taken by this unpin (0 unless
        /// `last_pin_released`).
        deferred_release: u32,
    };

    /// Bookkeeping half of the handoff unpin (the peer emits the frame): drop
    /// one pin; when the LAST pin drops, take — and zero — the deferred
    /// wire-release tally, whether or not the emission that follows succeeds.
    /// A second pin/unpin cycle therefore starts from 0: no stale re-emission,
    /// no ReleaseCountExceeded at a compliant introducer, no wrong-object
    /// decrement after id recycling. Never removes the entry — removal (and
    /// the resolved-import cleanup retention deferred) happens via
    /// `removeImportIfFullyReleased` AFTER the caller's emission, so a send
    /// that reenters the table never sees a half-removed entry. Unknown id /
    /// underflow are warn-only no-ops, matching the other release paths.
    pub fn releaseHandoffImportPin(self: *CapTable, remote_id: u32) HandoffImportUnpin {
        var entry = self.imports.getEntry(remote_id) orelse {
            log.warn("handoff unpin for unknown import id={}", .{remote_id});
            return .{ .last_pin_released = false, .deferred_release = 0 };
        };
        if (entry.value_ptr.handoff_pin_count == 0) {
            log.warn("handoff unpin underflow for import id={}", .{remote_id});
            return .{ .last_pin_released = false, .deferred_release = 0 };
        }
        entry.value_ptr.handoff_pin_count -= 1;
        if (entry.value_ptr.handoff_pin_count != 0) {
            return .{ .last_pin_released = false, .deferred_release = 0 };
        }
        const deferred = entry.value_ptr.deferred_release;
        entry.value_ptr.deferred_release = 0;
        return .{ .last_pin_released = true, .deferred_release = deferred };
    }

    /// Removal half of the handoff unpin: after the deferred emission, remove
    /// the entry iff no wire refs, promise pins, or handoff pins remain.
    /// Returns true when removed here — the caller must then perform the same
    /// resolved-import cleanup the generic release path performs on removal
    /// (retention-under-pin deferred exactly that cleanup). Re-checks the live
    /// entry state rather than trusting a pre-send snapshot, so a send
    /// callback that reentered the table cannot desynchronize the check.
    pub fn removeImportIfFullyReleased(self: *CapTable, remote_id: u32) bool {
        const entry = self.imports.getEntry(remote_id) orelse return false;
        if (entry.value_ptr.ref_count != 0) return false;
        if (entry.value_ptr.promise_ref_count != 0) return false;
        if (entry.value_ptr.handoff_pin_count != 0) return false;
        _ = self.imports.remove(remote_id);
        return true;
    }

    /// Release one promise-held pin taken by `notePromiseImportRef`. Removes the
    /// import entry once no wire references and no other pins — promise OR
    /// handoff — remain. Never sends a wire Release (the pin was never a wire
    /// reference); returns true iff the entry was removed here. An entry still
    /// leased by a handoff pin survives; its removal (and deferred-Release
    /// emission) belongs to the handoff unpin.
    pub fn releasePromiseImportRef(self: *CapTable, remote_id: u32) bool {
        var entry = self.imports.getEntry(remote_id) orelse return false;
        if (entry.value_ptr.promise_ref_count == 0) return false;
        entry.value_ptr.promise_ref_count -= 1;
        if (entry.value_ptr.promise_ref_count == 0 and
            entry.value_ptr.ref_count == 0 and
            entry.value_ptr.handoff_pin_count == 0)
        {
            _ = self.imports.remove(remote_id);
            return true;
        }
        return false;
    }

    fn allocLocalCapId(self: *CapTable) error{CapTableFull}!u32 {
        try self.ensureCanAddEntry();
        var iterations: u32 = 0;
        while (iterations < max_table_size + 1) : (iterations += 1) {
            const id = self.next_export_id;
            self.next_export_id +%= 1;
            if (self.exports.contains(id)) continue;
            if (self.imports.contains(id)) continue;
            if (self.receiver_answers.contains(id)) continue;
            return id;
        }
        log.err("cap table full after exhaustive ID search", .{});
        return error.CapTableFull;
    }

    fn ensureCanAddEntry(self: *const CapTable) error{CapTableFull}!void {
        const total = self.totalEntries();
        if (total >= max_table_size) {
            log.err("cap table full ({} entries)", .{total});
            return error.CapTableFull;
        }
        if (total >= max_table_size * 9 / 10) {
            log.warn("cap table near full: {}/{} entries", .{ total, max_table_size });
        }
    }
};

const ImportEntry = struct {
    /// Wire references: one per cap descriptor the remote handed us for this
    /// import, released by outbound Release messages when we drop them.
    ref_count: u32,
    /// Promise-held pins: local lifetime leases taken when one of our promise
    /// exports resolves to this import (see `notePromiseImportRef`). NOT wire
    /// references — releasing a pin never sends a Release. The entry survives
    /// while any count is non-zero.
    promise_ref_count: u32 = 0,
    /// Handoff pins: leases taken by a live provision whose receiverHosted
    /// target this import is, or by a cross-peer proxy forwarding to it (see
    /// `noteHandoffImportPin`). While non-zero the entry is retained AND every
    /// outbound wire Release for this import is WITHHELD into
    /// `deferred_release` — a receiverHosted grant carries no transferable
    /// wire reference, so an eagerly-emitted Release would let the introducer
    /// destroy the very capability a pending Accept still has to reach.
    handoff_pin_count: u32 = 0,
    /// Wire-release count withheld while `handoff_pin_count > 0`, emitted as
    /// one Release frame (and zeroed) when the last handoff pin drops. Keeps
    /// granted == released across the handoff window.
    deferred_release: u32 = 0,
};
