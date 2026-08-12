const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../../caps/table.zig");
const events = @import("../../events.zig");
const protocol = @import("../../wire/protocol.zig");
const finish = @import("../finish.zig");
const peer_outbound_control = @import("../peer_outbound_control.zig");
const peer_provision_drain = @import("./peer_provision_drain.zig");
const provide_accept_join = @import("../provide_accept_join.zig");
const state = @import("../state.zig");
const third_party = @import("../third_party.zig");
const vat_provisions = @import("../../vat/provisions.zig");

/// Experimental L3 vat-wide provision HOSTING (VatC), extracted from
/// `peer/mod.zig` and made generic over the peer type (the JoinCoordinator
/// extraction contract). Implements the FINAL-v2 hosting design as amended by
/// its adversarial verdict: Provide registration/adoption, Accept
/// index-routing (park / queue-embargoed / serve), parked-accept TTL sweeps,
/// provision embargo release, and the cross-peer serve path. `peer/mod.zig`
/// keeps every caller-visible name as a thunk on `Peer`, so signatures, hook
/// fn-types, and the api-snapshot rendering are unchanged.
///
/// The canonical drain/teardown choreography lives in the sibling
/// `peer_provision_drain.zig` (`Drain` below); hosting paths call into it for
/// the shared fail/detach helpers so every release of a queued/parked
/// Accept's +1 goes through one procedure.
pub fn Hosting(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const ProvideTarget = state.ProvideTarget;
        const ProvideEntry = state.ProvideEntry;
        const PendingEmbargoedAccept = state.PendingEmbargoedAccept;
        const Drain = peer_provision_drain.Drain(Peer);

        pub fn attachProvisionIndex(self: *Peer, index: *ProvisionIndex) !void {
            self.assertThreadAffinity();
            index.assertThreadAffinity();
            if (self.provision_index != null) return error.PeerAlreadyAttachedToProvisionIndex;
            if (self.provides_by_question.count() != 0 or
                self.pending_accepts_by_embargo.count() != 0 or
                self.provisions_by_question.count() != 0 or
                self.cross_peer_pending_accepts.count() != 0)
            {
                return error.PeerAlreadyHasHandoffState;
            }
            try index.attached_peers.append(index.allocator, self);
            self.provision_index = index;
        }

        /// Detach from the provision index. Symmetric precondition: no live
        /// provisions owned by this peer and no queued cross-peer accepts — a
        /// cleanly detached peer can re-attach (its handoff maps are empty).
        pub fn detachProvisionIndex(self: *Peer) !void {
            self.assertThreadAffinity();
            const idx = self.provision_index orelse return;
            if (self.provisions_by_question.count() != 0 or
                self.cross_peer_pending_accepts.count() != 0)
            {
                return error.PeerHasActiveHandoffState;
            }
            var i: usize = 0;
            while (i < idx.attached_peers.items.len) {
                if (idx.attached_peers.items[i] == self) {
                    _ = idx.attached_peers.swapRemove(i);
                } else i += 1;
            }
            self.provision_index = null;
        }

        /// Allocator-parameterized clone of a provide target (the provision owns
        /// COPIES on the INDEX allocator; `Peer.cloneProvideTarget` clones on the
        /// peer allocator and stays untouched).
        fn cloneProvideTargetWith(allocator: std.mem.Allocator, target: *const ProvideTarget) !ProvideTarget {
            return switch (target.*) {
                .local => |t| .{ .local = t },
                .promised => |promised| .{ .promised = try cap_table.OwnedPromisedAnswer.fromQuestionAndOps(
                    allocator,
                    promised.question_id,
                    promised.ops,
                ) },
            };
        }

        /// Roll back a handoff pin taken by a ladder that has NOT yet transferred
        /// ownership (plain decrement, no destroy check — the pin was never
        /// exposed). Never used after a transfer: the transferee releases.
        pub fn rollbackHandoffExportRef(self: *Peer, id: u32) void {
            var entry = self.exports.getEntry(id) orelse return;
            if (entry.value_ptr.handoff_ref_count == 0) return;
            entry.value_ptr.handoff_ref_count -= 1;
        }

        /// PHASE A of Provide registration into the vat index (OOM ladder: all
        /// fallible work first, one infallible tail takes refs/flags/state).
        /// Errors are rolled back by the caller (clearProvide + abort). `adopted`
        /// is reserved for Accept-before-Provide adoption (parking landing).
        pub fn registerProvisionForProvide(
            self: *Peer,
            idx: *ProvisionIndex,
            provide_question_id: u32,
            adopted: *?*ProvisionIndex.Provision,
        ) !void {
            idx.assertThreadAffinity();
            adopted.* = null;
            const entry = self.provides_by_question.getPtr(provide_question_id) orelse
                return error.UnknownProvision;

            if (idx.by_key.get(entry.recipient_key)) |existing| {
                if (existing.state != .awaiting) return error.DuplicateProvideRecipient;
                // ADOPTION: early Accepts parked under this token — activate the
                // awaiting provision in place. Errdefers restore `.awaiting` on
                // any failure; parked entries are untouched in phase A.
                const prov = existing;
                prov.target = try cloneProvideTargetWith(idx.allocator, &entry.target);
                errdefer if (prov.target) |*t| {
                    t.deinit(idx.allocator);
                    prov.target = null;
                };
                var pinned_export: ?u32 = null;
                var pinned_import: ?u32 = null;
                switch (entry.target) {
                    .local => |t| {
                        const tag = try cap_table.descriptors.tagForOriginCode(t.origin_code);
                        if (tag == .senderHosted or tag == .senderPromise) {
                            try self.noteHandoffExportRef(t.cap_id);
                            pinned_export = t.cap_id;
                        } else if (tag == .receiverHosted) {
                            // Provide-time IMPORT pin (the receiverHosted lift):
                            // covers the [Provide, close) window — see the window
                            // note in serveProvisionOnPeer. CRITICAL: import and
                            // export ids share one numeric space per connection,
                            // so this pin is recorded in the SEPARATE
                            // `target_import_pinned` flag; reusing
                            // `target_export_pinned` would make
                            // closeProvisionAsOwner unpin the WRONG TABLE by a
                            // colliding bare id, silently.
                            try self.noteHandoffImportPin(t.cap_id);
                            pinned_import = t.cap_id;
                        }
                    },
                    .promised => {},
                }
                errdefer if (pinned_export) |id| rollbackHandoffExportRef(self, id);
                errdefer if (pinned_import) |id| self.rollbackHandoffImportPin(id);
                try self.provisions_by_question.put(provide_question_id, prov);
                // INFALLIBLE TAIL.
                prov.state = .active;
                prov.owner = self;
                prov.provide_question_id = provide_question_id;
                prov.target_export_pinned = pinned_export != null;
                prov.target_import_pinned = pinned_import != null;
                prov.retain(); // the owner map's +1
                adopted.* = prov;
                return;
            }
            if (idx.provision_count >= idx.limits.max_provisions) return error.ProvisionBudgetExceeded;
            if (idx.provision_key_bytes + entry.recipient_key.len > idx.limits.max_provision_key_bytes)
                return error.ProvisionBudgetExceeded;

            // 1. Create (index allocator).
            const prov = try idx.allocator.create(ProvisionIndex.Provision);
            errdefer idx.allocator.destroy(prov);
            prov.* = .{
                .allocator = idx.allocator,
                .recipient_key = &.{},
                .embargoes = std.StringHashMap(ProvisionIndex.ProvisionEmbargo).init(idx.allocator),
            };
            errdefer prov.embargoes.deinit();
            // 2. Own the key bytes.
            prov.recipient_key = try idx.allocator.dupe(u8, entry.recipient_key);
            errdefer idx.allocator.free(prov.recipient_key);
            // 3. Own the target copy (index allocator — freed by Provision.release
            //    with the matching allocator).
            prov.target = try cloneProvideTargetWith(idx.allocator, &entry.target);
            errdefer if (prov.target) |*t| t.deinit(idx.allocator);
            // 4. Pin a sender-hosted target export — or a receiverHosted target
            //    IMPORT (the lift; see the adoption arm's table-collision note) —
            //    for the provision's lifetime.
            var pinned_export: ?u32 = null;
            var pinned_import: ?u32 = null;
            switch (entry.target) {
                .local => |t| {
                    const tag = try cap_table.descriptors.tagForOriginCode(t.origin_code);
                    if (tag == .senderHosted or tag == .senderPromise) {
                        try self.noteHandoffExportRef(t.cap_id);
                        pinned_export = t.cap_id;
                    } else if (tag == .receiverHosted) {
                        try self.noteHandoffImportPin(t.cap_id);
                        pinned_import = t.cap_id;
                    }
                },
                .promised => {},
            }
            errdefer if (pinned_export) |id| rollbackHandoffExportRef(self, id);
            errdefer if (pinned_import) |id| self.rollbackHandoffImportPin(id);
            // 5. Index entry (key borrows prov.recipient_key — rule R3).
            try idx.by_key.put(prov.recipient_key, prov);
            errdefer _ = idx.by_key.remove(prov.recipient_key);
            // 6. Owner map entry (peer allocator) — last fallible operation.
            try self.provisions_by_question.put(provide_question_id, prov);
            // 7. INFALLIBLE TAIL: refs, flags, state, counters.
            prov.state = .active;
            prov.owner = self;
            prov.provide_question_id = provide_question_id;
            prov.target_export_pinned = pinned_export != null;
            prov.target_import_pinned = pinned_import != null;
            prov.indexed = true;
            prov.retain(); // the index's +1
            prov.retain(); // the owner map's +1
            idx.provision_count += 1;
            idx.provision_key_bytes += prov.recipient_key.len;
        }

        /// Index-mode Accept path. The same-peer arm delegates to the EXACT legacy
        /// orchestration call (byte-identical Returns and error strings); only the
        /// cross-peer arm is new.
        pub fn handleAcceptWithProvisionIndex(self: *Peer, idx: *ProvisionIndex, accept: protocol.Accept) !void {
            // Same single-thread contract `attachProvisionIndex` and
            // `registerProvisionForProvide` enforce: WorkerPool vats are
            // unsupported, so the index pins itself to the first thread that
            // touches it. The Accept path touches it as hard as the Provide path
            // does — the sweep immediately below mutates vat-wide counters and can
            // unindex provisions — so it must fail the SAME loud way rather than
            // racing silently. First statement, before anything reads or writes the
            // index.
            idx.assertThreadAffinity();

            // Accept is one of several parked-expiry drivers (every inbound frame,
            // Provide, deadline maintenance, and the public Vat/index sweep are
            // the others). Keeping this explicit pre-lookup sweep is load-bearing
            // in TWO ways: every park-budget check below sees post-eviction
            // counters, AND the eviction
            // happens strictly BEFORE `by_key.get` — a sweep that ran after the
            // lookup could unindex the very `.awaiting` provision this Accept is
            // about to park onto, orphaning a legitimate accept on a provision no
            // later `Provide` can ever adopt.
            _ = idx.sweepExpiredParkedAccepts();

            const key_opt = try provide_accept_join.orchestration.captureAcceptProvisionForPeer(
                Peer,
                self,
                accept,
                third_party.captureAnyPointerPayloadForPeerFn(Peer, Peer.captureAnyPointerPayload),
            );
            defer if (key_opt) |bytes| self.allocator.free(bytes);
            const key = key_opt orelse
                return self.sendReturnException(accept.question_id, "unknown provision");
            const prov = idx.by_key.get(key) orelse {
                // Accept-before-Provide: PARK (the rendezvous contract is
                // order-independent; an Accept may be the first frame on a
                // brand-new connection). A fresh `.awaiting` provision holds it.
                parkAcceptBeforeProvide(self, idx, key, accept.question_id, accept.embargo) catch |err| {
                    // A host Clock callback may inject another Accept. That nested
                    // park must unwind without emitting a Return from inside the
                    // clock; the outer sampling operation remains the sole owner
                    // of this inbound answer.
                    if (err == error.ParkClockReentrant) return err;
                    try convertParkErrorToReturn(self, accept.question_id, err);
                };
                return;
            };

            if (prov.state == .awaiting) {
                // A sibling (or this peer) already parked accepts for this token:
                // park alongside them.
                prov.retain();
                defer prov.release();
                parkAcceptOntoAwaiting(self, idx, prov, accept.question_id, accept.embargo) catch |err| {
                    if (err == error.ParkClockReentrant) return err;
                    try convertParkErrorToReturn(self, accept.question_id, err);
                };
                return;
            }

            if (prov.owner == self and prov.state == .active) {
                // Degenerate same-peer arm: the same function, arguments, and
                // hooks as the no-index path.
                return provide_accept_join.orchestration.handleAccept(
                    Peer,
                    ProvideEntry,
                    ProvideTarget,
                    self,
                    accept,
                    &self.provides_by_question,
                    &self.provides_by_key,
                    provide_accept_join.orchestration.captureAcceptProvisionForPeerFn(
                        Peer,
                        third_party.captureAnyPointerPayloadForPeerFn(Peer, Peer.captureAnyPointerPayload),
                    ),
                    finish.freeOwnedFrameForPeerFn(Peer),
                    queueEmbargoedAcceptRouted,
                    Peer.sendReturnProvidedTarget,
                    Peer.sendReturnException,
                );
            }

            // Cross-peer arm.
            prov.retain();
            defer prov.release();
            if (accept.embargo) |embargo| {
                queueCrossPeerEmbargoedAccept(self, prov, accept.question_id, embargo) catch |err|
                    try convertQueueErrorToReturn(self, accept.question_id, err);
                return;
            }
            serveProvisionOnPeer(self, prov, accept.question_id) catch |err|
                try self.sendReturnException(accept.question_id, @errorName(err));
        }

        /// Convert a queue-ladder failure into the Accept's exception Return —
        /// except OOM, which re-raises out of dispatch (dropping a rendezvous
        /// marker silently would wedge the recipient; loud and terminal is the
        /// rule).
        fn convertQueueErrorToReturn(self: *Peer, answer_id: u32, err: anyerror) !void {
            switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.DuplicateEmbargoId => try self.sendReturnException(answer_id, "duplicate embargo id"),
                error.DuplicateAcceptQuestionId => try self.sendReturnException(answer_id, "duplicate accept question id"),
                error.EmbargoBudgetExceeded, error.QueuedAcceptBudgetExceeded => try self.sendReturnException(answer_id, "provision queued-accept budget exhausted"),
                error.ProvisionClosed => try self.sendReturnException(answer_id, "provision lost: provider connection closed"),
                else => try self.sendReturnException(answer_id, @errorName(err)),
            }
        }

        /// Remove one embargo entry from a provision, freeing its owned key.
        fn eraseProvisionEmbargoEntry(prov: *ProvisionIndex.Provision, key_bytes: []const u8) void {
            if (prov.embargoes.fetchRemove(key_bytes)) |kv| {
                prov.embargo_key_bytes -= kv.key.len;
                prov.allocator.free(kv.key);
            }
        }

        /// Queue one embargoed accept into the provision's embargo map (single
        /// pending slot per embargo id; find-or-create meets the
        /// Disembargo-before-Accept race). Ladder discipline: ALL fallible work
        /// first, one infallible tail takes the flags, slot, ref, and counters —
        /// an OOM unwinds to zero residue (no half-queued accept, no second
        /// Return, no unspent ref). `self` is the accept peer; it may equal the
        /// owner (same-peer accepts on attached peers route here too).
        fn queueCrossPeerEmbargoedAccept(
            self: *Peer,
            prov: *ProvisionIndex.Provision,
            answer_id: u32,
            embargo: []const u8,
        ) !void {
            if (prov.state != .active) return error.ProvisionClosed;
            const idx = self.provision_index orelse return error.ProvisionClosed;
            if (self.cross_peer_pending_accepts.contains(answer_id)) return error.DuplicateAcceptQuestionId;

            const entry_exists = prov.embargoes.contains(embargo);
            if (!entry_exists) {
                if (prov.embargoes.count() >= idx.limits.max_embargoes_per_provision) return error.EmbargoBudgetExceeded;
                if (prov.embargo_key_bytes + embargo.len > idx.limits.max_embargo_key_bytes_per_provision) return error.EmbargoBudgetExceeded;
            }
            if (idx.queued_accept_count >= idx.limits.max_queued_accepts) return error.QueuedAcceptBudgetExceeded;
            if (idx.queued_accept_bytes + embargo.len > idx.limits.max_queued_accept_bytes) return error.QueuedAcceptBudgetExceeded;

            const gop = try prov.embargoes.getOrPut(embargo);
            var created_here = false;
            if (!gop.found_existing) {
                gop.key_ptr.* = try prov.allocator.dupe(u8, embargo);
                gop.value_ptr.* = .{};
                prov.embargo_key_bytes += embargo.len;
                created_here = true;
            }
            errdefer if (created_here) eraseProvisionEmbargoEntry(prov, embargo);

            if (gop.value_ptr.used_by_accept) return error.DuplicateEmbargoId;
            if (gop.value_ptr.disembargoed) {
                // The Disembargo won the race: nothing to withhold — consume the
                // tombstone and serve immediately.
                eraseProvisionEmbargoEntry(prov, embargo);
                return serveProvisionOnPeer(self, prov, answer_id);
            }

            const r_key = try self.allocator.dupe(u8, embargo);
            errdefer self.allocator.free(r_key);
            try self.cross_peer_pending_accepts.put(answer_id, .{
                .provision = prov,
                .embargo_key = r_key,
                .parked = false,
            });

            // INFALLIBLE TAIL.
            gop.value_ptr.used_by_accept = true;
            gop.value_ptr.pending = .{ .accept_peer = self, .answer_id = answer_id };
            prov.retain(); // the pending slot's +1
            idx.queued_accept_count += 1;
            idx.queued_accept_bytes += embargo.len;
        }

        /// Same-typed replacement for the `queue_embargoed_accept` hook VALUE on
        /// index-attached peers: same-peer embargoed accepts route into the
        /// provision store (per-provision keying) instead of the byte-keyed
        /// legacy bucket — the byte-collision co-drain class dies with the dual
        /// store. A miss NEVER falls back to the legacy queue (that would
        /// silently repopulate the byte-keyed store).
        fn queueEmbargoedAcceptRouted(
            self: *Peer,
            answer_id: u32,
            provided_question_id: u32,
            embargo: []const u8,
        ) !void {
            const prov = self.provisions_by_question.get(provided_question_id) orelse {
                try self.sendReturnException(answer_id, "unknown provision");
                return;
            };
            prov.retain();
            defer prov.release();
            queueCrossPeerEmbargoedAccept(self, prov, answer_id, embargo) catch |err|
                try convertQueueErrorToReturn(self, answer_id, err);
        }

        fn convertParkErrorToReturn(self: *Peer, answer_id: u32, err: anyerror) !void {
            switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                error.ParkBudgetExceeded,
                error.PeerParkBudgetExceeded,
                => try self.sendReturnException(answer_id, "provision park budget exhausted"),
                error.DuplicateAcceptQuestionId => try self.sendReturnException(answer_id, "duplicate accept question id"),
                else => try self.sendReturnException(answer_id, @errorName(err)),
            }
        }

        /// Park an early Accept onto a freshly-created `.awaiting` provision.
        /// Ladder discipline: all fallible work first, one infallible tail.
        fn parkAcceptBeforeProvide(self: *Peer, idx: *ProvisionIndex, key: []const u8, answer_id: u32, embargo: ?[]const u8) !void {
            // Clock.now is arbitrary host code. Sample before duplicate/admission
            // checks and, critically, before a ref_count=0 provision is exposed in
            // by_key. After it returns there are no callback seams in this ladder.
            const deadline_ns = try parkDeadlineNs(idx);
            if (self.transport_close_notified) return error.ProvisionClosed;
            const attached_idx = self.provision_index orelse return error.ProvisionClosed;
            if (attached_idx != idx) return error.ProvisionClosed;
            // A reentrant callback may have registered this token. Never replace
            // that live entry with the fresh provision this miss path was about to
            // construct; the outer Accept fails cleanly and may be retried.
            if (idx.by_key.contains(key)) return error.ProvisionClosed;
            if (self.cross_peer_pending_accepts.contains(answer_id)) return error.DuplicateAcceptQuestionId;
            const embargo_len: usize = if (embargo) |e| e.len else 0;
            const charge = try checkParkAdmissionWithEvents(self, idx, key.len, embargo_len);
            if (idx.provision_count >= idx.limits.max_provisions) return error.ParkBudgetExceeded;
            const provision_key_bytes = std.math.add(usize, idx.provision_key_bytes, key.len) catch
                return error.ParkBudgetExceeded;
            if (provision_key_bytes > idx.limits.max_provision_key_bytes) return error.ParkBudgetExceeded;

            const prov = try idx.allocator.create(ProvisionIndex.Provision);
            errdefer idx.allocator.destroy(prov);
            prov.* = .{
                .allocator = idx.allocator,
                .recipient_key = &.{},
                .embargoes = std.StringHashMap(ProvisionIndex.ProvisionEmbargo).init(idx.allocator),
            };
            errdefer prov.embargoes.deinit();
            // Parking may reserve the list before the holder map's final fallible
            // insertion. A brand-new provision is destroyed on that failure, so
            // its separately allocated list capacity must unwind with it.
            errdefer prov.parked.deinit(idx.allocator);
            prov.recipient_key = try idx.allocator.dupe(u8, key);
            errdefer idx.allocator.free(prov.recipient_key);
            try idx.by_key.put(prov.recipient_key, prov);
            errdefer _ = idx.by_key.remove(prov.recipient_key);

            // The awaiting provision is live and indexed from here; parking onto
            // it either succeeds or destroys it again via the unwind above (its
            // refcount is still zero, so no release choreography is needed).
            prov.indexed = true;
            errdefer prov.indexed = false;
            try parkAcceptOntoAwaitingCharged(self, idx, prov, answer_id, embargo, charge, deadline_ns);
            // INFALLIBLE TAIL for the provision itself.
            prov.retain(); // the index's +1
            idx.provision_count += 1;
            idx.provision_key_bytes += prov.recipient_key.len;
            emitParkAdmissionPressure(self, idx, charge);
        }

        /// True when this parked entry's TTL has run out. An unstamped entry (the
        /// index had no clock or no TTL when it parked) never expires.
        fn isParkExpired(parked: ProvisionIndex.ParkedAccept, now: i64) bool {
            const deadline = parked.deadline_ns orelse return false;
            return now >= deadline;
        }

        /// L9: evict every parked accept whose TTL has run out. Inert unless the
        /// index has both `limits.park_ttl_ms` and an index-owned time source (a
        /// custom Clock or value-stored Io fallback); without either, parking
        /// behaves exactly as it did before the TTL existed.
        ///
        /// Restricted to `.awaiting` provisions. An `.active` provision's parked
        /// list belongs to adoption, not to this sweep; its independent refs keep
        /// a matched handoff live across index-first teardown.
        ///
        /// OOM POLICY (deliberate; the codebase has three): BEST-EFFORT, like
        /// `Drain.failPendingAccept(.best_effort)` and unlike `convertParkErrorToReturn`.
        /// This is background reclamation running underneath an unrelated inbound
        /// Accept, so it must never convert that Accept into a terminal error. An
        /// OOM in either collection pass abandons the round — the next inbound,
        /// deadline, or explicit-sweep trigger retries it, and subsequent budget
        /// checks still fail closed — and an exception `Return` that cannot be
        /// built is logged and dropped rather than re-raised.
        pub fn sweepExpiredParkedAcceptsForProvisionIndex(idx: *ProvisionIndex) usize {
            idx.assertThreadAffinity();
            if (idx.limits.park_ttl_ms == null) return 0;
            if (idx.parked_accept_count == 0) return 0;
            if (idx.park_sweep_in_progress or idx.park_clock_sample_in_progress) return 0;
            const next_deadline = idx.next_park_deadline_ns orelse return 0;

            // Guard BEFORE sampling the host-supplied clock: a custom Clock.now
            // callback may re-enter the public sweep. The common not-due path is
            // still O(1) and does not rebuild the deadline cache.
            idx.park_sweep_in_progress = true;
            idx.park_clock_sample_in_progress = true;
            const now_opt = idx.clockNow();
            // Clock.now may deinitialize and reconstruct the Index at this exact
            // address. Reconstruction clears both guards; in that case this stack
            // frame belongs to the old generation and must not read, sweep, or
            // clear any state in the replacement.
            if (!idx.park_sweep_in_progress or !idx.park_clock_sample_in_progress) return 0;
            const now = now_opt orelse {
                idx.park_clock_sample_in_progress = false;
                idx.park_sweep_in_progress = false;
                return 0;
            };
            idx.park_clock_sample_in_progress = false;
            if (now < next_deadline) {
                idx.park_sweep_in_progress = false;
                return 0;
            }
            defer {
                idx.park_sweep_in_progress = false;
                idx.refreshNextParkDeadline();
            }
            var detached: usize = 0;

            // PHASE 1 — collect. No sends here, so nothing re-enters dispatch and
            // the `by_key` iterator stays valid. Each candidate is RETAINED for
            // the whole walk, so a nested frame in phase 2 can never leave a
            // dangling pointer in this list.
            var expired_provs: std.ArrayList(*ProvisionIndex.Provision) = .empty;
            defer {
                for (expired_provs.items) |prov| prov.release();
                expired_provs.deinit(idx.allocator);
            }
            var it = idx.by_key.valueIterator();
            while (it.next()) |prov_ptr| {
                const prov = prov_ptr.*;
                if (prov.state != .awaiting) continue;
                var any_expired = false;
                for (prov.parked.items) |parked| {
                    if (isParkExpired(parked, now)) {
                        any_expired = true;
                        break;
                    }
                }
                if (!any_expired) continue;
                expired_provs.append(idx.allocator, prov) catch return detached; // OOM: retry later
                prov.retain();
            }

            // PHASE 2 — act. Sends re-enter dispatch (a nested Finish can mutate
            // `prov.parked`, and a nested path can unindex a provision), so this
            // walks the collected list, never the live map.
            for (expired_provs.items) |prov| detached += sweepExpiredParkedAcceptsOn(idx, prov, now);
            return detached;
        }

        /// Evict one provision's expired parked accepts. The caller holds a ref on
        /// `prov` for the whole call.
        fn sweepExpiredParkedAcceptsOn(idx: *ProvisionIndex, prov: *ProvisionIndex.Provision, now: i64) usize {
            // A nested frame during an earlier provision's sends may have adopted
            // (or closed) this one since phase 1.
            if (prov.state != .awaiting) return 0;

            // Move the expired entries OUT of the live list BEFORE any send.
            const ExpiredParkedAccept = struct {
                parked: ProvisionIndex.ParkedAccept,
                observer: ?events.Observer,
            };
            var expired: std.ArrayList(ExpiredParkedAccept) = .empty;
            defer expired.deinit(idx.allocator);
            var i: usize = prov.parked.items.len;
            while (i > 0) {
                i -= 1;
                if (!isParkExpired(prov.parked.items[i], now)) continue;
                // Append (fallible) BEFORE the removal (infallible), so an OOM
                // leaves the entry parked and intact rather than orphaned.
                const parked = prov.parked.items[i];
                expired.append(idx.allocator, .{
                    .parked = parked,
                    .observer = parked.accept_peer.observer,
                }) catch break;
                _ = prov.parked.orderedRemove(i);
            }

            // SEND-FREE NEUTRALIZATION for the entire moved batch. A first expiry
            // Return may close its holder synchronously; that callback must see no
            // later moved park charges or holder records that the close walk can
            // no longer reach.
            for (expired.items) |*item| {
                const parked = &item.parked;
                if (parked.embargo) |b| idx.allocator.free(b);
                parked.embargo = null;
                idx.refundParkAdmission(parked.accept_peer, parked.*);
                Drain.detachPendingAcceptRecord(.{
                    .accept_peer = parked.accept_peer,
                    .answer_id = parked.answer_id,
                });
            }

            // SEND/EVENT PHASE. Observers were captured during collection, before
            // any callback could mutate peer lifecycle. Each moved entry still
            // owns exactly its parked +1 until failPendingAccept returns.
            for (expired.items) |item| {
                const parked = item.parked;
                Drain.failPendingAccept(
                    .{ .accept_peer = parked.accept_peer, .answer_id = parked.answer_id },
                    prov,
                    "parked accept expired",
                    .best_effort,
                ) catch {};
                events.emitParkedAcceptTimeout(item.observer, parked.answer_id);
            }

            // Nothing can ever adopt an `.awaiting` provision with no parked
            // accepts — adoption needs the index entry this removes — so drop the
            // index's +1, exactly as `maybeUnindexEmptyAwaiting` does. Done WHILE
            // THE CALLER STILL HOLDS A REF: `by_key`'s key BORROWS
            // `prov.recipient_key`, and `Provision.release` asserts `!indexed` at
            // ref zero, so unindex must strictly precede the last release.
            if (prov.state != .awaiting) return expired.items.len;
            if (prov.parked.items.len != 0) return expired.items.len;
            if (!prov.indexed) return expired.items.len;
            _ = idx.by_key.remove(prov.recipient_key);
            idx.provision_count -= 1;
            idx.provision_key_bytes -= prov.recipient_key.len;
            prov.indexed = false;
            prov.release(); // the index's +1
            return expired.items.len;
        }

        /// Absolute expiry stamp for a parked accept, read from the INDEX-owned
        /// clock — never from a peer's, so the connection whose accepts are being
        /// timed out cannot steer its own deadline. Null (TTL off, or no index
        /// clock) marks an entry that never expires: the pre-TTL behaviour.
        fn parkDeadlineNs(idx: *ProvisionIndex) !?i64 {
            const ttl_ms = idx.limits.park_ttl_ms orelse return null;
            if (idx.park_clock_sample_in_progress) return error.ParkClockReentrant;
            idx.park_clock_sample_in_progress = true;
            const now_opt = idx.clockNow();
            // A same-address Index replacement resets this ownership bit. Do not
            // let an old-generation deadline (or the deferred guard clear) escape
            // into the replacement generation.
            if (!idx.park_clock_sample_in_progress) return error.ProvisionClosed;
            idx.park_clock_sample_in_progress = false;
            const now = now_opt orelse return null;
            // Saturating throughout: an absurd configured TTL must clamp to "never
            // expires", not panic on the integer cast.
            const ttl_ms_i: i64 = std.math.cast(i64, ttl_ms) orelse std.math.maxInt(i64);
            return now +| (ttl_ms_i *| std.time.ns_per_ms);
        }

        /// Park an early Accept onto an existing `.awaiting` provision.
        fn parkAcceptOntoAwaiting(self: *Peer, idx: *ProvisionIndex, prov: *ProvisionIndex.Provision, answer_id: u32, embargo: ?[]const u8) !void {
            // Sample before every state/duplicate/admission check. The caller's
            // transient provision ref keeps `prov` alive if the callback adopts,
            // expires, or tears down the index; the checks below then fail closed.
            const deadline_ns = try parkDeadlineNs(idx);
            if (prov.state != .awaiting or self.transport_close_notified) return error.ProvisionClosed;
            const attached_idx = self.provision_index orelse return error.ProvisionClosed;
            if (attached_idx != idx) return error.ProvisionClosed;
            // Pointer identity alone is insufficient: Clock.now can deinit and
            // reinitialize the same Index storage, then reattach this peer. The
            // retained old provision must still be the indexed value in the new
            // generation or parking here would create an unreachable orphan.
            if (!prov.indexed) return error.ProvisionClosed;
            const indexed_prov = idx.by_key.get(prov.recipient_key) orelse return error.ProvisionClosed;
            if (indexed_prov != prov) return error.ProvisionClosed;
            if (self.cross_peer_pending_accepts.contains(answer_id)) return error.DuplicateAcceptQuestionId;
            const embargo_len: usize = if (embargo) |e| e.len else 0;
            const charge = try checkParkAdmissionWithEvents(self, idx, prov.recipient_key.len, embargo_len);
            try parkAcceptOntoAwaitingCharged(self, idx, prov, answer_id, embargo, charge, deadline_ns);
            emitParkAdmissionPressure(self, idx, charge);
        }

        /// Admission diagnostics run before any new awaiting provision is exposed
        /// in the index. This keeps synchronous observer re-entry from seeing a
        /// half-constructed rendezvous.
        fn checkParkAdmissionWithEvents(
            self: *Peer,
            idx: *ProvisionIndex,
            recipient_bytes: usize,
            embargo_bytes: usize,
        ) !ProvisionIndex.ParkCharge {
            return idx.checkParkAdmission(self, recipient_bytes, embargo_bytes) catch |err| {
                const attributed = std.math.add(usize, recipient_bytes, embargo_bytes) catch std.math.maxInt(usize);
                const peer_failure = err == error.PeerParkBudgetExceeded;
                const count_current = if (peer_failure) self.parked_accept_count else idx.parked_accept_count;
                const bytes_current = if (peer_failure) self.parked_accept_bytes else idx.parked_accept_bytes;
                const count_limit = if (peer_failure) idx.limits.max_parked_accepts_per_peer else idx.limits.max_parked_accepts;
                const bytes_limit = if (peer_failure) idx.limits.max_parked_accept_bytes_per_peer else idx.limits.max_parked_accept_bytes;
                const count_attempted = std.math.add(usize, count_current, 1) catch std.math.maxInt(usize);
                const byte_attempted = std.math.add(usize, bytes_current, attributed) catch std.math.maxInt(usize);
                const count_exhausted = count_attempted > count_limit;
                events.emitResourceRejection(
                    self.observer,
                    .peer,
                    .unknown,
                    if (count_exhausted) .parked_accepts else .parked_accept_bytes,
                    if (count_exhausted) count_attempted else byte_attempted,
                    if (count_exhausted) count_limit else bytes_limit,
                    err,
                );
                return err;
            };
        }

        /// Allocate and publish one parked entry after admission has been checked.
        /// No observer callbacks run until the caller has completed any enclosing
        /// provision-index ownership tail.
        fn parkAcceptOntoAwaitingCharged(
            self: *Peer,
            idx: *ProvisionIndex,
            prov: *ProvisionIndex.Provision,
            answer_id: u32,
            embargo: ?[]const u8,
            charge: ProvisionIndex.ParkCharge,
            deadline_ns: ?i64,
        ) !void {
            std.debug.assert(prov.state == .awaiting);

            const parked_embargo: ?[]u8 = if (embargo) |e| try idx.allocator.dupe(u8, e) else null;
            errdefer if (parked_embargo) |b| idx.allocator.free(b);
            const r_key: ?[]u8 = if (embargo) |e| try self.allocator.dupe(u8, e) else null;
            errdefer if (r_key) |k| self.allocator.free(k);
            try prov.parked.ensureUnusedCapacity(prov.allocator, 1);
            try self.cross_peer_pending_accepts.put(answer_id, .{
                .provision = prov,
                .embargo_key = r_key,
                .parked = true,
            });

            // INFALLIBLE TAIL.
            const parked: ProvisionIndex.ParkedAccept = .{
                .accept_peer = self,
                .answer_id = answer_id,
                .embargo = parked_embargo,
                .attributed_bytes = charge.attributed_bytes,
                .embargo_bytes = charge.embargo_bytes,
                .deadline_ns = deadline_ns,
            };
            prov.parked.appendAssumeCapacity(parked);
            prov.retain(); // the parked entry's +1
            idx.commitParkAdmission(self, charge);
            idx.noteParkDeadline(deadline_ns);
        }

        fn emitParkAdmissionPressure(self: *Peer, idx: *ProvisionIndex, charge: ProvisionIndex.ParkCharge) void {
            const count_after = self.parked_accept_count;
            const bytes_after = self.parked_accept_bytes;
            std.debug.assert(count_after > 0);
            std.debug.assert(bytes_after >= charge.attributed_bytes);
            const count_before = count_after - 1;
            const bytes_before = bytes_after - charge.attributed_bytes;
            const park_observer = self.observer;
            events.emitPressureCrossing(
                park_observer,
                .peer,
                .unknown,
                .parked_accepts,
                count_before,
                count_after,
                idx.limits.max_parked_accepts_per_peer,
            );
            events.emitPressureCrossing(
                park_observer,
                .peer,
                .unknown,
                .parked_accept_bytes,
                bytes_before,
                bytes_after,
                idx.limits.max_parked_accept_bytes_per_peer,
            );
        }

        /// PHASE B of adoption: a Provide just activated an `.awaiting` provision.
        /// Transition the WHOLE moved parked batch without sends first, then emit
        /// terminal Returns. This ordering is load-bearing: the first Return may
        /// synchronously deliver Finish, Disembargo, or holder transport-close.
        /// Before that callback can run, every later entry must already be either
        /// visible in its live embargo slot or fully detached as a terminal action.
        /// The moved parked buffer doubles as the action buffer after its admission
        /// fields have been refunded, avoiding a new allocation/OOM seam.
        pub fn drainAdoptedParkedAccepts(self: *Peer, idx: *ProvisionIndex, prov: *ProvisionIndex.Provision) !void {
            _ = self;
            const Disposition = enum(u8) {
                queued,
                serve,
                fail_closed,
                fail_budget,
                fail_oom,
                fail_duplicate,
            };

            prov.retain();
            defer prov.release();
            var owned = prov.parked;
            prov.parked = .empty;
            defer owned.deinit(prov.allocator);
            var terminal_oom = false;

            // SEND-FREE PREPARATION. `embargo_bytes` was consumed by the refund and
            // is repurposed as a compact disposition tag for the later send pass.
            for (owned.items) |*parked| {
                const accept_peer = parked.accept_peer;
                idx.refundParkAdmission(accept_peer, parked.*);

                if (prov.state != .active or prov.owner == null) {
                    prepareAdoptedParkedTerminal(prov, parked);
                    parked.embargo_bytes = @backingInt(Disposition.fail_closed);
                    continue;
                }
                if (parked.embargo) |embargo_bytes| {
                    // A Disembargo that beat Provide left the normal pre-Accept
                    // tombstone. Consume it before queue admission: this entry
                    // needs no slot/gauge and belongs in the later serve pass.
                    if (prov.embargoes.get(embargo_bytes)) |existing| {
                        if (!existing.used_by_accept and existing.disembargoed) {
                            eraseProvisionEmbargoEntry(prov, embargo_bytes);
                            prepareAdoptedParkedTerminal(prov, parked);
                            parked.embargo_bytes = @backingInt(Disposition.serve);
                            continue;
                        }
                        prepareAdoptedParkedTerminal(prov, parked);
                        parked.embargo_bytes = @backingInt(Disposition.fail_duplicate);
                        continue;
                    }

                    // Admission must match the normal active-provision queue path.
                    // Adoption transfers a PARKED charge into a QUEUED charge; it
                    // must not bypass either aggregate ceiling or wrap its byte
                    // arithmetic merely because all allocations happened earlier.
                    const provision_bytes_after = std.math.add(
                        usize,
                        prov.embargo_key_bytes,
                        embargo_bytes.len,
                    ) catch {
                        prepareAdoptedParkedTerminal(prov, parked);
                        parked.embargo_bytes = @backingInt(Disposition.fail_budget);
                        continue;
                    };
                    if (prov.embargoes.count() >= idx.limits.max_embargoes_per_provision or
                        provision_bytes_after > idx.limits.max_embargo_key_bytes_per_provision)
                    {
                        prepareAdoptedParkedTerminal(prov, parked);
                        parked.embargo_bytes = @backingInt(Disposition.fail_budget);
                        continue;
                    }
                    const queued_count_after = std.math.add(usize, idx.queued_accept_count, 1) catch {
                        prepareAdoptedParkedTerminal(prov, parked);
                        parked.embargo_bytes = @backingInt(Disposition.fail_budget);
                        continue;
                    };
                    const queued_bytes_after = std.math.add(
                        usize,
                        idx.queued_accept_bytes,
                        embargo_bytes.len,
                    ) catch {
                        prepareAdoptedParkedTerminal(prov, parked);
                        parked.embargo_bytes = @backingInt(Disposition.fail_budget);
                        continue;
                    };
                    if (queued_count_after > idx.limits.max_queued_accepts or
                        queued_bytes_after > idx.limits.max_queued_accept_bytes)
                    {
                        prepareAdoptedParkedTerminal(prov, parked);
                        parked.embargo_bytes = @backingInt(Disposition.fail_budget);
                        continue;
                    }

                    // The parked half and holder record are published together.
                    // Treat a missing holder record as terminal corruption rather
                    // than assuming it cannot happen in a security boundary.
                    const accept_record = accept_peer.cross_peer_pending_accepts.getPtr(parked.answer_id) orelse {
                        prepareAdoptedParkedTerminal(prov, parked);
                        parked.embargo_bytes = @backingInt(Disposition.fail_closed);
                        continue;
                    };

                    // OWNERSHIP TRANSFER into the embargo map: the parked dupe
                    // becomes the map key (both index-allocator-owned), the
                    // parked +1 becomes the pending slot's +1, and the R-side
                    // record is mutated in place — nothing re-allocated.
                    const gop = prov.embargoes.getOrPut(embargo_bytes) catch {
                        prepareAdoptedParkedTerminal(prov, parked);
                        parked.embargo_bytes = @backingInt(Disposition.fail_oom);
                        continue;
                    };
                    if (gop.found_existing) {
                        prepareAdoptedParkedTerminal(prov, parked);
                        parked.embargo_bytes = @backingInt(Disposition.fail_duplicate);
                    } else {
                        gop.key_ptr.* = embargo_bytes;
                        gop.value_ptr.* = .{
                            .used_by_accept = true,
                            .pending = .{ .accept_peer = accept_peer, .answer_id = parked.answer_id },
                        };
                        prov.embargo_key_bytes = provision_bytes_after;
                        parked.embargo = null; // map key inherited the index-owned dupe
                        accept_record.parked = false;
                        idx.queued_accept_count = queued_count_after;
                        idx.queued_accept_bytes = queued_bytes_after;
                        parked.embargo_bytes = @backingInt(Disposition.queued);
                        // The parked +1 became the slot's +1: no release.
                    }
                } else {
                    prepareAdoptedParkedTerminal(prov, parked);
                    parked.embargo_bytes = @backingInt(Disposition.serve);
                }
            }
            idx.refreshNextParkDeadline();
            std.debug.assert(prov.parked.items.len == 0);

            // SEND-BEARING TERMINAL PASS. Reentrant lifecycle work now sees every
            // queued tail entry in the live maps and every non-queued one already
            // absent, so it cannot miss a moved-out park or double-transfer a ref.
            for (owned.items) |parked| {
                const disposition: Disposition = @fromBackingInt(@intCast(@as(u8, @intCast(parked.embargo_bytes))));
                switch (disposition) {
                    .queued => {},
                    .serve => {
                        serveProvisionOnPeer(parked.accept_peer, prov, parked.answer_id) catch |serve_err| {
                            parked.accept_peer.sendReturnException(parked.answer_id, @errorName(serve_err)) catch |err| {
                                if (err == error.OutOfMemory) terminal_oom = true;
                            };
                        };
                        prov.release(); // the parked entry's +1
                    },
                    .fail_closed, .fail_budget, .fail_oom, .fail_duplicate => {
                        const reason: []const u8 = switch (disposition) {
                            .fail_closed => "provision finished before disembargo",
                            .fail_budget => "provision queued-accept budget exhausted",
                            .fail_oom => "OutOfMemory",
                            .fail_duplicate => "duplicate embargo id",
                            .queued, .serve => "provision adoption failed",
                        };
                        parked.accept_peer.sendReturnException(parked.answer_id, reason) catch |err| {
                            if (err == error.OutOfMemory) terminal_oom = true;
                        };
                        prov.release(); // the parked entry's +1
                    },
                }
            }
            if (terminal_oom) return error.OutOfMemory;
        }

        /// Send-free half of a terminal adopted action. The caller already
        /// refunded its park charge. Free both embargo dupes and remove the holder
        /// record, but retain the parked +1 until the later send pass completes.
        fn prepareAdoptedParkedTerminal(
            prov: *ProvisionIndex.Provision,
            parked: *ProvisionIndex.ParkedAccept,
        ) void {
            if (parked.embargo) |b| {
                prov.allocator.free(b);
                parked.embargo = null;
            }
            if (parked.accept_peer.cross_peer_pending_accepts.fetchRemove(parked.answer_id)) |kv| {
                if (kv.value.embargo_key) |k| parked.accept_peer.allocator.free(k);
            }
        }

        /// Release (or pre-mark) one embargo on a provision — the host arm of a
        /// spec-form accept-Disembargo. Walks the provision's own embargo map,
        /// never any vat-wide or byte-keyed store; find-or-create leaves a
        /// tombstone when the Disembargo arrives before its Accept. Consumed
        /// entries are ERASED, so completed embargo ids are reusable.
        pub fn releaseProvisionEmbargo(self: *Peer, prov: *ProvisionIndex.Provision, embargo: []const u8) !void {
            if (prov.state == .closed) return;

            if (!prov.embargoes.contains(embargo)) {
                // Tombstone-create budget: only the introducer sends
                // accept-Disembargos here, so exceeding the per-provision bound is
                // that introducer misbehaving — abort loudly, never drop a
                // rendezvous marker silently.
                const over = if (self.provision_index) |idx|
                    prov.embargoes.count() >= idx.limits.max_embargoes_per_provision
                else
                    false;
                if (over) {
                    peer_outbound_control.sendAbortViaSendFrame(Peer, self, "provision embargo budget exhausted", Peer.sendFrameControl) catch |send_err| {
                        log.debug("embargo budget abort send failed: {}", .{send_err});
                    };
                    return error.EmbargoBudgetExceeded;
                }
            }
            const gop = try prov.embargoes.getOrPut(embargo);
            if (!gop.found_existing) {
                gop.key_ptr.* = try prov.allocator.dupe(u8, embargo);
                gop.value_ptr.* = .{};
                prov.embargo_key_bytes += embargo.len;
            }
            gop.value_ptr.disembargoed = true;
            const pending = gop.value_ptr.pending orelse return; // tombstone kept until the Accept arrives
            gop.value_ptr.pending = null; // detach: we inherit the slot's +1
            prov.retain();
            defer prov.release();
            eraseProvisionEmbargoEntry(prov, embargo);

            // Canonical fail/serve ordering: holder record removed (and the accept
            // peer's key dupe freed) BEFORE the send.
            if (pending.accept_peer.cross_peer_pending_accepts.fetchRemove(pending.answer_id)) |kv| {
                if (kv.value.embargo_key) |k| pending.accept_peer.allocator.free(k);
            }
            if (pending.accept_peer.provision_index) |idx| {
                idx.queued_accept_count -= 1;
                idx.queued_accept_bytes -= embargo.len;
            }
            serveProvisionOnPeer(pending.accept_peer, prov, pending.answer_id) catch |err| {
                pending.accept_peer.sendReturnException(pending.answer_id, @errorName(err)) catch |e2| {
                    log.debug("provision release: failed to fail accept {}: {}", .{ pending.answer_id, e2 });
                };
            };
            prov.release(); // the pending slot's +1
        }

        /// FinishOps `clear_pending_accept_question` replacement (same fn type):
        /// a Finish cancelling a queued cross-peer Accept clears the provision
        /// slot — releasing the slot's +1 ONLY when a LIVE slot was actually
        /// cleared (a record whose slot a concurrent drain already detached is a
        /// clean miss) — then always falls through to the legacy per-peer path.
        pub fn clearPendingAcceptQuestionRouted(peer: *Peer, question_id: u32) void {
            if (peer.cross_peer_pending_accepts.fetchRemove(question_id)) |kv| {
                const rec = kv.value;
                const prov = rec.provision; // valid: the slot's +1 pins it (INV-REC)
                var cleared_live = false;
                if (rec.parked) {
                    var i: usize = 0;
                    while (i < prov.parked.items.len) : (i += 1) {
                        const parked = prov.parked.items[i];
                        if (parked.accept_peer == peer and parked.answer_id == question_id) {
                            if (peer.provision_index) |idx| {
                                idx.refundParkAdmission(peer, parked);
                            }
                            if (parked.embargo) |bytes| prov.allocator.free(bytes);
                            _ = prov.parked.swapRemove(i);
                            cleared_live = true;
                            break;
                        }
                    }
                } else if (rec.embargo_key) |key| {
                    if (prov.embargoes.getPtr(key)) |emb| {
                        if (emb.pending) |pending| {
                            if (pending.accept_peer == peer and pending.answer_id == question_id) {
                                emb.pending = null;
                                cleared_live = true;
                            }
                        }
                    }
                    if (cleared_live) {
                        if (peer.provision_index) |idx| {
                            idx.queued_accept_count -= 1;
                            idx.queued_accept_bytes -= key.len;
                        }
                        eraseProvisionEmbargoEntry(prov, key);
                    }
                }
                if (rec.embargo_key) |k| peer.allocator.free(k);
                if (cleared_live) {
                    // An `.awaiting` provision that lost its last parked accept is
                    // unreachable garbage: unindex it while alive so it dies with
                    // the release below instead of squatting on the budget.
                    Drain.maybeUnindexEmptyAwaiting(peer, prov);
                    if (peer.provision_index) |idx| idx.refreshNextParkDeadline();
                    prov.release(); // the slot's/parked entry's +1
                }
            }
            provide_accept_join.embargo_accepts.clearPendingAcceptQuestionForPeer(
                Peer,
                PendingEmbargoedAccept,
                peer,
                question_id,
            );
        }

        /// Serve one accept from a provision on an arbitrary holder peer — the
        /// core of cross-peer hosting. The Return carries senderHosted{proxy}
        /// where the proxy (minted on the accept peer) forwards to the OWNER's
        /// export, holding its own handoff pin so the accepted capability
        /// outlives the provision's Finish.
        fn serveProvisionOnPeer(accept_peer: *Peer, prov: *ProvisionIndex.Provision, answer_id: u32) !void {
            const owner = prov.owner orelse
                return accept_peer.sendReturnException(answer_id, "provision lost: provider connection closed");
            if (owner == accept_peer) {
                // Degenerate arm: byte-identical to today against the owner's
                // LIVE ProvideEntry.
                const entry = owner.provides_by_question.getPtr(prov.provide_question_id) orelse
                    return accept_peer.sendReturnException(answer_id, "unknown provision");
                return accept_peer.sendReturnProvidedTarget(answer_id, &entry.target);
            }

            // Cross-peer arm: target-kind gate. A null target (an `.awaiting`
            // provision that lost its owner) fails closed like an ownerless one.
            const stored_target = prov.target orelse
                return accept_peer.sendReturnException(answer_id, "provision lost: provider connection closed");
            const source: cap_table.ResolvedCap = switch (stored_target) {
                .local => |t| switch (try cap_table.descriptors.tagForOriginCode(t.origin_code)) {
                    .senderHosted, .senderPromise => .{ .exported = .{ .id = t.cap_id } },
                    // The receiverHosted lift, SITE 1: a target the owner merely
                    // IMPORTS serves through the same proxy machinery — the proxy
                    // forwards to the owner's import via the forwarded-vine call
                    // path (`sendCrossPeerProxyResolvedCall`'s `.imported` arm).
                    .receiverHosted => .{ .imported = .{ .id = t.cap_id } },
                    else => return error.CrossPeerProvisionTargetUnsupported,
                },
                // OWNER-SIDE re-resolution: the stored ops name an inbound answer
                // on the OWNER — the id is consumed only against the table it
                // actually names, and no id ever crosses a connection. Chained
                // promised results are followed to a fixed depth.
                .promised => |promised| blk: {
                    var resolved = owner.resolveProvidePromisedOps(promised.question_id, promised.ops) catch
                        return error.CrossPeerProvisionTargetUnavailable;
                    var depth: u8 = 0;
                    while (true) {
                        switch (resolved) {
                            .exported => |cap| break :blk .{ .exported = .{ .id = cap.id } },
                            // The receiverHosted lift, SITE 2: the re-resolution
                            // landing on `.imported` serves like site 1.
                            .imported => |cap| break :blk .{ .imported = .{ .id = cap.id } },
                            .promised => |chained| {
                                depth += 1;
                                if (depth >= 4) return error.CrossPeerProvisionTargetUnsupported;
                                resolved = owner.resolvePromisedAnswer(chained) catch
                                    return error.CrossPeerProvisionTargetUnavailable;
                            },
                            .none => return error.CrossPeerProvisionTargetUnavailable,
                        }
                    }
                },
            };

            // Pin the source — export or import — for the PROXY's lifetime
            // (survives the provision's Finish). Ownership of the pin transfers
            // AT the addCrossPeerProxyExport call: the callee's pre-ctx errdefer
            // arm or its ctx deinit releases it on ANY failure from here on —
            // this caller performs NO pin rollback and NO destroy sweep of a
            // source the serve does not own.
            //
            // IMPORT-source pin windows (the receiverHosted lift):
            //
            //   SITE 1 (stored `.local{receiverHosted}`): the import was known at
            //   Provide receipt, so `registerProvisionForProvide` already holds a
            //   Provide-time pin (`target_import_pinned`) covering the
            //   [Provide, close) window — the introducer may legally drain its
            //   wire refs on the target between Provide and Accept, and only that
            //   pin (plus its withheld Release) keeps the import and the far-side
            //   export alive until this serve runs. The pin taken HERE is the
            //   PROXY's own lease for the [serve, proxy-destroy) window: the
            //   provision's Finish must not kill the accepted capability.
            //
            //   SITE 2 (stored `.promised` re-resolved to `.imported`): there is
            //   NO Provide-time import pin — at Provide receipt the answer's cap
            //   was not yet a settled import (that is exactly what made the
            //   stored target `.promised`), so there was nothing to pin then.
            //   Over [Provide, serve) the target is covered by the stored
            //   ANSWER's own liveness instead: this serve re-resolves the ops
            //   against the owner's still-recorded answer, and a target whose
            //   import has since died re-resolves to a miss and fails closed
            //   (CrossPeerProvisionTargetUnavailable) rather than serving a dead
            //   cap. The pin taken here covers the same [serve, proxy-destroy)
            //   window as site 1.
            const proxy_id = switch (source) {
                .exported => |cap| blk: {
                    try owner.noteHandoffExportRef(cap.id);
                    break :blk try accept_peer.addCrossPeerProxyExport(
                        owner,
                        source,
                        null,
                        cap.id,
                        null,
                    );
                },
                .imported => |cap| blk: {
                    owner.noteHandoffImportPin(cap.id) catch |err| switch (err) {
                        // The import died out from under the stored target (only
                        // reachable for site 2 — site 1's Provide-time pin holds
                        // the entry for the provision's whole life).
                        error.UnknownImport => return error.CrossPeerProvisionTargetUnavailable,
                        else => return err,
                    };
                    break :blk try accept_peer.addCrossPeerProxyExport(
                        owner,
                        source,
                        null,
                        null,
                        cap.id,
                    );
                },
                // Unreachable by construction (the gate above only produces
                // .exported/.imported), kept total and fail-closed defensively.
                .promised, .none => return error.CrossPeerProvisionTargetUnsupported,
            };
            errdefer accept_peer.destroyUnreferencedProxyExport(proxy_id);
            try sendReturnSenderHostedCap(accept_peer, answer_id, proxy_id);
        }

        /// Single-capability Return whose payload content is a senderHosted cap
        /// pointer at index 0 — the same shape as bootstrap returns, through the
        /// same origin-tagged encode path and reserve/rollback machinery.
        fn sendReturnSenderHostedCap(self: *Peer, answer_id: u32, export_id: u32) !void {
            const BuildCtx = struct {
                export_id: u32,
                fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                    const ctx: *const @This() = @ptrCast(@alignCast(ctx_ptr));
                    var payload = try ret.payloadTyped();
                    var any = try payload.initContent();
                    try any.setCapabilityOriginTagged(
                        cap_table.descriptors.originCodeForTag(.senderHosted),
                        ctx.export_id,
                    );
                }
            };
            var ctx = BuildCtx{ .export_id = export_id };
            try self.sendReturnResults(answer_id, &ctx, BuildCtx.build);
        }
    };
}
