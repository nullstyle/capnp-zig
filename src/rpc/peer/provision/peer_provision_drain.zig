const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const state = @import("../state.zig");
const vat_provisions = @import("../../vat/provisions.zig");

/// The canonical L3 provision drain/teardown procedure, extracted from
/// `peer/mod.zig` and made generic over the peer type (the JoinCoordinator
/// extraction contract). Every path that releases a queued/parked Accept's +1
/// funnels through here: detach/refund strictly before the send, the single
/// release strictly after it. Owner close (`closeProvisionAsOwner` via
/// Finish), peer deinit (`neutralizeProvisionsOnOwnerPeer` +
/// `drainClosedProvisionsOnOwnerPeer`), and holder teardown
/// (`detachCrossPeerAcceptsOnHolderPeer`) all neutralize shared state before
/// callbacks, and every send-bearing walk operates on a moved-out or
/// pre-neutralized container, never a live map. `peer/mod.zig` keeps the
/// caller-visible entry points as thunks on `Peer`.
pub fn Drain(comptime Peer: type) type {
    return struct {
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const CrossPeerAcceptsMap = @FieldType(Peer, "cross_peer_pending_accepts");

        const DrainPosture = enum { fallible, best_effort };

        fn DrainError(comptime posture: DrainPosture) type {
            return switch (posture) {
                .fallible => error{OutOfMemory},
                .best_effort => error{},
            };
        }

        pub fn detachPendingAcceptRecord(pending: ProvisionIndex.PendingAccept) void {
            if (pending.accept_peer.cross_peer_pending_accepts.fetchRemove(pending.answer_id)) |kv| {
                if (kv.value.embargo_key) |k| pending.accept_peer.allocator.free(k);
            }
        }

        /// THE one shared fail helper for a queued/parked accept. Input: a pending
        /// ALREADY detached from its slot/list by the caller. Order is law: holder
        /// record removed (and the accept peer's key dupe freed) BEFORE the send;
        /// the single release of the pending's +1 AFTER the send (via defer, so it
        /// drops exactly once even on an OOM re-raise). A reentrant Finish of the
        /// accept question racing the send finds the record already gone — a
        /// clean miss, never a double release.
        pub fn failPendingAccept(
            pending: ProvisionIndex.PendingAccept,
            prov: *ProvisionIndex.Provision,
            reason: []const u8,
            comptime posture: DrainPosture,
        ) DrainError(posture)!void {
            detachPendingAcceptRecord(pending);
            defer prov.release();
            pending.accept_peer.sendReturnException(pending.answer_id, reason) catch |err| {
                if (posture == .fallible and err == error.OutOfMemory) return error.OutOfMemory;
                log.debug("failPendingAccept: exception send for answer {} failed: {}", .{ pending.answer_id, err });
            };
        }

        /// Callback-free close preparation. Refund each gauge only while its
        /// holder still names this exact live index, detach all holder records,
        /// and free parked embargo dupes. Slot/list entries retain their +1 refs
        /// for the later send-bearing drain. Idempotence is explicit because an
        /// import-unpin or question-cancel callback may re-enter teardown.
        fn neutralizeProvisionEntries(idx: ?*ProvisionIndex, prov: *ProvisionIndex.Provision) void {
            if (prov.entries_neutralized) return;

            var embargo_it = prov.embargoes.iterator();
            while (embargo_it.next()) |entry| {
                const pending = entry.value_ptr.pending orelse continue;
                if (idx) |index| {
                    if (pending.accept_peer.provision_index == index) {
                        std.debug.assert(index.queued_accept_count > 0);
                        std.debug.assert(index.queued_accept_bytes >= entry.key_ptr.*.len);
                        index.queued_accept_count -= 1;
                        index.queued_accept_bytes -= entry.key_ptr.*.len;
                    }
                }
                detachPendingAcceptRecord(pending);
            }
            for (prov.parked.items) |*parked| {
                if (idx) |index| {
                    if (parked.accept_peer.provision_index == index) {
                        index.refundParkAdmission(parked.accept_peer, parked.*);
                    }
                }
                if (parked.embargo) |bytes| prov.allocator.free(bytes);
                parked.embargo = null;
                detachPendingAcceptRecord(.{
                    .accept_peer = parked.accept_peer,
                    .answer_id = parked.answer_id,
                });
            }
            if (idx) |index| index.refreshNextParkDeadline();
            prov.entries_neutralized = true;
        }

        /// Drain a provision's embargo slots and parked accepts through
        /// failPendingAccept, over MOVED-OUT containers (nested frames see the
        /// fresh empty containers, and `.closed`-state guards forbid new
        /// insertions). All index/holder state is neutralized before the first
        /// send, so this phase never dereferences `idx` after callback reentry.
        /// Under `.fallible`, send OOM is remembered while the entire moved set is
        /// still retired.
        fn drainProvisionEntries(
            idx: ?*ProvisionIndex,
            prov: *ProvisionIndex.Provision,
            reason: []const u8,
            comptime posture: DrainPosture,
        ) DrainError(posture)!void {
            neutralizeProvisionEntries(idx, prov);
            var owned_embargoes = prov.embargoes;
            prov.embargoes = std.StringHashMap(ProvisionIndex.ProvisionEmbargo).init(prov.allocator);
            prov.embargo_key_bytes = 0;
            var owned_parked = prov.parked;
            prov.parked = .empty;
            var terminal_oom = false;

            // This phase may send/re-enter, but all close-visible records and gauges
            // are already gone. Every slot/list entry now owns only its final +1.
            var send_it = owned_embargoes.iterator();
            while (send_it.next()) |entry| {
                if (entry.value_ptr.pending) |pending| {
                    entry.value_ptr.pending = null;
                    failPendingAccept(pending, prov, reason, posture) catch {
                        terminal_oom = true;
                    };
                }
            }
            var kit = owned_embargoes.keyIterator();
            while (kit.next()) |k| prov.allocator.free(k.*);
            owned_embargoes.deinit();

            var i: usize = 0;
            while (i < owned_parked.items.len) : (i += 1) {
                const parked = owned_parked.items[i];
                failPendingAccept(
                    .{ .accept_peer = parked.accept_peer, .answer_id = parked.answer_id },
                    prov,
                    reason,
                    posture,
                ) catch {
                    terminal_oom = true;
                };
            }
            owned_parked.deinit(prov.allocator);
            if (posture == .fallible and terminal_oom) return error.OutOfMemory;
        }

        const CloseOutcome = enum { closed_now, already_closed };

        /// Canonical wire-Finish close. The `.closed` transition is the ownership
        /// token: exactly one caller may unindex, unpin, drain, and drop the owner
        /// map ref; nested/duplicate Finish observes `.already_closed`.
        fn closeProvisionAsOwner(
            self: *Peer,
            idx: ?*ProvisionIndex,
            prov: *ProvisionIndex.Provision,
        ) error{ OutOfMemory, ProvisionIndexUnavailable }!CloseOutcome {
            if (prov.state == .closed) return .already_closed;
            // Do not begin the terminal ownership transition unless an indexed
            // provision can also be removed from its owning index. Supported
            // index-first teardown clears `indexed`, so this is a fail-closed
            // invariant check rather than a normal lifecycle branch.
            if (prov.indexed and idx == null) return error.ProvisionIndexUnavailable;
            prov.retain();
            defer prov.release();
            prov.state = .closed;
            var terminal_oom = false;

            // CALLBACK-FREE OWNERSHIP CUT. The index entry, every aggregate/peer
            // charge, and every holder back-link disappear before an export/import
            // unpin can emit Release and synchronously deinitialize the index.
            // No code below this point may dereference `idx`.
            prov.owner = null;
            if (prov.indexed) {
                if (idx) |index| {
                    _ = index.by_key.remove(prov.recipient_key);
                    index.provision_count -= 1;
                    index.provision_key_bytes -= prov.recipient_key.len;
                    prov.indexed = false;
                    prov.release(); // the index's +1
                } else return error.ProvisionIndexUnavailable;
            }
            neutralizeProvisionEntries(idx, prov);

            if (prov.target_export_pinned) {
                if (prov.target) |t| switch (t) {
                    .local => |lt| self.releaseHandoffHeldExport(lt.cap_id),
                    else => {},
                };
                prov.target_export_pinned = false;
            }
            if (prov.target_import_pinned) {
                // Table-correct unpin: the flag names the IMPORT table (bare ids
                // collide across tables). Clear it first so callback reentry can
                // never double-unpin; deferred Release OOM propagates after the
                // locally terminal close has retired all shared state.
                prov.target_import_pinned = false;
                if (prov.target) |t| switch (t) {
                    .local => |lt| self.releaseHandoffImportPin(lt.cap_id) catch |err| {
                        if (err == error.OutOfMemory) {
                            terminal_oom = true;
                        } else {
                            log.debug("provision close: handoff import unpin for {} failed: {}", .{ lt.cap_id, err });
                        }
                    },
                    else => {},
                };
            }
            drainProvisionEntries(null, prov, "provision finished before disembargo", .fallible) catch {
                terminal_oom = true;
            };
            if (terminal_oom) return error.OutOfMemory;
            return .closed_now;
        }

        /// Fallible Finish pre-step, called from handleFinish BEFORE the FinishOps
        /// chain — UNGATED (the orelse-return is the gate: no-index peers have an
        /// empty map; mid-deinit the live map is empty). The `.closed` early
        /// return makes a nested duplicate Finish (the vine-release cascade) a
        /// clean no-op that transfers nothing.
        pub fn detachProvisionForFinish(self: *Peer, question_id: u32) !void {
            const prov = self.provisions_by_question.get(question_id) orelse return;
            if (prov.state == .closed) return;
            const outcome = closeProvisionAsOwner(self, self.provision_index, prov) catch |err| {
                // The close transition is terminal even when a best-effort wire
                // notification or deferred import Release ran out of memory. Drop
                // the owner map ref before propagating so a retry is a clean miss
                // instead of a permanently closed entry with live tail state.
                if (prov.state == .closed and prov.owner == null) {
                    if (self.provisions_by_question.remove(question_id)) prov.release();
                }
                return err;
            };
            if (outcome != .closed_now) return;
            // Only after the close COMPLETED (no residue): drop the map entry and
            // the owner's +1.
            _ = self.provisions_by_question.remove(question_id);
            prov.release();
        }

        /// State threaded from the neutralize step of Peer.deinit to its drain
        /// phase (after forceCancelAllQuestions).
        pub const OwnerProvisionTeardown = struct {
            owned_provisions: std.AutoHashMap(u32, *ProvisionIndex.Provision),
        };

        /// Neutralize step: callback-free unindex/account/holder retirement for
        /// every owned provision, followed by index back-link removal. It must
        /// finish before forceCancelAllQuestions, whose user callbacks may destroy
        /// the index. Runs for HOLDER-only peers too: self-removal is unconditional.
        pub fn neutralizeProvisionsOnOwnerPeer(self: *Peer) OwnerProvisionTeardown {
            const saved_idx = self.provision_index;
            var owned = self.provisions_by_question;
            self.provisions_by_question = std.AutoHashMap(u32, *ProvisionIndex.Provision).init(self.allocator);
            var it = owned.valueIterator();
            while (it.next()) |prov_ptr| {
                const prov = prov_ptr.*;
                prov.state = .closed;
                prov.owner = null; // pins abandoned (the export/import tables die with us)
                if (prov.indexed) {
                    if (saved_idx) |idx| {
                        _ = idx.by_key.remove(prov.recipient_key);
                        idx.provision_count -= 1;
                        idx.provision_key_bytes -= prov.recipient_key.len;
                        prov.indexed = false;
                        prov.release(); // the index's +1
                    } else {
                        // Supported index-first teardown clears `indexed`. If a
                        // corrupted host state violates that contract, preserve
                        // the unknown index ref rather than freeing an object a
                        // still-live map may borrow as its key.
                        log.err("peer deinit found an indexed provision without its owning index; retaining the index ref", .{});
                    }
                }
                neutralizeProvisionEntries(saved_idx, prov);
            }

            if (saved_idx) |idx| {
                var i: usize = 0;
                while (i < idx.attached_peers.items.len) {
                    if (idx.attached_peers.items[i] == self) {
                        _ = idx.attached_peers.swapRemove(i);
                    } else i += 1;
                }
                self.provision_index = null;
            }
            return .{ .owned_provisions = owned };
        }

        /// Drain phase: send-bearing, runs AFTER forceCancelAllQuestions over the
        /// moved-out snapshot. Exception Returns go to LIVE sibling peers.
        pub fn drainClosedProvisionsOnOwnerPeer(self: *Peer, teardown: *OwnerProvisionTeardown) void {
            _ = self;
            var it = teardown.owned_provisions.valueIterator();
            while (it.next()) |prov_ptr| {
                const prov = prov_ptr.*;
                prov.retain();
                drainProvisionEntries(null, prov, "provision lost: provider connection closed", .best_effort) catch {};
                prov.release(); // the transient guard
                prov.release(); // the owner map's +1
            }
            teardown.owned_provisions.deinit();
        }

        /// Holder-peer neutralize: clear this peer's queued/parked cross-peer
        /// accepts. Send-free and infallible, but must run BEFORE
        /// forceCancelAllQuestions (whose Finishes can nest frames serving INTO
        /// this half-dead peer). Releases the pending's +1 ONLY when a LIVE
        /// slot/parked entry was actually cleared — a record whose slot was
        /// already detached by a concurrent drain transfers nothing.
        pub fn detachCrossPeerAcceptsOnHolderPeer(self: *Peer) void {
            var owned = self.cross_peer_pending_accepts;
            self.cross_peer_pending_accepts = CrossPeerAcceptsMap.init(self.allocator);
            var it = owned.iterator();
            while (it.next()) |entry| {
                const answer_id = entry.key_ptr.*;
                const rec = entry.value_ptr.*;
                const prov = rec.provision; // valid: the slot's +1 pins it (INV-REC)
                var cleared_live = false;
                if (rec.parked) {
                    var i: usize = 0;
                    while (i < prov.parked.items.len) : (i += 1) {
                        const parked = prov.parked.items[i];
                        if (parked.accept_peer == self and parked.answer_id == answer_id) {
                            if (self.provision_index) |idx| {
                                idx.refundParkAdmission(self, parked);
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
                            if (pending.accept_peer == self and pending.answer_id == answer_id) {
                                emb.pending = null;
                                cleared_live = true;
                            }
                        }
                    }
                    if (cleared_live) {
                        if (prov.embargoes.fetchRemove(key)) |kv| {
                            prov.embargo_key_bytes -= kv.key.len;
                            prov.allocator.free(kv.key);
                        }
                    }
                }
                if (rec.embargo_key) |k| {
                    if (cleared_live and !rec.parked) {
                        if (self.provision_index) |idx| {
                            idx.queued_accept_count -= 1;
                            idx.queued_accept_bytes -= k.len;
                        }
                    }
                    self.allocator.free(k);
                }
                if (cleared_live) {
                    maybeUnindexEmptyAwaiting(self, prov);
                    if (self.provision_index) |idx| idx.refreshNextParkDeadline();
                    prov.release(); // the slot's/parked entry's +1
                }
            }
            owned.deinit();
        }

        /// V2-M5: drop the index's ref on an `.awaiting` provision whose last
        /// parked accept just went away (nothing can ever reach it again — an
        /// adoption needs the index entry this removes). The caller still holds
        /// the entry's +1, so the provision is alive throughout.
        pub fn maybeUnindexEmptyAwaiting(peer: *Peer, prov: *ProvisionIndex.Provision) void {
            if (prov.state != .awaiting) return;
            if (prov.parked.items.len != 0) return;
            if (!prov.indexed) return;
            const idx = peer.provision_index orelse return;
            _ = idx.by_key.remove(prov.recipient_key);
            idx.provision_count -= 1;
            idx.provision_key_bytes -= prov.recipient_key.len;
            prov.indexed = false;
            prov.release(); // the index's +1
        }
    };
}
