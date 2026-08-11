//! Experimental vat-wide provision index for Level-3 three-party HOSTING.
//!
//! A vat that hosts a provided capability receives the `Provide` on its
//! introducer-facing connection and the `Accept` on its recipient-facing
//! connection — different `Peer` objects. Per-peer provide tables cannot match
//! them, so the vat's peers share ONE `ProvisionIndex`: a rendezvous mapping
//! normalized recipient-token bytes to a refcounted, connection-independent
//! `Provision` object (the C++ reference's ThirdPartyExchangeValue analogue,
//! vendor/ext/capnproto/c++/src/capnp/rpc.c++).
//!
//! Design contract (docs: the L3 VatC design, FINAL-v2 §3.1/§5):
//! - The index owns admission, accounting, and expiry for unmatched Accepts.
//!   Matched provisions retain independent refs, so index-first death cannot
//!   wedge an active handoff even though Finish/teardown refund index gauges.
//! - The `Provision` owns COPIES of its key and target (index allocator);
//!   per-peer `ProvideEntry` ownership is untouched.
//! - Refcount protocol: `by_key` (+1 while `indexed`), the owner peer's
//!   `provisions_by_question` entry (+1), each queued/parked accept (+1).
//!   Holder-side records on accept peers are UNCOUNTED back-links.
//! - Single-threaded: all attached peers and every index operation share one
//!   thread (debug-asserted; WorkerPool vats are unsupported).

const std = @import("std");
const builtin = @import("builtin");
const rpc_time = @import("../time.zig");

/// Vat-wide budgets. Defaults are chosen so a single-peer vat attaching an
/// index is never tighter than the Stable per-peer ceilings
/// (PeerLimits.max_active_provides default 4096 — NOT tightened; per-peer
/// limits are still enforced first on each connection).
pub const ProvisionIndexLimits = struct {
    max_provisions: usize = 4096,
    max_provision_key_bytes: usize = 1 << 20,
    max_parked_accepts: usize = 1024,
    /// Per-peer admission ceiling. This is intentionally much smaller than
    /// the vat-wide bound so one unauthenticated connection cannot consume
    /// every rendezvous slot shared by its siblings.
    max_parked_accepts_per_peer: usize = 64,
    /// Vat-wide attributable bytes held by parked Accepts. Each Accept is
    /// charged for its normalized recipient token plus its embargo bytes,
    /// even when several Accepts share the same token/provision.
    max_parked_accept_bytes: usize = 256 << 10,
    /// Per-peer counterpart of `max_parked_accept_bytes`.
    max_parked_accept_bytes_per_peer: usize = 16 << 10,
    max_embargoes_per_provision: usize = 4096,
    max_embargo_key_bytes_per_provision: usize = 64 << 10,
    /// Aggregate bounds on QUEUED embargoed accepts across all provisions
    /// (each queued accept also holds accept-peer records and key dupes).
    /// Without these, the worst case inside the per-provision bounds is
    /// max_provisions x max_embargoes_per_provision live entries.
    max_queued_accepts: usize = 4096,
    max_queued_accept_bytes: usize = 1 << 20,
    /// Time bound on Accept-before-Provide PARKING. An inbound `Accept` whose
    /// recipient token matches nothing parks; the token is arbitrary bytes and
    /// needs no prior Provide, no bootstrap, and no authentication, so the
    /// count/byte budgets above bound the reservation. Holder-side records are
    /// reclaimed on terminal transport close (and again idempotently at peer
    /// teardown). With a TTL set, an expired park is evicted loudly, with an
    /// exception `Return`, by inbound traffic, Accept/Provide, deadline
    /// maintenance, or an explicit sweep.
    ///
    /// Null (the default) keeps the pre-TTL behaviour bit-identical, and the
    /// bound is additionally inert without an INDEX-OWNED clock
    /// (`ProvisionIndex.setClock` or `setClockIo`) — same opt-in contract as
    /// `PeerTimeouts`.
    /// The clock is never taken from a peer or a frame: the party whose
    /// accepts are being timed out must not be able to steer their expiry.
    park_ttl_ms: ?u64 = null,
};

pub fn ProvisionIndex(comptime PeerType: type) type {
    return struct {
        const Self = @This();

        /// Refcounted, connection-independent provision object. Allocated with
        /// the index's allocator; destroyed by `release` at refcount zero.
        pub const Provision = struct {
            allocator: std.mem.Allocator,
            ref_count: u32 = 0,
            state: State = .awaiting,
            /// Normalized recipient/completion key (captureAnyPointerPayload
            /// bytes). OWNED by this object; `ProvisionIndex.by_key`'s map key
            /// borrows it (rule R3: every index removal happens strictly
            /// before the provision could be destroyed).
            recipient_key: []u8,
            /// Provide-arrival peer. Null while `.awaiting` and after close.
            owner: ?*PeerType = null,
            /// Provide question id in the owner's answer table. Meaningful iff
            /// `owner != null`.
            provide_question_id: u32 = 0,
            /// OWNED copy of the owner's resolved target (cloned with the
            /// INDEX allocator). Ids are in the OWNER's id spaces; consulted
            /// only while `owner != null`. Null while `.awaiting`.
            target: ?PeerType.ProvideTargetType = null,
            /// True when this provision holds a handoff pin on the owner's
            /// export (noteHandoffExportRef). Released on the `.finished`
            /// close; abandoned (left set, never released) at owner
            /// neutralization (rule R5 — the export table dies with the peer).
            target_export_pinned: bool = false,
            /// True when this provision holds a handoff pin on the owner's
            /// IMPORT of a receiverHosted target (noteHandoffImportPin).
            /// SEPARATE from `target_export_pinned` on purpose: import and
            /// export ids share one numeric space per connection, and a
            /// single flag would unpin the WRONG TABLE by a colliding bare
            /// id. Released (with its deferred wire Release) on the
            /// `.finished` close; abandoned at owner neutralization — the
            /// import table dies with the peer.
            target_import_pinned: bool = false,
            /// True while `ProvisionIndex.by_key` holds this object (+1 ref).
            indexed: bool = false,
            /// Per-provision embargo map, keyed by an OWNED dupe of the raw
            /// embargo bytes (zero length is legal). find-or-create from BOTH
            /// sides handles the Disembargo-before-Accept race. Entries are
            /// ERASED at consume, so completed embargo ids are reusable.
            embargoes: std.StringHashMap(ProvisionEmbargo),
            embargo_key_bytes: usize = 0,
            /// Accept-before-Provide queue (landing L5; empty until then).
            parked: std.ArrayList(ParkedAccept) = .empty,
            /// Close preparation has already detached every holder record and
            /// refunded every index/peer gauge. The slot/list entries retain
            /// only their final +1 refs for the later send-bearing drain.
            entries_neutralized: bool = false,

            pub const State = enum(u8) { awaiting, active, closed };

            pub fn retain(p: *Provision) void {
                p.ref_count += 1;
            }

            /// Decrement; at zero, destroy. Every counted holder is +1, so at
            /// zero there is no owner map entry, no index entry, and no
            /// queued/parked accept naming this provision. Infallible; frees
            /// only.
            pub fn release(p: *Provision) void {
                std.debug.assert(p.ref_count > 0);
                p.ref_count -= 1;
                if (p.ref_count != 0) return;
                std.debug.assert(p.owner == null);
                std.debug.assert(!p.indexed);
                const allocator = p.allocator;
                var it = p.embargoes.keyIterator();
                while (it.next()) |key| allocator.free(key.*);
                p.embargoes.deinit();
                for (p.parked.items) |*parked| {
                    if (parked.embargo) |bytes| allocator.free(bytes);
                }
                p.parked.deinit(allocator);
                if (p.target) |*target| target.deinit(allocator);
                allocator.free(p.recipient_key);
                allocator.destroy(p);
            }
        };

        /// One embargo id's state inside a provision. Single pending slot: a
        /// second Accept reusing the bytes while the entry lives is rejected
        /// before touching it. The entry is ERASED when consumed or when its
        /// queued accept is cancelled; after erasure the bytes are reusable.
        pub const ProvisionEmbargo = struct {
            /// The context.accept Disembargo for these bytes arrived on the
            /// owner connection.
            disembargoed: bool = false,
            /// An Accept has claimed these bytes (a duplicate is rejected).
            used_by_accept: bool = false,
            /// The one accept withheld behind this embargo. OWNS +1 ref; the
            /// code that moves it out of the slot inherits that ref.
            pending: ?PendingAccept = null,
        };

        pub const PendingAccept = struct {
            /// Back-linked via accept_peer.cross_peer_pending_accepts. May
            /// equal the owner (same-peer accepts on attached peers route into
            /// the provision store too).
            accept_peer: *PeerType,
            answer_id: u32,
        };

        /// Landing L5. `embargo` is owned by the index allocator; null = the
        /// parked Accept carried no embargo. OWNS +1 ref.
        pub const ParkedAccept = struct {
            accept_peer: *PeerType,
            answer_id: u32,
            embargo: ?[]u8,
            /// Per-Accept admission charge: normalized recipient token bytes
            /// plus embargo bytes. Stored explicitly so every detach path can
            /// refund exactly the amount admitted, even after either owned
            /// byte slice has been freed.
            attributed_bytes: usize,
            embargo_bytes: usize,
            /// Absolute monotonic deadline in INDEX-clock nanoseconds, stamped
            /// at park time (the `Question.deadline_ns` form). Null when the
            /// index had no clock or no `park_ttl_ms` when this entry parked —
            /// such an entry never expires, which is exactly the pre-TTL
            /// behaviour.
            deadline_ns: ?i64 = null,
        };

        allocator: std.mem.Allocator,
        /// Key BORROWS provision.recipient_key (the provision owns it).
        by_key: std.StringHashMap(*Provision),
        /// Back-links for neutralize-on-index-deinit. Maintained by
        /// Peer.attachProvisionIndex (dedupe-checked) and Peer.deinit
        /// (self-removal loops to exhaustion).
        attached_peers: std.ArrayList(*PeerType) = .empty,
        limits: ProvisionIndexLimits,
        /// INDEX-OWNED monotonic clock backing `limits.park_ttl_ms`. Never
        /// sourced from a peer or a frame — expiry of a stranger's parked
        /// accept must not be steerable by that same stranger. With no custom
        /// clock and no Io fallback, the raw index leaves TTL enforcement
        /// inert; the high-level Vat rejects that finite-TTL configuration.
        clock: ?rpc_time.Clock = null,
        /// Value-stored `std.Io` fallback for monotonic time. A custom Clock
        /// takes precedence. Storing Io here (rather than a Clock pointing at
        /// a movable Vat field) keeps the time source valid across moves.
        clock_io: ?std.Io = null,
        /// Running counters — no O(n) walks.
        provision_count: usize = 0,
        provision_key_bytes: usize = 0,
        parked_accept_count: usize = 0,
        /// Normalized recipient-token plus embargo bytes, charged per Accept.
        parked_accept_bytes: usize = 0,
        /// Embargo-only subset of `parked_accept_bytes`.
        parked_accept_embargo_bytes: usize = 0,
        queued_accept_count: usize = 0,
        queued_accept_bytes: usize = 0,
        /// Earliest stamped parked-Accept deadline. Inbound-frame checks read
        /// only this field before deciding whether a full sweep is due.
        next_park_deadline_ns: ?i64 = null,
        /// Suppresses recursive sweeps while an expiry Return re-enters peer
        /// dispatch. The outer sweep refreshes the deadline cache afterward.
        park_sweep_in_progress: bool = false,
        /// Guards every host-supplied park-clock callback, including the
        /// public due predicate and deadline stamping. A clock is arbitrary
        /// host code and may synchronously re-enter either path.
        park_clock_sample_in_progress: bool = false,
        /// Debug-only thread pin, recorded at first attach.
        thread_id: if (builtin.mode == .debug) ?std.Thread.Id else void =
            if (builtin.mode == .debug) null else {},
        thread_affinity_enabled: bool = true,

        pub fn init(allocator: std.mem.Allocator, limits: ProvisionIndexLimits) Self {
            return .{
                .allocator = allocator,
                .by_key = std.StringHashMap(*Provision).init(allocator),
                .limits = limits,
            };
        }

        /// Install (or clear) the index-owned custom monotonic clock. It takes
        /// precedence over the value-stored Io fallback; without either time
        /// source, `ProvisionIndexLimits.park_ttl_ms` is inert. The clock's
        /// `ctx` must outlive the index.
        pub fn setClock(self: *Self, clock: ?rpc_time.Clock) void {
            self.clock = clock;
        }

        /// Install (or clear) a value-stored `std.Io` monotonic fallback.
        /// `setClock(non-null)` always takes precedence over this source.
        pub fn setClockIo(self: *Self, io: ?std.Io) void {
            self.clock_io = io;
        }

        pub fn clockNow(self: *const Self) ?i64 {
            if (self.clock) |clock| return clock.now();
            if (self.clock_io) |io| {
                return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
            }
            return null;
        }

        /// Point-in-time gauges for the Experimental vat-hosting surface.
        pub const Stats = struct {
            provisions: usize,
            provision_key_bytes: usize,
            parked_accepts: usize,
            parked_accept_attributed_bytes: usize,
            parked_accept_embargo_bytes: usize,
            queued_accepts: usize,
            queued_accept_bytes: usize,
        };

        pub fn stats(self: *const Self) Stats {
            return .{
                .provisions = self.provision_count,
                .provision_key_bytes = self.provision_key_bytes,
                .parked_accepts = self.parked_accept_count,
                .parked_accept_attributed_bytes = self.parked_accept_bytes,
                .parked_accept_embargo_bytes = self.parked_accept_embargo_bytes,
                .queued_accepts = self.queued_accept_count,
                .queued_accept_bytes = self.queued_accept_bytes,
            };
        }

        pub const ParkCharge = struct {
            attributed_bytes: usize,
            embargo_bytes: usize,
        };

        /// Validate both peer-local and vat-wide park admission without
        /// changing state. Single-threaded index affinity makes the later
        /// infallible commit atomic with respect to other admissions.
        pub fn checkParkAdmission(
            self: *const Self,
            peer: *const PeerType,
            recipient_bytes: usize,
            embargo_bytes: usize,
        ) !ParkCharge {
            const attributed = std.math.add(usize, recipient_bytes, embargo_bytes) catch
                return error.ParkBudgetExceeded;
            if (peer.parked_accept_count >= self.limits.max_parked_accepts_per_peer) {
                return error.PeerParkBudgetExceeded;
            }
            const peer_bytes = std.math.add(usize, peer.parked_accept_bytes, attributed) catch
                return error.PeerParkBudgetExceeded;
            if (peer_bytes > self.limits.max_parked_accept_bytes_per_peer) {
                return error.PeerParkBudgetExceeded;
            }
            if (self.parked_accept_count >= self.limits.max_parked_accepts) {
                return error.ParkBudgetExceeded;
            }
            const vat_bytes = std.math.add(usize, self.parked_accept_bytes, attributed) catch
                return error.ParkBudgetExceeded;
            if (vat_bytes > self.limits.max_parked_accept_bytes) return error.ParkBudgetExceeded;
            return .{ .attributed_bytes = attributed, .embargo_bytes = embargo_bytes };
        }

        /// Infallible tail after every allocation/map insertion succeeds.
        pub fn commitParkAdmission(self: *Self, peer: *PeerType, charge: ParkCharge) void {
            self.parked_accept_count += 1;
            self.parked_accept_bytes += charge.attributed_bytes;
            self.parked_accept_embargo_bytes += charge.embargo_bytes;
            peer.parked_accept_count += 1;
            peer.parked_accept_bytes += charge.attributed_bytes;
        }

        /// The sole parked-Accept refund primitive. Callers detach the live
        /// entry first, then refund once, before sends or frees can re-enter.
        pub fn refundParkAdmission(self: *Self, peer: *PeerType, parked: ParkedAccept) void {
            std.debug.assert(self.parked_accept_count > 0);
            std.debug.assert(self.parked_accept_bytes >= parked.attributed_bytes);
            std.debug.assert(self.parked_accept_embargo_bytes >= parked.embargo_bytes);
            std.debug.assert(peer.parked_accept_count > 0);
            std.debug.assert(peer.parked_accept_bytes >= parked.attributed_bytes);
            self.parked_accept_count -= 1;
            self.parked_accept_bytes -= parked.attributed_bytes;
            self.parked_accept_embargo_bytes -= parked.embargo_bytes;
            peer.parked_accept_count -= 1;
            peer.parked_accept_bytes -= parked.attributed_bytes;
        }

        pub fn noteParkDeadline(self: *Self, deadline_ns: ?i64) void {
            const deadline = deadline_ns orelse return;
            if (self.next_park_deadline_ns) |next| {
                if (deadline >= next) return;
            }
            self.next_park_deadline_ns = deadline;
        }

        /// Rebuild the earliest-deadline cache after a bulk removal/adoption.
        /// The hot inbound-frame due check remains O(1).
        pub fn refreshNextParkDeadline(self: *Self) void {
            var next: ?i64 = null;
            var it = self.by_key.valueIterator();
            while (it.next()) |prov_ptr| {
                const prov = prov_ptr.*;
                if (prov.state != .awaiting) continue;
                for (prov.parked.items) |parked| {
                    const deadline = parked.deadline_ns orelse continue;
                    if (next) |earliest| {
                        if (deadline >= earliest) continue;
                    }
                    next = deadline;
                }
            }
            self.next_park_deadline_ns = next;
        }

        pub fn isParkSweepDue(self: *Self) bool {
            if (self.park_sweep_in_progress or self.park_clock_sample_in_progress) return false;
            const deadline = self.next_park_deadline_ns orelse return false;
            self.park_clock_sample_in_progress = true;
            const now_opt = self.clockNow();
            // Clock.now may replace this Index at the same address. A cleared
            // guard means the sample belongs to the old generation; leave the
            // replacement untouched and report not-due.
            if (!self.park_clock_sample_in_progress) return false;
            self.park_clock_sample_in_progress = false;
            const now = now_opt orelse return false;
            return now >= deadline;
        }

        /// Best-effort public maintenance hook. The Peer implementation owns
        /// the send/reentrancy choreography; the generic index exposes it to
        /// Vat and transport deadline drivers without duplicating that logic.
        pub fn sweepExpiredParkedAccepts(self: *Self) usize {
            return PeerType.sweepExpiredParkedAcceptsForProvisionIndex(self);
        }

        pub fn disableThreadAffinity(self: *Self) void {
            self.thread_affinity_enabled = false;
        }

        pub fn assertThreadAffinity(self: *Self) void {
            if (builtin.mode != .debug) return;
            if (!self.thread_affinity_enabled) return;
            // wasm32-freestanding has no threads; skip the pin there (the
            // comptime guard pattern of src/rpc/peer/state.zig).
            if (builtin.target.cpu.arch == .wasm32 and builtin.target.os.tag == .freestanding) return;
            if (self.thread_id) |id| {
                std.debug.assert(id == std.Thread.getCurrentId());
            } else {
                self.thread_id = std.Thread.getCurrentId();
            }
        }

        /// Tear the index down. Active provisions are left to their owners, so
        /// matched handoffs keep working (the F6 anti-wedge property). Awaiting
        /// parks are globally detached and refunded before peer back-pointers
        /// are nulled. Destruction is callback-free: it does not attempt wire
        /// Returns through peers whose owner may deinitialize them on close.
        pub fn deinit(self: *Self) void {
            // (1) Move by_key out; the live map is emptied before any release,
            // so no map key can dangle into a freed recipient_key.
            var owned_by_key = self.by_key;
            self.by_key = std.StringHashMap(*Provision).init(self.allocator);

            // (2) SEND-FREE neutralization across EVERY awaiting provision.
            // Detach holder records and refund all park charges while every
            // peer still has a live index pointer and all of its maps are
            // reachable. Index destruction deliberately invokes no peer send,
            // observer, or lifecycle callback: any of those can synchronously
            // deinitialize a peer and invalidate later entries in this walk.
            var neutralize_it = owned_by_key.valueIterator();
            while (neutralize_it.next()) |prov_ptr| {
                const prov = prov_ptr.*;
                prov.indexed = false;
                self.provision_count -= 1;
                self.provision_key_bytes -= prov.recipient_key.len;
                if (prov.state == .awaiting) {
                    for (prov.parked.items) |parked| {
                        self.refundParkAdmission(parked.accept_peer, parked);
                        if (parked.accept_peer.cross_peer_pending_accepts.fetchRemove(parked.answer_id)) |kv| {
                            if (kv.value.embargo_key) |k| parked.accept_peer.allocator.free(k);
                        }
                    }
                }
            }

            // (3) Only after all parked holder state is neutralized, sever the
            // peer back-pointers. Safe against already-deinited peers:
            // Peer.deinit removes all its occurrences from this list.
            for (self.attached_peers.items) |peer| {
                peer.provision_index = null;
            }
            self.attached_peers.deinit(self.allocator);

            // (4) Freeing-only phase. Active provisions keep their owner refs
            // and stay fully functional.
            var drain_it = owned_by_key.valueIterator();
            while (drain_it.next()) |prov_ptr| {
                const prov = prov_ptr.*;
                if (prov.state == .awaiting) {
                    var owned_parked = prov.parked;
                    prov.parked = .empty;
                    for (owned_parked.items) |parked| {
                        if (parked.embargo) |bytes| self.allocator.free(bytes);
                        prov.release(); // the parked entry's +1
                    }
                    owned_parked.deinit(self.allocator);
                }
                prov.release(); // the index's +1
            }
            owned_by_key.deinit();
            self.next_park_deadline_ns = null;
            // Active provisions (including an already-matched embargo queue)
            // intentionally remain owned by their peers so the established
            // index-death anti-wedge contract still holds. The peer back-links
            // were severed above, so later queue release cannot touch this
            // destroyed index; retire its aggregate gauges exactly once here.
            self.queued_accept_count = 0;
            self.queued_accept_bytes = 0;
            std.debug.assert(self.provision_count == 0);
            std.debug.assert(self.provision_key_bytes == 0);
            std.debug.assert(self.parked_accept_count == 0);
            std.debug.assert(self.parked_accept_bytes == 0);
            std.debug.assert(self.parked_accept_embargo_bytes == 0);
        }
    };
}
