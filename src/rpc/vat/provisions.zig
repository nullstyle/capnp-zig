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
//! - The index is consulted ONLY at Provide registration, Accept lookup, and
//!   (later) Accept parking. Release, Finish, and teardown paths never touch
//!   it — index-first death cannot wedge a matched handoff.
//! - The `Provision` owns COPIES of its key and target (index allocator);
//!   per-peer `ProvideEntry` ownership is untouched.
//! - Refcount protocol: `by_key` (+1 while `indexed`), the owner peer's
//!   `provisions_by_question` entry (+1), each queued/parked accept (+1).
//!   Holder-side records on accept peers are UNCOUNTED back-links.
//! - Single-threaded: all attached peers and every index operation share one
//!   thread (debug-asserted; WorkerPool vats are unsupported).

const std = @import("std");
const builtin = @import("builtin");

/// Vat-wide budgets. Defaults are chosen so a single-peer vat attaching an
/// index is never tighter than the Stable per-peer ceilings
/// (PeerLimits.max_active_provides default 4096 — NOT tightened; per-peer
/// limits are still enforced first on each connection).
pub const ProvisionIndexLimits = struct {
    max_provisions: usize = 4096,
    max_provision_key_bytes: usize = 1 << 20,
    max_parked_accepts: usize = 1024,
    max_parked_accept_bytes: usize = 256 << 10,
    max_embargoes_per_provision: usize = 4096,
    max_embargo_key_bytes_per_provision: usize = 64 << 10,
    /// Aggregate bounds on QUEUED embargoed accepts across all provisions
    /// (each queued accept also holds accept-peer records and key dupes).
    /// Without these, the worst case inside the per-provision bounds is
    /// max_provisions x max_embargoes_per_provision live entries.
    max_queued_accepts: usize = 4096,
    max_queued_accept_bytes: usize = 1 << 20,
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
        };

        allocator: std.mem.Allocator,
        /// Key BORROWS provision.recipient_key (the provision owns it).
        by_key: std.StringHashMap(*Provision),
        /// Back-links for neutralize-on-index-deinit. Maintained by
        /// Peer.attachProvisionIndex (dedupe-checked) and Peer.deinit
        /// (self-removal loops to exhaustion).
        attached_peers: std.ArrayList(*PeerType) = .empty,
        limits: ProvisionIndexLimits,
        /// Running counters — no O(n) walks.
        provision_count: usize = 0,
        provision_key_bytes: usize = 0,
        parked_accept_count: usize = 0,
        parked_accept_bytes: usize = 0,
        queued_accept_count: usize = 0,
        queued_accept_bytes: usize = 0,
        /// Debug-only thread pin, recorded at first attach.
        thread_id: if (builtin.mode == .Debug) ?std.Thread.Id else void =
            if (builtin.mode == .Debug) null else {},
        thread_affinity_enabled: bool = true,

        pub fn init(allocator: std.mem.Allocator, limits: ProvisionIndexLimits) Self {
            return .{
                .allocator = allocator,
                .by_key = std.StringHashMap(*Provision).init(allocator),
                .limits = limits,
            };
        }

        pub fn disableThreadAffinity(self: *Self) void {
            self.thread_affinity_enabled = false;
        }

        pub fn assertThreadAffinity(self: *Self) void {
            if (builtin.mode != .Debug) return;
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

        /// Tear the index down. Active provisions are left to their owners
        /// (release/Finish/teardown never consult the index, so matched
        /// handoffs keep working — the F6 anti-wedge property); each attached
        /// peer's back-pointer is nulled. Awaiting provisions cannot exist
        /// before Accept-parking lands; when it does, their parked accepts are
        /// failed through the shared peer-side fail helper before the index
        /// ref is dropped.
        pub fn deinit(self: *Self) void {
            // (1) Sever every attached peer's back-pointer. Safe against
            // already-deinited peers: Peer.deinit removes ALL its occurrences.
            for (self.attached_peers.items) |peer| {
                peer.provision_index = null;
            }
            self.attached_peers.deinit(self.allocator);

            // (2) Move by_key out; the live map is emptied before any release,
            // so no map key can dangle into a freed recipient_key.
            var owned_by_key = self.by_key;
            self.by_key = std.StringHashMap(*Provision).init(self.allocator);

            // (3) Per provision: unindex + drop the index ref. Active/closed
            // provisions keep their owner refs and stay fully functional.
            var it = owned_by_key.valueIterator();
            while (it.next()) |prov_ptr| {
                const prov = prov_ptr.*;
                prov.indexed = false;
                self.provision_count -= 1;
                self.provision_key_bytes -= prov.recipient_key.len;
                prov.release(); // the index's +1
            }
            owned_by_key.deinit();
        }
    };
}
