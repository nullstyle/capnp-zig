//! Experimental DX facade for a multi-connection VAT that hosts Level-3
//! three-party handoffs: one object owning the vat-wide `ProvisionIndex` and
//! the accept-embargo CSPRNG, so "a vat listening on two transports" is one
//! `Vat` plus one enrolled `Peer` per connection.
//!
//! Threading contract: single-threaded, like the index it owns — every
//! enrolled peer must live on the same thread (WorkerPool vats are
//! unsupported). Teardown order is free: `deinit` severs every enrolled
//! peer's back-pointer, and peers that die first remove themselves.

const std = @import("std");
const provisions = @import("./provisions.zig");
const rpc_time = @import("../time.zig");

pub fn Vat(comptime PeerType: type) type {
    return struct {
        const Self = @This();
        const Index = provisions.ProvisionIndex(PeerType);

        index: Index,
        rng: std.Random.DefaultCsprng,

        pub const Options = struct {
            pub const default_park_ttl_ms: u64 = 30_000;

            /// Vat-facing provision limits. These mirror the raw index limits
            /// but deliberately carry the high-level 30-second TTL default.
            /// Keeping a distinct type ensures a partial literal such as
            /// `.limits = .{ .max_parked_accepts = 8 }` cannot accidentally
            /// inherit the raw index's null (opt-in) expiry policy.
            pub const Limits = struct {
                max_provisions: usize = 4096,
                max_provision_key_bytes: usize = 1 << 20,
                max_parked_accepts: usize = 1024,
                max_parked_accepts_per_peer: usize = 64,
                max_parked_accept_bytes: usize = 256 << 10,
                max_parked_accept_bytes_per_peer: usize = 16 << 10,
                max_embargoes_per_provision: usize = 4096,
                max_embargo_key_bytes_per_provision: usize = 64 << 10,
                max_queued_accepts: usize = 4096,
                max_queued_accept_bytes: usize = 1 << 20,
                park_ttl_ms: ?u64 = default_park_ttl_ms,

                fn toIndexLimits(self: @This()) provisions.ProvisionIndexLimits {
                    return .{
                        .max_provisions = self.max_provisions,
                        .max_provision_key_bytes = self.max_provision_key_bytes,
                        .max_parked_accepts = self.max_parked_accepts,
                        .max_parked_accepts_per_peer = self.max_parked_accepts_per_peer,
                        .max_parked_accept_bytes = self.max_parked_accept_bytes,
                        .max_parked_accept_bytes_per_peer = self.max_parked_accept_bytes_per_peer,
                        .max_embargoes_per_provision = self.max_embargoes_per_provision,
                        .max_embargo_key_bytes_per_provision = self.max_embargo_key_bytes_per_provision,
                        .max_queued_accepts = self.max_queued_accepts,
                        .max_queued_accept_bytes = self.max_queued_accept_bytes,
                        .park_ttl_ms = self.park_ttl_ms,
                    };
                }
            };

            /// Seed the embargo-id CSPRNG from this Io's OS entropy
            /// (`randomSecure`, FAIL CLOSED on unavailability). When no custom
            /// `clock` is supplied, the same Io is stored by value in the
            /// provision index and supplies monotonic parked-Accept time.
            io: ?std.Io = null,
            /// Deterministic seed for tests and freestanding targets.
            /// Takes precedence over `io` when both are set.
            seed: ?[32]u8 = null,
            /// Secure high-level default: unmatched Accepts expire after 30s.
            /// Set `park_ttl_ms = null` explicitly for compatibility with the
            /// raw `ProvisionIndex` opt-in policy.
            limits: Limits = .{},
            /// Vat-owned monotonic clock for `limits.park_ttl_ms`. The clock's
            /// `ctx` must outlive the vat. Takes precedence over `io`.
            clock: ?rpc_time.Clock = null,
        };

        pub const Stats = Index.Stats;

        /// Construct the vat. Requires an entropy decision: an explicit
        /// deterministic `seed`, or an `io` whose `randomSecure` succeeds —
        /// neither is `error.EntropyUnavailable` (never a guessable default).
        /// A finite parked-Accept TTL additionally requires `clock` or `io`;
        /// absence is `error.ParkClockUnavailable` after entropy validation.
        pub fn init(allocator: std.mem.Allocator, options: Options) !Self {
            const index_limits = options.limits.toIndexLimits();
            var seed: [std.Random.DefaultCsprng.secret_seed_length]u8 = undefined;
            if (options.seed) |s| {
                seed = s;
            } else if (options.io) |io| {
                try io.randomSecure(&seed);
            } else {
                return error.EntropyUnavailable;
            }
            // Keep entropy failure precedence stable: callers that supplied no
            // entropy decision still receive EntropyUnavailable even though
            // the secure Vat default also requires a time source.
            if (index_limits.park_ttl_ms != null and options.clock == null and options.io == null) {
                return error.ParkClockUnavailable;
            }

            var index = Index.init(allocator, index_limits);
            index.setClock(options.clock);
            index.setClockIo(options.io);
            return .{
                .index = index,
                .rng = std.Random.DefaultCsprng.init(seed),
            };
        }

        /// Install (or clear) the vat-owned custom clock after construction.
        /// Clearing it falls back to the value-stored Io, when configured.
        /// Park deadlines are absolute in their source clock's domain, so an
        /// effective source change is rejected while any parked Accept is live.
        /// The same rejection applies while a park deadline is being sampled,
        /// before the new reservation has committed its accounting.
        /// Reinstalling the identical custom handle (or clearing an already
        /// cleared handle) is a no-op and remains allowed.
        pub fn setClock(self: *Self, clock: ?rpc_time.Clock) !void {
            if (!sameCustomClock(self.index.clock, clock) and
                (self.index.parked_accept_count != 0 or self.index.park_clock_sample_in_progress))
            {
                return error.ParkClockInUse;
            }
            if (clock == null and self.index.limits.park_ttl_ms != null and self.index.clock_io == null) {
                return error.ParkClockUnavailable;
            }
            self.index.setClock(clock);
        }

        fn sameCustomClock(a: ?rpc_time.Clock, b: ?rpc_time.Clock) bool {
            const lhs = a orelse return b == null;
            const rhs = b orelse return false;
            return lhs.ctx == rhs.ctx and lhs.now_fn == rhs.now_fn;
        }

        /// Point-in-time vat-wide provision and parked-Accept gauges.
        pub fn stats(self: *const Self) Stats {
            return self.index.stats();
        }

        /// Expire every parked Accept whose deadline is due. Returns the
        /// number detached; exception Returns are best-effort.
        pub fn sweepExpiredParkedAccepts(self: *Self) usize {
            return self.index.sweepExpiredParkedAccepts();
        }

        pub fn deinit(self: *Self) void {
            self.index.deinit();
        }

        pub fn disableThreadAffinity(self: *Self) void {
            self.index.disableThreadAffinity();
        }

        /// Enroll one connection's peer: attach it to the vat-wide provision
        /// index (preconditions: not already attached, no pre-existing
        /// per-peer handoff state) and install the vat's embargo-id entropy.
        pub fn enroll(self: *Self, peer: *PeerType) !void {
            try peer.attachProvisionIndex(&self.index);
            peer.setEntropySource(.{ .ctx = &self.rng, .fill = fillFromRng });
        }

        /// Withdraw a peer (precondition: none of its handoff state is live).
        pub fn withdraw(self: *Self, peer: *PeerType) !void {
            _ = self;
            try peer.detachProvisionIndex();
        }

        fn fillFromRng(ctx: *anyopaque, buf: []u8) void {
            const rng: *std.Random.DefaultCsprng = @ptrCast(@alignCast(ctx));
            rng.random().bytes(buf);
        }
    };
}
