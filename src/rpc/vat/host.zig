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

        index: provisions.ProvisionIndex(PeerType),
        rng: std.Random.DefaultCsprng,

        pub const Options = struct {
            /// Seed the embargo-id CSPRNG from this Io's OS entropy
            /// (`randomSecure`, FAIL CLOSED on unavailability).
            io: ?std.Io = null,
            /// Deterministic seed for tests and freestanding targets.
            /// Takes precedence over `io` when both are set.
            seed: ?[32]u8 = null,
            limits: provisions.ProvisionIndexLimits = .{},
            /// Vat-owned monotonic clock for `limits.park_ttl_ms`. The clock's
            /// `ctx` must outlive the vat. Null (the default) leaves the
            /// parked-accept TTL inert even when `park_ttl_ms` is set — the
            /// clock is deliberately NOT derived from any enrolled peer.
            clock: ?rpc_time.Clock = null,
        };

        /// Construct the vat. Requires an entropy decision: an explicit
        /// deterministic `seed`, or an `io` whose `randomSecure` succeeds —
        /// neither is `error.EntropyUnavailable` (never a guessable default).
        pub fn init(allocator: std.mem.Allocator, options: Options) !Self {
            var seed: [std.Random.DefaultCsprng.secret_seed_length]u8 = undefined;
            if (options.seed) |s| {
                seed = s;
            } else if (options.io) |io| {
                try io.randomSecure(&seed);
            } else {
                return error.EntropyUnavailable;
            }
            var index = provisions.ProvisionIndex(PeerType).init(allocator, options.limits);
            index.setClock(options.clock);
            return .{
                .index = index,
                .rng = std.Random.DefaultCsprng.init(seed),
            };
        }

        /// Install (or clear) the vat-owned monotonic clock after construction.
        pub fn setClock(self: *Self, clock: ?rpc_time.Clock) void {
            self.index.setClock(clock);
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
