//! VatNetwork seam for Cap'n Proto RPC Level-3 three-party handoff.
//!
//! Origination (Provide/Accept) inherently reaches across more than one
//! connection: the vat originating a handoff (VatB in the spec's Alice/Bob/Carol
//! example) must name a *third* vat (VatC) that the recipient (VatA) is not
//! currently connected to, mint an opaque addressing token the recipient can
//! redeem on a *different* connection, and the recipient must resolve that token
//! back to a live `Peer` connected to VatC. The two-party `Peer` core knows
//! nothing of this — it owns exactly one `TransportBinding` and one remote.
//!
//! `VatNetwork` is the application-supplied vtable that closes that gap. It is
//! modeled on the `TransportBinding` pattern (`peer/transport.zig`): an opaque
//! `ctx` plus function pointers, generic over the concrete `Peer` type so the
//! seam never depends on the peer module (which in turn imports this one only
//! for an optional field).
//!
//! Two operations, one per role:
//!
//!   * `mint_introduction` — the **host of the provided cap side** (VatB).
//!     Given the peer B is provided-to (the host-of-recipient connection) and an
//!     opaque `recipient_hint` naming the recipient vat, produce the paired
//!     opaque tokens: a `ThirdPartyToAwait` (goes to VatC inside the `Provide`)
//!     and a `ThirdPartyToContact` (goes to VatA inside the resolved
//!     `thirdPartyHosted` descriptor), plus the raw nonce for bookkeeping.
//!
//!   * `connect_to_introduced` — the **recipient side** (VatA). Given a
//!     `ThirdPartyToContact` parsed from an inbound descriptor, resolve it to a
//!     live `Peer` connected to VatC (dial or registry lookup) plus the
//!     `ThirdPartyCompletion` to present in the `Accept`.
//!
//! The host-of-provided-cap side (VatC) needs no callback: its inbound
//! `handleProvide`/`handleAccept` already match the `Accept.provision` bytes
//! against the stored `Provide.recipient` bytes. The ONE invariant the network
//! must uphold for that match to succeed: the `ThirdPartyCompletion` returned by
//! `connect_to_introduced` on VatA must serialize **byte-for-byte identically**
//! to the `ThirdPartyToAwait` produced by `mint_introduction` on VatB, because
//! VatC keys its provide table on the serialized bytes of each.
//!
//! The concrete transport wiring (dialing over TCP/QUIC, authenticating the
//! third vat, negotiating token semantics with a non-Zig peer) is deliberately
//! out of this file. `LoopbackVatNetwork` below is the in-process concrete impl
//! used by tests: tokens are just the nonce bytes and "dialing" is a lookup in a
//! shared registry.

const std = @import("std");
const message = @import("../../serialization/message.zig");

/// The paired opaque tokens `mint_introduction` produces. All three slices are
/// heap-allocated with the network's allocator and owned by the caller
/// (`Peer.sendProvide`), which frees them once the Provide/descriptor have been
/// serialized (the wire builders clone the payloads).
pub const Introduction = struct {
    /// `ThirdPartyToAwait`: serialized AnyPointer message. Sent to the host of
    /// the provided cap (VatC) as `Provide.recipient`. VatC stores its bytes as
    /// the provide's recipient key.
    to_await: []u8,
    /// `ThirdPartyToContact`: serialized AnyPointer message. Sent to the
    /// recipient (VatA) inside the `thirdPartyHosted` descriptor's `id` field.
    to_contact: []u8,
    /// Raw nonce identifying this introduction. Owned; freed by the caller.
    /// Kept distinct from the token payloads so the concrete network can encode
    /// tokens however it likes without the peer layer reaching into them.
    nonce: []u8,

    pub fn deinit(self: *Introduction, allocator: std.mem.Allocator) void {
        allocator.free(self.to_await);
        allocator.free(self.to_contact);
        allocator.free(self.nonce);
    }
};

/// What `connect_to_introduced` resolves a `ThirdPartyToContact` to on the
/// recipient side (VatA): the live peer connected to the third vat (VatC) and
/// the `ThirdPartyCompletion` to present in the `Accept`.
pub fn Introduced(comptime PeerType: type) type {
    return struct {
        /// Live peer connected to the third vat (VatC). Borrowed — the network
        /// owns the connection's lifetime; the caller only sends an `Accept` on
        /// it.
        peer: *PeerType,
        /// `ThirdPartyToContact` → `ThirdPartyCompletion`: serialized AnyPointer
        /// message to present as `Accept.provision`. Owned; `Peer.sendAccept`
        /// frees it once the Accept has been serialized.
        completion: []u8,

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            allocator.free(self.completion);
        }
    };
}

/// Application-supplied three-party addressing vtable. Generic over the concrete
/// `Peer` type so this module never imports the peer module.
pub fn VatNetwork(comptime PeerType: type) type {
    return struct {
        const Self = @This();
        pub const IntroducedType = Introduced(PeerType);

        /// Host-of-provided-cap side (VatB): mint the paired tokens naming the
        /// recipient. `host_peer` is the connection VatB was provided-to (the
        /// host-of-recipient peer); `recipient_hint` is an opaque,
        /// network-defined descriptor of the recipient vat.
        pub const MintIntroductionFn = *const fn (
            ctx: *anyopaque,
            host_peer: *PeerType,
            recipient_hint: []const u8,
        ) anyerror!Introduction;

        /// Recipient side (VatA): resolve a `ThirdPartyToContact` to a live peer
        /// connected to the third vat plus the completion to present in Accept.
        pub const ConnectToIntroducedFn = *const fn (
            ctx: *anyopaque,
            contact: message.AnyPointerReader,
        ) anyerror!IntroducedType;

        ctx: *anyopaque,
        mint_introduction: MintIntroductionFn,
        connect_to_introduced: ConnectToIntroducedFn,

        pub fn init(
            ctx: *anyopaque,
            mint_introduction: MintIntroductionFn,
            connect_to_introduced: ConnectToIntroducedFn,
        ) Self {
            return .{
                .ctx = ctx,
                .mint_introduction = mint_introduction,
                .connect_to_introduced = connect_to_introduced,
            };
        }

        pub fn mintIntroduction(
            self: Self,
            host_peer: *PeerType,
            recipient_hint: []const u8,
        ) !Introduction {
            return self.mint_introduction(self.ctx, host_peer, recipient_hint);
        }

        pub fn connectToIntroduced(
            self: Self,
            contact: message.AnyPointerReader,
        ) !IntroducedType {
            return self.connect_to_introduced(self.ctx, contact);
        }
    };
}

/// Build an opaque token payload: a standalone message whose root AnyPointer
/// holds `nonce` as a Data blob. The loopback network uses the same encoding for
/// all three token kinds (to_await / to_contact / completion), so the
/// serialized bytes of `to_await` and `completion` are identical — exactly the
/// byte-equality VatC's provide table relies on.
pub fn encodeNonceToken(allocator: std.mem.Allocator, nonce: []const u8) ![]u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.initRootAnyPointer();
    try root.setData(nonce);
    const bytes = try builder.toBytes();
    return @constCast(bytes);
}

/// In-process concrete `VatNetwork` for tests and single-process topologies.
///
/// A registry maps a nonce to the peer that hosts (is connected to) the third
/// vat. The app pre-registers `(nonce -> c_peer)` via `register`, exactly as a
/// real network learns of connections; `mint_introduction` then takes that
/// nonce as its `recipient_hint` and encodes the three tokens from it, and
/// `connect_to_introduced` decodes the contact's nonce, looks the peer up, and
/// hands back a completion token byte-identical to the await token.
///
/// Keeping the concrete third-vat connection out of the vtable arguments (the
/// hint is an opaque app-defined nonce, never a smuggled pointer) is what keeps
/// the `VatNetwork` seam free of the loopback assumption.
pub fn LoopbackVatNetwork(comptime PeerType: type) type {
    return struct {
        const Self = @This();
        pub const Net = VatNetwork(PeerType);
        pub const IntroducedType = Introduced(PeerType);

        allocator: std.mem.Allocator,
        /// nonce (owned) -> peer connected to the third vat (borrowed).
        registry: std.StringHashMap(*PeerType),

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .registry = std.StringHashMap(*PeerType).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.registry.keyIterator();
            while (it.next()) |key| self.allocator.free(key.*);
            self.registry.deinit();
        }

        /// Return the vtable view over this loopback network.
        pub fn network(self: *Self) Net {
            return Net.init(self, mintIntroduction, connectToIntroduced);
        }

        /// Register the peer connected to the third vat under `nonce`. The nonce
        /// is the opaque token `mint_introduction` will be handed as its
        /// `recipient_hint` and that `connect_to_introduced` redeems.
        pub fn register(self: *Self, nonce: []const u8, third_vat_peer: *PeerType) !void {
            const key = try self.allocator.dupe(u8, nonce);
            var owns_key = true;
            errdefer if (owns_key) self.allocator.free(key);
            const gop = try self.registry.getOrPut(key);
            if (gop.found_existing) {
                return error.DuplicateNonce;
            }
            owns_key = false;
            gop.value_ptr.* = third_vat_peer;
        }

        fn mintIntroduction(
            ctx: *anyopaque,
            host_peer: *PeerType,
            recipient_hint: []const u8,
        ) anyerror!Introduction {
            _ = host_peer;
            const self: *Self = @ptrCast(@alignCast(ctx));

            // The recipient hint IS the nonce; the third-vat peer must already be
            // registered under it. A real network would instead consult its own
            // routing to locate/dial the third vat named by the hint.
            if (!self.registry.contains(recipient_hint)) return error.UnknownIntroduction;

            const nonce = try self.allocator.dupe(u8, recipient_hint);
            errdefer self.allocator.free(nonce);
            const to_await = try encodeNonceToken(self.allocator, recipient_hint);
            errdefer self.allocator.free(to_await);
            const to_contact = try encodeNonceToken(self.allocator, recipient_hint);
            errdefer self.allocator.free(to_contact);

            return .{ .to_await = to_await, .to_contact = to_contact, .nonce = nonce };
        }

        fn connectToIntroduced(
            ctx: *anyopaque,
            contact: message.AnyPointerReader,
        ) anyerror!IntroducedType {
            const self: *Self = @ptrCast(@alignCast(ctx));
            // Nonce Data blob is read straight from the reader's frame; it is
            // consumed before this function returns, so no owned copy is needed.
            const nonce = try contact.getData();
            const third_vat_peer = self.registry.get(nonce) orelse return error.UnknownIntroduction;
            // The completion token is byte-identical to the await token because
            // both encode the same nonce with the same encoder.
            const completion = try encodeNonceToken(self.allocator, nonce);
            return .{ .peer = third_vat_peer, .completion = completion };
        }
    };
}
