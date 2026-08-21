//! QuicVatNetwork — the first out-of-process `VatNetwork` shape: three-party
//! introductions whose `ThirdPartyToContact` is a **provision ticket** carrying
//! everything a recipient needs to reach the third vat over QUIC:
//!
//!   { version, dictated initial DCID, nonce, vat key, address hints,
//!     reset token (reserved) }
//!
//! The ticket encodes the QUIC durable-caps rendezvous mechanic ("a CID names
//! a route, crypto authorizes"): the DCID lets the third vat's endpoint
//! recognize and route a pre-arranged dial from the very first datagram
//! (RFC 8999 §5.1 header peek), while authorization stays with the nonce,
//! which only ever travels inside TLS-protected capnp frames (the Provide to
//! VatC and the Accept's completion). The DCID is plaintext on the wire —
//! routing, never authorization.
//!
//! Deliberate v1 boundaries (each recorded in docs/quic-durable-caps-plan.md):
//!
//!   * `connect_to_introduced` is synchronous and runs inside frame dispatch,
//!     so redemption only consults the **pre-established pool** — a live,
//!     same-thread `Peer` the application registered for the ticket's vat key.
//!     There is no dial-on-miss; a miss is an error (the recipient role then
//!     falls back to the vine proxy, the callee role fails the call).
//!   * The DCID and nonce are minted from this network's CSPRNG. quic-zig
//!     validates only the 8..20 length at connect; unpredictability (RFC 9000
//!     §7.2 — Initial keys derive from the DCID) is entirely on the minter.
//!     The mint uses 8 bytes so the value survives inside a Retry token's
//!     plaintext budget, though provision dials must not be Retried at all:
//!     a Retry replaces the dictated DCID on the wire and the claim misses.
//!   * `reset_token` is reserved and empty: quic v0.16 has no client-side
//!     knob to preinstall an expected stateless-reset token, and the §18.2
//!     transport parameter already covers the primary CID.
//!
//! Ownership contract (mirrors `LoopbackVatNetwork`): the network's allocator
//! owns every token it returns (`owner_allocator` is always set); registered
//! vats and pooled peers are borrowed and must outlive their use. Everything
//! here is single-threaded — both seam consumers assert thread affinity on
//! the returned peer. Unregistering a pooled peer only stops NEW redemptions;
//! in-flight handoff state (pending Accept questions, automatic third-party
//! routes) still borrows the peer, so tear peers down only once that state
//! has drained.

const std = @import("std");
const message = @import("../../serialization/message.zig");
const network_mod = @import("network.zig");

/// Current provision-ticket wire version.
pub const ticket_version: u16 = 1;

/// Nonce length minted into tickets. Redemption requires exactly this length.
pub const nonce_len: usize = 16;

/// Dictated-DCID length minted into tickets (Retry-token plaintext budget).
/// Redemption accepts the full RFC 9000 8..20 range.
pub const minted_dcid_len: usize = 8;

/// Ticket field bounds enforced by `decodeTicket`.
pub const min_dcid_len: usize = 8;
pub const max_dcid_len: usize = 20;
pub const max_vat_key_len: usize = 64;
pub const max_addr_hints: usize = 8;

/// One dial target for the third vat: raw address bytes + port, so no text
/// grammar exists to drift. `bytes[0..len]` is valid; `len` is 4 or 16.
pub const AddrHint = struct {
    bytes: [16]u8,
    len: u8,
    port: u16,

    pub fn ip4(bytes: [4]u8, port: u16) AddrHint {
        var hint = AddrHint{ .bytes = @splat(0), .len = 4, .port = port };
        @memcpy(hint.bytes[0..4], &bytes);
        return hint;
    }

    pub fn ip6(bytes: [16]u8, port: u16) AddrHint {
        return .{ .bytes = bytes, .len = 16, .port = port };
    }

    pub fn slice(self: *const AddrHint) []const u8 {
        return self.bytes[0..self.len];
    }
};

/// Borrowed view of a decoded provision ticket. All slices point into the
/// backing message frame and are only valid while it lives.
pub const TicketView = struct {
    version: u16,
    dcid: []const u8,
    nonce: []const u8,
    vat_key: []const u8,
    reset_token: []const u8,
    hints_packed: []const u8,

    /// Unpack the address hints into `out`; returns the filled prefix.
    /// `decodeTicket` already validated the packed bytes, so the only error
    /// here is an undersized `out`.
    pub fn parseHints(self: TicketView, out: []AddrHint) ![]AddrHint {
        var count: usize = 0;
        var i: usize = 0;
        while (i < self.hints_packed.len) {
            if (count == out.len) return error.OutOfSpace;
            const len: usize = self.hints_packed[i];
            const port = std.mem.readInt(u16, self.hints_packed[i + 1 ..][0..2], .big);
            var hint = AddrHint{ .bytes = @splat(0), .len = @intCast(len), .port = port };
            @memcpy(hint.bytes[0..len], self.hints_packed[i + 3 ..][0..len]);
            out[count] = hint;
            count += 1;
            i += 3 + len;
        }
        return out[0..count];
    }
};

/// Serialize a provision ticket: a standalone message whose root AnyPointer
/// holds the compact Zig convention `struct(1 data word, 5 pointers)`:
/// `version :UInt16 @0`; pointers `dcid :Data`, `nonce :Data`,
/// `vat_key :Data`, `reset_token :Data` (empty in v1), packed hints `:Data`.
/// One deterministic encoder; field order never varies.
pub fn encodeTicket(
    allocator: std.mem.Allocator,
    dcid: []const u8,
    nonce: []const u8,
    vat_key: []const u8,
    hints: []const AddrHint,
) ![]u8 {
    if (dcid.len < min_dcid_len or dcid.len > max_dcid_len) return error.InvalidDcid;
    if (nonce.len != nonce_len) return error.InvalidNonce;
    if (vat_key.len == 0 or vat_key.len > max_vat_key_len) return error.InvalidVatKey;
    var packed_buf: [max_addr_hints * 19]u8 = undefined;
    var packed_len: usize = 0;
    if (hints.len > max_addr_hints) return error.TooManyAddrHints;
    for (hints) |*hint| {
        if (hint.len != 4 and hint.len != 16) return error.InvalidAddrHint;
        packed_buf[packed_len] = hint.len;
        std.mem.writeInt(u16, packed_buf[packed_len + 1 ..][0..2], hint.port, .big);
        @memcpy(packed_buf[packed_len + 3 ..][0..hint.len], hint.slice());
        packed_len += 3 + hint.len;
    }

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.initRootAnyPointer();
    const ticket = try root.initStruct(1, 5);
    ticket.writeU16(0, ticket_version);
    try ticket.writeData(0, dcid);
    try ticket.writeData(1, nonce);
    try ticket.writeData(2, vat_key);
    try ticket.writeData(3, &.{});
    try ticket.writeData(4, packed_buf[0..packed_len]);
    const bytes = try builder.toBytes();
    return @constCast(bytes);
}

/// Decode and bounds-check a provision ticket from an already-validated
/// AnyPointer (the seam hands one straight from the inbound frame). Fails
/// cleanly on foreign tokens — a Data-rooted loopback nonce token is
/// `error.MalformedTicket`, never a crash.
pub fn decodeTicket(contact: message.AnyPointerReader) !TicketView {
    const ticket = contact.getStruct() catch return error.MalformedTicket;
    const version = ticket.readU16(0);
    if (version != ticket_version) return error.UnsupportedTicketVersion;
    const dcid = ticket.readData(0) catch return error.MalformedTicket;
    const nonce = ticket.readData(1) catch return error.MalformedTicket;
    const vat_key = ticket.readData(2) catch return error.MalformedTicket;
    const reset_token = ticket.readData(3) catch return error.MalformedTicket;
    const hints_packed = ticket.readData(4) catch return error.MalformedTicket;
    if (dcid.len < min_dcid_len or dcid.len > max_dcid_len) return error.MalformedTicket;
    if (nonce.len != nonce_len) return error.MalformedTicket;
    if (vat_key.len == 0 or vat_key.len > max_vat_key_len) return error.MalformedTicket;

    var hint_count: usize = 0;
    var i: usize = 0;
    while (i < hints_packed.len) {
        if (i + 3 > hints_packed.len) return error.MalformedTicket;
        const len: usize = hints_packed[i];
        if (len != 4 and len != 16) return error.MalformedTicket;
        if (i + 3 + len > hints_packed.len) return error.MalformedTicket;
        hint_count += 1;
        if (hint_count > max_addr_hints) return error.MalformedTicket;
        i += 3 + len;
    }

    return .{
        .version = version,
        .dcid = dcid,
        .nonce = nonce,
        .vat_key = vat_key,
        .reset_token = reset_token,
        .hints_packed = hints_packed,
    };
}

/// Serialize the `ThirdPartyToAwait` / `ThirdPartyCompletion` token:
/// `struct(1, 2)` with `version :UInt16 @0`, `nonce :Data`, `dcid :Data`.
/// The SAME function produces both sides, which is what upholds the seam's
/// byte-identity invariant — VatC matches Accept.provision against
/// Provide.recipient by raw serialized bytes.
pub fn encodeAwaitToken(
    allocator: std.mem.Allocator,
    nonce: []const u8,
    dcid: []const u8,
) ![]u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.initRootAnyPointer();
    const token = try root.initStruct(1, 2);
    token.writeU16(0, ticket_version);
    try token.writeData(0, nonce);
    try token.writeData(1, dcid);
    const bytes = try builder.toBytes();
    return @constCast(bytes);
}

/// Concrete `VatNetwork` whose introductions ride provision tickets and whose
/// redemptions resolve against a pre-established pool of live peers.
pub fn QuicVatNetwork(comptime PeerType: type) type {
    return struct {
        const Self = @This();
        pub const Net = network_mod.VatNetwork(PeerType);
        pub const IntroducedType = network_mod.Introduced(PeerType);

        pub const Options = struct {
            /// Seed the nonce/DCID CSPRNG from this Io's OS entropy
            /// (`randomSecure`, FAIL CLOSED on unavailability).
            io: ?std.Io = null,
            /// Deterministic seed for tests. Takes precedence over `io`.
            seed: ?[std.Random.DefaultCsprng.secret_seed_length]u8 = null,
        };

        allocator: std.mem.Allocator,
        /// Source of nonce/DCID bytes. Ticket security rests on this being
        /// cryptographically unpredictable (RFC 9000 §7.2 — quic-zig
        /// validates only the DCID length).
        rng: std.Random.DefaultCsprng,
        /// vat key (owned) -> address hints (owned) for minting tickets.
        vats: std.StringHashMap([]AddrHint),
        /// vat key (owned) -> live peer connected to that vat (borrowed).
        pool: std.StringHashMap(*PeerType),

        /// Construct the network. Requires an entropy decision: an explicit
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
            return .{
                .allocator = allocator,
                .rng = std.Random.DefaultCsprng.init(seed),
                .vats = std.StringHashMap([]AddrHint).init(allocator),
                .pool = std.StringHashMap(*PeerType).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var vit = self.vats.iterator();
            while (vit.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
            }
            self.vats.deinit();
            var pit = self.pool.keyIterator();
            while (pit.next()) |key| self.allocator.free(key.*);
            self.pool.deinit();
        }

        /// Return the vtable view over this network.
        pub fn network(self: *Self) Net {
            return Net.init(self, mintIntroduction, connectToIntroduced);
        }

        /// Declare a vat this network can mint introductions to. `vat_key` is
        /// the opaque name `mint_introduction` receives as its recipient hint
        /// and that tickets carry; `hints` are the dial targets a redeemer
        /// may use once dial-on-miss exists.
        pub fn addVat(self: *Self, vat_key: []const u8, hints: []const AddrHint) !void {
            if (vat_key.len == 0 or vat_key.len > max_vat_key_len) return error.InvalidVatKey;
            if (hints.len > max_addr_hints) return error.TooManyAddrHints;
            for (hints) |*hint| {
                if (hint.len != 4 and hint.len != 16) return error.InvalidAddrHint;
            }
            const key = try self.allocator.dupe(u8, vat_key);
            errdefer self.allocator.free(key);
            const owned_hints = try self.allocator.dupe(AddrHint, hints);
            errdefer self.allocator.free(owned_hints);
            const gop = try self.vats.getOrPut(key);
            if (gop.found_existing) return error.DuplicateVat;
            gop.value_ptr.* = owned_hints;
        }

        /// Register the live, same-thread peer redemptions for `vat_key`
        /// resolve to. The peer is borrowed; unregister before tearing it
        /// down, and only once in-flight handoff state has drained.
        pub fn registerPeer(self: *Self, vat_key: []const u8, peer: *PeerType) !void {
            const key = try self.allocator.dupe(u8, vat_key);
            errdefer self.allocator.free(key);
            const gop = try self.pool.getOrPut(key);
            if (gop.found_existing) return error.DuplicatePeer;
            gop.value_ptr.* = peer;
        }

        /// Stop resolving redemptions for `vat_key`. Returns whether a peer
        /// was registered. Does not touch in-flight handoffs.
        pub fn unregisterPeer(self: *Self, vat_key: []const u8) bool {
            const entry = self.pool.fetchRemove(vat_key) orelse return false;
            self.allocator.free(entry.key);
            return true;
        }

        fn mintIntroduction(
            ctx: *anyopaque,
            host_peer: *PeerType,
            recipient_hint: []const u8,
        ) anyerror!network_mod.Introduction {
            _ = host_peer;
            const self: *Self = @ptrCast(@alignCast(ctx));

            const hints = self.vats.get(recipient_hint) orelse return error.UnknownVat;

            var nonce_buf: [nonce_len]u8 = undefined;
            self.rng.random().bytes(&nonce_buf);
            var dcid_buf: [minted_dcid_len]u8 = undefined;
            self.rng.random().bytes(&dcid_buf);

            const nonce = try self.allocator.dupe(u8, &nonce_buf);
            errdefer self.allocator.free(nonce);
            const to_await = try encodeAwaitToken(self.allocator, &nonce_buf, &dcid_buf);
            errdefer self.allocator.free(to_await);
            const to_contact = try encodeTicket(self.allocator, &dcid_buf, &nonce_buf, recipient_hint, hints);
            errdefer self.allocator.free(to_contact);

            return .{
                .to_await = to_await,
                .to_contact = to_contact,
                .nonce = nonce,
                .owner_allocator = self.allocator,
            };
        }

        fn connectToIntroduced(
            ctx: *anyopaque,
            contact: message.AnyPointerReader,
        ) anyerror!IntroducedType {
            const self: *Self = @ptrCast(@alignCast(ctx));
            // All view slices borrow the inbound frame; they are consumed
            // before this function returns.
            const view = try decodeTicket(contact);
            const peer = self.pool.get(view.vat_key) orelse return error.NoPathToVat;
            const completion = try encodeAwaitToken(self.allocator, view.nonce, view.dcid);
            return .{
                .peer = peer,
                .completion = completion,
                .owner_allocator = self.allocator,
            };
        }
    };
}
