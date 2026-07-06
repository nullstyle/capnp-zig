//! Experimental Level-4 Join network seam and compact Zig test tokens.
//!
//! This module deliberately models only the narrow Zig runtime pilot: a joined
//! object's host returns an opaque `JoinResult` payload, the joiner resolves
//! that payload to a direct peer plus an `Accept.provision` token, and the host
//! matches the provision to a pending joined target.

const std = @import("std");
const message = @import("../../serialization/message.zig");
const l3_network = @import("./network.zig");

/// Host-side output for one completed Join. Both slices are owned by the caller
/// and must be freed with the allocator passed to `JoinNetwork.hostJoinResult`.
pub fn HostJoinResult(comptime PeerType: type) type {
    return struct {
        /// Host-side peer that should receive the future `Accept`.
        accept_peer: *PeerType,
        /// Serialized AnyPointer message used as the future `Accept.provision`.
        provision: []u8,
        /// Serialized AnyPointer message whose root is the Zig `JoinResult` struct.
        result: []u8,

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            allocator.free(self.provision);
            allocator.free(self.result);
        }
    };
}

/// Recipient-side resolution of a Zig `JoinResult`.
pub fn Joined(comptime PeerType: type) type {
    return struct {
        /// Live direct peer connected to the joined object's host. Borrowed.
        peer: *PeerType,
        /// Serialized AnyPointer message to present as `Accept.provision`.
        /// Owned by the caller and freed with the allocator passed to
        /// `JoinNetwork.connectJoined`.
        provision: []u8,
        /// Optional network-owned cleanup for JoinResult resolution caches.
        release_ctx: ?*anyopaque = null,
        release_provision: ?*const fn (*anyopaque, []const u8) void = null,

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (self.release_provision) |release_fn| {
                if (self.release_ctx) |ctx| release_fn(ctx, self.provision);
                self.release_provision = null;
                self.release_ctx = null;
            }
            allocator.free(self.provision);
        }
    };
}

/// Experimental L4 Join addressing vtable.
pub fn JoinNetwork(comptime PeerType: type) type {
    return struct {
        const Self = @This();
        pub const HostJoinResultType = HostJoinResult(PeerType);
        pub const JoinedType = Joined(PeerType);

        pub const HostJoinResultFn = *const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            host_peer: *PeerType,
            join_id: u32,
        ) anyerror!HostJoinResultType;

        pub const ConnectJoinedFn = *const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            result: message.AnyPointerReader,
        ) anyerror!JoinedType;

        pub const CancelHostJoinResultFn = *const fn (
            ctx: *anyopaque,
            provision: []const u8,
        ) void;

        ctx: *anyopaque,
        host_join_result: HostJoinResultFn,
        connect_joined: ConnectJoinedFn,
        cancel_host_join_result: CancelHostJoinResultFn,

        pub fn init(
            ctx: *anyopaque,
            host_join_result: HostJoinResultFn,
            connect_joined: ConnectJoinedFn,
            cancel_host_join_result: CancelHostJoinResultFn,
        ) Self {
            return .{
                .ctx = ctx,
                .host_join_result = host_join_result,
                .connect_joined = connect_joined,
                .cancel_host_join_result = cancel_host_join_result,
            };
        }

        pub fn hostJoinResult(self: Self, allocator: std.mem.Allocator, host_peer: *PeerType, join_id: u32) !HostJoinResultType {
            return self.host_join_result(self.ctx, allocator, host_peer, join_id);
        }

        pub fn connectJoined(self: Self, allocator: std.mem.Allocator, result: message.AnyPointerReader) !JoinedType {
            return self.connect_joined(self.ctx, allocator, result);
        }

        pub fn cancelHostJoinResult(self: Self, provision: []const u8) void {
            self.cancel_host_join_result(self.ctx, provision);
        }
    };
}

/// Decoded compact Zig `JoinResult`.
pub const DecodedJoinResult = struct {
    join_id: u32,
    succeeded: bool,
    /// Borrowed from the decoded result frame.
    provision: []const u8,
};

/// Build the existing compact Zig `Join.keyPart` convention.
pub fn encodeJoinKeyPart(
    allocator: std.mem.Allocator,
    join_id: u32,
    part_count: u16,
    part_num: u16,
) ![]u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.initRootAnyPointer();
    const key = try root.initStruct(1, 0);
    key.writeU32(0, join_id);
    key.writeU16(4, part_count);
    key.writeU16(6, part_num);
    const bytes = try builder.toBytes();
    return @constCast(bytes);
}

/// Build the compact Zig `JoinResult` convention:
/// data word: `join_id :UInt32`, `succeeded :Bool` at byte 4 bit 0;
/// pointer 0: `provision :Data`.
pub fn encodeJoinResult(
    allocator: std.mem.Allocator,
    join_id: u32,
    succeeded: bool,
    provision: []const u8,
) ![]u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const root = try builder.initRootAnyPointer();
    const result = try root.initStruct(1, 1);
    result.writeU32(0, join_id);
    result.writeBool(4, 0, succeeded);
    try result.writeData(0, provision);
    const bytes = try builder.toBytes();
    return @constCast(bytes);
}

pub fn decodeJoinResult(result: message.AnyPointerReader) !DecodedJoinResult {
    const result_struct = result.getStruct() catch return error.InvalidJoinResult;
    const provision = result_struct.readData(0) catch return error.InvalidJoinResult;
    return .{
        .join_id = result_struct.readU32(0),
        .succeeded = result_struct.readBool(4, 0),
        .provision = provision,
    };
}

/// In-process concrete Join network used by the Zig runtime pilot tests.
pub fn LoopbackJoinNetwork(comptime PeerType: type) type {
    return struct {
        const Self = @This();
        pub const Net = JoinNetwork(PeerType);
        pub const JoinedType = Joined(PeerType);

        allocator: std.mem.Allocator,
        next_nonce: u64 = 1,
        /// host-side peer -> joiner-side direct peer (both borrowed).
        direct_peers: std.AutoHashMap(*PeerType, DirectPeer),
        /// provision bytes (owned) -> joiner-side direct peer (borrowed).
        registry: std.StringHashMap(*PeerType),

        const DirectPeer = struct {
            joiner_peer: *PeerType,
            accept_peer: *PeerType,
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .direct_peers = std.AutoHashMap(*PeerType, DirectPeer).init(allocator),
                .registry = std.StringHashMap(*PeerType).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.registry.keyIterator();
            while (it.next()) |key| self.allocator.free(key.*);
            self.registry.deinit();
            self.direct_peers.deinit();
        }

        pub fn network(self: *Self) Net {
            return Net.init(self, hostJoinResult, connectJoined, cancelHostJoinResult);
        }

        /// Register the joiner-side direct peer that can reach `host_peer`.
        pub fn registerDirectPeer(self: *Self, host_peer: *PeerType, joiner_peer: *PeerType) !void {
            try self.registerDirectPeerWithAcceptHost(host_peer, joiner_peer, host_peer);
        }

        /// Register a direct peer plus the host-side peer that will receive the
        /// final `Accept`. Tests use this when Join reaches the host through a
        /// relay connection but Accept should arrive on a separate direct link.
        pub fn registerDirectPeerWithAcceptHost(
            self: *Self,
            host_peer: *PeerType,
            joiner_peer: *PeerType,
            accept_peer: *PeerType,
        ) !void {
            try self.direct_peers.put(host_peer, .{
                .joiner_peer = joiner_peer,
                .accept_peer = accept_peer,
            });
        }

        fn hostJoinResult(
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            host_peer: *PeerType,
            join_id: u32,
        ) anyerror!HostJoinResult(PeerType) {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const direct = self.direct_peers.get(host_peer) orelse return error.UnknownDirectJoinPeer;
            var nonce_buf: [16]u8 = undefined;
            std.mem.writeInt(u64, nonce_buf[0..8], join_id, .big);
            std.mem.writeInt(u64, nonce_buf[8..16], self.next_nonce, .big);
            self.next_nonce +%= 1;

            const provision = try l3_network.encodeNonceToken(allocator, nonce_buf[0..]);
            errdefer allocator.free(provision);
            const registry_key = try self.allocator.dupe(u8, provision);
            var registry_inserted = false;
            errdefer {
                if (registry_inserted) {
                    if (self.registry.fetchRemove(registry_key)) |removed| {
                        self.allocator.free(removed.key);
                    }
                } else {
                    self.allocator.free(registry_key);
                }
            }

            const gop = try self.registry.getOrPut(registry_key);
            if (gop.found_existing) return error.DuplicateJoinProvision;
            registry_inserted = true;
            gop.value_ptr.* = direct.joiner_peer;

            const result = try encodeJoinResult(allocator, join_id, true, provision);
            errdefer allocator.free(result);

            return .{
                .accept_peer = direct.accept_peer,
                .provision = provision,
                .result = result,
            };
        }

        fn connectJoined(ctx: *anyopaque, allocator: std.mem.Allocator, result: message.AnyPointerReader) anyerror!JoinedType {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const decoded = try decodeJoinResult(result);
            if (!decoded.succeeded) return error.JoinResultFailed;
            const direct_peer = self.registry.get(decoded.provision) orelse return error.UnknownJoinProvision;
            const provision = try allocator.dupe(u8, decoded.provision);
            return .{
                .peer = direct_peer,
                .provision = provision,
            };
        }

        fn cancelHostJoinResult(ctx: *anyopaque, provision: []const u8) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            if (self.registry.fetchRemove(provision)) |removed| {
                self.allocator.free(removed.key);
            }
        }
    };
}

/// Experimental app-supplied Join addressing registry.
///
/// Unlike `LoopbackJoinNetwork`, this helper requires callers to associate each
/// host peer with an opaque application address. Joiners resolve `JoinResult`
/// payloads through already-live registry entries first, or through an optional
/// application connector that turns the address into a direct peer. The helper
/// still does not define a production address format or own a transport dialer.
pub fn AddressedJoinNetwork(comptime PeerType: type) type {
    return struct {
        const Self = @This();
        pub const Net = JoinNetwork(PeerType);
        pub const JoinedType = Joined(PeerType);
        pub const ConnectAddressFn = *const fn (
            ctx: *anyopaque,
            address: []const u8,
        ) anyerror!*PeerType;

        allocator: std.mem.Allocator,
        next_nonce: u64 = 1,
        direct_peers: std.AutoHashMap(*PeerType, DirectPeer),
        registry: std.StringHashMap(RegistryEntry),
        connect_ctx: ?*anyopaque = null,
        connect_address: ?ConnectAddressFn = null,

        const provision_prefix = "capnp-zig:l4:join:";
        const nonce_len = 16;

        const DirectPeer = struct {
            joiner_peer: *PeerType,
            accept_peer: *PeerType,
            address: []u8,
        };

        const RegistryEntry = struct {
            peer: *PeerType,
            source: enum { hosted_result, connected_result },
            connected_state: ?*ConnectedProvisionState = null,
        };

        const ConnectedProvisionState = struct {
            allocator: std.mem.Allocator,
            network: ?*Self,
            leases: usize,
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .direct_peers = std.AutoHashMap(*PeerType, DirectPeer).init(allocator),
                .registry = std.StringHashMap(RegistryEntry).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var registry_it = self.registry.iterator();
            while (registry_it.next()) |entry| {
                if (entry.value_ptr.connected_state) |state| {
                    state.network = null;
                    if (state.leases == 0) state.allocator.destroy(state);
                }
                self.allocator.free(entry.key_ptr.*);
            }
            self.registry.deinit();

            var direct_it = self.direct_peers.valueIterator();
            while (direct_it.next()) |entry| self.allocator.free(entry.address);
            self.direct_peers.deinit();
        }

        pub fn network(self: *Self) Net {
            return Net.init(self, hostJoinResult, connectJoined, cancelHostJoinResult);
        }

        /// Install an application connector used by `connectJoined()` when the
        /// provision token names an address not already present in the local
        /// registry. The connector owns the returned peer's lifetime and must
        /// keep it live at least until the returned `Joined` is deinitialized.
        pub fn setAddressConnector(
            self: *Self,
            ctx: *anyopaque,
            connect_address: ConnectAddressFn,
        ) void {
            self.connect_ctx = ctx;
            self.connect_address = connect_address;
        }

        pub fn clearAddressConnector(self: *Self) void {
            self.connect_ctx = null;
            self.connect_address = null;
        }

        pub fn registerDirectPeer(
            self: *Self,
            host_peer: *PeerType,
            joiner_peer: *PeerType,
            address: []const u8,
        ) !void {
            try self.registerDirectPeerWithAcceptHost(host_peer, joiner_peer, host_peer, address);
        }

        pub fn registerDirectPeerWithAcceptHost(
            self: *Self,
            host_peer: *PeerType,
            joiner_peer: *PeerType,
            accept_peer: *PeerType,
            address: []const u8,
        ) !void {
            const owned_address = try self.allocator.dupe(u8, address);
            errdefer self.allocator.free(owned_address);

            const gop = try self.direct_peers.getOrPut(host_peer);
            if (gop.found_existing) self.allocator.free(gop.value_ptr.address);
            gop.value_ptr.* = .{
                .joiner_peer = joiner_peer,
                .accept_peer = accept_peer,
                .address = owned_address,
            };
        }

        pub fn removeDirectPeer(self: *Self, host_peer: *PeerType) void {
            if (self.direct_peers.fetchRemove(host_peer)) |removed| {
                self.allocator.free(removed.value.address);
            }
        }

        fn hostJoinResult(
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            host_peer: *PeerType,
            join_id: u32,
        ) anyerror!HostJoinResult(PeerType) {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const direct = self.direct_peers.get(host_peer) orelse return error.UnknownDirectJoinPeer;
            const token = try self.buildProvisionToken(direct.address, join_id);
            defer self.allocator.free(token);

            const provision = try l3_network.encodeNonceToken(allocator, token);
            errdefer allocator.free(provision);
            const registry_key = try self.allocator.dupe(u8, provision);
            var registry_inserted = false;
            errdefer {
                if (registry_inserted) {
                    if (self.registry.fetchRemove(registry_key)) |removed| {
                        self.allocator.free(removed.key);
                    }
                } else {
                    self.allocator.free(registry_key);
                }
            }

            const gop = try self.registry.getOrPut(registry_key);
            if (gop.found_existing) return error.DuplicateJoinProvision;
            registry_inserted = true;
            gop.value_ptr.* = .{ .peer = direct.joiner_peer, .source = .hosted_result };

            const result = try encodeJoinResult(allocator, join_id, true, provision);
            errdefer allocator.free(result);

            return .{
                .accept_peer = direct.accept_peer,
                .provision = provision,
                .result = result,
            };
        }

        fn connectJoined(ctx: *anyopaque, allocator: std.mem.Allocator, result: message.AnyPointerReader) anyerror!JoinedType {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const decoded = try decodeJoinResult(result);
            if (!decoded.succeeded) return error.JoinResultFailed;
            if (self.registry.getPtr(decoded.provision)) |entry| {
                const provision = try allocator.dupe(u8, decoded.provision);
                var release_state: ?*ConnectedProvisionState = null;
                if (entry.source == .connected_result) {
                    release_state = entry.connected_state orelse return error.InvalidJoinProvision;
                }
                if (release_state) |state| state.leases += 1;
                return .{
                    .peer = entry.peer,
                    .provision = provision,
                    .release_ctx = if (release_state) |state| @ptrCast(state) else null,
                    .release_provision = if (release_state != null) releaseJoinedProvision else null,
                };
            }
            return try self.connectAddressedProvision(allocator, decoded.provision);
        }

        fn connectAddressedProvision(self: *Self, allocator: std.mem.Allocator, provision_bytes: []const u8) anyerror!JoinedType {
            const connect = self.connect_address orelse return error.UnknownJoinProvision;
            const connect_ctx = self.connect_ctx orelse return error.UnknownJoinProvision;

            const registry_key = try self.allocator.dupe(u8, provision_bytes);
            var registry_key_owned = true;
            errdefer if (registry_key_owned) self.allocator.free(registry_key);
            const joined_provision = try allocator.dupe(u8, provision_bytes);
            errdefer allocator.free(joined_provision);
            const connected_state = try self.allocator.create(ConnectedProvisionState);
            var connected_state_owned = true;
            errdefer if (connected_state_owned) self.allocator.destroy(connected_state);
            connected_state.* = .{
                .allocator = self.allocator,
                .network = self,
                .leases = 1,
            };

            try self.registry.ensureUnusedCapacity(1);

            var provision_msg = try message.Message.init(self.allocator, provision_bytes, .{});
            defer provision_msg.deinit();
            const token = (try provision_msg.getRootAnyPointer()).getData() catch return error.InvalidJoinProvision;
            const address = parseAddressedProvisionToken(token) catch return error.InvalidJoinProvision;

            const peer = try connect(connect_ctx, address);
            const gop = self.registry.getOrPutAssumeCapacity(registry_key);
            var release_state: ?*ConnectedProvisionState = connected_state;
            if (gop.found_existing) {
                self.allocator.free(registry_key);
                registry_key_owned = false;
                self.allocator.destroy(connected_state);
                connected_state_owned = false;
                release_state = null;
                if (gop.value_ptr.source == .connected_result) {
                    release_state = gop.value_ptr.connected_state orelse return error.InvalidJoinProvision;
                }
                if (release_state) |state| state.leases += 1;
            } else {
                registry_key_owned = false;
                connected_state_owned = false;
                gop.value_ptr.* = .{
                    .peer = peer,
                    .source = .connected_result,
                    .connected_state = connected_state,
                };
            }

            return .{
                .peer = gop.value_ptr.*.peer,
                .provision = joined_provision,
                .release_ctx = if (release_state) |state| @ptrCast(state) else null,
                .release_provision = if (release_state != null) releaseJoinedProvision else null,
            };
        }

        fn cancelHostJoinResult(ctx: *anyopaque, provision: []const u8) void {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const entry = self.registry.get(provision) orelse return;
            if (entry.source != .hosted_result) return;
            if (self.registry.fetchRemove(provision)) |removed| {
                self.allocator.free(removed.key);
            }
        }

        fn releaseJoinedProvision(ctx: *anyopaque, provision: []const u8) void {
            const state: *ConnectedProvisionState = @ptrCast(@alignCast(ctx));
            std.debug.assert(state.leases > 0);
            state.leases -= 1;
            if (state.leases != 0) return;

            if (state.network) |owner_network| {
                if (owner_network.registry.get(provision)) |entry| {
                    if (entry.connected_state == state) {
                        if (owner_network.registry.fetchRemove(provision)) |removed| {
                            owner_network.allocator.free(removed.key);
                        }
                    }
                }
            }

            state.network = null;
            state.allocator.destroy(state);
        }

        fn buildProvisionToken(self: *Self, address: []const u8, join_id: u32) ![]u8 {
            var token = std.ArrayList(u8).empty;
            errdefer token.deinit(self.allocator);
            try token.appendSlice(self.allocator, provision_prefix);
            try token.appendSlice(self.allocator, address);
            try token.appendSlice(self.allocator, &.{0});

            var nonce_buf: [16]u8 = undefined;
            std.mem.writeInt(u64, nonce_buf[0..8], join_id, .big);
            std.mem.writeInt(u64, nonce_buf[8..16], self.next_nonce, .big);
            self.next_nonce +%= 1;
            try token.appendSlice(self.allocator, &nonce_buf);
            return token.toOwnedSlice(self.allocator);
        }

        fn parseAddressedProvisionToken(token: []const u8) ![]const u8 {
            if (!std.mem.startsWith(u8, token, provision_prefix)) return error.InvalidJoinProvision;
            const rest = token[provision_prefix.len..];
            const nul = std.mem.indexOfScalar(u8, rest, 0) orelse return error.InvalidJoinProvision;
            if (nul == 0) return error.InvalidJoinProvision;
            if (rest.len != nul + 1 + nonce_len) return error.InvalidJoinProvision;
            return rest[0..nul];
        }
    };
}

test "vat.join encodes and decodes compact JoinResult" {
    const provision = try l3_network.encodeNonceToken(std.testing.allocator, "join-token");
    defer std.testing.allocator.free(provision);
    const bytes = try encodeJoinResult(std.testing.allocator, 0x1234_5678, true, provision);
    defer std.testing.allocator.free(bytes);

    var msg = try message.Message.init(std.testing.allocator, bytes, .{});
    defer msg.deinit();
    const decoded = try decodeJoinResult(try msg.getRootAnyPointer());
    try std.testing.expectEqual(@as(u32, 0x1234_5678), decoded.join_id);
    try std.testing.expect(decoded.succeeded);
    try std.testing.expectEqualSlices(u8, provision, decoded.provision);
}

fn loopbackJoinResultOomImpl(allocator: std.mem.Allocator) !void {
    const DummyPeer = struct {};

    var host = DummyPeer{};
    var joiner = DummyPeer{};
    var loopback = LoopbackJoinNetwork(DummyPeer).init(allocator);
    defer loopback.deinit();
    try loopback.registerDirectPeer(&host, &joiner);

    const network = loopback.network();
    var hosted = try network.hostJoinResult(allocator, &host, 0x55aa);
    defer hosted.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), loopback.registry.count());

    network.cancelHostJoinResult(hosted.provision);
    try std.testing.expectEqual(@as(usize, 0), loopback.registry.count());
}

test "LoopbackJoinNetwork hostJoinResult rolls back registry under OOM" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, loopbackJoinResultOomImpl, .{});
}

test "AddressedJoinNetwork rejects unknown and stale JoinResult provisions" {
    const DummyPeer = struct {};

    var host = DummyPeer{};
    var joiner = DummyPeer{};
    var addressed = AddressedJoinNetwork(DummyPeer).init(std.testing.allocator);
    defer addressed.deinit();

    const network = addressed.network();
    try std.testing.expectError(error.UnknownDirectJoinPeer, network.hostJoinResult(std.testing.allocator, &host, 1));

    try addressed.registerDirectPeer(&host, &joiner, "tcp://127.0.0.1:4707");
    var hosted = try network.hostJoinResult(std.testing.allocator, &host, 2);
    defer hosted.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), addressed.registry.count());

    network.cancelHostJoinResult(hosted.provision);
    try std.testing.expectEqual(@as(usize, 0), addressed.registry.count());

    var result_msg = try message.Message.init(std.testing.allocator, hosted.result, .{});
    defer result_msg.deinit();
    try std.testing.expectError(error.UnknownJoinProvision, network.connectJoined(std.testing.allocator, try result_msg.getRootAnyPointer()));

    addressed.removeDirectPeer(&host);
    try std.testing.expectError(error.UnknownDirectJoinPeer, network.hostJoinResult(std.testing.allocator, &host, 3));
}

test "AddressedJoinNetwork detects duplicate provisions without losing the original" {
    const DummyPeer = struct {};

    var host = DummyPeer{};
    var joiner = DummyPeer{};
    var addressed = AddressedJoinNetwork(DummyPeer).init(std.testing.allocator);
    defer addressed.deinit();
    try addressed.registerDirectPeer(&host, &joiner, "tcp://127.0.0.1:4708");

    const network = addressed.network();
    var hosted = try network.hostJoinResult(std.testing.allocator, &host, 0x77);
    defer hosted.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), addressed.registry.count());

    addressed.next_nonce = 1;
    try std.testing.expectError(error.DuplicateJoinProvision, network.hostJoinResult(std.testing.allocator, &host, 0x77));
    try std.testing.expectEqual(@as(usize, 1), addressed.registry.count());

    var result_msg = try message.Message.init(std.testing.allocator, hosted.result, .{});
    defer result_msg.deinit();
    var joined = try network.connectJoined(std.testing.allocator, try result_msg.getRootAnyPointer());
    defer joined.deinit(std.testing.allocator);
    try std.testing.expectEqual(&joiner, joined.peer);
}

test "AddressedJoinNetwork connector resolves and drains JoinResult cache" {
    const DummyPeer = struct {};
    const DialCtx = struct {
        peer: *DummyPeer,
        calls: usize = 0,

        fn connect(ctx_ptr: *anyopaque, address: []const u8) anyerror!*DummyPeer {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            try std.testing.expectEqualStrings("tcp://127.0.0.1:4710", address);
            self.calls += 1;
            return self.peer;
        }
    };

    var host = DummyPeer{};
    var host_to_joiner = DummyPeer{};
    var dialed_peer = DummyPeer{};

    var host_net = AddressedJoinNetwork(DummyPeer).init(std.testing.allocator);
    defer host_net.deinit();
    try host_net.registerDirectPeer(&host, &host_to_joiner, "tcp://127.0.0.1:4710");
    var hosted = try host_net.network().hostJoinResult(std.testing.allocator, &host, 0x99);
    defer hosted.deinit(std.testing.allocator);

    var joiner_net = AddressedJoinNetwork(DummyPeer).init(std.testing.allocator);
    defer joiner_net.deinit();
    var dial = DialCtx{ .peer = &dialed_peer };
    joiner_net.setAddressConnector(&dial, DialCtx.connect);

    var result_msg = try message.Message.init(std.testing.allocator, hosted.result, .{});
    defer result_msg.deinit();
    const result = try result_msg.getRootAnyPointer();

    var joined0 = try joiner_net.network().connectJoined(std.testing.allocator, result);
    var joined1 = try joiner_net.network().connectJoined(std.testing.allocator, result);

    try std.testing.expectEqual(@as(usize, 1), dial.calls);
    try std.testing.expectEqual(&dialed_peer, joined0.peer);
    try std.testing.expectEqual(&dialed_peer, joined1.peer);
    try std.testing.expectEqual(@as(usize, 1), joiner_net.registry.count());

    joined0.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), joiner_net.registry.count());
    joined1.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), joiner_net.registry.count());
}

test "AddressedJoinNetwork connector lease survives network teardown" {
    const DummyPeer = struct {};
    const DialCtx = struct {
        peer: *DummyPeer,

        fn connect(ctx_ptr: *anyopaque, address: []const u8) anyerror!*DummyPeer {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            try std.testing.expectEqualStrings("tcp://127.0.0.1:4712", address);
            return self.peer;
        }
    };

    var host = DummyPeer{};
    var host_to_joiner = DummyPeer{};
    var dialed_peer = DummyPeer{};

    var host_net = AddressedJoinNetwork(DummyPeer).init(std.testing.allocator);
    defer host_net.deinit();
    try host_net.registerDirectPeer(&host, &host_to_joiner, "tcp://127.0.0.1:4712");
    var hosted = try host_net.network().hostJoinResult(std.testing.allocator, &host, 0xa2);
    defer hosted.deinit(std.testing.allocator);

    var result_msg = try message.Message.init(std.testing.allocator, hosted.result, .{});
    defer result_msg.deinit();
    const result = try result_msg.getRootAnyPointer();

    var joined = joined: {
        var joiner_net = AddressedJoinNetwork(DummyPeer).init(std.testing.allocator);
        defer joiner_net.deinit();
        var dial = DialCtx{ .peer = &dialed_peer };
        joiner_net.setAddressConnector(&dial, DialCtx.connect);
        const resolved = try joiner_net.network().connectJoined(std.testing.allocator, result);
        try std.testing.expectEqual(@as(usize, 1), joiner_net.registry.count());
        break :joined resolved;
    };
    defer joined.deinit(std.testing.allocator);

    try std.testing.expectEqual(&dialed_peer, joined.peer);
}

test "AddressedJoinNetwork connector rejects malformed provision tokens" {
    const DummyPeer = struct {};
    const DialCtx = struct {
        calls: usize = 0,

        fn connect(ctx_ptr: *anyopaque, _: []const u8) anyerror!*DummyPeer {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            self.calls += 1;
            return error.UnexpectedConnect;
        }
    };

    const bad_provision = try l3_network.encodeNonceToken(std.testing.allocator, "not-an-addressed-token");
    defer std.testing.allocator.free(bad_provision);
    const bad_result = try encodeJoinResult(std.testing.allocator, 0xa0, true, bad_provision);
    defer std.testing.allocator.free(bad_result);

    var result_msg = try message.Message.init(std.testing.allocator, bad_result, .{});
    defer result_msg.deinit();

    var network = AddressedJoinNetwork(DummyPeer).init(std.testing.allocator);
    defer network.deinit();
    var dial = DialCtx{};
    network.setAddressConnector(&dial, DialCtx.connect);

    try std.testing.expectError(error.InvalidJoinProvision, network.network().connectJoined(std.testing.allocator, try result_msg.getRootAnyPointer()));
    try std.testing.expectEqual(@as(usize, 0), dial.calls);
    try std.testing.expectEqual(@as(usize, 0), network.registry.count());
}

fn addressedJoinResultOomImpl(allocator: std.mem.Allocator) !void {
    const DummyPeer = struct {};

    var host = DummyPeer{};
    var joiner = DummyPeer{};
    var addressed = AddressedJoinNetwork(DummyPeer).init(allocator);
    defer addressed.deinit();
    try addressed.registerDirectPeer(&host, &joiner, "tcp://127.0.0.1:4709");

    const network = addressed.network();
    var hosted = try network.hostJoinResult(allocator, &host, 0x88);
    defer hosted.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), addressed.registry.count());

    network.cancelHostJoinResult(hosted.provision);
    try std.testing.expectEqual(@as(usize, 0), addressed.registry.count());
}

test "AddressedJoinNetwork hostJoinResult rolls back registry under OOM" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, addressedJoinResultOomImpl, .{});
}

fn addressedJoinConnectorOomImpl(allocator: std.mem.Allocator) !void {
    const DummyPeer = struct {};
    const DialCtx = struct {
        peer: *DummyPeer,
        calls: usize = 0,

        fn connect(ctx_ptr: *anyopaque, address: []const u8) anyerror!*DummyPeer {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            try std.testing.expectEqualStrings("tcp://127.0.0.1:4711", address);
            self.calls += 1;
            return self.peer;
        }
    };

    var host = DummyPeer{};
    var host_to_joiner = DummyPeer{};
    var dialed_peer = DummyPeer{};

    var host_net = AddressedJoinNetwork(DummyPeer).init(allocator);
    defer host_net.deinit();
    try host_net.registerDirectPeer(&host, &host_to_joiner, "tcp://127.0.0.1:4711");
    var hosted = try host_net.network().hostJoinResult(allocator, &host, 0xa1);
    defer hosted.deinit(allocator);

    var joiner_net = AddressedJoinNetwork(DummyPeer).init(allocator);
    defer joiner_net.deinit();
    var dial = DialCtx{ .peer = &dialed_peer };
    joiner_net.setAddressConnector(&dial, DialCtx.connect);

    var result_msg = try message.Message.init(allocator, hosted.result, .{});
    defer result_msg.deinit();

    var joined = try joiner_net.network().connectJoined(allocator, try result_msg.getRootAnyPointer());
    defer joined.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), dial.calls);
    try std.testing.expectEqual(@as(usize, 1), joiner_net.registry.count());
}

test "AddressedJoinNetwork connector path does not dial before rollback-safe allocations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, addressedJoinConnectorOomImpl, .{});
}
