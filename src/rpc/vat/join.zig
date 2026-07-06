//! Experimental Level-4 Join network seam and compact Zig test tokens.
//!
//! This module deliberately models only the narrow Zig runtime pilot: a joined
//! object's host returns an opaque `JoinResult` payload, the joiner resolves
//! that payload to a direct peer plus an `Accept.provision` token, and the host
//! matches the provision to a pending joined target.

const std = @import("std");
const message = @import("../../serialization/message.zig");
const l3_network = @import("./network.zig");

/// Host-side output for one completed Join. Both slices are owned by the caller.
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
        provision: []u8,

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
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
            host_peer: *PeerType,
            join_id: u32,
        ) anyerror!HostJoinResultType;

        pub const ConnectJoinedFn = *const fn (
            ctx: *anyopaque,
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

        pub fn hostJoinResult(self: Self, host_peer: *PeerType, join_id: u32) !HostJoinResultType {
            return self.host_join_result(self.ctx, host_peer, join_id);
        }

        pub fn connectJoined(self: Self, result: message.AnyPointerReader) !JoinedType {
            return self.connect_joined(self.ctx, result);
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

        fn hostJoinResult(ctx: *anyopaque, host_peer: *PeerType, join_id: u32) anyerror!HostJoinResult(PeerType) {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const direct = self.direct_peers.get(host_peer) orelse return error.UnknownDirectJoinPeer;
            var nonce_buf: [16]u8 = undefined;
            std.mem.writeInt(u64, nonce_buf[0..8], join_id, .big);
            std.mem.writeInt(u64, nonce_buf[8..16], self.next_nonce, .big);
            self.next_nonce +%= 1;

            const provision = try l3_network.encodeNonceToken(self.allocator, nonce_buf[0..]);
            errdefer self.allocator.free(provision);
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

            const result = try encodeJoinResult(self.allocator, join_id, true, provision);
            errdefer self.allocator.free(result);

            return .{
                .accept_peer = direct.accept_peer,
                .provision = provision,
                .result = result,
            };
        }

        fn connectJoined(ctx: *anyopaque, result: message.AnyPointerReader) anyerror!JoinedType {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const decoded = try decodeJoinResult(result);
            if (!decoded.succeeded) return error.JoinResultFailed;
            const direct_peer = self.registry.get(decoded.provision) orelse return error.UnknownJoinProvision;
            const provision = try self.allocator.dupe(u8, decoded.provision);
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
/// host peer with an opaque application address. The current Zig runtime still
/// resolves `JoinResult` to already-live direct peers supplied by the app; the
/// address is carried in the generated provision token so e2e tests can exercise
/// a non-loopback addressing policy without claiming a production dialer.
pub fn AddressedJoinNetwork(comptime PeerType: type) type {
    return struct {
        const Self = @This();
        pub const Net = JoinNetwork(PeerType);
        pub const JoinedType = Joined(PeerType);

        allocator: std.mem.Allocator,
        next_nonce: u64 = 1,
        direct_peers: std.AutoHashMap(*PeerType, DirectPeer),
        registry: std.StringHashMap(*PeerType),

        const provision_prefix = "capnp-zig:l4:join:";

        const DirectPeer = struct {
            joiner_peer: *PeerType,
            accept_peer: *PeerType,
            address: []u8,
        };

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .direct_peers = std.AutoHashMap(*PeerType, DirectPeer).init(allocator),
                .registry = std.StringHashMap(*PeerType).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var registry_it = self.registry.keyIterator();
            while (registry_it.next()) |key| self.allocator.free(key.*);
            self.registry.deinit();

            var direct_it = self.direct_peers.valueIterator();
            while (direct_it.next()) |entry| self.allocator.free(entry.address);
            self.direct_peers.deinit();
        }

        pub fn network(self: *Self) Net {
            return Net.init(self, hostJoinResult, connectJoined, cancelHostJoinResult);
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

        fn hostJoinResult(ctx: *anyopaque, host_peer: *PeerType, join_id: u32) anyerror!HostJoinResult(PeerType) {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const direct = self.direct_peers.get(host_peer) orelse return error.UnknownDirectJoinPeer;
            const token = try self.buildProvisionToken(direct.address, join_id);
            defer self.allocator.free(token);

            const provision = try l3_network.encodeNonceToken(self.allocator, token);
            errdefer self.allocator.free(provision);
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

            const result = try encodeJoinResult(self.allocator, join_id, true, provision);
            errdefer self.allocator.free(result);

            return .{
                .accept_peer = direct.accept_peer,
                .provision = provision,
                .result = result,
            };
        }

        fn connectJoined(ctx: *anyopaque, result: message.AnyPointerReader) anyerror!JoinedType {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const decoded = try decodeJoinResult(result);
            if (!decoded.succeeded) return error.JoinResultFailed;
            const direct_peer = self.registry.get(decoded.provision) orelse return error.UnknownJoinProvision;
            const provision = try self.allocator.dupe(u8, decoded.provision);
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
    var hosted = try network.hostJoinResult(&host, 0x55aa);
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
    try std.testing.expectError(error.UnknownDirectJoinPeer, network.hostJoinResult(&host, 1));

    try addressed.registerDirectPeer(&host, &joiner, "tcp://127.0.0.1:4707");
    var hosted = try network.hostJoinResult(&host, 2);
    defer hosted.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), addressed.registry.count());

    network.cancelHostJoinResult(hosted.provision);
    try std.testing.expectEqual(@as(usize, 0), addressed.registry.count());

    var result_msg = try message.Message.init(std.testing.allocator, hosted.result, .{});
    defer result_msg.deinit();
    try std.testing.expectError(error.UnknownJoinProvision, network.connectJoined(try result_msg.getRootAnyPointer()));

    addressed.removeDirectPeer(&host);
    try std.testing.expectError(error.UnknownDirectJoinPeer, network.hostJoinResult(&host, 3));
}

test "AddressedJoinNetwork detects duplicate provisions without losing the original" {
    const DummyPeer = struct {};

    var host = DummyPeer{};
    var joiner = DummyPeer{};
    var addressed = AddressedJoinNetwork(DummyPeer).init(std.testing.allocator);
    defer addressed.deinit();
    try addressed.registerDirectPeer(&host, &joiner, "tcp://127.0.0.1:4708");

    const network = addressed.network();
    var hosted = try network.hostJoinResult(&host, 0x77);
    defer hosted.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), addressed.registry.count());

    addressed.next_nonce = 1;
    try std.testing.expectError(error.DuplicateJoinProvision, network.hostJoinResult(&host, 0x77));
    try std.testing.expectEqual(@as(usize, 1), addressed.registry.count());

    var result_msg = try message.Message.init(std.testing.allocator, hosted.result, .{});
    defer result_msg.deinit();
    var joined = try network.connectJoined(try result_msg.getRootAnyPointer());
    defer joined.deinit(std.testing.allocator);
    try std.testing.expectEqual(&joiner, joined.peer);
}

fn addressedJoinResultOomImpl(allocator: std.mem.Allocator) !void {
    const DummyPeer = struct {};

    var host = DummyPeer{};
    var joiner = DummyPeer{};
    var addressed = AddressedJoinNetwork(DummyPeer).init(allocator);
    defer addressed.deinit();
    try addressed.registerDirectPeer(&host, &joiner, "tcp://127.0.0.1:4709");

    const network = addressed.network();
    var hosted = try network.hostJoinResult(&host, 0x88);
    defer hosted.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), addressed.registry.count());

    network.cancelHostJoinResult(hosted.provision);
    try std.testing.expectEqual(@as(usize, 0), addressed.registry.count());
}

test "AddressedJoinNetwork hostJoinResult rolls back registry under OOM" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, addressedJoinResultOomImpl, .{});
}
