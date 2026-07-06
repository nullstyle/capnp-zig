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
pub const HostJoinResult = struct {
    /// Serialized AnyPointer message used as the future `Accept.provision`.
    provision: []u8,
    /// Serialized AnyPointer message whose root is the Zig `JoinResult` struct.
    result: []u8,

    pub fn deinit(self: *HostJoinResult, allocator: std.mem.Allocator) void {
        allocator.free(self.provision);
        allocator.free(self.result);
    }
};

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
        pub const JoinedType = Joined(PeerType);

        pub const HostJoinResultFn = *const fn (
            ctx: *anyopaque,
            host_peer: *PeerType,
            join_id: u32,
        ) anyerror!HostJoinResult;

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

        pub fn hostJoinResult(self: Self, host_peer: *PeerType, join_id: u32) !HostJoinResult {
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
        direct_peers: std.AutoHashMap(*PeerType, *PeerType),
        /// provision bytes (owned) -> joiner-side direct peer (borrowed).
        registry: std.StringHashMap(*PeerType),

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .direct_peers = std.AutoHashMap(*PeerType, *PeerType).init(allocator),
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
            try self.direct_peers.put(host_peer, joiner_peer);
        }

        fn hostJoinResult(ctx: *anyopaque, host_peer: *PeerType, join_id: u32) anyerror!HostJoinResult {
            const self: *Self = @ptrCast(@alignCast(ctx));
            const joiner_peer = self.direct_peers.get(host_peer) orelse return error.UnknownDirectJoinPeer;
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
            gop.value_ptr.* = joiner_peer;

            const result = try encodeJoinResult(self.allocator, join_id, true, provision);
            errdefer self.allocator.free(result);

            return .{
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
