const std = @import("std");
const capnpc = @import("capnpc-zig");

pub const HostPeer = capnpc.rpc.integration.host_peer.HostPeer;
pub const Peer = capnpc.rpc.peer.Peer;
pub const events = capnpc.rpc.events;

pub const Capture = struct {
    allocator: std.mem.Allocator,
    frames: std.ArrayList([]u8) = .empty,

    pub fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
        const copy = try ctx.allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, copy, frame);
        try ctx.frames.append(ctx.allocator, copy);
    }

    pub fn deinit(self: *@This()) void {
        for (self.frames.items) |frame| self.allocator.free(frame);
        self.frames.deinit(self.allocator);
    }
};

pub const EventRecorder = struct {
    persistent_export_pressure: usize = 0,
    sturdy_ref_rejections: usize = 0,

    pub fn onEvent(ctx: *anyopaque, event: events.Event) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        switch (event) {
            .pressure => |pressure| {
                if (pressure.resource == .persistent_exports) self.persistent_export_pressure += 1;
            },
            .resource_rejection => |rejection| {
                if (rejection.resource == .sturdy_ref_bytes) self.sturdy_ref_rejections += 1;
            },
            else => {},
        }
    }

    pub fn observer(self: *@This()) events.Observer {
        return events.Observer.init(self, onEvent);
    }
};

pub fn pumpAll(src: *HostPeer, dst: *HostPeer) !void {
    while (src.popOutgoingFrame()) |frame| {
        errdefer src.freeFrame(frame);
        try dst.pushFrame(frame);
        src.freeFrame(frame);
    }
}

pub fn pumpBothWays(a: *HostPeer, b: *HostPeer) !void {
    var rounds: usize = 0;
    while (a.pendingOutgoingCount() > 0 or b.pendingOutgoingCount() > 0) {
        try pumpAll(a, b);
        try pumpAll(b, a);
        rounds += 1;
        if (rounds > 16) return error.PumpDidNotSettle;
    }
}

pub fn expectPeerIdle(peer: *const Peer) !void {
    const stats = peer.stats();
    try std.testing.expectEqual(@as(u32, 0), stats.outbound_questions);
    try std.testing.expectEqual(@as(u32, 0), stats.cancelled_questions);
    try std.testing.expectEqual(@as(u32, 0), stats.active_inbound_questions);
}

pub fn expectPersistenceStats(
    peer: *const Peer,
    persistent_exports: u32,
    saves_served: u64,
    restores_served: u64,
) !void {
    const stats = peer.stats();
    try std.testing.expectEqual(persistent_exports, stats.persistent_exports);
    try std.testing.expectEqual(saves_served, stats.saves_served);
    try std.testing.expectEqual(restores_served, stats.restores_served);
}

pub fn expectImport(peer: *const Peer, import_id: u32) !void {
    try std.testing.expect(peer.caps.imports.contains(import_id));
}

pub fn expectNoImport(peer: *const Peer, import_id: u32) !void {
    try std.testing.expect(!peer.caps.imports.contains(import_id));
}

pub fn expectExport(peer: *const Peer, export_id: u32) !void {
    try std.testing.expect(peer.exports.contains(export_id));
}

pub fn expectNoExport(peer: *const Peer, export_id: u32) !void {
    try std.testing.expect(!peer.exports.contains(export_id));
}
