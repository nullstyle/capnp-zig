const std = @import("std");
const peer_mod = @import("peer.zig");

pub const HostPeer = struct {
    const MAX_CAPTURED_FRAME_BYTES: usize = 16 * 1024 * 1024;

    allocator: std.mem.Allocator,
    outgoing_allocator: std.mem.Allocator,
    peer: peer_mod.Peer,
    outgoing: std.ArrayList([]u8),
    wired_override: bool = false,

    pub fn init(allocator: std.mem.Allocator) HostPeer {
        return initWithOutgoingAllocator(allocator, allocator);
    }

    pub fn initWithOutgoingAllocator(
        allocator: std.mem.Allocator,
        outgoing_allocator: std.mem.Allocator,
    ) HostPeer {
        return .{
            .allocator = allocator,
            .outgoing_allocator = outgoing_allocator,
            .peer = peer_mod.Peer.initDetached(allocator),
            .outgoing = std.ArrayList([]u8){},
        };
    }

    pub fn deinit(self: *HostPeer) void {
        self.clearOutgoing();
        self.outgoing.deinit(self.allocator);
        self.peer.deinit();
    }

    pub fn start(
        self: *HostPeer,
        on_error: ?*const fn (peer: *peer_mod.Peer, err: anyerror) void,
        on_close: ?*const fn (peer: *peer_mod.Peer) void,
    ) void {
        self.ensureOverride();
        self.peer.start(on_error, on_close);
    }

    pub fn pushFrame(self: *HostPeer, frame: []const u8) !void {
        self.ensureOverride();
        try self.peer.handleFrame(frame);
    }

    pub fn popOutgoingFrame(self: *HostPeer) ?[]u8 {
        if (self.outgoing.items.len == 0) return null;
        return self.outgoing.orderedRemove(0);
    }

    pub fn pendingOutgoingCount(self: *const HostPeer) usize {
        return self.outgoing.items.len;
    }

    pub fn freeFrame(self: *HostPeer, frame: []u8) void {
        self.outgoing_allocator.free(frame);
    }

    pub fn clearOutgoing(self: *HostPeer) void {
        for (self.outgoing.items) |frame| self.outgoing_allocator.free(frame);
        self.outgoing.clearRetainingCapacity();
    }

    fn ensureOverride(self: *HostPeer) void {
        if (self.wired_override) return;
        self.peer.setSendFrameOverride(self, captureOutgoingFrame);
        self.wired_override = true;
    }

    fn captureOutgoingFrame(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *HostPeer = @ptrCast(@alignCast(ctx));
        if (frame.len > MAX_CAPTURED_FRAME_BYTES) return error.FrameTooLarge;
        const owned = try self.outgoing_allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, owned, frame);
        try self.outgoing.append(self.allocator, owned);
    }
};
