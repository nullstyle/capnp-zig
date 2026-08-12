const std = @import("std");
const capnpc = @import("capnpc-zig");

const callbacks = capnpc.rpc.testing.peer_transport_callbacks;

test "peer_transport_callbacks onConnectionMessageFor forwards frame to peer handler" {
    const PeerState = struct {
        calls: usize = 0,
        last_len: usize = 0,
    };
    const Conn = struct {
        ctx: ?*anyopaque = null,

        pub fn context(self: *const @This()) ?*anyopaque {
            return self.ctx;
        }
    };
    const Hooks = struct {
        fn handleFrame(peer: *PeerState, frame: []const u8) !void {
            peer.calls += 1;
            peer.last_len = frame.len;
        }
    };

    var peer = PeerState{};
    var conn = Conn{ .ctx = @ptrCast(&peer) };
    const cb = callbacks.onConnectionMessageFor(PeerState, *Conn, Hooks.handleFrame);
    try cb(&conn, "abcd");

    try std.testing.expectEqual(@as(usize, 1), peer.calls);
    try std.testing.expectEqual(@as(usize, 4), peer.last_len);
}

test "peer_transport_callbacks onConnectionErrorFor forwards errors to peer error hook" {
    const PeerState = struct {
        calls: usize = 0,
        saw_err: bool = false,
    };
    const Conn = struct {
        ctx: ?*anyopaque = null,

        pub fn context(self: *const @This()) ?*anyopaque {
            return self.ctx;
        }
    };
    const Hooks = struct {
        fn onError(peer: *PeerState, err: anyerror) void {
            peer.calls += 1;
            peer.saw_err = err == error.TestUnexpectedResult;
        }
    };

    var peer = PeerState{};
    var conn = Conn{ .ctx = @ptrCast(&peer) };
    const cb = callbacks.onConnectionErrorFor(PeerState, *Conn, Hooks.onError);
    cb(&conn, error.TestUnexpectedResult);

    try std.testing.expectEqual(@as(usize, 1), peer.calls);
    try std.testing.expect(peer.saw_err);
}

test "peer_transport_callbacks onConnectionCloseFor forwards close notifications" {
    const PeerState = struct {
        calls: usize = 0,
        detached: bool = false,

        pub fn detachTransport(self: *@This()) void {
            self.detached = true;
        }
    };
    const Conn = struct {
        ctx: ?*anyopaque = null,

        pub fn context(self: *const @This()) ?*anyopaque {
            return self.ctx;
        }
    };
    const Hooks = struct {
        fn onClose(peer: *PeerState) void {
            std.debug.assert(peer.detached);
            peer.calls += 1;
        }
    };

    var peer = PeerState{};
    var conn = Conn{ .ctx = @ptrCast(&peer) };
    const cb = callbacks.onConnectionCloseFor(PeerState, *Conn, Hooks.onClose);
    cb(&conn);

    try std.testing.expectEqual(@as(usize, 1), peer.calls);
    try std.testing.expect(peer.detached);
}

test "peer_transport_callbacks terminal close detaches a real Peer before notification" {
    const Peer = capnpc.rpc.peer.Peer;
    const peer_test_hooks = Peer.test_hooks;
    const Conn = struct {
        ctx: ?*anyopaque = null,

        pub fn context(self: *const @This()) ?*anyopaque {
            return self.ctx;
        }
    };
    const Transport = struct {
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
    };
    const Hooks = struct {
        var saw_detached = false;

        fn onClose(peer: *Peer) void {
            saw_detached = !peer.hasAttachedTransport();
            peer.notifyTransportClosed();
        }
    };

    var peer = Peer.initDetached(std.testing.allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();
    var conn = Conn{ .ctx = @ptrCast(&peer) };
    peer.attachTransport(&conn, null, Transport.send, null, null);
    try std.testing.expect(peer.hasAttachedTransport());

    Hooks.saw_detached = false;
    const cb = callbacks.onConnectionCloseFor(Peer, *Conn, Hooks.onClose);
    cb(&conn);

    try std.testing.expect(Hooks.saw_detached);
    try std.testing.expect(!peer.hasAttachedTransport());
    try std.testing.expectError(
        error.TransportNotAttached,
        peer_test_hooks.sendFrame(&peer, &.{0}),
    );
}
