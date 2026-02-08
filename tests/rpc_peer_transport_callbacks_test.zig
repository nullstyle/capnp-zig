const std = @import("std");
const capnpc = @import("capnpc-zig");

const callbacks = capnpc.rpc.peer_transport_callbacks;

test "peer_transport_callbacks onConnectionMessageFor forwards frame to peer handler" {
    const PeerState = struct {
        calls: usize = 0,
        last_len: usize = 0,
    };
    const Conn = struct {
        ctx: ?*anyopaque = null,
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
    };
    const Conn = struct {
        ctx: ?*anyopaque = null,
    };
    const Hooks = struct {
        fn onClose(peer: *PeerState) void {
            peer.calls += 1;
        }
    };

    var peer = PeerState{};
    var conn = Conn{ .ctx = @ptrCast(&peer) };
    const cb = callbacks.onConnectionCloseFor(PeerState, *Conn, Hooks.onClose);
    cb(&conn);

    try std.testing.expectEqual(@as(usize, 1), peer.calls);
}
