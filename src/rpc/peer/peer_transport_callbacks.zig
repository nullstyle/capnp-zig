pub fn peerFromConnection(
    comptime PeerType: type,
    comptime ConnPtr: type,
    conn: ConnPtr,
) *PeerType {
    return @ptrCast(@alignCast(conn.ctx.?));
}

pub fn onConnectionMessageFor(
    comptime PeerType: type,
    comptime ConnPtr: type,
    comptime handle_frame: *const fn (*PeerType, []const u8) anyerror!void,
) *const fn (conn: ConnPtr, frame: []const u8) anyerror!void {
    return struct {
        fn call(conn: ConnPtr, frame: []const u8) anyerror!void {
            const peer = peerFromConnection(PeerType, ConnPtr, conn);
            try handle_frame(peer, frame);
        }
    }.call;
}

pub fn onConnectionErrorFor(
    comptime PeerType: type,
    comptime ConnPtr: type,
    comptime notify_error: *const fn (*PeerType, anyerror) void,
) *const fn (conn: ConnPtr, err: anyerror) void {
    return struct {
        fn call(conn: ConnPtr, err: anyerror) void {
            const peer = peerFromConnection(PeerType, ConnPtr, conn);
            notify_error(peer, err);
        }
    }.call;
}

pub fn onConnectionCloseFor(
    comptime PeerType: type,
    comptime ConnPtr: type,
    comptime notify_close: *const fn (*PeerType) void,
) *const fn (conn: ConnPtr) void {
    return struct {
        fn call(conn: ConnPtr) void {
            const peer = peerFromConnection(PeerType, ConnPtr, conn);
            notify_close(peer);
        }
    }.call;
}
