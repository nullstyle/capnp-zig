pub fn attachTransportForPeer(
    comptime PeerType: type,
    comptime TransportStartFn: type,
    comptime TransportSendFn: type,
    comptime TransportCloseFn: type,
    comptime TransportIsClosingFn: type,
    peer: *PeerType,
    ctx: *anyopaque,
    start_fn: ?TransportStartFn,
    send_fn: ?TransportSendFn,
    close_fn: ?TransportCloseFn,
    is_closing: ?TransportIsClosingFn,
) void {
    peer.transport_ctx = ctx;
    peer.transport_start = start_fn;
    peer.transport_send = send_fn;
    peer.transport_close = close_fn;
    peer.transport_is_closing = is_closing;
}

pub fn detachTransportForPeer(comptime PeerType: type, peer: *PeerType) void {
    peer.transport_ctx = null;
    peer.transport_start = null;
    peer.transport_send = null;
    peer.transport_close = null;
    peer.transport_is_closing = null;
}

pub fn hasAttachedTransportForPeer(comptime PeerType: type, peer: *const PeerType) bool {
    return peer.transport_ctx != null and peer.transport_send != null;
}

pub fn closeAttachedTransportForPeer(comptime PeerType: type, peer: *PeerType) void {
    if (peer.transport_ctx) |ctx| {
        if (peer.transport_close) |close| close(ctx);
    }
}

pub fn isAttachedTransportClosingForPeer(comptime PeerType: type, peer: *const PeerType) bool {
    if (peer.transport_ctx) |ctx| {
        if (peer.transport_is_closing) |is_closing| return is_closing(ctx);
    }
    return false;
}

pub fn getAttachedConnectionForPeer(
    comptime PeerType: type,
    comptime ConnPtr: type,
    peer: *const PeerType,
) ?ConnPtr {
    const ctx = peer.transport_ctx orelse return null;
    return @ptrCast(@alignCast(ctx));
}

pub fn takeAttachedConnectionForPeer(
    comptime PeerType: type,
    comptime ConnPtr: type,
    peer: *PeerType,
    detach_transport: *const fn (*PeerType) void,
) ?ConnPtr {
    const conn = getAttachedConnectionForPeer(PeerType, ConnPtr, peer);
    detach_transport(peer);
    return conn;
}
