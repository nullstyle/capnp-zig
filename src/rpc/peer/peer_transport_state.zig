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
    peer.transport.ctx = ctx;
    peer.transport.start = start_fn;
    peer.transport.send = send_fn;
    peer.transport.close = close_fn;
    peer.transport.is_closing = is_closing;
}

pub fn detachTransportForPeer(comptime PeerType: type, peer: *PeerType) void {
    peer.transport = .{};
}

pub fn hasAttachedTransportForPeer(comptime PeerType: type, peer: *const PeerType) bool {
    const Transport = @TypeOf(peer.transport);
    if (@hasDecl(Transport, "isAttached")) return peer.transport.isAttached();
    return peer.transport.ctx != null and peer.transport.send != null;
}

pub fn closeAttachedTransportForPeer(comptime PeerType: type, peer: *PeerType) void {
    const Transport = @TypeOf(peer.transport);
    if (@hasDecl(Transport, "closeIfPresent")) {
        peer.transport.closeIfPresent();
        return;
    }
    if (peer.transport.ctx) |ctx| {
        if (peer.transport.close) |close| close(ctx);
    }
}

pub fn isAttachedTransportClosingForPeer(comptime PeerType: type, peer: *const PeerType) bool {
    const Transport = @TypeOf(peer.transport);
    if (@hasDecl(Transport, "isClosing")) return peer.transport.isClosing();
    if (peer.transport.ctx) |ctx| {
        if (peer.transport.is_closing) |is_closing| return is_closing(ctx);
    }
    return false;
}

pub fn getAttachedConnectionForPeer(
    comptime PeerType: type,
    comptime ConnPtr: type,
    peer: *const PeerType,
) ?ConnPtr {
    const ctx = peer.transport.ctx orelse return null;
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
