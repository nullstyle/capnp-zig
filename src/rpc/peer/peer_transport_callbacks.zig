const transport_binding = @import("../transport/binding.zig");
const fd_passing = @import("../transport/fd_passing.zig");

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

pub fn peerFromConnection(
    comptime PeerType: type,
    comptime ConnPtr: type,
    conn: ConnPtr,
) *PeerType {
    return @ptrCast(@alignCast(conn.context().?));
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

pub fn bindingForConnection(
    comptime PeerType: type,
    comptime ConnPtr: type,
    conn: ConnPtr,
    comptime handle_frame: *const fn (*PeerType, []const u8) anyerror!void,
    comptime notify_error: *const fn (*PeerType, anyerror) void,
    comptime notify_close: *const fn (*PeerType) void,
) transport_binding.Binding(PeerType) {
    const Binding = transport_binding.Binding(PeerType);
    var binding = Binding.init(
        conn,
        struct {
            fn call(ctx: *anyopaque, peer: *PeerType) void {
                const typed: ConnPtr = castCtx(ConnPtr, ctx);
                typed.start(
                    peer,
                    onConnectionMessageFor(PeerType, ConnPtr, handle_frame),
                    onConnectionErrorFor(PeerType, ConnPtr, notify_error),
                    onConnectionCloseFor(PeerType, ConnPtr, notify_close),
                );
            }
        }.call,
        struct {
            fn call(ctx: *anyopaque, frame: []const u8) anyerror!void {
                const typed: ConnPtr = castCtx(ConnPtr, ctx);
                try typed.sendFrame(frame);
            }
        }.call,
        struct {
            fn call(ctx: *anyopaque) void {
                const typed: ConnPtr = castCtx(ConnPtr, ctx);
                typed.close();
            }
        }.call,
        struct {
            fn call(ctx: *anyopaque) bool {
                const typed: ConnPtr = castCtx(ConnPtr, ctx);
                return typed.isClosing();
            }
        }.call,
    );
    // Fd passing (Experimental): a connection that can carry fds says how
    // many one frame may carry (0 for TCP, and until fd passing is on: it
    // is asked for every frame, so `enableFdPassing` may come after the
    // attach), and hands the peer the fds of the frame it is dispatching.
    const Conn = @typeInfo(ConnPtr).pointer.child;
    if (comptime @hasDecl(Conn, "sendFrameWithFds") and @hasDecl(Conn, "takeFrameFd") and @hasDecl(Conn, "maxOutboundFds")) {
        binding.max_outbound_fds = struct {
            fn call(ctx: *anyopaque) u8 {
                const typed: ConnPtr = castCtx(ConnPtr, ctx);
                return typed.maxOutboundFds();
            }
        }.call;
        binding.send_with_fds = struct {
            fn call(ctx: *anyopaque, frame: []const u8, fds: []const fd_passing.FdHandle) anyerror!void {
                const typed: ConnPtr = castCtx(ConnPtr, ctx);
                try typed.sendFrameWithFds(frame, fds);
            }
        }.call;
        binding.take_frame_fd = struct {
            fn call(ctx: *anyopaque, index: u8) ?fd_passing.FdHandle {
                const typed: ConnPtr = castCtx(ConnPtr, ctx);
                return typed.takeFrameFd(index);
            }
        }.call;
    }
    return binding;
}

/// Tick callback that drives the peer's deadline sweep from the
/// connection's run loop. The connection's `ctx` is the peer once
/// `start()` has run; ticks before that are ignored.
pub fn onConnectionTickFor(
    comptime PeerType: type,
    comptime ConnPtr: type,
) *const fn (conn: ConnPtr) void {
    return struct {
        fn call(conn: ConnPtr) void {
            const ctx = conn.context() orelse return;
            const peer: *PeerType = @ptrCast(@alignCast(ctx));
            _ = peer.checkDeadlines();
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
            // Capture the transport's typed close cause while `conn` is
            // still valid — question callbacks cancelled during
            // notify_close read it via `lastDisconnectCause()`. Same
            // capability-detection pattern as `on_tick`.
            const Conn = @typeInfo(ConnPtr).pointer.child;
            if (comptime @hasDecl(Conn, "closeCause")) {
                peer.last_disconnect_cause = conn.closeCause();
            }
            // The connection owns this callback and may free itself as soon as
            // the terminal notification returns. Clear the peer's borrowed
            // transport context while `conn` is still valid, before user close
            // callbacks can destroy the enclosing session or re-enter Peer.
            peer.detachTransport();
            notify_close(peer);
        }
    }.call;
}
