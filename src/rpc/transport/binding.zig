const std = @import("std");
const fd_passing = @import("./fd_passing.zig");

const FdHandle = fd_passing.FdHandle;

/// Explicit callback contract used by an RPC peer to talk to an underlying
/// transport.
///
/// `PeerType` is usually `peer.Peer`. Keeping it as a type parameter
/// lets unit tests bind lightweight peer-shaped structs without importing the
/// full runtime.
pub fn Binding(comptime PeerType: type) type {
    return struct {
        const Self = @This();

        /// Transport callback: start listening for inbound frames.
        pub const StartFn = *const fn (ctx: *anyopaque, peer: *PeerType) void;
        /// Transport callback: send a framed message to the remote peer.
        pub const SendFn = *const fn (ctx: *anyopaque, frame: []const u8) anyerror!void;
        /// Transport callback: close the underlying connection.
        pub const CloseFn = *const fn (ctx: *anyopaque) void;
        /// Transport callback: check if the connection is in the process of closing.
        pub const IsClosingFn = *const fn (ctx: *anyopaque) bool;
        /// Transport callback (Experimental, fd passing): send a framed
        /// message with `fds` attached, in order (`CapDescriptor.attachedFd`
        /// indexes them). The fds are borrowed: the transport keeps its own
        /// copies for as long as it needs them.
        pub const SendWithFdsFn = *const fn (ctx: *anyopaque, frame: []const u8, fds: []const FdHandle) anyerror!void;
        /// Transport callback (Experimental, fd passing): take fd `index` of
        /// the inbound frame being dispatched (the peer's `handleFrame` call
        /// in progress). The caller owns it from then on. Null when the frame
        /// has no fd at `index`, or it was already taken.
        pub const TakeFrameFdFn = *const fn (ctx: *anyopaque, index: u8) ?FdHandle;

        /// Opaque pointer to the attached transport/connection. Must remain
        /// valid until the peer detaches the binding or is deinitialized.
        ctx: ?*anyopaque = null,
        start: ?StartFn = null,
        send: ?SendFn = null,
        close: ?CloseFn = null,
        is_closing: ?IsClosingFn = null,
        /// Experimental (fd passing). Null on a transport that cannot carry
        /// fds; then the peer never attaches one.
        send_with_fds: ?SendWithFdsFn = null,
        /// Experimental (fd passing). Null on a transport that delivers no
        /// fds.
        take_frame_fd: ?TakeFrameFdFn = null,
        /// Experimental (fd passing). The most fds one outbound frame may
        /// carry through `send_with_fds`: 0 for TCP and QUIC, at most
        /// `fd_passing.max_fds_per_message_cap` (253) on an AF_UNIX
        /// connection.
        max_outbound_fds: u8 = 0,

        pub fn init(
            ctx: *anyopaque,
            start: ?StartFn,
            send: ?SendFn,
            close: ?CloseFn,
            is_closing: ?IsClosingFn,
        ) Self {
            return .{
                .ctx = ctx,
                .start = start,
                .send = send,
                .close = close,
                .is_closing = is_closing,
            };
        }

        pub fn isAttached(self: Self) bool {
            return self.ctx != null and self.send != null;
        }

        pub fn startIfPresent(self: Self, peer: *PeerType) void {
            const ctx = self.ctx orelse return;
            const start = self.start orelse return;
            start(ctx, peer);
        }

        pub fn sendFrame(self: Self, frame: []const u8) !void {
            const send = self.send orelse return error.TransportNotAttached;
            const ctx = self.ctx orelse return error.TransportNotAttached;
            try send(ctx, frame);
        }

        pub fn closeIfPresent(self: Self) void {
            const ctx = self.ctx orelse return;
            const close = self.close orelse return;
            close(ctx);
        }

        pub fn isClosing(self: Self) bool {
            const ctx = self.ctx orelse return false;
            const is_closing = self.is_closing orelse return false;
            return is_closing(ctx);
        }

        /// Experimental. The most fds one outbound frame may carry: 0 unless
        /// the binding has a `send_with_fds` hook, and never more than
        /// `fd_passing.max_fds_per_message_cap`.
        pub fn outboundFdLimit(self: Self) u8 {
            if (comptime !fd_passing.supported) return 0;
            if (self.ctx == null or self.send_with_fds == null) return 0;
            return @min(self.max_outbound_fds, fd_passing.max_fds_per_message_cap);
        }

        /// Experimental. Send `frame` with `fds` attached through the
        /// `send_with_fds` hook. With no fds this is `sendFrame`. The error
        /// set is open because the hook is the transport's own code.
        pub fn sendFrameWithFds(self: Self, frame: []const u8, fds: []const FdHandle) anyerror!void {
            if (fds.len == 0) return self.sendFrame(frame);
            const ctx = self.ctx orelse return error.TransportNotAttached;
            const send_with_fds = self.send_with_fds orelse return error.FdPassingUnsupported;
            if (fds.len > self.outboundFdLimit()) return error.TooManyFds;
            try send_with_fds(ctx, frame, fds);
        }

        /// Experimental. Take fd `index` of the inbound frame being
        /// dispatched (see `TakeFrameFdFn`). Null without a hook.
        pub fn takeFrameFd(self: Self, index: u8) ?FdHandle {
            const ctx = self.ctx orelse return null;
            const take = self.take_frame_fd orelse return null;
            return take(ctx, index);
        }
    };
}

test "transport binding reports attached state from context and send callback" {
    const FakePeer = struct {};
    const FakeTransport = struct {
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
    };
    const TestBinding = Binding(FakePeer);

    var ctx: u8 = 0;
    const detached = TestBinding{};
    const attached = TestBinding.init(&ctx, null, FakeTransport.send, null, null);
    const missing_send = TestBinding.init(&ctx, null, null, null, null);

    try std.testing.expect(!detached.isAttached());
    try std.testing.expect(attached.isAttached());
    try std.testing.expect(!missing_send.isAttached());
}
