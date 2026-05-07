const std = @import("std");

/// Explicit callback contract used by a level3 RPC peer to talk to an
/// underlying transport.
///
/// `PeerType` is usually `level3.peer.Peer`. Keeping it as a type parameter
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

        /// Opaque pointer to the attached transport/connection. Must remain
        /// valid until the peer detaches the binding or is deinitialized.
        ctx: ?*anyopaque = null,
        start: ?StartFn = null,
        send: ?SendFn = null,
        close: ?CloseFn = null,
        is_closing: ?IsClosingFn = null,

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
