const std = @import("std");
const capnpc = @import("capnpc-zig");

const tcp = capnpc.rpc.transport.tcp;
const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const peer_impl = capnpc.rpc.peer;
const Peer = peer_impl.Peer;
const ServerSession = tcp.ServerSession;
const ClientSession = tcp.ClientSession;

// End-to-end lifecycle coverage for ServerSession: a real client drives a real
// ServerSession over loopback TCP through bootstrap + one call, and both sides
// tear down cleanly with no leaks. This is the server-side mirror of the
// ClientSession suite.

// -- Server: an echo bootstrap that answers every call with an empty struct --

const EchoServer = struct {
    fn onCall(_: *anyopaque, peer: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
        try peer.sendReturnEmptyStruct(call.question_id);
    }
};

const ServerCtx = struct {
    allocator: std.mem.Allocator,
    listener: *tcp.Listener,
    handler_ctx: u8 = 0,

    fn main(self: *ServerCtx) void {
        var session = ServerSession.accept(self.allocator, self.listener, .{}) catch return;
        defer session.deinit();
        _ = session.peer.setBootstrap(.{ .ctx = &self.handler_ctx, .on_call = EchoServer.onCall }) catch return;
        session.run();
    }
};

// -- Client: bootstrap, one call, then close ---------------------------------

const ClientApp = struct {
    session: *ClientSession = undefined,
    bootstrap_id: ?u32 = null,
    calls_ok: usize = 0,
    call_returned: bool = false,
    failed: bool = false,

    fn onBootstrap(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ClientApp = @ptrCast(@alignCast(ctx));
        if (ret.tag != .results) {
            self.failed = true;
            self.session.close();
            return;
        }
        const payload = ret.results orelse {
            self.failed = true;
            self.session.close();
            return;
        };
        const cap = try payload.content.getCapability();
        const resolved = try caps.resolveCapability(cap);
        switch (resolved) {
            .imported => |imported| self.bootstrap_id = imported.id,
            else => {
                self.failed = true;
                self.session.close();
                return;
            },
        }
        _ = try peer.sendCallResolved(
            .{ .imported = .{ .id = self.bootstrap_id.? } },
            0x1234,
            0,
            self,
            buildEmpty,
            onCallReturn,
        );
    }

    fn buildEmpty(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        _ = try call.initCapTableTyped(0);
    }

    fn onCallReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
        const self: *ClientApp = @ptrCast(@alignCast(ctx));
        if (ret.tag == .results) self.calls_ok += 1;
        self.call_returned = true;
        self.session.close();
    }
};

test "ServerSession serves a bootstrap + call against a real ClientSession" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    const listen = try tcp.createListenSocket(io, address, 1, false);
    var listener = tcp.Listener.initFd(allocator, io, .{ .handle = listen.socket.handle }, .{});
    defer listener.close();

    var server_ctx = ServerCtx{ .allocator = allocator, .listener = &listener };
    const server_thread = try std.Thread.spawn(.{}, ServerCtx.main, .{&server_ctx});

    var app = ClientApp{};
    const session = try ClientSession.connect(allocator, io, listen.socket.address, .{});
    defer session.deinit();
    app.session = session;

    _ = try session.peer.sendBootstrap(&app, ClientApp.onBootstrap);
    session.run();
    server_thread.join();

    try std.testing.expect(!app.failed);
    try std.testing.expect(app.call_returned);
    try std.testing.expectEqual(@as(usize, 1), app.calls_ok);
}

test "ServerSession fromPeer recovers the session; deinit-without-run is leak-free" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    const listen = try tcp.createListenSocket(io, address, 1, false);
    var listener = tcp.Listener.initFd(allocator, io, .{ .handle = listen.socket.handle }, .{});
    defer listener.close();

    // A client just to complete the accept() handshake, then dropped.
    const client_stream = try std.Io.net.IpAddress.connect(&listen.socket.address, io, .{ .mode = .stream });
    defer tcp.closeFd(io, .{ .handle = client_stream.socket.handle });

    var session = try ServerSession.accept(allocator, &listener, .{});
    defer session.deinit();
    try std.testing.expectEqual(session, ServerSession.fromPeer(&session.peer));
    try std.testing.expectEqual(@as(?u64, 30_000), session.peer.timeouts.join_timeout_ms);
    try std.testing.expectEqual(@as(?u32, 100), session.conn.tick_interval_ms);
    // deinit (via defer) without ever calling run() must not leak.
}

test "TCP session Join lease options default secure and preserve explicit null opt-out" {
    try std.testing.expectEqual(@as(?u64, 30_000), (tcp.ConnectOptions{}).join_timeout_ms);
    try std.testing.expectEqual(@as(?u64, 30_000), (tcp.ServeOptions{}).join_timeout_ms);

    const client_compat = tcp.ConnectOptions{
        .default_call_timeout_ms = null,
        .join_timeout_ms = null,
    };
    const server_compat = tcp.ServeOptions{
        .default_call_timeout_ms = null,
        .join_timeout_ms = null,
    };
    try std.testing.expectEqual(@as(?u64, null), client_compat.join_timeout_ms);
    try std.testing.expectEqual(@as(?u64, null), server_compat.join_timeout_ms);
}
