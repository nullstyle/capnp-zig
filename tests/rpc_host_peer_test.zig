const std = @import("std");
const capnpc = @import("capnpc-zig");

const cap_table = capnpc.rpc.cap_table;
const HostPeer = capnpc.rpc.host_peer.HostPeer;
const Peer = capnpc.rpc.peer.Peer;
const protocol = capnpc.rpc.protocol;

test "host peer queues outbound frame from detached sendBootstrap" {
    const allocator = std.testing.allocator;

    const Harness = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null);

    var ctx: u8 = 0;
    _ = try host.peer.sendBootstrap(&ctx, Harness.onReturn);
    try std.testing.expectEqual(@as(usize, 1), host.pendingOutgoingCount());

    const frame = host.popOutgoingFrame() orelse return error.ExpectedFrame;
    defer host.freeFrame(frame);
    try std.testing.expect(frame.len > 0);
    try std.testing.expectEqual(@as(usize, 0), host.pendingOutgoingCount());
}

test "host peers can pump bootstrap exchange" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        seen_call: bool = false,
    };
    const ClientCtx = struct {
        returned: bool = false,
        imported_id: ?u32 = null,
    };
    const Handlers = struct {
        fn onCall(ctx: *anyopaque, called_peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ServerCtx = @ptrCast(@alignCast(ctx));
            _ = called_peer;
            _ = call;
            _ = caps;
            state.seen_call = true;
        }

        fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ClientCtx = @ptrCast(@alignCast(ctx));
            state.returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
            const payload = ret.results orelse return error.MissingPayload;
            const cap = try payload.content.getCapability();
            const resolved = try caps.resolveCapability(cap);
            switch (resolved) {
                .imported => |imported| state.imported_id = imported.id,
                else => return error.UnexpectedResolvedCapability,
            }
        }
    };

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null);

    var server_ctx = ServerCtx{};
    _ = try server.peer.setBootstrap(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    var client_ctx = ClientCtx{};
    _ = try client.peer.sendBootstrap(&client_ctx, Handlers.onReturn);
    try std.testing.expectEqual(@as(usize, 1), client.pendingOutgoingCount());

    const bootstrap = client.popOutgoingFrame() orelse return error.ExpectedBootstrapFrame;
    defer client.freeFrame(bootstrap);
    try server.pushFrame(bootstrap);

    const response = server.popOutgoingFrame() orelse return error.ExpectedReturnFrame;
    defer server.freeFrame(response);
    try client.pushFrame(response);

    try std.testing.expect(client_ctx.returned);
    try std.testing.expect(client_ctx.imported_id != null);
    try std.testing.expect(!server_ctx.seen_call);

    try std.testing.expectEqual(@as(usize, 2), client.pendingOutgoingCount());
    while (client.popOutgoingFrame()) |frame| {
        errdefer client.freeFrame(frame);
        try server.pushFrame(frame);
        client.freeFrame(frame);
    }
    try std.testing.expectEqual(@as(usize, 0), server.pendingOutgoingCount());
}

test "host peer rejects oversized outbound frame capture" {
    const allocator = std.testing.allocator;

    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null);

    const too_large_len: usize = 16 * 1024 * 1024 + 1024;
    const reason = try allocator.alloc(u8, too_large_len);
    defer allocator.free(reason);
    @memset(reason, 'x');

    try std.testing.expectError(error.FrameTooLarge, host.peer.sendReturnException(1, reason));
    try std.testing.expectEqual(@as(usize, 0), host.pendingOutgoingCount());
}

test "host peer propagates OOM from outgoing frame allocator" {
    const allocator = std.testing.allocator;

    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    var host = HostPeer.initWithOutgoingAllocator(allocator, failing.allocator());
    defer host.deinit();
    host.start(null, null);

    try std.testing.expectError(error.OutOfMemory, host.peer.sendReturnException(2, "oom"));
    try std.testing.expectEqual(@as(usize, 0), host.pendingOutgoingCount());
}
