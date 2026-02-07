const std = @import("std");
const capnpc = @import("capnpc-zig");

const cap_table = capnpc.rpc.cap_table;
const HostPeer = capnpc.rpc.host_peer.HostPeer;
const Peer = capnpc.rpc.peer.Peer;
const protocol = capnpc.rpc.protocol;

fn pumpAll(src: *HostPeer, dst: *HostPeer) !void {
    while (src.popOutgoingFrame()) |frame| {
        errdefer src.freeFrame(frame);
        try dst.pushFrame(frame);
        src.freeFrame(frame);
    }
}

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

test "host peer tracks outbound bytes and enforces queue limits" {
    const allocator = std.testing.allocator;

    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null);

    host.setLimits(.{ .outbound_count_limit = 1 });
    const limits_after_set = host.getLimits();
    try std.testing.expectEqual(@as(usize, 1), limits_after_set.outbound_count_limit);
    try std.testing.expectEqual(@as(usize, 0), limits_after_set.outbound_bytes_limit);

    try host.peer.sendReturnException(1, "first");
    const first_pending_bytes = host.pendingOutgoingBytes();
    try std.testing.expect(first_pending_bytes > 0);
    try std.testing.expectEqual(@as(usize, 1), host.pendingOutgoingCount());

    try std.testing.expectError(error.OutgoingQueueLimitExceeded, host.peer.sendReturnException(2, "second"));
    try std.testing.expectEqual(@as(usize, 1), host.pendingOutgoingCount());

    const first = host.popOutgoingFrame() orelse return error.ExpectedFrame;
    defer host.freeFrame(first);
    try std.testing.expectEqual(@as(usize, 0), host.pendingOutgoingCount());
    try std.testing.expectEqual(@as(usize, 0), host.pendingOutgoingBytes());

    const bytes_limit: usize = if (first.len > 1) first.len - 1 else 1;
    host.setLimits(.{ .outbound_bytes_limit = bytes_limit });
    try std.testing.expectError(error.OutgoingBytesLimitExceeded, host.peer.sendReturnException(3, "third"));

    host.setLimits(.{});
    try host.peer.sendReturnException(4, "fourth");
    try std.testing.expectEqual(@as(usize, 1), host.pendingOutgoingCount());
}

test "host peer host-call bridge queues call and allows exception response" {
    const allocator = std.testing.allocator;

    const ClientCtx = struct {
        bootstrap_import_id: ?u32 = null,
        call_returned: bool = false,
        saw_expected_exception: bool = false,
    };
    const Handlers = struct {
        fn onBootstrapReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ClientCtx = @ptrCast(@alignCast(ctx));
            try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
            const payload = ret.results orelse return error.MissingPayload;
            const cap = try payload.content.getCapability();
            const resolved = try caps.resolveCapability(cap);
            switch (resolved) {
                .imported => |imported| state.bootstrap_import_id = imported.id,
                else => return error.UnexpectedResolvedCapability,
            }
        }

        fn onCallReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ClientCtx = @ptrCast(@alignCast(ctx));
            state.call_returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("bridge exception", ex.reason);
            state.saw_expected_exception = true;
        }

        fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            try call.setEmptyCapTable();
        }
    };

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null);
    try server.enableHostCallBridge();

    var client_ctx = ClientCtx{};
    _ = try client.peer.sendBootstrap(&client_ctx, Handlers.onBootstrapReturn);

    try pumpAll(&client, &server);
    try pumpAll(&server, &client);
    try pumpAll(&client, &server);

    const bootstrap_import_id = client_ctx.bootstrap_import_id orelse return error.MissingBootstrapImport;
    _ = try client.peer.sendCallResolved(
        .{ .imported = .{ .id = bootstrap_import_id } },
        0x1234,
        9,
        &client_ctx,
        Handlers.buildEmptyCall,
        Handlers.onCallReturn,
    );

    try pumpAll(&client, &server);
    try std.testing.expectEqual(@as(usize, 1), server.pendingHostCallCount());

    const call = server.popHostCall() orelse return error.MissingHostCall;
    try std.testing.expectEqual(@as(u64, 0x1234), call.interface_id);
    try std.testing.expectEqual(@as(u16, 9), call.method_id);

    var decoded = try protocol.DecodedMessage.init(allocator, call.frame);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.call, decoded.tag);
    const decoded_call = try decoded.asCall();
    try std.testing.expectEqual(call.question_id, decoded_call.question_id);

    try server.respondHostCallException(call.question_id, "bridge exception");
    server.freeHostCallFrame(call.frame);

    try pumpAll(&server, &client);
    try std.testing.expect(client_ctx.call_returned);
    try std.testing.expect(client_ctx.saw_expected_exception);
}

test "host peer host-call bridge can respond with results payload" {
    const allocator = std.testing.allocator;

    const ClientCtx = struct {
        bootstrap_import_id: ?u32 = null,
        call_returned: bool = false,
        saw_expected_text: bool = false,
    };
    const Handlers = struct {
        fn onBootstrapReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ClientCtx = @ptrCast(@alignCast(ctx));
            try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
            const payload = ret.results orelse return error.MissingPayload;
            const cap = try payload.content.getCapability();
            const resolved = try caps.resolveCapability(cap);
            switch (resolved) {
                .imported => |imported| state.bootstrap_import_id = imported.id,
                else => return error.UnexpectedResolvedCapability,
            }
        }

        fn onCallReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ClientCtx = @ptrCast(@alignCast(ctx));
            state.call_returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
            const payload = ret.results orelse return error.MissingPayload;
            const text = try payload.content.getText();
            try std.testing.expectEqualStrings("bridge results", text);
            state.saw_expected_text = true;
        }

        fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            try call.setEmptyCapTable();
        }
    };

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null);
    try server.enableHostCallBridge();

    var client_ctx = ClientCtx{};
    _ = try client.peer.sendBootstrap(&client_ctx, Handlers.onBootstrapReturn);

    try pumpAll(&client, &server);
    try pumpAll(&server, &client);
    try pumpAll(&client, &server);

    const bootstrap_import_id = client_ctx.bootstrap_import_id orelse return error.MissingBootstrapImport;
    _ = try client.peer.sendCallResolved(
        .{ .imported = .{ .id = bootstrap_import_id } },
        0x2222,
        3,
        &client_ctx,
        Handlers.buildEmptyCall,
        Handlers.onCallReturn,
    );

    try pumpAll(&client, &server);
    const call = server.popHostCall() orelse return error.MissingHostCall;
    defer server.freeHostCallFrame(call.frame);

    var payload_builder = capnpc.message.MessageBuilder.init(allocator);
    defer payload_builder.deinit();
    const root = try payload_builder.initRootAnyPointer();
    try root.setText("bridge results");
    const payload = try payload_builder.toBytes();
    defer allocator.free(payload);

    try server.respondHostCallResults(call.question_id, payload);
    try pumpAll(&server, &client);

    try std.testing.expect(client_ctx.call_returned);
    try std.testing.expect(client_ctx.saw_expected_text);
}
