const std = @import("std");
const capnpc = @import("capnpc-zig");

const cap_table = capnpc.rpc.caps.table;
const HostPeer = capnpc.rpc.integration.host_peer.HostPeer;
const Peer = capnpc.rpc.peer.Peer;
const protocol = capnpc.rpc.wire.protocol;

const default_host_error_reason = HostPeer.ErrorDisclosurePolicy.default_generic_reason;

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
    host.start(null, null, null);

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
    client.start(null, null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null, null);

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
    host.start(null, null, null);

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
    host.start(null, null, null);

    try std.testing.expectError(error.OutOfMemory, host.peer.sendReturnException(2, "oom"));
    try std.testing.expectEqual(@as(usize, 0), host.pendingOutgoingCount());
}

test "host peer defaults bound outbound queues" {
    const allocator = std.testing.allocator;

    var host = HostPeer.init(allocator);
    defer host.deinit();

    const limits = host.getLimits();
    try std.testing.expectEqual(HostPeer.Limits.default_outbound_count_limit, limits.outbound_count_limit);
    try std.testing.expectEqual(HostPeer.Limits.default_outbound_bytes_limit, limits.outbound_bytes_limit);
    try std.testing.expectEqual(HostPeer.Limits.default_host_call_count_limit, limits.host_call_count_limit);
    try std.testing.expectEqual(HostPeer.Limits.default_host_call_bytes_limit, limits.host_call_bytes_limit);
}

test "host peer redacts captured exception frames by default" {
    const allocator = std.testing.allocator;

    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null, null);

    try host.peer.sendReturnException(7, "internal detail\nwith control");

    const frame = host.popOutgoingFrame() orelse return error.ExpectedFrame;
    defer host.freeFrame(frame);

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();

    const ret = try decoded.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings(default_host_error_reason, ex.reason);
}

test "host peer debug error disclosure caps and sanitizes exception frames" {
    const allocator = std.testing.allocator;

    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null, null);
    host.setErrorDisclosurePolicy(.{
        .reveal_details = true,
        .max_reason_bytes = 6,
    });

    try host.peer.sendReturnException(8, "abc\ndefgh");

    const frame = host.popOutgoingFrame() orelse return error.ExpectedFrame;
    defer host.freeFrame(frame);

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();

    const ret = try decoded.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("abc de", ex.reason);
}

test "host peer redacts captured abort frames by default" {
    const allocator = std.testing.allocator;

    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null, null);

    try std.testing.expectError(error.TruncatedMessage, host.pushFrame(&[_]u8{}));

    const frame = host.popOutgoingFrame() orelse return error.ExpectedFrame;
    defer host.freeFrame(frame);

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();

    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings(default_host_error_reason, abort.exception.reason);
    try std.testing.expect(std.mem.indexOf(u8, abort.exception.reason, "TruncatedMessage") == null);
}

test "host peer custom abort disclosure caps and sanitizes generic reason" {
    const allocator = std.testing.allocator;

    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null, null);
    host.setErrorDisclosurePolicy(.{
        .max_reason_bytes = 12,
        .generic_reason = "src/rpc\nsecret.zig:42",
    });

    try std.testing.expectError(error.TruncatedMessage, host.pushFrame(&[_]u8{}));

    const frame = host.popOutgoingFrame() orelse return error.ExpectedFrame;
    defer host.freeFrame(frame);

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();

    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings("src/rpc secr", abort.exception.reason);
}

test "host peer tracks outbound bytes and enforces queue limits" {
    const allocator = std.testing.allocator;

    var host = HostPeer.init(allocator);
    defer host.deinit();
    host.start(null, null, null);

    host.setLimits(.{ .outbound_count_limit = 1 });
    const limits_after_set = host.getLimits();
    try std.testing.expectEqual(@as(usize, 1), limits_after_set.outbound_count_limit);
    try std.testing.expectEqual(HostPeer.Limits.default_outbound_bytes_limit, limits_after_set.outbound_bytes_limit);
    try std.testing.expectEqual(HostPeer.Limits.default_host_call_count_limit, limits_after_set.host_call_count_limit);
    try std.testing.expectEqual(HostPeer.Limits.default_host_call_bytes_limit, limits_after_set.host_call_bytes_limit);

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

test "host peer host-call bridge enforces queued call count limit" {
    const allocator = std.testing.allocator;

    const ClientCtx = struct {
        bootstrap_import_id: ?u32 = null,
        limit_returned: bool = false,
    };
    const Handlers = struct {
        fn onBootstrapReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ClientCtx = @ptrCast(@alignCast(ctx));
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
            state.limit_returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings(default_host_error_reason, ex.reason);
        }

        fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            _ = try call.initCapTableTyped(0);
        }
    };

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null, null);
    server.setLimits(.{ .host_call_count_limit = 1, .host_call_bytes_limit = 0 });
    try server.enableHostCallBridge();

    var client_ctx = ClientCtx{};
    _ = try client.peer.sendBootstrap(&client_ctx, Handlers.onBootstrapReturn);
    try pumpAll(&client, &server);
    try pumpAll(&server, &client);
    try pumpAll(&client, &server);

    const bootstrap_import_id = client_ctx.bootstrap_import_id orelse return error.MissingBootstrapImport;
    _ = try client.peer.sendCallResolved(
        .{ .imported = .{ .id = bootstrap_import_id } },
        0x1111,
        1,
        &client_ctx,
        Handlers.buildEmptyCall,
        Handlers.onCallReturn,
    );
    try pumpAll(&client, &server);
    try std.testing.expectEqual(@as(usize, 1), server.pendingHostCallCount());
    try std.testing.expect(server.pendingHostCallBytes() > 0);

    _ = try client.peer.sendCallResolved(
        .{ .imported = .{ .id = bootstrap_import_id } },
        0x1111,
        2,
        &client_ctx,
        Handlers.buildEmptyCall,
        Handlers.onCallReturn,
    );
    try pumpAll(&client, &server);
    try std.testing.expectEqual(@as(usize, 1), server.pendingHostCallCount());
    try pumpAll(&server, &client);
    try std.testing.expect(client_ctx.limit_returned);

    const call = server.popHostCall() orelse return error.MissingHostCall;
    defer server.freeHostCallFrame(call.frame);
    try std.testing.expectEqual(@as(usize, 0), server.pendingHostCallBytes());
    try server.respondHostCallException(call.question_id, "dropped");
}

test "host peer host-call bridge enforces queued call byte limit before copy" {
    const allocator = std.testing.allocator;

    const ClientCtx = struct {
        bootstrap_import_id: ?u32 = null,
        limit_returned: bool = false,
    };
    const Handlers = struct {
        fn onBootstrapReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ClientCtx = @ptrCast(@alignCast(ctx));
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
            state.limit_returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings(default_host_error_reason, ex.reason);
        }

        fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            _ = try call.initCapTableTyped(0);
        }
    };

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null, null);
    server.setLimits(.{ .host_call_count_limit = 0, .host_call_bytes_limit = 1 });
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
    try std.testing.expectEqual(@as(usize, 0), server.pendingHostCallCount());
    try std.testing.expectEqual(@as(usize, 0), server.pendingHostCallBytes());
    try pumpAll(&server, &client);
    try std.testing.expect(client_ctx.limit_returned);
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
            try std.testing.expectEqualStrings(default_host_error_reason, ex.reason);
            state.saw_expected_exception = true;
        }

        fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            _ = try call.initCapTableTyped(0);
        }
    };

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null, null);
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

test "host peer host-call bridge redacts prebuilt exception returns" {
    const allocator = std.testing.allocator;

    const ClientCtx = struct {
        bootstrap_import_id: ?u32 = null,
        call_returned: bool = false,
        saw_redacted_exception: bool = false,
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
            try std.testing.expectEqualStrings(default_host_error_reason, ex.reason);
            state.saw_redacted_exception = true;
        }

        fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            _ = try call.initCapTableTyped(0);
        }
    };

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null, null);
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
    const call = server.popHostCall() orelse return error.MissingHostCall;
    defer server.freeHostCallFrame(call.frame);

    var return_builder = protocol.MessageBuilder.init(allocator);
    defer return_builder.deinit();
    var ret = try return_builder.beginReturn(call.question_id, .exception);
    try ret.setException("prebuilt secret\nstack path");
    const return_frame = try return_builder.finish();
    defer allocator.free(return_frame);

    try server.respondHostCallReturnFrame(return_frame);
    try pumpAll(&server, &client);

    try std.testing.expect(client_ctx.call_returned);
    try std.testing.expect(client_ctx.saw_redacted_exception);
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
            _ = try call.initCapTableTyped(0);
        }
    };

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null, null);
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

// --- respondHostCallResults amplification-DoS hardening ---
//
// respondHostCallResults deep-copies the guest-supplied payload via
// cloneAnyPointer, which enforces per-read bounds and a depth limit but no
// traversal budget and no visited-set. A hostile guest can hand us a small
// physical frame whose pointer graph either aliases one large blob thousands
// of times or forms a deep chain, turning the clone into effectively unbounded
// work while the ABI global mutex is held. The fix runs the payload through
// Message.init (full validation walk with a bounded traversal/nesting budget)
// and caps the raw frame size, so these payloads are rejected up front.

// Build a single-segment framed message from a segment's word buffer.
// `segment_words` holds the little-endian u64 words of segment 0; word 0 is
// the root pointer. Caller owns the returned slice.
fn framedSingleSegment(allocator: std.mem.Allocator, segment_words: []const u64) ![]u8 {
    const seg_bytes = segment_words.len * 8;
    var out = try allocator.alloc(u8, 8 + seg_bytes);
    // Header: segment_count-1 = 0, then segment 0 size in words. One segment is
    // odd, so no trailing padding word is required.
    std.mem.writeInt(u32, out[0..4], 0, .little);
    std.mem.writeInt(u32, out[4..8], @intCast(segment_words.len), .little);
    for (segment_words, 0..) |word, i| {
        std.mem.writeInt(u64, out[8 + i * 8 ..][0..8], word, .little);
    }
    return out;
}

fn structPtr(offset_words: i32, data_words: u16, pointer_words: u16) u64 {
    var word: u64 = 0; // struct pointer tag (0)
    // Encode the 30-bit signed word offset in bits 2..32 (two's complement).
    const raw_offset: u32 = @bitCast(offset_words);
    word |= @as(u64, raw_offset & 0x3FFF_FFFF) << 2;
    word |= @as(u64, data_words) << 32;
    word |= @as(u64, pointer_words) << 48;
    return word;
}

// Drive the host-call bridge handshake and invoke `respondHostCallResults` with
// `crafted_payload` against the real pending question id. Returns the result so
// the caller can assert on the error (or success). Fully self-contained: sets up
// two host peers, performs bootstrap + call, then responds.
fn respondWithCraftedPayload(allocator: std.mem.Allocator, crafted_payload: []const u8) !void {
    const ClientCtx = struct {
        bootstrap_import_id: ?u32 = null,
        call_returned: bool = false,
    };
    const Handlers = struct {
        fn onBootstrapReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ClientCtx = @ptrCast(@alignCast(ctx));
            const payload = ret.results orelse return error.MissingPayload;
            const cap = try payload.content.getCapability();
            const resolved = try caps.resolveCapability(cap);
            switch (resolved) {
                .imported => |imported| state.bootstrap_import_id = imported.id,
                else => return error.UnexpectedResolvedCapability,
            }
        }
        fn onCallReturn(ctx: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ClientCtx = @ptrCast(@alignCast(ctx));
            state.call_returned = true;
        }
        fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            _ = try call.initCapTableTyped(0);
        }
    };

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null, null);
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

    // Propagate the exact error from respondHostCallResults to the caller.
    try server.respondHostCallResults(call.question_id, crafted_payload);
    // On success, drain the outbound frame so no allocation leaks.
    try pumpAll(&server, &client);
}

test "respondHostCallResults rejects pointer-aliasing amplification payload" {
    const allocator = std.testing.allocator;

    // Root struct has N pointer words, each a struct pointer aliasing the same
    // blob struct (D data words). Physically tiny, but the validator counts
    // every logical visit (N * D words) with no visited-set, so a bounded
    // traversal budget rejects it. N * D is chosen to exceed the 8M-word
    // default traversal_limit_words.
    const n_pointers: u16 = 300;
    const blob_data_words: u16 = 30_000; // 300 * 30000 = 9,000,000 > 8,388,608

    const total_words = 1 + @as(usize, n_pointers) + @as(usize, blob_data_words);
    const words = try allocator.alloc(u64, total_words);
    defer allocator.free(words);
    @memset(words, 0);

    // Word 0: root struct pointer -> root struct at word 1 (offset 0), with
    // 0 data words and n_pointers pointer words.
    words[0] = structPtr(0, 0, n_pointers);

    // Root struct pointer section occupies words 1..1+n_pointers. The blob
    // struct begins at word 1 + n_pointers.
    const blob_word: usize = 1 + @as(usize, n_pointers);
    var i: usize = 0;
    while (i < n_pointers) : (i += 1) {
        const ptr_word_index = 1 + i;
        // A struct pointer at word `ptr_word_index` points to `ptr_word_index
        // + 1 + offset`. Aim all of them at the single blob.
        const offset: i32 = @intCast(blob_word - (ptr_word_index + 1));
        words[ptr_word_index] = structPtr(offset, blob_data_words, 0);
    }

    const payload = try framedSingleSegment(allocator, words);
    defer allocator.free(payload);

    try std.testing.expectError(error.TraversalLimitExceeded, respondWithCraftedPayload(allocator, payload));
}

const HostCallParamCapHarness = struct {
    const client_sink_export_id: u32 = 7;
    const call_question_id: u32 = 2;

    server: HostPeer,

    fn init(allocator: std.mem.Allocator) HostCallParamCapHarness {
        return .{ .server = HostPeer.init(allocator) };
    }

    /// Wire the server and push the bootstrap + cap-bearing Call frames.
    /// Must run after the harness reached its final address: `start` captures
    /// `&self.server` in the peer's send-frame override, so the struct cannot
    /// move afterwards.
    fn setup(self: *HostCallParamCapHarness, allocator: std.mem.Allocator) !void {
        self.server.start(null, null, null);
        try self.server.enableHostCallBridge();

        // Raw-frame client (a host-side wire client with no peer of its own,
        // the wasm-relay topology). Bootstrap to learn the root export id.
        var bootstrap_builder = protocol.MessageBuilder.init(allocator);
        defer bootstrap_builder.deinit();
        try bootstrap_builder.buildBootstrap(1);
        const bootstrap_frame = try bootstrap_builder.finish();
        defer allocator.free(bootstrap_frame);
        try self.server.pushFrame(bootstrap_frame);

        var bootstrap_export_id: ?u32 = null;
        while (self.server.popOutgoingFrame()) |frame| {
            defer self.server.freeFrame(frame);
            var decoded = try protocol.DecodedMessage.init(allocator, frame);
            defer decoded.deinit();
            if (decoded.tag != .@"return") continue;
            const ret = try decoded.asReturn();
            const payload = ret.results orelse return error.MissingPayload;
            const list = payload.cap_table orelse return error.MissingCapTable;
            const desc = try protocol.CapDescriptor.fromReader(try list.get(0));
            bootstrap_export_id = desc.id orelse return error.MissingId;
        }

        // Call the root export with one senderHosted param cap: the client's
        // sink capability (client-minted export id).
        var call_builder = protocol.MessageBuilder.init(allocator);
        defer call_builder.deinit();
        var call = try call_builder.beginCall(call_question_id, 0x2222, 3);
        try call.setTargetImportedCap(bootstrap_export_id orelse return error.MissingBootstrapExport);
        var cap_list = try call.initCapTableTyped(1);
        protocol.CapDescriptor.writeSenderHosted(try cap_list.get(0), client_sink_export_id);
        const call_frame = try call_builder.finish();
        defer allocator.free(call_frame);
        try self.server.pushFrame(call_frame);
    }

    fn deinit(self: *HostCallParamCapHarness) void {
        self.server.deinit();
    }

    const DrainedFrames = struct {
        return_count: usize = 0,
        release_count: usize = 0,
        released_id: ?u32 = null,
        released_reference_count: u32 = 0,
    };

    fn drainOutgoing(self: *HostCallParamCapHarness, allocator: std.mem.Allocator) !DrainedFrames {
        var drained = DrainedFrames{};
        while (self.server.popOutgoingFrame()) |frame| {
            defer self.server.freeFrame(frame);
            var decoded = try protocol.DecodedMessage.init(allocator, frame);
            defer decoded.deinit();
            switch (decoded.tag) {
                .@"return" => drained.return_count += 1,
                .release => {
                    const release = try decoded.asRelease();
                    drained.release_count += 1;
                    drained.released_id = release.id;
                    drained.released_reference_count = release.reference_count;
                },
                else => {},
            }
        }
        return drained;
    }

    fn buildResultsReturn(
        allocator: std.mem.Allocator,
        question_id: u32,
        release_param_caps: bool,
    ) ![]const u8 {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        var ret = try builder.beginReturn(question_id, .results);
        ret.setReleaseParamCaps(release_param_caps);
        var payload = try ret.payloadTyped();
        const content = try payload.initContent();
        try content.setText("done");
        _ = try ret.initCapTableTyped(0);
        return try builder.finish();
    }
};

test "host-call param caps stay alive until the host answers (releaseParamCaps=true releases them)" {
    const allocator = std.testing.allocator;

    var harness = HostCallParamCapHarness.init(allocator);
    defer harness.deinit();
    try harness.setup(allocator);

    // The call is queued for the host; its param cap must NOT be released
    // yet — the host may still be using it (or deciding to keep it).
    const before_respond = try harness.drainOutgoing(allocator);
    try std.testing.expectEqual(@as(usize, 0), before_respond.release_count);
    try std.testing.expect(harness.server.peer.caps.hasImport(HostCallParamCapHarness.client_sink_export_id));

    const call = harness.server.popHostCall() orelse return error.MissingHostCall;
    defer harness.server.freeHostCallFrame(call.frame);

    // Host answers without retaining (rpc.capnp default releaseParamCaps).
    const return_frame = try HostCallParamCapHarness.buildResultsReturn(allocator, call.question_id, true);
    defer allocator.free(return_frame);
    try harness.server.respondHostCallReturnFrame(return_frame);

    // The Return goes out first, then exactly one Release spending the
    // param-cap reference back to the client.
    const after_respond = try harness.drainOutgoing(allocator);
    try std.testing.expectEqual(@as(usize, 1), after_respond.return_count);
    try std.testing.expectEqual(@as(usize, 1), after_respond.release_count);
    try std.testing.expectEqual(
        @as(?u32, HostCallParamCapHarness.client_sink_export_id),
        after_respond.released_id,
    );
    try std.testing.expectEqual(@as(u32, 1), after_respond.released_reference_count);
    try std.testing.expect(!harness.server.peer.caps.hasImport(HostCallParamCapHarness.client_sink_export_id));
}

test "host-call Return with releaseParamCaps=false retains the param cap for the host" {
    const allocator = std.testing.allocator;

    var harness = HostCallParamCapHarness.init(allocator);
    defer harness.deinit();
    try harness.setup(allocator);

    const call = harness.server.popHostCall() orelse return error.MissingHostCall;
    defer harness.server.freeHostCallFrame(call.frame);

    // Host retains the param cap past the call (the OutputSink pump
    // pattern): the Return says releaseParamCaps=false.
    const return_frame = try HostCallParamCapHarness.buildResultsReturn(allocator, call.question_id, false);
    defer allocator.free(return_frame);
    try harness.server.respondHostCallReturnFrame(return_frame);

    // No Release may reach the client: the host now owns the reference. The
    // peer's own import bookkeeping is dropped (the host bypasses the peer
    // for its later calls and release).
    const after_respond = try harness.drainOutgoing(allocator);
    try std.testing.expectEqual(@as(usize, 1), after_respond.return_count);
    try std.testing.expectEqual(@as(usize, 0), after_respond.release_count);
    try std.testing.expect(!harness.server.peer.caps.hasImport(HostCallParamCapHarness.client_sink_export_id));

    // When the host finally drops the capability it sends the Release
    // through the lifecycle helper; the client-facing reference is spent
    // exactly once, AFTER the originating call returned.
    try harness.server.peer.sendReleaseForHost(HostCallParamCapHarness.client_sink_export_id, 1);
    const after_host_release = try harness.drainOutgoing(allocator);
    try std.testing.expectEqual(@as(usize, 1), after_host_release.release_count);
    try std.testing.expectEqual(
        @as(?u32, HostCallParamCapHarness.client_sink_export_id),
        after_host_release.released_id,
    );
}

test "host-call exception and legacy results paths release param caps at answer time" {
    const allocator = std.testing.allocator;

    // Exception path.
    {
        var harness = HostCallParamCapHarness.init(allocator);
        defer harness.deinit();
        try harness.setup(allocator);

        const call = harness.server.popHostCall() orelse return error.MissingHostCall;
        defer harness.server.freeHostCallFrame(call.frame);

        const before_respond = try harness.drainOutgoing(allocator);
        try std.testing.expectEqual(@as(usize, 0), before_respond.release_count);

        try harness.server.respondHostCallException(call.question_id, "boom");
        const after_respond = try harness.drainOutgoing(allocator);
        try std.testing.expectEqual(@as(usize, 1), after_respond.return_count);
        try std.testing.expectEqual(@as(usize, 1), after_respond.release_count);
        try std.testing.expectEqual(
            @as(?u32, HostCallParamCapHarness.client_sink_export_id),
            after_respond.released_id,
        );
    }

    // Legacy respondHostCallResults path (no retention control).
    {
        var harness = HostCallParamCapHarness.init(allocator);
        defer harness.deinit();
        try harness.setup(allocator);

        const call = harness.server.popHostCall() orelse return error.MissingHostCall;
        defer harness.server.freeHostCallFrame(call.frame);

        var payload_builder = capnpc.message.MessageBuilder.init(allocator);
        defer payload_builder.deinit();
        const root = try payload_builder.initRootAnyPointer();
        try root.setText("ok");
        const payload = try payload_builder.toBytes();
        defer allocator.free(payload);

        try harness.server.respondHostCallResults(call.question_id, payload);
        const after_respond = try harness.drainOutgoing(allocator);
        try std.testing.expectEqual(@as(usize, 1), after_respond.return_count);
        try std.testing.expectEqual(@as(usize, 1), after_respond.release_count);
        try std.testing.expectEqual(
            @as(?u32, HostCallParamCapHarness.client_sink_export_id),
            after_respond.released_id,
        );
    }
}

test "respondHostCallResults rejects deep pointer-chain payload" {
    const allocator = std.testing.allocator;

    // A chain of `depth` structs, each 0 data words + 1 pointer word pointing
    // to the next. depth > nesting_limit (default 64) trips NestingLimitExceeded.
    const depth: usize = 70;
    // Layout: word 0 root pointer, then `depth` structs each occupying 1 word
    // (their single pointer word). Struct k's pointer lives at word 1+k and
    // points to struct k+1 at word 2+k. The final struct's pointer is null.
    const total_words = 1 + depth;
    const words = try allocator.alloc(u64, total_words);
    defer allocator.free(words);
    @memset(words, 0);

    // Root pointer -> struct 0 at word 1 (offset 0), 0 data, 1 pointer.
    words[0] = structPtr(0, 0, 1);
    var k: usize = 0;
    while (k < depth) : (k += 1) {
        const this_word = 1 + k;
        if (k + 1 < depth) {
            // Point at the next struct's pointer word (word this_word + 1).
            // Offset from word `this_word` to word `this_word + 1` is 0.
            words[this_word] = structPtr(0, 0, 1);
        } else {
            words[this_word] = 0; // terminate the chain with a null pointer
        }
    }

    const payload = try framedSingleSegment(allocator, words);
    defer allocator.free(payload);

    try std.testing.expectError(error.NestingLimitExceeded, respondWithCraftedPayload(allocator, payload));
}

test "respondHostCallResults rejects an oversized payload frame" {
    const allocator = std.testing.allocator;

    // A frame larger than MAX_CAPTURED_FRAME_BYTES must be rejected before any
    // validation/clone work. Build a header claiming a huge segment; the guard
    // trips on payload_frame.len alone, so the (zeroed) body is never walked.
    const cap = 16 * 1024 * 1024; // HostPeer.MAX_CAPTURED_FRAME_BYTES
    const oversized = try allocator.alloc(u8, cap + 64);
    defer allocator.free(oversized);
    @memset(oversized, 0);

    try std.testing.expectError(error.FrameTooLarge, respondWithCraftedPayload(allocator, oversized));
}

test "respondHostCallResults still accepts a normal small valid payload" {
    const allocator = std.testing.allocator;

    var payload_builder = capnpc.message.MessageBuilder.init(allocator);
    defer payload_builder.deinit();
    const root = try payload_builder.initRootAnyPointer();
    try root.setText("ok");
    const payload = try payload_builder.toBytes();
    defer allocator.free(payload);

    try respondWithCraftedPayload(allocator, payload);
}

test "host peer host-call bridge auto-registers senderHosted exports in relayed returns" {
    const allocator = std.testing.allocator;

    // The host mints this id itself (it owns the export-id space in relay
    // mode); the peer has never seen an addExport for it.
    const host_minted_export_id: u32 = 41;

    const ClientCtx = struct {
        bootstrap_import_id: ?u32 = null,
        call_returned: bool = false,
        minted_import_id: ?u32 = null,
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

        fn onCallReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ClientCtx = @ptrCast(@alignCast(ctx));
            state.call_returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
            const payload = ret.results orelse return error.MissingPayload;
            const cap = try payload.content.getCapability();
            // Retain so the post-callback release pass does not send a
            // Release for the freshly minted import before we call it.
            var mutable_caps = caps.*;
            try mutable_caps.retainCapability(cap);
            const resolved = try caps.resolveCapability(cap);
            switch (resolved) {
                .imported => |imported| state.minted_import_id = imported.id,
                else => return error.UnexpectedResolvedCapability,
            }
        }

        fn onSecondCallReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}

        fn buildEmptyCall(_: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            _ = try call.initCapTableTyped(0);
        }
    };

    var client = HostPeer.init(allocator);
    defer client.deinit();
    client.start(null, null, null);

    var server = HostPeer.init(allocator);
    defer server.deinit();
    server.start(null, null, null);
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

    // The host answers with a Return whose results carry a capability the
    // peer has never exported: senderHosted with a host-minted id.
    var return_builder = protocol.MessageBuilder.init(allocator);
    defer return_builder.deinit();
    var ret = try return_builder.beginReturn(call.question_id, .results);
    var payload = try ret.payloadTyped();
    const content = try payload.initContent();
    try content.setCapability(.{ .id = 0 });
    var cap_list = try ret.initCapTableTyped(1);
    protocol.CapDescriptor.writeSenderHosted(try cap_list.get(0), host_minted_export_id);
    const return_frame = try return_builder.finish();
    defer allocator.free(return_frame);

    // Before the fix this failed with error.UnknownExport from the outbound
    // cap-ref accounting in sendPrebuiltReturnFrame.
    try server.respondHostCallReturnFrame(return_frame);
    try pumpAll(&server, &client);

    try std.testing.expect(client_ctx.call_returned);
    try std.testing.expectEqual(
        @as(?u32, host_minted_export_id),
        client_ctx.minted_import_id,
    );

    // Calling the minted capability must route back to the host-call queue,
    // proving the auto-registered export got the host-bridge handler.
    _ = try client.peer.sendCallResolved(
        .{ .imported = .{ .id = host_minted_export_id } },
        0x3333,
        5,
        &client_ctx,
        Handlers.buildEmptyCall,
        Handlers.onSecondCallReturn,
    );
    try pumpAll(&client, &server);
    try std.testing.expectEqual(@as(usize, 1), server.pendingHostCallCount());

    const second = server.popHostCall() orelse return error.MissingSecondHostCall;
    defer server.freeHostCallFrame(second.frame);
    try std.testing.expectEqual(@as(u64, 0x3333), second.interface_id);
    try std.testing.expectEqual(@as(u16, 5), second.method_id);

    try server.respondHostCallException(second.question_id, "drained");
    try pumpAll(&server, &client);
}
