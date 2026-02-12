const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.cap_table;
const payload_remap = capnpc.rpc._internal.payload_remap;
const peer_embargo_accepts = capnpc.rpc._internal.peer_embargo_accepts;
const Connection = capnpc.rpc.connection.Connection;
const Peer = peer_impl.Peer;
const peer_test_hooks = Peer.test_hooks;
const ForwardCallContext = peer_test_hooks.ForwardCallContextType;

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

test "peer initDetached starts without attached transport" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    peer.start(null, null);
    try std.testing.expect(!peer.hasAttachedTransport());
}

test "peer detached sendFrame requires override or attached transport" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    try std.testing.expectError(error.TransportNotAttached, peer_test_hooks.sendFrame(&peer, &[_]u8{ 0x01, 0x02 }));
}

test "peer on_error callback fires and null callback is safe" {
    const Ctx = struct {
        called: usize = 0,
        last_error: ?anyerror = null,
    };
    const Hooks = struct {
        var ctx_ptr: ?*Ctx = null;

        fn onError(_: *Peer, err: anyerror) void {
            const ctx = ctx_ptr orelse unreachable;
            ctx.called += 1;
            ctx.last_error = err;
        }
    };

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    var ctx = Ctx{};
    Hooks.ctx_ptr = &ctx;
    defer Hooks.ctx_ptr = null;
    peer.setSendFrameOverride(&ctx, struct {
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
    }.send);
    peer.start(Hooks.onError, null);

    peer_test_hooks.onConnectionError(&peer, error.ConnectionResetByPeer);
    try std.testing.expectEqual(@as(usize, 1), ctx.called);
    try std.testing.expectEqual(error.ConnectionResetByPeer, ctx.last_error.?);

    peer.start(null, null);
    peer_test_hooks.onConnectionError(&peer, error.ConnectionResetByPeer);
    try std.testing.expectEqual(@as(usize, 1), ctx.called);
}

test "peer shutdown callback and transport close fire when questions drain" {
    const State = struct {
        const Self = @This();

        close_calls: usize = 0,
        shutdown_calls: usize = 0,
        transport_closing: bool = false,

        fn start(_: *anyopaque, _: *Peer) void {}
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
        fn close(ctx: *anyopaque) void {
            const state: *Self = castCtx(*Self, ctx);
            state.close_calls += 1;
            state.transport_closing = true;
        }
        fn isClosing(ctx: *anyopaque) bool {
            const state: *Self = castCtx(*Self, ctx);
            return state.transport_closing;
        }
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
        fn onShutdown(peer: *Peer) void {
            const state: *Self = castCtx(*Self, peer.transport_ctx.?);
            state.shutdown_calls += 1;
        }
    };

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    var state = State{};
    peer.attachTransport(&state, State.start, State.send, State.close, State.isClosing);

    var callback_ctx: u8 = 0;
    const question_id = try peer.sendBootstrap(&callback_ctx, State.onReturn);
    try std.testing.expect(peer.questions.contains(question_id));

    peer.shutdown(State.onShutdown);
    try std.testing.expectEqual(@as(usize, 0), state.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 0), state.close_calls);

    peer_test_hooks.removeQuestion(&peer, question_id);
    try std.testing.expectEqual(@as(usize, 1), state.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 1), state.close_calls);

    peer.shutdown(State.onShutdown);
    try std.testing.expectEqual(@as(usize, 1), state.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 1), state.close_calls);
}

test "peer shutdown callback fires immediately with no outstanding questions" {
    const State = struct {
        const Self = @This();

        close_calls: usize = 0,
        shutdown_calls: usize = 0,
        transport_closing: bool = false,

        fn start(_: *anyopaque, _: *Peer) void {}
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
        fn close(ctx: *anyopaque) void {
            const state: *Self = castCtx(*Self, ctx);
            state.close_calls += 1;
            state.transport_closing = true;
        }
        fn isClosing(ctx: *anyopaque) bool {
            const state: *Self = castCtx(*Self, ctx);
            return state.transport_closing;
        }
        fn onShutdown(peer: *Peer) void {
            const state: *Self = castCtx(*Self, peer.transport_ctx.?);
            state.shutdown_calls += 1;
        }
    };

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    var state = State{};
    peer.attachTransport(&state, State.start, State.send, State.close, State.isClosing);

    peer.shutdown(State.onShutdown);
    try std.testing.expectEqual(@as(usize, 1), state.shutdown_calls);
    try std.testing.expectEqual(@as(usize, 1), state.close_calls);
}

test "peer question allocation probes past occupied ID across wrap-around" {
    const allocator = std.testing.allocator;
    const Noop = struct {
        fn sendFrame(_: *anyopaque, _: []const u8) anyerror!void {}
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var blocked_ctx: u8 = 0;
    var bootstrap_ctx: u8 = 0;

    peer.next_question_id = std.math.maxInt(u32);
    try peer.questions.put(peer.next_question_id, .{
        .ctx = &blocked_ctx,
        .on_return = Noop.onReturn,
        .is_loopback = false,
    });

    peer.setSendFrameOverride(&bootstrap_ctx, Noop.sendFrame);
    const question_id = try peer.sendBootstrap(&bootstrap_ctx, Noop.onReturn);

    try std.testing.expectEqual(@as(u32, 0), question_id);
    try std.testing.expect(peer.questions.contains(std.math.maxInt(u32)));
    try std.testing.expect(peer.questions.contains(@as(u32, 0)));
}

test "release batching aggregates per import id" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    try peer.caps.noteImport(5);
    try peer.caps.noteImport(5);
    try peer.caps.noteImport(7);
    try peer.caps.noteImport(9);

    var inbound = cap_table.InboundCapTable{
        .allocator = allocator,
        .entries = try allocator.alloc(cap_table.ResolvedCap, 4),
        .retained = try allocator.alloc(bool, 4),
    };
    defer inbound.deinit();

    inbound.entries[0] = .{ .imported = .{ .id = 5 } };
    inbound.entries[1] = .{ .imported = .{ .id = 5 } };
    inbound.entries[2] = .{ .imported = .{ .id = 7 } };
    inbound.entries[3] = .{ .imported = .{ .id = 9 } };
    @memset(inbound.retained, false);
    inbound.retained[3] = true;

    var releases = try peer_test_hooks.collectReleaseCounts(&peer, &inbound);
    defer releases.deinit();

    try std.testing.expectEqual(@as(usize, 2), releases.count());
    try std.testing.expectEqual(@as(u32, 2), releases.get(5).?);
    try std.testing.expectEqual(@as(u32, 1), releases.get(7).?);
    try std.testing.expectEqual(@as(usize, 1), peer.caps.imports.count());
    try std.testing.expect(peer.caps.imports.contains(9));
}

test "sendCall rolls back outbound cap effects when send fails" {
    const allocator = std.testing.allocator;

    const NoopHandler = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    const Ctx = struct {
        export_id: u32,
        receiver_answer_id: u32,
    };
    const Hooks = struct {
        fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            const ctx: *Ctx = castCtx(*Ctx, ctx_ptr);
            var payload = try call.payloadTyped();
            const any = try payload.initContent();
            var caps = try any.initPointerList(2);
            try caps.setCapability(0, .{ .id = ctx.export_id });
            try caps.setCapability(1, .{ .id = ctx.receiver_answer_id });
        }

        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}

        fn failSend(_: *anyopaque, _: []const u8) anyerror!void {
            return error.TestSendFailed;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    const empty_ops = [_]protocol.PromisedAnswerOp{};
    const receiver_answer_id = try peer.caps.noteReceiverAnswerOps(77, empty_ops[0..]);
    try std.testing.expect(peer.caps.receiver_answers.contains(receiver_answer_id));

    var send_ctx: u8 = 0;
    peer.setSendFrameOverride(&send_ctx, Hooks.failSend);

    var build_ctx = Ctx{
        .export_id = export_id,
        .receiver_answer_id = receiver_answer_id,
    };
    try std.testing.expectError(
        error.TestSendFailed,
        peer.sendCall(1, 0xAAAABBBB, 3, &build_ctx, Hooks.build, Hooks.onReturn),
    );

    const export_entry = peer.exports.getEntry(export_id) orelse return error.UnknownExport;
    try std.testing.expectEqual(@as(u32, 0), export_entry.value_ptr.ref_count);
    try std.testing.expect(peer.caps.receiver_answers.contains(receiver_answer_id));
    try std.testing.expectEqual(@as(usize, 0), peer.questions.count());
}

test "sendReturnResults rolls back outbound cap effects when send fails" {
    const allocator = std.testing.allocator;

    const NoopHandler = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    const Ctx = struct {
        export_id: u32,
        receiver_answer_id: u32,
    };
    const Hooks = struct {
        fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            const ctx: *Ctx = castCtx(*Ctx, ctx_ptr);
            var payload = try ret.payloadTyped();
            const any = try payload.initContent();
            var caps = try any.initPointerList(2);
            try caps.setCapability(0, .{ .id = ctx.export_id });
            try caps.setCapability(1, .{ .id = ctx.receiver_answer_id });
        }

        fn failSend(_: *anyopaque, _: []const u8) anyerror!void {
            return error.TestSendFailed;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    const empty_ops = [_]protocol.PromisedAnswerOp{};
    const receiver_answer_id = try peer.caps.noteReceiverAnswerOps(88, empty_ops[0..]);
    try std.testing.expect(peer.caps.receiver_answers.contains(receiver_answer_id));

    var send_ctx: u8 = 0;
    peer.setSendFrameOverride(&send_ctx, Hooks.failSend);

    var build_ctx = Ctx{
        .export_id = export_id,
        .receiver_answer_id = receiver_answer_id,
    };
    try std.testing.expectError(
        error.TestSendFailed,
        peer.sendReturnResults(1234, &build_ctx, Hooks.build),
    );

    const export_entry = peer.exports.getEntry(export_id) orelse return error.UnknownExport;
    try std.testing.expectEqual(@as(u32, 0), export_entry.value_ptr.ref_count);
    try std.testing.expect(peer.caps.receiver_answers.contains(receiver_answer_id));
    try std.testing.expect(!peer.resolved_answers.contains(1234));
}

test "sendPrebuiltReturnFrame rolls back outbound refs when send fails" {
    const allocator = std.testing.allocator;

    const NoopHandler = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    const Hooks = struct {
        fn failSend(_: *anyopaque, _: []const u8) anyerror!void {
            return error.TestSendFailed;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var ret_builder = protocol.MessageBuilder.init(allocator);
    defer ret_builder.deinit();
    var ret = try ret_builder.beginReturn(333, .results);
    var payload = try ret.payloadTyped();
    const any = try payload.initContent();
    try any.setCapability(.{ .id = 0 });
    var cap_list = try ret.initCapTableTyped(1);
    var cap_entry = try cap_list.get(0);
    try cap_entry.setSenderHosted(export_id);

    const frame = try ret_builder.finish();
    defer allocator.free(frame);
    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    const parsed_ret = try decoded.asReturn();

    var send_ctx: u8 = 0;
    peer.setSendFrameOverride(&send_ctx, Hooks.failSend);

    try std.testing.expectError(
        error.TestSendFailed,
        peer.sendPrebuiltReturnFrame(parsed_ret, frame),
    );

    const export_entry = peer.exports.getEntry(export_id) orelse return error.UnknownExport;
    try std.testing.expectEqual(@as(u32, 0), export_entry.value_ptr.ref_count);
}

test "sendCallResolved routes exported target through local loopback" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
    };
    const ClientCtx = struct {
        returned: bool = false,
    };
    const Handlers = struct {
        fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const server: *ServerCtx = castCtx(*ServerCtx, ctx);
            server.called = true;
            try peer.sendReturnException(call.question_id, "loopback");
        }

        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const client: *ClientCtx = castCtx(*ClientCtx, ctx);
            client.returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("loopback", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    var client_ctx = ClientCtx{};
    _ = try peer.sendCallResolved(
        .{ .exported = .{ .id = export_id } },
        0x99,
        0,
        &client_ctx,
        null,
        Handlers.onReturn,
    );

    try std.testing.expect(server_ctx.called);
    try std.testing.expect(client_ctx.returned);
}

test "forwarded payload remaps capability index to local id" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var inbound = cap_table.InboundCapTable{
        .allocator = allocator,
        .entries = try allocator.alloc(cap_table.ResolvedCap, 1),
        .retained = try allocator.alloc(bool, 1),
    };
    defer inbound.deinit();
    inbound.entries[0] = .{ .imported = .{ .id = 42 } };
    inbound.retained[0] = false;

    var src_builder = protocol.MessageBuilder.init(allocator);
    defer src_builder.deinit();
    var src_call = try src_builder.beginCall(1, 0x01, 0x02);
    try src_call.setTargetImportedCap(0);
    const src_payload_typed = try src_call.payloadTyped();

    var src_payload = src_payload_typed._builder;

    var src_any = try src_payload.getAnyPointer(protocol.PAYLOAD_CONTENT_PTR);
    try src_any.setCapability(.{ .id = 0 });

    const src_bytes = try src_builder.finish();
    defer allocator.free(src_bytes);
    var src_decoded = try protocol.DecodedMessage.init(allocator, src_bytes);
    defer src_decoded.deinit();
    const parsed_src_call = try src_decoded.asCall();

    var dst_builder = protocol.MessageBuilder.init(allocator);
    defer dst_builder.deinit();
    var dst_call = try dst_builder.beginCall(7, 0x03, 0x04);
    try dst_call.setTargetImportedCap(0);
    const dst_payload_typed = try dst_call.payloadTyped();

    const dst_payload = dst_payload_typed._builder;

    try peer_test_hooks.clonePayloadWithRemappedCaps(
        &peer,
        dst_call.call.builder,
        dst_payload,
        parsed_src_call.params,
        &inbound,
    );

    const dst_bytes = try dst_builder.finish();
    defer allocator.free(dst_bytes);
    var dst_decoded = try protocol.DecodedMessage.init(allocator, dst_bytes);
    defer dst_decoded.deinit();
    const parsed_dst_call = try dst_decoded.asCall();
    const cap = try parsed_dst_call.params.content.getCapability();
    try std.testing.expectEqual(@as(u32, 42), cap.id);
}

test "forwarded payload converts none capability to null pointer" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var inbound = cap_table.InboundCapTable{
        .allocator = allocator,
        .entries = try allocator.alloc(cap_table.ResolvedCap, 1),
        .retained = try allocator.alloc(bool, 1),
    };
    defer inbound.deinit();
    inbound.entries[0] = .none;
    inbound.retained[0] = false;

    var src_builder = protocol.MessageBuilder.init(allocator);
    defer src_builder.deinit();
    var src_call = try src_builder.beginCall(1, 0x01, 0x02);
    try src_call.setTargetImportedCap(0);
    const src_payload_typed = try src_call.payloadTyped();

    var src_payload = src_payload_typed._builder;

    var src_any = try src_payload.getAnyPointer(protocol.PAYLOAD_CONTENT_PTR);
    try src_any.setCapability(.{ .id = 0 });

    const src_bytes = try src_builder.finish();
    defer allocator.free(src_bytes);
    var src_decoded = try protocol.DecodedMessage.init(allocator, src_bytes);
    defer src_decoded.deinit();
    const parsed_src_call = try src_decoded.asCall();

    var dst_builder = protocol.MessageBuilder.init(allocator);
    defer dst_builder.deinit();
    var dst_call = try dst_builder.beginCall(7, 0x03, 0x04);
    try dst_call.setTargetImportedCap(0);
    const dst_payload_typed = try dst_call.payloadTyped();

    const dst_payload = dst_payload_typed._builder;

    try peer_test_hooks.clonePayloadWithRemappedCaps(
        &peer,
        dst_call.call.builder,
        dst_payload,
        parsed_src_call.params,
        &inbound,
    );

    const dst_bytes = try dst_builder.finish();
    defer allocator.free(dst_bytes);
    var dst_decoded = try protocol.DecodedMessage.init(allocator, dst_bytes);
    defer dst_decoded.deinit();
    const parsed_dst_call = try dst_decoded.asCall();
    try std.testing.expect(parsed_dst_call.params.content.isNull());
}

test "forwarded payload encodes promised capability descriptors as receiverAnswer" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var inbound = cap_table.InboundCapTable{
        .allocator = allocator,
        .entries = try allocator.alloc(cap_table.ResolvedCap, 1),
        .retained = try allocator.alloc(bool, 1),
    };
    defer inbound.deinit();
    inbound.entries[0] = .{
        .promised = .{
            .question_id = 9,
            .transform = .{ .list = null },
        },
    };
    inbound.retained[0] = false;

    var src_builder = protocol.MessageBuilder.init(allocator);
    defer src_builder.deinit();
    var src_call = try src_builder.beginCall(1, 0x01, 0x02);
    try src_call.setTargetImportedCap(0);
    const src_payload_typed = try src_call.payloadTyped();

    var src_payload = src_payload_typed._builder;

    var src_any = try src_payload.getAnyPointer(protocol.PAYLOAD_CONTENT_PTR);
    try src_any.setCapability(.{ .id = 0 });

    const src_bytes = try src_builder.finish();
    defer allocator.free(src_bytes);
    var src_decoded = try protocol.DecodedMessage.init(allocator, src_bytes);
    defer src_decoded.deinit();
    const parsed_src_call = try src_decoded.asCall();

    var dst_builder = protocol.MessageBuilder.init(allocator);
    defer dst_builder.deinit();
    var dst_call = try dst_builder.beginCall(7, 0x03, 0x04);
    try dst_call.setTargetImportedCap(0);
    const dst_payload_typed = try dst_call.payloadTyped();

    const dst_payload = dst_payload_typed._builder;

    try peer_test_hooks.clonePayloadWithRemappedCaps(
        &peer,
        dst_call.call.builder,
        dst_payload,
        parsed_src_call.params,
        &inbound,
    );
    try cap_table.encodeCallPayloadCaps(&peer.caps, &dst_call, null, null, null);

    const dst_bytes = try dst_builder.finish();
    defer allocator.free(dst_bytes);
    var dst_decoded = try protocol.DecodedMessage.init(allocator, dst_bytes);
    defer dst_decoded.deinit();
    const parsed_dst_call = try dst_decoded.asCall();
    const cap = try parsed_dst_call.params.content.getCapability();
    try std.testing.expectEqual(@as(u32, 0), cap.id);

    const cap_table_reader = parsed_dst_call.params.cap_table orelse return error.MissingCapTable;
    const desc = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(0));
    try std.testing.expectEqual(protocol.CapDescriptorTag.receiverAnswer, desc.tag);
    const promised = desc.promised_answer orelse return error.MissingPromisedAnswer;
    try std.testing.expectEqual(@as(u32, 9), promised.question_id);
    try std.testing.expectEqual(@as(u32, 0), promised.transform.len());
    try std.testing.expectEqual(@as(usize, 0), peer.caps.receiver_answers.count());
}

test "forwarded return passes through canceled tag" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.canceled, ret.tag);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const upstream_answer_id: u32 = 55;
    const local_forwarded_question_id: u32 = 99;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});
    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = try cap_table.InboundCapTable.init(allocator, null, &peer.caps),
        .send_results_to = .caller,
        .answer_id = upstream_answer_id,
        .mode = .translate_to_caller,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .canceled,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try peer_test_hooks.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return translates takeFromOtherQuestion id" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
        referenced_answer: u32 = 0,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, ret.tag);
            state.referenced_answer = ret.take_from_other_question orelse return error.MissingQuestionId;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const upstream_answer_id: u32 = 100;
    const local_forwarded_question_id: u32 = 200;
    const local_referenced_question_id: u32 = 201;
    const translated_upstream_answer_id: u32 = 77;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});

    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);
    try peer.forwarded_questions.put(local_referenced_question_id, translated_upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = try cap_table.InboundCapTable.init(allocator, null, &peer.caps),
        .send_results_to = .caller,
        .answer_id = upstream_answer_id,
        .mode = .translate_to_caller,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .takeFromOtherQuestion,
        .results = null,
        .exception = null,
        .take_from_other_question = local_referenced_question_id,
    };
    try peer_test_hooks.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expectEqual(translated_upstream_answer_id, callback_ctx.referenced_answer);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return converts resultsSentElsewhere to exception" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("forwarded resultsSentElsewhere unsupported", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const upstream_answer_id: u32 = 300;
    const local_forwarded_question_id: u32 = 301;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});
    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = try cap_table.InboundCapTable.init(allocator, null, &peer.caps),
        .send_results_to = .caller,
        .answer_id = upstream_answer_id,
        .mode = .translate_to_caller,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .resultsSentElsewhere,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try peer_test_hooks.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return translate mode missing payload sends exception" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("forwarded return missing payload", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const upstream_answer_id: u32 = 350;
    const local_forwarded_question_id: u32 = 351;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});
    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = try cap_table.InboundCapTable.init(allocator, null, &peer.caps),
        .send_results_to = .caller,
        .answer_id = upstream_answer_id,
        .mode = .translate_to_caller,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .results,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try peer_test_hooks.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return propagate-results mode rejects takeFromOtherQuestion" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("forwarded takeFromOtherQuestion unsupported", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const upstream_answer_id: u32 = 352;
    const local_forwarded_question_id: u32 = 353;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});
    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = try cap_table.InboundCapTable.init(allocator, null, &peer.caps),
        .send_results_to = .yourself,
        .answer_id = upstream_answer_id,
        .mode = .propagate_results_sent_elsewhere,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .takeFromOtherQuestion,
        .results = null,
        .exception = null,
        .take_from_other_question = 900,
    };
    try peer_test_hooks.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return forwards awaitFromThirdParty to caller" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const upstream_answer_id: u32 = 400;
    const local_forwarded_question_id: u32 = 401;

    // Register the upstream question as non-loopback so the return goes to the wire (capture).
    try peer.questions.put(upstream_answer_id, .{
        .ctx = undefined,
        .on_return = undefined,
        .is_loopback = false,
    });
    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);

    // Build an AnyPointerReader with third-party payload data.
    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("await-destination");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);
    var third_msg = try message.Message.init(allocator, third_bytes);
    defer third_msg.deinit();
    const third_ptr = try third_msg.getRootAnyPointer();

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = try cap_table.InboundCapTable.init(allocator, null, &peer.caps),
        .send_results_to = .caller,
        .answer_id = upstream_answer_id,
        .mode = .translate_to_caller,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .awaitFromThirdParty,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
        .accept_from_third_party = third_ptr,
    };
    try peer_test_hooks.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    // Verify the forwarded return was sent as accept_from_third_party with the payload.
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    const forwarded_ret = try decoded.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.awaitFromThirdParty, forwarded_ret.tag);
    try std.testing.expect(forwarded_ret.exception == null);
    const await_ptr = forwarded_ret.accept_from_third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("await-destination", try await_ptr.getText());
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return sentElsewhere mode accepts resultsSentElsewhere without upstream return" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = ret;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const upstream_answer_id: u32 = 500;
    const local_forwarded_question_id: u32 = 501;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});
    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = try cap_table.InboundCapTable.init(allocator, null, &peer.caps),
        .send_results_to = .yourself,
        .answer_id = upstream_answer_id,
        .mode = .sent_elsewhere,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .resultsSentElsewhere,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try peer_test_hooks.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(!callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return sentElsewhere mode rejects unexpected result payload" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const local_forwarded_question_id: u32 = 601;
    try peer.forwarded_questions.put(local_forwarded_question_id, 600);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = try cap_table.InboundCapTable.init(allocator, null, &peer.caps),
        .send_results_to = .yourself,
        .answer_id = 600,
        .mode = .sent_elsewhere,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .results,
        .results = protocol.Payload{
            .content = undefined,
            .cap_table = null,
        },
        .exception = null,
        .take_from_other_question = null,
    };

    try std.testing.expectError(error.UnexpectedForwardedTailReturn, peer_test_hooks.onForwardedReturn(forward_ctx, &peer, ret, &inbound));
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "handleResolvedCall forwards sendResultsTo.yourself when forwarding imported target" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.resultsSentElsewhere, ret.tag);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(700, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(700, {});

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(700, 0x10, 1);
    try call.setTargetImportedCap(77);
    call.setSendResultsToYourself();
    _ = try call.initCapTableTyped(0);

    const bytes = try call_builder.finish();
    defer allocator.free(bytes);
    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const parsed = try decoded.asCall();

    try peer_test_hooks.handleResolvedCall(&peer, parsed, &inbound, .{ .imported = .{ .id = 77 } });
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var forwarded_call_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer forwarded_call_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.call, forwarded_call_msg.tag);
    const forwarded_call = try forwarded_call_msg.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.yourself, forwarded_call.send_results_to.tag);
    const forwarded_question_id = forwarded_call.question_id;

    var ret_builder = protocol.MessageBuilder.init(allocator);
    defer ret_builder.deinit();
    _ = try ret_builder.beginReturn(forwarded_question_id, .resultsSentElsewhere);
    const ret_frame = try ret_builder.finish();
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));
}

test "handleResolvedCall forwards sendResultsTo.thirdParty when forwarding promised target" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Register the upstream question as non-loopback so the forwarded return
    // goes to the wire (capture) instead of through the third-party adoption path.
    try peer.questions.put(800, .{
        .ctx = undefined,
        .on_return = undefined,
        .is_loopback = false,
    });

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("third-party-destination");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);
    var third_msg = try message.Message.init(allocator, third_bytes);
    defer third_msg.deinit();
    const third_ptr = try third_msg.getRootAnyPointer();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(800, 0x10, 1);
    try call.setTargetImportedCap(77);
    try call.setSendResultsToThirdParty(third_ptr);
    _ = try call.initCapTableTyped(0);

    const bytes = try call_builder.finish();
    defer allocator.free(bytes);
    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const parsed = try decoded.asCall();

    try peer_test_hooks.handleResolvedCall(&peer, parsed, &inbound, .{
        .promised = .{
            .question_id = 1,
            .transform = .{ .list = null },
        },
    });
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var forwarded_call_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer forwarded_call_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.call, forwarded_call_msg.tag);
    const forwarded_call = try forwarded_call_msg.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.thirdParty, forwarded_call.send_results_to.tag);
    const forwarded_third_party = forwarded_call.send_results_to.third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("third-party-destination", try forwarded_third_party.getText());
    const forwarded_question_id = forwarded_call.question_id;

    // Send a results_sent_elsewhere return to the forwarded question.
    // In propagate_accept_from_third_party mode, this triggers sending an
    // accept_from_third_party return to the upstream caller with the captured payload.
    var ret_builder = protocol.MessageBuilder.init(allocator);
    defer ret_builder.deinit();
    _ = try ret_builder.beginReturn(forwarded_question_id, .resultsSentElsewhere);
    const ret_frame = try ret_builder.finish();
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    // Verify the accept_from_third_party return was sent to the upstream caller.
    // Frames: [0] forwarded call, [1] accept_from_third_party return, [2] auto-finish.
    try std.testing.expectEqual(@as(usize, 3), capture.frames.items.len);
    var ret_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer ret_decoded.deinit();
    const forwarded_ret = try ret_decoded.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.awaitFromThirdParty, forwarded_ret.tag);
    try std.testing.expect(forwarded_ret.exception == null);
    const await_ptr = forwarded_ret.accept_from_third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("third-party-destination", try await_ptr.getText());
    try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));
}

test "handleCall supports sendResultsTo.yourself for local export target" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
    };
    const ClientCtx = struct {
        returned: bool = false,
    };
    const Handlers = struct {
        fn buildCall(ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            _ = ctx;
            call.setSendResultsToYourself();
        }

        fn buildResults(ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            _ = ctx;
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            _ = try any.initStruct(0, 0);
            _ = try ret.initCapTableTyped(0);
        }

        fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const server: *ServerCtx = castCtx(*ServerCtx, ctx);
            server.called = true;
            try peer.sendReturnResults(call.question_id, server, buildResults);
        }

        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const client: *ClientCtx = castCtx(*ClientCtx, ctx);
            client.returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.resultsSentElsewhere, ret.tag);
            try std.testing.expect(ret.results == null);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    var client_ctx = ClientCtx{};
    client_ctx.returned = false;
    _ = try peer.sendCallResolved(
        .{ .exported = .{ .id = export_id } },
        0x99,
        0,
        &client_ctx,
        Handlers.buildCall,
        Handlers.onReturn,
    );

    try std.testing.expect(client_ctx.returned);
    try std.testing.expect(server_ctx.called);
}

test "handleCall supports sendResultsTo.thirdParty for local export target" {
    const allocator = std.testing.allocator;

    // When a loopback call uses sendResultsTo.thirdParty the production code
    // converts the results to an accept_from_third_party return.  That return
    // then enters the third-party adoption path: the callback is NOT invoked
    // immediately but instead a pending await is stored.  This test verifies
    // that the server handler fires and that the pending third-party state is
    // correctly established.

    const ServerCtx = struct {
        called: bool = false,
    };
    const ClientBuildCtx = struct {
        destination: message.AnyPointerReader,
    };
    const Handlers = struct {
        fn buildCall(ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            const cb: *const ClientBuildCtx = castCtx(*const ClientBuildCtx, ctx);
            try call.setSendResultsToThirdParty(cb.destination);
        }

        fn buildResults(ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            _ = ctx;
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            _ = try any.initStruct(0, 0);
            _ = try ret.initCapTableTyped(0);
        }

        fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const server: *ServerCtx = castCtx(*ServerCtx, ctx);
            server.called = true;
            try peer.sendReturnResults(call.question_id, server, buildResults);
        }

        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            // Should not be called synchronously in the thirdParty flow.
            return error.UnexpectedCallback;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("local-third-party");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);
    var third_msg = try message.Message.init(allocator, third_bytes);
    defer third_msg.deinit();

    var client_build_ctx = ClientBuildCtx{
        .destination = try third_msg.getRootAnyPointer(),
    };
    _ = try peer.sendCallResolved(
        .{ .exported = .{ .id = export_id } },
        0x99,
        0,
        &client_build_ctx,
        Handlers.buildCall,
        Handlers.onReturn,
    );

    // The server handler should have fired.
    try std.testing.expect(server_ctx.called);
    // The accept_from_third_party return entered the third-party adoption
    // path so a pending await should have been stored.
    try std.testing.expectEqual(@as(usize, 1), peer.pending_third_party_awaits.count());
}

test "handleReturn adopts thirdPartyAnswer when await arrives first" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
        answer_id: u32 = 0,
        reason: []const u8 = "",
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            state.answer_id = ret.answer_id;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            state.reason = ex.reason;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const original_answer_id: u32 = 1100;
    const adopted_answer_id: u32 = 0x4000_0011;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(original_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = false,
    });

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("await-first-completion");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_msg = try message.Message.init(allocator, completion_bytes);
    defer completion_msg.deinit();
    const completion_ptr = try completion_msg.getRootAnyPointer();

    var await_builder = protocol.MessageBuilder.init(allocator);
    defer await_builder.deinit();
    var await_ret = try await_builder.beginReturn(original_answer_id, .awaitFromThirdParty);
    try await_ret.setAcceptFromThirdParty(completion_ptr);
    const await_frame = try await_builder.finish();
    defer allocator.free(await_frame);
    try peer.handleFrame(await_frame);

    try std.testing.expect(!callback_ctx.seen);
    try std.testing.expectEqual(@as(usize, 1), peer.pending_third_party_awaits.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var third_party_answer_builder = protocol.MessageBuilder.init(allocator);
    defer third_party_answer_builder.deinit();
    try third_party_answer_builder.buildThirdPartyAnswer(adopted_answer_id, completion_ptr);
    const third_party_answer_frame = try third_party_answer_builder.finish();
    defer allocator.free(third_party_answer_frame);
    try peer.handleFrame(third_party_answer_frame);

    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
    try std.testing.expect(peer.questions.contains(adopted_answer_id));

    var final_builder = protocol.MessageBuilder.init(allocator);
    defer final_builder.deinit();
    var final_ret = try final_builder.beginReturn(adopted_answer_id, .exception);
    try final_ret.setException("done-through-third-party");
    const final_frame = try final_builder.finish();
    defer allocator.free(final_frame);
    try peer.handleFrame(final_frame);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expectEqual(original_answer_id, callback_ctx.answer_id);
    try std.testing.expectEqualStrings("done-through-third-party", callback_ctx.reason);
    try std.testing.expect(!peer.questions.contains(adopted_answer_id));
    try std.testing.expectEqual(@as(usize, 0), peer.adopted_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var finish0 = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer finish0.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, finish0.tag);
    const finish0_body = try finish0.asFinish();
    try std.testing.expectEqual(original_answer_id, finish0_body.question_id);

    var finish1 = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer finish1.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, finish1.tag);
    const finish1_body = try finish1.asFinish();
    try std.testing.expectEqual(adopted_answer_id, finish1_body.question_id);
}

test "handleReturn replays buffered thirdPartyAnswer return when await arrives later" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
        answer_id: u32 = 0,
        // Validate the reason string inside the callback while the frame is
        // still alive.  Storing a slice to the reason would point into freed
        // memory after the replayed frame is released by the production code.
        reason_ok: bool = false,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            state.answer_id = ret.answer_id;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            std.testing.expectEqualStrings("replayed-from-buffer", ex.reason) catch {
                state.reason_ok = false;
                return;
            };
            state.reason_ok = true;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const original_answer_id: u32 = 1200;
    const adopted_answer_id: u32 = 0x4000_0012;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(original_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = false,
    });

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("answer-first-completion");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_msg = try message.Message.init(allocator, completion_bytes);
    defer completion_msg.deinit();
    const completion_ptr = try completion_msg.getRootAnyPointer();

    var third_party_answer_builder = protocol.MessageBuilder.init(allocator);
    defer third_party_answer_builder.deinit();
    try third_party_answer_builder.buildThirdPartyAnswer(adopted_answer_id, completion_ptr);
    const third_party_answer_frame = try third_party_answer_builder.finish();
    defer allocator.free(third_party_answer_frame);
    try peer.handleFrame(third_party_answer_frame);

    try std.testing.expectEqual(@as(usize, 1), peer.pending_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());

    var early_ret_builder = protocol.MessageBuilder.init(allocator);
    defer early_ret_builder.deinit();
    var early_ret = try early_ret_builder.beginReturn(adopted_answer_id, .exception);
    try early_ret.setException("replayed-from-buffer");
    const early_ret_frame = try early_ret_builder.finish();
    defer allocator.free(early_ret_frame);
    try peer.handleFrame(early_ret_frame);

    try std.testing.expect(!callback_ctx.seen);
    try std.testing.expectEqual(@as(usize, 1), peer.pending_third_party_returns.count());

    var await_builder = protocol.MessageBuilder.init(allocator);
    defer await_builder.deinit();
    var await_ret = try await_builder.beginReturn(original_answer_id, .awaitFromThirdParty);
    try await_ret.setAcceptFromThirdParty(completion_ptr);
    const await_frame = try await_builder.finish();
    defer allocator.free(await_frame);
    try peer.handleFrame(await_frame);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expectEqual(original_answer_id, callback_ctx.answer_id);
    try std.testing.expect(callback_ctx.reason_ok);
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_returns.count());
    try std.testing.expectEqual(@as(usize, 0), peer.adopted_third_party_answers.count());
    try std.testing.expect(!peer.questions.contains(adopted_answer_id));
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var finish0 = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer finish0.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, finish0.tag);
    const finish0_body = try finish0.asFinish();
    try std.testing.expectEqual(adopted_answer_id, finish0_body.question_id);

    var finish1 = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer finish1.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, finish1.tag);
    const finish1_body = try finish1.asFinish();
    try std.testing.expectEqual(original_answer_id, finish1_body.question_id);
}

test "thirdPartyAnswer stress race keeps pending state empty" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: usize = 0,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen += 1;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("stress-third-party", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("stress-completion");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_msg = try message.Message.init(allocator, completion_bytes);
    defer completion_msg.deinit();
    const completion_ptr = try completion_msg.getRootAnyPointer();

    var callback_ctx = CallbackCtx{};
    const rounds: u32 = 96;
    var round: u32 = 0;
    while (round < rounds) : (round += 1) {
        const original_answer_id: u32 = 1400 + round;
        const adopted_answer_id: u32 = 0x4000_1000 + round;
        try peer.questions.put(original_answer_id, .{
            .ctx = &callback_ctx,
            .on_return = Handlers.onReturn,
            .is_loopback = false,
        });

        if ((round % 2) == 0) {
            var await_builder = protocol.MessageBuilder.init(allocator);
            defer await_builder.deinit();
            var await_ret = try await_builder.beginReturn(original_answer_id, .awaitFromThirdParty);
            try await_ret.setAcceptFromThirdParty(completion_ptr);
            const await_frame = try await_builder.finish();
            defer allocator.free(await_frame);
            try peer.handleFrame(await_frame);

            var third_party_answer_builder = protocol.MessageBuilder.init(allocator);
            defer third_party_answer_builder.deinit();
            try third_party_answer_builder.buildThirdPartyAnswer(adopted_answer_id, completion_ptr);
            const third_party_answer_frame = try third_party_answer_builder.finish();
            defer allocator.free(third_party_answer_frame);
            try peer.handleFrame(third_party_answer_frame);

            var final_builder = protocol.MessageBuilder.init(allocator);
            defer final_builder.deinit();
            var final_ret = try final_builder.beginReturn(adopted_answer_id, .exception);
            try final_ret.setException("stress-third-party");
            const final_frame = try final_builder.finish();
            defer allocator.free(final_frame);
            try peer.handleFrame(final_frame);
        } else {
            var third_party_answer_builder = protocol.MessageBuilder.init(allocator);
            defer third_party_answer_builder.deinit();
            try third_party_answer_builder.buildThirdPartyAnswer(adopted_answer_id, completion_ptr);
            const third_party_answer_frame = try third_party_answer_builder.finish();
            defer allocator.free(third_party_answer_frame);
            try peer.handleFrame(third_party_answer_frame);

            var early_builder = protocol.MessageBuilder.init(allocator);
            defer early_builder.deinit();
            var early_ret = try early_builder.beginReturn(adopted_answer_id, .exception);
            try early_ret.setException("stress-third-party");
            const early_frame = try early_builder.finish();
            defer allocator.free(early_frame);
            try peer.handleFrame(early_frame);

            var await_builder = protocol.MessageBuilder.init(allocator);
            defer await_builder.deinit();
            var await_ret = try await_builder.beginReturn(original_answer_id, .awaitFromThirdParty);
            try await_ret.setAcceptFromThirdParty(completion_ptr);
            const await_frame = try await_builder.finish();
            defer allocator.free(await_frame);
            try peer.handleFrame(await_frame);
        }

        try std.testing.expectEqual(@as(usize, @intCast(round + 1)), callback_ctx.seen);
        try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
        try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
        try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_returns.count());
        try std.testing.expectEqual(@as(usize, 0), peer.adopted_third_party_answers.count());
        try std.testing.expect(!peer.questions.contains(adopted_answer_id));
    }

    try std.testing.expectEqual(@as(usize, rounds), callback_ctx.seen);
    try std.testing.expectEqual(@as(usize, rounds * 2), capture.frames.items.len);
}

test "peer deinit releases pending embargo and promised-call queues under load" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(ctx: *anyopaque, called_peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    {
        var peer = Peer.initDetached(allocator);
        defer peer.deinit();

        var handler_state: u8 = 0;
        const export_id = try peer.addExport(.{
            .ctx = &handler_state,
            .on_call = Handlers.onCall,
        });

        var recipient_builder = message.MessageBuilder.init(allocator);
        defer recipient_builder.deinit();
        const recipient_root = try recipient_builder.initRootAnyPointer();
        try recipient_root.setText("deinit-pending-recipient");
        const recipient_bytes = try recipient_builder.toBytes();
        defer allocator.free(recipient_bytes);
        var recipient_msg = try message.Message.init(allocator, recipient_bytes);
        defer recipient_msg.deinit();
        const recipient_ptr = try recipient_msg.getRootAnyPointer();

        var provide_builder = protocol.MessageBuilder.init(allocator);
        defer provide_builder.deinit();
        try provide_builder.buildProvide(
            6000,
            .{
                .tag = .importedCap,
                .imported_cap = export_id,
                .promised_answer = null,
            },
            recipient_ptr,
        );
        const provide_frame = try provide_builder.finish();
        defer allocator.free(provide_frame);
        try peer.handleFrame(provide_frame);

        const rounds: u32 = 80;
        var round: u32 = 0;
        while (round < rounds) : (round += 1) {
            const accept_qid: u32 = 6100 + (round * 2);
            const call_qid: u32 = accept_qid + 1;

            var accept_builder = protocol.MessageBuilder.init(allocator);
            defer accept_builder.deinit();
            try accept_builder.buildAccept(accept_qid, recipient_ptr, "deinit-embargo");
            const accept_frame = try accept_builder.finish();
            defer allocator.free(accept_frame);
            try peer.handleFrame(accept_frame);

            var call_builder = protocol.MessageBuilder.init(allocator);
            defer call_builder.deinit();
            var call = try call_builder.beginCall(call_qid, 0xA1, 0);
            try call.setTargetPromisedAnswer(accept_qid);
            _ = try call.initCapTableTyped(0);

            const call_frame = try call_builder.finish();
            defer allocator.free(call_frame);
            try peer.handleFrame(call_frame);
        }

        try std.testing.expectEqual(rounds, @as(u32, @intCast(peer.pending_promises.count())));
        try std.testing.expectEqual(@as(usize, 1), peer.pending_accepts_by_embargo.count());
        try std.testing.expectEqual(rounds, @as(u32, @intCast(peer.pending_accept_embargo_by_question.count())));
        try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
        try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
    }
}

test "handleFinish forwards mapped tail finish question id" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }

    peer.setSendFrameOverride(&capture, Capture.onFrame);
    try peer.forwarded_tail_questions.put(10, 20);

    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = 10,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });

    try std.testing.expect(!peer.forwarded_tail_questions.contains(10));
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, decoded.tag);
    const finish = try decoded.asFinish();
    try std.testing.expectEqual(@as(u32, 20), finish.question_id);
    try std.testing.expect(!finish.release_result_caps);
}

test "handleFinish without tail mapping does not send finish" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        count: usize = 0,

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            _ = frame;
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            ctx.count += 1;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{};
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = 1234,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });

    try std.testing.expectEqual(@as(usize, 0), capture.count);
}

test "handleFinish cancels queued promised call when early-cancel workaround is disabled" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(100, 0xAA55, 1);
    try call.setTargetPromisedAnswer(77);
    _ = try call.initCapTableTyped(0);

    const frame = try call_builder.finish();
    defer allocator.free(frame);
    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    const parsed = try decoded.asCall();

    try peer_test_hooks.handleCall(&peer, frame, parsed);
    const pending_before = peer.pending_promises.getPtr(77) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 1), pending_before.items.len);

    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = 100,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });

    const pending_after = peer.pending_promises.getPtr(77);
    if (pending_after) |list| {
        try std.testing.expectEqual(@as(usize, 0), list.items.len);
    }
}

test "handleFinish keeps queued promised call when early-cancel workaround is enabled" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(101, 0xAA55, 1);
    try call.setTargetPromisedAnswer(77);
    _ = try call.initCapTableTyped(0);

    const frame = try call_builder.finish();
    defer allocator.free(frame);
    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    const parsed = try decoded.asCall();

    try peer_test_hooks.handleCall(&peer, frame, parsed);
    const pending_before = peer.pending_promises.getPtr(77) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 1), pending_before.items.len);

    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = 101,
        .release_result_caps = false,
        .require_early_cancellation = true,
    });

    const pending_after = peer.pending_promises.getPtr(77) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 1), pending_after.items.len);
}

test "forwarded caller tail call emits yourself call, takeFromOtherQuestion, and propagated finish" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }

    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const upstream_question_id: u32 = 900;
    const interface_id: u64 = 0x01020304;
    const method_id: u16 = 7;
    const target_import_id: u32 = 77;

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(upstream_question_id, interface_id, method_id);
    try call.setTargetImportedCap(999);
    call.setSendResultsToCaller();
    _ = try call.initCapTableTyped(0);

    const call_bytes = try call_builder.finish();
    defer allocator.free(call_bytes);
    var decoded_call = try protocol.DecodedMessage.init(allocator, call_bytes);
    defer decoded_call.deinit();
    const parsed = try decoded_call.asCall();

    try peer_test_hooks.handleResolvedCall(&peer, parsed, &inbound, .{ .imported = .{ .id = target_import_id } });

    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var out_call_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_call_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.call, out_call_decoded.tag);
    const forwarded_call = try out_call_decoded.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.yourself, forwarded_call.send_results_to.tag);
    try std.testing.expectEqual(protocol.MessageTargetTag.importedCap, forwarded_call.target.tag);
    try std.testing.expectEqual(target_import_id, forwarded_call.target.imported_cap.?);
    try std.testing.expectEqual(interface_id, forwarded_call.interface_id);
    try std.testing.expectEqual(method_id, forwarded_call.method_id);
    const forwarded_question_id = forwarded_call.question_id;

    var out_ret_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer out_ret_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", out_ret_decoded.tag);
    const tail_ret = try out_ret_decoded.asReturn();
    try std.testing.expectEqual(upstream_question_id, tail_ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, tail_ret.tag);
    try std.testing.expectEqual(forwarded_question_id, tail_ret.take_from_other_question.?);

    try std.testing.expectEqual(upstream_question_id, peer.forwarded_questions.get(forwarded_question_id).?);
    try std.testing.expectEqual(forwarded_question_id, peer.forwarded_tail_questions.get(upstream_question_id).?);
    const question_entry = peer.questions.getEntry(forwarded_question_id) orelse return error.UnknownQuestion;
    try std.testing.expect(question_entry.value_ptr.suppress_auto_finish);

    var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
    defer fwd_ret_builder.deinit();
    _ = try fwd_ret_builder.beginReturn(forwarded_question_id, .resultsSentElsewhere);
    const fwd_ret_frame = try fwd_ret_builder.finish();
    defer allocator.free(fwd_ret_frame);
    try peer.handleFrame(fwd_ret_frame);

    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);
    try std.testing.expect(!peer.questions.contains(forwarded_question_id));

    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = upstream_question_id,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });

    try std.testing.expect(!peer.forwarded_tail_questions.contains(upstream_question_id));
    try std.testing.expectEqual(@as(usize, 3), capture.frames.items.len);
    var out_finish_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[2]);
    defer out_finish_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, out_finish_decoded.tag);
    const forwarded_finish = try out_finish_decoded.asFinish();
    try std.testing.expectEqual(forwarded_question_id, forwarded_finish.question_id);
}

test "forwarded tail finish before forwarded return still emits single finish and drains state" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }

    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const upstream_question_id: u32 = 1000;
    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(upstream_question_id, 0x44, 3);
    try call.setTargetImportedCap(111);
    call.setSendResultsToCaller();
    _ = try call.initCapTableTyped(0);

    const call_bytes = try call_builder.finish();
    defer allocator.free(call_bytes);
    var decoded_call = try protocol.DecodedMessage.init(allocator, call_bytes);
    defer decoded_call.deinit();
    const parsed = try decoded_call.asCall();

    try peer_test_hooks.handleResolvedCall(&peer, parsed, &inbound, .{ .imported = .{ .id = 222 } });
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var out_call_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_call_decoded.deinit();
    const forwarded_call = try out_call_decoded.asCall();
    const forwarded_question_id = forwarded_call.question_id;

    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = upstream_question_id,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });

    try std.testing.expectEqual(@as(usize, 3), capture.frames.items.len);
    try std.testing.expect(!peer.forwarded_tail_questions.contains(upstream_question_id));

    var out_finish_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[2]);
    defer out_finish_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, out_finish_decoded.tag);
    const forwarded_finish = try out_finish_decoded.asFinish();
    try std.testing.expectEqual(forwarded_question_id, forwarded_finish.question_id);

    var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
    defer fwd_ret_builder.deinit();
    _ = try fwd_ret_builder.beginReturn(forwarded_question_id, .resultsSentElsewhere);
    const fwd_ret_frame = try fwd_ret_builder.finish();
    defer allocator.free(fwd_ret_frame);
    try peer.handleFrame(fwd_ret_frame);

    try std.testing.expectEqual(@as(usize, 3), capture.frames.items.len);
    try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));
    try std.testing.expect(!peer.questions.contains(forwarded_question_id));
}

test "forwarded tail cleanup stays stable under repeated finish/return ordering races" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const rounds: u32 = 64;
    var round: u32 = 0;
    while (round < rounds) : (round += 1) {
        const frame_start = capture.frames.items.len;
        const upstream_question_id: u32 = 2000 + round;

        var call_builder = protocol.MessageBuilder.init(allocator);
        defer call_builder.deinit();
        var call = try call_builder.beginCall(upstream_question_id, 0x44, 3);
        try call.setTargetImportedCap(111);
        call.setSendResultsToCaller();
        _ = try call.initCapTableTyped(0);

        const call_bytes = try call_builder.finish();
        defer allocator.free(call_bytes);
        var decoded_call = try protocol.DecodedMessage.init(allocator, call_bytes);
        defer decoded_call.deinit();
        const parsed = try decoded_call.asCall();

        try peer_test_hooks.handleResolvedCall(&peer, parsed, &inbound, .{ .imported = .{ .id = 222 } });
        try std.testing.expectEqual(frame_start + 2, capture.frames.items.len);

        var out_call_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start]);
        defer out_call_decoded.deinit();
        const forwarded_call = try out_call_decoded.asCall();
        const forwarded_question_id = forwarded_call.question_id;

        var out_ret_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start + 1]);
        defer out_ret_decoded.deinit();
        const tail_ret = try out_ret_decoded.asReturn();
        try std.testing.expectEqual(upstream_question_id, tail_ret.answer_id);
        try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, tail_ret.tag);
        try std.testing.expectEqual(forwarded_question_id, tail_ret.take_from_other_question.?);

        if ((round & 1) == 0) {
            var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
            defer fwd_ret_builder.deinit();
            _ = try fwd_ret_builder.beginReturn(forwarded_question_id, .resultsSentElsewhere);
            const fwd_ret_frame = try fwd_ret_builder.finish();
            defer allocator.free(fwd_ret_frame);
            try peer.handleFrame(fwd_ret_frame);

            try std.testing.expectEqual(frame_start + 2, capture.frames.items.len);
            try std.testing.expect(!peer.questions.contains(forwarded_question_id));

            try peer_test_hooks.handleFinish(&peer, .{
                .question_id = upstream_question_id,
                .release_result_caps = false,
                .require_early_cancellation = false,
            });
        } else {
            try peer_test_hooks.handleFinish(&peer, .{
                .question_id = upstream_question_id,
                .release_result_caps = false,
                .require_early_cancellation = false,
            });

            var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
            defer fwd_ret_builder.deinit();
            _ = try fwd_ret_builder.beginReturn(forwarded_question_id, .resultsSentElsewhere);
            const fwd_ret_frame = try fwd_ret_builder.finish();
            defer allocator.free(fwd_ret_frame);
            try peer.handleFrame(fwd_ret_frame);
        }

        try std.testing.expectEqual(frame_start + 3, capture.frames.items.len);
        try std.testing.expect(!peer.forwarded_tail_questions.contains(upstream_question_id));
        try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));
        try std.testing.expect(!peer.questions.contains(forwarded_question_id));

        var finish_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start + 2]);
        defer finish_decoded.deinit();
        try std.testing.expectEqual(protocol.MessageTag.finish, finish_decoded.tag);
        const finish = try finish_decoded.asFinish();
        try std.testing.expectEqual(forwarded_question_id, finish.question_id);
    }
}

test "promisedAnswer target queues when resolved cap is unresolved promise export and replays on resolve" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
        question_id: u32 = 0,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const ctx: *ServerCtx = castCtx(*ServerCtx, ctx_ptr);
            ctx.called = true;
            ctx.question_id = call.question_id;
            try peer.sendReturnException(call.question_id, "resolved");
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var server_ctx = ServerCtx{};
    const concrete_export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });
    const promise_export_id = try peer.addPromiseExport();

    const promised_answer_id: u32 = 300;
    {
        var ret_builder = protocol.MessageBuilder.init(allocator);
        defer ret_builder.deinit();
        var ret = try ret_builder.beginReturn(promised_answer_id, .results);
        var any_payload = try ret.payloadTyped();
        var any = try any_payload.initContent();

        try any.setCapability(.{ .id = 0 });
        var cap_list = try ret.initCapTableTyped(1);

        const entry = try cap_list.get(0);
        protocol.CapDescriptor.writeSenderPromise(entry, promise_export_id);

        const frame = try ret_builder.finish();
        defer allocator.free(frame);
        const stored = try allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, stored, frame);
        try peer.resolved_answers.put(promised_answer_id, .{ .frame = stored });
    }

    const queued_question_id: u32 = 301;
    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(queued_question_id, 0xABCD, 2);
    try call.setTargetPromisedAnswer(promised_answer_id);
    _ = try call.initCapTableTyped(0);

    const frame = try call_builder.finish();
    defer allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    const parsed = try decoded.asCall();

    try peer_test_hooks.handleCall(&peer, frame, parsed);
    try std.testing.expect(!server_ctx.called);
    try std.testing.expect(peer.pending_export_promises.contains(promise_export_id));
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    try peer.resolvePromiseExportToExport(promise_export_id, concrete_export_id);

    try std.testing.expect(server_ctx.called);
    try std.testing.expectEqual(queued_question_id, server_ctx.question_id);
    try std.testing.expect(!peer.pending_export_promises.contains(promise_export_id));
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var resolve_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer resolve_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.resolve, resolve_msg.tag);
    const resolve = try resolve_msg.asResolve();
    try std.testing.expectEqual(promise_export_id, resolve.promise_id);
    try std.testing.expectEqual(protocol.ResolveTag.cap, resolve.tag);
    const cap = resolve.cap orelse return error.MissingResolveCap;
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, cap.tag);
    try std.testing.expectEqual(concrete_export_id, cap.id.?);

    var ret_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer ret_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", ret_msg.tag);
    const ret = try ret_msg.asReturn();
    try std.testing.expectEqual(queued_question_id, ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("resolved", ex.reason);
}

test "bootstrap return is recorded for promisedAnswer pipelined calls" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
        question_id: u32 = 0,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const ctx: *ServerCtx = castCtx(*ServerCtx, ctx_ptr);
            ctx.called = true;
            ctx.question_id = call.question_id;
            try peer.sendReturnException(call.question_id, "ok");
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var server_ctx = ServerCtx{};
    _ = try peer.setBootstrap(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    const bootstrap_question_id: u32 = 41;
    {
        var bootstrap_builder = protocol.MessageBuilder.init(allocator);
        defer bootstrap_builder.deinit();
        try bootstrap_builder.buildBootstrap(bootstrap_question_id);

        const bootstrap_frame = try bootstrap_builder.finish();
        defer allocator.free(bootstrap_frame);
        try peer.handleFrame(bootstrap_frame);
    }
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    const pipelined_question_id: u32 = 42;
    {
        var call_builder = protocol.MessageBuilder.init(allocator);
        defer call_builder.deinit();
        var call = try call_builder.beginCall(pipelined_question_id, 0xABCD, 7);
        try call.setTargetPromisedAnswer(bootstrap_question_id);
        _ = try call.initCapTableTyped(0);

        const call_frame = try call_builder.finish();
        defer allocator.free(call_frame);
        try peer.handleFrame(call_frame);
    }

    try std.testing.expect(server_ctx.called);
    try std.testing.expectEqual(pipelined_question_id, server_ctx.question_id);
    try std.testing.expect(!peer.pending_promises.contains(bootstrap_question_id));
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var ret_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer ret_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", ret_msg.tag);
    const ret = try ret_msg.asReturn();
    try std.testing.expectEqual(pipelined_question_id, ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
}

test "bootstrap promisedAnswer call still resolves after bootstrap export release" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const ctx: *ServerCtx = castCtx(*ServerCtx, ctx_ptr);
            ctx.called = true;
            try peer.sendReturnException(call.question_id, "ok");
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var server_ctx = ServerCtx{};
    const bootstrap_export_id = try peer.setBootstrap(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    const bootstrap_question_id: u32 = 101;
    {
        var bootstrap_builder = protocol.MessageBuilder.init(allocator);
        defer bootstrap_builder.deinit();
        try bootstrap_builder.buildBootstrap(bootstrap_question_id);

        const bootstrap_frame = try bootstrap_builder.finish();
        defer allocator.free(bootstrap_frame);
        try peer.handleFrame(bootstrap_frame);
    }

    {
        var release_builder = protocol.MessageBuilder.init(allocator);
        defer release_builder.deinit();
        try release_builder.buildRelease(bootstrap_export_id, 1);
        const release_frame = try release_builder.finish();
        defer allocator.free(release_frame);
        try peer.handleFrame(release_frame);
    }

    const pipelined_question_id: u32 = 102;
    {
        var call_builder = protocol.MessageBuilder.init(allocator);
        defer call_builder.deinit();
        var call = try call_builder.beginCall(pipelined_question_id, 0xCCDD, 7);
        try call.setTargetPromisedAnswer(bootstrap_question_id);
        _ = try call.initCapTableTyped(0);

        const call_frame = try call_builder.finish();
        defer allocator.free(call_frame);
        try peer.handleFrame(call_frame);
    }

    try std.testing.expect(server_ctx.called);
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var ret_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer ret_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", ret_msg.tag);
    const ret = try ret_msg.asReturn();
    try std.testing.expectEqual(pipelined_question_id, ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("ok", ex.reason);
}

test "handleFrame unimplemented call converts outstanding question to exception" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("unimplemented", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const question_id: u32 = 420;
    var callback_ctx = CallbackCtx{};
    try peer.questions.put(question_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });

    var inner_builder = protocol.MessageBuilder.init(allocator);
    defer inner_builder.deinit();
    var inner_call = try inner_builder.beginCall(question_id, 0x44, 3);
    try inner_call.setTargetImportedCap(1);
    _ = try inner_call.initCapTableTyped(0);

    const inner_bytes = try inner_builder.finish();
    defer allocator.free(inner_bytes);

    var inner_msg = try message.Message.init(allocator, inner_bytes);
    defer inner_msg.deinit();
    const inner_root = try inner_msg.getRootAnyPointer();

    var outer_builder = protocol.MessageBuilder.init(allocator);
    defer outer_builder.deinit();
    try outer_builder.buildUnimplementedFromAnyPointer(inner_root);
    const outer_bytes = try outer_builder.finish();
    defer allocator.free(outer_bytes);

    try peer.handleFrame(outer_bytes);
    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.questions.contains(question_id));
}

test "handleFrame abort returns remote abort error" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildAbort("fatal");
    const frame = try builder.finish();
    defer allocator.free(frame);

    try std.testing.expectError(error.RemoteAbort, peer.handleFrame(frame));
}

test "handleFrame provide stores provision without immediate return" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("vat-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildProvide(
        900,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const in_frame = try in_builder.finish();
    defer allocator.free(in_frame);

    try peer.handleFrame(in_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);
    try std.testing.expect(peer.provides_by_question.contains(900));
    try std.testing.expectEqual(@as(usize, 1), peer.provides_by_key.count());
}

test "handleFrame duplicate provide recipient sends abort" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("same-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildProvide(
        901,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const first_frame = try in_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);

    var duplicate_builder = protocol.MessageBuilder.init(allocator);
    defer duplicate_builder.deinit();
    try duplicate_builder.buildProvide(
        902,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const duplicate_frame = try duplicate_builder.finish();
    defer allocator.free(duplicate_frame);

    try std.testing.expectError(error.DuplicateProvideRecipient, peer.handleFrame(duplicate_frame));
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, out_decoded.tag);
    const abort = try out_decoded.asAbort();
    try std.testing.expectEqualStrings("duplicate provide recipient", abort.exception.reason);
}

test "handleFrame accept returns provided capability" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("accept-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildProvide(
        902,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const provide_frame = try in_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(903, recipient_ptr, null);
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);

    try peer.handleFrame(accept_frame);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", out_decoded.tag);
    const ret = try out_decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 903), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, descriptor.tag);
    try std.testing.expectEqual(export_id, descriptor.id.?);
}

test "handleFrame accept unknown provision returns exception" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildAccept(904, null, null);
    const in_frame = try in_builder.finish();
    defer allocator.free(in_frame);

    try peer.handleFrame(in_frame);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", out_decoded.tag);
    const ret = try out_decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 904), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("unknown provision", ex.reason);
}

test "handleFrame finish clears stored provide entry" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("finish-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        905,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    var finish_builder = protocol.MessageBuilder.init(allocator);
    defer finish_builder.deinit();
    try finish_builder.buildFinish(905, false, false);
    const finish_frame = try finish_builder.finish();
    defer allocator.free(finish_frame);
    try peer.handleFrame(finish_frame);

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(906, recipient_ptr, null);
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);
    try peer.handleFrame(accept_frame);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", out_decoded.tag);
    const ret = try out_decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 906), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("unknown provision", ex.reason);
}

test "handleFrame join returns capability" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var key_part_builder = message.MessageBuilder.init(allocator);
    defer key_part_builder.deinit();
    const key_part_root = try key_part_builder.initRootAnyPointer();
    var key_part_struct = try key_part_root.initStruct(1, 0);
    key_part_struct.writeU32(0, 0xA1);
    key_part_struct.writeU16(4, 1);
    key_part_struct.writeU16(6, 0);
    const key_part_bytes = try key_part_builder.toBytes();
    defer allocator.free(key_part_bytes);
    var key_part_msg = try message.Message.init(allocator, key_part_bytes);
    defer key_part_msg.deinit();
    const key_part_ptr = try key_part_msg.getRootAnyPointer();

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildJoin(
        907,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        key_part_ptr,
    );
    const in_frame = try in_builder.finish();
    defer allocator.free(in_frame);

    try peer.handleFrame(in_frame);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", out_decoded.tag);
    const ret = try out_decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 907), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderHosted, descriptor.tag);
    try std.testing.expectEqual(export_id, descriptor.id.?);
}

test "handleFrame join returns exceptions when targets mismatch across parts" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_a: u8 = 0;
    const export_a = try peer.addExport(.{
        .ctx = &handler_a,
        .on_call = Handlers.onCall,
    });
    var handler_b: u8 = 0;
    const export_b = try peer.addExport(.{
        .ctx = &handler_b,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var key0_builder = message.MessageBuilder.init(allocator);
    defer key0_builder.deinit();
    const key0_root = try key0_builder.initRootAnyPointer();
    var key0_struct = try key0_root.initStruct(1, 0);
    key0_struct.writeU32(0, 0xC3);
    key0_struct.writeU16(4, 2);
    key0_struct.writeU16(6, 0);
    const key0_bytes = try key0_builder.toBytes();
    defer allocator.free(key0_bytes);
    var key0_msg = try message.Message.init(allocator, key0_bytes);
    defer key0_msg.deinit();
    const key0_ptr = try key0_msg.getRootAnyPointer();

    var join0_builder = protocol.MessageBuilder.init(allocator);
    defer join0_builder.deinit();
    try join0_builder.buildJoin(
        920,
        .{
            .tag = .importedCap,
            .imported_cap = export_a,
            .promised_answer = null,
        },
        key0_ptr,
    );
    const join0_frame = try join0_builder.finish();
    defer allocator.free(join0_frame);
    try peer.handleFrame(join0_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var key1_builder = message.MessageBuilder.init(allocator);
    defer key1_builder.deinit();
    const key1_root = try key1_builder.initRootAnyPointer();
    var key1_struct = try key1_root.initStruct(1, 0);
    key1_struct.writeU32(0, 0xC3);
    key1_struct.writeU16(4, 2);
    key1_struct.writeU16(6, 1);
    const key1_bytes = try key1_builder.toBytes();
    defer allocator.free(key1_bytes);
    var key1_msg = try message.Message.init(allocator, key1_bytes);
    defer key1_msg.deinit();
    const key1_ptr = try key1_msg.getRootAnyPointer();

    var join1_builder = protocol.MessageBuilder.init(allocator);
    defer join1_builder.deinit();
    try join1_builder.buildJoin(
        921,
        .{
            .tag = .importedCap,
            .imported_cap = export_b,
            .promised_answer = null,
        },
        key1_ptr,
    );
    const join1_frame = try join1_builder.finish();
    defer allocator.free(join1_frame);
    try peer.handleFrame(join1_frame);
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var seen_920 = false;
    var seen_921 = false;
    for (capture.frames.items) |out_frame| {
        var out_decoded = try protocol.DecodedMessage.init(allocator, out_frame);
        defer out_decoded.deinit();
        try std.testing.expectEqual(protocol.MessageTag.@"return", out_decoded.tag);
        const ret = try out_decoded.asReturn();
        try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
        const ex = ret.exception orelse return error.MissingException;
        try std.testing.expectEqualStrings("join target mismatch", ex.reason);

        if (ret.answer_id == 920) {
            seen_920 = true;
        } else if (ret.answer_id == 921) {
            seen_921 = true;
        } else {
            return error.UnexpectedQuestionId;
        }
    }
    try std.testing.expect(seen_920);
    try std.testing.expect(seen_921);
}

test "handleFrame thirdPartyAnswer rejects missing completion" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildThirdPartyAnswer(0x4000_004D, null);
    const in_frame = try in_builder.finish();
    defer allocator.free(in_frame);

    try std.testing.expectError(error.MissingThirdPartyPayload, peer.handleFrame(in_frame));
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, out_decoded.tag);
    const abort = try out_decoded.asAbort();
    try std.testing.expectEqualStrings("thirdPartyAnswer missing completion", abort.exception.reason);
}

test "handleFrame unknown message tag sends unimplemented" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var in_builder = message.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    var root = try in_builder.allocateStruct(1, 1);
    root.writeUnionDiscriminant(0, 0xFFFF);
    const in_frame = try in_builder.toBytes();
    defer allocator.free(in_frame);

    try peer.handleFrame(in_frame);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.unimplemented, out_decoded.tag);
    const unimplemented = try out_decoded.asUnimplemented();
    try std.testing.expect(unimplemented.message_tag == null);
    try std.testing.expect(unimplemented.question_id == null);
}

fn queuePromisedCallOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(100, 0xAA55, 1);
    try call.setTargetPromisedAnswer(77);
    _ = try call.initCapTableTyped(0);

    const frame = try call_builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);
    try std.testing.expect(peer.pending_promises.contains(77));
}

test "peer queuePromisedCall path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, queuePromisedCallOomImpl, .{});
}

fn queuePromiseExportCallOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const promise_export_id = try peer.addPromiseExport();
    const promised_answer_id: u32 = 300;

    {
        var ret_builder = protocol.MessageBuilder.init(allocator);
        defer ret_builder.deinit();
        var ret = try ret_builder.beginReturn(promised_answer_id, .results);
        var any_payload = try ret.payloadTyped();
        var any = try any_payload.initContent();

        try any.setCapability(.{ .id = 0 });
        var cap_list = try ret.initCapTableTyped(1);

        const entry = try cap_list.get(0);
        protocol.CapDescriptor.writeSenderPromise(entry, promise_export_id);

        const frame = try ret_builder.finish();
        defer allocator.free(frame);
        const stored = try allocator.alloc(u8, frame.len);
        errdefer allocator.free(stored);
        std.mem.copyForwards(u8, stored, frame);
        try peer.resolved_answers.put(promised_answer_id, .{ .frame = stored });
    }

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(301, 0xABCD, 2);
    try call.setTargetPromisedAnswer(promised_answer_id);
    _ = try call.initCapTableTyped(0);

    const frame = try call_builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);
    try std.testing.expect(peer.pending_export_promises.contains(promise_export_id));
}

test "peer queuePromiseExportCall path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, queuePromiseExportCallOomImpl, .{});
}

fn embargoAcceptQueueOomImpl(allocator: std.mem.Allocator) !void {
    const NoopHandler = struct {
        fn onCall(
            _: *anyopaque,
            _: *Peer,
            _: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("oom-accept-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        910,
        .{
            .tag = .importedCap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(911, recipient_ptr, "oom-accept-embargo");
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);
    try peer.handleFrame(accept_frame);

    try std.testing.expectEqual(@as(usize, 1), peer.pending_accepts_by_embargo.count());
    try std.testing.expectEqual(@as(usize, 1), peer.pending_accept_embargo_by_question.count());
}

test "peer embargo accept queue path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, embargoAcceptQueueOomImpl, .{});
}

fn sendResultsToThirdPartyLocalExportOomImpl(allocator: std.mem.Allocator) !void {
    const NoopHandler = struct {
        fn onCall(
            _: *anyopaque,
            _: *Peer,
            _: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var destination_builder = message.MessageBuilder.init(allocator);
    defer destination_builder.deinit();
    const destination_root = try destination_builder.initRootAnyPointer();
    try destination_root.setText("oom-send-results-third-party");
    const destination_bytes = try destination_builder.toBytes();
    defer allocator.free(destination_bytes);
    var destination_msg = try message.Message.init(allocator, destination_bytes);
    defer destination_msg.deinit();
    const destination_ptr = try destination_msg.getRootAnyPointer();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(920, 0xBEEF, 9);
    try call.setTargetImportedCap(export_id);
    try call.setSendResultsToThirdParty(destination_ptr);
    _ = try call.initCapTableTyped(0);

    const call_frame = try call_builder.finish();
    defer allocator.free(call_frame);

    try peer.handleFrame(call_frame);
    try std.testing.expect(peer.send_results_to_third_party.contains(920));
}

test "peer local sendResultsTo.thirdParty path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        sendResultsToThirdPartyLocalExportOomImpl,
        .{},
    );
}

fn sendResultsToYourselfLocalExportOomImpl(allocator: std.mem.Allocator) !void {
    const NoopHandler = struct {
        fn onCall(
            _: *anyopaque,
            _: *Peer,
            _: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(921, 0xBEEF, 10);
    try call.setTargetImportedCap(export_id);
    call.setSendResultsToYourself();
    _ = try call.initCapTableTyped(0);

    const call_frame = try call_builder.finish();
    defer allocator.free(call_frame);

    try peer.handleFrame(call_frame);
    try std.testing.expect(peer.send_results_to_yourself.contains(921));
}

test "peer local sendResultsTo.yourself path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        sendResultsToYourselfLocalExportOomImpl,
        .{},
    );
}

fn bufferThirdPartyReturnOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const adopted_answer_id: u32 = 0x4000_0301;

    var ret_builder = protocol.MessageBuilder.init(allocator);
    defer ret_builder.deinit();
    var ret = try ret_builder.beginReturn(adopted_answer_id, .exception);
    try ret.setException("oom-buffer-third-party-return");
    const ret_frame = try ret_builder.finish();
    defer allocator.free(ret_frame);

    try peer.handleFrame(ret_frame);
    try std.testing.expect(peer.pending_third_party_returns.contains(adopted_answer_id));
}

test "peer buffer thirdParty return path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        bufferThirdPartyReturnOomImpl,
        .{},
    );
}

fn acceptFromThirdPartyAwaitQueueOomImpl(allocator: std.mem.Allocator) !void {
    const Callback = struct {
        fn onReturn(
            _: *anyopaque,
            _: *Peer,
            _: protocol.Return,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const original_answer_id: u32 = 930;
    var callback_ctx: u8 = 0;
    try peer.questions.put(original_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Callback.onReturn,
        .is_loopback = true,
    });

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("oom-await-queue");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_msg = try message.Message.init(allocator, completion_bytes);
    defer completion_msg.deinit();
    const completion_ptr = try completion_msg.getRootAnyPointer();

    var await_builder = protocol.MessageBuilder.init(allocator);
    defer await_builder.deinit();
    var await_ret = try await_builder.beginReturn(original_answer_id, .awaitFromThirdParty);
    try await_ret.setAcceptFromThirdParty(completion_ptr);
    const await_frame = try await_builder.finish();
    defer allocator.free(await_frame);

    try peer.handleFrame(await_frame);
    try std.testing.expectEqual(@as(usize, 1), peer.pending_third_party_awaits.count());
    try std.testing.expect(!peer.questions.contains(original_answer_id));
}

test "peer awaitFromThirdParty queue path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        acceptFromThirdPartyAwaitQueueOomImpl,
        .{},
    );
}

fn forwardResolvedCallThirdPartyContextOomImpl(allocator: std.mem.Allocator) !void {
    const Sink = struct {
        fn onFrame(_: *anyopaque, _: []const u8) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink_ctx: u8 = 0;
    peer.setSendFrameOverride(&sink_ctx, Sink.onFrame);

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("oom-forward-context-third-party");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);
    var third_msg = try message.Message.init(allocator, third_bytes);
    defer third_msg.deinit();
    const third_ptr = try third_msg.getRootAnyPointer();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(940, 0xCAFE, 1);
    try call.setTargetImportedCap(77);
    try call.setSendResultsToThirdParty(third_ptr);
    _ = try call.initCapTableTyped(0);

    const call_frame = try call_builder.finish();
    defer allocator.free(call_frame);

    var call_decoded = try protocol.DecodedMessage.init(allocator, call_frame);
    defer call_decoded.deinit();
    const parsed_call = try call_decoded.asCall();

    try peer_test_hooks.handleResolvedCall(&peer, parsed_call, &inbound, .{ .imported = .{ .id = 77 } });

    const forwarded_question_id = blk: {
        var it = peer.forwarded_questions.iterator();
        const entry = it.next() orelse return error.UnknownQuestion;
        break :blk entry.key_ptr.*;
    };
    const question = peer.questions.get(forwarded_question_id) orelse return error.UnknownQuestion;
    const fwd_ctx: *const ForwardCallContext = @ptrCast(@alignCast(question.ctx));
    try std.testing.expectEqual(protocol.SendResultsToTag.thirdParty, fwd_ctx.send_results_to);
    try std.testing.expect(fwd_ctx.send_results_to_third_party_payload != null);

    var ret_builder = protocol.MessageBuilder.init(allocator);
    defer ret_builder.deinit();
    _ = try ret_builder.beginReturn(forwarded_question_id, .resultsSentElsewhere);
    const ret_frame = try ret_builder.finish();
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);
}

test "peer forwardResolvedCall third-party context path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        forwardResolvedCallThirdPartyContextOomImpl,
        .{},
    );
}
