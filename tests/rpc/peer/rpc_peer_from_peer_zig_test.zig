const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const payload_remap = capnpc.rpc.testing.payload_remap;
const peer_embargo_accepts = capnpc.rpc.testing.peer_embargo_accepts;
const vat_network = capnpc.rpc.vat.network;
const Connection = capnpc.rpc.transport.tcp.Connection;
const Peer = peer_impl.Peer;
const peer_test_hooks = Peer.test_hooks;
const ForwardCallContext = peer_test_hooks.ForwardCallContextType;

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

/// Wrap a finished RPC frame in an Unimplemented message, exactly as a peer
/// that does not understand the frame's tag would echo it back. Caller frees
/// the returned frame.
fn buildUnimplementedEcho(allocator: std.mem.Allocator, inner_bytes: []const u8) ![]const u8 {
    var inner_msg = try message.Message.init(allocator, inner_bytes, .{});
    defer inner_msg.deinit();
    const inner_root = try inner_msg.getRootAnyPointer();
    var outer_builder = protocol.MessageBuilder.init(allocator);
    defer outer_builder.deinit();
    try outer_builder.buildUnimplementedFromAnyPointer(inner_root);
    return outer_builder.finish();
}

test "peer initDetached starts without attached transport" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    peer.start(null, null, null);
    try std.testing.expect(!peer.hasAttachedTransport());
}

test "peer limits bound outbound question allocation" {
    const Noop = struct {
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var peer = Peer.initDetachedWithLimits(std.testing.allocator, .{
        .max_outbound_questions = 0,
    });
    defer peer.deinit();

    var send_ctx: u8 = 0;
    peer.setSendFrameOverride(&send_ctx, Noop.send);
    var callback_ctx: u8 = 0;

    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer.sendBootstrap(&callback_ctx, Noop.onReturn),
    );
    try std.testing.expectEqual(@as(usize, 0), peer.questions.count());
}

test "peer limits bound active inbound questions before tracking" {
    var peer = Peer.initDetachedWithLimits(std.testing.allocator, .{
        .max_active_inbound_questions = 0,
    });
    defer peer.deinit();

    var builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    var call_builder = try builder.beginCall(7, 0xAB, 1);
    try call_builder.setTargetImportedCap(0);
    _ = try call_builder.initCapTableTyped(0);
    const frame = try builder.finish();
    defer std.testing.allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, frame);
    defer decoded.deinit();
    const call = try decoded.asCall();

    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.handleCall(&peer, frame, call),
    );
    try std.testing.expectEqual(@as(usize, 0), peer.active_inbound_questions.count());
}

test "peer limits bound queued promised calls by count and bytes" {
    const allocator = std.testing.allocator;
    var peer = Peer.initDetachedWithLimits(allocator, .{
        .max_pending_queued_calls = 1,
        .max_pending_queued_call_bytes = 4,
    });
    defer peer.deinit();

    const inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    try peer_test_hooks.queuePromisedCall(&peer, 10, &[_]u8{ 1, 2, 3, 4 }, inbound);

    var over_count = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.queuePromisedCall(&peer, 11, &[_]u8{1}, over_count),
    );
    over_count.deinit();

    try std.testing.expectEqual(@as(usize, 1), peer.pending_promises.count());

    var byte_peer = Peer.initDetachedWithLimits(allocator, .{
        .max_pending_queued_calls = 2,
        .max_pending_queued_call_bytes = 3,
    });
    defer byte_peer.deinit();

    var over_bytes = try cap_table.InboundCapTable.init(allocator, null, &byte_peer.caps);
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.queuePromisedCall(&byte_peer, 12, &[_]u8{ 1, 2, 3, 4 }, over_bytes),
    );
    over_bytes.deinit();
    try std.testing.expectEqual(@as(usize, 0), byte_peer.pending_promises.count());
}

test "peer limits bound resolve and embargo state" {
    var peer = Peer.initDetachedWithLimits(std.testing.allocator, .{
        .max_resolved_imports = 0,
        .max_pending_embargoes = 0,
    });
    defer peer.deinit();

    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.storeResolvedImport(&peer, 1, .none, null, false),
    );
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.rememberPendingEmbargo(&peer, 2, 1),
    );
    try std.testing.expectEqual(@as(usize, 0), peer.resolved_imports.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_embargoes.count());
}

test "peer limits bound pending third-party returns and embargoed accepts" {
    var peer = Peer.initDetachedWithLimits(std.testing.allocator, .{
        .max_pending_third_party_returns = 0,
        .max_pending_accepts = 0,
    });
    defer peer.deinit();

    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.bufferPendingThirdPartyReturn(&peer, 0x4000_0001, &[_]u8{1}),
    );
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.queueEmbargoedAccept(&peer, 3, 4, "embargo"),
    );
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_returns.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_accept_embargo_by_question.count());
}

test "peer limits bound provide join third-party maps and abort reason" {
    var peer = Peer.initDetachedWithLimits(std.testing.allocator, .{
        .max_active_provides = 0,
        .max_pending_joins = 0,
        .max_pending_join_questions = 0,
        .max_pending_third_party_awaits = 0,
        .max_pending_third_party_answers = 0,
        .max_adopted_third_party_answers = 0,
        .max_remote_abort_reason_bytes = 4,
    });
    defer peer.deinit();

    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.ensureProvideBudget(&peer, 1, "recipient"),
    );
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.ensureJoinBudget(&peer, 7, 2, 0, 2),
    );
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.ensurePendingThirdPartyAwaitBudget(&peer, "completion"),
    );
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.ensurePendingThirdPartyAnswerBudget(&peer, "completion"),
    );
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.ensureThirdPartyAdoptionBudget(&peer, 0x4000_0001),
    );

    var abort_builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer abort_builder.deinit();
    try abort_builder.buildAbort("abcdef");
    const abort_frame = try abort_builder.finish();
    defer std.testing.allocator.free(abort_frame);

    try std.testing.expectError(error.RemoteAbort, peer.handleFrame(abort_frame));
    try std.testing.expectEqualStrings("abcd", peer.getLastRemoteAbortReason().?);
}

test "peer limits bound provide and third-party key byte budgets" {
    var provide_peer = Peer.initDetachedWithLimits(std.testing.allocator, .{
        .max_active_provides = 8,
        .max_active_provide_key_bytes = 3,
    });
    defer provide_peer.deinit();

    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.ensureProvideBudget(&provide_peer, 1, "four"),
    );

    var third_party_peer = Peer.initDetachedWithLimits(std.testing.allocator, .{
        .max_pending_third_party_awaits = 8,
        .max_pending_third_party_answers = 8,
        .max_pending_third_party_completion_bytes = 3,
    });
    defer third_party_peer.deinit();

    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.ensurePendingThirdPartyAwaitBudget(&third_party_peer, "four"),
    );
    try std.testing.expectError(
        error.PeerLimitExceeded,
        peer_test_hooks.ensurePendingThirdPartyAnswerBudget(&third_party_peer, "four"),
    );
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
        fn onError(ctx_opt: ?*anyopaque, _: *Peer, err: anyerror) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ctx_opt orelse unreachable));
            ctx.called += 1;
            ctx.last_error = err;
        }
    };

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    var ctx = Ctx{};
    peer.setSendFrameOverride(&ctx, struct {
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
    }.send);
    peer.start(&ctx, Hooks.onError, null);

    peer_test_hooks.onConnectionError(&peer, error.ConnectionResetByPeer);
    try std.testing.expectEqual(@as(usize, 1), ctx.called);
    try std.testing.expectEqual(error.ConnectionResetByPeer, ctx.last_error.?);

    peer.start(null, null, null);
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
            const state: *Self = castCtx(*Self, peer.transport.ctx.?);
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
            const state: *Self = castCtx(*Self, peer.transport.ctx.?);
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

test "peer shutdown callback fires for detached peer with no transport" {
    const State = struct {
        var ctx: ?*@This() = null;
        calls: usize = 0,

        fn onShutdown(_: *Peer) void {
            const state = ctx orelse unreachable;
            state.calls += 1;
        }
    };

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    var state = State{};
    State.ctx = &state;
    defer State.ctx = null;

    peer.shutdown(State.onShutdown);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
}

test "peer detached shutdown callback fires after outstanding questions drain" {
    const State = struct {
        var ctx: ?*@This() = null;
        shutdown_calls: usize = 0,

        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
        fn onShutdown(_: *Peer) void {
            const state = ctx orelse unreachable;
            state.shutdown_calls += 1;
        }
    };

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    var send_ctx: u8 = 0;
    peer.setSendFrameOverride(&send_ctx, State.send);

    var callback_ctx: u8 = 0;
    const question_id = try peer.sendBootstrap(&callback_ctx, State.onReturn);
    try std.testing.expect(peer.questions.contains(question_id));

    var state = State{};
    State.ctx = &state;
    defer State.ctx = null;

    peer.shutdown(State.onShutdown);
    try std.testing.expectEqual(@as(usize, 0), state.shutdown_calls);

    peer_test_hooks.removeQuestion(&peer, question_id);
    try std.testing.expectEqual(@as(usize, 1), state.shutdown_calls);
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

test "sendBootstrap rolls back question when send fails" {
    const Hooks = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
        fn failSend(_: *anyopaque, _: []const u8) anyerror!void {
            return error.TestSendFailed;
        }
    };

    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    var send_ctx: u8 = 0;
    peer.setSendFrameOverride(&send_ctx, Hooks.failSend);

    var callback_ctx: u8 = 0;
    const expected_question_id = peer.next_question_id;
    try std.testing.expectError(error.TestSendFailed, peer.sendBootstrap(&callback_ctx, Hooks.onReturn));
    try std.testing.expect(!peer.questions.contains(expected_question_id));
    try std.testing.expectEqual(@as(usize, 0), peer.questions.count());
}

test "addExport rolls back cap table export identity when insertion fails" {
    const NoopHandler = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    // First allocation (caps.noteExport) succeeds, second (peer.exports.put)
    // fails, exercising rollback symmetry.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var peer = Peer.initDetached(failing.allocator());
    defer peer.deinit();

    var handler_state: u8 = 0;
    try std.testing.expectError(
        error.OutOfMemory,
        peer.addExport(.{
            .ctx = &handler_state,
            .on_call = NoopHandler.onCall,
        }),
    );
    try std.testing.expectEqual(@as(usize, 0), peer.exports.count());
    try std.testing.expectEqual(@as(u32, 0), peer.caps.totalEntries());
}

test "addPromiseExport rolls back cap table export identity when insertion fails" {
    // First allocation (caps.noteExport) succeeds, second (peer.exports.put)
    // fails, exercising rollback symmetry.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var peer = Peer.initDetached(failing.allocator());
    defer peer.deinit();

    try std.testing.expectError(error.OutOfMemory, peer.addPromiseExport());
    try std.testing.expectEqual(@as(usize, 0), peer.exports.count());
    try std.testing.expectEqual(@as(u32, 0), peer.caps.totalEntries());
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

test "forwarded payload remaps inbound import to receiverHosted preserving its origin" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    // An adversary can force export_id == import_id (noteImport does not
    // deconflict against exports — a spec-legal collision). If the forwarded
    // import's origin were re-derived from the bare id, classifyCap's
    // export-before-import priority would mis-encode it as OUR senderHosted
    // export 42 (capability substitution). Set up the collision and assert the
    // forward path honors the import origin instead.
    try peer.caps.noteExport(42);
    try peer.caps.noteImport(42);

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

    // Encode the forwarded cap table: the origin-tagged intermediate pointer is
    // rewritten to a real descriptor. It MUST be receiverHosted{42} (the import
    // forwarded back), NOT our senderHosted export 42.
    try cap_table.encodeCallPayloadCaps(&peer.caps, &dst_call, null, null, null);

    const dst_bytes = try dst_builder.finish();
    defer allocator.free(dst_bytes);
    var dst_decoded = try protocol.DecodedMessage.init(allocator, dst_bytes);
    defer dst_decoded.deinit();
    const parsed_dst_call = try dst_decoded.asCall();

    // The payload pointer now holds the cap-table index (0), not the id.
    const cap = try parsed_dst_call.params.content.getCapability();
    try std.testing.expectEqual(@as(u32, 0), cap.id);

    const cap_list = parsed_dst_call.params.cap_table orelse return error.MissingCapTable;
    const desc = try protocol.CapDescriptor.fromReader(try cap_list.get(0));
    try std.testing.expectEqual(protocol.CapDescriptorTag.receiverHosted, desc.tag);
    try std.testing.expectEqual(@as(u32, 42), desc.id.?);
}

test "forwarded payload preserves an export promise as senderPromise" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    // We host an unresolved export PROMISE at id 5. Forwarding it back must keep
    // the senderPromise variant so the peer still expects our later Resolve — a
    // naive .exported → senderHosted mapping would drop it.
    try peer.caps.markExportPromise(5);

    var inbound = cap_table.InboundCapTable{
        .allocator = allocator,
        .entries = try allocator.alloc(cap_table.ResolvedCap, 1),
        .retained = try allocator.alloc(bool, 1),
    };
    defer inbound.deinit();
    inbound.entries[0] = .{ .exported = .{ .id = 5 } };
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
    const cap_list = parsed_dst_call.params.cap_table orelse return error.MissingCapTable;
    const desc = try protocol.CapDescriptor.fromReader(try cap_list.get(0));
    try std.testing.expectEqual(protocol.CapDescriptorTag.senderPromise, desc.tag);
    try std.testing.expectEqual(@as(u32, 5), desc.id.?);
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

test "forwarded return propagates resultsSentElsewhere tag" {
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
            try std.testing.expectEqual(protocol.ReturnTag.resultsSentElsewhere, ret.tag);
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

test "forwarded return propagate-results mode translates takeFromOtherQuestion id" {
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

    const upstream_answer_id: u32 = 352;
    const local_forwarded_question_id: u32 = 353;
    const local_referenced_question_id: u32 = 354;
    const translated_upstream_answer_id: u32 = 355;

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
        .take_from_other_question = local_referenced_question_id,
    };
    try peer_test_hooks.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expectEqual(translated_upstream_answer_id, callback_ctx.referenced_answer);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return propagate-accept mode translates takeFromOtherQuestion id" {
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

    const upstream_answer_id: u32 = 356;
    const local_forwarded_question_id: u32 = 357;
    const local_referenced_question_id: u32 = 358;
    const translated_upstream_answer_id: u32 = 359;

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
        .send_results_to = .thirdParty,
        .send_results_to_third_party_payload = null,
        .answer_id = upstream_answer_id,
        .mode = .propagate_accept_from_third_party,
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

    // Declared before the peer: deinit's terminal question pass sends Finish
    // frames through the override, so the capture must outlive the peer.
    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const upstream_answer_id: u32 = 400;
    const local_forwarded_question_id: u32 = 401;

    // Register the upstream question as non-loopback so the return goes to
    // the wire (capture). The callback must be real: deinit's terminal pass
    // delivers a synthetic Disconnected Return to any question still in the
    // map at teardown.
    const NoopReturn = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    var question_ctx: u8 = 0;
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &question_ctx,
        .on_return = NoopReturn.onReturn,
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
    var third_msg = try message.Message.init(allocator, third_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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

    // Declared before the peer: deinit's terminal question pass sends Finish
    // frames through the override, so the capture must outlive the peer.
    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Register the upstream question as non-loopback so the forwarded return
    // goes to the wire (capture) instead of through the third-party adoption
    // path. The callback must be real: deinit's terminal pass delivers a
    // synthetic Disconnected Return to any question still in the map.
    const NoopReturn = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };
    var question_ctx: u8 = 0;
    try peer.questions.put(800, .{
        .ctx = &question_ctx,
        .on_return = NoopReturn.onReturn,
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
    var third_msg = try message.Message.init(allocator, third_bytes, .{});
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

test "handleCall rejects sendResultsTo.thirdParty by default" {
    const allocator = std.testing.allocator;

    // `sendResultsTo = thirdParty` asks this vat to deliver the results to a
    // third vat. It cannot, so it must refuse before dispatching -- accepting
    // the call and then dropping its results is the one outcome the protocol
    // never permits. Both reference implementations refuse too.

    const ServerCtx = struct {
        called: bool = false,
    };
    const ClientBuildCtx = struct {
        destination: message.AnyPointerReader,
    };
    const ReturnCtx = struct {
        returns: usize = 0,
        tag: ?protocol.ReturnTag = null,
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

        fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const rc: *ReturnCtx = castCtx(*ReturnCtx, ctx);
            rc.returns += 1;
            rc.tag = ret.tag;
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
    var third_msg = try message.Message.init(allocator, third_bytes, .{});
    defer third_msg.deinit();

    var client_build_ctx = ClientBuildCtx{
        .destination = try third_msg.getRootAnyPointer(),
    };
    var return_ctx = ReturnCtx{};
    _ = try peer.sendCallResolved(
        .{ .exported = .{ .id = export_id } },
        0x99,
        0,
        &client_build_ctx,
        Handlers.buildCall,
        Handlers.onReturn,
    );
    // The loopback Return is delivered synchronously inside sendCallResolved,
    // so the callback has already run by the time it returns.
    _ = &return_ctx;

    // The call was refused before dispatch.
    try std.testing.expect(!server_ctx.called);
    // Exactly one Return, and it is an exception -- not an instruction to await
    // a third party that will never be contacted.
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
    try std.testing.expectEqual(@as(usize, 0), peer.send_results_to_third_party.count());
    try std.testing.expectEqual(@as(usize, 0), peer.active_inbound_questions.count());
}

test "handleCall dispatches sendResultsTo.thirdParty under the application policy" {
    const allocator = std.testing.allocator;

    // With the host opting in, the call dispatches and the handler is
    // responsible for delivering results to the third vat itself, then settling
    // this answer with resultsSentElsewhere -- the tag the protocol mandates for
    // a Return answering a Call whose sendResultsTo was not `caller`.

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

        fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const server: *ServerCtx = castCtx(*ServerCtx, ctx);
            server.called = true;
            try peer.sendReturnResultsSentElsewhere(call.question_id);
        }

        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setThirdPartyResultPolicy(.application);

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
    var third_msg = try message.Message.init(allocator, third_bytes, .{});
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

    try std.testing.expect(server_ctx.called);
    // The routing marker and the answer are both drained by the settle.
    try std.testing.expectEqual(@as(usize, 0), peer.send_results_to_third_party.count());
    try std.testing.expectEqual(@as(usize, 0), peer.active_inbound_questions.count());
}

test "sendReturnResults refuses to discard results redirected to a third party" {
    const allocator = std.testing.allocator;

    // The regression for the original defect: a handler that builds results for
    // a redirected answer must not have them silently thrown away. Under the
    // application policy the call dispatches, so a handler that calls
    // sendReturnResults (rather than the settle API) gets a hard error.

    const ServerCtx = struct {
        build_ran: bool = false,
        got: ?anyerror = null,
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
            const server: *ServerCtx = castCtx(*ServerCtx, ctx);
            server.build_ran = true;
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            _ = try any.initStruct(0, 0);
            _ = try ret.initCapTableTyped(0);
        }

        fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const server: *ServerCtx = castCtx(*ServerCtx, ctx);
            peer.sendReturnResults(call.question_id, server, buildResults) catch |err| {
                server.got = err;
                // Settle the answer so the caller still gets exactly one Return.
                try peer.sendReturnResultsSentElsewhere(call.question_id);
                return;
            };
        }

        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setThirdPartyResultPolicy(.application);

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
    var third_msg = try message.Message.init(allocator, third_bytes, .{});
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

    // Before the fix this returned void and the built results vanished.
    try std.testing.expectEqual(@as(?anyerror, error.ThirdPartyResultsNotRedirected), server_ctx.got);
    try std.testing.expectEqual(@as(usize, 0), peer.send_results_to_third_party.count());
}

test "sendReturnResultsSentElsewhere rejects an answer whose results were not redirected" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        got: ?anyerror = null,
    };
    const Handlers = struct {
        fn buildCall(_: *anyopaque, _: *protocol.CallBuilder) anyerror!void {}

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
            // A plain caller-routed answer must never be settled without its
            // results.
            peer.sendReturnResultsSentElsewhere(call.question_id) catch |err| {
                server.got = err;
            };
            try peer.sendReturnResults(call.question_id, server, buildResults);
        }

        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    _ = try peer.sendCallResolved(
        .{ .exported = .{ .id = export_id } },
        0x99,
        0,
        &server_ctx,
        Handlers.buildCall,
        Handlers.onReturn,
    );

    try std.testing.expectEqual(@as(?anyerror, error.ResultsNotRedirected), server_ctx.got);
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
        .frames = std.ArrayList([]u8).empty,
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
    var completion_msg = try message.Message.init(allocator, completion_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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
    var completion_msg = try message.Message.init(allocator, completion_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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
    var completion_msg = try message.Message.init(allocator, completion_bytes, .{});
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
        var recipient_msg = try message.Message.init(allocator, recipient_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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

    try std.testing.expect(!peer.pending_promises.contains(77));
}

test "queued promised target key does not trigger duplicate question id for a distinct queued call id" {
    const allocator = std.testing.allocator;

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

    var queued_builder = protocol.MessageBuilder.init(allocator);
    defer queued_builder.deinit();
    var queued_call = try queued_builder.beginCall(100, 0xAA55, 1);
    try queued_call.setTargetPromisedAnswer(77);
    _ = try queued_call.initCapTableTyped(0);

    const queued_frame = try queued_builder.finish();
    defer allocator.free(queued_frame);

    var queued_decoded = try protocol.DecodedMessage.init(allocator, queued_frame);
    defer queued_decoded.deinit();
    try peer_test_hooks.handleCall(&peer, queued_frame, try queued_decoded.asCall());
    try std.testing.expect(peer.pending_promises.contains(77));

    // Question ID 77 collides with the promised-target key above, but this is
    // a distinct queued call ID and must not be rejected as duplicate.
    var inbound_builder = protocol.MessageBuilder.init(allocator);
    defer inbound_builder.deinit();
    var inbound_call = try inbound_builder.beginCall(77, 0xAA55, 2);
    try inbound_call.setTargetImportedCap(export_id);
    _ = try inbound_call.initCapTableTyped(0);

    const inbound_frame = try inbound_builder.finish();
    defer allocator.free(inbound_frame);

    var inbound_decoded = try protocol.DecodedMessage.init(allocator, inbound_frame);
    defer inbound_decoded.deinit();
    try peer_test_hooks.handleCall(&peer, inbound_frame, try inbound_decoded.asCall());
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

test "reflected caller call forwards with sendResultsTo=caller and translates results back (W1)" {
    // W1: when a parked pipelined call (sendResultsTo=caller) is replayed against
    // a promise that resolved to a CALLER-hosted cap (reflected loopback,
    // target == .imported), the forward uses `.translate_to_caller`, NOT the
    // spec-canonical `yourself` + `takeFromOtherQuestion` tail-call. The relayed
    // call carries `sendResultsTo=caller` and the real results are translated
    // straight back to the upstream question as a plain `.results` Return — the
    // only shapes go-capnp and capnp-rpc can consume in this topology. The
    // forwarded question auto-finishes normally (no tail-question state).
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
        .frames = std.ArrayList([]u8).empty,
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

    // Only the forwarded CALL is emitted; no eager `takeFromOtherQuestion`
    // return — the upstream question waits for the real results.
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_call_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_call_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.call, out_call_decoded.tag);
    const forwarded_call = try out_call_decoded.asCall();
    // *** W1: the relayed call goes out with sendResultsTo=caller, not yourself. ***
    try std.testing.expectEqual(protocol.SendResultsToTag.caller, forwarded_call.send_results_to.tag);
    // The call reached the reflected (caller-hosted) cap, preserving iface/method.
    try std.testing.expectEqual(protocol.MessageTargetTag.importedCap, forwarded_call.target.tag);
    try std.testing.expectEqual(target_import_id, forwarded_call.target.imported_cap.?);
    try std.testing.expectEqual(interface_id, forwarded_call.interface_id);
    try std.testing.expectEqual(method_id, forwarded_call.method_id);
    const forwarded_question_id = forwarded_call.question_id;

    // Forwarding state: the forwarded question maps back to the upstream one AND
    // is held open by a tail mapping with suppressed auto-finish — the forwarded
    // question is Finished exactly once, driven by the upstream caller's Finish,
    // not eagerly on its own return. (This is what makes the ordering-race clean;
    // see the companion premature-finish test.)
    try std.testing.expectEqual(upstream_question_id, peer.forwarded_questions.get(forwarded_question_id).?);
    try std.testing.expectEqual(forwarded_question_id, peer.forwarded_tail_questions.get(upstream_question_id).?);
    const question_entry = peer.questions.getEntry(forwarded_question_id) orelse return error.UnknownQuestion;
    try std.testing.expect(question_entry.value_ptr.suppress_auto_finish);

    // The reflected cap answers with real results (one data word n=42). B must
    // translate those results straight back to the upstream question as a plain
    // `.results` Return (the shape every reference impl consumes). Auto-finish is
    // suppressed, so no forwarded Finish is emitted yet.
    const ResultBuild = struct {
        fn build(_: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            const results = try any.initStruct(1, 0);
            results.writeU32(0, 42);
        }
    };
    var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
    defer fwd_ret_builder.deinit();
    var fwd_ret = try fwd_ret_builder.beginReturn(forwarded_question_id, .results);
    var dummy: u8 = 0;
    try ResultBuild.build(&dummy, &fwd_ret);
    const fwd_ret_frame = try fwd_ret_builder.finish();
    defer allocator.free(fwd_ret_frame);
    try peer.handleFrame(fwd_ret_frame);

    // Exactly one new frame: the translated `.results` Return to the upstream
    // caller carrying the real value (42). The forwarded question is drained from
    // the questions table but its tail mapping remains until the upstream Finish.
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);
    try std.testing.expect(!peer.questions.contains(forwarded_question_id));
    try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));

    var upstream_ret_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer upstream_ret_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.@"return", upstream_ret_decoded.tag);
    const upstream_ret = try upstream_ret_decoded.asReturn();
    try std.testing.expectEqual(upstream_question_id, upstream_ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, upstream_ret.tag);
    const upstream_payload = upstream_ret.results orelse return error.MissingResults;
    const upstream_content = try upstream_payload.content.getStruct();
    try std.testing.expectEqual(@as(u32, 42), upstream_content.readU32(0));

    // Upstream caller Finishes the original question → B relays a single Finish
    // for the forwarded question and clears the tail mapping. All state drains.
    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = upstream_question_id,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });
    try std.testing.expectEqual(@as(usize, 3), capture.frames.items.len);
    try std.testing.expect(!peer.forwarded_tail_questions.contains(upstream_question_id));
    var forwarded_finish_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[2]);
    defer forwarded_finish_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, forwarded_finish_decoded.tag);
    const forwarded_finish = try forwarded_finish_decoded.asFinish();
    try std.testing.expectEqual(forwarded_question_id, forwarded_finish.question_id);
}

test "reflected caller call: upstream finish before forwarded return cancels and drains (W1)" {
    // W1 translate-to-caller ordering race: the upstream caller sends Finish for
    // the reflected pipelined call BEFORE the forwarded call returns. B must
    // cancel the forwarded outbound question (single Finish to the target) and
    // drain all forwarding state, tolerating the late forwarded return.
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
        .frames = std.ArrayList([]u8).empty,
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
    // Only the forwarded CALL (sendResultsTo=caller); no eager tail return.
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_call_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_call_decoded.deinit();
    const forwarded_call = try out_call_decoded.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.caller, forwarded_call.send_results_to.tag);
    const forwarded_question_id = forwarded_call.question_id;

    // Upstream Finish arrives first. B cancels the forwarded question and emits a
    // single Finish for it.
    try peer_test_hooks.handleFinish(&peer, .{
        .question_id = upstream_question_id,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });

    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);
    var out_finish_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer out_finish_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, out_finish_decoded.tag);
    const forwarded_finish = try out_finish_decoded.asFinish();
    try std.testing.expectEqual(forwarded_question_id, forwarded_finish.question_id);

    // The late forwarded return is absorbed without emitting anything further.
    var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
    defer fwd_ret_builder.deinit();
    var fwd_ret = try fwd_ret_builder.beginReturn(forwarded_question_id, .results);
    var payload = try fwd_ret.payloadTyped();
    var any = try payload.initContent();
    _ = try any.initStruct(1, 0);
    const fwd_ret_frame = try fwd_ret_builder.finish();
    defer allocator.free(fwd_ret_frame);
    try peer.handleFrame(fwd_ret_frame);

    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);
    try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));
    try std.testing.expect(!peer.forwarded_tail_questions.contains(upstream_question_id));
    try std.testing.expect(!peer.questions.contains(forwarded_question_id));
}

test "reflected caller call: translate-to-caller stays stable under finish/return ordering races (W1)" {
    // W1: exercise both interleavings of the reflected-loopback forward under
    // `.translate_to_caller` across many rounds, asserting each round drains all
    // forwarding state with no leaks and emits exactly the right frames:
    //   - return-then-finish: forwarded `.results` → translated `.results` to the
    //     upstream caller; upstream Finish → single forwarded Finish.
    //   - finish-then-return (race): upstream Finish → single forwarded Finish +
    //     neutralize; the late forwarded `.results` is absorbed with no extra
    //     frame (never a spurious Return to the finished upstream question).
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

    const ResultBuild = struct {
        fn build(_: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            var payload = try ret.payloadTyped();
            var any = try payload.initContent();
            const results = try any.initStruct(1, 0);
            results.writeU32(0, 7);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
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
        // Only the forwarded call (sendResultsTo=caller); no eager tail return.
        try std.testing.expectEqual(frame_start + 1, capture.frames.items.len);

        var out_call_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start]);
        defer out_call_decoded.deinit();
        const forwarded_call = try out_call_decoded.asCall();
        try std.testing.expectEqual(protocol.SendResultsToTag.caller, forwarded_call.send_results_to.tag);
        const forwarded_question_id = forwarded_call.question_id;

        if ((round & 1) == 0) {
            // return-then-finish: real results come back first and translate to
            // the upstream caller as a plain `.results` Return.
            var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
            defer fwd_ret_builder.deinit();
            var fwd_ret = try fwd_ret_builder.beginReturn(forwarded_question_id, .results);
            var dummy: u8 = 0;
            try ResultBuild.build(&dummy, &fwd_ret);
            const fwd_ret_frame = try fwd_ret_builder.finish();
            defer allocator.free(fwd_ret_frame);
            try peer.handleFrame(fwd_ret_frame);

            try std.testing.expectEqual(frame_start + 2, capture.frames.items.len);
            try std.testing.expect(!peer.questions.contains(forwarded_question_id));
            var up_ret = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start + 1]);
            defer up_ret.deinit();
            const ret = try up_ret.asReturn();
            try std.testing.expectEqual(upstream_question_id, ret.answer_id);
            try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);

            try peer_test_hooks.handleFinish(&peer, .{
                .question_id = upstream_question_id,
                .release_result_caps = false,
                .require_early_cancellation = false,
            });
            // The upstream Finish relays the single forwarded Finish.
            try std.testing.expectEqual(frame_start + 3, capture.frames.items.len);
            var fin = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start + 2]);
            defer fin.deinit();
            try std.testing.expectEqual(protocol.MessageTag.finish, fin.tag);
            try std.testing.expectEqual(forwarded_question_id, (try fin.asFinish()).question_id);
        } else {
            // finish-then-return race: upstream Finishes first → one forwarded
            // Finish + neutralize; the late results Return is absorbed silently.
            try peer_test_hooks.handleFinish(&peer, .{
                .question_id = upstream_question_id,
                .release_result_caps = false,
                .require_early_cancellation = false,
            });
            try std.testing.expectEqual(frame_start + 2, capture.frames.items.len);
            var fin = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start + 1]);
            defer fin.deinit();
            try std.testing.expectEqual(protocol.MessageTag.finish, fin.tag);
            try std.testing.expectEqual(forwarded_question_id, (try fin.asFinish()).question_id);

            var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
            defer fwd_ret_builder.deinit();
            var fwd_ret = try fwd_ret_builder.beginReturn(forwarded_question_id, .results);
            var dummy: u8 = 0;
            try ResultBuild.build(&dummy, &fwd_ret);
            const fwd_ret_frame = try fwd_ret_builder.finish();
            defer allocator.free(fwd_ret_frame);
            try peer.handleFrame(fwd_ret_frame);
            // No spurious Return to the finished upstream question.
            try std.testing.expectEqual(frame_start + 2, capture.frames.items.len);
        }

        // Every round drains all forwarding state, whichever order fired.
        try std.testing.expect(!peer.forwarded_tail_questions.contains(upstream_question_id));
        try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));
        try std.testing.expect(!peer.questions.contains(forwarded_question_id));
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
        .frames = std.ArrayList([]u8).empty,
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

test "resolvePromiseExportToExport rejects unknown target export id" {
    const allocator = std.testing.allocator;

    const SendState = struct {
        sends: usize = 0,
        fn onFrame(ctx_ptr: *anyopaque, _: []const u8) anyerror!void {
            const state: *@This() = castCtx(*@This(), ctx_ptr);
            state.sends += 1;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var send_state = SendState{};
    peer.setSendFrameOverride(&send_state, SendState.onFrame);

    const promise_export_id = try peer.addPromiseExport();
    try std.testing.expectError(error.UnknownExport, peer.resolvePromiseExportToExport(promise_export_id, 999_999));
    try std.testing.expectEqual(@as(usize, 0), send_state.sends);

    const promise_entry = peer.exports.get(promise_export_id) orelse return error.UnknownExport;
    try std.testing.expect(promise_entry.is_promise);
    try std.testing.expect(promise_entry.resolved == null);
    try std.testing.expect(peer.caps.isExportPromise(promise_export_id));
}

test "Unimplemented(Resolve) echo releases the resolution target's export ref" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
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

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var handler_state: u8 = 0;
    const concrete_export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });
    const promise_a = try peer.addPromiseExport();
    const promise_b = try peer.addPromiseExport();

    try peer.resolvePromiseExportToExport(promise_a, concrete_export_id);
    try peer.resolvePromiseExportToExport(promise_b, concrete_export_id);

    // Each outgoing Resolve descriptor hands the remote one wire ref on the
    // resolution target.
    const refs_after_resolves = peer.exports.get(concrete_export_id) orelse return error.UnknownExport;
    try std.testing.expectEqual(@as(u32, 2), refs_after_resolves.ref_count);
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    // Level-0 echo of promise_a's Resolve (hand-built copy, as the remote
    // would embed it): spends exactly one ref, export survives on the other.
    {
        var resolve_builder = protocol.MessageBuilder.init(allocator);
        defer resolve_builder.deinit();
        try resolve_builder.buildResolveCap(promise_a, .{
            .tag = .senderHosted,
            .id = concrete_export_id,
        });
        const resolve_bytes = try resolve_builder.finish();
        defer allocator.free(resolve_bytes);
        const echo = try buildUnimplementedEcho(allocator, resolve_bytes);
        defer allocator.free(echo);
        try peer.handleFrame(echo);
    }
    const surviving = peer.exports.get(concrete_export_id) orelse return error.UnknownExport;
    try std.testing.expectEqual(@as(u32, 1), surviving.ref_count);

    // Echo of promise_b's Resolve: the last WIRE ref is spent, but the two
    // resolved promise exports still pin the target with promise-held refs, so
    // it survives at wire ref_count 0. (A call through either promise must
    // still dispatch to it — see the dedicated pinning regression test below.)
    {
        var resolve_builder = protocol.MessageBuilder.init(allocator);
        defer resolve_builder.deinit();
        try resolve_builder.buildResolveCap(promise_b, .{
            .tag = .senderHosted,
            .id = concrete_export_id,
        });
        const resolve_bytes = try resolve_builder.finish();
        defer allocator.free(resolve_bytes);
        const echo = try buildUnimplementedEcho(allocator, resolve_bytes);
        defer allocator.free(echo);
        try peer.handleFrame(echo);
    }
    const pinned = peer.exports.get(concrete_export_id) orelse return error.TargetDestroyedByRelease;
    try std.testing.expectEqual(@as(u32, 0), pinned.ref_count);
    try std.testing.expectEqual(@as(u32, 2), pinned.promise_ref_count);

    // The promise exports themselves are untouched: the remote still holds
    // them, and releasing the resolution produced no outbound frames.
    try std.testing.expect(peer.exports.contains(promise_a));
    try std.testing.expect(peer.exports.contains(promise_b));
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);
}

test "level-0 peer echo of our own Resolve frame leaves no leaked export state" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
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

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var handler_state: u8 = 0;
    const concrete_export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });
    const promise_export_id = try peer.addPromiseExport();

    try peer.resolvePromiseExportToExport(promise_export_id, concrete_export_id);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    // A level-0 peer echoes back the exact Resolve frame we sent, embedded in
    // an Unimplemented message.
    const echo = try buildUnimplementedEcho(allocator, capture.frames.items[0]);
    defer allocator.free(echo);
    try peer.handleFrame(echo);

    // The echoed Resolve releases the WIRE ref its descriptor carried, but the
    // promise export still pins its resolution target with a promise-held ref,
    // so the target survives at wire ref_count 0. The promise stays resolved
    // locally: a level-0 peer never picked up the resolution, so it may still
    // call the promise export and we must still dispatch to the target.
    const target = peer.exports.get(concrete_export_id) orelse return error.TargetDestroyedByRelease;
    try std.testing.expectEqual(@as(u32, 0), target.ref_count);
    try std.testing.expectEqual(@as(u32, 1), target.promise_ref_count);
    const promise_entry = peer.exports.get(promise_export_id) orelse return error.UnknownExport;
    try std.testing.expect(promise_entry.resolved != null);
    // peer.deinit() under std.testing.allocator verifies no allocation leaks.
}

test "unimplemented echoes of other tags and malformed resolves are safe no-ops" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
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

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var handler_state: u8 = 0;
    const concrete_export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });
    const promise_export_id = try peer.addPromiseExport();
    try peer.resolvePromiseExportToExport(promise_export_id, concrete_export_id);
    const frames_after_resolve = capture.frames.items.len;

    const expectStateUnchanged = struct {
        fn check(p: *Peer, concrete: u32) !void {
            const entry = p.exports.get(concrete) orelse return error.UnknownExport;
            try std.testing.expectEqual(@as(u32, 1), entry.ref_count);
        }
    }.check;

    // Echoed resolve-to-exception: carried no cap descriptor, nothing to
    // release.
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildResolveException(promise_export_id, "broken");
        const bytes = try builder.finish();
        defer allocator.free(bytes);
        const echo = try buildUnimplementedEcho(allocator, bytes);
        defer allocator.free(echo);
        try peer.handleFrame(echo);
        try expectStateUnchanged(&peer, concrete_export_id);
    }

    // Echoed Resolve naming an unknown export id is tolerated (warn only),
    // matching releaseExport's tolerance for unknown ids in Release frames.
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildResolveCap(promise_export_id, .{
            .tag = .senderHosted,
            .id = 999_999,
        });
        const bytes = try builder.finish();
        defer allocator.free(bytes);
        const echo = try buildUnimplementedEcho(allocator, bytes);
        defer allocator.free(echo);
        try peer.handleFrame(echo);
        try expectStateUnchanged(&peer, concrete_export_id);
    }

    // A forged echo naming a live export with no spendable wire refs hits
    // the same ReleaseCountExceeded guard a bogus Release frame does.
    {
        var bare_state: u8 = 0;
        const bare_export_id = try peer.addExport(.{
            .ctx = &bare_state,
            .on_call = Handlers.onCall,
        });
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildResolveCap(promise_export_id, .{
            .tag = .senderHosted,
            .id = bare_export_id,
        });
        const bytes = try builder.finish();
        defer allocator.free(bytes);
        const echo = try buildUnimplementedEcho(allocator, bytes);
        defer allocator.free(echo);
        try std.testing.expectError(error.ReleaseCountExceeded, peer.handleFrame(echo));
        try expectStateUnchanged(&peer, concrete_export_id);
    }

    // Echoed Release remains an explicit no-op: it carries no sender state
    // that must be unwound on echo. (Echoed Disembargo is NOT a no-op — it is a
    // protocol violation surfaced as a connection-level abort; see the
    // dedicated W2 test below.)
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildRelease(5, 1);
        const bytes = try builder.finish();
        defer allocator.free(bytes);
        const echo = try buildUnimplementedEcho(allocator, bytes);
        defer allocator.free(echo);
        try peer.handleFrame(echo);
        try expectStateUnchanged(&peer, concrete_export_id);
    }

    // None of the echoes provoked an outbound frame.
    try std.testing.expectEqual(frames_after_resolve, capture.frames.items.len);
}

// W2 (docs/supported-surface.md known limitation #1): a peer that echoes our
// Disembargo back inside an Unimplemented cannot uphold e-order across the
// resolve the Disembargo was protecting — the embargo can never be lifted by a
// receiverLoopback. Previously this was silently dropped and the target import
// stayed flagged `embargoed` with a retained `pending_embargoes` entry. It must
// now surface as a connection-level error: an outbound Abort plus a returned
// error that tears the connection down.
test "echoed Unimplemented(Disembargo) aborts the connection instead of stranding the embargo" {
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
        .frames = std.ArrayList([]u8).empty,
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Build a Disembargo exactly as we would have sent one, then wrap it in an
    // Unimplemented as a broken/hostile Level-0 peer would echo it back.
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildDisembargoSenderLoopback(.{
        .tag = .importedCap,
        .imported_cap = 1,
        .promised_answer = null,
    }, 9);
    const bytes = try builder.finish();
    defer allocator.free(bytes);
    const echo = try buildUnimplementedEcho(allocator, bytes);
    defer allocator.free(echo);

    // The connection-level error propagates out of handleFrame (the read loop
    // tears the connection down on it), instead of being silently dropped.
    try std.testing.expectError(error.EchoedDisembargoUnimplemented, peer.handleFrame(echo));

    // Exactly one outbound frame: an Abort with a clear reason.
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, out_decoded.tag);
    const abort = try out_decoded.asAbort();
    try std.testing.expectEqualStrings("peer echoed Disembargo as Unimplemented", abort.exception.reason);
}

// W3 (docs/supported-surface.md known limitation #2): the senderLoopback
// disembargo-target gate (hasKnownDisembargoTarget for an importedCap target)
// used to accept a cap id that matched EITHER an export OR an import. The spec
// only ever names an export here: a compliant peer originates a senderLoopback
// disembargo targeting the promise it imported from us as that promise resolves
// (resolve.zig sets imported_cap = promise_id), and on the wire that id is the
// promise WE exported. This test proves the previously over-accepted import-only
// id is now rejected, while the spec-correct export-id path still validates and
// echoes receiverLoopback.
test "senderLoopback disembargo target is validated against exports only, not imports" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
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
    const Build = struct {
        fn senderLoopback(alloc: std.mem.Allocator, cap_id: u32, embargo_id: u32) ![]const u8 {
            var builder = protocol.MessageBuilder.init(alloc);
            defer builder.deinit();
            try builder.buildDisembargoSenderLoopback(.{
                .tag = .importedCap,
                .imported_cap = cap_id,
                .promised_answer = null,
            }, embargo_id);
            return builder.finish();
        }
    };

    // `capture` is declared before `peer` so peer.deinit() (which runs its
    // teardown Release for the un-released import through the send-frame
    // override) fires while `capture` is still live; capture's own cleanup then
    // frees every recorded frame, including any deinit-time Release.
    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // An id that exists ONLY as an import — never as an export. This is exactly
    // the shape the old `exports.contains(id) or imports.contains(id)` gate
    // over-accepted.
    const import_only_id: u32 = 4001;
    try peer.caps.noteImport(import_only_id);
    try std.testing.expect(peer.caps.imports.contains(import_only_id));
    try std.testing.expect(!peer.exports.contains(import_only_id));

    // REJECT: a senderLoopback disembargo naming the import-only id is no longer
    // a known target. It surfaces as a connection-level error rather than being
    // silently echoed back as a receiverLoopback.
    {
        const frame = try Build.senderLoopback(allocator, import_only_id, 71);
        defer allocator.free(frame);
        try std.testing.expectError(error.UnknownDisembargoTarget, peer.handleFrame(frame));
        // The rejected disembargo produced no receiverLoopback echo.
        try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);
    }

    // ACCEPT: the spec-correct path — a senderLoopback disembargo naming one of
    // OUR exports (the promise we exported and the remote resolved) still
    // validates and echoes a receiverLoopback disembargo.
    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });
    try std.testing.expect(peer.exports.contains(export_id));
    {
        const frame = try Build.senderLoopback(allocator, export_id, 72);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
        // Exactly one outbound frame: the echoed receiverLoopback disembargo.
        try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
        var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
        defer out_decoded.deinit();
        try std.testing.expectEqual(protocol.MessageTag.disembargo, out_decoded.tag);
        const echoed = try out_decoded.asDisembargo();
        try std.testing.expectEqual(protocol.DisembargoContextTag.receiverLoopback, echoed.context_tag);
        try std.testing.expectEqual(@as(?u32, 72), echoed.embargo_id);
        try std.testing.expectEqual(protocol.MessageTargetTag.importedCap, echoed.target.tag);
        try std.testing.expectEqual(@as(?u32, export_id), echoed.target.imported_cap);
    }
}

test "resolved promise pins its target export against remote Release until the promise is dropped" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        calls: usize = 0,
        last_question_id: u32 = 0,
    };
    const Handlers = struct {
        fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const ctx: *ServerCtx = castCtx(*ServerCtx, ctx_ptr);
            ctx.calls += 1;
            ctx.last_question_id = call.question_id;
            try peer.sendReturnException(call.question_id, "dispatched");
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

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
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

    // Model the remote holding one wire ref on the promise export (it imported
    // the promise from a prior Return/Resolve). Bumping ref_count directly is
    // the established idiom for "the remote received this capability".
    {
        var pe = peer.exports.getEntry(promise_export_id) orelse return error.MissingExport;
        pe.value_ptr.ref_count = 1;
    }

    // Resolve the promise to the concrete export: this hands the remote one
    // wire ref on the target AND pins the target with one promise-held ref.
    try peer.resolvePromiseExportToExport(promise_export_id, concrete_export_id);
    {
        const c = peer.exports.get(concrete_export_id) orelse return error.MissingExport;
        try std.testing.expectEqual(@as(u32, 1), c.ref_count);
        try std.testing.expectEqual(@as(u32, 1), c.promise_ref_count);
    }

    // The remote Releases its wire ref to the target the moment it drops the
    // resolved cap — a legal interleaving. Before the fix this zeroed the
    // target's only refcount and destroyed the export while the promise still
    // routed calls at it.
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildRelease(concrete_export_id, 1);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    // The promise-held ref keeps the target alive at wire ref_count 0.
    {
        const c = peer.exports.get(concrete_export_id) orelse return error.TargetDestroyedByRelease;
        try std.testing.expectEqual(@as(u32, 0), c.ref_count);
        try std.testing.expectEqual(@as(u32, 1), c.promise_ref_count);
    }

    // A call THROUGH the promise export must still dispatch to the concrete
    // handler (pre-fix: "unknown promised capability" because the target was
    // gone).
    {
        var call_builder = protocol.MessageBuilder.init(allocator);
        defer call_builder.deinit();
        var call = try call_builder.beginCall(4242, 0xFEED, 3);
        try call.setTargetImportedCap(promise_export_id);
        _ = try call.initCapTableTyped(0);
        const frame = try call_builder.finish();
        defer allocator.free(frame);

        var decoded = try protocol.DecodedMessage.init(allocator, frame);
        defer decoded.deinit();
        const parsed = try decoded.asCall();
        try peer_test_hooks.handleCall(&peer, frame, parsed);
    }
    try std.testing.expectEqual(@as(usize, 1), server_ctx.calls);
    try std.testing.expectEqual(@as(u32, 4242), server_ctx.last_question_id);

    // Releasing the promise export itself finally releases the promise-held
    // ref, and with no references of any class left the target is destroyed.
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildRelease(promise_export_id, 1);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
    try std.testing.expect(!peer.exports.contains(promise_export_id));
    try std.testing.expect(!peer.exports.contains(concrete_export_id));
    // peer.deinit() under std.testing.allocator verifies no allocation leaks.
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
        .frames = std.ArrayList([]u8).empty,
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
        .frames = std.ArrayList([]u8).empty,
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

    var inner_msg = try message.Message.init(allocator, inner_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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

    var recipient_msg = try message.Message.init(allocator, recipient_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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

    var recipient_msg = try message.Message.init(allocator, recipient_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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

    var recipient_msg = try message.Message.init(allocator, recipient_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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
        .frames = std.ArrayList([]u8).empty,
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

    var recipient_msg = try message.Message.init(allocator, recipient_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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
    var key_part_msg = try message.Message.init(allocator, key_part_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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
    var key0_msg = try message.Message.init(allocator, key0_bytes, .{});
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
    var key1_msg = try message.Message.init(allocator, key1_bytes, .{});
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
        .frames = std.ArrayList([]u8).empty,
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
        .frames = std.ArrayList([]u8).empty,
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

fn loopbackVatNetworkIntroductionOomImpl(allocator: std.mem.Allocator) !void {
    var third_vat_peer = Peer.initDetached(allocator);
    defer third_vat_peer.deinit();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    const nonce = "oom-loopback-introduction";
    try net.register(nonce, &third_vat_peer);

    const network = net.network();
    var introduction = try network.mintIntroduction(&third_vat_peer, nonce);
    defer introduction.deinit(allocator);

    var contact_msg = try message.Message.initUnvalidated(allocator, introduction.to_contact);
    defer contact_msg.deinit();
    const contact = try contact_msg.getRootAnyPointer();

    var introduced = try network.connectToIntroduced(contact);
    defer introduced.deinit(allocator);
    try std.testing.expectEqual(&third_vat_peer, introduced.peer);
    try std.testing.expectEqualSlices(u8, introduction.to_await, introduced.completion);
}

test "loopback VatNetwork introduction path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        loopbackVatNetworkIntroductionOomImpl,
        .{},
    );
}

test "loopback VatNetwork rejects duplicate and unknown introductions" {
    const allocator = std.testing.allocator;

    var third_vat_peer = Peer.initDetached(allocator);
    defer third_vat_peer.deinit();

    var net = vat_network.LoopbackVatNetwork(Peer).init(allocator);
    defer net.deinit();

    try net.register("known-nonce", &third_vat_peer);
    try std.testing.expectError(error.DuplicateNonce, net.register("known-nonce", &third_vat_peer));
    try std.testing.expectError(error.UnknownIntroduction, net.network().mintIntroduction(&third_vat_peer, "missing-nonce"));

    const unknown_token = try vat_network.encodeNonceToken(allocator, "missing-nonce");
    defer allocator.free(unknown_token);
    var unknown_msg = try message.Message.initUnvalidated(allocator, unknown_token);
    defer unknown_msg.deinit();
    const unknown_contact = try unknown_msg.getRootAnyPointer();
    try std.testing.expectError(error.UnknownIntroduction, net.network().connectToIntroduced(unknown_contact));
}

fn sendProvideOomImpl(allocator: std.mem.Allocator) !void {
    const Sink = struct {
        fn onFrame(_: *anyopaque, _: []const u8) anyerror!void {}
    };

    var provide_peer = Peer.initDetached(allocator);
    defer provide_peer.deinit();
    var recipient_peer = Peer.initDetached(allocator);
    defer recipient_peer.deinit();

    var sink_ctx: u8 = 0;
    provide_peer.setSendFrameOverride(&sink_ctx, Sink.onFrame);
    recipient_peer.setSendFrameOverride(&sink_ctx, Sink.onFrame);

    const await_payload = try vat_network.encodeNonceToken(allocator, "oom-send-provide-await");
    defer allocator.free(await_payload);
    var await_msg = try message.Message.initUnvalidated(allocator, await_payload);
    defer await_msg.deinit();
    const recipient = try await_msg.getRootAnyPointer();

    const contact_payload = try vat_network.encodeNonceToken(allocator, "oom-send-provide-contact");
    defer allocator.free(contact_payload);

    const provided_target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = 77,
        .promised_answer = null,
    };

    const handle = try provide_peer.sendProvide(
        provided_target,
        recipient,
        &recipient_peer,
        contact_payload,
    );

    try std.testing.expect(provide_peer.questions.contains(handle.question_id));
    try std.testing.expect(recipient_peer.exports.contains(handle.vine_id));
    try std.testing.expect(recipient_peer.outbound_provides.contains(handle.vine_id));
    try std.testing.expect(recipient_peer.caps.getThirdPartyHosted(handle.vine_id) != null);
    try std.testing.expectEqual(@as(usize, 1), provide_peer.coupled_vines.items.len);

    peer_test_hooks.removeQuestion(&provide_peer, handle.question_id);
    _ = recipient_peer.outbound_provides.remove(handle.vine_id);
    recipient_peer.caps.clearThirdPartyHosted(handle.vine_id);
    peer_test_hooks.releaseVineExport(&recipient_peer, handle.vine_id);
    provide_peer.coupled_vines.clearRetainingCapacity();
}

test "peer sendProvide origination path propagates OOM without stale handoff state" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sendProvideOomImpl, .{});
}

fn sendAcceptOomImpl(allocator: std.mem.Allocator) !void {
    const Sink = struct {
        fn onFrame(_: *anyopaque, _: []const u8) anyerror!void {}
    };
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

    var sink_ctx: u8 = 0;
    peer.setSendFrameOverride(&sink_ctx, Sink.onFrame);

    const provision_payload = try vat_network.encodeNonceToken(allocator, "oom-send-accept");
    defer allocator.free(provision_payload);
    var provision_msg = try message.Message.initUnvalidated(allocator, provision_payload);
    defer provision_msg.deinit();
    const provision = try provision_msg.getRootAnyPointer();

    var callback_ctx: u8 = 0;
    const question_id = try peer.sendAccept(provision, "oom-accept-embargo", &callback_ctx, Callback.onReturn);
    try std.testing.expect(peer.questions.contains(question_id));
    peer_test_hooks.removeQuestion(&peer, question_id);
}

test "peer sendAccept origination path propagates OOM without stale question state" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sendAcceptOomImpl, .{});
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

    var recipient_msg = try message.Message.init(allocator, recipient_bytes, .{});
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
    // Opt in: under the default `.reject` policy handleCall refuses such calls
    // before the routing marker is ever recorded, so the allocator-failure paths
    // this test covers (noteSendResultsToThirdParty's budget accounting and
    // payload clone) would never be reached.
    peer.setThirdPartyResultPolicy(.application);

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
    var destination_msg = try message.Message.init(allocator, destination_bytes, .{});
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
    var completion_msg = try message.Message.init(allocator, completion_bytes, .{});
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

    // Drain the parked await before deinit: the terminal question pass sweeps
    // awaits with a (fallible, swallowed) allocation, which
    // checkAllAllocationFailures would flag as a swallowed OOM.
    while (true) {
        var it = peer.pending_third_party_awaits.iterator();
        const kv = it.next() orelse break;
        const removed = peer.pending_third_party_awaits.fetchRemove(kv.key_ptr.*) orelse break;
        if (removed.value.question.deinit_ctx) |dc| dc(peer.allocator, removed.value.question.ctx);
        peer.allocator.free(removed.key);
    }
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
    var third_msg = try message.Message.init(allocator, third_bytes, .{});
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

test "sendReturnResultsSentElsewhere drains calls pipelined on the redirected answer" {
    const allocator = std.testing.allocator;

    // A redirected answer's results never pass through this vat, so it cannot
    // resolve a promised-answer target against them. Calls already pipelined on
    // that answer must therefore each get their own Return -- left waiting they
    // hang a compliant caller forever, which is exactly what a plain
    // sendReturnTag would do (failQueuedPromisedCalls has no caller on the tag
    // path).

    const ServerCtx = struct {
        parked_question: ?u32 = null,
    };
    const Handlers = struct {
        // Deferred: record the question and return without settling, so the
        // pipelined child can be delivered while the answer is still pending.
        fn onCall(ctx: *anyopaque, _: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const server: *ServerCtx = castCtx(*ServerCtx, ctx);
            server.parked_question = call.question_id;
        }
    };

    var captured = std.ArrayList([]u8).empty;
    defer {
        for (captured.items) |f| allocator.free(f);
        captured.deinit(allocator);
    }
    const Capture = struct {
        fn send(ctx: *anyopaque, frame: []const u8) anyerror!void {
            const list: *std.ArrayList([]u8) = @ptrCast(@alignCast(ctx));
            try list.append(std.testing.allocator, try std.testing.allocator.dupe(u8, frame));
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setThirdPartyResultPolicy(.application);
    peer.setSendFrameOverride(&captured, Capture.send);

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    // A third-party recipient token for the redirected call.
    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("recipient");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);
    var third_msg = try message.Message.init(allocator, third_bytes, .{});
    defer third_msg.deinit();
    const recipient = try third_msg.getRootAnyPointer();

    // (1) An inbound Call with sendResultsTo = thirdParty targeting our export.
    const parent_qid: u32 = 41;
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        var call = try builder.beginCall(parent_qid, 0xABCD, 0);
        try call.setTargetImportedCap(export_id);
        try call.setSendResultsToThirdParty(recipient);
        _ = try call.initCapTableTyped(0);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
    try std.testing.expectEqual(@as(?u32, parent_qid), server_ctx.parked_question);
    try std.testing.expect(peer.send_results_to_third_party.contains(parent_qid));

    // (2) A second Call pipelined on that still-pending answer.
    const child_qid: u32 = 42;
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        var call = try builder.beginCall(child_qid, 0xABCD, 0);
        try call.setTargetPromisedAnswer(parent_qid);
        _ = try call.initCapTableTyped(0);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    const frames_before = captured.items.len;

    // (3) The application settles the parent with resultsSentElsewhere.
    try peer.sendReturnResultsSentElsewhere(parent_qid);

    // Two Returns must have been emitted: the parent's resultsSentElsewhere and
    // the child's exception. Without the drain the child never hears back.
    try std.testing.expect(captured.items.len >= frames_before + 2);

    var saw_parent = false;
    var saw_child = false;
    for (captured.items) |frame| {
        var decoded = protocol.DecodedMessage.init(allocator, frame) catch continue;
        defer decoded.deinit();
        if (decoded.tag != .@"return") continue;
        const ret = decoded.asReturn() catch continue;
        if (ret.answer_id == parent_qid and ret.tag == .resultsSentElsewhere) saw_parent = true;
        if (ret.answer_id == child_qid and ret.tag == .exception) saw_child = true;
    }
    try std.testing.expect(saw_parent);
    try std.testing.expect(saw_child);
    try std.testing.expectEqual(@as(usize, 0), peer.pending_promises.count());
}

test "locally synthesized exceptions carry the spec Exception.Type" {
    const allocator = std.testing.allocator;

    // The retryability signal must be on the wire, not inferred from reason
    // text. Before this, every outbound exception carried type 0 (`failed`) and
    // disconnect detection was a string compare against capnp-zig's own
    // literal -- which no other implementation emits, so a disconnect reported
    // by a C++/Go/Rust peer read as a plain application error, and an
    // application error whose text happened to be "disconnected" read as a
    // transport loss.

    const Captured = struct {
        tag: ?protocol.ReturnTag = null,
        type_value: ?u16 = null,
    };
    const Handlers = struct {
        fn buildCall(_: *anyopaque, _: *protocol.CallBuilder) anyerror!void {}
        fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const c: *Captured = castCtx(*Captured, ctx);
            c.tag = ret.tag;
            if (ret.exception) |ex| c.type_value = ex.type_value;
        }
        // Never answers, so the question stays outstanding for the drain.
        fn onCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    // (1) A transport drain reports `disconnected`.
    {
        var peer = Peer.initDetached(allocator);
        var handler_state: u8 = 0;
        const export_id = try peer.addExport(.{
            .ctx = &handler_state,
            .on_call = Handlers.onCall,
        });
        var captured = Captured{};
        _ = try peer.sendCallResolved(
            .{ .exported = .{ .id = export_id } },
            0x99,
            0,
            &captured,
            Handlers.buildCall,
            Handlers.onReturn,
        );
        // deinit drains outstanding questions with a synthetic exception.
        peer.deinit();
        try std.testing.expectEqual(@as(?protocol.ReturnTag, .exception), captured.tag);
        try std.testing.expectEqual(
            @as(?u16, @intFromEnum(protocol.ExceptionType.disconnected)),
            captured.type_value,
        );
    }

    // (2) An ordinary application failure stays `failed`.
    {
        var peer = Peer.initDetached(allocator);
        defer peer.deinit();
        const Failing = struct {
            fn onCall(_: *anyopaque, p: *Peer, call: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {
                try p.sendReturnException(call.question_id, "application blew up");
            }
        };
        var handler_state: u8 = 0;
        const export_id = try peer.addExport(.{
            .ctx = &handler_state,
            .on_call = Failing.onCall,
        });
        var captured = Captured{};
        _ = try peer.sendCallResolved(
            .{ .exported = .{ .id = export_id } },
            0x99,
            0,
            &captured,
            Handlers.buildCall,
            Handlers.onReturn,
        );
        try std.testing.expectEqual(
            @as(?u16, @intFromEnum(protocol.ExceptionType.failed)),
            captured.type_value,
        );
    }
}

test "exceptionTypeForError classifies retryability" {
    const errors = @import("capnpc-zig").rpc.peer.errors;
    try std.testing.expectEqual(protocol.ExceptionType.overloaded, errors.exceptionTypeForError(error.OutOfMemory));
    try std.testing.expectEqual(protocol.ExceptionType.overloaded, errors.exceptionTypeForError(error.PeerLimitExceeded));
    try std.testing.expectEqual(protocol.ExceptionType.disconnected, errors.exceptionTypeForError(error.PeerShuttingDown));
    try std.testing.expectEqual(protocol.ExceptionType.disconnected, errors.exceptionTypeForError(error.RemoteAbort));
    try std.testing.expectEqual(protocol.ExceptionType.unimplemented, errors.exceptionTypeForError(error.EchoedDisembargoUnimplemented));
    // The catch-all: repeating the call fails the same way.
    try std.testing.expectEqual(protocol.ExceptionType.failed, errors.exceptionTypeForError(error.DuplicateQuestionId));
    try std.testing.expectEqual(protocol.ExceptionType.failed, errors.exceptionTypeForError(error.PromiseBroken));
}
