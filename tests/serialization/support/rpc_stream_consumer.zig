const std = @import("std");
const capnpc = @import("capnpc-zig");
const g = @import("generated.zig");
const rpc = capnpc.rpc;
const Peer = rpc.peer.Peer;
const State = struct {
    total: u32 = 0,
    drained: bool = false,
    fn i(ctx: *anyopaque, _: *Peer, params: g.TestStreaming.DoStreamI.Params.Reader, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.total += try params.getI();
    }
    fn j(_: *anyopaque, _: *Peer, _: g.TestStreaming.DoStreamJ.Params.Reader, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
    fn finish(_: *anyopaque, _: *Peer, _: g.TestStreaming.FinishStream.Params.Reader, _: *g.TestStreaming.FinishStream.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
    fn build(_: *anyopaque, params: *g.TestStreaming.DoStreamI.Params.Builder) anyerror!void {
        try params.setI(17);
    }
    fn drain(ctx: *anyopaque, err: ?anyerror) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.drained = err == null;
    }
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const peer: *Peer = @ptrCast(@alignCast(ctx));
        try peer.handleFrame(bytes);
    }
};
test "stream result is bundled and generated stream calls acknowledge" {
    try std.testing.expect(g.TestStreaming.DoStreamI.Results == rpc.generated.stream.StreamResult);
    var client_peer = Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, State.send);
    server_peer.setSendFrameOverride(&client_peer, State.send);
    var state = State{};
    var server = g.TestStreaming.Server{ .ctx = &state, .vtable = .{ .doStreamI = State.i, .doStreamJ = State.j, .finishStream = State.finish } };
    const id = try g.TestStreaming.exportServer(&server_peer, &server);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&client_peer, id));
    try client.callDoStreamI(&state, State.build);
    client.waitStreaming(&state, State.drain);
    try std.testing.expectEqual(@as(u32, 17), state.total);
    try std.testing.expect(state.drained);
}

const Capture = struct {
    peer: *Peer,
    fail_after_call: bool = false,
    question_id: ?u32 = null,
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        var msg = try rpc.wire.protocol.DecodedMessage.init(std.testing.allocator, bytes);
        defer msg.deinit();
        const is_call = msg.tag == .call;
        if (is_call) self.question_id = (try msg.asCall()).question_id;
        try self.peer.handleFrame(bytes);
        if (is_call and self.fail_after_call) return error.TransportWriteFailed;
    }
};
test "generated streaming synchronous acknowledgement followed by send error settles once" {
    var client_peer = Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    var link = Capture{ .peer = &server_peer, .fail_after_call = true };
    client_peer.setSendFrameOverride(&link, Capture.send);
    server_peer.setSendFrameOverride(&client_peer, State.send);
    var state = State{};
    var server = g.TestStreaming.Server{ .ctx = &state, .vtable = .{ .doStreamI = State.i, .doStreamJ = State.j, .finishStream = State.finish } };
    const id = try g.TestStreaming.exportServer(&server_peer, &server);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&client_peer, id));
    try std.testing.expectError(error.TransportWriteFailed, client.callDoStreamI(&state, State.build));
    try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    try std.testing.expectEqual(@as(u32, 17), state.total);
    client.waitStreaming(&state, State.drain);
    try std.testing.expect(state.drained);
}

const Pending = struct {
    question_id: ?u32 = null,
    drain_count: u32 = 0,
    drain_error: ?anyerror = null,
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        var decoded = try rpc.wire.protocol.DecodedMessage.init(std.testing.allocator, bytes);
        defer decoded.deinit();
        if (decoded.tag == .call) self.question_id = (try decoded.asCall()).question_id;
    }
    fn drain(ctx: *anyopaque, err: ?anyerror) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.drain_count += 1;
        self.drain_error = err;
    }
};

test "generated pending stream teardown settles even when terminal frame allocation fails" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var peer = Peer.initDetached(failing.allocator());
    var pending = Pending{};
    peer.setSendFrameOverride(&pending, Pending.send);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&peer, 0));
    try client.callDoStreamI(&pending, null);
    client.waitStreaming(&pending, Pending.drain);
    failing.fail_index = failing.alloc_index;
    peer.deinit();
    try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    try std.testing.expectEqual(@as(u32, 1), pending.drain_count);
    try std.testing.expectEqual(error.StreamingCallFailed, pending.drain_error.?);
}

test "transport teardown during streaming send preserves pending context destruction under OOM" {
    const Closing = struct {
        peer: *Peer,
        allocator: *std.testing.FailingAllocator,
        fn send(ctx: *anyopaque, _: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.allocator.fail_index = self.allocator.alloc_index;
            self.peer.deinit();
        }
    };
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var peer = Peer.initDetached(failing.allocator());
    var closing = Closing{ .peer = &peer, .allocator = &failing };
    peer.setSendFrameOverride(&closing, Closing.send);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&peer, 0));
    try client.callDoStreamI(&closing, null);
    try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    try std.testing.expectEqual(@as(usize, 0), client.stream.in_flight_bytes);
    try std.testing.expectEqual(error.StreamingCallFailed, client.stream.stream_error.?);
}

test "generated stream cancellation absorbs late acknowledgement and disconnect settles once" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var pending = Pending{};
    peer.setSendFrameOverride(&pending, Pending.send);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&peer, 0));
    try client.callDoStreamI(&pending, null);
    client.waitStreaming(&pending, Pending.drain);
    try peer.cancelQuestion(pending.question_id.?, "cancel stream");
    try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    try std.testing.expectEqual(@as(u32, 1), pending.drain_count);
    var reply = rpc.wire.protocol.MessageBuilder.init(std.testing.allocator);
    defer reply.deinit();
    _ = try reply.beginReturn(pending.question_id.?, .canceled);
    const bytes = try reply.finish();
    defer std.testing.allocator.free(bytes);
    try peer.handleFrame(bytes);
    peer.notifyTransportClosed();
    try std.testing.expectEqual(@as(u32, 1), pending.drain_count);
    try std.testing.expectError(error.StreamingCallFailed, client.callDoStreamI(&pending, null));
}

fn streamAllocationFailures(fail_index: usize) !usize {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
    var peer = Peer.initDetached(failing.allocator());
    var pending = Pending{};
    peer.setSendFrameOverride(&pending, Pending.send);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&peer, 0));
    client.callDoStreamI(&pending, null) catch |err| {
        try std.testing.expectEqual(error.OutOfMemory, err);
        try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    };
    peer.deinit();
    try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    return failing.alloc_index;
}
test "generated streaming construction and teardown unwind every allocation failure" {
    // Teardown deliberately recovers from OOM through its infallible context
    // destructor; the ordinary helper rejects that documented recovery.
    const allocations = try streamAllocationFailures(std.math.maxInt(usize));
    for (0..allocations) |fail_index| _ = try streamAllocationFailures(fail_index);
}

test "generated streaming reserves exact encoded bytes before sending and restores readiness" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var pending = Pending{};
    peer.setSendFrameOverride(&pending, Pending.send);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&peer, 0));
    client.stream.max_in_flight_bytes = 1;
    try std.testing.expectError(error.StreamCallTooLarge, client.callDoStreamI(&pending, null));
    try std.testing.expectEqual(@as(?u32, null), pending.question_id);
    try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    try std.testing.expectEqual(@as(usize, 0), client.stream.in_flight_bytes);
    client.stream.max_in_flight_bytes = 4096;
    try client.callDoStreamI(&pending, null);
    const used = client.stream.in_flight_bytes;
    try std.testing.expect(used > 1);
    client.stream.max_in_flight_bytes = used;
    try std.testing.expectError(error.StreamByteLimitExceeded, client.callDoStreamI(&pending, null));
    try std.testing.expectEqual(@as(usize, used), client.stream.in_flight_bytes);
    var ready = Pending{};
    try std.testing.expectEqual(used, client.stream.last_rejected_bytes);
    client.whenStreamingReady(client.stream.last_rejected_bytes, &ready, Pending.drain);
    try std.testing.expectEqual(@as(u32, 0), ready.drain_count);
    var reply = rpc.wire.protocol.MessageBuilder.init(std.testing.allocator);
    defer reply.deinit();
    var ret = try reply.beginReturn(pending.question_id.?, .results);
    var payload = try ret.payloadTyped();
    var content = try payload.initContent();
    _ = try content.initStruct(0, 0);
    const bytes = try reply.finish();
    defer std.testing.allocator.free(bytes);
    try peer.handleFrame(bytes);
    try std.testing.expectEqual(@as(u32, 1), ready.drain_count);
    try std.testing.expectEqual(@as(?anyerror, null), ready.drain_error);
    try std.testing.expectEqual(@as(usize, 0), client.stream.in_flight_bytes);
}

const Slow = struct {
    values: [4]u32 = undefined,
    count: usize = 0,
    ack: ?g.TestStreaming.DoStreamI.StreamReturnSender = null,
    finished: bool = false,
    fn deferred(ctx: *anyopaque, _: *Peer, params: g.TestStreaming.DoStreamI.Params.Reader, _: *const rpc.caps.table.InboundCapTable, ack: g.TestStreaming.DoStreamI.StreamReturnSender) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.values[self.count] = try params.getI();
        self.count += 1;
        self.ack = ack;
    }
    fn finish(ctx: *anyopaque, _: *Peer, _: g.TestStreaming.FinishStream.Params.Reader, results: *g.TestStreaming.FinishStream.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try results.setTotalI(@intCast(self.count));
        self.finished = true;
    }
    fn result(_: *anyopaque, _: *Peer, response: g.TestStreaming.FinishStream.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        try std.testing.expectEqual(@as(u32, 2), try (try response.unwrap()).getTotalI());
    }
};
test "deferred streaming acknowledgement preserves handler order and final barrier" {
    var client_peer = Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, State.send);
    server_peer.setSendFrameOverride(&client_peer, State.send);
    var state = Slow{};
    var server = g.TestStreaming.Server{ .ctx = &state, .vtable = .{ .doStreamI = State.i, .doStreamI_deferred = Slow.deferred, .doStreamJ = State.j, .finishStream = Slow.finish } };
    const id = try g.TestStreaming.exportServer(&server_peer, &server);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&client_peer, id));
    try client.callDoStreamI(&state, State.build);
    try client.callDoStreamI(&state, State.build);
    _ = try client.callFinishStream(&state, null, Slow.result);
    try std.testing.expectEqual(@as(usize, 1), state.count);
    try std.testing.expectEqual(@as(u32, 2), client.stream.in_flight);
    try std.testing.expect(!state.finished);
    const first = state.ack.?;
    try first.send();
    try std.testing.expectEqual(@as(usize, 2), state.count);
    try std.testing.expect(!state.finished);
    try std.testing.expectError(error.StreamingCallClosed, first.send());
    try state.ack.?.send();
    try std.testing.expect(state.finished);
    try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    try std.testing.expectEqual(@as(usize, 0), client.stream.in_flight_bytes);
}

test "deferred receiver bounds queued calls and cancellation closes the stream" {
    var client_peer = Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    var link = Capture{ .peer = &server_peer };
    client_peer.setSendFrameOverride(&link, Capture.send);
    server_peer.setSendFrameOverride(&client_peer, State.send);
    server_peer.streaming.limits.max_calls = 2;
    var state = Slow{};
    var server = g.TestStreaming.Server{ .ctx = &state, .vtable = .{ .doStreamI = State.i, .doStreamI_deferred = Slow.deferred, .doStreamJ = State.j, .finishStream = Slow.finish } };
    const id = try g.TestStreaming.exportServer(&server_peer, &server);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&client_peer, id));
    try client.callDoStreamI(&state, State.build);
    const first_id = link.question_id.?;
    try client.callDoStreamI(&state, State.build);
    const bytes = server_peer.streaming.outstanding_bytes;
    try client.callDoStreamI(&state, State.build);
    try std.testing.expectEqual(@as(usize, 2), server_peer.streaming.outstanding_calls);
    try std.testing.expectEqual(bytes, server_peer.streaming.outstanding_bytes);
    try std.testing.expect(client.stream.hasFailed());
    try client_peer.cancelQuestion(first_id, "cancel pending work");
    try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    try std.testing.expectEqual(@as(usize, 0), client.stream.in_flight_bytes);
    try std.testing.expectEqual(@as(usize, 0), server_peer.streaming.outstanding_calls);
    try std.testing.expectEqual(@as(usize, 0), server_peer.streaming.outstanding_bytes);
    try std.testing.expectEqual(@as(usize, 1), state.count);
    try std.testing.expectError(error.StreamingCallClosed, state.ack.?.send());
}

fn deferredAllocationFailures(fail_index: usize) !usize {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
    var client_peer = Peer.initDetached(failing.allocator());
    var server_peer = Peer.initDetached(failing.allocator());
    var ignored = Pending{};
    defer {
        client_peer.setSendFrameOverride(&ignored, Pending.send);
        server_peer.setSendFrameOverride(&ignored, Pending.send);
        client_peer.deinit();
        server_peer.deinit();
    }
    client_peer.setSendFrameOverride(&server_peer, State.send);
    server_peer.setSendFrameOverride(&client_peer, State.send);
    var slow = Slow{};
    var server = g.TestStreaming.Server{ .ctx = &slow, .vtable = .{ .doStreamI = State.i, .doStreamI_deferred = Slow.deferred, .doStreamJ = State.j, .finishStream = Slow.finish } };
    const id = g.TestStreaming.exportServer(&server_peer, &server) catch return failing.alloc_index;
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&client_peer, id));
    client.callDoStreamI(&slow, State.build) catch return failing.alloc_index;
    client.callDoStreamI(&slow, State.build) catch return failing.alloc_index;
    if (slow.ack) |ack| ack.send() catch {};
    if (slow.ack) |ack| ack.send() catch {};
    return failing.alloc_index;
}
test "deferred streaming queued input and replay release allocations on failure" {
    const allocations = try deferredAllocationFailures(std.math.maxInt(usize));
    for (0..allocations) |fail_index| _ = try deferredAllocationFailures(fail_index);
}

const CapabilityState = struct {
    cap_id: u32 = 0,
    invoked: u32 = 0,
    returned: u32 = 0,
    ack: ?g.CapabilityStream.Push.StreamReturnSender = null,
    fn build(ctx: *anyopaque, params: *g.CapabilityStream.Push.Params.Builder) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try params.setData("queued bytes");
        try params.setCallbackCapability(.{ .id = self.cap_id });
    }
    fn ping(ctx: *anyopaque, _: *Peer, _: g.StreamCapability.Ping.Params.Reader, result: *g.StreamCapability.Ping.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.invoked += 1;
        try result.setValue(42);
    }
    fn returnedPing(ctx: *anyopaque, _: *Peer, response: g.StreamCapability.Ping.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try std.testing.expectEqual(@as(u32, 42), try (try response.unwrap()).getValue());
        self.returned += 1;
    }
    fn forbidden(_: *anyopaque, _: *Peer, _: g.CapabilityStream.Push.Params.Reader, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        return error.ExpectedDeferredDispatch;
    }
    fn push(ctx: *anyopaque, peer: *Peer, params: g.CapabilityStream.Push.Params.Reader, caps: *const rpc.caps.table.InboundCapTable, ack: g.CapabilityStream.Push.StreamReturnSender) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try std.testing.expectEqualStrings("queued bytes", try params.getData());
        const callback = try caps.resolveCapability(try params.getCallback());
        try std.testing.expect(callback == .imported);
        _ = try g.StreamCapability.Client.init(peer, callback.imported.id).callPing(self, null, CapabilityState.returnedPing);
        self.ack = ack;
    }
    fn finish(_: *anyopaque, _: *Peer, _: g.CapabilityStream.Finish.Params.Reader, _: *g.CapabilityStream.Finish.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
};
test "queued streaming payload preserves capability ownership until invocation or cancellation" {
    for ([_]bool{ false, true }) |cancel| {
        var client_peer = Peer.initDetached(std.testing.allocator);
        defer client_peer.deinit();
        var server_peer = Peer.initDetached(std.testing.allocator);
        defer server_peer.deinit();
        var link = Capture{ .peer = &server_peer };
        client_peer.setSendFrameOverride(&link, Capture.send);
        server_peer.setSendFrameOverride(&client_peer, State.send);
        var state = CapabilityState{};
        var callback = g.StreamCapability.Server{ .ctx = &state, .vtable = .{ .ping = CapabilityState.ping } };
        state.cap_id = try g.StreamCapability.exportServer(&client_peer, &callback);
        try client_peer.noteHandoffExportRef(state.cap_id);
        var server = g.CapabilityStream.Server{ .ctx = &state, .vtable = .{ .push = CapabilityState.forbidden, .push_deferred = CapabilityState.push, .finish = CapabilityState.finish } };
        const id = try g.CapabilityStream.exportServer(&server_peer, &server);
        var client = g.CapabilityStream.StreamClient.init(g.CapabilityStream.Client.init(&client_peer, id));
        try client.callPush(&state, CapabilityState.build);
        const first_id = link.question_id.?;
        try client.callPush(&state, CapabilityState.build);
        client_peer.releaseHandoffHeldExport(state.cap_id);
        try std.testing.expectEqual(@as(u32, 1), state.invoked);
        try std.testing.expect(client_peer.exports.contains(state.cap_id));
        if (cancel) {
            try client_peer.cancelQuestion(first_id, "cancel queued capability");
            try std.testing.expectEqual(@as(u32, 1), state.invoked);
        } else {
            try state.ack.?.send();
            try std.testing.expectEqual(@as(u32, 2), state.invoked);
            try std.testing.expectEqual(@as(u32, 2), state.returned);
            try state.ack.?.send();
        }
        try std.testing.expect(!client_peer.exports.contains(state.cap_id));
        try std.testing.expectEqual(@as(usize, 0), server_peer.streaming.outstanding_calls);
        try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    }
}

const Reentrant = struct {
    client: *g.TestStreaming.StreamClient,
    drained: bool = false,
    fn build(ctx: *anyopaque, _: *g.TestStreaming.DoStreamI.Params.Builder) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.client.waitStreaming(self, Reentrant.drain);
    }
    fn drain(ctx: *anyopaque, err: ?anyerror) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.drained = err == null;
        self.client.client.peer.deinit();
    }
};
test "synchronous streaming drain may tear down peer before generated send returns" {
    var client_peer = Peer.initDetached(std.testing.allocator);
    var server_peer = Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, State.send);
    server_peer.setSendFrameOverride(&client_peer, State.send);
    var state = State{};
    var server = g.TestStreaming.Server{ .ctx = &state, .vtable = .{ .doStreamI = State.i, .doStreamJ = State.j, .finishStream = State.finish } };
    const id = try g.TestStreaming.exportServer(&server_peer, &server);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&client_peer, id));
    var reentrant = Reentrant{ .client = &client };
    try client.callDoStreamI(&reentrant, Reentrant.build);
    try std.testing.expect(reentrant.drained);
    try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
}

test "nested frame unwind defers transport close until streaming acknowledgement settles" {
    const Closing = struct {
        peer: *Peer,
        nested: []const u8,
        fn send(ctx: *anyopaque, _: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.peer.notifyTransportClosed();
            try self.peer.handleLoopbackFrame(self.nested);
        }
    };
    var client_peer = Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, State.send);
    var state = Slow{};
    var server = g.TestStreaming.Server{ .ctx = &state, .vtable = .{ .doStreamI = State.i, .doStreamI_deferred = Slow.deferred, .doStreamJ = State.j, .finishStream = Slow.finish } };
    const id = try g.TestStreaming.exportServer(&server_peer, &server);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&client_peer, id));
    try client.callDoStreamI(&state, State.build);
    var nested = rpc.wire.protocol.MessageBuilder.init(std.testing.allocator);
    defer nested.deinit();
    try nested.buildRelease(1000, 0);
    const bytes = try nested.finish();
    defer std.testing.allocator.free(bytes);
    var closing = Closing{ .peer = &server_peer, .nested = bytes };
    server_peer.setSendFrameOverride(&closing, Closing.send);
    try state.ack.?.send();
    try std.testing.expect(server_peer.transport_close_notified);
    try std.testing.expectEqual(@as(usize, 0), server_peer.streaming.outstanding_calls);
}

test "deferred receiver charges exact incoming bytes before retaining queued input" {
    var client_peer = Peer.initDetached(std.testing.allocator);
    defer client_peer.deinit();
    var server_peer = Peer.initDetached(std.testing.allocator);
    defer server_peer.deinit();
    client_peer.setSendFrameOverride(&server_peer, State.send);
    server_peer.setSendFrameOverride(&client_peer, State.send);
    var state = Slow{};
    var server = g.TestStreaming.Server{ .ctx = &state, .vtable = .{ .doStreamI = State.i, .doStreamI_deferred = Slow.deferred, .doStreamJ = State.j, .finishStream = Slow.finish } };
    const id = try g.TestStreaming.exportServer(&server_peer, &server);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&client_peer, id));
    try client.callDoStreamI(&state, State.build);
    const size = server_peer.streaming.outstanding_bytes;
    try std.testing.expectEqual(client.stream.in_flight_bytes, size);
    server_peer.streaming.limits.max_bytes = size * 2;
    try client.callDoStreamI(&state, State.build);
    try std.testing.expectEqual(size * 2, server_peer.streaming.outstanding_bytes);
    try client.callDoStreamI(&state, State.build);
    try std.testing.expect(client.stream.hasFailed());
    try std.testing.expectEqual(size * 2, server_peer.streaming.outstanding_bytes);
    try std.testing.expectEqual(@as(usize, 1), state.count);
    try state.ack.?.send();
    try state.ack.?.send();
    try std.testing.expectEqual(@as(usize, 0), server_peer.streaming.outstanding_bytes);
    try std.testing.expectEqual(@as(usize, 0), client.stream.in_flight_bytes);
}

fn wireStreamCall(peer: *Peer, target: u32, question: u32, value: u32) !void {
    var builder = rpc.wire.protocol.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question, g.TestStreaming.interface_id, g.TestStreaming.DoStreamI.ordinal);
    try call.setTargetImportedCap(target);
    var payload = try call.payloadTyped();
    var any = try payload.initContent();
    var params = g.TestStreaming.DoStreamI.Params.Builder.wrap(try any.initStruct(1, 0));
    try params.setI(value);
    const bytes = try builder.finish();
    defer std.testing.allocator.free(bytes);
    try peer.handleFrame(bytes);
}
test "stale streaming acknowledgement cannot settle a reused remote question id" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();
    var sink = Pending{};
    peer.setSendFrameOverride(&sink, Pending.send);
    var slow = Slow{};
    var server = g.TestStreaming.Server{ .ctx = &slow, .vtable = .{ .doStreamI = State.i, .doStreamI_deferred = Slow.deferred, .doStreamJ = State.j, .finishStream = Slow.finish } };
    const target = try g.TestStreaming.exportServer(&peer, &server);
    try wireStreamCall(&peer, target, 17, 1);
    const old = slow.ack.?;
    try old.send();
    var finish = rpc.wire.protocol.MessageBuilder.init(std.testing.allocator);
    defer finish.deinit();
    try finish.buildFinish(17, true, false);
    const bytes = try finish.finish();
    defer std.testing.allocator.free(bytes);
    try peer.handleFrame(bytes);
    try wireStreamCall(&peer, target, 17, 2);
    try std.testing.expectError(error.StreamingCallClosed, old.send());
    try std.testing.expectEqual(@as(usize, 1), peer.streaming.outstanding_calls);
    try slow.ack.?.send();
}
