//! Bounded, structured lifecycle operations through actual generated APIs.
const std = @import("std");
const capnp = @import("capnpc-zig");
const g = @import("generated");
const rpc = capnp.rpc;
const Peer = rpc.peer.Peer;
const Context = struct {
    next: u32 = 0,
    previous: u32 = 0,
    delivered: u32 = 0,
    ack: ?g.TestStreaming.DoStreamI.StreamReturnSender = null,
    barrier: bool = false,
    fn build(ctx: *anyopaque, params: *g.TestStreaming.DoStreamI.Params.Builder) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.next += 1;
        try params.setI(self.next);
    }
    fn forbidden(_: *anyopaque, _: *Peer, _: g.TestStreaming.DoStreamI.Params.Reader, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        return error.ExpectedDeferredStream;
    }
    fn push(ctx: *anyopaque, _: *Peer, params: g.TestStreaming.DoStreamI.Params.Reader, _: *const rpc.caps.table.InboundCapTable, ack: g.TestStreaming.DoStreamI.StreamReturnSender) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        const value = try params.getI();
        try std.testing.expect(value > self.previous and value <= self.next);
        self.previous = value;
        self.delivered += 1;
        self.ack = ack;
    }
    fn j(_: *anyopaque, _: *Peer, _: g.TestStreaming.DoStreamJ.Params.Reader, _: *const rpc.caps.table.InboundCapTable) anyerror!void {}
    fn finish(ctx: *anyopaque, _: *Peer, _: g.TestStreaming.FinishStream.Params.Reader, result: *g.TestStreaming.FinishStream.Results.Builder, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try std.testing.expectEqual(self.next, self.delivered);
        try result.setTotalI(self.delivered);
    }
    fn returned(ctx: *anyopaque, _: *Peer, response: g.TestStreaming.FinishStream.Response, _: *const rpc.caps.table.InboundCapTable) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        try std.testing.expectEqual(self.next, try (try response.unwrap()).getTotalI());
        self.barrier = true;
    }
};
const Link = struct {
    peer: *Peer,
    question: ?u32 = null,
    fail_after_call: bool = false,
    live: bool = true,
    fn send(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (!self.live) return;
        var decoded = try rpc.wire.protocol.DecodedMessage.init(std.testing.allocator, bytes);
        defer decoded.deinit();
        if (decoded.tag == .call) self.question = (try decoded.asCall()).question_id;
        try self.peer.handleFrame(bytes);
        if (decoded.tag == .call and self.fail_after_call) return error.TransportWriteFailed;
    }
};
fn lifecycle(_: void, smith: *std.testing.Smith) !void {
    var caller = Peer.initDetached(std.testing.allocator);
    var callee = Peer.initDetached(std.testing.allocator);
    var outbound = Link{ .peer = &callee };
    var inbound = Link{ .peer = &caller };
    defer {
        outbound.live = false;
        inbound.live = false;
        caller.deinit();
        callee.deinit();
    }
    caller.setSendFrameOverride(&outbound, Link.send);
    callee.setSendFrameOverride(&inbound, Link.send);
    var ctx = Context{};
    var server = g.TestStreaming.Server{ .ctx = &ctx, .vtable = .{ .doStreamI = Context.forbidden, .doStreamI_deferred = Context.push, .doStreamJ = Context.j, .finishStream = Context.finish } };
    const id = try g.TestStreaming.exportServer(&callee, &server);
    var client = g.TestStreaming.StreamClient.init(g.TestStreaming.Client.init(&caller, id));
    client.stream.max_in_flight = 4;
    client.stream.max_in_flight_bytes = 4096;
    callee.streaming.limits = .{ .max_calls = 6, .max_bytes = 8192 };
    const operations = smith.valueRangeAtMost(u8, 1, 16);
    for (0..operations) |_| {
        if (client.stream.hasFailed()) break;
        switch (smith.valueRangeAtMost(u8, 0, 6)) {
            0, 1 => client.callDoStreamI(&ctx, Context.build) catch |err| try std.testing.expectEqual(error.StreamInFlightLimitExceeded, err),
            2 => if (callee.streaming.outstanding_calls != 0) {
                try ctx.ack.?.send();
            },
            3 => if (callee.streaming.outstanding_calls != 0) {
                try ctx.ack.?.sendException("fuzz handler failure");
            },
            4 => if (client.stream.in_flight != 0) {
                try caller.cancelQuestion(outbound.question.?, "fuzz cancellation");
            },
            5 => {
                outbound.live = false;
                inbound.live = false;
                caller.notifyTransportClosed();
                callee.notifyTransportClosed();
                break;
            },
            6 => {
                outbound.fail_after_call = true;
                client.callDoStreamI(&ctx, Context.build) catch |err| {
                    try std.testing.expect(err == error.TransportWriteFailed or err == error.StreamInFlightLimitExceeded);
                };
                outbound.live = false;
                inbound.live = false;
                caller.notifyTransportClosed();
                callee.notifyTransportClosed();
                break;
            },
            else => unreachable,
        }
        try std.testing.expect(client.stream.in_flight <= 4 and client.stream.in_flight_bytes <= 4096);
        try std.testing.expect(callee.streaming.outstanding_calls <= 6 and callee.streaming.outstanding_bytes <= 8192);
    }
    if (!caller.transport_close_notified and !client.stream.hasFailed()) {
        _ = try client.callFinishStream(&ctx, null, Context.returned);
        while (callee.streaming.outstanding_calls != 0) try ctx.ack.?.send();
        try std.testing.expect(ctx.barrier);
        try std.testing.expectEqual(ctx.next, ctx.delivered);
    }
    outbound.live = false;
    inbound.live = false;
    caller.notifyTransportClosed();
    callee.notifyTransportClosed();
    try std.testing.expectEqual(@as(u32, 0), client.stream.in_flight);
    try std.testing.expectEqual(@as(usize, 0), client.stream.in_flight_bytes);
    try std.testing.expectEqual(@as(usize, 0), callee.streaming.outstanding_calls);
    try std.testing.expectEqual(@as(usize, 0), callee.streaming.outstanding_bytes);
}
test "fuzz: generated streaming lifecycle and ordered barriers" {
    try std.testing.fuzz({}, lifecycle, .{});
}
