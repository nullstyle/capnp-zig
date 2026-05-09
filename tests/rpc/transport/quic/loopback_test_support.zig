const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const quic = capnpc.rpc.transport.quic;

pub const loopback_cert_pem = @embedFile("loopback_cert.pem");
pub const loopback_key_pem = @embedFile("loopback_key.pem");
pub const loopback_timeout_ms: u64 = 3_000;
pub const loopback_poll_ms: u64 = 5;

pub fn testListenAddr() std.Io.net.IpAddress {
    return .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 0,
    } };
}

pub fn captureServerLog(_: ?*anyopaque, _: quic.ServerLogEvent) void {}

pub const QuicEndpointState = struct {
    messages: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    errors: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    last_error: ?anyerror = null,
    received: [8192]u8 = undefined,
    received_len: usize = 0,

    pub fn recordMessage(self: *QuicEndpointState, frame: []const u8) !void {
        if (frame.len > self.received.len) return error.QuicLoopbackPayloadTooLarge;
        @memcpy(self.received[0..frame.len], frame);
        self.received_len = frame.len;
        _ = self.messages.fetchAdd(1, .acq_rel);
    }

    pub fn receivedSlice(self: *const QuicEndpointState) []const u8 {
        return self.received[0..self.received_len];
    }
};

pub const OrderedQuicEndpointState = struct {
    const max_frames = 8;

    messages: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    errors: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    last_error: ?anyerror = null,
    expected: []const []const u8 = &.{},
    close_after_messages: usize = 0,
    received_order: [max_frames]usize = @splat(std.math.maxInt(usize)),
    received_lengths: [max_frames]usize = @splat(0),

    pub fn recordExpected(self: *OrderedQuicEndpointState, frame: []const u8) !usize {
        const slot = self.messages.load(.acquire);
        if (slot >= max_frames) return error.QuicLoopbackTooManyMessages;

        var matched: ?usize = null;
        for (self.expected, 0..) |expected, index| {
            if (std.mem.eql(u8, frame, expected)) {
                matched = index;
                break;
            }
        }
        const index = matched orelse return error.QuicLoopbackUnexpectedFrame;

        self.received_order[slot] = index;
        self.received_lengths[slot] = frame.len;
        _ = self.messages.fetchAdd(1, .acq_rel);
        return slot + 1;
    }

    pub fn expectOrder(self: *const OrderedQuicEndpointState, expected_order: []const usize) !void {
        try std.testing.expectEqual(expected_order.len, self.messages.load(.acquire));
        for (expected_order, 0..) |expected_index, slot| {
            try std.testing.expectEqual(expected_index, self.received_order[slot]);
            try std.testing.expectEqual(self.expected[expected_index].len, self.received_lengths[slot]);
        }
    }
};

pub fn buildBootstrapFrame(allocator: std.mem.Allocator, question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildBootstrap(question_id);
    return builder.finish();
}

pub fn buildCallFrameWithData(allocator: std.mem.Allocator, question_id: u32, payload_len: usize) ![]const u8 {
    const payload = try allocator.alloc(u8, payload_len);
    defer allocator.free(payload);
    for (payload, 0..) |*byte, index| {
        byte.* = @truncate(index);
    }

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, 0x5155_4943, 7);
    try call.setTargetImportedCap(0);
    call.setSendResultsToCaller();
    var params = try call.payloadTyped();
    try params.setContentData(payload);
    _ = try call.initCapTableTyped(0);
    return builder.finish();
}

pub fn sleepMs(ms: u64) void {
    std.Io.sleep(std.testing.io, std.Io.Duration.fromMilliseconds(@intCast(ms)), .awake) catch {};
}

pub fn runQuicConnection(conn: *quic.Connection) void {
    conn.run();
}

pub fn waitForClientMessageOrError(
    client_state: *const QuicEndpointState,
    server_state: *const QuicEndpointState,
) bool {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback_timeout_ms) : (waited_ms += loopback_poll_ms) {
        if (client_state.messages.load(.acquire) > 0) return true;
        if (client_state.errors.load(.acquire) > 0 or server_state.errors.load(.acquire) > 0) return false;
        sleepMs(loopback_poll_ms);
    }
    return client_state.messages.load(.acquire) > 0;
}

pub fn waitForServerError(server_state: *const QuicEndpointState) bool {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback_timeout_ms) : (waited_ms += loopback_poll_ms) {
        if (server_state.errors.load(.acquire) > 0) return true;
        sleepMs(loopback_poll_ms);
    }
    return server_state.errors.load(.acquire) > 0;
}

pub fn waitForOrderedClientMessagesOrError(
    client_state: *const OrderedQuicEndpointState,
    server_state: *const OrderedQuicEndpointState,
    expected_messages: usize,
) bool {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback_timeout_ms) : (waited_ms += loopback_poll_ms) {
        if (client_state.messages.load(.acquire) >= expected_messages) return true;
        if (client_state.errors.load(.acquire) > 0 or server_state.errors.load(.acquire) > 0) return false;
        sleepMs(loopback_poll_ms);
    }
    return client_state.messages.load(.acquire) >= expected_messages;
}

pub fn echoQuicMessage(conn: *quic.Connection, frame: []const u8) !void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    try state.recordMessage(frame);
    try conn.sendFrame(frame);
}

pub fn captureQuicMessage(conn: *quic.Connection, frame: []const u8) !void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    try state.recordMessage(frame);
    conn.requestClose();
}

pub fn rejectUnexpectedQuicMessage(conn: *quic.Connection, frame: []const u8) !void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    try state.recordMessage(frame);
    return error.UnexpectedQuicLoopbackMessage;
}

pub fn recordQuicError(conn: *quic.Connection, err: anyerror) void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    state.last_error = err;
    _ = state.errors.fetchAdd(1, .acq_rel);
    conn.requestClose();
}

pub fn recordQuicClose(conn: *quic.Connection) void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    _ = state.closes.fetchAdd(1, .acq_rel);
}

pub fn echoOrderedQuicMessage(conn: *quic.Connection, frame: []const u8) !void {
    const state: *OrderedQuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    _ = try state.recordExpected(frame);
    try conn.sendFrame(frame);
}

pub fn captureOrderedQuicMessage(conn: *quic.Connection, frame: []const u8) !void {
    const state: *OrderedQuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    const received = try state.recordExpected(frame);
    if (state.close_after_messages != 0 and received >= state.close_after_messages) {
        conn.requestClose();
    }
}

pub fn recordOrderedQuicError(conn: *quic.Connection, err: anyerror) void {
    const state: *OrderedQuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    state.last_error = err;
    _ = state.errors.fetchAdd(1, .acq_rel);
    conn.requestClose();
}

pub fn recordOrderedQuicClose(conn: *quic.Connection) void {
    const state: *OrderedQuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    _ = state.closes.fetchAdd(1, .acq_rel);
}
