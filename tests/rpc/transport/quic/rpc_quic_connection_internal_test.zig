const std = @import("std");
const capnp = @import("capnpc-zig");

const quic = capnp.rpc.quic;
const native_framer = quic.native;

const Net = std.Io.net;
const Connection = quic.Connection;
const TestAccess = quic.testing.ConnectionAccess;
const NativeOptions = TestAccess.NativeOptions;

fn testRemoteAddr() Net.IpAddress {
    return .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 4433,
    } };
}

fn initTestClient(allocator: std.mem.Allocator) !Connection {
    return try Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = testRemoteAddr(),
        .server_name = "localhost",
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
    });
}

fn initTestNativeClient(allocator: std.mem.Allocator, native_options: NativeOptions) !Connection {
    return try Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = testRemoteAddr(),
        .server_name = "localhost",
        .receive_timeout = std.Io.Duration.fromMilliseconds(5),
        .mode = .native,
        .native = native_options,
    });
}

test "QUIC sendFrame requests scheduler wake" {
    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();

    try std.testing.expect(!TestAccess.wakeRequested(&conn));
    try conn.sendFrame("abc");
    try std.testing.expect(TestAccess.wakeRequested(&conn));
    try std.testing.expect(!conn.baseline.outbound.isEmpty());
    try std.testing.expect(TestAccess.consumeWakeRequested(&conn));
    try std.testing.expect(!TestAccess.wakeRequested(&conn));
}

test "QUIC native sendFrame uses native outbound queue" {
    var conn = try initTestNativeClient(std.testing.allocator, .{});
    defer conn.deinit();

    try conn.sendFrame("abc");
    try std.testing.expect(TestAccess.wakeRequested(&conn));
    try std.testing.expect(conn.baseline.outbound.isEmpty());
    try std.testing.expect(!conn.native.outbound.isEmpty());
}

test "QUIC native control dispatch preserves order behind pending data stream" {
    const Harness = struct {
        const State = struct {
            messages: usize = 0,
        };

        fn onMessage(conn: *Connection, _: []const u8) !void {
            const state: *State = @ptrCast(@alignCast(conn.context().?));
            state.messages += 1;
        }

        fn onError(_: *Connection, _: anyerror) void {}
    };

    var conn = try initTestNativeClient(std.testing.allocator, .{
        .inline_frame_threshold = 16,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 1024,
    });
    defer conn.deinit();

    var state = Harness.State{};
    TestAccess.setCallbacks(&conn, &state, Harness.onMessage, Harness.onError, null);
    conn.native.hello_received = true;

    const data_control = try native_framer.encodeDataRpc(std.testing.allocator, 0, 3, 8, 64);
    defer std.testing.allocator.free(data_control);
    const inline_control = try native_framer.encodeInlineRpc(std.testing.allocator, 1, "later", 64);
    defer std.testing.allocator.free(inline_control);

    try conn.native.inbound.push(data_control);
    try conn.native.inbound.push(inline_control);
    try TestAccess.processNativeControlFrames(&conn);

    try std.testing.expectEqual(@as(usize, 0), state.messages);
    try std.testing.expect(conn.native.pending_data != null);
    try std.testing.expectEqual(@as(u64, 0), conn.native.next_in_sequence);
    try std.testing.expect(conn.native.inbound.buffer.items.len > 0);
}

test "QUIC native control rejects missing and duplicate hello frames" {
    {
        var conn = try initTestNativeClient(std.testing.allocator, .{});
        defer conn.deinit();

        const inline_control = try native_framer.encodeInlineRpc(std.testing.allocator, 0, "rpc", 64);
        defer std.testing.allocator.free(inline_control);

        try conn.native.inbound.push(inline_control);
        try std.testing.expectError(
            error.InvalidFrame,
            TestAccess.processNativeControlFrames(&conn),
        );
    }

    {
        var conn = try initTestNativeClient(std.testing.allocator, .{});
        defer conn.deinit();

        var hello: [native_framer.encodedHelloLen()]u8 = undefined;
        const hello_len = try native_framer.encodeHello(&hello);

        try conn.native.inbound.push(hello[0..hello_len]);
        try TestAccess.processNativeControlFrames(&conn);
        try std.testing.expect(conn.native.hello_received);

        try conn.native.inbound.push(hello[0..hello_len]);
        try std.testing.expectError(
            error.InvalidFrame,
            TestAccess.processNativeControlFrames(&conn),
        );
    }
}

test "QUIC native control rejects non-monotonic rpc sequences" {
    var conn = try initTestNativeClient(std.testing.allocator, .{});
    defer conn.deinit();
    conn.native.hello_received = true;
    conn.native.next_in_sequence = 1;

    const stale_inline = try native_framer.encodeInlineRpc(std.testing.allocator, 0, "stale", 64);
    defer std.testing.allocator.free(stale_inline);

    try conn.native.inbound.push(stale_inline);
    try std.testing.expectError(
        error.InvalidFrame,
        TestAccess.processNativeControlFrames(&conn),
    );
    try std.testing.expectEqual(@as(u64, 1), conn.native.next_in_sequence);
}

test "QUIC native validates data stream references and budgets" {
    var conn = try initTestNativeClient(std.testing.allocator, .{
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 8,
    });
    defer conn.deinit();

    try std.testing.expectError(error.InvalidFrame, TestAccess.startNativePendingData(&conn, .{
        .sequence = 0,
        .stream_id = 0,
        .length = 4,
    }));
    try std.testing.expectError(error.InvalidFrame, TestAccess.startNativePendingData(&conn, .{
        .sequence = 0,
        .stream_id = 2,
        .length = 4,
    }));
    try std.testing.expectError(error.FrameTooLarge, TestAccess.startNativePendingData(&conn, .{
        .sequence = 0,
        .stream_id = 3,
        .length = 0,
    }));
    try std.testing.expectError(error.FrameTooLarge, TestAccess.startNativePendingData(&conn, .{
        .sequence = 0,
        .stream_id = 3,
        .length = 9,
    }));
    try std.testing.expect(conn.native.pending_data == null);
}

test "QUIC native frame errors record typed terminal close status" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.context().?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var state = Harness.State{};
    var conn = try initTestNativeClient(std.testing.allocator, .{});
    defer conn.deinit();
    TestAccess.setCallbacks(&conn, &state, Harness.onMessage, Harness.onError, null);

    const pending_bytes = try std.testing.allocator.alloc(u8, 4);
    conn.native.pending_data = .{
        .sequence = 0,
        .stream_id = 3,
        .bytes = pending_bytes,
    };

    TestAccess.terminateFrameError(&conn, error.InvalidFrame);

    try std.testing.expect(conn.isClosing());
    try std.testing.expect(conn.native.pending_data == null);
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(TestAccess.ApplicationCloseCode.frame_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), status.err);
    try std.testing.expectEqualStrings("rpc frame error", TestAccess.closeReason(&conn));
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC native control allocation OOM is terminal when serviced" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.context().?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var state = Harness.State{};
    var conn = try initTestNativeClient(std.testing.allocator, .{});
    defer conn.deinit();
    TestAccess.resetNativeInbound(&conn, failing.allocator(), 64, 64);
    TestAccess.setCallbacks(&conn, &state, Harness.onMessage, Harness.onError, null);
    conn.native.hello_received = true;

    const inline_control = try native_framer.encodeInlineRpc(std.testing.allocator, 0, "abc", 64);
    defer std.testing.allocator.free(inline_control);

    try conn.native.inbound.push(inline_control);
    TestAccess.processNativeControlFrames(&conn) catch |err| {
        switch (err) {
            error.InvalidFrame, error.FrameTooLarge, error.OutOfMemory => TestAccess.terminateFrameError(&conn, err),
            else => return err,
        }
    };

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(TestAccess.ApplicationCloseCode.internal_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), status.err);
    try std.testing.expectEqualStrings("rpc transport error", TestAccess.closeReason(&conn));
    try std.testing.expectEqual(@as(usize, 0), conn.native.inbound.buffer.items.len);
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC dispatch treats malformed frames as terminal" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.context().?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var state = Harness.State{};
    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();
    TestAccess.setCallbacks(&conn, &state, Harness.onMessage, Harness.onError, null);

    const bad_frame = [_]u8{ 0, 0, 0, 0 };
    try conn.baseline.inbound.push(&bad_frame);
    try TestAccess.dispatchBaselineFrames(&conn);

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(TestAccess.ApplicationCloseCode.frame_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), status.err);
    try std.testing.expectEqualStrings("rpc frame error", TestAccess.closeReason(&conn));
    try std.testing.expect(!status.reason_reveals_detail);
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC dispatch can reveal sanitized frame error details when configured" {
    const Harness = struct {
        fn onMessage(_: *Connection, _: []const u8) !void {}
        fn onError(_: *Connection, _: anyerror) void {}
    };

    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();
    conn.close_state.reveal_detail_on_wire = true;
    TestAccess.setCallbacks(&conn, null, Harness.onMessage, Harness.onError, null);

    const bad_frame = [_]u8{ 0, 0, 0, 0 };
    try conn.baseline.inbound.push(&bad_frame);
    try TestAccess.dispatchBaselineFrames(&conn);

    const status = conn.closeStatus().?;
    try std.testing.expect(status.reason_reveals_detail);
    try std.testing.expectEqualStrings("rpc frame error: InvalidFrame", TestAccess.closeReason(&conn));
}

test "QUIC dispatch treats inbound frame allocation OOM as terminal" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.context().?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var state = Harness.State{};
    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();
    TestAccess.resetBaselineInbound(&conn, failing.allocator(), 1024);
    TestAccess.setCallbacks(&conn, &state, Harness.onMessage, Harness.onError, null);

    var encoded: [7]u8 = undefined;
    std.mem.writeInt(u32, encoded[0..4], 3, .little);
    @memcpy(encoded[4..7], "abc");

    try conn.baseline.inbound.push(&encoded);
    try TestAccess.dispatchBaselineFrames(&conn);

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(TestAccess.ApplicationCloseCode.internal_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), status.err);
    try std.testing.expectEqualStrings("rpc transport error", TestAccess.closeReason(&conn));
    try std.testing.expectEqual(@as(usize, 0), conn.baseline.inbound.buffer.items.len);
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC dispatch treats message callback failure as terminal" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {
            return error.CallbackFailed;
        }

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.context().?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var state = Harness.State{};
    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();
    TestAccess.setCallbacks(&conn, &state, Harness.onMessage, Harness.onError, null);

    var encoded: [7]u8 = undefined;
    std.mem.writeInt(u32, encoded[0..4], 3, .little);
    @memcpy(encoded[4..7], "abc");

    try conn.baseline.inbound.push(&encoded);
    try TestAccess.dispatchBaselineFrames(&conn);

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.CallbackFailed), state.last_error);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(TestAccess.ApplicationCloseCode.peer_callback_failure, status.code);
    try std.testing.expectEqualStrings("rpc callback error", TestAccess.closeReason(&conn));
    try std.testing.expectError(error.BrokenPipe, conn.sendFrame("late"));
}

test "QUIC deinit requested from error callback is deferred without panic" {
    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            close_count: usize = 0,
            deinit_seen_in_error: bool = false,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, _: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.context().?));
            state.error_count += 1;
            conn.deinit();
            state.deinit_seen_in_error = TestAccess.deinitRequested(conn);
        }

        fn onClose(conn: *Connection) void {
            const state: *State = @ptrCast(@alignCast(conn.context().?));
            state.close_count += 1;
        }
    };

    var state = Harness.State{};
    var conn = try initTestClient(std.testing.allocator);
    defer conn.deinit();
    TestAccess.setCallbacks(&conn, &state, Harness.onMessage, Harness.onError, Harness.onClose);

    TestAccess.terminateFrameError(&conn, error.InvalidFrame);
    if (TestAccess.closeCallback(&conn)) |cb| TestAccess.invokeCloseCallback(&conn, cb);

    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(usize, 1), state.close_count);
    try std.testing.expect(state.deinit_seen_in_error);
    try std.testing.expect(TestAccess.deinitRequested(&conn));
}
