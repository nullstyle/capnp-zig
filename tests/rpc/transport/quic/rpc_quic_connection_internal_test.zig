const std = @import("std");
const capnp = @import("capnpc-zig");

const quic = capnp.rpc.transport.quic;
const native_framer = quic.native;

const Net = std.Io.net;
const Connection = quic.Connection;
const TestAccess = quic.testing.ConnectionAccess;
const UdpReceiveBridge = quic.testing.UdpReceiveBridge;
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

fn bindUdpSocket() !Net.Socket {
    const addr: Net.IpAddress = .{ .ip4 = .loopback(0) };
    return try Net.IpAddress.bind(&addr, std.testing.io, .{
        .mode = .dgram,
        .protocol = .udp,
    });
}

test "QUIC UDP receive bridge poll keeps one cancellable receive in flight" {
    var socket = try bindUdpSocket();
    defer socket.close(std.testing.io);

    var bridge = UdpReceiveBridge{};
    defer bridge.cancel(std.testing.io);
    var rx: [64]u8 = undefined;

    try std.testing.expectEqual(
        UdpReceiveBridge.WaitResult.timeout,
        try bridge.receive(std.testing.io, socket, &rx, std.Io.Duration.zero),
    );
    try std.testing.expect(bridge.hasInFlight());
    try std.testing.expectEqual(@as(usize, 0), bridge.cancellationCount());

    // A second poll observes the same still-valid task rather than launching a
    // duplicate or canceling it merely because a timer/poll boundary fired.
    try std.testing.expectEqual(
        UdpReceiveBridge.WaitResult.timeout,
        try bridge.receive(std.testing.io, socket, &rx, std.Io.Duration.zero),
    );
    try std.testing.expect(bridge.hasInFlight());
}

test "QUIC UDP receive bridge handles wake before and during wait" {
    var socket = try bindUdpSocket();
    defer socket.close(std.testing.io);

    var bridge = UdpReceiveBridge{};
    defer bridge.cancel(std.testing.io);
    var rx: [64]u8 = undefined;

    bridge.wake(std.testing.io);
    try std.testing.expectEqual(
        UdpReceiveBridge.WaitResult.wake,
        try bridge.receive(std.testing.io, socket, &rx, std.Io.Duration.fromSeconds(1)),
    );
    try std.testing.expect(bridge.hasInFlight());

    const Waker = struct {
        fn run(target: *UdpReceiveBridge) void {
            std.Io.sleep(std.testing.io, std.Io.Duration.fromMilliseconds(5), .awake) catch {};
            target.wake(std.testing.io);
        }
    };
    var thread = try std.Thread.spawn(.{}, Waker.run, .{&bridge});
    defer thread.join();

    try std.testing.expectEqual(
        UdpReceiveBridge.WaitResult.wake,
        try bridge.receive(std.testing.io, socket, &rx, std.Io.Duration.fromSeconds(1)),
    );
    try std.testing.expect(bridge.hasInFlight());
}

test "QUIC UDP receive bridge completes a pending receive after timer expiry" {
    var receiver = try bindUdpSocket();
    defer receiver.close(std.testing.io);
    var sender = try bindUdpSocket();
    defer sender.close(std.testing.io);

    var bridge = UdpReceiveBridge{};
    defer bridge.cancel(std.testing.io);
    var rx: [64]u8 = undefined;

    try std.testing.expectEqual(
        UdpReceiveBridge.WaitResult.timeout,
        try bridge.receive(std.testing.io, receiver, &rx, std.Io.Duration.fromMilliseconds(1)),
    );
    try std.testing.expect(bridge.hasInFlight());
    try std.testing.expectEqual(@as(usize, 0), bridge.cancellationCount());

    try sender.send(std.testing.io, &receiver.address, "after-timer");
    const result = try bridge.receive(
        std.testing.io,
        receiver,
        &rx,
        std.Io.Duration.fromSeconds(1),
    );
    switch (result) {
        .datagram => |datagram| {
            try std.testing.expectEqualStrings("after-timer", datagram.data);
            try std.testing.expect(!datagram.flags.trunc);
        },
        else => return error.ExpectedUdpDatagram,
    }
    try std.testing.expect(!bridge.hasInFlight());
    try std.testing.expectEqual(@as(usize, 0), bridge.cancellationCount());
}

test "QUIC UDP receive bridge reports truncation without processing partial bytes" {
    var receiver = try bindUdpSocket();
    defer receiver.close(std.testing.io);
    var sender = try bindUdpSocket();
    defer sender.close(std.testing.io);

    var bridge = UdpReceiveBridge{};
    defer bridge.cancel(std.testing.io);
    var rx: [2]u8 = undefined;
    try sender.send(std.testing.io, &receiver.address, "oversized");

    const result = try bridge.receive(
        std.testing.io,
        receiver,
        &rx,
        std.Io.Duration.fromSeconds(1),
    );
    // One outcome on every platform, which is the point: POSIX reports
    // truncation through MSG_TRUNC while Windows fails the receive with
    // MessageOversize. If the Windows arm regressed to propagating that error,
    // this returns it instead and the test fails rather than silently
    // exercising only the POSIX path.
    try std.testing.expectEqual(UdpReceiveBridge.WaitResult.truncated, result);
}

test "QUIC UDP receive bridge retains the launch buffer across a later argument" {
    var receiver = try bindUdpSocket();
    defer receiver.close(std.testing.io);
    var sender = try bindUdpSocket();
    defer sender.close(std.testing.io);

    var bridge = UdpReceiveBridge{};
    defer bridge.cancel(std.testing.io);
    var launch_buffer: [64]u8 = @splat(0xa1);
    var resume_buffer: [64]u8 = @splat(0xb2);

    try std.testing.expectEqual(
        UdpReceiveBridge.WaitResult.timeout,
        try bridge.receive(
            std.testing.io,
            receiver,
            &launch_buffer,
            std.Io.Duration.fromMilliseconds(1),
        ),
    );
    try sender.send(std.testing.io, &receiver.address, "retained-buffer");

    const result = try bridge.receive(
        std.testing.io,
        receiver,
        &resume_buffer,
        std.Io.Duration.fromSeconds(1),
    );
    switch (result) {
        .datagram => |datagram| {
            try std.testing.expectEqual(@intFromPtr(&launch_buffer), @intFromPtr(datagram.data.ptr));
            try std.testing.expectEqualStrings("retained-buffer", datagram.data);
            try std.testing.expectEqual(@as(u8, 0xb2), resume_buffer[0]);
        },
        else => return error.ExpectedUdpDatagram,
    }
}

test "QUIC UDP receive bridge cancellation is exactly once" {
    var socket = try bindUdpSocket();
    defer socket.close(std.testing.io);

    var bridge = UdpReceiveBridge{};
    var rx: [64]u8 = undefined;
    _ = try bridge.receive(std.testing.io, socket, &rx, std.Io.Duration.zero);
    try std.testing.expect(bridge.hasInFlight());

    bridge.cancel(std.testing.io);
    try std.testing.expect(!bridge.hasInFlight());
    try std.testing.expectEqual(@as(usize, 1), bridge.cancellationCount());
    bridge.cancel(std.testing.io);
    try std.testing.expectEqual(@as(usize, 1), bridge.cancellationCount());
}

test "QUIC UDP receive bridge start failure is transactional and retryable" {
    var socket = try bindUdpSocket();
    defer socket.close(std.testing.io);

    var bridge = UdpReceiveBridge{};
    defer bridge.cancel(std.testing.io);
    var rx: [64]u8 = undefined;

    bridge.failNextStartForTesting();
    try std.testing.expectError(
        error.ConcurrencyUnavailable,
        bridge.receive(std.testing.io, socket, &rx, std.Io.Duration.zero),
    );
    try std.testing.expect(!bridge.hasInFlight());
    try std.testing.expectEqual(@as(usize, 0), bridge.cancellationCount());

    try std.testing.expectEqual(
        UdpReceiveBridge.WaitResult.timeout,
        try bridge.receive(std.testing.io, socket, &rx, std.Io.Duration.zero),
    );
    try std.testing.expect(bridge.hasInFlight());
}

test "QUIC connection repeated close and deinit are idempotent" {
    var conn = try initTestClient(std.testing.allocator);
    conn.requestClose();
    conn.requestClose();
    conn.deinit();
    conn.deinit();
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

test "QUIC native sendFrame enforces data stream budgets separately from inline frames" {
    var stream_limited = try initTestNativeClient(std.testing.allocator, .{
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 1,
        .max_pending_data_bytes = 16,
    });
    defer stream_limited.deinit();

    try stream_limited.sendFrame("ab");
    try std.testing.expectError(error.OutboundQueueFull, stream_limited.sendFrame("cd"));
    try stream_limited.sendFrame("x");
    try std.testing.expectEqual(@as(usize, 1), stream_limited.native.outbound.queued_data_items);
    try std.testing.expectEqual(@as(usize, 2), stream_limited.native.outbound.queued_data_bytes);

    var byte_limited = try initTestNativeClient(std.testing.allocator, .{
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 3,
    });
    defer byte_limited.deinit();

    try byte_limited.sendFrame("ab");
    try std.testing.expectError(error.OutboundQueueFull, byte_limited.sendFrame("cd"));
    try std.testing.expectEqual(@as(usize, 1), byte_limited.native.outbound.queued_data_items);
    try std.testing.expectEqual(@as(usize, 2), byte_limited.native.outbound.queued_data_bytes);
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

test "QUIC native rejects invalid data rpc references from control stream" {
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
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 1024,
    });
    defer conn.deinit();

    var state = Harness.State{};
    TestAccess.setCallbacks(&conn, &state, Harness.onMessage, Harness.onError, null);
    conn.native.hello_received = true;

    const local_stream_ref = try native_framer.encodeDataRpc(std.testing.allocator, 0, 2, 4, 64);
    defer std.testing.allocator.free(local_stream_ref);

    try conn.native.inbound.push(local_stream_ref);
    try std.testing.expectError(
        error.InvalidFrame,
        TestAccess.processNativeControlFrames(&conn),
    );
    try std.testing.expectEqual(@as(usize, 0), state.messages);
    try std.testing.expect(conn.native.pending_data == null);
    try std.testing.expectEqual(@as(u64, 0), conn.native.next_in_sequence);
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

    try TestAccess.startNativePendingData(&conn, .{
        .sequence = 0,
        .stream_id = 3,
        .length = 4,
    });
    try std.testing.expectError(error.InvalidFrame, TestAccess.startNativePendingData(&conn, .{
        .sequence = 1,
        .stream_id = 7,
        .length = 4,
    }));
    try std.testing.expect(conn.native.pending_data != null);
    try std.testing.expectEqual(@as(u64, 0), conn.native.pending_data.?.sequence);
    try std.testing.expectEqual(@as(u64, 3), conn.native.pending_data.?.stream_id);
    try std.testing.expectEqual(@as(usize, 4), conn.native.pending_data.?.bytes.len);
}

test "QUIC native aborts a stalled announced data stream at the completion deadline" {
    const deadline_us: u64 = 1_000;

    var conn = try initTestNativeClient(std.testing.allocator, .{
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 64,
        .data_stream_completion_deadline_us = deadline_us,
    });
    defer conn.deinit();

    conn.native.hello_received = true;

    // Announce a peer-initiated (server) unidirectional data stream at t=0 but
    // never open or feed it: the buffer is allocated (std.testing.allocator)
    // and the completion deadline is armed.
    try TestAccess.startNativePendingDataAt(&conn, .{
        .sequence = 0,
        .stream_id = 3,
        .length = 8,
    }, 0);
    try std.testing.expect(conn.native.pending_data != null);
    try std.testing.expectEqual(@as(?u64, deadline_us), conn.native.pending_data.?.deadline_us);

    // Before the deadline the stream is still pending, not aborted.
    try TestAccess.processNativeControlFramesAt(&conn, deadline_us - 1);
    try std.testing.expect(conn.native.pending_data != null);

    // At/after the deadline with no progress the session is aborted with a
    // protocol close and the pending buffer is freed (no std.testing.allocator
    // leak). service() maps the timeout onto terminate_frame_error, so drive
    // that same path here.
    try std.testing.expectError(error.DataStreamTimeout, TestAccess.processNativeControlFramesAt(&conn, deadline_us + 1));
    TestAccess.terminateFrameError(&conn, error.DataStreamTimeout);

    try std.testing.expect(conn.native.pending_data == null);
    try std.testing.expect(conn.isClosing());
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(TestAccess.ApplicationCloseCode.protocol_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.DataStreamTimeout), status.err);
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

test "QUIC native order violations close with frame error status" {
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
    TestAccess.revealCloseDetailOnWire(&conn, true);
    TestAccess.setCallbacks(&conn, &state, Harness.onMessage, Harness.onError, null);
    conn.native.hello_received = true;
    conn.native.next_in_sequence = 1;

    const stale_inline = try native_framer.encodeInlineRpc(std.testing.allocator, 0, "stale", 64);
    defer std.testing.allocator.free(stale_inline);

    try conn.native.inbound.push(stale_inline);
    TestAccess.processNativeControlFrames(&conn) catch |err| {
        switch (err) {
            error.InvalidFrame, error.FrameTooLarge, error.OutOfMemory => TestAccess.terminateFrameError(&conn, err),
            else => return err,
        }
    };

    try std.testing.expect(conn.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), state.last_error);
    try std.testing.expectEqual(@as(u64, 1), conn.native.next_in_sequence);
    const status = conn.closeStatus().?;
    try std.testing.expectEqual(TestAccess.ApplicationCloseCode.frame_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), status.err);
    try std.testing.expect(status.reason_reveals_detail);
    try std.testing.expectEqualStrings("rpc frame error: InvalidFrame", TestAccess.closeReason(&conn));
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
    TestAccess.revealCloseDetailOnWire(&conn, true);
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

test "transient peer faults are per-datagram; local faults stay fatal" {
    // Lives in this root, not beside the classifier in datagram_io.zig, because
    // a `test` block there never runs — `refAllRecursive` does not reach that
    // depth, so an inverted assertion left every gate exiting 0.
    //
    // Enumerates BOTH halves on purpose. Asserting only the tolerated set would
    // still pass if the classifier were widened to swallow everything, which is
    // the failure mode that matters: a local fault silently demoted to "dropped
    // a datagram" turns a broken endpoint into a spin.
    const isTransientPeerFault = quic.testing.isTransientPeerFault;

    // ICMP-driven, remote-influenced, socket still usable. std documents
    // PortUnreachable as ICMP feedback queued against the bound socket and
    // reported at the next receive, so a peer that went away must not be able
    // to tear down an endpoint.
    try std.testing.expect(isTransientPeerFault(error.PortUnreachable));
    try std.testing.expect(isTransientPeerFault(error.ConnectionResetByPeer));

    // Local faults — these must keep propagating out of the connection step.
    try std.testing.expect(!isTransientPeerFault(error.SystemResources));
    try std.testing.expect(!isTransientPeerFault(error.ProcessFdQuotaExceeded));
    try std.testing.expect(!isTransientPeerFault(error.SystemFdQuotaExceeded));
    try std.testing.expect(!isTransientPeerFault(error.SocketUnconnected));
    try std.testing.expect(!isTransientPeerFault(error.NetworkDown));
    try std.testing.expect(!isTransientPeerFault(error.Unexpected));

    // Truncation has its own outcome and must not be routed through here.
    try std.testing.expect(!isTransientPeerFault(error.MessageOversize));
}

// ---------------------------------------------------------------------------
// Early-dispatch posture: the 0-RTT replay window's idempotent prefix.
// ---------------------------------------------------------------------------

const early_dispatch = quic.early_dispatch;
const protocol = capnp.rpc.wire.protocol;
const engine_mod = quic.testing.baseline_engine;

test "early_dispatch restorer id matches the pinned persistence constant" {
    try std.testing.expectEqual(
        capnp.rpc.peer.persistence.restorer_interface_id,
        early_dispatch.restorer_interface_id,
    );
}

fn buildBootstrapFrame(allocator: std.mem.Allocator) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildBootstrap(1);
    // finish() == toBytes(): the caller owns the returned bytes.
    return try builder.finish();
}

fn buildCallFrame(allocator: std.mem.Allocator, interface_id: u64) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(2, interface_id, 0);
    try call.setTargetImportedCap(0);
    _ = try call.initCapTableTyped(0);
    return try builder.finish();
}

test "frameQualifies admits exactly the idempotent prefix vocabulary" {
    const allocator = std.testing.allocator;

    const bootstrap = try buildBootstrapFrame(allocator);
    defer allocator.free(bootstrap);
    try std.testing.expect(early_dispatch.frameQualifies(allocator, bootstrap));

    const restore = try buildCallFrame(allocator, early_dispatch.restorer_interface_id);
    defer allocator.free(restore);
    try std.testing.expect(early_dispatch.frameQualifies(allocator, restore));

    const echo = try buildCallFrame(allocator, 0x5155_4943);
    defer allocator.free(echo);
    try std.testing.expect(!early_dispatch.frameQualifies(allocator, echo));

    // Garbage never qualifies — it holds for the post-handshake decode
    // path, which owns the error handling.
    const garbage = [_]u8{ 0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88 };
    try std.testing.expect(!early_dispatch.frameQualifies(allocator, &garbage));
}

const DispatchRecorder = struct {
    frames: std.ArrayList([]u8) = .empty,
    allocator: std.mem.Allocator,

    fn deinitRecorder(self: *DispatchRecorder) void {
        for (self.frames.items) |f| self.allocator.free(f);
        self.frames.deinit(self.allocator);
    }

    fn isClosing(_: *anyopaque) bool {
        return false;
    }
    fn callbacksReady(_: *anyopaque) bool {
        return true;
    }
    fn dispatch(ptr: *anyopaque, frame: []const u8) anyerror!void {
        const self: *DispatchRecorder = @ptrCast(@alignCast(ptr));
        try self.frames.append(self.allocator, try self.allocator.dupe(u8, frame));
    }
    fn terminate(_: *anyopaque, _: anyerror) void {}
    fn deinitRequested(_: *anyopaque) bool {
        return false;
    }

    fn owner(self: *DispatchRecorder, buf: []u8) engine_mod.Owner {
        return .{
            .ptr = self,
            .allocator = self.allocator,
            .role = .server,
            .observer = null,
            .stream_read_buf = buf,
            .is_closing = isClosing,
            .callbacks_ready = callbacksReady,
            .dispatch_rpc_frame = dispatch,
            .terminate_frame_error = terminate,
            .deinit_requested = deinitRequested,
        };
    }
};

fn pushFramed(engine: *engine_mod.BaselineEngine, frame: []const u8) !void {
    var prefix: [4]u8 = undefined;
    std.mem.writeInt(u32, &prefix, @intCast(frame.len), .little);
    try engine.inbound.push(&prefix);
    try engine.inbound.push(frame);
}

test "restore_only dispatches the idempotent prefix early and parks the rest in order" {
    const allocator = std.testing.allocator;
    var engine = engine_mod.BaselineEngine.init(allocator, 1 << 20, 64, 1 << 20, false, true, .restore_only);
    defer engine.deinit(allocator);

    var recorder = DispatchRecorder{ .allocator = allocator };
    defer recorder.deinitRecorder();
    var scratch: [64]u8 = undefined;
    const owner = recorder.owner(&scratch);

    const bootstrap = try buildBootstrapFrame(allocator);
    defer allocator.free(bootstrap);
    const restore = try buildCallFrame(allocator, early_dispatch.restorer_interface_id);
    defer allocator.free(restore);
    const echo_a = try buildCallFrame(allocator, 0xaaaa);
    defer allocator.free(echo_a);
    const echo_b = try buildCallFrame(allocator, 0xbbbb);
    defer allocator.free(echo_b);

    try pushFramed(&engine, bootstrap);
    try pushFramed(&engine, restore);
    try pushFramed(&engine, echo_a);
    try pushFramed(&engine, echo_b);

    // Inside the replay window: only the idempotent prefix executes.
    engine.hold_dispatch = true;
    try engine.dispatchAvailableFrames(owner);
    try std.testing.expectEqual(@as(usize, 2), recorder.frames.items.len);
    try std.testing.expectEqualSlices(u8, bootstrap, recorder.frames.items[0]);
    try std.testing.expectEqualSlices(u8, restore, recorder.frames.items[1]);
    try std.testing.expect(engine.held_frame != null);

    // Re-servicing inside the window must not leak past the parked frame.
    try engine.dispatchAvailableFrames(owner);
    try std.testing.expectEqual(@as(usize, 2), recorder.frames.items.len);

    // Handshake lands: the parked frame drains FIRST, then the framer, so
    // wire order is preserved end to end.
    engine.hold_dispatch = false;
    try engine.dispatchAvailableFrames(owner);
    try std.testing.expectEqual(@as(usize, 4), recorder.frames.items.len);
    try std.testing.expectEqualSlices(u8, echo_a, recorder.frames.items[2]);
    try std.testing.expectEqualSlices(u8, echo_b, recorder.frames.items[3]);
    try std.testing.expect(engine.held_frame == null);
}

test "hold_until_handshake dispatches nothing inside the window" {
    const allocator = std.testing.allocator;
    var engine = engine_mod.BaselineEngine.init(allocator, 1 << 20, 64, 1 << 20, false, true, .hold_until_handshake);
    defer engine.deinit(allocator);

    var recorder = DispatchRecorder{ .allocator = allocator };
    defer recorder.deinitRecorder();
    var scratch: [64]u8 = undefined;
    const owner = recorder.owner(&scratch);

    const restore = try buildCallFrame(allocator, early_dispatch.restorer_interface_id);
    defer allocator.free(restore);
    try pushFramed(&engine, restore);

    engine.hold_dispatch = true;
    try engine.dispatchAvailableFrames(owner);
    try std.testing.expectEqual(@as(usize, 0), recorder.frames.items.len);

    engine.hold_dispatch = false;
    try engine.dispatchAvailableFrames(owner);
    try std.testing.expectEqual(@as(usize, 1), recorder.frames.items.len);
}

test "warm-state envelope round-trips and rejects every malformed shape" {
    const allocator = std.testing.allocator;
    const ws = quic.warm_state;

    const encoded = try ws.encode(allocator, "ticket-bytes", "token-bytes");
    defer allocator.free(encoded);
    const decoded = try ws.decode(encoded);
    try std.testing.expectEqualSlices(u8, "ticket-bytes", decoded.ticket);
    try std.testing.expectEqualSlices(u8, "token-bytes", decoded.token);

    // Empty token is legal (quic-zig delivers it separately and later).
    const no_token = try ws.encode(allocator, "t", "");
    defer allocator.free(no_token);
    try std.testing.expectEqual(@as(usize, 0), (try ws.decode(no_token)).token.len);

    // Truncations, bad version, and trailing garbage all fail closed.
    try std.testing.expectError(error.InvalidWarmState, ws.decode(""));
    try std.testing.expectError(error.InvalidWarmState, ws.decode(encoded[0..4]));
    try std.testing.expectError(error.InvalidWarmState, ws.decode(encoded[0 .. encoded.len - 1]));
    var bad_version = try allocator.dupe(u8, encoded);
    defer allocator.free(bad_version);
    bad_version[0] = 0xff;
    try std.testing.expectError(error.InvalidWarmState, ws.decode(bad_version));
    const trailing = try std.mem.concat(allocator, u8, &.{ encoded, "x" });
    defer allocator.free(trailing);
    try std.testing.expectError(error.InvalidWarmState, ws.decode(trailing));
    // A ticket length that overruns the buffer must not be believed.
    var huge_len = try allocator.dupe(u8, encoded);
    defer allocator.free(huge_len);
    std.mem.writeInt(u32, huge_len[2..6], 0xffff_ffff, .little);
    try std.testing.expectError(error.InvalidWarmState, ws.decode(huge_len));
}

test "a frame parked at engine deinit does not leak" {
    const allocator = std.testing.allocator;
    var engine = engine_mod.BaselineEngine.init(allocator, 1 << 20, 64, 1 << 20, false, true, .restore_only);

    var recorder = DispatchRecorder{ .allocator = allocator };
    defer recorder.deinitRecorder();
    var scratch: [64]u8 = undefined;
    const owner = recorder.owner(&scratch);

    const echo = try buildCallFrame(allocator, 0xcccc);
    defer allocator.free(echo);
    try pushFramed(&engine, echo);

    engine.hold_dispatch = true;
    try engine.dispatchAvailableFrames(owner);
    try std.testing.expectEqual(@as(usize, 0), recorder.frames.items.len);
    try std.testing.expect(engine.held_frame != null);
    // std.testing.allocator fails the test on leak if deinit forgets it.
    engine.deinit(allocator);
}
