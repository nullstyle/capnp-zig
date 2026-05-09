const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.protocol;
const quic = capnpc.rpc.quic;

const loopback_cert_pem = @embedFile("loopback_cert.pem");
const loopback_key_pem = @embedFile("loopback_key.pem");
const loopback_timeout_ms: u64 = 3_000;
const loopback_poll_ms: u64 = 5;

fn testListenAddr() std.Io.net.IpAddress {
    return .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 0,
    } };
}

fn captureServerLog(_: ?*anyopaque, _: quic.ServerLogEvent) void {}

const QuicEndpointState = struct {
    messages: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    errors: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    last_error: ?anyerror = null,
    received: [256]u8 = undefined,
    received_len: usize = 0,

    fn recordMessage(self: *QuicEndpointState, frame: []const u8) !void {
        if (frame.len > self.received.len) return error.QuicLoopbackPayloadTooLarge;
        @memcpy(self.received[0..frame.len], frame);
        self.received_len = frame.len;
        _ = self.messages.fetchAdd(1, .acq_rel);
    }

    fn receivedSlice(self: *const QuicEndpointState) []const u8 {
        return self.received[0..self.received_len];
    }
};

fn buildBootstrapFrame(allocator: std.mem.Allocator, question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildBootstrap(question_id);
    return builder.finish();
}

fn sleepMs(ms: u64) void {
    std.Io.sleep(std.testing.io, std.Io.Duration.fromMilliseconds(@intCast(ms)), .awake) catch {};
}

fn runQuicConnection(conn: *quic.Connection) void {
    conn.run();
}

fn waitForClientMessageOrError(
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

fn waitForServerError(server_state: *const QuicEndpointState) bool {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback_timeout_ms) : (waited_ms += loopback_poll_ms) {
        if (server_state.errors.load(.acquire) > 0) return true;
        sleepMs(loopback_poll_ms);
    }
    return server_state.errors.load(.acquire) > 0;
}

fn echoQuicMessage(conn: *quic.Connection, frame: []const u8) !void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.ctx.?));
    try state.recordMessage(frame);
    try conn.sendFrame(frame);
}

fn captureQuicMessage(conn: *quic.Connection, frame: []const u8) !void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.ctx.?));
    try state.recordMessage(frame);
    conn.requestClose();
}

fn rejectUnexpectedQuicMessage(conn: *quic.Connection, frame: []const u8) !void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.ctx.?));
    try state.recordMessage(frame);
    return error.UnexpectedQuicLoopbackMessage;
}

fn recordQuicError(conn: *quic.Connection, err: anyerror) void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.ctx.?));
    state.last_error = err;
    _ = state.errors.fetchAdd(1, .acq_rel);
    conn.requestClose();
}

fn recordQuicClose(conn: *quic.Connection) void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.ctx.?));
    _ = state.closes.fetchAdd(1, .acq_rel);
}

test "quic transport exposes native Cap'n Proto RPC ALPN" {
    try std.testing.expectEqualStrings("capnp-rpc/1", quic.alpn);
    try std.testing.expectEqual(@as(u64, 0), quic.baseline_stream_id);
    const default_client_options = quic.ClientOptions{
        .remote_addr = testListenAddr(),
        .server_name = "localhost",
    };
    const default_server_options = quic.ServerOptions{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
    };
    try std.testing.expectEqual(quic.TransportMode.baseline, default_client_options.mode);
    try std.testing.expectEqual(quic.TransportMode.baseline, default_server_options.mode);
    try std.testing.expect(quic.default_native_inline_frame_threshold > 0);
    try std.testing.expect(quic.default_native_max_control_frame_bytes > quic.default_native_inline_frame_threshold);
    try std.testing.expect(quic.default_native_max_pending_data_streams > 0);
    try std.testing.expect(quic.default_max_outbound_queue_items > 0);
    try std.testing.expect(quic.default_max_outbound_queue_bytes > quic.default_max_message_bytes);
    try std.testing.expectEqual(@as(u32, 1), quic.supported_max_concurrent_sessions);
}

test "quic transport exposes typed application close policy" {
    try std.testing.expectEqual(@as(u64, 0), @intFromEnum(quic.ApplicationCloseCode.normal));
    try std.testing.expectEqual(@as(u64, 0x434e_5001), @intFromEnum(quic.ApplicationCloseCode.frame_error));

    var reason_buf: [8]u8 = undefined;
    const prepared = quic.close.sanitizeReason(&reason_buf, "bad\nframe!");

    try std.testing.expect(prepared.truncated);
    try std.testing.expectEqualStrings("bad?fram", reason_buf[0..prepared.len]);
}

test "quic exposes listener and session API boundary" {
    try std.testing.expect(@hasDecl(quic, "Listener"));
    try std.testing.expect(@hasDecl(quic, "Session"));
    try std.testing.expect(@hasDecl(quic.listener, "Listener"));
    try std.testing.expect(@hasDecl(quic.session, "Session"));
    try std.testing.expect(@hasDecl(quic.endpoint, "Endpoint"));
}

test "quic server options propagate quic_zig hardening controls" {
    const retry_key: quic.ServerRetryTokenKey = @splat(0x11);
    const new_token_key: quic.ServerNewTokenKey = @splat(0x22);
    var log_user_data: u8 = 0;

    const config = try quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .local_cid_len = 12,
        .log_callback = captureServerLog,
        .log_user_data = &log_user_data,
        .max_initials_per_source_per_window = 32,
        .source_rate_window_us = 123_000,
        .source_rate_table_capacity = 256,
        .max_vn_per_source_per_window = 7,
        .retry_token_key = retry_key,
        .retry_token_lifetime_us = 456_000,
        .retry_state_table_capacity = 64,
        .new_token_key = new_token_key,
        .new_token_lifetime_us = 789_000,
        .enable_0rtt = true,
        .reveal_close_reason_on_wire = true,
        .max_connection_memory = 4 * 1024 * 1024,
        .max_datagrams_per_window = 100,
        .max_bytes_per_window = 64 * 1024,
        .listener_rate_window_us = 42_000,
        .max_bytes_per_source_per_second = 32 * 1024,
        .max_log_events_per_source_per_window = 5,
    });

    try std.testing.expectEqual(@as(u8, 12), config.local_cid_len);
    try std.testing.expect(config.log_callback != null);
    try std.testing.expectEqual(@intFromPtr(&log_user_data), @intFromPtr(config.log_user_data.?));
    try std.testing.expectEqual(@as(?u32, 32), config.max_initials_per_source_per_window);
    try std.testing.expectEqual(@as(u64, 123_000), config.source_rate_window_us);
    try std.testing.expectEqual(@as(u32, 256), config.source_rate_table_capacity);
    try std.testing.expectEqual(@as(?u32, 7), config.max_vn_per_source_per_window);
    try std.testing.expectEqual(retry_key, config.retry_token_key.?);
    try std.testing.expectEqual(@as(u64, 456_000), config.retry_token_lifetime_us);
    try std.testing.expectEqual(@as(u32, 64), config.retry_state_table_capacity);
    try std.testing.expectEqual(new_token_key, config.new_token_key.?);
    try std.testing.expectEqual(@as(u64, 789_000), config.new_token_lifetime_us);
    try std.testing.expect(config.enable_0rtt);
    try std.testing.expect(config.reveal_close_reason_on_wire);
    try std.testing.expectEqual(@as(u64, 4 * 1024 * 1024), config.max_connection_memory);
    try std.testing.expectEqual(@as(?u32, 100), config.max_datagrams_per_window);
    try std.testing.expectEqual(@as(?u64, 64 * 1024), config.max_bytes_per_window);
    try std.testing.expectEqual(@as(u64, 42_000), config.listener_rate_window_us);
    try std.testing.expectEqual(@as(?u64, 32 * 1024), config.max_bytes_per_source_per_second);
    try std.testing.expectEqual(@as(?u32, 5), config.max_log_events_per_source_per_window);
}

test "quic production hardening preset enables retry and rate gates" {
    const retry_key: quic.ServerRetryTokenKey = @splat(0x33);
    const new_token_key: quic.ServerNewTokenKey = @splat(0x44);

    const options = quic.withProductionServerHardening(.{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .enable_0rtt = true,
        .reveal_close_reason_on_wire = true,
    }, .{
        .retry_token_key = retry_key,
        .new_token_key = new_token_key,
    });

    try std.testing.expectEqual(retry_key, options.retry_token_key.?);
    try std.testing.expectEqual(new_token_key, options.new_token_key.?);
    try std.testing.expectEqual(@as(?u32, 32), options.max_initials_per_source_per_window);
    try std.testing.expect(options.max_datagrams_per_window.? > 0);
    try std.testing.expect(options.max_bytes_per_window.? > 0);
    try std.testing.expect(options.max_bytes_per_source_per_second.? > 0);
    try std.testing.expect(!options.enable_0rtt);
    try std.testing.expect(!options.reveal_close_reason_on_wire);

    const config = try quic.serverConfigFromOptions(std.testing.allocator, options);
    try std.testing.expectEqual(retry_key, config.retry_token_key.?);
    try std.testing.expectEqual(new_token_key, config.new_token_key.?);
    try std.testing.expectEqual(options.max_initials_per_source_per_window, config.max_initials_per_source_per_window);
    try std.testing.expectEqual(options.max_datagrams_per_window, config.max_datagrams_per_window);
    try std.testing.expectEqual(options.max_bytes_per_window, config.max_bytes_per_window);
}

test "quic length-delimited framer handles fragmented and coalesced payloads" {
    var framer = quic.LengthDelimitedFramer.init(std.testing.allocator, 1024);
    defer framer.deinit();

    var bytes: [16]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 3, .little);
    @memcpy(bytes[4..7], "abc");
    std.mem.writeInt(u32, bytes[7..11], 5, .little);
    @memcpy(bytes[11..16], "hello");

    try framer.push(bytes[0..5]);
    try std.testing.expect(try framer.popFrame() == null);
    try framer.push(bytes[5..]);

    const first = (try framer.popFrame()).?;
    defer std.testing.allocator.free(first);
    try std.testing.expectEqualStrings("abc", first);

    const second = (try framer.popFrame()).?;
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualStrings("hello", second);
    try std.testing.expect(try framer.popFrame() == null);
}

test "quic path address conversion round-trips IPv4" {
    const addr: std.Io.net.IpAddress = .{ .ip4 = .{
        .bytes = .{ 10, 20, 30, 40 },
        .port = 4321,
    } };

    const path_addr = quic.ipAddressToPathAddress(addr);
    const round_trip = quic.pathAddressToIpAddress(path_addr).?;

    try std.testing.expect(round_trip == .ip4);
    try std.testing.expectEqual(addr.ip4.port, round_trip.ip4.port);
    try std.testing.expectEqualSlices(u8, &addr.ip4.bytes, &round_trip.ip4.bytes);
}

test "quic localhost connection exchanges framed RPC bootstrap payload" {
    const allocator = std.testing.allocator;
    const frame = try buildBootstrapFrame(allocator, 0xC0DE);
    defer allocator.free(frame);

    var server = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
    });
    defer server.deinit();

    const server_addr = server.getAddress();
    try std.testing.expect(server_addr == .ip4);
    try std.testing.expect(server_addr.ip4.port != 0);

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
    });
    defer client.deinit();

    var server_state = QuicEndpointState{};
    var client_state = QuicEndpointState{};
    server.start(&server_state, echoQuicMessage, recordQuicError, recordQuicClose);
    client.start(&client_state, captureQuicMessage, recordQuicError, recordQuicClose);

    try client.sendFrame(frame);

    var server_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&server});
    var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        server.requestClose();
        client_thread.join();
        server_thread.join();
    };

    const exchanged = waitForClientMessageOrError(&client_state, &server_state);
    client.requestClose();
    server.requestClose();
    client_thread.join();
    server_thread.join();
    joined = true;

    if (!exchanged) return error.QuicLoopbackTimedOut;

    try std.testing.expectEqual(@as(usize, 1), server_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), client_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), client_state.errors.load(.acquire));
    try std.testing.expect(client_state.closes.load(.acquire) > 0);
    try std.testing.expect(server_state.closes.load(.acquire) > 0);
    try std.testing.expectEqualSlices(u8, frame, client_state.receivedSlice());

    var decoded = try protocol.DecodedMessage.init(allocator, client_state.receivedSlice());
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.bootstrap, decoded.tag);
    const bootstrap = try decoded.asBootstrap();
    try std.testing.expectEqual(@as(u32, 0xC0DE), bootstrap.question_id);
}

test "quic native localhost connection exchanges inline RPC bootstrap payload" {
    const allocator = std.testing.allocator;
    const frame = try buildBootstrapFrame(allocator, 0xCAFE);
    defer allocator.free(frame);

    var server = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
    });
    defer server.deinit();

    const server_addr = server.getAddress();
    try std.testing.expect(server_addr == .ip4);
    try std.testing.expect(server_addr.ip4.port != 0);

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
    });
    defer client.deinit();

    var server_state = QuicEndpointState{};
    var client_state = QuicEndpointState{};
    server.start(&server_state, echoQuicMessage, recordQuicError, recordQuicClose);
    client.start(&client_state, captureQuicMessage, recordQuicError, recordQuicClose);

    try client.sendFrame(frame);

    var server_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&server});
    var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        server.requestClose();
        client_thread.join();
        server_thread.join();
    };

    const exchanged = waitForClientMessageOrError(&client_state, &server_state);
    client.requestClose();
    server.requestClose();
    client_thread.join();
    server_thread.join();
    joined = true;

    if (!exchanged) return error.QuicLoopbackTimedOut;

    try std.testing.expectEqual(@as(usize, 1), server_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), client_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), client_state.errors.load(.acquire));
    try std.testing.expectEqualSlices(u8, frame, client_state.receivedSlice());
}

test "quic native localhost routes large RPC frame over data stream" {
    const allocator = std.testing.allocator;
    const frame = try buildBootstrapFrame(allocator, 0xDADA);
    defer allocator.free(frame);

    const native_options = quic.NativeOptions{
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 4096,
    };

    var server = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer server.deinit();

    const server_addr = server.getAddress();
    try std.testing.expect(server_addr == .ip4);
    try std.testing.expect(server_addr.ip4.port != 0);

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer client.deinit();

    var server_state = QuicEndpointState{};
    var client_state = QuicEndpointState{};
    server.start(&server_state, echoQuicMessage, recordQuicError, recordQuicClose);
    client.start(&client_state, captureQuicMessage, recordQuicError, recordQuicClose);

    try client.sendFrame(frame);

    var server_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&server});
    var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        server.requestClose();
        client_thread.join();
        server_thread.join();
    };

    const exchanged = waitForClientMessageOrError(&client_state, &server_state);
    client.requestClose();
    server.requestClose();
    client_thread.join();
    server_thread.join();
    joined = true;

    if (!exchanged) return error.QuicLoopbackTimedOut;

    try std.testing.expectEqual(@as(usize, 1), server_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), client_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), client_state.errors.load(.acquire));
    try std.testing.expectEqualSlices(u8, frame, client_state.receivedSlice());
}

test "quic native mode mismatch closes baseline peer cleanly" {
    const allocator = std.testing.allocator;
    const frame = try buildBootstrapFrame(allocator, 0xBAD);
    defer allocator.free(frame);

    var server = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
    });
    defer server.deinit();

    const server_addr = server.getAddress();
    try std.testing.expect(server_addr == .ip4);
    try std.testing.expect(server_addr.ip4.port != 0);

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
    });
    defer client.deinit();

    var server_state = QuicEndpointState{};
    var client_state = QuicEndpointState{};
    server.start(&server_state, rejectUnexpectedQuicMessage, recordQuicError, recordQuicClose);
    client.start(&client_state, captureQuicMessage, recordQuicError, recordQuicClose);

    try client.sendFrame(frame);

    var server_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&server});
    var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        server.requestClose();
        client_thread.join();
        server_thread.join();
    };

    const rejected = waitForServerError(&server_state);
    client.requestClose();
    server.requestClose();
    client_thread.join();
    server_thread.join();
    joined = true;

    if (!rejected) return error.QuicLoopbackTimedOut;

    try std.testing.expectEqual(@as(usize, 0), server_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), server_state.errors.load(.acquire));
    const last_error = server_state.last_error orelse return error.QuicLoopbackMissingError;
    try std.testing.expect(last_error == error.FrameTooLarge or last_error == error.InvalidFrame);
    try std.testing.expect(server.isClosing());
}

test "quic localhost oversized baseline frame terminates server" {
    const allocator = std.testing.allocator;

    var server = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .stream_read_buffer_size = 128,
        .max_message_bytes = 32,
    });
    defer server.deinit();

    const server_addr = server.getAddress();
    try std.testing.expect(server_addr == .ip4);
    try std.testing.expect(server_addr.ip4.port != 0);

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .max_message_bytes = 128,
        .max_outbound_queue_bytes = quic.length_prefix_bytes + 128,
    });
    defer client.deinit();

    var oversized_payload: [64]u8 = @splat(0xa5);
    try client.sendFrame(&oversized_payload);

    var server_state = QuicEndpointState{};
    var client_state = QuicEndpointState{};
    server.start(&server_state, rejectUnexpectedQuicMessage, recordQuicError, recordQuicClose);
    client.start(&client_state, captureQuicMessage, recordQuicError, recordQuicClose);

    var server_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&server});
    var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        server.requestClose();
        client_thread.join();
        server_thread.join();
    };

    const rejected = waitForServerError(&server_state);
    client.requestClose();
    server.requestClose();
    client_thread.join();
    server_thread.join();
    joined = true;

    if (!rejected) return error.QuicLoopbackTimedOut;

    try std.testing.expectEqual(@as(usize, 0), server_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), server_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(?anyerror, error.FrameTooLarge), server_state.last_error);
    try std.testing.expect(server_state.closes.load(.acquire) > 0);
    try std.testing.expect(server.isClosing());
}

test "quic client outbound queue enforces item and byte bounds" {
    const remote_addr: std.Io.net.IpAddress = .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 4433,
    } };

    var item_limited = try quic.Connection.initClient(std.testing.allocator, std.testing.io, .{
        .remote_addr = remote_addr,
        .server_name = "localhost",
        .max_outbound_queue_items = 1,
        .max_outbound_queue_bytes = 1024,
    });
    defer item_limited.deinit();

    try item_limited.sendFrame("abc");
    try std.testing.expectError(error.OutboundQueueFull, item_limited.sendFrame("def"));

    var byte_limited = try quic.Connection.initClient(std.testing.allocator, std.testing.io, .{
        .remote_addr = remote_addr,
        .server_name = "localhost",
        .max_outbound_queue_items = 4,
        .max_outbound_queue_bytes = 8,
    });
    defer byte_limited.deinit();

    try byte_limited.sendFrame("abcd");
    try std.testing.expectError(error.OutboundQueueFull, byte_limited.sendFrame("e"));
}

test "quic server rejects multi-connection option until fanout exists" {
    try std.testing.expectError(error.InvalidConfig, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .max_concurrent_connections = 2,
    }));

    try std.testing.expectError(error.InvalidConfig, quic.Listener.init(std.testing.allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .max_concurrent_connections = 2,
    }));
}

test "quic server options reject unusable hardening limits" {
    try std.testing.expectError(error.InvalidConfig, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .local_cid_len = 0,
    }));

    try std.testing.expectError(error.InvalidConfig, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .max_datagrams_per_window = 0,
    }));

    try std.testing.expectError(error.InvalidConfig, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .retry_token_key = @splat(0x55),
        .retry_state_table_capacity = 0,
    }));
}
