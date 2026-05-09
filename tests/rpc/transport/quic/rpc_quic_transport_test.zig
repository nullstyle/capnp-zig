const std = @import("std");
const capnpc = @import("capnpc-zig");
const loopback = @import("loopback_test_support.zig");
const raw_faults = @import("raw_fault_client.zig");

const protocol = capnpc.rpc.wire.protocol;
const quic = capnpc.rpc.transport.quic;

const loopback_cert_pem = loopback.loopback_cert_pem;
const loopback_key_pem = loopback.loopback_key_pem;
const testListenAddr = loopback.testListenAddr;
const captureServerLog = loopback.captureServerLog;
const QuicEndpointState = loopback.QuicEndpointState;
const OrderedQuicEndpointState = loopback.OrderedQuicEndpointState;
const buildBootstrapFrame = loopback.buildBootstrapFrame;
const buildCallFrameWithData = loopback.buildCallFrameWithData;
const runQuicConnection = loopback.runQuicConnection;
const waitForClientMessageOrError = loopback.waitForClientMessageOrError;
const waitForServerError = loopback.waitForServerError;
const waitForOrderedClientMessagesOrError = loopback.waitForOrderedClientMessagesOrError;
const echoQuicMessage = loopback.echoQuicMessage;
const captureQuicMessage = loopback.captureQuicMessage;
const rejectUnexpectedQuicMessage = loopback.rejectUnexpectedQuicMessage;
const recordQuicError = loopback.recordQuicError;
const recordQuicClose = loopback.recordQuicClose;
const echoOrderedQuicMessage = loopback.echoOrderedQuicMessage;
const captureOrderedQuicMessage = loopback.captureOrderedQuicMessage;
const recordOrderedQuicError = loopback.recordOrderedQuicError;
const recordOrderedQuicClose = loopback.recordOrderedQuicClose;
const runRawNativeFaultCase = raw_faults.runRawNativeFaultCase;

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
    try std.testing.expect(@hasDecl(quic, "AcceptedSession"));
    try std.testing.expect(@hasDecl(quic, "AcceptedSessionDriver"));
    try std.testing.expect(@hasDecl(quic, "ClientEndpoint"));
    try std.testing.expect(@hasDecl(quic, "ServerEndpoint"));
    try std.testing.expect(@hasDecl(quic, "EndpointDriver"));
    try std.testing.expect(@hasDecl(quic.listener, "Listener"));
    try std.testing.expect(@hasDecl(quic.session, "Session"));
    try std.testing.expect(@hasDecl(quic.session, "AcceptedSession"));
    try std.testing.expect(@hasDecl(quic.session, "AcceptedSessionDriver"));
    try std.testing.expect(@hasDecl(quic.endpoint, "Endpoint"));
    try std.testing.expect(@hasField(quic.ServerEndpoint, "listener"));
    try std.testing.expect(@hasField(quic.ServerEndpoint, "session"));
    try std.testing.expect(@hasField(quic.ClientEndpoint, "socket"));
    try std.testing.expect(@hasField(quic.ClientEndpoint, "transport"));

    var driver = quic.AcceptedSessionDriver{};
    try std.testing.expect(!driver.isAttached());
    try std.testing.expect(driver.current() == null);
    try std.testing.expect(driver.quicConnection() == null);
}

test "quic listener owns server endpoint before session attachment" {
    var listener = try quic.Listener.init(std.testing.allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
    });
    defer listener.deinit();

    const addr = listener.getAddress();
    try std.testing.expect(addr == .ip4);
    try std.testing.expect(addr.ip4.port != 0);
    try std.testing.expectEqual(quic.supported_max_concurrent_sessions, listener.sessionCapacity());
    try std.testing.expectEqual(@as(usize, 0), listener.sessionCount());
    try std.testing.expect(listener.firstSession() == null);
    try std.testing.expect(listener.firstAcceptedSession() == null);
    try std.testing.expect(listener.sessionAt(0) == null);
    try std.testing.expect(listener.acceptedSessionAt(0) == null);
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
    const frame = try buildCallFrameWithData(allocator, 0xDADA, 1536);
    defer allocator.free(frame);

    const native_options = quic.NativeOptions{
        .inline_frame_threshold = 128,
        .max_control_frame_bytes = 256,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 8192,
    };
    try std.testing.expect(frame.len > native_options.inline_frame_threshold);

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

test "quic native localhost preserves E-order across data stream and inline frames" {
    const allocator = std.testing.allocator;
    const data_frame = try buildCallFrameWithData(allocator, 0xE000, 2048);
    defer allocator.free(data_frame);
    const inline_frame_1 = try buildBootstrapFrame(allocator, 0xE001);
    defer allocator.free(inline_frame_1);
    const inline_frame_2 = try buildBootstrapFrame(allocator, 0xE002);
    defer allocator.free(inline_frame_2);

    const native_options = quic.NativeOptions{
        .inline_frame_threshold = 256,
        .max_control_frame_bytes = 512,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 8192,
    };
    try std.testing.expect(data_frame.len > native_options.inline_frame_threshold);
    try std.testing.expect(inline_frame_1.len <= native_options.inline_frame_threshold);
    try std.testing.expect(inline_frame_2.len <= native_options.inline_frame_threshold);

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

    const expected_frames = [_][]const u8{ data_frame, inline_frame_1, inline_frame_2 };
    var server_state = OrderedQuicEndpointState{ .expected = &expected_frames };
    var client_state = OrderedQuicEndpointState{
        .expected = &expected_frames,
        .close_after_messages = expected_frames.len,
    };
    server.start(&server_state, echoOrderedQuicMessage, recordOrderedQuicError, recordOrderedQuicClose);
    client.start(&client_state, captureOrderedQuicMessage, recordOrderedQuicError, recordOrderedQuicClose);

    try client.sendFrame(data_frame);
    try client.sendFrame(inline_frame_1);
    try client.sendFrame(inline_frame_2);

    var server_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&server});
    var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        server.requestClose();
        client_thread.join();
        server_thread.join();
    };

    const exchanged = waitForOrderedClientMessagesOrError(&client_state, &server_state, expected_frames.len);
    client.requestClose();
    server.requestClose();
    client_thread.join();
    server_thread.join();
    joined = true;

    if (!exchanged) return error.QuicLoopbackTimedOut;

    try std.testing.expectEqual(expected_frames.len, server_state.messages.load(.acquire));
    try std.testing.expectEqual(expected_frames.len, client_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), client_state.errors.load(.acquire));
    const expected_order = [_]usize{ 0, 1, 2 };
    try server_state.expectOrder(&expected_order);
    try client_state.expectOrder(&expected_order);
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

test "quic native mode mismatch closes native peer cleanly" {
    const allocator = std.testing.allocator;
    const frame = try buildBootstrapFrame(allocator, 0xBEE);
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
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), server_state.last_error);
    try std.testing.expect(server.isClosing());
}

test "quic native raw peer malformed control closes with typed frame errors" {
    try runRawNativeFaultCase(.malformed_preface, .{}, error.InvalidFrame);
    try runRawNativeFaultCase(.malformed_hello, .{}, error.InvalidFrame);
    try runRawNativeFaultCase(.malformed_control, .{}, error.InvalidFrame);
}

test "quic native raw peer data stream violations close with typed frame errors" {
    try runRawNativeFaultCase(.data_final_size_mismatch, .{
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 1024,
    }, error.InvalidFrame);

    try runRawNativeFaultCase(.data_budget_violation, .{
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 4,
    }, error.FrameTooLarge);
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

test "quic native options reject unusable budgets with specific errors" {
    try std.testing.expectError(error.NativeControlFrameLimitTooSmall, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .mode = .native,
        .native = .{
            .max_control_frame_bytes = quic.native.common_header_bytes - 1,
        },
    }));

    try std.testing.expectError(error.NativePendingDataStreamLimitRequired, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .mode = .native,
        .native = .{
            .max_pending_data_streams = 0,
        },
    }));

    try std.testing.expectError(error.NativePendingDataByteLimitRequired, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .mode = .native,
        .native = .{
            .max_pending_data_bytes = 0,
        },
    }));

    try std.testing.expectError(error.NativeInlineFrameExceedsControlFrameLimit, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .max_message_bytes = 64,
        .mode = .native,
        .native = .{
            .inline_frame_threshold = 64,
            .max_control_frame_bytes = quic.native.rpc_header_bytes + 63,
        },
    }));

    try std.testing.expectError(error.NativeControlFrameLimitExceedsWireLimit, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .mode = .native,
        .native = .{
            .inline_frame_threshold = 0,
            .max_control_frame_bytes = @as(usize, std.math.maxInt(u32)) + 1,
        },
    }));

    try std.testing.expectError(error.NativeInlineFrameExceedsControlFrameLimit, quic.Connection.initClient(std.testing.allocator, std.testing.io, .{
        .remote_addr = .{ .ip4 = .{
            .bytes = .{ 127, 0, 0, 1 },
            .port = 4433,
        } },
        .server_name = "localhost",
        .max_message_bytes = 64,
        .mode = .native,
        .native = .{
            .inline_frame_threshold = 64,
            .max_control_frame_bytes = quic.native.rpc_header_bytes + 63,
        },
    }));
}
