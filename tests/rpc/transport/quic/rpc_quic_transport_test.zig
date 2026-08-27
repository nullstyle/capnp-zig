const std = @import("std");
const capnpc = @import("capnpc-zig");
const loopback = @import("loopback_test_support.zig");
const raw_faults = @import("raw_fault_client.zig");

const events = capnpc.rpc.events;
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
const runQuicServer = loopback.runQuicServer;
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
const echoQuicServerMessage = loopback.echoQuicServerMessage;
const recordQuicServerError = loopback.recordQuicServerError;
const recordQuicServerClose = loopback.recordQuicServerClose;
const runRawNativeFaultCase = raw_faults.runRawNativeFaultCase;

fn waitForFanoutSessions(server: *quic.Server, expected_sessions: usize) !void {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
        _ = try server.stepOnce(.wait);
        if (server.sessionCount() >= expected_sessions) return;
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    return error.QuicLoopbackTimedOut;
}

/// Records the observer side of a dropped oversized datagram, ignoring the
/// session lifecycle events the same observer also receives.
const DroppedDatagramObserver = struct {
    rejections: usize = 0,
    last_attempted: ?usize = null,
    last_limit: ?usize = null,
    last_err: ?anyerror = null,

    fn observer(self: *DroppedDatagramObserver) events.Observer {
        return events.Observer.init(self, onEvent);
    }

    fn onEvent(ctx: *anyopaque, event: events.Event) void {
        const self: *DroppedDatagramObserver = @ptrCast(@alignCast(ctx));
        switch (event) {
            .resource_rejection => |rejection| {
                if (rejection.resource != .udp_datagram_bytes) return;
                self.rejections += 1;
                self.last_attempted = rejection.attempted;
                self.last_limit = rejection.limit;
                self.last_err = rejection.err;
            },
            else => {},
        }
    }
};

/// Client-side message callback that keeps the connection open, so one client
/// can complete several round trips within a single test.
fn recordQuicClientFrame(conn: *quic.Connection, frame: []const u8) !void {
    const state: *QuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    try state.recordMessage(frame);
}

/// Send one oversized UDP datagram from an unrelated socket — the spoofed
/// packet any host on the network could send.
fn sendSpoofedDatagram(dest: std.Io.net.IpAddress, payload_len: usize) !void {
    const bind_addr = testListenAddr();
    const socket = try std.Io.net.IpAddress.bind(&bind_addr, std.testing.io, .{
        .mode = .dgram,
        .protocol = .udp,
    });
    defer socket.close(std.testing.io);

    const payload = try std.testing.allocator.alloc(u8, payload_len);
    defer std.testing.allocator.free(payload);
    @memset(payload, 0x5a);
    try socket.send(std.testing.io, &dest, payload);
}

fn driveServerUntilClientMessages(
    server: *quic.Server,
    client_state: *const QuicEndpointState,
    server_state: *const QuicEndpointState,
    expected_messages: usize,
) !void {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
        _ = try server.stepOnce(.wait);
        if (client_state.messages.load(.acquire) >= expected_messages) return;
        if (client_state.errors.load(.acquire) > 0 or server_state.errors.load(.acquire) > 0) {
            return error.QuicLoopbackUnexpectedError;
        }
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    return error.QuicLoopbackTimedOut;
}

fn driveServerUntilDroppedDatagrams(server: *quic.Server, expected_drops: u64) !void {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
        // The `try` is load-bearing: an oversized datagram must be a
        // per-datagram fault, not a failed step.
        _ = try server.stepOnce(.wait);
        if (server.droppedDatagramCount() >= expected_drops) return;
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    return error.QuicLoopbackTimedOut;
}

fn driveFanoutUntilTwoClientMessages(
    server: *quic.Server,
    client_a: *const QuicEndpointState,
    client_b: *const QuicEndpointState,
    server_a: *const QuicEndpointState,
    server_b: *const QuicEndpointState,
) !void {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
        _ = try server.receiveOne();
        var index: usize = 0;
        while (index < server.sessionCount()) : (index += 1) {
            try server.stepSession(index);
        }
        if (client_a.messages.load(.acquire) > 0 and client_b.messages.load(.acquire) > 0) return;
        if (client_a.errors.load(.acquire) > 0 or
            client_b.errors.load(.acquire) > 0 or
            server_a.errors.load(.acquire) > 0 or
            server_b.errors.load(.acquire) > 0)
        {
            return error.QuicLoopbackUnexpectedError;
        }
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    return error.QuicLoopbackTimedOut;
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
    try std.testing.expectEqual(@as(u32, 1), quic.compatibility_max_concurrent_sessions);
    try std.testing.expect(quic.supported_max_concurrent_sessions > quic.compatibility_max_concurrent_sessions);
}

test "quic transport exposes typed application close policy" {
    try std.testing.expectEqual(@as(u64, 0), @backingInt(quic.ApplicationCloseCode.normal));
    try std.testing.expectEqual(@as(u64, 0x434e_5001), @backingInt(quic.ApplicationCloseCode.frame_error));

    var reason_buf: [8]u8 = undefined;
    const prepared = quic.close.sanitizeReason(&reason_buf, "bad\nframe!");

    try std.testing.expect(prepared.truncated);
    try std.testing.expectEqualStrings("bad?fram", reason_buf[0..prepared.len]);
}

test "quic close state serializes concurrent cross-thread record calls" {
    // Two threads race record() the way the run loop and a cross-thread
    // requestClose can. The first recorder must win and readers must always see
    // a consistent (untorn) status/reason snapshot — never a partially written
    // reason buffer.
    const Racer = struct {
        fn recordFrameError(state: *quic.close.State) void {
            state.record(.frame_error, error.InvalidFrame);
        }

        fn recordInternalError(state: *quic.close.State) void {
            state.record(.internal_error, error.OutOfMemory);
        }
    };

    var iteration: usize = 0;
    while (iteration < 200) : (iteration += 1) {
        var state = quic.close.State.init(true);

        var a = try std.Thread.spawn(.{}, Racer.recordFrameError, .{&state});
        var b = try std.Thread.spawn(.{}, Racer.recordInternalError, .{&state});
        a.join();
        b.join();

        const status = state.status() orelse return error.QuicCloseStatusMissing;
        const reason = state.reason();

        // Whichever recorder won, the published reason must exactly match that
        // status's code+detail — proving the buffer was not interleaved.
        switch (status.code) {
            .frame_error => {
                try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), status.err);
                try std.testing.expectEqualStrings("rpc frame error: InvalidFrame", reason);
            },
            .internal_error => {
                try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), status.err);
                try std.testing.expectEqualStrings("rpc transport error: OutOfMemory", reason);
            },
            else => return error.QuicCloseStatusUnexpected,
        }
    }
}

test "quic exposes listener and session API boundary" {
    try std.testing.expect(@hasDecl(quic, "Listener"));
    try std.testing.expect(@hasDecl(quic, "Server"));
    try std.testing.expect(@hasDecl(quic, "ServerSession"));
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
    try std.testing.expect(@hasDecl(quic.Server, "Session"));
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
    try std.testing.expectEqual(quic.compatibility_max_concurrent_sessions, listener.sessionCapacity());
    try std.testing.expectEqual(@as(usize, 0), listener.sessionCount());
    try std.testing.expect(listener.firstSession() == null);
    try std.testing.expect(listener.firstAcceptedSession() == null);
    try std.testing.expect(listener.sessionAt(0) == null);
    try std.testing.expect(listener.acceptedSessionAt(0) == null);
}

test "quic listener drops an oversized datagram instead of failing the receive" {
    // The bare `Listener` shares the fanout server's exposure and now shares
    // its answer. `receiveOne` reports "nothing fed" the same way it reports a
    // timeout; the drop surfaces through the observer and the counter instead
    // of through an error the caller has no useful response to.
    const rx_buffer_size: usize = 2048;

    var drops = DroppedDatagramObserver{};
    var listener = try quic.Listener.init(std.testing.allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(50),
        // Matches the caller buffer below so the reported limit is the same on
        // POSIX (which receives into `rx_buf`) and Windows (which receives into
        // listener-owned storage of this size).
        .udp_rx_buffer_size = rx_buffer_size,
        .observer = drops.observer(),
    });
    defer listener.deinit();

    try sendSpoofedDatagram(listener.getAddress(), rx_buffer_size * 2);

    var rx_buf: [rx_buffer_size]u8 = undefined;
    var attempts: usize = 0;
    while (attempts < 20 and listener.droppedDatagramCount() == 0) : (attempts += 1) {
        // `try` is the ablation: restoring `return error.DatagramTooLarge` in
        // either arm fails here rather than spinning out the attempt budget.
        try std.testing.expect((try listener.receiveOne(&rx_buf)) == null);
    }

    try std.testing.expectEqual(@as(u64, 1), listener.droppedDatagramCount());
    try std.testing.expectEqual(@as(usize, 1), drops.rejections);
    try std.testing.expectEqual(@as(?usize, rx_buffer_size), drops.last_limit);
    try std.testing.expectEqual(@as(?usize, null), drops.last_attempted);
    try std.testing.expectEqual(@as(?anyerror, error.DatagramTooLarge), drops.last_err);

    // Still usable afterwards: the next receive is an ordinary empty poll, and
    // the drop is not re-reported.
    try std.testing.expect((try listener.receiveOne(&rx_buf)) == null);
    try std.testing.expectEqual(@as(u64, 1), listener.droppedDatagramCount());
    try std.testing.expectEqual(@as(usize, 1), drops.rejections);
}

test "quic listener retains owned Windows receive storage across caller buffers" {
    var listener = try quic.Listener.init(std.testing.allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
    });
    defer listener.deinit();
    const sender_addr = testListenAddr();
    var sender = try std.Io.net.IpAddress.bind(&sender_addr, std.testing.io, .{
        .mode = .dgram,
        .protocol = .udp,
    });
    defer sender.close(std.testing.io);

    var timeout_buffer: [32]u8 = @splat(0xa1);
    var resume_buffer: [32]u8 = @splat(0xb2);
    try std.testing.expectEqual(
        quic.testing.UdpReceiveBridge.WaitResult.timeout,
        try quic.testing.ListenerAccess.receiveConcurrent(
            &listener,
            &timeout_buffer,
            std.Io.Duration.fromMilliseconds(1),
        ),
    );
    try sender.send(std.testing.io, &listener.getAddress(), "listener-owned");

    const received = try quic.testing.ListenerAccess.receiveConcurrent(
        &listener,
        &resume_buffer,
        std.Io.Duration.fromSeconds(1),
    );
    switch (received) {
        .datagram => |datagram| {
            const owned_ptr = quic.testing.ListenerAccess.receiveStoragePtr(&listener);
            try std.testing.expectEqual(@intFromPtr(owned_ptr), @intFromPtr(datagram.data.ptr));
            try std.testing.expect(@intFromPtr(datagram.data.ptr) != @intFromPtr(&timeout_buffer));
            try std.testing.expect(@intFromPtr(datagram.data.ptr) != @intFromPtr(&resume_buffer));
            try std.testing.expectEqualStrings("listener-owned", datagram.data);
            try std.testing.expectEqual(@as(u8, 0xb2), resume_buffer[0]);
        },
        else => return error.ExpectedUdpDatagram,
    }
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
        .initial_source_rate_limit = .{ .limit = 32 },
        .source_rate_window_us = 123_000,
        .source_rate_table_capacity = 256,
        .vn_source_rate_limit = .{ .limit = 7 },
        .retry_token_key = retry_key,
        .retry_token_lifetime_us = 456_000,
        .retry_state_table_capacity = 64,
        .new_token_key = new_token_key,
        .new_token_lifetime_us = 789_000,
        .early_data = .without_replay_protection,
        .reveal_close_reason_on_wire = true,
        .max_connection_memory = 4 * 1024 * 1024,
        .listener_datagram_rate_limit = .{ .limit = 100 },
        .listener_byte_rate_limit = .{ .limit = 64 * 1024 },
        .listener_rate_window_us = 42_000,
        .source_byte_rate_limit = .{ .limit = 32 * 1024 },
        .log_source_rate_limit = .{ .limit = 5 },
    });

    try std.testing.expectEqual(@as(u8, 12), config.local_cid_len);
    try std.testing.expect(config.log_callback != null);
    try std.testing.expectEqual(@intFromPtr(&log_user_data), @intFromPtr(config.log_user_data.?));
    // `.resolve(default_cap)` is quic-zig's accessor for the effective cap;
    // passing 0 means "recommended off", so an explicit `.limit` must survive.
    try std.testing.expectEqual(@as(?u64, 32), config.initial_source_rate_limit.resolve(0));
    try std.testing.expectEqual(@as(u64, 123_000), config.source_rate_window_us);
    try std.testing.expectEqual(@as(u32, 256), config.source_rate_table_capacity);
    try std.testing.expectEqual(@as(?u64, 7), config.vn_source_rate_limit.resolve(0));
    try std.testing.expectEqual(retry_key, config.retry_token_key.?);
    try std.testing.expectEqual(@as(u64, 456_000), config.retry_token_lifetime_us);
    try std.testing.expectEqual(@as(u32, 64), config.retry_state_table_capacity);
    try std.testing.expectEqual(new_token_key, config.new_token_key.?);
    try std.testing.expectEqual(@as(u64, 789_000), config.new_token_lifetime_us);
    try std.testing.expect(config.early_data == .without_replay_protection);
    try std.testing.expect(config.reveal_close_reason_on_wire);
    try std.testing.expectEqual(@as(u64, 4 * 1024 * 1024), config.max_connection_memory);
    try std.testing.expectEqual(@as(?u64, 100), config.listener_datagram_rate_limit.resolve(0));
    try std.testing.expectEqual(@as(?u64, 64 * 1024), config.listener_byte_rate_limit.resolve(0));
    try std.testing.expectEqual(@as(u64, 42_000), config.listener_rate_window_us);
    try std.testing.expectEqual(@as(?u64, 32 * 1024), config.source_byte_rate_limit.resolve(0));
    try std.testing.expectEqual(@as(?u64, 5), config.log_source_rate_limit.resolve(0));
}

test "quic production hardening preset enables retry and rate gates" {
    const retry_key: quic.ServerRetryTokenKey = @splat(0x33);
    const new_token_key: quic.ServerNewTokenKey = @splat(0x44);

    const options = quic.withProductionServerHardening(.{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .early_data = .without_replay_protection,
        .reveal_close_reason_on_wire = true,
    }, .{
        .retry_token_key = retry_key,
        .new_token_key = new_token_key,
    });

    try std.testing.expectEqual(retry_key, options.retry_token_key.?);
    try std.testing.expectEqual(new_token_key, options.new_token_key.?);
    try std.testing.expectEqual(
        @as(?u64, quic.default_quic_initial_source_rate_cap),
        options.initial_source_rate_limit.resolve(0),
    );
    try std.testing.expect(options.listener_datagram_rate_limit.resolve(0).? > 0);
    try std.testing.expect(options.listener_byte_rate_limit.resolve(0).? > 0);
    try std.testing.expect(options.source_byte_rate_limit.resolve(0).? > 0);
    // Hardening OVERRIDES an opt-in 0-RTT posture back to disabled: the preset
    // is the conservative one, and replay-exposed 0-RTT is never part of it.
    try std.testing.expect(options.early_data == .disabled);
    try std.testing.expect(!options.reveal_close_reason_on_wire);

    const config = try quic.serverConfigFromOptions(std.testing.allocator, options);
    try std.testing.expectEqual(retry_key, config.retry_token_key.?);
    try std.testing.expectEqual(new_token_key, config.new_token_key.?);
    try std.testing.expectEqual(
        options.initial_source_rate_limit.resolve(0),
        config.initial_source_rate_limit.resolve(0),
    );
    try std.testing.expectEqual(
        options.listener_datagram_rate_limit.resolve(0),
        config.listener_datagram_rate_limit.resolve(0),
    );
    try std.testing.expectEqual(
        options.listener_byte_rate_limit.resolve(0),
        config.listener_byte_rate_limit.resolve(0),
    );
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
        .insecure_skip_verify = true,
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
        .insecure_skip_verify = true,
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
        .insecure_skip_verify = true,
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
        .insecure_skip_verify = true,
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

test "quic native fanout server drives two sessions independently" {
    const allocator = std.testing.allocator;
    const frame_a = try buildBootstrapFrame(allocator, 0xA11C);
    defer allocator.free(frame_a);
    const frame_b = try buildBootstrapFrame(allocator, 0xB22D);
    defer allocator.free(frame_b);

    const native_options = quic.NativeOptions{
        .inline_frame_threshold = 128,
        .max_control_frame_bytes = 256,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 8192,
    };

    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .max_concurrent_connections = 2,
        .mode = .native,
        .native = native_options,
    });
    defer server.deinit();

    const server_addr = server.getAddress();
    try std.testing.expect(server_addr == .ip4);
    try std.testing.expect(server_addr.ip4.port != 0);
    try std.testing.expectEqual(@as(u32, 2), server.sessionCapacity());

    var client_a = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer client_a.deinit();

    var client_b = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer client_b.deinit();

    var client_state_a = QuicEndpointState{};
    var client_state_b = QuicEndpointState{};
    client_a.start(&client_state_a, captureQuicMessage, recordQuicError, recordQuicClose);
    client_b.start(&client_state_b, captureQuicMessage, recordQuicError, recordQuicClose);

    var client_thread_a = try std.Thread.spawn(.{}, runQuicConnection, .{&client_a});
    var client_thread_b = try std.Thread.spawn(.{}, runQuicConnection, .{&client_b});
    var joined = false;
    defer if (!joined) {
        client_a.requestClose();
        client_b.requestClose();
        server.requestClose();
        client_thread_a.join();
        client_thread_b.join();
    };

    try waitForFanoutSessions(&server, 2);

    var server_state_a = QuicEndpointState{};
    var server_state_b = QuicEndpointState{};
    server.sessionAt(0).?.start(&server_state_a, echoQuicServerMessage, recordQuicServerError, recordQuicServerClose);
    server.sessionAt(1).?.start(&server_state_b, echoQuicServerMessage, recordQuicServerError, recordQuicServerClose);

    try client_a.sendFrame(frame_a);
    try client_b.sendFrame(frame_b);

    try driveFanoutUntilTwoClientMessages(
        &server,
        &client_state_a,
        &client_state_b,
        &server_state_a,
        &server_state_b,
    );

    client_a.requestClose();
    client_b.requestClose();
    server.requestClose();
    client_thread_a.join();
    client_thread_b.join();
    joined = true;

    try std.testing.expectEqual(@as(usize, 2), server.quicConnectionCount());
    try std.testing.expectEqual(@as(usize, 2), server.sessionCount());
    try std.testing.expectEqual(@as(usize, 1), client_state_a.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), client_state_b.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), client_state_a.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), client_state_b.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server_state_a.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server_state_b.errors.load(.acquire));
    try std.testing.expectEqualSlices(u8, frame_a, client_state_a.receivedSlice());
    try std.testing.expectEqualSlices(u8, frame_b, client_state_b.receivedSlice());
    try std.testing.expectEqual(@as(usize, 1), server_state_a.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), server_state_b.messages.load(.acquire));
}

test "quic fanout server run loop terminates on cross-thread requestClose" {
    // Drives Server.run() on a spawned loop thread while a live client keeps the
    // accept/reap path mutating the session list, then requests close from the
    // test thread. The cross-thread requestClose must never touch the session
    // list — it only raises the atomic flag and wakes the loop, which closes
    // every session on its own thread — so run() returns without a data race or
    // hang.
    const allocator = std.testing.allocator;

    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .max_concurrent_connections = 2,
    });
    defer server.deinit();

    const server_addr = server.getAddress();

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
    });
    defer client.deinit();

    var client_state = QuicEndpointState{};
    client.start(&client_state, captureQuicMessage, recordQuicError, recordQuicClose);

    var server_thread = try std.Thread.spawn(.{}, runQuicServer, .{&server});
    var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});

    // Give the handshake time to complete so the loop thread has appended a
    // live session to the list; the cross-thread close below then races the
    // accept/reap path. State is owned by the loop threads, so the test thread
    // only sleeps rather than inspecting connection internals cross-thread.
    loopback.sleepMs(loopback.loopback_poll_ms * 5);

    // Cross-thread close of both the server loop and the client loop. Neither
    // requestClose iterates the loop-owned session list.
    server.requestClose();
    client.requestClose();

    client_thread.join();
    server_thread.join();

    try std.testing.expect(server.isClosing());
    try std.testing.expectEqual(@as(usize, 0), client_state.errors.load(.acquire));
}

test "quic fanout server fires on_close for a live session on deinit" {
    const allocator = std.testing.allocator;

    const native_options = quic.NativeOptions{
        .inline_frame_threshold = 128,
        .max_control_frame_bytes = 256,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 8192,
    };

    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .max_concurrent_connections = 1,
        .mode = .native,
        .native = native_options,
    });
    var server_deinited = false;
    defer if (!server_deinited) server.deinit();

    const server_addr = server.getAddress();

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer client.deinit();

    var client_state = QuicEndpointState{};
    client.start(&client_state, captureQuicMessage, recordQuicError, recordQuicClose);

    var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        server.requestClose();
        client_thread.join();
    };

    try waitForFanoutSessions(&server, 1);

    var server_state = QuicEndpointState{};
    server.sessionAt(0).?.start(&server_state, echoQuicServerMessage, recordQuicServerError, recordQuicServerClose);

    // Tear down the client thread, leaving the server session live and
    // unreaped (we do not step the server afterwards).
    client.requestClose();
    server.requestClose();
    client_thread.join();
    joined = true;

    try std.testing.expectEqual(@as(usize, 1), server.sessionCount());
    try std.testing.expectEqual(@as(usize, 0), server_state.closes.load(.acquire));

    // Deinit the server while the session is still live: on_close must fire
    // exactly once. Previously deinit dropped sessions with no close callback.
    server.deinit();
    server_deinited = true;

    try std.testing.expectEqual(@as(usize, 1), server_state.closes.load(.acquire));
}

test "quic fanout server survives a spoofed oversized datagram and keeps serving" {
    // UDP is unauthenticated, so any host that can reach this port can send an
    // oversized datagram. `Server.run` closes the server on a failed step, so
    // failing the step here would hand that host a one-packet kill switch for
    // every session on the endpoint.
    //
    // The server is driven on this thread with `try` on purpose: that `try` is
    // the ablation. Restore `return error.DatagramTooLarge` in either arm of
    // `Server.receiveOneFor` and the drop loop below fails with exactly that
    // error instead of timing out on a missing counter.
    const allocator = std.testing.allocator;

    // Sized well above what the QUIC handshake needs (`udp_tx_buffer_size`
    // defaults to 1500) but far below the 65507-byte IPv4 UDP payload ceiling,
    // so a single spoofed datagram can actually exceed it. The 64 KiB default
    // is unreachable over IPv4, which is why the exposure only shows up on
    // endpoints tuned closer to their path MTU.
    const rx_buffer_size: usize = 2048;

    const frame_before = try buildBootstrapFrame(allocator, 0xBEF0);
    defer allocator.free(frame_before);
    const frame_after = try buildBootstrapFrame(allocator, 0xAF7E);
    defer allocator.free(frame_after);

    var drops = DroppedDatagramObserver{};

    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .max_concurrent_connections = 2,
        .udp_rx_buffer_size = rx_buffer_size,
        .observer = drops.observer(),
    });
    defer server.deinit();

    const server_addr = server.getAddress();

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
    });
    defer client.deinit();

    var client_state = QuicEndpointState{};
    client.start(&client_state, recordQuicClientFrame, recordQuicError, recordQuicClose);

    var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        server.requestClose();
        client_thread.join();
    };

    try waitForFanoutSessions(&server, 1);

    var server_state = QuicEndpointState{};
    server.sessionAt(0).?.start(&server_state, echoQuicServerMessage, recordQuicServerError, recordQuicServerClose);

    // Establish that the session round-trips before the attack, so a later
    // failure cannot be blamed on a session that never worked.
    try client.sendFrame(frame_before);
    try driveServerUntilClientMessages(&server, &client_state, &server_state, 1);
    try std.testing.expectEqual(@as(u64, 0), server.droppedDatagramCount());

    try sendSpoofedDatagram(server_addr, rx_buffer_size * 2);
    try driveServerUntilDroppedDatagrams(&server, 1);

    // The endpoint and its session are untouched by the drop.
    try std.testing.expect(!server.isClosing());
    try std.testing.expectEqual(@as(usize, 1), server.sessionCount());
    try std.testing.expectEqual(@as(usize, 1), server.quicConnectionCount());
    try std.testing.expectEqual(@as(usize, 0), server_state.closes.load(.acquire));

    // Still serving, not merely still alive: the pre-existing session carries
    // another full round trip after the attack.
    try client.sendFrame(frame_after);
    try driveServerUntilClientMessages(&server, &client_state, &server_state, 2);
    try std.testing.expectEqualSlices(u8, frame_after, client_state.receivedSlice());

    client.requestClose();
    server.requestClose();
    client_thread.join();
    joined = true;

    try std.testing.expectEqual(@as(usize, 0), client_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), server_state.messages.load(.acquire));

    // Exactly one datagram dropped, reported once, and attributed to the
    // buffer that was actually exceeded rather than to a session.
    try std.testing.expectEqual(@as(u64, 1), server.droppedDatagramCount());
    try std.testing.expectEqual(@as(usize, 1), drops.rejections);
    try std.testing.expectEqual(@as(?usize, rx_buffer_size), drops.last_limit);
    // Neither platform can report the true datagram size, so `attempted` must
    // stay null rather than carry the truncated length as if it were one.
    try std.testing.expectEqual(@as(?usize, null), drops.last_attempted);
    try std.testing.expectEqual(@as(?anyerror, error.DatagramTooLarge), drops.last_err);
}

test "quic native localhost streams large RPC data payload" {
    const allocator = std.testing.allocator;
    const frame = try buildCallFrameWithData(allocator, 0xD17A, 128 * 1024);
    defer allocator.free(frame);

    const native_options = quic.NativeOptions{
        .inline_frame_threshold = 512,
        .max_control_frame_bytes = 1024,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = frame.len + 1024,
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
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer client.deinit();

    const expected_frames = [_][]const u8{frame};
    var server_state = OrderedQuicEndpointState{ .expected = &expected_frames };
    var client_state = OrderedQuicEndpointState{
        .expected = &expected_frames,
        .close_after_messages = expected_frames.len,
    };
    server.start(&server_state, echoOrderedQuicMessage, recordOrderedQuicError, recordOrderedQuicClose);
    client.start(&client_state, captureOrderedQuicMessage, recordOrderedQuicError, recordOrderedQuicClose);

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
    const expected_order = [_]usize{0};
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
        .insecure_skip_verify = true,
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
        .insecure_skip_verify = true,
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
    try runRawNativeFaultCase(.unknown_control_tag, .{}, error.InvalidFrame);
    try runRawNativeFaultCase(.oversized_control_frame, .{
        .inline_frame_threshold = 1,
        .max_control_frame_bytes = 64,
        .max_pending_data_streams = 4,
        .max_pending_data_bytes = 1024,
    }, error.FrameTooLarge);
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
        .insecure_skip_verify = true,
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

test "quic server fanout accepts multi-connection capacity" {
    const config = try quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .max_concurrent_connections = 2,
    });
    try std.testing.expectEqual(@as(u32, 2), config.max_concurrent_connections);

    var listener = try quic.Listener.init(std.testing.allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .max_concurrent_connections = 2,
    });
    defer listener.deinit();
    try std.testing.expectEqual(@as(u32, 2), listener.sessionCapacity());

    var server = try quic.Server.init(std.testing.allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .max_concurrent_connections = 2,
    });
    defer server.deinit();
    try std.testing.expectEqual(@as(u32, 2), server.sessionCapacity());

    try std.testing.expectError(error.InvalidConfig, quic.Connection.initServer(std.testing.allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .max_concurrent_connections = 2,
    }));

    try std.testing.expectError(error.InvalidConfig, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .max_concurrent_connections = 0,
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
        .listener_datagram_rate_limit = .{ .limit = 0 },
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

    try std.testing.expectError(error.NativeDataStreamDeadlineRequired, quic.serverConfigFromOptions(std.testing.allocator, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = "cert",
        .tls_key_pem = "key",
        .mode = .native,
        .native = .{
            .data_stream_completion_deadline_us = 0,
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

// ---------------------------------------------------------------------------
// Warm restore (durable-caps ladder, prototype #2): a resumed dial stages its
// first RPC frame before the handshake and rides 0-RTT; a stale ticket falls
// back to 1-RTT without losing the frame.
// ---------------------------------------------------------------------------

/// Captures the FIRST resumption envelope `new_session_callback` delivers.
/// The callback runs on the connection's run thread; `len` is the
/// release-store the test thread acquires before reading `bytes`. Later
/// tickets are ignored so the reader can never observe a torn overwrite.
const ResumptionSink = struct {
    bytes: [4096]u8 = undefined,
    len: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn capture(user_data: ?*anyopaque, resumption_state: []const u8) void {
        const self: *ResumptionSink = @ptrCast(@alignCast(user_data.?));
        if (self.len.load(.acquire) != 0) return;
        if (resumption_state.len == 0 or resumption_state.len > self.bytes.len) return;
        @memcpy(self.bytes[0..resumption_state.len], resumption_state);
        self.len.store(resumption_state.len, .release);
    }

    fn slice(self: *const ResumptionSink) []const u8 {
        return self.bytes[0..self.len.load(.acquire)];
    }
};

/// Echo callback that also records whether any dispatch happened while the
/// session's QUIC handshake was still incomplete. Under
/// `early_data = .without_replay_protection` the transport defers
/// dispatch of early-data frames until the handshake completes (the
/// replay-execution guard), so this must never observe an incomplete
/// handshake at dispatch time. (`.with_anti_replay` would legitimately
/// dispatch early — the tracker guarantees single use — but that posture
/// needs a tracker instance and is not exercised here.)
const EarlyGateState = struct {
    inner: QuicEndpointState = .{},
    dispatched_before_handshake: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

fn echoRecordingHandshakePhase(session: *quic.ServerSession, frame: []const u8) !void {
    const st: *EarlyGateState = @ptrCast(@alignCast(session.context().?));
    if (session.activeQuicConnection()) |q| {
        if (!q.handshakeDone()) st.dispatched_before_handshake.store(true, .release);
    }
    try st.inner.recordMessage(frame);
    try session.sendFrame(frame);
}

fn earlyGateServerError(session: *quic.ServerSession, err: anyerror) void {
    const st: *EarlyGateState = @ptrCast(@alignCast(session.context().?));
    st.inner.last_error = err;
    _ = st.inner.errors.fetchAdd(1, .acq_rel);
    session.requestClose();
}

fn earlyGateServerClose(session: *quic.ServerSession) void {
    const st: *EarlyGateState = @ptrCast(@alignCast(session.context().?));
    _ = st.inner.closes.fetchAdd(1, .acq_rel);
}

/// Drive the fanout server until `client_state` has a message AND the sink
/// captured a ticket — the resumed dial needs both the echo and the envelope.
fn driveUntilEchoAndTicket(
    server: *quic.Server,
    client_state: *const QuicEndpointState,
    server_state: *const QuicEndpointState,
    sink: *const ResumptionSink,
) !void {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
        _ = try server.stepOnce(.wait);
        if (client_state.messages.load(.acquire) > 0 and sink.len.load(.acquire) > 0) return;
        if (client_state.errors.load(.acquire) > 0 or server_state.errors.load(.acquire) > 0) {
            return error.QuicLoopbackUnexpectedError;
        }
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    return error.QuicLoopbackTimedOut;
}

fn driveUntilSessions(server: *quic.Server, expected: usize) !void {
    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
        _ = try server.stepOnce(.wait);
        if (server.sessionCount() >= expected) return;
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    return error.QuicLoopbackTimedOut;
}

test "quic warm restore: resumed dial sends its first RPC frame as accepted 0-RTT" {
    const allocator = std.testing.allocator;
    const frame_first = try buildBootstrapFrame(allocator, 0x0AAA);
    defer allocator.free(frame_first);
    const frame_restore = try buildBootstrapFrame(allocator, 0x0BBB);
    defer allocator.free(frame_restore);

    // One server, two sequential sessions: the resumed ticket only decrypts
    // under the SAME server TLS context that minted it.
    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .max_concurrent_connections = 2,
        .early_data = .without_replay_protection,
    });
    defer server.deinit();
    const server_addr = server.getAddress();

    var sink = ResumptionSink{};

    // ---- Dial 1: earn the ticket over an ordinary handshake. ----
    {
        var client = try quic.Connection.initClient(allocator, std.testing.io, .{
            .remote_addr = server_addr,
            .server_name = "localhost",
            .insecure_skip_verify = true,
            .receive_timeout = std.Io.Duration.fromMilliseconds(1),
            .new_session_callback = ResumptionSink.capture,
            .new_session_user_data = &sink,
        });
        defer client.deinit();

        var client_state = QuicEndpointState{};
        client.start(&client_state, recordQuicClientFrame, recordQuicError, recordQuicClose);
        var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});
        var joined = false;
        defer if (!joined) {
            client.requestClose();
            client_thread.join();
        };

        try driveUntilSessions(&server, 1);
        var server_state = QuicEndpointState{};
        server.sessionAt(0).?.start(&server_state, echoQuicServerMessage, recordQuicServerError, recordQuicServerClose);
        try client.sendFrame(frame_first);
        try driveUntilEchoAndTicket(&server, &client_state, &server_state, &sink);

        client.requestClose();
        client_thread.join();
        joined = true;
    }
    try std.testing.expect(sink.len.load(.acquire) > 0);

    // Drive the server until the closed first session is REAPED, so the
    // resumed dial deterministically lands at session index 0. Starting a
    // session at index 1 and letting a reap swap-remove index 0 under it
    // is how the first version of this test silently echoed to nobody.
    {
        var waited_ms: u64 = 0;
        while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
            _ = try server.stepOnce(.wait);
            if (server.sessionCount() == 0) break;
            loopback.sleepMs(loopback.loopback_poll_ms);
        }
        try std.testing.expectEqual(@as(usize, 0), server.sessionCount());
    }

    // ---- Dial 2: resume. The frame is enqueued BEFORE the run thread
    // starts, so the relaxed early-open gate flushes it as 0-RTT. ----
    var client2 = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .resumption_state = sink.slice(),
    });
    defer client2.deinit();

    var client2_state = QuicEndpointState{};
    client2.start(&client2_state, recordQuicClientFrame, recordQuicError, recordQuicClose);
    try client2.sendFrame(frame_restore);

    var client2_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client2});
    var joined2 = false;
    defer if (!joined2) {
        client2.requestClose();
        client2_thread.join();
    };

    try driveUntilSessions(&server, 1);
    var server2_state = EarlyGateState{};
    server.sessionAt(0).?.start(&server2_state, echoRecordingHandshakePhase, earlyGateServerError, earlyGateServerClose);
    // 0-RTT ordering contract: the restore frame arrived DURING the
    // handshake — before these callbacks were bound — so it sits parsed in
    // the session engine with nothing left on the wire to trigger another
    // service pass. Step the session once to dispatch what buffered. A
    // real embedder binding callbacks at accept time has the same window
    // whenever early data is enabled.
    try server.stepSession(0);

    var waited_ms: u64 = 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
        _ = try server.stepOnce(.wait);
        if (client2_state.messages.load(.acquire) > 0) break;
        if (client2_state.errors.load(.acquire) > 0 or server2_state.inner.errors.load(.acquire) > 0) {
            return error.QuicLoopbackUnexpectedError;
        }
        loopback.sleepMs(loopback.loopback_poll_ms);
    }

    client2.requestClose();
    server.requestClose();
    client2_thread.join();
    joined2 = true;

    try std.testing.expectEqual(@as(usize, 1), client2_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), client2_state.errors.load(.acquire));
    try std.testing.expectEqualSlices(u8, frame_restore, client2_state.receivedSlice());
    // The decisive assertion: the resumed dial's early data was ACCEPTED —
    // the restore frame rode 0-RTT, not a post-handshake stream.
    const q2 = client2.endpoint.activeQuicConnection() orelse return error.QuicConnectionGone;
    try std.testing.expectEqual(quic.EarlyDataStatus.accepted, q2.earlyDataStatus());
    // And the replay-execution guard held: with
    // `.without_replay_protection`, the frame that ARRIVED in 0-RTT was
    // not DISPATCHED until the handshake completed (a replayed first
    // flight can never complete one).
    try std.testing.expect(!server2_state.dispatched_before_handshake.load(.acquire));
}

test "quic warm restore: stale ticket is rejected but the staged frame still arrives at 1-RTT" {
    const allocator = std.testing.allocator;
    const frame_first = try buildBootstrapFrame(allocator, 0x0CCC);
    defer allocator.free(frame_first);
    const frame_restore = try buildBootstrapFrame(allocator, 0x0DDD);
    defer allocator.free(frame_restore);

    var sink = ResumptionSink{};

    // ---- Earn a ticket from server 1 (compat single-session server). ----
    {
        var server1 = try quic.Connection.initServer(allocator, std.testing.io, .{
            .listen_addr = testListenAddr(),
            .tls_cert_pem = loopback_cert_pem,
            .tls_key_pem = loopback_key_pem,
            .receive_timeout = std.Io.Duration.fromMilliseconds(1),
            .early_data = .without_replay_protection,
        });
        defer server1.deinit();

        var client = try quic.Connection.initClient(allocator, std.testing.io, .{
            .remote_addr = server1.getAddress(),
            .server_name = "localhost",
            .insecure_skip_verify = true,
            .receive_timeout = std.Io.Duration.fromMilliseconds(1),
            .new_session_callback = ResumptionSink.capture,
            .new_session_user_data = &sink,
        });
        defer client.deinit();

        var server_state = QuicEndpointState{};
        var client_state = QuicEndpointState{};
        server1.start(&server_state, echoQuicMessage, recordQuicError, recordQuicClose);
        client.start(&client_state, recordQuicClientFrame, recordQuicError, recordQuicClose);
        try client.sendFrame(frame_first);

        var server_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&server1});
        var client_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client});
        var joined = false;
        defer if (!joined) {
            client.requestClose();
            server1.requestClose();
            client_thread.join();
            server_thread.join();
        };

        var waited_ms: u64 = 0;
        while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
            if (client_state.messages.load(.acquire) > 0 and sink.len.load(.acquire) > 0) break;
            if (client_state.errors.load(.acquire) > 0 or server_state.errors.load(.acquire) > 0) break;
            loopback.sleepMs(loopback.loopback_poll_ms);
        }

        client.requestClose();
        server1.requestClose();
        client_thread.join();
        server_thread.join();
        joined = true;
    }
    try std.testing.expect(sink.len.load(.acquire) > 0);

    // ---- Resume against a FRESH server: new TLS context, new ticket keys,
    // so 0-RTT is rejected — the routine server-restart scenario. quic-zig
    // requeues the staged frame verbatim at 1-RTT; nothing may be lost. ----
    var server2 = try quic.Connection.initServer(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .early_data = .without_replay_protection,
    });
    defer server2.deinit();

    var client2 = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server2.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .resumption_state = sink.slice(),
    });
    defer client2.deinit();

    var server2_state = QuicEndpointState{};
    var client2_state = QuicEndpointState{};
    server2.start(&server2_state, echoQuicMessage, recordQuicError, recordQuicClose);
    client2.start(&client2_state, recordQuicClientFrame, recordQuicError, recordQuicClose);
    try client2.sendFrame(frame_restore);

    var server2_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&server2});
    var client2_thread = try std.Thread.spawn(.{}, runQuicConnection, .{&client2});
    var joined2 = false;
    defer if (!joined2) {
        client2.requestClose();
        server2.requestClose();
        client2_thread.join();
        server2_thread.join();
    };

    const exchanged = waitForClientMessageOrError(&client2_state, &server2_state);
    client2.requestClose();
    server2.requestClose();
    client2_thread.join();
    server2_thread.join();
    joined2 = true;

    if (!exchanged) return error.QuicLoopbackTimedOut;
    try std.testing.expectEqual(@as(usize, 1), client2_state.messages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), client2_state.errors.load(.acquire));
    try std.testing.expectEqualSlices(u8, frame_restore, client2_state.receivedSlice());
    // Rejection recovery held: 0-RTT was refused, the frame arrived anyway.
    const q2 = client2.endpoint.activeQuicConnection() orelse return error.QuicConnectionGone;
    try std.testing.expectEqual(quic.EarlyDataStatus.rejected, q2.earlyDataStatus());
}

// ---------------------------------------------------------------------------
// Half-open handshake guard: a session whose handshake never completes must
// die by deadline — otherwise half-opens are immortal, accumulate under
// churn/loss/attack, pin max_concurrent_connections, and the server silently
// refuses every new dial (the QUIC analog of a SYN flood; observed in the
// soak with the whole table `.open` and hundreds of silent table_full drops).
// ---------------------------------------------------------------------------

/// Dial the server and step the client just long enough to land its Initial
/// (creating the server-side half-open), then ABANDON it mid-handshake.
fn abandonHalfOpenDial(allocator: std.mem.Allocator, server: *quic.Server) !quic.Connection {
    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        // The abandoning client must not close itself first.
        .handshake_timeout_ms = null,
    });
    errdefer client.deinit();
    var sent: usize = 0;
    while (sent < 3) : (sent += 1) {
        _ = try client.stepOnce(.poll);
        _ = try server.stepOnce(.poll);
        if (server.sessionCount() > 0) break;
        loopback.sleepMs(1);
    }
    return client;
}

test "server sweeps a half-open session at the handshake deadline" {
    const allocator = std.testing.allocator;
    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .max_concurrent_connections = 2,
        .handshake_timeout_ms = 250,
    });
    defer server.deinit();

    var abandoned = try abandonHalfOpenDial(allocator, &server);
    defer abandoned.deinit();

    // The half-open exists; now the client goes silent and only the server
    // steps. The guard must certify and free the slot.
    var waited_ms: u64 = 0;
    var saw_session = server.sessionCount() > 0;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += 1) {
        _ = try server.stepOnce(.poll);
        saw_session = saw_session or server.sessionCount() > 0;
        if (saw_session and server.sessionCount() == 0 and server.quicConnectionCount() == 0) break;
        loopback.sleepMs(1);
    }
    try std.testing.expect(saw_session);
    try std.testing.expectEqual(@as(usize, 0), server.sessionCount());
    try std.testing.expectEqual(@as(u64, 1), server.handshakeTimeouts());
}

test "without the guard a half-open session is immortal (ablation)" {
    const allocator = std.testing.allocator;
    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .max_concurrent_connections = 2,
        .handshake_timeout_ms = null,
    });
    defer server.deinit();

    var abandoned = try abandonHalfOpenDial(allocator, &server);
    defer abandoned.deinit();

    // Step for well past the guarded test's deadline: the half-open stays.
    var waited_ms: u64 = 0;
    while (waited_ms < 700) : (waited_ms += 1) {
        _ = try server.stepOnce(.poll);
        loopback.sleepMs(1);
    }
    try std.testing.expect(server.sessionCount() > 0);
    try std.testing.expectEqual(@as(u64, 0), server.handshakeTimeouts());
}

test "client abandons a black-hole dial at the handshake deadline" {
    const allocator = std.testing.allocator;
    // A bound-but-never-serviced UDP socket: every Initial vanishes into it.
    const hole_addr_want = testListenAddr();
    const hole = try std.Io.net.IpAddress.bind(&hole_addr_want, std.testing.io, .{
        .mode = .dgram,
        .protocol = .udp,
    });
    defer hole.close(std.testing.io);

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = hole.address,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .handshake_timeout_ms = 250,
    });
    defer client.deinit();

    // run() must RETURN (the old behavior waited forever) with the
    // certified cause on the connection.
    client.run();
    try std.testing.expectEqual(events.DisconnectCause.handshake_timeout, client.closeCause());
}
