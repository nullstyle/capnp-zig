//! Frame-level coverage for the embedded (foreign-host) QUIC seat.
//!
//! A real capnp-zig QUIC client (`rpc.transport.quic.Connection.initClient`)
//! talks to a hand-rolled embedder that owns the UDP socket, the
//! `quic_zig.Server`, and ONE `quic.app.Driver`, routing connections by
//! negotiated ALPN into `rpc.transport.quic.EmbeddedSession` seats. This is
//! the inbound-attach shape a multi-protocol host (e.g. one also serving
//! `qmsg/1`) uses; the ALPN list below deliberately puts a foreign protocol
//! first to prove routing does not depend on position.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const loopback = @import("loopback_test_support.zig");

const quic = capnpc.rpc.transport.quic;

const loopback_cert_pem = loopback.loopback_cert_pem;
const loopback_key_pem = loopback.loopback_key_pem;

/// The foreign host: one App whose Driver hooks route `capnp-rpc/1`
/// connections to embedded sessions.
const HostApp = struct {
    allocator: std.mem.Allocator,
    mode: quic.EmbeddedSessionOptions,
    state: *loopback.QuicEndpointState,
    /// The seats' message callback; the default echoes each frame.
    on_message: quic.EmbeddedSession.MessageCallback = echoEmbeddedMessage,
    /// The seats' close callback; the default counts the close.
    on_close: quic.EmbeddedSession.CloseCallback = recordEmbeddedClose,
    seats: std.ArrayListUnmanaged(*quic.EmbeddedSession) = .empty,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub const ConnState = ?*quic.EmbeddedSession;
    pub const StreamState = void;

    fn onConnect(app: *HostApp, session: *D.Session) anyerror!void {
        // Created pre-handshake on purpose: a resumed (0-RTT) dial can push
        // stream bytes before the handshake completes, and the seat must
        // exist to buffer them.
        const seat = try quic.EmbeddedSession.create(app.allocator, session.conn, app.mode);
        session.app = seat;
        seat.start(
            app.state,
            app.on_message,
            recordEmbeddedError,
            app.on_close,
        );
        try app.seats.append(app.allocator, seat);
    }

    fn onHandshake(app: *HostApp, session: *D.Session) anyerror!void {
        if (session.app == null) return;
        if (quic.isCapnpSessionAlpn(session.conn)) return;
        // A foreign protocol riding the same listener: tear the capnp seat
        // back down. Real hosts hand the connection to their other
        // protocol's seat here instead.
        dropSeat(app, session);
    }

    fn onStreamOpen(app: *HostApp, session: *D.Session, entry: *D.StreamEntry, bidi: bool) anyerror!void {
        _ = app;
        const seat = session.app orelse return;
        try seat.onStreamOpen(entry.id, bidi);
    }

    fn onStreamData(app: *HostApp, session: *D.Session, entry: *D.StreamEntry, chunk: []const u8) anyerror!void {
        _ = app;
        const seat = session.app orelse return;
        try seat.onStreamData(entry.id, chunk);
    }

    fn onStreamEnd(app: *HostApp, session: *D.Session, entry: *D.StreamEntry, end: quic.quic_app.StreamEnd) anyerror!void {
        _ = app;
        const seat = session.app orelse return;
        seat.onStreamEnd(entry.id, end);
    }

    fn onDisconnect(app: *HostApp, session: *D.Session) void {
        const seat = session.app orelse return;
        seat.notifyDisconnected();
        dropSeat(app, session);
    }

    fn dropSeat(app: *HostApp, session: *D.Session) void {
        const seat = session.app orelse return;
        session.app = null;
        for (app.seats.items, 0..) |candidate, index| {
            if (candidate == seat) {
                _ = app.seats.swapRemove(index);
                break;
            }
        }
        seat.destroy();
    }
};

const D = quic.quic_app.Driver(HostApp);

fn echoEmbeddedMessage(seat: *quic.EmbeddedSession, frame: []const u8) anyerror!void {
    const state: *loopback.QuicEndpointState = @ptrCast(@alignCast(seat.context().?));
    try state.recordMessage(frame);
    try seat.sendFrame(frame);
}

fn recordEmbeddedError(seat: *quic.EmbeddedSession, err: anyerror) void {
    const state: *loopback.QuicEndpointState = @ptrCast(@alignCast(seat.context().?));
    state.last_error = err;
    _ = state.errors.fetchAdd(1, .acq_rel);
    seat.requestClose();
}

fn recordEmbeddedClose(seat: *quic.EmbeddedSession) void {
    const state: *loopback.QuicEndpointState = @ptrCast(@alignCast(seat.context().?));
    _ = state.closes.fetchAdd(1, .acq_rel);
}

/// The embedder's driving loop: receive+feed one datagram, service the ONE
/// Driver, service every live seat, drain outbound datagrams, then tick and
/// reap — the ordering the Driver contract prescribes.
fn runHost(host: anytype, listener: *quic.Listener, driver: anytype) void {
    var rx_buf: [4096]u8 = undefined;
    var tx_buf: [4096]u8 = undefined;
    while (!host.stop.load(.acquire)) {
        _ = listener.receiveOne(&rx_buf) catch break;
        driver.service(&listener.server) catch break;
        const now_us = listener.nowUs();
        for (host.seats.items) |seat| {
            // Errors surface through the session's own error callbacks.
            seat.service(now_us) catch {
                seat.requestClose();
            };
        }
        for (listener.server.iterator()) |slot| {
            const session = quic.Session.fromSlot(slot);
            listener.drainSessionDatagrams(session, &tx_buf, now_us) catch break;
        }
        listener.tick(now_us) catch break;
        _ = listener.reapClosedSessions();
    }
}

fn runEmbeddedEchoExchange(allocator: std.mem.Allocator, mode: quic.EmbeddedSessionOptions) !void {
    var host = HostApp{
        .allocator = allocator,
        .mode = mode,
        .state = undefined,
    };
    defer host.seats.deinit(allocator);

    var server_state = loopback.QuicEndpointState{};
    host.state = &server_state;
    var client_state = loopback.QuicEndpointState{};

    var driver = try D.init(.{
        .allocator = allocator,
        .app = &host,
        .max_tracked_streams = 16,
        .hooks = .{
            .on_connect = HostApp.onConnect,
            .on_handshake = HostApp.onHandshake,
            .on_stream_open = HostApp.onStreamOpen,
            .on_stream_data = HostApp.onStreamData,
            .on_stream_end = HostApp.onStreamEnd,
            .on_disconnect = HostApp.onDisconnect,
        },
    });
    var listener = quic.Listener.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .alpn_protocols = &.{ "foreign-protocol/1", "capnp-rpc/1" },
        .max_concurrent_connections = 4,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = mode.mode,
    }) catch |err| {
        driver.deinit();
        return err;
    };
    driver.attach(&listener.server);
    // Deinit order is load-bearing: the Driver must OUTLIVE the server,
    // because server.deinit() fires the will-close hook into the driver
    // (`driver.deinit()` undefined-ifies its memory). LIFO defers mean the
    // driver's defer is registered first so it runs last.
    defer driver.deinit();
    defer listener.deinit();

    var host_thread = try std.Thread.spawn(.{}, runHost, .{ &host, &listener, &driver });
    defer {
        host.stop.store(true, .release);
        host_thread.join();
    }

    const server_addr = listener.getAddress();
    try std.testing.expect(server_addr == .ip4);
    try std.testing.expect(server_addr.ip4.port != 0);

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = mode.mode,
    });
    defer client.deinit();
    client.start(&client_state, loopback.captureQuicMessage, loopback.recordQuicError, loopback.recordQuicClose);

    const frame = try loopback.buildBootstrapFrame(allocator, 0);
    defer allocator.free(frame);
    try client.sendFrame(frame);

    var client_thread = try std.Thread.spawn(.{}, loopback.runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        client_thread.join();
    };

    const exchanged = loopback.waitForClientMessageOrError(&client_state, &server_state);
    try std.testing.expect(exchanged);
    try std.testing.expectEqualStrings(frame, client_state.receivedSlice());

    client.requestClose();
    client_thread.join();
    joined = true;
}

test "embedded quic session echoes a baseline frame over a foreign host loop" {
    const allocator = std.testing.allocator;
    try runEmbeddedEchoExchange(allocator, .{ .mode = .baseline });
}

test "embedded quic session echoes a native inline frame over a foreign host loop" {
    const allocator = std.testing.allocator;
    try runEmbeddedEchoExchange(allocator, .{ .mode = .native });
}

const raw_faults = @import("raw_fault_client.zig");

/// The seat refuses peer streams the protocol never uses, so they do not
/// hold the host's stream window. The host's Driver has table room for
/// every stream here, so any refusal comes from the seat, not the Driver.
fn runEmbeddedRefusal(allocator: std.mem.Allocator, mode: quic.EmbeddedSessionOptions) !void {
    var host = HostApp{
        .allocator = allocator,
        .mode = mode,
        .state = undefined,
    };
    defer host.seats.deinit(allocator);

    var server_state = loopback.QuicEndpointState{};
    host.state = &server_state;

    var driver = try D.init(.{
        .allocator = allocator,
        .app = &host,
        .max_tracked_streams = 64,
        .hooks = .{
            .on_connect = HostApp.onConnect,
            .on_handshake = HostApp.onHandshake,
            .on_stream_open = HostApp.onStreamOpen,
            .on_stream_data = HostApp.onStreamData,
            .on_stream_end = HostApp.onStreamEnd,
            .on_disconnect = HostApp.onDisconnect,
        },
    });
    var params = quic.defaultTransportParams();
    params.initial_max_streams_bidi = raw_faults.refusal_case_window;
    params.initial_max_streams_uni = raw_faults.refusal_case_window;
    var listener = quic.Listener.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .alpn_protocols = &.{"capnp-rpc/1"},
        .transport_params = params,
        .max_concurrent_connections = 4,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = mode.mode,
    }) catch |err| {
        driver.deinit();
        return err;
    };
    driver.attach(&listener.server);
    // Same load-bearing deinit order as runEmbeddedEchoExchange.
    defer driver.deinit();
    defer listener.deinit();

    var host_thread = try std.Thread.spawn(.{}, runHost, .{ &host, &listener, &driver });
    defer {
        host.stop.store(true, .release);
        host_thread.join();
    }

    try raw_faults.openUnexpectedPeerStreams(allocator, listener.getAddress(), &server_state, mode.mode, 40);
    try std.testing.expectEqual(@as(usize, 0), server_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server_state.closes.load(.acquire));
}

test "embedded quic session refuses peer streams it never uses (baseline)" {
    try runEmbeddedRefusal(std.testing.allocator, .{ .mode = .baseline });
}

test "embedded quic session refuses peer streams it never uses (native)" {
    try runEmbeddedRefusal(std.testing.allocator, .{ .mode = .native });
}

// The stream-end trap in the seat: a data stream's bytes reach the seat
// before its announcement, and its end follows alone. Since quic-zig v0.28.0
// the end comes as `.fin` or `.reset` in both orders; when the host ticks
// first, quic-zig has already freed the stream, and the seat reads how it
// ended from `Connection.streamRecvEnd`. Either way the seat must keep the
// bytes (and the final size and code of a RESET), finish the frame when the
// announcement arrives, then free the stream's buffer. Ablation: with
// `onStreamEnd` dropping the bytes on a reset (the v0.19.1 code), both RESET
// cases lose the stream's buffer; with the seat reading the RESET from the
// live stream only (`conn.stream(id).recv.reset`, the v0.27.0-era code), the
// trap-order RESET case keeps no final size and no code; with
// `releaseDrainedDataStreams` removed, the seat still holds the data stream
// (2 buffers, not 1).

const stream_end = @import("stream_end_support.zig");

test "embedded native seat keeps a data stream's bytes past its end, service then tick" {
    try stream_end.runEmbedded(.fin, .service_then_tick);
    try stream_end.runEmbedded(.reset, .service_then_tick);
}

test "embedded native seat keeps a data stream's bytes past its end, tick then service (the trap order)" {
    try stream_end.runEmbedded(.fin, .tick_then_service);
    try stream_end.runEmbedded(.reset, .tick_then_service);
}

test "embedded native seat judges a reset data stream by the final size of the RESET, as the owned server does" {
    // A RESET whose final size is larger than the announced length fails the
    // frame with `InvalidFrame`; a RESET with bytes missing fails it with
    // `DataStreamReset`. Ablation: when the seat ignores the RESET (its final
    // size is then the bytes in hand), the longer RESET completes the frame
    // and the cut one fails with `InvalidFrame`.
    try stream_end.runEmbeddedReset(.longer_than_announced, .service_then_tick);
    try stream_end.runEmbeddedReset(.cut, .service_then_tick);
}

test "embedded native seat judges a reset data stream by the final size of the RESET when a tick freed it first (the trap order)" {
    // quic-zig v0.28.0 reports `.reset` for a stream that a tick freed before
    // the Driver read it, and keeps the RESET's final size and code in its
    // note of the end. Ablation: with the seat reading the RESET from the
    // live stream only, the seat keeps no RESET for the stream, and without
    // that check the longer RESET completes the frame.
    try stream_end.runEmbeddedReset(.longer_than_announced, .tick_then_service);
    try stream_end.runEmbeddedReset(.cut, .tick_then_service);
}

test "embedded native seat drops a stream that this side stopped, and never completes a frame from it" {
    // Since quic-zig v0.28.0 the Driver reports a stopped stream as `.reaped`
    // (its end came before the peer answered the stop) or as `.reset` (the
    // peer answered the stop with RESET_STREAM), also while it is live.
    // Ablation: with `classifyEnd` using the rule of
    // the v0.27.0 era (a `.reaped` of a freed stream is a GC end, any other
    // `.reaped` is teardown), the trap order keeps the stopped stream's
    // bytes, and without that check it completes a frame from them.
    try stream_end.runEmbeddedStopped(.service_then_tick);
    try stream_end.runEmbeddedStopped(.tick_then_service);
}

test "embedded native seat closes the session on a RESET of stream 0, also when a tick freed stream 0 first" {
    // Ablation: with stream 0 dropping its entry on a reset without the
    // session loss, the seat never closes, and each order (run alone first)
    // times out.
    try stream_end.runEmbeddedControlReset(.service_then_tick);
    try stream_end.runEmbeddedControlReset(.tick_then_service);
}

test "embedded native seat closes the session when the host stops stream 0 and the peer then ends it" {
    // A stop throws away what arrives after it, so stream 0 lost bytes. The
    // peer answers a stop with a RESET (the Driver reports `.reset`) or ends
    // with a FIN that left first (`.reaped`); `streamRecvEnd(0).stopped` is
    // true either way. Ablation: with a stopped stream 0 dropping its entry
    // without the session loss, the seat never closes, and every case (run
    // alone first) times out.
    try stream_end.runEmbeddedControlStopped(.reset, .service_then_tick);
    try stream_end.runEmbeddedControlStopped(.reset, .tick_then_service);
    try stream_end.runEmbeddedControlStopped(.fin, .service_then_tick);
    try stream_end.runEmbeddedControlStopped(.fin, .tick_then_service);
}

test "embedded native seat keeps the peer's close cause when a stopped stream 0 ends as the connection closes" {
    // The end of stream 0 closes the session, but the peer's CONNECTION_CLOSE
    // came first, so the cause stays `peer_close`. Ablation: with
    // `requestControlStreamLoss` setting `transport_error` without reading
    // the close that began first, each order (run alone first) gives
    // `transport_error`.
    try stream_end.runEmbeddedControlStoppedThenPeerClose(.service_then_tick);
    try stream_end.runEmbeddedControlStoppedThenPeerClose(.tick_then_service);
}

fn countEmbeddedClientMessage(conn: *quic.Connection, frame: []const u8) anyerror!void {
    const state: *loopback.QuicEndpointState = @ptrCast(@alignCast(conn.context().?));
    try state.recordMessage(frame);
}

/// A capnp-zig client sends `frame_count` frames large enough for native
/// data streams; the seat echoes each one. After the last echo, the seat
/// holds no buffer for any of those data streams. Ablation: without
/// `releaseDrainedDataStreams` (and on the seat code before it), the seat
/// holds 9 buffers after 8 frames.
fn runEmbeddedDataStreamRelease(allocator: std.mem.Allocator, frame_count: usize) !void {
    // Room for every frame at once: the client queues them all before
    // its loop runs.
    const native_options = quic.NativeOptions{
        .inline_frame_threshold = 128,
        .max_control_frame_bytes = 256,
        .max_pending_data_streams = 16,
        .max_pending_data_bytes = 16 * 1024,
    };
    var host = HostApp{
        .allocator = allocator,
        .mode = .{ .mode = .native, .native = native_options },
        .state = undefined,
    };
    defer host.seats.deinit(allocator);
    var server_state = loopback.QuicEndpointState{};
    host.state = &server_state;
    var client_state = loopback.QuicEndpointState{};

    var driver = try D.init(.{
        .allocator = allocator,
        .app = &host,
        .max_tracked_streams = 16,
        .hooks = .{
            .on_connect = HostApp.onConnect,
            .on_handshake = HostApp.onHandshake,
            .on_stream_open = HostApp.onStreamOpen,
            .on_stream_data = HostApp.onStreamData,
            .on_stream_end = HostApp.onStreamEnd,
            .on_disconnect = HostApp.onDisconnect,
        },
    });
    var listener = quic.Listener.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .alpn_protocols = &.{"capnp-rpc/1"},
        .max_concurrent_connections = 4,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    }) catch |err| {
        driver.deinit();
        return err;
    };
    driver.attach(&listener.server);
    // Same load-bearing deinit order as runEmbeddedEchoExchange.
    defer driver.deinit();
    defer listener.deinit();

    var host_thread = try std.Thread.spawn(.{}, runHost, .{ &host, &listener, &driver });
    var host_joined = false;
    defer if (!host_joined) {
        host.stop.store(true, .release);
        host_thread.join();
    };

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = listener.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer client.deinit();
    client.start(&client_state, countEmbeddedClientMessage, loopback.recordQuicError, loopback.recordQuicClose);

    for (0..frame_count) |index| {
        const frame = try loopback.buildCallFrameWithData(allocator, @intCast(index), 512);
        defer allocator.free(frame);
        try std.testing.expect(frame.len > native_options.inline_frame_threshold);
        try client.sendFrame(frame);
    }

    var client_thread = try std.Thread.spawn(.{}, loopback.runQuicConnection, .{&client});
    var client_joined = false;
    defer if (!client_joined) {
        client.requestClose();
        client_thread.join();
    };

    var waited_ms: u64 = 0;
    while (client_state.messages.load(.acquire) < frame_count) : (waited_ms += loopback.loopback_poll_ms) {
        if (client_state.errors.load(.acquire) > 0 or server_state.errors.load(.acquire) > 0) break;
        if (waited_ms >= loopback.loopback_timeout_ms) return error.QuicLoopbackTimedOut;
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    try std.testing.expectEqual(@as(usize, 0), server_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), client_state.errors.load(.acquire));
    try std.testing.expectEqual(frame_count, server_state.messages.load(.acquire));

    // Stop the host loop, then read the seat on this thread.
    host.stop.store(true, .release);
    host_thread.join();
    host_joined = true;
    try std.testing.expectEqual(@as(usize, 1), host.seats.items.len);
    const seat = host.seats.items[0];
    try std.testing.expectEqual(@as(usize, 1), seat.streams.count());
    try std.testing.expect(seat.streams.get(quic.baseline_stream_id) != null);
    try std.testing.expectEqual(@as(usize, 0), seat.ended_data_streams);

    client.requestClose();
    client_thread.join();
    client_joined = true;
}

test "embedded native seat frees each data stream's buffer once the engine has read it" {
    try runEmbeddedDataStreamRelease(std.testing.allocator, 8);
}

/// The seat in the memory-budget test: `endpoint` is what `HostApp` hands
/// each seat as its context, and the reply rides next to it.
const LargeReplySeatState = struct {
    endpoint: loopback.QuicEndpointState = .{},
    reply: []const u8,
};

fn replyOnceWithLargeFrame(seat: *quic.EmbeddedSession, _: []const u8) anyerror!void {
    const endpoint: *loopback.QuicEndpointState = @ptrCast(@alignCast(seat.context().?));
    const state: *LargeReplySeatState = @fieldParentPtr("endpoint", endpoint);
    if (endpoint.messages.fetchAdd(1, .acq_rel) == 0) try seat.sendFrame(state.reply);
}

/// The client in the memory-budget test: compares the reply with the frame
/// the seat sent, byte for byte.
const LargeReplyClientState = struct {
    expected: []const u8,
    messages: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    matched: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    errors: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
};

fn checkEmbeddedLargeReply(conn: *quic.Connection, frame: []const u8) anyerror!void {
    const state: *LargeReplyClientState = @ptrCast(@alignCast(conn.context().?));
    if (std.mem.eql(u8, frame, state.expected)) _ = state.matched.fetchAdd(1, .acq_rel);
    _ = state.messages.fetchAdd(1, .acq_rel);
}

fn recordEmbeddedLargeReplyClientError(conn: *quic.Connection, _: anyerror) void {
    const state: *LargeReplyClientState = @ptrCast(@alignCast(conn.context().?));
    _ = state.errors.fetchAdd(1, .acq_rel);
    conn.requestClose();
}

fn countEmbeddedLargeReplyClientClose(conn: *quic.Connection) void {
    const state: *LargeReplyClientState = @ptrCast(@alignCast(conn.context().?));
    _ = state.closes.fetchAdd(1, .acq_rel);
}

/// The seat's writes leave half of the host's `max_connection_memory` for
/// what the peer sends, as the owned loops' writes do
/// (`quic_zig_adapter.streamWrite`). A 1 MiB reply goes through a 256 KiB
/// budget while the client sends a small frame every millisecond, and
/// arrives whole. A write that took the whole budget (quic-zig's own
/// streamWrite) left no room for the client's next frame: quic-zig closed the
/// connection with EXCESSIVE_LOAD.
fn runEmbeddedLargeReplyBesideClientFrames(allocator: std.mem.Allocator, mode: quic.EmbeddedSessionOptions) !void {
    const budget: u64 = 256 * 1024;
    const reply = try loopback.buildCallFrameWithData(allocator, 0xB0D6E9, 1024 * 1024);
    defer allocator.free(reply);
    try std.testing.expect(reply.len > 4 * budget);
    const request = try loopback.buildBootstrapFrame(allocator, 0xB0D8);
    defer allocator.free(request);
    const small = try loopback.buildBootstrapFrame(allocator, 0x5A12);
    defer allocator.free(small);

    var seat_state = LargeReplySeatState{ .reply = reply };
    var host = HostApp{
        .allocator = allocator,
        .mode = mode,
        .state = &seat_state.endpoint,
        .on_message = replyOnceWithLargeFrame,
    };
    defer host.seats.deinit(allocator);

    var driver = try D.init(.{
        .allocator = allocator,
        .app = &host,
        .max_tracked_streams = 16,
        .hooks = .{
            .on_connect = HostApp.onConnect,
            .on_handshake = HostApp.onHandshake,
            .on_stream_open = HostApp.onStreamOpen,
            .on_stream_data = HostApp.onStreamData,
            .on_stream_end = HostApp.onStreamEnd,
            .on_disconnect = HostApp.onDisconnect,
        },
    });
    var listener = quic.Listener.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .alpn_protocols = &.{"capnp-rpc/1"},
        .max_concurrent_connections = 4,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = mode.mode,
        .max_connection_memory = budget,
    }) catch |err| {
        driver.deinit();
        return err;
    };
    driver.attach(&listener.server);
    // Same load-bearing deinit order as runEmbeddedEchoExchange.
    defer driver.deinit();
    defer listener.deinit();

    var host_thread = try std.Thread.spawn(.{}, runHost, .{ &host, &listener, &driver });
    defer {
        host.stop.store(true, .release);
        host_thread.join();
    }

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = listener.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = mode.mode,
    });
    defer client.deinit();
    var client_state = LargeReplyClientState{ .expected = reply };
    client.start(&client_state, checkEmbeddedLargeReply, recordEmbeddedLargeReplyClientError, countEmbeddedLargeReplyClientClose);
    try client.sendFrame(request);

    var client_thread = try std.Thread.spawn(.{}, loopback.runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        client_thread.join();
    };

    const seat_endpoint = &seat_state.endpoint;
    const max_small_frames: usize = 2_000;
    var small_frames: usize = 0;
    var waited_ms: u64 = 0;
    while (waited_ms < 20_000) : (waited_ms += 1) {
        if (client_state.messages.load(.acquire) > 0) break;
        if (client_state.errors.load(.acquire) > 0 or seat_endpoint.errors.load(.acquire) > 0) break;
        if (client_state.closes.load(.acquire) > 0 or seat_endpoint.closes.load(.acquire) > 0) break;
        if (small_frames < max_small_frames) {
            try client.sendFrame(small);
            small_frames += 1;
        }
        loopback.sleepMs(1);
    }
    client.requestClose();
    client_thread.join();
    joined = true;

    const reply_matched = client_state.matched.load(.acquire);
    if (reply_matched != 1) {
        std.debug.print(
            "reply not delivered: waited {d} ms, {d} small frames sent, seat saw {d} frames, client close cause {s}\n",
            .{ waited_ms, small_frames, seat_endpoint.messages.load(.acquire), @tagName(client.closeCause()) },
        );
        if (client.quicCloseEvent()) |ev| std.debug.print("client QUIC close: code 0x{x}, source {s}\n", .{ ev.error_code, @tagName(ev.source) });
    }
    try std.testing.expectEqual(@as(usize, 0), seat_endpoint.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), client_state.errors.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), reply_matched);
    // The first small frame goes out with the request; at least one more
    // went out while the reply was on its way.
    try std.testing.expect(small_frames >= 2);
}

test "embedded quic seat leaves memory budget for client frames during a large reply (baseline)" {
    try runEmbeddedLargeReplyBesideClientFrames(std.testing.allocator, .{ .mode = .baseline });
}

test "embedded quic seat leaves memory budget for client frames during a large reply (native)" {
    try runEmbeddedLargeReplyBesideClientFrames(std.testing.allocator, .{ .mode = .native });
}

// ---------------------------------------------------------------------------
// A seat whose client vanishes with a reply queued. Through v0.24.0 the owned
// loops ended a closed QUIC connection only once the engine's outbound queue
// was empty (rpc_quic_transport_test.zig, "A closed connection with frames
// still queued"). The seat never had that condition: its service pass runs
// the close callback once quic-zig reports the connection closed, whatever
// is still queued. This test pins that down.
// ---------------------------------------------------------------------------

/// Seat state of the vanishing-client test: `endpoint` is what `HostApp`
/// hands the seat as its context, and the reply rides next to it. The close
/// callback records, on the host thread, whether the reply was still queued
/// and the cause; `endpoint.closes` publishes both.
const VanishingClientSeatState = struct {
    endpoint: loopback.QuicEndpointState = .{},
    reply: []const u8,
    queued_at_close: bool = false,
    /// Whether quic-zig had already latched the terminal closed state, which
    /// the host's reap waits for: the seat should close before, in the
    /// service pass that sees the connection closed (draining).
    terminal_at_close: bool = false,
    cause_at_close: ?capnpc.rpc.events.DisconnectCause = null,
};

fn replyOnceToVanishingClient(seat: *quic.EmbeddedSession, _: []const u8) anyerror!void {
    const endpoint: *loopback.QuicEndpointState = @ptrCast(@alignCast(seat.context().?));
    const state: *VanishingClientSeatState = @fieldParentPtr("endpoint", endpoint);
    if (endpoint.messages.fetchAdd(1, .acq_rel) == 0) try seat.sendFrame(state.reply);
}

fn recordVanishingClientSeatClose(seat: *quic.EmbeddedSession) void {
    const endpoint: *loopback.QuicEndpointState = @ptrCast(@alignCast(seat.context().?));
    const state: *VanishingClientSeatState = @fieldParentPtr("endpoint", endpoint);
    state.queued_at_close = switch (seat.mode) {
        .baseline => !seat.baseline.outboundEmpty(),
        .native => !seat.native.outboundEmpty(),
    };
    state.terminal_at_close = seat.conn.closeState() == .closed;
    state.cause_at_close = seat.closeCause();
    _ = endpoint.closes.fetchAdd(1, .acq_rel);
}

fn ignoreEmbeddedClientFrame(_: *quic.Connection, _: []const u8) anyerror!void {}

/// The client sends one frame and is never stepped again once the seat has
/// it. The seat's reply is larger than its stream send buffer (quic-zig's
/// 1 MiB) and nothing acknowledges it, so the rest is still queued when the
/// idle timeout closes the connection. The seat's close callback must run
/// then; a 10 s watchdog fails the test instead of hanging.
fn runEmbeddedSeatClosesWhenClientVanishes(allocator: std.mem.Allocator, mode: quic.EmbeddedSessionOptions) !void {
    const idle_timeout_ms: u64 = 500;
    const watchdog_ms: u64 = 10_000;
    const reply = try loopback.buildCallFrameWithData(allocator, 0xFADE2, 1536 * 1024);
    defer allocator.free(reply);
    const request = try loopback.buildBootstrapFrame(allocator, 0xFADE3);
    defer allocator.free(request);

    var params = quic.defaultTransportParams();
    params.max_idle_timeout_ms = idle_timeout_ms;

    var seat_state = VanishingClientSeatState{ .reply = reply };
    var host = HostApp{
        .allocator = allocator,
        .mode = mode,
        .state = &seat_state.endpoint,
        .on_message = replyOnceToVanishingClient,
        .on_close = recordVanishingClientSeatClose,
    };
    defer host.seats.deinit(allocator);

    var driver = try D.init(.{
        .allocator = allocator,
        .app = &host,
        .max_tracked_streams = 16,
        .hooks = .{
            .on_connect = HostApp.onConnect,
            .on_handshake = HostApp.onHandshake,
            .on_stream_open = HostApp.onStreamOpen,
            .on_stream_data = HostApp.onStreamData,
            .on_stream_end = HostApp.onStreamEnd,
            .on_disconnect = HostApp.onDisconnect,
        },
    });
    var listener = quic.Listener.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .alpn_protocols = &.{"capnp-rpc/1"},
        .max_concurrent_connections = 4,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .transport_params = params,
        .mode = mode.mode,
    }) catch |err| {
        driver.deinit();
        return err;
    };
    driver.attach(&listener.server);
    // Same load-bearing deinit order as runEmbeddedEchoExchange.
    defer driver.deinit();
    defer listener.deinit();

    var host_thread = try std.Thread.spawn(.{}, runHost, .{ &host, &listener, &driver });
    defer {
        host.stop.store(true, .release);
        host_thread.join();
    }

    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = listener.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .transport_params = params,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = mode.mode,
    });
    defer client.deinit();
    var client_state = loopback.QuicEndpointState{};
    client.start(&client_state, ignoreEmbeddedClientFrame, loopback.recordQuicError, loopback.recordQuicClose);
    try client.sendFrame(request);

    // The client steps on this thread until the seat has the request (and
    // has queued the reply), then vanishes: it is never stepped again.
    const seat_endpoint = &seat_state.endpoint;
    var waited_ms: u64 = 0;
    while (seat_endpoint.messages.load(.acquire) == 0) : (waited_ms += 1) {
        if (waited_ms >= loopback.loopback_timeout_ms) return error.QuicLoopbackTimedOut;
        _ = try client.stepOnce(.poll);
        loopback.sleepMs(1);
    }

    waited_ms = 0;
    while (seat_endpoint.closes.load(.acquire) == 0 and waited_ms < watchdog_ms) : (waited_ms += loopback.loopback_poll_ms) {
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    if (seat_endpoint.closes.load(.acquire) == 0) {
        std.debug.print("the {s} seat did not close within {d} ms (idle timeout {d} ms)\n", .{ @tagName(mode.mode), watchdog_ms, idle_timeout_ms });
        return error.EmbeddedSeatNeverClosed;
    }
    // The case under test: the reply was still queued when the seat closed.
    try std.testing.expect(seat_state.queued_at_close);
    try std.testing.expectEqual(@as(usize, 1), seat_endpoint.closes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), seat_endpoint.errors.load(.acquire));
    try std.testing.expectEqual(capnpc.rpc.events.DisconnectCause.idle_timeout, seat_state.cause_at_close orelse return error.NoCloseCause);
    // The seat closed in the service pass that saw the connection closed,
    // while it drained, not at the host's reap of the terminal connection.
    try std.testing.expect(!seat_state.terminal_at_close);
}

test "embedded seat whose client vanishes with a reply queued gets its close at the idle timeout (baseline)" {
    try runEmbeddedSeatClosesWhenClientVanishes(std.testing.allocator, .{ .mode = .baseline });
}

test "embedded seat whose client vanishes with a reply queued gets its close at the idle timeout (native)" {
    try runEmbeddedSeatClosesWhenClientVanishes(std.testing.allocator, .{ .mode = .native });
}

// ---------------------------------------------------------------------------
// Peer-level coverage: a real `Peer` attached to an embedded session over a
// foreign host loop — the full Bootstrap → Call → Return → Finish lifecycle
// through the connection-shape facade (`Peer.init` duck-typing, `on_tick`
// deadline sweeps on the host thread, transport close notification).
// Modeled on rpc_quic_peer_test.zig's `runBasic`.
// ---------------------------------------------------------------------------

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const Peer = capnpc.rpc.peer.Peer;

const PeerClientState = struct {
    bootstrap_returned: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    call_returned: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn onBootstrap(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *PeerClientState = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.ExpectedBootstrapResults;
        const results = ret.results orelse return error.MissingBootstrapResults;
        const descriptor = try results.content.getCapability();
        const resolved = try caps.resolveCapability(descriptor);
        self.bootstrap_returned.store(true, .release);
        _ = try peer.sendCallResolved(
            resolved,
            0x5155_4943,
            7,
            self,
            buildCall,
            onCallReturn,
        );
    }

    fn buildCall(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        _ = ctx_ptr;
        _ = try call.initCapTableTyped(0);
    }

    fn onCallReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *PeerClientState = @ptrCast(@alignCast(ctx_ptr));
        if (ret.tag != .results) return error.ExpectedCallResults;
        self.call_returned.store(true, .release);
    }

    fn peerError(ctx: ?*anyopaque, _: *Peer, _: anyerror) void {
        const self: *PeerClientState = @ptrCast(@alignCast(ctx.?));
        self.failed.store(true, .release);
    }

    fn peerClose(ctx: ?*anyopaque, _: *Peer) void {
        const self: *PeerClientState = @ptrCast(@alignCast(ctx.?));
        _ = self.closes.fetchAdd(1, .acq_rel);
    }
};

const PeerServerState = struct {
    calls: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    closes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

    fn onCall(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        call: protocol.Call,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *PeerServerState = @ptrCast(@alignCast(ctx_ptr));
        _ = self.calls.fetchAdd(1, .acq_rel);
        try peer.sendReturnEmptyStruct(call.question_id);
    }

    fn peerError(ctx: ?*anyopaque, _: *Peer, _: anyerror) void {
        const self: *PeerServerState = @ptrCast(@alignCast(ctx.?));
        self.failed.store(true, .release);
    }

    fn peerClose(ctx: ?*anyopaque, _: *Peer) void {
        const self: *PeerServerState = @ptrCast(@alignCast(ctx.?));
        _ = self.closes.fetchAdd(1, .acq_rel);
    }
};

/// Foreign host whose capnp sessions get real `Peer`s. The host creates the
/// seat and the Peer; the PEER owns the seat's callbacks from there on —
/// nothing calls `session.start()` by hand, which is precisely the facade
/// contract being exercised.
const PeerHostApp = struct {
    allocator: std.mem.Allocator,
    options: quic.EmbeddedSessionOptions,
    server_state: *PeerServerState,
    seats: std.ArrayListUnmanaged(*quic.EmbeddedSession) = .empty,
    peers: std.ArrayListUnmanaged(*Peer) = .empty,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub const ConnState = ?*quic.EmbeddedSession;
    pub const StreamState = void;

    fn onConnect(app: *PeerHostApp, session: *PeerD.Session) anyerror!void {
        const seat = try quic.EmbeddedSession.create(app.allocator, session.conn, app.options);
        session.app = seat;
        try app.seats.append(app.allocator, seat);
    }

    fn onHandshake(app: *PeerHostApp, session: *PeerD.Session) anyerror!void {
        const seat = session.app orelse return;
        if (quic.isCapnpSessionAlpn(session.conn)) {
            const peer = try app.allocator.create(Peer);
            errdefer app.allocator.destroy(peer);
            peer.* = Peer.init(app.allocator, seat);
            peer.enableRuntimeThreadChecks(true);
            _ = try peer.setBootstrap(.{ .ctx = app.server_state, .on_call = PeerServerState.onCall });
            peer.start(app.server_state, PeerServerState.peerError, PeerServerState.peerClose);
            try app.peers.append(app.allocator, peer);
        } else {
            dropSeat(app, session);
        }
    }

    fn onStreamOpen(app: *PeerHostApp, session: *PeerD.Session, entry: *D.StreamEntry, bidi: bool) anyerror!void {
        _ = app;
        const seat = session.app orelse return;
        try seat.onStreamOpen(entry.id, bidi);
    }

    fn onStreamData(app: *PeerHostApp, session: *PeerD.Session, entry: *D.StreamEntry, chunk: []const u8) anyerror!void {
        _ = app;
        const seat = session.app orelse return;
        try seat.onStreamData(entry.id, chunk);
    }

    fn onStreamEnd(app: *PeerHostApp, session: *PeerD.Session, entry: *D.StreamEntry, end: quic.quic_app.StreamEnd) anyerror!void {
        _ = app;
        const seat = session.app orelse return;
        seat.onStreamEnd(entry.id, end);
    }

    fn onDisconnect(app: *PeerHostApp, session: *PeerD.Session) void {
        const seat = session.app orelse return;
        seat.notifyDisconnected();
        dropSeat(app, session);
    }

    fn dropSeat(app: *PeerHostApp, session: *PeerD.Session) void {
        const seat = session.app orelse return;
        session.app = null;
        for (app.seats.items, 0..) |candidate, index| {
            if (candidate == seat) {
                _ = app.seats.swapRemove(index);
                break;
            }
        }
        seat.destroy();
    }
};

const PeerD = quic.quic_app.Driver(PeerHostApp);

fn runPeerHost(app: *PeerHostApp, listener: *quic.Listener, driver: *PeerD) void {
    defer {
        // The listener's will-close hooks still borrow each seat's Peer.
        // Deliver those callbacks on their owner thread before freeing peers,
        // including seats that the loop stopped before it could reap.
        listener.deinit();
        for (app.peers.items) |peer| {
            peer.deinit();
            app.allocator.destroy(peer);
        }
        app.peers.clearRetainingCapacity();
    }
    var rx_buf: [4096]u8 = undefined;
    var tx_buf: [4096]u8 = undefined;
    while (!app.stop.load(.acquire)) {
        _ = listener.receiveOne(&rx_buf) catch break;
        driver.service(&listener.server) catch break;
        const now_us = listener.nowUs();
        for (app.seats.items) |seat| {
            seat.service(now_us) catch {
                seat.requestClose();
            };
        }
        for (listener.server.iterator()) |slot| {
            const session = quic.Session.fromSlot(slot);
            listener.drainSessionDatagrams(session, &tx_buf, now_us) catch break;
        }
        listener.tick(now_us) catch break;
        _ = listener.reapClosedSessions();
    }
}

test "Peer over an embedded quic session completes Bootstrap Call Return Finish" {
    const allocator = std.testing.allocator;

    var server_state = PeerServerState{};
    var app = PeerHostApp{
        .allocator = allocator,
        .options = .{ .mode = .baseline },
        .server_state = &server_state,
    };
    defer app.seats.deinit(allocator);
    defer app.peers.deinit(allocator);

    var driver = try PeerD.init(.{
        .allocator = allocator,
        .app = &app,
        .max_tracked_streams = 16,
        .hooks = .{
            .on_connect = PeerHostApp.onConnect,
            .on_handshake = PeerHostApp.onHandshake,
            .on_stream_open = PeerHostApp.onStreamOpen,
            .on_stream_data = PeerHostApp.onStreamData,
            .on_stream_end = PeerHostApp.onStreamEnd,
            .on_disconnect = PeerHostApp.onDisconnect,
        },
    });
    var listener = quic.Listener.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .alpn_protocols = &.{"capnp-rpc/1"},
        .max_concurrent_connections = 4,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .baseline,
    }) catch |err| {
        driver.deinit();
        return err;
    };
    driver.attach(&listener.server);
    defer driver.deinit();

    const server_addr = listener.getAddress();
    // A successfully started host owns listener and Peer teardown. On spawn
    // failure there are no peers or callbacks, so clean up here instead.
    var host_thread = std.Thread.spawn(.{}, runPeerHost, .{ &app, &listener, &driver }) catch |err| {
        listener.deinit();
        return err;
    };
    var host_joined = false;
    defer if (!host_joined) {
        app.stop.store(true, .release);
        host_thread.join();
    };

    var client_state = PeerClientState{};
    var client = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
    });
    defer client.deinit();
    var client_peer = Peer.init(allocator, &client);
    defer client_peer.deinit();
    client_peer.disableThreadAffinity();
    client_peer.start(&client_state, PeerClientState.peerError, PeerClientState.peerClose);

    _ = try client_peer.sendBootstrap(&client_state, PeerClientState.onBootstrap);

    var client_thread = try std.Thread.spawn(.{}, loopback.runQuicConnection, .{&client});
    var joined = false;
    defer if (!joined) {
        client.requestClose();
        client_thread.join();
    };

    var waited_ms: u64 = 0;
    var completed = false;
    while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
        if (client_state.call_returned.load(.acquire)) {
            completed = true;
            break;
        }
        if (client_state.failed.load(.acquire) or server_state.failed.load(.acquire)) break;
        loopback.sleepMs(loopback.loopback_poll_ms);
    }
    if (completed) loopback.sleepMs(10);

    // Stop the host with a live client: shutdown must deliver the remaining
    // seat's close callback before destroying its Peer, even without a reap.
    app.stop.store(true, .release);
    host_thread.join();
    host_joined = true;
    client.requestClose();
    client_thread.join();
    joined = true;

    try std.testing.expectEqual(@as(usize, 1), server_state.closes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), app.seats.items.len);
    try std.testing.expectEqual(@as(usize, 0), app.peers.items.len);

    if (!completed) return error.EmbeddedPeerRoundTripTimedOut;
    try std.testing.expect(client_state.bootstrap_returned.load(.acquire));
    try std.testing.expect(!client_state.failed.load(.acquire));
    try std.testing.expect(!server_state.failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), server_state.calls.load(.acquire));
}

// ---------------------------------------------------------------------------
// 0-RTT coverage: a RESUMED dial through a foreign host. The resumed first
// flight pushes stream bytes before the handshake names the protocol, so the
// host buffers them pre-handshake (quic.prehandshake) and replays them into
// the seat at handshake time; the seat's replay hold (armed by
// `.early_data = .without_replay_protection`, the same posture as the host
// listener) keeps those frames from DISPATCHING until the handshake
// completes. Proves the whole composition: prehandshake replay + embedded
// seat + engine replay hold deliver the early frame exactly once, never
// before the handshake.
// ---------------------------------------------------------------------------

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

/// Echo state that also records whether any dispatch happened while the
/// handshake was still incomplete (the replay-execution guard's observable).
const EarlyGateState = struct {
    messages: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    errors: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    dispatched_before_handshake: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    hold_armed_at_creation: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Set at handshake time by the host thread when the pre-handshake
    /// buffer actually carried stream events for this connection.
    replayed_prehandshake_bytes: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
};

fn earlyEcho(seat: *quic.EmbeddedSession, frame: []const u8) anyerror!void {
    const st: *EarlyGateState = @ptrCast(@alignCast(seat.context().?));
    if (seat.activeQuicConnection()) |q| {
        if (!q.handshakeDone()) st.dispatched_before_handshake.store(true, .release);
    }
    _ = st.messages.fetchAdd(1, .acq_rel);
    try seat.sendFrame(frame);
}

fn earlyError(seat: *quic.EmbeddedSession, err: anyerror) void {
    std.debug.print("[0rtt] session error: {s}\n", .{@errorName(err)});
    const st: *EarlyGateState = @ptrCast(@alignCast(seat.context().?));
    _ = st.errors.fetchAdd(1, .acq_rel);
    seat.requestClose();
}

fn earlyClose(seat: *quic.EmbeddedSession) void {
    _ = seat;
}

const ZD = quic.quic_app.Driver(ZeroRttHostApp);

const ZeroRttHostApp = struct {
    allocator: std.mem.Allocator,
    state: *EarlyGateState,
    seats: std.ArrayListUnmanaged(*quic.EmbeddedSession) = .empty,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub const ConnState = struct {
        seat: ?*quic.EmbeddedSession = null,
        pending: ?quic.prehandshake.Buffer = null,
    };
    pub const StreamState = void;

    fn onHandshake(app: *ZeroRttHostApp, session: *ZD.Session) anyerror!void {
        if (!quic.isCapnpSessionAlpn(session.conn)) return;
        const seat = try quic.EmbeddedSession.create(app.allocator, session.conn, .{
            .early_data = .without_replay_protection, // mirror the host listener
        });
        session.app.seat = seat;
        // The hold must be armed by the options — the parity this test pins.
        app.state.hold_armed_at_creation.store(seat.baseline.defer_early_dispatch, .release);
        if (session.app.pending) |*pending| {
            if (!pending.isEmpty()) {
                app.state.replayed_prehandshake_bytes.store(pendingTotalBytes(pending), .release);
                try pending.replayInto(seat, zeroRttStreamEnd);
            }
            pending.deinit();
            session.app.pending = null;
        }
        seat.start(app.state, earlyEcho, earlyError, earlyClose);
        try app.seats.append(app.allocator, seat);
    }

    fn pendingTotalBytes(pending: *const quic.prehandshake.Buffer) usize {
        _ = pending;
        return 1; // nonzero marker; byte-exact accounting is the buffer's own tests'
    }

    fn zeroRttStreamEnd(kind: quic.prehandshake.EndKind) quic.quic_app.StreamEnd {
        return switch (kind) {
            .fin => .fin,
            .reset => .reset,
            .reaped => .reaped,
        };
    }

    fn onStreamOpen(app: *ZeroRttHostApp, session: *ZD.Session, entry: *ZD.StreamEntry, bidi: bool) anyerror!void {
        if (session.app.seat) |seat| return seat.onStreamOpen(entry.id, bidi);
        if (session.app.pending == null) session.app.pending = quic.prehandshake.Buffer.init(app.allocator);
        try session.app.pending.?.recordOpen(entry.id, bidi);
    }

    fn onStreamData(app: *ZeroRttHostApp, session: *ZD.Session, entry: *ZD.StreamEntry, chunk: []const u8) anyerror!void {
        if (session.app.seat) |seat| return seat.onStreamData(entry.id, chunk);
        if (session.app.pending == null) session.app.pending = quic.prehandshake.Buffer.init(app.allocator);
        try session.app.pending.?.recordData(entry.id, chunk);
    }

    fn onStreamEnd(app: *ZeroRttHostApp, session: *ZD.Session, entry: *ZD.StreamEntry, end: quic.quic_app.StreamEnd) anyerror!void {
        if (session.app.seat) |seat| return seat.onStreamEnd(entry.id, end);
        if (session.app.pending == null) session.app.pending = quic.prehandshake.Buffer.init(app.allocator);
        try session.app.pending.?.recordEnd(entry.id, switch (end) {
            .fin => .fin,
            .reset => .reset,
            .reaped => .reaped,
        });
    }

    fn onDisconnect(app: *ZeroRttHostApp, session: *ZD.Session) void {
        if (session.app.pending) |*pending| {
            pending.deinit();
            session.app.pending = null;
        }
        const seat = session.app.seat orelse return;
        session.app.seat = null;
        for (app.seats.items, 0..) |candidate, index| {
            if (candidate == seat) {
                _ = app.seats.swapRemove(index);
                break;
            }
        }
        seat.notifyDisconnected();
        seat.destroy();
    }
};

test "embedded quic session delivers a resumed 0-RTT frame once, never before the handshake" {
    const allocator = std.testing.allocator;

    var state = EarlyGateState{};
    var app = ZeroRttHostApp{ .allocator = allocator, .state = &state };
    defer app.seats.deinit(allocator);

    var driver = try ZD.init(.{
        .allocator = allocator,
        .app = &app,
        .max_tracked_streams = 16,
        .hooks = .{
            .on_handshake = ZeroRttHostApp.onHandshake,
            .on_stream_open = ZeroRttHostApp.onStreamOpen,
            .on_stream_data = ZeroRttHostApp.onStreamData,
            .on_stream_end = ZeroRttHostApp.onStreamEnd,
            .on_disconnect = ZeroRttHostApp.onDisconnect,
        },
    });
    var listener = quic.Listener.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback_cert_pem,
        .tls_key_pem = loopback_key_pem,
        .alpn_protocols = &.{"capnp-rpc/1"},
        .max_concurrent_connections = 2,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .early_data = .without_replay_protection,
    }) catch |err| {
        driver.deinit();
        return err;
    };
    driver.attach(&listener.server);
    defer driver.deinit();
    defer listener.deinit();

    var host_thread = try std.Thread.spawn(.{}, runHost, .{ &app, &listener, &driver });
    var host_started = false;
    defer if (!host_started) {
        app.stop.store(true, .release);
        host_thread.join();
    };

    const server_addr = listener.getAddress();
    const frame_first = try loopback.buildBootstrapFrame(allocator, 0x0AAA);
    defer allocator.free(frame_first);
    const frame_early = try loopback.buildBootstrapFrame(allocator, 0x0BBB);
    defer allocator.free(frame_early);

    // ---- Dial 1: earn the ticket under this listener's TLS context. -----
    var sink = ResumptionSink{};
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
        var client_state = loopback.QuicEndpointState{};
        client.start(&client_state, loopback.captureQuicMessage, loopback.recordQuicError, loopback.recordQuicClose);

        var client_thread = try std.Thread.spawn(.{}, loopback.runQuicConnection, .{&client});
        var joined = false;
        defer if (!joined) {
            client.requestClose();
            client_thread.join();
        };

        try client.sendFrame(frame_first);
        var waited_ms: u64 = 0;
        while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
            if (client_state.messages.load(.acquire) > 0 and sink.len.load(.acquire) > 0) break;
            if (client_state.errors.load(.acquire) > 0 or state.errors.load(.acquire) > 0) {
                return error.ZeroRttDialOneFailed;
            }
            loopback.sleepMs(loopback.loopback_poll_ms);
        }
        try std.testing.expect(sink.len.load(.acquire) > 0);
        try std.testing.expectEqual(@as(usize, 1), state.messages.load(.acquire));

        client.requestClose();
        client_thread.join();
        joined = true;
    }

    // ---- Dial 2: resume. The frame is enqueued BEFORE the run thread, so
    // it rides 0-RTT and arrives before the handshake completes; the host
    // buffers it pre-handshake and the seat holds dispatch until the
    // handshake lands. -----------------------------------------------------
    var client2 = try quic.Connection.initClient(allocator, std.testing.io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .resumption_state = sink.slice(),
    });
    defer client2.deinit();
    var client2_state = loopback.QuicEndpointState{};
    client2.start(&client2_state, loopback.captureQuicMessage, loopback.recordQuicError, loopback.recordQuicClose);
    try client2.sendFrame(frame_early);

    var client2_thread = try std.Thread.spawn(.{}, loopback.runQuicConnection, .{&client2});
    {
        var joined = false;
        defer if (!joined) {
            client2.requestClose();
            client2_thread.join();
        };

        var waited_ms: u64 = 0;
        var echoed = false;
        while (waited_ms < loopback.loopback_timeout_ms) : (waited_ms += loopback.loopback_poll_ms) {
            if (client2_state.messages.load(.acquire) > 0) {
                echoed = true;
                break;
            }
            if (client2_state.errors.load(.acquire) > 0 or state.errors.load(.acquire) > 0) {
                return error.ZeroRttDialTwoFailed;
            }
            loopback.sleepMs(loopback.loopback_poll_ms);
        }
        try std.testing.expect(echoed);
        try std.testing.expectEqualStrings(frame_early, client2_state.receivedSlice());

        client2.requestClose();
        client2_thread.join();
        joined = true;
    }

    app.stop.store(true, .release);
    host_thread.join();
    host_started = true;

    // Exactly one dispatch of the early frame (dial 1's + dial 2's).
    try std.testing.expectEqual(@as(usize, 2), state.messages.load(.acquire));
    // The parity this test exists to pin: the seat armed the replay hold
    // from the host listener's 0-RTT posture.
    try std.testing.expect(state.hold_armed_at_creation.load(.acquire));
    // Nothing ever dispatched before the handshake completed.
    try std.testing.expect(!state.dispatched_before_handshake.load(.acquire));
    // The resumed first flight really did carry pre-handshake stream bytes
    // through the host's buffer.
    try std.testing.expect(state.replayed_prehandshake_bytes.load(.acquire) > 0);
}

// ---------------------------------------------------------------------------
// Seat teardown. The close callback is the seat's last call into the host; a
// `destroy` from a seat callback frees the seat before the seat call returns;
// `requestClose` closes the QUIC connection; and quic-zig keeps no pointer
// into a freed seat. A raw quic client and the host loop both step on the
// test thread, so each order below is exact.
// ---------------------------------------------------------------------------

const quic_zig = @import("quic");
const RawFaultClient = raw_faults.RawFaultClient;

/// Counts the live allocations made through it, to see when a seat is
/// freed.
const CountingAllocator = struct {
    parent: std.mem.Allocator,
    live: usize = 0,

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        } };
    }

    fn cast(ctx: *anyopaque) *CountingAllocator {
        return @ptrCast(@alignCast(ctx));
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self = cast(ctx);
        const ptr = self.parent.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.live += 1;
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        return cast(ctx).parent.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        return cast(ctx).parent.rawRemap(memory, alignment, new_len, ret_addr);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self = cast(ctx);
        self.parent.rawFree(memory, alignment, ret_addr);
        self.live -= 1;
    }
};

/// A host with one seat. Its hooks route through `seat`, so a callback that
/// destroys the seat takes it out of the host by setting `seat` to null.
const TeardownHost = struct {
    allocator: std.mem.Allocator,
    /// The seat's own allocator (a `CountingAllocator`).
    seat_allocator: std.mem.Allocator,
    options: quic.EmbeddedSessionOptions,
    plan: Plan,
    seat: ?*quic.EmbeddedSession = null,
    /// The server side of the connection, which outlives the seat.
    quic_conn: ?*quic_zig.Connection = null,
    peer: ?*Peer = null,
    messages: usize = 0,
    closes: usize = 0,
    errors: usize = 0,
    errors_after_close: usize = 0,
    last_error: ?anyerror = null,
    disconnects: usize = 0,

    const Plan = enum {
        /// Seat callbacks that only count.
        plain,
        /// A `Peer` on the seat, whose close callback frees it: the pattern
        /// that `Peer.on_close` documents.
        peer_freed_in_close,
        /// `on_message` takes the seat out of the host and destroys it.
        destroy_in_message,
    };

    pub const ConnState = void;
    pub const StreamState = void;

    fn onConnect(host: *TeardownHost, session: *TD.Session) anyerror!void {
        const seat = try quic.EmbeddedSession.create(host.seat_allocator, session.conn, host.options);
        host.seat = seat;
        host.quic_conn = session.conn;
        if (host.plan != .peer_freed_in_close) seat.start(host, onMessage, onError, onClose);
    }

    fn onHandshake(host: *TeardownHost, _: *TD.Session) anyerror!void {
        if (host.plan != .peer_freed_in_close) return;
        const seat = host.seat orelse return;
        const peer = try host.allocator.create(Peer);
        peer.* = Peer.init(host.allocator, seat);
        peer.start(host, peerError, peerClose);
        host.peer = peer;
    }

    fn onStreamOpen(host: *TeardownHost, _: *TD.Session, entry: *TD.StreamEntry, bidi: bool) anyerror!void {
        const seat = host.seat orelse return;
        try seat.onStreamOpen(entry.id, bidi);
    }

    fn onStreamData(host: *TeardownHost, _: *TD.Session, entry: *TD.StreamEntry, chunk: []const u8) anyerror!void {
        const seat = host.seat orelse return;
        try seat.onStreamData(entry.id, chunk);
    }

    fn onStreamEnd(host: *TeardownHost, _: *TD.Session, entry: *TD.StreamEntry, end: quic.quic_app.StreamEnd) anyerror!void {
        const seat = host.seat orelse return;
        seat.onStreamEnd(entry.id, end);
    }

    fn onDisconnect(host: *TeardownHost, _: *TD.Session) void {
        host.disconnects += 1;
        host.quic_conn = null;
        const seat = host.seat orelse return;
        host.seat = null;
        seat.notifyDisconnected();
        seat.destroy();
    }

    fn of(ctx: ?*anyopaque) *TeardownHost {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn noteError(host: *TeardownHost, err: anyerror) void {
        host.errors += 1;
        host.last_error = err;
        if (host.closes > 0) host.errors_after_close += 1;
    }

    fn onMessage(seat: *quic.EmbeddedSession, _: []const u8) anyerror!void {
        const host = of(seat.context());
        host.messages += 1;
        if (host.plan == .destroy_in_message) {
            host.seat = null;
            seat.destroy();
        }
    }

    fn onError(seat: *quic.EmbeddedSession, err: anyerror) void {
        of(seat.context()).noteError(err);
    }

    fn onClose(seat: *quic.EmbeddedSession) void {
        of(seat.context()).closes += 1;
    }

    fn peerError(ctx: ?*anyopaque, _: *Peer, err: anyerror) void {
        of(ctx).noteError(err);
    }

    fn peerClose(ctx: ?*anyopaque, peer: *Peer) void {
        const host = of(ctx);
        host.closes += 1;
        host.peer = null;
        peer.deinit();
        host.allocator.destroy(peer);
    }
};

const TD = quic.quic_app.Driver(TeardownHost);

const teardown_data_deadline_us: u64 = 300_000;

const teardown_native_options: quic.NativeOptions = .{
    .inline_frame_threshold = 256,
    .max_control_frame_bytes = 512,
    .max_pending_data_streams = 4,
    .max_pending_data_bytes = 4096,
    .data_stream_completion_deadline_us = teardown_data_deadline_us,
};

const TeardownRigOptions = struct {
    plan: TeardownHost.Plan = .plain,
    max_buffered_stream_bytes: usize = 512 * 1024,
    /// quic-level `reveal_close_reason_on_wire` on the host's listener.
    reveal_close_reason_on_wire: bool = false,
    /// The raw client's `max_ack_delay` transport parameter. The server's
    /// draining period is three of its PTOs, which include this delay.
    client_max_ack_delay_ms: ?u64 = null,
};

/// One host (Driver + listener + seat) and one raw quic client, on the
/// heap: the Driver, the listener and the host point at each other.
const TeardownRig = struct {
    counting: CountingAllocator,
    host: TeardownHost,
    driver: TD,
    listener: quic.Listener,
    raw: RawFaultClient,
    rx_buf: [64 * 1024]u8,
    tx_buf: [2048]u8,

    fn create(allocator: std.mem.Allocator, options: TeardownRigOptions) !*TeardownRig {
        const rig = try allocator.create(TeardownRig);
        errdefer allocator.destroy(rig);
        rig.counting = .{ .parent = allocator };
        rig.host = .{
            .allocator = allocator,
            .seat_allocator = rig.counting.allocator(),
            .options = .{
                .mode = .native,
                .native = teardown_native_options,
                .max_buffered_stream_bytes = options.max_buffered_stream_bytes,
            },
            .plan = options.plan,
        };
        rig.driver = try TD.init(.{
            .allocator = allocator,
            .app = &rig.host,
            .max_tracked_streams = 16,
            .hooks = .{
                .on_connect = TeardownHost.onConnect,
                .on_handshake = TeardownHost.onHandshake,
                .on_stream_open = TeardownHost.onStreamOpen,
                .on_stream_data = TeardownHost.onStreamData,
                .on_stream_end = TeardownHost.onStreamEnd,
                .on_disconnect = TeardownHost.onDisconnect,
            },
        });
        errdefer rig.driver.deinit();
        rig.listener = try quic.Listener.init(allocator, std.testing.io, .{
            .listen_addr = loopback.testListenAddr(),
            .tls_cert_pem = loopback_cert_pem,
            .tls_key_pem = loopback_key_pem,
            .alpn_protocols = &.{"capnp-rpc/1"},
            .max_concurrent_connections = 4,
            .receive_timeout = std.Io.Duration.fromMilliseconds(1),
            .mode = .native,
            .native = teardown_native_options,
            .reveal_close_reason_on_wire = options.reveal_close_reason_on_wire,
        });
        // The Driver must outlive the server: `listener.deinit` fires the
        // Driver's will-close hook.
        errdefer rig.listener.deinit();
        rig.driver.attach(&rig.listener.server);
        rig.raw = if (options.client_max_ack_delay_ms) |delay_ms|
            try rawClientWithAckDelay(allocator, rig.listener.getAddress(), delay_ms)
        else
            try RawFaultClient.init(allocator, std.testing.io, rig.listener.getAddress());
        return rig;
    }

    fn destroy(self: *TeardownRig) void {
        const allocator = self.host.allocator;
        self.raw.deinit();
        self.listener.deinit();
        self.driver.deinit();
        if (self.host.peer) |peer| {
            peer.deinit();
            allocator.destroy(peer);
        }
        allocator.destroy(self);
    }

    /// One embedder pass in the documented order: feed, Driver, seat,
    /// flush, tick, reap.
    fn hostStep(self: *TeardownRig) !void {
        _ = try self.listener.receiveOne(&self.rx_buf);
        try self.driver.service(&self.listener.server);
        const now_us = self.listener.nowUs();
        if (self.host.seat) |seat| seat.service(now_us) catch seat.requestClose();
        try self.flush(now_us);
        try self.listener.tick(now_us);
        try self.flush(now_us);
        _ = self.listener.reapClosedSessions();
    }

    fn flush(self: *TeardownRig, now_us: u64) !void {
        for (self.listener.server.iterator()) |slot| {
            try self.listener.drainSessionDatagrams(quic.Session.fromSlot(slot), &self.tx_buf, now_us);
        }
    }

    fn step(self: *TeardownRig) !void {
        try self.hostStep();
        try self.raw.step(std.Io.Duration.zero);
    }

    fn handshake(self: *TeardownRig) !*quic.EmbeddedSession {
        const patience = TeardownPatience.begin();
        while (true) {
            try self.raw.step(std.Io.Duration.zero);
            try self.hostStep();
            if (self.raw.client.conn.handshakeDone()) {
                if (self.host.seat) |seat| {
                    if (seat.conn.handshakeDone()) return seat;
                }
            }
            try patience.wait();
        }
    }

    /// The native preface and hello on stream 0.
    fn writePreamble(self: *TeardownRig) !void {
        var hello: [quic.native.encodedHelloLen()]u8 = undefined;
        const hello_len = try quic.native.encodeHello(&hello);
        try self.raw.ensureControlStream();
        try self.raw.writeAll(quic.baseline_stream_id, quic.native.preface);
        try self.raw.writeAll(quic.baseline_stream_id, hello[0..hello_len]);
    }

    /// One inline RPC frame (a Bootstrap) on stream 0.
    fn writeInlineFrame(self: *TeardownRig, allocator: std.mem.Allocator) !void {
        const frame = try loopback.buildBootstrapFrame(allocator, 0);
        defer allocator.free(frame);
        const inline_rpc = try quic.native.encodeInlineRpc(allocator, 0, frame, teardown_native_options.max_control_frame_bytes);
        defer allocator.free(inline_rpc);
        try self.raw.writeAll(quic.baseline_stream_id, inline_rpc);
    }

    /// Step both sides until the raw client has the server's close.
    fn waitForClientClose(self: *TeardownRig) !quic_zig.CloseEvent {
        const patience = TeardownPatience.begin();
        while (true) {
            if (self.raw.client.conn.closeEvent()) |ev| return ev;
            try self.step();
            try patience.wait();
        }
    }
};

fn rawClientWithAckDelay(
    allocator: std.mem.Allocator,
    remote_addr: std.Io.net.IpAddress,
    max_ack_delay_ms: u64,
) !RawFaultClient {
    const io = std.testing.io;
    const local_addr = quic.defaultClientBindAddress(remote_addr);
    const socket = try std.Io.net.IpAddress.bind(&local_addr, io, .{ .mode = .dgram, .protocol = .udp });
    errdefer socket.close(io);
    var params = quic.defaultTransportParams();
    params.max_ack_delay_ms = max_ack_delay_ms;
    var client = try quic_zig.Client.connect(.{
        .allocator = allocator,
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .alpn_protocols = &.{quic.alpn},
        .transport_params = params,
    });
    errdefer client.deinit();
    const rx_buf = try allocator.alloc(u8, 64 * 1024);
    errdefer allocator.free(rx_buf);
    const tx_buf = try allocator.alloc(u8, 1500);
    errdefer allocator.free(tx_buf);
    return .{
        .allocator = allocator,
        .io = io,
        .socket = socket,
        .remote_addr = remote_addr,
        .client = client,
        .start_timestamp = std.Io.Timestamp.now(io, .awake),
        .rx_buf = rx_buf,
        .tx_buf = tx_buf,
    };
}

const TeardownPatience = struct {
    start: std.Io.Timestamp,

    fn begin() TeardownPatience {
        return .{ .start = std.Io.Timestamp.now(std.testing.io, .awake) };
    }

    /// Fail once the loop has waited `loopback_timeout_ms`; else pause 1 ms.
    fn wait(self: *const TeardownPatience) !void {
        const now = std.Io.Timestamp.now(std.testing.io, .awake);
        if (self.start.durationTo(now).toMilliseconds() >= loopback.loopback_timeout_ms) {
            return error.QuicLoopbackTimedOut;
        }
        loopback.sleepMs(1);
    }
};

/// A native seat waits on a data stream that a DataRpc announced and the
/// client never opens. The client then closes. The seat runs its close
/// callback when the connection starts draining, and the draining period
/// (three PTOs with the client's 2 s `max_ack_delay`) outlasts the data
/// stream's completion deadline. The seat must not report the deadline's
/// `DataStreamTimeout` after its close callback: the host may have freed the
/// `Peer` there. Ablation: with the seat servicing its engines and keeping
/// its callbacks after the close callback (the code before this fix), the
/// plain plan gets the error after the close, and the Peer plan crashes in
/// `peer_transport_callbacks.zig` on the freed Peer.
fn runSeatQuietAfterClose(plan: TeardownHost.Plan) !void {
    const allocator = std.testing.allocator;
    const rig = try TeardownRig.create(allocator, .{ .plan = plan, .client_max_ack_delay_ms = 2000 });
    defer rig.destroy();

    const seat = try rig.handshake();
    try rig.writePreamble();
    const announce = try quic.native.encodeDataRpc(allocator, 0, 2, 8, teardown_native_options.max_control_frame_bytes);
    defer allocator.free(announce);
    try rig.raw.writeAll(quic.baseline_stream_id, announce);

    // 1. Step until the seat waits on the data stream: its deadline runs.
    var patience = TeardownPatience.begin();
    while (seat.native.pending_data == null) {
        try rig.step();
        try patience.wait();
    }
    const announced_at_us = rig.listener.nowUs();

    // 2. The client closes. Step the host until the seat's close callback.
    rig.raw.client.conn.close(false, 0, "bye");
    try rig.raw.drainOutgoing(rig.raw.nowUs());
    patience = TeardownPatience.begin();
    while (rig.host.closes == 0) {
        try rig.hostStep();
        try patience.wait();
    }
    try std.testing.expectEqual(@as(usize, 0), rig.host.errors);

    // 3. Step the host past the deadline. The connection is still draining:
    //    the Driver has not reaped it.
    patience = TeardownPatience.begin();
    while (rig.listener.nowUs() -| announced_at_us < teardown_data_deadline_us + 200_000) {
        try rig.hostStep();
        try patience.wait();
    }
    try std.testing.expectEqual(@as(usize, 0), rig.host.disconnects);
    try std.testing.expect(rig.host.seat != null);

    try std.testing.expectEqual(@as(usize, 1), rig.host.closes);
    try std.testing.expectEqual(@as(usize, 0), rig.host.errors_after_close);
    try std.testing.expectEqual(@as(usize, 0), rig.host.errors);
}

test "embedded seat calls no callback after its close callback while the connection drains" {
    try runSeatQuietAfterClose(.plain);
}

test "embedded seat never reaches a Peer that its close callback freed" {
    try runSeatQuietAfterClose(.peer_freed_in_close);
}

// `on_message` takes the seat out of the host and destroys it: a destroy
// from inside a seat callback. The seat must be freed before the service
// pass that ran the callback returns, run its close callback once before
// that, and close the QUIC connection with a normal close. Ablation: with
// the deferred teardown completing only in `notifyDisconnected` (the code
// before this fix), the seat stays allocated after the pass, and the
// testing allocator reports it as leaked.
test "embedded seat destroyed from its message callback is freed before the service pass returns" {
    const allocator = std.testing.allocator;
    const rig = try TeardownRig.create(allocator, .{ .plan = .destroy_in_message });
    defer rig.destroy();

    _ = try rig.handshake();
    try std.testing.expect(rig.counting.live > 0);
    try rig.writePreamble();
    try rig.writeInlineFrame(allocator);

    const patience = TeardownPatience.begin();
    while (rig.host.messages == 0) {
        try rig.step();
        try patience.wait();
    }
    // The pass that ran `on_message` freed the seat, after its close callback.
    try std.testing.expect(rig.host.seat == null);
    try std.testing.expectEqual(@as(usize, 0), rig.counting.live);
    try std.testing.expectEqual(@as(usize, 1), rig.host.closes);
    try std.testing.expectEqual(@as(usize, 0), rig.host.errors);

    // The client sees a normal application close.
    const ev = try rig.waitForClientClose();
    try std.testing.expectEqual(quic_zig.CloseSource.peer, ev.source);
    try std.testing.expectEqual(quic_zig.CloseErrorSpace.application, ev.error_space);
    try std.testing.expectEqual(@as(u64, 0), ev.error_code);
}

const RequestCloseCase = enum {
    /// The next service pass carries the request out.
    service_pass,
    /// The host destroys the seat before any service pass.
    destroy_before_service,
};

/// `requestClose` closes the QUIC connection: the client gets a normal
/// application CONNECTION_CLOSE well before the 30 s idle timeout, and when
/// the seat lives on, its close callback follows. A `destroy` before the
/// next service pass does not lose the request. Ablation: with
/// `requestClose` closing only the engines (the code before this fix), the
/// client sees no close and each case times out.
fn runRequestClose(case: RequestCloseCase) !void {
    const allocator = std.testing.allocator;
    const rig = try TeardownRig.create(allocator, .{});
    defer rig.destroy();

    const seat = try rig.handshake();
    try rig.writePreamble();
    try rig.writeInlineFrame(allocator);
    var patience = TeardownPatience.begin();
    while (rig.host.messages == 0) {
        try rig.step();
        try patience.wait();
    }

    seat.requestClose();
    switch (case) {
        .service_pass => {},
        .destroy_before_service => {
            rig.host.seat = null;
            seat.destroy();
            try std.testing.expectEqual(@as(usize, 0), rig.counting.live);
        },
    }

    const ev = try rig.waitForClientClose();
    try std.testing.expectEqual(quic_zig.CloseSource.peer, ev.source);
    try std.testing.expectEqual(quic_zig.CloseErrorSpace.application, ev.error_space);
    try std.testing.expectEqual(@as(u64, 0), ev.error_code);

    switch (case) {
        .service_pass => {
            patience = TeardownPatience.begin();
            while (rig.host.closes == 0) {
                try rig.hostStep();
                try patience.wait();
            }
            try std.testing.expectEqual(@as(usize, 1), rig.host.closes);
        },
        .destroy_before_service => try std.testing.expectEqual(@as(usize, 0), rig.host.closes),
    }
    try std.testing.expectEqual(@as(usize, 0), rig.host.errors);
}

test "embedded seat requestClose sends a QUIC close the peer sees" {
    try runRequestClose(.service_pass);
}

test "embedded seat destroyed right after requestClose still sends the QUIC close" {
    try runRequestClose(.destroy_before_service);
}

// A frame error closes the connection with a reason that lives in the
// seat ("rpc frame error"). The host destroys the seat before it sends the
// close datagram, with quic-level `reveal_close_reason_on_wire` on, so
// quic-zig builds the CONNECTION_CLOSE after the seat is gone. The peer
// must get the reason, not freed memory. Ablation: with the queued close
// keeping the seat's slice (the code before this fix), the peer gets the
// freed seat's bytes and the test fails.
test "embedded seat destroyed before its close datagram leaves keeps the close reason" {
    const allocator = std.testing.allocator;
    const rig = try TeardownRig.create(allocator, .{
        .max_buffered_stream_bytes = 8,
        .reveal_close_reason_on_wire = true,
    });
    defer rig.destroy();

    const seat = try rig.handshake();
    // The preface alone overflows the 8 buffered bytes: a frame error.
    try rig.writePreamble();

    // Feed the server and run its Driver, but send nothing: the close stays
    // queued.
    const patience = TeardownPatience.begin();
    while (seat.closeStatus() == null) {
        try rig.raw.step(std.Io.Duration.zero);
        _ = try rig.listener.receiveOne(&rig.rx_buf);
        try rig.driver.service(&rig.listener.server);
        try patience.wait();
    }
    const status = seat.closeStatus() orelse return error.TestExpectedCloseStatus;
    try std.testing.expectEqual(quic.ApplicationCloseCode.frame_error, status.code);
    try std.testing.expectEqual(@as(usize, 1), rig.host.errors);
    const quic_conn = rig.host.quic_conn orelse return error.TestExpectedQuicConnection;
    // The close is queued and has not left yet.
    try std.testing.expect(!quic_conn.isClosed());
    try std.testing.expectEqual(quic_zig.CloseState.closing, quic_conn.closeState());

    rig.host.seat = null;
    seat.destroy();
    try std.testing.expectEqual(@as(usize, 0), rig.counting.live);

    const ev = try rig.waitForClientClose();
    try std.testing.expectEqual(quic_zig.CloseSource.peer, ev.source);
    try std.testing.expectEqual(@backingInt(quic.ApplicationCloseCode.frame_error), ev.error_code);
    try std.testing.expectEqualStrings("rpc frame error", ev.reason);
}
