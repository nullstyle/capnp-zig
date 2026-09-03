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
            echoEmbeddedMessage,
            recordEmbeddedError,
            recordEmbeddedClose,
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
fn runHost(host: *HostApp, listener: *quic.Listener, driver: *D) void {
    var rx_buf: [4096]u8 = undefined;
    var tx_buf: [4096]u8 = undefined;
    while (!host.stop.load(.acquire)) {
        _ = listener.receiveOne(&rx_buf) catch break;
        driver.service(&listener.server) catch break;
        const now_us = listener.nowUs();
        for (host.seats.items) |seat| {
            seat.service(now_us) catch |err| {
                host.state.last_error = err;
                _ = host.state.errors.fetchAdd(1, .acq_rel);
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
            peer.disableThreadAffinity();
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
    defer listener.deinit();

    var host_thread = try std.Thread.spawn(.{}, runPeerHost, .{ &app, &listener, &driver });
    var started = false;
    defer if (!started) {
        app.stop.store(true, .release);
        host_thread.join();
    };

    const server_addr = listener.getAddress();

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

    client.requestClose();
    for (app.seats.items) |seat| seat.requestClose();
    client_thread.join();
    joined = true;
    app.stop.store(true, .release);
    host_thread.join();
    started = true;

    if (!completed) return error.EmbeddedPeerRoundTripTimedOut;
    try std.testing.expect(client_state.bootstrap_returned.load(.acquire));
    try std.testing.expect(!client_state.failed.load(.acquire));
    try std.testing.expect(!server_state.failed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), server_state.calls.load(.acquire));

    for (app.peers.items) |peer| {
        peer.deinit();
        allocator.destroy(peer);
    }
    app.peers.clearRetainingCapacity();
}
