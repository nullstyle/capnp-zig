//! Runners for the stream-end trap (quic-zig finding of 2026-10-05).
//!
//! quic-zig's `Connection.tick` frees a stream once its receive half has
//! ended. When the receiver has read every byte of a stream and the FIN or
//! RESET then arrives alone, a tick before the next read removes the stream:
//! the read gets `StreamNotFound`, and a clean end looks like a cut one. A
//! native RPC data frame announces its length on the control stream, so the
//! transport can still finish the frame from the bytes it has.
//!
//! Each runner steps both peers on the test thread. It sends a data frame's
//! bytes, waits until the receiver has every byte, then sends the end alone
//! in a later datagram. The receiver runs in the safe order (service, then
//! tick) or in the trap order (tick, then service). The owned loops get the
//! trap order from the test-only knob
//! `quic.testing.knobs.setTickBeforeService`; the embedded host loop here
//! takes the order as an argument.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const quic_zig = @import("quic");
const loopback = @import("loopback_test_support.zig");
const raw_faults = @import("raw_fault_client.zig");

const quic = capnpc.rpc.transport.quic;
const RawFaultClient = raw_faults.RawFaultClient;

pub const End = enum { fin, reset };

pub const Order = enum {
    /// Service every stream, then tick: the order of capnp-zig's own loops.
    service_then_tick,
    /// Tick between the receive and the service pass: the trap.
    tick_then_service,
};

const payload_len: usize = 64;
const payload: [payload_len]u8 = @splat(0xab);
const reset_code: u64 = 77;
/// A passing run never comes near it on loopback. A failing run (the trap
/// order without the fix) shows its `DataStreamTimeout` after this long.
const completion_deadline_us: u64 = 300_000;

/// Wall-clock patience for one wait loop: `loopback_timeout_ms` (3 s), ten
/// times the completion deadline, so a stalled frame shows its
/// `DataStreamTimeout` long before the loop gives up.
const Patience = struct {
    start: std.Io.Timestamp,

    fn begin() Patience {
        return .{ .start = std.Io.Timestamp.now(std.testing.io, .awake) };
    }

    /// Fail once the loop has waited too long; else pause 1 ms.
    fn wait(self: *const Patience) !void {
        const now = std.Io.Timestamp.now(std.testing.io, .awake);
        const elapsed_ms = self.start.durationTo(now).toMilliseconds();
        if (elapsed_ms >= loopback.loopback_timeout_ms) return error.QuicLoopbackTimedOut;
        loopback.sleepMs(1);
    }
};

const native_options: quic.NativeOptions = .{
    .inline_frame_threshold = 16,
    .max_control_frame_bytes = 128,
    .max_pending_data_streams = 4,
    .max_pending_data_bytes = 4096,
    .data_stream_completion_deadline_us = completion_deadline_us,
};

/// What the receiver's callbacks saw. Every runner steps on the test thread,
/// so plain fields are enough.
const Recorder = struct {
    messages: usize = 0,
    errors: usize = 0,
    last_error: ?anyerror = null,
    received: [payload_len]u8 = undefined,
    received_len: usize = 0,

    fn recordMessage(self: *Recorder, frame: []const u8) void {
        self.messages += 1;
        self.received_len = @min(frame.len, self.received.len);
        @memcpy(self.received[0..self.received_len], frame[0..self.received_len]);
    }

    fn recordError(self: *Recorder, err: anyerror) void {
        self.errors += 1;
        self.last_error = err;
    }

    fn done(self: *const Recorder) bool {
        return self.messages != 0 or self.errors != 0;
    }

    /// The one data frame arrived whole, and nothing failed.
    fn expectWholeFrame(self: *const Recorder) !void {
        try std.testing.expectEqual(@as(?anyerror, null), self.last_error);
        try std.testing.expectEqual(@as(usize, 0), self.errors);
        try std.testing.expectEqual(@as(usize, 1), self.messages);
        try std.testing.expectEqualSlices(u8, &payload, self.received[0..self.received_len]);
    }
};

fn writePreamble(writer: anytype) !void {
    var hello: [quic.native.encodedHelloLen()]u8 = undefined;
    const hello_len = try quic.native.encodeHello(&hello);
    try writer.writeAll(quic.baseline_stream_id, quic.native.preface);
    try writer.writeAll(quic.baseline_stream_id, hello[0..hello_len]);
}

fn writeAnnouncement(allocator: std.mem.Allocator, writer: anytype, stream_id: u64) !void {
    const announce = try quic.native.encodeDataRpc(allocator, 0, stream_id, payload_len, native_options.max_control_frame_bytes);
    defer allocator.free(announce);
    try writer.writeAll(quic.baseline_stream_id, announce);
}

fn sendEnd(conn: *quic_zig.Connection, stream_id: u64, end: End) !void {
    switch (end) {
        .fin => try conn.streamFinish(stream_id),
        .reset => try conn.streamReset(stream_id, reset_code),
    }
}

// ---------------------------------------------------------------------------
// Server direction: a raw quic client sends; the fanout `quic.Server` reads.
// ---------------------------------------------------------------------------

const ServerHooks = struct {
    fn onAccepted(ctx: ?*anyopaque, _: *quic.Server, session: *quic.ServerSession) anyerror!void {
        session.start(ctx.?, onMessage, onError, onClose);
    }

    fn onMessage(session: *quic.ServerSession, frame: []const u8) anyerror!void {
        recorderOf(session).recordMessage(frame);
    }

    fn onError(session: *quic.ServerSession, err: anyerror) void {
        recorderOf(session).recordError(err);
    }

    fn onClose(_: *quic.ServerSession) void {}

    fn recorderOf(session: *quic.ServerSession) *Recorder {
        return @ptrCast(@alignCast(session.context().?));
    }
};

/// The raw client's half of the native wire: the preamble and announcement
/// on stream 0, every payload byte on its unidirectional stream 2.
pub fn runServerDirection(end: End, order: Order) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    quic.testing.knobs.setTickBeforeService(order == .tick_then_service);
    defer quic.testing.knobs.setTickBeforeService(false);

    var server = try quic.Server.init(allocator, io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer server.deinit();
    var recorder = Recorder{};
    server.setOnSessionAccepted(&recorder, ServerHooks.onAccepted);

    var raw = try RawFaultClient.init(allocator, io, server.getAddress());
    defer raw.deinit();

    // 1. The handshake, on both sides.
    var patience = Patience.begin();
    while (true) {
        try raw.step(std.Io.Duration.zero);
        _ = try server.stepOnce(.poll);
        if (raw.client.conn.handshakeDone() and server.sessionCount() == 1) {
            if (server.sessionAt(0).?.activeQuicConnection()) |server_conn| {
                if (server_conn.handshakeDone()) break;
            }
        }
        try patience.wait();
    }
    const session = server.sessionAt(0).?;
    const data_stream: u64 = 2;

    // 2. The frame without its end.
    try raw.ensureControlStream();
    try writePreamble(&raw);
    try writeAnnouncement(allocator, &raw, data_stream);
    try raw.ensureUniStream(data_stream);
    try raw.writeAll(data_stream, &payload);

    // 3. Step until the server has read every byte. The frame then waits
    //    for its end only.
    patience = Patience.begin();
    while (true) {
        _ = try server.stepOnce(.poll);
        if (session.native.pending_data) |pending| {
            if (pending.offset == payload_len) break;
        }
        if (recorder.done()) break;
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    try std.testing.expectEqual(@as(usize, 0), recorder.messages);
    try std.testing.expectEqual(@as(usize, 0), recorder.errors);
    // Send the client's acknowledgements now, so that the end goes alone.
    for (0..4) |_| try raw.step(std.Io.Duration.zero);

    // 4. The end, alone in a later datagram.
    try sendEnd(raw.client.conn, data_stream, end);
    try raw.drainOutgoing(raw.nowUs());

    // 5. The frame completes from the bytes in hand, in either order.
    patience = Patience.begin();
    while (!recorder.done()) {
        _ = try server.stepOnce(.poll);
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    try recorder.expectWholeFrame();
    // The end arrived: quic-zig freed the stream.
    try std.testing.expect(session.activeQuicConnection().?.streamRecvWasReaped(data_stream));
}

// ---------------------------------------------------------------------------
// Client direction: a raw quic server sends; the owned client `Connection`
// reads.
// ---------------------------------------------------------------------------

/// A bare quic-zig server on capnp-zig's `Listener` (its receive works on
/// every target), stepped by hand.
const RawServerPeer = struct {
    listener: quic.Listener,
    rx_buf: [64 * 1024]u8 = undefined,
    tx_buf: [2048]u8 = undefined,

    fn conn(self: *RawServerPeer) ?*quic_zig.Connection {
        const slots = self.listener.server.iterator();
        if (slots.len == 0) return null;
        return slots[0].conn;
    }

    fn step(self: *RawServerPeer) !void {
        _ = try self.listener.receiveOne(&self.rx_buf);
        try self.flush();
        try self.listener.tick(self.listener.nowUs());
        try self.flush();
    }

    fn flush(self: *RawServerPeer) !void {
        const now_us = self.listener.nowUs();
        for (self.listener.server.iterator()) |slot| {
            try slot.conn.advance();
            try self.listener.drainSessionDatagrams(quic.Session.fromSlot(slot), &self.tx_buf, now_us);
        }
    }

    pub fn writeAll(self: *RawServerPeer, stream_id: u64, bytes: []const u8) !void {
        const c = self.conn() orelse return error.QuicLoopbackMissingSession;
        var offset: usize = 0;
        const patience = Patience.begin();
        while (offset < bytes.len) {
            const written = try c.streamWrite(stream_id, bytes[offset..]);
            if (written == 0) {
                try patience.wait();
                try self.step();
                continue;
            }
            offset += written;
        }
        try self.flush();
    }
};

const ClientHooks = struct {
    fn onMessage(conn: *quic.Connection, frame: []const u8) anyerror!void {
        recorderOf(conn).recordMessage(frame);
    }

    fn onError(conn: *quic.Connection, err: anyerror) void {
        recorderOf(conn).recordError(err);
    }

    fn onClose(_: *quic.Connection) void {}

    fn recorderOf(conn: *quic.Connection) *Recorder {
        return @ptrCast(@alignCast(conn.context().?));
    }
};

/// The raw server's half of the native wire: the preamble and announcement
/// on the client's stream 0, every payload byte on its unidirectional
/// stream 3.
pub fn runClientDirection(end: End, order: Order) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    quic.testing.knobs.setTickBeforeService(order == .tick_then_service);
    defer quic.testing.knobs.setTickBeforeService(false);

    var peer = RawServerPeer{ .listener = try quic.Listener.init(allocator, io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    }) };
    defer peer.listener.deinit();

    var client = try quic.Connection.initClient(allocator, io, .{
        .remote_addr = peer.listener.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer client.deinit();
    var recorder = Recorder{};
    client.start(&recorder, ClientHooks.onMessage, ClientHooks.onError, ClientHooks.onClose);
    var closed = false;
    defer if (!closed) {
        client.requestClose();
        client.run();
    };

    // 1. The handshake. The client's stream 0 (its preamble) reaches the
    //    peer, so the peer can answer on it.
    var patience = Patience.begin();
    while (true) {
        _ = try client.stepOnce(.poll);
        try peer.step();
        if (peer.conn()) |server_conn| {
            if (server_conn.handshakeDone() and server_conn.stream(quic.baseline_stream_id) != null) break;
        }
        try patience.wait();
    }
    const server_conn = peer.conn().?;
    const data_stream: u64 = 3;

    // 2. The frame without its end.
    try writePreamble(&peer);
    try writeAnnouncement(allocator, &peer, data_stream);
    _ = try server_conn.openUni(data_stream);
    try peer.writeAll(data_stream, &payload);

    // 3. Step until the client has read every byte.
    patience = Patience.begin();
    while (true) {
        _ = try client.stepOnce(.poll);
        if (client.native.pending_data) |pending| {
            if (pending.offset == payload_len) break;
        }
        if (recorder.done()) break;
        try peer.step();
        try patience.wait();
    }
    try std.testing.expectEqual(@as(usize, 0), recorder.messages);
    try std.testing.expectEqual(@as(usize, 0), recorder.errors);
    // The client loop takes one datagram a step: take what is queued for it
    // (acknowledgements), so that the end goes alone.
    var flushes: usize = 0;
    while (flushes < 20) : (flushes += 1) {
        try peer.step();
        const result = try client.stepOnce(.poll);
        if (!result.received_datagram) break;
    }

    // 4. The end, alone in a later datagram.
    try sendEnd(server_conn, data_stream, end);
    try peer.flush();

    // 5. The frame completes from the bytes in hand, in either order.
    patience = Patience.begin();
    while (!recorder.done()) {
        _ = try client.stepOnce(.poll);
        try peer.step();
        try patience.wait();
    }
    try recorder.expectWholeFrame();
    try std.testing.expect(client.activeQuicConnection().?.streamRecvWasReaped(data_stream));

    client.requestClose();
    client.run();
    closed = true;
}

// ---------------------------------------------------------------------------
// Embedded seat: a raw quic client sends; a host loop on the test thread
// feeds one `quic.app.Driver` and one `EmbeddedSession`.
// ---------------------------------------------------------------------------

const SeatHost = struct {
    allocator: std.mem.Allocator,
    recorder: *Recorder,
    seat: ?*quic.EmbeddedSession = null,

    pub const ConnState = ?*quic.EmbeddedSession;
    pub const StreamState = void;

    fn onConnect(host: *SeatHost, session: *HostDriver.Session) anyerror!void {
        const seat = try quic.EmbeddedSession.create(host.allocator, session.conn, .{
            .mode = .native,
            .native = native_options,
        });
        seat.start(host.recorder, onMessage, onError, onClose);
        session.app = seat;
        host.seat = seat;
    }

    fn onStreamOpen(_: *SeatHost, session: *HostDriver.Session, entry: *HostDriver.StreamEntry, bidi: bool) anyerror!void {
        const seat = session.app orelse return;
        try seat.onStreamOpen(entry.id, bidi);
    }

    fn onStreamData(_: *SeatHost, session: *HostDriver.Session, entry: *HostDriver.StreamEntry, chunk: []const u8) anyerror!void {
        const seat = session.app orelse return;
        try seat.onStreamData(entry.id, chunk);
    }

    fn onStreamEnd(_: *SeatHost, session: *HostDriver.Session, entry: *HostDriver.StreamEntry, end: quic.quic_app.StreamEnd) anyerror!void {
        const seat = session.app orelse return;
        seat.onStreamEnd(entry.id, end);
    }

    fn onDisconnect(host: *SeatHost, session: *HostDriver.Session) void {
        const seat = session.app orelse return;
        session.app = null;
        host.seat = null;
        seat.notifyDisconnected();
        seat.destroy();
    }

    fn onMessage(seat: *quic.EmbeddedSession, frame: []const u8) anyerror!void {
        recorderOf(seat).recordMessage(frame);
    }

    fn onError(seat: *quic.EmbeddedSession, err: anyerror) void {
        recorderOf(seat).recordError(err);
    }

    fn onClose(_: *quic.EmbeddedSession) void {}

    fn recorderOf(seat: *quic.EmbeddedSession) *Recorder {
        return @ptrCast(@alignCast(seat.context().?));
    }
};

const HostDriver = quic.quic_app.Driver(SeatHost);

const HostLoop = struct {
    host: *SeatHost,
    listener: *quic.Listener,
    driver: *HostDriver,
    order: Order,
    rx_buf: [64 * 1024]u8 = undefined,
    tx_buf: [2048]u8 = undefined,

    /// One embedder pass: feed one datagram, then the Driver, the seat and
    /// the outbound flush, with the tick before or after them.
    fn step(self: *HostLoop) !void {
        _ = try self.listener.receiveOne(&self.rx_buf);
        const now_us = self.listener.nowUs();
        if (self.order == .tick_then_service) try self.listener.tick(now_us);
        try self.driver.service(&self.listener.server);
        // A seat error reaches the recorder through the error callback.
        if (self.host.seat) |seat| seat.service(now_us) catch {};
        for (self.listener.server.iterator()) |slot| {
            try self.listener.drainSessionDatagrams(quic.Session.fromSlot(slot), &self.tx_buf, now_us);
        }
        if (self.order == .service_then_tick) try self.listener.tick(now_us);
        for (self.listener.server.iterator()) |slot| {
            try self.listener.drainSessionDatagrams(quic.Session.fromSlot(slot), &self.tx_buf, now_us);
        }
        _ = self.listener.reapClosedSessions();
    }
};

/// The case the seat must survive: the data stream's bytes reach the seat
/// before the announcement, so the engine has read none of them when the
/// end comes. In the trap order the end comes as `.reaped`, in the safe
/// order as `.fin` or `.reset`. The seat must keep the bytes and finish the
/// frame when the announcement arrives, and then free the stream's buffer.
pub fn runEmbedded(end: End, order: Order) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var recorder = Recorder{};
    var host = SeatHost{ .allocator = allocator, .recorder = &recorder };
    var driver = try HostDriver.init(.{
        .allocator = allocator,
        .app = &host,
        .max_tracked_streams = 16,
        .hooks = .{
            .on_connect = SeatHost.onConnect,
            .on_stream_open = SeatHost.onStreamOpen,
            .on_stream_data = SeatHost.onStreamData,
            .on_stream_end = SeatHost.onStreamEnd,
            .on_disconnect = SeatHost.onDisconnect,
        },
    });
    var listener = quic.Listener.init(allocator, io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    }) catch |err| {
        driver.deinit();
        return err;
    };
    driver.attach(&listener.server);
    // The Driver must outlive the server: `listener.deinit` fires the
    // Driver's will-close hook. LIFO defers run this one last.
    defer driver.deinit();
    defer listener.deinit();

    var loop = HostLoop{ .host = &host, .listener = &listener, .driver = &driver, .order = order };
    var raw = try RawFaultClient.init(allocator, io, listener.getAddress());
    defer raw.deinit();

    // 1. The handshake, on both sides.
    var patience = Patience.begin();
    while (true) {
        try raw.step(std.Io.Duration.zero);
        try loop.step();
        if (raw.client.conn.handshakeDone() and host.seat != null) {
            if (host.seat.?.conn.handshakeDone()) break;
        }
        try patience.wait();
    }
    const seat = host.seat.?;
    const host_conn = seat.conn;
    const data_stream: u64 = 2;

    // 2. The preamble, and every payload byte with no end and no
    //    announcement yet.
    try raw.ensureControlStream();
    try writePreamble(&raw);
    try raw.ensureUniStream(data_stream);
    try raw.writeAll(data_stream, &payload);

    // 3. Step until the seat holds every byte. The engine reads none: it has
    //    no announcement.
    patience = Patience.begin();
    while (true) {
        try loop.step();
        if (seat.streams.get(data_stream)) |buf| {
            if (buf.total == payload_len) break;
        }
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    for (0..4) |_| {
        try raw.step(std.Io.Duration.zero);
        try loop.step();
    }

    // 4. The end, alone in a later datagram. Step until quic-zig has freed
    //    the stream; by then the Driver has passed the end to the seat.
    try sendEnd(raw.client.conn, data_stream, end);
    try raw.drainOutgoing(raw.nowUs());
    patience = Patience.begin();
    while (!host_conn.streamRecvWasReaped(data_stream)) {
        try loop.step();
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    try std.testing.expectEqual(@as(usize, 0), recorder.messages);
    try std.testing.expectEqual(@as(usize, 0), recorder.errors);

    // 5. The announcement. The frame completes from the bytes the seat kept.
    try writeAnnouncement(allocator, &raw, data_stream);
    patience = Patience.begin();
    while (!recorder.done()) {
        try loop.step();
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    try recorder.expectWholeFrame();

    // 6. The seat freed the data stream's buffer. Stream 0 is all it holds.
    try std.testing.expectEqual(@as(usize, 1), seat.streams.count());
    try std.testing.expect(seat.streams.get(quic.baseline_stream_id) != null);
    try std.testing.expectEqual(@as(usize, 0), seat.ended_data_streams);
}

// ---------------------------------------------------------------------------
// A cut frame: the peer resets the data stream before the server read it.
// ---------------------------------------------------------------------------

/// Every payload byte reaches the server's QUIC stream, but no announcement
/// yet, so the engine reads none of them. Then ONE datagram brings the
/// announcement and the RESET_STREAM. quic-zig drops the unread bytes at the
/// reset, so the frame can never complete. The session must fail at once
/// with `DataStreamReset` (a protocol close), not wait for the completion
/// deadline and fail with `DataStreamTimeout`.
pub fn runServerResetBeforeRead() !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var server = try quic.Server.init(allocator, io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer server.deinit();
    var recorder = Recorder{};
    server.setOnSessionAccepted(&recorder, ServerHooks.onAccepted);

    var raw = try RawFaultClient.init(allocator, io, server.getAddress());
    defer raw.deinit();

    var patience = Patience.begin();
    while (true) {
        try raw.step(std.Io.Duration.zero);
        _ = try server.stepOnce(.poll);
        if (raw.client.conn.handshakeDone() and server.sessionCount() == 1) {
            if (server.sessionAt(0).?.activeQuicConnection()) |server_conn| {
                if (server_conn.handshakeDone()) break;
            }
        }
        try patience.wait();
    }
    const session = server.sessionAt(0).?;
    const server_conn = session.activeQuicConnection().?;
    const data_stream: u64 = 2;

    // 1. The preamble, and every payload byte with no end and no
    //    announcement.
    try raw.ensureControlStream();
    try writePreamble(&raw);
    try raw.ensureUniStream(data_stream);
    try raw.writeAll(data_stream, &payload);
    patience = Patience.begin();
    while (true) {
        _ = try server.stepOnce(.poll);
        if (server_conn.stream(data_stream)) |stream| {
            if (stream.recv.readableBytes() == payload_len) break;
        }
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    for (0..4) |_| try raw.step(std.Io.Duration.zero);

    // 2. The announcement and the reset, in one datagram, so the server
    //    takes both in one step: the reset drops the bytes before any read.
    const announce = try quic.native.encodeDataRpc(allocator, 0, data_stream, payload_len, native_options.max_control_frame_bytes);
    defer allocator.free(announce);
    try std.testing.expectEqual(announce.len, try raw.client.conn.streamWrite(quic.baseline_stream_id, announce));
    try raw.client.conn.streamReset(data_stream, reset_code);
    var datagrams: usize = 0;
    while (try raw.client.conn.pollDatagram(raw.tx_buf, raw.nowUs())) |out| {
        try raw.socket.send(io, &raw.remote_addr, raw.tx_buf[0..out.len]);
        datagrams += 1;
    }
    try std.testing.expectEqual(@as(usize, 1), datagrams);

    // 3. The server fails the session in the step that takes that datagram.
    patience = Patience.begin();
    while (!recorder.done()) {
        _ = try server.stepOnce(.poll);
        try patience.wait();
    }
    try std.testing.expectEqual(@as(usize, 0), recorder.messages);
    try std.testing.expectEqual(@as(usize, 1), recorder.errors);
    try std.testing.expectEqual(@as(?anyerror, error.DataStreamReset), recorder.last_error);
    const status = session.closeStatus() orelse return error.QuicLoopbackMissingCloseStatus;
    try std.testing.expectEqual(quic.ApplicationCloseCode.protocol_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.DataStreamReset), status.err);
}
