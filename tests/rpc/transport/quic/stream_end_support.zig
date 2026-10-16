//! Runners for the stream-end trap (quic-zig finding of 2026-10-05).
//!
//! quic-zig's `Connection.tick` frees a stream once its receive half has
//! ended. When the receiver has read every byte of a stream and the FIN or
//! RESET then arrives alone, a tick before the next read removes the stream,
//! and a read gets `StreamNotFound`. Through quic-zig v0.27.0 a clean end then
//! looked like a cut one. Since v0.28.0, `Connection.streamRecvEnd` still
//! tells how the stream ended (a clean FIN, or a reset and its code), and
//! `quic.app` reports `.fin` or `.reset` for it, not `.reaped`. A native RPC
//! data frame announces its length on the control stream, so the transport
//! settles the frame at once from the bytes it has and that answer.
//!
//! Each runner steps both peers on the test thread. It sends a data frame's
//! bytes, waits until the receiver has every byte, then sends the end alone
//! in a later datagram. The receiver runs in the safe order (service, then
//! tick) or in the trap order (tick, then service). The owned loops get the
//! trap order from the test-only knob
//! `quic.testing.knobs.setTickBeforeService`; the embedded host loop here
//! takes the order as an argument.
//!
//! The last section has the runners for the end of stream 0 itself (see
//! "Wire constants (frozen)" in docs/quic-transport.md): the owned client
//! under a `Peer`, the owned fanout server and the embedded seat.

const std = @import("std");
const capnpc = @import("capnpc-zig");
const quic_zig = @import("quic");
const loopback = @import("loopback_test_support.zig");
const raw_faults = @import("raw_fault_client.zig");

const cap_table = capnpc.rpc.caps.table;
const protocol = capnpc.rpc.wire.protocol;
const quic = capnpc.rpc.transport.quic;
const DisconnectCause = capnpc.rpc.events.DisconnectCause;
const Peer = capnpc.rpc.peer.Peer;
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
/// A failing run (the trap order without the fix) shows its
/// `DataStreamTimeout` after this long.
///
/// The deadline runs from the last byte the receiver read, also while the
/// runners below step both peers on to the stream end. On Windows those
/// steps are slow: each 1 ms wait (a 1 ms receive timeout, the pause in
/// `Patience.wait`) lasts a timer tick of 15.6 ms, and a polled loop gets
/// at most one datagram every second step from the UDP receive bridge. The
/// acknowledgement flush then takes no datagram, and the end waits behind
/// the queued ones. With that timing emulated on macOS, a passing client run
/// used 150 to 250 ms between its last read and the end. The old 300 ms
/// deadline failed one windows-latest run with `DataStreamTimeout` (CI run
/// 38027925212), and every emulated run with a 150 ms stall added failed.
const completion_deadline_us: u64 = 2_000_000;

/// Wall-clock patience for one wait loop: `loopback_timeout_ms` (3 s). It is
/// longer than the completion deadline, so a stalled frame shows its
/// `DataStreamTimeout` before the loop gives up.
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

comptime {
    // The deadline starts before a wait loop's patience does, so a stalled
    // frame times out before that loop gives up.
    std.debug.assert(completion_deadline_us < loopback.loopback_timeout_ms * 1000);
}

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

    /// No frame arrived, and the session failed once, with `err`.
    fn expectFailure(self: *const Recorder, err: anyerror) !void {
        try std.testing.expectEqual(@as(usize, 0), self.messages);
        try std.testing.expectEqual(@as(usize, 1), self.errors);
        try std.testing.expectEqual(@as(?anyerror, err), self.last_error);
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

/// After quic-zig freed the data stream, it still says how the stream ended
/// (quic-zig v0.28.0, `Connection.streamRecvEnd`).
fn expectRecvEnd(conn: *quic_zig.Connection, stream_id: u64, end: End) !void {
    try std.testing.expect(conn.streamRecvWasReaped(stream_id));
    const recv_end = conn.streamRecvEnd(stream_id) orelse return error.TestExpectedStreamRecvEnd;
    switch (end) {
        .fin => try std.testing.expect(recv_end.isClean()),
        .reset => try std.testing.expectEqual(@as(?u64, reset_code), recv_end.reset_code),
    }
    try std.testing.expectEqual(@as(u64, payload_len), recv_end.final_size);
}

/// A failure that came "at once" came before the completion deadline: a
/// stalled frame shows `DataStreamTimeout` only after it.
fn expectBeforeDeadline(since: Patience) !void {
    const now = std.Io.Timestamp.now(std.testing.io, .awake);
    const elapsed_us = since.start.durationTo(now).toMicroseconds();
    try std.testing.expect(elapsed_us < completion_deadline_us);
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

fn initServer(allocator: std.mem.Allocator) !quic.Server {
    return quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
}

/// Step the raw client and the server until the handshake is done on both
/// sides. Returns the server's one session.
fn handshakeServer(raw: *RawFaultClient, server: *quic.Server) !*quic.ServerSession {
    const patience = Patience.begin();
    while (true) {
        try raw.step(std.Io.Duration.zero);
        _ = try server.stepOnce(.poll);
        if (raw.client.conn.handshakeDone() and server.sessionCount() == 1) {
            const session = server.sessionAt(0).?;
            if (session.activeQuicConnection()) |server_conn| {
                if (server_conn.handshakeDone()) return session;
            }
        }
        try patience.wait();
    }
}

/// The raw client's half of the native wire: the preamble and announcement
/// on stream 0, every payload byte on its unidirectional stream 2.
pub fn runServerDirection(end: End, order: Order) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    quic.testing.knobs.setTickBeforeService(order == .tick_then_service);
    defer quic.testing.knobs.setTickBeforeService(false);

    var server = try initServer(allocator);
    defer server.deinit();
    var recorder = Recorder{};
    server.setOnSessionAccepted(&recorder, ServerHooks.onAccepted);

    var raw = try RawFaultClient.init(allocator, io, server.getAddress());
    defer raw.deinit();

    // 1. The handshake, on both sides.
    const session = try handshakeServer(&raw, &server);
    const data_stream: u64 = 2;

    // 2. The frame without its end.
    try raw.ensureControlStream();
    try writePreamble(&raw);
    try writeAnnouncement(allocator, &raw, data_stream);
    try raw.ensureUniStream(data_stream);
    try raw.writeAll(data_stream, &payload);

    // 3. Step until the server has read every byte. The frame then waits
    //    for its end only.
    var patience = Patience.begin();
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
    // The end arrived: quic-zig freed the stream, and still knows its end.
    try expectRecvEnd(session.activeQuicConnection().?, data_stream, end);
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
    try expectRecvEnd(client.activeQuicConnection().?, data_stream, end);

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
    mode: quic.TransportMode = .native,
    seat: ?*quic.EmbeddedSession = null,
    /// The seat's close cause, kept when `onDisconnect` destroys the seat.
    last_close_cause: ?capnpc.rpc.events.DisconnectCause = null,

    pub const ConnState = ?*quic.EmbeddedSession;
    pub const StreamState = void;

    fn onConnect(host: *SeatHost, session: *HostDriver.Session) anyerror!void {
        const seat = try quic.EmbeddedSession.create(host.allocator, session.conn, .{
            .mode = host.mode,
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
        host.last_close_cause = seat.closeCause();
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

/// One `quic.app.Driver` with its seat on a `Listener`, a host loop on the
/// test thread, and a raw quic client that talks to it. It lives on the
/// heap: the Driver, the listener and the loop point at each other.
const SeatRig = struct {
    recorder: Recorder,
    host: SeatHost,
    driver: HostDriver,
    listener: quic.Listener,
    loop: HostLoop,
    raw: RawFaultClient,

    fn create(allocator: std.mem.Allocator, order: Order) !*SeatRig {
        return createWithMode(allocator, order, .native);
    }

    fn createWithMode(allocator: std.mem.Allocator, order: Order, mode: quic.TransportMode) !*SeatRig {
        const rig = try allocator.create(SeatRig);
        errdefer allocator.destroy(rig);
        rig.recorder = .{};
        rig.host = .{ .allocator = allocator, .recorder = &rig.recorder, .mode = mode };
        rig.driver = try HostDriver.init(.{
            .allocator = allocator,
            .app = &rig.host,
            .max_tracked_streams = 16,
            .hooks = .{
                .on_connect = SeatHost.onConnect,
                .on_stream_open = SeatHost.onStreamOpen,
                .on_stream_data = SeatHost.onStreamData,
                .on_stream_end = SeatHost.onStreamEnd,
                .on_disconnect = SeatHost.onDisconnect,
            },
        });
        errdefer rig.driver.deinit();
        rig.listener = try quic.Listener.init(allocator, std.testing.io, .{
            .listen_addr = loopback.testListenAddr(),
            .tls_cert_pem = loopback.loopback_cert_pem,
            .tls_key_pem = loopback.loopback_key_pem,
            .receive_timeout = std.Io.Duration.fromMilliseconds(1),
            .mode = mode,
            .native = native_options,
        });
        // The Driver must outlive the server: `listener.deinit` fires the
        // Driver's will-close hook.
        errdefer rig.listener.deinit();
        rig.driver.attach(&rig.listener.server);
        rig.loop = .{ .host = &rig.host, .listener = &rig.listener, .driver = &rig.driver, .order = order };
        rig.raw = try RawFaultClient.init(allocator, std.testing.io, rig.listener.getAddress());
        return rig;
    }

    fn destroy(self: *SeatRig) void {
        const allocator = self.host.allocator;
        self.raw.deinit();
        self.listener.deinit();
        self.driver.deinit();
        allocator.destroy(self);
    }

    /// Step both sides until the handshake is done on both. Returns the
    /// seat.
    fn handshake(self: *SeatRig) !*quic.EmbeddedSession {
        const patience = Patience.begin();
        while (true) {
            try self.raw.step(std.Io.Duration.zero);
            try self.loop.step();
            if (self.raw.client.conn.handshakeDone()) {
                if (self.host.seat) |seat| {
                    if (seat.conn.handshakeDone()) return seat;
                }
            }
            try patience.wait();
        }
    }

    fn step(self: *SeatRig) !void {
        try self.loop.step();
        try self.raw.step(std.Io.Duration.zero);
    }

    /// Step until the seat holds `len` bytes of `stream_id`. Then let the
    /// raw client send its acknowledgements, so that what the test sends
    /// next goes alone.
    fn waitForSeatBytes(self: *SeatRig, seat: *quic.EmbeddedSession, stream_id: u64, len: usize) !void {
        const patience = Patience.begin();
        while (true) {
            try self.loop.step();
            if (seat.streams.get(stream_id)) |buf| {
                if (buf.total == len) break;
            }
            try self.raw.step(std.Io.Duration.zero);
            try patience.wait();
        }
        for (0..4) |_| {
            try self.raw.step(std.Io.Duration.zero);
            try self.loop.step();
        }
    }

    /// Step until quic-zig has freed `stream_id`. By then the Driver has
    /// passed the end of the stream to the seat.
    fn waitForReap(self: *SeatRig, seat: *quic.EmbeddedSession, stream_id: u64) !void {
        const patience = Patience.begin();
        while (!seat.conn.streamRecvWasReaped(stream_id)) {
            try self.step();
            try patience.wait();
        }
    }

    fn waitForRecorder(self: *SeatRig) !void {
        const patience = Patience.begin();
        while (!self.recorder.done()) {
            try self.step();
            try patience.wait();
        }
    }
};

/// The case the seat must survive: the data stream's bytes reach the seat
/// before the announcement, so the engine has read none of them when the
/// end comes. In both orders the end comes as `.fin` or `.reset` (through
/// quic-zig v0.27.0 the trap order gave `.reaped`). In the trap order a tick
/// freed the stream first, so the seat takes the RESET's final size and
/// code from quic-zig's note of the end. The seat must keep the bytes and
/// finish the frame when the announcement arrives, and then free the
/// stream's buffer.
pub fn runEmbedded(end: End, order: Order) !void {
    const allocator = std.testing.allocator;
    const rig = try SeatRig.create(allocator, order);
    defer rig.destroy();

    // 1. The handshake, on both sides.
    const seat = try rig.handshake();
    const data_stream: u64 = 2;

    // 2. The preamble, and every payload byte with no end and no
    //    announcement yet.
    try rig.raw.ensureControlStream();
    try writePreamble(&rig.raw);
    try rig.raw.ensureUniStream(data_stream);
    try rig.raw.writeAll(data_stream, &payload);

    // 3. Step until the seat holds every byte. The engine reads none: it has
    //    no announcement.
    try rig.waitForSeatBytes(seat, data_stream, payload_len);

    // 4. The end, alone in a later datagram. Step until quic-zig has freed
    //    the stream.
    try sendEnd(rig.raw.client.conn, data_stream, end);
    try rig.raw.drainOutgoing(rig.raw.nowUs());
    try rig.waitForReap(seat, data_stream);
    try std.testing.expectEqual(@as(usize, 0), rig.recorder.messages);
    try std.testing.expectEqual(@as(usize, 0), rig.recorder.errors);
    // The seat kept the bytes, marked the end, and for a RESET kept its final
    // size and code, in both orders.
    const buf = seat.streams.get(data_stream) orelse return error.TestExpectedSeatStream;
    try std.testing.expect(buf.ended);
    try std.testing.expectEqual(@as(usize, payload_len), buf.total);
    switch (end) {
        .fin => try std.testing.expect(buf.reset == null),
        .reset => {
            const reset = buf.reset orelse return error.TestExpectedReset;
            try std.testing.expectEqual(reset_code, reset.error_code);
            try std.testing.expectEqual(@as(u64, payload_len), reset.final_size);
        },
    }

    // 5. The announcement. The frame completes from the bytes the seat kept.
    try writeAnnouncement(allocator, &rig.raw, data_stream);
    try rig.waitForRecorder();
    try rig.recorder.expectWholeFrame();

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

    var server = try initServer(allocator);
    defer server.deinit();
    var recorder = Recorder{};
    server.setOnSessionAccepted(&recorder, ServerHooks.onAccepted);

    var raw = try RawFaultClient.init(allocator, io, server.getAddress());
    defer raw.deinit();

    const session = try handshakeServer(&raw, &server);
    const server_conn = session.activeQuicConnection().?;
    const data_stream: u64 = 2;

    // 1. The preamble, and every payload byte with no end and no
    //    announcement.
    try raw.ensureControlStream();
    try writePreamble(&raw);
    try raw.ensureUniStream(data_stream);
    try raw.writeAll(data_stream, &payload);
    var patience = Patience.begin();
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
    try recorder.expectFailure(error.DataStreamReset);
    const status = session.closeStatus() orelse return error.QuicLoopbackMissingCloseStatus;
    try std.testing.expectEqual(quic.ApplicationCloseCode.protocol_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.DataStreamReset), status.err);
}

// ---------------------------------------------------------------------------
// The final size of a RESET: the seat judges it as the owned loops do.
// ---------------------------------------------------------------------------

/// The peer sends part of the data stream, queues more bytes, and resets the
/// stream before they leave. quic-zig sends only the RESET_STREAM, and its
/// final size counts the queued bytes.
pub const ResetShape = enum {
    /// Every announced byte arrives. The final size of the RESET is 10 bytes
    /// more than the announced length, so the frame fails with
    /// `InvalidFrame` (`frame_error`).
    longer_than_announced,
    /// Half the announced bytes arrive. The final size of the RESET is the
    /// announced length, so the frame can never complete. It fails with
    /// `DataStreamReset` (`protocol_error`).
    cut,

    fn sentBytes(self: ResetShape) usize {
        return switch (self) {
            .longer_than_announced => payload_len,
            .cut => payload_len / 2,
        };
    }

    /// The bytes queued before the reset, which never leave the peer.
    fn queuedBytes(self: ResetShape) usize {
        return switch (self) {
            .longer_than_announced => 10,
            .cut => payload_len - payload_len / 2,
        };
    }

    fn expectedError(self: ResetShape) anyerror {
        return switch (self) {
            .longer_than_announced => error.InvalidFrame,
            .cut => error.DataStreamReset,
        };
    }

    fn expectedCode(self: ResetShape) quic.ApplicationCloseCode {
        return switch (self) {
            .longer_than_announced => .frame_error,
            .cut => .protocol_error,
        };
    }
};

/// Queue the bytes of `shape` that never leave, then reset the stream.
fn resetWithQueuedBytes(conn: *quic_zig.Connection, stream_id: u64, shape: ResetShape) !void {
    const filler: [payload_len]u8 = @splat(0xcd);
    const queued = filler[0..shape.queuedBytes()];
    try std.testing.expectEqual(queued.len, try conn.streamWrite(stream_id, queued));
    try conn.streamReset(stream_id, reset_code);
    const reset = conn.stream(stream_id).?.send.reset orelse return error.TestExpectedReset;
    try std.testing.expectEqual(@as(u64, shape.sentBytes() + queued.len), reset.final_size);
}

/// The owned server. The announcement comes first, so the engine reads the
/// sent bytes before the RESET arrives. The RESET arrives alone. In the trap
/// order a tick frees the stream before the engine reads again, so the
/// engine must take the final size and the code of the RESET from quic-zig's
/// note of the end (`Connection.streamRecvEnd`). In both orders the frame
/// fails at once, not at the completion deadline.
pub fn runServerReset(shape: ResetShape, order: Order) !void {
    const allocator = std.testing.allocator;
    quic.testing.knobs.setTickBeforeService(order == .tick_then_service);
    defer quic.testing.knobs.setTickBeforeService(false);

    var server = try initServer(allocator);
    defer server.deinit();
    var recorder = Recorder{};
    server.setOnSessionAccepted(&recorder, ServerHooks.onAccepted);

    var raw = try RawFaultClient.init(allocator, std.testing.io, server.getAddress());
    defer raw.deinit();

    const session = try handshakeServer(&raw, &server);
    const data_stream: u64 = 2;

    // 1. The announcement, and the bytes sent before the reset.
    try raw.ensureControlStream();
    try writePreamble(&raw);
    try writeAnnouncement(allocator, &raw, data_stream);
    try raw.ensureUniStream(data_stream);
    try raw.writeAll(data_stream, payload[0..shape.sentBytes()]);
    var patience = Patience.begin();
    while (true) {
        _ = try server.stepOnce(.poll);
        if (session.native.pending_data) |pending| {
            if (pending.offset == shape.sentBytes()) break;
        }
        if (recorder.done()) break;
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    try std.testing.expectEqual(@as(usize, 0), recorder.messages);
    try std.testing.expectEqual(@as(usize, 0), recorder.errors);
    for (0..4) |_| try raw.step(std.Io.Duration.zero);

    // 2. The reset.
    try resetWithQueuedBytes(raw.client.conn, data_stream, shape);
    try raw.drainOutgoing(raw.nowUs());

    // 3. The final size of the RESET settles the frame at once, well before
    //    the completion deadline.
    patience = Patience.begin();
    while (!recorder.done()) {
        _ = try server.stepOnce(.poll);
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    try expectBeforeDeadline(patience);
    try recorder.expectFailure(shape.expectedError());
    const status = session.closeStatus() orelse return error.QuicLoopbackMissingCloseStatus;
    try std.testing.expectEqual(shape.expectedCode(), status.code);
    try std.testing.expectEqual(@as(?anyerror, shape.expectedError()), status.err);
    // quic-zig keeps the code of the RESET. In the trap order the tick freed
    // the stream before the engine read it again.
    const server_conn = session.activeQuicConnection().?;
    if (order == .tick_then_service) try std.testing.expect(server_conn.streamRecvWasReaped(data_stream));
    const recv_end = server_conn.streamRecvEnd(data_stream) orelse return error.TestExpectedStreamRecvEnd;
    try std.testing.expectEqual(@as(?u64, reset_code), recv_end.reset_code);
}

/// The seat. The sent bytes reach the seat before the announcement, so the
/// engine reads none of them before the RESET. The seat must give the error
/// that the owned server gives. In the trap order a tick frees the stream
/// before the Driver reports its end, so the seat takes the RESET's final
/// size from quic-zig's note of the end (quic-zig v0.28.0).
pub fn runEmbeddedReset(shape: ResetShape, order: Order) !void {
    const allocator = std.testing.allocator;
    const rig = try SeatRig.create(allocator, order);
    defer rig.destroy();

    const seat = try rig.handshake();
    const data_stream: u64 = 2;

    // 1. The bytes sent before the reset, with no announcement yet.
    try rig.raw.ensureControlStream();
    try writePreamble(&rig.raw);
    try rig.raw.ensureUniStream(data_stream);
    try rig.raw.writeAll(data_stream, payload[0..shape.sentBytes()]);
    try rig.waitForSeatBytes(seat, data_stream, shape.sentBytes());

    // 2. The reset. In the safe order the Driver passes `.reset` to the seat
    //    before the tick frees the stream. In the trap order the tick frees
    //    it first. Either way the seat holds the sent bytes only, and the
    //    final size of the RESET.
    try resetWithQueuedBytes(rig.raw.client.conn, data_stream, shape);
    try rig.raw.drainOutgoing(rig.raw.nowUs());
    try rig.waitForReap(seat, data_stream);
    const buf = seat.streams.get(data_stream) orelse return error.TestExpectedSeatStream;
    try std.testing.expectEqual(shape.sentBytes(), buf.total);
    const reset = buf.reset orelse return error.TestExpectedReset;
    try std.testing.expectEqual(@as(u64, shape.sentBytes() + shape.queuedBytes()), reset.final_size);
    try std.testing.expectEqual(reset_code, reset.error_code);
    try std.testing.expectEqual(@as(usize, 0), rig.recorder.messages);
    try std.testing.expectEqual(@as(usize, 0), rig.recorder.errors);

    // 3. The announcement. The final size of the RESET settles the frame.
    try writeAnnouncement(allocator, &rig.raw, data_stream);
    try rig.waitForRecorder();
    try rig.recorder.expectFailure(shape.expectedError());
    const status = seat.closeStatus() orelse return error.QuicLoopbackMissingCloseStatus;
    try std.testing.expectEqual(shape.expectedCode(), status.code);
    try std.testing.expectEqual(@as(?anyerror, shape.expectedError()), status.err);
}

// ---------------------------------------------------------------------------
// A stream this side stopped. Since quic-zig v0.28.0 the Driver reports such
// a stream as `.reaped` or, when the peer answered the stop with
// RESET_STREAM, as `.reset`, also while it is live. So `streamRecvWasReaped`
// alone does not tell it from the teardown pass, and `.reset` alone does not
// tell it from a reset that the peer started.
// ---------------------------------------------------------------------------

const stop_code: u64 = 99;

/// The seat refuses a stream that it has no use for, and the host can stop
/// a stream too. Either way quic-zig throws away what arrives after the
/// stop, so the seat must drop the stream and never treat it as a completed
/// data stream.
///
/// 1. The peer opens a bidirectional stream, which a native server never
///    uses. The seat refuses it (STOP_SENDING and RESET_STREAM), keeps no
///    buffer for it, and the session stays up.
/// 2. Half the payload of data stream 2 reaches the seat. Then the host
///    stops the stream, and the rest of the payload and the FIN arrive:
///    quic-zig drops them. The seat must drop its buffer of stream 2. In the
///    trap order the end reaches the seat after a tick freed the stream.
/// 3. An announcement for stream 2 with the length of the bytes that reached
///    the seat. A seat that kept those bytes would complete the frame.
///    quic-zig recorded the full payload as the final size, so the frame
///    fails with `InvalidFrame`.
pub fn runEmbeddedStopped(order: Order) !void {
    const allocator = std.testing.allocator;
    const rig = try SeatRig.create(allocator, order);
    defer rig.destroy();

    const seat = try rig.handshake();
    try rig.raw.ensureControlStream();
    try writePreamble(&rig.raw);
    const client_conn = rig.raw.client.conn;

    // 1. A refused stream.
    const refused_stream: u64 = 4;
    _ = try client_conn.openBidi(refused_stream);
    try rig.raw.writeAll(refused_stream, "refused");
    try client_conn.streamFinish(refused_stream);
    try rig.raw.drainOutgoing(rig.raw.nowUs());
    try rig.waitForReap(seat, refused_stream);
    try std.testing.expect(seat.streams.get(refused_stream) == null);
    try std.testing.expect(!seat.isClosing());

    // 2. A data stream that the host stops after half its bytes.
    const data_stream: u64 = 2;
    const half = payload_len / 2;
    try rig.raw.ensureUniStream(data_stream);
    try rig.raw.writeAll(data_stream, payload[0..half]);
    try rig.waitForSeatBytes(seat, data_stream, half);
    try seat.conn.streamStopSending(data_stream, stop_code);
    // The rest and the FIN leave the peer before it learns of the stop.
    try std.testing.expectEqual(payload_len - half, try client_conn.streamWrite(data_stream, payload[half..]));
    try client_conn.streamFinish(data_stream);
    try rig.raw.drainOutgoing(rig.raw.nowUs());
    try rig.waitForReap(seat, data_stream);
    const recv_end = seat.conn.streamRecvEnd(data_stream) orelse return error.TestExpectedStreamRecvEnd;
    try std.testing.expect(recv_end.stopped);
    try std.testing.expectEqual(@as(u64, payload_len), recv_end.final_size);
    // The seat dropped the stream: stream 0 is all it holds.
    try std.testing.expect(seat.streams.get(data_stream) == null);
    try std.testing.expectEqual(@as(usize, 1), seat.streams.count());
    try std.testing.expectEqual(@as(usize, 0), seat.ended_data_streams);
    try std.testing.expectEqual(@as(usize, 0), rig.recorder.messages);
    try std.testing.expectEqual(@as(usize, 0), rig.recorder.errors);

    // 3. The announcement names the bytes that reached the seat.
    const announce = try quic.native.encodeDataRpc(allocator, 0, data_stream, half, native_options.max_control_frame_bytes);
    defer allocator.free(announce);
    try rig.raw.writeAll(quic.baseline_stream_id, announce);
    try rig.waitForRecorder();
    try rig.recorder.expectFailure(error.InvalidFrame);
    const status = seat.closeStatus() orelse return error.QuicLoopbackMissingCloseStatus;
    try std.testing.expectEqual(quic.ApplicationCloseCode.frame_error, status.code);
}

// ---------------------------------------------------------------------------
// A RESET of the control stream (stream 0).
// ---------------------------------------------------------------------------

/// A RESET of stream 0 is session loss (the E-order contract). Here it
/// arrives when the server's own half of stream 0 has ended too (the peer
/// stopped it first), so in the trap order a tick frees stream 0 before the
/// Driver reports the end. Through quic-zig v0.27.0 the seat then got
/// `.reaped`, and the session stayed up with no control stream. Since
/// v0.28.0 the seat gets `.reset` and closes the session, in both orders.
pub fn runEmbeddedControlReset(order: Order) !void {
    const allocator = std.testing.allocator;
    const rig = try SeatRig.create(allocator, order);
    defer rig.destroy();

    const seat = try rig.handshake();
    const control: u64 = quic.baseline_stream_id;
    try rig.raw.ensureControlStream();
    try writePreamble(&rig.raw);
    try endServerHalfOfControl(rig, seat);

    // 3. The peer resets stream 0, alone in a later datagram. Step the
    //    server only, until the seat closes the session.
    try rig.raw.client.conn.streamReset(control, reset_code);
    try rig.raw.drainOutgoing(rig.raw.nowUs());
    try waitForSeatClosing(rig, seat);
    // Both halves of stream 0 have ended. In the trap order the tick freed
    // the stream before the Driver reported the end.
    if (order == .tick_then_service) try std.testing.expect(seat.conn.streamRecvWasReaped(control));
    try std.testing.expectEqual(capnpc.rpc.events.DisconnectCause.transport_error, seat.closeCause());
    try std.testing.expect(seat.streams.get(control) == null);
}

/// The host stops stream 0, and then ONE service pass gets the FIN of
/// stream 0 and the peer's CONNECTION_CLOSE after it. The seat closes the
/// session for the stopped stream 0, but the connection was closing first,
/// so the session keeps the cause of that close (`peer_close`), not
/// `transport_error`.
pub fn runEmbeddedControlStoppedThenPeerClose(order: Order) !void {
    const allocator = std.testing.allocator;
    const rig = try SeatRig.create(allocator, order);
    defer rig.destroy();

    const seat = try rig.handshake();
    const control: u64 = quic.baseline_stream_id;
    try rig.raw.ensureControlStream();
    try writePreamble(&rig.raw);

    // 1. Step until the server has sent its own preamble on stream 0.
    var patience = Patience.begin();
    while (seat.native.preamble_len == 0 or seat.native.preamble_offset != seat.native.preamble_len) {
        try rig.step();
        try patience.wait();
    }
    for (0..4) |_| {
        try rig.raw.step(std.Io.Duration.zero);
        try rig.loop.step();
    }
    try std.testing.expect(!seat.isClosing());

    // 2. The host stops stream 0. The peer sends the FIN of stream 0, and
    //    then closes the connection, in later datagrams.
    try seat.conn.streamStopSending(control, stop_code);
    const client_conn = rig.raw.client.conn;
    try client_conn.streamFinish(control);
    try rig.raw.drainOutgoing(rig.raw.nowUs());
    client_conn.close(false, 0, "done");
    try rig.raw.drainOutgoing(rig.raw.nowUs());

    // 3. The server takes datagrams, with no service pass, until it has the
    //    peer's close. Then one service pass.
    patience = Patience.begin();
    while (seat.conn.closeEvent() == null) {
        _ = try rig.listener.receiveOne(&rig.loop.rx_buf);
        try patience.wait();
    }
    try std.testing.expect(!seat.isClosing());
    try rig.loop.step();

    // The connection was closing (the peer's close) when the Driver gave the
    // end of stream 0 to the seat. The seat closed the session for the stop
    // and kept the cause of the peer's close.
    const close_event = seat.conn.closeEvent() orelse return error.TestExpectedCloseEvent;
    try std.testing.expectEqual(quic_zig.CloseSource.peer, close_event.source);
    const recv_end = seat.conn.streamRecvEnd(control) orelse return error.TestExpectedStreamRecvEnd;
    try std.testing.expect(recv_end.stopped);
    try std.testing.expect(seat.isClosing());
    try std.testing.expect(seat.streams.get(control) == null);
    try std.testing.expectEqual(capnpc.rpc.events.DisconnectCause.peer_close, seat.closeCause());
}

/// Steps 1 and 2 of the stream-0 runners: end the server's own half of
/// stream 0, so that a tick can free stream 0 once its receive half ends
/// too (the trap order).
fn endServerHalfOfControl(rig: *SeatRig, seat: *quic.EmbeddedSession) !void {
    const control: u64 = quic.baseline_stream_id;

    // 1. Step until the server has sent its own preamble on stream 0, so no
    //    write of the seat meets the stop below.
    var patience = Patience.begin();
    while (seat.native.preamble_len == 0 or seat.native.preamble_offset != seat.native.preamble_len) {
        try rig.step();
        try patience.wait();
    }

    // 2. The peer stops the server's half of stream 0. The server answers
    //    with RESET_STREAM. Step until the peer has acknowledged it: the
    //    server's half has then ended.
    try rig.raw.client.conn.streamStopSending(control, stop_code);
    try rig.raw.drainOutgoing(rig.raw.nowUs());
    patience = Patience.begin();
    while (true) {
        try rig.step();
        const stream = seat.conn.stream(control) orelse return error.TestExpectedControlStream;
        if (stream.send.isTerminal()) break;
        try patience.wait();
    }
    try std.testing.expect(!seat.isClosing());
}

/// Step the server only, until the seat closes the session.
fn waitForSeatClosing(rig: *SeatRig, seat: *quic.EmbeddedSession) !void {
    const patience = Patience.begin();
    while (!seat.isClosing()) {
        try rig.loop.step();
        if (rig.host.seat == null) return error.TestSeatGoneBeforeClose;
        try patience.wait();
    }
}

/// The host stops stream 0 (the seat never does), and then the peer ends
/// it: with a RESET_STREAM, which is how a peer answers a stop, or with a
/// FIN that left the peer before the stop reached it. quic-zig threw away
/// what arrived after the stop, so stream 0 lost bytes: session loss by the
/// E-order contract. The Driver reports the end as `.reset` (a RESET) or
/// `.reaped` (a FIN), and `streamRecvEnd(0).stopped` is true. In the trap
/// order a tick frees stream 0 before the Driver reports the end.
pub fn runEmbeddedControlStopped(end: End, order: Order) !void {
    const allocator = std.testing.allocator;
    const rig = try SeatRig.create(allocator, order);
    defer rig.destroy();

    const seat = try rig.handshake();
    const control: u64 = quic.baseline_stream_id;
    try rig.raw.ensureControlStream();
    try writePreamble(&rig.raw);
    try endServerHalfOfControl(rig, seat);

    // 3. The host stops stream 0. The peer ends stream 0 before it learns
    //    of the stop: the raw client does not step again.
    try seat.conn.streamStopSending(control, stop_code);
    try sendEnd(rig.raw.client.conn, control, end);
    try rig.raw.drainOutgoing(rig.raw.nowUs());
    try waitForSeatClosing(rig, seat);

    const recv_end = seat.conn.streamRecvEnd(control) orelse return error.TestExpectedStreamRecvEnd;
    try std.testing.expect(recv_end.stopped);
    switch (end) {
        .fin => try std.testing.expectEqual(@as(?u64, null), recv_end.reset_code),
        .reset => try std.testing.expectEqual(@as(?u64, reset_code), recv_end.reset_code),
    }
    if (order == .tick_then_service) try std.testing.expect(seat.conn.streamRecvWasReaped(control));
    try std.testing.expectEqual(capnpc.rpc.events.DisconnectCause.transport_error, seat.closeCause());
    try std.testing.expect(seat.streams.get(control) == null);
}

// ---------------------------------------------------------------------------
// The end of stream 0. A clean FIN ends the session once every whole frame
// before it has been dispatched: close code `normal`, cause `peer_close`. A
// partial frame at the FIN is a `frame_error` close. A RESET ends the session
// at once: close code `normal`, cause `transport_error`, as the seat already
// did. Through v0.26.0 the owned loops ignored both ends and the seat ignored
// a FIN, so a question that waited on the session ran to its deadline.
// ---------------------------------------------------------------------------

/// How the peer ends its half of stream 0.
pub const ControlEnd = enum {
    /// RESET_STREAM.
    reset,
    /// A whole frame, and then a FIN, in one STREAM frame.
    fin,
    /// A frame without its last byte, and then a FIN, in one STREAM frame.
    fin_mid_frame,
};

/// The deadline of the client question that waits on the session. A session
/// that stays up after stream 0 ends settles the question only at this
/// deadline, with `CallTimedOut`.
const question_deadline_ms: u64 = 5_000;
/// The session must end within this time after the end of stream 0.
const prompt_end_ms: i64 = 1_000;
/// How long the client runner waits for the question: past its deadline, so
/// that a session that stays up shows `CallTimedOut`, not a wait timeout.
const question_patience_ms: i64 = question_deadline_ms + 3_000;
/// The reason of the Return that the raw server puts before its FIN.
const answered_reason = "answered before the end";

fn elapsedMs(since: std.Io.Timestamp) i64 {
    const now = std.Io.Timestamp.now(std.testing.io, .awake);
    return since.durationTo(now).toMilliseconds();
}

/// `frame` as one RPC frame on stream 0 in the wire of `mode`: a 32-bit
/// length prefix (baseline) or an inline control frame (native). Caller frees.
fn encodeControlFrame(
    allocator: std.mem.Allocator,
    mode: quic.TransportMode,
    sequence: u64,
    frame: []const u8,
    max_control_frame_bytes: usize,
) ![]u8 {
    switch (mode) {
        .baseline => {
            const out = try allocator.alloc(u8, quic.length_prefix_bytes + frame.len);
            std.mem.writeInt(u32, out[0..quic.length_prefix_bytes], @intCast(frame.len), .little);
            @memcpy(out[quic.length_prefix_bytes..], frame);
            return out;
        },
        .native => return quic.native.encodeInlineRpc(allocator, sequence, frame, max_control_frame_bytes),
    }
}

/// End this side's half of stream 0 on `conn`. For the FIN cases, `framed`
/// (whole, or without its last byte) goes into the same STREAM frame as the
/// FIN. The caller sends the datagram.
fn endControlStream(conn: *quic_zig.Connection, end: ControlEnd, framed: []const u8) !void {
    const control = quic.baseline_stream_id;
    switch (end) {
        .reset => try conn.streamReset(control, reset_code),
        .fin, .fin_mid_frame => {
            const bytes = if (end == .fin) framed else framed[0 .. framed.len - 1];
            try std.testing.expectEqual(bytes.len, try conn.streamWrite(control, bytes));
            try conn.streamFinish(control);
        },
    }
}

/// The close that `end` must give. `recorder` saw one whole frame before the
/// end. Pass a null `cause` to skip that check.
fn expectControlEndClose(
    end: ControlEnd,
    status: ?quic.CloseStatus,
    cause: ?DisconnectCause,
    recorder: *const Recorder,
) !void {
    const close_status = status orelse return error.QuicLoopbackMissingCloseStatus;
    switch (end) {
        .reset => {
            try std.testing.expectEqual(quic.ApplicationCloseCode.normal, close_status.code);
            try std.testing.expectEqual(@as(?anyerror, null), close_status.err);
            if (cause) |c| try std.testing.expectEqual(DisconnectCause.transport_error, c);
            try std.testing.expectEqual(@as(usize, 1), recorder.messages);
            try std.testing.expectEqual(@as(usize, 0), recorder.errors);
        },
        .fin => {
            try std.testing.expectEqual(quic.ApplicationCloseCode.normal, close_status.code);
            try std.testing.expectEqual(@as(?anyerror, null), close_status.err);
            if (cause) |c| try std.testing.expectEqual(DisconnectCause.peer_close, c);
            // The frame in the same STREAM frame as the FIN arrived too.
            try std.testing.expectEqual(@as(usize, 2), recorder.messages);
            try std.testing.expectEqual(@as(usize, 0), recorder.errors);
            try std.testing.expectEqualSlices(u8, &payload, recorder.received[0..recorder.received_len]);
        },
        .fin_mid_frame => {
            try std.testing.expectEqual(quic.ApplicationCloseCode.frame_error, close_status.code);
            try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), close_status.err);
            if (cause) |c| try std.testing.expectEqual(DisconnectCause.transport_error, c);
            try std.testing.expectEqual(@as(usize, 1), recorder.messages);
            try std.testing.expectEqual(@as(usize, 1), recorder.errors);
            try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), recorder.last_error);
        },
    }
}

/// A server session's callbacks, and how it closed. The close callback
/// copies the cause and the status: the server may free the session after
/// it.
const SessionProbe = struct {
    recorder: Recorder = .{},
    closed: bool = false,
    close_cause: DisconnectCause = .unknown,
    close_status: ?quic.CloseStatus = null,

    fn onAccepted(ctx: ?*anyopaque, _: *quic.Server, session: *quic.ServerSession) anyerror!void {
        session.start(ctx.?, onMessage, onError, onClose);
    }

    fn onMessage(session: *quic.ServerSession, frame: []const u8) anyerror!void {
        probeOf(session).recorder.recordMessage(frame);
    }

    fn onError(session: *quic.ServerSession, err: anyerror) void {
        probeOf(session).recorder.recordError(err);
    }

    fn onClose(session: *quic.ServerSession) void {
        const probe = probeOf(session);
        probe.closed = true;
        probe.close_cause = session.closeCause();
        probe.close_status = session.closeStatus();
    }

    fn probeOf(session: *quic.ServerSession) *SessionProbe {
        return @ptrCast(@alignCast(session.context().?));
    }
};

/// The owned fanout `Server`. A raw quic client sends one whole frame on
/// stream 0 and waits until the server has dispatched it. Then it ends
/// stream 0 as `end` says, and the server must close the session at once.
pub fn runServerControlEnd(mode: quic.TransportMode, end: ControlEnd) !void {
    const allocator = std.testing.allocator;

    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = mode,
        .native = native_options,
    });
    defer server.deinit();
    var probe = SessionProbe{};
    server.setOnSessionAccepted(&probe, SessionProbe.onAccepted);

    var raw = try RawFaultClient.init(allocator, std.testing.io, server.getAddress());
    defer raw.deinit();
    _ = try handshakeServer(&raw, &server);

    // 1. One whole frame. Step until the server has dispatched it.
    try raw.ensureControlStream();
    if (mode == .native) try writePreamble(&raw);
    const first = try encodeControlFrame(allocator, mode, 0, &payload, native_options.max_control_frame_bytes);
    defer allocator.free(first);
    try raw.writeAll(quic.baseline_stream_id, first);
    var patience = Patience.begin();
    while (!probe.recorder.done()) {
        _ = try server.stepOnce(.poll);
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    try probe.recorder.expectWholeFrame();
    for (0..4) |_| try raw.step(std.Io.Duration.zero);

    // 2. The end, with a second frame before a FIN.
    const second = try encodeControlFrame(allocator, mode, 1, &payload, native_options.max_control_frame_bytes);
    defer allocator.free(second);
    try endControlStream(raw.client.conn, end, second);
    try raw.drainOutgoing(raw.nowUs());

    // 3. Step until the server closes the session.
    patience = Patience.begin();
    while (!probe.closed) {
        _ = try server.stepOnce(.poll);
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    try std.testing.expect(elapsedMs(patience.start) < prompt_end_ms);
    try expectControlEndClose(end, probe.close_status, probe.close_cause, &probe.recorder);
}

/// Native mode, the owned fanout `Server`. A `data_rpc` announcement and the
/// FIN of stream 0 come in one STREAM frame, before the data stream opens.
/// The session must stay up until the frame completes from its data stream,
/// then dispatch it and close with `normal` and `peer_close`.
pub fn runNativeServerFinBeforeDataStream() !void {
    const allocator = std.testing.allocator;

    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer server.deinit();
    var probe = SessionProbe{};
    server.setOnSessionAccepted(&probe, SessionProbe.onAccepted);

    var raw = try RawFaultClient.init(allocator, std.testing.io, server.getAddress());
    defer raw.deinit();
    const session = try handshakeServer(&raw, &server);
    const data_stream: u64 = 2;

    // 1. The preamble, then the announcement and the FIN of stream 0.
    try raw.ensureControlStream();
    try writePreamble(&raw);
    const announce = try quic.native.encodeDataRpc(allocator, 0, data_stream, payload_len, native_options.max_control_frame_bytes);
    defer allocator.free(announce);
    try std.testing.expectEqual(announce.len, try raw.client.conn.streamWrite(quic.baseline_stream_id, announce));
    try raw.client.conn.streamFinish(quic.baseline_stream_id);
    try raw.drainOutgoing(raw.nowUs());

    // 2. Step until the server waits on the data stream. The session stays
    //    up: the announced frame came before the FIN.
    var patience = Patience.begin();
    while (session.native.pending_data == null) {
        _ = try server.stepOnce(.poll);
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    for (0..4) |_| {
        _ = try server.stepOnce(.poll);
        try raw.step(std.Io.Duration.zero);
    }
    try std.testing.expect(!probe.closed);
    try std.testing.expect(!session.isClosing());

    // 3. The data stream. The frame completes, then the session ends.
    try raw.ensureUniStream(data_stream);
    try raw.writeAll(data_stream, &payload);
    try raw.client.conn.streamFinish(data_stream);
    try raw.drainOutgoing(raw.nowUs());
    patience = Patience.begin();
    while (!probe.closed) {
        _ = try server.stepOnce(.poll);
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    try probe.recorder.expectWholeFrame();
    const status = probe.close_status orelse return error.QuicLoopbackMissingCloseStatus;
    try std.testing.expectEqual(quic.ApplicationCloseCode.normal, status.code);
    try std.testing.expectEqual(DisconnectCause.peer_close, probe.close_cause);
}

/// Native mode, the owned fanout `Server`. The peer sends half of the
/// native preface and then the FIN of stream 0: a partial frame at the FIN,
/// so the server closes with `frame_error`.
pub fn runNativeServerFinMidPreface() !void {
    const allocator = std.testing.allocator;

    var server = try quic.Server.init(allocator, std.testing.io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = .native,
        .native = native_options,
    });
    defer server.deinit();
    var probe = SessionProbe{};
    server.setOnSessionAccepted(&probe, SessionProbe.onAccepted);

    var raw = try RawFaultClient.init(allocator, std.testing.io, server.getAddress());
    defer raw.deinit();
    _ = try handshakeServer(&raw, &server);

    try raw.ensureControlStream();
    const half = quic.native.preface[0 .. quic.native.preface.len / 2];
    try std.testing.expectEqual(half.len, try raw.client.conn.streamWrite(quic.baseline_stream_id, half));
    try raw.client.conn.streamFinish(quic.baseline_stream_id);
    try raw.drainOutgoing(raw.nowUs());

    const patience = Patience.begin();
    while (!probe.closed) {
        _ = try server.stepOnce(.poll);
        try raw.step(std.Io.Duration.zero);
        try patience.wait();
    }
    try std.testing.expect(elapsedMs(patience.start) < prompt_end_ms);
    const status = probe.close_status orelse return error.QuicLoopbackMissingCloseStatus;
    try std.testing.expectEqual(quic.ApplicationCloseCode.frame_error, status.code);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), status.err);
    try std.testing.expectEqual(@as(usize, 0), probe.recorder.messages);
}

/// The embedded seat, as in `runServerControlEnd`. For a frame error the
/// seat records its cause only when quic-zig reports the connection closed,
/// a pass after the close begins, so that case checks the status only.
pub fn runEmbeddedControlEnd(mode: quic.TransportMode, end: ControlEnd, order: Order) !void {
    const allocator = std.testing.allocator;
    const rig = try SeatRig.createWithMode(allocator, order, mode);
    defer rig.destroy();
    const seat = try rig.handshake();

    // 1. One whole frame. Step until the seat has dispatched it.
    try rig.raw.ensureControlStream();
    if (mode == .native) try writePreamble(&rig.raw);
    const first = try encodeControlFrame(allocator, mode, 0, &payload, native_options.max_control_frame_bytes);
    defer allocator.free(first);
    try rig.raw.writeAll(quic.baseline_stream_id, first);
    try rig.waitForRecorder();
    try rig.recorder.expectWholeFrame();
    for (0..4) |_| {
        try rig.raw.step(std.Io.Duration.zero);
        try rig.loop.step();
    }

    // 2. The end, with a second frame before a FIN.
    const second = try encodeControlFrame(allocator, mode, 1, &payload, native_options.max_control_frame_bytes);
    defer allocator.free(second);
    try endControlStream(rig.raw.client.conn, end, second);
    try rig.raw.drainOutgoing(rig.raw.nowUs());

    // 3. Step the server until the seat closes the session.
    const since = Patience.begin();
    try waitForSeatClosing(rig, seat);
    try std.testing.expect(elapsedMs(since.start) < prompt_end_ms);
    const cause: ?DisconnectCause = if (end == .fin_mid_frame) null else seat.closeCause();
    try expectControlEndClose(end, seat.closeStatus(), cause, &rig.recorder);
}

const QuestionOutcome = enum { pending, answered, disconnected, timed_out, other };

/// The outcome of one client question, from its Return.
const QuestionProbe = struct {
    outcome: QuestionOutcome = .pending,

    fn onReturn(
        ctx_ptr: *anyopaque,
        _: *Peer,
        ret: protocol.Return,
        _: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const self: *QuestionProbe = @ptrCast(@alignCast(ctx_ptr));
        self.outcome = outcomeOf(ret);
    }

    fn outcomeOf(ret: protocol.Return) QuestionOutcome {
        if (ret.tag != .exception) return .other;
        const ex = ret.exception orelse return .other;
        if (std.mem.eql(u8, ex.reason, answered_reason)) return .answered;
        return switch (ex.kind()) {
            .disconnected => .disconnected,
            .overloaded => if (std.mem.eql(u8, ex.reason, Peer.deadline_reason_text)) .timed_out else .other,
            else => .other,
        };
    }
};

/// The Return that answers the client's question `answer_id` with an
/// exception that names it. Caller frees.
fn buildAnsweredReturn(allocator: std.mem.Allocator, answer_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .exception);
    try ret.setException(answered_reason);
    return builder.finish();
}

/// The owned client `Connection`, under a `Peer`, against a raw quic
/// server. The client asks two questions, each with a 5 s deadline. The
/// server ends its half of stream 0 as `end` says; for the FIN cases a
/// Return that answers the first question comes in the same STREAM frame as
/// the FIN. The second question must then fail with `Disconnected` within
/// 1 s, not with `CallTimedOut` at its deadline.
pub fn runClientControlEnd(mode: quic.TransportMode, end: ControlEnd) !void {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var server = RawServerPeer{ .listener = try quic.Listener.init(allocator, io, .{
        .listen_addr = loopback.testListenAddr(),
        .tls_cert_pem = loopback.loopback_cert_pem,
        .tls_key_pem = loopback.loopback_key_pem,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = mode,
    }) };
    defer server.listener.deinit();

    var client = try quic.Connection.initClient(allocator, io, .{
        .remote_addr = server.listener.getAddress(),
        .server_name = "localhost",
        .insecure_skip_verify = true,
        .receive_timeout = std.Io.Duration.fromMilliseconds(1),
        .mode = mode,
    });
    defer client.deinit();

    var client_peer = Peer.init(allocator, &client);
    defer client_peer.deinit();
    client_peer.disableThreadAffinity();
    client_peer.setClockIo(io);
    client_peer.setTimeouts(.{ .default_call_timeout_ms = question_deadline_ms });
    client_peer.start(null, null, null);
    // The loop's terminal pass runs the close callback, where the `Peer`
    // fails every question that still waits. It runs once.
    var finished = false;
    defer if (!finished) {
        client.requestClose();
        client.run();
    };

    var answered = QuestionProbe{};
    var waiting = QuestionProbe{};
    const answered_id = try client_peer.sendBootstrap(&answered, QuestionProbe.onReturn);
    _ = try client_peer.sendBootstrap(&waiting, QuestionProbe.onReturn);

    // 1. The handshake. The client opens stream 0 and sends its questions.
    var patience = Patience.begin();
    while (true) {
        _ = try client.stepOnce(.poll);
        try server.step();
        if (server.conn()) |server_conn| {
            if (server_conn.handshakeDone() and server_conn.stream(quic.baseline_stream_id) != null) break;
        }
        try patience.wait();
    }
    const server_conn = server.conn().?;
    if (mode == .native) try writePreamble(&server);
    for (0..4) |_| {
        _ = try client.stepOnce(.poll);
        try server.step();
    }
    try std.testing.expect(!client.isClosing());

    // 2. The server ends its half of stream 0.
    const answer = try buildAnsweredReturn(allocator, answered_id);
    defer allocator.free(answer);
    const framed = try encodeControlFrame(allocator, mode, 0, answer, quic.default_native_max_control_frame_bytes);
    defer allocator.free(framed);
    try endControlStream(server_conn, end, framed);
    try server.flush();
    const since = std.Io.Timestamp.now(io, .awake);

    // 3. Step until the waiting question settles: at once when the client
    //    ends the session, at its deadline when the session stays up.
    while (waiting.outcome == .pending) {
        if (client.isClosing()) {
            client.run();
            finished = true;
            break;
        }
        if (elapsedMs(since) >= question_patience_ms) return error.QuicLoopbackTimedOut;
        _ = try client.stepOnce(.poll);
        try server.step();
        loopback.sleepMs(1);
    }
    const elapsed_ms = elapsedMs(since);
    if (waiting.outcome != .disconnected or elapsed_ms >= prompt_end_ms) {
        std.debug.print("{s} {s}: the waiting question ended {s} after {d} ms\n", .{
            @tagName(mode), @tagName(end), @tagName(waiting.outcome), elapsed_ms,
        });
    }
    try std.testing.expectEqual(QuestionOutcome.disconnected, waiting.outcome);
    try std.testing.expect(elapsed_ms < prompt_end_ms);

    const status = client.closeStatus() orelse return error.QuicLoopbackMissingCloseStatus;
    switch (end) {
        .reset => {
            try std.testing.expectEqual(QuestionOutcome.disconnected, answered.outcome);
            try std.testing.expectEqual(quic.ApplicationCloseCode.normal, status.code);
            try std.testing.expectEqual(DisconnectCause.transport_error, client.closeCause());
        },
        .fin => {
            // The Return in the same STREAM frame as the FIN reached the Peer.
            try std.testing.expectEqual(QuestionOutcome.answered, answered.outcome);
            try std.testing.expectEqual(quic.ApplicationCloseCode.normal, status.code);
            try std.testing.expectEqual(DisconnectCause.peer_close, client.closeCause());
        },
        .fin_mid_frame => {
            try std.testing.expectEqual(QuestionOutcome.disconnected, answered.outcome);
            try std.testing.expectEqual(quic.ApplicationCloseCode.frame_error, status.code);
            try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), status.err);
            try std.testing.expectEqual(DisconnectCause.transport_error, client.closeCause());
        },
    }
}
