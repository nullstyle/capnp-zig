const std = @import("std");
const quic_zig = @import("quic");

const events = @import("../../events.zig");
const baseline_engine = @import("baseline_engine.zig");
const callback_lifecycle_mod = @import("callback_lifecycle.zig");
const close_controller_mod = @import("close_controller.zig");
const connection_adapters = @import("connection_adapters.zig");
const connection_dispatch = @import("connection_dispatch.zig");
const connection_termination = @import("connection_termination.zig");
const mode_router = @import("mode_router.zig");
const native_engine = @import("native_engine.zig");
const peer_streams = @import("peer_streams.zig");
const quic_options = @import("options.zig");

const BaselineEngine = baseline_engine.BaselineEngine;
const NativeEngine = native_engine.NativeEngine;
const Role = @import("endpoint.zig").Role;

/// Whether a QUIC connection negotiated capnp-zig's RPC ALPN. A foreign
/// embedder hosting several protocols on one listener branches its
/// `quic.app.Driver` hooks on this (the mirror of qmsg's `isQmsgAlpn`).
pub fn isCapnpSessionAlpn(conn: *quic_zig.Connection) bool {
    const negotiated = conn.negotiatedAlpn() orelse return false;
    return std.mem.eql(u8, negotiated, quic_options.alpn);
}

pub const EmbeddedSessionOptions = struct {
    /// Wire mode, exactly like `ServerOptions.mode`: NOT negotiated. Both
    /// peers must be configured for the same mode; a mismatch is malformed
    /// transport input and closes the session.
    mode: quic_options.TransportMode = .baseline,
    /// 0-RTT posture of the HOST listener this session rides, exactly like
    /// `ServerOptions.early_data`: the seat derives the same server-side
    /// replay hold the owned loop does. If the host's listener accepts
    /// early data without TLS-level anti-replay, this MUST say so —
    /// `.without_replay_protection` arms the hold that keeps a replayed
    /// first flight from executing RPC frames before the handshake (which
    /// a replay can never complete).
    early_data: quic_options.EarlyData = .disabled,
    /// What may execute inside the hold window, exactly like
    /// `ServerOptions.early_dispatch`.
    early_dispatch: quic_options.EarlyDispatchMode = .hold_until_handshake,
    max_message_bytes: usize = quic_options.default_max_message_bytes,
    max_outbound_queue_items: usize = quic_options.default_max_outbound_queue_items,
    max_outbound_queue_bytes: usize = quic_options.default_max_outbound_queue_bytes,
    native: quic_options.NativeOptions = .{},
    /// Engine read scratch, sized like `ServerOptions.stream_read_buffer_size`.
    stream_read_buffer_size: usize = 16 * 1024,
    /// Cap on stream bytes pushed by the embedder's hooks but not yet
    /// consumed by the engines. The engines' own frame/pending-data budgets
    /// bound what a conforming peer can make the session buffer; this bounds
    /// the in-flight window between hook delivery and the next `service`
    /// pass against a peer that sprays bytes on streams the session never
    /// ordered. Overflow closes the session as a frame error.
    max_buffered_stream_bytes: usize = 512 * 1024,
    reveal_close_reason_on_wire: bool = false,
    observer: ?events.Observer = null,
};

/// One Cap'n Proto RPC vat session riding a connection owned by a foreign
/// embedder.
///
/// The embedder owns the UDP socket, the `quic_zig.Server`, the ONE
/// `quic.app.Driver`, and the driving loop; this session is the protocol
/// seat for connections that negotiated `capnp-rpc/1`. Because the socket is
/// the embedder's, so are its kernel buffers: the owned loop asks for
/// `default_udp_socket_recv_buffer_bytes`, and an embedder should do the
/// same (quic-zig's `transport.applyServerTuning`), or native mode loses
/// full-window bursts to the OS default. The contract mirrors the owned-loop
/// transport on the embedder side:
///
///   1. When a connection's ALPN matches, create one session
///      (`create`) — before the handshake completes, so 0-RTT stream data
///      is not lost.
///   2. Attach a `Peer` via `peer.attachConnection(session)`; the session
///      satisfies the same connection shape as `rpc.transport.quic.Connection`
///      (`start`/`sendFrame`/`close`/`isClosing`/`context`, the `on_tick`
///      field, `closeCause`).
///   3. Forward the embedder's Driver hooks: `onStreamOpen`,
///      `onStreamData`, `onStreamEnd`, and `notifyDisconnected`.
///   4. Call `service(now_us)` once per loop pass, after `driver.service`
///      and before `Server.tick` ("Embedder rules" in
///      docs/quic-transport.md: a tick first can reclaim a stream before the
///      Driver reads it).
///
/// Frames reach the `Peer` strictly in stream order (QUIC per-stream order
/// plus FIFO seat buffers), preserving the E-order contract of
/// `rpc.capnp`. A RESET observed on the ordered control stream (stream 0)
/// ends the whole session — sub-session failure semantics do not exist in
/// the Cap'n Proto RPC protocol.
pub const EmbeddedSession = struct {
    const CallbackLifecycle = callback_lifecycle_mod.State(EmbeddedSession);
    const Adapters = connection_adapters.State(EmbeddedSession);
    const Dispatch = connection_dispatch.State(EmbeddedSession);
    const Termination = connection_termination.State(EmbeddedSession);

    pub const MessageCallback = CallbackLifecycle.MessageCallback;
    pub const ErrorCallback = CallbackLifecycle.ErrorCallback;
    pub const CloseCallback = CallbackLifecycle.CloseCallback;
    pub const TickCallback = CallbackLifecycle.TickCallback;

    allocator: std.mem.Allocator,
    conn: *quic_zig.Connection,
    role: Role = .server,
    observer: ?events.Observer,
    max_message_bytes: usize,
    mode: quic_options.TransportMode,
    baseline: BaselineEngine,
    native: NativeEngine,
    close_controller: close_controller_mod.Controller,
    callback_lifecycle: CallbackLifecycle = .{},
    closing_emitted: bool = false,
    closed_notified: bool = false,
    close_cause: events.DisconnectCause = .unknown,

    /// Deadline sweep hook, wired by `Peer.attachConnection`. Invoked from
    /// `service` on the `min_tick_interval_us` cadence — without a driven
    /// `service`, call deadlines never fire.
    on_tick: ?TickCallback = null,
    last_tick_us: u64 = 0,

    stream_read_buf: []u8,
    streams: std.AutoHashMapUnmanaged(u64, StreamBuffer) = .empty,
    max_buffered_stream_bytes: usize,
    buffered_bytes: usize = 0,
    /// How many entries of `streams` other than stream 0 have ended. While
    /// it is zero, `releaseDrainedDataStreams` has nothing to do.
    ended_data_streams: usize = 0,
    wake_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    const StreamBuffer = struct {
        data: std.ArrayListUnmanaged(u8) = .empty,
        /// Bytes already consumed by `streamRead`; `data[consumed..]` unread.
        consumed: usize = 0,
        /// Total bytes ever pushed on this stream (the final size once
        /// `ended`, unless `reset` gives it).
        total: usize = 0,
        /// No more bytes come on this stream: it ended clean, the peer reset
        /// it, or quic-zig reclaimed it and cannot say how it ended
        /// (`onStreamEnd`). The bytes already pushed stay here until the
        /// engine reads them.
        ended: bool = false,
        /// The peer's RESET_STREAM, with the final size and error code from
        /// `Connection.streamRecvEnd`. Its final size can be larger than
        /// `total`: quic-zig drops the bytes it held unread when the reset
        /// arrives.
        reset: ?PeerReset = null,

        fn drained(self: *const StreamBuffer) bool {
            return self.consumed == self.data.items.len;
        }

        fn compactIfNeeded(self: *StreamBuffer) void {
            if (self.consumed == 0) return;
            if (self.consumed == self.data.items.len) {
                self.data.clearRetainingCapacity();
                self.consumed = 0;
                return;
            }
            if (self.consumed < 32 or self.consumed < self.data.items.len - self.consumed) return;
            const live = self.data.items.len - self.consumed;
            // Disjoint: the guard above returns unless consumed >= live, so
            // the destination [0, live) ends at or before the source starts.
            @memcpy(self.data.items[0..live], self.data.items[self.consumed..]);
            self.data.items.len = live;
            self.consumed = 0;
        }
    };

    /// The two values of a RESET_STREAM frame that the engines read.
    const PeerReset = struct {
        final_size: u64,
        error_code: u64,
    };

    pub fn create(
        allocator: std.mem.Allocator,
        conn: *quic_zig.Connection,
        options: EmbeddedSessionOptions,
    ) !*EmbeddedSession {
        const stream_read_buf = try allocator.alloc(u8, options.stream_read_buffer_size);
        errdefer allocator.free(stream_read_buf);
        const self = try allocator.create(EmbeddedSession);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .conn = conn,
            .observer = options.observer,
            .max_message_bytes = options.max_message_bytes,
            .mode = options.mode,
            .baseline = BaselineEngine.init(
                allocator,
                options.max_message_bytes,
                options.max_outbound_queue_items,
                options.max_outbound_queue_bytes,
                // `early_open` is a dialer knob; an embedded session is
                // always a server session. The replay hold derives from the
                // host listener's 0-RTT posture, exactly like the owned
                // loop's `fromServer`.
                false,
                std.meta.activeTag(options.early_data) == .without_replay_protection,
                options.early_dispatch,
            ),
            .native = NativeEngine.init(
                allocator,
                .server,
                options.max_message_bytes,
                options.max_outbound_queue_items,
                options.max_outbound_queue_bytes,
                options.native,
                false,
                std.meta.activeTag(options.early_data) == .without_replay_protection,
            ),
            .close_controller = close_controller_mod.Controller.init(
                options.reveal_close_reason_on_wire,
            ),
            .stream_read_buf = stream_read_buf,
            .max_buffered_stream_bytes = options.max_buffered_stream_bytes,
        };
        return self;
    }

    /// Host-side teardown. If a callback is currently running on this
    /// session, the teardown is deferred to `service`/`notifyDisconnected`
    /// (same decision table as the owned connection's `deinit`).
    pub fn destroy(self: *EmbeddedSession) void {
        switch (self.callback_lifecycle.decideDeinit()) {
            .already_deinitialized => return,
            .defer_until_callback_exits => {
                self.requestClose();
                return;
            },
            .deinit_now => self.deinitNow(),
        }
    }

    fn deinitNow(self: *EmbeddedSession) void {
        if (!self.callback_lifecycle.beginDeinit()) return;
        self.baseline.deinit(self.allocator);
        self.native.deinit(self.allocator);
        self.callback_lifecycle.clearCallbacks();
        var it = self.streams.valueIterator();
        while (it.next()) |buf| buf.data.deinit(self.allocator);
        self.streams.deinit(self.allocator);
        self.allocator.free(self.stream_read_buf);
        self.allocator.destroy(self);
    }

    // ---- Peer-facing connection shape ------------------------------------

    pub fn start(
        self: *EmbeddedSession,
        ctx: *anyopaque,
        on_message: MessageCallback,
        on_error: ErrorCallback,
        on_close: CloseCallback,
    ) void {
        self.callback_lifecycle.start(ctx, on_message, on_error, on_close);
        events.emitConnection(self.observer, eventSource(self.mode), eventRole(self.role), .started);
    }

    pub fn context(self: *const EmbeddedSession) ?*anyopaque {
        return self.callback_lifecycle.context();
    }

    /// Any-thread safe, like the owned QUIC connection: enqueues into the
    /// mutex-guarded engine outbound queue; the next `service` pass flushes.
    pub fn sendFrame(self: *EmbeddedSession, frame: []const u8) !void {
        return try Dispatch.sendFrame(self, frame);
    }

    pub fn close(self: *EmbeddedSession) void {
        Termination.emitClosingOnce(self);
        Termination.close(self);
    }

    pub fn requestClose(self: *EmbeddedSession) void {
        Termination.requestClose(self);
    }

    pub fn isClosing(self: *const EmbeddedSession) bool {
        return Termination.isClosing(self);
    }

    pub fn closeStatus(self: *const EmbeddedSession) ?@import("close.zig").Status {
        return Termination.status(self);
    }

    pub fn closeCause(self: *const EmbeddedSession) events.DisconnectCause {
        return self.close_cause;
    }

    /// The live quic-zig connection this session rides — the same accessor
    /// the owned `Connection` exposes, consumed by the shared termination
    /// and close-controller layers.
    pub fn activeQuicConnection(self: *EmbeddedSession) ?*quic_zig.Connection {
        return self.conn;
    }

    /// Whether `sendFrame`/`requestClose` asked for a service pass since the
    /// last one — advisory for embedders that sleep instead of polling.
    pub fn needsService(self: *EmbeddedSession) bool {
        return self.wake_requested.swap(false, .acq_rel);
    }

    pub fn wake(self: *EmbeddedSession) void {
        self.wake_requested.store(true, .release);
    }

    pub const AdapterAccess = struct {
        pub fn dispatchRpcFrame(sess: *EmbeddedSession, frame: []const u8) !void {
            try Dispatch.dispatchRpcFrame(sess, frame);
        }

        pub fn terminateFrameError(sess: *EmbeddedSession, err: anyerror) void {
            Termination.frameError(sess, err);
        }

        pub fn terminateInternalError(sess: *EmbeddedSession, err: anyerror) void {
            Termination.internalError(sess, err);
        }
    };

    // ---- Embedder hook bodies ---------------------------------------------

    /// Forward from the embedder's `on_stream_open`. A stream with no place
    /// in the protocol (anything but the client's stream 0 and, in native
    /// mode, the peer's unidirectional data streams) is refused on both
    /// halves at once, as the owned loops do (`peer_streams.zig`): left
    /// unanswered it would keep its place in the peer's stream window for
    /// the life of the connection.
    pub fn onStreamOpen(self: *EmbeddedSession, stream_id: u64, bidi: bool) !void {
        if (!peer_streams.expected(self.role, self.mode, stream_id)) {
            peer_streams.refuse(self.conn, stream_id, bidi);
            return;
        }
        const gop = try self.streams.getOrPut(self.allocator, stream_id);
        if (!gop.found_existing) gop.value_ptr.* = .{};
    }

    /// Forward from the embedder's `on_stream_data`. Bytes buffer in
    /// arrival order; the next `service` pass feeds them to the engines.
    /// Bytes of a refused stream (see `onStreamOpen`) are dropped.
    pub fn onStreamData(self: *EmbeddedSession, stream_id: u64, chunk: []const u8) !void {
        if (chunk.len == 0) return;
        if (!peer_streams.expected(self.role, self.mode, stream_id)) return;
        const gop = try self.streams.getOrPut(self.allocator, stream_id);
        if (!gop.found_existing) gop.value_ptr.* = .{};
        const buf = gop.value_ptr;
        if (buf.ended) return error.StreamClosed;
        if (self.buffered_bytes + chunk.len > self.max_buffered_stream_bytes) {
            Termination.frameError(self, error.FrameTooLarge);
            return;
        }
        try buf.data.appendSlice(self.allocator, chunk);
        self.buffered_bytes += chunk.len;
        buf.total += chunk.len;
    }

    /// Forward from the embedder's `on_stream_end`.
    ///
    /// The seat does not act on the Driver's `end` alone. It asks quic-zig
    /// how the stream ended (`Connection.streamRecvEnd`), which answers the
    /// same before and after the `tick` that reclaims the stream (quic-zig
    /// v0.28.0). See `EndKind` for the five kinds of end.
    ///
    /// The ordered control stream (stream 0): a clean end keeps its entry.
    /// A reset, or an end that quic-zig cannot classify, is session loss by
    /// the E-order contract: it drops the entry and closes the session. A
    /// stopped stream and the teardown pass drop the entry only.
    ///
    /// A native data stream keeps the bytes the Driver delivered and is
    /// marked as ended for a clean end, a reset and an unknown end. The end
    /// then settles the announced message at once, with no wait for the
    /// completion deadline. For a reset the seat also keeps the peer's final
    /// size and error code, so the engine judges the message as the owned
    /// loops do: a final size other than the announced length fails it
    /// (`InvalidFrame`), missing bytes fail it (`DataStreamReset`), and all
    /// the bytes complete it. For a clean end and an unknown end the bytes
    /// in hand are the final size: fewer than announced fail the message
    /// (`InvalidFrame`). A stopped stream is dropped, never treated as a
    /// completed data stream: after the stop quic-zig threw away the bytes
    /// that still arrived.
    pub fn onStreamEnd(self: *EmbeddedSession, stream_id: u64, end: quic_zig.app.StreamEnd) void {
        const kind = self.classifyEnd(stream_id, end);
        if (stream_id == quic_options.baseline_stream_id) {
            switch (kind) {
                .fin => if (self.streams.getPtr(stream_id)) |buf| {
                    buf.ended = true;
                },
                .reset, .unknown => {
                    self.dropStream(stream_id);
                    self.requestControlStreamLoss();
                },
                .stopped, .teardown => self.dropStream(stream_id),
            }
            return;
        }
        switch (kind) {
            .fin, .unknown => self.markDataStreamEnded(stream_id, null),
            .reset => |reset| self.markDataStreamEnded(stream_id, reset),
            .stopped, .teardown => self.dropStream(stream_id),
        }
    }

    /// How the receive half of a stream ended, as the seat acts on it.
    const EndKind = union(enum) {
        /// A clean FIN: every byte reached the seat.
        fin,
        /// The peer reset the stream (RESET_STREAM), with the final size and
        /// the error code that quic-zig still knows.
        reset: PeerReset,
        /// The stream ended, but quic-zig cannot say how: it reclaimed the
        /// stream and its note of the end is gone, or it cannot give the
        /// final size of a reset that the Driver reported. Treat it as cut,
        /// never as a clean end.
        unknown,
        /// This side stopped the stream (`streamStopSending`), as the seat
        /// does to refuse a stream (`peer_streams.refuse`). quic-zig threw
        /// away the bytes that arrived after the stop. Since quic-zig
        /// v0.28.0 the Driver reports such a stream as `.reaped` while it is
        /// still live (or as `.reset` when the peer answered the stop with a
        /// RESET_STREAM), so `streamRecvWasReaped` alone cannot tell it from
        /// the teardown pass.
        stopped,
        /// The Driver's teardown pass before `onDisconnect`.
        teardown,
    };

    /// Classify the end of `stream_id`. `Connection.streamRecvEnd` is the
    /// direct signal: it answers for a live stream whose receive half has
    /// ended and, through the tick after the reclaiming one, for a stream
    /// that `tick` reclaimed. The Driver's `end` decides only when quic-zig
    /// has no answer.
    fn classifyEnd(self: *EmbeddedSession, stream_id: u64, end: quic_zig.app.StreamEnd) EndKind {
        const recv_end = self.conn.streamRecvEnd(stream_id);
        if (recv_end) |e| if (e.stopped) return .stopped;
        return switch (end) {
            // The Driver reports `.fin` only for an end that it saw clean.
            .fin => .fin,
            .reset => {
                const e = recv_end orelse return .unknown;
                const code = e.reset_code orelse return .unknown;
                return .{ .reset = .{ .final_size = e.final_size, .error_code = code } };
            },
            // The Driver reports `.reaped` for a live end only when the
            // stream was stopped (above), and for a reclaimed stream only
            // when quic-zig does not know its end. Any other `.reaped` is the
            // teardown pass: a stream that has not ended (no answer, not
            // reclaimed), or one whose end the Driver did not report yet.
            .reaped => if (recv_end == null and self.conn.streamRecvWasReaped(stream_id))
                .unknown
            else
                .teardown,
        };
    }

    fn markDataStreamEnded(self: *EmbeddedSession, stream_id: u64, reset: ?PeerReset) void {
        const buf = self.streams.getPtr(stream_id) orelse return;
        if (buf.ended) return;
        buf.ended = true;
        buf.reset = reset;
        self.ended_data_streams += 1;
    }

    fn dropStream(self: *EmbeddedSession, stream_id: u64) void {
        var removed = (self.streams.fetchRemove(stream_id) orelse return).value;
        self.buffered_bytes -= unreadBytes(&removed);
        if (removed.ended and stream_id != quic_options.baseline_stream_id) {
            self.ended_data_streams -= 1;
        }
        removed.data.deinit(self.allocator);
    }

    /// Free the buffer of each data stream that has ended and whose bytes
    /// the engine has read, except the one it still waits on. The engine
    /// never reads such a stream again. Without this pass the seat kept one
    /// entry, and the capacity of its buffer, for every large frame until
    /// the session closed.
    fn releaseDrainedDataStreams(self: *EmbeddedSession) void {
        if (self.ended_data_streams == 0) return;
        const waiting: ?u64 = if (self.native.pending_data) |pending| pending.stream_id else null;
        var batch: [32]u64 = undefined;
        while (true) {
            var n: usize = 0;
            var it = self.streams.iterator();
            while (it.next()) |entry| {
                const id = entry.key_ptr.*;
                const buf = entry.value_ptr;
                if (id == quic_options.baseline_stream_id) continue;
                if (!buf.ended or !buf.drained()) continue;
                if (waiting) |waiting_id| if (waiting_id == id) continue;
                batch[n] = id;
                n += 1;
                if (n == batch.len) break;
            }
            for (batch[0..n]) |id| self.dropStream(id);
            if (n < batch.len) return;
        }
    }

    fn requestControlStreamLoss(self: *EmbeddedSession) void {
        if (self.close_cause == .unknown) self.close_cause = .transport_error;
        self.close();
    }

    /// Forward from the embedder's `on_disconnect` (the Driver's will-close
    /// path). Captures the typed close cause from the sticky close
    /// certificate while the connection is still live, then fires the
    /// peer's close callback exactly once.
    pub fn notifyDisconnected(self: *EmbeddedSession) void {
        self.captureCloseCause();
        if (self.closed_notified) return;
        self.closed_notified = true;
        if (self.close_controller.hasPendingCrossThreadClose()) {
            Termination.emitClosingOnce(self);
        }
        events.emitClose(self.observer, eventSource(self.mode), eventRole(self.role), closeErr(self));
        events.emitConnection(self.observer, eventSource(self.mode), eventRole(self.role), .closed);
        if (self.callback_lifecycle.closeCallback()) |cb| {
            self.callback_lifecycle.invokeClose(self, cb);
        }
        if (self.callback_lifecycle.shouldCompleteDeferredDeinit()) {
            self.deinitNow();
        }
    }

    fn captureCloseCause(self: *EmbeddedSession) void {
        if (self.close_cause != .unknown) return;
        const ev = self.conn.closeEvent() orelse return;
        self.close_cause = @import("close.zig").disconnectCauseFor(ev);
    }

    // ---- Service pass ------------------------------------------------------

    /// One service pass: drain a deferred cross-thread close, run the mode
    /// engine against the buffered adapter (inbound dispatch + outbound
    /// flush), notice a dead connection, and drive the `Peer` deadline
    /// sweep on the tick cadence.
    ///
    /// Call once per embedder loop pass, AFTER `driver.service(server)` and
    /// BEFORE `server.tick(now_us)` — the same ordering rule the Driver
    /// itself imposes.
    pub fn service(self: *EmbeddedSession, now_us: u64) !void {
        if (self.close_controller.drainPendingClose(&self.baseline, &self.native)) {
            Termination.emitClosingOnce(self);
        }

        const adapter = BufferedConn{ .session = self };
        const router = mode_router.fromConnection(self);
        const owner = Adapters.engineOwner(self);
        switch (self.mode) {
            .baseline => try router.baseline.service(owner.baseline(), adapter),
            .native => {
                try router.native.service(owner.native(), adapter, now_us);
                self.releaseDrainedDataStreams();
            },
        }

        if (self.conn.isClosed()) {
            if (!self.closed_notified) self.notifyDisconnected();
            return;
        }

        self.invokeTick(now_us);
    }

    fn invokeTick(self: *EmbeddedSession, now_us: u64) void {
        const cb = self.on_tick orelse return;
        if (now_us -| self.last_tick_us < quic_options.min_tick_interval_us) return;
        self.last_tick_us = now_us;
        self.callback_lifecycle.invokeTick(self, cb);
    }

    fn unreadBytes(buf: *const StreamBuffer) usize {
        return buf.data.items.len - buf.consumed;
    }

    fn closeErr(self: *const EmbeddedSession) ?anyerror {
        const status = self.close_controller.status() orelse return null;
        return status.err;
    }

    /// The engines' view of the connection: reads come from the seat's
    /// per-stream buffers (the embedder's Driver already consumed them from
    /// the wire); writes and control queries go to the real connection.
    const BufferedConn = struct {
        session: *EmbeddedSession,

        pub fn handshakeDone(self: BufferedConn) bool {
            return self.session.conn.handshakeDone();
        }

        pub fn streamArrivedInEarlyData(self: BufferedConn, stream_id: u64) ?bool {
            return self.session.conn.streamArrivedInEarlyData(stream_id);
        }

        /// The part of quic-zig's `Stream` the engines read, with the same
        /// values quic-zig gives. After a RESET, `final_size` is the final
        /// size of the RESET and `reset` holds its error code. After a clean
        /// end, or an end that quic-zig cannot classify, `final_size` is the
        /// bytes in hand.
        const StreamView = struct {
            recv: struct {
                final_size: ?u64,
                reset: ?struct { error_code: u64 } = null,
            },
        };

        pub fn stream(self: BufferedConn, stream_id: u64) ?StreamView {
            const buf = self.session.streams.getPtr(stream_id) orelse return null;
            if (buf.reset) |reset| return .{ .recv = .{
                .final_size = reset.final_size,
                .reset = .{ .error_code = reset.error_code },
            } };
            return .{ .recv = .{ .final_size = if (buf.ended) buf.total else null } };
        }

        /// For a stream that has no buffer in the seat (`stream` gives
        /// null): how quic-zig says it ended. The engine settles a frame from
        /// it when the seat dropped the stream (this side stopped it) or
        /// freed it (it ended empty before its announcement).
        pub fn streamRecvEnd(self: BufferedConn, stream_id: u64) ?quic_zig.StreamRecvEnd {
            return self.session.conn.streamRecvEnd(stream_id);
        }

        pub fn streamRecvWasReaped(self: BufferedConn, stream_id: u64) bool {
            return self.session.conn.streamRecvWasReaped(stream_id);
        }

        pub fn openBidi(self: BufferedConn, stream_id: u64) !*quic_zig.Connection.Stream {
            return self.session.conn.openBidi(stream_id);
        }

        pub fn openUni(self: BufferedConn, stream_id: u64) !*quic_zig.Connection.Stream {
            return self.session.conn.openUni(stream_id);
        }

        /// `anyerror` (not an inferred set): the engines' catch switches
        /// carry `else` prongs for transport-level errors, which must stay
        /// reachable against the buffered adapter's narrow failure set.
        pub fn streamRead(self: BufferedConn, stream_id: u64, dst: []u8) anyerror!usize {
            const buf = self.session.streams.getPtr(stream_id) orelse return error.StreamNotFound;
            const unread = buf.data.items[buf.consumed..];
            const n = @min(unread.len, dst.len);
            @memcpy(dst[0..n], unread[0..n]);
            buf.consumed += n;
            self.session.buffered_bytes -= n;
            buf.compactIfNeeded();
            return n;
        }

        pub fn streamWrite(self: BufferedConn, stream_id: u64, data: []const u8) !usize {
            return self.session.conn.streamWrite(stream_id, data);
        }

        pub fn streamFinish(self: BufferedConn, stream_id: u64) !void {
            return self.session.conn.streamFinish(stream_id);
        }
    };

    fn eventSource(mode: quic_options.TransportMode) events.Source {
        return switch (mode) {
            .baseline => .quic_baseline,
            .native => .quic_native,
        };
    }

    fn eventRole(role: Role) events.Role {
        return switch (role) {
            .client => .client,
            .server => .server,
        };
    }
};
