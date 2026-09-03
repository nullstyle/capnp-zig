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
/// seat for connections that negotiated `capnp-rpc/1`. The contract mirrors
/// the owned-loop transport on the embedder side:
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
///      and before `Server.tick`.
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
    wake_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    const StreamBuffer = struct {
        data: std.ArrayListUnmanaged(u8) = .empty,
        /// Bytes already consumed by `streamRead`; `data[consumed..]` unread.
        consumed: usize = 0,
        /// Total bytes ever pushed on this stream (the final size once `fin`).
        total: usize = 0,
        fin: bool = false,

        fn compactIfNeeded(self: *StreamBuffer) void {
            if (self.consumed == 0) return;
            if (self.consumed == self.data.items.len) {
                self.data.clearRetainingCapacity();
                self.consumed = 0;
                return;
            }
            if (self.consumed < 32 or self.consumed < self.data.items.len - self.consumed) return;
            const live = self.data.items.len - self.consumed;
            std.mem.copyForwards(u8, self.data.items[0..live], self.data.items[self.consumed..]);
            self.data.items.len = live;
            self.consumed = 0;
        }
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
                // `early_open` and `defer_early_dispatch` are dialer/server
                // posture knobs of the owned loop; an embedded session is
                // always a fresh server session.
                false,
                false,
                .hold_until_handshake,
            ),
            .native = NativeEngine.init(
                allocator,
                .server,
                options.max_message_bytes,
                options.max_outbound_queue_items,
                options.max_outbound_queue_bytes,
                options.native,
                false,
                false,
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

    /// Forward from the embedder's `on_stream_open`. `bidi` is ignored: the
    /// engines validate stream roles themselves (baseline reads only stream
    /// 0; native reads stream 0 plus peer-initiated uni streams).
    pub fn onStreamOpen(self: *EmbeddedSession, stream_id: u64, bidi: bool) !void {
        _ = bidi;
        const gop = try self.streams.getOrPut(self.allocator, stream_id);
        if (!gop.found_existing) gop.value_ptr.* = .{};
    }

    /// Forward from the embedder's `on_stream_data`. Bytes buffer in
    /// arrival order; the next `service` pass feeds them to the engines.
    pub fn onStreamData(self: *EmbeddedSession, stream_id: u64, chunk: []const u8) !void {
        if (chunk.len == 0) return;
        const gop = try self.streams.getOrPut(self.allocator, stream_id);
        if (!gop.found_existing) gop.value_ptr.* = .{};
        const buf = gop.value_ptr;
        if (buf.fin) return error.StreamClosed;
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
    /// `.fin` keeps the stream entry (its final size settles native
    /// pending-data completion); `.reset` and `.reaped` drop it — the bytes
    /// are gone and no read can complete. A reset of the ordered control
    /// stream (stream 0) is session loss by the E-order contract and closes
    /// the session; a reset of an announced native data stream surfaces
    /// through the engine's completion deadline instead.
    pub fn onStreamEnd(self: *EmbeddedSession, stream_id: u64, end: quic_zig.app.StreamEnd) void {
        switch (end) {
            .fin => {
                if (self.streams.getPtr(stream_id)) |buf| buf.fin = true;
            },
            .reset, .reaped => {
                if (self.streams.fetchRemove(stream_id)) |removed| {
                    var buf = removed.value;
                    self.buffered_bytes -= unreadBytes(&buf);
                    buf.data.deinit(self.allocator);
                }
                if (stream_id == quic_options.baseline_stream_id and end == .reset) {
                    self.requestControlStreamLoss();
                }
            },
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
            .native => try router.native.service(owner.native(), adapter, now_us),
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

        const StreamView = struct {
            recv: struct { final_size: ?usize },
        };

        pub fn stream(self: BufferedConn, stream_id: u64) ?StreamView {
            const buf = self.session.streams.getPtr(stream_id) orelse return null;
            return .{ .recv = .{ .final_size = if (buf.fin) buf.total else null } };
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
