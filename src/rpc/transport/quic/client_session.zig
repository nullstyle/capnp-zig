//! One-call QUIC client lifecycle: `connect()` returns a heap-owned
//! `ClientSession` that bundles the QUIC `Connection` and the `Peer`, the
//! QUIC twin of `rpc.transport.tcp.ClientSession`. It owns the same blessed
//! wiring and teardown order, so a QUIC client is the same few lines as a TCP
//! one: connect, bootstrap, `run()`, `deinit()`.
//!
//! Threading contract (identical to TCP): the session is thread-affine.
//! `connect`, calls, `run`, `close`, and `deinit` all happen on one thread,
//! and every callback fires inside `run()` on that thread. Issuing calls
//! before `run()` is legal: outbound frames only queue until the loop drives
//! the handshake. To background a session, run its whole lifecycle on your
//! own thread, or `connect` on thread A and call `run()` on thread B: `run()`
//! re-adopts affinity on entry, which is safe because nothing else may touch
//! the session between `connect` returning and `run()` starting.
//! `requestStop()` is the single thread-safe entry point.
//!
//! Experimental, like the rest of the QUIC transport.

const std = @import("std");

const connection_mod = @import("./connection.zig");
const options_mod = @import("./options.zig");
const peer_mod = @import("../../peer/mod.zig");
const events = @import("../../events.zig");

const Connection = connection_mod.Connection;
const ClientOptions = options_mod.ClientOptions;
const Peer = peer_mod.Peer;

pub const ConnectOptions = struct {
    /// Passed through to `Connection.initClient`: the server address and
    /// name, TLS verification, transport mode and resource budgets. Keep
    /// certificate verification on outside local tests.
    conn: ClientOptions,

    /// Deadline stamped on every outbound question at send time. On by
    /// default; null disables. The QUIC loop drives the peer's deadline
    /// sweep on its own step cadence, so no tick option is needed.
    default_call_timeout_ms: ?u64 = 30_000,

    /// Graceful-drain bound for close(); outstanding questions are
    /// force-cancelled after it expires.
    shutdown_drain_timeout_ms: ?u64 = 5_000,

    /// Secure lease for inbound L4 Join phases. Null explicitly restores the
    /// raw-Peer compatibility behavior (no Join expiry).
    join_timeout_ms: ?u64 = 30_000,

    limits: peer_mod.PeerLimits = .{},

    /// Observer applied to the peer, and to the connection when `conn` does
    /// not set its own.
    observer: ?events.Observer = null,

    /// User context + lifecycle callbacks. `ctx` must outlive the session.
    /// Both fire on the run() thread. `on_close` must NOT call `deinit()` —
    /// it runs inside `run()`; deinit only after `run()` returns.
    ctx: ?*anyopaque = null,
    on_error: ?*const fn (ctx: ?*anyopaque, session: *ClientSession, err: anyerror) void = null,
    on_close: ?*const fn (ctx: ?*anyopaque, session: *ClientSession) void = null,
};

pub const ClientSession = struct {
    // Single heap allocation: Connection and Peer are embedded BY VALUE so
    // `fromPeer` works via @fieldParentPtr and there is exactly one
    // create/destroy. The Peer is attached only once the Connection sits at
    // its final address.
    conn: Connection,
    peer: Peer,
    /// Backs the peer's accept-embargo entropy source (seeded fail-closed
    /// from `io.randomSecure` at construction).
    embargo_rng: std.Random.DefaultCsprng,
    allocator: std.mem.Allocator,
    io: std.Io,
    user_ctx: ?*anyopaque,
    user_on_error: ?*const fn (?*anyopaque, *ClientSession, anyerror) void,
    user_on_close: ?*const fn (?*anyopaque, *ClientSession) void,

    /// Dial + wire + `peer.start`. On return the session is live: bootstrap
    /// and calls are legal immediately (frames queue until `run()` drives
    /// the handshake).
    pub fn connect(
        gpa: std.mem.Allocator,
        io: std.Io,
        options: ConnectOptions,
    ) !*ClientSession {
        const self = try gpa.create(ClientSession);
        errdefer gpa.destroy(self);

        var conn_opts = options.conn;
        if (conn_opts.observer == null) conn_opts.observer = options.observer;

        self.allocator = gpa;
        self.io = io;
        self.user_ctx = options.ctx;
        self.user_on_error = options.on_error;
        self.user_on_close = options.on_close;

        self.conn = try Connection.initClient(gpa, io, conn_opts);
        errdefer self.conn.deinit();

        self.peer = Peer.init(gpa, &self.conn);
        // On a later failure, detach before peer teardown so nothing it
        // releases is written into the transport, then let the conn errdefer
        // close it.
        errdefer {
            _ = self.peer.takeAttachedConnection(*Connection);
            self.peer.deinit();
        }
        self.peer.setLimits(options.limits);
        self.peer.setClockIo(io);
        // FAIL CLOSED on missing OS entropy (never a guessable fallback id),
        // mapped into the existing error set exactly as the TCP session does.
        self.embargo_rng = peer_mod.seedEntropyCsprng(io) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.EntropyUnavailable => {
                std.log.scoped(.rpc_quic).warn("OS entropy unavailable; refusing to construct session", .{});
                return error.Unexpected;
            },
        };
        self.peer.setEntropySource(peer_mod.EntropySource.fromCsprng(&self.embargo_rng));
        self.peer.setTimeouts(.{
            .default_call_timeout_ms = options.default_call_timeout_ms,
            .shutdown_drain_timeout_ms = options.shutdown_drain_timeout_ms,
            .join_timeout_ms = options.join_timeout_ms,
        });
        if (options.observer) |obs| self.peer.setObserver(obs);
        self.peer.start(self, onPeerError, onPeerClose);
        return self;
    }

    /// Recover the session inside generated callbacks (which receive a
    /// `*Peer`). Only valid for peers embedded in a ClientSession.
    pub fn fromPeer(peer: *Peer) *ClientSession {
        return @alignCast(@fieldParentPtr("peer", peer));
    }

    /// Blocking loop: drives the handshake, every frame, and the peer's
    /// deadline sweep. Adopts thread affinity on entry (see the module doc's
    /// threading contract). Returns after the transport has closed and
    /// `on_close` has fired.
    pub fn run(self: *ClientSession) void {
        self.peer.adoptOwnerThread();
        self.conn.adoptOwnerThread();
        self.conn.run();
    }

    /// Graceful close. Idempotent. Session-thread only — legal from any
    /// callback (they run on the session thread).
    pub fn close(self: *ClientSession) void {
        if (!self.peer.isAttachedTransportClosing()) {
            self.peer.closeAttachedTransport();
        }
    }

    /// Thread-safe stop: the run loop closes the connection on its own
    /// thread and in-flight questions resolve as Disconnected. The only
    /// cross-thread entry point.
    pub fn requestStop(self: *ClientSession) void {
        self.conn.requestClose();
    }

    /// Typed close cause ("death certificate"); `.unknown` while the
    /// connection is alive. Valid inside `on_close` and after `run()`
    /// returns; `.stateless_reset` proves the server lost its state.
    pub fn closeCause(self: *const ClientSession) events.DisconnectCause {
        return self.conn.closeCause();
    }

    /// Free everything. Legal ONLY after `run()` returned, or if `run()`
    /// was never called. Same blessed ordering as the TCP session: detach
    /// before peer teardown (releases would write to a dead transport), peer
    /// before connection, destroy last.
    pub fn deinit(self: *ClientSession) void {
        const gpa = self.allocator;
        _ = self.peer.takeAttachedConnection(*Connection);
        self.peer.deinit();
        self.conn.deinit();
        gpa.destroy(self);
    }

    fn onPeerError(ctx: ?*anyopaque, peer: *Peer, err: anyerror) void {
        // A peer error means the transport is done; close it so run()
        // unwinds instead of leaving every consumer to do it.
        if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        const raw = ctx orelse return;
        const self: *ClientSession = @ptrCast(@alignCast(raw));
        if (self.user_on_error) |cb| cb(self.user_ctx, self, err);
    }

    fn onPeerClose(ctx: ?*anyopaque, _: *Peer) void {
        const raw = ctx orelse return;
        const self: *ClientSession = @ptrCast(@alignCast(raw));
        if (self.user_on_close) |cb| cb(self.user_ctx, self);
    }
};
