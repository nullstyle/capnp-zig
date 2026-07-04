//! One-call TCP client lifecycle: `connect()` returns a heap-owned
//! `ClientSession` that bundles the `Connection` and `Peer` every RPC client
//! previously had to allocate, wire, and tear down by hand — including the
//! run()/on_close ordering that two shipped consumers solved two different
//! ways (and one of them got wrong first).
//!
//! Threading contract: the session is thread-affine. `connect`, calls,
//! `run`, `close`, and `deinit` all happen on one thread; every callback
//! fires inside `run()` on that thread. Issuing calls before `run()` is
//! legal — outbound frames only enqueue; nothing reads the socket until the
//! run loop starts. To background a session, either run its whole lifecycle
//! on your own thread, or `connect` on thread A and call `run()` on thread
//! B: `run()` re-adopts affinity on entry, which is safe because nothing
//! else may touch the session between `connect` returning and `run()`
//! starting. `requestStop()` is the single thread-safe entry point.

const std = @import("std");
const builtin = @import("builtin");

const connection_mod = @import("./connection.zig");
const runtime = @import("./runtime.zig");
const peer_mod = @import("../../peer/mod.zig");
const events = @import("../../events.zig");

const Connection = connection_mod.Connection;
const Peer = peer_mod.Peer;

pub const ConnectOptions = struct {
    /// Passed through to `Connection.init`. `tick_interval_ms` inside this
    /// is filled with 100 when null and `default_call_timeout_ms` is
    /// non-null, so deadlines actually fire without extra wiring; an
    /// explicit user tick is never overridden.
    conn: Connection.Options = .{},

    /// Deadline stamped on every outbound question at send time. On by
    /// default; null disables (and disables the auto-tick rule).
    default_call_timeout_ms: ?u64 = 30_000,

    /// Graceful-drain bound for close(); outstanding questions are
    /// force-cancelled after it expires.
    shutdown_drain_timeout_ms: ?u64 = 5_000,

    limits: peer_mod.PeerLimits = .{},

    /// Observer applied to the peer, and to the connection when the conn
    /// sub-options do not set their own.
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
    // create/destroy. Both are initialized directly into their final
    // addresses — no moves after `attachConnection`.
    conn: Connection,
    peer: Peer,
    allocator: std.mem.Allocator,
    io: std.Io,
    user_ctx: ?*anyopaque,
    user_on_error: ?*const fn (?*anyopaque, *ClientSession, anyerror) void,
    user_on_close: ?*const fn (?*anyopaque, *ClientSession) void,

    /// TCP connect + wire + `peer.start`. On return the session is live:
    /// bootstrap and calls are legal immediately (writes enqueue; nothing
    /// reads the socket until `run()`).
    pub fn connect(
        gpa: std.mem.Allocator,
        io: std.Io,
        address: std.Io.net.IpAddress,
        options: ConnectOptions,
    ) !*ClientSession {
        const tcp_stream = try std.Io.net.IpAddress.connect(&address, io, .{ .mode = .stream });
        const fd = tcp_stream.socket.handle;
        var socket_owned = true;
        errdefer if (socket_owned) runtime.closeFd(io, .{ .handle = fd });
        runtime.setTcpNoDelay(.{ .handle = fd });

        const self = try gpa.create(ClientSession);
        errdefer gpa.destroy(self);

        var conn_opts = options.conn;
        if (options.default_call_timeout_ms != null and conn_opts.tick_interval_ms == null) {
            conn_opts.tick_interval_ms = 100;
        }
        if (conn_opts.observer == null) conn_opts.observer = options.observer;

        self.allocator = gpa;
        self.io = io;
        self.user_ctx = options.ctx;
        self.user_on_error = options.on_error;
        self.user_on_close = options.on_close;

        self.conn = try Connection.init(gpa, io, .{ .handle = fd }, conn_opts);
        socket_owned = false; // conn.deinit() closes the socket from here on
        errdefer self.conn.deinit();

        self.peer = Peer.init(gpa, &self.conn);
        self.peer.setLimits(options.limits);
        self.peer.setClockIo(io);
        self.peer.setTimeouts(.{
            .default_call_timeout_ms = options.default_call_timeout_ms,
            .shutdown_drain_timeout_ms = options.shutdown_drain_timeout_ms,
        });
        if (options.observer) |obs| self.peer.setObserver(obs);
        self.peer.start(self, onPeerError, onPeerClose);
        return self;
    }

    /// `IpAddress.parse(host, port)` + `connect`.
    pub fn connectHost(
        gpa: std.mem.Allocator,
        io: std.Io,
        host: []const u8,
        port: u16,
        options: ConnectOptions,
    ) !*ClientSession {
        const address = try std.Io.net.IpAddress.parse(host, port);
        return connect(gpa, io, address, options);
    }

    /// Recover the session inside generated callbacks (which receive a
    /// `*Peer`). Only valid for peers embedded in a ClientSession.
    pub fn fromPeer(peer: *Peer) *ClientSession {
        return @alignCast(@fieldParentPtr("peer", peer));
    }

    /// Blocking read loop. Adopts thread affinity on entry (see the module
    /// doc's threading contract). Returns after the transport has closed
    /// and `on_close` has fired.
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

    /// Thread-safe abort: no graceful drain; in-flight questions resolve
    /// as Disconnected (not Canceled). The only cross-thread entry point.
    pub fn requestStop(self: *ClientSession) void {
        self.conn.requestClose();
    }

    /// Free everything. Legal ONLY after `run()` returned, or if `run()`
    /// was never called. Encapsulates the one blessed teardown ordering —
    /// detach before peer teardown (releases would write to a dead
    /// transport), peer before connection, destroy last. Written
    /// straight-line on purpose: three defers read backwards.
    pub fn deinit(self: *ClientSession) void {
        const gpa = self.allocator;
        _ = self.peer.takeAttachedConnection(*Connection);
        self.peer.deinit();
        self.conn.deinit();
        gpa.destroy(self);
    }

    fn onPeerError(ctx: ?*anyopaque, peer: *Peer, err: anyerror) void {
        // The boilerplate every consumer used to carry: a peer error means
        // the transport is done; close it so run() unwinds.
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
