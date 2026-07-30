//! One-connection TCP server lifecycle: `ServerSession.accept()` takes a
//! connection off a `Listener` and returns a heap-owned session bundling the
//! `Connection` and `Peer` a server previously had to allocate, wire, start,
//! and tear down by hand. It is the server-side mirror of `ClientSession`.
//!
//! Set the bootstrap capability between `accept()` and `run()` with the
//! generated `<Interface>.setBootstrap(&session.peer, &server)` helper; the
//! peer is not started until `run()`, so the bootstrap is in place before any
//! inbound message is dispatched.
//!
//! Scope: strictly one connection per session. A server that fans out over
//! many connections keeps using `Listener` + `WorkerPool`; a `ServerSession`
//! is the accept-one-and-serve-it building block (spawn one per accepted
//! connection if you want a simple concurrent server).
//!
//! Threading contract is identical to `ClientSession`: the session is
//! thread-affine (`accept`, `setBootstrap` on `.peer`, `run`, `close`,
//! `deinit`, and all callbacks on one thread); `run()` re-adopts affinity on
//! entry so accept-on-A / run-on-B is legal as long as nothing else touches
//! the session in between. `requestStop()` is the only thread-safe entry.

const std = @import("std");

const connection_mod = @import("./connection.zig");
const runtime = @import("./runtime.zig");
const peer_mod = @import("../../peer/mod.zig");
const events = @import("../../events.zig");

const Connection = connection_mod.Connection;
const Peer = peer_mod.Peer;
const Listener = runtime.Listener;

pub const ServeOptions = struct {
    /// Passed through to `Connection.init`. `tick_interval_ms` is filled with
    /// 100 when null and `default_call_timeout_ms` is non-null, so deadlines
    /// fire without extra wiring; an explicit user tick is never overridden.
    conn: Connection.Options = .{},

    /// Deadline stamped on every outbound question this peer makes (a server
    /// that holds client capabilities calls back through them). On by default;
    /// null disables (and disables the auto-tick rule).
    default_call_timeout_ms: ?u64 = 30_000,

    /// Graceful-drain bound for close(); outstanding questions are
    /// force-cancelled after it expires.
    shutdown_drain_timeout_ms: ?u64 = 5_000,

    limits: peer_mod.PeerLimits = .{},

    /// Observer applied to the peer, and to the connection when the conn
    /// sub-options do not set their own.
    observer: ?events.Observer = null,

    /// User context + lifecycle callbacks. `ctx` must outlive the session.
    /// Both fire on the run() thread. `on_close` must NOT call `deinit()`.
    ctx: ?*anyopaque = null,
    on_error: ?*const fn (ctx: ?*anyopaque, session: *ServerSession, err: anyerror) void = null,
    on_close: ?*const fn (ctx: ?*anyopaque, session: *ServerSession) void = null,
};

pub const ServerSession = struct {
    // Single heap allocation; Connection and Peer embedded BY VALUE so
    // `fromPeer` works via @fieldParentPtr and there is exactly one
    // create/destroy.
    conn: Connection,
    peer: Peer,
    /// Backs the peer's accept-embargo entropy source (seeded fail-closed
    /// from `io.randomSecure` at construction).
    embargo_rng: std.Random.DefaultCsprng,
    allocator: std.mem.Allocator,
    io: std.Io,
    started: bool,
    user_ctx: ?*anyopaque,
    user_on_error: ?*const fn (?*anyopaque, *ServerSession, anyerror) void,
    user_on_close: ?*const fn (?*anyopaque, *ServerSession) void,

    /// Accept one connection off `listener` (blocks until a client connects)
    /// and wire it into a session. The peer is NOT started yet: set the
    /// bootstrap on `session.peer` (via the generated `setBootstrap`) and then
    /// call `run()`. The session uses the listener's `std.Io` backend.
    pub fn accept(
        gpa: std.mem.Allocator,
        listener: *Listener,
        options: ServeOptions,
    ) !*ServerSession {
        const io = listener.ioBackend();
        const fd = try listener.acceptFd();
        var socket_owned = true;
        errdefer if (socket_owned) runtime.closeFd(io, .{ .handle = fd.handle });

        const self = try gpa.create(ServerSession);
        errdefer gpa.destroy(self);

        var conn_opts = options.conn;
        if (options.default_call_timeout_ms != null and conn_opts.tick_interval_ms == null) {
            conn_opts.tick_interval_ms = 100;
        }
        if (conn_opts.observer == null) conn_opts.observer = options.observer;

        self.allocator = gpa;
        self.io = io;
        self.started = false;
        self.user_ctx = options.ctx;
        self.user_on_error = options.on_error;
        self.user_on_close = options.on_close;

        self.conn = try Connection.init(gpa, io, .{ .handle = fd.handle }, conn_opts);
        socket_owned = false; // conn.deinit() closes the socket from here on
        errdefer self.conn.deinit();

        self.peer = Peer.init(gpa, &self.conn);
        self.peer.setLimits(options.limits);
        self.peer.setClockIo(io);
        // FAIL CLOSED on missing OS entropy (never a guessable fallback id),
        // mapped into the existing error set: an entropy syscall failure is a
        // system-level fault, reported as Unexpected with the cause logged.
        self.embargo_rng = peer_mod.seedEntropyCsprng(io) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.EntropyUnavailable => {
                std.log.scoped(.rpc_tcp).warn("OS entropy unavailable; refusing to construct session", .{});
                return error.Unexpected;
            },
        };
        self.peer.setEntropySource(peer_mod.EntropySource.fromCsprng(&self.embargo_rng));
        self.peer.setTimeouts(.{
            .default_call_timeout_ms = options.default_call_timeout_ms,
            .shutdown_drain_timeout_ms = options.shutdown_drain_timeout_ms,
        });
        if (options.observer) |obs| self.peer.setObserver(obs);
        return self;
    }

    /// Recover the session inside generated callbacks (which receive a
    /// `*Peer`). Only valid for peers embedded in a ServerSession.
    pub fn fromPeer(peer: *Peer) *ServerSession {
        return @alignCast(@fieldParentPtr("peer", peer));
    }

    /// Start the peer (on first call) and run the blocking read loop until the
    /// transport closes and `on_close` has fired. Adopts thread affinity on
    /// entry (see the module doc's threading contract). Set the bootstrap
    /// before calling this.
    pub fn run(self: *ServerSession) void {
        self.peer.adoptOwnerThread();
        self.conn.adoptOwnerThread();
        if (!self.started) {
            self.started = true;
            self.peer.start(self, onPeerError, onPeerClose);
        }
        self.conn.run();
    }

    /// Graceful close. Idempotent. Session-thread only — legal from any
    /// callback (they run on the session thread).
    pub fn close(self: *ServerSession) void {
        if (!self.peer.isAttachedTransportClosing()) {
            self.peer.closeAttachedTransport();
        }
    }

    /// Thread-safe abort: no graceful drain. The only cross-thread entry point.
    pub fn requestStop(self: *ServerSession) void {
        self.conn.requestClose();
    }

    /// Free everything. Legal ONLY after `run()` returned, or if `run()` was
    /// never called. Encapsulates the one blessed teardown ordering — detach
    /// before peer teardown (releases would write to a dead transport), peer
    /// before connection, destroy last.
    pub fn deinit(self: *ServerSession) void {
        const gpa = self.allocator;
        _ = self.peer.takeAttachedConnection(*Connection);
        self.peer.deinit();
        self.conn.deinit();
        gpa.destroy(self);
    }

    fn onPeerError(ctx: ?*anyopaque, peer: *Peer, err: anyerror) void {
        if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        const raw = ctx orelse return;
        const self: *ServerSession = @ptrCast(@alignCast(raw));
        if (self.user_on_error) |cb| cb(self.user_ctx, self, err);
    }

    fn onPeerClose(ctx: ?*anyopaque, _: *Peer) void {
        const raw = ctx orelse return;
        const self: *ServerSession = @ptrCast(@alignCast(raw));
        if (self.user_on_close) |cb| cb(self.user_ctx, self);
    }
};
