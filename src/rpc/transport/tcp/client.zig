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
const client_wiring = @import("./client_wiring.zig");
const peer_mod = @import("../../peer/mod.zig");
const events = @import("../../events.zig");

const Connection = connection_mod.Connection;
const Peer = peer_mod.Peer;

pub const ConnectOptions = struct {
    /// Passed through to `Connection.init`. `tick_interval_ms` inside this
    /// is filled with 100 when null and either the call or Join timeout is
    /// finite, so deadlines actually fire without extra wiring; an explicit
    /// user tick is never overridden.
    conn: Connection.Options = .{},

    /// Deadline stamped on every outbound question at send time. On by
    /// default; null disables (and disables the auto-tick rule).
    default_call_timeout_ms: ?u64 = 30_000,

    /// Graceful-drain bound for close(); outstanding questions are
    /// force-cancelled after it expires.
    shutdown_drain_timeout_ms: ?u64 = 5_000,

    /// Secure lease for inbound L4 Join phases. Null explicitly restores the
    /// raw-Peer compatibility behavior (no Join expiry).
    join_timeout_ms: ?u64 = 30_000,

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
    /// Backs the peer's accept-embargo entropy source (seeded fail-closed
    /// from `io.randomSecure` at construction).
    embargo_rng: std.Random.DefaultCsprng,
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
        runtime.setTcpNoDelay(.{ .handle = fd });
        // `wire` owns the socket from here, on success and on error. The
        // AF_UNIX `rpc.transport.unix.connect` shares it.
        return client_wiring.wire(gpa, io, .{ .handle = fd }, options);
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
};
