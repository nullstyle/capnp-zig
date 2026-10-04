//! One-call QUIC RPC server: `serve()` binds a fanout `Server` and gives
//! every accepted QUIC session its own heap-owned `Peer`. It is the QUIC
//! counterpart of `rpc.transport.tcp.ServerSession` (one Connection + Peer
//! bundle per accepted connection), lifted to the many-session listener QUIC
//! already has, and it replaces the hand-rolled "scan `sessionAt()` for new
//! sessions, attach a Peer, free it later" loop.
//!
//! Lifecycle:
//!
//! 1. `serve(gpa, io, server_options, serve_options)` binds the UDP listener.
//!    `getAddress()` is valid immediately (useful with port 0).
//! 2. `run()` blocks, driving every session. For each accepted session it
//!    builds a `Peer` with the same secure defaults as the TCP sessions, then
//!    calls `ServeOptions.on_accept`, which sets the bootstrap
//!    (`_ = try Iface.setBootstrap(&session.peer, &impl)`), then starts the
//!    peer.
//! 3. `requestStop()` (any thread) closes every session; `run()` returns once
//!    they have all drained, and every `on_close` has fired.
//! 4. `deinit()` frees everything.
//!
//! Threading contract: like the TCP sessions, everything is thread-affine to
//! the thread that calls `run()`, and every callback fires there. `init` may
//! happen on another thread; `deinit` may happen on another thread once
//! `run()` has returned. `requestStop()` is the only cross-thread entry point.
//!
//! Experimental, like the rest of the QUIC transport.

const std = @import("std");

const server_mod = @import("./server.zig");
const options_mod = @import("./options.zig");
const peer_mod = @import("../../peer/mod.zig");
const events = @import("../../events.zig");

const Server = server_mod.Server;
const ServerSession = server_mod.ServerSession;
const ServerOptions = options_mod.ServerOptions;
const Peer = peer_mod.Peer;

pub const ServeOptions = struct {
    /// Deadline stamped on every outbound question a session's peer makes (a
    /// server that holds client capabilities calls back through them). On by
    /// default; null disables. The server loop drives each peer's deadline
    /// sweep on its own step cadence.
    default_call_timeout_ms: ?u64 = 30_000,

    /// Graceful-drain bound for a session's close; outstanding questions are
    /// force-cancelled after it expires.
    shutdown_drain_timeout_ms: ?u64 = 5_000,

    /// Secure lease for inbound L4 Join phases. Null explicitly restores the
    /// raw-Peer compatibility behavior (no Join expiry).
    join_timeout_ms: ?u64 = 30_000,

    /// Applied to every session's peer.
    limits: peer_mod.PeerLimits = .{},

    /// Applied to every session's peer. Set `ServerOptions.observer` for
    /// transport events.
    observer: ?events.Observer = null,

    /// User context handed back to every callback. Must outlive the server.
    ctx: ?*anyopaque = null,

    /// Required. Fires once per accepted session, on the `run()` thread,
    /// after the session's peer is built and before it starts: set the
    /// bootstrap here. The session's first inbound frame cannot arrive before
    /// this returns. Returning an error rejects the session: its peer is
    /// discarded and the QUIC connection is closed. See
    /// `Server.setOnSessionAccepted` for when a session counts as accepted
    /// (before its handshake completes) and why the work here must stay
    /// bounded.
    on_accept: *const fn (ctx: ?*anyopaque, session: *PeerServer.Session) anyerror!void,

    /// Optional; fires on the `run()` thread. The session's transport is
    /// already being closed when this runs.
    on_error: ?*const fn (ctx: ?*anyopaque, session: *PeerServer.Session, err: anyerror) void = null,

    /// Optional; fires exactly once per started session, on the `run()`
    /// thread (or on the `deinit()` thread for a session that `run()` left
    /// open). The session stays valid until the step returns; it must not be
    /// freed from here.
    on_close: ?*const fn (ctx: ?*anyopaque, session: *PeerServer.Session) void = null,
};

pub const PeerServer = struct {
    /// One accepted QUIC session and its `Peer`. Owned by the `PeerServer`,
    /// which frees it once the transport is gone.
    pub const Session = struct {
        /// Embedded by value so `fromPeer` works from generated callbacks.
        peer: Peer,
        /// Backs the peer's accept-embargo entropy source (seeded fail-closed
        /// from `io.randomSecure` at construction).
        embargo_rng: std.Random.DefaultCsprng,
        owner: *PeerServer,
        /// The QUIC session id (`ServerSession.id`). Unique for the server's
        /// lifetime, so `owner.server.sessionById(id)` finds the transport
        /// while it lives.
        id: u64,
        peer_addr: ?std.Io.net.IpAddress,
        /// Borrowed transport; null once the session's close callback has
        /// fired. Do not keep it past that point.
        transport: ?*ServerSession,
        /// Free for the application; the server never reads it.
        user_data: ?*anyopaque = null,

        /// Recover the session inside generated callbacks (which receive a
        /// `*Peer`). Only valid for peers created by a `PeerServer`.
        pub fn fromPeer(peer: *Peer) *Session {
            return @alignCast(@fieldParentPtr("peer", peer));
        }

        /// Graceful close of this one session. Idempotent. `run()` thread
        /// only, which includes every callback.
        pub fn close(self: *Session) void {
            if (!self.peer.isAttachedTransportClosing()) {
                self.peer.closeAttachedTransport();
            }
        }

        /// True once the session's close callback has fired.
        pub fn isClosed(self: *const Session) bool {
            return self.transport == null;
        }

        /// Typed close cause ("death certificate"); `.unknown` while the
        /// session is alive. Valid inside `on_close`.
        pub fn closeCause(self: *const Session) events.DisconnectCause {
            return self.peer.lastDisconnectCause();
        }

        /// The client's address at accept time.
        pub fn peerAddress(self: *const Session) ?std.Io.net.IpAddress {
            return self.peer_addr;
        }
    };

    allocator: std.mem.Allocator,
    io: std.Io,
    /// The fanout transport. Its counters (`droppedDatagramCount`,
    /// `feedOutcomeCounts`, ...) are loop-thread readable as usual; do not
    /// step it or replace its accept hook.
    server: Server,
    options: ServeOptions,
    /// Every session whose peer is still allocated, including closed
    /// sessions whose transport is still draining (freed after the step in
    /// which the server destroys it).
    sessions: std.ArrayList(*Session) = .empty,
    /// Internal, loop-thread only. Listed sessions whose close callback has
    /// fired (`transport == null`), so whose transport the server may have
    /// destroyed. While it is zero no session can be gone, and the
    /// after-step reap returns at once.
    closed_pending: usize = 0,
    /// Internal reap scratch: the ids the server still lists, rebuilt on
    /// each reap pass. Capacity is kept, so passes stop allocating once it
    /// has grown to the peak session count.
    live_ids: std.AutoHashMapUnmanaged(u64, void) = .empty,
    /// Internal; read it through `reapPassCount`.
    reap_passes: u64 = 0,

    /// Bind the listener and install the per-session wiring. Nothing is
    /// accepted until `run()`. Set `server_options.max_concurrent_connections`
    /// to the number of simultaneous clients you want.
    pub fn init(
        gpa: std.mem.Allocator,
        io: std.Io,
        server_options: ServerOptions,
        serve_options: ServeOptions,
    ) !*PeerServer {
        const self = try gpa.create(PeerServer);
        errdefer gpa.destroy(self);
        self.* = .{
            .allocator = gpa,
            .io = io,
            .server = undefined,
            .options = serve_options,
        };
        // Sessions borrow the server's wake and receive state, so it must
        // reach its final heap address before the first adoption.
        self.server = try Server.init(gpa, io, server_options);
        self.server.setOnSessionAccepted(self, onSessionAccepted);
        return self;
    }

    /// The bound listener address (valid from `init`, any thread).
    pub fn getAddress(self: *const PeerServer) std.Io.net.IpAddress {
        return self.server.getAddress();
    }

    /// Sessions whose peer is still allocated. `run()` thread only.
    pub fn sessionCount(self: *const PeerServer) usize {
        return self.sessions.items.len;
    }

    /// Reap passes run so far. A pass looks for closed sessions whose
    /// transport the server has destroyed, and runs only after a step that
    /// ends with a closed session still allocated, so the count does not
    /// move while every session is live. `run()` thread only, or after
    /// `run()` returned.
    pub fn reapPassCount(self: *const PeerServer) u64 {
        return self.reap_passes;
    }

    /// Blocking loop. Returns after `requestStop()` once every session has
    /// closed and drained, or after a fatal endpoint error.
    pub fn run(self: *PeerServer) void {
        self.server.runWithAfterStep(self, afterStep);
    }

    /// Thread-safe stop: closes every session and makes `run()` return.
    pub fn requestStop(self: *PeerServer) void {
        self.server.requestClose();
    }

    /// Free the server, every session, and every peer. Legal only after
    /// `run()` returned, or if it was never called, and once no other thread
    /// still calls `requestStop()`. A session `run()` left open gets its
    /// `on_close` here, on this thread.
    pub fn deinit(self: *PeerServer) void {
        const gpa = self.allocator;
        // run() has returned, so nothing else touches these peers; move
        // their affinity here for the close callbacks below.
        for (self.sessions.items) |session| session.peer.adoptOwnerThread();
        // Fires the close callback of every still-live session, then frees
        // the transports. Peers detach in that callback.
        self.server.deinit();
        for (self.sessions.items) |session| self.destroySession(session);
        self.sessions.deinit(gpa);
        self.live_ids.deinit(gpa);
        gpa.destroy(self);
    }

    /// `init` installs both hooks with the server itself as context, so a
    /// null here would be a wiring bug; the hooks then refuse or skip
    /// rather than trap.
    fn cast(ctx: ?*anyopaque) ?*PeerServer {
        const raw = ctx orelse return null;
        return @ptrCast(@alignCast(raw));
    }

    fn onSessionAccepted(ctx: ?*anyopaque, _: *Server, transport: *ServerSession) anyerror!void {
        const self = cast(ctx) orelse return error.MissingPeerServerContext;
        try self.sessions.ensureUnusedCapacity(self.allocator, 1);

        const session = try self.allocator.create(Session);
        errdefer self.allocator.destroy(session);
        session.* = .{
            .peer = undefined,
            .embargo_rng = undefined,
            .owner = self,
            .id = transport.id,
            .peer_addr = transport.peerAddress(),
            .transport = transport,
        };

        session.peer = Peer.init(self.allocator, transport);
        errdefer {
            _ = session.peer.takeAttachedConnection(*ServerSession);
            session.peer.deinit();
            // `Peer.init` pointed the transport's deadline tick at this peer.
            transport.on_tick = null;
        }
        session.peer.setLimits(self.options.limits);
        session.peer.setClockIo(self.io);
        // FAIL CLOSED on missing OS entropy, as the TCP sessions do: the
        // session is rejected rather than given a guessable embargo source.
        session.embargo_rng = peer_mod.seedEntropyCsprng(self.io) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.EntropyUnavailable => {
                std.log.scoped(.rpc_quic_server).warn("OS entropy unavailable; rejecting session", .{});
                return error.Unexpected;
            },
        };
        session.peer.setEntropySource(peer_mod.EntropySource.fromCsprng(&session.embargo_rng));
        session.peer.setTimeouts(.{
            .default_call_timeout_ms = self.options.default_call_timeout_ms,
            .shutdown_drain_timeout_ms = self.options.shutdown_drain_timeout_ms,
            .join_timeout_ms = self.options.join_timeout_ms,
        });
        if (self.options.observer) |obs| session.peer.setObserver(obs);

        try self.options.on_accept(self.options.ctx, session);

        self.sessions.appendAssumeCapacity(session);
        session.peer.start(session, onPeerError, onPeerClose);
    }

    fn afterStep(ctx: ?*anyopaque) void {
        const self = cast(ctx) orelse return;
        self.reapClosedSessions();
    }

    /// Free every session whose transport the server has destroyed. Not at
    /// close-callback time: a closing transport is still stepped while its
    /// QUIC connection drains, and can still reach its peer then.
    ///
    /// This runs after every step, so it must not cost O(sessions) per step,
    /// let alone O(sessions^2) through `sessionById` (a linear scan). The
    /// server destroys a transport only after firing its close callback,
    /// which sets `transport = null` and counts the session in
    /// `closed_pending`, so with that count at zero nothing can be gone.
    /// Otherwise one pass collects the server's live ids into a set: O(server
    /// sessions + our sessions) however many closed at once.
    fn reapClosedSessions(self: *PeerServer) void {
        if (self.closed_pending == 0) return;
        self.reap_passes += 1;
        // On OOM fall back to per-session lookups: slower, but it still
        // frees the peers, which is what relieves the memory pressure.
        var live: ?*const std.AutoHashMapUnmanaged(u64, void) = &self.live_ids;
        self.collectLiveIds() catch {
            live = null;
        };
        var index: usize = 0;
        while (index < self.sessions.items.len) {
            const session = self.sessions.items[index];
            const transport_alive = if (live) |ids|
                ids.contains(session.id)
            else
                session.transport != null or self.server.sessionById(session.id) != null;
            if (transport_alive) {
                index += 1;
                continue;
            }
            _ = self.sessions.swapRemove(index);
            if (session.transport == null) self.closed_pending -= 1;
            self.destroySession(session);
        }
    }

    fn collectLiveIds(self: *PeerServer) std.mem.Allocator.Error!void {
        self.live_ids.clearRetainingCapacity();
        const count = self.server.sessionCount();
        try self.live_ids.ensureTotalCapacity(self.allocator, @intCast(count));
        for (0..count) |index| {
            const transport = self.server.sessionAt(index) orelse break;
            self.live_ids.putAssumeCapacity(transport.id, {});
        }
    }

    fn destroySession(self: *PeerServer, session: *Session) void {
        // A no-op once the close callback detached the peer; kept for the
        // same reason as in the TCP sessions: releases must never be written
        // into a dead transport.
        _ = session.peer.takeAttachedConnection(*ServerSession);
        session.peer.deinit();
        self.allocator.destroy(session);
    }

    fn onPeerError(ctx: ?*anyopaque, peer: *Peer, err: anyerror) void {
        // A peer error means the transport is done; close it so the session
        // drains instead of leaving every consumer to do it.
        if (!peer.isAttachedTransportClosing()) peer.closeAttachedTransport();
        const raw = ctx orelse return;
        const session: *Session = @ptrCast(@alignCast(raw));
        const opts = session.owner.options;
        if (opts.on_error) |cb| cb(opts.ctx, session, err);
    }

    fn onPeerClose(ctx: ?*anyopaque, _: *Peer) void {
        const raw = ctx orelse return;
        const session: *Session = @ptrCast(@alignCast(raw));
        // The peer reports a transport close once; the guard keeps the
        // pending count exact even if that ever changed.
        if (session.transport != null) {
            session.transport = null;
            session.owner.closed_pending += 1;
        }
        const opts = session.owner.options;
        if (opts.on_close) |cb| cb(opts.ctx, session);
    }
};
