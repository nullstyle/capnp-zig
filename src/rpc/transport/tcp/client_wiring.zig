//! Builds a `ClientSession` around a stream socket that is already
//! connected. `tcp.connect` (TCP) and `rpc.transport.unix.connect`
//! (AF_UNIX) both end here, so the two transports wire a session the same
//! way.
//!
//! Internal: no module root exports this file, so nothing in it is public
//! API (it is not in the API snapshots).

const std = @import("std");

const client = @import("./client.zig");
const connection_mod = @import("./connection.zig");
const runtime = @import("./runtime.zig");
const peer_mod = @import("../../peer/mod.zig");
const fd_passing_mod = @import("../fd_passing.zig");

const ClientSession = client.ClientSession;
const ConnectOptions = client.ConnectOptions;
const Connection = connection_mod.Connection;
const Peer = peer_mod.Peer;

/// Every way `wire` fails. `tcp.connect`'s frozen error set is the TCP
/// connect errors plus exactly these, so this set must not grow.
pub const Error = error{ OutOfMemory, Canceled, Unexpected };

/// Wire + `peer.start`. Takes ownership of `socket`: on success the
/// session's connection closes it, and on error it is closed before this
/// returns. On return the session is live: bootstrap and calls are legal
/// immediately (writes enqueue; nothing reads the socket until `run()`).
pub fn wire(
    gpa: std.mem.Allocator,
    io: std.Io,
    socket: runtime.SocketFd,
    options: ConnectOptions,
) Error!*ClientSession {
    return wireWithFdPassing(gpa, io, socket, options, .{});
}

/// `wire`, with fd passing configured before anything reads the socket
/// (`rpc.transport.unix.connect`). `fd_passing.max_fds_per_message = 0` (the
/// default) leaves the connection in drain mode.
pub fn wireWithFdPassing(
    gpa: std.mem.Allocator,
    io: std.Io,
    socket: runtime.SocketFd,
    options: ConnectOptions,
    fd_passing: fd_passing_mod.FdPassing,
) Error!*ClientSession {
    var socket_owned = true;
    errdefer if (socket_owned) runtime.closeFd(io, socket);

    const self = try gpa.create(ClientSession);
    errdefer gpa.destroy(self);

    var conn_opts = options.conn;
    if ((options.default_call_timeout_ms != null or options.join_timeout_ms != null) and
        conn_opts.tick_interval_ms == null)
    {
        conn_opts.tick_interval_ms = 100;
    }
    if (conn_opts.observer == null) conn_opts.observer = options.observer;

    self.allocator = gpa;
    self.io = io;
    self.user_ctx = options.ctx;
    self.user_on_error = options.on_error;
    self.user_on_close = options.on_close;

    self.conn = try Connection.init(gpa, io, socket, conn_opts);
    socket_owned = false; // conn.deinit() closes the socket from here on
    errdefer self.conn.deinit();
    try enableFdPassing(&self.conn, fd_passing);

    self.peer = Peer.init(gpa, &self.conn);
    self.peer.setLimits(options.limits);
    self.peer.setMaxLiveImportedFds(fd_passing.max_live_imported_fds);
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
        .join_timeout_ms = options.join_timeout_ms,
    });
    if (options.observer) |obs| self.peer.setObserver(obs);
    self.peer.start(self, onPeerError, onPeerClose);
    return self;
}

/// Turn fd passing on for a fresh connection, before its first read, mapped
/// into `Error` (the frozen session error sets must not grow). Only an
/// AF_UNIX connection on Linux or macOS can do it; anything else is a
/// caller bug, reported as `Unexpected`.
pub fn enableFdPassing(conn: *Connection, fd_passing: fd_passing_mod.FdPassing) Error!void {
    if (fd_passing.max_fds_per_message == 0) return;
    conn.enableFdPassing(fd_passing.max_fds_per_message) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        error.FdPassingUnsupported, error.AlreadyReading => {
            std.log.scoped(.rpc_tcp).warn("fd passing cannot be enabled on this connection: {t}", .{err});
            return error.Unexpected;
        },
    };
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
