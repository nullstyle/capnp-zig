const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_worker_pool);
const Connection = @import("../transport/tcp/connection.zig").Connection;
const Listener = @import("../transport/tcp/runtime.zig").Listener;
const runtime_helpers = @import("../transport/tcp/runtime.zig");
const Runtime = @import("../transport/tcp/runtime.zig").Runtime;
const peer_mod = @import("../peer/mod.zig");
const Peer = peer_mod.Peer;
const net = std.Io.net;

/// A multi-threaded worker pool that accepts connections from a single
/// shared listen socket. All worker threads call `accept()` on the same
/// fd; the kernel wakes one thread per incoming connection.
///
/// Each accepted connection is handled on the worker thread that accepted
/// it. The worker blocks in `Connection.run()` for the lifetime of the
/// connection, then loops back to accept the next one.
///
/// The user-provided `AcceptFn` callback fires on the worker thread when a
/// connection is accepted. WorkerPool owns the accepted peer/connection; the
/// callback configures it and returns whether to run or reject it.
pub const WorkerPool = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    workers: []Worker,
    server: net.Server,
    ctx: *anyopaque,
    on_accept: AcceptFn,
    conn_options: Connection.Options,
    peer_limits: peer_mod.PeerLimits,
    join_timeout_ms: ?u64,
    active_connections: []?*Connection,
    active_mu: std.Io.Mutex,
    run_active: std.atomic.Value(bool),
    should_stop: std.atomic.Value(bool),
    fd_closed: std.atomic.Value(bool),
    /// Number of workers currently parked inside `listener.accept()`. The
    /// teardown path must drive this to zero before closing the listen
    /// socket — see `stopAccepting` for why that order is load-bearing on
    /// Windows.
    acceptors_parked: std.atomic.Value(u32),

    pub const Config = struct {
        concurrency: ?u32 = null,
        listen_backlog: u31 = 128,
        connection_options: Connection.Options = .{},
        peer_limits: peer_mod.PeerLimits = .{},
        /// Secure default for inbound L4 Join phases. Null is the explicit
        /// compatibility opt-out.
        join_timeout_ms: ?u64 = 30_000,
    };

    /// Result returned by `AcceptFn`.
    pub const AcceptDecision = enum {
        /// The callback configured the peer and the worker should enter
        /// `Connection.run()`.
        accept,
        /// Close and clean up the accepted peer/connection without running it.
        reject,
    };

    /// Called on worker thread when a connection is accepted. WorkerPool owns
    /// the `Peer` and `Connection` for every outcome; the callback must not
    /// deinit or destroy either pointer. On `.accept`, the callback should
    /// configure the peer and normally call `peer.start()`. WorkerPool cleans
    /// both objects after `Connection.run()` returns. On `.reject` or error,
    /// WorkerPool closes and frees both objects without running the connection.
    pub const AcceptFn = *const fn (
        ctx: *anyopaque,
        peer: *Peer,
        conn: *Connection,
        worker_index: u32,
    ) anyerror!AcceptDecision;

    const Worker = struct {
        thread: ?std.Thread = null,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        addr: net.IpAddress,
        ctx: *anyopaque,
        on_accept: AcceptFn,
        config: Config,
    ) !WorkerPool {
        const concurrency: u32 = config.concurrency orelse @intCast(std.Thread.getCpuCount() catch 1);
        if (concurrency == 0) return error.InvalidConcurrency;

        const server = try runtime_helpers.createListenSocket(io, addr, config.listen_backlog, false);
        errdefer runtime_helpers.closeFd(io, .{ .handle = server.socket.handle });

        const workers = try allocator.alloc(Worker, concurrency);
        errdefer allocator.free(workers);

        for (workers) |*w| {
            w.* = .{};
        }

        const active_connections = try allocator.alloc(?*Connection, concurrency);
        errdefer allocator.free(active_connections);
        @memset(active_connections, null);

        var connection_options = config.connection_options;
        if (config.join_timeout_ms != null and connection_options.tick_interval_ms == null) {
            connection_options.tick_interval_ms = 100;
        }

        return .{
            .allocator = allocator,
            .io = io,
            .workers = workers,
            .server = server,
            .ctx = ctx,
            .on_accept = on_accept,
            .conn_options = connection_options,
            .peer_limits = config.peer_limits,
            .join_timeout_ms = config.join_timeout_ms,
            .active_connections = active_connections,
            .active_mu = .init,
            .run_active = std.atomic.Value(bool).init(false),
            .should_stop = std.atomic.Value(bool).init(false),
            .fd_closed = std.atomic.Value(bool).init(false),
            .acceptors_parked = std.atomic.Value(u32).init(0),
        };
    }

    /// Blocks until shutdown. Spawns N-1 threads; the calling thread runs
    /// worker 0.
    pub fn run(self: *WorkerPool) !void {
        if (self.run_active.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) {
            return error.WorkerPoolAlreadyRunning;
        }
        defer self.run_active.store(false, .release);

        var spawned: usize = 0;
        errdefer {
            self.shutdown();
            for (self.workers[1..][0..spawned]) |*w| {
                if (w.thread) |t| t.join();
                w.thread = null;
            }
        }

        for (self.workers[1..]) |*w| {
            w.thread = try std.Thread.spawn(.{}, workerMain, .{ self, @as(u32, @intCast(1 + spawned)) });
            spawned += 1;
        }

        // Run worker 0 on the calling thread.
        workerMain(self, 0);

        // After worker 0 returns, join all others.
        for (self.workers[1..]) |*w| {
            if (w.thread) |t| t.join();
            w.thread = null;
        }
    }

    /// Signal all workers to stop. Closes the listen socket to unblock
    /// any threads blocked in `accept()`, and requests close on every
    /// connection currently running on a worker. This is an abrupt pool
    /// shutdown: callbacks still receive normal peer/connection close
    /// notifications, but active transports are not drained gracefully.
    pub fn shutdown(self: *WorkerPool) void {
        self.stopAccepting();
        self.closeActiveConnections();
    }

    /// Graceful pool shutdown: stop accepting immediately, then give the
    /// connections currently running on workers up to `drain_ms` to finish
    /// on their own before requesting close on the stragglers. Safe to call
    /// from any thread; like `shutdown()`, pair it with `run()` returning
    /// (or `deinit()`) to join workers.
    pub fn shutdownGraceful(self: *WorkerPool, drain_ms: u64) void {
        self.stopAccepting();
        const deadline = nowNs(self.io) + @as(i64, @intCast(drain_ms)) * std.time.ns_per_ms;
        while (nowNs(self.io) < deadline) {
            if (!self.hasActiveConnections()) return;
            sleepMs(self.io, drain_poll_interval_ms);
        }
        self.closeActiveConnections();
    }

    const drain_poll_interval_ms: u64 = 10;

    /// Safety valve for `stopAccepting`'s unpark wait. Generous: on the
    /// happy path the wait ends in single-digit milliseconds.
    const accept_unpark_timeout_ms: i64 = 2_000;

    /// Stop accepting: signal, pop every parked accept, and close the
    /// listen socket only once no worker is parked on it.
    ///
    /// THE ORDER IS LOAD-BEARING ON WINDOWS. Closing the handle while a
    /// worker is parked in the AFD listen wait completes that wait with
    /// STATUS_CANCELLED, which std's netAcceptWindows treats as
    /// `unreachable` — a process abort (first seen as the Nightly
    /// 64-worker soak teardown panic, run 33029339794; the old
    /// nudge-once-then-close order also gave up all remaining nudges on
    /// the first failed dial). So: wake parked accepts first (POSIX:
    /// shutdown() on the fd; everywhere: self-connect nudges, retried),
    /// wait for the parked count to hit zero, then close. The deadline is
    /// a safety valve — if nudges persistently fail (e.g. a firewall
    /// blocking self-connects) we close anyway, accepting the pre-fix
    /// risk rather than hanging shutdown forever.
    fn stopAccepting(self: *WorkerPool) void {
        self.should_stop.store(true, .release);
        if (comptime builtin.target.os.tag != .windows) {
            // POSIX: shutting the listener down pops threads parked in
            // accept(). Windows AFD rejects shutdown on a listening socket
            // (noisy INVALID_PARAMETER), so it relies on the nudges alone.
            if (!self.fd_closed.load(.acquire)) {
                self.io.vtable.netShutdown(self.io.userdata, self.server.socket.handle, .both) catch {};
            }
        }
        self.nudgeAcceptors();
        const deadline = nowNs(self.io) + accept_unpark_timeout_ms * std.time.ns_per_ms;
        while (self.acceptors_parked.load(.acquire) != 0 and nowNs(self.io) < deadline) {
            self.nudgeAcceptors();
            sleepMs(self.io, drain_poll_interval_ms);
        }
        if (!self.fd_closed.swap(true, .acq_rel)) {
            runtime_helpers.closeFd(self.io, .{ .handle = self.server.socket.handle });
        }
    }

    fn hasActiveConnections(self: *WorkerPool) bool {
        self.active_mu.lockUncancelable(self.io);
        defer self.active_mu.unlock(self.io);
        for (self.active_connections) |maybe_conn| {
            if (maybe_conn != null) return true;
        }
        return false;
    }

    pub fn deinit(self: *WorkerPool) void {
        self.shutdown();
        while (self.run_active.load(.acquire)) {
            std.Thread.yield() catch {};
        }
        // Join any worker threads that are still running. Normally run()
        // joins all threads before returning, but deinit must be safe if
        // called after shutdown() without a completed run(); shutdown()
        // requests close on active connections so these joins do not wait
        // for clients to disconnect on their own.
        for (self.workers) |*w| {
            if (w.thread) |t| {
                t.join();
                w.thread = null;
            }
        }
        self.allocator.free(self.active_connections);
        self.allocator.free(self.workers);
    }

    /// Short fixed backoff after an accept() error, so a persistent failure
    /// (e.g. fd exhaustion) cannot spin a worker at 100% CPU. Long enough to
    /// keep the loop off a core, short enough not to delay recovery.
    fn backoffAfterAcceptError(pool: *WorkerPool) void {
        const backoff: std.Io.Clock.Duration = .{
            .raw = .{ .nanoseconds = 20 * std.time.ns_per_ms },
            .clock = .awake,
        };
        backoff.sleep(pool.io) catch {};
    }

    fn workerMain(pool: *WorkerPool, worker_index: u32) void {
        var listener = Listener.initFd(
            pool.allocator,
            pool.io,
            .{ .handle = pool.server.socket.handle },
            pool.conn_options,
        );

        // One CSPRNG per worker THREAD, living on this frame. Thread-confined
        // by construction -- nothing outside this worker can reach it -- so it
        // needs no lock and imposes no thread-safety requirement on
        // DefaultCsprng. Handing every accepted peer a pointer to it is sound
        // because each peer is destroyed by `destroyAccepted` inside the same
        // loop iteration that created it, so no peer outlives this frame.
        //
        // Without this, `entropy` stayed null on every pool peer and
        // `nextAcceptEmbargoId` fell back to a COUNTER, where the spec wants
        // unguessable accept-embargo ids.
        var rng: ?std.Random.DefaultCsprng = peer_mod.seedEntropyCsprng(pool.io) catch |err| blk: {
            log.err("worker {}: secure entropy unavailable ({}); connections will be refused", .{ worker_index, err });
            break :blk null;
        };

        while (!pool.should_stop.load(.acquire)) {
            const accepted = blk: {
                _ = pool.acceptors_parked.fetchAdd(1, .acq_rel);
                defer _ = pool.acceptors_parked.fetchSub(1, .release);
                break :blk listener.accept();
            };
            const conn_ptr = accepted catch |err| {
                if (pool.should_stop.load(.acquire)) break;
                // Back off before retrying: a persistent accept failure (fd
                // exhaustion — EMFILE/ENFILE under a connection flood, each
                // connection also costing a read buffer and a writer thread)
                // would otherwise spin this worker at 100% CPU across every
                // core, starving the very connections whose close would free
                // the fds.
                log.debug("worker {}: accept failed: {}, backing off", .{ worker_index, err });
                pool.backoffAfterAcceptError();
                continue;
            };

            // A shutdown nudge (see nudgeAcceptors) pops blocked accepts
            // with a throwaway connection; discard it and exit.
            if (pool.should_stop.load(.acquire)) {
                destroyConnection(pool.allocator, conn_ptr);
                break;
            }

            const peer_ptr = pool.allocator.create(Peer) catch {
                destroyConnection(pool.allocator, conn_ptr);
                continue;
            };

            peer_ptr.* = Peer.init(pool.allocator, conn_ptr);
            peer_ptr.setLimits(pool.peer_limits);
            peer_ptr.setClockIo(pool.io);
            peer_ptr.setTimeouts(.{ .join_timeout_ms = pool.join_timeout_ms });

            // Fail closed, the way ServerSession refuses to construct without
            // secure entropy -- rejecting the connection is the closest
            // analogue available to a worker loop that returns void. Retry the
            // seed here rather than poisoning the worker for its whole life on
            // one transient `randomSecure` failure.
            if (rng == null) {
                rng = peer_mod.seedEntropyCsprng(pool.io) catch null;
            }
            if (rng) |*seeded| {
                peer_ptr.setEntropySource(peer_mod.EntropySource.fromCsprng(seeded));
            } else {
                log.err("worker {}: refusing connection, secure entropy unavailable", .{worker_index});
                destroyAccepted(pool.allocator, peer_ptr, conn_ptr);
                continue;
            }

            const decision = pool.on_accept(pool.ctx, peer_ptr, conn_ptr, worker_index) catch |err| {
                log.debug("worker {}: on_accept failed: {}", .{ worker_index, err });
                destroyAccepted(pool.allocator, peer_ptr, conn_ptr);
                continue;
            };

            if (decision == .reject) {
                destroyAccepted(pool.allocator, peer_ptr, conn_ptr);
                continue;
            }

            // Run the connection's blocking read loop.
            // This returns when the connection closes or errors.
            pool.setActiveConnection(worker_index, conn_ptr);
            conn_ptr.run();
            pool.clearActiveConnection(worker_index, conn_ptr);
            destroyAccepted(pool.allocator, peer_ptr, conn_ptr);
        }
    }

    fn setActiveConnection(self: *WorkerPool, worker_index: u32, conn: *Connection) void {
        self.active_mu.lockUncancelable(self.io);
        defer self.active_mu.unlock(self.io);
        self.active_connections[@intCast(worker_index)] = conn;
        if (self.should_stop.load(.acquire)) {
            conn.requestClose();
        }
    }

    fn clearActiveConnection(self: *WorkerPool, worker_index: u32, conn: *Connection) void {
        self.active_mu.lockUncancelable(self.io);
        defer self.active_mu.unlock(self.io);
        const index: usize = @intCast(worker_index);
        if (self.active_connections[index] == conn) {
            self.active_connections[index] = null;
        }
    }

    fn closeActiveConnections(self: *WorkerPool) void {
        self.active_mu.lockUncancelable(self.io);
        defer self.active_mu.unlock(self.io);
        for (self.active_connections) |maybe_conn| {
            if (maybe_conn) |conn| conn.requestClose();
        }
    }

    fn destroyAccepted(allocator: std.mem.Allocator, peer: *Peer, conn: *Connection) void {
        _ = peer.takeAttachedConnection(*Connection);
        peer.deinit();
        allocator.destroy(peer);
        destroyConnection(allocator, conn);
    }

    fn destroyConnection(allocator: std.mem.Allocator, conn: *Connection) void {
        conn.deinit();
        allocator.destroy(conn);
    }

    /// Pop every worker that may be blocked in accept() by dialing the
    /// listener once per parked worker with a throwaway loopback connection.
    /// Portable: unlike the POSIX shutdown(2)-on-listener trick, this
    /// works on Windows AFD sockets too. Errors are ignored — if the
    /// listener is already closed or unreachable the workers are not
    /// blocked on it.
    fn nudgeAcceptors(self: *WorkerPool) void {
        var addr = self.server.socket.address;
        switch (addr) {
            .ip4 => |*a| {
                if (std.mem.allEqual(u8, &a.bytes, 0)) a.bytes = .{ 127, 0, 0, 1 };
            },
            .ip6 => |*a| {
                if (std.mem.allEqual(u8, &a.bytes, 0)) {
                    a.bytes = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 };
                }
            },
        }
        // One successful dial pops one parked accept. `catch continue`, not
        // `catch break`: on a loaded host individual dials fail transiently,
        // and giving up on the first failure left every remaining worker
        // parked — which is exactly the state that made the Windows close
        // panic reachable. `stopAccepting` re-invokes this until the parked
        // count reaches zero or its deadline expires.
        var remaining = self.acceptors_parked.load(.acquire);
        while (remaining > 0) : (remaining -= 1) {
            const stream = net.IpAddress.connect(&addr, self.io, .{
                .mode = .stream,
                .protocol = .tcp,
            }) catch continue;
            runtime_helpers.closeFd(self.io, .{ .handle = stream.socket.handle });
        }
    }
};

fn nowNs(io: std.Io) i64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn sleepMs(io: std.Io, ms: u64) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}
