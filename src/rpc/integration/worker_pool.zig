const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_worker_pool);
const Connection = @import("../transport/tcp/connection.zig").Connection;
const Listener = @import("../transport/tcp/runtime.zig").Listener;
const runtime_helpers = @import("../transport/tcp/runtime.zig");
const Runtime = @import("../transport/tcp/runtime.zig").Runtime;
const events = @import("../events.zig");
const wake_lock = @import("../transport/wake_lock.zig");
const client_wiring = @import("../transport/tcp/client_wiring.zig");
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
///
/// `init` binds a TCP address. `initListener` (Experimental, Linux and
/// Darwin) serves a `tcp.Listener` the caller already has, such as one from
/// `rpc.transport.unix.listen`; see its doc for how its workers wait.
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
    /// Applied to every accepted `Connection` before `on_accept` runs (see
    /// `Config.first_frame_timeout_ms`).
    first_frame_timeout_ms: ?u64,
    active_connections: []?*Connection,
    active_mu: std.Io.Mutex,
    run_active: std.atomic.Value(bool),
    should_stop: std.atomic.Value(bool),
    fd_closed: std.atomic.Value(bool),
    /// Number of workers currently parked inside `listener.accept()` (for
    /// `initListener`: waiting in `poll` on the listener and the wake door,
    /// or in the non-blocking `accept` that follows). The teardown path
    /// must drive this to zero before closing the listen socket — see
    /// `stopAccepting` for why that order is load-bearing on Windows.
    acceptors_parked: std.atomic.Value(u32),
    /// The listener `initListener` took; null for `init`. The pool owns it
    /// and closes it with `Listener.close` once no worker waits on it, which
    /// for a listener from `rpc.transport.unix.listen` also removes its
    /// socket file and releases its lock. Experimental.
    listener: ?Listener = null,
    /// `initListener` only: the wake door, a non-blocking close-on-exec pipe
    /// (`[0]` read end, `[1]` write end). Workers wait in `poll` on it and
    /// on the listener. Shutdown writes one byte and nothing ever reads it,
    /// so every worker that polls from then on wakes at once. `deinit`
    /// closes it. Experimental.
    park_door: ?[2]i32 = null,

    pub const Config = struct {
        concurrency: ?u32 = null,
        listen_backlog: u31 = 128,
        connection_options: Connection.Options = .{},
        peer_limits: peer_mod.PeerLimits = .{},
        /// Secure default for inbound L4 Join phases. Null is the explicit
        /// compatibility opt-out.
        join_timeout_ms: ?u64 = 30_000,
        /// Secure default: reap an accepted connection that has not
        /// delivered one complete frame within this long of its accept
        /// (`Connection.first_frame_timeout_ms`). Each worker serves one
        /// connection to completion, so without it `concurrency` clients
        /// that connect and never speak pin every worker forever, and every
        /// later client waits in the kernel backlog. Before the first frame,
        /// a client trickling the bytes of a frame it never finishes is
        /// reaped too. It is set on the connection before `on_accept` runs,
        /// so the callback may override it per connection (for example, a
        /// server that speaks first). Null is the explicit opt-out.
        first_frame_timeout_ms: ?u64 = default_first_frame_timeout_ms,
        /// Secure default: reap a connection after this long with no
        /// inbound read and no outbound enqueue (`Connection.idle_timeout_ms`),
        /// so a client that vanished without a FIN cannot hold its worker
        /// for good. Applies when `connection_options.idle_timeout_ms` is
        /// null; an explicit value there wins. Cap'n Proto has no keepalive,
        /// so clients that sit idle for longer than this get disconnected;
        /// raise it, or set null (the explicit opt-out), if yours do.
        /// Neither deadline stops a client that keeps sending: any inbound
        /// read refreshes the idle clock, even a lone byte of a frame that
        /// never completes, so such a client holds its worker indefinitely.
        /// Bound those with admission control in `on_accept`.
        idle_timeout_ms: ?u64 = default_idle_timeout_ms,
    };

    /// Default for `Config.first_frame_timeout_ms`. Matches the QUIC
    /// server's `handshake_timeout_ms` default.
    pub const default_first_frame_timeout_ms: u64 = 10_000;
    /// Default for `Config.idle_timeout_ms`.
    pub const default_idle_timeout_ms: u64 = 300_000;

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

        return .{
            .allocator = allocator,
            .io = io,
            .workers = workers,
            .server = server,
            .ctx = ctx,
            .on_accept = on_accept,
            .conn_options = poolConnectionOptions(config),
            .peer_limits = config.peer_limits,
            .join_timeout_ms = config.join_timeout_ms,
            .first_frame_timeout_ms = config.first_frame_timeout_ms,
            .active_connections = active_connections,
            .active_mu = .init,
            .run_active = std.atomic.Value(bool).init(false),
            .should_stop = std.atomic.Value(bool).init(false),
            .fd_closed = std.atomic.Value(bool).init(false),
            .acceptors_parked = std.atomic.Value(u32).init(0),
        };
    }

    /// Every way `initListener` fails. On each of them the caller still
    /// owns `listener.*`, unchanged.
    pub const InitListenerError = error{
        /// `Config.concurrency` is 0.
        InvalidConcurrency,
        /// `listener` was already closed.
        ListenerClosed,
        OutOfMemory,
        /// The wake door's pipe hit the process fd limit.
        ProcessFdQuotaExceeded,
        /// The wake door's pipe hit the system fd limit.
        SystemFdQuotaExceeded,
        Unexpected,
        /// Not Linux or Darwin. Use `init` for TCP there.
        UnixSocketsUnsupported,
    };

    /// Serve connections from a listener the caller already has, such as
    /// one from `rpc.transport.unix.listen`. Experimental. Linux and Darwin
    /// only; elsewhere this returns `error.UnixSocketsUnsupported`.
    ///
    /// On success the pool takes the listener: it moves `listener.*` into
    /// the pool and marks the caller's copy closed, so `close` on that copy
    /// does nothing (a `defer listener.close()` left in place is harmless)
    /// and `accept` on it returns `error.ListenerClosed`. On error the
    /// caller still owns `listener.*`, unchanged. Shutdown (`shutdown`,
    /// `shutdownGraceful` or `deinit`) closes the pool's listener with
    /// `Listener.close` once no worker waits on it, so a socket file from
    /// `unix.listen` is removed and its lock released. That close never
    /// blocks on an AF_UNIX listener (see `Listener.close`), so shutdown
    /// does not wait for fds riding on connections nobody accepted. The
    /// pool runs on the listener's `std.Io` (`Listener.ioBackend`).
    /// `config.connection_options` applies to every connection; the
    /// listener's own `conn_options` and `config.listen_backlog` are not
    /// used. The listener's `fd_passing` (`unix.ListenOptions.fd_passing`)
    /// applies to every connection, as with `ServerSession.accept`.
    ///
    /// Like `Listener.accept`, the workers of a pool on an AF_UNIX listener
    /// (`unix.listen`, or `Listener.initFd` on an AF_UNIX socket) take no
    /// connection while the fd closer's `.socket` lane is full
    /// (`tcp.runtime.awaitSocketLane`, with one `.backpressure` event per
    /// wait to `config.connection_options.observer`): a peer that stalls a
    /// socket close and reconnects in a loop then waits in the kernel's
    /// backlog instead of growing this process's fds. Shutdown ends that
    /// wait too.
    ///
    /// How the workers wait: each one parks in `poll` on the listen socket
    /// and on a wake door (a pipe the pool owns), then makes a non-blocking
    /// `accept`. A worker that loses the race for a connection parks again.
    /// Shutdown writes the wake door, which wakes every parked worker. It
    /// never dials the listener, so it finishes even when the socket file
    /// was removed or another server now has the path. For this the pool
    /// makes the listen socket non-blocking, and accepts on it with raw
    /// syscalls: `std.Io`'s accept treats EAGAIN as a bug. An accepted
    /// socket is close-on-exec and blocking (Darwin's `accept` would copy
    /// O_NONBLOCK from the listener; the pool clears it).
    pub fn initListener(
        allocator: std.mem.Allocator,
        listener: *Listener,
        ctx: *anyopaque,
        on_accept: AcceptFn,
        config: Config,
    ) InitListenerError!WorkerPool {
        if (comptime !park_door_supported) return error.UnixSocketsUnsupported;
        const concurrency: u32 = config.concurrency orelse @intCast(std.Thread.getCpuCount() catch 1);
        if (concurrency == 0) return error.InvalidConcurrency;
        if (listener.close_requested.load(.acquire)) return error.ListenerClosed;

        const workers = try allocator.alloc(Worker, concurrency);
        errdefer allocator.free(workers);
        for (workers) |*w| {
            w.* = .{};
        }

        const active_connections = try allocator.alloc(?*Connection, concurrency);
        errdefer allocator.free(active_connections);
        @memset(active_connections, null);

        const door = try park.openDoor();
        errdefer park.closeDoor(door);

        // Last: nothing below can fail, so on any error above the caller's
        // listener is still blocking and still theirs.
        try park.setNonBlocking(listener.server.socket.handle, true);

        // Move it: the pool's copy is the live one, and the caller's is
        // marked closed. A leftover `close` on the caller's copy would
        // otherwise unlink the path, close the listen fd under the parked
        // workers, and close the lock fd's number a second time later.
        const owned = listener.*;
        listener.close_requested.store(true, .release);

        return .{
            .allocator = allocator,
            .io = owned.io,
            .workers = workers,
            .server = owned.server,
            .ctx = ctx,
            .on_accept = on_accept,
            .conn_options = poolConnectionOptions(config),
            .peer_limits = config.peer_limits,
            .join_timeout_ms = config.join_timeout_ms,
            .first_frame_timeout_ms = config.first_frame_timeout_ms,
            .active_connections = active_connections,
            .active_mu = .init,
            .run_active = std.atomic.Value(bool).init(false),
            .should_stop = std.atomic.Value(bool).init(false),
            .fd_closed = std.atomic.Value(bool).init(false),
            .acceptors_parked = std.atomic.Value(u32).init(0),
            .listener = owned,
            .park_door = door,
        };
    }

    fn poolConnectionOptions(config: Config) Connection.Options {
        var connection_options = config.connection_options;
        if (config.join_timeout_ms != null and connection_options.tick_interval_ms == null) {
            connection_options.tick_interval_ms = 100;
        }
        if (connection_options.idle_timeout_ms == null) {
            connection_options.idle_timeout_ms = config.idle_timeout_ms;
        }
        return connection_options;
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

    /// Signal all workers to stop. Wakes pending accepts before closing
    /// the listen socket, and requests close on every
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
    /// The drain bound begins after accepts stop. A permanently failed backend
    /// wake operation or blocked synchronous handler can still prevent return.
    pub fn shutdownGraceful(self: *WorkerPool, drain_ms: u64) void {
        self.stopAccepting();
        const drain_ns = std.math.cast(i64, @as(u128, drain_ms) * std.time.ns_per_ms) orelse std.math.maxInt(i64);
        const deadline = nowNs(self.io) +| drain_ns;
        while (nowNs(self.io) < deadline) {
            if (!self.hasActiveConnections()) return;
            sleepMs(self.io, drain_poll_interval_ms);
        }
        self.closeActiveConnections();
    }

    const drain_poll_interval_ms: u64 = 10;

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
    /// wait for the parked count to hit zero, then close. A timeout cannot
    /// make closing under a pending accept safe. Persistent loopback failures
    /// can delay shutdown; transient failures are retried without canceling
    /// the kernel operation by closing its handle.
    ///
    /// `initListener` pools neither shut the socket down nor dial it. Their
    /// workers wait in `poll` on the wake door too, so one byte written to
    /// the door wakes them all. Neither of the others would do: `shutdown`
    /// does not wake an AF_UNIX accept on macOS, and a dial to a socket file
    /// that was removed or rebound never reaches this listener, so the wait
    /// below would never end.
    fn stopAccepting(self: *WorkerPool) void {
        // Registering an accept and checking should_stop use this same lock.
        // After this transition the parked count can only decrease.
        self.active_mu.lockUncancelable(self.io);
        self.should_stop.store(true, .release);
        if (self.fd_closed.load(.acquire)) {
            self.active_mu.unlock(self.io);
            return;
        }
        if (self.park_door) |door| {
            // Under the lock, so `deinit` cannot have closed the door yet.
            park.signalDoor(door);
        } else if (comptime builtin.target.os.tag != .windows) {
            // POSIX: shutting the listener down pops threads parked in
            // accept(). Windows AFD rejects shutdown on a listening socket
            // (noisy INVALID_PARAMETER), so it relies on the nudges alone.
            self.io.vtable.netShutdown(self.io.userdata, self.server.socket.handle, .both) catch {};
        }
        self.active_mu.unlock(self.io);
        while (self.acceptors_parked.load(.acquire) != 0) {
            if (self.park_door == null) self.nudgeAcceptors();
            if (self.acceptors_parked.load(.acquire) != 0) sleepMs(self.io, drain_poll_interval_ms);
        }
        // The close runs under the lock, so a second `stopAccepting` (say,
        // `deinit` on another thread) cannot return, and free the pool,
        // while this one is still closing. That is only sound because the
        // close is short: `Listener.close` never does an AF_UNIX listener's
        // final close here, where the kernel would close the fds riding on
        // connections still in the backlog (a lingering one blocks).
        self.active_mu.lockUncancelable(self.io);
        defer self.active_mu.unlock(self.io);
        if (!self.fd_closed.swap(true, .acq_rel)) {
            if (self.listener) |*listener| {
                listener.close();
            } else {
                runtime_helpers.closeFd(self.io, .{ .handle = self.server.socket.handle });
            }
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
        // No worker polls the door any more, and `shutdown` above marked the
        // listener closed, so no `stopAccepting` writes to it again.
        if (comptime park_door_supported) {
            if (self.park_door) |door| park.closeDoor(door);
        }
        self.park_door = null;
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

        while (true) {
            pool.active_mu.lockUncancelable(pool.io);
            if (pool.should_stop.load(.acquire)) {
                pool.active_mu.unlock(pool.io);
                break;
            }
            _ = pool.acceptors_parked.fetchAdd(1, .acq_rel);
            pool.active_mu.unlock(pool.io);
            const accepted = blk: {
                defer _ = pool.acceptors_parked.fetchSub(1, .release);
                break :blk pool.acceptNext(&listener);
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

            // Before on_accept, so the callback can override it per connection.
            conn_ptr.first_frame_timeout_ms = pool.first_frame_timeout_ms;

            const peer_ptr = pool.allocator.create(Peer) catch {
                destroyConnection(pool.allocator, conn_ptr);
                continue;
            };

            peer_ptr.* = Peer.init(pool.allocator, conn_ptr);
            peer_ptr.setLimits(pool.peer_limits);
            // The listener's fd passing limit, as `ServerSession.accept`
            // sets it (the default where fd passing is off). Only this
            // field is read: `stopAccepting` may be closing the listener.
            if (pool.listener) |*owned| peer_ptr.setMaxLiveImportedFds(owned.fd_passing.max_live_imported_fds);
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

    /// One accepted connection: from `Listener.accept` for `init`, from the
    /// wake-door wait for `initListener`.
    fn acceptNext(pool: *WorkerPool, listener: *Listener) !*Connection {
        if (comptime park_door_supported) {
            // `initListener` sets both, together.
            if (pool.park_door) |door| {
                if (pool.listener) |*owned| return pool.acceptParked(door, owned);
            }
        }
        return listener.accept();
    }

    /// Park in `poll` on the listener and the wake door until a connection
    /// is accepted (or the door says stop: `error.ListenerClosed`), then
    /// wrap it exactly as `Listener.accept` does.
    ///
    /// `owned` is the pool's own listener (`initListener`). The pool closes
    /// it only once no worker is parked here (`stopAccepting`), so reading
    /// it here never races that close.
    fn acceptParked(pool: *WorkerPool, door: [2]i32, owned: *const Listener) !*Connection {
        const listen_fd = pool.server.socket.handle;
        const fd = while (true) {
            switch (try park.wait(listen_fd, door[0])) {
                .stop => return error.ListenerClosed,
                .listener => {
                    // The accept gate of an AF_UNIX listener (`unix.listen`,
                    // or `Listener.initFd` on an AF_UNIX socket), as in
                    // `Listener.accept`: take nothing while the closer's
                    // `.socket` lane is full. Shutdown ends the wait.
                    if (owned.non_ip_socket) {
                        try runtime_helpers.awaitSocketLane(pool.conn_options.observer, &pool.should_stop);
                    }
                    // Null: another worker took the connection, or its
                    // client gave up first. Park again.
                    if (try park.accept(listen_fd)) |fd| break fd;
                },
            }
        };
        const conn_ptr = blk: {
            errdefer runtime_helpers.closeFd(pool.io, .{ .handle = fd });
            runtime_helpers.setTcpNoDelay(.{ .handle = fd });
            const created = try pool.allocator.create(Connection);
            errdefer pool.allocator.destroy(created);
            created.* = try Connection.init(pool.allocator, pool.io, .{ .handle = fd }, pool.conn_options);
            break :blk created;
        };
        events.emitConnection(pool.conn_options.observer, conn_ptr.transport.source, .server, .accepted);
        // Before anything reads, as `Listener.accept` and
        // `ServerSession.accept` do: fd passing starts at the stream's first
        // byte. The connection owns the socket now.
        client_wiring.enableFdPassing(conn_ptr, owned.fd_passing) catch |err| {
            destroyConnection(pool.allocator, conn_ptr);
            return err;
        };
        return conn_ptr;
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

    /// Wake one pending accept with a loopback connection, retaining the
    /// connection until a worker has returned from accept.
    /// Portable: unlike the POSIX shutdown(2)-on-listener trick, this
    /// works on Windows AFD sockets too. Failed dials are retried by the caller.
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
        const before = self.acceptors_parked.load(.acquire);
        if (before == 0) return;
        const stream = net.IpAddress.connect(&addr, self.io, .{
            .mode = .stream,
            .protocol = .tcp,
        }) catch return;
        defer runtime_helpers.closeFd(self.io, .{ .handle = stream.socket.handle });
        // A completed connect does not prove that the peer accepted it yet.
        // Closing immediately can discard the wake before Windows consumes it.
        // The registration lock makes this count monotonic during shutdown.
        while (self.acceptors_parked.load(.acquire) >= before) sleepMs(self.io, drain_poll_interval_ms);
    }
};

/// Where `initListener` works: Linux and Darwin, the targets of
/// `rpc.transport.unix`.
const park_door_supported: bool = builtin.target.os.tag == .linux or builtin.target.os.tag.isDarwin();

/// The raw syscalls behind `initListener`'s wait: the wake door, the
/// listener's non-blocking mode, `poll` and `accept`. Reached only where
/// `park_door_supported` holds.
///
/// Raw, not `std.Io`: the listen socket is non-blocking, and
/// `Io.Threaded`'s accept treats EAGAIN as a bug (it panics in Debug). The
/// wake door follows the wake-door contract of `wake_lock.zig`: both ends
/// non-blocking, and a raw write for which EAGAIN means "already signalled".
const park = struct {
    const posix = std.posix;
    const sys = posix.system;
    const is_linux = builtin.target.os.tag == .linux;
    const nonblock_bit: usize = 1 << @bitOffsetOf(posix.O, "NONBLOCK");

    const DoorError = error{ ProcessFdQuotaExceeded, SystemFdQuotaExceeded, Unexpected };

    fn unexpected(what: []const u8, err: posix.E) error{Unexpected} {
        // The number, never the tag: `posix.E` does not name every errno.
        log.debug("{s} failed: errno {d}", .{ what, @backingInt(err) });
        return error.Unexpected;
    }

    fn closeRaw(fd: i32) void {
        _ = sys.close(fd);
    }

    /// A pipe, both ends non-blocking and close-on-exec.
    fn openDoor() DoorError![2]i32 {
        var fds: [2]i32 = undefined;
        const rc = if (is_linux)
            sys.pipe2(&fds, .{ .CLOEXEC = true, .NONBLOCK = true })
        else
            sys.pipe(&fds);
        switch (posix.errno(rc)) {
            .SUCCESS => {},
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            else => |err| return unexpected("pipe", err),
        }
        if (!is_linux) {
            // Darwin has no pipe2.
            errdefer closeDoor(fds);
            for (fds) |fd| {
                try setCloexec(fd);
                try setNonBlocking(fd, true);
            }
        }
        return fds;
    }

    fn closeDoor(fds: [2]i32) void {
        closeRaw(fds[0]);
        closeRaw(fds[1]);
    }

    /// One byte into the door. Never blocks; EAGAIN means bytes are already
    /// there, which is the state this makes.
    fn signalDoor(fds: [2]i32) void {
        // Only `initListener` opens a door, and only where it is supported.
        if (comptime !park_door_supported) return;
        wake_lock.writeByte(fds[1]);
    }

    fn setCloexec(fd: i32) error{Unexpected}!void {
        while (true) {
            switch (posix.errno(sys.fcntl(fd, posix.F.SETFD, @as(usize, posix.FD_CLOEXEC)))) {
                .SUCCESS => return,
                .INTR => continue,
                else => |err| return unexpected("fcntl(F_SETFD)", err),
            }
        }
    }

    fn setNonBlocking(fd: i32, on: bool) error{Unexpected}!void {
        const get = sys.fcntl(fd, posix.F.GETFL, @as(usize, 0));
        switch (posix.errno(get)) {
            .SUCCESS => {},
            else => |err| return unexpected("fcntl(F_GETFL)", err),
        }
        const old: usize = @intCast(get);
        const new = if (on) old | nonblock_bit else old & ~nonblock_bit;
        if (new == old) return;
        switch (posix.errno(sys.fcntl(fd, posix.F.SETFL, new))) {
            .SUCCESS => {},
            else => |err| return unexpected("fcntl(F_SETFL)", err),
        }
    }

    const Ready = enum { stop, listener };

    /// Wait, without a bound, until the door or the listener is readable.
    /// The door wins a tie.
    fn wait(listen_fd: i32, door_fd: i32) error{ SystemResources, Unexpected }!Ready {
        var fds = [2]posix.pollfd{
            .{ .fd = door_fd, .events = posix.POLL.IN, .revents = 0 },
            .{ .fd = listen_fd, .events = posix.POLL.IN, .revents = 0 },
        };
        while (true) {
            const rc = sys.poll(&fds, fds.len, -1);
            switch (posix.errno(rc)) {
                .SUCCESS => {},
                .INTR => continue,
                .AGAIN, .NOMEM => return error.SystemResources,
                else => |err| return unexpected("poll", err),
            }
            // Any event on the door (data, or an error on a door that
            // should not have one) stops the worker.
            if (fds[0].revents != 0) return .stop;
            // Readable, or an error state that `accept` reports.
            if (fds[1].revents != 0) return .listener;
        }
    }

    const AcceptError = error{
        ProcessFdQuotaExceeded,
        SystemFdQuotaExceeded,
        SystemResources,
        Unexpected,
    };

    /// One non-blocking `accept`: the new socket, close-on-exec and
    /// blocking, or null when there is nothing to take now (another worker
    /// took it, or the client went away first).
    fn accept(listen_fd: i32) AcceptError!?i32 {
        const fd: i32 = while (true) {
            const rc = if (is_linux)
                sys.accept4(listen_fd, null, null, posix.SOCK.CLOEXEC)
            else
                sys.accept(listen_fd, null, null);
            switch (posix.errno(rc)) {
                .SUCCESS => break @intCast(rc),
                .INTR => continue,
                // Nothing pending any more, or the pending connection was
                // reset or refused before we took it. Park again.
                .AGAIN, .CONNABORTED, .PERM => return null,
                // Linux reports a TCP connection's pending network error
                // from accept; accept(2) says to treat these like EAGAIN.
                .NETDOWN, .PROTO, .NOPROTOOPT, .HOSTDOWN, .HOSTUNREACH, .OPNOTSUPP, .NETUNREACH => return null,
                .MFILE => return error.ProcessFdQuotaExceeded,
                .NFILE => return error.SystemFdQuotaExceeded,
                .NOBUFS, .NOMEM => return error.SystemResources,
                else => |err| return unexpected("accept", err),
            }
        };
        if (!is_linux) {
            // Darwin has no accept4, and its accept copies O_NONBLOCK from
            // the listener. The transport writes through `std.Io`, which
            // panics on EAGAIN, so the socket must block.
            errdefer closeRaw(fd);
            try setCloexec(fd);
            try setNonBlocking(fd, false);
        }
        return fd;
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
