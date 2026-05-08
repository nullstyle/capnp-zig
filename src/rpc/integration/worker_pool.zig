const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_worker_pool);
const Connection = @import("../transport/tcp/connection.zig").Connection;
const Listener = @import("../transport/tcp/runtime.zig").Listener;
const runtime_helpers = @import("../transport/tcp/runtime.zig");
const Runtime = @import("../transport/tcp/runtime.zig").Runtime;
const Peer = @import("../peer/mod.zig").Peer;
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
    active_connections: []?*Connection,
    active_mu: std.Io.Mutex,
    run_active: std.atomic.Value(bool),
    should_stop: std.atomic.Value(bool),
    fd_closed: std.atomic.Value(bool),

    pub const Config = struct {
        concurrency: ?u32 = null,
        listen_backlog: u31 = 128,
        connection_options: Connection.Options = .{},
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
        errdefer runtime_helpers.closeFd(io, server.socket.handle);

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
            .conn_options = config.connection_options,
            .active_connections = active_connections,
            .active_mu = .init,
            .run_active = std.atomic.Value(bool).init(false),
            .should_stop = std.atomic.Value(bool).init(false),
            .fd_closed = std.atomic.Value(bool).init(false),
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
        self.should_stop.store(true, .release);
        if (!self.fd_closed.swap(true, .acq_rel)) {
            closeListenFd(self.io, self.server.socket.handle);
        }
        self.closeActiveConnections();
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

    fn workerMain(pool: *WorkerPool, worker_index: u32) void {
        var listener = Listener.initFd(
            pool.allocator,
            pool.io,
            pool.server.socket.handle,
            pool.conn_options,
        );

        while (!pool.should_stop.load(.acquire)) {
            const conn_ptr = listener.accept() catch |err| {
                if (pool.should_stop.load(.acquire)) break;
                log.debug("worker {}: accept failed: {}", .{ worker_index, err });
                continue;
            };

            if (pool.should_stop.load(.acquire)) {
                destroyConnection(pool.allocator, conn_ptr);
                break;
            }

            const peer_ptr = pool.allocator.create(Peer) catch {
                destroyConnection(pool.allocator, conn_ptr);
                continue;
            };

            peer_ptr.* = Peer.init(pool.allocator, conn_ptr);

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

    fn closeListenFd(io: std.Io, fd: net.Socket.Handle) void {
        io.vtable.netShutdown(io.userdata, fd, .both) catch {};
        runtime_helpers.closeFd(io, fd);
    }
};
