const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_worker_pool);
const Connection = @import("../level2/connection.zig").Connection;
const Listener = @import("../level2/runtime.zig").Listener;
const runtime_helpers = @import("../level2/runtime.zig");
const Runtime = @import("../level2/runtime.zig").Runtime;
const Peer = @import("../level3/peer.zig").Peer;
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
/// connection is accepted; the user sets the bootstrap capability and starts
/// the peer inside the callback.
pub const WorkerPool = struct {
    allocator: std.mem.Allocator,
    workers: []Worker,
    listen_fd: std.posix.fd_t,
    ctx: *anyopaque,
    on_accept: AcceptFn,
    conn_options: Connection.Options,
    should_stop: std.atomic.Value(bool),
    fd_closed: std.atomic.Value(bool),

    pub const Config = struct {
        concurrency: ?u32 = null,
        listen_backlog: u31 = 128,
        connection_options: Connection.Options = .{},
    };

    /// Called on worker thread when a connection is accepted.
    /// User sets bootstrap capability and calls peer.start().
    pub const AcceptFn = *const fn (
        ctx: *anyopaque,
        peer: *Peer,
        conn: *Connection,
        worker_index: u32,
    ) void;

    const Worker = struct {
        thread: ?std.Thread = null,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        addr: net.IpAddress,
        ctx: *anyopaque,
        on_accept: AcceptFn,
        config: Config,
    ) !WorkerPool {
        const concurrency: u32 = config.concurrency orelse @intCast(std.Thread.getCpuCount() catch 1);
        if (concurrency == 0) return error.InvalidConcurrency;

        const listen_fd = try runtime_helpers.createListenSocket(addr, config.listen_backlog, false);
        errdefer runtime_helpers.closeFd(listen_fd);

        const workers = try allocator.alloc(Worker, concurrency);
        errdefer allocator.free(workers);

        for (workers) |*w| {
            w.* = .{};
        }

        return .{
            .allocator = allocator,
            .workers = workers,
            .listen_fd = listen_fd,
            .ctx = ctx,
            .on_accept = on_accept,
            .conn_options = config.connection_options,
            .should_stop = std.atomic.Value(bool).init(false),
            .fd_closed = std.atomic.Value(bool).init(false),
        };
    }

    /// Blocks until shutdown. Spawns N-1 threads; the calling thread runs
    /// worker 0.
    pub fn run(self: *WorkerPool) !void {
        var spawned: usize = 0;
        errdefer {
            self.should_stop.store(true, .release);
            if (!self.fd_closed.swap(true, .acq_rel)) {
                closeListenFd(self.listen_fd);
            }
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
    /// any threads blocked in `accept()`.
    pub fn shutdown(self: *WorkerPool) void {
        self.should_stop.store(true, .release);
        if (!self.fd_closed.swap(true, .acq_rel)) {
            closeListenFd(self.listen_fd);
        }
    }

    pub fn deinit(self: *WorkerPool) void {
        if (!self.fd_closed.swap(true, .acq_rel)) {
            closeListenFd(self.listen_fd);
        }
        // Join any worker threads that are still running. Normally run()
        // joins all threads before returning, but deinit must be safe if
        // called after shutdown() without a completed run().
        for (self.workers) |*w| {
            if (w.thread) |t| {
                t.join();
                w.thread = null;
            }
        }
        self.allocator.free(self.workers);
    }

    fn workerMain(pool: *WorkerPool, worker_index: u32) void {
        var listener = Listener.initFd(
            pool.allocator,
            pool.listen_fd,
            pool.conn_options,
        );

        while (!pool.should_stop.load(.acquire)) {
            const conn_ptr = listener.accept() catch |err| {
                if (pool.should_stop.load(.acquire)) break;
                log.debug("worker {}: accept failed: {}", .{ worker_index, err });
                continue;
            };

            if (pool.should_stop.load(.acquire)) {
                conn_ptr.deinit();
                pool.allocator.destroy(conn_ptr);
                break;
            }

            const peer_ptr = pool.allocator.create(Peer) catch {
                conn_ptr.deinit();
                pool.allocator.destroy(conn_ptr);
                continue;
            };

            peer_ptr.* = Peer.init(pool.allocator, conn_ptr);

            pool.on_accept(pool.ctx, peer_ptr, conn_ptr, worker_index);

            // Run the connection's blocking read loop.
            // This returns when the connection closes or errors.
            conn_ptr.run();
        }
    }

    fn closeListenFd(fd: std.posix.fd_t) void {
        if (builtin.target.os.tag == .windows) {
            runtime_helpers.closeFd(fd);
            return;
        }

        // Treat BADF as already-closed and ignore EINTR per POSIX close rules.
        switch (std.posix.errno(std.posix.system.close(fd))) {
            .SUCCESS, .INTR, .BADF => {},
            else => {},
        }
    }
};
