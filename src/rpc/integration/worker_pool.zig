const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_worker_pool);
const xev = @import("xev").Dynamic;
const Connection = @import("../level2/connection.zig").Connection;
const Listener = @import("../level2/runtime.zig").Listener;
const Runtime = @import("../level2/runtime.zig").Runtime;
const Peer = @import("../level3/peer.zig").Peer;

/// A multi-threaded worker pool that runs N independent event loops, each
/// accepting connections on the same address via `SO_REUSEPORT`. This is
/// the nginx/envoy model: the OS kernel distributes incoming connections
/// across workers with no cross-thread communication.
///
/// Each worker thread owns its own `Runtime`, `Listener`, and event loop.
/// The user-provided `AcceptFn` callback fires on the worker thread when a
/// connection is accepted; the user sets the bootstrap capability and starts
/// the peer inside the callback.
pub const WorkerPool = struct {
    allocator: std.mem.Allocator,
    workers: []Worker,
    addr: std.net.Address,
    ctx: *anyopaque,
    on_accept: AcceptFn,
    conn_options: Connection.Options,
    listen_backlog: u31,
    should_stop: std.atomic.Value(bool),

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
        listen_fd: std.posix.fd_t,
        thread: ?std.Thread = null,
        /// Set to true when the fd is closed (by shutdown or deinit).
        fd_closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    };

    pub fn init(
        allocator: std.mem.Allocator,
        addr: std.net.Address,
        ctx: *anyopaque,
        on_accept: AcceptFn,
        config: Config,
    ) !WorkerPool {
        try Runtime.ensureBackend();

        const concurrency: u32 = config.concurrency orelse @intCast(std.Thread.getCpuCount() catch 1);
        if (concurrency == 0) return error.InvalidConcurrency;

        const workers = try allocator.alloc(Worker, concurrency);
        errdefer allocator.free(workers);

        var created: usize = 0;
        errdefer for (workers[0..created]) |w| {
            std.posix.close(w.listen_fd);
        };

        for (workers) |*w| {
            w.* = .{
                .listen_fd = try createListenSocket(addr, config.listen_backlog, concurrency > 1),
            };
            created += 1;
        }

        return .{
            .allocator = allocator,
            .workers = workers,
            .addr = addr,
            .ctx = ctx,
            .on_accept = on_accept,
            .conn_options = config.connection_options,
            .listen_backlog = config.listen_backlog,
            .should_stop = std.atomic.Value(bool).init(false),
        };
    }

    /// Blocks until shutdown. Spawns N-1 threads; the calling thread runs
    /// worker 0.
    pub fn run(self: *WorkerPool) !void {
        var spawned: usize = 0;
        errdefer {
            self.should_stop.store(true, .release);
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

    /// Signal all workers to stop. Workers detect this within ~100ms
    /// via their periodic timer and exit their event loops.
    pub fn shutdown(self: *WorkerPool) void {
        self.should_stop.store(true, .release);
    }

    pub fn deinit(self: *WorkerPool) void {
        for (self.workers) |*w| {
            if (!w.fd_closed.swap(true, .acq_rel)) {
                std.posix.close(w.listen_fd);
            }
        }
        self.allocator.free(self.workers);
    }

    fn workerMain(pool: *WorkerPool, worker_index: u32) void {
        var runtime = Runtime.init(pool.allocator) catch |err| {
            log.err("worker {}: runtime init failed: {}", .{ worker_index, err });
            return;
        };
        defer runtime.deinit();

        var wctx = WorkerCtx{
            .pool = pool,
            .worker_index = worker_index,
            .listener = undefined,
            .timer_completion = .{},
        };

        wctx.listener = Listener.initFd(
            pool.allocator,
            &runtime.loop,
            pool.workers[worker_index].listen_fd,
            internalOnAccept,
            pool.conn_options,
        );
        wctx.listener.start();

        // Periodic timer checks the should_stop flag every 100ms.
        // When set, it calls loop.stop() which makes until_done return
        // immediately regardless of active completions.
        const timer = xev.Timer.init() catch |err| {
            log.err("worker {}: timer init failed: {}", .{ worker_index, err });
            return;
        };
        defer timer.deinit();

        timer.run(&runtime.loop, &wctx.timer_completion, 100, WorkerCtx, &wctx, onTimerCheck);

        runtime.run(.until_done) catch |err| {
            log.err("worker {}: event loop error: {}", .{ worker_index, err });
        };

        // Mark the fd as closed since the listener owns the socket
        // lifecycle within the event loop. We close it here after the
        // loop exits to ensure no more events reference it.
        if (!pool.workers[worker_index].fd_closed.swap(true, .acq_rel)) {
            std.posix.close(pool.workers[worker_index].listen_fd);
        }
    }

    const WorkerCtx = struct {
        pool: *WorkerPool,
        worker_index: u32,
        listener: Listener,
        timer_completion: xev.Completion,
    };

    fn onTimerCheck(
        wctx: ?*WorkerCtx,
        loop: *xev.Loop,
        _: *xev.Completion,
        _: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        const ctx = wctx.?;
        if (ctx.pool.should_stop.load(.acquire)) {
            // Stop the loop immediately. This makes until_done return
            // even though the listener's accept completion is still
            // active. The fd is closed after the loop exits.
            loop.stop();
            return .disarm;
        }
        return .rearm;
    }

    fn internalOnAccept(listener: *Listener, conn: *Connection) void {
        const wctx: *WorkerCtx = @fieldParentPtr("listener", listener);

        const peer_ptr = wctx.pool.allocator.create(Peer) catch {
            conn.deinit();
            wctx.pool.allocator.destroy(conn);
            return;
        };

        peer_ptr.* = Peer.init(wctx.pool.allocator, conn);

        wctx.pool.on_accept(wctx.pool.ctx, peer_ptr, conn, wctx.worker_index);
    }

    fn createListenSocket(addr: std.net.Address, backlog: u31, reuseport: bool) !std.posix.fd_t {
        const flags: u32 = blk: {
            var f: u32 = std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC;
            if (builtin.target.os.tag != .linux or xev.backend != .io_uring) {
                f |= std.posix.SOCK.NONBLOCK;
            }
            break :blk f;
        };

        const fd = try std.posix.socket(addr.any.family, flags, 0);
        errdefer std.posix.close(fd);

        try std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        if (reuseport) {
            try std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
        }

        try std.posix.bind(fd, &addr.any, addr.getOsSockLen());
        try std.posix.listen(fd, backlog);

        return fd;
    }
};
