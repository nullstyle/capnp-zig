const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_conn);
const framing = @import("../../wire/framing.zig");
const transport_mod = @import("./stream_transport.zig");
const runtime_helpers = @import("./runtime.zig");
const message = @import("../../../serialization/message.zig");
const events = @import("../../events.zig");

/// A framed Cap'n Proto connection over TCP.
///
/// Combines a `Transport` (raw TCP I/O) with a `Framer` (Cap'n Proto
/// segment-based message framing) to deliver complete RPC messages to
/// the `on_message` callback.
///
/// ## Usage
///
/// 1. Create a connection with `init()`, passing a connected socket fd.
/// 2. Call `start()` to register message/error/close callbacks (typically
///    done by `Peer.attachConnection` + `Peer.start`).
/// 3. Call `run()` to enter the blocking read loop. This method returns
///    when the connection closes (EOF, error, or explicit `close()`).
/// 4. Call `deinit()` to free resources.
///
/// ## Callback context lifetime
///
/// The `ctx` pointer set via `start()` must remain valid until `deinit`
/// is called. All callbacks (`on_message`, `on_error`, `on_close`) are
/// invoked on the thread that calls `run()`.
///
/// ## Ownership
///
/// The `Connection` owns its `Transport` and `Framer`. Call `deinit` to
/// release both. The `Connection` does **not** own the `ctx` pointer.
pub const Connection = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    transport: transport_mod.Transport,
    framer: framing.Framer,
    observer: ?events.Observer = null,
    /// Opaque context pointer passed to `start()`. Must remain valid until
    /// `deinit`. All callbacks may dereference this pointer.
    ctx: ?*anyopaque = null,
    /// Called for each complete inbound Cap'n Proto message frame.
    on_message: ?*const fn (conn: *Connection, frame: []const u8) anyerror!void = null,
    /// Called on transport or framing errors. The connection may be
    /// in a degraded state after an error callback.
    on_error: ?*const fn (conn: *Connection, err: anyerror) void = null,
    /// Called exactly once when the connection's run loop exits.
    ///
    /// If close includes an error, `on_error` is dispatched first and then
    /// `on_close`. If a callback calls `deinit()`, cleanup is deferred until
    /// after `on_close` returns so the active callback stack can unwind.
    ///
    /// **Do not free the `Connection`'s memory from `on_close`.** `run()`
    /// still dereferences `self` after `on_close` returns (unwinding the
    /// callback stack and finishing deferred deinit), so destroying the
    /// object here is a use-after-free. To free a heap-allocated
    /// `Connection` as part of its own lifecycle, register `on_destroy`;
    /// otherwise free it after `run()` returns.
    on_close: ?*const fn (conn: *Connection) void = null,
    /// Called as the very last action of `run()`, after the run loop has
    /// exited, `on_close` has fired, and internal state has been released.
    /// This is the only callback from which it is safe to free the
    /// `Connection`'s own memory: `run()` touches `self` no further once
    /// `on_destroy` is invoked.
    ///
    /// When set, `run()` guarantees `deinit()` has run (idempotently) before
    /// calling `on_destroy`, so the hook only needs to release the heap
    /// allocation, e.g. `allocator.destroy(conn)`. Leave it null for
    /// stack-allocated connections or when the owner frees after `run()`.
    on_destroy: ?*const fn (conn: *Connection) void = null,
    callback_depth: usize = 0,
    deinit_requested: bool = false,
    deinitialized: bool = false,
    last_error: ?anyerror = null,

    // -- Cross-thread wake support -------------------------------------------

    /// Connected socket pair for cross-thread signaling. [0]=read,
    /// [1]=write. When set, `run()` waits on both the socket and the wake
    /// channel, and calls `on_wake` when the channel is readable. POSIX
    /// uses `socketpair(2)`; Windows uses a loopback TCP pair.
    wake_fds: ?[2]std.posix.fd_t = null,

    /// Serializes POSIX `wake()` (called from any thread) against the fd close
    /// in `deinitNow` (owner thread). Without it a wake racing teardown reads
    /// `wake_fds` just before deinit closes them and writes to a closed —
    /// possibly kernel-recycled — descriptor. Uses `std.atomic.Mutex` (like the
    /// transport's fd lock) rather than `std.Io.Mutex` because `wake()` may run
    /// outside any io task; the critical sections are tiny so a spin is fine.
    wake_mu: std.atomic.Mutex = .unlocked,

    /// Called on the run loop's thread when woken by `wake()`.
    /// Use this to drain a per-connection work queue on the correct thread.
    on_wake: ?*const fn (conn: *Connection) void = null,

    /// Set by cross-thread `requestClose()` and consumed by the run loop,
    /// which emits the deferred `.closing` observer event on its own thread.
    /// Atomic because `requestClose` may run on any thread; the observer
    /// itself is only ever invoked on the owner/loop thread (see
    /// `events.Observer`).
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// True once the `.closing` connection phase has been emitted, so the
    /// event fires at most once per connection. Owner/loop-thread only.
    closing_emitted: bool = false,

    /// Windows read-loop bridge; `{}` elsewhere. std's Windows sockets are
    /// raw AFD handles that `WSAPoll` rejects, so the run loop there runs
    /// each blocking read as a cancellable `io.async` task and waits on a
    /// timed condition for read completion, wake, or a tick timeout. All
    /// callbacks still fire on the `run()` thread.
    win_read: if (builtin.target.os.tag == .windows) WinReadBridge else void =
        if (builtin.target.os.tag == .windows) .{} else {},

    // -- Periodic tick / idle reaping -----------------------------------------

    /// When set, `run()` waits with this timeout instead of blocking
    /// indefinitely, and invokes `on_tick` whenever the interval elapses
    /// with no inbound I/O. Works on POSIX (poll) and Windows (WSAPoll);
    /// on freestanding targets there is no readiness primitive and ticks
    /// do not fire.
    tick_interval_ms: ?u32 = null,

    /// Reap the connection when no inbound read or outbound enqueue has
    /// happened for this long. Checked on the tick cadence; if no
    /// `tick_interval_ms` is configured a 500ms default tick drives the
    /// check.
    idle_timeout_ms: ?u64 = null,

    /// Called on the run loop's thread each time the tick interval elapses.
    /// `Peer.attachConnection` wires this to the peer's deadline sweep.
    on_tick: ?*const fn (conn: *Connection) void = null,

    /// Monotonic timestamp of the last inbound read or outbound enqueue.
    last_activity_ns: i64 = 0,

    // -- Thread-affinity check (debug only) ---------------------------------

    /// Thread ID captured at init time. In debug builds, key entry points
    /// assert that the current thread matches this value. Stored widened to
    /// `u64` (not `std.Thread.Id`, whose width varies by target) so the
    /// rendered api-snapshot surface is platform-stable.
    owner_thread_id: ?u64 = null,

    /// When true, thread-affinity checks also run in release builds
    /// (always on in Debug). Mirrors `Peer.enableRuntimeThreadChecks`.
    runtime_thread_checks: bool = false,

    /// Assert that the caller is on the thread that created this connection.
    /// Always enforced in Debug builds; enforced in release builds only
    /// when `runtime_thread_checks` is set. Panics on violation.
    pub fn assertThreadAffinity(self: *const Connection) void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        if (comptime builtin.mode != .debug) {
            if (!self.runtime_thread_checks) return;
        }
        const owner = self.owner_thread_id orelse return;
        const current = std.Thread.getCurrentId();
        if (current != owner) {
            @panic("Connection method called from wrong thread: Connection is not thread-safe, all calls must be on the owner thread");
        }
    }

    /// Re-capture thread affinity on the current thread. Legal only at a
    /// quiescent handoff point — no other thread may touch this connection
    /// concurrently (e.g. `ClientSession.run()` adopting a session that
    /// was connected on another thread before its run loop started).
    pub fn adoptOwnerThread(self: *Connection) void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        self.owner_thread_id = std.Thread.getCurrentId();
    }

    pub const Options = struct {
        /// Size of the per-connection inbound read buffer. Default: 64 KiB.
        read_buffer_size: usize = 64 * 1024,
        /// Maximum number of frames queued for the writer before backpressure.
        /// Default: `Transport.default_max_queued_items` (1024).
        write_queue_max_items: usize = transport_mod.Transport.default_max_queued_items,
        /// Maximum bytes queued for the writer before backpressure.
        /// Default: `Transport.default_max_queued_bytes` (64 MiB).
        write_queue_max_bytes: usize = transport_mod.Transport.default_max_queued_bytes,
        /// Optional event observer. Default: none.
        observer: ?events.Observer = null,
        /// See `Connection.tick_interval_ms`. Default: none (no tick cadence).
        tick_interval_ms: ?u32 = null,
        /// See `Connection.idle_timeout_ms`. Default: none (no idle timeout).
        idle_timeout_ms: ?u64 = null,
        /// Cap on bytes buffered while assembling one inbound frame
        /// (header plus segments). Exceeding it is a fatal framing error.
        /// Mirrors the QUIC transport's per-message bound; lower this for
        /// untrusted peers. Default: `Framer.default_max_buffered_bytes`.
        max_buffered_frame_bytes: usize = framing.Framer.default_max_buffered_bytes,

        /// The documented default `Options` set, spelled out explicitly.
        ///
        /// This establishes the stable defaults contract for the frozen
        /// `Options` surface: `Options.default()` and the field-default form
        /// `.{}` are equivalent and will remain so. Prefer `.{ ...overrides }`
        /// for callers that only change a few tunables; use `default()` when
        /// you want an explicit, named starting point (e.g. copying then
        /// mutating a base config).
        pub fn default() Options {
            return .{
                .read_buffer_size = 64 * 1024,
                .write_queue_max_items = transport_mod.Transport.default_max_queued_items,
                .write_queue_max_bytes = transport_mod.Transport.default_max_queued_bytes,
                .observer = null,
                .tick_interval_ms = null,
                .idle_timeout_ms = null,
                .max_buffered_frame_bytes = framing.Framer.default_max_buffered_bytes,
            };
        }
    };

    /// Reachable failures of `init`. The only fallible step is allocating the
    /// underlying `Transport` read buffer, so this is exactly `OutOfMemory`.
    /// (`Framer.initWithOptions` and the timestamp read are infallible.)
    pub const InitError = error{OutOfMemory};

    /// Reachable failures of `enableWake`. On POSIX the sole failure is the
    /// `socketpair(2)` used to build the cross-thread wake channel; on Windows
    /// and freestanding the function is infallible but shares this set.
    pub const EnableWakeError = error{WakePipeCreateFailed};

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        socket: transport_mod.SocketFd,
        options: Options,
    ) InitError!Connection {
        const conn = Connection{
            .allocator = allocator,
            .io = io,
            .transport = try transport_mod.Transport.initWithOptions(allocator, io, socket, .{
                .read_buffer_size = options.read_buffer_size,
                .write_queue_max_items = options.write_queue_max_items,
                .write_queue_max_bytes = options.write_queue_max_bytes,
                .observer = options.observer,
            }),
            .framer = framing.Framer.initWithOptions(allocator, .{
                .max_buffered_bytes = options.max_buffered_frame_bytes,
            }),
            .observer = options.observer,
            .owner_thread_id = if (comptime builtin.target.os.tag == .freestanding) null else std.Thread.getCurrentId(),
            .tick_interval_ms = options.tick_interval_ms,
            .idle_timeout_ms = options.idle_timeout_ms,
            .last_activity_ns = nowNs(io),
        };
        events.emitConnection(conn.observer, .tcp, .unknown, .initialized);
        return conn;
    }

    /// Enable cross-thread wake support so that other threads can call
    /// `wake()` to interrupt the blocking `run()` loop. The `on_wake`
    /// callback is invoked on the run loop's thread.
    ///
    /// POSIX creates a `socketpair(2)` that the run loop polls alongside
    /// the socket. Windows needs no channel: the run loop there waits on a
    /// timed condition (see `WinReadBridge`) that `wake()` signals
    /// directly.
    fn lockWake(self: *Connection) void {
        while (!self.wake_mu.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn enableWake(
        self: *Connection,
        on_wake: *const fn (conn: *Connection) void,
    ) EnableWakeError!void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        if (comptime builtin.target.os.tag != .windows) {
            var fds: [2]std.posix.fd_t = undefined;
            if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
                return error.WakePipeCreateFailed;
            }
            self.lockWake();
            defer self.wake_mu.unlock();
            // Close a previously installed pair so a second enableWake does not
            // leak the first socketpair.
            if (self.wake_fds) |old| {
                runtime_helpers.closeFd(self.io, .{ .handle = old[0] });
                runtime_helpers.closeFd(self.io, .{ .handle = old[1] });
            }
            self.wake_fds = fds;
        }
        self.on_wake = on_wake;
    }

    /// Wake the run loop from any thread. Thread-safe.
    /// The run loop will call `on_wake` on its own thread.
    pub fn wake(self: *Connection) void {
        if (comptime builtin.target.os.tag == .windows) {
            const io = self.io;
            self.win_read.mu.lockUncancelable(io);
            self.win_read.wake_requested = true;
            self.win_read.cond.broadcast(io);
            self.win_read.mu.unlock(io);
            return;
        }
        // Hold wake_mu across the write so deinitNow cannot close the fd
        // mid-write (which could otherwise land on a kernel-recycled fd).
        self.lockWake();
        defer self.wake_mu.unlock();
        const fds = self.wake_fds orelse return;
        const pattern: []const u8 = &.{};
        const data: [1][]const u8 = .{pattern};
        _ = self.io.vtable.netWrite(self.io.userdata, fds[1], &[_]u8{1}, &data, 0) catch {};
    }

    /// Release all internal state: tears down the transport and framer.
    ///
    /// This does **not** free the `Connection` object itself. When the
    /// `Connection` was heap-allocated (e.g., via `Listener.createConnection`
    /// or `allocator.create(Connection)`), the caller must follow up with
    /// `allocator.destroy(conn)` to release the heap memory:
    ///
    /// ```
    /// conn.deinit();
    /// allocator.destroy(conn);
    /// ```
    ///
    /// For stack-allocated or embedded connections (e.g., in tests), only
    /// `deinit()` is needed.
    pub fn deinit(self: *Connection) void {
        if (self.deinitialized) return;
        if (self.callback_depth != 0) {
            // Deferred: deinit() was called re-entrantly from a callback on the
            // run-loop thread, which need not be the owner thread — so this
            // branch must NOT assert affinity. It only flags intent and wakes
            // the loop; the real teardown runs later via deinitNow, which does
            // assert.
            self.deinit_requested = true;
            self.requestClose();
            return;
        }
        self.deinitNow(true);
    }

    fn deinitNow(self: *Connection, comptime check_affinity: bool) void {
        if (self.deinitialized) return;
        if (check_affinity) self.assertThreadAffinity();
        self.deinitialized = true;
        self.deinit_requested = false;
        self.ctx = null;
        self.on_message = null;
        self.on_error = null;
        self.on_close = null;
        self.on_wake = null;
        self.observer = null;
        {
            // Serialize with wake(): take wake_mu so no concurrent waker is
            // mid-write when we close and null the fds.
            self.lockWake();
            defer self.wake_mu.unlock();
            if (self.wake_fds) |fds| {
                runtime_helpers.closeFd(self.io, .{ .handle = fds[0] });
                runtime_helpers.closeFd(self.io, .{ .handle = fds[1] });
                self.wake_fds = null;
            }
        }
        self.transport.deinit();
        self.framer.deinit();
    }

    /// Register callbacks for inbound messages, errors, and close events.
    ///
    /// Callbacks are invoked during `run()` on the calling thread.
    /// After `start()`, call `run()` to begin the blocking read loop.
    pub fn start(
        self: *Connection,
        ctx: *anyopaque,
        on_message: *const fn (conn: *Connection, frame: []const u8) anyerror!void,
        on_error: *const fn (conn: *Connection, err: anyerror) void,
        on_close: *const fn (conn: *Connection) void,
    ) void {
        self.assertThreadAffinity();
        log.debug("connection starting", .{});
        self.ctx = ctx;
        self.on_message = on_message;
        self.on_error = on_error;
        self.on_close = on_close;
        events.emitConnection(self.observer, .tcp, .unknown, .started);
    }

    pub fn context(self: *const Connection) ?*anyopaque {
        return self.ctx;
    }

    /// Blocking read loop. Reads from the transport, pushes data through
    /// the framer, and dispatches complete message frames to callbacks.
    ///
    /// Returns when:
    /// - The peer closes the connection (EOF / read returns 0)
    /// - A read error occurs
    /// - `close()` is called (from a callback or another thread)
    ///
    /// Starts a dedicated writer thread before entering the read loop.
    /// Writes enqueued via `sendFrame()` are drained by the writer thread
    /// concurrently with reads. The writer thread is joined before
    /// `on_close` fires.
    ///
    /// If wake support is enabled via `enableWake()`, the loop uses
    /// `poll()` to wait on both the socket and the wake pipe. When
    /// woken by another thread, `on_wake` is called before the next read.
    ///
    /// After `run()` returns, the `on_close` callback is invoked.
    pub fn run(self: *Connection) void {
        // run() does NOT auto-adopt thread affinity. A connection run on a
        // thread other than the one that constructed it must adopt first (the
        // ClientSession/ServerSession wrappers call adoptOwnerThread() right
        // before run() for exactly this reason). Auto-adopting here was tried
        // and rejected: it cannot be un-done on exit (run() may free `self` via
        // its on_close/on_destroy teardown, so a restore-on-return would be a
        // use-after-free), which left every construct-here / run-on-a-worker /
        // deinit-here caller owning the connection on a dead thread.
        self.transport.startWriter() catch |err| {
            log.debug("failed to start writer thread: {}", .{err});
            self.last_error = err;
            self.invokeOnError(err);
            self.emitClosed();
            if (self.on_close) |cb| {
                self.invokeCloseCallback(cb);
            }
            self.finishTeardown();
            return;
        };

        if (comptime builtin.target.os.tag == .windows) {
            self.runLoopWindows();
        } else self.runLoopShared();

        // Stop the writer thread before invoking on_close, since on_close
        // may deinit the connection.
        self.transport.stopWriter();

        // Signal close to the owner (typically the Peer).
        self.emitClosed();
        if (self.on_close) |cb| {
            self.invokeCloseCallback(cb);
        }
        self.finishTeardown();
    }

    /// Final teardown of `run()`: complete any deferred deinit, then hand
    /// the (possibly heap-allocated) object to `on_destroy`. Nothing may
    /// touch `self` after `on_destroy` returns, because the hook may free
    /// the memory — so this is intentionally the last statement of `run()`.
    fn finishTeardown(self: *Connection) void {
        self.completeDeferredDeinit();
        if (self.on_destroy) |destroy| {
            // Guarantee internal state is released even if no callback
            // called deinit(). deinitNow is idempotent, so this is safe
            // whether or not a deferred deinit already ran above.
            self.deinitNow(false);
            destroy(self);
        }
    }

    /// The POSIX/freestanding read loop: optional poll-based readiness for
    /// ticks and wake, then blocking reads on this thread.
    fn runLoopShared(self: *Connection) void {
        while (!self.transport.isClosing()) {
            // Wait for readiness when wake support or a tick cadence is
            // enabled, so the loop can wait on the socket (plus the wake
            // channel) with a bounded timeout (freestanding has no
            // readiness primitive: ticks/wake do not fire there). Windows
            // never enters this loop; see runLoopWindows.
            if (comptime builtin.target.os.tag != .freestanding and builtin.target.os.tag != .windows) {
                // An idle bound without an explicit tick still needs the
                // loop to wake periodically; default to 500ms checks.
                const effective_tick_ms: ?u32 = self.tick_interval_ms orelse
                    (if (self.idle_timeout_ms != null) @as(?u32, 500) else null);
                if (self.wake_fds != null or effective_tick_ms != null) {
                    var fds_buf: [2]PollFd = undefined;
                    fds_buf[0] = makePollFd(self.transport.fd);
                    var nfds: usize = 1;
                    if (self.wake_fds) |wfds| {
                        fds_buf[1] = makePollFd(wfds[0]);
                        nfds = 2;
                    }
                    const timeout_ms: i32 = if (effective_tick_ms) |t|
                        @intCast(@min(t, @as(u32, std.math.maxInt(i32))))
                    else
                        -1;
                    const poll_result = pollRetryIntr(fds_buf[0..nfds], timeout_ms);
                    if (poll_result == .timeout) {
                        // Tick: the interval elapsed with no inbound I/O.
                        if (self.idleDeadlineExceeded()) {
                            log.debug("idle timeout exceeded, reaping connection", .{});
                            events.emitTimeout(self.observer, .tcp, .unknown, .idle_connection, null);
                            break;
                        }
                        if (self.on_tick) |cb| self.invokeTickWakeCallback(cb);
                        if (self.deinit_requested) break;
                        continue;
                    }
                    if (poll_result == .ready) {
                        // Drain wake channel and invoke callback.
                        if (nfds == 2 and fds_buf[1].revents & poll_in_mask != 0) {
                            if (self.wake_fds) |wake_fds| {
                                var drain_buf: [64]u8 = undefined;
                                var bufs: [1][]u8 = .{&drain_buf};
                                var wake_stream = std.Io.net.Stream{ .socket = .{ .handle = wake_fds[0], .address = undefined } };
                                _ = wake_stream.read(self.io, &bufs) catch {};
                            }
                            if (self.on_wake) |cb| self.invokeTickWakeCallback(cb);
                            if (self.deinit_requested) break;
                        }
                        // POLLNVAL means the fd is invalid — break out of the run loop.
                        if (fds_buf[0].revents & poll_nval_mask != 0) {
                            log.debug("poll NVAL on socket fd, exiting run loop", .{});
                            break;
                        }
                        // If socket has no data, loop back (might have been woken only).
                        if (fds_buf[0].revents & poll_in_mask == 0 and
                            fds_buf[0].revents & poll_hup_mask == 0 and
                            fds_buf[0].revents & poll_err_mask == 0)
                        {
                            continue;
                        }
                    }
                }
            }

            const n = self.transport.read() catch |err| {
                log.debug("transport read error: {}", .{err});
                self.last_error = err;
                self.invokeOnError(err);
                self.on_message = null;
                self.on_error = null;
                self.transport.shutdown();
                break;
            };
            if (n == 0) {
                log.debug("transport EOF", .{});
                break;
            }
            self.last_activity_ns = nowNs(self.io);
            if (!self.handleRead(self.transport.read_buf[0..n])) {
                self.transport.shutdown();
                break;
            }
        }
    }

    /// The Windows read loop. std's Windows sockets are raw AFD handles
    /// with no readiness primitive (`WSAPoll` rejects them), so the
    /// blocking read runs as a cancellable `io.concurrent` task on an io
    /// worker thread while this thread waits on a timed condition — that
    /// keeps ticks, idle reaping, and wake working. Crucially the read is
    /// on an io worker (not a raw `std.Thread`), so teardown can `cancel`
    /// it: `NtCancelIoFileEx` then unblocks the pending AFD receive,
    /// instead of waiting out the kernel's multi-minute read timeout.
    /// `concurrent` (not `async`) is used so the read never silently runs
    /// inline on this thread — that would reintroduce the un-cancellable
    /// blocking read this design exists to avoid; its `concurrent_limit`
    /// defaults to unlimited, so any number of connections each get a real
    /// cancellable read thread. Reads target `transport.read_buf`;
    /// ownership passes to the task while a read is in flight and back to
    /// this thread when its `result` is published, so there is no
    /// concurrent access and no copying.
    fn runLoopWindows(self: *Connection) void {
        const io = self.io;

        // The in-flight read task, if any. `cancel`/`await` on it run only
        // from this thread (the framework's threadsafety requirement).
        var read_future: ?std.Io.Future(void) = null;
        defer if (read_future) |*f| {
            _ = f.cancel(io);
        };

        while (!self.transport.isClosing()) {
            const effective_tick_ms: ?u32 = self.tick_interval_ms orelse
                (if (self.idle_timeout_ms != null) @as(?u32, 500) else null);

            // Launch a read if none is in flight and no result is pending.
            if (read_future == null) {
                const have_result = blk: {
                    self.win_read.mu.lockUncancelable(io);
                    defer self.win_read.mu.unlock(io);
                    break :blk self.win_read.result != null;
                };
                if (!have_result) {
                    read_future = io.concurrent(winReadTask, .{self}) catch null;
                    if (read_future == null) {
                        // ConcurrencyUnavailable (single-threaded, OOM, or
                        // thread-spawn exhaustion — never in practice with
                        // the default unlimited concurrent_limit). Fall
                        // back to a single inline read; it cannot be
                        // cancelled, but this path is effectively
                        // unreachable in the threaded runtime.
                        winReadTask(self);
                    }
                }
            }

            var outcome: ?WinReadBridge.ReadOutcome = null;
            var woke = false;
            {
                self.win_read.mu.lockUncancelable(io);
                defer self.win_read.mu.unlock(io);
                if (self.win_read.result == null and !self.win_read.wake_requested) {
                    if (effective_tick_ms) |tick_ms| {
                        const timeout: std.Io.Timeout = .{ .duration = .{
                            .raw = .{ .nanoseconds = @as(i96, tick_ms) * std.time.ns_per_ms },
                            .clock = .awake,
                        } };
                        // error.Timeout is the tick; anything else is
                        // treated the same (spurious wake, re-checked
                        // below).
                        self.win_read.cond.waitTimeout(io, &self.win_read.mu, timeout) catch {};
                    } else {
                        self.win_read.cond.waitUncancelable(io, &self.win_read.mu);
                    }
                }
                if (self.win_read.wake_requested) {
                    self.win_read.wake_requested = false;
                    woke = true;
                }
                if (self.win_read.result) |r| {
                    outcome = r;
                    self.win_read.result = null;
                }
            }

            if (woke) {
                if (self.on_wake) |cb| self.invokeTickWakeCallback(cb);
                if (self.deinit_requested) break;
            }

            const r = outcome orelse {
                if (woke) continue;
                // Tick: the interval elapsed with no read completion.
                if (self.idleDeadlineExceeded()) {
                    log.debug("idle timeout exceeded, reaping connection", .{});
                    events.emitTimeout(self.observer, .tcp, .unknown, .idle_connection, null);
                    break;
                }
                if (self.on_tick) |cb| self.invokeTickWakeCallback(cb);
                if (self.deinit_requested) break;
                continue;
            };

            // The task published a result and is returning; reap it.
            if (read_future) |*f| {
                f.await(io);
                read_future = null;
            }

            switch (r) {
                .eof => {
                    log.debug("transport EOF", .{});
                    break;
                },
                .fail => |err| {
                    log.debug("transport read error: {}", .{err});
                    self.last_error = err;
                    self.invokeOnError(err);
                    self.on_message = null;
                    self.on_error = null;
                    self.transport.shutdown();
                    break;
                },
                .data => |n| {
                    self.last_activity_ns = nowNs(io);
                    if (!self.handleRead(self.transport.read_buf[0..n])) {
                        self.transport.shutdown();
                        break;
                    }
                },
            }
        }
    }

    /// One cancellable blocking read, run as an `io.concurrent` task on an
    /// io worker thread (see `runLoopWindows`). Publishes the outcome under
    /// the bridge mutex and signals the run loop. On cancel the read
    /// returns `error.Canceled`, which is published like any other failure
    /// but ignored by the tearing-down run loop.
    fn winReadTask(self: *Connection) void {
        const io = self.io;
        const outcome: WinReadBridge.ReadOutcome = blk: {
            const n = self.transport.read() catch |err| break :blk .{ .fail = err };
            if (n == 0) break :blk .eof;
            break :blk .{ .data = n };
        };
        self.win_read.mu.lockUncancelable(io);
        self.win_read.result = outcome;
        self.win_read.cond.broadcast(io);
        self.win_read.mu.unlock(io);
    }

    /// Enqueue a framed message for sending to the remote peer.
    /// Non-blocking — the frame is copied into the write queue and
    /// delivered by the writer thread.
    pub fn sendFrame(self: *Connection, frame: []const u8) !void {
        self.assertThreadAffinity();
        self.transport.enqueueWrite(frame) catch |err| {
            log.debug("write enqueue failed: {}", .{err});
            self.invokeOnError(err);
            return err;
        };
        self.last_activity_ns = nowNs(self.io);
    }

    /// Initiate connection close. This shuts down the socket, which will
    /// cause `run()` to exit on the next read attempt.
    pub fn close(self: *Connection) void {
        self.assertThreadAffinity();
        log.debug("connection closing", .{});
        self.emitClosingOnce();
        self.transport.close();
    }

    /// Signal the transport to stop from any thread. Wakes a blocked
    /// `run()` loop by shutting down the socket.
    ///
    /// Never invokes the observer directly: observer callbacks fire on the
    /// owner/loop thread only, so the `.closing` event is deferred to the run
    /// loop, which emits it when it observes the request. If the loop never
    /// runs (or has already exited) the deferred event is dropped.
    pub fn requestClose(self: *Connection) void {
        self.close_requested.store(true, .release);
        self.transport.shutdown();
        if (comptime builtin.target.os.tag == .windows) {
            // On Windows the run loop parks on win_read.cond when it has no tick
            // cadence, and a bare socket shutdown never unblocks the pending AFD
            // receive (the reason runLoopWindows runs reads as cancellable
            // io.concurrent tasks). Nudge the bridge — the same channel wake()
            // uses — so the loop wakes, sees isClosing()==true at the top, and
            // exits; the defer-cancel then reaps the in-flight read. Without
            // this a cross-thread requestClose (e.g. WorkerPool shutdown against
            // a silent connection) hangs until the kernel's multi-minute read
            // timeout. Thread-safe via win_read.mu.
            self.win_read.mu.lockUncancelable(self.io);
            self.win_read.wake_requested = true;
            self.win_read.cond.broadcast(self.io);
            self.win_read.mu.unlock(self.io);
        }
    }

    pub fn isClosing(self: *const Connection) bool {
        self.assertThreadAffinity();
        return self.transport.isClosing();
    }

    /// Point-in-time write queue occupancy, for metrics scraping.
    pub fn writeQueueStats(self: *Connection) transport_mod.Transport.QueueStats {
        self.assertThreadAffinity();
        return self.transport.queueStats();
    }

    fn handleRead(self: *Connection, data: []const u8) bool {
        if (self.on_message == null or self.on_error == null) return false;

        const push_result = self.framer.push(data);
        if (push_result) |_| {} else |err| {
            log.debug("framer push failed: {}", .{err});
            self.framer.reset();
            events.emitProtocolError(self.observer, .tcp, .unknown, err, null);
            self.invokeTerminalError(err);
            return false;
        }

        while (true) {
            // Re-check callbacks each iteration: a prior on_message callback
            // may have nulled them (e.g. by calling close/deinit on the
            // connection). Without this guard the `.?` unwrap would panic.
            if (self.on_message == null or self.on_error == null) break;

            const frame = self.framer.popFrame() catch |err| {
                if (err == error.OutOfMemory) {
                    log.debug("popFrame OOM, closing connection", .{});
                    self.framer.reset();
                    events.emitProtocolError(self.observer, .tcp, .unknown, err, null);
                    self.invokeTerminalError(err);
                    return false;
                }
                // Framing errors (InvalidFrame, FrameTooLarge) corrupt the
                // byte stream — reset the framer and null the callbacks.
                log.debug("framing error, connection unrecoverable: {}", .{err});
                self.framer.reset();
                events.emitProtocolError(self.observer, .tcp, .unknown, err, null);
                self.invokeTerminalError(err);
                return false;
            };
            if (frame == null) break;
            const bytes = frame.?;
            defer self.allocator.free(bytes);
            events.emitFrame(self.observer, .tcp, .unknown, .received, bytes.len);

            // Design note: message handler errors are treated as non-fatal.
            // Unlike framing errors (which corrupt the byte stream and make
            // all subsequent frames untrustworthy), a handler error is
            // application-level and recoverable — the framer state remains
            // valid. We report the error via on_error and stop processing
            // further buffered frames from *this* read, but do not reset the
            // framer or null the callbacks, so the connection can continue
            // receiving future reads normally.
            self.invokeMessageCallback(self.on_message.?, bytes) catch |err| {
                self.invokeOnError(err);
                return true;
            };
            if (self.deinit_requested) return false;
        }
        return true;
    }

    fn invokeOnError(self: *Connection, err: anyerror) void {
        const cb = self.on_error orelse return;
        self.invokeErrorCallback(cb, err);
    }

    fn invokeTerminalError(self: *Connection, err: anyerror) void {
        self.last_error = err;
        self.transport.shutdown();
        const on_error = self.on_error;
        if (on_error) |cb| {
            self.invokeErrorCallback(cb, err);
        }
        self.on_message = null;
        self.on_error = null;
    }

    fn invokeErrorCallback(
        self: *Connection,
        cb: *const fn (conn: *Connection, callback_err: anyerror) void,
        err: anyerror,
    ) void {
        self.callback_depth += 1;
        defer self.callback_depth -= 1;
        cb(self, err);
    }

    fn invokeMessageCallback(
        self: *Connection,
        cb: *const fn (conn: *Connection, frame: []const u8) anyerror!void,
        frame: []const u8,
    ) !void {
        self.callback_depth += 1;
        defer self.callback_depth -= 1;
        try cb(self, frame);
    }

    fn invokeCloseCallback(
        self: *Connection,
        cb: *const fn (conn: *Connection) void,
    ) void {
        self.callback_depth += 1;
        defer self.callback_depth -= 1;
        cb(self);
    }

    /// Invoke `on_tick`/`on_wake` under callback-depth accounting so that a
    /// `deinit()` from inside them is deferred (like every other callback)
    /// rather than freeing state — including, on Windows, the `read_buf`
    /// targeted by an in-flight concurrent read — out from under the loop.
    /// Callers must check `deinit_requested` afterwards and break the loop.
    fn invokeTickWakeCallback(
        self: *Connection,
        cb: *const fn (conn: *Connection) void,
    ) void {
        self.callback_depth += 1;
        defer self.callback_depth -= 1;
        cb(self);
    }

    fn completeDeferredDeinit(self: *Connection) void {
        if (self.deinit_requested and self.callback_depth == 0) {
            self.deinitNow(false);
        }
    }

    fn emitClosed(self: *Connection) void {
        // A cross-thread requestClose() defers its `.closing` event to this
        // (the loop) thread; emit it before the terminal events so observers
        // see closing → close → closed in order.
        if (self.close_requested.load(.acquire)) self.emitClosingOnce();
        events.emitClose(self.observer, .tcp, .unknown, self.last_error);
        events.emitConnection(self.observer, .tcp, .unknown, .closed);
    }

    /// Emit the `.closing` connection phase at most once. Owner/loop-thread
    /// only: called synchronously by `close()` and by the run loop when it
    /// observes a cross-thread `requestClose()`.
    fn emitClosingOnce(self: *Connection) void {
        if (self.closing_emitted) return;
        self.closing_emitted = true;
        events.emitConnection(self.observer, .tcp, .unknown, .closing);
    }

    fn idleDeadlineExceeded(self: *const Connection) bool {
        const timeout_ms = self.idle_timeout_ms orelse return false;
        const now = nowNs(self.io);
        return now - self.last_activity_ns >= @as(i64, @intCast(timeout_ms)) * std.time.ns_per_ms;
    }
};

/// Monotonic now in nanoseconds via the connection's `std.Io`.
fn nowNs(io: std.Io) i64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

/// Synchronization state for the Windows read loop (see
/// `Connection.runLoopWindows`). Ownership of `transport.read_buf`
/// alternates strictly: the in-flight read task owns it until it publishes
/// `result`; the run thread owns it after taking `result` until it
/// launches the next read.
const WinReadBridge = struct {
    mu: std.Io.Mutex = .init,
    cond: std.Io.Condition = .init,
    /// Set by `wake()` from any thread; consumed by the run loop.
    wake_requested: bool = false,
    /// Outcome of the in-flight read once it completes. Published by the
    /// `io.async` read task under `mu`; consumed by the run loop.
    result: ?ReadOutcome = null,

    const ReadOutcome = union(enum) {
        data: usize,
        eof,
        fail: anyerror,
    };
};

const PollOutcome = enum { ready, timeout, err };

/// POSIX readiness-wait entry. The Windows loop does not poll at all
/// (std's AFD-created sockets are rejected by WSAPoll); see
/// `Connection.runLoopWindows`.
const PollFd = std.posix.pollfd;

const poll_in_event: i16 = if (builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) 0 else std.posix.POLL.IN;
const poll_in_mask: i16 = poll_in_event;
const poll_err_mask: i16 = if (builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) 0 else std.posix.POLL.ERR;
const poll_hup_mask: i16 = if (builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) 0 else std.posix.POLL.HUP;
const poll_nval_mask: i16 = if (builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) 0 else std.posix.POLL.NVAL;

fn makePollFd(handle: std.Io.net.Socket.Handle) PollFd {
    return .{ .fd = handle, .events = poll_in_event, .revents = 0 };
}

/// poll(2) with EINTR retry. The raw return type differs across platforms
/// (usize on Linux syscalls, c_int via libc elsewhere), so outcomes are
/// classified via errno rather than the sign of the return value.
fn pollRetryIntr(fds: []PollFd, timeout_ms: i32) PollOutcome {
    if (comptime builtin.target.os.tag == .windows or builtin.target.os.tag == .freestanding) {
        return .err;
    }
    while (true) {
        const rc = std.posix.system.poll(@ptrCast(fds.ptr), @intCast(fds.len), timeout_ms);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return if (rc == 0) .timeout else .ready,
            .INTR => continue,
            else => return .err,
        }
    }
}

fn buildTestFrame(allocator: std.mem.Allocator, value: u32) ![]const u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(1, 0);
    root.writeU32(0, value);
    return builder.toBytes();
}

test "connection handleRead assembles fragmented frame and dispatches once complete" {
    const allocator = std.testing.allocator;

    const Harness = struct {
        const State = struct {
            allocator: std.mem.Allocator,
            received: std.ArrayList(u32),
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(conn: *Connection, frame: []const u8) !void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            var msg = try message.Message.initUnvalidated(state.allocator, frame);
            defer msg.deinit();
            const root = try msg.getRootStruct();
            try state.received.append(state.allocator, root.readU32(0));
        }

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    const frame = try buildTestFrame(allocator, 0xA1B2_C3D4);
    defer allocator.free(frame);

    var state = Harness.State{
        .allocator = allocator,
        .received = std.ArrayList(u32).empty,
    };
    defer state.received.deinit(allocator);

    var conn = Connection{
        .allocator = allocator,
        .io = std.testing.io,
        .transport = undefined,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.framer.deinit();

    try std.testing.expect(frame.len > 8);
    try std.testing.expect(conn.handleRead(frame[0..5]));
    try std.testing.expectEqual(@as(usize, 0), state.received.items.len);
    try std.testing.expectEqual(@as(usize, 0), state.error_count);

    try std.testing.expect(conn.handleRead(frame[5..]));
    try std.testing.expectEqual(@as(usize, 1), state.received.items.len);
    try std.testing.expectEqual(@as(u32, 0xA1B2_C3D4), state.received.items[0]);
    try std.testing.expectEqual(@as(usize, 0), state.error_count);
}

test "connection handleRead dispatches coalesced frames in order" {
    const allocator = std.testing.allocator;

    const Harness = struct {
        const State = struct {
            allocator: std.mem.Allocator,
            received: std.ArrayList(u32),
            error_count: usize = 0,
        };

        fn onMessage(conn: *Connection, frame: []const u8) !void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            var msg = try message.Message.initUnvalidated(state.allocator, frame);
            defer msg.deinit();
            const root = try msg.getRootStruct();
            try state.received.append(state.allocator, root.readU32(0));
        }

        fn onError(conn: *Connection, _: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
        }
    };

    const first = try buildTestFrame(allocator, 10);
    defer allocator.free(first);
    const second = try buildTestFrame(allocator, 20);
    defer allocator.free(second);

    const combined = try allocator.alloc(u8, first.len + second.len);
    defer allocator.free(combined);
    std.mem.copyForwards(u8, combined[0..first.len], first);
    std.mem.copyForwards(u8, combined[first.len..], second);

    var state = Harness.State{
        .allocator = allocator,
        .received = std.ArrayList(u32).empty,
    };
    defer state.received.deinit(allocator);

    var conn = Connection{
        .allocator = allocator,
        .io = std.testing.io,
        .transport = undefined,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.framer.deinit();

    try std.testing.expect(conn.handleRead(combined));
    try std.testing.expectEqual(@as(usize, 2), state.received.items.len);
    try std.testing.expectEqual(@as(u32, 10), state.received.items[0]);
    try std.testing.expectEqual(@as(u32, 20), state.received.items[1]);
    try std.testing.expectEqual(@as(usize, 0), state.error_count);
}

test "connection handleRead stops draining when message handler errors" {
    const allocator = std.testing.allocator;

    const Harness = struct {
        const State = struct {
            allocator: std.mem.Allocator,
            received: std.ArrayList(u32),
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(conn: *Connection, frame: []const u8) !void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            var msg = try message.Message.initUnvalidated(state.allocator, frame);
            defer msg.deinit();
            const root = try msg.getRootStruct();
            try state.received.append(state.allocator, root.readU32(0));
            if (state.received.items.len == 1) return error.TestMessageHandlerFailure;
        }

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    const first = try buildTestFrame(allocator, 111);
    defer allocator.free(first);
    const second = try buildTestFrame(allocator, 222);
    defer allocator.free(second);

    const combined = try allocator.alloc(u8, first.len + second.len);
    defer allocator.free(combined);
    std.mem.copyForwards(u8, combined[0..first.len], first);
    std.mem.copyForwards(u8, combined[first.len..], second);

    var state = Harness.State{
        .allocator = allocator,
        .received = std.ArrayList(u32).empty,
    };
    defer state.received.deinit(allocator);

    var conn = Connection{
        .allocator = allocator,
        .io = std.testing.io,
        .transport = undefined,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.framer.deinit();

    try std.testing.expect(conn.handleRead(combined));
    try std.testing.expectEqual(@as(usize, 1), state.received.items.len);
    try std.testing.expectEqual(@as(u32, 111), state.received.items[0]);
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.TestMessageHandlerFailure), state.last_error);
    try std.testing.expect(conn.framer.bufferedBytes() > 0);
}

test "connection handleRead reports malformed frame errors" {
    const allocator = std.testing.allocator;

    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var state = Harness.State{};
    // A real Transport rather than `undefined`. `handleRead`'s terminal-error
    // path runs `invokeTerminalError` -> `transport.shutdown()`, which reads
    // the io vtable and closes the write queue; against an undefined transport
    // that is a segfault dereferencing `io.vtable`. These two tests were
    // written before `invokeTerminalError` shut the transport down, and never
    // compiled afterwards, so nothing caught the drift.
    //
    // The socket is pre-marked closed so `shutdown` skips `netShutdown` and
    // `deinit` skips `close` -- the sentinel fd is never handed to the OS.
    var transport = try transport_mod.Transport.init(allocator, std.testing.io, .{ .handle = invalid_socket_handle }, 64);
    transport.fd_closed.store(true, .release);
    defer transport.deinit();

    var conn = Connection{
        .allocator = allocator,
        .io = std.testing.io,
        .transport = transport,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.framer.deinit();

    // segment_count_minus_one = max u32 overflows on +1 in framer.updateExpected()
    const bad_header = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try std.testing.expect(!conn.handleRead(&bad_header));

    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), state.last_error);
    try std.testing.expect(conn.on_message == null);
    try std.testing.expect(conn.on_error == null);
}

/// A socket handle that is never handed to the OS. Used by the `handleRead`
/// terminal-error tests, whose transports are constructed already-closed so
/// the fd is only ever compared, never operated on. Comptime-selected because
/// `net.Socket.Handle` is an integer fd on POSIX and a pointer HANDLE on
/// Windows.
const invalid_socket_handle: std.Io.net.Socket.Handle = switch (@typeInfo(std.Io.net.Socket.Handle)) {
    .pointer => @ptrFromInt(std.math.maxInt(usize)),
    else => -1,
};

test "connection handleRead rejects oversized frame headers" {
    const allocator = std.testing.allocator;

    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }
    };

    var state = Harness.State{};
    // A real Transport rather than `undefined`. `handleRead`'s terminal-error
    // path runs `invokeTerminalError` -> `transport.shutdown()`, which reads
    // the io vtable and closes the write queue; against an undefined transport
    // that is a segfault dereferencing `io.vtable`. These two tests were
    // written before `invokeTerminalError` shut the transport down, and never
    // compiled afterwards, so nothing caught the drift.
    //
    // The socket is pre-marked closed so `shutdown` skips `netShutdown` and
    // `deinit` skips `close` -- the sentinel fd is never handed to the OS.
    var transport = try transport_mod.Transport.init(allocator, std.testing.io, .{ .handle = invalid_socket_handle }, 64);
    transport.fd_closed.store(true, .release);
    defer transport.deinit();

    var conn = Connection{
        .allocator = allocator,
        .io = std.testing.io,
        .transport = transport,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.framer.deinit();

    const oversized_words: u32 = (8 * 1024 * 1024) + 1;
    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 0, .little); // 1 segment
    std.mem.writeInt(u32, header[4..8], oversized_words, .little);

    try std.testing.expect(!conn.handleRead(&header));
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.FrameTooLarge), state.last_error);
    try std.testing.expect(conn.on_message == null);
    try std.testing.expect(conn.on_error == null);
}

test "connection isClosing reflects transport state" {
    const allocator = std.testing.allocator;
    var read_buf: [8]u8 = @splat(0);

    var conn = Connection{
        .allocator = allocator,
        .io = std.testing.io,
        .transport = .{
            .allocator = allocator,
            .io = std.testing.io,
            .fd = undefined,
            .read_buf = read_buf[0..],
        },
        .framer = framing.Framer.init(allocator),
    };
    defer conn.framer.deinit();

    try std.testing.expect(!conn.isClosing());

    conn.transport.close_requested.store(true, .release);
    try std.testing.expect(conn.isClosing());
}

test "Connection.Options.default matches field-default form" {
    const defaults = Connection.Options.default();
    const empty: Connection.Options = .{};
    try std.testing.expectEqual(empty.read_buffer_size, defaults.read_buffer_size);
    try std.testing.expectEqual(empty.write_queue_max_items, defaults.write_queue_max_items);
    try std.testing.expectEqual(empty.write_queue_max_bytes, defaults.write_queue_max_bytes);
    try std.testing.expectEqual(empty.observer, defaults.observer);
    try std.testing.expectEqual(empty.tick_interval_ms, defaults.tick_interval_ms);
    try std.testing.expectEqual(empty.idle_timeout_ms, defaults.idle_timeout_ms);
    try std.testing.expectEqual(empty.max_buffered_frame_bytes, defaults.max_buffered_frame_bytes);
}
