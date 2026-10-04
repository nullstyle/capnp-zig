const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_transport);
const net = std.Io.net;
const events = @import("../../events.zig");
const fd_io = @import("../unix/fd_io.zig");

/// Opaque platform-stable socket handle wrapper passed into the transport
/// layer. This is the canonical handle type in every public, handle-taking
/// entry point (`Connection.init`, `Listener.initFd`/`acceptFd`/`listenHandle`,
/// `Transport.init*`, `createLoopbackSocketPair`, and the `runtime` re-export).
///
/// It is a thin named wrapper so the raw platform handle never leaks into a
/// frozen signature: `std.Io.net.Socket.Handle` is an `i32` on POSIX and a
/// pointer (`*anyopaque`/`HANDLE`) on Windows. Wrapping it keeps the public
/// surface — and the `docs/api-snapshot.txt` gate built from it — byte-for-byte
/// identical on every platform. Treat `handle` as opaque; do not depend on its
/// concrete type across targets.
pub const SocketFd = struct {
    handle: net.Socket.Handle,
};

/// TCP transport layer with concurrent read/write support.
///
/// `Transport` owns a TCP socket handle and provides blocking
/// read operations on the reader thread and asynchronous writes via a
/// dedicated writer thread.
///
/// ## Concurrent architecture
///
/// Reads happen on the caller's thread (blocking `read()`). Writes are
/// enqueued via `enqueueWrite()` (thread-safe, non-blocking) and drained
/// by a dedicated writer thread started with `startWriter()`.
///
/// ## Shutdown sequence
///
/// Call `close()` to shut down the socket and signal the writer to stop.
/// After close, `read()` returns 0 and `enqueueWrite()` returns
/// `error.BrokenPipe`. Call `stopWriter()` to join the writer thread and
/// drain remaining queued writes. `deinit()` calls `stopWriter()` then
/// closes the fd if not already closed.
///
/// ## Cross-platform
///
/// Uses `std.Io` for all socket operations, supporting both POSIX and
/// Windows via the Io VTable abstraction.
///
/// ## AF_UNIX sockets: drain mode
///
/// On Linux and macOS, `initWithOptions` reads the socket family once (with
/// `getsockname`). On an AF_UNIX socket, and on any socket whose family it
/// cannot read, the transport runs in drain mode: every read is a `recvmsg`
/// with a control buffer (`rpc.transport.unix.fd_io.recvWithFds`), and every
/// file descriptor the peer attached goes to the process-wide closer thread
/// (`fd_io.closer`), never closed on the reading thread. Each read that
/// brought fds emits a `.resource_rejection` event (`resource =
/// .attached_fds`), and the transport reports `events.Source.unix`.
///
/// Fds still riding on unread messages are closed by the kernel inside the
/// final close of this socket, and on macOS already inside
/// `shutdown(SHUT_RD)`. So `deinit` hands the socket close to the closer
/// thread, and on macOS `shutdown` does its read half there too (the write
/// half stays inline). Neither ever waits for a close that blocks.
///
/// See `fd_io` for why: without drain mode, macOS installs and leaks every fd
/// a local peer sends, and Linux can stall the reader on a lingering socket.
/// TCP sockets read exactly as before, and so does a handle that is not a
/// socket at all (only a test's fake `Io` passes one).
pub const Transport = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    fd: net.Socket.Handle,
    read_buf: []u8,
    observer: ?events.Observer = null,
    /// The `events.Source` this transport reports: `.unix` for an AF_UNIX
    /// socket, `.tcp` otherwise. Set by `initWithOptions`.
    source: events.Source = .tcp,
    /// Drain-mode state; null for a socket read the plain way (see the type
    /// doc). Owned by the transport and freed by `deinit`.
    drain: ?*FdDrain = null,
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    fd_closed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    fd_mu: std.atomic.Mutex = .unlocked,

    // Write queue for concurrent write support
    write_queue: WriteQueue = .{},
    writer_thread: ?std.Thread = null,

    pub const default_max_queued_items: usize = 1024;
    pub const default_max_queued_bytes: usize = 64 * 1024 * 1024;

    pub const ReadError = net.Stream.Reader.Error;
    pub const WriteError = net.Stream.Writer.Error;

    pub const EnqueueError = error{
        BrokenPipe,
        OutOfMemory,
        WriteQueueFull,
        WriteQueueBytesExceeded,
    };

    /// Point-in-time write queue occupancy, for metrics scraping.
    pub const QueueStats = struct {
        items: usize,
        bytes: usize,
        max_items: usize,
        max_bytes: usize,
    };

    pub const Options = struct {
        read_buffer_size: usize,
        write_queue_max_items: usize = default_max_queued_items,
        write_queue_max_bytes: usize = default_max_queued_bytes,
        observer: ?events.Observer = null,
    };

    /// Thread-safe write queue. The queue state and condition wait use the
    /// same mutex so enqueue/close cannot signal between the writer's state
    /// check and its wait registration.
    const WriteQueue = struct {
        mu: std.Io.Mutex = .init,
        cond: std.Io.Condition = .init,
        items: std.ArrayListUnmanaged([]u8) = .empty,
        queued_bytes: usize = 0,
        in_flight_bytes: usize = 0,
        max_items: usize = default_max_queued_items,
        max_bytes: usize = default_max_queued_bytes,
        closed: bool = false,

        /// Queue occupancy before and after a successful enqueue, for
        /// pressure-crossing emission outside the queue lock.
        const EnqueueOutcome = struct {
            prev_items: usize,
            items: usize,
            prev_bytes: usize,
            bytes: usize,
        };

        /// Copy bytes into the queue after checking the configured item and
        /// byte bounds. The copy is deliberately made while the queue lock is
        /// held so a full queue is rejected before allocating.
        fn enqueueCopy(self: *WriteQueue, io: std.Io, allocator: std.mem.Allocator, bytes: []const u8) EnqueueError!EnqueueOutcome {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);

            if (self.closed) {
                return error.BrokenPipe;
            }
            if (self.items.items.len >= self.max_items) {
                return error.WriteQueueFull;
            }
            const accounted_bytes = self.queued_bytes + self.in_flight_bytes;
            if (bytes.len > self.max_bytes or accounted_bytes > self.max_bytes - bytes.len) {
                return error.WriteQueueBytesExceeded;
            }

            const data = allocator.dupe(u8, bytes) catch return error.OutOfMemory;
            errdefer allocator.free(data);
            self.items.append(allocator, data) catch {
                return error.OutOfMemory;
            };
            self.queued_bytes += data.len;
            self.cond.signal(io);
            return .{
                .prev_items = self.items.items.len - 1,
                .items = self.items.items.len,
                .prev_bytes = accounted_bytes,
                .bytes = accounted_bytes + data.len,
            };
        }

        /// Snapshot of queue occupancy (queued plus in-flight bytes).
        fn stats(self: *WriteQueue, io: std.Io) QueueStats {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);
            return .{
                .items = self.items.items.len,
                .bytes = self.queued_bytes + self.in_flight_bytes,
                .max_items = self.max_items,
                .max_bytes = self.max_bytes,
            };
        }

        /// Block until queued items are available or the queue is closed.
        /// Returns null when the queue is closed and empty.
        fn waitForBatch(self: *WriteQueue, io: std.Io) ?std.ArrayListUnmanaged([]u8) {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);

            while (self.items.items.len == 0 and !self.closed) {
                self.cond.waitUncancelable(io, &self.mu);
            }
            if (self.items.items.len == 0) return null;

            const result = self.items;
            for (result.items) |item| {
                self.in_flight_bytes += item.len;
            }
            self.items = .empty;
            self.queued_bytes = 0;
            return result;
        }

        /// Release byte budget for a batch after the writer has freed it.
        fn releaseBatchBytes(self: *WriteQueue, io: std.Io, batch_bytes: usize) void {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);
            self.in_flight_bytes -= batch_bytes;
        }

        /// Mark the queue as closed and wake the writer thread.
        /// Idempotent.
        fn close(self: *WriteQueue, io: std.Io) void {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);
            self.closed = true;
            self.cond.broadcast(io);
        }

        /// Free all remaining queued items and the backing storage.
        /// Idempotent — safe to call multiple times.
        fn drain(self: *WriteQueue, io: std.Io, allocator: std.mem.Allocator) void {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);
            for (self.items.items) |item| {
                allocator.free(item);
            }
            self.items.deinit(allocator);
            // Reinitialize to valid empty state. ArrayListUnmanaged.deinit
            // sets self.* = undefined, so a second drain would crash without
            // this reset.
            self.items = .empty;
            self.queued_bytes = 0;
            self.in_flight_bytes = 0;
        }
    };

    /// Create a transport wrapping the given socket handle.
    ///
    /// Allocates a read buffer of `read_buffer_size` bytes from `allocator`.
    /// The caller must later call `deinit` to free the buffer and close
    /// the socket (if not already closed).
    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        socket: SocketFd,
        read_buffer_size: usize,
    ) !Transport {
        return initWithOptions(allocator, io, socket, .{ .read_buffer_size = read_buffer_size });
    }

    /// Reads the socket family once (see "AF_UNIX sockets: drain mode" on
    /// the type). The only failure is an allocation: the read buffer, and in
    /// drain mode the drain state and its closer-queue reservation.
    pub fn initWithOptions(
        allocator: std.mem.Allocator,
        io: std.Io,
        socket: SocketFd,
        options: Options,
    ) !Transport {
        ignoreSigpipe();
        const buf = try allocator.alloc(u8, options.read_buffer_size);
        errdefer allocator.free(buf);
        var source: events.Source = .tcp;
        var drain: ?*FdDrain = null;
        if (comptime fd_io.supported) {
            switch (socketFamily(socket.handle)) {
                .ip, .not_socket => {},
                .unix => {
                    source = .unix;
                    drain = try FdDrain.create(allocator);
                },
                // recvmsg works on any stream socket, so an unreadable
                // family costs nothing but the drain state.
                .unknown => drain = try FdDrain.create(allocator),
            }
        }
        return .{
            .allocator = allocator,
            .io = io,
            .fd = socket.handle,
            .read_buf = buf,
            .observer = options.observer,
            .source = source,
            .drain = drain,
            .write_queue = .{
                .max_items = options.write_queue_max_items,
                .max_bytes = options.write_queue_max_bytes,
            },
        };
    }

    /// Release the read buffer. Stops the writer thread and closes the
    /// socket if not already closed. In drain mode the close happens on the
    /// closer thread (see the type doc), so this never blocks on it.
    pub fn deinit(self: *Transport) void {
        self.stopWriter();
        self.lockFd();
        defer self.fd_mu.unlock();
        if (!self.fd_closed.swap(true, .acq_rel)) {
            _ = self.close_requested.swap(true, .acq_rel);
            self.closeSocket();
        }
        if (self.drain) |drain| {
            drain.destroy(self.allocator);
            self.drain = null;
        }
        self.allocator.free(self.read_buf);
    }

    /// The final close of the socket. In drain mode it goes to the closer
    /// thread: the kernel closes any fds still riding on unread messages
    /// inside this close, and one of them may block.
    fn closeSocket(self: *Transport) void {
        if (comptime fd_io.supported) {
            if (self.drain) |drain| {
                drain.closeSocket(self.fd);
                return;
            }
        }
        ioClose(self.io, self.fd);
    }

    /// Blocking read into the internal buffer. Returns the number of bytes
    /// read, or 0 on EOF or if the transport is closed.
    ///
    /// In drain mode (AF_UNIX) this is one `recvmsg` with a control buffer,
    /// and every attached fd goes to the closer thread. Two conditions there
    /// close the connection with `error.SystemResources`, each after a
    /// `.resource_rejection` event that names the cause in `err`: the process
    /// fd table stayed full for a read and its one retry
    /// (`error.ProcessFdQuotaExceeded` or `error.SystemFdQuotaExceeded`), or
    /// this read's fds pushed the closer queue past its bound
    /// (`error.FdCloseQueueFull`).
    pub fn read(self: *Transport) ReadError!usize {
        if (self.close_requested.load(.acquire)) return 0;
        if (comptime fd_io.supported) {
            if (self.drain) |drain| return self.readDrain(drain);
        }
        var bufs: [1][]u8 = .{self.read_buf};
        return ioReadVec(self.io, self.fd, &bufs);
    }

    fn readDrain(self: *Transport, drain: *FdDrain) ReadError!usize {
        // A thread must exist before any fd can arrive: received fds are
        // never closed on this one.
        fd_io.closer.ensureStarted() catch |err| {
            log.debug("fd closer thread unavailable: {}", .{err});
            return error.SystemResources;
        };
        // Top the reservation back up, so handing this read's fds to the
        // closer cannot allocate (or fail) while this thread holds them.
        fd_io.closer.reserve(&drain.read_reservation, FdDrain.read_slots) catch return error.SystemResources;

        var fd_quota_retried = false;
        while (true) {
            const got = fd_io.recvWithFds(self.fd, self.read_buf, &drain.control, &drain.fds) catch |err| switch (err) {
                error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded => {
                    // macOS fails the read and installs nothing. The retry
                    // returns the data, and the kernel drops the fds. A
                    // second failure closes the connection: never spin.
                    self.emitAttachedFds(null, null, err);
                    if (fd_quota_retried) return error.SystemResources;
                    fd_quota_retried = true;
                    continue;
                },
                error.ConnectionResetByPeer => return error.ConnectionResetByPeer,
                error.ConnectionTimedOut => return error.ConnectionTimedOut,
                error.SocketUnconnected => return error.SocketUnconnected,
                error.SystemResources => return error.SystemResources,
                error.Unexpected, error.UnixSocketsUnsupported => return error.Unexpected,
            };
            if (got.fd_count != 0) {
                const fds = drain.fds[0..got.fd_count];
                const admission = fd_io.closer.handOff(&drain.read_reservation, fds);
                self.emitAttachedFds(fds.len, 0, error.AttachedFdsRejected);
                if (admission.over_limit) {
                    self.emitAttachedFds(admission.pending, admission.limit, error.FdCloseQueueFull);
                    return error.SystemResources;
                }
            }
            if (got.control_truncated) {
                self.emitAttachedFds(got.fd_count, drain.fds.len, error.AttachedFdsTruncated);
            }
            return got.data_len;
        }
    }

    fn emitAttachedFds(self: *const Transport, attempted: ?usize, limit: ?usize, err: anyerror) void {
        events.emitResourceRejection(self.observer, self.source, .unknown, .attached_fds, attempted, limit, err);
    }

    /// Error set for `readTimeout`: a read plus the deadline's own outcome.
    pub const ReadTimeoutError = ReadError || std.Io.Timeout.Error || std.Io.ConcurrentError;

    /// Blocking read with a DEADLINE, into the internal buffer. Returns
    /// bytes read, 0 on EOF/closed, or `error.Timeout` when the deadline
    /// expires first.
    ///
    /// Use this instead of arming `SO_RCVTIMEO` on the raw fd: a timed-out
    /// `recv` returns EAGAIN, and `Io.Threaded`'s read path classifies
    /// EAGAIN as a programmer bug (`errnoBug`), so a sockopt deadline turns
    /// a normal timeout into a debug-build panic. The deadline belongs to
    /// the Io operation, not to the socket. Windows uses an owned AFD receive
    /// batch when concurrent network reads are unavailable; other backends
    /// race a cancellable read task against the deadline. Every timed path joins
    /// the read and preserves successful completions before returning.
    ///
    /// In drain mode the deadline is a `poll` before the drain-mode `read`.
    pub fn readTimeout(self: *Transport, timeout: std.Io.Timeout) ReadTimeoutError!usize {
        if (self.close_requested.load(.acquire)) return 0;
        if (comptime fd_io.supported) {
            if (self.drain) |drain| {
                try pollReadable(self.io, self.fd, timeout);
                return self.readDrain(drain);
            }
        }
        var bufs: [1][]u8 = .{self.read_buf};
        return ioReadVecTimeout(self.io, self.fd, &bufs, timeout);
    }

    /// Blocking write of all bytes. Retries partial writes until the
    /// entire buffer is sent. Used internally by the writer thread.
    pub fn write(self: *Transport, bytes: []const u8) WriteError!void {
        if (self.close_requested.load(.acquire)) return error.ConnectionResetByPeer;
        var offset: usize = 0;
        while (offset < bytes.len) {
            const n = ioWrite(self.io, self.fd, bytes[offset..]) catch |err| {
                return err;
            };
            if (n == 0) return error.ConnectionResetByPeer;
            offset += n;
        }
    }

    /// Enqueue bytes for asynchronous writing by the writer thread.
    /// Makes an owned copy of `bytes`. Called from the owner thread.
    /// The write queue itself is thread-safe via its internal mutex.
    /// If the async queue would exceed its item or byte bound, this returns
    /// `error.WriteQueueFull` or `error.WriteQueueBytesExceeded` without
    /// closing the transport.
    ///
    /// If the writer thread has not been started yet (i.e., before
    /// `startWriter()`), falls back to a synchronous blocking write.
    /// This supports the common pattern of sending initial messages
    /// (e.g., bootstrap) before entering the read loop.
    pub fn enqueueWrite(self: *Transport, bytes: []const u8) EnqueueError!void {
        if (self.writer_thread == null) {
            // Writer not started yet — write synchronously.
            self.write(bytes) catch return error.BrokenPipe;
            events.emitFrame(self.observer, self.source, .unknown, .sent, bytes.len);
            return;
        }
        const outcome = self.write_queue.enqueueCopy(self.io, self.allocator, bytes) catch |err| {
            switch (err) {
                error.WriteQueueFull => events.emitBackpressure(
                    self.observer,
                    self.source,
                    .unknown,
                    .write_queue_items,
                    bytes.len,
                    self.write_queue.max_items,
                    err,
                ),
                error.WriteQueueBytesExceeded => events.emitBackpressure(
                    self.observer,
                    self.source,
                    .unknown,
                    .write_queue_bytes,
                    bytes.len,
                    self.write_queue.max_bytes,
                    err,
                ),
                else => {},
            }
            return err;
        };
        events.emitPressureCrossing(
            self.observer,
            self.source,
            .unknown,
            .write_queue_items,
            outcome.prev_items,
            outcome.items,
            self.write_queue.max_items,
        );
        events.emitPressureCrossing(
            self.observer,
            self.source,
            .unknown,
            .write_queue_bytes,
            outcome.prev_bytes,
            outcome.bytes,
            self.write_queue.max_bytes,
        );
        events.emitFrame(self.observer, self.source, .unknown, .enqueued, bytes.len);
    }

    /// Point-in-time write queue occupancy. Takes the queue lock briefly;
    /// safe to call from the owner thread for metrics scraping.
    pub fn queueStats(self: *Transport) QueueStats {
        return self.write_queue.stats(self.io);
    }

    /// Spawn the dedicated writer thread. Call before entering the read loop.
    pub fn startWriter(self: *Transport) !void {
        self.writer_thread = try std.Thread.spawn(.{}, writerLoop, .{self});
    }

    /// Stop the writer thread and drain any remaining queued writes.
    /// Idempotent — safe to call multiple times.
    pub fn stopWriter(self: *Transport) void {
        self.shutdown();
        if (self.writer_thread) |t| {
            t.join();
            self.writer_thread = null;
        }
        self.write_queue.drain(self.io, self.allocator);
    }

    /// Writer thread entry point. Blocks on the condition variable,
    /// drains the write queue in batches, and performs blocking writes.
    /// Exits when the queue is closed or on write error.
    fn writerLoop(self: *Transport) void {
        while (true) {
            var batch = self.write_queue.waitForBatch(self.io) orelse break;
            var batch_bytes: usize = 0;
            for (batch.items) |item| {
                batch_bytes += item.len;
            }
            defer self.write_queue.releaseBatchBytes(self.io, batch_bytes);
            defer batch.deinit(self.allocator);

            var write_failed = false;
            for (batch.items) |item| {
                if (!write_failed) {
                    self.write(item) catch |err| {
                        log.debug("writer thread write error: {}", .{err});
                        self.shutdown();
                        write_failed = true;
                    };
                    if (!write_failed) {
                        events.emitFrame(self.observer, self.source, .unknown, .sent, item.len);
                    }
                }
                self.allocator.free(item);
            }
            if (write_failed) break;
        }
    }

    /// Shut down the socket for both reading and writing without closing
    /// the file descriptor. This unblocks any thread currently blocked in
    /// `read()` or `write()` on this socket. Safe to call from any thread.
    ///
    /// In drain mode on macOS the read half runs on the fd closer thread
    /// (see the type doc), so a blocked reader wakes when that thread gets
    /// to it: at once unless it is stuck in a blocking close.
    ///
    /// Also closes the write queue so the writer thread will exit.
    /// The owning thread should subsequently call `deinit()`.
    pub fn shutdown(self: *Transport) void {
        const already_closing = self.close_requested.swap(true, .acq_rel);
        self.write_queue.close(self.io);
        if (already_closing) return;

        self.lockFd();
        defer self.fd_mu.unlock();
        if (!self.fd_closed.load(.acquire)) {
            if (comptime fd_io.supported) {
                if (self.drain) |drain| {
                    drain.shutdownSocket(self.io, self.fd);
                    return;
                }
            }
            ioShutdown(self.io, self.fd);
        }
    }

    /// Shut down the socket and signal the writer to stop. Idempotent.
    /// The fd is not closed here — `deinit()` closes it after the writer
    /// thread has been joined.
    pub fn close(self: *Transport) void {
        const was_closing = self.close_requested.load(.acquire);
        if (!was_closing) {
            log.debug("transport close requested", .{});
        }
        self.shutdown();
    }

    /// Returns `true` if `close()` or `shutdown()` has been called.
    pub fn isClosing(self: *const Transport) bool {
        return self.close_requested.load(.acquire);
    }

    fn lockFd(self: *Transport) void {
        while (!self.fd_mu.tryLock()) std.atomic.spinLoopHint();
    }
};

/// Drain-mode state of one `Transport` (see "AF_UNIX sockets: drain mode" on
/// `Transport`). Created only where `fd_io.supported`.
const FdDrain = struct {
    control: [control_bytes]u8 align(8) = undefined,
    fds: [fd_io.max_fds_per_read]fd_io.Fd = undefined,
    /// Closer-queue capacity held for this transport, one reservation per
    /// thread that may use it, so a hand-off never allocates: one read's
    /// worth of fds (the reader thread; topped up before each read), the
    /// socket shutdown (`Transport.shutdown`, from any thread, once) and the
    /// socket close (`deinit`, once).
    read_reservation: fd_io.closer.Reservation = .{},
    shutdown_reservation: fd_io.closer.Reservation = .{},
    close_reservation: fd_io.closer.Reservation = .{},

    const control_bytes = fd_io.controlSpace(fd_io.max_fds_per_read);
    const read_slots = fd_io.max_fds_per_read;

    /// Darwin disposes of the fds on unread messages inside
    /// `shutdown(SHUT_RD)`, on the calling thread, so the read half of the
    /// shutdown goes to the closer there. The write half stays inline: it
    /// cannot block, and it is what wakes a writer thread parked in a full
    /// send buffer (`deinit` joins that thread). Linux disposes of nothing
    /// in `shutdown` (measured: 0 ms), and an inline shutdown wakes a
    /// blocked reader at once.
    const shutdown_off_thread = builtin.target.os.tag.isDarwin();

    fn create(allocator: std.mem.Allocator) error{OutOfMemory}!*FdDrain {
        const drain = try allocator.create(FdDrain);
        errdefer allocator.destroy(drain);
        drain.* = .{};
        errdefer drain.releaseAll();
        try reserve(&drain.read_reservation, read_slots);
        try reserve(&drain.shutdown_reservation, 1);
        try reserve(&drain.close_reservation, 1);
        return drain;
    }

    fn reserve(r: *fd_io.closer.Reservation, slots: usize) error{OutOfMemory}!void {
        fd_io.closer.reserve(r, slots) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            // Never on a target that creates drain state.
            error.UnixSocketsUnsupported => {},
        };
    }

    fn shutdownSocket(self: *FdDrain, io: std.Io, socket: net.Socket.Handle) void {
        if (comptime shutdown_off_thread) {
            io.vtable.netShutdown(io.userdata, socket, .send) catch {};
            fd_io.closer.handOffShutdown(&self.shutdown_reservation, socket);
        } else {
            ioShutdown(io, socket);
        }
    }

    fn closeSocket(self: *FdDrain, socket: fd_io.Fd) void {
        const fds = [1]fd_io.Fd{socket};
        _ = fd_io.closer.handOff(&self.close_reservation, &fds);
    }

    fn releaseAll(self: *FdDrain) void {
        fd_io.closer.release(&self.read_reservation);
        fd_io.closer.release(&self.shutdown_reservation);
        fd_io.closer.release(&self.close_reservation);
    }

    fn destroy(self: *FdDrain, allocator: std.mem.Allocator) void {
        self.releaseAll();
        allocator.destroy(self);
    }
};

const SocketFamily = enum {
    ip,
    unix,
    /// A socket whose family `getsockname` did not report. Read in drain
    /// mode: `recvmsg` works on any stream socket.
    unknown,
    /// Not a socket, or not an open fd (ENOTSOCK, EBADF). `recvmsg` cannot
    /// read it, so it keeps the plain `std.Io` path; only a test's fake `Io`
    /// hands the transport such a handle.
    not_socket,
};

/// The address family of a socket, from `getsockname`.
fn socketFamily(handle: net.Socket.Handle) SocketFamily {
    var addr: std.posix.sockaddr.storage = undefined;
    var len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.storage);
    const rc = std.posix.system.getsockname(handle, @ptrCast(&addr), &len);
    switch (std.posix.errno(rc)) {
        .SUCCESS => {},
        .NOTSOCK, .BADF => return .not_socket,
        else => return .unknown,
    }
    const family_end = @offsetOf(std.posix.sockaddr.storage, "family") + @sizeOf(std.posix.sa_family_t);
    if (len < family_end) return .unknown;
    return switch (addr.family) {
        std.posix.AF.INET, std.posix.AF.INET6 => .ip,
        std.posix.AF.UNIX => .unix,
        else => .unknown,
    };
}

/// Wait with a deadline until `handle` is readable or hung up (drain-mode
/// `readTimeout`). The socket stays blocking; the deadline is the poll's.
fn pollReadable(io: std.Io, handle: net.Socket.Handle, timeout: std.Io.Timeout) Transport.ReadTimeoutError!void {
    const deadline = timeout.toDeadline(io);
    var fds = [1]std.posix.pollfd{.{ .fd = handle, .events = std.posix.POLL.IN, .revents = 0 }};
    while (true) {
        const remaining = deadline.toDurationFromNow(io) orelse return;
        const ns = remaining.raw.nanoseconds;
        if (ns <= 0) return error.Timeout;
        const ms_wanted = @divFloor(ns + std.time.ns_per_ms - 1, std.time.ns_per_ms);
        const ms: i32 = @intCast(@min(ms_wanted, std.math.maxInt(i32)));
        const rc = std.posix.system.poll(&fds, 1, ms);
        switch (std.posix.errno(rc)) {
            .SUCCESS => if (rc != 0) return,
            .INTR => {},
            .NOMEM => return error.SystemResources,
            else => |err| {
                log.debug("poll failed: errno {d}", .{@backingInt(err)});
                return error.Unexpected;
            },
        }
    }
}

/// Ignore SIGPIPE process-wide so that writing to a broken TCP connection
/// returns EPIPE instead of killing the process. Called from Transport.init;
/// the atomic guard ensures the sigaction call happens at most once.
/// This is standard practice for TCP servers/clients.
fn ignoreSigpipe() void {
    if (comptime builtin.target.os.tag == .windows or builtin.target.os.tag == .freestanding) return;
    const static = struct {
        var done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);
    };
    if (static.done.load(.acquire)) return;
    if (static.done.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;
    const act = std.posix.Sigaction{
        .handler = .{ .handler = std.posix.SIG.IGN },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.PIPE, &act, null);
}

/// Write bytes to a socket handle via Io. Returns bytes written.
///
/// Version-adaptive: zig moved socket writes off the `Io.VTable` and onto
/// the `Operation` union (`netWrite` vtable entry deleted; `net_write`
/// operation added) around 0.17.0-dev.1786. Selecting on the OPERATION's
/// presence — not a version number — keeps one source tree building on
/// both sides of that move, which is what a downstream pinned to a
/// different dev snapshot actually needs. Same shape as `ioReadVec`'s
/// existing `net.Stream.read` preference. Both arms land in
/// `WriteError` exactly: `Stream.Writer.Error = NetWrite.Error ||
/// Cancelable`, and `operate` contributes the `Cancelable` half.
fn ioWrite(io: std.Io, fd: net.Socket.Handle, bytes: []const u8) Transport.WriteError!usize {
    if (bytes.len == 0) return 0;
    const pattern: []const u8 = &.{};
    const data: [1][]const u8 = .{pattern};
    if (comptime @hasField(std.Io.Operation, "net_write")) {
        const result = try io.operate(.{ .net_write = .{
            .socket_handle = fd,
            .header = bytes,
            .data = &data,
            .splat = 0,
        } });
        return result.net_write;
    }
    return io.vtable.netWrite(io.userdata, fd, bytes, &data, 0);
}

/// Submits the `net_read` operation directly rather than calling
/// `net.Stream.read`: at 0.17.0 that std wrapper destructures the
/// operation's new `ReadResult` struct as a tuple, so it stops compiling
/// the moment anything references it.
fn ioReadVec(io: std.Io, fd: net.Socket.Handle, bufs: [][]u8) Transport.ReadError!usize {
    if (comptime @hasField(std.Io.Operation, "net_read")) {
        const result = try io.operate(.{ .net_read = .{
            .socket_handle = fd,
            .data = bufs,
        } });
        return netReadLen(try result.net_read);
    }
    return io.vtable.netRead(io.userdata, fd, bufs);
}

/// Byte count of a completed `net_read`. zig 0.17.0 widened the result from
/// a bare `usize` to `net.Stream.ReadResult` (data plus control lengths).
/// Selecting on the result TYPE, as `ioWrite` selects on the operation,
/// keeps one source tree building on both shapes.
fn netReadLen(result: anytype) usize {
    return if (@TypeOf(result) == usize) result else result.data_len;
}

/// Shut down a socket for both reading and writing via Io. Ignores errors.
fn ioShutdown(io: std.Io, fd: net.Socket.Handle) void {
    io.vtable.netShutdown(io.userdata, fd, .both) catch {};
}

/// Close a socket handle via Io.
fn ioClose(io: std.Io, fd: net.Socket.Handle) void {
    // See runtime.closeFd: netClose now takes `[]const net.Socket`.
    const sockets = [_]net.Socket{.{ .handle = fd, .address = undefined }};
    io.vtable.netClose(io.userdata, &sockets);
}

/// Create a connected loopback TCP pair for tests. Unlike socketpair(2), this
/// works under the same std.Io socket implementation on Windows.
fn createSocketPair() ![2]net.Socket.Handle {
    const io = std.testing.io;
    const listen_addr: net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try net.IpAddress.listen(&listen_addr, io, .{
        .kernel_backlog = 1,
        .reuse_address = true,
    });
    defer server.socket.close(io);

    var connect_addr = server.socket.address;
    const client = try net.IpAddress.connect(&connect_addr, io, .{
        .mode = .stream,
        .protocol = .tcp,
    });
    errdefer client.socket.close(io);

    const accepted = try server.accept(io);
    return .{ client.socket.handle, accepted.socket.handle };
}

/// Vectored read with a deadline, via the Io operation's own timeout.
///
/// Prefer the backend's batched operation support. The pinned Windows
/// Threaded backend cannot submit concurrent net_read batches, so use an
/// owned AFD receive batch there to preserve completions racing cancellation.
fn ioReadVecTimeout(
    io: std.Io,
    fd: net.Socket.Handle,
    bufs: [][]u8,
    timeout: std.Io.Timeout,
) Transport.ReadTimeoutError!usize {
    if (timeout == .none) return ioReadVec(io, fd, bufs);
    const deadline = timeout.toDeadline(io);
    var storage: [1]std.Io.Operation.Storage = undefined;
    var batch: std.Io.Batch = .init(&storage);
    defer batch.cancel(io);
    batch.addAt(0, .{ .net_read = .{
        .socket_handle = fd,
        .data = bufs,
    } });
    batch.awaitConcurrent(io, deadline) catch |err| {
        // The pinned Windows backend releases a rejected net_read slot but
        // leaves its submitted head pointing at that now-unused slot. This
        // batch owns one operation: repair only that fully inactive state,
        // preserving backend userdata for the normal cancellation cleanup.
        if (err == error.ConcurrencyUnavailable and batch.unused.head != .none and
            batch.pending.head == .none and batch.completed.head == .none)
        {
            batch.submitted = .empty;
        }
        // An await error may leave pending work or completed reads. Join it
        // before reusing the buffer and preserve bytes already consumed.
        batch.cancel(io);
        if (batch.next()) |completion| return netReadLen(try completion.result.net_read);
        return switch (err) {
            error.ConcurrencyUnavailable => if (comptime builtin.os.tag == .windows)
                ioReadVecWindowsTimeout(io, fd, bufs, deadline)
            else
                ioReadVecTaskTimeout(io, fd, bufs, deadline),
            else => err,
        };
    };
    const completion = batch.next() orelse return error.Unexpected;
    return netReadLen(try completion.result.net_read);
}

fn ioReadVecWindowsTimeout(io: std.Io, fd: net.Socket.Handle, bufs: [][]u8, deadline: std.Io.Timeout) Transport.ReadTimeoutError!usize {
    const windows = std.os.windows;
    var vectors: [std.Io.Threaded.max_iovecs_len]windows.AFD.WSABUF(.@"var") = undefined;
    var vector_count: u32 = 0;
    buffers: for (bufs) |buf| {
        var remaining = buf;
        while (remaining.len != 0) {
            if (vector_count == vectors.len) break :buffers;
            const len: u32 = @intCast(@min(remaining.len, std.math.maxInt(u32)));
            vectors[vector_count] = .{ .buf = remaining.ptr, .len = len };
            vector_count += 1;
            remaining = remaining[len..];
        }
    }
    if (vector_count == 0) return 0;
    const receive: windows.AFD.RECV_INFO = .{
        .BufferArray = &vectors,
        .BufferCount = vector_count,
        .AfdFlags = .{ .NO_FAST_IO = true, .OVERLAPPED = true },
        .TdiFlags = .{ .NORMAL = true },
    };
    var storage: [1]std.Io.Operation.Storage = undefined;
    var batch: std.Io.Batch = .init(&storage);
    defer cancelWindowsReceive(io, &batch);
    batch.addAt(0, .{ .device_io_control = .{
        .file = .{ .handle = fd, .flags = .{ .nonblocking = true } },
        .code = windows.IOCTL.AFD.RECEIVE,
        .in = std.mem.asBytes(&receive),
    } });
    batch.awaitConcurrent(io, deadline) catch |err| {
        // Ordinary Windows netRead can return Canceled after AFD consumed
        // bytes. Batch cancellation instead retains the final successful
        // IOSB, and keeps receive/vectors alive until the APC has completed.
        cancelWindowsReceive(io, &batch);
        if (batch.next()) |completion| return windowsReadResult(completion.result.device_io_control);
        return err;
    };
    const completion = batch.next() orelse return error.Unexpected;
    return windowsReadResult(completion.result.device_io_control);
}

fn cancelWindowsReceive(io: std.Io, batch: *std.Io.Batch) void {
    if (batch.pending.head != .none) {
        // Pinned Threaded.batchCancel first waits indefinitely for an APC or
        // alert, before issuing NtCancelIoFileEx. A silent receive has neither
        // after its deadline. Wake that initial wait on the submitting thread;
        // cancellation still targets this batch and joins its final APCs.
        const windows = std.os.windows;
        const status = windows.ntdll.NtAlertThread(windows.GetCurrentThread());
        if (status != .SUCCESS) std.debug.panic("cannot wake Windows receive cancellation: NTSTATUS=0x{x}", .{@backingInt(status)});
    }
    batch.cancel(io);
}

fn windowsReadResult(iosb: std.os.windows.IO_STATUS_BLOCK) Transport.ReadError!usize {
    return switch (iosb.u.Status) {
        .SUCCESS => iosb.Information,
        .CANCELLED => error.Canceled,
        .INSUFFICIENT_RESOURCES => error.SystemResources,
        .CONNECTION_RESET => error.ConnectionResetByPeer,
        else => |status| std.os.windows.unexpectedStatus(status),
    };
}

fn ioReadVecTaskTimeout(io: std.Io, fd: net.Socket.Handle, bufs: [][]u8, deadline: std.Io.Timeout) Transport.ReadTimeoutError!usize {
    const Result = union(enum) {
        read: Transport.ReadError!usize,
        deadline: std.Io.Cancelable!void,
    };
    var completed: [2]Result = undefined;
    var tasks = std.Io.Select(Result).init(io, &completed);
    defer tasks.cancelDiscard();
    // Start the timer first: failure to assign either task must not leave a
    // read consuming bytes without a deadline or a caller to receive them.
    try tasks.concurrent(.deadline, std.Io.Timeout.sleep, .{ deadline, io });
    try tasks.concurrent(.read, ioReadVec, .{ io, fd, bufs });
    const selected = tasks.await() catch |err| {
        // Caller cancellation can race a successful read too. Join while the
        // result queue remains open so consumed bytes still reach the caller.
        while (tasks.cancel()) |pending| switch (pending) {
            .read => |read| return read,
            .deadline => {},
        };
        return err;
    };
    switch (selected) {
        .read => |result| return result,
        .deadline => |result| {
            try result;
            // Cancellation races completion. Preserve bytes (or EOF) if the
            // read finished before its cancellation was observed.
            while (tasks.cancel()) |pending| switch (pending) {
                .read => |read| return read catch |err| switch (err) {
                    error.Canceled => return error.Timeout,
                    else => return err,
                },
                .deadline => {},
            };
            return error.Timeout;
        },
    }
}

/// Read from a socket handle via Io into a buffer.
fn ioRead(io: std.Io, fd: net.Socket.Handle, buf: []u8) Transport.ReadError!usize {
    var bufs: [1][]u8 = .{buf};
    return ioReadVec(io, fd, &bufs);
}

test "transport init and deinit" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    try std.testing.expect(!transport.isClosing());
    transport.deinit();
}

test "transport read returns data written to peer" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    // Write data into the other end of the socketpair.
    _ = try ioWrite(std.testing.io, pair[1], "hello");
    const n = try transport.read();
    try std.testing.expectEqualStrings("hello", transport.read_buf[0..n]);
}

test "transport read returns 0 after close" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expect(transport.isClosing());
    const n = try transport.read();
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "transport write sends data to peer" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    try transport.write("world");

    var buf: [64]u8 = undefined;
    const n = try ioRead(std.testing.io, pair[1], &buf);
    try std.testing.expectEqualStrings("world", buf[0..n]);
}

test "transport write after close returns error" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expectError(error.ConnectionResetByPeer, transport.write("fail"));
}

test "transport read returns 0 on peer close (EOF)" {
    const pair = try createSocketPair();

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    // Close the peer end — transport should see EOF.
    ioClose(std.testing.io, pair[1]);
    const n = try transport.read();
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "transport isClosing tracks close state" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    try std.testing.expect(!transport.isClosing());
    transport.shutdown();
    try std.testing.expect(transport.isClosing());
}

test "transport close is idempotent" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    transport.close();
    transport.close(); // should not panic or double-close
    try std.testing.expect(transport.isClosing());
}

test "transport shutdown then deinit does not double-close" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    transport.shutdown();
    transport.deinit(); // should close fd and free buffer without error
}

test "transport shutdown after deinit observes closed fd guard" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    transport.deinit();
    transport.shutdown();
    try std.testing.expect(transport.isClosing());
}

test "transport enqueue write delivers data to peer" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    try transport.startWriter();

    try transport.enqueueWrite("hello");

    // Read from the peer end — the writer thread should have delivered it.
    var buf: [64]u8 = undefined;
    const n = try ioRead(std.testing.io, pair[1], &buf);
    try std.testing.expectEqualStrings("hello", buf[0..n]);

    transport.stopWriter();
}

test "transport enqueue write delivers multiple frames in order" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    try transport.startWriter();

    try transport.enqueueWrite("aaa");
    try transport.enqueueWrite("bbb");
    try transport.enqueueWrite("ccc");

    // Read all data from the peer end.
    var buf: [64]u8 = undefined;
    var total: usize = 0;
    while (total < 9) {
        const n = try ioRead(std.testing.io, pair[1], buf[total..]);
        if (n == 0) break;
        total += n;
    }
    try std.testing.expectEqualStrings("aaabbbccc", buf[0..total]);

    transport.stopWriter();
}

test "transport enqueue write after close returns error" {
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, .{ .handle = pair[0] }, 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expectError(error.BrokenPipe, transport.enqueueWrite("fail"));
}
