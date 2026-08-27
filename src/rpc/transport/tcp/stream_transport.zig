const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_transport);
const net = std.Io.net;
const events = @import("../../events.zig");

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
pub const Transport = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    fd: net.Socket.Handle,
    read_buf: []u8,
    observer: ?events.Observer = null,
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

    pub fn initWithOptions(
        allocator: std.mem.Allocator,
        io: std.Io,
        socket: SocketFd,
        options: Options,
    ) !Transport {
        ignoreSigpipe();
        const buf = try allocator.alloc(u8, options.read_buffer_size);
        return .{
            .allocator = allocator,
            .io = io,
            .fd = socket.handle,
            .read_buf = buf,
            .observer = options.observer,
            .write_queue = .{
                .max_items = options.write_queue_max_items,
                .max_bytes = options.write_queue_max_bytes,
            },
        };
    }

    /// Release the read buffer. Stops the writer thread and closes the
    /// socket if not already closed.
    pub fn deinit(self: *Transport) void {
        self.stopWriter();
        self.lockFd();
        defer self.fd_mu.unlock();
        if (!self.fd_closed.swap(true, .acq_rel)) {
            _ = self.close_requested.swap(true, .acq_rel);
            ioClose(self.io, self.fd);
        }
        self.allocator.free(self.read_buf);
    }

    /// Blocking read into the internal buffer. Returns the number of bytes
    /// read, or 0 on EOF or if the transport is closed.
    pub fn read(self: *Transport) ReadError!usize {
        if (self.close_requested.load(.acquire)) return 0;
        var bufs: [1][]u8 = .{self.read_buf};
        return ioReadVec(self.io, self.fd, &bufs);
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
    /// the Io operation, not to the socket — `operateTimeout` cancels the
    /// operation itself, so no EAGAIN ever reaches the read path.
    pub fn readTimeout(self: *Transport, timeout: std.Io.Timeout) ReadTimeoutError!usize {
        if (self.close_requested.load(.acquire)) return 0;
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
            events.emitFrame(self.observer, .tcp, .unknown, .sent, bytes.len);
            return;
        }
        const outcome = self.write_queue.enqueueCopy(self.io, self.allocator, bytes) catch |err| {
            switch (err) {
                error.WriteQueueFull => events.emitBackpressure(
                    self.observer,
                    .tcp,
                    .unknown,
                    .write_queue_items,
                    bytes.len,
                    self.write_queue.max_items,
                    err,
                ),
                error.WriteQueueBytesExceeded => events.emitBackpressure(
                    self.observer,
                    .tcp,
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
            .tcp,
            .unknown,
            .write_queue_items,
            outcome.prev_items,
            outcome.items,
            self.write_queue.max_items,
        );
        events.emitPressureCrossing(
            self.observer,
            .tcp,
            .unknown,
            .write_queue_bytes,
            outcome.prev_bytes,
            outcome.bytes,
            self.write_queue.max_bytes,
        );
        events.emitFrame(self.observer, .tcp, .unknown, .enqueued, bytes.len);
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
                        events.emitFrame(self.observer, .tcp, .unknown, .sent, item.len);
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
    /// Also closes the write queue so the writer thread will exit.
    /// The owning thread should subsequently call `deinit()`.
    pub fn shutdown(self: *Transport) void {
        const already_closing = self.close_requested.swap(true, .acq_rel);
        self.write_queue.close(self.io);
        if (already_closing) return;

        self.lockFd();
        defer self.fd_mu.unlock();
        if (!self.fd_closed.load(.acquire)) {
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

fn ioReadVec(io: std.Io, fd: net.Socket.Handle, bufs: [][]u8) Transport.ReadError!usize {
    if (comptime @hasDecl(net.Stream, "read")) {
        var stream = net.Stream{ .socket = .{ .handle = fd, .address = undefined } };
        return stream.read(io, bufs);
    }
    return io.vtable.netRead(io.userdata, fd, bufs);
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
/// `net_read` is an `Io.Operation` on every std this tree supports, and
/// `operateTimeout` cancels the operation when the deadline expires — the
/// portable alternative to per-fd `SO_RCVTIMEO` games (see
/// `Transport.readTimeout` for why those are a trap).
fn ioReadVecTimeout(
    io: std.Io,
    fd: net.Socket.Handle,
    bufs: [][]u8,
    timeout: std.Io.Timeout,
) Transport.ReadTimeoutError!usize {
    const result = try io.operateTimeout(.{ .net_read = .{
        .socket_handle = fd,
        .data = bufs,
    } }, timeout);
    return result.net_read;
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
