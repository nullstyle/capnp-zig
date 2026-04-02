const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_transport);
const net = std.Io.net;

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
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    fd_closed: bool = false,

    // Write queue for concurrent write support
    write_queue: WriteQueue = .{},
    writer_thread: ?std.Thread = null,

    pub const ReadError = net.Stream.Reader.Error;
    pub const WriteError = net.Stream.Writer.Error;

    pub const EnqueueError = error{
        BrokenPipe,
        OutOfMemory,
    };

    /// Thread-safe write queue. Uses a spinlock for the data structure and
    /// a condition variable for blocking/waking the writer thread.
    const WriteQueue = struct {
        mu: std.atomic.Mutex = .unlocked,
        items: std.ArrayListUnmanaged([]u8) = .empty,
        closed: bool = false,
        /// Condition variable and mutex for writer thread notification.
        cond: std.Io.Condition = .init,
        cond_mu: std.Io.Mutex = .init,

        fn lock(self: *WriteQueue) void {
            while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        }

        /// Push an owned allocation onto the queue. Frees `data` and returns
        /// `error.BrokenPipe` if the queue is already closed.
        fn enqueue(self: *WriteQueue, io: std.Io, allocator: std.mem.Allocator, data: []u8) EnqueueError!void {
            self.lock();
            defer self.mu.unlock();
            if (self.closed) {
                allocator.free(data);
                return error.BrokenPipe;
            }
            self.items.append(allocator, data) catch {
                allocator.free(data);
                return error.OutOfMemory;
            };
            // Signal the writer thread.
            self.cond.signal(io);
        }

        /// Block until items are available or the queue is closed.
        /// Returns false when the queue is closed and empty.
        fn waitForSignal(self: *WriteQueue, io: std.Io) bool {
            self.cond_mu.lockUncancelable(io);
            defer self.cond_mu.unlock(io);
            while (true) {
                self.lock();
                const has_items = self.items.items.len > 0;
                const is_closed = self.closed;
                self.mu.unlock();
                if (has_items) return true;
                if (is_closed) return false;
                self.cond.waitUncancelable(io, &self.cond_mu);
            }
        }

        /// Take all queued items at once. Returns null if the queue is empty.
        fn takeBatch(self: *WriteQueue) ?std.ArrayListUnmanaged([]u8) {
            self.lock();
            defer self.mu.unlock();
            if (self.items.items.len == 0) return null;
            const result = self.items;
            self.items = .empty;
            return result;
        }

        /// Mark the queue as closed and wake the writer thread.
        /// Idempotent.
        fn close(self: *WriteQueue, io: std.Io) void {
            self.lock();
            self.closed = true;
            self.mu.unlock();
            self.cond.broadcast(io);
        }

        /// Free all remaining queued items and the backing storage.
        /// Idempotent — safe to call multiple times.
        fn drain(self: *WriteQueue, allocator: std.mem.Allocator) void {
            self.lock();
            defer self.mu.unlock();
            for (self.items.items) |item| {
                allocator.free(item);
            }
            self.items.deinit(allocator);
            // Reinitialize to valid empty state. ArrayListUnmanaged.deinit
            // sets self.* = undefined, so a second drain would crash without
            // this reset.
            self.items = .empty;
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
        fd: net.Socket.Handle,
        read_buffer_size: usize,
    ) !Transport {
        ignoreSigpipe();
        const buf = try allocator.alloc(u8, read_buffer_size);
        return .{
            .allocator = allocator,
            .io = io,
            .fd = fd,
            .read_buf = buf,
        };
    }

    /// Release the read buffer. Stops the writer thread and closes the
    /// socket if not already closed.
    pub fn deinit(self: *Transport) void {
        self.stopWriter();
        if (!self.fd_closed) {
            self.fd_closed = true;
            self.close_requested.store(true, .release);
            ioClose(self.io, self.fd);
        }
        self.allocator.free(self.read_buf);
    }

    /// Blocking read into the internal buffer. Returns the number of bytes
    /// read, or 0 on EOF or if the transport is closed.
    pub fn read(self: *Transport) ReadError!usize {
        if (self.close_requested.load(.acquire)) return 0;
        var bufs: [1][]u8 = .{self.read_buf};
        return self.io.vtable.netRead(self.io.userdata, self.fd, &bufs);
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
    ///
    /// If the writer thread has not been started yet (i.e., before
    /// `startWriter()`), falls back to a synchronous blocking write.
    /// This supports the common pattern of sending initial messages
    /// (e.g., bootstrap) before entering the read loop.
    pub fn enqueueWrite(self: *Transport, bytes: []const u8) EnqueueError!void {
        if (self.writer_thread == null) {
            // Writer not started yet — write synchronously.
            self.write(bytes) catch return error.BrokenPipe;
            return;
        }
        const owned = self.allocator.dupe(u8, bytes) catch return error.OutOfMemory;
        self.write_queue.enqueue(self.io, self.allocator, owned) catch |err| {
            return err;
        };
    }

    /// Spawn the dedicated writer thread. Call before entering the read loop.
    pub fn startWriter(self: *Transport) !void {
        self.writer_thread = try std.Thread.spawn(.{}, writerLoop, .{self});
    }

    /// Stop the writer thread and drain any remaining queued writes.
    /// Idempotent — safe to call multiple times.
    pub fn stopWriter(self: *Transport) void {
        self.write_queue.close(self.io);
        if (self.writer_thread) |t| {
            t.join();
            self.writer_thread = null;
        }
        self.write_queue.drain(self.allocator);
    }

    /// Writer thread entry point. Blocks on the condition variable,
    /// drains the write queue in batches, and performs blocking writes.
    /// Exits when the queue is closed or on write error.
    fn writerLoop(self: *Transport) void {
        while (true) {
            // Block until signaled by enqueue or close.
            if (!self.write_queue.waitForSignal(self.io)) break;

            var batch = self.write_queue.takeBatch() orelse continue;
            defer batch.deinit(self.allocator);

            var write_failed = false;
            for (batch.items) |item| {
                if (!write_failed) {
                    self.write(item) catch |err| {
                        log.debug("writer thread write error: {}", .{err});
                        self.close_requested.store(true, .release);
                        self.write_queue.close(self.io);
                        write_failed = true;
                    };
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
        if (self.fd_closed) return;
        self.close_requested.store(true, .release);
        self.write_queue.close(self.io);
        ioShutdown(self.io, self.fd);
    }

    /// Shut down the socket and signal the writer to stop. Idempotent.
    /// The fd is not closed here — `deinit()` closes it after the writer
    /// thread has been joined.
    pub fn close(self: *Transport) void {
        if (self.close_requested.load(.acquire)) return;
        log.debug("transport close requested", .{});
        self.close_requested.store(true, .release);
        self.write_queue.close(self.io);
        ioShutdown(self.io, self.fd);
    }

    /// Returns `true` if `close()` or `shutdown()` has been called.
    pub fn isClosing(self: *const Transport) bool {
        return self.close_requested.load(.acquire);
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
fn ioWrite(io: std.Io, fd: net.Socket.Handle, bytes: []const u8) Transport.WriteError!usize {
    if (bytes.len == 0) return 0;
    var data: [1][]const u8 = .{bytes};
    return io.vtable.netWrite(io.userdata, fd, &.{}, &data, 0);
}

/// Shut down a socket for both reading and writing via Io. Ignores errors.
fn ioShutdown(io: std.Io, fd: net.Socket.Handle) void {
    io.vtable.netShutdown(io.userdata, fd, .both) catch {};
}

/// Close a socket handle via Io.
fn ioClose(io: std.Io, fd: net.Socket.Handle) void {
    io.vtable.netClose(io.userdata, (&fd)[0..1]);
}

/// Create a UNIX socketpair for testing. POSIX-only.
fn createSocketPair() !struct { [2]std.posix.fd_t } {
    if (comptime builtin.target.os.tag == .windows) return error.SocketPairFailed;
    var fds: [2]std.posix.fd_t = undefined;
    if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
        return error.SocketPairFailed;
    }
    return .{fds};
}

/// Read from a socket handle via Io into a buffer.
fn ioRead(io: std.Io, fd: net.Socket.Handle, buf: []u8) Transport.ReadError!usize {
    var bufs: [1][]u8 = .{buf};
    return io.vtable.netRead(io.userdata, fd, &bufs);
}

test "transport init and deinit" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    try std.testing.expect(!transport.isClosing());
    transport.deinit();
}

test "transport read returns data written to peer" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    defer transport.deinit();

    // Write data into the other end of the socketpair.
    _ = try ioWrite(std.testing.io, pair[0][1], "hello");
    const n = try transport.read();
    try std.testing.expectEqualStrings("hello", transport.read_buf[0..n]);
}

test "transport read returns 0 after close" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expect(transport.isClosing());
    const n = try transport.read();
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "transport write sends data to peer" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    defer transport.deinit();

    try transport.write("world");

    var buf: [64]u8 = undefined;
    const n = try ioRead(std.testing.io, pair[0][1], &buf);
    try std.testing.expectEqualStrings("world", buf[0..n]);
}

test "transport write after close returns error" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expectError(error.ConnectionResetByPeer, transport.write("fail"));
}

test "transport read returns 0 on peer close (EOF)" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    defer transport.deinit();

    // Close the peer end — transport should see EOF.
    ioClose(std.testing.io, pair[0][1]);
    const n = try transport.read();
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "transport isClosing tracks close state" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    defer transport.deinit();

    try std.testing.expect(!transport.isClosing());
    transport.shutdown();
    try std.testing.expect(transport.isClosing());
}

test "transport close is idempotent" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    defer transport.deinit();

    transport.close();
    transport.close(); // should not panic or double-close
    try std.testing.expect(transport.isClosing());
}

test "transport shutdown then deinit does not double-close" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    transport.shutdown();
    transport.deinit(); // should close fd and free buffer without error
}

test "transport enqueue write delivers data to peer" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    defer transport.deinit();

    try transport.startWriter();

    try transport.enqueueWrite("hello");

    // Read from the peer end — the writer thread should have delivered it.
    var buf: [64]u8 = undefined;
    const n = try ioRead(std.testing.io, pair[0][1], &buf);
    try std.testing.expectEqualStrings("hello", buf[0..n]);

    transport.stopWriter();
}

test "transport enqueue write delivers multiple frames in order" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    defer transport.deinit();

    try transport.startWriter();

    try transport.enqueueWrite("aaa");
    try transport.enqueueWrite("bbb");
    try transport.enqueueWrite("ccc");

    // Read all data from the peer end.
    var buf: [64]u8 = undefined;
    var total: usize = 0;
    while (total < 9) {
        const n = try ioRead(std.testing.io, pair[0][1], buf[total..]);
        if (n == 0) break;
        total += n;
    }
    try std.testing.expectEqualStrings("aaabbbccc", buf[0..total]);

    transport.stopWriter();
}

test "transport enqueue write after close returns error" {
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
    const pair = try createSocketPair();
    defer ioClose(std.testing.io, pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, std.testing.io, pair[0][0], 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expectError(error.BrokenPipe, transport.enqueueWrite("fail"));
}
