const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_transport);

/// Sentinel for "no fd". POSIX uses -1; Windows uses INVALID_HANDLE_VALUE.
const invalid_fd: std.posix.fd_t = if (builtin.target.os.tag == .windows)
    @ptrFromInt(std.math.maxInt(usize))
else
    -1;

/// TCP transport layer with concurrent read/write support.
///
/// `Transport` owns a TCP socket file descriptor and provides blocking
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
/// To interrupt a blocked `read()` from another thread, call `shutdown()`
/// which performs a socket shutdown without closing the fd. The owning
/// thread should then call `close()` or `deinit()`.
pub const Transport = struct {
    allocator: std.mem.Allocator,
    fd: std.posix.fd_t,
    read_buf: []u8,
    close_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    fd_closed: bool = false,

    // Write queue for concurrent write support
    write_queue: WriteQueue = .{},
    writer_thread: ?std.Thread = null,

    pub const ReadError = std.posix.ReadError;
    pub const WriteError = error{
        BrokenPipe,
        ConnectionResetByPeer,
        SystemResources,
        Unexpected,
    };

    pub const EnqueueError = error{
        BrokenPipe,
        OutOfMemory,
    };

    /// Thread-safe write queue. Uses a spinlock for the data structure and
    /// a socketpair for blocking/waking the writer thread.
    const WriteQueue = struct {
        mu: std.atomic.Mutex = .unlocked,
        items: std.ArrayListUnmanaged([]u8) = .{},
        closed: bool = false,
        /// Notification socketpair. [0]=read (writer thread), [1]=write (enqueue).
        /// Set to -1 when not initialized or after close.
        notify_fds: [2]std.posix.fd_t = .{ invalid_fd, invalid_fd },

        fn initNotify(self: *WriteQueue) !void {
            if (builtin.target.os.tag == .windows) return error.SystemResources;
            var fds: [2]std.posix.fd_t = undefined;
            if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
                return error.SystemResources;
            }
            self.notify_fds = fds;
        }

        fn deinitNotify(self: *WriteQueue) void {
            if (self.notify_fds[0] != invalid_fd) {
                closeFd(self.notify_fds[0]);
                self.notify_fds[0] = invalid_fd;
            }
            if (self.notify_fds[1] != invalid_fd) {
                closeFd(self.notify_fds[1]);
                self.notify_fds[1] = invalid_fd;
            }
        }

        fn lock(self: *WriteQueue) void {
            while (!self.mu.tryLock()) std.atomic.spinLoopHint();
        }

        /// Push an owned allocation onto the queue. Frees `data` and returns
        /// `error.BrokenPipe` if the queue is already closed.
        fn enqueue(self: *WriteQueue, allocator: std.mem.Allocator, data: []u8) EnqueueError!void {
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
            if (builtin.target.os.tag != .windows) {
                _ = std.posix.system.write(self.notify_fds[1], &[_]u8{1}, 1);
            }
        }

        /// Block until the notification pipe is readable. Returns false on
        /// EOF (pipe closed — queue is shutting down).
        fn waitForSignal(self: *WriteQueue) bool {
            if (builtin.target.os.tag == .windows) return false;
            var buf: [64]u8 = undefined;
            while (true) {
                const rc = std.posix.system.read(self.notify_fds[0], &buf, buf.len);
                if (rc > 0) return true;
                if (rc == 0) return false; // EOF — close was called
                const e = std.posix.errno(rc);
                if (e == .INTR) continue;
                return false; // error
            }
        }

        /// Take all queued items at once. Returns null if the queue is empty.
        fn takeBatch(self: *WriteQueue) ?std.ArrayListUnmanaged([]u8) {
            self.lock();
            defer self.mu.unlock();
            if (self.items.items.len == 0) return null;
            const result = self.items;
            self.items = .{};
            return result;
        }

        /// Mark the queue as closed and wake the writer thread by closing
        /// the notification pipe write end. Idempotent.
        fn close(self: *WriteQueue) void {
            self.lock();
            const was_closed = self.closed;
            self.closed = true;
            self.mu.unlock();
            if (!was_closed and self.notify_fds[1] != invalid_fd) {
                closeFd(self.notify_fds[1]);
                self.notify_fds[1] = invalid_fd;
            }
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
            self.items = .{};
        }
    };

    /// Create a transport wrapping the given socket fd.
    ///
    /// Allocates a read buffer of `read_buffer_size` bytes from `allocator`.
    /// The caller must later call `deinit` to free the buffer and close
    /// the socket (if not already closed).
    pub fn init(
        allocator: std.mem.Allocator,
        fd: std.posix.fd_t,
        read_buffer_size: usize,
    ) !Transport {
        ignoreSigpipe();
        const buf = try allocator.alloc(u8, read_buffer_size);
        return .{
            .allocator = allocator,
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
            closeFd(self.fd);
        }
        self.allocator.free(self.read_buf);
    }

    /// Blocking read into the internal buffer. Returns the number of bytes
    /// read, or 0 on EOF or if the transport is closed.
    pub fn read(self: *Transport) ReadError!usize {
        if (self.close_requested.load(.acquire)) return 0;
        return std.posix.read(self.fd, self.read_buf);
    }

    /// Blocking write of all bytes. Retries partial writes until the
    /// entire buffer is sent. Used internally by the writer thread.
    pub fn write(self: *Transport, bytes: []const u8) WriteError!void {
        if (self.close_requested.load(.acquire)) return error.BrokenPipe;
        var offset: usize = 0;
        while (offset < bytes.len) {
            const n = sysWrite(self.fd, bytes[offset..]) catch |err| {
                return err;
            };
            if (n == 0) return error.BrokenPipe;
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
        self.write_queue.enqueue(self.allocator, owned) catch |err| {
            return err;
        };
    }

    /// Spawn the dedicated writer thread. Call before entering the read loop.
    pub fn startWriter(self: *Transport) !void {
        try self.write_queue.initNotify();
        errdefer self.write_queue.deinitNotify();
        self.writer_thread = try std.Thread.spawn(.{}, writerLoop, .{self});
    }

    /// Stop the writer thread and drain any remaining queued writes.
    /// Idempotent — safe to call multiple times.
    pub fn stopWriter(self: *Transport) void {
        self.write_queue.close();
        if (self.writer_thread) |t| {
            t.join();
            self.writer_thread = null;
        }
        self.write_queue.drain(self.allocator);
        self.write_queue.deinitNotify();
    }

    /// Writer thread entry point. Blocks on the notification pipe,
    /// drains the write queue in batches, and performs blocking writes.
    /// Exits when the pipe is closed or on write error.
    fn writerLoop(self: *Transport) void {
        while (true) {
            // Block until signaled by enqueue or close.
            if (!self.write_queue.waitForSignal()) break;

            var batch = self.write_queue.takeBatch() orelse continue;
            defer batch.deinit(self.allocator);

            var write_failed = false;
            for (batch.items) |item| {
                if (!write_failed) {
                    self.write(item) catch |err| {
                        log.debug("writer thread write error: {}", .{err});
                        self.close_requested.store(true, .release);
                        self.write_queue.close();
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
        self.write_queue.close();
        sysShutdown(self.fd);
    }

    /// Shut down the socket and signal the writer to stop. Idempotent.
    /// The fd is not closed here — `deinit()` closes it after the writer
    /// thread has been joined.
    pub fn close(self: *Transport) void {
        if (self.close_requested.load(.acquire)) return;
        log.debug("transport close requested", .{});
        self.close_requested.store(true, .release);
        self.write_queue.close();
        sysShutdown(self.fd);
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

/// Close a file descriptor, tolerating EINTR and EBADF.
fn closeFd(fd: std.posix.fd_t) void {
    if (builtin.target.os.tag == .windows) return;
    switch (std.posix.errno(std.posix.system.close(fd))) {
        .SUCCESS, .INTR, .BADF => {},
        else => {},
    }
}

/// Blocking write using the raw system call. Returns bytes written.
fn sysWrite(fd: std.posix.fd_t, bytes: []const u8) Transport.WriteError!usize {
    if (builtin.target.os.tag == .windows) return error.Unexpected;
    if (bytes.len == 0) return 0;
    const max_count: usize = switch (builtin.target.os.tag) {
        .linux => 0x7ffff000,
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => @intCast(std.math.maxInt(i32)),
        else => @intCast(std.math.maxInt(isize)),
    };
    while (true) {
        const rc = std.posix.system.write(fd, bytes.ptr, @min(bytes.len, max_count));
        switch (std.posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .PIPE => return error.BrokenPipe,
            .CONNRESET => return error.ConnectionResetByPeer,
            .NOBUFS, .NOMEM => return error.SystemResources,
            else => return error.Unexpected,
        }
    }
}

/// Shut down a socket for both reading and writing. Ignores errors.
fn sysShutdown(fd: std.posix.fd_t) void {
    if (builtin.target.os.tag == .windows) return;
    _ = std.posix.system.shutdown(fd, std.posix.SHUT.RDWR);
}

/// Create a UNIX socketpair for testing.
fn createSocketPair() !struct { [2]std.posix.fd_t } {
    if (builtin.target.os.tag == .windows) return error.SocketPairFailed;
    var fds: [2]std.posix.fd_t = undefined;
    if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
        return error.SocketPairFailed;
    }
    return .{fds};
}

test "transport init and deinit" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    try std.testing.expect(!transport.isClosing());
    transport.deinit();
}

test "transport read returns data written to peer" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    defer transport.deinit();

    // Write data into the other end of the socketpair.
    _ = try sysWrite(pair[0][1], "hello");
    const n = try transport.read();
    try std.testing.expectEqualStrings("hello", transport.read_buf[0..n]);
}

test "transport read returns 0 after close" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expect(transport.isClosing());
    const n = try transport.read();
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "transport write sends data to peer" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    defer transport.deinit();

    try transport.write("world");

    var buf: [64]u8 = undefined;
    const n = try std.posix.read(pair[0][1], &buf);
    try std.testing.expectEqualStrings("world", buf[0..n]);
}

test "transport write after close returns error" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expectError(error.BrokenPipe, transport.write("fail"));
}

test "transport read returns 0 on peer close (EOF)" {
    const pair = try createSocketPair();

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    defer transport.deinit();

    // Close the peer end — transport should see EOF.
    closeFd(pair[0][1]);
    const n = try transport.read();
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "transport isClosing tracks close state" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    defer transport.deinit();

    try std.testing.expect(!transport.isClosing());
    transport.shutdown();
    try std.testing.expect(transport.isClosing());
}

test "transport close is idempotent" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    defer transport.deinit();

    transport.close();
    transport.close(); // should not panic or double-close
    try std.testing.expect(transport.isClosing());
}

test "transport shutdown then deinit does not double-close" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    transport.shutdown();
    transport.deinit(); // should close fd and free buffer without error
}

test "transport enqueue write delivers data to peer" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    defer transport.deinit();

    try transport.startWriter();

    try transport.enqueueWrite("hello");

    // Read from the peer end — the writer thread should have delivered it.
    var buf: [64]u8 = undefined;
    const n = try std.posix.read(pair[0][1], &buf);
    try std.testing.expectEqualStrings("hello", buf[0..n]);

    transport.stopWriter();
}

test "transport enqueue write delivers multiple frames in order" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    defer transport.deinit();

    try transport.startWriter();

    try transport.enqueueWrite("aaa");
    try transport.enqueueWrite("bbb");
    try transport.enqueueWrite("ccc");

    // Read all data from the peer end.
    var buf: [64]u8 = undefined;
    var total: usize = 0;
    while (total < 9) {
        const n = try std.posix.read(pair[0][1], buf[total..]);
        if (n == 0) break;
        total += n;
    }
    try std.testing.expectEqualStrings("aaabbbccc", buf[0..total]);

    transport.stopWriter();
}

test "transport enqueue write after close returns error" {
    const pair = try createSocketPair();
    defer closeFd(pair[0][1]);

    var transport = try Transport.init(std.testing.allocator, pair[0][0], 64);
    defer transport.deinit();

    transport.close();
    try std.testing.expectError(error.BrokenPipe, transport.enqueueWrite("fail"));
}
