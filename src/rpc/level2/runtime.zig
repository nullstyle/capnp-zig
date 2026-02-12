const std = @import("std");
const builtin = @import("builtin");
const log = std.log.scoped(.rpc_runtime);
const xev = @import("xev").Dynamic;
const Connection = @import("connection.zig").Connection;

/// The RPC event-loop runtime.
///
/// Wraps a libxev `Loop` and provides the single-threaded execution context
/// for all RPC I/O. All `Peer`, `Connection`, and `Transport` operations
/// associated with this runtime **must** be called from the thread that
/// created the `Runtime` (i.e., the thread that calls `run`). In debug
/// builds, key entry points assert this invariant.
pub const Runtime = struct {
    allocator: std.mem.Allocator,
    loop: xev.Loop,

    /// Thread ID captured at creation time. Used by debug-mode assertions
    /// to verify single-threaded access. Initialized to 0 and set to the
    /// real thread ID in `init`.
    owner_thread_id: std.Thread.Id = 0,

    /// Protects one-time backend detection for dynamic xev mode.
    var backend_detect_mutex: std.Thread.Mutex = .{};
    var backend_detected: bool = false;
    var backend_detect_error: ?anyerror = null;

    /// Ensure the xev backend is selected once process-wide before using
    /// any loop/socket types. In dynamic mode this picks the preferred
    /// available backend (io_uring first, then epoll on Linux).
    pub fn ensureBackend() !void {
        if (comptime !xev.dynamic) return;

        backend_detect_mutex.lock();
        defer backend_detect_mutex.unlock();

        if (backend_detected) return;
        if (backend_detect_error) |err| return err;

        xev.detect() catch |err| {
            backend_detect_error = err;
            return err;
        };
        backend_detected = true;
    }

    /// Assert that the caller is on the thread that created this runtime.
    /// No-op in release builds.
    fn assertThreadAffinity(self: *const Runtime) void {
        if (comptime builtin.target.os.tag == .freestanding) return;
        if (builtin.mode == .Debug) {
            const current = std.Thread.getCurrentId();
            if (current != self.owner_thread_id) {
                @panic("Runtime method called from wrong thread: the event loop must be driven from its owner thread");
            }
        }
    }

    /// Create a new runtime with a fresh xev event loop.
    ///
    /// The calling thread becomes the owner thread for thread-affinity
    /// checks.
    pub fn init(allocator: std.mem.Allocator) !Runtime {
        try Runtime.ensureBackend();
        const loop = try xev.Loop.init(.{});
        return .{
            .allocator = allocator,
            .loop = loop,
            .owner_thread_id = if (comptime builtin.target.os.tag == .freestanding) 0 else std.Thread.getCurrentId(),
        };
    }

    /// Tear down the event loop. Must be called from the owner thread.
    pub fn deinit(self: *Runtime) void {
        self.assertThreadAffinity();
        self.loop.deinit();
    }

    /// Drive the event loop. Must be called from the owner thread.
    ///
    /// `mode` controls whether to block waiting for events (`.until_done`)
    /// or return immediately after processing ready events (`.no_wait`).
    pub fn run(self: *Runtime, mode: xev.RunMode) !void {
        self.assertThreadAffinity();
        try self.loop.run(mode);
    }
};

/// TCP listener that accepts inbound connections and wraps them in
/// `Connection` objects.
///
/// The listener is tied to the event loop passed at construction time.
/// All methods and callbacks execute on the event-loop owner thread.
/// The `on_accept` callback is invoked for each new connection; the
/// callee takes ownership of the heap-allocated `Connection`.
///
/// ## Cleanup
///
/// `Listener` has no synchronous `deinit`. The only cleanup path is
/// `close()`, which enqueues an asynchronous close on the event loop.
/// The underlying socket fd is not released until the event loop
/// processes the close completion. Therefore:
///
///  1. Call `close()` to initiate shutdown.
///  2. Continue driving the event loop (e.g., `runtime.run(.until_done)`)
///     until all pending completions — including the listener's close —
///     have fired.
///  3. Only after the event loop has been fully drained is it safe to
///     tear down the `Runtime`.
///
/// If the process exits or the event loop is torn down before the close
/// completion fires, the listener's socket file descriptor will leak.
pub const Listener = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    socket: xev.TCP,
    accept_completion: xev.Completion = .{},
    close_completion: xev.Completion = .{},
    on_accept: *const fn (listener: *Listener, conn: *Connection) void,
    conn_options: Connection.Options,

    /// Bind and listen on the given address.
    ///
    /// The `on_accept` callback fires on the event-loop thread for each
    /// accepted connection. Call `start()` to begin accepting.
    pub fn init(
        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        addr: std.net.Address,
        on_accept: *const fn (listener: *Listener, conn: *Connection) void,
        conn_options: Connection.Options,
    ) !Listener {
        var socket = try xev.TCP.init(addr);
        errdefer std.posix.close(socketFd(socket));
        try socket.bind(addr);
        try socket.listen(128);

        return .{
            .allocator = allocator,
            .loop = loop,
            .socket = socket,
            .on_accept = on_accept,
            .conn_options = conn_options,
        };
    }

    /// Wrap an already-bound and listening socket fd.
    ///
    /// Use this when the parent process creates the listening socket and
    /// passes the fd to the child (e.g., to avoid ephemeral port races
    /// in test harnesses).
    pub fn initFd(
        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        fd: std.posix.fd_t,
        on_accept: *const fn (listener: *Listener, conn: *Connection) void,
        conn_options: Connection.Options,
    ) Listener {
        return .{
            .allocator = allocator,
            .loop = loop,
            .socket = xev.TCP.initFd(fd),
            .on_accept = on_accept,
            .conn_options = conn_options,
        };
    }

    /// Begin accepting connections. Must be called after `init`.
    pub fn start(self: *Listener) void {
        self.queueAccept();
    }

    /// Stop accepting and close the listener socket asynchronously.
    ///
    /// The socket is not actually closed until the event loop processes
    /// the close completion. Callers must continue running the event loop
    /// after calling this method to ensure the fd is released. See the
    /// struct-level documentation for the full cleanup protocol.
    pub fn close(self: *Listener) void {
        self.socket.close(self.loop, &self.close_completion, Listener, self, Listener.onClosed);
    }

    fn queueAccept(self: *Listener) void {
        self.socket.accept(self.loop, &self.accept_completion, Listener, self, Listener.onAccept);
    }

    fn onAccept(
        self: ?*Listener,
        _: *xev.Loop,
        _: *xev.Completion,
        res: xev.AcceptError!xev.TCP,
    ) xev.CallbackAction {
        const listener = self.?;
        const socket = res catch |err| {
            log.debug("accept failed: {}", .{err});
            listener.queueAccept();
            return .disarm;
        };

        // Disable Nagle on accepted RPC sockets to avoid delayed-ACK/Nagle
        // interaction adding ~40ms request/response stalls for some clients.
        enableTcpNoDelay(socket);

        const conn_ptr = listener.createConnection(socket) catch |err| {
            log.debug("connection setup failed: {}", .{err});
            std.posix.close(socketFd(socket));
            listener.queueAccept();
            return .disarm;
        };

        listener.on_accept(listener, conn_ptr);
        listener.queueAccept();
        return .disarm;
    }

    /// Allocate and initialize a Connection. Uses errdefer to guarantee
    /// the heap allocation is freed if Connection.init fails.
    fn createConnection(self: *Listener, socket: xev.TCP) !*Connection {
        const conn_ptr = try self.allocator.create(Connection);
        errdefer self.allocator.destroy(conn_ptr);

        conn_ptr.* = try Connection.init(
            self.allocator,
            self.loop,
            socket,
            self.conn_options,
        );
        return conn_ptr;
    }

    fn onClosed(
        _: ?*Listener,
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.TCP,
        _: xev.CloseError!void,
    ) xev.CallbackAction {
        return .disarm;
    }

    fn socketFd(socket: xev.TCP) std.posix.fd_t {
        if (comptime xev.dynamic) {
            return socket.fd();
        }
        return socket.fd;
    }

    fn enableTcpNoDelay(socket: xev.TCP) void {
        const one = std.mem.toBytes(@as(c_int, 1));
        std.posix.setsockopt(
            socketFd(socket),
            std.posix.IPPROTO.TCP,
            std.posix.TCP.NODELAY,
            &one,
        ) catch |err| {
            log.debug("failed to set TCP_NODELAY on accepted socket: {}", .{err});
        };
    }
};

test "runtime backend selection can initialize a loop" {
    try Runtime.ensureBackend();
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();
}

test "createConnection returns OOM when Connection allocation fails" {
    try Runtime.ensureBackend();
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const DummyAccept = struct {
        fn onAccept(_: *Listener, _: *Connection) void {}
    };

    // fail_index = 0: the very first allocation (create(Connection)) fails.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var listener = Listener{
        .allocator = failing.allocator(),
        .loop = &loop,
        .socket = undefined,
        .on_accept = DummyAccept.onAccept,
        .conn_options = .{},
    };

    // createConnection should propagate OutOfMemory. No memory should leak.
    try std.testing.expectError(error.OutOfMemory, listener.createConnection(undefined));
}

test "createConnection errdefer frees Connection when Transport.init fails" {
    try Runtime.ensureBackend();
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const DummyAccept = struct {
        fn onAccept(_: *Listener, _: *Connection) void {}
    };

    // fail_index = 1: the first allocation (create(Connection)) succeeds,
    // but the second allocation (read buffer inside Transport.init) fails.
    // The errdefer in createConnection must free the Connection to avoid a leak.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    var listener = Listener{
        .allocator = failing.allocator(),
        .loop = &loop,
        .socket = undefined,
        .on_accept = DummyAccept.onAccept,
        .conn_options = .{},
    };

    // createConnection should propagate OutOfMemory. The errdefer ensures
    // the already-allocated Connection is freed — no leak.
    try std.testing.expectError(error.OutOfMemory, listener.createConnection(undefined));
}
