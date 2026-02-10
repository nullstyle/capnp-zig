const std = @import("std");
const log = std.log.scoped(.rpc_transport);
const xev = @import("xev").Dynamic;

/// Union of all xev I/O error types that can surface through transport
/// callbacks (read errors, write errors, and close errors).
pub const TransportError = xev.ReadError || xev.WriteError || xev.CloseError;

/// Asynchronous TCP transport layer backed by libxev.
///
/// `Transport` owns a TCP socket and drives reads/writes through an xev event
/// loop. All public methods **must** be called from the thread that owns the
/// associated `xev.Loop` -- the transport is not thread-safe.
///
/// ## Callback context lifetime
///
/// Several methods accept an opaque `ctx: *anyopaque` together with a
/// function-pointer callback. The caller **must** guarantee the following:
///
/// * The `ctx` pointer remains valid (not freed, not moved) for the
///   entire duration the callback is registered. For `startRead` and
///   `setCloseHandler`, that means until `clearHandlers` is called, or
///   until `deinit` completes.
/// * For `queueWrite`, the `ctx` pointer must remain valid until the
///   write-completion callback fires (success or error). The transport
///   takes ownership of the byte payload (copies it internally), so the
///   caller may free the source bytes immediately after `queueWrite`
///   returns.
/// * Callbacks are always invoked on the event-loop thread -- never from
///   a background thread or signal handler.
/// * After `deinit` returns, no further callbacks will fire. `deinit`
///   drains pending writes with a bounded timeout and then abandons any
///   remaining operations, clearing their callback pointers so they
///   become no-ops when the event loop eventually retires them.
///
/// ## Shutdown sequence
///
/// The transport goes through three close-related flags:
///
/// 1. `close_requested` -- set by an explicit `close()` call.
/// 2. `shutting_down`   -- set by `close()` or `deinit()`, suppresses
///    new read re-arms and write callbacks.
/// 3. `close_signaled`  -- set exactly once by `signalClose`, which
///    invokes the registered close callback (if any).
///
/// `isClosing()` returns `true` when **any** of these flags is set.
pub const Transport = struct {
    loop: *xev.Loop,
    socket: xev.TCP,
    allocator: std.mem.Allocator,
    read_buf: []u8,
    read_completion: xev.Completion = .{},
    close_completion: xev.Completion = .{},
    write_queue: xev.WriteQueue = .{},

    /// Opaque context pointer passed to `read_cb` on each read completion.
    /// Must remain valid until `clearHandlers` or `deinit`.
    read_ctx: ?*anyopaque = null,
    /// Callback invoked on the event-loop thread each time data arrives.
    read_cb: ?*const fn (ctx: *anyopaque, data: []const u8) void = null,
    /// Opaque context pointer passed to `close_cb` when the socket closes.
    /// Must remain valid until `clearHandlers` or `deinit`.
    close_ctx: ?*anyopaque = null,
    /// Callback invoked exactly once when the transport detects a close
    /// (EOF, read error, or explicit `close()`). The error is `null` for
    /// a clean shutdown.
    close_cb: ?*const fn (ctx: *anyopaque, err: ?TransportError) void = null,
    /// Head of the intrusive doubly-linked list of in-flight write operations.
    write_head: ?*WriteOp = null,
    /// Number of write operations currently tracked (in-flight or queued).
    pending_writes: usize = 0,
    /// Set when `close()` has been called. Prevents double-close.
    close_requested: bool = false,
    /// Set exactly once by `signalClose`. Guards against duplicate close callbacks.
    close_signaled: bool = false,
    /// Set by `close()` or `deinit()`. Suppresses read re-arming and new callbacks.
    shutting_down: bool = false,

    /// Create a transport bound to the given event loop and socket.
    ///
    /// Allocates a read buffer of `read_buffer_size` bytes from `allocator`.
    /// The caller must later call `deinit` to free the buffer and drain
    /// pending writes.
    pub fn init(
        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        socket: xev.TCP,
        read_buffer_size: usize,
    ) !Transport {
        const buf = try allocator.alloc(u8, read_buffer_size);
        return .{
            .loop = loop,
            .socket = socket,
            .allocator = allocator,
            .read_buf = buf,
        };
    }

    /// Tear down the transport, draining pending writes with a bounded
    /// timeout and then freeing the read buffer.
    ///
    /// After `deinit` returns, no registered callbacks will fire. Any
    /// write operations still in the xev queue have their callback
    /// pointers cleared so they become harmless no-ops.
    pub fn deinit(self: *Transport) void {
        self.shutting_down = true;
        self.drainPendingWrites();
        if (self.pending_writes != 0) {
            log.warn("abandoning {} pending writes during deinit", .{self.pending_writes});
            self.abandonPendingWrites();
        }
        self.allocator.free(self.read_buf);
    }

    /// Begin reading from the socket, delivering chunks to `cb`.
    ///
    /// `ctx` must remain valid until `clearHandlers` or `deinit` is called.
    /// The callback is always invoked on the event-loop thread. The `data`
    /// slice passed to the callback points into the transport's internal
    /// read buffer and is only valid for the duration of the callback
    /// invocation.
    pub fn startRead(
        self: *Transport,
        ctx: *anyopaque,
        cb: *const fn (ctx: *anyopaque, data: []const u8) void,
    ) void {
        self.read_ctx = ctx;
        self.read_cb = cb;
        self.queueRead();
    }

    /// Register a close handler that fires exactly once when the transport
    /// detects a close condition (EOF, error, or explicit `close()`).
    ///
    /// `ctx` must remain valid until `clearHandlers` or `deinit` is called.
    /// The callback is invoked on the event-loop thread.
    pub fn setCloseHandler(
        self: *Transport,
        ctx: *anyopaque,
        cb: *const fn (ctx: *anyopaque, err: ?TransportError) void,
    ) void {
        self.close_ctx = ctx;
        self.close_cb = cb;
    }

    /// Unregister all read and close callbacks. After this call, any
    /// pending read completions or close events are silently discarded.
    pub fn clearHandlers(self: *Transport) void {
        self.read_ctx = null;
        self.read_cb = null;
        self.close_ctx = null;
        self.close_cb = null;
    }

    /// Enqueue a write of `bytes` on the socket.
    ///
    /// The transport copies `bytes` into a heap-allocated buffer, so the
    /// caller may free the source slice immediately after this returns.
    ///
    /// `ctx` (if non-null) must remain valid until the completion callback
    /// `cb` fires. The callback is invoked on the event-loop thread with
    /// `null` error on success or the write error on failure. After the
    /// callback returns, the transport frees the internal copy and the
    /// `WriteOp`.
    pub fn queueWrite(
        self: *Transport,
        bytes: []const u8,
        ctx: ?*anyopaque,
        cb: ?*const fn (ctx: *anyopaque, err: ?TransportError) void,
    ) !void {
        var op = try self.allocator.create(WriteOp);
        errdefer self.allocator.destroy(op);

        const owned = try self.allocator.alloc(u8, bytes.len);
        errdefer self.allocator.free(owned);
        std.mem.copyForwards(u8, owned, bytes);

        op.* = .{
            .allocator = self.allocator,
            .transport = self,
            // Dynamic xev initializes/tag-dispatches WriteRequest inside queueWrite.
            .request = undefined,
            .bytes = owned,
            .ctx = ctx,
            .cb = cb,
        };
        self.trackWriteOp(op);

        self.socket.queueWrite(
            self.loop,
            &self.write_queue,
            &op.request,
            .{ .slice = op.bytes },
            WriteOp,
            op,
            WriteOp.onWrite,
        );
    }

    /// Initiate an orderly close of the underlying TCP socket.
    ///
    /// Idempotent: subsequent calls are no-ops. Sets `close_requested` and
    /// `shutting_down`, then asks xev to close the socket. When the close
    /// completes, `signalClose` fires the registered close callback exactly
    /// once.
    pub fn close(self: *Transport) void {
        if (self.close_requested) return;
        log.debug("transport close requested", .{});
        self.close_requested = true;
        self.shutting_down = true;
        self.socket.close(
            self.loop,
            &self.close_completion,
            Transport,
            self,
            Transport.onClose,
        );
    }

    /// Returns `true` if the transport is in any stage of closing
    /// (close requested, close signaled, or shutting down).
    pub fn isClosing(self: *const Transport) bool {
        return self.close_requested or self.close_signaled or self.shutting_down;
    }

    /// Submit a read request to the xev loop. Re-called automatically after
    /// each successful read until `shutting_down` is set.
    fn queueRead(self: *Transport) void {
        self.socket.read(
            self.loop,
            &self.read_completion,
            .{ .slice = self.read_buf },
            Transport,
            self,
            Transport.onRead,
        );
    }

    /// xev read completion callback. Invoked on the event-loop thread.
    /// Delivers data to the registered read callback, then re-arms the read
    /// unless the transport is shutting down.
    fn onRead(
        self: ?*Transport,
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.TCP,
        buf: xev.ReadBuffer,
        res: xev.ReadError!usize,
    ) xev.CallbackAction {
        const transport = self.?;
        if (transport.shutting_down) return .disarm;
        const n = res catch |err| {
            transport.signalClose(err);
            return .disarm;
        };

        if (n == 0) {
            transport.signalClose(null);
            return .disarm;
        }

        if (transport.read_cb) |cb| {
            const slice = switch (buf) {
                .slice => |s| s[0..n],
                .array => |s| s[0..n],
            };
            cb(transport.read_ctx.?, slice);
        }

        // Re-check after callback: the callback may have called
        // clearHandlers() or initiated shutdown, invalidating state.
        if (!transport.shutting_down and transport.read_cb != null) transport.queueRead();
        return .disarm;
    }

    /// xev close completion callback. Invoked on the event-loop thread.
    /// Signals the close handler with the close result.
    fn onClose(
        self: ?*Transport,
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.TCP,
        res: xev.CloseError!void,
    ) xev.CallbackAction {
        const transport = self.?;
        transport.shutting_down = true;
        if (res) |_| {
            transport.signalClose(null);
        } else |err| {
            transport.signalClose(err);
        }
        return .disarm;
    }

    /// Fire the close callback exactly once. Subsequent calls are no-ops
    /// (guarded by `close_signaled`). This is the single funnel through
    /// which EOF, read errors, and explicit close all notify the owner.
    fn signalClose(self: *Transport, err: ?TransportError) void {
        if (self.close_signaled) return;
        self.close_signaled = true;
        if (err) |e| {
            log.debug("transport signaling close with error: {}", .{e});
        } else {
            log.debug("transport signaling clean close", .{});
        }
        if (self.close_cb) |cb| {
            cb(self.close_ctx.?, err);
        }
    }

    /// Insert a write op at the head of the intrusive linked list.
    fn trackWriteOp(self: *Transport, op: *WriteOp) void {
        op.prev = null;
        op.next = self.write_head;
        if (self.write_head) |head| head.prev = op;
        self.write_head = op;
        self.pending_writes += 1;
    }

    /// Remove a write op from the intrusive linked list.
    fn untrackWriteOp(self: *Transport, op: *WriteOp) void {
        if (op.prev) |prev| {
            prev.next = op.next;
        } else {
            self.write_head = op.next;
        }
        if (op.next) |next| {
            next.prev = op.prev;
        }
        op.prev = null;
        op.next = null;
        if (self.pending_writes > 0) self.pending_writes -= 1;
    }

    /// Spin the event loop for up to 200 ms to let in-flight writes complete.
    /// Called during `deinit` to give the OS a chance to flush buffers.
    fn drainPendingWrites(self: *Transport) void {
        if (self.pending_writes == 0) return;
        if (!self.close_requested) self.close();

        const timeout_ns: i128 = 200 * std.time.ns_per_ms;
        const deadline = std.time.nanoTimestamp() + timeout_ns;
        while (self.pending_writes != 0 and std.time.nanoTimestamp() < deadline) {
            self.loop.run(.no_wait) catch break;
            if (self.pending_writes != 0) std.Thread.sleep(std.time.ns_per_ms);
        }
    }

    /// Detach all remaining write ops from this transport. Their callback
    /// pointers are cleared so the xev completion becomes a no-op that
    /// just frees the write buffer and the op itself.
    pub fn abandonPendingWrites(self: *Transport) void {
        var op_opt = self.write_head;
        while (op_opt) |op| {
            const next = op.next;
            op.transport = null;
            op.ctx = null;
            op.cb = null;
            op.prev = null;
            op.next = null;
            op_opt = next;
        }
        self.write_head = null;
        self.pending_writes = 0;
    }
};

/// A single in-flight write operation, heap-allocated by `Transport.queueWrite`.
///
/// `WriteOp` forms an intrusive doubly-linked list anchored at
/// `Transport.write_head` so the transport can track (and optionally abandon)
/// outstanding writes during shutdown.
///
/// Ownership: the `WriteOp` and its `bytes` buffer are freed inside `onWrite`
/// after the xev completion fires. If the transport is torn down before the
/// write completes, `abandonPendingWrites` clears the `transport`, `ctx`,
/// and `cb` pointers so the eventual xev callback becomes a no-op that still
/// frees memory.
const WriteOp = struct {
    allocator: std.mem.Allocator,
    /// Back-pointer to the owning transport. Set to `null` by
    /// `abandonPendingWrites` to signal that the transport is gone.
    transport: ?*Transport,
    request: xev.WriteRequest,
    /// Heap-allocated copy of the data to send. Freed in `onWrite`.
    bytes: []u8,
    /// Caller-provided context for the write-completion callback.
    /// Must remain valid until the callback fires.
    ctx: ?*anyopaque,
    /// Write-completion callback. Receives `null` error on success.
    cb: ?*const fn (ctx: *anyopaque, err: ?TransportError) void,
    prev: ?*WriteOp = null,
    next: ?*WriteOp = null,

    /// xev write completion. Untracks the op from the transport, invokes
    /// the caller's callback, then frees the byte buffer and the op itself.
    fn onWrite(
        self: ?*WriteOp,
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.TCP,
        _: xev.WriteBuffer,
        res: xev.WriteError!usize,
    ) xev.CallbackAction {
        const op = self.?;
        if (op.transport) |transport| {
            transport.untrackWriteOp(op);
            if (op.cb) |cb| {
                if (op.ctx) |ctx| {
                    if (res) |_| {
                        cb(ctx, null);
                    } else |err| {
                        cb(ctx, err);
                    }
                }
            }
        }

        op.allocator.free(op.bytes);
        op.allocator.destroy(op);
        return .disarm;
    }
};

test "transport write op tracking and abandon" {
    const Noop = struct {
        fn onWriteDone(_: *anyopaque, _: ?TransportError) void {}
    };

    var read_buf = [_]u8{0} ** 8;
    var op1_bytes = [_]u8{1};
    var op2_bytes = [_]u8{2};
    var callback_ctx: u8 = 0;

    var transport = Transport{
        .loop = undefined,
        .socket = undefined,
        .allocator = std.testing.allocator,
        .read_buf = read_buf[0..],
    };

    var op1 = WriteOp{
        .allocator = std.testing.allocator,
        .transport = &transport,
        .request = undefined,
        .bytes = op1_bytes[0..],
        .ctx = &callback_ctx,
        .cb = Noop.onWriteDone,
    };
    var op2 = WriteOp{
        .allocator = std.testing.allocator,
        .transport = &transport,
        .request = undefined,
        .bytes = op2_bytes[0..],
        .ctx = &callback_ctx,
        .cb = Noop.onWriteDone,
    };

    transport.trackWriteOp(&op1);
    transport.trackWriteOp(&op2);

    try std.testing.expectEqual(@as(usize, 2), transport.pending_writes);
    try std.testing.expect(transport.write_head == &op2);
    try std.testing.expect(op2.next == &op1);
    try std.testing.expect(op1.prev == &op2);

    transport.untrackWriteOp(&op2);
    try std.testing.expectEqual(@as(usize, 1), transport.pending_writes);
    try std.testing.expect(transport.write_head == &op1);
    try std.testing.expect(op1.prev == null);
    try std.testing.expect(op2.next == null);
    try std.testing.expect(op2.prev == null);

    transport.abandonPendingWrites();
    try std.testing.expectEqual(@as(usize, 0), transport.pending_writes);
    try std.testing.expect(transport.write_head == null);
    try std.testing.expect(op1.transport == null);
    try std.testing.expect(op1.ctx == null);
    try std.testing.expect(op1.cb == null);
}

test "transport signalClose notifies callback once" {
    const CloseHarness = struct {
        const State = struct {
            calls: usize = 0,
            last_err: ?TransportError = null,
        };

        fn onClose(ctx: *anyopaque, err: ?TransportError) void {
            const state: *State = @ptrCast(@alignCast(ctx));
            state.calls += 1;
            state.last_err = err;
        }
    };

    var read_buf = [_]u8{0} ** 8;
    var state = CloseHarness.State{};
    var transport = Transport{
        .loop = undefined,
        .socket = undefined,
        .allocator = std.testing.allocator,
        .read_buf = read_buf[0..],
    };

    transport.setCloseHandler(&state, CloseHarness.onClose);
    transport.signalClose(error.ConnectionResetByPeer);
    transport.signalClose(null);

    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(?TransportError, error.ConnectionResetByPeer), state.last_err);
    try std.testing.expect(transport.close_signaled);
}

test "transport onRead read error signals close callback" {
    const CloseHarness = struct {
        const State = struct {
            calls: usize = 0,
            last_err: ?TransportError = null,
        };

        fn onClose(ctx: *anyopaque, err: ?TransportError) void {
            const state: *State = @ptrCast(@alignCast(ctx));
            state.calls += 1;
            state.last_err = err;
        }
    };

    var read_buf = [_]u8{0} ** 8;
    var state = CloseHarness.State{};
    var transport = Transport{
        .loop = undefined,
        .socket = undefined,
        .allocator = std.testing.allocator,
        .read_buf = read_buf[0..],
    };
    transport.setCloseHandler(&state, CloseHarness.onClose);

    const action = Transport.onRead(
        &transport,
        undefined,
        undefined,
        undefined,
        undefined,
        error.ConnectionResetByPeer,
    );
    try std.testing.expectEqual(xev.CallbackAction.disarm, action);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(?TransportError, error.ConnectionResetByPeer), state.last_err);
    try std.testing.expect(transport.close_signaled);
}

test "transport onRead EOF signals close callback with null error" {
    const CloseHarness = struct {
        const State = struct {
            calls: usize = 0,
            last_err: ?TransportError = error.Unexpected,
        };

        fn onClose(ctx: *anyopaque, err: ?TransportError) void {
            const state: *State = @ptrCast(@alignCast(ctx));
            state.calls += 1;
            state.last_err = err;
        }
    };

    var read_buf = [_]u8{0} ** 8;
    var state = CloseHarness.State{};
    var transport = Transport{
        .loop = undefined,
        .socket = undefined,
        .allocator = std.testing.allocator,
        .read_buf = read_buf[0..],
    };
    transport.setCloseHandler(&state, CloseHarness.onClose);

    const action = Transport.onRead(
        &transport,
        undefined,
        undefined,
        undefined,
        undefined,
        0,
    );
    try std.testing.expectEqual(xev.CallbackAction.disarm, action);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(?TransportError, null), state.last_err);
    try std.testing.expect(transport.close_signaled);
}

test "transport onClose after prior signalClose does not double notify" {
    const CloseHarness = struct {
        const State = struct {
            calls: usize = 0,
            last_err: ?TransportError = null,
        };

        fn onClose(ctx: *anyopaque, err: ?TransportError) void {
            const state: *State = @ptrCast(@alignCast(ctx));
            state.calls += 1;
            state.last_err = err;
        }
    };

    var read_buf = [_]u8{0} ** 8;
    var state = CloseHarness.State{};
    var transport = Transport{
        .loop = undefined,
        .socket = undefined,
        .allocator = std.testing.allocator,
        .read_buf = read_buf[0..],
    };
    transport.setCloseHandler(&state, CloseHarness.onClose);

    transport.signalClose(error.ConnectionResetByPeer);
    const action = Transport.onClose(&transport, undefined, undefined, undefined, {});

    try std.testing.expectEqual(xev.CallbackAction.disarm, action);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(?TransportError, error.ConnectionResetByPeer), state.last_err);
}

test "transport onClose error propagates error to callback" {
    const CloseHarness = struct {
        const State = struct {
            calls: usize = 0,
            last_err: ?TransportError = null,
        };

        fn onClose(ctx: *anyopaque, err: ?TransportError) void {
            const state: *State = @ptrCast(@alignCast(ctx));
            state.calls += 1;
            state.last_err = err;
        }
    };

    var read_buf = [_]u8{0} ** 8;
    var state = CloseHarness.State{};
    var transport = Transport{
        .loop = undefined,
        .socket = undefined,
        .allocator = std.testing.allocator,
        .read_buf = read_buf[0..],
    };
    transport.setCloseHandler(&state, CloseHarness.onClose);

    const action = Transport.onClose(&transport, undefined, undefined, undefined, error.Unexpected);
    try std.testing.expectEqual(xev.CallbackAction.disarm, action);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(?TransportError, error.Unexpected), state.last_err);
    try std.testing.expect(transport.close_signaled);
}

test "transport isClosing tracks close state flags" {
    var read_buf = [_]u8{0} ** 8;
    var transport = Transport{
        .loop = undefined,
        .socket = undefined,
        .allocator = std.testing.allocator,
        .read_buf = read_buf[0..],
    };

    try std.testing.expect(!transport.isClosing());

    transport.close_requested = true;
    try std.testing.expect(transport.isClosing());

    transport.close_requested = false;
    transport.close_signaled = true;
    try std.testing.expect(transport.isClosing());

    transport.close_signaled = false;
    transport.shutting_down = true;
    try std.testing.expect(transport.isClosing());
}

test "write op completion untracks and calls callback" {
    const WriteHarness = struct {
        const State = struct {
            calls: usize = 0,
            last_err: ?TransportError = null,
        };

        fn onWrite(ctx: *anyopaque, err: ?TransportError) void {
            const state: *State = @ptrCast(@alignCast(ctx));
            state.calls += 1;
            state.last_err = err;
        }
    };

    var read_buf = [_]u8{0} ** 8;
    var transport = Transport{
        .loop = undefined,
        .socket = undefined,
        .allocator = std.testing.allocator,
        .read_buf = read_buf[0..],
    };

    var callback_state = WriteHarness.State{};
    const bytes = try std.testing.allocator.alloc(u8, 4);
    bytes[0] = 1;
    bytes[1] = 2;
    bytes[2] = 3;
    bytes[3] = 4;

    const op = try std.testing.allocator.create(WriteOp);
    op.* = .{
        .allocator = std.testing.allocator,
        .transport = &transport,
        .request = undefined,
        .bytes = bytes,
        .ctx = &callback_state,
        .cb = WriteHarness.onWrite,
    };

    transport.trackWriteOp(op);
    try std.testing.expectEqual(@as(usize, 1), transport.pending_writes);
    try std.testing.expect(transport.write_head == op);

    const action = WriteOp.onWrite(
        op,
        undefined,
        undefined,
        undefined,
        undefined,
        4,
    );
    try std.testing.expectEqual(xev.CallbackAction.disarm, action);
    try std.testing.expectEqual(@as(usize, 0), transport.pending_writes);
    try std.testing.expect(transport.write_head == null);
    try std.testing.expectEqual(@as(usize, 1), callback_state.calls);
    try std.testing.expectEqual(@as(?TransportError, null), callback_state.last_err);
}

test "write op completion forwards write errors" {
    const WriteHarness = struct {
        const State = struct {
            calls: usize = 0,
            last_err: ?TransportError = null,
        };

        fn onWrite(ctx: *anyopaque, err: ?TransportError) void {
            const state: *State = @ptrCast(@alignCast(ctx));
            state.calls += 1;
            state.last_err = err;
        }
    };

    var read_buf = [_]u8{0} ** 8;
    var transport = Transport{
        .loop = undefined,
        .socket = undefined,
        .allocator = std.testing.allocator,
        .read_buf = read_buf[0..],
    };

    var callback_state = WriteHarness.State{};
    const bytes = try std.testing.allocator.alloc(u8, 2);
    bytes[0] = 9;
    bytes[1] = 7;

    const op = try std.testing.allocator.create(WriteOp);
    op.* = .{
        .allocator = std.testing.allocator,
        .transport = &transport,
        .request = undefined,
        .bytes = bytes,
        .ctx = &callback_state,
        .cb = WriteHarness.onWrite,
    };

    transport.trackWriteOp(op);
    const action = WriteOp.onWrite(
        op,
        undefined,
        undefined,
        undefined,
        undefined,
        error.ConnectionResetByPeer,
    );

    try std.testing.expectEqual(xev.CallbackAction.disarm, action);
    try std.testing.expectEqual(@as(usize, 0), transport.pending_writes);
    try std.testing.expectEqual(@as(usize, 1), callback_state.calls);
    try std.testing.expectEqual(@as(?TransportError, error.ConnectionResetByPeer), callback_state.last_err);
}

test "write op completion with null callback context skips callback invocation" {
    const WriteHarness = struct {
        const State = struct {
            calls: usize = 0,
        };

        fn onWrite(ctx: *anyopaque, err: ?TransportError) void {
            _ = err;
            const state: *State = @ptrCast(@alignCast(ctx));
            state.calls += 1;
        }
    };

    var read_buf = [_]u8{0} ** 8;
    var transport = Transport{
        .loop = undefined,
        .socket = undefined,
        .allocator = std.testing.allocator,
        .read_buf = read_buf[0..],
    };

    const callback_state = WriteHarness.State{};
    const bytes = try std.testing.allocator.alloc(u8, 2);
    bytes[0] = 3;
    bytes[1] = 4;

    const op = try std.testing.allocator.create(WriteOp);
    op.* = .{
        .allocator = std.testing.allocator,
        .transport = &transport,
        .request = undefined,
        .bytes = bytes,
        .ctx = null,
        .cb = WriteHarness.onWrite,
    };

    transport.trackWriteOp(op);
    const action = WriteOp.onWrite(
        op,
        undefined,
        undefined,
        undefined,
        undefined,
        2,
    );

    try std.testing.expectEqual(xev.CallbackAction.disarm, action);
    try std.testing.expectEqual(@as(usize, 0), transport.pending_writes);
    try std.testing.expectEqual(@as(usize, 0), callback_state.calls);
}
