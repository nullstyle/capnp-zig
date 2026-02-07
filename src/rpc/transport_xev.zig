const std = @import("std");
const xev = @import("xev");

pub const Transport = struct {
    loop: *xev.Loop,
    socket: xev.TCP,
    allocator: std.mem.Allocator,
    read_buf: []u8,
    read_completion: xev.Completion = .{},
    close_completion: xev.Completion = .{},
    write_queue: xev.WriteQueue = .{},

    read_ctx: ?*anyopaque = null,
    read_cb: ?*const fn (ctx: *anyopaque, data: []const u8) void = null,
    close_ctx: ?*anyopaque = null,
    close_cb: ?*const fn (ctx: *anyopaque, err: ?anyerror) void = null,
    write_head: ?*WriteOp = null,
    pending_writes: usize = 0,
    close_requested: bool = false,
    close_signaled: bool = false,
    shutting_down: bool = false,

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

    pub fn deinit(self: *Transport) void {
        self.shutting_down = true;
        self.drainPendingWrites();
        if (self.pending_writes != 0) {
            self.abandonPendingWrites();
        }
        self.allocator.free(self.read_buf);
    }

    pub fn startRead(
        self: *Transport,
        ctx: *anyopaque,
        cb: *const fn (ctx: *anyopaque, data: []const u8) void,
    ) void {
        self.read_ctx = ctx;
        self.read_cb = cb;
        self.queueRead();
    }

    pub fn setCloseHandler(
        self: *Transport,
        ctx: *anyopaque,
        cb: *const fn (ctx: *anyopaque, err: ?anyerror) void,
    ) void {
        self.close_ctx = ctx;
        self.close_cb = cb;
    }

    pub fn clearHandlers(self: *Transport) void {
        self.read_ctx = null;
        self.read_cb = null;
        self.close_ctx = null;
        self.close_cb = null;
    }

    pub fn queueWrite(
        self: *Transport,
        bytes: []const u8,
        ctx: ?*anyopaque,
        cb: ?*const fn (ctx: *anyopaque, err: ?anyerror) void,
    ) !void {
        var op = try self.allocator.create(WriteOp);
        errdefer self.allocator.destroy(op);

        const owned = try self.allocator.alloc(u8, bytes.len);
        std.mem.copyForwards(u8, owned, bytes);

        op.* = .{
            .allocator = self.allocator,
            .transport = self,
            .request = .{ .full_write_buffer = .{ .slice = owned } },
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

    pub fn close(self: *Transport) void {
        if (self.close_requested) return;
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

    pub fn isClosing(self: *const Transport) bool {
        return self.close_requested or self.close_signaled or self.shutting_down;
    }

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

        if (!transport.shutting_down) transport.queueRead();
        return .disarm;
    }

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

    fn signalClose(self: *Transport, err: ?anyerror) void {
        if (self.close_signaled) return;
        self.close_signaled = true;
        if (self.close_cb) |cb| {
            cb(self.close_ctx.?, err);
        }
    }

    fn trackWriteOp(self: *Transport, op: *WriteOp) void {
        op.prev = null;
        op.next = self.write_head;
        if (self.write_head) |head| head.prev = op;
        self.write_head = op;
        self.pending_writes += 1;
    }

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

    fn abandonPendingWrites(self: *Transport) void {
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

const WriteOp = struct {
    allocator: std.mem.Allocator,
    transport: ?*Transport,
    request: xev.WriteRequest,
    bytes: []u8,
    ctx: ?*anyopaque,
    cb: ?*const fn (ctx: *anyopaque, err: ?anyerror) void,
    prev: ?*WriteOp = null,
    next: ?*WriteOp = null,

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
        fn onWriteDone(_: *anyopaque, _: ?anyerror) void {}
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
            last_err: ?anyerror = null,
        };

        fn onClose(ctx: *anyopaque, err: ?anyerror) void {
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
    transport.signalClose(error.TestTransportClosed);
    transport.signalClose(null);

    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(?anyerror, error.TestTransportClosed), state.last_err);
    try std.testing.expect(transport.close_signaled);
}

test "transport onRead read error signals close callback" {
    const CloseHarness = struct {
        const State = struct {
            calls: usize = 0,
            last_err: ?anyerror = null,
        };

        fn onClose(ctx: *anyopaque, err: ?anyerror) void {
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
    try std.testing.expectEqual(@as(?anyerror, error.ConnectionResetByPeer), state.last_err);
    try std.testing.expect(transport.close_signaled);
}

test "transport onClose after prior signalClose does not double notify" {
    const CloseHarness = struct {
        const State = struct {
            calls: usize = 0,
            last_err: ?anyerror = null,
        };

        fn onClose(ctx: *anyopaque, err: ?anyerror) void {
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

    transport.signalClose(error.TestTransportClosed);
    const action = Transport.onClose(&transport, undefined, undefined, undefined, {});

    try std.testing.expectEqual(xev.CallbackAction.disarm, action);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(?anyerror, error.TestTransportClosed), state.last_err);
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
            last_err: ?anyerror = null,
        };

        fn onWrite(ctx: *anyopaque, err: ?anyerror) void {
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
    try std.testing.expectEqual(@as(?anyerror, null), callback_state.last_err);
}

test "write op completion forwards write errors" {
    const WriteHarness = struct {
        const State = struct {
            calls: usize = 0,
            last_err: ?anyerror = null,
        };

        fn onWrite(ctx: *anyopaque, err: ?anyerror) void {
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
    try std.testing.expectEqual(@as(?anyerror, error.ConnectionResetByPeer), callback_state.last_err);
}
