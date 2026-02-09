const std = @import("std");
const log = std.log.scoped(.rpc_conn);
const framing = @import("framing.zig");
const transport_xev = @import("transport_xev.zig");
const xev = @import("xev").Dynamic;
const message = @import("../message.zig");

/// A framed Cap'n Proto connection over TCP.
///
/// Combines a `Transport` (raw TCP I/O) with a `Framer` (Cap'n Proto
/// segment-based message framing) to deliver complete RPC messages to
/// the `on_message` callback.
///
/// ## Callback context lifetime
///
/// The `ctx` pointer set via `start()` must remain valid until `deinit`
/// is called. All callbacks (`on_message`, `on_error`, `on_close`) are
/// invoked on the event-loop thread and may reference `ctx`.
///
/// ## Ownership
///
/// The `Connection` owns its `Transport` and `Framer`. Call `deinit` to
/// release both (which also clears transport callbacks and drains pending
/// writes). The `Connection` does **not** own the `ctx` pointer.
pub const Connection = struct {
    allocator: std.mem.Allocator,
    transport: transport_xev.Transport,
    framer: framing.Framer,
    /// Opaque context pointer passed to `start()`. Must remain valid until
    /// `deinit`. All callbacks may dereference this pointer.
    ctx: ?*anyopaque = null,
    /// Called for each complete inbound Cap'n Proto message frame.
    on_message: ?*const fn (conn: *Connection, frame: []const u8) anyerror!void = null,
    /// Called on transport or framing errors. The connection may be
    /// in a degraded state after an error callback.
    on_error: ?*const fn (conn: *Connection, err: anyerror) void = null,
    /// Called exactly once when the transport closes (after any error
    /// callback, if applicable).
    on_close: ?*const fn (conn: *Connection) void = null,

    pub const Options = struct {
        read_buffer_size: usize = 64 * 1024,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        socket: xev.TCP,
        options: Options,
    ) !Connection {
        return .{
            .allocator = allocator,
            .transport = try transport_xev.Transport.init(allocator, loop, socket, options.read_buffer_size),
            .framer = framing.Framer.init(allocator),
        };
    }

    pub fn deinit(self: *Connection) void {
        self.ctx = null;
        self.on_message = null;
        self.on_error = null;
        self.on_close = null;
        self.transport.clearHandlers();
        self.transport.abandonPendingWrites();
        self.transport.deinit();
        self.framer.deinit();
    }

    pub fn start(
        self: *Connection,
        ctx: *anyopaque,
        on_message: *const fn (conn: *Connection, frame: []const u8) anyerror!void,
        on_error: *const fn (conn: *Connection, err: anyerror) void,
        on_close: *const fn (conn: *Connection) void,
    ) void {
        log.debug("connection starting", .{});
        self.ctx = ctx;
        self.on_message = on_message;
        self.on_error = on_error;
        self.on_close = on_close;

        self.transport.setCloseHandler(self, onTransportClose);
        self.transport.startRead(self, onTransportRead);
    }

    pub fn sendFrame(self: *Connection, frame: []const u8) !void {
        try self.transport.queueWrite(frame, self, onWriteDone);
    }

    pub fn close(self: *Connection) void {
        log.debug("connection closing", .{});
        self.transport.close();
    }

    pub fn isClosing(self: *const Connection) bool {
        return self.transport.isClosing();
    }

    fn onTransportRead(ctx: *anyopaque, data: []const u8) void {
        const conn: *Connection = @ptrCast(@alignCast(ctx));
        conn.handleRead(data);
    }

    fn handleRead(self: *Connection, data: []const u8) void {
        if (self.on_message == null or self.on_error == null) return;

        const push_result = self.framer.push(data);
        if (push_result) |_| {} else |err| {
            log.debug("framer push failed: {}", .{err});
            self.on_error.?(self, err);
            return;
        }

        while (true) {
            const frame = self.framer.popFrame() catch |err| {
                self.on_error.?(self, err);
                return;
            };
            if (frame == null) break;
            const bytes = frame.?;
            defer self.allocator.free(bytes);

            self.on_message.?(self, bytes) catch |err| {
                self.on_error.?(self, err);
                return;
            };
        }
    }

    fn onTransportClose(ctx: *anyopaque, err: ?transport_xev.TransportError) void {
        const conn: *Connection = @ptrCast(@alignCast(ctx));
        if (err) |e| {
            log.debug("transport closed with error: {}", .{e});
            if (conn.on_error) |cb| cb(conn, e);
        } else {
            log.debug("transport closed cleanly", .{});
        }
        if (conn.on_close) |cb| cb(conn);
    }

    fn onWriteDone(ctx: *anyopaque, err: ?transport_xev.TransportError) void {
        if (err) |e| {
            log.debug("write failed: {}", .{e});
            const conn: *Connection = @ptrCast(@alignCast(ctx));
            if (conn.on_error) |cb| cb(conn, e);
        }
    }
};

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
            var msg = try message.Message.init(state.allocator, frame);
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
        .received = std.ArrayList(u32){},
    };
    defer state.received.deinit(allocator);

    var conn = Connection{
        .allocator = allocator,
        .transport = undefined,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.framer.deinit();

    try std.testing.expect(frame.len > 8);
    conn.handleRead(frame[0..5]);
    try std.testing.expectEqual(@as(usize, 0), state.received.items.len);
    try std.testing.expectEqual(@as(usize, 0), state.error_count);

    conn.handleRead(frame[5..]);
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
            var msg = try message.Message.init(state.allocator, frame);
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
        .received = std.ArrayList(u32){},
    };
    defer state.received.deinit(allocator);

    var conn = Connection{
        .allocator = allocator,
        .transport = undefined,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.framer.deinit();

    conn.handleRead(combined);
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
            var msg = try message.Message.init(state.allocator, frame);
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
        .received = std.ArrayList(u32){},
    };
    defer state.received.deinit(allocator);

    var conn = Connection{
        .allocator = allocator,
        .transport = undefined,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.framer.deinit();

    conn.handleRead(combined);
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
    var conn = Connection{
        .allocator = allocator,
        .transport = undefined,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.framer.deinit();

    // segment_count_minus_one = max u32 overflows on +1 in framer.updateExpected()
    const bad_header = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    conn.handleRead(&bad_header);

    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), state.last_error);
}

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
    var conn = Connection{
        .allocator = allocator,
        .transport = undefined,
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

    conn.handleRead(&header);
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.FrameTooLarge), state.last_error);
}

test "connection onTransportClose reports error then close" {
    const allocator = std.testing.allocator;

    const Harness = struct {
        const State = struct {
            error_count: usize = 0,
            close_count: usize = 0,
            last_error: ?anyerror = null,
        };

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
        }

        fn onClose(conn: *Connection) void {
            const state: *State = @ptrCast(@alignCast(conn.ctx.?));
            state.close_count += 1;
        }
    };

    var state = Harness.State{};
    var conn = Connection{
        .allocator = allocator,
        .transport = undefined,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
        .on_close = Harness.onClose,
    };
    defer conn.framer.deinit();

    Connection.onTransportClose(&conn, error.ConnectionResetByPeer);
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(usize, 1), state.close_count);
    try std.testing.expectEqual(@as(?anyerror, error.ConnectionResetByPeer), state.last_error);
}

test "connection onWriteDone forwards write errors to on_error" {
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
    var conn = Connection{
        .allocator = allocator,
        .transport = undefined,
        .framer = framing.Framer.init(allocator),
        .ctx = &state,
        .on_message = Harness.onMessage,
        .on_error = Harness.onError,
    };
    defer conn.framer.deinit();

    Connection.onWriteDone(&conn, error.ConnectionResetByPeer);
    Connection.onWriteDone(&conn, null);

    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.ConnectionResetByPeer), state.last_error);
}

test "connection isClosing reflects transport state" {
    const allocator = std.testing.allocator;
    var read_buf = [_]u8{0} ** 8;

    var conn = Connection{
        .allocator = allocator,
        .transport = .{
            .loop = undefined,
            .socket = undefined,
            .allocator = allocator,
            .read_buf = read_buf[0..],
        },
        .framer = framing.Framer.init(allocator),
    };
    defer conn.framer.deinit();

    try std.testing.expect(!conn.isClosing());

    conn.transport.close_requested = true;
    try std.testing.expect(conn.isClosing());
}
