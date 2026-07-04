const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.wire.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.caps.table;
const Connection = capnpc.rpc.transport.tcp.Connection;
const Peer = peer_impl.Peer;
const Transport = capnpc.rpc.transport.tcp.Transport;
const Listener = capnpc.rpc.transport.tcp.Listener;

/// Dummy socket handle for tests that drive a fake Io vtable — never used
/// for real I/O. `fd_t` is an integer on POSIX and a pointer (HANDLE) on
/// Windows, so a literal 0 does not compile there.
const fake_socket_handle: std.Io.net.Socket.Handle =
    if (builtin.os.tag == .windows) std.os.windows.INVALID_HANDLE_VALUE else 0;

// ---------------------------------------------------------------------------
// Shared test infrastructure
// ---------------------------------------------------------------------------

const Capture = struct {
    allocator: std.mem.Allocator,
    frames: std.ArrayList([]u8),

    fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
        const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
        const copy = try ctx.allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, copy, frame);
        try ctx.frames.append(ctx.allocator, copy);
    }

    fn deinit(self: *@This()) void {
        for (self.frames.items) |frame| self.allocator.free(frame);
        self.frames.deinit(self.allocator);
    }
};

fn buildCallFrame(
    allocator: std.mem.Allocator,
    question_id: u32,
    target_export_id: u32,
    interface_id: u64,
    method_id: u16,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, interface_id, method_id);
    try call.setTargetImportedCap(target_export_id);
    _ = try call.initCapTableTyped(0);
    return builder.finish();
}

fn buildReturnResultsFrame(allocator: std.mem.Allocator, answer_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .results);
    _ = try ret.initCapTableTyped(0);
    return builder.finish();
}

fn buildReturnExceptionFrame(allocator: std.mem.Allocator, answer_id: u32, reason: []const u8) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .exception);
    try ret.setException(reason);
    return builder.finish();
}

fn buildFinishFrame(allocator: std.mem.Allocator, question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildFinish(question_id, false, false);
    return builder.finish();
}

const tcp = capnpc.rpc.transport.tcp;

/// Portable connected pair: loopback TCP via std.Io, so these suites run
/// on every platform (POSIX socketpair does not exist on Windows).
fn createSocketPair() ![2]tcp.SocketFd {
    return tcp.createLoopbackSocketPair(std.testing.io);
}

fn closeFd(socket: tcp.SocketFd) void {
    tcp.closeFd(std.testing.io, socket);
}

fn writeAll(socket: tcp.SocketFd, bytes: []const u8) !void {
    const io = std.testing.io;
    const pattern: []const u8 = &.{};
    const data: [1][]const u8 = .{pattern};
    var offset: usize = 0;
    while (offset < bytes.len) {
        const n = io.vtable.netWrite(io.userdata, socket.handle, bytes[offset..], &data, 0) catch
            return error.WriteFailed;
        if (n == 0) return error.BrokenPipe;
        offset += n;
    }
}

// ---------------------------------------------------------------------------
// 1. Peer deinit with pending outbound calls
// ---------------------------------------------------------------------------

test "deinit with pending outbound calls: no leaks" {
    const allocator = std.testing.allocator;

    const Callback = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var ctx: u8 = 0;

    // Send several outbound calls without delivering any returns.
    for (0..5) |i| {
        _ = try peer.sendCall(0, 0xAAAA, @intCast(i), &ctx, null, Callback.onReturn);
    }

    // Deinit without receiving any returns. The allocator will catch leaks.
    peer.deinit();
}

// ---------------------------------------------------------------------------
// 2. Peer deinit with pending inbound calls
// ---------------------------------------------------------------------------

test "deinit with pending inbound calls: clean shutdown" {
    const allocator = std.testing.allocator;

    // Handler that records calls but does NOT respond.
    const ServerCtx = struct {
        call_count: u32 = 0,
    };
    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            _: *Peer,
            _: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const state: *ServerCtx = @ptrCast(@alignCast(ctx));
            state.call_count += 1;
            // Intentionally do not respond — simulates a stalled handler.
        }
    };

    var peer = Peer.initDetached(allocator);

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Inject 4 inbound calls that are never answered.
    for (0..4) |i| {
        const qid: u32 = @intCast(400 + i);
        const frame = try buildCallFrame(allocator, qid, export_id, 0xBBBB, 0);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    try std.testing.expectEqual(@as(u32, 4), server_ctx.call_count);
    // No returns should have been sent.
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    // Deinit with unanswered inbound calls. Allocator catches leaks.
    peer.deinit();
}

// ---------------------------------------------------------------------------
// 3. Shutdown drains pending callbacks
// ---------------------------------------------------------------------------

test "shutdown drains pending callbacks when returns arrive" {
    const allocator = std.testing.allocator;

    const ReturnCtx = struct {
        count: u32 = 0,
    };
    const Callback = struct {
        fn onReturn(ctx: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ReturnCtx = @ptrCast(@alignCast(ctx));
            state.count += 1;
        }
    };

    // Use initDetached because completeShutdown will fire once all
    // questions are answered, and an undefined Connection would panic.
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var return_ctx = ReturnCtx{};

    // Send 3 outbound calls.
    var qids: [3]u32 = undefined;
    for (0..3) |i| {
        qids[i] = try peer.sendCall(0, 0xCCCC, @intCast(i), &return_ctx, null, Callback.onReturn);
    }

    // Initiate shutdown — calls are still pending, new calls are rejected.
    peer.shutdown(null);
    try std.testing.expectError(
        error.PeerShuttingDown,
        peer.sendCall(0, 0xCCCC, 99, &return_ctx, null, Callback.onReturn),
    );

    // Deliver returns for all pending questions.
    for (qids) |qid| {
        const frame = try buildReturnResultsFrame(allocator, qid);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    // All return callbacks should have fired.
    try std.testing.expectEqual(@as(u32, 3), return_ctx.count);

    // After all returns delivered, the peer's question table should be empty
    // (shutdown completion drains pending questions).
    // Verify by confirming new calls are still rejected (peer stays shut down).
    try std.testing.expectError(
        error.PeerShuttingDown,
        peer.sendCall(0, 0xCCCC, 100, &return_ctx, null, Callback.onReturn),
    );
}

// ---------------------------------------------------------------------------
// 4. Shutdown then deinit cleans up without delivering all returns
// ---------------------------------------------------------------------------

test "shutdown then deinit without delivering all returns" {
    const allocator = std.testing.allocator;

    const ReturnCtx = struct {
        count: u32 = 0,
    };
    const Callback = struct {
        fn onReturn(ctx: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ReturnCtx = @ptrCast(@alignCast(ctx));
            state.count += 1;
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var return_ctx = ReturnCtx{};

    // Send 4 outbound calls.
    var qids: [4]u32 = undefined;
    for (0..4) |i| {
        qids[i] = try peer.sendCall(0, 0xDDDD, @intCast(i), &return_ctx, null, Callback.onReturn);
    }

    // Begin shutdown.
    peer.shutdown(null);

    // Deliver only the first 2 returns.
    for (0..2) |i| {
        const frame = try buildReturnResultsFrame(allocator, qids[i]);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    try std.testing.expectEqual(@as(u32, 2), return_ctx.count);

    // Deinit with 2 questions still outstanding. Allocator catches leaks.
    peer.deinit();
}

// ---------------------------------------------------------------------------
// 5. DetachTransport while calls are pending
// ---------------------------------------------------------------------------

test "detach transport while calls pending: peer survives" {
    const allocator = std.testing.allocator;

    const Callback = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    // Declared before the peer: deinit's terminal question pass sends Finish
    // frames through the override, so the capture must outlive the peer.
    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var ctx: u8 = 0;

    // Send 3 outbound calls.
    var qids: [3]u32 = undefined;
    for (0..3) |i| {
        qids[i] = try peer.sendCall(0, 0xEEEE, @intCast(i), &ctx, null, Callback.onReturn);
    }

    try std.testing.expectEqual(@as(usize, 3), capture.frames.items.len);

    // Detach the transport while calls are pending.
    // The peer should not crash or leak.
    peer.detachTransport();
    try std.testing.expect(!peer.hasAttachedTransport());
}

// ---------------------------------------------------------------------------
// 6. Calls rejected after shutdown
// ---------------------------------------------------------------------------

test "sendCall returns PeerShuttingDown after shutdown" {
    const allocator = std.testing.allocator;

    const Callback = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    // Use initDetached because shutdown with zero pending questions
    // triggers completeShutdown, which would panic on an undefined Connection.
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var ctx: u8 = 0;

    // Shutdown immediately (no pending calls).
    peer.shutdown(null);

    // All send variants should return PeerShuttingDown.
    try std.testing.expectError(
        error.PeerShuttingDown,
        peer.sendCall(0, 0x1111, 0, &ctx, null, Callback.onReturn),
    );
}

// ---------------------------------------------------------------------------
// 7. Bootstrap rejected after shutdown
// ---------------------------------------------------------------------------

test "sendBootstrap returns PeerShuttingDown after shutdown" {
    const allocator = std.testing.allocator;

    const Callback = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    // Use initDetached because shutdown with zero pending questions
    // triggers completeShutdown, which would panic on an undefined Connection.
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var ctx: u8 = 0;

    // Shutdown immediately.
    peer.shutdown(null);

    // sendBootstrap should also return PeerShuttingDown.
    try std.testing.expectError(
        error.PeerShuttingDown,
        peer.sendBootstrap(&ctx, Callback.onReturn),
    );
}

// ---------------------------------------------------------------------------
// 8. handleFrame after shutdown with return — callbacks still fire
// ---------------------------------------------------------------------------

test "handleFrame after shutdown: returns for pending questions are delivered" {
    const allocator = std.testing.allocator;

    const ReturnCtx = struct {
        answer_ids: [8]u32 = @splat(0),
        count: u32 = 0,
    };
    const Callback = struct {
        fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ReturnCtx = @ptrCast(@alignCast(ctx));
            state.answer_ids[state.count] = ret.answer_id;
            state.count += 1;
        }
    };

    // Use initDetached because delivering the final return triggers
    // completeShutdown, which would panic on an undefined Connection.
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var return_ctx = ReturnCtx{};

    // Send 3 calls before shutdown.
    var qids: [3]u32 = undefined;
    for (0..3) |i| {
        qids[i] = try peer.sendCall(0, 0xFFFF, @intCast(i), &return_ctx, null, Callback.onReturn);
    }

    // Shut down.
    peer.shutdown(null);

    // Deliver returns after shutdown — they should still reach the callbacks.
    for (0..3) |i| {
        const frame = try buildReturnResultsFrame(allocator, qids[i]);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    try std.testing.expectEqual(@as(u32, 3), return_ctx.count);
    for (0..3) |i| {
        try std.testing.expectEqual(qids[i], return_ctx.answer_ids[i]);
    }
}

test "transport enqueueWrite rejects queued item overflow" {
    const allocator = std.testing.allocator;

    const pair = try createSocketPair();
    defer closeFd(pair[1]);

    var transport = try Transport.initWithOptions(allocator, std.testing.io, pair[0], .{
        .read_buffer_size = 64,
        .write_queue_max_items = 0,
        .write_queue_max_bytes = 1024,
    });
    defer transport.deinit();

    try transport.startWriter();
    try std.testing.expectError(error.WriteQueueFull, transport.enqueueWrite("x"));
}

test "transport enqueueWrite rejects queued byte overflow" {
    const allocator = std.testing.allocator;

    const pair = try createSocketPair();
    defer closeFd(pair[1]);

    var transport = try Transport.initWithOptions(allocator, std.testing.io, pair[0], .{
        .read_buffer_size = 64,
        .write_queue_max_items = 8,
        .write_queue_max_bytes = 2,
    });
    defer transport.deinit();

    try transport.startWriter();
    try std.testing.expectError(error.WriteQueueBytesExceeded, transport.enqueueWrite("abc"));
}

test "transport enqueueWrite counts writer-owned bytes against byte budget" {
    const allocator = std.testing.allocator;
    const net = std.Io.net;

    const BlockingIo = struct {
        const State = struct {
            entered: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
            release: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        };

        var active_state: ?*State = null;

        fn netWrite(
            _: ?*anyopaque,
            _: net.Socket.Handle,
            _: []const u8,
            data: []const []const u8,
            _: usize,
        ) net.Stream.Writer.Error!usize {
            const state = active_state.?;
            state.entered.store(true, .release);
            while (!state.release.load(.acquire)) {
                std.Thread.yield() catch {};
            }

            var len: usize = 0;
            for (data) |chunk| {
                len += chunk.len;
            }
            return len;
        }

        fn netShutdown(_: ?*anyopaque, _: net.Socket.Handle, _: net.ShutdownHow) net.ShutdownError!void {}

        fn netClose(_: ?*anyopaque, _: []const net.Socket.Handle) void {}
    };

    var state = BlockingIo.State{};
    BlockingIo.active_state = &state;
    defer BlockingIo.active_state = null;

    var vtable = std.testing.io.vtable.*;
    vtable.netWrite = BlockingIo.netWrite;
    vtable.netShutdown = BlockingIo.netShutdown;
    vtable.netClose = BlockingIo.netClose;
    const io = std.Io{
        .userdata = std.testing.io.userdata,
        .vtable = &vtable,
    };

    var transport = try Transport.initWithOptions(allocator, io, .{ .handle = fake_socket_handle }, .{
        .read_buffer_size = 64,
        .write_queue_max_items = 8,
        .write_queue_max_bytes = 4,
    });
    defer transport.deinit();
    defer state.release.store(true, .release);

    try transport.startWriter();
    try transport.enqueueWrite("abcd");

    for (0..10_000) |_| {
        if (state.entered.load(.acquire)) break;
        std.Thread.yield() catch {};
    }
    try std.testing.expect(state.entered.load(.acquire));

    try std.testing.expectError(error.WriteQueueBytesExceeded, transport.enqueueWrite("x"));

    state.release.store(true, .release);
    transport.stopWriter();
}

test "transport stopWriter shuts down before joining idle writer" {
    const allocator = std.testing.allocator;

    const pair = try createSocketPair();
    defer closeFd(pair[1]);

    var transport = try Transport.init(allocator, std.testing.io, pair[0], 64);
    defer transport.deinit();

    try transport.startWriter();
    transport.stopWriter();

    try std.testing.expect(transport.isClosing());
    try std.testing.expectError(error.BrokenPipe, transport.enqueueWrite("after-stop"));
}

test "connection run treats malformed frame as terminal after on_error" {
    const allocator = std.testing.allocator;

    const pair = try createSocketPair();
    defer closeFd(pair[1]);

    const State = struct {
        error_count: usize = 0,
        close_count: usize = 0,
        message_count: usize = 0,
        last_error: ?anyerror = null,
        closing_seen_in_error: bool = false,

        fn onMessage(conn: *Connection, _: []const u8) !void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.message_count += 1;
        }

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
            state.closing_seen_in_error = conn.transport.isClosing();
        }

        fn onClose(conn: *Connection) void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.close_count += 1;
        }
    };

    const Runner = struct {
        fn run(conn: *Connection) void {
            conn.run();
        }
    };

    var conn = try Connection.init(allocator, std.testing.io, pair[0], .{});
    defer conn.deinit();

    var state = State{};
    conn.start(&state, State.onMessage, State.onError, State.onClose);

    const thread = try std.Thread.spawn(.{}, Runner.run, .{&conn});
    const bad_header = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try writeAll(pair[1], &bad_header);
    thread.join();

    try std.testing.expectEqual(@as(usize, 0), state.message_count);
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.InvalidFrame), state.last_error);
    try std.testing.expect(state.closing_seen_in_error);
    try std.testing.expect(conn.transport.isClosing());
    try std.testing.expectEqual(@as(usize, 1), state.close_count);
}

test "connection run closes after inbound frame allocation OOM" {
    const allocator = std.testing.allocator;

    const pair = try createSocketPair();

    const State = struct {
        error_count: usize = 0,
        close_count: usize = 0,
        last_error: ?anyerror = null,
        closing_seen_in_error: bool = false,

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, err: anyerror) void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            state.last_error = err;
            state.closing_seen_in_error = conn.transport.isClosing();
        }

        fn onClose(conn: *Connection) void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.close_count += 1;
        }
    };

    const Runner = struct {
        fn run(conn: *Connection) void {
            conn.run();
        }
    };

    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 2 });
    var conn = try Connection.init(failing.allocator(), std.testing.io, pair[0], .{});
    defer if (!conn.deinitialized) conn.deinit();

    var state = State{};
    conn.start(&state, State.onMessage, State.onError, State.onClose);

    const frame = try buildReturnResultsFrame(allocator, 99);
    defer allocator.free(frame);

    const thread = try std.Thread.spawn(.{}, Runner.run, .{&conn});
    try writeAll(pair[1], frame);
    closeFd(pair[1]);
    thread.join();

    try std.testing.expectEqual(@as(usize, 1), state.close_count);
    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(?anyerror, error.OutOfMemory), state.last_error);
    try std.testing.expect(state.closing_seen_in_error);
    try std.testing.expect(conn.transport.isClosing());
    try std.testing.expectEqual(@as(usize, 0), conn.framer.bufferedBytes());
}

test "connection deinit requested from error callback is deferred without panic" {
    const allocator = std.testing.allocator;

    const pair = try createSocketPair();
    defer closeFd(pair[1]);

    const State = struct {
        error_count: usize = 0,
        close_count: usize = 0,
        deinit_seen_in_error: bool = false,

        fn onMessage(_: *Connection, _: []const u8) !void {}

        fn onError(conn: *Connection, _: anyerror) void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.error_count += 1;
            conn.deinit();
            state.deinit_seen_in_error = conn.deinit_requested;
        }

        fn onClose(conn: *Connection) void {
            const state: *@This() = @ptrCast(@alignCast(conn.ctx.?));
            state.close_count += 1;
        }
    };

    const Runner = struct {
        fn run(conn: *Connection) void {
            conn.run();
        }
    };

    var conn = try Connection.init(allocator, std.testing.io, pair[0], .{});
    defer if (!conn.deinitialized) conn.deinit();

    var state = State{};
    conn.start(&state, State.onMessage, State.onError, State.onClose);

    const thread = try std.Thread.spawn(.{}, Runner.run, .{&conn});
    const bad_header = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try writeAll(pair[1], &bad_header);
    thread.join();

    try std.testing.expectEqual(@as(usize, 1), state.error_count);
    try std.testing.expectEqual(@as(usize, 1), state.close_count);
    try std.testing.expect(state.deinit_seen_in_error);
    try std.testing.expect(conn.deinitialized);
}

test "transport concurrent close and shutdown call socket shutdown once" {
    const net = std.Io.net;

    const CountingIo = struct {
        const State = struct {
            shutdown_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
            close_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        };

        var active_state: ?*State = null;

        fn netShutdown(_: ?*anyopaque, _: net.Socket.Handle, _: net.ShutdownHow) net.ShutdownError!void {
            _ = active_state.?.shutdown_count.fetchAdd(1, .acq_rel);
        }

        fn netClose(_: ?*anyopaque, _: []const net.Socket.Handle) void {
            _ = active_state.?.close_count.fetchAdd(1, .acq_rel);
        }
    };

    const Runner = struct {
        fn closeTransport(transport: *Transport) void {
            transport.close();
        }
    };

    var state = CountingIo.State{};
    CountingIo.active_state = &state;
    defer CountingIo.active_state = null;

    var vtable = std.testing.io.vtable.*;
    vtable.netShutdown = CountingIo.netShutdown;
    vtable.netClose = CountingIo.netClose;
    const io = std.Io{
        .userdata = std.testing.io.userdata,
        .vtable = &vtable,
    };

    var transport = try Transport.init(std.testing.allocator, io, .{ .handle = fake_socket_handle }, 64);
    var threads: [8]std.Thread = undefined;
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Runner.closeTransport, .{&transport});
    }
    for (threads) |thread| thread.join();
    transport.deinit();

    try std.testing.expectEqual(@as(usize, 1), state.shutdown_count.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), state.close_count.load(.acquire));
}

test "listener concurrent close calls socket close once" {
    const net = std.Io.net;

    const CountingIo = struct {
        const State = struct {
            close_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
            shutdown_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
        };

        var active_state: ?*State = null;

        fn netClose(_: ?*anyopaque, _: []const net.Socket.Handle) void {
            _ = active_state.?.close_count.fetchAdd(1, .acq_rel);
        }
        fn netShutdown(_: ?*anyopaque, _: net.Socket.Handle, _: net.ShutdownHow) net.ShutdownError!void {
            _ = active_state.?.shutdown_count.fetchAdd(1, .acq_rel);
        }
    };

    const Runner = struct {
        fn closeListener(listener: *Listener) void {
            listener.close();
        }
    };

    var state = CountingIo.State{};
    CountingIo.active_state = &state;
    defer CountingIo.active_state = null;

    var vtable = std.testing.io.vtable.*;
    vtable.netClose = CountingIo.netClose;
    vtable.netShutdown = CountingIo.netShutdown;
    const io = std.Io{
        .userdata = std.testing.io.userdata,
        .vtable = &vtable,
    };

    var listener = Listener.initFd(std.testing.allocator, io, .{ .handle = fake_socket_handle }, .{});
    var threads: [8]std.Thread = undefined;
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, Runner.closeListener, .{&listener});
    }
    for (threads) |thread| thread.join();

    // Idempotent across concurrent close(): exactly one shutdown + one close.
    try std.testing.expectEqual(@as(usize, 1), state.shutdown_count.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), state.close_count.load(.acquire));
}
