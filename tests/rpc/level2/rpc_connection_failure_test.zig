const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.cap_table;
const Connection = capnpc.rpc.connection.Connection;
const Peer = peer_impl.Peer;
const Transport = capnpc.rpc.transport.Transport;

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

fn createSocketPair() ![2]std.posix.fd_t {
    if (comptime builtin.target.os.tag == .windows) return error.SocketPairFailed;
    var fds: [2]std.posix.fd_t = undefined;
    if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
        return error.SocketPairFailed;
    }
    return fds;
}

fn closeFd(fd: std.posix.fd_t) void {
    switch (std.posix.errno(std.posix.system.close(fd))) {
        .SUCCESS, .INTR, .BADF => {},
        else => {},
    }
}

fn writeAll(fd: std.posix.fd_t, bytes: []const u8) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        const rc = std.posix.system.write(fd, bytes[offset..].ptr, bytes.len - offset);
        const n: usize = switch (std.posix.errno(rc)) {
            .SUCCESS => @intCast(rc),
            .INTR => continue,
            .PIPE => return error.BrokenPipe,
            else => return error.WriteFailed,
        };
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

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    // Attach a simple transport override.
    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
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
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
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
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
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

    var transport = try Transport.initWithOptions(allocator, io, 0, .{
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
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
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
    if (comptime builtin.target.os.tag == .windows) return error.SkipZigTest;
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
